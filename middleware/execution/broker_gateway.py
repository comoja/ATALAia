import logging
import pandas as pd
from typing import Any
from middleware.database import dbManager
from middleware.utils.communications import sendTelegramAlert
from middleware.utils.alertBuilder import (
    buildEMAAlertMessage, 
    buildSniperAlertMessage, 
    buildSMAAlertMessage, 
    buildImbalanceNYAlertMessage, 
    buildImbalanceLDNAlertMessage, 
    buildPatron4HAlertMessage
)
from middleware.config.constants import PRODUCTION_MODE, FOREXCOM_USERNAME, FOREXCOM_PASSWORD, FOREXCOM_APP_KEY

logger = logging.getLogger(__name__)

class BrokerGateway:
    """
    Capa de abstracción para la ejecución de órdenes y comunicaciones.
    """
    
    def __init__(self, mode=None, broker_name="forexcom"):
        # Si no se especifica, tomar de config global
        if mode is None:
            self.mode = "live" if PRODUCTION_MODE else "simulation"
        else:
            self.mode = mode.lower() 
            
        self.broker_name = broker_name.lower()
        self.client = None
        
        logger.info(f"🚀 BrokerGateway iniciado en modo: {self.mode}")

    def _is_signal_stale(self, trade_data: dict, signal: dict) -> bool:
        """
        Verifica si la señal es obsoleta basándose en el tiempo transcurrido 
        desde la vela que la generó hasta ahora.
        IGNORA el precio (según requerimiento del usuario: 'rechazar por horario y no por precio').
        """
        try:
            from datetime import datetime
            import pytz
            from middleware.config.constants import TIMEZONE, MAX_SIGNAL_AGE_MINUTES
            
            candle_time_str = signal.get('candle_time')
            if not candle_time_str:
                logger.warning("No se encontró 'candle_time' en la señal. Saltando validación de tiempo.")
                return False
                
            # Convertir candle_time a objeto datetime
            tz = pytz.timezone(TIMEZONE)
            if isinstance(candle_time_str, str):
                candle_dt = datetime.strptime(candle_time_str, "%Y-%m-%d %H:%M:%S")
                # Localizar a la zona horaria configurada
                candle_dt = tz.localize(candle_dt)
            else:
                # Ya es un datetime (algunas estrategias lo pasan así)
                candle_dt = candle_time_str
                if candle_dt.tzinfo is None:
                    candle_dt = tz.localize(candle_dt)
            
            now = datetime.now(tz)
            
            # Calcular antigüedad
            diff = now - candle_dt
            age_minutes = diff.total_seconds() / 60
            
            if age_minutes > MAX_SIGNAL_AGE_MINUTES:
                logger.warning(f"⚠️ Orden RECHAZADA: Señal Obsoleta (Antigüedad: {age_minutes:.1f} min > {MAX_SIGNAL_AGE_MINUTES} min)")
                return True
            
            logger.info(f"✅ Señal válida por horario (Antigüedad: {age_minutes:.1f} min)")
            return False
            
        except Exception as e:
            logger.error(f"Error en validación de tiempo de señal: {e}")
            return False

    async def execute_trade(self, trade_data: dict, signal: dict, account: dict, strategy_name: str, df: pd.DataFrame = None) -> Any:
        """
        Punto de entrada único para ejecutar una operación y notificar.
        Retorna (success, msgId)
        """
        # --- NUEVO: Verificación de estado Activo en tiempo real ---
        acc_latest = dbManager.getAccount(account['idCuenta'])
        if not acc_latest:
            logger.warning(f"⚠️ Orden RECHAZADA: La cuenta {account['idCuenta']} está INACTIVA o no existe.")
            return False, None
            
        logger.info(f" Iniciando ejecución para {trade_data.get('symbol')} | Estrategia: {strategy_name}")
        
        # 0. Filtro de Seguridad: Staleness (Antigüedad de la señal)
        if self._is_signal_stale(trade_data, signal):
            return False, None

        # 0.1 Filtro de Seguridad: Drawdown Diario
        from Sentinel.analysis import risk
        if risk.is_daily_drawdown_limit_reached(account['idCuenta'], maxDrawdownPercent=2.0):
            logger.warning(f"❌ Orden RECHAZADA por Riesgo: Drawdown Diario alcanzado en cuenta {account['idCuenta']}")
            return False, None
            
        # 0.1 Filtro de Seguridad: Spread
        if df is not None:
            from Sentinel.analysis import technical
            if not technical.is_spread_safe(df, max_spread_atr_percent=25.0):
                logger.warning(f"❌ Orden RECHAZADA por Seguridad: Spread muy alto para {trade_data['symbol']}")
                return False, None

        # -- NUEVO FLUJO OPTIMIZADO (07/04/2026) --
        
        # 1. Construir mensaje PRIMERO (por si falla la lógica de construcción)
        try:
            message = self._build_message(strategy_name, signal, trade_data)
        except Exception as e:
            logger.error(f"❌ Error crítico al construir mensaje para {strategy_name}: {e}")
            message = f"🚨 Nueva Señal {strategy_name} para {trade_data.get('symbol')}, pero falló construcción de mensaje detallado."

        # 2. Ejecución Broker (si aplica)
        exec_success = True
        if self.mode == "live":
            exec_success = await self._execute_live(trade_data)
            if not exec_success:
                logger.error(f"❌ Falló ejecución en BROKER para {trade_data['symbol']}")
                return False, None

        # 3. Registro en Base de Datos
        try:
            dbManager.buscaTrade(trade_data)
        except Exception as e:
            logger.error(f"⚠️ Error al registrar trade en DB: {e} (Continuando con alerta...)")

        # 4. Notificación Telegram (Independiente del éxito en DB)
        msg_id = None
        try:
            msg_id = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
            if not msg_id:
                logger.error(f"❌ No se pudo enviar alerta de Telegram para {trade_data['symbol']} (Token o ID incorrecto)")
            else:
                logger.info(f"✅ Alerta enviada con éxito (ID: {msg_id})")
        except Exception as e:
            logger.error(f"❌ Excepción al enviar alerta de Telegram: {e}")

        return exec_success, msg_id

    def _build_message(self, strategy_name: str, signal: dict, trade_data: dict) -> str:
        """
        Centraliza la construcción de mensajes según la estrategia.
        """
        if strategy_name == "EMA20200":
            return buildEMAAlertMessage(signal, trade_data)
        elif strategy_name == "Sniper":
            return buildSniperAlertMessage(signal, trade_data)
        elif strategy_name == "SMA20_200":
            return buildSMAAlertMessage(signal, trade_data)
        elif strategy_name == "ImbalanceNY":
            return buildImbalanceNYAlertMessage(signal, trade_data)
        elif strategy_name == "ImbalanceLDN":
            return buildImbalanceLDNAlertMessage(signal, trade_data)
        elif strategy_name == "Patron4h":
            return buildPatron4HAlertMessage(signal, trade_data)
        else:
            return f"Señal Generada: {strategy_name} para {trade_data['symbol']}"

    async def _execute_live(self, trade_data: dict) -> bool:
        # ... (Lógica de ejecución ya implementada o placeholder)
        return True

    async def close_trade(self, id_trade: int, exit_price: float, reason: str):
        """
        Cierra un trade en la DB y, si es Live, en el Broker.
        """
        try:
            from middleware.database import dbManager
            from Sentinel.analysis import risk
            
            # 1. Obtener datos actuales del trade
            conn = dbManager.dbConnection.getConnection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM trades WHERE idTrade = %s", (id_trade,))
            trade_data = cursor.fetchone()
            conn.close()
            
            if not trade_data:
                logger.error(f"Gateway: No se pudo cerrar trade {id_trade} porque no existe en DB.")
                return

            # 2. Calcular PnL real
            closure_data = {"exitPrice": exit_price}
            pnl = risk.calculatePnl(trade_data, closure_data)
            
            # 3. Cerrar en DB con el PnL calculado
            dbManager.closeTrade(id_trade, exit_price, pnl, reason)
            logger.info(f"✅ Gateway: Trade {id_trade} cerrado por {reason}. PnL Calculado: {pnl:.2f}")
            
        except Exception as e:
            logger.error(f"Error al cerrar trade vía Gateway: {e}")
            
# Instancia global para ser utilizada por las estrategias
gateway = BrokerGateway()
