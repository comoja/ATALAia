import logging
import pandas as pd
from typing import Any
from middleware.database import dbManager
from middleware.utils.loggerConfig import setupLogging
from middleware.utils.communications import sendTelegramAlert
from datetime import datetime
import pytz
from middleware.config.constants import TIMEZONE, MAX_SIGNAL_AGE_MINUTES
from middleware.utils.alertBuilder import (
    buildEMAAlertMessage, 
    buildSniperAlertMessage, 
    buildSMAAlertMessage, 
    buildImbalanceNYAlertMessage, 
    buildImbalanceLDNAlertMessage, 
    buildPatron4HAlertMessage,
    buildSesgoBiasHTFAlertMessage,
    buildSilverBulletAlertMessage,
    buildImbalancePMNYAlertMessage,
    buildGenericFVGAlertMessage,
    buildFVGDiarioAlertMessage,
    buildSpeedBotAlertMessage,
    buildBreakoutNYAlertMessage,
    buildIchimokuAlertMessage
)
from middleware.config.constants import PRODUCTION_MODE, FOREXCOM_USERNAME, FOREXCOM_PASSWORD, FOREXCOM_APP_KEY
from middleware.database import dbManager as _db

setupLogging("execution")
logger = logging.getLogger("execution")

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

    # Mapa de intervalo a minutos (centralizado para toda la clase)
    INTERVAL_MINUTES_MAP = {
        '1min': 1, '5min': 5, '15min': 15, '30min': 30,
        '1h': 60, '2h': 120, '4h': 240, '1d': 1440
    }

    def _isSignalStale(self, trade_data: dict, signal: dict) -> bool:
        """
        Verifica si la señal es obsoleta basándose en el número de velas transcurridas
        desde la vela que la generó hasta ahora.

        Criterio: antigüedad > max_age_candles × duración_del_intervalo_en_minutos
        Esto evita que un FVG de 4H sea rechazado con el mismo umbral que uno de 5min.

        - max_age_candles: configurable por estrategia en DB (default 3 velas)
        - El intervalo se toma de signal['intervalo'] o trade_data['intervalo']
        - Fallback a MAX_SIGNAL_AGE_MINUTES si no hay intervalo disponible
        """
        try:
            

            candleTimeStr = signal.get('candleTime')
            if not candleTimeStr:
                logger.warning("No se encontró 'candleTime' en la señal. Saltando validación de tiempo.")
                return False

            # --- Calcular umbral dinámico en función del intervalo ---
            strategyName = trade_data.get('strategy', signal.get('strategy', ''))
            stratConfig  = {}
            try:
                
                stratConfig = _db.getStrategyConfig(strategyName) or {}
            except Exception:
                pass

            maxAgeCandles   = int(stratConfig.get('max_age_candles', 5))
            intervalo       = signal.get('intervalo') or trade_data.get('intervalo', '')
            intervalMinutes = self.INTERVAL_MINUTES_MAP.get(intervalo, 0)

            if intervalMinutes > 0:
                maxAgeMinutes = maxAgeCandles * intervalMinutes
            else:
                # Fallback a constante global si no se puede determinar el intervalo
                maxAgeMinutes = MAX_SIGNAL_AGE_MINUTES
                logger.debug(f"Intervalo '{intervalo}' no reconocido, usando fallback {maxAgeMinutes} min")

            # --- Parsear candleTime ---
            tz = pytz.timezone(TIMEZONE)
            if isinstance(candleTimeStr, str):
                try:
                    if '+' in candleTimeStr or (candleTimeStr.count('-') > 2 and '-' in candleTimeStr[-6:]):
                        candleTimeStr = candleTimeStr.split('-')[0].rstrip()
                    if ' ' not in candleTimeStr or len(candleTimeStr) < 10:
                        logger.warning(f"Formato de tiempo inválido: {candleTimeStr}")
                        return False
                    candleDt = datetime.strptime(candleTimeStr, "%Y-%m-%d %H:%M:%S")
                    candleDt = tz.localize(candleDt)
                except Exception as e:
                    logger.error(f"Error parsing tiempo '{candleTimeStr}': {e}")
                    return False
            else:
                candleDt = candleTimeStr
                if candleDt.tzinfo is None:
                    candleDt = tz.localize(candleDt)

            # --- Verificar antigüedad ---
            ageMinutes = (datetime.now(tz) - candleDt).total_seconds() / 60
            ageCandles = ageMinutes / intervalMinutes if intervalMinutes > 0 else None

            if ageMinutes > maxAgeMinutes:
                candlesStr = f"{ageCandles:.1f} velas" if ageCandles else f"{ageMinutes:.1f} min"
                logger.warning(
                    f"⚠️ Orden RECHAZADA: Señal Obsoleta "
                    f"(Antigüedad: {ageMinutes:.1f} min [{candlesStr}] "
                    f"> máx {maxAgeMinutes} min [{maxAgeCandles} velas de {intervalo}])"
                )
                return True

            logger.info(
                f"✅ Señal válida por horario "
                f"(Antigüedad: {ageMinutes:.1f} min | "
                f"Máx: {maxAgeMinutes} min [{maxAgeCandles} velas de {intervalo or 'N/A'}])"
            )
            return False

        except Exception as e:
            logger.error(f"Error en validación de tiempo de señal: {e}")
            return False


    def _is_entry_price_valid(self, signal: dict, df: pd.DataFrame) -> bool:
        """
        Verifica si el precio de entrada estuvo vigente dentro del timeframe de confirmación.
        Si 'timeframe_confirmacion' es '1H', el precio debe haber estado dentro del rango
        de las últimas 4 velas de 15min. Similar para '4H' (16 velas).
        """
        try:
            if df is None or df.empty:
                logger.warning("No hay datos de velas para validar precio de entrada")
                return True
            
            tf_confirm = signal.get('timeframe_confirmacion', signal.get('timeframe_entrada', '1H'))
            entry_price = signal.get('entryPrice', signal.get('entrada'))
            takeProfit = signal.get('takeProfit', signal.get('tp'))
            stopLoss = signal.get('stopLoss', signal.get('sl')) 
            
            if entry_price is None:
                logger.warning("No se encontró precio de entrada en la señal")
                return True
            
            velas_map = {'1H': 4, '4H': 16, '1D': 96, '15M': 1}
            num_velas = int(velas_map.get(tf_confirm, 4) * 1.5)
            
            num_velas = min(num_velas, len(df))
            if num_velas == 0:
                return True
            
            recientes = df.iloc[-num_velas:]
            min_price = float(recientes['low'].min())
            max_price = float(recientes['high'].max())
            
            if min_price <= entry_price <= max_price:
                logger.info(f"✅ Precio entrada {entry_price} vigente en {tf_confirm} ({num_velas} velas)")
                logger.info(f"   Rango de velas recientes: [{min_price:.5f}, {max_price:.5f}] | TP: {takeProfit} | SL: {stopLoss}")
                return True
            else:
                logger.warning(f"⚠️ Orden RECHAZADA: Precio entrada {entry_price} NO vigente en {tf_confirm}. Rango: [{min_price:.5f}, {max_price:.5f}]")
                return False
                
            return True
        except Exception as e:
            logger.error(f"Error en validación de precio de entrada: {e}")
            return True

    async def execute_trade(self, trade_data: dict, signal: dict, account: dict, strategy_name: str, df: pd.DataFrame = None) -> Any:
        """
        Punto de entrada único para ejecutar una operación y notificar.
        Retorna (success, msgId)
        """
        # --- NUEVO: Verificación de estado Activo en tiempo real ---
        acc_latest = dbManager.getAccount(account['idCuenta'])
        if not acc_latest:
            logger.warning(f"⚠️ Orden RECHAZADA: La cuenta {account['idCuenta']} está INACTIVA o no existe.")
            return False, "cuenta_inactiva"
            
        logger.info(f" Iniciando ejecución para {trade_data.get('symbol')} | Estrategia: {strategy_name}")
        
        # 0. Filtro de Seguridad: Staleness (Antigüedad dinámica por velas del intervalo)
        if self._isSignalStale(trade_data, signal):
            return False, "senal_obseleta"

        # 0.1 Filtro de Seguridad: Precio entrada vigente - COMENTADO PARA PRUEBAS
        # if not self._is_entry_price_valid(signal, df):
        #     return False, "drawdown_superado"

        # 0.2 Filtro de Seguridad: Drawdown Diario
        from Sentinel.analysis import risk
        strategy_config = dbManager.getStrategyConfig(strategy_name)
        max_dd_percent = float(strategy_config.get('max_drawdown_percent', 5.0)) if strategy_config else 5.0
        if risk.isDailyDrawdownLimitReached(account['idCuenta'], maxDrawdownPercent=max_dd_percent):
            logger.warning(f"❌ Orden RECHAZADA por Riesgo: Drawdown Diario >= {max_dd_percent}% en cuenta {account['idCuenta']}")
            return False, "drawdown_superado"
            
        # 0.3 Filtro de Seguridad: Spread
        if df is not None:
            from Sentinel.analysis import technical
            if not technical.is_spread_safe(df, max_spread_atr_percent=25.0):
                logger.warning(f"❌ Orden RECHAZADA por Seguridad: Spread muy alto para {trade_data['symbol']}")
                return False, "drawdown_superado"

        # -- NUEVO FLUJO OPTIMIZADO (07/04/2026) --
        
        # 1. Construir mensaje PRIMERO (por si falla la lógica de construcción)
        try:
            message = self._build_message(strategy_name, signal, trade_data)
            logger.debug(f"[DEBUG] Mensaje construido ({len(message)} chars): {message[:100]}...")
        except Exception as e:
            logger.error(f"❌ Error crítico al construir mensaje para {strategy_name}: {e}")
            message = f"🚨 Nueva Señal {strategy_name} para {trade_data.get('symbol')}, pero falló construcción de mensaje detallado."

        # 2. Ejecución Broker (si aplica)
        exec_success = True
        if self.mode == "live":
            exec_success = await self._execute_live(trade_data)
            if not exec_success:
                logger.error(f"❌ Falló ejecución en BROKER para {trade_data['symbol']}")
                return False, "drawdown_superado"

        # 3. Verificar TRADE DUPLICADO antes de Telegram (mismo symbol, strategy, intervalo, direction, size)
        is_adjustment = signal.get('is_adjustment', False)
        
        if not is_adjustment:
            is_dup = dbManager.is_trade_duplicate(
                trade_data['symbol'],
                strategy_name,
                trade_data.get('intervalo', '15min'),
                trade_data['direction'],
                trade_data['size'],
                trade_data.get('idCuenta')
            )
            if is_dup:
                logger.warning(f"⏭️ Trade duplicado detectado: {trade_data['symbol']} | {strategy_name} | {trade_data['direction']} | size={trade_data['size']} - Omitiendo Telegram y DB")
                return exec_success, "DUPLICATE_TRADE"

        # 3.1 Verificar si la alerta ya fue enviada ANTES de guardar en DB (por símbolo + estrategia + vela + cuenta)
        # Para AJUSTES permitimos re-enviar la alerta aunque la vela sea la misma
        candle_time = trade_data.get('candleTime')
        id_cuenta = trade_data.get('idCuenta')
        if not is_adjustment and candle_time and dbManager.is_alert_sent(trade_data['symbol'], strategy_name, candle_time, id_cuenta):
            logger.info(f"⏭️ Alerta ya enviada para {trade_data['symbol']} | {strategy_name} | cuenta {id_cuenta} | {candle_time} - Omitiendo.")
            return exec_success, "ALREADY_SEND"

        # 4. Notificación Telegram
        msg_id = None
        
        logger.debug(f"[DEBUG] Telegram - Token: {account['TokenMsg'][:10]}... | ChatId: {account['idGrupoMsg']} | Msg length: {len(message)}")
        logger.info(f"[TELEGRAM] Mensaje a enviar: \n{message[:500]}...")
        logger.info(f"[TELEGRAM] 🔐 Token: {account['TokenMsg'][:15]}... | 💬 ChatId: {account['idGrupoMsg']}")
        try:
            msg_id = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
            if not msg_id:
                logger.error(f"❌ No se pudo enviar alerta de Telegram para {trade_data['symbol']} (Token o ID incorrecto)")
            else:
                logger.info(f"✅ Alerta enviada con éxito (ID: {msg_id})")
        except Exception as e:
            logger.error(f"❌ Excepción al enviar alerta de Telegram: {e}")

        # 5. Registro en Base de Datos (solo si se envió Telegram exitosamente)
        if msg_id:
            try:
                if is_adjustment and trade_data.get('idTrade'):
                    dbManager.updateTradeLevels(
                        trade_data['idTrade'], 
                        trade_data['stopLoss'], 
                        trade_data['takeProfit']
                    )
                else:
                    dbManager.buscaTrade(trade_data)
            except Exception as e:
                logger.error(f"⚠️ Error al registrar/actualizar trade en DB: {e} (Continuando con alerta...)")

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
        elif strategy_name.startswith("Patron4h"):
            return buildPatron4HAlertMessage(signal, trade_data)
        elif strategy_name == "SesgoBiasHTF":
            return buildSesgoBiasHTFAlertMessage(signal, trade_data)
        elif strategy_name == "SilverBullet":
            return buildSilverBulletAlertMessage(signal, trade_data)
        elif strategy_name == "ImbalancePMNY":
            return buildImbalancePMNYAlertMessage(signal, trade_data)
        elif strategy_name == "GenericFVG":
            return buildGenericFVGAlertMessage(signal, trade_data)
        elif strategy_name == "FVGDiario":
            return buildFVGDiarioAlertMessage(signal, trade_data)
        elif strategy_name == "SpeedBot":
            return buildSpeedBotAlertMessage(signal, trade_data)
        elif strategy_name == "BreakoutNY":
            return buildBreakoutNYAlertMessage(signal, trade_data)
        elif strategy_name == "Ichimoku":
            return buildIchimokuAlertMessage(signal, trade_data)
        else:
            return f"Señal Generada: {strategy_name} para {trade_data['symbol']}"

    async def _execute_live(self, trade_data: dict) -> bool:
        # ... (Lógica de ejecución ya implementada o placeholder)
        return True

    async def close_trade(self, id_trade: int, exit_price: float, reason: str, capital_anterior: float = None, pnl_anterior: float = None):
        """
        Cierra un trade en la DB y, si es Live, en el Broker.
        """
        import traceback
        try:
            from middleware.database import dbManager, dbConnection
            from Sentinel.analysis import risk
            
            # Log detallado para debug
            logger.info(f"[DEBUG] close_trade llamado: id={id_trade}, reason={reason}, exit_price={exit_price}")
            logger.debug(f"[DEBUG] Stack trace: {traceback.format_stack()[-5:-1]}")
            
            conn = dbConnection.getConnection()
            if conn is None:
                logger.error(f"Gateway: No se pudo obtener conexión para trade {id_trade}")
                return
            
            cursor = conn.cursor(dictionary=True)
            cursor.execute("SELECT * FROM trades WHERE idTrade = %s", (id_trade,))
            trade_data = cursor.fetchone()
            conn.close()
            
            if not trade_data:
                logger.error(f"Gateway: No se pudo cerrar trade {id_trade} porque no existe en DB.")
                return
            
            if trade_data.get('closeTime') is not None:
                logger.debug(f"Gateway: Trade {id_trade} ya está cerrado (closeTime: {trade_data['closeTime']}). Omitiendo.")
                return

            closure_data = {"exitPrice": exit_price}
            pnl = risk.calculatePnl(trade_data, closure_data)
            
            dbManager.closeTrade(id_trade, exit_price, pnl, reason, capital_anterior, pnl_anterior)
            logger.info(f"✅ Gateway: Trade {id_trade} symbol {trade_data['symbol']} cuenta {trade_data['idCuenta']} cerrado por {reason}. PnL Calculado: {pnl:.2f}")
            
        except Exception as e:
            logger.error(f"Error al cerrar trade vía Gateway: {e}")
            
# Instancia global para ser utilizada por las estrategias
gateway = BrokerGateway()
