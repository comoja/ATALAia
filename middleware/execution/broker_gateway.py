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
    buildCruceEMAAlertMessage, 
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
    buildIchimokuAlertMessage,
    buildReversionMediaAlertMessage,
    buildQTrendAlertMessage,
    buildBreakoutProbabilityAlertMessage,
    buildPremiumConfluenceAlertMessage
)
from middleware.config.constants import PRODUCTION_MODE, FOREXCOM_USERNAME, FOREXCOM_PASSWORD, FOREXCOM_APP_KEY, mt5Login, mt5Password, mt5Server

try:
    import MetaTrader5 as mt5
except ImportError:
    mt5 = None

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
            
        symbolName = trade_data.get('symbol')
        symbolConfig = dbManager.getSymbol(symbolName)
        if not symbolConfig or not symbolConfig.get('Activo'):
            logger.warning(f"⚠️ Orden RECHAZADA: El símbolo {symbolName} está INACTIVO en Sentinel.")
            return False, "simbolo_inactivo"
            
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

        # 0.4 Filtro de Seguridad: Slippage y Riesgo Dinámico en Real Time
        if self.mode == "live" and symbolConfig.get('broker'):
            try:
                import MetaTrader5 as mt5
                mt5Symbol = self.findMt5Symbol(symbolName)
                tickInfo = mt5.symbol_info_tick(mt5Symbol)
                if tickInfo:
                    direction = trade_data['direction'].upper()
                    currentPrice = float(tickInfo.ask if direction == "BUY" else tickInfo.bid)
                    intendedPrice = float(trade_data.get('entryPrice', 0))
                    
                    sl = float(trade_data.get('stopLoss', 0))
                    tp = float(trade_data.get('takeProfit', 0))
                    
                    if sl > 0 and tp > 0 and intendedPrice > 0:
                        realRisk = (currentPrice - sl) if direction == "BUY" else (sl - currentPrice)
                        realReward = (tp - currentPrice) if direction == "BUY" else (currentPrice - tp)
                            
                        if realRisk > 0:
                            realRR = realReward / realRisk
                            intendedRR = float(signal.get('rr_ratio', 1.0))
                            
                            min_rr_allowed = 0.80
                            if realRR < min_rr_allowed or realReward <= 0:
                                logger.warning(f"❌ Orden RECHAZADA por Slippage: Precio MT5 {currentPrice} arruina el RR. (Real RR: {realRR:.2f} < {min_rr_allowed})")
                                return False, "slippage_rr_ruined"
                            
                            # Si el deslizamiento es aceptable, ajustamos los valores ANTES de enviar a Telegram
                            trade_data['entryPrice'] = currentPrice
                            signal['entryPrice'] = currentPrice
                            signal['entrada'] = currentPrice
                            signal['entry_price'] = currentPrice
                            signal['rr_ratio'] = realRR
                            
                            from middleware.utils.alertBuilder import getPipMultiplier
                            pip_mult = getPipMultiplier(symbolName)
                            signal['riesgo_pips'] = realRisk * pip_mult
                            
                            oldRiskPips = abs(intendedPrice - sl) * pip_mult
                            if oldRiskPips > 0:
                                riskIncreaseRatio = (realRisk * pip_mult) / oldRiskPips
                                signal['profit'] = float(signal.get('profit', 0)) * riskIncreaseRatio
                                trade_data['margin_used'] = float(trade_data.get('margin_used', 0)) * riskIncreaseRatio
            except Exception as e:
                logger.error(f"Error evaluando Slippage en real-time: {e}")

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
            if symbolConfig.get('broker'):
                exec_success = await self._execute_live(trade_data)
                if not exec_success:
                    logger.error(f"❌ Falló ejecución en BROKER para {trade_data['symbol']}")
                    message = f"⚠️ <b>[BROKER ERROR]</b> No se pudo ejecutar en el Broker.\n\n{message}"
            else:
                logger.info(f"ℹ️ Modo 'live' activo pero el símbolo {symbolName} tiene 'broker' = 0. Se omite ejecución en el bróker (se procesa como simulación).")

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
        
        # Desactivado a petición del usuario: Volvemos a formato Texto para operar más cómodamente
        photoBytes = None
        # try:
        #     from middleware.utils.imageGenerator import generateSignalCard
        #     photoBytes = generateSignalCard(strategy_name, signal, trade_data)
        # except Exception as imgErr:
        #     logger.error(f"❌ Error al generar la tarjeta visual de señal: {imgErr}")
        
        logger.debug(f"[DEBUG] Telegram - Token: {account['TokenMsg'][:10]}... | ChatId: {account['idGrupoMsg']} | Msg length: {len(message)}")
        logger.info(f"[TELEGRAM] Mensaje a enviar: \n{message[:500]}...")
        logger.info(f"[TELEGRAM] 🔐 Token: {account['TokenMsg'][:15]}... | 💬 ChatId: {account['idGrupoMsg']}")
        # 4. Registro en Base de Datos
        trade_inserted = False
        try:
            if is_adjustment and trade_data.get('idTrade'):
                dbManager.updateTradeLevels(
                    trade_data['idTrade'], 
                    trade_data['stopLoss'], 
                    trade_data['takeProfit']
                )
                trade_inserted = True
            else:
                dbManager.buscaTrade(trade_data)
                trade_inserted = True
        except Exception as e:
            if "Duplicate entry" in str(e):
                logger.warning(f"⚠️ Trade duplicado bloqueado por DB (misma vela): {trade_data['symbol']}")
                return # Detener aquí para no enviar alerta duplicada
            else:
                logger.error(f"❌ Error al registrar/actualizar trade en DB: {e}")
                # Aunque haya otro tipo de error, podríamos intentar enviar alerta, o detener. 
                # Dejamos que pase la alerta si no es un duplicado, para que el usuario sepa de la señal.
                trade_inserted = True 

        # 5. Enviar Alerta de Telegram solo si no es duplicado
        if trade_inserted:
            try:
                msg_id = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message, photoBytes=photoBytes)
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
        elif strategy_name in ("SMA20_200", "CruceEMA"):
            return buildCruceEMAAlertMessage(signal, trade_data)
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
        elif strategy_name == "ReversionMedia":
            return buildReversionMediaAlertMessage(signal, trade_data)
        elif strategy_name == "QTrend":
            return buildQTrendAlertMessage(signal, trade_data)
        elif strategy_name == "BreakoutProbability":
            return buildBreakoutProbabilityAlertMessage(signal, trade_data)
        elif strategy_name == "PremiumConfluence":
            return buildPremiumConfluenceAlertMessage(signal, trade_data)
        else:
            return f"Señal Generada: {strategy_name} para {trade_data['symbol']}"

    def connectMt5(self) -> bool:
        """
        Inicializa la conexión con la terminal de MetaTrader 5 y realiza el login.
        """
        if mt5 is None:
            logger.error("❌ El módulo MetaTrader5 no está disponible o no es compatible con esta plataforma.")
            return False
            
        logger.info("Inicializando conexión con MT5...")
        if not mt5.initialize():
            logger.error(f"❌ Error al inicializar MT5: {mt5.last_error()}")
            return False
            
        # Verificar primero si el terminal ya tiene una sesión activa autorizada
        accountInfo = mt5.account_info()
        if accountInfo is not None:
            logger.info(f"✅ Usando sesión activa autorizada en el terminal MT5 (Login: {accountInfo.login}, Servidor: {accountInfo.server})")
            return True
            
        # Si no hay sesión activa, intentamos hacer login programático con las credenciales del .env
        logger.info(f"Intentando login programático a la cuenta {mt5Login} en el servidor {mt5Server}...")
        authorized = mt5.login(mt5Login, password=mt5Password, server=mt5Server)
        if not authorized:
            logger.error(f"❌ Error al autenticar en MT5 con credenciales: {mt5.last_error()}")
            mt5.shutdown()
            return False
            
        logger.info("✅ Conexión con MT5 establecida con éxito vía login programático.")
        return True

    def findMt5Symbol(self, baseSymbol: str) -> str:
        """
        Busca el símbolo correspondiente en MT5 manejando posibles sufijos.
        """
        if mt5 is None:
            return baseSymbol.replace("/", "")
            
        cleanSymbol = baseSymbol.replace("/", "")
        
        # Primero intentamos coincidencia exacta
        symbolInfo = mt5.symbol_info(cleanSymbol)
        if symbolInfo is not None:
            return cleanSymbol
            
        # Si no se encuentra, buscamos en la lista completa de símbolos de MT5
        symbols = mt5.symbols_get()
        if symbols:
            for s in symbols:
                if s.name.startswith(cleanSymbol):
                    logger.info(f"🔍 Símbolo coincidente encontrado: {s.name} para {baseSymbol}")
                    return s.name
                    
        logger.warning(f"⚠️ No se encontró coincidencia exacta ni sufijo para {baseSymbol}. Se usará {cleanSymbol}")
        return cleanSymbol

    def updateLiveSLTP(self, ticketId: int, new_sl: float, mt5Symbol: str = None) -> bool:
        """
        Actualiza el Stop Loss y (opcionalmente Take Profit) de una posición abierta en MT5.
        """
        if mt5 is None:
            logger.error("❌ Módulo MT5 no disponible para actualizar SL/TP.")
            return False
            
        if not self.connectMt5():
            return False
            
        logger.info(f"Actualizando SL de la posición {ticketId} a {new_sl} en MT5...")
        
        # Validar si existe la posición
        positions = mt5.positions_get(ticket=ticketId)
        if not positions:
            logger.warning(f"⚠️ Posición con ticket {ticketId} no encontrada en MT5 al intentar actualizar SL.")
            mt5.shutdown()
            return False
            
        pos = positions[0]
        symbol = mt5Symbol if mt5Symbol else pos.symbol
        
        request = {
            "action": mt5.TRADE_ACTION_SLTP,
            "symbol": symbol,
            "position": ticketId,
            "sl": float(new_sl),
            "tp": float(pos.tp), # Mantener TP actual
        }
        
        result = mt5.order_send(request)
        if result is None:
            logger.error(f"❌ Error enviando actualización SLTP a MT5: {mt5.last_error()}")
            mt5.shutdown()
            return False
            
        if result.retcode != mt5.TRADE_RETCODE_DONE:
            logger.error(f"❌ Actualización SLTP rechazada por MT5. Código: {result.retcode}, Error: {result.comment}")
            mt5.shutdown()
            return False
            
        logger.info(f"✅ Stop Loss actualizado exitosamente en MT5 (Ticket: {ticketId}).")
        mt5.shutdown()
        return True

    def _closeLive(self, tradeData: dict) -> bool:
        """
        Cierra una posición abierta en MT5 usando su ticketId.
        """
        if mt5 is None:
            logger.error("❌ El módulo MetaTrader5 no está disponible o no es compatible con esta plataforma.")
            return False
            
        ticketIdVal = tradeData.get('ticketId')
        if not ticketIdVal:
            logger.warning("⚠️ No se encontró ticketId en los datos del trade para cerrar en MT5.")
            return True
            
        if not self.connectMt5():
            return False
            
        ticketId = int(ticketIdVal)
        logger.info(f"Buscando posición abierta con ticket {ticketId} en MT5...")
        positionsList = mt5.positions_get(ticket=ticketId)
        
        if not positionsList:
            logger.warning(f"⚠️ Posición con ticket {ticketId} no encontrada en MT5. Puede haber sido cerrada manualmente o por SL/TP.")
            mt5.shutdown()
            return True
            
        positionInfo = positionsList[0]
        mt5Symbol = positionInfo.symbol
        orderVolume = positionInfo.volume
        
        # Para cerrar la posición, realizamos una transacción opuesta (BUY -> SELL, SELL -> BUY)
        # y especificamos el ticket de la posición a cerrar.
        tickInfo = mt5.symbol_info_tick(mt5Symbol)
        if tickInfo is None:
            logger.error(f"❌ No se pudo obtener tick info para {mt5Symbol}")
            mt5.shutdown()
            return False
            
        if positionInfo.type == mt5.POSITION_TYPE_BUY:
            orderType = mt5.ORDER_TYPE_SELL
            orderPrice = tickInfo.bid
        else:
            orderType = mt5.ORDER_TYPE_BUY
            orderPrice = tickInfo.ask
            
        # Determinar el tipo de filling compatible
        symbolInfo = mt5.symbol_info(mt5Symbol)
        if symbolInfo is None:
            logger.error(f"❌ No se pudo obtener información de símbolo para {mt5Symbol}")
            mt5.shutdown()
            return False
            
        fillingMode = symbolInfo.filling_mode
        if fillingMode & mt5.SYMBOL_FILLING_FOK:
            typeFilling = mt5.ORDER_FILLING_FOK
        elif fillingMode & mt5.SYMBOL_FILLING_IOC:
            typeFilling = mt5.ORDER_FILLING_IOC
        else:
            typeFilling = mt5.ORDER_FILLING_RETURN
            
        tradeRequest = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": mt5Symbol,
            "volume": orderVolume,
            "type": orderType,
            "position": ticketId,
            "price": orderPrice,
            "deviation": 20,
            "magic": 123456,
            "comment": "Cierre Sentinel",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": typeFilling,
        }
        
        logger.info(f"Enviando orden de cierre a MT5: {tradeRequest}")
        orderResult = mt5.order_send(tradeRequest)
        
        if orderResult is None:
            logger.error(f"❌ Error al cerrar posición {ticketId}: order_send retornó None. Error: {mt5.last_error()}")
            mt5.shutdown()
            return False
            
        if orderResult.retcode != mt5.TRADE_RETCODE_DONE:
            logger.error(f"❌ Orden de cierre rechazada por MT5. Código: {orderResult.retcode}. Error: {orderResult.comment}")
            mt5.shutdown()
            return False
            
        logger.info(f"✅ Posición {ticketId} cerrada exitosamente en MT5.")
        mt5.shutdown()
        return True

    async def _execute_live(self, trade_data: dict) -> bool:
        """
        Ejecuta una orden de compra o venta en MT5.
        """
        if mt5 is None:
            logger.error("❌ El módulo MetaTrader5 no está disponible o no es compatible con esta plataforma.")
            return False
            
        if not self.connectMt5():
            return False
            
        mt5Symbol = self.findMt5Symbol(trade_data['symbol'])
        
        # Seleccionar el símbolo en Market Watch
        if not mt5.symbol_select(mt5Symbol, True):
            logger.error(f"❌ No se pudo seleccionar el símbolo {mt5Symbol} en Market Watch: {mt5.last_error()}")
            mt5.shutdown()
            return False
            
        # Calcular el volumen en lotes (1 lote estándar = 100k unidades en Forex, 100 onzas en Oro)
        cleanSymbol = trade_data['symbol'].replace("/", "")
        if "XAU" in cleanSymbol.upper():
            orderVolume = float(trade_data['size']) / 100.0
        else:
            orderVolume = float(trade_data['size']) / 100000.0
            
        orderVolume = round(orderVolume, 2)
        if orderVolume < 0.01:
            orderVolume = 0.01
            
        # Obtener información del tick actual
        tickInfo = mt5.symbol_info_tick(mt5Symbol)
        if tickInfo is None:
            logger.error(f"❌ No se pudo obtener tick info para {mt5Symbol}: {mt5.last_error()}")
            mt5.shutdown()
            return False
            
        direction = trade_data['direction'].upper()
        if direction == "BUY":
            orderType = mt5.ORDER_TYPE_BUY
            orderPrice = tickInfo.ask
        elif direction == "SELL":
            orderType = mt5.ORDER_TYPE_SELL
            orderPrice = tickInfo.bid
        else:
            logger.error(f"❌ Dirección de orden inválida: {direction}")
            mt5.shutdown()
            return False
            
        # Determinar el tipo de filling compatible del símbolo
        symbolInfo = mt5.symbol_info(mt5Symbol)
        if symbolInfo is None:
            logger.error(f"❌ No se pudo obtener información del símbolo para {mt5Symbol}")
            mt5.shutdown()
            return False
            
        fillingMode = symbolInfo.filling_mode
        if fillingMode & mt5.SYMBOL_FILLING_FOK:
            typeFilling = mt5.ORDER_FILLING_FOK
        elif fillingMode & mt5.SYMBOL_FILLING_IOC:
            typeFilling = mt5.ORDER_FILLING_IOC
        else:
            typeFilling = mt5.ORDER_FILLING_RETURN
            
        slValue = float(trade_data['stopLoss']) if trade_data.get('stopLoss') else 0.0
        tpValue = float(trade_data['takeProfit']) if trade_data.get('takeProfit') else 0.0
        
        tradeRequest = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": mt5Symbol,
            "volume": orderVolume,
            "type": orderType,
            "price": orderPrice,
            "sl": slValue,
            "tp": tpValue,
            "deviation": 20,
            "magic": 123456,
            "comment": f"Sentinel {trade_data.get('strategy', '')}",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": typeFilling,
        }
        
        logger.info(f"Enviando orden a MT5: {tradeRequest}")
        orderResult = mt5.order_send(tradeRequest)
        
        if orderResult is None:
            logger.error(f"❌ Error al enviar orden a MT5: order_send retornó None. Error: {mt5.last_error()}")
            mt5.shutdown()
            return False
            
        if orderResult.retcode != mt5.TRADE_RETCODE_DONE:
            logger.error(f"❌ Orden rechazada por MT5. Código de retorno: {orderResult.retcode}. Error: {orderResult.comment}")
            mt5.shutdown()
            return False
            
        positionTicket = orderResult.position if orderResult.position else orderResult.order
        trade_data['ticketId'] = str(positionTicket)
        logger.info(f"✅ Orden ejecutada con éxito en MT5. Ticket Posición: {positionTicket}")
        
        mt5.shutdown()
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

            # Cierre en MT5 si es en vivo
            if self.mode == "live" and trade_data.get("ticketId"):
                logger.info(f"Cerrando trade en MT5 para la posición {trade_data.get('ticketId')}...")
                closeSuccess = self._closeLive(trade_data)
                if not closeSuccess:
                    logger.error(f"❌ Error al cerrar trade {id_trade} en el broker MT5. Abortando cierre en DB.")
                    return

            closure_data = {"exitPrice": exit_price}
            pnl = risk.calculatePnl(trade_data, closure_data)
            
            dbManager.closeTrade(id_trade, exit_price, pnl, reason, capital_anterior, pnl_anterior)
            logger.info(f"✅ Gateway: Trade {id_trade} symbol {trade_data['symbol']} cuenta {trade_data['idCuenta']} cerrado por {reason}. PnL Calculado: {pnl:.2f}")

            
        except Exception as e:
            logger.error(f"Error al cerrar trade vía Gateway: {e}")
            
# Instancia global para ser utilizada por las estrategias
gateway = BrokerGateway()
