"""
Main entry point for the refactored Trading Bot.
"""
import asyncio 
import logging
import sys
import os
import time
import pandas as pd
import pytz
import numpy as np
import talib as ta
from datetime import datetime

# --- Path Setup ---
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

# --- Module Imports ---
from Sentinel.utils.loggerConfig import setupLoggingSentinel as setupLogging
from Sentinel.core.Sniper import SniperBot
from Sentinel.core.SMA20_200 import SMABot
from Sentinel.core.ImbalanceNY import ImbalanceNYBot
from Sentinel.core.ImbalanceLDN import ImbalanceLDNBot
from Sentinel.core.EMA20200 import EMA20200Bot
from Sentinel.core.Patron4h import Patron4HBot
from Sentinel.core.SesgoBiasHTF import SesgoBiasHTFBot
from Sentinel.core.SilverBullet import SilverBulletBot
from Sentinel.core.ImbalancePMNY import ImbalancePMNYBot
from Sentinel.core.GenericFVG import GenericFVGBot
from Sentinel.core.FVGDiario import FVGDiarioBot
from Sentinel.core.SpeedBot import SpeedBot
from Sentinel.ml import model as mlModel
from Sentinel.analysis.technical import calculateFeatures, resample_to_interval
from Sentinel.analysis import risk
from middleware.utils.momentum import momentum as momentumAnalyzer, _enviar_resumen_inicial
from middleware.api.finnhub_client import getLatestMarketNews, getHighImpactEvents
from middleware.utils.aiManager import getMarketSentiment

try:
    from middleware.config.constants import DATA_SOURCE
except ImportError:
    DATA_SOURCE = "db"


from middleware.config import constants as config
from middleware.database import dbManager
from middleware.database.dbManager import get_min_wait_time
from middleware.utils.communications import sendTelegramAlert, alertaInmediata, deleteTelegramMessage   

# --- External Project Imports ---
from middleware.scheduler.autoScheduler import getTiempoEspera, isRestTime
from Sentinel.data.dataLoader import getParametros
from middleware.config import settings
from middleware.config.constants import API_KEYS, FESTIVOS, TIMEZONE
from Sentinel.core.execution import ExecutionEngine
from middleware.execution.broker_gateway import gateway

TIMEZONE_LOCAL = pytz.timezone(TIMEZONE)
MAX_CANDLES_PER_CALL = 5000

from middleware.utils.time_utils import get_localized_session_times, isRestTime, get_sleep_minutes, get_seconds_to_next_sync
from middleware.api import twelvedata as tdApi

INTERVAL = settings.INTERVAL
INTERVALmax = settings.INTERVALmax
# Flag para enviar resumen solo una vez
_resumen_momentum_enviado = False
_momentum_data_cache = {}  # Cache para收集 datos de momentum
_weekly_trend_cache = {}  # Cache para tendencia semanal por símbolo
diasTendencia = 21


# --- Funciones de Tendencia ---
def _linear_regression_slope(series):
    """Calcula la pendiente de regresión lineal"""
    x = np.arange(len(series))
    y = series.values
    slope, _ = np.polyfit(x, y, 1)
    return slope

def _classify_trend(df_daily: pd.DataFrame, symbol: str = None) -> dict:
    """
    Clasifica tendencia usando últimas velas D1 con:
    - EMA 20
    - ATR (volatilidad)
    - Regresión lineal (pendiente normalizada por ATR)
    """
    if df_daily is None or len(df_daily) < diasTendencia:
        return {"trend": "NEUTRAL", "strength": 0, "slope": 0, "price": 0, "ema20": 0, "atr": 0}
    
    # Normalizar columnas a minúsculas
    rename_map = {}
    for col in df_daily.columns:
        if col.lower() in ['high', 'low', 'open', 'close']:
            rename_map[col] = col.lower()
    if rename_map:
        df_daily = df_daily.rename(columns=rename_map)
    
    # Usar últimas diasTendencia velas
    df = df_daily.tail(diasTendencia).copy()
    
    if len(df) < diasTendencia:
        return {"trend": "NEUTRAL", "strength": 0, "slope": 0, "price": 0, "ema20": 0, "atr": 0}

    
    # Calcular EMA 20 y ATR
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['atr'] = ta.ATR(df['high'], df['low'], df['close'], timeperiod=14)
    
    # Obtener valores actuales
    last_price = float(df['close'].iloc[-1])
    last_ema = float(df['ema20'].iloc[-1])
    last_atr = float(df['atr'].iloc[-1])
    
    # Calcular pendiente
    slope = _linear_regression_slope(df['close'])
    
    # Normalizar pendiente por ATR (strength)
    strength = slope / last_atr if last_atr != 0 else 0
    
    # Clasificar - solo por signo de pendiente (simple)
    # Pendiente positiva = ALCISTA, negativa = BAJISTA
    if slope > 0:
        trend = "ALCISTA"
    elif slope < 0:
        trend = "BAJISTA"
    else:
        trend = "NEUTRAL"
    
    result = {
        "trend": trend,
        "strength": round(strength, 4),
        "slope": round(slope, 4),
        "price": round(last_price, 5),
        "ema20": round(last_ema, 5),
        "atr": round(last_atr, 5)
    }
    
    logger.debug(f"[{symbol or 'UNKNOWN'}] Tendencia: {trend} | slope={slope:.4f}, price={last_price:.5f}, ema20={last_ema:.5f}")
    
    return result

def _calculate_monthly_trend(df_daily: pd.DataFrame, symbol: str = None) -> str:
    """Legacy wrapper - retorna solo el trend string"""
    return _classify_trend(df_daily, symbol)["trend"]

def _calculate_monthly_trend_debug(df_daily: pd.DataFrame, symbol: str = None) -> str:
    """Versión con debug de _calculate_monthly_trend"""
    result = _calculate_monthly_trend(df_daily)
    logger.info(f"[{symbol or 'UNKNOWN'}] Tendencia: {result}")
    return result

async def _load_monthly_trends(symbolsToScan, apiKey):
    """
    Calcula la tendencia mensual para cada símbolo UNA SOLA VEZ al inicio.
    Resamplea velas 5min a 1D y evalúa los últimos 30 días.
    Para 30 días necesitamos ~6500 velas de 5min.
    """
    global _weekly_trend_cache
    _weekly_trend_cache = {}
    
    logger.info("✅ Cargando tendencias mensuales (últimos {} dias)...".format(diasTendencia))
    msg = ""
    for symbolInfo in symbolsToScan:
        symbol = symbolInfo['symbol']
        try:
            # Descargar velas 5min - 6500 para cubrir 30 días
            params = {"symbol": symbol, "interval": "5min", "apikey": apiKey, "outputSize": 6500 if  DATA_SOURCE == "db" else MAX_CANDLES_PER_CALL}
            df_5m = await tdApi.getTimeSeries(params)
            
            if df_5m is not None and len(df_5m) >= 200:
                # Asegurar columnas minúsculas
                df_5m = df_5m.dropna(subset=['close', 'high', 'low', 'open'])
                
                # Resamplear a 1D
                df_1d = resample_to_interval(df_5m, "1d")
                
                if df_1d is not None and len(df_1d) >= 5:
                    trend = _calculate_monthly_trend(df_1d, symbol)
                    _weekly_trend_cache[symbol] = trend
                    logger.info(f"  [{symbol}] Tendencia de los {diasTendencia} ultimos dias: {trend}")
                    msg += f"\n  [{symbol}]: {trend}"
                else:
                    _weekly_trend_cache[symbol] = "NEUTRAL"
                    logger.warning(f"  [{symbol}] Sin datos 1D suficientes tras resampleo")
            else:
                _weekly_trend_cache[symbol] = "NEUTRAL"
                logger.warning(f"  [{symbol}] Sin datos 5min suficientes")
        except Exception as e:
            logger.error(f"  [{symbol}] Error calculando tendencia: {e}")
            _weekly_trend_cache[symbol] = "NEUTRAL"
    await alertaInmediata(1,f"<b>Tendencia de {diasTendencia} dias</b> {msg}", False)
    logger.info(f"✅ Tendencias mensuales cargadas para {len(_weekly_trend_cache)} símbolos")

def get_weekly_trend(symbol: str) -> str:
    """Obtiene la tendencia mensual cacheada para un símbolo."""
    return _weekly_trend_cache.get(symbol, "NEUTRAL")

async def preload_time_series_data(symbolsToScan, apiKey, interval, nVelas):
    """
    Obtiene los datos de time series una sola vez para todos los símbolos.
    Retorna un diccionario: {symbol: dataframe}
    """
    preloaded_data = {}
    for symbolInfo in symbolsToScan:
        symbol = symbolInfo['symbol']
        logger.info(f"Obteniendo datos de 12Data para {symbol} (intervalo base 5min)...")
        df = await tdApi.getTimeSeries({"symbol": symbol, "interval": "5min", "apikey": apiKey, "outputSize": nVelas})
        if df is not None and len(df) >= 100:
            preloaded_data[symbol] = df
        else:
            logger.warning(f"[{symbol}] Datos insuficientes ({len(df) if df is not None else 0} velas).")
    return preloaded_data

async def run_sequential_analysis(engine, sniper_bot, sma_bot, imbalance_ny_bot, imbalance_ldn_bot, imbalance_pm_bot, ema20200_bot, patron4_h_bot, sesgo_bias_htf_bot, silver_bullet_bot, generic_fvg_bot, fvg_diario_bot, speed_bot, symbolsToScan, apiKey, interval, nVelas, marketSentiment=0.0, marketSentiment_crypto=0.0, imminentNews=None):
    """
    Ejecuta el análisis de forma secuencial y centraliza la ejecución vía ExecutionEngine.
    """
    MIN_WAIT_SECONDS = get_min_wait_time()
    from middleware.utils.communications import alertaInmediata as _alertaInmediata
    all_signals = []

    # Cargar cuenta de referencia para sizing de señales
    
    refAccount = dbManager.getAccountById(2)
    if not refAccount:
        cuentas = dbManager.getAccount()
        refAccount = next((a for a in cuentas if a['idCuenta'] != 1), None)
    
    refCapital = float(refAccount['Capital']) if refAccount and refAccount.get('Capital') else 10000.0
    refRiskPct = float(refAccount['riesgoPorOperacion']) if refAccount and refAccount.get('riesgoPorOperacion') else 1.0

    for idx, symbolInfo in enumerate(symbolsToScan):
        symbol = symbolInfo['symbol']
        start_time = time.time()
        
        # Obtener API key para este símbolo (rota entre cuentas)
        symbolApiKey, _, nombreKey, _, _ = getParametros()
        logger.info(f"Procesando {symbol} ({idx+1}/{len(symbolsToScan)}) con cuenta {nombreKey}...", extra={"color": "orange"})
        
        # 1. Descargar datos
        params = {"symbol": symbol, "interval": "5min", "apikey": symbolApiKey, "outputSize": MAX_CANDLES_PER_CALL}
        df = await tdApi.getTimeSeries(params)
        
        if df is None or len(df) < 200:
            logger.warning(f"[{symbol}] Datos insuficientes. Saltando...")
            if idx < len(symbolsToScan) - 1: await asyncio.sleep(MIN_WAIT_SECONDS)
            continue
        
        # --- Punto 3: Diccionario Maestro de Datos (Optimización Pandas) ---
        # Calculamos resampleos y features UNA SOLA VEZ para todos los bots
        df_5m = df.dropna(subset=['close', 'high', 'low', 'open'])
        
        # --- Revisar trades abiertos para este símbolo ---
        try:
            
            open_trades = dbManager.getOpenTradesBySymbol(symbol)
            for trade in open_trades:
                # Filtrar velas desde la hora de apertura del trade
                trade_open_time = trade.get('candleTime') or trade.get('openTime')
                if trade_open_time and len(df_5m) > 0:
                    # Asegurar que ambas fechas sean comparables
                    trade_time = pd.to_datetime(trade_open_time)
                    if df_5m.index.tz is not None and trade_time.tz is None:
                        trade_time = trade_time.tz_localize(df_5m.index.tz)
                    df_since_trade = df_5m[df_5m.index >= trade_time]
                else:
                    df_since_trade = df_5m
                
                sl_val = trade.get('stopLoss') or 0
                tp_val = trade.get('takeProfit') or 0
                logger.info(f"Revisando trade {trade.get('idTrade')} - SL={sl_val:.5f}, TP={tp_val:.5f}, direction={trade.get('direction')}, desde={trade_open_time}")
                closure = risk.checkTradeClosure(df_since_trade, trade)
                if closure and closure.get('status') == 'CLOSED':
                    reason = closure.get('reason', 'UNKNOWN')
                    exit_price = closure.get('exitPrice')
                    logger.info(f"[{symbol}] Trade {trade['idTrade']} alcanzando {reason} - Cerrando...")
                    await gateway.close_trade(trade['idTrade'], exit_price, reason)
        except Exception as e:
            logger.error(f"Error revisando trades abiertos para {symbol}: {e}")
        
        # Enriquecer con info de riesgo y sentimiento para sizing de señales
        symbolInfo['refCapital'] = refCapital
        symbolInfo['refRiskPct'] = refRiskPct
        symbolInfo['marketSentiment'] = marketSentiment
        symbolInfo['imminentNews'] = imminentNews

        # Asegurar consistencia de zona horaria (tz-aware)
        cdmxTz = pytz.timezone(TIMEZONE)

        if df_5m.index.tzinfo is None:
            df_5m.index = df_5m.index.tz_localize(cdmxTz)
        else:
            df_5m.index = df_5m.index.tz_convert(cdmxTz)

        # --- FILTRO: Velas Terminadas 5min ---
        from middleware.utils.time_utils import get_last_closed_candle
        lastClosed5m = get_last_closed_candle(datetime.now(cdmxTz), 5)
        df_5m = df_5m[df_5m.index <= lastClosed5m]


        df_5m = calculateFeatures(df_5m)

        
        df_15m = calculateFeatures(resample_to_interval(df_5m, "15min"))
        df_1h  = calculateFeatures(resample_to_interval(df_5m, "1h"))
        df_4h  = calculateFeatures(resample_to_interval(df_5m, "4h"))
        df_1d  = calculateFeatures(resample_to_interval(df_5m, "1d"))

        preloaded_master = {
            '5min':  df_5m,
            '15min': df_15m,
            '1h':    df_1h,
            '4h':    df_4h,
            '1d':    df_1d
        }

        # Momentum calculation (usando el master)
        try:
            estadosMomentum = await momentumAnalyzer(symbol, df_5m)
            symbolInfo['momentum'] = estadosMomentum
        except:
            symbolInfo['momentum'] = None
        
        # Enriquecer momentum por TF (usando el master)
        from middleware.utils.momentum import calcularAngulos, obtenerEstado
        
        last_15m = calcularAngulos(df_15m.copy()).iloc[-1]
        momentum_15m, _ = obtenerEstado(last_15m.get('ang_rsi'), last_15m.get('ang_close'))
        
        last_1h = calcularAngulos(df_1h.copy()).iloc[-1]
        momentum_1h, _ = obtenerEstado(last_1h.get('ang_rsi'), last_1h.get('ang_close'))
        
        last_4h = calcularAngulos(df_4h.copy()).iloc[-1]
        momentum_4h, _ = obtenerEstado(last_4h.get('ang_rsi'), last_4h.get('ang_close'))

        symbolInfo['momentum'] = momentum_15m
        symbolInfo['momentum_by_tf'] = {'5min': symbolInfo.get('momentum'), '15min': momentum_15m, '1h': momentum_1h, '4h': momentum_4h}
        symbolInfo['weekly_trend'] = get_weekly_trend(symbol)
        
        
        # --- Preparación de Tareas en Paralelo (Punto 2: Optimización) ---
        tasks = []
        
        # 1. Sniper & SMA (15min)
        tasks.append(sniper_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}, symbolApiKey))
        tasks.append(sma_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}, symbolApiKey))

        # 2. Imbalances (Pre-cálculo de niveles para seguridad en paralelo)
        ahoraMX = datetime.now(pytz.timezone(TIMEZONE))
        iNY, fNY, cNY = get_localized_session_times('America/New_York', 8, 0, 9, 0, 14, 0)
        if ahoraMX > fNY:
            symbolInfo_NY = symbolInfo.copy()
            symbolInfo_NY['precioMaximo'], symbolInfo_NY['precioMinimo'] = df_5m.loc[(df_5m.index >= iNY) & (df_5m.index < fNY)]['high'].max(), df_5m.loc[(df_5m.index >= iNY) & (df_5m.index < fNY)]['low'].min()
            tasks.append(imbalance_ny_bot.runAnalysisCycleForSymbol(symbolInfo_NY, {symbol: preloaded_master}, symbolApiKey))
            
        iLDN, fLDN, cLDN = get_localized_session_times('Europe/London', 8, 0, 9, 0, 14, 0)
        if ahoraMX > fLDN:
            symbolInfo_LDN = symbolInfo.copy()
            symbolInfo_LDN['precioMaximo'], symbolInfo_LDN['precioMinimo'] = df_5m.loc[(df_5m.index >= iLDN) & (df_5m.index < fLDN)]['high'].max(), df_5m.loc[(df_5m.index >= iLDN) & (df_5m.index < fLDN)]['low'].min()
            tasks.append(imbalance_ldn_bot.runAnalysisCycleForSymbol(symbolInfo_LDN, {symbol: preloaded_master}, symbolApiKey))

        # 3. EMA, Patron4H, Sesgo, SB, FVGs
        tasks.append(ema20200_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        tasks.append(patron4_h_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        tasks.append(sesgo_bias_htf_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        tasks.append(silver_bullet_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        tasks.append(generic_fvg_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        tasks.append(fvg_diario_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        tasks.append(speed_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        


        # Ejecutar todas las estrategias en paralelo
        results = await asyncio.gather(*tasks)

        # Recolectar señales
        for res in results:
            if not res: continue
            if isinstance(res, list):
                all_signals.extend(res)
            else:
                all_signals.append(res)

        elapsed = time.time() - start_time
        wait_time = max(0, MIN_WAIT_SECONDS - elapsed)
        if wait_time > 0: await asyncio.sleep(wait_time)


    # --- Procesamiento Centralizado de Señales ---
    if all_signals:
        # Deduplicar señales: misma estrategia + símbolo + dirección = una sola ejecución
        seen_keys = set()
        unique_signals = []
        for sig in all_signals:
            key = (sig.strategy, sig.symbol, sig.direction)
            
            # Verificar si ya existe trade en DB (mismo symbol, strategy, direction, status=OPEN)
            try:
               
                existing = dbManager.is_trade_duplicate(
                    sig.symbol, sig.strategy, sig.intervalo, sig.direction, None, None
                )
            except:
                existing = False
            
            if key not in seen_keys and not existing:
                seen_keys.add(key)
                unique_signals.append(sig)
            else:
                if existing:
                    logger.warning(f"⚠️ Trade existente en BD: {sig.strategy} {sig.symbol} {sig.direction}")
                else:
                    logger.warning(f"⚠️ Señal duplicada descartada: {sig.strategy} {sig.symbol} {sig.direction}")
        logger.info(f"Enviando {len(unique_signals)} señales únicas al ExecutionEngine (de {len(all_signals)} generadas)...")
        await engine.processSignals(unique_signals, marketSentiment=marketSentiment, marketSentiment_crypto=marketSentiment_crypto, imminentNews=imminentNews)

setupLogging(enableConsole=True)
logger = logging.getLogger("sentinel")

async def _auto_retrain_ml():
    """Reentrena modelos ML en background (si el modelo tiene >24h)."""
    try:
        import os
        from middleware.config.constants import MODEL_FILE_PATH
        if os.path.exists(MODEL_FILE_PATH):
            mtime = os.path.getmtime(MODEL_FILE_PATH)
            age_hours = (datetime.now().timestamp() - mtime) / 3600
            if age_hours < 24:
                logger.info(f"⏭️ Modelo ML tiene {age_hours:.0f}h - menor a 24h, omitiendo retraining")
                return
        
        from Sentinel.ml import retrain_ml, train_reg_model
        logger.info("🚀 Iniciando auto-reentrenamiento de modelos ML...")
        await retrain_ml.retrain()
        await train_reg_model.train_reg()
        logger.info("✅ Auto-reentrenamiento completado")
    except Exception as e:
        logger.error(f"Error en auto-reentrenamiento: {e}")

async def main():
    logger.info("====== Inicializando Bot de Trading Sentinel (Decoupled) ======")
    
    # Auto-reentrenamiento al iniciar la app (siempre reentrena al inicio)
    from Sentinel.ml.auto_retrain import should_retrain
    from Sentinel.ml import retrain_ml, train_reg_model
    try:
        logger.info("🚀 Iniciando auto-reentrenamiento de modelos ML al inicio...")
        await retrain_ml.retrain()
        await train_reg_model.train_reg()
        logger.info("✅ Auto-reentrenamiento completado")
    except Exception as e:
        logger.error(f"Error en auto-reentrenamiento: {e}")
    
    model = mlModel.loadModel(config.MODEL_FILE_PATH)
    if model is None:
        logger.error("No se pudo cargar el modelo ML.")
        return

    engine = ExecutionEngine()
    sniper_bot = SniperBot(mlModelInstance=model)
    sma_bot = SMABot()
    imbalance_ny_bot = ImbalanceNYBot()
    imbalance_ldn_bot = ImbalanceLDNBot()
    ema20200_bot = EMA20200Bot()
    patron4_h_bot = Patron4HBot()
    sesgo_bias_htf_bot = SesgoBiasHTFBot()
    silver_bullet_bot = SilverBulletBot()
    imbalance_pm_bot  = ImbalancePMNYBot()
    generic_fvg_bot = GenericFVGBot(intervals=['5min', '15min', '1h', '4h'])
    fvg_diario_bot = FVGDiarioBot()
    speed_bot = SpeedBot(intervals=['5min', '15min'])
    
    _weekly_trends_loaded_today = None  # Track fecha de última carga
    _ml_retrained_today = None  # Track fecha de último retraining
    
    while True:
        try:
            if not isRestTime():
                logger.info("Iniciando ciclo de análisis...")
                
                # --- Tareas de Nuevo Día (Solo al abrir el ciclo) ---
                today_str = datetime.now(TIMEZONE_LOCAL).strftime("%Y-%m-%d")
                if _weekly_trends_loaded_today != today_str:
                    logger.info("🆕 Nuevo día detectado - Cargando tendencias mensuales...")
                    apiKey, _, _, nVelas, _ = getParametros()
                    symbolsToScan = dbManager.getSymbols()
                    await _load_monthly_trends(symbolsToScan, apiKey)
                    _weekly_trends_loaded_today = today_str
                else:
                    apiKey, _, _, nVelas, _ = getParametros()
                    symbolsToScan = dbManager.getSymbols()

                # --- Análisis de Sentimiento y Calendario (Sentinel 'Bien Hacha') ---
                # Solo se ejecuta si el mercado está abierto
                headlines_gen = await getLatestMarketNews("general")
                marketSentiment = await getMarketSentiment(headlines_gen)
                
                headlines_cry = await getLatestMarketNews("crypto")
                marketSentiment_crypto = await getMarketSentiment(headlines_cry)
                
                highEvents = await getHighImpactEvents()
                imminentNews = None
                ahora_utc = datetime.now(pytz.UTC)
                for event in highEvents:
                    try:
                        event_time = pd.to_datetime(event.get('time')).tz_convert(pytz.UTC)
                        diff_min = (event_time - ahora_utc).total_seconds() / 60
                        if 0 <= diff_min <= 45:
                            imminentNews = f"⚠️ {event.get('event')} ({event.get('country')}) en {int(diff_min)} min"
                            break
                    except: continue
                
                logger.info(f"AI Sentiment: Gen={marketSentiment:.2f}, Cry={marketSentiment_crypto:.2f} | News: {imminentNews or 'Limpio'}", extra={"color": "cyan"})
                # await alertaInmediata(1,f"<b>Sentimiento AI:</b> {marketSentiment:.2f} <b>Noticias:</b> {imminentNews or 'Limpio'}", False)
                # Auto-reentrenamiento ML (una vez al día, en background)
                if _ml_retrained_today != today_str:
                    _ml_retrained_today = today_str
                    asyncio.create_task(_auto_retrain_ml())
                
                # 2. Ejecutar análisis (Centralizado)
                await run_sequential_analysis(
                    engine, sniper_bot, sma_bot, imbalance_ny_bot, imbalance_ldn_bot, imbalance_pm_bot,
                    ema20200_bot, patron4_h_bot, sesgo_bias_htf_bot, silver_bullet_bot, 
                    generic_fvg_bot, fvg_diario_bot, speed_bot, symbolsToScan, 
                    apiKey, "5min", nVelas, marketSentiment=marketSentiment, marketSentiment_crypto=marketSentiment_crypto, imminentNews=imminentNews
                )
                
                await getTiempoEspera(5)
            else:
                sleep_min = get_sleep_minutes()
                segundos_sueño = get_seconds_to_next_sync(sleep_min)
                logger.info(f"💤 Periodo de descanso detectado. Dormirá {int(segundos_sueño // 60)}m {int(segundos_sueño % 60)}s para sincronizar a los {sleep_min} min...", extra={"color": "blue"})
                await asyncio.sleep(segundos_sueño)
        except Exception as e:
            logger.critical(f"Error en bucle: {e}", exc_info=True)
            await asyncio.sleep(60)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido manualmente. ¡Adiós!")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Ocurrió un error fatal fuera del bucle principal: {e}", exc_info=True)
        sys.exit(1)

