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

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")

# --- Path Setup ---

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

# --- Module Imports ---
from Sentinel.utils.loggerConfig import setupLoggingSentinel as setupLogging
from Sentinel.core.Sniper import SniperBot
from Sentinel.core.ImbalanceNY import ImbalanceNYBot
from Sentinel.core.ImbalanceLDN import ImbalanceLDNBot
from Sentinel.core.CruceEMA import CruceEMABot
from Sentinel.core.Patron4h import Patron4HBot
from Sentinel.core.SesgoBiasHTF import SesgoBiasHTFBot
from Sentinel.core.SilverBullet import SilverBulletBot
from Sentinel.core.ImbalancePMNY import ImbalancePMNYBot
from Sentinel.core.GenericFVG import GenericFVGBot
from Sentinel.core.FVGDiario import FVGDiarioBot
from Sentinel.core.SpeedBot import SpeedBot
from Sentinel.core.BreakoutNY import BreakoutNYBot
from Sentinel.core.Ichimoku import IchimokuBot
from Sentinel.core.ReversionMedia import ReversionMediaBot
from Sentinel.core.QTrend import QTrendBot
from Sentinel.core.BreakoutProbability import BreakoutProbabilityBot
from Sentinel.core.PremiumConfluence import PremiumConfluenceBot
from Sentinel.core.TradeManager import TradeManager
from Sentinel.ml import model as mlModel
from Sentinel.analysis.technical import calculateFeatures, resample_to_interval
from Sentinel.analysis import risk
from middleware.utils.momentum import momentum as momentumAnalyzer, _enviar_resumen_inicial
from middleware.api.finnhub_client import getLatestMarketNews, getHighImpactEvents
from middleware.utils.aiManager import getMarketSentiment

#try:
#    from middleware.config.constants import DATA_SOURCE
#except ImportError:
#    DATA_SOURCE = "db"


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

from middleware.utils.time_utils import get_localized_session_times, isRestTime, get_sleep_minutes, get_seconds_to_next_sync, get_seconds_until_market_opens
from middleware.api import twelvedata as tdApi

INTERVAL = settings.INTERVAL
INTERVALmax = settings.INTERVALmax
# Flag para enviar resumen solo una vez
_resumen_momentum_enviado = False
_momentum_data_cache = {}  # Cache para收集 datos de momentum
_weekly_trend_cache = {}  # Cache para tendencia semanal por símbolo
diasTendencia = 14


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
    """
    global _weekly_trend_cache
    _weekly_trend_cache = {}
    
    logger.info("✅ Cargando tendencias mensuales (últimos {} dias)...".format(diasTendencia))
    msg = ""
    for symbolInfo in symbolsToScan:
        symbol = symbolInfo['symbol']
        try:
            # Si es DB local, pedimos 15,000 velas de 5min para asegurar >30 días diarios
            # Si es TwelveData, nos limitamos al máximo permitido (MAX_CANDLES_PER_CALL)
            nVelas = 15000 #if DATA_SOURCE == "db" else MAX_CANDLES_PER_CALL
            symbolApiKey, _, nombreKey, _, _ = getParametros()
            params = {"symbol": symbol, "interval": "5min", "apikey": symbolApiKey, "outputSize": nVelas}
            df_5m = await tdApi.getTimeSeries(params,True)
            #df_5m = await dbManager.getCandles(symbol, n_velas=nVelas)
            
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

def _load_enabled_strategy_configs(strategy_names):
    """
    Carga la configuración habilitada de estrategias usando la rutina existente.
    getStrategyConfig retorna None si la estrategia está disabled o no existe.
    """
    enabled_configs = {}
    for strategy_name in strategy_names:
        config_row = dbManager.getStrategyConfig(strategy_name)
        if config_row:
            enabled_configs[strategy_name] = config_row
        else:
            logger.info(f"[StrategyConfig] {strategy_name} deshabilitada o sin configuración; se omite.")
    return enabled_configs

def _is_strategy_enabled(enabled_configs, strategy_name: str) -> bool:
    return strategy_name in enabled_configs

async def preload_time_series_data(symbolsToScan, apiKey, interval, nVelas):
    """
    Obtiene los datos de time series una sola vez para todos los símbolos.
    Retorna un diccionario: {symbol: dataframe}
    """
    preloaded_data = {}
    for symbolInfo in symbolsToScan:
        symbol = symbolInfo['symbol']
        logger.info(f"Obteniendo datos  para {symbol} (intervalo base 5min)...")
        symbolApiKey, _, nombreKey, _, _ = getParametros()
        params = {"symbol": symbol, "interval": "5min", "apikey": symbolApiKey, "outputSize": nVelas}
        df = await tdApi.getTimeSeries(params,True)
        #df = await tdApi.getTimeSeries({"symbol": symbol, "interval": "5min", "apikey": apiKey, "outputSize": nVelas})
        if df is not None and len(df) >= 100:
            preloaded_data[symbol] = df
        else:
            logger.warning(f"[{symbol}] Datos insuficientes ({len(df) if df is not None else 0} velas).")
    return preloaded_data

async def run_sequential_analysis(engine, trade_manager, sniper_bot, imbalance_ny_bot, imbalance_ldn_bot, imbalance_pm_bot, cruceema_bot, patron4_h_bot, sesgo_bias_htf_bot, silver_bullet_bot, generic_fvg_bot, fvg_diario_bot, speed_bot, breakout_ny_bot, ichimoku_bot, reversion_media_bot, qtrend_bot, breakout_probability_bot, premium_confluence_bot, symbolsToScan, apiKey, interval, nVelas, marketSentiment=0.0, marketSentiment_crypto=0.0, imminentNews=None):
    """
    Ejecuta el análisis de forma secuencial y centraliza la ejecución vía ExecutionEngine.
    """
    MIN_WAIT_SECONDS = get_min_wait_time()
    from middleware.utils.communications import alertaInmediata as _alertaInmediata
    all_signals = []
    strategy_configs = _load_enabled_strategy_configs([
        "Sniper",
        "ImbalanceNY",
        "ImbalanceLDN",
        "ImbalancePMNY",
        "CruceEMA",
        "Patron4h",
        "SesgoBiasHTF",
        "SilverBullet",
        "GenericFVG",
        "FVGDiario",
        "SpeedBot",
        "BreakoutNY",
        "Ichimoku",
        "ReversionMedia",
        "QTrend",
        "BreakoutProbability",
        "PremiumConfluence",
    ])

    # Cargar exclusiones dinámicas de symbolNotStrategia de la base de datos
    exclusions = set()
    _conn = None
    _cur = None
    try:
        from middleware.database import dbConnection as _dbConnection
        _conn = _dbConnection.getConnection()
        _cur = _conn.cursor()
        _cur.execute("SELECT symbol, strategy FROM symbolNotStrategia")
        exclusions = {(r[0], r[1]) for r in _cur.fetchall()}
        logger.info(f"🛡️  [Exclusiones] Cargadas {len(exclusions)} exclusiones desde la tabla symbolNotStrategia")
    except Exception as e:
        logger.error(f"⚠️  Error cargando exclusiones desde symbolNotStrategia: {e}")
    finally:
        if _cur:
            try: _cur.close()
            except: pass
        if _conn:
            try: _conn.close()
            except: pass

    # Cargar cuenta de referencia para sizing de señales
    
    refAccount = dbManager.getAccountById(2)
    if not refAccount:
        cuentas = dbManager.getAccount()
        refAccount = next((a for a in cuentas if a['idCuenta'] != 1), None)
    
    refCapital = float(refAccount['Capital']) if refAccount and refAccount.get('Capital') else 10000.0
    refRiskPct = float(refAccount['riesgoPorOperacion']) if refAccount and refAccount.get('riesgoPorOperacion') else 1.0

    master_data_dict = {}
    cycleSeenKeys = set()
    executedSignalsCount = 0

    for idx, symbolInfo in enumerate(symbolsToScan):
        symbol = symbolInfo['symbol']
        start_time = time.time()
        
        # Obtener API key para este símbolo (rota entre cuentas)
        symbolApiKey, _, nombreKey, _, _ = getParametros()
        logger.info(f"Procesando {symbol} ({idx+1}/{len(symbolsToScan)}) con cuenta {nombreKey}...", extra={"color": "orange"})
        
        # 1. Descargar datos de forma consistente (local o remota según configuración y aplicando spread de broker)
        nVelas = 15000 # if DATA_SOURCE == "db" else MAX_CANDLES_PER_CALL
        params = {"symbol": symbol, "interval": "5min", "apikey": symbolApiKey, "outputSize": nVelas}
        df = await tdApi.getTimeSeries(params,True)
        
        
        if df is None or len(df) < 200:
            logger.warning(f"[{symbol}] Datos insuficientes. Saltando...")
            if idx < len(symbolsToScan) - 1: await asyncio.sleep(MIN_WAIT_SECONDS)
            continue
        
        # --- Punto 3: Diccionario Maestro de Datos (Optimización Pandas) ---
        # Calculamos resampleos y features UNA SOLA VEZ para todos los bots
        df_5m = df.dropna(subset=['close', 'high', 'low', 'open'])
        if not isinstance(df_5m.index, pd.DatetimeIndex):
            if 'datetime' in df_5m.columns:
                df_5m = df_5m.set_index('datetime')
            elif 'timestamp' in df_5m.columns:
                df_5m = df_5m.set_index('timestamp')

        
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
        df_30m = calculateFeatures(resample_to_interval(df_5m, "30min"))
        df_1h  = calculateFeatures(resample_to_interval(df_5m, "1h"))
        df_4h  = calculateFeatures(resample_to_interval(df_5m, "4h"))
        df_1d  = calculateFeatures(resample_to_interval(df_5m, "1d"))

        preloaded_master = {
            '5min':  df_5m,
            '15min': df_15m,
            '30min': df_30m,
            '1h':    df_1h,
            '4h':    df_4h,
            '1d':    df_1d
        }
        
        master_data_dict[symbol] = preloaded_master

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
        if _is_strategy_enabled(strategy_configs, "Sniper") and (symbol, "Sniper") not in exclusions:
            tasks.append(sniper_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}, symbolApiKey))

        # 2. Imbalances (Pre-cálculo de niveles para seguridad en paralelo)
        ahoraMX = datetime.now(pytz.timezone(TIMEZONE))
        iNY, fNY, cNY = get_localized_session_times('America/New_York', 8, 0, 9, 0, 14, 0)
        if ahoraMX > fNY and _is_strategy_enabled(strategy_configs, "ImbalanceNY") and (symbol, "ImbalanceNY") not in exclusions:
            symbolInfo_NY = symbolInfo.copy()
            symbolInfo_NY['precioMaximo'], symbolInfo_NY['precioMinimo'] = df_5m.loc[(df_5m.index >= iNY) & (df_5m.index < fNY)]['high'].max(), df_5m.loc[(df_5m.index >= iNY) & (df_5m.index < fNY)]['low'].min()
            tasks.append(imbalance_ny_bot.runAnalysisCycleForSymbol(symbolInfo_NY, {symbol: preloaded_master}, symbolApiKey))
            
        iLDN, fLDN, cLDN = get_localized_session_times('Europe/London', 8, 0, 9, 0, 14, 0)
        if ahoraMX > fLDN and _is_strategy_enabled(strategy_configs, "ImbalanceLDN") and (symbol, "ImbalanceLDN") not in exclusions:
            symbolInfo_LDN = symbolInfo.copy()
            symbolInfo_LDN['precioMaximo'], symbolInfo_LDN['precioMinimo'] = df_5m.loc[(df_5m.index >= iLDN) & (df_5m.index < fLDN)]['high'].max(), df_5m.loc[(df_5m.index >= iLDN) & (df_5m.index < fLDN)]['low'].min()
            tasks.append(imbalance_ldn_bot.runAnalysisCycleForSymbol(symbolInfo_LDN, {symbol: preloaded_master}, symbolApiKey))

        iPMNY, fPMNY, cPMNY = get_localized_session_times('America/New_York', 14, 0, 15, 0, 17, 0)
        if ahoraMX > fPMNY and _is_strategy_enabled(strategy_configs, "ImbalancePMNY") and (symbol, "ImbalancePMNY") not in exclusions:
            symbolInfo_PMNY = symbolInfo.copy()
            symbolInfo_PMNY['precioMaximo'], symbolInfo_PMNY['precioMinimo'] = df_5m.loc[(df_5m.index >= iPMNY) & (df_5m.index < fPMNY)]['high'].max(), df_5m.loc[(df_5m.index >= iPMNY) & (df_5m.index < fPMNY)]['low'].min()
            tasks.append(imbalance_pm_bot.runAnalysisCycleForSymbol(symbolInfo_PMNY, {symbol: preloaded_master}, symbolApiKey))

        # 3. EMA, Patron4H, Sesgo, SB, FVGs
        if _is_strategy_enabled(strategy_configs, "CruceEMA") and (symbol, "CruceEMA") not in exclusions:
            tasks.append(cruceema_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        if _is_strategy_enabled(strategy_configs, "Patron4h") and (symbol, "Patron4h") not in exclusions:
            tasks.append(patron4_h_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        if _is_strategy_enabled(strategy_configs, "SesgoBiasHTF") and (symbol, "SesgoBiasHTF") not in exclusions:
            tasks.append(sesgo_bias_htf_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))

        # Filtro de Horas Muertas (11:00 AM a 12:00 PM CDMX) - Pausa de bots de ruptura rápida
        ahoraMX = datetime.now(cdmxTz)
        is_dead_hour = (11 <= ahoraMX.hour < 12)
      

        if is_dead_hour:
            logger.info(f"[{symbol}] [Filtro Horas Muertas] Pausando temporalmente bots de ruptura rápida (11:00-12:00 CDMX)", extra={"color": "yellow"})

        if not is_dead_hour and _is_strategy_enabled(strategy_configs, "SilverBullet") and (symbol, "SilverBullet") not in exclusions:
            tasks.append(silver_bullet_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        if not is_dead_hour and _is_strategy_enabled(strategy_configs, "GenericFVG") and (symbol, "GenericFVG") not in exclusions:
            tasks.append(generic_fvg_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        if _is_strategy_enabled(strategy_configs, "FVGDiario") and (symbol, "FVGDiario") not in exclusions:
            tasks.append(fvg_diario_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        if not is_dead_hour and _is_strategy_enabled(strategy_configs, "SpeedBot") and (symbol, "SpeedBot") not in exclusions:
            tasks.append(speed_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        if not is_dead_hour and _is_strategy_enabled(strategy_configs, "BreakoutNY") and (symbol, "BreakoutNY") not in exclusions:
            tasks.append(breakout_ny_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
            
        if _is_strategy_enabled(strategy_configs, "Ichimoku") and (symbol, "Ichimoku") not in exclusions:
            tasks.append(ichimoku_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        if _is_strategy_enabled(strategy_configs, "ReversionMedia") and (symbol, "ReversionMedia") not in exclusions:
            tasks.append(reversion_media_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        if _is_strategy_enabled(strategy_configs, "QTrend") and (symbol, "QTrend") not in exclusions:
            tasks.append(qtrend_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        if _is_strategy_enabled(strategy_configs, "BreakoutProbability") and (symbol, "BreakoutProbability") not in exclusions:
            tasks.append(breakout_probability_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        if _is_strategy_enabled(strategy_configs, "PremiumConfluence") and (symbol, "PremiumConfluence") not in exclusions:
            tasks.append(premium_confluence_bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master}))
        


        # Ejecutar todas las estrategias en paralelo
        results = await asyncio.gather(*tasks) if tasks else []

        # Recolectar e inyectar señales INMEDIATAMENTE para el símbolo actual en tiempo real
        symbolSignals = []
        for res in results:
            if not res: continue
            if isinstance(res, list):
                symbolSignals.extend(res)
            else:
                symbolSignals.append(res)

        if symbolSignals:
            uniqueSymbolSignals = []
            for sig in symbolSignals:
                key = (sig.strategy, sig.symbol, sig.direction)
                try:
                    existing = dbManager.is_trade_duplicate(
                        sig.symbol, sig.strategy, sig.intervalo, sig.direction, None, None
                    )
                except Exception:
                    existing = False
                
                if key not in cycleSeenKeys and not existing:
                    if executedSignalsCount < 3:
                        cycleSeenKeys.add(key)
                        uniqueSymbolSignals.append(sig)
                        executedSignalsCount += 1
                    else:
                        logger.warning(f"⚠️ [Exposición Máxima] Límite de 3 señales por ciclo alcanzado. Omitiendo señal para {sig.symbol} ({sig.strategy})")
                else:
                    if existing:
                        logger.warning(f"⚠️ Trade existente en BD: {sig.strategy} {sig.symbol} {sig.direction}")
                    else:
                        logger.warning(f"⚠️ Señal duplicada descartada: {sig.strategy} {sig.symbol} {sig.direction}")

            if uniqueSymbolSignals:
                logger.info(f"⚡ Inyectando INMEDIATAMENTE {len(uniqueSymbolSignals)} señal(es) para [{symbol}] en tiempo real...")
                await engine.processSignals(
                    uniqueSymbolSignals,
                    marketSentiment=marketSentiment,
                    marketSentiment_crypto=marketSentiment_crypto,
                    imminentNews=imminentNews
                )

        elapsed = time.time() - start_time
        wait_time = max(0, MIN_WAIT_SECONDS - elapsed)
        if wait_time > 0: await asyncio.sleep(wait_time)

    # --- Ejecutar TradeManager (Trailing Stop & ML Smart Exits) ---
    if master_data_dict:
        try:
            await trade_manager.manageOpenPositions(master_data_dict)
        except Exception as e:
            logger.error(f"Error executing TradeManager: {e}")

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
    dbManager.init_alerts_table()
    
    # Tarea programada: Backtest Semanal automático los Sábados
    import subprocess
    today = datetime.now().date()
    if today.weekday() == 5: # 5 es Sábado
        flagFile = os.path.join(rutaRaiz, "Sentinel", "backtesting", "last_weekly_backtest.txt")
        shouldRun = True
        if os.path.exists(flagFile):
            try:
                with open(flagFile, 'r') as f:
                    if f.read().strip() == str(today):
                        shouldRun = False
            except Exception as e:
                logger.warning(f"No se pudo leer el archivo flag de mantenimiento: {e}")
        if shouldRun:
            logger.info("📅 ¡Es Sábado! Lanzando el Mantenimiento Semanal en segundo plano...")
            try:
                os.makedirs(os.path.dirname(flagFile), exist_ok=True)
                with open(flagFile, 'w') as f:
                    f.write(str(today))
            except Exception as e:
                logger.error(f"No se pudo crear el archivo flag de mantenimiento: {e}")
            
            scriptPath = os.path.join(rutaRaiz, "Sentinel", "backtesting", "run_maintenance.sh")
            logDir = os.path.join(rutaRaiz, "logs")
            os.makedirs(logDir, exist_ok=True)
            logFile = os.path.join(logDir, "cron_maintenance.log")
            
            if os.name == 'posix':
                try:
                    subprocess.Popen(["/bin/bash", scriptPath],
                                     cwd=rutaRaiz,
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
                except Exception as e:
                    logger.error(f"Error iniciando mantenimiento semanal en Unix: {e}")
            else:
                pythonBin = sys.executable
                scriptOptim = os.path.join(rutaRaiz, "Sentinel", "backtesting", "run_all_optimizations.py")
                scriptGlobal = os.path.join(rutaRaiz, "Sentinel", "backtesting", "run_two_week_global_backtest.py")
                scriptCompounding = os.path.join(rutaRaiz, "Sentinel", "backtesting", "run_weekly_backtest_compounding_v6.py")
                cmdSeq = f'"{pythonBin}" "{scriptOptim}" && "{pythonBin}" "{scriptGlobal}" && "{pythonBin}" "{scriptCompounding}"'

                try:
                    subprocess.Popen(f'cmd.exe /c "{cmdSeq} >> "{logFile}" 2>&1"',
                                     shell=True,
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
                except Exception as e:
                    logger.error(f"Error iniciando mantenimiento semanal en Windows: {e}")


    
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

    # Instanciar el motor de ejecución y el trade manager central
    engine = ExecutionEngine()
    trade_manager = TradeManager()
    sniper_bot = SniperBot(mlModelInstance=model)
    imbalance_ny_bot = ImbalanceNYBot()
    imbalance_ldn_bot = ImbalanceLDNBot()
    cruceema_bot = CruceEMABot()
    patron4_h_bot = Patron4HBot()
    sesgo_bias_htf_bot = SesgoBiasHTFBot()
    silver_bullet_bot = SilverBulletBot()
    imbalance_pm_bot  = ImbalancePMNYBot()
    generic_fvg_bot = GenericFVGBot(intervals=['5min', '15min', '1h', '4h'])
    fvg_diario_bot = FVGDiarioBot()
    speed_bot = SpeedBot(intervals=['5min'])
    breakout_ny_bot = BreakoutNYBot()
    ichimoku_bot = IchimokuBot()
    reversion_media_bot = ReversionMediaBot()
    qtrend_bot = QTrendBot()
    breakout_probability_bot = BreakoutProbabilityBot()
    premium_confluence_bot = PremiumConfluenceBot()
    
    _weekly_trends_loaded_today = None  # Track fecha de última carga
    _ml_retrained_today = None  # Track fecha de último retraining
    ultimoLogMinuto = -1
    
    while True:
        try:
            if not isRestTime():
                logger.info("Iniciando ciclo de análisis...")

                # --- FILTRO: Límite de Drawdown Diario (20%) ---
                # DESACTIVADO A PETICIÓN DEL USUARIO: Permite a Sentinel enviar señales de forma ininterrumpida sin bloqueo de drawdown.
                # from Sentinel.analysis.risk import isDailyDrawdownLimitReached
                # refAccount = dbManager.getAccountById(2)
                # if not refAccount:
                #     cuentas = dbManager.getAccount()
                #     refAccount = next((a for a in cuentas if a['idCuenta'] != 1), None)
                # refAccountId = refAccount['idCuenta'] if refAccount else 2
                # 
                # if isDailyDrawdownLimitReached(refAccountId, maxDrawdownPercent=20.0):
                #     logger.warning(f"⚠️ BLOQUEO OPERACIONAL: Límite de Drawdown Diario del 20% alcanzado para la cuenta {refAccountId}. Ciclo omitido.", extra={"color": "red"})
                #     await sendTelegramAlert(f"⚠️ <b>Alerta de Drawdown Diario (20%)</b>\nEl portafolio ha alcanzado el límite de pérdida diario del 20%. Se suspenden las operaciones intradiarias de forma automática por seguridad para proteger la integridad del capital. Las operaciones se reanudarán de forma automática mañana.")
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
                
                import time
                inicio_analisis = time.time()
                
                # 2. Ejecutar análisis (Centralizado)
                await run_sequential_analysis(
                    engine, trade_manager, sniper_bot, imbalance_ny_bot, imbalance_ldn_bot, imbalance_pm_bot,
                    cruceema_bot, patron4_h_bot, sesgo_bias_htf_bot, silver_bullet_bot, 
                    generic_fvg_bot, fvg_diario_bot, speed_bot, breakout_ny_bot, ichimoku_bot, reversion_media_bot, qtrend_bot, breakout_probability_bot, premium_confluence_bot, symbolsToScan, 
                    apiKey, "5min", nVelas, marketSentiment=marketSentiment, marketSentiment_crypto=marketSentiment_crypto, imminentNews=imminentNews
                )
                
                fin_analisis = time.time()
                duracion_analisis = fin_analisis - inicio_analisis
                if duracion_analisis >= 300:
                    logger.warning(f"⚠️ El análisis tardó {duracion_analisis:.2f}s (más de 5 minutos). Se omite el tiempo de espera y se reinicia el ciclo inmediatamente.")
                else:
                    await getTiempoEspera(5)
            else:
                segundosSueño = get_seconds_until_market_opens()
                # Margen de seguridad de 10 segundos
                segundosSueño += 10.0
                
                horasRestantes = int(segundosSueño // 3600)
                minutosRestantes = int((segundosSueño % 3600) // 60)
                segundosRestantes = int(segundosSueño % 60)
                
                # Para evitar congelamientos del event loop de asyncio en periodos largos y mantener activo el proceso,
                # dormimos en tramos cortos de máximo 60 segundos
                tiempoSueñoParcial = min(60.0, segundosSueño)
                
                mensajeDescanso = (
                    f"💤 Periodo de descanso activo. Apertura en {horasRestantes}h {minutosRestantes}m {segundosRestantes}s. "
                    f"Modo de espera activo..."
                )
                
                # Loguear en INFO cada 15 minutos, y en DEBUG el resto de los ciclos
                if minutosRestantes % 15 == 0 and minutosRestantes != ultimoLogMinuto:
                    logger.info(mensajeDescanso, extra={"color": "blue"})
                    ultimoLogMinuto = minutosRestantes
                else:
                    logger.debug(mensajeDescanso + f" (durmiendo ciclo de {int(tiempoSueñoParcial)}s)")
                
                await asyncio.sleep(tiempoSueñoParcial)
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
