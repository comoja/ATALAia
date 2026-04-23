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
from Sentinel.ml import model as mlModel
from Sentinel.analysis.technical import calculateFeatures, resample_to_interval
from Sentinel.analysis import risk
from middleware.utils.momentum import momentum as momentumAnalyzer, _enviar_resumen_inicial

# Flag para enviar resumen solo una vez
_resumen_momentum_enviado = False
_momentum_data_cache = {}  # Cache para收集 datos de momentum
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

from middleware.utils.time_utils import get_localized_session_times
from middleware.api import twelvedata as tdApi

INTERVAL = settings.INTERVAL
INTERVALmax = settings.INTERVALmax

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

async def run_sequential_analysis(engine, sniper_bot, sma_bot, imbalance_ny_bot, imbalance_ldn_bot, imbalance_pm_bot, ema20200_bot, patron4_h_bot, sesgo_bias_htf_bot, silver_bullet_bot, generic_fvg_bot, fvg_diario_bot, symbolsToScan, apiKey, interval, nVelas):
    """
    Ejecuta el análisis de forma secuencial y centraliza la ejecución vía ExecutionEngine.
    """
    MIN_WAIT_SECONDS = get_min_wait_time()
    from middleware.utils.communications import alertaInmediata as _alertaInmediata
    
    all_signals = []

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
            from middleware.database import dbManager
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
        
        # Asegurar consistencia de zona horaria (tz-aware)
        cdmx_tz = pytz.timezone(TIMEZONE)
        if df_5m.index.tzinfo is None:
            df_5m.index = df_5m.index.tz_localize(cdmx_tz)
        else:
            df_5m.index = df_5m.index.tz_convert(cdmx_tz)

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
                from middleware.database import dbManager
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
        await engine.process_signals(unique_signals)

setupLogging(enableConsole=True)
logger = logging.getLogger("sentinel")

async def main():
    logger.info("====== Inicializando Bot de Trading Sentinel (Decoupled) ======")
    
    # Auto-reentrenamiento diario a la 1am
    from Sentinel.ml.auto_retrain import should_retrain
    if asyncio.iscoroutinefunction(should_retrain) or callable(should_retrain):
        try:
            should_run = should_retrain()
            if asyncio.iscoroutine(should_run):
                should_run = await should_run
            if should_run:
                logger.info("🚀 Iniciando auto-reentrenamiento de modelos ML...")
                from Sentinel.ml import retrain_ml, train_reg_model
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
    generic_fvg_bot = GenericFVGBot()
    fvg_diario_bot = FVGDiarioBot()
    
    while True:
        try:
            if not isRestTime():
                logger.info("Iniciando ciclo de análisis...")
                # El motor ya gestiona los cierres y el riesgo
                
                apiKey, _, _, nVelas, _ = getParametros()
                symbolsToScan = dbManager.getSymbols()
                
                await run_sequential_analysis(engine, sniper_bot, sma_bot, imbalance_ny_bot, imbalance_ldn_bot, imbalance_pm_bot, ema20200_bot, patron4_h_bot, sesgo_bias_htf_bot, silver_bullet_bot, generic_fvg_bot, fvg_diario_bot, symbolsToScan, apiKey, INTERVAL, nVelas)
                
                await getTiempoEspera(5)
            else:
                logger.info("Mercado cerrado. Durmiendo.")
                await asyncio.sleep(600)
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

