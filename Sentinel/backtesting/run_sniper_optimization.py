import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
import asyncio
from datetime import datetime, time, timedelta
import pytz

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection, dbManager
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR
from middleware.config import constants as config
from Sentinel.analysis import technical
from Sentinel.ml import model as mlModel
from Sentinel.core.Sniper import SniperBot

# Configuración de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
# Silenciar logs internos para optimización rápida
logging.getLogger("sentinel").setLevel(logging.ERROR)

ALL_SYMBOLS = [
    'EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD',
    'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD',
    'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD'
]

PIP_MULTIPLIERS = {
    'EUR/USD': 10000.0,
    'GBP/USD': 10000.0,
    'AUD/USD': 10000.0,
    'NZD/USD': 10000.0,
    'USD/CAD': 10000.0,
    'USD/CHF': 10000.0,
    'EUR/GBP': 10000.0,
    'GBP/CAD': 10000.0,
    'GBP/JPY': 100.0,
    'USD/JPY': 100.0,
    'USD/MXN': 10000.0,
    'XAU/USD': 1.0,
    'BTC/USD': 1.0,
}

SPREADS = {
    'EUR/USD': 1.0,
    'GBP/USD': 1.5,
    'AUD/USD': 1.2,
    'NZD/USD': 1.5,
    'USD/CAD': 1.5,
    'USD/CHF': 1.6,
    'EUR/GBP': 1.5,
    'GBP/CAD': 2.2,
    'GBP/JPY': 2.0,
    'USD/JPY': 1.2,
    'USD/MXN': 25.0,
    'XAU/USD': 0.35,
    'BTC/USD': 30.0,
}

def loadCandles(symbol: str, startDate: str, endDate: str) -> pd.DataFrame:
    try:
        connection = dbConnection.getConnection()
        if connection is None:
            return pd.DataFrame()
        query = """
            SELECT timestamp as datetime, open, high, low, close, volume
            FROM candles
            WHERE symbol = %s AND timeframe = '5min' AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, connection, params=(symbol, startDate, endDate))
        connection.close()
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
        return df
    except Exception as e:
        logger.error(f"Error cargando velas para {symbol}: {e}")
        return pd.DataFrame()

async def runBacktestForCombo(df15m: pd.DataFrame, model, symbol: str, probaThresholdLong: float, minConfidence: float, minRr: float) -> dict:
    # 1. Precalcular features, target y limpiar datos en lote
    df_prepared = technical.calculateFeatures(df15m.copy())
    if df_prepared is None or df_prepared.empty:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
    df_prepared = mlModel.defineMlTarget(df_prepared)
    if df_prepared is None or df_prepared.empty:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    X, _ = mlModel.cleanDataForModel(df_prepared)
    if len(X) < 100:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    # Predecir probabilidades de ML en lote
    features = mlModel.MODEL_FEATURES
    X_clean = X.dropna(subset=features)
    if X_clean.empty:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
    probas = model.predict_proba(X_clean[features])[:, 1]
    proba_dict = dict(zip(X_clean.index, probas))
    
    bot = SniperBot(mlModelInstance=model)
    
    # Mockear dbManager.getSymbolStrategyConfig para forzar los parámetros de la iteración
    mock_config = {
        "probaThresholdLong": probaThresholdLong,
        "probaThresholdShort": 1.0 - probaThresholdLong,
        "minConfidence": minConfidence,
        "minRr": minRr,
        "riskUsd": 100.0,
        "minUsdProfit": 10.0,
        "jpyThresholdAdjustPct": 0.0,
        "jpyMinConfidenceAdjustPct": 0.0,
        "jpyExtraConfirmations": 0,
        "maxRr": 2.5
    }
    
    orig_getSymbolConfig = dbManager.getSymbolStrategyConfig
    dbManager.getSymbolStrategyConfig = lambda strat, sym: mock_config
    
    # Usar un contenedor mutable para poder modificar 'i' desde el bucle y que los lambdas lo lean
    state = {'i': 200}
    
    # Mockear las funciones lentas en el bot e importaciones
    orig_get_and_prepare = bot._get_and_prepare_data
    bot._get_and_prepare_data = lambda symbolInfo, apiKey, nVelas, interval, raw_df=None: df_prepared.iloc[:state['i']]
    
    import Sentinel.core.Sniper as SniperModule
    orig_clean_data = SniperModule.mlModel.cleanDataForModel
    SniperModule.mlModel.cleanDataForModel = lambda df: (X.iloc[:state['i']], None)
    
    orig_predict_proba = SniperModule.mlModel.predictProba
    SniperModule.mlModel.predictProba = lambda model_inst, X_slice: proba_dict.get(X_slice.index[-1], 0.5)
    
    symbol_info = {
        'symbol': symbol,
        'tipo': 'METALES' if 'XAU' in symbol else ('CRYPTO' if 'BTC' in symbol else 'FOREX'),
        'pip': 1.0,
        'intervalo': '15min'
    }
    
    trades = []
    pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
    
    start_idx = 200
    total_len = len(df15m)
    
    state['i'] = start_idx
    while state['i'] < total_len:
        timestamp_curr = df15m.index[state['i'] - 1]
        
        try:
            signal = await bot.runAnalysisCycleForSymbol(symbol_info, preloaded_data={symbol: df15m})
        except Exception as e:
            state['i'] += 1
            continue
            
        if signal:
            entry_price = float(signal.entry_price)
            sl_price = float(signal.stop_loss)
            tp_price = float(signal.take_profit)
            direction = signal.direction
            
            closed = False
            pnl_pips = 0.0
            
            for k in range(state['i'], total_len):
                v_k_low = float(df15m['low'].iloc[k])
                v_k_high = float(df15m['high'].iloc[k])
                
                if direction == "LARGO":
                    if v_k_low <= sl_price:
                        closed = True
                        pnl_pips = (sl_price - entry_price) * pipMult
                        break
                    elif v_k_high >= tp_price:
                        closed = True
                        pnl_pips = (tp_price - entry_price) * pipMult
                        break
                else: # CORTO
                    if v_k_high >= sl_price:
                        closed = True
                        pnl_pips = (entry_price - sl_price) * pipMult
                        break
                    elif v_k_low <= tp_price:
                        closed = True
                        pnl_pips = (entry_price - tp_price) * pipMult
                        break
            
            if closed:
                pnl_pips -= SPREADS.get(symbol, 1.0)
                trades.append({
                    "direction": direction,
                    "entryTime": timestamp_curr,
                    "pnl": pnl_pips,
                    "result": "WIN" if pnl_pips > 0 else "LOSS"
                })
                state['i'] = k + 1
                continue
        state['i'] += 1
        
    # Restaurar originales
    dbManager.getSymbolStrategyConfig = orig_getSymbolConfig
    bot._get_and_prepare_data = orig_get_and_prepare
    SniperModule.mlModel.cleanDataForModel = orig_clean_data
    SniperModule.mlModel.predictProba = orig_predict_proba
    
    if not trades:
        return {"trades": [], "winRate": 0.0, "profitFactor": 0.0, "pnl": 0.0}
        
    wins = [t for t in trades if t['pnl'] > 0]
    losses = [t for t in trades if t['pnl'] <= 0]
    
    totalProfit = sum(t['pnl'] for t in wins)
    totalLoss = abs(sum(t['pnl'] for t in losses))
    
    winRate = (len(wins) / len(trades)) * 100.0
    profitFactor = totalProfit / totalLoss if totalLoss > 0 else 999.0 if totalProfit > 0 else 0.0
    pnlTotal = sum(t['pnl'] for t in trades)
    
    return {
        "trades": trades,
        "winRate": winRate,
        "profitFactor": profitFactor,
        "pnl": pnlTotal
    }

async def runSniperGridSearch():
    logger.info("==========================================================")
    logger.info(" INICIANDO GRID SEARCH OPTIMIZER (SNIPER - 15MIN) ")
    logger.info("==========================================================")
    
    model = mlModel.loadModel(config.MODEL_FILE_PATH)
    if model is None:
        logger.error("❌ No se pudo cargar el modelo ML trainedModel.joblib")
        return
        
    startDateStr = '2026-05-15 00:00:00'
    endDateStr = '2026-06-11 14:00:00'
    
    # Grid de Parámetros
    probaThresholdLongCombos = [0.60, 0.65]
    minConfidenceCombos = [70, 75]
    minRrCombos = [1.5, 2.0]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            logger.warning(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        from middleware.config.constants import TIMEZONE
        df5m.index = df5m.index.tz_localize(TIMEZONE, ambiguous='infer', nonexistent='shift_forward')
        
        # Resamplear de 5min a 15min
        df15m = df5m.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        bestCombo = None
        bestPf = 0.0
        bestWr = 0.0
        
        for probaThresholdLong in probaThresholdLongCombos:
            for minConfidence in minConfidenceCombos:
                for minRr in minRrCombos:
                    res = await runBacktestForCombo(df15m, model, symbol, probaThresholdLong, minConfidence, minRr)
                    numTrades = len(res['trades'])
                    
                    row = {
                        "symbol": symbol,
                        "probaThresholdLong": probaThresholdLong,
                        "minConfidence": minConfidence,
                        "minRr": minRr,
                        "totalTrades": numTrades,
                        "winRate": round(res['winRate'], 2),
                        "profitFactor": round(res['profitFactor'], 2),
                        "pnl": round(res['pnl'], 2)
                    }
                    allResultsRaw.append(row)
                    
                    # Criterio de viabilidad: WR >= 42% y PF >= 1.25, al menos 1 trade
                    if numTrades >= 1 and res['winRate'] >= 42.0 and res['profitFactor'] >= 1.25:
                        if res['profitFactor'] > bestPf or (res['profitFactor'] == bestPf and res['winRate'] > bestWr):
                            bestPf = res['profitFactor']
                            bestWr = res['winRate']
                            bestCombo = row
                            
        if bestCombo:
            logger.info(f"  ✨ Mejor combo viable para {symbol}: ProbaLong={bestCombo['probaThresholdLong']}, MinConf={bestCombo['minConfidence']}, Min R:R={bestCombo['minRr']} (PF={bestCombo['profitFactor']:.2f}, WR={bestCombo['winRate']:.2f}%)")
            bestResults.append(bestCombo)
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.25 y WR >= 42%) para {symbol}.")
            
    # Guardar resultados en CSV
    dfAll = pd.DataFrame(allResultsRaw)
    dfAll.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/sniper_grid_results_all.csv", index=False)
    
    dfBest = pd.DataFrame(bestResults)
    dfBest.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/sniper_grid_results_best.csv", index=False)
    
    logger.info("==========================================================")
    logger.info(" GRID SEARCH COMPLETADO. Archivos CSV generados con éxito.")
    logger.info("==========================================================")

if __name__ == "__main__":
    asyncio.run(runSniperGridSearch())
