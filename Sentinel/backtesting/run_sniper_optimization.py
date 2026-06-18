import sys
import os
import pandas as pd
import numpy as np
import logging
import asyncio

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection, dbManager
from middleware.config import constants as config
from Sentinel.analysis import technical
from Sentinel.ml import model as mlModel

# Configuración de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
logging.getLogger("sentinel").setLevel(logging.ERROR)

ALL_SYMBOLS = [
    'EUR/USD', 'GBP/USD', 'AUD/USD', 'NZD/USD',
    'USD/CAD', 'USD/CHF', 'EUR/GBP', 'GBP/CAD',
    'GBP/JPY', 'USD/JPY', 'USD/MXN', 'XAU/USD', 'BTC/USD'
]

PIP_MULTIPLIERS = {
    'EUR/USD': 10000.0, 'GBP/USD': 10000.0, 'AUD/USD': 10000.0, 'NZD/USD': 10000.0,
    'USD/CAD': 10000.0, 'USD/CHF': 10000.0, 'EUR/GBP': 10000.0, 'GBP/CAD': 10000.0,
    'GBP/JPY': 100.0, 'USD/JPY': 100.0, 'USD/MXN': 10000.0, 'XAU/USD': 1.0, 'BTC/USD': 1.0,
}

SPREADS = {
    'EUR/USD': 1.0, 'GBP/USD': 1.5, 'AUD/USD': 1.2, 'NZD/USD': 1.5,
    'USD/CAD': 1.5, 'USD/CHF': 1.6, 'EUR/GBP': 1.5, 'GBP/CAD': 2.2,
    'GBP/JPY': 2.0, 'USD/JPY': 1.2, 'USD/MXN': 25.0, 'XAU/USD': 0.35, 'BTC/USD': 30.0,
}

def loadCandles(symbol: str, startDate: str, endDate: str) -> pd.DataFrame:
    try:
        connection = dbConnection.getConnection()
        if connection is None: return pd.DataFrame()
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

def evaluateGrid(symbol: str, df15m: pd.DataFrame, model, probaThresholdLongCombos, minConfidenceCombos, minRrCombos):
    df_prepared = technical.calculateFeatures(df15m.copy())
    if df_prepared is None or df_prepared.empty: return []
    df_prepared = mlModel.defineMlTarget(df_prepared)
    if df_prepared is None or df_prepared.empty: return []
        
    X, _ = mlModel.cleanDataForModel(df_prepared)
    if len(X) < 100: return []
        
    features = mlModel.MODEL_FEATURES
    X_clean = X.dropna(subset=features)
    if X_clean.empty: return []
    
    probas = model.predict_proba(X_clean[features])[:, 1]
    
    # Extraer columnas operativas de df_prepared usando el índice limpio
    df_op = df_prepared.loc[X_clean.index]
    close_arr = df_op['close'].values
    high_arr = df_op['high'].values
    low_arr = df_op['low'].values
    atr_arr = df_op['atr'].values
    
    pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
    spread = SPREADS.get(symbol, 1.0) / pipMult  # En unidades de precio
    
    results = []
    
    for probaThresholdLong in probaThresholdLongCombos:
        probaThresholdShort = 1.0 - probaThresholdLong
        
        # Encontrar índices donde hay señal (vectorizado)
        long_signals = probas >= probaThresholdLong
        short_signals = probas <= probaThresholdShort
        
        for minConfidence in minConfidenceCombos:
            for minRr in minRrCombos:
                
                trades = []
                in_trade = False
                
                for i in range(100, len(X_clean)):
                    if in_trade:
                        # Simulador rápido de trade
                        v_k_low = low_arr[i]
                        v_k_high = high_arr[i]
                        
                        if direction == "LARGO":
                            if v_k_low <= sl_price:
                                in_trade = False
                                trades.append((sl_price - entry_price) * pipMult - SPREADS.get(symbol, 1.0))
                            elif v_k_high >= tp_price:
                                in_trade = False
                                trades.append((tp_price - entry_price) * pipMult - SPREADS.get(symbol, 1.0))
                        else:
                            if v_k_high >= sl_price:
                                in_trade = False
                                trades.append((entry_price - sl_price) * pipMult - SPREADS.get(symbol, 1.0))
                            elif v_k_low <= tp_price:
                                in_trade = False
                                trades.append((entry_price - tp_price) * pipMult - SPREADS.get(symbol, 1.0))
                        continue
                        
                    # Buscar entradas
                    is_long = long_signals[i-1]
                    is_short = short_signals[i-1]
                    
                    if not is_long and not is_short:
                        continue
                        
                    # Validaciones
                    confianza = (probas[i-1] * 100) if is_long else ((1 - probas[i-1]) * 100)
                    if confianza < minConfidence:
                        continue
                        
                    atr = atr_arr[i-1]
                    close = close_arr[i-1]
                    
                    direction = "LARGO" if is_long else "CORTO"
                    entry_price = close
                    
                    # Aproximación de SL y TP
                    sl_dist = atr * 1.5
                    
                    if direction == "LARGO":
                        sl_price = entry_price - sl_dist
                        tp_price = entry_price + (sl_dist * minRr)
                    else:
                        sl_price = entry_price + sl_dist
                        tp_price = entry_price - (sl_dist * minRr)
                        
                    in_trade = True
                    
                # Evaluar resultados para esta combinación
                numTrades = len(trades)
                if numTrades == 0:
                    continue
                    
                wins = [t for t in trades if t > 0]
                losses = [t for t in trades if t <= 0]
                
                totalProfit = sum(wins)
                totalLoss = abs(sum(losses))
                
                winRate = (len(wins) / numTrades) * 100.0
                profitFactor = totalProfit / totalLoss if totalLoss > 0 else 999.0 if totalProfit > 0 else 0.0
                pnlTotal = sum(trades)
                
                if numTrades >= 1 and winRate >= 35.0 and profitFactor >= 1.00:
                    results.append({
                        "symbol": symbol,
                        "probaThresholdLong": probaThresholdLong,
                        "minConfidence": minConfidence,
                        "minRr": minRr,
                        "totalTrades": numTrades,
                        "winRate": round(winRate, 2),
                        "profitFactor": round(profitFactor, 2),
                        "pnl": round(pnlTotal, 2)
                    })
                    
    return results

async def runSniperGridSearch():
    logger.info("==========================================================")
    logger.info(" INICIANDO DEEP GRID SEARCH OPTIMIZER (SNIPER - VECTORIZED) ")
    logger.info("==========================================================")
    
    model = mlModel.loadModel(config.MODEL_FILE_PATH)
    if model is None: return
        
    startDateStr = '2026-04-16 00:00:00'
    endDateStr = '2026-06-16 23:59:59'
    
    # Deep Grid
    probaThresholdLongCombos = [0.55, 0.60, 0.65, 0.70]
    minConfidenceCombos = [60, 70, 80]
    minRrCombos = [1.0, 1.2, 1.5, 1.8, 2.0, 2.5]
    
    bestResults = []
    allResultsRaw = []
    
    for symbol in ALL_SYMBOLS:
        logger.info(f"⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400: continue
            
        from middleware.config.constants import TIMEZONE
        df5m.index = df5m.index.tz_localize(TIMEZONE, ambiguous='infer', nonexistent='shift_forward')
        
        df15m = df5m.resample('15min').agg({
            'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'
        }).dropna()
        
        results = evaluateGrid(symbol, df15m, model, probaThresholdLongCombos, minConfidenceCombos, minRrCombos)
        allResultsRaw.extend(results)
        
        if results:
            best = max(results, key=lambda x: (x['profitFactor'], x['winRate']))
            logger.info(f"  ✨ Mejor combo para {symbol}: PF={best['profitFactor']}, MinConf={best['minConfidence']}, R:R={best['minRr']}")
            bestResults.append(best)
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.0) para {symbol}.")
            
    dfAll = pd.DataFrame(allResultsRaw)
    if not dfAll.empty: dfAll.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/sniper_grid_results_all.csv", index=False)
    
    dfBest = pd.DataFrame(bestResults)
    if not dfBest.empty: dfBest.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/sniper_grid_results_best.csv", index=False)
    logger.info("==========================================================")

if __name__ == "__main__":
    asyncio.run(runSniperGridSearch())
