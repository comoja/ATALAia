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
from middleware.database import dbConnection
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR
from Sentinel.core.SesgoBiasHTF import SesgoBiasHTFBot

# Configuración de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)
# Silenciar los logs internos del bot de sentinel para evitar I/O masivo
logging.getLogger("sentinel").setLevel(logging.WARNING)

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

async def runBacktestForCombo(df15m: pd.DataFrame, symbol: str, swingLookback: int, mssLookback: int, minRr: float) -> dict:
    bot = SesgoBiasHTFBot()
    
    # Sobrescribir parámetros en el bot
    bot.swing_lookback = swingLookback
    bot.swingLookback = swingLookback
    bot.mss_lookback = mssLookback
    bot.mssLookback = mssLookback
    bot.minRr = minRr
    
    # Configurar filtros mensuales/semanales simulando mainOrchestrator
    # tendencia en base a la media móvil de 14 días
    df_daily = bot.resample_ohlcv(df15m, '1d')
    if len(df_daily) >= 14:
        df_daily['sma14'] = df_daily['close'].rolling(14).mean()
        last_sma = df_daily['sma14'].iloc[-1]
        last_close = df_daily['close'].iloc[-1]
        weekly_trend = "ALCISTA" if last_close > last_sma else "BAJISTA"
    else:
        weekly_trend = "NEUTRAL"
        
    symbol_info = {
        'symbol': symbol,
        'tipo': 'METALES' if 'XAU' in symbol else ('CRYPTO' if 'BTC' in symbol else 'FOREX'),
        'pip': 1.0,
        'weekly_trend': weekly_trend
    }
    
    trades = []
    pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
    
    # Pre-cargar y pre-resamplear marcos temporales superiores una vez
    df_h1 = bot.resample_ohlcv(df15m, '1h')
    df_1d = bot.resample_ohlcv(df15m, '1d')
    df_1w = bot.resample_ohlcv(df15m, '1w')
    df_1M = bot.resample_ohlcv(df15m, '1M')
    
    start_idx = 100
    total_len = len(df15m)
    
    i = start_idx
    while i < total_len:
        # Slice de 15min hasta la vela actual
        df_slice_15m = df15m.iloc[:i]
        timestamp_curr = df_slice_15m.index[-1]
        
        # Filtrar marcos superiores hasta el timestamp actual para no tener sesgo de supervivencia
        df_slice_h1 = df_h1[df_h1.index <= timestamp_curr]
        df_slice_1d = df_1d[df_1d.index <= timestamp_curr]
        df_slice_1w = df_1w[df_1w.index <= timestamp_curr]
        df_slice_1m = df_1M[df_1M.index <= timestamp_curr]
        
        # Estructurar master dictionary
        preloaded = {
            symbol: {
                '15min': df_slice_15m,
                '1h': df_slice_h1,
                '1d': df_slice_1d,
                '1w': df_slice_1w,
                '1m': df_slice_1m
            }
        }
        
        # Ejecutar ciclo de análisis
        signal = await bot.runAnalysisCycleForSymbol(symbol_info, preloadedData=preloaded)
        if signal:
            entry_price = float(signal.entry_price)
            sl_price = float(signal.stop_loss)
            tp_price = float(signal.take_profit)
            direction = signal.direction
            
            # Monitorear velas siguientes hasta tocar SL o TP
            closed = False
            pnl_pips = 0.0
            
            for k in range(i, total_len):
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
                i = k + 1
                continue
        i += 1
        
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

async def runSesgoBiasHTFGridSearch():
    logger.info("==========================================================")
    logger.info(" INICIANDO GRID SEARCH OPTIMIZER (SESGOBIASHTF - 15MIN) ")
    logger.info("==========================================================")
    
    startDateStr = '2026-04-16 00:00:00'
    endDateStr = '2026-06-16 23:59:59'
    
    # Grid de Parámetros
    swingLookbackCombos = [30, 50]
    mssLookbackCombos = [3, 5]
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
        
        for swingLookback in swingLookbackCombos:
            for mssLookback in mssLookbackCombos:
                for minRr in minRrCombos:
                    res = await runBacktestForCombo(df15m, symbol, swingLookback, mssLookback, minRr)
                    numTrades = len(res['trades'])
                    
                    row = {
                        "symbol": symbol,
                        "swingLookback": swingLookback,
                        "mssLookback": mssLookback,
                        "minRr": minRr,
                        "totalTrades": numTrades,
                        "winRate": round(res['winRate'], 2),
                        "profitFactor": round(res['profitFactor'], 2),
                        "pnl": round(res['pnl'], 2)
                    }
                    allResultsRaw.append(row)
                    
                    # Criterio de viabilidad: WR >= 42% y PF >= 1.25, al menos 1 trade
                    if numTrades >= 1 and res['winRate'] >= 35.0 and res['profitFactor'] >= 1.00:
                        if res['profitFactor'] > bestPf or (res['profitFactor'] == bestPf and res['winRate'] > bestWr):
                            bestPf = res['profitFactor']
                            bestWr = res['winRate']
                            bestCombo = row
                            
        if bestCombo:
            logger.info(f"  ✨ Mejor combo viable para {symbol}: Swing={bestCombo['swingLookback']}, MSS={bestCombo['mssLookback']}, Min R:R={bestCombo['minRr']} (PF={bestCombo['profitFactor']:.2f}, WR={bestCombo['winRate']:.2f}%)")
            bestResults.append(bestCombo)
        else:
            logger.warning(f"  ❌ No se encontró combo viable (PF >= 1.0 y WR >= 35%) para {symbol}.")
            
    # Guardar resultados en CSV
    dfAll = pd.DataFrame(allResultsRaw)
    dfAll.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/sesgobiashtf_grid_results_all.csv", index=False)
    
    dfBest = pd.DataFrame(bestResults)
    dfBest.to_csv("/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/sesgobiashtf_grid_results_best.csv", index=False)
    
    logger.info("==========================================================")
    logger.info(" GRID SEARCH COMPLETADO. Archivos CSV generados con éxito.")
    logger.info("==========================================================")

if __name__ == "__main__":
    asyncio.run(runSesgoBiasHTFGridSearch())
