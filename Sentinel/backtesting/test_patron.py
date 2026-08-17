import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import logging
import asyncio
from datetime import datetime
import pytz

# Asegurar path del proyecto en sys.path
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection, dbManager
from Sentinel.core.Patron4h import Patron4HBot
from middleware.utils.alertBuilder import getPipMultiplier
from Sentinel.analysis import technical
from Sentinel.analysis.fvg_analyzer import FvgAnalyzer

# Silenciar logs para no saturar la pantalla
logging.getLogger('sentinel').setLevel(logging.ERROR)
logging.basicConfig(level=logging.ERROR)

# --- Inyección de Caché para Funciones SMC Costosas ---
orig_detect_fvgs = technical.detect_fvgs
orig_detect_mss = technical.detect_mss

fvg_cache = {}
mss_cache = {}

def cached_detect_fvgs(df, min_gap_pct=0.00005, validate_mitigation=True, apply_high_prob_filters=True):
    if df is None or len(df) == 0:
        return []
    key = (df.index[-1], len(df), min_gap_pct, validate_mitigation, apply_high_prob_filters)
    if key not in fvg_cache:
        fvg_cache[key] = orig_detect_fvgs(df, min_gap_pct, validate_mitigation, apply_high_prob_filters)
    return fvg_cache[key]

def cached_detect_mss(df, direction, lookback=15):
    if df is None or len(df) == 0:
        return False
    key = (df.index[-1], len(df), direction, lookback)
    if key not in mss_cache:
        mss_cache[key] = orig_detect_mss(df, direction, lookback)
    return mss_cache[key]

technical.detect_fvgs = cached_detect_fvgs
technical.detect_mss = cached_detect_mss

# Inyectar Mock para FvgAnalyzer.detectFvg que precalcula de forma global
orig_detectFvg = FvgAnalyzer.detectFvg

# Variables globales para los FVGs completos del símbolo actual
global_raw_fvgs = {
    '15min': [],
    '1h': [],
    '4h': [],
    '1d': []
}

df_15m_global = None
df_1h_global = None
df_4h_global = None
df_1d_global = None

def mock_detectFvg(self, df):
    if df is None or len(df) < 3:
        return []
    
    # Calcular la diferencia de tiempo para determinar el marco de tiempo
    diff = (df.index[1] - df.index[0]).total_seconds()
    if diff <= 900: # 15min
        tf = '15min'
    elif diff <= 3600: # 1h
        tf = '1h'
    elif diff <= 14400: # 4h
        tf = '4h'
    else: # 1D
        tf = '1d'
        
    # Obtener el índice máximo según la fecha del último registro del df en el df global correspondiente
    t_last = df.index[-1]
    
    # Filtrar los FVGs precalculados que ocurren antes o en el timestamp de la vela actual
    # Usamos la fecha en lugar del idx directamente para evitar desajustes en el re-slicing
    res = []
    for f in global_raw_fvgs[tf]:
        fvg_time = pd.to_datetime(f['timestamp']).tz_localize(t_last.tzinfo)
        if fvg_time <= t_last:
            res.append(f.copy())
    return res

FvgAnalyzer.detectFvg = mock_detectFvg
# -----------------------------------------------------

ALL_SYMBOLS = ['EUR/USD']

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
        print(f"Error cargando velas para {symbol}: {e}")
        return pd.DataFrame()

async def runPatron4HGridSearch() -> None:
    print("==========================================================")
    print("  INICIANDO GRID SEARCH OPTIMIZER (PATRON4H - FAST CACHED) ")
    print("==========================================================")
    
    startDateStr = '2026-04-16 00:00:00'
    endDateStr = '2026-06-16 23:59:59'
    
    # Grid de Parámetros
    minRrCombos = [1.2, 1.5, 1.8, 2.0, 2.5]
    minConfidenceCombos = [60.0, 65.0, 70.0, 75.0, 80.0]
    displacementPctCombos = [0.0003, 0.0005, 0.0008, 0.0010]
    fvgMinPctCombos = [0.00003, 0.00005, 0.00008, 0.0001]
    
    bestResults = []
    allResultsRaw = []
    
    bot = Patron4HBot()
    
    # Mockear dbManager.getSymbolStrategyConfig en memoria
    global current_config
    current_config = {}
    dbManager.getSymbolStrategyConfig = lambda strat, sym: current_config
    
    global df_15m_global, df_1h_global, df_4h_global, df_1d_global
    
    for symbol in ALL_SYMBOLS:
        print(f"\n⚙️ Analizando combinaciones para {symbol}...")
        df5m = loadCandles(symbol, startDateStr, endDateStr)
        if df5m.empty or len(df5m) < 400:
            print(f"  ⚠️ Datos insuficientes para {symbol}. Saltando.")
            continue
            
        from middleware.config.constants import TIMEZONE
        df5m.index = df5m.index.tz_localize(TIMEZONE, ambiguous='infer', nonexistent='shift_forward')
            
        # Resamplear globalmente para este símbolo
        df_15m_global = df5m.resample('15min').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        df_1h_global = df5m.resample('1h').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        df_4h_global = df5m.resample('4h').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        df_1d_global = df5m.resample('1D').agg({
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }).dropna()
        
        if len(df_15m_global) < 200 or len(df_1d_global) < 3:
            print(f"  ⚠️ Datos insuficientes en 15m/1d para {symbol}. Saltando.")
            continue
            
        pipMult = PIP_MULTIPLIERS.get(symbol, 10000.0)
        spreadPrice = SPREADS.get(symbol, 1.0) / pipMult
        
        # Limpiar cachés para el nuevo símbolo
        fvg_cache.clear()
        mss_cache.clear()
        
        # Precalcular FVGs globales usando el método original sobre el dataset completo
        print(f"  ⚡ Precalculando FVGs de forma global para {symbol}...")
        analyzer = FvgAnalyzer(minGapPct=0.00005)
        global_raw_fvgs['15min'] = orig_detectFvg(analyzer, df_15m_global)
        global_raw_fvgs['1h'] = orig_detectFvg(analyzer, df_1h_global)
        global_raw_fvgs['4h'] = orig_detectFvg(analyzer, df_4h_global)
        global_raw_fvgs['1d'] = orig_detectFvg(analyzer, df_1d_global)
        
        # Precalcular mapeo de índices
        idx1h_map = df_1h_global.index.get_indexer(df_15m_global.index, method='pad')
        idx4h_map = df_4h_global.index.get_indexer(df_15m_global.index, method='pad')
        idx1d_map = df_1d_global.index.get_indexer(df_15m_global.index, method='pad')
        
        # Precalcular catalizadores confirmados en 4H y 1H de forma rápida una sola vez
        print(f"  ⚡ Precalculando catalizadores en 4H y 1H para {symbol}...")
        catalizadores = set()
        
        # Precalcular tendencia diaria para cada día
        tendenciaDiaria = {}
        for idx_d in range(2, len(df_1d_global)):
            t_day = df_1d_global.index[idx_d]
            bot.currentTime = t_day
            df_1d_sliced = df_1d_global.iloc[:idx_d+1]
            ctx = bot.obtener_contexto_diario(df_1d_sliced, 0.00005)
            tendenciaDiaria[t_day.date()] = ctx
            
        # Analizar catalizadores 4H
        for i in range(20, len(df_4h_global)):
            t = df_4h_global.index[i]
            bot.currentTime = t
            ctx = tendenciaDiaria.get(t.date())
            if not ctx or ctx['tendencia'] == 'LATERAL':
                continue
            df_tf_sliced = df_4h_global.iloc[:i+1]
            c4h = bot.analizar_catalizador(df_tf_sliced, ctx, None, '4H', 0.00005, 0.0005)
            if c4h['confirmado']:
                catalizadores.add(t)
                
        # Analizar catalizadores 1H
        for j in range(20, len(df_1h_global)):
            t = df_1h_global.index[j]
            bot.currentTime = t
            if t in catalizadores:
                continue
            ctx = tendenciaDiaria.get(t.date())
            if not ctx or ctx['tendencia'] == 'LATERAL':
                continue
            df_tf_sliced = df_1h_global.iloc[:j+1]
            c1h = bot.analizar_catalizador(df_tf_sliced, ctx, None, '1h', 0.00005, 0.0005)
            if c1h['confirmado']:
                catalizadores.add(t)
                
        print(f"  📌 Encontrados {len(catalizadores)} catalizadores potenciales.")
        if len(catalizadores) == 0:
            print(f"  ❌ No hay catalizadores para {symbol}. Saltando.")
            continue
            
        symbolBestCombo = None
        symbolBestProfit = -9999.0
        
        # 1. Precalcular todas las señales crudas (Structural Simulation)
        raw_signals = {} # key: (displacementPct, fvgMinPct), value: list of raw signals
        
        for displacementPct in displacementPctCombos:
            for fvgMinPct in fvgMinPctCombos:
                current_config = {
                    'fvgMinPct': fvgMinPct,
                    'displacementPct': displacementPct,
                    'rrRatioMin': 0.0, # Para obtener TP estructural puro
                    'maxMinutosFvg': 240.0,
                    'minConfidence': 0.0, # Para obtener todas las señales viables
                    'lookback': 50
                }
                
                combo_signals = []
                idx = 100
                n = len(df_15m_global)
                
                while idx < n:
                    t_current = df_15m_global.index[idx]
                    
                    # Obtener índices posicionales
                    i1h = idx1h_map[idx]
                    i4h = idx4h_map[idx]
                    i1d = idx1d_map[idx]
                    
                    if i1d < 2:
                        idx += 1
                        continue
                        
                    t_4h_closed = df_4h_global.index[i4h]
                    t_1h_closed = df_1h_global.index[i1h]
                    
                    # Filtro rápido
                    if t_4h_closed not in catalizadores and t_1h_closed not in catalizadores:
                        idx += 1
                        continue
                        
                    df_15m_sliced = df_15m_global.iloc[:idx+1]
                    df_1h_sliced = df_1h_global.iloc[:i1h+1]
                    df_4h_sliced = df_4h_global.iloc[:i4h+1]
                    df_1d_sliced = df_1d_global.iloc[:i1d+1]
                    
                    symbolInfo = {
                        'symbol': symbol,
                        'intervalo': '15min',
                        'momentum': '☁️ SIN DATOS',
                        'weekly_trend': 'NEUTRAL',
                        'refCapital': 10000.0,
                        'refRiskPct': 1.0
                    }
                    
                    preloaded = {
                        symbol: {
                            '15min': df_15m_sliced,
                            '1h': df_1h_sliced,
                            '4h': df_4h_sliced,
                            '1d': df_1d_sliced
                        }
                    }
                    
                    try:
                        bot.currentTime = t_current
                        # Importamos asincronía aquí si es necesario
                        import asyncio
                        signals = await bot.runAnalysisCycleForSymbol(symbolInfo, preloadedData=preloaded)
                        if signals and len(signals) > 0:
                            sig = signals[0]
                            combo_signals.append({
                                'idx': idx, # índice de la vela actual
                                'direction': sig.direction,
                                'entry': sig.entry_price,
                                'sl': sig.stop_loss,
                                'original_tp': sig.take_profit,
                                'confidence': sig.confidence
                            })
                    except Exception as e:
                        pass
                        
                    idx += 1
                    
                raw_signals[(displacementPct, fvgMinPct)] = combo_signals
                print(f"    - Precalculadas {len(combo_signals)} señales para Disp={displacementPct}, FVG={fvgMinPct}")

        # 2. Evaluación Rápida (RR y Confianza) sobre las señales crudas
        for displacementPct in displacementPctCombos:
            for fvgMinPct in fvgMinPctCombos:
                signals = raw_signals[(displacementPct, fvgMinPct)]
                
                for minRr in minRrCombos:
                    for minConfidence in minConfidenceCombos:
                        
                        trades = []
                        activeTrade = None
                        
                        # Loop muy rápido sobre los índices 15m
                        idx = 100
                        n = len(df_15m_global)
                        
                        # Convertir a generador o lista para procesar las señales secuencialmente
                        signal_queue = [s for s in signals if s['confidence'] >= minConfidence]
                        sig_idx = 0
                        num_sigs = len(signal_queue)
                        
                        while idx < n:
                            if activeTrade:
                                row = df_15m_global.iloc[idx]
                                vHigh = row['high']
                                vLow = row['low']
                                
                                if activeTrade['direction'] == 'LARGO':
                                    lowAdj = vLow - (spreadPrice / 2.0)
                                    highAdj = vHigh + (spreadPrice / 2.0)
                                    if lowAdj <= activeTrade['sl']:
                                        trades.append(-100.0)
                                        activeTrade = None
                                    elif highAdj >= activeTrade['tp']:
                                        trades.append(100.0 * activeTrade['rr'])
                                        activeTrade = None
                                else:
                                    highAdj = vHigh + (spreadPrice / 2.0)
                                    lowAdj = vLow - (spreadPrice / 2.0)
                                    if highAdj >= activeTrade['sl']:
                                        trades.append(-100.0)
                                        activeTrade = None
                                    elif lowAdj <= activeTrade['tp']:
                                        trades.append(100.0 * activeTrade['rr'])
                                        activeTrade = None
                                        
                                idx += 1
                                continue
                            
                            # Si no hay trade activo, ver si hay una señal en este índice
                            if sig_idx < num_sigs and signal_queue[sig_idx]['idx'] == idx:
                                s = signal_queue[sig_idx]
                                # Ajustar el TP según el minRr
                                riesgo = abs(s['entry'] - s['sl'])
                                if riesgo > 0:
                                    if s['direction'] == 'LARGO':
                                        min_tp = s['entry'] + (riesgo * minRr)
                                        adjusted_tp = max(s['original_tp'], min_tp)
                                    else:
                                        min_tp = s['entry'] - (riesgo * minRr)
                                        adjusted_tp = min(s['original_tp'], min_tp)
                                        
                                    adjusted_rr = abs(adjusted_tp - s['entry']) / riesgo
                                    activeTrade = {
                                        'direction': s['direction'],
                                        'entry': s['entry'],
                                        'sl': s['sl'],
                                        'tp': adjusted_tp,
                                        'rr': adjusted_rr
                                    }
                                sig_idx += 1
                            
                            # Optimizador: saltar directo al siguiente idx de señal o vela
                            if not activeTrade:
                                if sig_idx < num_sigs:
                                    idx = signal_queue[sig_idx]['idx']
                                else:
                                    break # Ya no hay más señales
                            else:
                                idx += 1
                                
                        # Métricas finales del combo
                        tCount = len(trades)
                        if tCount >= 2:
                            wCount = len([t for t in trades if t > 0])
                            wRate = (wCount / tCount) * 100
                            pnlNet = sum(trades)
                            
                            profitCount = sum([t for t in trades if t > 0])
                            lossCount = abs(sum([t for t in trades if t <= 0]))
                            profFactor = profitCount / lossCount if lossCount > 0 else float('inf')
                            
                            comboData = {
                                'Símbolo': symbol,
                                'Min RR': minRr,
                                'Min Conf': minConfidence,
                                'Displacement Pct': displacementPct,
                                'FVG Min Pct': fvgMinPct,
                                'Trades': tCount,
                                'Win Rate': f"{wRate:.1f}%",
                                'Profit Factor': round(profFactor, 2),
                                'PnL USD': pnlNet
                            }
                            allResultsRaw.append(comboData)
                            
                            if pnlNet > symbolBestProfit and profFactor >= 1.25 and wRate >= 42.0:
                                symbolBestProfit = pnlNet
                                symbolBestCombo = comboData

        if symbolBestCombo:
            bestResults.append(symbolBestCombo)
            print(f"  🏆 Mejor combo para {symbol}: RR={symbolBestCombo['Min RR']} | Conf={symbolBestCombo['Min Conf']}% | Disp={symbolBestCombo['Displacement Pct']} | FVG={symbolBestCombo['FVG Min Pct']} | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            print(f"  ❌ No se encontró ninguna combinación rentable y viable para {symbol}.")
            
    # Guardar reportes
    if allResultsRaw:
        dfRaw = pd.DataFrame(allResultsRaw)
        rawPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/patron4h_grid_results_all.csv"
        dfRaw.to_csv(rawPath, index=False)
        print(f"\n💾 Todos los combos guardados en: {rawPath}")
        
    if bestResults:
        dfBest = pd.DataFrame(bestResults)
        bestPath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/patron4h_grid_results_best.csv"
        dfBest.to_csv(bestPath, index=False)
        print(f"🏆 Resumen de los mejores combos guardado en: {bestPath}")
    else:
        print("\n⚠️ Ningún activo tuvo combinaciones rentables viables (PF >= 1.25, WR >= 42%).")

        try:
            import json
            from middleware.database import dbConnection
            conn = dbConnection.getConnection()
            cursor = conn.cursor()
            
            successful_symbols = {combo['Símbolo'] for combo in bestResults}
            
            for sym in ALL_SYMBOLS:
                if sym in successful_symbols:
                    combo = next(c for c in bestResults if c['Símbolo'] == sym)
                    params = {"min_rr": combo["Min RR"]}
                    paramsJson = json.dumps(params)
                    
                    sql = """
                        INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                        VALUES ('Patron4H', %s, TRUE, %s)
                        ON DUPLICATE KEY UPDATE parametersJson = VALUES(parametersJson), enabled = TRUE
                    """
                    cursor.execute(sql, (sym, paramsJson))
                else:
                    sql = """
                        INSERT INTO symbolStrategyConfig (strategy, symbol, enabled, parametersJson)
                        VALUES ('Patron4H', %s, FALSE, '{{}}')
                        ON DUPLICATE KEY UPDATE enabled = FALSE
                    """
                    cursor.execute(sql, (sym,))
                    
            conn.commit()
            print("✅ Parámetros guardados/desactivados automáticamente en la BD por símbolo (symbolStrategyConfig).")
        except Exception as e:
            print(f"❌ Error guardando parámetros en BD: {{e}}")
        finally:
            if 'cursor' in locals(): cursor.close()
            if 'conn' in locals() and hasattr(conn, 'close'): conn.close()

if __name__ == '__main__':
    asyncio.run(runPatron4HGridSearch())
