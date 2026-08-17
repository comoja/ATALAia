import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import asyncio
from datetime import datetime, timedelta
import pytz

import datetime as real_datetime

simulated_now = None

class MockDatetime(real_datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        if simulated_now is not None:
            if tz:
                return simulated_now.astimezone(tz)
            return simulated_now.replace(tzinfo=None)
        return real_datetime.datetime.now(tz)

import Sentinel.core.BaseImbalanceBot as BImb
import Sentinel.core.BreakoutNY as BNY
import Sentinel.core.BreakoutProbability as BProb
import Sentinel.core.CruceEMA as CruceEMA
import Sentinel.core.FVGDiario as FVGD
import Sentinel.core.GenericFVG as GFVG
import Sentinel.core.Ichimoku as Ichimoku
import Sentinel.core.Patron4h as Patron
import Sentinel.core.PremiumConfluence as Premium
import Sentinel.core.QTrend as QTrend
import Sentinel.core.ReversionMedia as Rev
import Sentinel.core.SesgoBiasHTF as Sesgo
import Sentinel.core.SilverBullet as Silver
import Sentinel.core.SpeedBot as Speed
import Sentinel.core.Sniper as Sniper
import dataSymbol.mainOrchestrator as mainOrch

BImb.datetime = MockDatetime
BNY.datetime = MockDatetime
BProb.datetime = MockDatetime
CruceEMA.datetime = MockDatetime
FVGD.datetime = MockDatetime
GFVG.datetime = MockDatetime
Ichimoku.datetime = MockDatetime
Patron.datetime = MockDatetime
Premium.datetime = MockDatetime
QTrend.datetime = MockDatetime
Rev.datetime = MockDatetime
Sesgo.datetime = MockDatetime
Silver.datetime = MockDatetime
Speed.datetime = MockDatetime
Sniper.datetime = MockDatetime
mainOrch.datetime = MockDatetime

# Path setup
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from middleware.database import dbManager
from middleware.utils.communications import alertaInmediata
from Sentinel.analysis import technical
from Sentinel.analysis.technical import calculateFeatures, resample_to_interval
from Sentinel.core.models import Signal
from middleware.config.constants import TIMEZONE

print("📦 Precargando configuraciones de base de datos en memoria...")
try:
    _conn = dbConnection.getConnection()
    _cursor = _conn.cursor(dictionary=True)
    
    # 1. strategyConfig
    _cursor.execute("SELECT * FROM strategyConfig")
    _strat_rows = _cursor.fetchall()
    strategy_configs = {r['strategy']: r for r in _strat_rows}
    for r in _strat_rows:
        strategy_configs[r['strategy'].lower()] = r
    
    # 2. SymbolTypeConfig
    _cursor.execute("SELECT * FROM SymbolTypeConfig")
    _type_rows = _cursor.fetchall()
    symbol_type_configs = {r['tipo']: r for r in _type_rows}
    for r in _type_rows:
        symbol_type_configs[r['tipo'].lower()] = r
    
    # 3. SentinelSymbol
    _cursor.execute("SELECT * FROM SentinelSymbol")
    _sym_rows = _cursor.fetchall()
    sentinel_symbols = {r['symbol']: r for r in _sym_rows}
    for r in _sym_rows:
        sentinel_symbols[r['symbol'].lower()] = r
        
    _cursor.close()
    _conn.close()
    print(f"✅ Configuración precargada con éxito. (Configuraciones de estrategia: {len(strategy_configs)}, Tipos: {len(symbol_type_configs)}, Símbolos: {len(sentinel_symbols)})")
except Exception as _e:
    print(f"⚠️ Error al precargar configuraciones de base de datos: {_e}")
    strategy_configs = {}
    symbol_type_configs = {}
    sentinel_symbols = {}

# Reemplazar funciones del dbManager con mocks en memoria para evitar agotar el pool
def mock_getStrategyConfig(nombreEstrategia: str):
    if not nombreEstrategia:
        return {}
    res = strategy_configs.get(nombreEstrategia) or strategy_configs.get(nombreEstrategia.lower())
    return res if res is not None else {}

def mock_getSymbol(symbol: str):
    if not symbol:
        return None
    return sentinel_symbols.get(symbol) or sentinel_symbols.get(symbol.lower())

def mock_getSymbolTypeConfig(tipo: str):
    if not tipo:
        return None
    return symbol_type_configs.get(tipo) or symbol_type_configs.get(tipo.lower())

dbManager.getStrategyConfig = mock_getStrategyConfig
dbManager.getSymbol = mock_getSymbol
dbManager.getSymbolTypeConfig = mock_getSymbolTypeConfig

# Import all strategy bots
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
from Sentinel.ml import model as mlModel
from middleware.config import constants as config

# Cachés y monkeypatching para optimización de rendimiento
current_symbol = ""
current_symbol_lrc = None
fvgs_precalc = {}

original_detect_fvgs = technical.detect_fvgs
original_calculateLrc = ReversionMediaBot.calculateLrc

def patched_detect_fvgs(df: pd.DataFrame, min_gap_pct: float = 0.0001, min_adx: float = 0, validate_mitigation: bool = True, apply_high_prob_filters: bool = False) -> list:
    if df is None or df.empty:
        return []
        
    global current_symbol
    from Sentinel.analysis.technical import _infer_interval_minutes
    minutes = _infer_interval_minutes(df)
    interval_map = {5: '5min', 15: '15min', 30: '30min', 60: '1h', 240: '4h', 1440: '1d'}
    interval_name = interval_map.get(minutes, '5min')
    
    key = (current_symbol, interval_name, apply_high_prob_filters)
    fvgs_full = fvgs_precalc.get(key)
    if fvgs_full is None:
        return original_detect_fvgs(df, min_gap_pct, min_adx, validate_mitigation, apply_high_prob_filters)
        
    last_timestamp = df.index[-1]
    last_ts_str = last_timestamp.strftime("%Y-%m-%d %H:%M:%S")
    df_len = len(df)
    
    valid_fvgs = []
    for fvg in fvgs_full:
        if fvg['timestamp'] <= last_ts_str:
            fvg_copy = fvg.copy()
            touch_idx = fvg_copy.get('touch_idx')
            fvg_copy['mitigated'] = touch_idx is not None and touch_idx < df_len
            
            if validate_mitigation:
                mitigation_idx = fvg_copy.get('mitigation_idx')
                if mitigation_idx is not None and mitigation_idx < df_len:
                    continue
            valid_fvgs.append(fvg_copy)
            
    return valid_fvgs

def patched_calculateLrc(self, closePrices, period=100, dev=2.0):
    global current_symbol_lrc
    n = len(closePrices)
    if current_symbol_lrc is not None and len(current_symbol_lrc[0]) >= n:
        return (
            current_symbol_lrc[0][:n],
            current_symbol_lrc[1][:n],
            current_symbol_lrc[2][:n],
            current_symbol_lrc[3][:n]
        )
    return original_calculateLrc(self, closePrices, period, dev)

original_calculateFeatures = technical.calculateFeatures

def patched_calculateFeatures(df):
    if df is not None and 'ema20' in df.columns:
        return df
    return original_calculateFeatures(df)

sniper_proba_cache = {}
sniper_X_full = {}
original_predictProba = mlModel.predictProba
original_cleanDataForModel = mlModel.cleanDataForModel

def patched_predictProba(model, X):
    global current_symbol
    if X.empty:
        return None
    last_ts = X.index[-1]
    proba = sniper_proba_cache.get(current_symbol, {}).get(last_ts)
    if proba is not None:
        return proba
    return original_predictProba(model, X)

def patched_cleanDataForModel(df):
    global current_symbol
    if df is None or df.empty:
        return pd.DataFrame(), pd.Series()
    if len(df) > 3000:
        return original_cleanDataForModel(df)
        
    X_full = sniper_X_full.get(current_symbol)
    if X_full is not None:
        last_ts = df.index[-1]
        return X_full.loc[:last_ts], pd.Series()
    return original_cleanDataForModel(df)

technical.calculateFeatures = patched_calculateFeatures
technical.detect_fvgs = patched_detect_fvgs
ReversionMediaBot.calculateLrc = patched_calculateLrc
mlModel.predictProba = patched_predictProba
mlModel.cleanDataForModel = patched_cleanDataForModel

def loadCandlesRange(symbol: str, startDate: str) -> pd.DataFrame:
    try:
        connection = dbConnection.getConnection()
        query = """
            SELECT timestamp as datetime, open, high, low, close, volume
            FROM candles
            WHERE symbol = %s AND timeframe = '5min' AND timestamp >= %s
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, connection, params=(symbol, startDate))
        connection.close()
        if df.empty:
            return pd.DataFrame()
        df['datetime'] = pd.to_datetime(df['datetime'])
        df.set_index('datetime', inplace=True)
        return df
    except Exception as e:
        print(f"❌ Error al cargar velas para {symbol}: {e}")
        return pd.DataFrame()

def _linear_regression_slope(series):
    x = np.arange(len(series))
    y = series.values
    slope, _ = np.polyfit(x, y, 1)
    return slope

def classify_weekly_trend(df_daily: pd.DataFrame, symbol: str) -> dict:
    diasTendencia = 14
    if df_daily is None or len(df_daily) < diasTendencia:
        return {"trend": "NEUTRAL", "strength": 0}
    
    df = df_daily.tail(diasTendencia).copy()
    df['ema20'] = df['close'].ewm(span=20, adjust=False).mean()
    df['atr'] = ta.ATR(df['high'].values.astype(float), df['low'].values.astype(float), df['close'].values.astype(float), timeperiod=14)
    
    last_price = float(df['close'].iloc[-1])
    last_atr = float(df['atr'].iloc[-1]) if not pd.isna(df['atr'].iloc[-1]) else 0.0001
    if last_atr == 0:
        last_atr = 0.0001
        
    slope = _linear_regression_slope(df['close'])
    strength = slope / last_atr
    
    if slope > 0:
        trend = "ALCISTA"
    elif slope < 0:
        trend = "BAJISTA"
    else:
        trend = "NEUTRAL"
        
    return {
        "trend": trend,
        "strength": round(strength, 4),
        "slope": round(slope, 4),
        "price": last_price
    }

def simulateTrade(df5m: pd.DataFrame, signal: Signal, t: datetime) -> dict:
    direction = signal.direction
    entryPrice = signal.entry_price
    stopLoss = signal.stop_loss
    takeProfit = signal.take_profit
    
    df_post = df5m.loc[t + pd.Timedelta(minutes=5):]
    if df_post.empty:
        return None
        
    win = None
    close_time = None
    
    for t_post, row in df_post.iterrows():
        if direction == "LARGO":
            if float(row['low']) <= stopLoss:
                win = False
                close_time = t_post
                break
            if float(row['high']) >= takeProfit:
                win = True
                close_time = t_post
                break
        elif direction == "CORTO":
            if float(row['high']) >= stopLoss:
                win = False
                close_time = t_post
                break
            if float(row['low']) <= takeProfit:
                win = True
                close_time = t_post
                break
                
    if win is None:
        last_close = float(df_post['close'].iloc[-1])
        close_time = df_post.index[-1]
        if direction == "LARGO":
            win = last_close > entryPrice
        else:
            win = last_close < entryPrice
            
    sl_dist = abs(entryPrice - stopLoss)
    pnl_mult = 0.0
    if sl_dist > 0:
        if win:
            pnl_mult = abs(takeProfit - entryPrice) / sl_dist
        else:
            pnl_mult = -1.0
            
    return {
        'win': win,
        'pnl_mult': pnl_mult,
        'close_time': close_time
    }

async def runTwoWeekGlobalBacktest() -> None:
    print("\n" + "="*70)
    print("🚀  INICIANDO BACKTEST GLOBAL DE 2 SEMANAS DE PRODUCCIÓN  🚀")
    print("="*70)

    # 1. Obtener todos los símbolos activos
    rawSymbols = dbManager.getSymbols()
    activeSymbols = [s['symbol'] for s in rawSymbols if s.get('Activo') == 1]
    
    # 2. Definir las 17 estrategias core
    allStrategies = [
        'BreakoutNY', 'BreakoutProbability', 'CruceEMA', 'FVGDiario', 'GenericFVG',
        'Ichimoku', 'ImbalanceLDN', 'ImbalanceNY', 'ImbalancePMNY', 'Patron4h',
        'PremiumConfluence', 'QTrend', 'ReversionMedia', 'SesgoBiasHTF', 'SilverBullet',
        'Sniper', 'SpeedBot'
    ]

    # Periodo de backtesting (últimos 14 días)
    compoundingStartDate = datetime.now() - timedelta(days=14)
    compoundingEndDate = datetime.now()
    
    # Calentamiento extendido de 60 días para convergencia de indicadores
    historyStartDateStr = (compoundingStartDate - timedelta(days=60)).strftime('%Y-%m-%d') + ' 00:00:00'
    
    print(f"📅 Rango Historial (Calentamiento): Desde {historyStartDateStr}")
    print(f"📅 Rango Evaluación (2 Semanas): Desde {compoundingStartDate.strftime('%Y-%m-%d')} hasta {compoundingEndDate.strftime('%Y-%m-%d')}")
    print(f"💱 Símbolos Activos ({len(activeSymbols)}): {activeSymbols}")
    print(f"⚙️  Estrategias Evaluadas ({len(allStrategies)}): {allStrategies}\n")

    # Cargar modelo ML para Sniper
    model = mlModel.loadModel(config.MODEL_FILE_PATH)
    
    # Instanciar bots
    bots = {
        'Sniper': SniperBot(mlModelInstance=model),
        'CruceEMA': CruceEMABot(),
        'ImbalanceNY': ImbalanceNYBot(),
        'ImbalanceLDN': ImbalanceLDNBot(),
        'Patron4h': Patron4HBot(),
        'SesgoBiasHTF': SesgoBiasHTFBot(),
        'SilverBullet': SilverBulletBot(),
        'ImbalancePMNY': ImbalancePMNYBot(),
        'GenericFVG': GenericFVGBot(intervals=['5min', '15min', '1h', '4h']),
        'FVGDiario': FVGDiarioBot(),
        'SpeedBot': SpeedBot(intervals=['5min']),
        'BreakoutNY': BreakoutNYBot(),
        'Ichimoku': IchimokuBot(),
        'ReversionMedia': ReversionMediaBot(),
        'QTrend': QTrendBot(),
        'BreakoutProbability': BreakoutProbabilityBot(),
        'PremiumConfluence': PremiumConfluenceBot()
    }

    allTrades = []

    # Bucle por símbolo
    for symbol in activeSymbols:
        print(f"\n📖 Cargando velas de 5min (historial extendido) para {symbol}...")
        df5m = loadCandlesRange(symbol, historyStartDateStr)
        if df5m.empty or len(df5m) < 100:
            print(f"  ⚠️ Datos insuficientes para {symbol}")
            continue

        # Localizar huso horario CDMX
        cdmxTz = pytz.timezone(TIMEZONE)
        if df5m.index.tzinfo is None:
            df5m.index = df5m.index.tz_localize(cdmxTz)
        else:
            df5m.index = df5m.index.tz_convert(cdmxTz)

        # Filtrar velas terminadas
        from middleware.utils.time_utils import get_last_closed_candle
        lastClosed5m = get_last_closed_candle(datetime.now(cdmxTz), 5)
        df5m = df5m[df5m.index <= lastClosed5m].copy()

        # Enriquecer la serie completa una sola vez
        df5m_feat = calculateFeatures(df5m)
        if df5m_feat is None:
            continue

        # Precalcular otros timeframes
        df15m = calculateFeatures(resample_to_interval(df5m_feat, "15min"))
        df30m = calculateFeatures(resample_to_interval(df5m_feat, "30min"))
        df1h  = calculateFeatures(resample_to_interval(df5m_feat, "1h"))
        df4h  = calculateFeatures(resample_to_interval(df5m_feat, "4h"))
        df1d  = calculateFeatures(resample_to_interval(df5m_feat, "1d"))

        compoundingStartAware = cdmxTz.localize(compoundingStartDate) if compoundingStartDate.tzinfo is None else compoundingStartDate.astimezone(cdmxTz)
        compoundingEndAware = cdmxTz.localize(compoundingEndDate) if compoundingEndDate.tzinfo is None else compoundingEndDate.astimezone(cdmxTz)
        test_timestamps = df15m.index[(df15m.index >= compoundingStartAware) & (df15m.index <= compoundingEndAware)]

        symbolInfo = next((s for s in rawSymbols if s['symbol'] == symbol), None)
        if not symbolInfo:
            symbolInfo = {
                'symbol': symbol,
                'tipo': 'MONEDA',
                'broker': 1,
                'refCapital': 10000.0,
                'refRiskPct': 1.0,
                'pip': 0.01 if "JPY" in symbol else 0.0001
            }

        # Calcular tendencia semanal
        symbolInfo['weekly_trend'] = classify_weekly_trend(df1d, symbol)

        # Inicializar cachés globales para este símbolo
        global current_symbol, current_symbol_lrc
        current_symbol = symbol
        
        # 1. Precalcular LRC para ReversionMedia en 1h
        if df1h is not None and not df1h.empty:
            try:
                _temp_rm_bot = ReversionMediaBot()
                _center, _upper, _lower, _slope = original_calculateLrc(_temp_rm_bot, df1h['close'].values, period=100, dev=2.0)
                current_symbol_lrc = (_center, _upper, _lower, _slope)
            except Exception as _e:
                print(f"  ⚠️ Error precalculando LRC para {symbol}: {_e}")
                current_symbol_lrc = None
        else:
            current_symbol_lrc = None

        # 2. Precalcular FVGs completos para este símbolo
        global fvgs_precalc
        fvgs_precalc = {}
        interval_data = {
            '5min': df5m_feat,
            '15min': df15m,
            '30min': df30m,
            '1h': df1h,
            '4h': df4h,
            '1d': df1d
        }
        for interval_name, df_full in interval_data.items():
            if df_full is not None and not df_full.empty:
                full_closes = df_full['close'].values.astype(float)
                full_highs = df_full['high'].values.astype(float)
                full_lows = df_full['low'].values.astype(float)
                
                for high_prob in [True, False]:
                    try:
                        fvgs_full = original_detect_fvgs(df_full, apply_high_prob_filters=high_prob, validate_mitigation=False)
                        for fvg in fvgs_full:
                            fvg_idx = fvg['idx']
                            is_bullish = fvg['type'] == 'Bullish_FVG'
                            gap_mid = fvg['mid']
                            top_val = fvg['top']
                            bottom_val = fvg['bottom']
                            
                            # touch_idx
                            touch_idx = None
                            for k in range(fvg_idx + 1, len(df_full)):
                                if is_bullish:
                                    if full_lows[k] <= top_val:
                                        touch_idx = k
                                        break
                                else:
                                    if full_highs[k] >= bottom_val:
                                        touch_idx = k
                                        break
                            fvg['touch_idx'] = touch_idx
                            
                            # mitigation_idx
                            mitigation_idx = None
                            for k in range(fvg_idx + 1, len(df_full)):
                                if is_bullish:
                                    if full_closes[k] <= gap_mid:
                                        mitigation_idx = k
                                        break
                                else:
                                    if full_closes[k] >= gap_mid:
                                        mitigation_idx = k
                                        break
                            fvg['mitigation_idx'] = mitigation_idx
                            
                        fvgs_precalc[(symbol, interval_name, high_prob)] = fvgs_full
                    except Exception as _e:
                        print(f"  ⚠️ Error precalculando FVGs para {symbol} {interval_name} (HighProb={high_prob}): {_e}")

        # 3. Precalcular Sniper ML
        global sniper_proba_cache, sniper_X_full
        sniper_proba_cache = {}
        sniper_X_full = {}
        if df15m is not None and not df15m.empty:
            try:
                df_target = mlModel.defineMlTarget(df15m)
                X_full, _ = original_cleanDataForModel(df_target)
                if not X_full.empty:
                    sniper_X_full[symbol] = X_full
                    probabilities = model.predict_proba(X_full)
                    classes = list(getattr(model, "classes_", []))
                    class_1_index = classes.index(1) if 1 in classes else 1
                    proba_dict = {X_full.index[k]: float(probabilities[k][class_1_index]) for k in range(len(X_full))}
                    sniper_proba_cache[symbol] = proba_dict
            except Exception as _e:
                print(f"  ⚠️ Error precalculando Sniper ML para {symbol}: {_e}")

        # Simular cada estrategia sobre este símbolo
        for strategy in allStrategies:
            bot = bots.get(strategy)
            if not bot:
                continue

            print(f"  ⏳ Simulando {strategy}...")
            active_trade_until = None

            for t in test_timestamps:
                global simulated_now
                simulated_now = t
                
                if active_trade_until is not None and t < active_trade_until:
                    continue
                else:
                    active_trade_until = None

                slice5m = df5m_feat.loc[:t]
                if len(slice5m) < 40:
                    continue

                preloaded_master = {
                    '5min': slice5m,
                    '15min': df15m.loc[:t],
                    '30min': df30m.loc[:t],
                    '1h': df1h.loc[:t],
                    '4h': df4h.loc[:t],
                    '1d': df1d.loc[:t]
                }

                try:
                    res = await bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master})
                except Exception:
                    continue

                signals = []
                if res is not None:
                    if isinstance(res, list):
                        signals = res
                    else:
                        signals = [res]

                for sig in signals:
                    trade_result = simulateTrade(df5m_feat, sig, t)
                    if trade_result:
                        allTrades.append({
                            'datetime': t,
                            'symbol': symbol,
                            'strategy': strategy,
                            'direction': sig.direction,
                            'pnl_mult': trade_result['pnl_mult'],
                            'win': trade_result['win'],
                            'close_time': trade_result['close_time'],
                            'hour': t.hour
                        })
                        active_trade_until = trade_result['close_time']
                        break

    print("\n" + "="*70)
    print("📊  PROCESANDO RESULTADOS Y ACTUALIZANDO BASE DE DATO MySQL  📊")
    print("="*70)

    # 1. Crear DataFrame con los trades simulados
    dfRawTrades = pd.DataFrame(allTrades)
    
    # 2. Filtrar trades por la ventana operativa (05:00 a 16:30 CDMX)
    if not dfRawTrades.empty:
        dfRawTrades = dfRawTrades.sort_values(by='datetime').reset_index(drop=True)
        dfRawTrades = dfRawTrades[
            (dfRawTrades['datetime'].dt.hour > 5) | 
            ((dfRawTrades['datetime'].dt.hour == 5) & (dfRawTrades['datetime'].dt.minute >= 0))
        ]
        dfRawTrades = dfRawTrades[
            (dfRawTrades['datetime'].dt.hour < 16) | 
            ((dfRawTrades['datetime'].dt.hour == 16) & (dfRawTrades['datetime'].dt.minute <= 30))
        ].reset_index(drop=True)

    # 3. Agrupar métricas para cada combinación (símbolo, estrategia)
    records = []
    
    # Generar todos los pares posibles (234 en total) para no omitir inactivos
    for symbol in activeSymbols:
        for strategy in allStrategies:
            # Filtrar los trades de esta combinación
            if not dfRawTrades.empty:
                combo_trades = dfRawTrades[(dfRawTrades['symbol'] == symbol) & (dfRawTrades['strategy'] == strategy)]
            else:
                combo_trades = pd.DataFrame()
                
            total_trades = len(combo_trades)
            
            if total_trades > 0:
                wins = int(combo_trades['win'].sum())
                losses = total_trades - wins
                win_rate = (wins / total_trades) * 100.0
                
                gains = combo_trades[combo_trades['pnl_mult'] > 0]['pnl_mult'].sum()
                loss_sum = abs(combo_trades[combo_trades['pnl_mult'] < 0]['pnl_mult'].sum())
                profit_factor = float(gains / loss_sum) if loss_sum > 0 else (99.99 if gains > 0 else 0.0)
                pnl_neto = float(combo_trades['pnl_mult'].sum())
            else:
                wins = 0
                losses = 0
                win_rate = 0.0
                profit_factor = 0.0
                pnl_neto = 0.0
                
            # Evaluar viabilidad
            # Umbrales: Si no hay trades, se asume viable. Si hay trades: Profit Factor >= 1.25, Win Rate >= 42.0%
            if total_trades > 0:
                is_viable = (profit_factor >= 1.25) and (win_rate >= 42.0)
            else:
                is_viable = True
            
            # Registrar resultado
            records.append({
                'symbol': symbol,
                'strategy': strategy,
                'total_trades': total_trades,
                'wins': wins,
                'losses': losses,
                'win_rate': round(win_rate, 2),
                'profit_factor': round(profit_factor, 2),
                'pnl_neto': round(pnl_neto, 2),
                'viable': is_viable
            })

    dfResults = pd.DataFrame(records)
    
    # Guardar CSV de resultados consolidados
    csv_path = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/backtest_2weeks_results.csv"
    dfResults.to_csv(csv_path, index=False)
    print(f"✅ Reporte CSV de 2 semanas guardado en: {csv_path}")

    # 4. Actualizar la base de datos MySQL (tabla symbolNotStrategia)
    try:
        conn = dbConnection.getConnection()
        if conn is None:
            print("❌ No se pudo conectar a la base de datos para sembrar exclusiones.")
            return
            
        cur = conn.cursor()
        
        # Eliminar y reemplazar de forma secuencial
        deletes = []
        inserts = []
        
        for res in records:
            symbol = res['symbol']
            strategy = res['strategy']
            total_trades = res['total_trades']
            viable = res['viable']
            
            if total_trades > 0 and not viable:
                inserts.append((symbol, strategy, f"Bajo rendimiento (Win Rate: {res['win_rate']:.2f}%, Profit Factor: {res['profit_factor']:.2f}, PNL: {res['pnl_neto']:.2f})"))
            elif total_trades > 0 and viable:
                deletes.append((symbol, strategy))
            elif total_trades == 0:
                # Ya no penalizamos las que tienen 0 trades. Solamente se remueven de la tabla si estuvieran antes por error.
                deletes.append((symbol, strategy))
                
        # Ejecutar DELETEs para los que se volvieron viables (para que se evalúen en Sentinel)
        if deletes:
            cur.executemany("DELETE FROM symbolNotStrategia WHERE symbol = %s AND strategy = %s", deletes)
            print(f"✅ Se eliminaron {len(deletes)} combinaciones viables de 'symbolNotStrategia'.")
            
        # Ejecutar REPLACEs para los ineficientes/inactivos
        if inserts:
            cur.executemany("REPLACE INTO symbolNotStrategia (symbol, strategy, reason) VALUES (%s, %s, %s)", inserts)
            print(f"✅ Se insertaron/actualizaron {len(inserts)} exclusiones en 'symbolNotStrategia'.")
            
        conn.commit()
        
        # Mostrar conteo final
        cur.execute("SELECT COUNT(*) FROM symbolNotStrategia")
        countVal = cur.fetchone()[0]
        print(f"📊 Total de registros actuales en 'symbolNotStrategia': {countVal}")
        
        cur.close()
        conn.close()
        print("🎉 Proceso de siembra finalizado con éxito.")
    except Exception as e:
        print(f"❌ Error al interactuar con MySQL: {e}")

if __name__ == '__main__':
    asyncio.run(runTwoWeekGlobalBacktest())

    # 5. Enviar el CSV a la cuenta 1 de Telegram
    try:
        import asyncio
        from middleware.utils.communications import alertaInmediata
        msg = "📊 *Reporte Global de Backtest (2 Semanas)*\n\nAdjunto el archivo CSV con los resultados y métricas."
        asyncio.run(alertaInmediata(1, msg, prioridad=False, filePath=csv_path))
        print("✅ Reporte enviado a Telegram (Cuenta 1).")
    except Exception as e:
        print(f"⚠️ Error enviando el CSV por Telegram: {e}")
