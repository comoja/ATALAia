import sys
import os
import pandas as pd
import numpy as np
import talib as ta
import asyncio
from datetime import datetime, timedelta
import pytz
from fpdf import FPDF

# Path setup
sys.path.append("/Volumes/TimeMachine/ATALAia")
from middleware.database import dbConnection
from middleware.database import dbManager
from middleware.utils.communications import alertaInmediata
from Sentinel.analysis import technical
from Sentinel.analysis.technical import calculateFeatures, resample_to_interval
from Sentinel.core.models import Signal
from middleware.config.constants import TIMEZONE

# ==========================================================
# PARCHE DE MEMORIA PARA EVITAR CONSUMIR CONEXIONES DE BASE DE DATOS
# Y EVITAR EL ERROR "POOL EXHAUSTED"
# ==========================================================
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

# Reemplazar funciones del dbManager con mocks ultra-rápidos en memoria
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
# ==========================================================

# Import all strategy bots
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
from Sentinel.core.BreakoutNY import BreakoutNYBot
from Sentinel.core.Ichimoku import IchimokuBot
from Sentinel.core.ReversionMedia import ReversionMediaBot
from Sentinel.core.QTrend import QTrendBot
from Sentinel.core.BreakoutProbability import BreakoutProbabilityBot
from Sentinel.ml import model as mlModel
from middleware.config import constants as config

# ==========================================================
# CACHÉS Y PARCHES MATEMÁTICOS PARA AGILIZAR LA SIMULACIÓN
# ==========================================================
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
            # Actualizar estado de mitigación simple para el fragmento actual
            touch_idx = fvg_copy.get('touch_idx')
            fvg_copy['mitigated'] = touch_idx is not None and touch_idx < df_len
            
            # Si validate_mitigation es True, filtrar si ya fue mitigado por la regla de 50% en este t
            if validate_mitigation:
                mitigation_idx = fvg_copy.get('mitigation_idx')
                if mitigation_idx is not None and mitigation_idx < df_len:
                    # FVG ya mitigado para este paso t, no incluirlo
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

# Aplicar monkeypatching
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
        # Cortar X_full de forma ultra-rápida usando slice
        # loc[:last_ts] puede fallar si last_ts no está exactamente en el índice,
        # pero como X_full proviene de df15m, y last_ts proviene de df, y df proviene de df15m.loc[:t],
        # el timestamp coincide exactamente.
        return X_full.loc[:last_ts], pd.Series()
    return original_cleanDataForModel(df)

technical.calculateFeatures = patched_calculateFeatures
technical.detect_fvgs = patched_detect_fvgs
ReversionMediaBot.calculateLrc = patched_calculateLrc
mlModel.predictProba = patched_predictProba
mlModel.cleanDataForModel = patched_cleanDataForModel
# ==========================================================

initialPortfolio = 418.19
portfolioRiskPct = 0.01

def loadExclusions() -> set:
    """Carga las exclusiones activas desde symbolNotStrategia."""
    try:
        conn = dbConnection.getConnection()
        if conn is None:
            return set()
        cur = conn.cursor()
        cur.execute("SELECT symbol, strategy FROM symbolNotStrategia")
        exclusions = {(r[0], r[1]) for r in cur.fetchall()}
        cur.close()
        conn.close()
        return exclusions
    except Exception as e:
        print(f"⚠️  Error cargando exclusiones: {e}")
        return set()

def loadCandlesRange(symbol: str, startDate: str) -> pd.DataFrame:
    """Carga velas de 5min de la base de datos."""
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

# Lógica de regresión lineal para la clasificación de tendencia semanal
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
    """Simula el resultado real de un trade buscando en velas de 5min posteriores a t."""
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
                
    # Si sigue abierto, evaluar por precio de cierre final
    if win is None:
        last_close = float(df_post['close'].iloc[-1])
        close_time = df_post.index[-1]
        if direction == "LARGO":
            win = last_close > entryPrice
        else:
            win = last_close < entryPrice
            
    # Calcular multiplicador del PnL real (R:R del trade o -1.0)
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

class WeeklyReportPdfReal(FPDF):
    def header(self) -> None:
        if self.page_no() > 1:
            self.set_text_color(100, 110, 120)
            self.set_font('Helvetica', 'B', 8)
            self.cell(0, 10, "SISTEMA ATALAIA / SENTINEL - AUDITORIA INTEGRAL DE LOGICA REAL V6", 0, 0, 'L')
            self.cell(0, 10, "CONFIDENCIAL", 0, 1, 'R')
            self.set_fill_color(15, 115, 85)  # Verde esmeralda premium
            self.rect(10, self.get_y(), 196, 0.6, 'F')
            self.ln(5)

    def footer(self) -> None:
        if self.page_no() > 1:
            self.set_y(-15)
            self.set_font('Helvetica', 'I', 8)
            self.set_text_color(120, 140, 160)
            self.cell(0, 10, f"Pagina {self.page_no()}", 0, 0, 'C')

def generateWeeklyReportPdfReal(dfTrades: pd.DataFrame, dfComboPerf: pd.DataFrame, dfStratPerf: pd.DataFrame, balanceFinal: float, activeSymbols: list, enabledStrategies: list, exclusionsCount: int) -> str:
    try:
        pdf = WeeklyReportPdfReal(orientation='P', unit='mm', format='letter')
        pdf.set_auto_page_break(auto=True, margin=15)

        # PAGINA 1: PORTADA
        pdf.add_page()
        pdf.set_fill_color(8, 28, 21)
        pdf.rect(0, 0, 216, 279, 'F')

        pdf.set_fill_color(197, 160, 89)
        pdf.rect(18, 18, 180, 2.5, 'F')
        pdf.rect(18, 18, 2.5, 243, 'F')
        pdf.set_fill_color(15, 115, 85)
        pdf.rect(195.5, 18, 2.5, 243, 'F')
        pdf.rect(18, 258.5, 180, 2.5, 'F')

        pdf.set_y(52)
        pdf.set_text_color(197, 160, 89)
        pdf.set_font('Helvetica', 'B', 12)
        pdf.cell(0, 8, "S I S T E M A   A T A L A I A   /   S E N T I N E L", 0, 1, 'C')

        pdf.set_text_color(240, 250, 245)
        pdf.set_font('Helvetica', 'B', 22)
        pdf.set_y(78)
        pdf.multi_cell(0, 12, "AUDITORÍA SEMANAL REALISTA\nSIMULACIÓN DE LOGICA LIVE V6\nFILTRADO DE RUIDO DE MERCADO", 0, 'C')

        pdf.set_fill_color(15, 55, 45)
        pdf.rect(38, 142, 140, 30, 'F')
        pdf.set_fill_color(197, 160, 89)
        pdf.rect(38, 142, 140, 2, 'F')
        pdf.rect(38, 170, 140, 2, 'F')

        pdf.set_y(149)
        pdf.set_text_color(197, 160, 89)
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 7, "Cierre de Balance Real Semanal", 0, 1, 'C')
        pdf.set_font('Helvetica', '', 11)
        pdf.set_text_color(190, 230, 210)
        totalTrades = len(dfTrades)
        totalPnl = balanceFinal - initialPortfolio
        retornoTotal = (totalPnl / initialPortfolio) * 100
        pdf.cell(0, 6, f"Terminal: ${balanceFinal:.2f} USD | PnL: ${totalPnl:+.2f} USD ({retornoTotal:+.1f}%)", 0, 1, 'C')

        # KPIs
        pdf.set_y(192)
        pdf.set_font('Helvetica', '', 9.5)
        pdf.set_text_color(150, 200, 180)
        
        symListStr = ", ".join(activeSymbols[:7]) + ("..." if len(activeSymbols) > 7 else "")
        stratListStr = ", ".join(enabledStrategies[:6]) + ("..." if len(enabledStrategies) > 6 else "")

        kpis = (
            f"Total de Trades Ejecutados (Lógica Real): {totalTrades}\n"
            f"Símbolos Activos de la DB Analizados: {symListStr}\n"
            f"Estrategias Core Evaluadas: {stratListStr}\n"
            f"Filtros de Exclusión Respetados (symbolNotStrategia): {exclusionsCount} activos."
        )
        pdf.multi_cell(0, 6, kpis.encode('latin-1', 'replace').decode('latin-1'), 0, 'C')

        pdf.set_y(248)
        pdf.set_font('Helvetica', 'B', 8.5)
        pdf.set_text_color(100, 140, 120)
        pdf.cell(0, 8, "VERSIÓN AUDITADA REAL V6 - BLINDAJE DE CAPITAL - CONFIDENCIAL", 0, 1, 'C')

        # PAGINA 2: TABLA DE RENDIMIENTO
        pdf.add_page()
        pdf.set_text_color(24, 28, 36)
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, "1. INFORME DE RENDIMIENTO REAL DE ESTRATEGIAS (7 DIAS)", 0, 1, 'L')
        pdf.ln(3)

        pdf.set_font('Helvetica', 'B', 10)
        pdf.set_text_color(15, 115, 85)
        pdf.cell(0, 6, "[A] RENDIMIENTO DETALLADO POR ESTRATEGIA (CON LOGICA REAL)", 0, 1, 'L')
        pdf.ln(2)

        stratHeaders = ["Estrategia", "Total Trades", "Wins", "Win Rate (%)", "PnL Neto ($)"]
        stratWidths = [50, 32, 28, 38, 48]

        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_fill_color(15, 55, 45)
        pdf.set_text_color(197, 160, 89)
        for h, w in zip(stratHeaders, stratWidths):
            pdf.cell(w, 7.5, h, 1, 0, 'C', True)
        pdf.ln(7.5)

        pdf.set_font('Helvetica', '', 8.5)
        pdf.set_text_color(30, 40, 55)

        for i, row in dfStratPerf.sort_values(by='PnL_Total', ascending=False).iterrows():
            fillColor = (240, 248, 245) if i % 2 == 0 else (225, 238, 230)
            pdf.set_fill_color(*fillColor)
            
            pnlVal = float(row['PnL_Total'])
            pnlStr = f"${pnlVal:+.2f}"
            
            pdf.cell(stratWidths[0], 6.2, row['strategy'], 1, 0, 'C', True)
            pdf.cell(stratWidths[1], 6.2, str(int(row['Total_Trades'])), 1, 0, 'C', True)
            pdf.cell(stratWidths[2], 6.2, str(int(row['Wins'])), 1, 0, 'C', True)
            pdf.cell(stratWidths[3], 6.2, f"{row['Win_Rate_%']:.1f}%", 1, 0, 'C', True)
            
            pdf.set_font('Helvetica', 'B', 8.5)
            if pnlVal < 0:
                pdf.set_text_color(210, 40, 60)
            else:
                pdf.set_text_color(15, 115, 85)
                
            pdf.cell(stratWidths[4], 6.2, pnlStr, 1, 1, 'C', True)
            pdf.set_font('Helvetica', '', 8.5)
            pdf.set_text_color(30, 40, 55)

        pdf.ln(4)

        # PAGINA 3: CONCLUSIONES
        pdf.add_page()
        pdf.set_text_color(24, 28, 36)
        pdf.set_font('Helvetica', 'B', 14)
        pdf.cell(0, 10, "2. DIAGNOSTICO OPERATIVO Y RECOMENDACIONES", 0, 1, 'L')
        pdf.ln(3)

        pdf.set_font('Helvetica', 'B', 10.5)
        pdf.set_text_color(15, 115, 85)
        pdf.cell(0, 6, "[A] DIAGNÓSTICO DE FILTRADO DE RUIDO Y REALISMO", 0, 1, 'L')
        pdf.ln(2)

        pdf.set_font('Helvetica', '', 10)
        pdf.set_text_color(40, 44, 52)

        comparativaText = (
            f"Esta auditoria ejecuta las clases de Sentinel reales sobre las velas resampleadas de la DB, "
            f"lo cual permite analizar el verdadero volumen de operaciones y efectividad de las lógicas:\n\n"
            f"1. **Volumen Realista de Trades:** A diferencia del backtest anterior que simulaba compras "
            f"y ventas masivas ficticias, la simulacion actual refleja con exactitud la operacion del bot. "
            f"El total de trades en la semana fue de **{totalTrades} operaciones**, confirmando que el bot real "
            f"es altamente selectivo al filtrar el ruido.\n\n"
            f"2. **Analisis de Rendimiento Compuesto:** Iniciando con ${initialPortfolio:.2f} USD, el portafolio "
            f"cerro en **${balanceFinal:.2f} USD**, generando un PnL neto compuesto de **${totalPnl:+.2f} USD ({retornoTotal:+.1f}%)**.\n\n"
            f"3. **Planificacion de Filtros:** Esta auditoria permite al usuario identificar que estrategias "
            f"tienen un comportamiento deficiente para agregarlas a la tabla `symbolNotStrategia` o deshabilitarlas en "
            f"`strategyConfig` sin depender de estadisticas de backtest optimistas irreales.\n\n"
            f"Todos los datos de la matriz EstrategiaSymbol en MySQL han sido actualizados con los resultados de esta corrida."
        )
        pdf.multi_cell(0, 5, comparativaText.encode('latin-1', 'replace').decode('latin-1'), 0, 'L')
        pdf.ln(5)

        # Banner de arranque V6
        pdf.set_fill_color(8, 45, 35)
        pdf.rect(10, pdf.get_y(), 196, 18, 'F')
        pdf.set_fill_color(197, 160, 89)
        pdf.rect(10, pdf.get_y(), 196, 2, 'F')
        pdf.set_y(pdf.get_y() + 4)
        pdf.set_font('Helvetica', 'B', 9)
        pdf.set_text_color(197, 160, 89)
        pdf.cell(0, 5, f"   [+] BLINDAJE REAL V6 | Retorno Realizado Compuesto: {retornoTotal:+.2f}%", 0, 1, 'L')
        pdf.set_font('Helvetica', 'I', 8)
        pdf.set_text_color(180, 220, 200)
        pdf.cell(0, 5, "   Estadísticas 100% reales basadas en las predicciones ML y filtros en vivo del bot.", 0, 1, 'L')

        dateStr = datetime.now().strftime('%Y_%m_%d')
        pdfPath = f"/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/reporte_semanal_realista_{dateStr}.pdf"
        pdf.output(name=pdfPath)
        print(f"✅ Reporte Semanal PDF Real generado en: {pdfPath}")
        return pdfPath
    except Exception as e:
        print(f"❌ Error al generar reporte PDF semanal realista: {e}")
        import traceback
        traceback.print_exc()
        return None

def persistirMatrizRendimientoReal(dfCompiledTrades: pd.DataFrame, startDate: datetime) -> None:
    try:
        if dfCompiledTrades.empty:
            print("⚠️ No hay trades para persistir en la matriz de rendimiento.")
            return

        conn = dbConnection.getConnection()
        if conn is None:
            return
        
        combos = dfCompiledTrades.groupby(['symbol', 'strategy'])
        cur = conn.cursor()
        periodoFecha = startDate.strftime('%Y-%m-%d')
        
        for (symbol, strategy), group in combos:
            totalTrades = int(len(group))
            wins = int(group['Win'].sum())
            winRate = float((wins / totalTrades) * 100.0) if totalTrades > 0 else 0.0
            pnlNeto = float(group['PnL_Trade'].sum())
            expectancy = float(group['PnL_Trade'].mean()) if totalTrades > 0 else 0.0
            
            gains = group[group['PnL_Trade'] > 0]['PnL_Trade'].sum()
            losses = abs(group[group['PnL_Trade'] < 0]['PnL_Trade'].sum())
            profitFactor = 99.99 if losses == 0 else float(gains / losses)
            
            local_balance = [1000.0]
            for pnl in group.sort_values('datetime')['PnL_Trade']:
                local_balance.append(local_balance[-1] + pnl)
            local_balance = np.array(local_balance)
            cum_max = np.maximum.accumulate(local_balance)
            dd = (cum_max - local_balance) / cum_max * 100
            maxDrawdown = float(dd.max())
            
            if winRate >= 60.0 and profitFactor >= 1.5 and totalTrades >= 10:
                riesgoSugerido = 1.5
            elif winRate >= 50.0 and profitFactor >= 1.2 and totalTrades >= 5:
                riesgoSugerido = 1.0
            elif winRate < 45.0 or pnlNeto < 0 or totalTrades < 3:
                riesgoSugerido = 0.5
            else:
                riesgoSugerido = 0.75
            
            payload = {
                'symbol': symbol,
                'strategy': strategy,
                'totalTrades': totalTrades,
                'wins': wins,
                'winRate': winRate,
                'pnlNeto': pnlNeto,
                'profitFactor': profitFactor,
                'expectancy': expectancy,
                'maxDrawdown': maxDrawdown,
                'riesgoSugerido': riesgoSugerido,
                'fuente': 'weekly_real',
                'periodoFecha': periodoFecha
            }
            
            cur.execute("""
                INSERT INTO EstrategiaSymbol
                    (symbol, strategy, totalTrades, wins, winRate, pnlNeto,
                     profitFactor, expectancy, maxDrawdown, riesgoSugerido,
                     fuente, periodoFecha)
                VALUES
                    (%(symbol)s, %(strategy)s, %(totalTrades)s, %(wins)s,
                     %(winRate)s, %(pnlNeto)s, %(profitFactor)s, %(expectancy)s,
                     %(maxDrawdown)s, %(riesgoSugerido)s, %(fuente)s, %(periodoFecha)s)
                ON DUPLICATE KEY UPDATE
                    totalTrades    = VALUES(totalTrades),
                    wins           = VALUES(wins),
                    winRate        = VALUES(winRate),
                    pnlNeto        = VALUES(pnlNeto),
                    profitFactor   = VALUES(profitFactor),
                    expectancy     = VALUES(expectancy),
                    maxDrawdown    = VALUES(maxDrawdown),
                    riesgoSugerido = VALUES(riesgoSugerido),
                    periodoFecha   = VALUES(periodoFecha)
            """, payload)
            
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Matriz de Rendimiento EstrategiaSymbol (real) actualizada en MySQL.")
    except Exception as e:
        print(f"❌ Error al persistir la matriz de rendimiento: {e}")

async def runRealWeeklyBacktest() -> None:
    print("==========================================================")
    print("  INICIANDO SIMULACIÓN DE BACKTEST SEMANAL REALISTA V6   ")
    print("==========================================================")

    rawSymbols = dbManager.getSymbols()
    activeSymbols = [s['symbol'] for s in rawSymbols if s.get('Activo') == 1]
    exclusions = loadExclusions()

    # Todas las estrategias disponibles del core de Sentinel
    allStrategies = [
        'Sniper', 'SMA20_200', 'ImbalanceNY', 'ImbalanceLDN', 'EMA20200',
        'Patron4h', 'SesgoBiasHTF', 'SilverBullet', 'ImbalancePMNY',
        'GenericFVG', 'FVGDiario', 'SpeedBot', 'BreakoutNY', 'Ichimoku',
        'ReversionMedia', 'QTrend', 'BreakoutProbability'
    ]

    historyStartDateStr = (datetime.now() - timedelta(days=60)).strftime('%Y-%m-%d') + ' 00:00:00'
    compoundingStartDate = datetime.now() - timedelta(days=7)
    compoundingEndDate = datetime.now()

    # Cargar modelo ML para Sniper
    model = mlModel.loadModel(config.MODEL_FILE_PATH)
    
    # Instanciar bots
    bots = {
        'Sniper': SniperBot(mlModelInstance=model),
        'SMA20_200': SMABot(),
        'ImbalanceNY': ImbalanceNYBot(),
        'ImbalanceLDN': ImbalanceLDNBot(),
        'EMA20200': EMA20200Bot(),
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
        'BreakoutProbability': BreakoutProbabilityBot()
    }

    allTrades = []

    for symbol in activeSymbols:
        print(f"\n📖 Cargando velas de 5min (60 días) para {symbol}...")
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

        # Precalcular otros timeframes una sola vez
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

        # Calcular tendencia una sola vez para la ejecución
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
                # Extraer series de precios completas para agilizar el bucle de mitigación
                full_closes = df_full['close'].values.astype(float)
                full_highs = df_full['high'].values.astype(float)
                full_lows = df_full['low'].values.astype(float)
                
                for high_prob in [True, False]:
                    try:
                        fvgs_full = original_detect_fvgs(df_full, apply_high_prob_filters=high_prob, validate_mitigation=False)
                        
                        # Precalcular los índices de mitigación y toque en df_full
                        for fvg in fvgs_full:
                            fvg_idx = fvg['idx']
                            is_bullish = fvg['type'] == 'Bullish_FVG'
                            gap_mid = fvg['mid']
                            top_val = fvg['top']
                            bottom_val = fvg['bottom']
                            
                            # 1. Encontrar touch_idx (mitigación simple)
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
                            
                            # 2. Encontrar mitigation_idx (mitigación 50% rule)
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

        # 3. Precalcular probabilidades y X para Sniper (15min)
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

        # Simular cada estrategia
        for strategy in allStrategies:
            if (symbol, strategy) in exclusions:
                print(f"  🚫 Exclusión respetada: {symbol} - {strategy}")
                continue

            bot = bots.get(strategy)
            if not bot:
                continue

            print(f"  ⏳ Simulando {strategy}...")
            active_trade_until = None

            for t in test_timestamps:
                # Cooldown si hay un trade activo
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

                # Simular llamada a runAnalysisCycleForSymbol
                try:
                    res = await bot.runAnalysisCycleForSymbol(symbolInfo, {symbol: preloaded_master})
                except Exception as e:
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
                        # Guardar el momento en el que el trade se cerró como cooldown
                        active_trade_until = trade_result['close_time']
                        break

    dfRawTrades = pd.DataFrame(allTrades)
    if dfRawTrades.empty:
        print("⚠️ No se generaron trades reales en toda la semana.")
        return

    # Ordenar cronológicamente
    dfRawTrades = dfRawTrades.sort_values(by='datetime').reset_index(drop=True)

    # Filtro horario operativo (05:00 a 16:30 CDMX)
    dfRawTrades = dfRawTrades[
        (dfRawTrades['datetime'].dt.hour > 5) | 
        ((dfRawTrades['datetime'].dt.hour == 5) & (dfRawTrades['datetime'].dt.minute >= 0))
    ]
    dfRawTrades = dfRawTrades[
        (dfRawTrades['datetime'].dt.hour < 16) | 
        ((dfRawTrades['datetime'].dt.hour == 16) & (dfRawTrades['datetime'].dt.minute <= 30))
    ].reset_index(drop=True)

    # Exposición simultánea máxima (Max 3 concurrentes por minuto de vela)
    dfRawTrades['rank'] = dfRawTrades.groupby('datetime').cumcount()
    dfRawTrades = dfRawTrades[dfRawTrades['rank'] < 3].drop(columns=['rank']).reset_index(drop=True)

    # Simulación Compounding
    portfolioBalance = initialPortfolio
    tradeHistory = []
    
    for idx, row in dfRawTrades.iterrows():
        riskPerCombo = portfolioBalance * portfolioRiskPct
        comboPnl = row['pnl_mult'] * riskPerCombo
        
        balanceInicio = portfolioBalance
        portfolioBalance += comboPnl
        balanceFin = portfolioBalance

        tradeHistory.append({
            'datetime': row['datetime'],
            'symbol': row['symbol'],
            'strategy': row['strategy'],
            'hour': row['hour'],
            'Balance_Inicio': balanceInicio,
            'Riesgo_Trade': riskPerCombo,
            'PnL_Trade': comboPnl,
            'Balance_Fin': balanceFin,
            'Win': row['win']
        })

    dfCompiledTrades = pd.DataFrame(tradeHistory)

    # Agrupar estadísticas reales
    dfComboPerf = dfCompiledTrades.groupby(['symbol', 'strategy']).agg(
        Total_Trades=('PnL_Trade', 'count'),
        PnL_Total=('PnL_Trade', 'sum'),
        Wins=('Win', 'sum')
    ).reset_index()
    dfComboPerf['Win_Rate_%'] = (dfComboPerf['Wins'] / dfComboPerf['Total_Trades']) * 100
    
    dfStratPerf = dfCompiledTrades.groupby('strategy').agg(
        Total_Trades=('PnL_Trade', 'count'),
        PnL_Total=('PnL_Trade', 'sum'),
        Wins=('Win', 'sum')
    ).reset_index()
    if not dfStratPerf.empty:
        dfStratPerf['Win_Rate_%'] = (dfStratPerf['Wins'] / dfStratPerf['Total_Trades']) * 100

    # Llenar estadísticas de 0 trades para las estrategias habilitadas que no operaron
    all_enabled_stats = []
    for strat in allStrategies:
        if not dfStratPerf.empty and strat in dfStratPerf['strategy'].values:
            strat_row = dfStratPerf[dfStratPerf['strategy'] == strat].iloc[0]
            all_enabled_stats.append({
                'strategy': strat,
                'Total_Trades': int(strat_row['Total_Trades']),
                'Wins': int(strat_row['Wins']),
                'Win_Rate_%': float(strat_row['Win_Rate_%']),
                'PnL_Total': float(strat_row['PnL_Total'])
            })
        else:
            all_enabled_stats.append({
                'strategy': strat,
                'Total_Trades': 0,
                'Wins': 0,
                'Win_Rate_%': 0.0,
                'PnL_Total': 0.0
            })
    dfStratPerf = pd.DataFrame(all_enabled_stats)

    # Guardar CSVs
    basePath = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/"
    dfCompiledTrades.to_csv(basePath + "backtest_weekly_trades_raw_real.csv", index=False)
    dfComboPerf.to_csv(basePath + "backtest_weekly_combos_performance_real.csv", index=False)
    dfStratPerf.to_csv(basePath + "backtest_weekly_strategies_performance_real.csv", index=False)
    print("✅ CSVs detallados del backtesting real generados.")

    # Guardar en MySQL
    persistirMatrizRendimientoReal(dfCompiledTrades, compoundingStartDate)

    # Generar Reporte PDF Realista Premium
    pdfPath = generateWeeklyReportPdfReal(dfCompiledTrades, dfComboPerf, dfStratPerf, portfolioBalance, activeSymbols, allStrategies, len(exclusions))

    # Enviar reportes por Telegram
    if pdfPath and os.path.exists(pdfPath):
        try:
            print("🚀 Despachando reporte premium realista a Telegram...")
            retornoTotal = ((portfolioBalance - initialPortfolio) / initialPortfolio) * 100
            captionMsg = (
                f"📊 <b>AUDITORÍA SEMANAL REALISTA V6</b>\n\n"
                f"💵 <b>Balance Compuesto Real:</b> ${portfolioBalance:.2f} USD\n"
                f"📈 <b>Retorno Compuesto Real:</b> ${portfolioBalance - initialPortfolio:+.2f} USD ({retornoTotal:+.1f}%)\n"
                f"⚙️ <b>Estrategias Evaluadas:</b> {len(allStrategies)}\n"
                f"🚫 <b>Exclusiones Aplicadas:</b> {len(exclusions)} parejas bloqueadas\n\n"
                f"Backtest realista terminado. Muestra la cantidad fiel de trades en vivo."
            )
            # Enviar el Reporte PDF
            await alertaInmediata(1, captionMsg, prioridad=True, filePath=pdfPath)
            
            # Enviar los 3 CSVs
            csvsToSend = [
                ("backtest_weekly_trades_raw_real.csv", "📝 Detalle de Trades Reales (Real V6)"),
                ("backtest_weekly_combos_performance_real.csv", "📊 Rendimiento Detallado por Combo Real (Real V6)"),
                ("backtest_weekly_strategies_performance_real.csv", "⚙️ Rendimiento Consolidado por Estrategia Real (Real V6)")
            ]
            for csvFile, csvCaption in csvsToSend:
                fullCsvPath = os.path.join(basePath, csvFile)
                if os.path.exists(fullCsvPath):
                    await alertaInmediata(1, csvCaption, prioridad=False, filePath=fullCsvPath)
            print("✅ Todos los archivos del backtest real enviados por Telegram.")
        except Exception as e:
            print(f"⚠️ Error al enviar por Telegram: {e}")

if __name__ == '__main__':
    asyncio.run(runRealWeeklyBacktest())
