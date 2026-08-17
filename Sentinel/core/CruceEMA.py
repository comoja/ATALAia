import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import talib as ta

from middleware.database import dbManager
from Sentinel.analysis import technical
from Sentinel.analysis.technical import check_tp_exhaustion, check_signal_health
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR, calculateBEPrice

from Sentinel.ml import model as mlModel
from middleware.config import constants as config
from middleware.utils import momentum
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class CruceEMABot:
    def __init__(self):
        self.strategy_name = "CruceEMA" 
        self.model_clf = mlModel.loadModel(config.MODEL_FILE_PATH)
        if self.model_clf is None:
            logger.warning("No se pudo cargar el modelo ML")
        logger.info("Bot EMA Pullback + Price Action + IMACD (15m) iniciado")

    def atr(self, df): return ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
    def slope(self, series):
        y = series.dropna().tail(10).values
        if len(y) < 10: return 0
        x = np.arange(len(y))
        m, _ = np.polyfit(x, y, 1)
        return (m / np.mean(y)) * 100

    def calc_imacd(self, df, lengthMA=34, lengthSignal=9):
        highs, lows, closes = df['high'].values, df['low'].values, df['close'].values
        hlc3 = (highs + lows + closes) / 3.0
        
        # SMMA es equivalente a EMA con alpha = 1/length
        hi = pd.Series(highs).ewm(alpha=1.0/lengthMA, adjust=False).mean().values
        lo = pd.Series(lows).ewm(alpha=1.0/lengthMA, adjust=False).mean().values
        
        ema1 = ta.EMA(hlc3, timeperiod=lengthMA)
        ema2 = ta.EMA(ema1, timeperiod=lengthMA)
        mi = ema1 + (ema1 - ema2) # ZLEMA
        
        md = np.where(mi > hi, mi - hi, np.where(mi < lo, mi - lo, 0.0))
        sb = ta.SMA(md, timeperiod=lengthSignal)
        
        df['imacd_md'] = md
        df['imacd_sb'] = sb
        return df

    def evaluateML(self, df: pd.DataFrame, emaFast_series: pd.Series, emaSlow_series: pd.Series) -> float:
        if self.model_clf is None: return 0.55
        try:
            row = df.iloc[-1]
            features = pd.DataFrame([{
                "close": row["close"], "atr": row["atr"], "atr_norm": row["atr"]/row["close"],
                "sma20": emaFast_series.iloc[-1], "sma200": emaSlow_series.iloc[-1],
                "dist_sma20": (row["close"] - emaFast_series.iloc[-1])/row["close"],
                "dist_sma200": (row["close"] - emaSlow_series.iloc[-1])/row["close"],
                "log_return": np.log(row["close"]/df["close"].iloc[-2]) if len(df)>1 else 0,
                "range": (row["high"]-row["low"])/row["close"],
                "sma_slope": self.slope(emaFast_series)
            }])
            return self.model_clf.predict_proba(features)[0][1]
        except: return 0.55

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> Optional[Signal]:
        symbol = symbolInfo['symbol']
        logger.info(f"[{symbol}] Analizando con EMA Pullback + IMACD (15m)...")
        master = preloadedData.get(symbol) if preloadedData else None
        
        # Operamos estrictamente en 15m para mayor precisión en Price Action + MACD
        if isinstance(master, dict):
            df = master.get('15min')
        else:
            df = master

        if df is None or len(df) < 55: return None
        
        globalConfig = dbManager.getStrategyConfig(self.strategy_name) or {}
        stratConfig = dbManager.getSymbolStrategyConfig(self.strategy_name, symbol) or {}
        
        emaFastPeriod = int(stratConfig.get("emaFast", globalConfig.get("emaFast", 15))) 
        emaSlowPeriod = int(stratConfig.get("emaSlow", globalConfig.get("emaSlow", 20))) 
        minRrVal = float(stratConfig.get("minRr", globalConfig.get("min_rr", 1.5)))
        minConfidence = float(stratConfig.get("minConfidence", globalConfig.get("min_confidence", 70))) / 100.0
        macdSlow = int(stratConfig.get("macdSlow", globalConfig.get("macdSlow", 34)))
        macdSignal = int(stratConfig.get("macdSignal", globalConfig.get("macdSignal", 9)))

        df = df.copy()
        df["emaFast"] = ta.EMA(df['close'].values, timeperiod=emaFastPeriod)
        df["emaSlow"] = ta.EMA(df['close'].values, timeperiod=emaSlowPeriod)
        df["atr"] = self.atr(df)
        
        # Calcular IMACD
        df = self.calc_imacd(df, lengthMA=macdSlow, lengthSignal=macdSignal)

        if pd.isna(df["emaSlow"].iloc[-1]) or pd.isna(df["imacd_sb"].iloc[-1]): return None

        emaF = df["emaFast"].iloc[-1]
        emaS = df["emaSlow"].iloc[-1]
        
        v_curr = df.iloc[-1]
        v_prev = df.iloc[-2]
        
        direction = None
        body_curr = abs(v_curr['close'] - v_curr['open'])
        body_prev = abs(v_prev['close'] - v_prev['open'])
        
        # Filtro IMACD
        imacd_md = v_curr['imacd_md']
        imacd_sb = v_curr['imacd_sb']
        
        # Histograma IMACD para ver si hay agotamiento severo
        imacd_hist_curr = imacd_md - imacd_sb
        imacd_hist_prev = v_prev['imacd_md'] - v_prev['imacd_sb']
        
        # --- Estrategia Pullback + Price Action + IMACD ---
        if emaF > emaS:
            in_zone = v_curr['low'] <= (emaF + (v_curr['atr']*0.1))
            
            lower_wick = min(v_curr['open'], v_curr['close']) - v_curr['low']
            is_pinbar = (lower_wick > (body_curr * 1.5)) and (v_curr['close'] > v_curr['open']) and body_curr > 0
            is_engulfing = (v_prev['close'] < v_prev['open']) and (v_curr['close'] > v_curr['open']) and (v_curr['close'] > v_prev['open']) and (v_curr['open'] < v_prev['close'])
            
            # Filtro de agotamiento alcista
            ema_pointing_up = v_curr['emaFast'] >= v_prev['emaFast']
            not_crashing = imacd_hist_curr >= (imacd_hist_prev * 0.5) # Permite leve caída por el pullback, pero no un colapso del 50%+
            
            # IMACD debe indicar momento alcista
            imacd_bullish = (imacd_md > imacd_sb) and (imacd_md > 0)
            
            if in_zone and (is_pinbar or is_engulfing) and imacd_bullish and ema_pointing_up and not_crashing:
                direction = "LARGO"
                
        elif emaF < emaS:
            in_zone = v_curr['high'] >= (emaF - (v_curr['atr']*0.1))
            
            upper_wick = v_curr['high'] - max(v_curr['open'], v_curr['close'])
            is_pinbar = (upper_wick > (body_curr * 1.5)) and (v_curr['close'] < v_curr['open']) and body_curr > 0
            is_engulfing = (v_prev['close'] > v_prev['open']) and (v_curr['close'] < v_curr['open']) and (v_curr['close'] < v_prev['open']) and (v_curr['open'] > v_prev['close'])
            
            # Filtro de agotamiento bajista
            ema_pointing_down = v_curr['emaFast'] <= v_prev['emaFast']
            # Histograma bajista es negativo. Queremos que no se esté acercando agresivamente a cero.
            not_crashing = imacd_hist_curr <= (imacd_hist_prev * 0.5) 
            
            # IMACD debe indicar momento bajista
            imacd_bearish = (imacd_md < imacd_sb) and (imacd_md < 0)
            
            if in_zone and (is_pinbar or is_engulfing) and imacd_bearish and ema_pointing_down and not_crashing:
                direction = "CORTO"

        if not direction:
            return None
        
        prob = self.evaluateML(df, df["emaFast"], df["emaSlow"])
        if False: # ML Deshabilitado temporalmente: prob < minConfidence
            return None
        
        levels = technical.get_structural_levels(df, lookback=20)
        sl_price = (levels['swing_low'] - df['atr'].iloc[-1]*0.2) if direction=="LARGO" else (levels['swing_high'] + df['atr'].iloc[-1]*0.2)
        sl_dist = abs(v_curr['close'] - sl_price)
        tp_price = adjustTPForMinRR(v_curr['close'], sl_price, (levels['high_zone'] if direction=="LARGO" else levels['low_zone']), direction, minRR=minRrVal)
        
        if not check_tp_exhaustion(df, len(df)-5, v_curr['close'], tp_price, sl_price, direction, threshold=0.60, timeframe="15min")[0]: return None
        
        last_v = get_last_closed_candle(datetime.now(ZoneInfo(TIMEZONE)), 15, df=df)
        candle_time = (last_v.name if hasattr(last_v, 'name') else last_v).strftime("%Y-%m-%d %H:%M:%S")
        if not check_signal_health(v_curr['close'], tp_price, sl_price, direction, v_curr['close'], threshold=0.65, candle_time=candle_time)[0]: return None
        
        multiplier = getPipMultiplier(symbol)
        mom_state = symbolInfo.get('momentum', '☁️ SIN DATOS')
        mom_bonus, _ = momentum.getMomentumBonus(mom_state, direction)
        
        from Sentinel.analysis import risk as riskAnalysis
        refCapital = symbolInfo.get('refCapital', 10000.0)
        refRiskPct = symbolInfo.get('refRiskPct', 1.0)
        
        size, riskUsdActual, marginUsed = riskAnalysis.calculatePositionSize(
            refCapital, refRiskPct, sl_dist, symbolInfo, entryPrice=v_curr['close']
        )
        
        if size is None or size <= 0:
            return None
            
        rrVal = round(abs(tp_price - v_curr['close']) / sl_dist, 2) if sl_dist > 0 else 0
        expectedProfit = riskUsdActual * rrVal
        
        minUsdProfit = float(stratConfig.get('minUsdProfit', globalConfig.get('min_usd_profit', 10.0)))
        if minUsdProfit < 6.0: minUsdProfit = 6.0
            
        if expectedProfit < minUsdProfit:
            return None
            
        be_trigger = calculateBEPrice(v_curr['close'], sl_price, tp_price, direction)

        return Signal(
            strategy="CruceEMA",
            symbol=symbol,
            direction=direction,
            entry_price=v_curr['close'],
            stop_loss=sl_price,
            take_profit=tp_price,
            sl_distance=sl_dist,
            confidence=int(prob*100) + mom_bonus,
            setup="EMA Pullback + IMACD",
            status="EN ZONA ✅",
            candleTime=candle_time,
            intervalo="15m",
            riesgo_pips=round(sl_dist * multiplier, 1),
            rr_ratio=rrVal,
            break_even=be_trigger,
            size=size,
            metadata={
                "prob": prob, 
                "momentum": mom_state,
                "risk_usd": round(riskUsdActual, 2),
                "expected_profit": round(expectedProfit, 2),
                "margin_used": round(marginUsed, 2),
                "emaFast": round(emaF, 5),
                "emaSlow": round(emaS, 5),
                "emaFastPeriod": emaFastPeriod,
                "emaSlowPeriod": emaSlowPeriod,
                "imacd_md": round(imacd_md, 6),
                "imacd_sb": round(imacd_sb, 6),
                "imacd_slow": macdSlow,
                "imacd_signal": macdSignal
            }
        )
