import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import talib as ta
import pytz

from middleware.database import dbManager
from Sentinel.analysis import technical
from Sentinel.analysis.technical import resample_to_interval, check_tp_exhaustion, check_signal_health
from middleware.utils.alertBuilder import getPipMultiplier, adjustTPForMinRR, calculateRR, calculateBEPrice

from Sentinel.ml import model as mlModel
from middleware.config import constants as config
from middleware.utils import momentum
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

class EMA20200Bot:
    def __init__(self):
        self.waitingPullback = {}
        self.pullbackTolerance = 0.0015
        self.model_clf = mlModel.loadModel(config.MODEL_FILE_PATH)
        if self.model_clf is None:
            logger.warning("No se pudo cargar el modelo ML")
        logger.info("Bot EMA + ML Genuino iniciado")

    def ema(self, df, period): return ta.EMA(df['close'].values, timeperiod=period)
    def atr(self, df): return ta.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=14)
    def slope(self, series):
        y = series.dropna().tail(10).values
        if len(y) < 10: return 0
        x = np.arange(len(y))
        m, _ = np.polyfit(x, y, 1)
        return (m / np.mean(y)) * 100

    def detectCross(self, emaFast, emaSlow):
        diff = emaFast - emaSlow
        if len(diff) < 2: return None
        if (diff.iloc[-1] > 0) and (diff.iloc[-2] <= 0): return "LARGO"
        if (diff.iloc[-1] < 0) and (diff.iloc[-2] >= 0): return "CORTO"
        return None

    def evaluateML(self, df: pd.DataFrame) -> float:
        if self.model_clf is None: return 0.55
        try:
            row = df.iloc[-1]
            features = pd.DataFrame([{
                "close": row["close"], "atr": row["atr"], "atr_norm": row["atr"]/row["close"],
                "sma20": row["ema20"], "sma200": row["ema200"],
                "dist_sma20": (row["close"] - row["ema20"])/row["close"],
                "dist_sma200": (row["close"] - row["ema200"])/row["close"],
                "log_return": np.log(row["close"]/df["close"].iloc[-2]) if len(df)>1 else 0,
                "range": (row["high"]-row["low"])/row["close"],
                "sma_slope": self.slope(df["ema20"])
            }])
            return self.model_clf.predict_proba(features)[0][1]
        except: return 0.55

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> Optional[Signal]:
        symbol = symbolInfo['symbol']
        logger.info(f"[{symbol}] Analizando...")
        master = preloadedData.get(symbol) if preloadedData else None
        
        # Punto 3: Master Dictionary integration
        if isinstance(master, dict):
            df = master.get('1h')
        else:
            df = master

        if df is None or len(df) < 200: return None
        
        # Ya no recalculamos EMA/ATR si ya vienen en el DF (Punto 3)
        if "ema20" not in df.columns or "ema200" not in df.columns:
            df = df.copy()
            df["ema20"] = self.ema(df, 20)
            df["ema200"] = self.ema(df, 200)
            df["atr"] = self.atr(df)

        
        cross = self.detectCross(df["ema20"], df["ema200"])
        if cross:
            self.waitingPullback[symbol] = {"direction": cross}
            return None
            
        if symbol not in self.waitingPullback: return None
        direction = self.waitingPullback[symbol]["direction"]
        price, ema20_last = df['close'].iloc[-1], df['ema20'].iloc[-1]
        
        if abs(price - ema20_last) / ema20_last >= self.pullbackTolerance: return None
        

        strat_config = dbManager.getStrategyConfig("EMA20200") or {}
        min_conf_val = float(strat_config.get('min_confidence', 70)) / 100.0
        
        prob = self.evaluateML(df)
        if prob < min_conf_val: return None
        
        levels = technical.get_structural_levels(df, lookback=40)
        sl_price = (levels['swing_low'] - df['atr'].iloc[-1]*0.2) if direction=="LARGO" else (levels['swing_high'] + df['atr'].iloc[-1]*0.2)
        sl_dist = abs(price - sl_price)
        min_rr_val = float(strat_config.get('min_rr', 1.5))
        tp_price = adjustTPForMinRR(price, sl_price, (levels['high_zone'] if direction=="LARGO" else levels['low_zone']), direction, minRR=min_rr_val)
        
        if not check_tp_exhaustion(df, len(df)-5, price, tp_price, sl_price, direction, threshold=0.60, timeframe="5min")[0]: return None
        
        current_price = float(df['close'].iloc[-1])
        last_v = get_last_closed_candle(datetime.now(ZoneInfo(TIMEZONE)), 60, df=df)
        candle_time = (last_v.name if hasattr(last_v, 'name') else last_v).strftime("%Y-%m-%d %H:%M:%S")
        if not check_signal_health(price, tp_price, sl_price, direction, current_price, threshold=0.65, candle_time=candle_time)[0]: return None
        
        self.waitingPullback.pop(symbol, None)
        multiplier = getPipMultiplier(symbol)
        mom_state = symbolInfo.get('momentum', '☁️ SIN DATOS')
        mom_bonus, _ = momentum.getMomentumBonus(mom_state, direction)
        
        # Calcular Break Even inteligente
        be_trigger = calculateBEPrice(price, sl_price, tp_price, direction)

        return Signal(
            strategy="EMA20200",
            symbol=symbol,
            direction=direction,
            entry_price=price,
            stop_loss=sl_price,
            take_profit=tp_price,
            sl_distance=sl_dist,
            confidence=int(prob*100) + mom_bonus,
            setup="EMA Pullback",
            status="EN ZONA ✅",
            candleTime=candle_time,

            intervalo="5min",
            riesgo_pips=round(sl_dist * multiplier, 1),
            rr_ratio=round(abs(tp_price - price)/sl_dist, 2),
            break_even=be_trigger,
            metadata={"prob": prob, "momentum": mom_state}
        )