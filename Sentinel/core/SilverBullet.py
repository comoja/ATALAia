import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import talib as ta
import pytz

from middleware.database import dbManager
from Sentinel.analysis import technical
from middleware.utils.alertBuilder import adjustTPForMinRR, getPipMultiplier, calculateRR
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

SILVER_BULLET_WINDOWS = {
    "LONDON_OPEN": {"label": "London Open 🇬🇧", "emoji": "🌅", "start": time(3, 0), "end": time(4, 0)},
    "NY_OPENING": {"label": "NY Opening 🇺🇸", "emoji": "🔔", "start": time(8, 30), "end": time(9, 30)},
    "NY_AM": {"label": "New York AM 🇺🇸", "emoji": "🗽", "start": time(10, 0), "end": time(11, 0)},
    "NY_PM": {"label": "New York PM 🇺🇸", "emoji": "🌆", "start": time(14, 0), "end": time(15, 0)},
}

NY_TZ = pytz.timezone("America/New_York")
MX_TZ = pytz.timezone(TIMEZONE)

class SilverBulletBot:
    def __init__(self):
        self._signals_sent: Dict[str, bool] = {}
        strategyConfig = dbManager.getStrategyConfig("SilverBullet") or {}
        self.fvg_min_pct: float     = strategyConfig.get("fvg_min_pct",        0.0001)
        self.max_signal_age_min: int = strategyConfig.get("max_signal_age_min", 45)
        self.min_adx: float          = strategyConfig.get("min_adx",            15.0)
        self.ote_fib_min: float      = strategyConfig.get("ote_fib_min",        0.62)
        self.ote_fib_max: float      = strategyConfig.get("ote_fib_max",        0.79)
        self.use_ote_filter: bool    = strategyConfig.get("use_ote_filter",     True)
        self.min_rr: float           = strategyConfig.get("min_rr",             1.5)
        logger.info("SilverBulletBot iniciado — London / NY AM / NY PM")

    def _now_ny(self) -> datetime: return datetime.now(NY_TZ)
    def _now_mx(self) -> datetime: return datetime.now(MX_TZ)
    def _signal_key(self, symbol: str, window_name: str) -> str: return f"{symbol}_{window_name}_{self._now_ny().strftime('%Y-%m-%d')}"

    def _calc_adx(self, df: pd.DataFrame) -> float:
        try:
            if len(df) < 28: return 25.0
            adx_s = ta.ADX(df["high"], df["low"], df["close"], timeperiod=14)
            val = float(adx_s.dropna().iloc[-1])
            return val if not np.isnan(val) else 25.0
        except: return 25.0

    def _get_reference_range(self, df: pd.DataFrame, window_start_ny: datetime, ref_min: int = 15) -> Optional[Dict]:
        ref_end = window_start_ny + timedelta(minutes=ref_min)
        idx_ny = df.index.tz_localize(MX_TZ).tz_convert(NY_TZ) if df.index.tzinfo is None else df.index.tz_convert(NY_TZ)
        df_ref = df.loc[(idx_ny >= window_start_ny) & (idx_ny < ref_end)]
        if df_ref.empty: return None
        return {"high": float(df_ref["high"].max()), "low": float(df_ref["low"].min()), "open": float(df_ref["open"].iloc[0]), "n_candles": len(df_ref)}

    def _detect_sweep(self, df: pd.DataFrame, ref: Dict, window_start_ny: datetime) -> Optional[Dict]:
        sweep_start = window_start_ny + timedelta(minutes=15)
        idx_ny = df.index.tz_localize(MX_TZ).tz_convert(NY_TZ) if df.index.tzinfo is None else df.index.tz_convert(NY_TZ)
        df_post = df.loc[(idx_ny >= sweep_start) & (idx_ny < window_start_ny + timedelta(hours=1))]
        if df_post.empty: return None
        for i in range(len(df_post)):
            v = df_post.iloc[i]
            if v["low"] < ref["low"] and v["close"] > ref["low"]:
                return {"type": "LARGO", "swept_level": ref["low"], "sweep_low": v["low"], "candle_idx": df_post.index[i]}
            if v["high"] > ref["high"] and v["close"] < ref["high"]:
                return {"type": "CORTO", "swept_level": ref["high"], "sweep_high": v["high"], "candle_idx": df_post.index[i]}
        return None

    def _detect_mss(self, df: pd.DataFrame, sweep: Dict) -> bool:
        if len(df) < 7: return False
        h, l, c = df["high"].values, df["low"].values, df["close"].values
        if sweep["type"] == "LARGO": return float(c[-1]) > float(np.max(h[-6:-1]))
        else: return float(c[-1]) < float(np.min(l[-6:-1]))

    def _detect_fvg(self, df: pd.DataFrame, direction: str) -> Optional[Dict]:
        for i in range(len(df)-1, max(2, len(df)-15), -1):
            if i > len(df)-2: continue
            h2, l2, h, l, c = df["high"].iloc[i-2], df["low"].iloc[i-2], df["high"].iloc[i], df["low"].iloc[i], df["close"].iloc[i]
            if direction == "LARGO" and l > h2 and (l-h2)/c >= self.fvg_min_pct:
                return {"type": "LARGO_FVG", "mid": (h2+l)/2, "idx": i, "candle_time": df.index[i]}
            if direction == "CORTO" and h < l2 and (l2-h)/c >= self.fvg_min_pct:
                return {"type": "CORTO_FVG", "mid": (h+l2)/2, "idx": i, "candle_time": df.index[i]}
        return None

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> Optional[Signal]:
        symbol = symbolInfo["symbol"]
        window_name, window = "ALL_DAY", {"label": "All Day 🕐", "emoji": "🕐", "start": None, "end": None}
        sig_key = self._signal_key(symbol, window_name)
        if self._signals_sent.get(sig_key, False): return None
        
        master = preloadedData.get(symbol) if preloadedData else None
        
        # Punto 6: Abstracción de parámetros dinámicos
        from middleware.database import dbManager
        strat_config = dbManager.getStrategyConfig("SilverBullet") or {}
        min_rr_val = float(strat_config.get('min_rr', 1.5))
        min_confidence = float(strat_config.get('min_confidence', 70))
        
        # Punto 3: Master Dictionary integration
        if isinstance(master, dict):
            df = master.get('5min')
        else:
            df = master

        if df is None or len(df) < 50: return None

        
        adx = self._calc_adx(df)
        if adx < self.min_adx: return None
        
        w_start = self._now_ny().replace(hour=8, minute=30, second=0, microsecond=0) # Example start
        ref = self._get_reference_range(df, w_start)
        if not ref: return None
        
        sweep = self._detect_sweep(df, ref, w_start)
        if not sweep or not self._detect_mss(df, sweep): return None
        
        fvg = self._detect_fvg(df, sweep["type"])
        if not fvg: return None
        
        entry = fvg["mid"]
        atr = ta.ATR(df["high"], df["low"], df["close"], 14).iloc[-1]
        sl = (sweep.get("sweep_low", sweep["swept_level"]) - atr*0.15) if sweep["type"] == "LARGO" else (sweep.get("sweep_high", sweep["swept_level"]) + atr*0.15)
        
        levels = technical.get_structural_levels(df, lookback=50)
        tp_ref = levels["high_zone"] if sweep["type"] == "LARGO" else levels["low_zone"]
        tp = adjustTPForMinRR(entry, sl, tp_ref, "LARGO" if sweep["type"] == "LARGO" else "CORTO", minRR=min_rr_val)
        
        sl_dist = abs(entry - sl)
        multiplier = getPipMultiplier(symbol)
        
        if not technical.check_tp_exhaustion(df, fvg["idx"], entry, tp, sl, "LARGO" if sweep["type"] == "LARGO" else "CORTO", threshold=0.60, timeframe="5min")[0]:
            return None
        
        current_price = float(df['close'].iloc[-1])
        fvg_time = fvg.get("candle_time", "")
        fvg_time_str = fvg_time.strftime("%Y-%m-%d %H:%M:%S") if fvg_time else ""
        if not technical.check_signal_health(entry, tp, sl, "LARGO" if sweep["type"] == "LARGO" else "CORTO", current_price, threshold=0.65, candle_time=fvg_time_str)[0]:
            return None
        
        base_confidence = 80
        if base_confidence < min_confidence:
            logger.info(f"[{symbol}] Señal descartada: confidence={base_confidence} < min_confidence={min_confidence}")
            return None
        
        self._signals_sent[sig_key] = True
        return Signal(
            strategy="SilverBullet",
            symbol=symbol,
            direction="LARGO" if sweep["type"] == "LARGO" else "CORTO",
            entry_price=entry,
            stop_loss=sl,
            take_profit=tp,
            sl_distance=sl_dist,
            confidence=base_confidence,
            setup=f"Silver Bullet {window['emoji']} {window['label']}",
            status="EN ZONA ✅",
            candleTime=get_last_closed_candle(self._now_mx(), 5).strftime("%Y-%m-%d %H:%M:%S"),
            intervalo="5min",
            riesgo_pips=round(sl_dist * multiplier, 1),
            rr_ratio=round(abs(tp - entry) / sl_dist, 2),
            metadata={"window": window_name, "fvg": fvg["type"], "adx": adx}
        )
