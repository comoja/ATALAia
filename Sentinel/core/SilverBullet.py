"""
==============================================================================
  ESTRATEGIA DE TRADING: SILVER BULLET — ICT / Inner Circle Trader
==============================================================================
  Implementa el modelo Silver Bullet de ICT basado en 3 ventanas horarias
  de alta probabilidad definidas por el algoritmo del mercado:

    • London Open    : 03:00 AM – 04:00 AM EST
    • NY Opening     : 08:30 AM – 09:30 AM EST
    • NY AM Session  : 10:00 AM – 11:00 AM EST
    • NY PM Session  : 02:00 PM – 03:00 PM EST

  Lógica del setup (en ese orden dentro de la ventana):
    1. ACUMULACIÓN: Precio establece un High y Low de referencia en los
       primeros 15 minutos de la ventana.
    2. MANIPULACIÓN: Precio barre (Liquidity Sweep) uno de esos niveles.
    3. MARKET STRUCTURE SHIFT (MSS): Tras el sweep, precio rompe con
       cierre de cuerpo la estructura contraria al sweep.
    4. FAIR VALUE GAP: El MSS debe crear un FVG. Se entra en el retroceso
       al midpoint del FVG.
    5. SL: Por debajo/encima del punto extremo del sweep.
    6. TP: Siguiente Draw on Liquidity (EQH, EQL, PDH, PDL).

  Filtros adicionales:
    • OTE (Optimal Trade Entry): Solo se confirma entrada si el precio
      retrocede al rango 62–79% del impulso creado por el sweep.
    • Antigüedad máxima del FVG: 45 minutos desde su formación.
    • El setup completo debe ocurrir DENTRO de la ventana de 60 min.
    • ADX mínimo de 15 para evitar mercados completamente laterales.
==============================================================================
"""

import logging
from datetime import datetime, time, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, List, Optional, Tuple
import pandas as pd
import numpy as np
import talib as ta
import pytz

import os
import sys
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.config import constants
from middleware.database import dbManager
from Sentinel.analysis import risk, technical
from middleware.utils.communications import sendTelegramAlert
from middleware.utils.alertBuilder import adjustTPForMinRR, getPipMultiplier, calculateRR
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle

logger = logging.getLogger("sentinel")


# ─────────────────────────────────────────────────────────────────────────────
#  Definición de las ventanas Silver Bullet (en hora EST / New York)
# ─────────────────────────────────────────────────────────────────────────────
SILVER_BULLET_WINDOWS = {
    "LONDON_OPEN": {
        "label": "London Open 🇬🇧",
        "emoji": "🌅",
        "start": time(3, 0),   # 3:00 AM NY
        "end":   time(4, 0),   # 4:00 AM NY
    },
    "NY_OPENING": {
        "label": "NY Opening 🇺🇸",
        "emoji": "🔔",
        "start": time(8, 30),  # 8:30 AM NY
        "end":   time(9, 30),  # 9:30 AM NY
    },
    "NY_AM": {
        "label": "New York AM 🇺🇸",
        "emoji": "🗽",
        "start": time(10, 0),  # 10:00 AM NY
        "end":   time(11, 0),  # 11:00 AM NY
    },
    "NY_PM": {
        "label": "New York PM 🇺🇸",
        "emoji": "🌆",
        "start": time(14, 0),  # 2:00 PM NY
        "end":   time(15, 0),  # 3:00 PM NY
    },
}

NY_TZ = pytz.timezone("America/New_York")
MX_TZ = pytz.timezone(TIMEZONE)


class SilverBulletBot:
    """
    Bot que implementa el modelo Silver Bullet de ICT.
    Activo únicamente en las 3 ventanas horarias de 60 minutos definidas por ICT.
    """

    def __init__(self):
        self.accounts: List[Dict] = []
        self.lastMessageIds: Dict[str, int] = {}

        # Control de señales por símbolo y ventana para evitar duplicados
        # Key: f"{symbol}_{window_name}_{date}"
        self._signals_sent: Dict[str, bool] = {}

        # Configuración desde DB (con fallbacks)
        strategyConfig = dbManager.getStrategyConfig("SilverBullet") or {}
        self.fvg_min_pct: float     = strategyConfig.get("fvg_min_pct",        0.0001)
        self.max_signal_age_min: int = strategyConfig.get("max_signal_age_min", 45)
        self.min_adx: float          = strategyConfig.get("min_adx",            15.0)
        self.ote_fib_min: float      = strategyConfig.get("ote_fib_min",        0.62)
        self.ote_fib_max: float      = strategyConfig.get("ote_fib_max",        0.79)
        self.use_ote_filter: bool    = strategyConfig.get("use_ote_filter",     True)
        self.min_rr: float           = strategyConfig.get("min_rr",             1.5)
        self.reference_candles: int  = strategyConfig.get("reference_candles",  3)

        logger.info("SilverBulletBot iniciado — London / NY AM / NY PM")

    # ─────────────────────────────────────────────────────────────────────────
    #  Helpers temporales
    # ─────────────────────────────────────────────────────────────────────────

    def _now_ny(self) -> datetime:
        return datetime.now(NY_TZ)

    def _now_mx(self) -> datetime:
        return datetime.now(MX_TZ)

    def _get_active_window(self) -> Optional[Tuple[str, Dict]]:
        """
        Retorna el nombre y definición de la ventana Silver Bullet activa,
        o None si no estamos en ninguna.
        """
        now_ny = self._now_ny()
        current_time = now_ny.time()
        for name, window in SILVER_BULLET_WINDOWS.items():
            if window["start"] <= current_time < window["end"]:
                return name, window
        return None

    def _window_start_ny(self, window: Dict) -> datetime:
        """Retorna el datetime de inicio de la ventana para hoy en NY."""
        now_ny = self._now_ny()
        if window.get("start") is None:
            return now_ny
        return now_ny.replace(
            hour=window["start"].hour,
            minute=window["start"].minute,
            second=0, microsecond=0
        )

    def _signal_key(self, symbol: str, window_name: str) -> str:
        date_str = self._now_ny().strftime("%Y-%m-%d")
        return f"{symbol}_{window_name}_{date_str}"


    # ─────────────────────────────────────────────────────────────────────────
    #  ADX check
    # ─────────────────────────────────────────────────────────────────────────

    def _calc_adx(self, df: pd.DataFrame) -> float:
        try:
            if len(df) < 28:
                return 25.0
            adx_series = ta.ADX(df["high"], df["low"], df["close"], timeperiod=14)
            val = float(adx_series.dropna().iloc[-1])
            return val if not np.isnan(val) else 25.0
        except Exception:
            return 25.0

    # ─────────────────────────────────────────────────────────────────────────
    #  Paso 1: Extraer velas del periodo de referencia (primeros N minutos)
    # ─────────────────────────────────────────────────────────────────────────

    def _get_reference_range(
        self,
        df: pd.DataFrame,
        window_start_ny: datetime,
        reference_minutes: int = 15
    ) -> Optional[Dict]:
        """
        Calcula el High y Low de los primeros `reference_minutes` de la ventana.
        Estos son los niveles que el algoritmo del mercado intentará barrer.
        """
        ref_end = window_start_ny + timedelta(minutes=reference_minutes)

        # Convertir index del df a NY si es necesario
        idx = df.index
        if idx.tzinfo is None:
            idx = idx.tz_localize(MX_TZ)
        idx_ny = idx.tz_convert(NY_TZ)

        mask = (idx_ny >= window_start_ny) & (idx_ny < ref_end)
        df_ref = df.loc[mask]

        if len(df_ref) == 0:
            logger.debug("[SilverBullet] Sin velas en el periodo de referencia")
            return None

        return {
            "high": float(df_ref["high"].max()),
            "low":  float(df_ref["low"].min()),
            "open": float(df_ref["open"].iloc[0]),
            "n_candles": len(df_ref),
        }

    # ─────────────────────────────────────────────────────────────────────────
    #  Paso 2: Detectar Liquidity Sweep del rango de referencia
    # ─────────────────────────────────────────────────────────────────────────

    def _detect_sweep(
        self,
        df: pd.DataFrame,
        ref_range: Dict,
        window_start_ny: datetime,
        ref_minutes: int = 15
    ) -> Optional[Dict]:
        """
        Busca una vela post-referencia que haya barrido el High o Low
        del rango de referencia pero cerrado de vuelta adentro.
        Esto confirma la fase de Manipulación del PO3.
        """
        sweep_start = window_start_ny + timedelta(minutes=ref_minutes)

        idx = df.index
        if idx.tzinfo is None:
            idx = idx.tz_localize(MX_TZ)
        idx_ny = idx.tz_convert(NY_TZ)

        # Solo miramos dentro de la ventana activa (60 min)
        window_end_ny = window_start_ny + timedelta(hours=1)
        mask = (idx_ny >= sweep_start) & (idx_ny < window_end_ny)
        df_post = df.loc[mask]

        if df_post.empty:
            return None

        ref_high = ref_range["high"]
        ref_low  = ref_range["low"]

        for i in range(len(df_post)):
            vela = df_post.iloc[i]
            h = float(vela["high"])
            l = float(vela["low"])
            c = float(vela["close"])
            o = float(vela["open"])

            # Sweep alcista (barre Low y regresa arriba): señal de compra
            if l < ref_low and c > ref_low:
                logger.info(f"[SilverBullet] ✅ Sweep BULLISH: low={l:.4f} < ref_low={ref_low:.4f}, close={c:.4f}")
                return {
                    "type": "BULLISH",       # dirección del trade
                    "swept_level": ref_low,
                    "sweep_low": l,
                    "candle_idx": df_post.index[i],
                    "candle_open": o,
                    "candle_high": h,
                    "candle_low": l,
                    "candle_close": c,
                }

            # Sweep bajista (barre High y regresa abajo): señal de venta
            if h > ref_high and c < ref_high:
                logger.info(f"[SilverBullet] ✅ Sweep BEARISH: high={h:.4f} > ref_high={ref_high:.4f}, close={c:.4f}")
                return {
                    "type": "BEARISH",
                    "swept_level": ref_high,
                    "sweep_high": h,
                    "candle_idx": df_post.index[i],
                    "candle_open": o,
                    "candle_high": h,
                    "candle_low": l,
                    "candle_close": c,
                }

        return None

    # ─────────────────────────────────────────────────────────────────────────
    #  Paso 3: Market Structure Shift (MSS) post-sweep
    # ─────────────────────────────────────────────────────────────────────────

    def _detect_mss(
        self,
        df: pd.DataFrame,
        sweep: Dict,
        lookback: int = 5
    ) -> bool:
        """
        Detecta un Market Structure Shift tras el sweep.
        BULLISH MSS: Cierre de cuerpo por encima del swing high de las últimas `lookback` velas.
        BEARISH MSS: Cierre de cuerpo por debajo del swing low de las últimas `lookback` velas.
        IMPORTANTE: Se confirma por CIERRE DE CUERPO, nunca por mecha (ICT canónico).
        """
        if len(df) < lookback + 2:
            return False

        closes = df["close"].values
        highs  = df["high"].values
        lows   = df["low"].values

        direction = sweep["type"]

        if direction == "BULLISH":
            # Necesitamos un cierre por encima del swing high previo
            prev_swing_high = float(np.max(highs[-lookback - 1:-1]))
            return float(closes[-1]) > prev_swing_high

        else:  # BEARISH
            prev_swing_low = float(np.min(lows[-lookback - 1:-1]))
            return float(closes[-1]) < prev_swing_low

    # ─────────────────────────────────────────────────────────────────────────
    #  Paso 4: Fair Value Gap resultante del MSS
    # ─────────────────────────────────────────────────────────────────────────

    def _detect_fvg(
        self,
        df: pd.DataFrame,
        direction: str,
        lookback: int = 15
    ) -> Optional[Dict]:
        """
        Detecta el FVG más reciente alineado con la dirección del trade.
        Busca en las últimas `lookback` velas para encontrar la brecha.
        SOLO usa velas terminadas (idx <= len(df) - 2).
        """
        start = max(2, len(df) - lookback)
        last_closed_idx = len(df) - 2

        for i in range(len(df) - 1, start, -1):
            if i < 2:
                continue
            
            if i > last_closed_idx:
                continue

            h_prev2 = float(df["high"].iloc[i - 2])
            l_prev2 = float(df["low"].iloc[i - 2])
            h_curr  = float(df["high"].iloc[i])
            l_curr  = float(df["low"].iloc[i])
            c_curr  = float(df["close"].iloc[i])

            if direction == "BULLISH":
                # Bullish FVG: low de vela actual > high de vela i-2
                if l_curr > h_prev2:
                    gap = l_curr - h_prev2
                    if gap / c_curr >= self.fvg_min_pct:
                        return {
                            "type":    "Bullish_FVG",
                            "top":     l_curr,
                            "bottom":  h_prev2,
                            "mid":     (h_prev2 + l_curr) / 2,
                            "size":    gap,
                            "idx":     i,
                            "candle_time": df.index[i],
                        }
            else:  # BEARISH
                # Bearish FVG: high de vela actual < low de vela i-2
                if h_curr < l_prev2:
                    gap = l_prev2 - h_curr
                    if gap / c_curr >= self.fvg_min_pct:
                        return {
                            "type":    "Bearish_FVG",
                            "top":     l_prev2,
                            "bottom":  h_curr,
                            "mid":     (h_curr + l_prev2) / 2,
                            "size":    gap,
                            "idx":     i,
                            "candle_time": df.index[i],
                        }

        return None

    # ─────────────────────────────────────────────────────────────────────────
    #  Paso 5: OTE — Optimal Trade Entry (Fibonacci 62-79%)
    # ─────────────────────────────────────────────────────────────────────────

    def _is_in_ote_zone(
        self,
        current_price: float,
        impulse_start: float,
        impulse_end: float,
        direction: str
    ) -> bool:
        """
        Verifica si el precio actual está en la zona OTE (62-79% de retroceso).
        El impulso va desde `impulse_start` (inicio del movimiento) hasta
        `impulse_end` (fin del impulso / sweep extremo).
        OTE es el retroceso desde el extremo hacia el origen.
        """
        rango = abs(impulse_end - impulse_start)
        if rango == 0:
            return True  # Sin rango, no filtrar

        if direction == "BULLISH":
            # El impulso fue bajista (sweep de lows). Extremo = sweep_low.
            # OTE es la zona donde el precio sube de vuelta al 62-79% del rango.
            ote_low  = impulse_end + rango * self.ote_fib_min   # 62% desde el sweep
            ote_high = impulse_end + rango * self.ote_fib_max   # 79% desde el sweep
            in_zone = ote_low <= current_price <= ote_high
        else:  # BEARISH
            # El impulso fue alcista (sweep de highs). Extremo = sweep_high.
            # OTE es donde el precio baja de vuelta al 62-79% del rango.
            ote_high = impulse_end - rango * self.ote_fib_min
            ote_low  = impulse_end - rango * self.ote_fib_max
            in_zone = ote_low <= current_price <= ote_high

        logger.info(
            f"[SilverBullet] OTE check: price={current_price:.4f} "
            f"zona=[{ote_low:.4f}, {ote_high:.4f}] -> {'✅ DENTRO' if in_zone else '❌ FUERA'}"
        )
        return in_zone

    # ─────────────────────────────────────────────────────────────────────────
    #  Paso 6: Calcular SL (extremo del sweep) y TP (Draw on Liquidity)
    # ─────────────────────────────────────────────────────────────────────────

    def _calculate_sl(self, sweep: Dict, atr: float) -> float:
        """SL se coloca por debajo/encima del extremo del sweep con un pequeño padding."""
        padding = atr * 0.15
        if sweep["type"] == "BULLISH":
            return float(sweep.get("sweep_low", sweep["swept_level"])) - padding
        else:
            return float(sweep.get("sweep_high", sweep["swept_level"])) + padding

    def _calculate_tp(
        self,
        df: pd.DataFrame,
        direction: str,
        entry: float,
        sl: float
    ) -> tuple:
        """
        TP apunta al próximo Draw on Liquidity:
        Priority 1: PDH/PDL (Previous Day High/Low).
        Priority 2: percentile 90/10 de highs/lows (macro) como zona de liquidez.
        Garantiza mínimo RR de self.min_rr.
        """
        levels = technical.get_structural_levels(df, lookback=50, lookback_macro=150)
        daily_liquidity = technical.get_prev_day_high_low(df)
        
        target_type = "ESTRUCTURAL"
        if direction == "BULLISH":
            tp_ref = levels["high_zone"]
            # Si PDH es mayor que la entrada y está cerca del rango estructural, lo usamos como imán
            if daily_liquidity['pdh'] and daily_liquidity['pdh'] > entry:
                tp_ref = daily_liquidity['pdh']
                target_type = "PREV_DAY_HIGH (PDH)"
        else:
            tp_ref = levels["low_zone"]
            # Si PDL es menor que la entrada y está cerca del rango estructural, lo usamos como imán
            if daily_liquidity['pdl'] and daily_liquidity['pdl'] < entry:
                tp_ref = daily_liquidity['pdl']
                target_type = "PREV_DAY_LOW (PDL)"

        tp = adjustTPForMinRR(entry, sl, tp_ref, 
                              "LARGO" if direction == "BULLISH" else "CORTO",
                              minRR=self.min_rr)
        return tp, target_type

    # ─────────────────────────────────────────────────────────────────────────
    #  Lógica principal de detección de señal
    # ─────────────────────────────────────────────────────────────────────────

    async def _analyze_window(
        self,
        df: pd.DataFrame,
        symbol: str,
        window_name: str,
        window: Dict
    ) -> Optional[Dict]:
        """
        Ejecuta el pipeline completo del Silver Bullet para la ventana activa.
        Retorna un dict de señal o None si no hay setup válido.
        """
        # ── ADX mínimo ──────────────────────────────────────────────────────
        adx = self._calc_adx(df)
        if adx < self.min_adx:
            logger.info(f"[SilverBullet][{symbol}] ADX={adx:.1f} < min={self.min_adx} → mercado lateral, sin trade")
            return None

        window_start_ny = self._window_start_ny(window)

        # ── Paso 1: Rango de referencia (primeros 15 min) ───────────────────
        ref_range = self._get_reference_range(df, window_start_ny, reference_minutes=15)
        if not ref_range:
            logger.info(f"[SilverBullet][{symbol}][{window_name}] Sin rango de referencia aún")
            return None

        logger.info(
            f"[SilverBullet][{symbol}][{window_name}] "
            f"Rango ref: H={ref_range['high']:.4f} L={ref_range['low']:.4f} "
            f"({ref_range['n_candles']} velas)"
        )

        # ── Paso 2: Detectar Liquidity Sweep ────────────────────────────────
        sweep = self._detect_sweep(df, ref_range, window_start_ny)
        if not sweep:
            logger.info(f"[SilverBullet][{symbol}][{window_name}] Sin sweep detectado")
            return None

        # ── Paso 3: MSS post-sweep ───────────────────────────────────────────
        mss_ok = self._detect_mss(df, sweep)
        if not mss_ok:
            logger.info(f"[SilverBullet][{symbol}][{window_name}] Sin MSS confirmado")
            return None
        logger.info(f"[SilverBullet][{symbol}][{window_name}] ✅ MSS confirma dirección {sweep['type']}")

        # ── Paso 4: FVG del impulso post-MSS ────────────────────────────────
        fvg = self._detect_fvg(df, sweep["type"])
        if not fvg:
            logger.info(f"[SilverBullet][{symbol}][{window_name}] Sin FVG tras MSS")
            return None

        # Verificar antigüedad del FVG
        now_mx = self._now_mx().replace(tzinfo=None)
        fvg_time = fvg["candle_time"]
        if hasattr(fvg_time, "to_pydatetime"):
            fvg_time = fvg_time.to_pydatetime()
        if fvg_time.tzinfo is not None:
            fvg_time = fvg_time.replace(tzinfo=None)

        age_min = (now_mx - fvg_time).total_seconds() / 60
        if age_min > self.max_signal_age_min:
            logger.info(
                f"[SilverBullet][{symbol}][{window_name}] FVG expirado "
                f"({age_min:.1f} > {self.max_signal_age_min} min)"
            )
            return None

        logger.info(
            f"[SilverBullet][{symbol}][{window_name}] ✅ FVG {fvg['type']} "
            f"| mid={fvg['mid']:.4f} | edad={age_min:.1f}min"
        )

        # ── Paso 5: OTE filter ───────────────────────────────────────────────
        current_price = float(df["close"].iloc[-1])
        entry_price   = fvg["mid"]

        if self.use_ote_filter:
            # El impulso va desde el swept_level (inicio) hasta el extremo del sweep
            if sweep["type"] == "BULLISH":
                impulse_start = ref_range["high"]        # lo que se barrió fue el Low
                impulse_end   = sweep.get("sweep_low", sweep["swept_level"])
            else:
                impulse_start = ref_range["low"]
                impulse_end   = sweep.get("sweep_high", sweep["swept_level"])

            if not self._is_in_ote_zone(current_price, impulse_start, impulse_end, sweep["type"]):
                logger.info(
                    f"[SilverBullet][{symbol}][{window_name}] Precio fuera de OTE "
                    f"(price={current_price:.4f}). Setup válido pero entrada no óptima."
                )
                # No cancelamos el setup — la orden se pondrá en el mid del FVG
                # si el precio llega. Continuamos con una confianza reducida.
                ote_ok = False
            else:
                ote_ok = True
        else:
            ote_ok = True

        # ── Paso 6: Niveles de riesgo ────────────────────────────────────────
        atr_series = ta.ATR(df["high"], df["low"], df["close"], 14)
        atr = float(atr_series.dropna().iloc[-1]) if not atr_series.dropna().empty else 0.001

        sl_price = self._calculate_sl(sweep, atr)
        tp_price, target_type = self._calculate_tp(df, sweep["type"], entry_price, sl_price)

        direction_str = "LARGO" if sweep["type"] == "BULLISH" else "CORTO"
        sl_dist     = abs(entry_price - sl_price)

        multiplier  = getPipMultiplier(symbol)
        min_dist_abs = 6.0 / multiplier

        if sl_dist < min_dist_abs:
            logger.info(f"[SilverBullet][{symbol}] SL muy pequeño ({sl_dist * multiplier:.1f} pips < 6)")
            return None

        tp_dist = abs(tp_price - entry_price)
        if tp_dist < min_dist_abs:
            logger.info(f"[SilverBullet][{symbol}] TP muy pequeño ({tp_dist * multiplier:.1f} pips < 6)")
            return None

        atr_min = atr * 0.3
        if sl_dist < atr_min:
            logger.info(f"[SilverBullet][{symbol}] SL ({sl_dist:.5f}) < 0.3*ATR ({atr_min:.5f})")
            return None

        rr = calculateRR(entry_price, sl_price, tp_price)
        confidence = 80 if ote_ok else 65
        confidence += 5 if adx > 25 else 0

        # --- SEMÁFORO DE ENTRADA (Price Action) ---
        total_dist = abs(tp_price - entry_price)
        current_dist = abs(current_price - entry_price)
        
        progress_pct = (current_dist / total_dist) * 100 if total_dist > 0 else 0
        
        status_msg = "EN ZONA ✅"
        if progress_pct > 100: status_msg = "META ALCANZADA 🚨"
        elif progress_pct > 50: status_msg = "ALEJÁNDOSE ⚠️"

        now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
        last_closed = get_last_closed_candle(now_cdmx, interval=5)

        return {
            "strategy":    "SILVER BULLET ICT",
            "window":      window_name,
            "window_label": window["label"],
            "direction":   direction_str,
            "entryPrice":  entry_price,
            "stopLoss":    sl_price,
            "takeProfit":  tp_price,
            "slDistance":  sl_dist,
            "status":      status_msg,
            "riesgo_pips": round(sl_dist * multiplier, 1),
            "rr_ratio":    round(rr, 2),
            "confidence":  confidence,
            "setup":       f"Silver Bullet {window['emoji']} {window['label']}",
            "fvg":         fvg["type"],
            "sweep_type":  sweep["type"],
            "ote_ok":      ote_ok,
            "adx":         round(adx, 1),
            "target_type": target_type,
            "candle_time": last_closed.strftime("%Y-%m-%d %H:%M:%S"),
        }

    # ─────────────────────────────────────────────────────────────────────────
    #  Ejecución de trades
    # ─────────────────────────────────────────────────────────────────────────

    async def _execute_trades(self, signal: Dict, symbolInfo: Dict):
        if not signal:
            return

        if not self.accounts:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                logger.warning("[SilverBullet] No hay cuentas disponibles")
                return

        for account in self.accounts:
            # Excluir cuenta maestra de señales (SENTINEL)
            if account['idCuenta'] == 1: continue
            if not dbManager.isEstrategiaHabilitadaParaCuenta(account["idCuenta"], "SilverBullet"):
                logger.info(
                    f"[SilverBullet] Estrategia deshabilitada para cuenta "
                    f"{account['idCuenta']}, omitiendo..."
                )
                continue

            posSize, riskUsd, marginUsed = risk.calculatePositionSize(
                capital=float(account["Capital"]),
                riskPercentage=float(account["ganancia"]),
                slDistance=signal["slDistance"],
                symbolInfo=symbolInfo,
                entryPrice=signal.get("entryPrice"),
            )

            if posSize is None or posSize == 0:
                logger.warning(
                    f"[SilverBullet] Size=0 para {symbolInfo['symbol']} "
                    f"cuenta {account['idCuenta']}"
                )
                continue

            signal['profit'] = riskUsd
            trade = {
                "idCuenta":   account["idCuenta"],
                "symbol":     symbolInfo["symbol"],
                "direction":  signal["direction"],
                "entryPrice": signal["entryPrice"],
                "openTime":   datetime.now(MX_TZ).strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss":   signal["stopLoss"],
                "takeProfit": signal["takeProfit"],
                "size":       posSize,
                "intervalo":  "5min",
                "status":     "OPEN",
                "strategy":   "SilverBullet",
                "margin_used": marginUsed,
            }

            from middleware.execution.broker_gateway import gateway
            success, msgId = await gateway.execute_trade(
                trade, signal, account, "SilverBullet"
            )

            if success and msgId:
                self.lastMessageIds[symbolInfo["symbol"]] = msgId
                logger.info(
                    f"✅ [SilverBullet] Alerta enviada para {symbolInfo['symbol']} "
                    f"cuenta {account['idCuenta']} | "
                    f"{signal['direction']} @ {signal['entryPrice']:.4f} "
                    f"SL={signal['stopLoss']:.4f} TP={signal['takeProfit']:.4f} "
                    f"RR={signal['rr_ratio']}"
                )

    # ─────────────────────────────────────────────────────────────────────────
    #  Entry point principal — llamado desde main.py
    # ─────────────────────────────────────────────────────────────────────────

    async def runAnalysisCycleForSymbol(
        self,
        symbolInfo: Dict,
        preloadedData: Dict = None,
        apiKey: str = None
    ):
        """
        Punto de entrada para el análisis por símbolo.
        Solo actúa si hay una ventana Silver Bullet activa en este momento.
        """
        symbol = symbolInfo["symbol"]
        logger.info(f"▶ [SilverBullet] Verificando ventana activa para {symbol}")

        # ── ¿Estamos en una ventana Silver Bullet? - COMENTADO PARA PRUEBAS ──
        # result = self._get_active_window()
        # if result is None:
        #     logger.info(f"[SilverBullet][{symbol}] Fuera de ventana Silver Bullet — sin acción")
        #     return
        # window_name, window = result
        window_name = "ALL_DAY"
        window = {"label": "All Day 🕐", "start": None, "end": None}

        logger.info(
            f"[SilverBullet][{symbol}] ✅ Ejecutando (ventana deshabilitada para pruebas)"
        )

        # ── Evitar señal duplicada en la misma ventana y día ─────────────────
        sig_key = self._signal_key(symbol, window_name)
        if self._signals_sent.get(sig_key, False):
            logger.info(f"[SilverBullet][{symbol}] Señal ya enviada para esta ventana hoy")
            return

        # ── Obtener datos ────────────────────────────────────────────────────
        df = preloadedData.get(symbol) if preloadedData else None
        if df is None or len(df) < 50:
            logger.warning(f"[SilverBullet][{symbol}] Datos insuficientes")
            return

        # ── Analizar ventana ─────────────────────────────────────────────────
        signal = await self._analyze_window(df, symbol, window_name, window)

        if signal:
            logger.info(
                f"[SilverBullet][{symbol}] 🎯 Señal generada: "
                f"{signal['direction']} | Conf={signal['confidence']}% | "
                f"RR={signal['rr_ratio']} | OTE={'✅' if signal['ote_ok'] else '⚠️'}"
            )
            await self._execute_trades(signal, symbolInfo)
            self._signals_sent[sig_key] = True
        else:
            logger.info(f"[SilverBullet][{symbol}] Sin señal en ventana {window_name}")

        logger.info(f"◀ [SilverBullet] SALIENDO análisis para {symbol}")
