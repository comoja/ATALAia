"""
==============================================================================
  MÓDULO: ORDER BLOCKS & BREAKER BLOCKS — ICT / Inner Circle Trader
==============================================================================
  Implementa la detección de dos conceptos avanzados de Smart Money:

  ─── ORDER BLOCK (OB) ────────────────────────────────────────────────────────
  El último candle en DIRECCIÓN OPUESTA inmediatamente antes de un
  DESPLAZAMIENTO FUERTE (Displacement). Representa la zona donde los
  institucionales dejaron órdenes abiertas. El precio tiende a regresar
  a rellenar esta área antes de continuar en la dirección original.

    • Bullish OB: Última vela BAJISTA antes de un impulso fuerte alcista.
    • Bearish OB: Última vela ALCISTA antes de un impulso fuerte bajista.

  Criterios del desplazamiento (displacement):
    - Cuerpo > 60% del rango (vela de intención)
    - Mecha en dirección contraria < 25%
    - El cierre supera el high/low de la vela OB (confirmación estructural)

  ─── BREAKER BLOCK (BB) ──────────────────────────────────────────────────────
  Un Order Block que ha sido ROTO (el precio cerró con cuerpo a través del OB)
  se convierte en un Breaker Block. Actúa como zona de resistencia/soporte
  nueva en la dirección CONTRARIA a su naturaleza original.

    • Bullish OB roto → Bearish Breaker (resistencia para futuros rallies)
    • Bearish OB roto → Bullish Breaker (soporte para futuros retrocesos)

  Uso típico:
    - OBs: entry zone para posiciones en HTF (H4/Daily) o re-entradas
    - Breakers: zona de rejection para trades contrariadores o ajuste de TP
==============================================================================
"""

import logging
from typing import List, Optional, Dict
import pandas as pd
import numpy as np

logger = logging.getLogger("sentinel")


# ─────────────────────────────────────────────────────────────────────────────
#  Detección de Order Blocks
# ─────────────────────────────────────────────────────────────────────────────

def detect_order_blocks(
    df: pd.DataFrame,
    direction: str,
    lookback: int = 80,
    min_displacement_body_pct: float = 0.60,
    min_displacement_wick_pct: float = 0.30,
    max_ob_age_candles: int = 50,
) -> List[Dict]:
    """
    Detecta Order Blocks alineados con `direction` en el DataFrame dado.

    Args:
        df                        : OHLCV DataFrame (cualquier timeframe).
        direction                 : 'LARGO' → Bullish OB.
                                    'CORTO' → Bearish OB.
        lookback                  : Número de velas a analizar desde el final.
        min_displacement_body_pct : % mínimo de cuerpo sobre rango para el
                                    displacement (0.60 = 60%).
        min_displacement_wick_pct : % máximo de mecha contratendencia sobre
                                    rango para el displacement (0.30 = 30%).
        max_ob_age_candles        : Edad máxima del OB en velas. Los OBs ya
                                    tocados/mitigados se descarten.

    Returns:
        Lista de OBs ordenada de más reciente a más antiguo (primero el mejor).
    """
    if len(df) < 5:
        return []

    direction_upper = direction.upper()
    obs: List[Dict] = []

    start_i = max(0, len(df) - lookback - 1)
    # Necesitamos la vela i (candidata OB) y la vela i+1 (displacement)
    for i in range(start_i, len(df) - 1):
        vela     = df.iloc[i]
        next_v   = df.iloc[i + 1]

        o, h, l, c = float(vela['open']), float(vela['high']), float(vela['low']), float(vela['close'])
        n_o, n_h, n_l, n_c = float(next_v['open']), float(next_v['high']), float(next_v['low']), float(next_v['close'])

        rango_next = n_h - n_l
        if rango_next <= 0:
            continue

        cuerpo_next = abs(n_c - n_o)
        body_pct_next = cuerpo_next / rango_next

        if direction_upper in ('LARGO'):
            # ── Bullish Order Block ─────────────────────────────────────────
            # Vela OB: bajista (body con cierre < apertura)
            # Displacement: alcista, cuerpo > 60%, mecha superior < 30%,
            #               cierra POR ENCIMA del high de la vela OB
            is_ob_candle        = c < o                              # vela bajista
            is_bullish_disp     = n_c > n_o                         # displacement alcista
            has_enough_body     = body_pct_next >= min_displacement_body_pct
            low_upper_wick      = ((n_h - n_c) / rango_next) <= min_displacement_wick_pct
            closes_above_ob     = n_c > h                           # confirmación estructural

            if is_ob_candle and is_bullish_disp and has_enough_body and low_upper_wick and closes_above_ob:
                # Verificar que el OB no haya sido mitigado (precio cerró por debajo del body bottom)
                recent_closes = df['close'].iloc[i + 2:].values
                mitigated = any(rc < l for rc in recent_closes) if len(recent_closes) > 0 else False

                obs.append({
                    'type':            'Bullish_OB',
                    'top':             h,
                    'bottom':          l,
                    'body_top':        max(o, c),   # techo del cuerpo
                    'body_bottom':     min(o, c),   # piso del cuerpo (zona de mayor probabilidad)
                    'mid':             (h + l) / 2,
                    'idx':             i,
                    'candle_time':     df.index[i],
                    'displacement_body_pct': round(body_pct_next, 3),
                    'mitigated':       mitigated,
                    'age_candles':     len(df) - 1 - i,
                })

        else:  # CORTO
            # ── Bearish Order Block ─────────────────────────────────────────
            # Vela OB: alcista (body con cierre > apertura)
            # Displacement: bajista, cuerpo > 60%, mecha inferior < 30%,
            #               cierra POR DEBAJO del low de la vela OB
            is_ob_candle        = c > o                              # vela alcista
            is_bearish_disp     = n_c < n_o                         # displacement bajista
            has_enough_body     = body_pct_next >= min_displacement_body_pct
            low_lower_wick      = ((n_c - n_l) / rango_next) <= min_displacement_wick_pct
            closes_below_ob     = n_c < l                           # confirmación estructural

            if is_ob_candle and is_bearish_disp and has_enough_body and low_lower_wick and closes_below_ob:
                recent_closes = df['close'].iloc[i + 2:].values
                mitigated = any(rc > h for rc in recent_closes) if len(recent_closes) > 0 else False

                obs.append({
                    'type':            'Bearish_OB',
                    'top':             h,
                    'bottom':          l,
                    'body_top':        max(o, c),
                    'body_bottom':     min(o, c),
                    'mid':             (h + l) / 2,
                    'idx':             i,
                    'candle_time':     df.index[i],
                    'displacement_body_pct': round(body_pct_next, 3),
                    'mitigated':       mitigated,
                    'age_candles':     len(df) - 1 - i,
                })

    # Filtrar OBs muy viejos y los ya mitigados
    obs = [ob for ob in obs if ob['age_candles'] <= max_ob_age_candles and not ob['mitigated']]

    # Ordenar: más reciente primero
    obs.sort(key=lambda x: x['idx'], reverse=True)

    logger.debug(f"[OrderBlocks] Detectados {len(obs)} OB ({direction_upper})")
    return obs


# ─────────────────────────────────────────────────────────────────────────────
#  Detección de Breaker Blocks
# ─────────────────────────────────────────────────────────────────────────────

def detect_breaker_blocks(
    df: pd.DataFrame,
    order_blocks: List[Dict],
) -> List[Dict]:
    """
    Convierte Order Blocks rotos en Breaker Blocks.

    Un OB se considera roto cuando el precio CIERRA con CUERPO a través
    de él (cierre de cuerpo, no solo mecha — principio ICT canónico).

    Returns:
        Lista de Breaker Blocks (tipo 'Bullish_Breaker' o 'Bearish_Breaker').
    """
    if not order_blocks or len(df) < 3:
        return []

    breakers: List[Dict] = []
    closes = df['close'].values
    opens  = df['open'].values

    for ob in order_blocks:
        ob_idx    = ob['idx']
        ob_bottom = ob['bottom']
        ob_top    = ob['top']
        ob_type   = ob['type']

        # Solo buscamos breaks DESPUÉS del OB
        subsequent_closes = closes[ob_idx + 2:]
        subsequent_opens  = opens[ob_idx + 2:]

        if len(subsequent_closes) == 0:
            continue

        if ob_type == 'Bullish_OB':
            # Roto si hay una vela bajista que cierra POR DEBAJO del piso del OB
            for j, (sc, so) in enumerate(zip(subsequent_closes, subsequent_opens)):
                if sc < ob_bottom and sc < so:   # cierre de cuerpo bajista bajo el OB
                    breakers.append({
                        **ob,
                        'type':          'Bearish_Breaker',
                        'broken_ob':     ob_type,
                        'break_idx':     ob_idx + 2 + j,
                        'support_zone':  None,
                        'resistance_zone': (ob_bottom, ob_top),  # actúa como resistencia
                    })
                    break  # Solo el primer break nos interesa

        else:  # Bearish_OB
            # Roto si hay una vela alcista que cierra POR ENCIMA del techo del OB
            for j, (sc, so) in enumerate(zip(subsequent_closes, subsequent_opens)):
                if sc > ob_top and sc > so:   # cierre de cuerpo alcista sobre el OB
                    breakers.append({
                        **ob,
                        'type':          'Bullish_Breaker',
                        'broken_ob':     ob_type,
                        'break_idx':     ob_idx + 2 + j,
                        'support_zone':  (ob_bottom, ob_top),    # actúa como soporte
                        'resistance_zone': None,
                    })
                    break

    logger.debug(f"[BreakerBlocks] Detectados {len(breakers)} BB de {len(order_blocks)} OBs")
    return breakers


# ─────────────────────────────────────────────────────────────────────────────
#  Helpers de consulta
# ─────────────────────────────────────────────────────────────────────────────

def is_price_in_ob(
    price: float,
    ob: Dict,
    use_body_only: bool = True,
    tolerance_pct: float = 0.001,
) -> bool:
    """
    Retorna True si `price` está dentro de la zona del OB.
    Si `use_body_only=True`, compara contra el rango del cuerpo (ICT recomienda
    entrar en el cuerpo, no solo en la mecha).
    """
    if use_body_only:
        low_ref  = ob['body_bottom'] * (1 - tolerance_pct)
        high_ref = ob['body_top']    * (1 + tolerance_pct)
    else:
        low_ref  = ob['bottom'] * (1 - tolerance_pct)
        high_ref = ob['top']    * (1 + tolerance_pct)

    return low_ref <= price <= high_ref


def get_nearest_ob(
    price: float,
    obs: List[Dict],
    direction: str,
) -> Optional[Dict]:
    """
    Retorna el OB más cercano al precio en la dirección dada.
    Para LARGO: el OB bullish más cercano por debajo del precio.
    Para CORTO: el OB bearish más cercano por encima del precio.
    """
    direction_upper = direction.upper()
    candidates = []

    for ob in obs:
        if direction_upper in ('LARGO') and ob['type'] == 'Bullish_OB':
            if ob['top'] <= price:  # OB está por DEBAJO del precio actual
                dist = price - ob['top']
                candidates.append((dist, ob))
        elif direction_upper in ('CORTO') and ob['type'] == 'Bearish_OB':
            if ob['bottom'] >= price:  # OB está Por ENCIMA del precio actual
                dist = ob['bottom'] - price
                candidates.append((dist, ob))

    if not candidates:
        return None

    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]


def ob_confluence_score(
    price: float,
    obs: List[Dict],
    direction: str,
    atr: float = 0.0,
) -> Dict:
    """
    Calcula un score de confluencia basado en la proximidad del precio a OBs.

    Returns dict con:
        - score         : int (0-30)
        - nearest_ob    : dict o None
        - in_ob_zone    : bool
        - ob_count      : número de OBs válidos detectados
    """
    score = 0
    nearest = get_nearest_ob(price, obs, direction)
    in_zone = False

    if nearest is None:
        return {'score': 0, 'nearest_ob': None, 'in_ob_zone': False, 'ob_count': len(obs)}

    # El precio está dentro del OB → máximo bonus
    if is_price_in_ob(price, nearest):
        score = 30
        in_zone = True
    else:
        # Distancia al OB en términos de ATR
        if atr > 0:
            if direction.upper() in ('LARGO'):
                dist = price - nearest['top']
            else:
                dist = nearest['bottom'] - price

            dist_atr = dist / atr
            if dist_atr < 0.5:
                score = 20    # muy cerca (< 0.5 ATR)
            elif dist_atr < 1.0:
                score = 10    # cerca (< 1 ATR)
            elif dist_atr < 2.0:
                score = 5     # medianamente cerca
        else:
            score = 5

    return {
        'score':      score,
        'nearest_ob': nearest,
        'in_ob_zone': in_zone,
        'ob_count':   len(obs),
    }
