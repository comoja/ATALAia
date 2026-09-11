"""
Módulo centralizado para el cálculo de PnL No Realizado, Valor de Pip y Margen Requerido
según la metodología institucional de FOREX.com / ATALAia.

Regla Oficial de FOREX.com:
1. PnL No Realizado:
   - Cuando el USD es el DENOMINADOR (cotización directa, ej: EUR/USD, GBP/USD, AUD/USD, NZD/USD, XAU/USD, XAG/USD):
       Se divide entre 1.0.
       PnL = (delta_precio * cantidad) / 1.0
       pip_val_usd = cantidad * pip_size

   - Cuando el USD NO es el DENOMINADOR (cotización indirecta, ej: USD/CAD, USD/MXN, USD/JPY, USD/CHF):
       Se divide entre el precio actual de mercado (cur_px).
       PnL = (delta_precio * cantidad) / cur_px
       pip_val_usd = (cantidad * pip_size) / cur_px

2. Margen Requerido:
   - Cuando el USD es la DIVISA BASE (ej: USD/CAD, USD/MXN, USD/JPY, USD/CHF):
       Margen = cantidad * (margin_factor / 100.0)
       (Para USD/CAD, USD/JPY, USD/CHF el margin_factor oficial es 0.5%, para USD/MXN es 1.0%)

   - Cuando el USD NO es la DIVISA BASE (ej: EUR/USD, GBP/USD, AUD/USD, NZD/USD):
       Margen = cantidad * precio * (margin_factor / 100.0)
       (Para EUR/USD, GBP/USD, AUD/USD el margin_factor oficial es 0.5%)
"""

from typing import Tuple


def is_usd_denominator(symbol: str) -> bool:
    """
    Determina si el USD es la divisa denominadora (segunda divisa / cotizada) del par.
    Soporta símbolos con barra (EUR/USD) o continuos (EURUSD).
    """
    if not symbol:
        return False
    clean = symbol.strip().upper()
    if "/" in clean:
        parts = clean.split("/")
        return parts[1].strip() == "USD"
    return clean.endswith("USD") and not clean.startswith("USD")


def calculate_unrealized_pnl(
    symbol: str,
    direction: str,
    entry_price: float,
    current_price: float,
    size: float,
    pip_size: float = 0.0001
) -> Tuple[float, float, float, float]:
    """
    Calcula de manera centralizada y uniforme:
    1. pnl: Ganancia/pérdida no realizada en USD (float redondeado a 2 decimales).
    2. pips: Movimiento en pips según la dirección de la orden (float redondeado a 1 decimal).
    3. pip_value_usd: Valor monetario de 1 pip en USD para el tamaño dado.
    4. delta: Diferencia de precio (cur_px - entry si largo, entry - cur_px si corto).
    """
    entry = float(entry_price or 0.0)
    cur_px = float(current_price or 0.0)
    sz = float(size or 0.0)
    pip_sz = float(pip_size or 0.0001)

    if cur_px <= 0.0:
        cur_px = entry

    d = str(direction or "").upper()
    is_long = ("LARG" in d) or ("BUY" in d) or ("LONG" in d) or ("COMPRA" in d)

    delta = (cur_px - entry) if is_long else (entry - cur_px)
    pips = round(delta / pip_sz, 1) if pip_sz > 0 else 0.0

    if is_usd_denominator(symbol):
        divisor = 1.0
    else:
        divisor = cur_px if cur_px > 0 else 1.0

    pnl = round((delta * sz) / divisor, 2)
    pip_val_usd = round((sz * pip_sz) / divisor, 5)

    return pnl, pips, pip_val_usd, delta


def calculate_margin(
    symbol: str,
    size: float,
    price: float,
    margin_factor: float = 0.5
) -> float:
    """
    Calcula el margen requerido en USD según la metodología oficial de FOREX.com.

    Parámetros:
    - symbol: Par operado (ej: 'EUR/USD', 'USD/CAD', 'USD/MXN', 'USD/JPY').
    - size: Unidades / tamaño de la orden.
    - price: Precio de ejecución / mercado.
    - margin_factor: Factor de margen porcentual (default 0.5 para 0.5% / apalancamiento 1:200).

    Retorna:
    - float: Margen retenido en USD redondeado a 2 decimales.
    """
    sz = float(size or 0.0)
    px = float(price or 0.0)
    mf = float(margin_factor or 0.5)
    rate = (mf / 100.0) if mf >= 0.05 else mf

    sym = str(symbol or "").strip().upper()
    if sym.startswith("USD/"):
        return round(sz * rate, 2)
    else:
        return round(sz * px * rate, 2)
