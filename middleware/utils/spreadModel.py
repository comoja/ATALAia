import statistics


# =========================
# 📊 ESTIMADOR DE SPREAD
# =========================
class SpreadEstimator:
    def __init__(self, window=20):
        self.ranges = []
        self.window = window

    def update(self, candle):
        r = candle['high'] - candle['low']
        self.ranges.append(r)

        if len(self.ranges) > self.window:
            self.ranges.pop(0)

    def get_spread(self):
        if not self.ranges:
            return 0.0002  # valor por defecto (~2 pips)

        avg_range = statistics.mean(self.ranges)

        # spread proporcional a volatilidad
        return avg_range * 0.2


# =========================
# 🔍 DETECTOR DE CAMBIOS
# =========================
def detect_spread_change(current, previous, threshold=0.0003):
    if previous is None:
        return False
    return abs(current - previous) > threshold


# =========================
# 📉 SIMULADOR BID/ASK (PRO)
# =========================
def simulate_bid_ask(candle, spread):
    """
    Simulación simétrica
    """
    o, h, l, c = candle['open'], candle['high'], candle['low'], candle['close']
    half = spread / 2

    return {
        "bid": {
            "open": o - half,
            "high": h - half,
            "low":  l - half,
            "close": c - half
        },
        "ask": {
            "open": o + half,
            "high": h + half,
            "low":  l + half,
            "close": c + half
        }
    }


def simulate_bid_ask_asymmetric(candle, spread):
    """
    Simulación más realista (tu caso)
    """
    o, h, l, c = candle['open'], candle['high'], candle['low'], candle['close']

    # distribución asimétrica (ajustada a lo que descubriste)
    spread_high = spread * 0.3
    spread_low  = spread * 0.8

    return {
        "bid": {
            "open": o - spread_low,
            "high": h - spread_low,
            "low":  l - spread_low,
            "close": c - spread_low
        },
        "ask": {
            "open": o + spread_high,
            "high": h + spread_high,
            "low":  l + spread_high,
            "close": c + spread_high
        }
    }
