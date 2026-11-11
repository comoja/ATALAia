"""
Script Maestro para sincronizar todos los símbolos con sus idForex y descargar
velas históricas en todos los TimeFrames (1min, 5min, 15min, 1h, 1d) desde el 2026-10-01.
"""
import sys
import os
import requests
import datetime
import time
from sqlalchemy import text

sys.path.insert(0, '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia')
from backend.database.models import SessionLocal

BASE_URL = 'https://ciapi.cityindex.com/TradingApi'
PAYLOAD = {
    'UserName': 'comoja66@gmail.comauui',
    'Password': 'PruebaJ34ny.',
    'AppKey': 'Ja.Morales'
}

# 20 Pares pendientes de ID
MISSING_FOREX_IDS = {
    'AUD/CAD': 401203112,
    'AUD/CHF': 401203113,
    'AUD/NZD': 401203116,
    'AUD/SGD': 401203167,
    'CAD/CHF': 401203117,
    'CAD/JPY': 401203118,
    'CHF/JPY': 401203119,
    'EUR/AUD': 401203121,
    'EUR/CHF': 401203189,
    'EUR/JPY': 401449249,
    'EUR/NZD': 401203127,
    'EUR/SGD': 401203172,
    'GBP/AUD': 401203129,
    'GBP/CHF': 401203131,
    'GBP/NZD': 401203135,
    'GBP/SGD': 401203177,
    'NZD/CAD': 401203138,
    'NZD/CHF': 401203139,
    'SGD/JPY': 401203180,
    'USD/SGD': 401203184,
}

TIMEFRAMES = [
    ('1d', 'DAY', 1),
    ('1h', 'HOUR', 1),
    ('15min', 'MINUTE', 15),
    ('5min', 'MINUTE', 5),
    ('1min', 'MINUTE', 1),
]

def parse_ms_date(d_str):
    if not d_str:
        return None
    cleaned = d_str.replace('/Date(', '').replace(')/', '')
    if '+' in cleaned:
        cleaned = cleaned.split('+')[0]
    elif '-' in cleaned:
        cleaned = cleaned.split('-')[0]
    ts = int(cleaned) / 1000.0
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).replace(tzinfo=None)

def main():
    print("1. Iniciando sesión en CIAPI FOREX.com...")
    resp = requests.post(f"{BASE_URL}/session", json=PAYLOAD, headers={'Content-Type': 'application/json'}, timeout=15)
    if resp.status_code != 200:
        print(f"❌ Error autenticando: {resp.status_code} {resp.text}")
        return
    
    session_id = resp.json().get('Session')
    headers = {
        'Content-Type': 'application/json',
        'Session': session_id,
        'UserName': PAYLOAD['UserName']
    }
    print(" Sesión CIAPI establecida correctamente.\n")

    db = SessionLocal()

    # 1. Actualizar los 20 idForex pendientes en symbols y sentinelsymbol
    print("2. Actualizando idForex pendientes en symbols y sentinelsymbol...")
    for sym, m_id in MISSING_FOREX_IDS.items():
        db.execute(text("UPDATE symbols SET idForex = :id WHERE symbol = :s AND (idForex IS NULL OR idForex = 0)"), {"id": m_id, "s": sym})
        db.execute(text("UPDATE sentinelsymbol SET idForex = :id WHERE symbol = :s AND (idForex IS NULL OR idForex = 0)"), {"id": m_id, "s": sym})
    db.commit()
    print(" idForex actualizados con éxito.\n")

    # 2. Consultar todos los símbolos activos que tienen idForex
    query_symbols = text("""
        SELECT symbol, FOREX, idForex, tipo
        FROM symbols
        WHERE idForex IS NOT NULL AND idForex > 0
        ORDER BY tipo ASC, symbol ASC
    """)
    symbols_to_sync = db.execute(query_symbols).fetchall()
    print(f"3. Iniciando sincronización de velas para {len(symbols_to_sync)} símbolos desde 2026-10-01...")
    print(f"   Timeframes a sincronizar: {[tf[0] for tf in TIMEFRAMES]}\n")

    start_filter_date = datetime.datetime(2026, 10, 1, 0, 0, 0)
    total_candles_global = 0
    total_stockprices_global = 0

    for idx, row in enumerate(symbols_to_sync, 1):
        sym = row.symbol
        m_id = row.idForex
        tipo = row.tipo
        print(f"[{idx:02d}/{len(symbols_to_sync)}] {sym:<12} (Tipo: {tipo:<7} | MarketId: {m_id})...", end="", flush=True)

        sym_candles = 0
        sym_prices = 0

        for tf_label, interval, span in TIMEFRAMES:
            try:
                b_url = f"{BASE_URL}/market/{m_id}/barhistory?interval={interval}&span={span}&priceTicks=4000"
                b_resp = requests.get(b_url, headers=headers, timeout=12)
                if b_resp.status_code != 200:
                    continue

                bars = b_resp.json().get('PriceBars', [])
                if not bars:
                    continue

                candles_batch = []
                prices_batch = []

                for b in bars:
                    dt = parse_ms_date(b.get('BarDate'))
                    if not dt or dt < start_filter_date:
                        continue

                    o = float(b.get('Open') or 0.0)
                    h = float(b.get('High') or 0.0)
                    l = float(b.get('Low') or 0.0)
                    c = float(b.get('Close') or 0.0)
                    v = float(b.get('Volume') or 0.0)

                    candles_batch.append({
                        'sym': sym,
                        'tf': tf_label,
                        'ts': dt,
                        'open': o,
                        'high': h,
                        'low': l,
                        'close': c,
                        'volume': v
                    })

                    if tf_label == '1d':
                        prices_batch.append({
                            'sym': sym,
                            'dt': dt,
                            'close': c
                        })

                if candles_batch:
                    db.execute(text("""
                        INSERT INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume)
                        VALUES (:sym, :tf, :ts, :open, :high, :low, :close, :volume)
                        ON DUPLICATE KEY UPDATE
                            open = VALUES(open),
                            high = VALUES(high),
                            low = VALUES(low),
                            close = VALUES(close),
                            volume = VALUES(volume)
                    """), candles_batch)
                    sym_candles += len(candles_batch)

                if prices_batch:
                    db.execute(text("""
                        INSERT INTO stockprices (symbol, priceDate, closePrice)
                        VALUES (:sym, :dt, :close)
                        ON DUPLICATE KEY UPDATE
                            closePrice = VALUES(closePrice)
                    """), prices_batch)
                    sym_prices += len(prices_batch)

            except Exception as ex_tf:
                pass

            time.sleep(0.04)

        db.commit()
        total_candles_global += sym_candles
        total_stockprices_global += sym_prices
        print(f" -> {sym_candles} velas ({sym_prices} daily stockprices)")

    db.close()
    print("\n" + "="*70)
    print(" SINCRONIZACIÓN GLOBAL COMPLETADA")
    print(f" Total símbolos sincronizados: {len(symbols_to_sync)}")
    print(f" Total velas registradas en 'candles': {total_candles_global}")
    print(f" Total precios diarios registrados en 'stockprices': {total_stockprices_global}")
    print("="*70)

if __name__ == '__main__':
    main()

