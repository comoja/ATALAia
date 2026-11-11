"""
Script para sincronizar emisoras del IPC mexicano en ATALAia y descargar velas históricas.
"""
import sys
import os
import requests
import datetime
import time
from decimal import Decimal
from sqlalchemy import text

# Configurar ruta
sys.path.insert(0, '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia')
from backend.database.models import SessionLocal

BASE_URL = 'https://ciapi.cityindex.com/TradingApi'
PAYLOAD = {
    'UserName': 'comoja66@gmail.comauui',
    'Password': 'PruebaJ34ny.',
    'AppKey': 'Ja.Morales'
}

# 26 Emisoras del IPC mexicano disponibles en FOREX.com / CityIndex
MEXICAN_IPC_STOCKS = [
    {
        'symbol': 'AMX',
        'market_id': 407324795,
        'forex_name': 'America Movil SAB de CV',
        'bmv_ticker': 'AMX.B',
        'currency': 'MXN'
    },
    {
        'symbol': 'WALMEX',
        'market_id': 401998575,
        'forex_name': 'Walmart De Mexico SAB',
        'bmv_ticker': 'WALMEX',
        'currency': 'MXN'
    },
    {
        'symbol': 'GFNORTE',
        'market_id': 402019086,
        'forex_name': 'Grupo Financiero Banorte O',
        'bmv_ticker': 'GFNORTE.O',
        'currency': 'MXN'
    },
    {
        'symbol': 'GMEXICO',
        'market_id': 402014082,
        'forex_name': 'Grupo Mexico SAB DE CV SER B',
        'bmv_ticker': 'GMEXICO.B',
        'currency': 'MXN'
    },
    {
        'symbol': 'FEMSA',
        'market_id': 402014084,
        'forex_name': 'Fomento Economico Mexica UBD',
        'bmv_ticker': 'FEMSA.UBD',
        'currency': 'MXN'
    },
    {
        'symbol': 'CEMEX',
        'market_id': 402017859,
        'forex_name': 'Cemex Sab de CV',
        'bmv_ticker': 'CEMEX.CPO',
        'currency': 'MXN'
    },
    {
        'symbol': 'BIMBO',
        'market_id': 402016606,
        'forex_name': 'Grupo Bimbo Sab Series A',
        'bmv_ticker': 'BIMBO.A',
        'currency': 'MXN'
    },
    {
        'symbol': 'AC',
        'market_id': 402652691,
        'forex_name': 'Arca Continental SAB de CV',
        'bmv_ticker': 'AC',
        'currency': 'MXN'
    },
    {
        'symbol': 'KOF',
        'market_id': 401998577,
        'forex_name': 'Coca Cola Femsa Sab',
        'bmv_ticker': 'KOF.UBL',
        'currency': 'MXN'
    },
    {
        'symbol': 'TLEVISA',
        'market_id': 402020969,
        'forex_name': 'Grupo Televisa SAB SER CPO',
        'bmv_ticker': 'TLEVISA.CPO',
        'currency': 'MXN'
    },
    {
        'symbol': 'GAP',
        'market_id': 402025819,
        'forex_name': 'Grupo Aeroportuario del Pacific Sab CV',
        'bmv_ticker': 'GAP.B',
        'currency': 'MXN'
    },
    {
        'symbol': 'ASUR',
        'market_id': 400204417,
        'forex_name': 'Grupo Aeroportuario Sureste',
        'bmv_ticker': 'ASUR.B',
        'currency': 'USD'  # ADR ASR
    },
    {
        'symbol': 'OMA',
        'market_id': 402027207,
        'forex_name': 'Grupo Aeroportuario del Centro Norte',
        'bmv_ticker': 'OMA.B',
        'currency': 'MXN'
    },
    {
        'symbol': 'VOLAR',
        'market_id': 406535525,
        'forex_name': 'Controladora Vuela Cia de Avia (ADR)',
        'bmv_ticker': 'VOLAR.A',
        'currency': 'USD'  # ADR VLRS
    },
    {
        'symbol': 'ALSEA',
        'market_id': 402035747,
        'forex_name': 'Alsea SAB de CV',
        'bmv_ticker': 'ALSEA',
        'currency': 'MXN'
    },
    {
        'symbol': 'PINFRA',
        'market_id': 402019416,
        'forex_name': 'Promotora Y Operadora DE INF',
        'bmv_ticker': 'PINFRA',
        'currency': 'MXN'
    },
    {
        'symbol': 'LIVEPOL',
        'market_id': 402025822,
        'forex_name': 'El Puerto de Liverpool',
        'bmv_ticker': 'LIVEPOL.C1',
        'currency': 'MXN'
    },
    {
        'symbol': 'PENOLES',
        'market_id': 402012151,
        'forex_name': 'Industrias Penoles SAB DE CV',
        'bmv_ticker': 'PE&OLES',
        'currency': 'MXN'
    },
    {
        'symbol': 'BBAJIO',
        'market_id': 402035743,
        'forex_name': 'Banco Del Bajio SA',
        'bmv_ticker': 'BBAJIO.O',
        'currency': 'MXN'
    },
    {
        'symbol': 'GRUMA',
        'market_id': 402012149,
        'forex_name': 'Gruma SA BB',
        'bmv_ticker': 'GRUMA.B',
        'currency': 'MXN'
    },
    {
        'symbol': 'KIMBER',
        'market_id': 402020971,
        'forex_name': 'Kimberly Clark DE Mexico A',
        'bmv_ticker': 'KIMBER.A',
        'currency': 'MXN'
    },
    {
        'symbol': 'ORBIA',
        'market_id': 402023434,
        'forex_name': 'Orbia Advance Corp SAB DE CV',
        'bmv_ticker': 'ORBIA',
        'currency': 'MXN'
    },
    {
        'symbol': 'MEGACABLE',
        'market_id': 402027204,
        'forex_name': 'Megacable Holdings SAB de CV',
        'bmv_ticker': 'MEGA.CPO',
        'currency': 'MXN'
    },
    {
        'symbol': 'VESTA',
        'market_id': 402036987,
        'forex_name': 'Corporacion Inmobiliaria Vesta SAB de CV',
        'bmv_ticker': 'VESTA',
        'currency': 'MXN'
    },
    {
        'symbol': 'CUERVO',
        'market_id': 402019419,
        'forex_name': 'Becle Sab De CV',
        'bmv_ticker': 'CUERVO',
        'currency': 'MXN'
    },
    {
        'symbol': 'GCC',
        'market_id': 402027209,
        'forex_name': 'GCC SAB DE CV',
        'bmv_ticker': 'GCC',
        'currency': 'MXN'
    }
]

def parse_ms_date(d_str):
    if not d_str:
        return None
    # Formato: /Date(1793923200000)/
    cleaned = d_str.replace('/Date(', '').replace(')/', '')
    if '+' in cleaned:
        cleaned = cleaned.split('+')[0]
    elif '-' in cleaned:
        cleaned = cleaned.split('-')[0]
    ts = int(cleaned) / 1000.0
    return datetime.datetime.fromtimestamp(ts, datetime.timezone.utc).replace(tzinfo=None)

def main():
    print("Iniciando sesión en CIAPI FOREX.com...")
    resp = requests.post(f"{BASE_URL}/session", json=PAYLOAD, headers={'Content-Type': 'application/json'})
    if resp.status_code != 200:
        print(f"Error autenticando: {resp.status_code} {resp.text}")
        return
    
    session_id = resp.json().get('Session')
    headers = {
        'Content-Type': 'application/json',
        'Session': session_id,
        'UserName': PAYLOAD['UserName']
    }
    print("Sesión establecida correctamente.")

    db = SessionLocal()
    start_filter_date = datetime.datetime(2026, 10, 1, 0, 0, 0)

    print(f"\n1. Registrando {len(MEXICAN_IPC_STOCKS)} símbolos del IPC en la base de datos...")
    for item in MEXICAN_IPC_STOCKS:
        sym = item['symbol']
        m_id = item['market_id']
        f_name = item['forex_name']
        ccy = item['currency']
        
        # Obtener información de mercado para apalancamiento/margen
        m_resp = requests.get(f"{BASE_URL}/market/{m_id}/information", headers=headers)
        margin = 5.0
        decimals = 2
        min_sz = 1.0
        if m_resp.status_code == 200:
            m_info = m_resp.json().get('MarketInformation', {})
            raw_margin = float(m_info.get('MarginFactor') or m_info.get('Margin') or 5.0)
            # En MySQL margen es decimal(5,4), maximo valor 9.9999
            margin = min(raw_margin, 9.9999) if raw_margin < 10.0 else 5.0
            decimals = int(m_info.get('PriceDecimalPlaces') or 2)
            min_sz = float(m_info.get('MinOrderSize') or 1.0)
        
        pip_val = 0.01 if decimals <= 2 else 0.001
        
        # 1. Tabla symbols
        db.execute(text("""
            INSERT INTO symbols (
                symbol, tipo, activoRatio, activoSentinel, startDate, margen, min_lots, 
                pip, quote_currency, multiplo, priceOffset, idForex, FOREX, MT5, TradingView, broker
            ) VALUES (
                :sym, 'ACCION', 1, 1, '2026-10-01', :margen, :min_sz,
                :pip, :ccy, 100, 0.0, :idForex, :forex_name, :sym, :tv, 0
            )
            ON DUPLICATE KEY UPDATE
                tipo = 'ACCION',
                activoRatio = 1,
                activoSentinel = 1,
                margen = VALUES(margen),
                min_lots = VALUES(min_lots),
                pip = VALUES(pip),
                quote_currency = VALUES(quote_currency),
                idForex = VALUES(idForex),
                FOREX = VALUES(FOREX),
                MT5 = VALUES(MT5)
        """), {
            "sym": sym,
            "margen": margin,
            "min_sz": min_sz,
            "pip": pip_val,
            "ccy": ccy,
            "idForex": m_id,
            "forex_name": f_name,
            "tv": f"BMV:{sym}"
        })

        # 2. Tabla sentinelsymbol
        db.execute(text("""
            INSERT INTO sentinelsymbol (
                symbol, tipo, Activo, startDate, margen, min_lots,
                pip, quote_currency, FOREX, MT5, TradingView, multiplo, priceOffset, idForex, broker
            ) VALUES (
                :sym, 'ACCION', 1, '2026-10-01', :margen, :min_sz,
                :pip, :ccy, :forex_name, :sym, :tv, 100, 0.0, :idForex, 0
            )
            ON DUPLICATE KEY UPDATE
                tipo = 'ACCION',
                Activo = 1,
                margen = VALUES(margen),
                min_lots = VALUES(min_lots),
                pip = VALUES(pip),
                quote_currency = VALUES(quote_currency),
                idForex = VALUES(idForex),
                FOREX = VALUES(FOREX),
                MT5 = VALUES(MT5)
        """), {
            "sym": sym,
            "margen": margin,
            "min_sz": min_sz,
            "pip": pip_val,
            "ccy": ccy,
            "idForex": m_id,
            "forex_name": f_name,
            "tv": f"BMV:{sym}"
        })

        # 3. Tabla ratiosymbol
        db.execute(text("""
            INSERT INTO ratiosymbol (symbol, Activo, tipo)
            VALUES (:sym, 1, 'ACCION')
            ON DUPLICATE KEY UPDATE
                Activo = 1,
                tipo = 'ACCION'
        """), {"sym": sym})

    db.commit()
    print(" Símbolos insertados/actualizados con éxito en symbols, sentinelsymbol y ratiosymbol.\n")

    # 2. Descargar velas históricas desde el 1 de octubre de 2026
    # Timeframes a sincronizar:
    #   - 1min: interval=MINUTE, span=1
    #   - 5min: interval=MINUTE, span=5
    #   - 1h:   interval=HOUR,   span=1
    #   - 1d:   interval=DAY,    span=1 (también llena stockprices)
    
    timeframes = [
        ('1d', 'DAY', 1),
        ('1h', 'HOUR', 1),
        ('5min', 'MINUTE', 5),
        ('1min', 'MINUTE', 1),
    ]

    print("2. Descargando velas históricas desde 2026-10-01...")
    
    total_candles_inserted = 0
    total_stockprices_inserted = 0

    for idx, item in enumerate(MEXICAN_IPC_STOCKS, 1):
        sym = item['symbol']
        m_id = item['market_id']
        print(f"[{idx}/{len(MEXICAN_IPC_STOCKS)}] Descargando {sym} (MarketId: {m_id})...")
        
        sym_candles = 0
        sym_prices = 0

        for tf_label, interval, span in timeframes:
            try:
                b_url = f"{BASE_URL}/market/{m_id}/barhistory?interval={interval}&span={span}&priceTicks=4000"
                b_resp = requests.get(b_url, headers=headers, timeout=15)
                if b_resp.status_code != 200:
                    print(f"   ⚠️ {sym} ({tf_label}) error {b_resp.status_code}")
                    continue

                bars = b_resp.json().get('PriceBars', [])
                if not bars:
                    continue

                candles_to_insert = []
                prices_to_insert = []

                for b in bars:
                    dt = parse_ms_date(b.get('BarDate'))
                    if not dt or dt < start_filter_date:
                        continue
                    
                    o = float(b.get('Open') or 0.0)
                    h = float(b.get('High') or 0.0)
                    l = float(b.get('Low') or 0.0)
                    c = float(b.get('Close') or 0.0)
                    v = float(b.get('Volume') or 0.0)

                    candles_to_insert.append({
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
                        prices_to_insert.append({
                            'sym': sym,
                            'dt': dt,
                            'close': c
                        })

                if candles_to_insert:
                    # Insertar en lotes en candles
                    db.execute(text("""
                        INSERT INTO candles (symbol, timeframe, timestamp, open, high, low, close, volume)
                        VALUES (:sym, :tf, :ts, :open, :high, :low, :close, :volume)
                        ON DUPLICATE KEY UPDATE
                            open = VALUES(open),
                            high = VALUES(high),
                            low = VALUES(low),
                            close = VALUES(close),
                            volume = VALUES(volume)
                    """), candles_to_insert)
                    sym_candles += len(candles_to_insert)

                if prices_to_insert:
                    # Insertar en stockprices
                    db.execute(text("""
                        INSERT INTO stockprices (symbol, priceDate, closePrice)
                        VALUES (:sym, :dt, :close)
                        ON DUPLICATE KEY UPDATE
                            closePrice = VALUES(closePrice)
                    """), prices_to_insert)
                    sym_prices += len(prices_to_insert)

            except Exception as ex_tf:
                print(f"   ❌ Error en {sym} ({tf_label}): {ex_tf}")

            # Pequeña pausa para no saturar la API
            time.sleep(0.1)

        db.commit()
        total_candles_inserted += sym_candles
        total_stockprices_inserted += sym_prices
        print(f"   -> Insertadas: {sym_candles} velas (candles) y {sym_prices} precios diarios (stockprices).")

    db.close()
    print(f"\n PROCESO COMPLETADO EXITOSAMENTE:")
    print(f"   - Total de símbolos sincronizados: {len(MEXICAN_IPC_STOCKS)}")
    print(f"   - Total de registros insertados en candles: {total_candles_inserted}")
    print(f"   - Total de registros insertados en stockprices: {total_stockprices_inserted}")

if __name__ == '__main__':
    main()
