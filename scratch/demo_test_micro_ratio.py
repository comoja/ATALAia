import sys, os
projectRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if projectRoot not in sys.path:
    sys.path.insert(0, projectRoot)

from datetime import datetime
from sqlalchemy import text
from backend.database.models import SessionLocal
from backend.services.microRatio import openRatioTrades, checkActiveOpenTrades, closeRatioTrades

db = SessionLocal()

# 1. Configuración de prueba
ratioConfig = {
    'id': 99,
    'idUsuario': 1,
    'idCuenta': 2, # Cuenta CLNMRLS (jcolin)
    'numerador': 'EUR/USD',
    'denominador': 'USD/MXN',
    'periodo': '1d'
}
signalInfo = {
    'signalType': 'TRIANGULO_VERDE (EUR/USD + USD/MXN)',
    'directionA': 'LARGO',
    'directionB': 'CORTO',
    'candleTime': datetime.now(),
    'latestPriceA': 1.0854,
    'latestPriceB': 19.4520
}
symA = {'minLots': 1000.0, 'margenRate': 0.0025, 'pip': 0.0001, 'quoteCurrency': 'USD'}
symB = {'minLots': 1000.0, 'margenRate': 0.0100, 'pip': 0.0100, 'quoteCurrency': 'MXN'}

row_c = db.execute(text('SELECT Capital FROM cuenta WHERE idCuenta = 2')).fetchone()
cap_inicial = float(row_c[0])
print(f'=== CAPITAL INICIAL EN CUENTA #2: ${cap_inicial:,.2f} USD ===')

# 2. Ejecutar Apertura de 2 Patas
print('\n1. EJECUTANDO APERTURA DE 2 PATAS EN trades...')
openRatioTrades(db, ratioConfig, signalInfo, symA, symB, {'idCuenta': 2, 'capital': cap_inicial})

# 3. Consultar las órdenes creadas en trades
print('\n2. REGISTROS INSERTADOS EN LA TABLA trades:')
trades = db.execute(text("""
    SELECT idTrade, idCuenta, strategy, setup, symbol, status, direction, size, entryPrice, margin_used, openTime
    FROM trades
    WHERE idCuenta = 2 AND strategy = 'RATIO ATALAia' AND status = 'OPEN'
""")).fetchall()
for t in trades:
    print('  ->', t)

row_c_mid = db.execute(text('SELECT Capital FROM cuenta WHERE idCuenta = 2')).fetchone()
print(f'\n=== CAPITAL TRAS RETENCIÓN DE MARGEN: ${float(row_c_mid[0]):,.2f} USD ===')

# 4. Simular Cierre en Círculo de Media (●)
print('\n3. EJECUTANDO CIERRE EN CÍRCULO DE MEDIA (●)...')
open_list = checkActiveOpenTrades(db, 2, 'EUR/USD - USD/MXN')
symMap = {'EUR/USD': symA, 'USD/MXN': symB}
priceMap = {'EUR/USD': 1.0920, 'USD/MXN': 19.1500}
closeRatioTrades(db, 2, 'EUR/USD - USD/MXN', open_list, symMap, priceMap)

# 5. Consultar los trades cerrados con PnL
print('\n4. REGISTROS TRAS EL CIERRE:')
closed = db.execute(text("""
    SELECT idTrade, symbol, status, direction, entryPrice, exitPrice, margin_used, pnl, closeTime
    FROM trades
    WHERE idCuenta = 2 AND strategy = 'RATIO ATALAia'
    ORDER BY idTrade DESC LIMIT 2
""")).fetchall()
for c in closed:
    print('  ->', c)

row_c_final = db.execute(text('SELECT Capital FROM cuenta WHERE idCuenta = 2')).fetchone()
print(f'\n=== CAPITAL FINAL TRAS CIERRE Y REINTEGRO: ${float(row_c_final[0]):,.2f} USD ===')

# Limpiar los 2 trades de prueba para mantener limpia la BD
db.execute(text("DELETE FROM trades WHERE idCuenta = 2 AND strategy = 'RATIO ATALAia'"))
db.execute(text("UPDATE cuenta SET Capital = :cap WHERE idCuenta = 2"), {'cap': cap_inicial})
db.commit()
print('\n🧹 Base de datos restaurada al estado original.')
