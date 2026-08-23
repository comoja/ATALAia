import os, sys, asyncio
projectRoot = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if projectRoot not in sys.path: sys.path.insert(0, projectRoot)

from backend.database.models import SessionLocal
from sqlalchemy import text
from backend.services.microRatio import buildRatioEntryAlertMessage, sendRatioTelegramAlert, fetchAccountData

async def main():
    db = SessionLocal()
    
    # 1. Obtener los trades agrupados por setup y cuenta
    sql = """
        SELECT idTrade, idCuenta, strategy, setup, symbol, status, direction, size, entryPrice, margin_used, candleTime, openTime
        FROM trades
        WHERE strategy = 'RATIO ATALAia' AND status = 'OPEN'
        ORDER BY setup, idTrade ASC
    """
    rows = db.execute(text(sql)).fetchall()
    print(f"Encontrados {len(rows)} trades abiertos en trades.")

    # Agrupar por (idCuenta, setup)
    grouped = {}
    for r in rows:
        key = (r[1], r[3]) # (idCuenta, setup)
        if key not in grouped:
            grouped[key] = []
        grouped[key].append({
            "idTrade": r[0],
            "idCuenta": r[1],
            "strategy": r[2],
            "setup": r[3],
            "symbol": r[4],
            "status": r[5],
            "direction": r[6],
            "size": float(r[7]),
            "entryPrice": float(r[8]),
            "margin_used": float(r[9]),
            "candleTime": str(r[10]),
            "openTime": str(r[11])
        })

    print(f"Total de Ratios pareados a notificar: {len(grouped)}")

    for (idCuenta, setupName), pairTrades in grouped.items():
        if len(pairTrades) < 2:
            print(f"⚠️ Ratio {setupName} en Cuenta #{idCuenta} tiene menos de 2 patas. Omitiendo...")
            continue

        tradeA = pairTrades[0]
        tradeB = pairTrades[1]

        numerador, denominador = setupName.split(" - ")
        if tradeA["symbol"] != numerador and tradeB["symbol"] == numerador:
            tradeA, tradeB = tradeB, tradeA

        accountData = fetchAccountData(db, idCuenta)
        accountCapital = accountData.get("capital", 300.0)
        accountName = accountData.get("nombre", f"Cuenta #{idCuenta}")

        marginA = tradeA["margin_used"]
        marginB = tradeB["margin_used"]
        totalMargin = marginA + marginB

        # Construir mensaje
        msg = buildRatioEntryAlertMessage(
            accountName=accountName,
            accountCapital=accountCapital,
            numerador=numerador,
            denominador=denominador,
            periodo="1d",
            signalType="ARBITRAJE DE RATIO ACTIVO",
            directionA=tradeA["direction"],
            directionB=tradeB["direction"],
            unitsA=tradeA["size"],
            unitsB=tradeB["size"],
            priceA=tradeA["entryPrice"],
            priceB=tradeB["entryPrice"],
            marginA=marginA,
            marginB=marginB,
            totalMargin=totalMargin
        )

        print(f"\n========================================================")
        print(f"Despachando alerta para Cuenta #{idCuenta} ({accountName}): {setupName}")
        print(msg)
        print(f"========================================================")

        await sendRatioTelegramAlert(idCuenta, msg)
        await asyncio.sleep(1.0)

    print("\n✅ ¡Todos los mensajes registrados han sido enviados exitosamente!")

if __name__ == "__main__":
    asyncio.run(main())
