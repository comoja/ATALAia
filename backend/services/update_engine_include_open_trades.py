quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_open_block = '''                active_trades.append({
                    "signalType": sigType,
                    "direction": direction,
                    "entryDate": d,
                    "entryIndex": i,
                    "entryPxA": pxA,
                    "entryPxB": pxB,
                    "margin": realMargenA + realMargenB,
                    "multA": multA,
                    "multB": multB,
                    "unitsA": unitsA,
                    "unitsB": unitsB
                })

        # Métricas consolidadas'''

replacement_open_block = '''                active_trades.append({
                    "signalType": sigType,
                    "direction": direction,
                    "entryDate": d,
                    "entryIndex": i,
                    "entryPxA": pxA,
                    "entryPxB": pxB,
                    "margin": realMargenA + realMargenB,
                    "multA": multA,
                    "multB": multB,
                    "unitsA": unitsA,
                    "unitsB": unitsB
                })

        # 3. PROCESAR POSICIONES ABIERTAS / EN CURSO AL FINAL DEL HISTORIAL
        if active_trades:
            lastPxA = pricesA[-1]
            lastPxB = pricesB[-1]
            lastDate = dates[-1]
            open_cycle_trades = []
            open_pnl = 0.0
            open_margin = sum(t["margin"] for t in active_trades)
            open_units_a = sum(t["unitsA"] for t in active_trades)
            open_units_b = sum(t["unitsB"] for t in active_trades)

            for t in active_trades:
                raw_trades_count += 1
                if "LONG " + nameA in t["direction"] or "LONG A" in t["direction"]:
                    deltaA = (lastPxA - t["entryPxA"])
                    deltaB = (t["entryPxB"] - lastPxB)
                else:
                    deltaA = (t["entryPxA"] - lastPxA)
                    deltaB = (lastPxB - t["entryPxB"])

                pipsA = deltaA / pipA if pipA > 0 else 0.0
                pipsB = deltaB / pipB if pipB > 0 else 0.0

                pipValA = t["unitsA"] * pipA if quoteA == "USD" else ((t["unitsA"] * pipA) / lastPxA if lastPxA > 0 else t["unitsA"] * pipA)
                pipValB = t["unitsB"] * pipB if quoteB == "USD" else ((t["unitsB"] * pipB) / lastPxB if lastPxB > 0 else t["unitsB"] * pipB)

                pnlA = pipsA * pipValA
                pnlB = pipsB * pipValB

                nominalA = t["unitsA"] * lastPxA if quoteA == "USD" else t["unitsA"]
                nominalB = t["unitsB"] if quoteB != "USD" else t["unitsB"] * lastPxB
                costs = (nominalA + nominalB) * (costRate * 2.0)

                tradePnl = pnlA + pnlB - costs
                tradeNetRet = (tradePnl / t["margin"]) if t["margin"] > 0 else 0.0
                open_pnl += tradePnl

                open_cycle_trades.append({
                    "tradeNum": raw_trades_count,
                    "isSubtotal": False,
                    "isOpen": True,
                    "cycleNum": cycle_counter + 1,
                    "signalType": f"{t['signalType']} (EN CURSO)",
                    "direction": t["direction"],
                    "entryDate": t["entryDate"],
                    "exitDate": "EN CURSO",
                    "allocatedCapital": round(float(t["margin"]), 2),
                    "multA": t["multA"],
                    "multB": t["multB"],
                    "unitsA": int(t["unitsA"]),
                    "unitsB": int(t["unitsB"]),
                    "pipsA": round(float(pipsA), 1),
                    "pipsB": round(float(pipsB), 1),
                    "pnlA": round(float(pnlA), 2),
                    "pnlB": round(float(pnlB), 2),
                    "entryPriceA": round(float(t["entryPxA"]), 5),
                    "exitPriceA": round(float(lastPxA), 5),
                    "entryPriceB": round(float(t["entryPxB"]), 5),
                    "exitPriceB": round(float(lastPxB), 5),
                    "durationBars": (n - 1) - t["entryIndex"],
                    "returnPct": round(float(tradeNetRet * 100.0), 2),
                    "pnl": round(float(tradePnl), 2), # PnL actual flotante
                    "exitReason": "POSICION_ABIERTA_EN_CURSO",
                    "isWin": bool(tradePnl > 0)
                })

            for ot in open_cycle_trades:
                finished_trades.append(ot)

            openEntrySpan = f"{open_cycle_trades[0]['entryDate']} a {open_cycle_trades[-1]['entryDate']}" if len(open_cycle_trades) > 1 else open_cycle_trades[0]['entryDate']
            finished_trades.append({
                "tradeNum": None,
                "isSubtotal": True,
                "isOpen": True,
                "cycleNum": cycle_counter + 1,
                "signalType": "SUBTOTAL POSICIONES ABIERTAS",
                "direction": f"● En Curso ({len(open_cycle_trades)} ops activas)",
                "entryDate": f"Entradas: {openEntrySpan}",
                "exitDate": "EN CURSO",
                "allocatedCapital": round(float(open_margin), 2),
                "multA": None,
                "multB": None,
                "unitsA": int(open_units_a),
                "unitsB": int(open_units_b),
                "pipsA": round(float(sum(ot["pipsA"] for ot in open_cycle_trades)), 1),
                "pipsB": round(float(sum(ot["pipsB"] for ot in open_cycle_trades)), 1),
                "pnlA": round(float(sum(ot["pnlA"] for ot in open_cycle_trades)), 2),
                "pnlB": round(float(sum(ot["pnlB"] for ot in open_cycle_trades)), 2),
                "entryPriceA": None,
                "exitPriceA": None,
                "entryPriceB": None,
                "exitPriceB": None,
                "durationBars": len(open_cycle_trades),
                "returnPct": round(float((open_pnl / open_margin) * 100.0), 2) if open_margin > 0 else 0.0,
                "pnl": round(float(open_pnl), 2),
                "exitReason": "FLOTANTE_ACTUAL",
                "isWin": bool(open_pnl > 0)
            })

        # Métricas consolidadas'''

code = code.replace(target_open_block, replacement_open_block)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated quant_pair_engine.py with open / floating positions!")
