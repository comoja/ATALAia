quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_trade_append = '''                        "unitsB": int(t["unitsB"]),
                        "pipsA": round(float(pipsA), 1),
                        "pipsB": round(float(pipsB), 1),
                        "entryPriceA": round(float(t["entryPxA"]), 5),
                        "exitPriceA": round(float(pxA), 5),
                        "entryPriceB": round(float(t["entryPxB"]), 5),
                        "exitPriceB": round(float(pxB), 5),
                        "durationBars": i - t["entryIndex"],
                        "returnPct": round(float(tradeNetRet * 100.0), 2),
                        "pnl": round(float(tradePnl), 2), # PnL Real en USD calculado con pips
                        "exitReason": "CRUCE_MEDIA_CIRCULO",
                        "isWin": bool(tradePnl > 0)
                    })'''

new_trade_append = '''                        "unitsB": int(t["unitsB"]),
                        "pipsA": round(float(pipsA), 1),
                        "pipsB": round(float(pipsB), 1),
                        "pnlA": round(float(pnlA), 2),
                        "pnlB": round(float(pnlB), 2),
                        "entryPriceA": round(float(t["entryPxA"]), 5),
                        "exitPriceA": round(float(pxA), 5),
                        "entryPriceB": round(float(t["entryPxB"]), 5),
                        "exitPriceB": round(float(pxB), 5),
                        "durationBars": i - t["entryIndex"],
                        "returnPct": round(float(tradeNetRet * 100.0), 2),
                        "pnl": round(float(tradePnl), 2), # PnL Real Total en USD
                        "exitReason": "CRUCE_MEDIA_CIRCULO",
                        "isWin": bool(tradePnl > 0)
                    })'''

code = code.replace(target_trade_append, new_trade_append)

target_subtotal_append = '''                # Fila de Subtotal de Cierre
                entrySpan = f"{cycle_trades[0]['entryDate'][:10]} a {cycle_trades[-1]['entryDate'][:10]}" if len(cycle_trades) > 1 else cycle_trades[0]['entryDate'][:10]
                finished_trades.append({
                    "tradeNum": None,
                    "isSubtotal": True,
                    "cycleNum": cycle_counter,
                    "signalType": f"SUBTOTAL CIERRE #{cycle_counter}",
                    "direction": f"● Salida Media ({len(cycle_trades)} ops)",
                    "entryDate": f"Entradas: {entrySpan}",
                    "exitDate": d,
                    "allocatedCapital": round(float(cycle_margin), 2),
                    "multA": None,
                    "multB": None,
                    "unitsA": int(cycle_units_a),
                    "unitsB": int(cycle_units_b),
                    "entryPriceA": None,
                    "exitPriceA": None,
                    "entryPriceB": None,
                    "exitPriceB": None,
                    "durationBars": len(cycle_trades),
                    "returnPct": cycleAvgRet,
                    "pnl": round(float(cycle_pnl), 2),
                    "exitReason": f"CIERRE EN MEDIA (●) {d}",
                    "isWin": bool(cycle_pnl > 0)
                })'''

new_subtotal_append = '''                # Fila de Subtotal de Cierre con PnL separado por par
                entrySpan = f"{cycle_trades[0]['entryDate'][:10]} a {cycle_trades[-1]['entryDate'][:10]}" if len(cycle_trades) > 1 else cycle_trades[0]['entryDate'][:10]
                finished_trades.append({
                    "tradeNum": None,
                    "isSubtotal": True,
                    "cycleNum": cycle_counter,
                    "signalType": f"SUBTOTAL CIERRE #{cycle_counter}",
                    "direction": f"● Salida Media ({len(cycle_trades)} ops)",
                    "entryDate": f"Entradas: {entrySpan}",
                    "exitDate": d,
                    "allocatedCapital": round(float(cycle_margin), 2),
                    "multA": None,
                    "multB": None,
                    "unitsA": int(cycle_units_a),
                    "unitsB": int(cycle_units_b),
                    "pipsA": round(float(sum(ct["pipsA"] for ct in cycle_trades)), 1),
                    "pipsB": round(float(sum(ct["pipsB"] for ct in cycle_trades)), 1),
                    "pnlA": round(float(sum(ct["pnlA"] for ct in cycle_trades)), 2),
                    "pnlB": round(float(sum(ct["pnlB"] for ct in cycle_trades)), 2),
                    "entryPriceA": None,
                    "exitPriceA": None,
                    "entryPriceB": None,
                    "exitPriceB": None,
                    "durationBars": len(cycle_trades),
                    "returnPct": cycleAvgRet,
                    "pnl": round(float(cycle_pnl), 2),
                    "exitReason": f"CIERRE EN MEDIA (●) {d}",
                    "isWin": bool(cycle_pnl > 0)
                })'''

code = code.replace(target_subtotal_append, new_subtotal_append)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Updated quant_pair_engine.py with pnlA and pnlB for individual trades and subtotals!")
