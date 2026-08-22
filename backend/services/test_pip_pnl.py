# Pip value and PnL calculation

def calc_pnl(entryA, exitA, dirA, unitsA, pipA, quoteA,
             entryB, exitB, dirB, unitsB, pipB, quoteB,
             commissionBps=2.0, slippageBps=1.0):
    
    # Par A
    deltaA = (exitA - entryA) if dirA == "BUY" else (entryA - exitA)
    pipsA = deltaA / pipA if pipA > 0 else 0.0
    if quoteA == "USD":
        pipValA = unitsA * pipA
    else:
        pipValA = (unitsA * pipA) / exitA if exitA > 0 else (unitsA * pipA)
    pnlA = pipsA * pipValA
    
    # Par B
    deltaB = (exitB - entryB) if dirB == "BUY" else (entryB - exitB)
    pipsB = deltaB / pipB if pipB > 0 else 0.0
    if quoteB == "USD":
        pipValB = unitsB * pipB
    else:
        pipValB = (unitsB * pipB) / exitB if exitB > 0 else (unitsB * pipB)
    pnlB = pipsB * pipValB
    
    # Costos
    nominalA = unitsA * exitA if quoteA == "USD" else unitsA
    nominalB = unitsB if quoteB != "USD" else unitsB * exitB
    costs = (nominalA + nominalB) * ((commissionBps + slippageBps) / 10000.0)
    
    netPnl = pnlA + pnlB - costs
    return {
        "pipsA": round(pipsA, 1),
        "pipValA": round(pipValA, 4),
        "pnlA": round(pnlA, 2),
        "pipsB": round(pipsB, 1),
        "pipValB": round(pipValB, 4),
        "pnlB": round(pnlB, 2),
        "costs": round(costs, 2),
        "netPnl": round(netPnl, 2)
    }

res = calc_pnl(
    entryA=1.17566, exitA=1.16861, dirA="SELL", unitsA=1000.0, pipA=0.00010, quoteA="USD",
    entryB=17.96648, exitB=17.48500, dirB="BUY", unitsB=1000.0, pipB=0.01000, quoteB="MXN"
)
print("Result with Pip Calculation:")
for k, v in res.items():
    print(f"  {k}: {v}")
