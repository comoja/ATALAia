equity = 300.0
cycle_available = equity
allocationRate = 0.03

trades = []
for i in range(1, 4):
    entryBudget = cycle_available * allocationRate
    budgetA = entryBudget / 2.0
    budgetB = entryBudget / 2.0
    
    margenA = 2.50
    margenB = 10.00
    totalMargen = margenA + margenB
    
    cycle_available -= totalMargen
    trades.append({
        "trade": i,
        "baseCapital": equity if i == 1 else trades[-1]["availableCapital"],
        "budget3pct": entryBudget,
        "margin": totalMargen,
        "availableCapital": cycle_available
    })

for t in trades:
    print(t)
