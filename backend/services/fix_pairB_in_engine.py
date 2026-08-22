quant_path = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/backend/services/quant_pair_engine.py'
with open(quant_path, 'r', encoding='utf-8') as f:
    code = f.read()

target_def = '''    def runSignalBacktest(
        self,
        dfA: pd.DataFrame,
        dfB: pd.DataFrame,
        smaPeriod: int = 3,
        sigmaWindow: int = 30,
        includeBoxes: bool = False,
        initialCapital: float = 10000.0,
        allocationPct: float = 3.0,  # 3% del capital de la cuenta por entrada
        minLotsA: float = 1000.0,    # symbols.min_lots de BD para Par A
        minLotsB: float = 1000.0,    # symbols.min_lots de BD para Par B
        margenPctA: float = 0.25,    # symbols.margen (%) de BD para Par A
        margenPctB: float = 1.0,     # symbols.margen (%) de BD para Par B
        commissionBps: float = 2.0,
        slippageBps: float = 1.0
    ) -> Dict[str, Any]:'''

new_def = '''    def runSignalBacktest(
        self,
        dfA: pd.DataFrame,
        dfB: pd.DataFrame,
        pairA: str = "",
        pairB: str = "",
        smaPeriod: int = 3,
        sigmaWindow: int = 30,
        includeBoxes: bool = False,
        initialCapital: float = 10000.0,
        allocationPct: float = 3.0,  # 3% del capital de la cuenta por entrada
        minLotsA: float = 1000.0,    # symbols.min_lots de BD para Par A
        minLotsB: float = 1000.0,    # symbols.min_lots de BD para Par B
        margenPctA: float = 0.25,    # symbols.margen (%) de BD para Par A
        margenPctB: float = 1.0,     # symbols.margen (%) de BD para Par B
        commissionBps: float = 2.0,
        slippageBps: float = 1.0
    ) -> Dict[str, Any]:'''

code = code.replace(target_def, new_def)

with open(quant_path, 'w', encoding='utf-8') as f:
    f.write(code)

print("Fixed pairA and pairB in runSignalBacktest signature!")
