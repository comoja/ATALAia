import sys
sys.path.append('/Volumes/TimeMachine/ATALAia')
from middleware.database import dbManager

def check_symbols():
    symbols = dbManager.getSymbols()
    print("Active Symbols:")
    for s in symbols:
        print(s)

if __name__ == "__main__":
    check_symbols()
