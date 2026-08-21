import os
import re

base_dir = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/Sentinel/backtesting'

def update_cruceema():
    path = os.path.join(base_dir, 'run_cruceema_optimization.py')
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()

    # Standardize header
    content = re.sub(
        r'sys\.path\.append\(["\']/Volumes/TimeMachine/ATALAia["\']\)\s*from middleware\.database import dbConnection\s*from Sentinel\.ml import model as mlModel\s*from middleware\.config import constants as config[\s\S]*?def loadCandles\(symbol: str, startDate: str, endDate: str\) -> pd\.DataFrame:[\s\S]*?return pd\.DataFrame\(\)',
        '''sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from Sentinel.ml import model as mlModel
from middleware.config import constants as config
from Sentinel.backtesting import opt_db_helper

ALL_SYMBOLS = opt_db_helper.getActiveSentinelSymbols()
PIP_MULTIPLIERS = opt_db_helper.PIP_MULTIPLIERS
SPREADS = opt_db_helper.SPREADS
loadCandles = opt_db_helper.loadCandles''',
        content
    )

    # Replace DB save block
    old_save_pattern = r'if symbolBestCombo:\s*bestResults\.append\(symbolBestCombo\)[\s\S]*?print\(f"\s*❌ No se encontró ninguna combinación rentable para \{symbol\}\."\)'
    new_save_block = '''if symbolBestCombo:
            bestResults.append(symbolBestCombo)
            params = {
                "emaFast": symbolBestCombo['EMA Fast'],
                "emaSlow": symbolBestCombo['EMA Slow'],
                "minRr": symbolBestCombo['Min RR']
            }
            imacd_params = {
                "useImpulseMacdFilter": 1,
                "macdFast": 12,
                "macdSlow": symbolBestCombo['IMACD Slow'],
                "macdSignal": symbolBestCombo['IMACD Signal']
            }
            opt_db_helper.saveSymbolStrategyConfig('CruceEMA', symbol, True, params, imacd_params)
            print(f"✅ DB: Guardado {symbol} (TRUE)")
            print(f"  🏆 Mejor combo para {symbol}: Fast={symbolBestCombo['EMA Fast']} | Slow={symbolBestCombo['EMA Slow']} | IMACD={symbolBestCombo['IMACD Slow']}/{symbolBestCombo['IMACD Signal']} | RR={symbolBestCombo['Min RR']} | Conf={symbolBestCombo['Min Conf']}% | Trades={symbolBestCombo['Trades']} | WR={symbolBestCombo['Win Rate']} | PF={symbolBestCombo['Profit Factor']} | PnL=${symbolBestCombo['PnL USD']:.2f}")
        else:
            fallbackParams = {"emaFast": 9, "emaSlow": 21, "minRr": 1.5}
            fallbackImacd = {"useImpulseMacdFilter": 1, "macdFast": 12, "macdSlow": 26, "macdSignal": 9}
            opt_db_helper.saveSymbolStrategyConfig('CruceEMA', symbol, False, fallbackParams, fallbackImacd)
            print(f"  ❌ No se encontró ninguna combinación rentable para {symbol}. Guardado en DB (FALSE).")'''

    content = re.sub(old_save_pattern, new_save_block, content)
    content = content.replace('/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/', base_dir + '/')
    
    with open(path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("Updated run_cruceema_optimization.py")

update_cruceema()
