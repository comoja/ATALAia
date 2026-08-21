import os
import re

base_dir = '/home/jcolinm/datos_mysql/DATOS/Sistema/ATALAia/Sentinel/backtesting'

def fix_header_and_paths(code: str) -> str:
    # Remove old hardcoded path and dbConnection imports
    code = re.sub(r'sys\.path\.append\([\'"]/Volumes/TimeMachine/ATALAia[\'"]\)', '', code)
    code = re.sub(r'from middleware\.database import dbConnection(, dbManager)?', '', code)
    
    # Replace /Volumes/TimeMachine/ATALAia/Sentinel/backtesting/ with opt_db_helper.getOutputPath(...)
    # Replace hardcoded CSV paths
    code = re.sub(r'[\'"]/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/([^\'"]+)[\'"]', r'opt_db_helper.getOutputPath("\1")', code)
    
    return code

print("Helper function defined.")
