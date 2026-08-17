import re

file_path = '/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_weekly_backtest_compounding_v6.py'
with open(file_path, 'r') as f:
    content = f.read()

# Encontrar lineas con useMacdFilter = ... y agregar extraccion de macdSlow y macdSignal
pattern = r"(useMacdFilter = bool\(int\(stratConfig\.get\('useImpulseMacdFilter', 0\)\)\))"
replacement = r"\1\n            macdSlow = int(stratConfig.get('macdSlow', 34))\n            macdSignal = int(stratConfig.get('macdSignal', 9))"
content = re.sub(pattern, replacement, content)

# Encontrar detect_fvgs y agregar macd_slow=macdSlow, macd_signal=macdSignal
pattern2 = r"(_tech\.detect_fvgs\(.*?use_impulse_macd_filter=useMacdFilter)\)"
replacement2 = r"\1, macd_slow=macdSlow, macd_signal=macdSignal)"
content = re.sub(pattern2, replacement2, content)

with open(file_path, 'w') as f:
    f.write(content)
print("Patch applied to run_weekly_backtest_compounding_v6.py")
