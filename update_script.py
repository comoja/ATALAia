import re

file_path = "/Volumes/TimeMachine/ATALAia/Sentinel/backtesting/run_weekly_backtest_compounding_v6.py"
with open(file_path, 'r') as f:
    content = f.read()

# For SilverBullet (line 188 approx)
content = re.sub(
    r"(\s*)raw_fvgs = _tech\.detect_fvgs\(df5m, apply_high_prob_filters=True\)",
    r"\1stratConfig = dbManager.getSymbolStrategyConfig(strategy, symbol) or {}\n\1useMacdFilter = bool(int(stratConfig.get('useImpulseMacdFilter', 0)))\n\1raw_fvgs = _tech.detect_fvgs(df5m, apply_high_prob_filters=True, use_impulse_macd_filter=useMacdFilter)",
    content,
    count=1
)

# For Patron4h (line 242)
content = re.sub(
    r"(\s*)fvgs = _tech\.detect_fvgs\(df4h, apply_high_prob_filters=True\)",
    r"\1stratConfig = dbManager.getSymbolStrategyConfig(strategy, symbol) or {}\n\1useMacdFilter = bool(int(stratConfig.get('useImpulseMacdFilter', 0)))\n\1fvgs = _tech.detect_fvgs(df4h, apply_high_prob_filters=True, use_impulse_macd_filter=useMacdFilter)",
    content,
    count=1
)

# For SesgoBiasHTF (line 299)
content = re.sub(
    r"(\s*)raw_fvgs = _tech\.detect_fvgs\(df15m, apply_high_prob_filters=True\)",
    r"\1stratConfig = dbManager.getSymbolStrategyConfig(strategy, symbol) or {}\n\1useMacdFilter = bool(int(stratConfig.get('useImpulseMacdFilter', 0)))\n\1raw_fvgs = _tech.detect_fvgs(df15m, apply_high_prob_filters=True, use_impulse_macd_filter=useMacdFilter)",
    content,
    count=1
)

# For GenericFVG (line 337)
content = re.sub(
    r"(\s*)fvgs = _tech\.detect_fvgs\(df15m, apply_high_prob_filters=True\)",
    r"\1stratConfig = dbManager.getSymbolStrategyConfig(strategy, symbol) or {}\n\1useMacdFilter = bool(int(stratConfig.get('useImpulseMacdFilter', 0)))\n\1fvgs = _tech.detect_fvgs(df15m, apply_high_prob_filters=True, use_impulse_macd_filter=useMacdFilter)",
    content,
    count=1
)

# For ImbalanceNY/LDN loop (line 394)
content = re.sub(
    r"(\s*)raw_fvgs = _tech\.detect_fvgs\(df5m, apply_high_prob_filters=True\)",
    r"\1stratConfig = dbManager.getSymbolStrategyConfig(strategy, symbol) or {}\n\1useMacdFilter = bool(int(stratConfig.get('useImpulseMacdFilter', 0)))\n\1raw_fvgs = _tech.detect_fvgs(df5m, apply_high_prob_filters=True, use_impulse_macd_filter=useMacdFilter)",
    content,
    count=1
)

# For ImbalancePMNY (line 440)
content = re.sub(
    r"(\s*)raw_fvgs_pmny = _tech\.detect_fvgs\(df15m, apply_high_prob_filters=True\)",
    r"\1stratConfig = dbManager.getSymbolStrategyConfig(strategy, symbol) or {}\n\1useMacdFilter = bool(int(stratConfig.get('useImpulseMacdFilter', 0)))\n\1raw_fvgs_pmny = _tech.detect_fvgs(df15m, apply_high_prob_filters=True, use_impulse_macd_filter=useMacdFilter)",
    content,
    count=1
)

with open(file_path, 'w') as f:
    f.write(content)

print("Done")
