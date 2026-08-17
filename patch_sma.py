import re

file_path = '/Volumes/TimeMachine/ATALAia/Sentinel/core/SMA20_200.py'
with open(file_path, 'r') as f:
    content = f.read()

# Modify getSMAFeatures signature
pattern_getSMA = r"(def getSMAFeatures\(self, symbol: str, df: pd\.DataFrame, start_idx: int\))"
replacement_getSMA = r"def getSMAFeatures(self, symbol: str, df: pd.DataFrame, start_idx: int, macdFast: int=12, macdSlow: int=26, macdSignal: int=9)"
content = re.sub(pattern_getSMA, replacement_getSMA, content)

# Modify getSMAFeatures MACD calculation
pattern_macdf1 = r"macd, macd_signal, macdHist = ta\.MACD\(df\['close'\], fastperiod=12, slowperiod=26, signalperiod=9\)"
replacement_macdf1 = r"macd, macd_signal, macdHist = ta.MACD(df['close'], fastperiod=macdFast, slowperiod=macdSlow, signalperiod=macdSignal)"
content = re.sub(pattern_macdf1, replacement_macdf1, content)

# Modify prepareDf MACD calculation
pattern_macdf2 = r"macd_vals = ta\.MACD\(dfInput\[\"close\"\]\.values, fastperiod=12, slowperiod=26, signalperiod=9\)"
replacement_macdf2 = r"macd_vals = ta.MACD(dfInput[\"close\"].values, fastperiod=macdFast, slowperiod=macdSlow, signalperiod=macdSignal)"
content = re.sub(pattern_macdf2, replacement_macdf2, content)

# Add stratConfig logic in _get_signal
pattern_sig = r"(stratConfig = dbManager\.getSymbolStrategyConfig\(\"SMA20_200\", symbol\) or \{\}\n\s*slopeThreshold =)"
replacement_sig = r"""stratConfig = dbManager.getSymbolStrategyConfig("SMA20_200", symbol) or {}
        macdFast = int(stratConfig.get('macdFast', 12))
        macdSlow = int(stratConfig.get('macdSlow', 26))
        macdSignal = int(stratConfig.get('macdSignal', 9))
        slopeThreshold ="""
content = re.sub(pattern_sig, replacement_sig, content)

# Pass them into prepareDf inside _get_signal
# We need to change `def prepareDf(dfInput: pd.DataFrame) -> Optional[pd.DataFrame]:` to accept params
pattern_prep_sig = r"def prepareDf\(dfInput: pd\.DataFrame\) -> Optional\[pd\.DataFrame\]:"
replacement_prep_sig = r"def prepareDf(dfInput: pd.DataFrame, macdFast: int=12, macdSlow: int=26, macdSignal: int=9) -> Optional[pd.DataFrame]:"
content = re.sub(pattern_prep_sig, replacement_prep_sig, content)

# Change call to prepareDf
pattern_call_prep = r"df = prepareDf\(rawDf\)"
replacement_call_prep = r"df = prepareDf(rawDf, macdFast, macdSlow, macdSignal)"
content = re.sub(pattern_call_prep, replacement_call_prep, content)

# Modify evaluate_signal signature
pattern_eval_sig = r"async def evaluate_signal\(self, symbol: str, df: pd\.DataFrame, start_idx: int, expected_return: float\) -> Tuple\[bool, float, float\]:"
replacement_eval_sig = r"async def evaluate_signal(self, symbol: str, df: pd.DataFrame, start_idx: int, expected_return: float, macdFast: int=12, macdSlow: int=26, macdSignal: int=9) -> Tuple[bool, float, float]:"
content = re.sub(pattern_eval_sig, replacement_eval_sig, content)

# Change call to getSMAFeatures inside evaluate_signal
pattern_call_getsma = r"features = self\.getSMAFeatures\(symbol, df, start_idx\)"
replacement_call_getsma = r"features = self.getSMAFeatures(symbol, df, start_idx, macdFast, macdSlow, macdSignal)"
content = re.sub(pattern_call_getsma, replacement_call_getsma, content)

# Change call to evaluate_signal inside _get_signal
pattern_call_eval = r"es_valida, prob, _ = await self\.evaluate_signal\(symbol, df, i, minUsdProfit\)"
replacement_call_eval = r"es_valida, prob, _ = await self.evaluate_signal(symbol, df, i, minUsdProfit, macdFast, macdSlow, macdSignal)"
content = re.sub(pattern_call_eval, replacement_call_eval, content)

with open(file_path, 'w') as f:
    f.write(content)

print("SMA patched!")
