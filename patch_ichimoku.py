import re

file_path = '/Volumes/TimeMachine/ATALAia/Sentinel/core/Ichimoku.py'
with open(file_path, 'r') as f:
    content = f.read()

# Add stratConfig to _analyze_timeframe signature
pattern1 = r"(def _analyze_timeframe\(\s*self,\s*symbol: str,\s*df: pd\.DataFrame,\s*interval_used: str,\s*intervaloAnterior: str,\s*preloadedData: Optional\[Dict\],\s*symbolInfo: Dict,\s*htfTrend: str,\s*tenkanPeriod: int = 9,\s*kijunPeriod: int = 26,\s*senkouPeriod: int = 52,\s*displacement: int = 26,\s*minRrVal: float = 1\.5,)"
replacement1 = r"\1\n        stratConfig: Dict = None,"
content = re.sub(pattern1, replacement1, content)

# Find where it uses df["impulseMacd"] and replace with actual calculation
pattern2 = r"macdHist_val = df\[\"impulseMacd\"\].iloc\[-1\] if \"impulseMacd\" in df.columns else 0\.0\s*macdhist_anterior = df\[\"impulseMacd\"\].iloc\[-2\] if \"impulseMacd\" in df.columns else 0\.0"
replacement2 = """if stratConfig is None: stratConfig = {}
        useImpulseMacdFilter = bool(int(stratConfig.get('useImpulseMacdFilter', 1)))
        macdSlow = int(stratConfig.get('macdSlow', 34))
        macdSignal = int(stratConfig.get('macdSignal', 9))
        
        if useImpulseMacdFilter:
            impulse_macd, _ = technical.calculateImpulseMacd(df, lengthMa=macdSlow, lengthSignal=macdSignal)
            macdHist_val = float(impulse_macd.iloc[-1])
            macdhist_anterior = float(impulse_macd.iloc[-2])
        else:
            macdHist_val = df["impulseMacd"].iloc[-1] if "impulseMacd" in df.columns else 0.0
            macdhist_anterior = df["impulseMacd"].iloc[-2] if "impulseMacd" in df.columns else 0.0"""
content = re.sub(pattern2, replacement2, content)

# Add stratConfig=stratConfig to _analyze_timeframe calls in runAnalysisCycleForSymbol
pattern3 = r"(signal = self\._analyze_timeframe\(\s*symbol=symbol,\s*df=df,\s*interval_used=interval,\s*intervaloAnterior=intervalo_anterior,\s*preloadedData=preloadedData,\s*symbolInfo=symbolInfo,\s*htfTrend=htf_trend,\s*tenkanPeriod=tenkanPeriod,\s*kijunPeriod=kijunPeriod,\s*senkouPeriod=senkouPeriod,\s*displacement=displacement,\s*minRrVal=minRrVal\s*\))"
replacement3 = r"\1[:-1] + \", stratConfig=stratConfig)\"" # This uses python to fix the string but we are in regex
content = re.sub(r"minRrVal=minRrVal\n\s*\)", "minRrVal=minRrVal,\n                stratConfig=stratConfig\n            )", content)

with open(file_path, 'w') as f:
    f.write(content)
print("Patch applied to Ichimoku.py")
