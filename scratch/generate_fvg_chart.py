import sys
import os
import pandas as pd
import numpy as np

sys.path.append("/Volumes/TimeMachine/ATALAia")
from Sentinel.analysis.fvg_analyzer import FvgAnalyzer

def generate_chart():
    # 1. Generate same mock data
    dates = pd.date_range(start="2026-05-20 09:00:00", periods=30, freq="15min")
    data = {
        'open':  [100.0] * 30,
        'high':  [101.0] * 30,
        'low':   [99.0] * 30,
        'close': [100.0] * 30,
        'volume': [1000] * 30
    }
    df = pd.DataFrame(data, index=dates)
    
    for idx in range(30):
        df.iloc[idx, df.columns.get_loc('open')]  = 100.0 + idx * 0.5
        df.iloc[idx, df.columns.get_loc('close')] = 100.5 + idx * 0.5
        df.iloc[idx, df.columns.get_loc('high')]  = 101.0 + idx * 0.5
        df.iloc[idx, df.columns.get_loc('low')]   = 99.8 + idx * 0.5

    # FVG 1
    df.iloc[15, df.columns.get_loc('open')]  = 108.0
    df.iloc[15, df.columns.get_loc('close')] = 108.5
    df.iloc[15, df.columns.get_loc('high')]  = 109.0
    df.iloc[15, df.columns.get_loc('low')]   = 107.5
    
    df.iloc[16, df.columns.get_loc('open')]  = 108.6
    df.iloc[16, df.columns.get_loc('close')] = 112.0
    df.iloc[16, df.columns.get_loc('high')]  = 112.5
    df.iloc[16, df.columns.get_loc('low')]   = 108.5
    
    df.iloc[17, df.columns.get_loc('open')]  = 110.2
    df.iloc[17, df.columns.get_loc('close')] = 110.75
    df.iloc[17, df.columns.get_loc('high')]  = 111.0
    df.iloc[17, df.columns.get_loc('low')]   = 110.0
    
    # FVG 2 (Breakaway)
    df.iloc[18, df.columns.get_loc('open')]  = 111.0
    df.iloc[18, df.columns.get_loc('close')] = 111.5
    df.iloc[18, df.columns.get_loc('high')]  = 112.0
    df.iloc[18, df.columns.get_loc('low')]   = 109.5
    
    df.iloc[19, df.columns.get_loc('open')]  = 111.6
    df.iloc[19, df.columns.get_loc('close')] = 116.0
    df.iloc[19, df.columns.get_loc('high')]  = 116.5
    df.iloc[19, df.columns.get_loc('low')]   = 111.5
    
    df.iloc[20, df.columns.get_loc('open')]  = 113.5
    df.iloc[20, df.columns.get_loc('close')] = 118.5
    df.iloc[20, df.columns.get_loc('high')]  = 119.0
    df.iloc[20, df.columns.get_loc('low')]   = 113.0
    
    for idx in range(21, 30):
        df.iloc[idx, df.columns.get_loc('open')]  = 118.0
        df.iloc[idx, df.columns.get_loc('close')] = 118.5
        df.iloc[idx, df.columns.get_loc('high')]  = 119.0
        df.iloc[idx, df.columns.get_loc('low')]   = 117.5

    # 2. Analyze
    analyzer = FvgAnalyzer(minGapPct=0.0001)
    rawFvgs = analyzer.detectFvg(df)
    filteredFvgs = analyzer.applyFilters(df, rawFvgs, applyHighProbFilters=False, validateMitigation=False)
    
    # 3. Visualize
    outputPath = "/Users/jclnmrls/.gemini/antigravity-ide/brain/1c71ec93-7f6d-4c87-8276-c5be489df3df/fvg_visualization.png"
    analyzer.visualizeZones(df, filteredFvgs, outputPath)
    print(f"✅ Gráfico FVG generado con éxito en: {outputPath}")

if __name__ == '__main__':
    generate_chart()
