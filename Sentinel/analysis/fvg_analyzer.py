"""
Module for FvgAnalyzer class, implementing Fair Value Gaps detection and filtering.
"""
import logging
import pandas as pd
import numpy as np
import talib as ta
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from datetime import datetime

logger = logging.getLogger("sentinel")

class FvgAnalyzer:
    """
    Class to analyze, detect, filter and visualize Fair Value Gaps (FVG)
    in candlestick data (OHLC).
    """
    def __init__(self, emaPeriod=200, atrPeriod=14, minGapPct=0.0001):
        """
        Initializes the FvgAnalyzer.
        
        Args:
            emaPeriod: Period for the EMA trend filter (default 200).
            atrPeriod: Period for the ATR volatility calculation (default 14).
            minGapPct: Minimum gap size relative to price (default 0.0001).
        """
        self.emaPeriod = emaPeriod
        self.atrPeriod = atrPeriod
        self.minGapPct = minGapPct

    def detectFvg(self, df):
        """
        Detects base Fair Value Gaps (FVG) in the DataFrame.
        BISI (Bullish FVG): Low[i] > High[i-2]
        SIBI (Bearish FVG): High[i] < Low[i-2]
        
        Args:
            df: pd.DataFrame with OHLC columns.
            
        Returns:
            list: List of dictionaries representing the detected FVGs.
        """
        fvgs = []
        if df is None or len(df) < 3:
            return fvgs

        highs = df['high'].values.astype(float)
        lows = df['low'].values.astype(float)
        closes = df['close'].values.astype(float)
        opens = df['open'].values.astype(float)
        times = df.index

        for i in range(2, len(df)):
            v1High = highs[i-2]
            v1Low = lows[i-2]
            v2Open = opens[i-1]
            v2Close = closes[i-1]
            v2High = highs[i-1]
            v2Low = lows[i-1]
            v3High = highs[i]
            v3Low = lows[i]
            v3Open = opens[i]
            v3Close = closes[i]

            # Validate body of Vela 2 (displacement must be > 50%)
            v2Range = v2High - v2Low
            if v2Range == 0:
                continue
            v2Body = abs(v2Close - v2Open)
            v2BodyPct = v2Body / v2Range

            if v2BodyPct < 0.50:
                continue

            closeVal = closes[i]
            isBullish = False
            isBearish = False
            gapSize = 0.0

            # 1. Base detection logic
            if v1High < v3Low and v2Close > v2Open:
                isBullish = True
                gapSize = v3Low - v1High
            elif v1Low > v3High and v2Close < v2Open:
                isBearish = True
                gapSize = v1Low - v3High

            if not (isBullish or isBearish) or (gapSize / closeVal < self.minGapPct):
                continue

            # Format timestamp to standard string
            fvgTime = times[i]
            if hasattr(fvgTime, 'strftime'):
                fvgTimeStr = fvgTime.strftime("%Y-%m-%d %H:%M:%S")
            else:
                fvgTimeStr = str(fvgTime)[:19]

            fvgObj = {
                'type': 'Bullish_FVG' if isBullish else 'Bearish_FVG',
                'top': float(v3Low) if isBullish else float(v1Low),
                'bottom': float(v1High) if isBullish else float(v3High),
                'gap_low': float(v1High) if isBullish else float(v3High),
                'gap_high': float(v3Low) if isBullish else float(v1Low),
                'gapLow': float(v1High) if isBullish else float(v3High),
                'gapHigh': float(v3Low) if isBullish else float(v1Low),
                'mid': float((v1High + v3Low) / 2.0),
                'size': float(gapSize),
                'v1_low': float(v1Low),
                'v1_high': float(v1High),
                'timestamp': fvgTimeStr,
                'idx': i,
                'classification': 'CONTINUATION',
                'highProbability': False,
                'breakawayGap': False,
                'rejectionLowProbability': False,
                'mitigated': False
            }
            fvgs.append(fvgObj)

        return fvgs

    def applyFilters(self, df, fvgs, applyHighProbFilters=False, validateMitigation=True):
        """
        Applies high probability and mitigation filters to detected FVGs.
        
        Args:
            df: pd.DataFrame with OHLC data.
            fvgs: list of detected FVGs from detectFvg.
            applyHighProbFilters: If True, applies EMA 200 and MSS filters.
            validateMitigation: If True, filters out traditionally mitigated FVGs.
            
        Returns:
            list: List of filtered FVG dictionaries.
        """
        if not fvgs or df is None or len(df) < 3:
            return []

        # 1. Calculate indicators handling initial null values
        closes = df['close'].values.astype(float)
        highs = df['high'].values.astype(float)
        lows = df['low'].values.astype(float)
        opens = df['open'].values.astype(float)

        # Use ewm with min_periods=1 to calculate values immediately
        ema200Series = df['close'].ewm(span=self.emaPeriod, adjust=False, min_periods=1).mean()
        
        # Calculate ATR 14 and ffill/bfill to handle nulls
        atr14Series = ta.ATR(highs, lows, closes, timeperiod=self.atrPeriod)
        atr14Series = pd.Series(atr14Series).ffill().bfill().values

        fvgsFiltered = []

        for fvg in fvgs:
            i = fvg['idx']
            v1High = fvg['v1_high']
            v1Low = fvg['v1_low']
            
            v2High = highs[i-1]
            v2Low = lows[i-1]
            v2Close = closes[i-1]
            v2Open = opens[i-1]
            
            v3High = highs[i]
            v3Low = lows[i]
            v3Open = opens[i]
            v3Close = closes[i]

            isBullish = fvg['type'] == 'Bullish_FVG'
            isBearish = fvg['type'] == 'Bearish_FVG'
            closeVal = closes[i]
            ema200Val = float(ema200Series.iloc[i])

            # A. Trend Filter (EMA 200)
            if applyHighProbFilters:
                if isBullish and closeVal <= ema200Val:
                    continue
                if isBearish and closeVal >= ema200Val:
                    continue

            # B. Market Structure Shift (MSS) local of Vela 2 (Vela 2 breaks local high/low)
            if applyHighProbFilters and i >= 7:
                if isBullish:
                    localMax = float(np.max(highs[i-7:i-2]))
                    if v2High <= localMax:
                        continue
                else:
                    localMin = float(np.min(lows[i-7:i-2]))
                    if v2Low >= localMin:
                        continue

            # C. Advanced classification of Vela 3
            rangoVela3 = v3High - v3Low
            cuerpoVela3 = abs(v3Close - v3Open)
            cuerpoPct3 = cuerpoVela3 / rangoVela3 if rangoVela3 > 0 else 0.0
            mechaVela3 = rangoVela3 - cuerpoVela3
            mechaPct3 = mechaVela3 / rangoVela3 if rangoVela3 > 0 else 0.0

            classification = 'CONTINUATION'
            highProbability = False
            breakawayGap = False
            rejectionLowProbability = False

            # a. High Probability (standard consolidation / pause)
            if cuerpoPct3 < 0.60:
                highProbability = True
                classification = 'Alta Probabilidad'

            # b. Breakaway Gap (institutional strength)
            currentAtr = float(atr14Series[i])
            isCappingAway = False
            if isBullish:
                isCappingAway = (v3Close >= v3Low + 0.8 * rangoVela3)
            else:
                isCappingAway = (v3Close <= v3Low + 0.2 * rangoVela3)

            if cuerpoPct3 > 0.80 and isCappingAway and currentAtr > 0 and rangoVela3 > 1.5 * currentAtr:
                breakawayGap = True
                classification = 'Breakaway Gap'
                highProbability = True

            # c. Rejection / Low Probability (Trap)
            isDeepPenetration = False
            gapTeorico = abs(v2Close - v1High) if isBullish else abs(v1Low - v2Close)
            if gapTeorico > 0:
                if isBullish and v3Low < v2Low:
                    penetration = v2Low - v3Low
                    if penetration > 0.3 * gapTeorico and v3Close > v2Low:
                        isDeepPenetration = True
                elif isBearish and v3High > v2High:
                    penetration = v3High - v2High
                    if penetration > 0.3 * gapTeorico and v3Close < v2High:
                        isDeepPenetration = True

            if isDeepPenetration or mechaPct3 > 0.65:
                rejectionLowProbability = True
                classification = 'Rechazo/Baja Probabilidad'
                highProbability = False
                breakawayGap = False

            # D. Evaluate physical price touch mitigation
            mitigated = False
            for j in range(i + 1, len(df)):
                if isBullish:
                    if lows[j] <= v3Low:  # Touches or crosses top of FVG gap
                        mitigated = True
                        break
                else:
                    if highs[j] >= v3High:  # Touches or crosses bottom of FVG gap
                        mitigated = True
                        break

            # E. Traditional destructive mitigation (50% rule)
            if validateMitigation and self._isFvgMitigated(df, i, fvg):
                continue

            # Update object fields
            fvgFilteredObj = fvg.copy()
            fvgFilteredObj.update({
                'classification': classification,
                'highProbability': highProbability,
                'breakawayGap': breakawayGap,
                'rejectionLowProbability': rejectionLowProbability,
                'mitigated': mitigated
            })
            fvgsFiltered.append(fvgFilteredObj)

        return fvgsFiltered

    def _isFvgMitigated(self, df, fvgStartIdx, fvg):
        """
        Validates if an FVG is traditionally mitigated using the 50% rule.
        """
        gapMid = fvg.get('mid')
        if gapMid is None:
            bottom = fvg.get('bottom')
            top = fvg.get('top')
            if bottom is not None and top is not None:
                gapMid = (bottom + top) / 2.0
            else:
                return False

        for i in range(fvgStartIdx + 1, len(df)):
            candleClose = float(df['close'].iloc[i])

            if fvg['type'] == 'Bullish_FVG':
                if candleClose <= gapMid:
                    return True
            else:
                if candleClose >= gapMid:
                    return True

        return False

    def visualizeZones(self, df, fvgs, outputPath):
        """
        Generates a premium OHLC chart visualizing active and mitigated FVG zones.
        
        Args:
            df: pd.DataFrame with OHLC data.
            fvgs: list of filtered FVGs.
            outputPath: Path to save the output chart image.
        """
        if df is None or len(df) == 0:
            return

        fig, ax = plt.subplots(figsize=(12, 6))
        
        # Sleek dark mode / premium style
        fig.patch.set_facecolor('#0E1117')
        ax.set_facecolor('#0E1117')
        ax.spines['bottom'].set_color('#262730')
        ax.spines['top'].set_color('#262730')
        ax.spines['right'].set_color('#262730')
        ax.spines['left'].set_color('#262730')
        ax.tick_params(axis='x', colors='#808495')
        ax.tick_params(axis='y', colors='#808495')
        ax.grid(color='#262730', linestyle='--', linewidth=0.5)

        highs = df['high'].values
        lows = df['low'].values
        opens = df['open'].values
        closes = df['close'].values
        times = df.index

        # Plot candles manually
        for idx in range(len(df)):
            opVal = opens[idx]
            clVal = closes[idx]
            hiVal = highs[idx]
            loVal = lows[idx]
            
            # Wicks
            color = '#26A69A' if clVal >= opVal else '#EF5350'
            ax.vlines(idx, loVal, hiVal, color=color, linewidth=1.5)
            
            # Body
            bodyTop = max(opVal, clVal)
            bodyBottom = min(opVal, clVal)
            bodyHeight = bodyTop - bodyBottom
            if bodyHeight == 0:
                bodyHeight = 0.00001
            
            rect = patches.Rectangle(
                (idx - 0.35, bodyBottom),
                0.7,
                bodyHeight,
                facecolor=color,
                edgecolor=color,
                zorder=3
            )
            ax.add_patch(rect)

        # Plot FVG zones
        for fvg in fvgs:
            fvgIdx = fvg['idx']
            fvgType = fvg['type']
            topVal = fvg['top']
            bottomVal = fvg['bottom']
            isMitigated = fvg['mitigated']

            # Define FVG drawing range (from vela 3 to end of chart or mitigation candle)
            startDrawIdx = fvgIdx
            endDrawIdx = len(df) - 1

            if isMitigated:
                # Find the first index after FVG start where price crossed the gap
                for k in range(fvgIdx + 1, len(df)):
                    if fvgType == 'Bullish_FVG' and lows[k] <= topVal:
                        endDrawIdx = k
                        break
                    elif fvgType == 'Bearish_FVG' and highs[k] >= bottomVal:
                        endDrawIdx = k
                        break

            # Drawing width
            width = max(1, endDrawIdx - startDrawIdx + 1)
            
            # Green for bullish FVG, Red for bearish FVG
            colorBase = '#26A69A' if fvgType == 'Bullish_FVG' else '#EF5350'
            alphaVal = 0.15 if isMitigated else 0.35
            
            rectZone = patches.Rectangle(
                (startDrawIdx - 0.5, bottomVal),
                width,
                topVal - bottomVal,
                facecolor=colorBase,
                edgecolor=colorBase,
                linestyle='--' if isMitigated else '-',
                alpha=alphaVal,
                zorder=2
            )
            ax.add_patch(rectZone)
            
            # Draw mid-line of the FVG
            ax.hlines(fvg['mid'], startDrawIdx - 0.5, startDrawIdx + width - 0.5, colors=colorBase, linestyles=':', alpha=0.6, linewidth=1)

        ax.set_title("FvgAnalyzer - Fair Value Gaps Visualization", color='#FFFFFF', fontsize=14, pad=15)
        ax.set_xlabel("Velas (Index)", color='#808495', fontsize=10, labelpad=10)
        ax.set_ylabel("Precio", color='#808495', fontsize=10, labelpad=10)
        
        plt.tight_layout()
        plt.savefig(outputPath, facecolor='#0E1117', edgecolor='none')
        plt.close()

    # snake_case aliases to maintain compatibility and fulfill technical requirements
    detect_fvg = detectFvg
    apply_filters = applyFilters
    visualize_zones = visualizeZones
