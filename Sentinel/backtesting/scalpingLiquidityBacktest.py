"""
Institutional Scalping Backtest - Liquidity & Interbank Algorithm
H1: Identify liquidity zones (NY Session 08:00-09:00 CDMX)
M3: Execute on breakout + FVG confirmation
Compatible with pre-loaded data from main.py
"""
import sys
import os

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

import numpy as np
import pandas as pd
import vectorbt as vbt
from datetime import datetime, time, timedelta
import warnings
warnings.filterwarnings('ignore')

from middleware.scheduler.autoScheduler import isRestTime


class LiquidityScalpingBacktest:
    def __init__(self, dataH1: pd.DataFrame, dataM3: pd.DataFrame, timeZone='America/Mexico_City', symbol: str = None):
        self.dataH1 = dataH1.copy()
        self.dataM3 = dataM3.copy()
        self.timeZone = timeZone
        self.symbol = symbol
        self.trades = []
        self.metrics = {}
        self._validateSymbolFromDb()
    
    def _validateSymbolFromDb(self):
        if not self.symbol:
            return
        from middleware.database import dbManager
        dbSymbol = dbManager.getSymbol(self.symbol)
        if not dbSymbol or not dbSymbol.get('Activo'):
            print(f"Symbol {self.symbol} not found or inactive in database")
            self._symbolValid = False
        else:
            self._symbolValid = True
        
    def convertToMexicoTime(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if df.index.tz is None:
            df.index = pd.to_datetime(df.index).tz_localize('UTC').tz_convert(self.timeZone)
        else:
            df.index = df.index.tz_convert(self.timeZone)
        df.index = df.index.tz_localize(None)
        return df
    
    @staticmethod
    def isNyDST(date: pd.Timestamp) -> bool:
        year = date.year
        dstStart = pd.Timestamp(year, 3, 8) 
        dstEnd = pd.Timestamp(year, 11, 1)
        
        while dstStart.weekday() != 6:
            dstStart += pd.Timedelta(days=1)
        while dstEnd.weekday() != 6:
            dstEnd += pd.Timedelta(days=1)
        
        return dstStart <= date < dstEnd
    
    def getNySessionH1Levels(self, date: pd.Timestamp) -> tuple:
        if self.isNyDST(date):
            sessionStart = date.replace(hour=6, minute=0, second=0, microsecond=0)
            sessionEnd = date.replace(hour=7, minute=0, second=0, microsecond=0)
            killzoneHour = 6
        else:
            sessionStart = date.replace(hour=7, minute=0, second=0, microsecond=0)
            sessionEnd = date.replace(hour=8, minute=0, second=0, microsecond=0)
            killzoneHour = 7
        
        self._currentKillzoneHour = killzoneHour
        
        mask = (self.dataH1.index >= sessionStart) & (self.dataH1.index < sessionEnd)
        sessionData = self.dataH1.loc[mask]
        
        if len(sessionData) > 0:
            high = sessionData['high'].max()
            low = sessionData['low'].min()
            return high, low
        return None, None
    
    def isKillzone(self, dt: datetime) -> bool:
        killzoneHour = getattr(self, '_currentKillzoneHour', 6)
        return dt.hour == killzoneHour and dt.minute < 60
    
    @staticmethod
    def resampleToM3(dataH1: pd.DataFrame) -> pd.DataFrame:
        from middleware.api.twelvedata import resample_candles as resampleCandles
        return resampleCandles(dataH1, '3min')
    
    def runWithSingleTimeframe(self, dataH1: pd.DataFrame) -> list:
        dataM3 = self.resampleToM3(dataH1)
        if dataM3 is None or len(dataM3) < 100:
            print("Failed to resample H1 to M3")
            return []
        return self.runBacktest()
    
    def detectFvg(self, df: pd.DataFrame, idx: int, direction: str) -> dict:
        if idx >= len(df) - 3:
            return None
        
        if direction == 'SHORT':
            lowN = df['low'].iloc[idx]
            highN2 = df['high'].iloc[idx + 2]
            if lowN > highN2:
                return {
                    'type': 'Bearish_FVG',
                    'start': highN2,
                    'end': lowN,
                    'mid': (highN2 + lowN) / 2,
                    'size': lowN - highN2,
                    'idx': idx
                }
        else:
            highN = df['high'].iloc[idx]
            lowN2 = df['low'].iloc[idx + 2]
            if highN < lowN2:
                return {
                    'type': 'Bullish_FVG',
                    'start': highN,
                    'end': lowN2,
                    'mid': (highN + lowN2) / 2,
                    'size': lowN2 - highN,
                    'idx': idx
                }
        return None
    
    def findFvgAfterImpulse(self, df: pd.DataFrame, startIdx: int, direction: str) -> dict:
        fvgs = []
        for i in range(startIdx, min(startIdx + 10, len(df) - 3)):
            fvg = self.detectFvg(df, i, direction)
            if fvg:
                fvgs.append(fvg)
        return fvgs[-1] if fvgs else None
    
    def runBacktest(self):
        if self.symbol and hasattr(self, '_symbolValid') and not self._symbolValid:
            print(f"Symbol {self.symbol} not valid or inactive in database")
            self.trades = []
            self.metrics = {'totalTrades': 0, 'winRate': 0, 'profitFactor': 0, 'maxDrawdown': 0, 'totalPnl': 0}
            return self.trades
        
        self.dataH1 = self.convertToMexicoTime(self.dataH1)
        self.dataM3 = self.convertToMexicoTime(self.dataM3)
        
        uniqueDates = self.dataH1.index.date
        h1LevelsCache = {}
        
        for date in uniqueDates:
            if isinstance(date, pd.Timestamp):
                date = date.toPandasTimestamp()
            high, low = self.getNySessionH1Levels(pd.Timestamp(date))
            killzoneHour = getattr(self, '_currentKillzoneHour', 6)
            if high and low:
                h1LevelsCache[pd.Timestamp(date)] = {'high': high, 'low': low, 'killzoneHour': killzoneHour}
        
        print(f"Found {len(h1LevelsCache)} NY session days with liquidity levels")
        
        for idx in range(1, len(self.dataM3)):
            current = self.dataM3.iloc[idx]
            dt = current.name
            
            dateKey = dt.replace(hour=0, minute=0, second=0)
            if dateKey not in h1LevelsCache:
                continue
            
            h1Level = h1LevelsCache[dateKey]
            killzoneHour = h1Level.get('killzoneHour', 6)
            if dt.hour != killzoneHour:
                continue
            
            prevCandle = self.dataM3.iloc[idx - 1]
            price = current['close']
            
            breakoutPrice = None
            direction = None
            manipulationCandle = None
            
            if price > h1Level['high'] and prevCandle['close'] < h1Level['high']:
                breakoutPrice = price
                direction = 'SHORT'
                manipulationCandle = prevCandle
            elif price < h1Level['low'] and prevCandle['close'] > h1Level['low']:
                breakoutPrice = price
                direction = 'LONG'
                manipulationCandle = prevCandle
            
            if not breakoutPrice:
                continue
            
            fvg = self.findFvgAfterImpulse(self.dataM3, idx, direction)
            
            if fvg:
                entryPrice = fvg['mid']
                
                if direction == 'SHORT':
                    sl = manipulationCandle['high'] * 1.001
                else:
                    sl = manipulationCandle['low'] * 0.999
                
                risk = abs(entryPrice - sl)
                tp = entryPrice + (risk * 2) if direction == 'LONG' else entryPrice - (risk * 2)
                
                self.trades.append({
                    'entryTime': dt,
                    'direction': direction,
                    'entryPrice': entryPrice,
                    'sl': sl,
                    'tp': tp,
                    'h1High': h1Level['high'],
                    'h1Low': h1Level['low'],
                    'fvgType': fvg['type']
                })
        
        self.calculateMetrics()
        return self.trades
    
    def calculateMetrics(self):
        if not self.trades:
            self.metrics = {
                'totalTrades': 0,
                'winRate': 0,
                'profitFactor': 0,
                'maxDrawdown': 0,
                'totalPnl': 0
            }
            return
        
        wins = 0
        losses = 0
        pnlList = []
        
        for trade in self.trades:
            entry = trade['entryPrice']
            sl = trade['sl']
            tp = trade['tp']
            direction = trade['direction']
            
            if direction == 'LONG':
                if tp > entry:
                    pnl = tp - entry
                    wins += 1
                else:
                    pnl = sl - entry
                    losses += 1
            else:
                if tp < entry:
                    pnl = entry - tp
                    wins += 1
                else:
                    pnl = entry - sl
                    losses += 1
            
            pnlList.append(pnl)
        
        totalPnl = sum(pnlList)
        grossProfit = sum(p for p in pnlList if p > 0)
        grossLoss = abs(sum(p for p in pnlList if p < 0))
        
        profitFactor = grossProfit / grossLoss if grossLoss > 0 else float('inf') if grossProfit > 0 else 0
        winRate = (wins / len(self.trades)) * 100 if self.trades else 0
        
        cumulative = np.cumsum(pnlList)
        runningMax = np.maximum.accumulate(cumulative)
        drawdowns = (cumulative - runningMax) / runningMax
        maxDrawdown = abs(min(drawdowns)) * 100 if len(drawdowns) > 0 else 0
        
        self.metrics = {
            'totalTrades': len(self.trades),
            'wins': wins,
            'losses': losses,
            'winRate': round(winRate, 2),
            'profitFactor': round(profitFactor, 2),
            'maxDrawdown': round(maxDrawdown, 2),
            'totalPnl': round(totalPnl, 4)
        }
    
    def printReport(self):
        print("\n" + "="*50)
        print("SCALPING LIQUIDITY BACKTEST REPORT")
        print("="*50)
        print(f"Total Trades: {self.metrics['totalTrades']}")
        print(f"Wins: {self.metrics['wins']} | Losses: {self.metrics['losses']}")
        print(f"Win Rate: {self.metrics['winRate']}%")
        print(f"Profit Factor: {self.metrics['profitFactor']}")
        print(f"Max Drawdown: {self.metrics['maxDrawdown']}%")
        print(f"Total PnL: {self.metrics['totalPnl']}")
        print("="*50)
        
        if self.trades:
            dfTrades = pd.DataFrame(self.trades)
            print("\nSample Trades (first 5):")
            print(dfTrades.head())
    
    def toVectorbt(self):
        if not self.trades:
            return None
        
        entries = pd.Series(False, index=self.dataM3.index)
        exits = pd.Series(False, index=self.dataM3.index)
        
        for trade in self.trades:
            entryTime = trade['entryTime']
            if entryTime in entries.index:
                entries.loc[entryTime] = True
                
            direction = trade['direction']
            tp = trade['tp']
            
            for idx in self.dataM3.index:
                if idx > entryTime:
                    if direction == 'LONG' and self.dataM3.loc[idx, 'low'] <= trade['sl']:
                        exits.loc[idx] = True
                        break
                    elif direction == 'SHORT' and self.dataM3.loc[idx, 'high'] >= trade['sl']:
                        exits.loc[idx] = True
                        break
                    elif direction == 'LONG' and self.dataM3.loc[idx, 'high'] >= tp:
                        exits.loc[idx] = True
                        break
                    elif direction == 'SHORT' and self.dataM3.loc[idx, 'low'] <= tp:
                        exits.loc[idx] = True
                        break
        
        return entries, exits


async def loadDataFromApi(symbol: str, apiKey: str, startDate: str, endDate: str):
    from middleware.api import twelvedata
    dataH1 = await twelvedata.getTimeSeries({"symbol": symbol, "interval": "1h", "apikey": apiKey, "outputSize": 5000})
    dataM3 = await twelvedata.getTimeSeries({"symbol": symbol, "interval": "3min", "apikey": apiKey, "outputSize": 5000})
    
    if dataH1 is not None and dataM3 is not None:
        dataH1 = dataH1.loc[startDate:endDate]
        dataM3 = dataM3.loc[startDate:endDate]
    
    return dataH1, dataM3


def runWithPreloadedData(dataH1: pd.DataFrame, dataM3: pd.DataFrame) -> 'LiquidityScalpingBacktest':
    if dataH1 is None or dataM3 is None:
        print("No data provided")
        return None
    
    print(f"Running backtest: H1={len(dataH1)} candles, M3={len(dataM3)} candles")
    
    backtest = LiquidityScalpingBacktest(dataH1, dataM3, symbol=symbol)
    trades = backtest.runBacktest()
    backtest.printReport()
    
    return backtest


def loadAndRunLocal(symbol: str, startDate: str, endDate: str):
    import yfinance as yf
    
    print(f"Loading data for {symbol}...")
    
    ticker = yf.Ticker(symbol)
    dataM3 = ticker.history(start=startDate, end=endDate, interval='3min')
    dataH1 = ticker.history(start=startDate, end=endDate, interval='1h')
    
    if dataM3.empty or dataH1.empty:
        print("No data available")
        return
    
    print(f"M3: {len(dataM3)} candles, H1: {len(dataH1)} candles")
    
    backtest = LiquidityScalpingBacktest(dataH1, dataM3, symbol=symbol)
    trades = backtest.runBacktest()
    backtest.printReport()
    
    return backtest


async def runWithApi(symbol: str, apiKey: str, startDate: str, endDate: str):
    print(f"Loading data from Twelve Data API for {symbol}...")
    
    dataH1, dataM3 = await loadDataFromApi(symbol, apiKey, startDate, endDate)
    
    if dataH1 is None or dataM3 is None:
        print("Failed to load data from API")
        return
    
    print(f"M3: {len(dataM3)} candles, H1: {len(dataH1)} candles")
    
    backtest = LiquidityScalpingBacktest(dataH1, dataM3, symbol=symbol)
    trades = backtest.runBacktest()
    backtest.printReport()
    
    return backtest


def runBacktestFromMain(preloadedData: dict, symbol: str) -> dict:
    if symbol not in preloadedData:
        print(f"Symbol {symbol} not in preloaded data")
        return None
    
    dataH1 = preloadedData[symbol]
    dataM3 = LiquidityScalpingBacktest.resampleToM3(dataH1)
    
    if dataM3 is None or len(dataM3) < 100:
        print(f"Failed to generate M3 data for {symbol}")
        return None
    
    print(f"Running backtest for {symbol}: H1={len(dataH1)}, M3={len(dataM3)}")
    
    backtest = LiquidityScalpingBacktest(dataH1, dataM3, symbol=symbol)
    trades = backtest.runBacktest()
    backtest.printReport()
    
    return {
        'metrics': backtest.metrics,
        'trades': backtest.trades
    }


def runH1M3Backtest(dataH1: pd.DataFrame, dataM3: pd.DataFrame = None, symbol: str = None) -> 'LiquidityScalpingBacktest':
    if dataM3 is None:
        dataM3 = LiquidityScalpingBacktest.resampleToM3(dataH1)
    
    if dataM3 is None or len(dataM3) < 100:
        print("Invalid M3 data")
        return None
    
    backtest = LiquidityScalpingBacktest(dataH1, dataM3, symbol=symbol)
    backtest.runBacktest()
    backtest.printReport()
    
    return backtest


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Scalping Liquidity Backtest')
    parser.add_argument('--symbol', type=str, default='XAU/USD', help='Symbol to test')
    parser.add_argument('--start', type=str, default='2024-01-01', help='Start date')
    parser.add_argument('--end', type=str, default='2024-12-31', help='End date')
    parser.add_argument('--api-key', type=str, default=None, help='Twelve Data API key')
    parser.add_argument('--local', action='store_true', help='Use local yfinance data')
    parser.add_argument('--resample', action='store_true', help='Generate M3 from H1')
    
    args = parser.parse_args()
    
    symbolMap = {
        'XAU/USD': 'XAUUSD',
        'XAUUSD': 'XAUUSD'
    }
    
    symbol = symbolMap.get(args.symbol, args.symbol)
    
    if args.resample:
        print("Running with H1 data only - will resample to M3...")
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        dataH1 = ticker.history(start=args.start, end=args.end, interval='1h')
        if dataH1.empty:
            print("No data available")
        else:
            backtest = LiquidityScalpingBacktest(dataH1, None, symbol=symbol)
            backtest.runWithSingleTimeframe(dataH1)
    elif args.local:
        loadAndRunLocal(symbol, args.start, args.end)
    else:
        if not args.api_key:
            print("Using local yfinance data (--local)")
            loadAndRunLocal(symbol, args.start, args.end)
        else:
            import asyncio
            asyncio.run(runWithApi(symbol, args.api_key, args.start, args.end))