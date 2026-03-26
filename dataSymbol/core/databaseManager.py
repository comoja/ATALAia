import pandas as pd
import logging
from typing import Optional
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

logger = logging.getLogger(__name__)

from middleware.config.constants import dbConfig


class DatabaseManager:
    def __init__(self, config: dict = None):
        cfg = config or dbConfig
        self.engine = create_engine(
            f"mysql+mysqlconnector://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['database']}"
        )

    def getLastTimestamp(self, symbol: str, timeframe: str = "5min") -> Optional[pd.Timestamp]:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT MAX(timestamp) FROM candles WHERE symbol=:symbol AND timeframe=:timeframe and timestamp <= NOW()"),
                    {"symbol": symbol, "timeframe": timeframe}
                )
                row = result.fetchone()
            return pd.Timestamp(row[0]) if row and row[0] else None
        except Exception as e:
            logger.error(f"Error en getLastTimestamp: {e}")
            return None

    def getFirstTimestamp(self, symbol: str, timeframe: str = "5min") -> Optional[pd.Timestamp]:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT MIN(timestamp) FROM candles WHERE symbol=:symbol AND timeframe=:timeframe"),
                    {"symbol": symbol, "timeframe": timeframe}
                )
                row = result.fetchone()
            return pd.Timestamp(row[0]) if row and row[0] else None
        except Exception as e:
            logger.error(f"Error en getFirstTimestamp: {e}")
            return None

    def hasData(self, symbol: str, timeframe: str = "5min") -> bool:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(
                    text("SELECT COUNT(*) FROM candles WHERE symbol=:symbol AND timeframe=:timeframe LIMIT 1"),
                    {"symbol": symbol, "timeframe": timeframe}
                )
                row = result.fetchone()
            return row[0] > 0 if row else False
        except Exception as e:
            logger.error(f"Error en hasData: {e}")
            return False

    def saveBulkData(self, dataFrame: pd.DataFrame, symbol: str, timeframe: str = "5min") -> int:
        if dataFrame.empty:
            return 0
        
        try:
            df = dataFrame.copy()
            
            if 'datetime' in df.columns:
                df.rename(columns={'datetime': 'timestamp'}, inplace=True)
            elif 'timestamp' not in df.columns:
                if isinstance(df.index, pd.DatetimeIndex):
                    df = df.reset_index()
                else:
                    df = pd.to_datetime(df.index).reset_index()
                    df.columns = ['timestamp'] + list(df.columns[1:]) if len(df.columns) > 1 else ['timestamp']
            
            if 'timestamp' not in df.columns:
                return 0
            
            df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d %H:%M:%S')
            
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col not in df.columns:
                    df[col] = 0
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
            
            with self.engine.begin() as conn:
                insertSql = text("""
                INSERT IGNORE INTO candles 
                (symbol, timeframe, timestamp, open, high, low, close, volume)
                VALUES (:symbol, :timeframe, :timestamp, :open, :high, :low, :close, :volume)
                """)
                
                inserted = 0
                for _, row in df.iterrows():
                    result = conn.execute(insertSql, {
                        "symbol": symbol,
                        "timeframe": timeframe,
                        "timestamp": row['timestamp'],
                        "open": float(row['open']),
                        "high": float(row['high']),
                        "low": float(row['low']),
                        "close": float(row['close']),
                        "volume": float(row['volume'])
                    })
                    if result.rowcount > 0:
                        inserted += 1
            
            logger.info(f"[{symbol}] {timeframe}: {inserted} velas insertadas")
            return inserted
            
        except Exception as e:
            logger.error(f"Error en saveBulkData: {e}")
            return 0

    def resampleAndSave(self, symbol: str, sourceTf: str = "5min", targetTf: str = "15min", fromDate: datetime = None) -> int:
        try:
            query = "SELECT timestamp, open, high, low, close, volume FROM candles WHERE symbol=:symbol AND timeframe=:timeframe"
            params = {"symbol": symbol, "timeframe": sourceTf}
            
            if fromDate:
                query += " AND timestamp >= :fromDate"
                params["fromDate"] = fromDate.strftime('%Y-%m-%d %H:%M:%S')
            
            query += " ORDER BY timestamp ASC"
            
            with self.engine.connect() as conn:
                df = pd.read_sql(text(query), conn, params=params)
            
            if df.empty:
                return 0
            
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
            df = df[df['timestamp'] >= pd.Timestamp('2000-01-01')]
            
            if df.empty:
                return 0
            
            df = df.set_index('timestamp')
            
            ruleMap = {
                "15min": "15min", "1h": "1h", "1day": "1D",
                "1week": "1W", "1month": "1MS"
            }
            
            dfResampled = df.resample(rule=ruleMap.get(targetTf, "15T"), closed='right', label='right').agg({
                'open': 'first', 'high': 'max', 'low': 'min',
                'close': 'last', 'volume': 'sum'
            }).dropna()
            
            if dfResampled.empty:
                return 0
            
            dfResampled = dfResampled.reset_index()
            dfResampled.rename(columns={'index': 'timestamp'}, inplace=True)
            
            return self.saveBulkData(dfResampled, symbol, targetTf)
            
        except Exception as e:
            logger.error(f"Error en resampleAndSave: {e}")
            return 0

    def resampleStandardIntervals(self, symbol: str, fromDate: datetime = None) -> dict:
        results = {}
        
        last15 = self.getLastTimestamp(symbol, "15min")
        if last15:
            fromDate15min = last15 + timedelta(minutes=1)
        else:
            first5 = self.getFirstTimestamp(symbol, "5min")
            fromDate15min = None if first5 else fromDate
        
        results["15min"] = self.resampleAndSave(symbol, "5min", "15min", fromDate15min)
        
        last1h = self.getLastTimestamp(symbol, "1h")
        if last1h:
            fromDate1h = last1h + timedelta(hours=1)
        else:
            first15 = self.getFirstTimestamp(symbol, "15min")
            fromDate1h = None if first15 else fromDate
        
        results["1h"] = self.resampleAndSave(symbol, "15min", "1h", fromDate1h)
        return results

    def resampleLongIntervals(self, symbol: str) -> dict:
        results = {}
        for interval in ["1day", "1week", "1month"]:
            results[interval] = self.resampleAndSave(symbol, "5min", interval)
        return results

    def cleanupWeekendData(self, symbol: str = None) -> int:
        from zoneinfo import ZoneInfo
        tzNY = ZoneInfo("America/New_York")
        deleted = 0
        
        try:
            with self.engine.begin() as conn:
                if symbol:
                    result = conn.execute(
                        text("SELECT symbol, timeframe, timestamp FROM candles WHERE symbol = :symbol"),
                        {"symbol": symbol}
                    )
                else:
                    result = conn.execute(
                        text("SELECT symbol, timeframe, timestamp FROM candles")
                    )
                
                rows = result.fetchall()
                
                for row in rows:
                    sym, tf, ts = row
                    if tf not in ["5min", "15min", "1h"]:
                        continue
                    
                    tsNY = ts.astimezone(tzNY)
                    weekday = tsNY.weekday()
                    
                    if weekday == 5:
                        conn.execute(
                            text("DELETE FROM candles WHERE symbol = :symbol AND timeframe = :timeframe AND timestamp = :ts"),
                            {"symbol": sym, "timeframe": tf, "ts": ts}
                        )
                        deleted += 1
                    elif weekday == 6:
                        if tsNY.hour < 17:
                            conn.execute(
                                text("DELETE FROM candles WHERE symbol = :symbol AND timeframe = :timeframe AND timestamp = :ts"),
                                {"symbol": sym, "timeframe": tf, "ts": ts}
                            )
                            deleted += 1
        
        except Exception as e:
            logger.error(f"Error en cleanupWeekendData: {e}")
        
        if deleted > 0:
            logger.info(f"Eliminadas {deleted} velas de fin de semana")
        return deleted
