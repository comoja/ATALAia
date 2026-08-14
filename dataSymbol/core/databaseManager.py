import pandas as pd
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from urllib.parse import quote
import pytz

logger = logging.getLogger(__name__)

from middleware.config.constants import TIMEZONE
from middleware.database.dbManager import _call_connection_pool


class DatabaseManager:
    """
    Gestor de base de datos para dataSymbol.
    Todas las operaciones de lectura, escritura y mantenimiento se realizan
    exclusivamente a través del microservicio ConnectionPool (http://127.0.0.1:8000/api/v1).
    """
    def __init__(self, config: dict = None):
        pass

    def getLastTimestamp(self, symbol: str, timeframe: str = "5min") -> Optional[pd.Timestamp]:
        """Obtiene la última fecha de vela guardada mediante ConnectionPool microservicio."""
        try:
            res = _call_connection_pool("GET", "/candles/stats/last-timestamp", params={"symbol": symbol, "timeframe": timeframe})
            if res and res.get("last_timestamp"):
                return pd.Timestamp(res["last_timestamp"])
            return None
        except Exception as e:
            logger.error(f"Error en getLastTimestamp: {e}")
            return None
    
    def getCandleCount(self, symbol: str, timeframe: str = "5min") -> int:
        """Obtiene el número total de velas registradas mediante ConnectionPool microservicio."""
        try:
            res = _call_connection_pool("GET", "/candles/stats/count", params={"symbol": symbol, "timeframe": timeframe})
            if res and "count" in res:
                return int(res["count"])
            return 0
        except Exception as e:
            logger.error(f"Error en getCandleCount: {e}")
            return 0

    def getFirstTimestamp(self, symbol: str, timeframe: str = "5min") -> Optional[pd.Timestamp]:
        """Obtiene la fecha más antigua registrada mediante ConnectionPool microservicio."""
        try:
            res = _call_connection_pool("GET", "/candles/stats/first-timestamp", params={"symbol": symbol, "timeframe": timeframe})
            if res and res.get("first_timestamp"):
                return pd.Timestamp(res["first_timestamp"])
            return None
        except Exception as e:
            logger.error(f"Error en getFirstTimestamp: {e}")
            return None

    def hasData(self, symbol: str, timeframe: str = "5min") -> bool:
        """Verifica si existen datos para un símbolo y temporalidad."""
        return self.getCandleCount(symbol, timeframe) > 0

    def saveBulkData(self, dataFrame: pd.DataFrame, symbol: str, timeframe: str = "5min") -> int:
        """Guarda masivamente un DataFrame de velas a través del microservicio ConnectionPool."""
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
            
            payload = []
            for _, row in df.iterrows():
                payload.append({
                    "symbol": symbol,
                    "timeframe": timeframe,
                    "timestamp": str(row['timestamp']),
                    "open": float(row['open']),
                    "high": float(row['high']),
                    "low": float(row['low']),
                    "close": float(row['close']),
                    "volume": float(row['volume'])
                })
            
            res = _call_connection_pool("POST", "/candles/bulk", json_data=payload, timeout=15.0)
            if res and res.get("status") == "ok":
                return res.get("inserted", len(payload))
            return 0

        except Exception as e:
            logger.error(f"Error en saveBulkData: {e}")
            return 0

    def resampleAndSave(self, symbol: str, sourceTf: str = "5min", targetTf: str = "15min", fromDate: datetime = None, minVelas: int = None) -> int:
        """Remuestrea datos de velas mediante ConnectionPool microservicio y los almacena."""
        try:
            res = _call_connection_pool("GET", "/candles/symbol-query", params={"symbol": symbol, "timeframe": sourceTf, "limit": 50000})
            if not res or not isinstance(res, list):
                return 0
            
            df = pd.DataFrame(res)
            if df.empty or 'timestamp' not in df.columns:
                return 0
            
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            if fromDate:
                from_naive = fromDate.replace(tzinfo=None)
                df = df[df['timestamp'].dt.tz_localize(None) >= from_naive]
            
            if df.empty:
                return 0
            
            df = df.sort_values('timestamp')
            if minVelas:
                df = df.head(minVelas)
            
            df = df.set_index('timestamp')
            
            now = datetime.now()
            df = df[df.index.tz_localize(None) <= now]
            
            if df.empty:
                return 0
            
            ruleMap = {
                "15min": "15min", "1h": "1h", "1day": "1D",
                "1week": "1W", "1month": "ME"
            }
            
            dfResampled = df.resample(rule=ruleMap.get(targetTf, "15min"), closed='right', label='right').agg({
                'open': 'first', 'high': 'max', 'low': 'min',
                'close': 'last', 'volume': 'sum'
            }).dropna()
            
            if dfResampled.empty:
                return 0
            
            dfResampled = dfResampled[dfResampled.index.tz_localize(None) <= now]
            
            if dfResampled.empty:
                return 0
            
            dfResampled = dfResampled.reset_index()
            dfResampled.rename(columns={'index': 'timestamp'}, inplace=True)
            
            return self.saveBulkData(dfResampled, symbol, targetTf)
            
        except Exception as e:
            logger.error(f"Error en resampleAndSave: {e}")
            return 0

    def _isIntervalComplete(self, df: pd.DataFrame, targetTf: str) -> bool:
        if df.empty:
            return False
        lastTs = df.index[-1]
        if targetTf == "15min":
            return lastTs.minute == 45 or lastTs.minute >= 50
        elif targetTf == "1h":
            return lastTs.minute == 45 and (lastTs.second >= 0 or lastTs.minute == 59)
        return False

    def resampleStandardIntervals(self, symbol: str, fromDate: datetime = None) -> dict:
        results = {}
        
        # 15min
        last15 = self.getLastTimestamp(symbol, "15min")
        count5 = self.getCandleCount(symbol, "5min")
        
        if last15:
            fromDate15min = last15 + timedelta(minutes=1)
            inserted = self.resampleAndSave(symbol, "5min", "15min", fromDate15min)
            results["15min"] = inserted
            logger.info(f"[{symbol}] 15min: {inserted} velas generadas desde {last15.strftime('%Y-%m-%d %H:%M')}")
        elif count5 >= 3:
            inserted = self.resampleAndSave(symbol, "5min", "15min", None)
            results["15min"] = inserted
            logger.info(f"[{symbol}] 15min (inicial): {inserted} velas generadas")
        else:
            results["15min"] = 0
            logger.info(f"[{symbol}] No hay suficientes 5min ({count5}) para generar 15min")
        
        # 1h
        last1h = self.getLastTimestamp(symbol, "1h")
        count15 = self.getCandleCount(symbol, "15min")
        
        if last1h:
            fromDate1h = last1h + timedelta(hours=1)
            inserted = self.resampleAndSave(symbol, "15min", "1h", fromDate1h)
            results["1h"] = inserted
            logger.info(f"[{symbol}] 1h: {inserted} velas generadas desde {last1h.strftime('%Y-%m-%d %H:%M')}")
        elif count15 >= 4:
            inserted = self.resampleAndSave(symbol, "15min", "1h", None)
            results["1h"] = inserted
            logger.info(f"[{symbol}] 1h (inicial): {inserted} velas generadas")
        else:
            results["1h"] = 0
            logger.info(f"[{symbol}] No hay suficientes 15min ({count15}) para generar 1h")
        
        return results

    def resampleLongIntervals(self, symbol: str) -> dict:
        results = {}
        for interval in ["1day", "1week", "1month"]:
            results[interval] = self.resampleAndSave(symbol, "5min", interval)
        return results

    def cleanupWeekendData(self, symbol: str = None) -> int:
        """Solicita la limpieza de velas de fin de semana al microservicio ConnectionPool."""
        try:
            params = {}
            if symbol:
                params["symbol"] = symbol
            res = _call_connection_pool("DELETE", "/candles/cleanup-weekend", params=params, timeout=30.0)
            if res and "deleted" in res:
                deleted = int(res["deleted"])
                if deleted > 0:
                    logger.info(f"Eliminadas {deleted} velas de fin de semana (Forex)")
                else:
                    logger.info("No se encontraron velas de fin de semana para eliminar.")
                return deleted
            return 0
        except Exception as e:
            logger.error(f"Error en cleanupWeekendData: {e}")
            return 0
