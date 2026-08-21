"""
Módulo auxiliar centralizado para scripts de optimización y backtesting de Sentinel.
Proporciona conexión directa y optimizada a MySQL, carga de velas, mapeo de símbolos/pips/spreads
y persistencia garantizada en la tabla symbolstrategyconfig.
"""
import os
import sys
import json
import logging
import pymysql
import pandas as pd
import numpy as np
from datetime import datetime
from typing import List, Dict, Any, Optional

# Configuración de rutas
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(CURRENT_DIR, '..', '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logger = logging.getLogger('opt_db_helper')

# Credenciales de base de datos MySQL directa
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'M1x&J34ny',
    'db': 'atalaia',
    'charset': 'utf8mb4',
    'autocommit': True
}

# Multiplicadores de Pips
PIP_MULTIPLIERS: Dict[str, float] = {
    'EUR/USD': 10000.0,
    'GBP/USD': 10000.0,
    'AUD/USD': 10000.0,
    'NZD/USD': 10000.0,
    'USD/CAD': 10000.0,
    'USD/CHF': 10000.0,
    'EUR/GBP': 10000.0,
    'GBP/CAD': 10000.0,
    'GBP/JPY': 100.0,
    'USD/JPY': 100.0,
    'USD/MXN': 10000.0,
    'XAU/USD': 1.0,
    'BTC/USD': 1.0,
    'NAS100': 1.0,
    'XAG/USD': 100.0,
    'SPX500': 1.0,
    'NZD/JPY': 100.0,
    'USD/HKD': 10000.0,
    'XBR/USD': 100.0,
    'XTI/USD': 100.0,
}

# Spreads estimados por símbolo
SPREADS: Dict[str, float] = {
    'EUR/USD': 1.0,
    'GBP/USD': 1.5,
    'AUD/USD': 1.2,
    'NZD/USD': 1.5,
    'USD/CAD': 1.5,
    'USD/CHF': 1.6,
    'EUR/GBP': 1.5,
    'GBP/CAD': 2.2,
    'GBP/JPY': 2.0,
    'USD/JPY': 1.2,
    'USD/MXN': 25.0,
    'XAU/USD': 0.35,
    'BTC/USD': 30.0,
    'NAS100': 1.5,
    'XAG/USD': 0.03,
    'SPX500': 0.5,
    'NZD/JPY': 2.0,
    'USD/HKD': 5.0,
    'XBR/USD': 0.05,
    'XTI/USD': 0.05,
}

def getDbConnection():
    """Retorna una conexión activa a la base de datos MySQL."""
    return pymysql.connect(**DB_CONFIG)

def getActiveSentinelSymbols() -> List[str]:
    """Obtiene la lista de símbolos activos configurados en SentinelSymbol."""
    try:
        conn = getDbConnection()
        cursor = conn.cursor()
        cursor.execute("SELECT symbol FROM sentinelsymbol WHERE Activo = 1 ORDER BY symbol")
        rows = cursor.fetchall()
        cursor.close()
        conn.close()
        symbols = [r[0] for r in rows]
        if symbols:
            return symbols
    except Exception as e:
        logger.error(f"Error obteniendo símbolos activos de BD: {e}")
    # Fallback si falla la consulta
    return [
        'AUD/USD', 'BTC/USD', 'EUR/GBP', 'EUR/USD', 'GBP/CAD',
        'GBP/USD', 'NAS100', 'NZD/USD', 'USD/CAD', 'USD/CHF',
        'USD/MXN', 'XAG/USD', 'XAU/USD'
    ]

def getPipMultiplier(symbol: str) -> float:
    """Obtiene el multiplicador de pip de forma segura."""
    if symbol in PIP_MULTIPLIERS:
        return PIP_MULTIPLIERS[symbol]
    if 'JPY' in symbol or 'HUF' in symbol:
        return 100.0
    return 10000.0

def getSpread(symbol: str) -> float:
    """Obtiene el spread de forma segura."""
    return SPREADS.get(symbol, 1.5)

def loadCandles(symbol: str, startDate: str, endDate: str, timeframe: str = '5min') -> pd.DataFrame:
    """Carga velas históricas desde la tabla 'candles' para un símbolo y rango."""
    try:
        conn = getDbConnection()
        query = """
            SELECT timestamp as datetime, open, high, low, close, volume
            FROM candles
            WHERE symbol = %s AND timeframe = %s AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        df = pd.read_sql(query, conn, params=(symbol, timeframe, startDate, endDate))
        conn.close()
        if not df.empty:
            df['datetime'] = pd.to_datetime(df['datetime'])
            df.set_index('datetime', inplace=True)
            for col in ['open', 'high', 'low', 'close', 'volume']:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            df = df[~df.index.duplicated(keep='last')]
            return df.dropna(subset=['close'])
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Error cargando velas para {symbol}: {e}")
        return pd.DataFrame()

def saveSymbolStrategyConfig(
    strategy: str,
    symbol: str,
    enabled: bool,
    parametersDict: Optional[Dict[str, Any]] = None,
    imacdDict: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Inserta o actualiza la configuración de una estrategia para un símbolo en MySQL.
    """
    try:
        conn = getDbConnection()
        cursor = conn.cursor()
        
        paramsJson = json.dumps(parametersDict) if parametersDict is not None else None
        imacdJson = json.dumps(imacdDict) if imacdDict is not None else None
        
        sql = """
            INSERT INTO symbolstrategyconfig (strategy, symbol, enabled, parametersJson, jsonIMACD)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                enabled = VALUES(enabled),
                parametersJson = VALUES(parametersJson),
                jsonIMACD = VALUES(jsonIMACD)
        """
        cursor.execute(sql, (strategy, symbol, bool(enabled), paramsJson, imacdJson))
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Error guardando symbolstrategyconfig ({strategy}, {symbol}): {e}")
        return False

def saveAllStrategyResults(
    strategy: str,
    allSymbols: List[str],
    bestResultsBySymbol: Dict[str, Dict[str, Any]],
    defaultParams: Optional[Dict[str, Any]] = None,
    defaultImacd: Optional[Dict[str, Any]] = None
) -> None:
    """
    Garantiza que TODOS los símbolos en allSymbols queden registrados en symbolstrategyconfig:
    - Los símbolos con resultados rentables se guardan con enabled=True y sus parámetros optimizados.
    - Los símbolos sin resultados rentables se guardan con enabled=False y parámetros por defecto/vacíos.
    """
    conn = getDbConnection()
    cursor = conn.cursor()
    
    savedTrue = 0
    savedFalse = 0
    
    for symbol in allSymbols:
        if symbol in bestResultsBySymbol:
            data = bestResultsBySymbol[symbol]
            params = data.get('params', defaultParams or {})
            imacd = data.get('imacd', defaultImacd)
            enabled = True
            savedTrue += 1
        else:
            params = defaultParams or {}
            imacd = defaultImacd
            enabled = False
            savedFalse += 1
            
        paramsJson = json.dumps(params) if params is not None else None
        imacdJson = json.dumps(imacd) if imacd is not None else None
        
        sql = """
            INSERT INTO symbolstrategyconfig (strategy, symbol, enabled, parametersJson, jsonIMACD)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                enabled = VALUES(enabled),
                parametersJson = VALUES(parametersJson),
                jsonIMACD = VALUES(jsonIMACD)
        """
        cursor.execute(sql, (strategy, symbol, enabled, paramsJson, imacdJson))
        
    cursor.close()
    conn.close()
    logger.info(f"📊 [{strategy}] Persistencia completa en symbolstrategyconfig: {savedTrue} activos (TRUE), {savedFalse} desactivados (FALSE). Total: {len(allSymbols)}")

def getOutputPath(filename: str) -> str:
    """Retorna la ruta absoluta dentro del directorio de backtesting."""
    return os.path.join(CURRENT_DIR, filename)
