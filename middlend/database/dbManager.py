import os
import sys
import mysql.connector
import pandas as pd
from datetime import datetime
import logging
import asyncio
logger = logging.getLogger(__name__)

from middlend.database import dbConnection

def cierraTradeEnDb(idTrade, precioCierre, fechaCierre, comentario):
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor()

        sqlClose = """
            UPDATE trades 
            SET exitPrice = %s, 
                closeTime = %s
            WHERE idTrade = %s
        """
        dbCursor.execute(sqlClose, (precioCierre, fechaCierre, comentario, idTrade))
        dbConn.commit()
        
    except Exception as error:
        logger.error(f"❌ Error al cerrar trade en DB: {error}")
    finally:
        dbCursor.close()
        dbConn.close()


def verificaCierreTrade(tradeData, dfVelas):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM trades WHERE closeTime is null and symbol = %s", (tradeData['symbol'],))
        
        trades = cursor.fetchall()    
        for trade in trades:
            stopLoss = float(trade['stopLoss'])
            takeProfit = float(trade['takeProfit'])
            direction = trade['direction'].lower()
            idTrade = trade['idTrade']
            
            openTime = tradeData['openTime']
            if isinstance(openTime, str):
                openTime = datetime.strptime(openTime, '%Y-%m-%d %H:%M:%S')

            dfVelas['datetime'] = pd.to_datetime(dfVelas['datetime'])
            dfPosterior = dfVelas[dfVelas['datetime'] > openTime].copy()

            if dfPosterior.empty:
                return False

            for index, row in dfPosterior.sort_values('datetime').iterrows():
                velaHigh = float(row['high'])
                velaLow = float(row['low'])
                fechaVela = row['datetime']
                precioCierre = 0
                motivoCierre = ""

                if direction == 'buy':
                    if velaLow <= stopLoss:
                        precioCierre = stopLoss
                        motivoCierre = "STOP_LOSS"
                    elif velaHigh >= takeProfit:
                        precioCierre = takeProfit
                        motivoCierre = "TAKE_PROFIT"

                elif direction == 'sell':
                    if velaHigh >= stopLoss:
                        precioCierre = stopLoss
                        motivoCierre = "STOP_LOSS"
                    elif velaLow <= takeProfit:
                        precioCierre = takeProfit
                        motivoCierre = "TAKE_PROFIT"

                if precioCierre > 0:
                    print(f"🎯 Trade {idTrade} cerrado por {motivoCierre} en {fechaVela}")
                    cierraTradeEnDb(idTrade, precioCierre, fechaVela, motivoCierre)
                    return True 
        conn.close()        
    except Exception as error:
        logger.error(f"❌ Error en verificaCierreTrade: {error}")
        return False


def logTrade(symbol, regime, pf, sharpe):
    conn = dbConnection.getConnection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO trades (symbol, regime, pf, sharpe)
        VALUES (%s, %s, %s, %s)
    """, (symbol, regime, pf, sharpe))

    conn.commit()
    conn.close()

def getAccount(id=None):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        if id:
            cursor.execute("SELECT * FROM CUENTA WHERE idCuenta = %s ", (id,))
        else:
            cursor.execute("SELECT * FROM CUENTA WHERE Activo=1")
        
        cuentas = cursor.fetchall()
        
        conn.close()
        if cuentas:
            return cuentas
        return []
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return []
    
def getSymbols():
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM SentinelSymbol WHERE Activo=1")
        
        symbols = cursor.fetchall()
        
        conn.close()
        if symbols:
            return symbols
        return []
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return []

def getSymbol(symbol: str):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM SentinelSymbol WHERE symbol = %s", (symbol,))
        
        result = cursor.fetchone()
        
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return None

def getSymbolTypeConfig(tipo: str):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM SymbolTypeConfig WHERE tipo = %s", (tipo,))
        
        result = cursor.fetchone()
        
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Error en la DB: {e}", exc_info=True)
        return None
    
def buscaTrade(tradeData):
    try:
        dbConn = dbConnection.getConnection()
        dbCursor = dbConn.cursor(dictionary=True)

        sqlCheck = """
            SELECT idTrade FROM trades 
            WHERE idCuenta = %s 
                AND symbol = %s 
                AND direction = %s 
                AND closeTime IS NULL 
                AND DATE(openTime) = CURDATE()
            LIMIT 1
        """
        
        paramsCheck = (
            tradeData['idCuenta'], 
            tradeData['symbol'], 
            tradeData['direction']
        )
        
        dbCursor.execute(sqlCheck, paramsCheck)
        tradeExistente = dbCursor.fetchone()

        if tradeExistente:
            logger.info(f"⚠️ Trade ya existente {tradeExistente['idTrade']} para {tradeData['symbol']} - se omite actualización")
        else:
            insertarTrade(tradeData)
            logger.info(f"🆕 Nuevo trade insertado para {tradeData['symbol']}")

    except Exception as error:
        logger.error(f"❌ Error en buscaTrade: {error}")
    finally:
        if 'dbCursor' in locals(): 
            dbCursor.close()

def actualizarTrade(idTrade, data):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor()
        
        sqlUpdate = """
            UPDATE trades 
            SET closeTime = %s, exitPrice = %s, pnl = %s, slippage = %s, commission = %s, openTime = %s
            WHERE idTrade = %s
        """
        valores = (
            data['closeTime'], data['exitPrice'], data['pnl'], 
            data.get('slippage', 0), data.get('commission', 0), data['openTime'], idTrade
        )
        
        cursor.execute(sqlUpdate, valores)
        conn.commit()
        logger.info(f"✅ Trade {idTrade} actualizado.")

    except Exception as e:
        logger.error(f"❌ Error al actualizarTrade {idTrade}: {e}")
        if 'conn' in locals(): conn.rollback()

def insertarTrade(data):
    try:
        conn = dbConnection.getConnection()
        cursor = conn.cursor()

        sqlInsert = """
            INSERT INTO trades (idCuenta, symbol, direction, openTime, size, entryPrice, stopLoss, takeProfit,intervalo)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s,%s)
        """
        valores = (
            data['idCuenta'], data['symbol'], data['direction'], 
            data['openTime'], data['size'], data['entryPrice'], 
            data.get('stopLoss'), data.get('takeProfit'),data['intervalo']
        )

        cursor.execute(sqlInsert, valores)
        conn.commit()
        logger.info(f"🚀 Nuevo trade insertado: {data['symbol']}")

    except Exception as e:
        logger.error(f"❌ Error al insertarTrade: {e}")
        if 'conn' in locals(): conn.rollback()


async def getLastCandleDatetime(symbol: str, timeframe: str):
    def query():
        try:
            conn = dbConnection.getConnection()
            cursor = conn.cursor()
            cursor.execute(
                "SELECT MAX(datetime) FROM candles WHERE symbol=%s AND timeframe=%s",
                (symbol, timeframe)
            )
            result = cursor.fetchone()
            conn.close()
            return result[0] if result[0] else None
        except Exception as e:
            logger.error(f"Error en getLastCandleDatetime: {e}", exc_info=True)
            return None

    return await asyncio.to_thread(query)

# --- Insertar nuevas velas ---
async def insertNewCandlesToDb(df, timeframe: str) -> int:
    """
    Inserta velas en la tabla 'candles' de forma segura y asincrónica.
    
    Args:
        df (pd.DataFrame): DataFrame con columnas ['symbol','datetime','open','high','low','close','volume'].
        timeframe (str): Intervalo de las velas, ej. '5min', '15min', '1h'.
    
    Returns:
        int: Número de velas insertadas.
    """
    if df.empty:
        logger.info(f"No hay velas para insertar en {timeframe}.")
        return 0

    # Asegurar que la columna 'volume' exista
    if 'volume' not in df.columns:
        df['volume'] = None  # o 0 si prefieres

    def insert():
        try:
            conn = dbConnection.getConnection()
            cursor = conn.cursor()
            insert_query = """
                INSERT IGNORE INTO candles 
                (symbol, timeframe, datetime, open, high, low, close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            values = [
                (
                    row['symbol'], 
                    timeframe, 
                    row['datetime'], 
                    row['open'], 
                    row['high'], 
                    row['low'], 
                    row['close'], 
                    row['volume']
                )
                for _, row in df.iterrows()
            ]

            cursor.executemany(insert_query, values)
            conn.commit()
            inserted = cursor.rowcount
            conn.close()
            return inserted
        except Exception as e:
            logger.error(f"Error en insertNewCandlesToDb: {e}", exc_info=True)
            return 0

    inserted_count = await asyncio.to_thread(insert)
    #logger.info(f"Insertadas {inserted_count} velas en '{timeframe}'")
    return inserted_count