import os
import sys
import logging
import asyncio
from datetime import datetime, time, timedelta
import pandas as pd
import pytz
from zoneinfo import ZoneInfo

# Configuración de rutas
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from middleware.utils.loggerConfig import setupLogging
setupLogging(logPara="dataSymbolDaily", projectDir=os.path.dirname(os.path.abspath(__file__)), enableConsole=True)

logger = logging.getLogger("dataSymbolDaily")

from middleware.database import dbManager
from middleware.config.constants import TIMEZONE
from Sentinel.backtesting.opt_db_helper import getDbConnection


def compute_symbol_daily_stock_prices(symbol: str, conn, from_date: datetime = None) -> pd.DataFrame:
    """
    Calcula los precios de cierre diarios al cierre de la sesión de Nueva York (17:00 NY),
    considerando automáticamente el Horario de Verano (EDT vs EST).
    
    Zona horaria de origen (candles): America/Mexico_City
    Zona horaria de referencia (sesión de trading): America/New_York
    """
    where_clause = "symbol = %s AND timeframe = '5min'"
    params = [symbol]
    
    if from_date:
        # Traemos un margen de 2 días antes para asegurar el cálculo correcto de la sesión
        margin_date = from_date - timedelta(days=2)
        where_clause += " AND timestamp >= %s"
        params.append(margin_date.strftime('%Y-%m-%d %H:%M:%S'))
        
    query = f"""
    SELECT timestamp, close 
    FROM candles 
    WHERE {where_clause}
    ORDER BY timestamp ASC
    """
    
    with conn.cursor() as cur:
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        
    if not rows:
        return pd.DataFrame()
        
    df = pd.DataFrame(rows, columns=['timestamp', 'close'])
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    
    # 1. Localizar en hora CDMX y convertir a America/New_York (ajuste automático de DST)
    df['dt_cdmx'] = pd.to_datetime(df['timestamp']).dt.tz_localize('America/Mexico_City', ambiguous='infer', nonexistent='shift_forward')
    df['dt_ny'] = df['dt_cdmx'].dt.tz_convert('America/New_York')

    # 2. Determinar la fecha de sesión en Nueva York
    # La sesión diaria de NY cierra a las 17:00:00 NY.
    # Velas con hora <= 17:00 pertenecen a la sesión del día.
    # Velas con hora > 17:00 (apertura de siguiente jornada) pertenecen a la sesión siguiente.
    df['session_date'] = df['dt_ny'].apply(
        lambda dt: dt.date() if dt.time() <= time(17, 0, 0) else dt.date() + timedelta(days=1)
    )
    
    # En Forex, las velas del domingo por la noche (apertura de semana a las 17:00 NY)
    # corresponden a la sesión de trading del lunes.
    df['session_date'] = df['session_date'].apply(
        lambda d: d + timedelta(days=1) if d.weekday() == 6 else d
    )

    # 3. Obtener el último precio de cierre disponible de cada sesión
    daily_df = df.groupby('session_date')['close'].last().reset_index()
    daily_df.rename(columns={'session_date': 'priceDate', 'close': 'closePrice'}, inplace=True)
    daily_df['symbol'] = symbol
    
    if from_date:
        from_d = from_date.date() if hasattr(from_date, 'date') else from_date
        daily_df = daily_df[daily_df['priceDate'] >= from_d]
        
    return daily_df


def save_stock_prices_bulk(df: pd.DataFrame, conn) -> int:
    """Inserta o actualiza masivamente registros en la tabla stockprices."""
    if df.empty:
        return 0
        
    records = [
        (str(row['symbol']), row['priceDate'].strftime('%Y-%m-%d 00:00:00') if hasattr(row['priceDate'], 'strftime') else str(row['priceDate']), float(row['closePrice']))
        for _, row in df.iterrows()
    ]
    
    sql = """
    INSERT INTO stockprices (symbol, priceDate, closePrice)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE closePrice = VALUES(closePrice)
    """
    
    with conn.cursor() as cur:
        cur.executemany(sql, records)
        conn.commit()
        
    return len(records)


async def syncDailyStockPrices(full_rebuild: bool = False):
    """Sincroniza la tabla stockprices desde candles con corte de sesión en NY close."""
    logger.info("==================================================================")
    logger.info(f"🚀 Iniciando sincronización de StockPrices con Cierre de NY (DST)... [full_rebuild={full_rebuild}]")
    logger.info("==================================================================")
    
    conn = getDbConnection()
    
    try:
        # Obtener lista de símbolos disponibles en candles
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT symbol FROM candles WHERE timeframe = '5min' ORDER BY symbol ASC")
            symbols = [r[0] for r in cur.fetchall()]
            
        if not symbols:
            logger.warning("No se encontraron símbolos con velas 5min en la base de datos.")
            return

        logger.info(f"Se encontraron {len(symbols)} símbolos para procesar en StockPrices.")
        total_inserted = 0
        
        for symbol in symbols:
            # Determinar última fecha procesada si no es full_rebuild
            from_date = None
            if not full_rebuild:
                with conn.cursor() as cur:
                    cur.execute("SELECT MAX(priceDate) FROM stockprices WHERE symbol = %s", (symbol,))
                    max_date = cur.fetchone()[0]
                    if max_date:
                        from_date = max_date - timedelta(days=1)
                        
            df_daily = compute_symbol_daily_stock_prices(symbol, conn, from_date=from_date)
            
            if not df_daily.empty:
                count = save_stock_prices_bulk(df_daily, conn)
                logger.info(f"  ✅ [{symbol}] Guardados {count} precios diarios (Cierre NY). Rango: {df_daily['priceDate'].min()} a {df_daily['priceDate'].max()}")
                total_inserted += count
            else:
                logger.info(f"  ℹ️ [{symbol}] Sin nuevos datos para actualizar.")
                
        logger.info("==================================================================")
        logger.info(f"🏁 Sincronización de StockPrices completada. Total registros: {total_inserted}")
        logger.info("==================================================================")
        
    finally:
        conn.close()


def run_sync(full_rebuild: bool = False):
    asyncio.run(syncDailyStockPrices(full_rebuild=full_rebuild))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Sincroniza StockPrices con Cierre de NY y DST")
    parser.add_argument("--full", action="store_true", help="Reconstrucción completa de todo el histórico")
    args = parser.parse_args()
    
    try:
        run_sync(full_rebuild=args.full)
    except KeyboardInterrupt:
        logger.info("Proceso detenido por el usuario.")
