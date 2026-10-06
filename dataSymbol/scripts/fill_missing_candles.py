import os
import sys
import asyncio
import logging
from datetime import datetime

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from middleware.database import dbManager as middlewareDb
from dataSymbol.core.databaseManager import DatabaseManager
from middleware.api import forex

logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')
logger = logging.getLogger('fill_candles')

async def fill_symbol(sym: str, db: DatabaseManager):
    count = db.getCandleCount(sym, '5min')
    if count >= 10000:
        logger.info(f'[{sym}] Ya cuenta con {count} velas. Omitiendo.')
        return

    logger.info(f'🚀 [{sym}] Iniciando llenado de velas faltantes (actual={count})...')

    # Chunk 1: Mayo 2025 - Enero 2026 (~45k velas)
    p1 = {
        'symbol': sym,
        'interval': '5min',
        'start_date': datetime(2025, 5, 1),
        'end_date': datetime(2026, 1, 1)
    }
    try:
        df1 = await forex.getTimeSeries(p1)
        if df1 is not None and not df1.empty:
            ins1 = db.saveBulkData(df1, sym, '5min')
            logger.info(f'[{sym}] Chunk 1 (2025): {len(df1)} obtenidas, {ins1} insertadas.')
        else:
            logger.warning(f'[{sym}] Chunk 1 no devolvió velas.')
    except Exception as e:
        logger.error(f'[{sym}] Error en Chunk 1: {e}')

    # Chunk 2: Enero 2026 - Septiembre 2026 (~55k velas)
    p2 = {
        'symbol': sym,
        'interval': '5min',
        'start_date': datetime(2026, 1, 1),
        'end_date': datetime(2026, 9, 26, 23, 59)
    }
    try:
        df2 = await forex.getTimeSeries(p2)
        if df2 is not None and not df2.empty:
            ins2 = db.saveBulkData(df2, sym, '5min')
            logger.info(f'[{sym}] Chunk 2 (2026): {len(df2)} obtenidas, {ins2} insertadas.')
        else:
            logger.warning(f'[{sym}] Chunk 2 no devolvió velas.')
    except Exception as e:
        logger.error(f'[{sym}] Error en Chunk 2: {e}')

    final_cnt = db.getCandleCount(sym, '5min')
    first = db.getFirstTimestamp(sym, '5min')
    last = db.getLastTimestamp(sym, '5min')
    logger.info(f'✅ [{sym}] Completado: {final_cnt} velas | {first} a {last}')

async def main():
    db = DatabaseManager()
    all_symbols = middlewareDb.getDataSymbols()
    target_symbols = []
    for s in all_symbols:
        sym = s['symbol']
        cnt = db.getCandleCount(sym, '5min')
        if cnt < 10000:
            target_symbols.append(sym)

    logger.info(f'Símbolos a llenar ({len(target_symbols)}): {target_symbols}')
    for sym in target_symbols:
        await fill_symbol(sym, db)

    logger.info('🎉 Todos los pares faltantes han sido llenados en candles!')
    
    # Sincronizar StockPrices diarios para estos pares
    logger.info('📊 Iniciando cálculo de StockPrices diarios...')
    try:
        from dataSymbol.dailyStockPrices import syncDailyStockPrices
        await syncDailyStockPrices()
        logger.info('✅ StockPrices diarios sincronizados exitosamente.')
    except Exception as e:
        logger.error(f'Error al sincronizar StockPrices: {e}')

if __name__ == '__main__':
    asyncio.run(main())
