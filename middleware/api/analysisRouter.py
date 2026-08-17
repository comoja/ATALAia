from fastapi import APIRouter, HTTPException, Query
import logging
from middleware.database import dbManager
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)
router = APIRouter()

@router.get("/analisis/conducta/{symbol}")
async def get_conducta_analisis(
    symbol: str, 
    days: int = Query(365, description="Días de historial a evaluar")
):
    """
    Endpoint para calcular las métricas conductuales (Similares al ATALA IA.xlsm).
    Retorna distribuciones de probabilidad, velocidad y rachas.
    """
    try:
        # Decodificamos barras o formato url
        symbol = symbol.replace("-", "/")
        
        # 1. Obtener historial de precios diarios
        df = await dbManager.getStockPricesFromDb(symbol, limit=days)
        if df is None or df.empty:
            return {"error": f"No data found for {symbol}"}
        
        # 2. Re-calcular métricas conductuales con closePrice
        # Como solo tenemos closePrice, definimos "sube_baja" comparando con el día anterior
        df['sube_baja'] = (df['closePrice'] > df['closePrice'].shift(1)).astype(int)
        
        # El rango puede ser la volatilidad diaria absoluta (cambio respecto al día anterior)
        df['range'] = (df['closePrice'] - df['closePrice'].shift(1)).abs()
        
        # Eliminamos el primer registro porque su shift(1) es NaN
        df = df.dropna().reset_index(drop=True)
        
        df['dir'] = np.where(df['sube_baja'] == 1, 4, 2)
        
        # 3. Probabilidad 1-3 Pasos (Rachas de 3 días)
        # Crear secuencias de 3 días (ej. 442, 244)
        s0 = df['dir'].astype(int).astype(str)
        s1 = df['dir'].shift(-1).fillna(0).astype(int).astype(str)
        s2 = df['dir'].shift(-2).fillna(0).astype(int).astype(str)
        df['seq_3'] = s0 + s1 + s2
        
        # Filtramos los que terminan en 0 (que fueron rellenados por fillna al final del df)
        seq_3_valid = df['seq_3'][~df['seq_3'].str.contains('0')]
        seq_3_counts = seq_3_valid.value_counts().to_dict()
        
        # 4. Velocidad ST (Short Term) y LT (Long Term)
        # Aproximación: Volatilidad normalizada
        df['vel_st'] = df['range'].rolling(window=5).mean()
        df['vel_lt'] = df['range'].rolling(window=20).mean()
        
        # Generar histogramas de velocidad
        st_hist, st_bins = np.histogram(df['vel_st'].dropna(), bins=10)
        lt_hist, lt_bins = np.histogram(df['vel_lt'].dropna(), bins=10)
        
        # Formatear respuesta
        return {
            "symbol": symbol,
            "probabilidad_3_pasos": {
                "labels": list(seq_3_counts.keys()),
                "data": list(seq_3_counts.values())
            },
            "velocidad_st": {
                "labels": [f"{round(b, 4)}" for b in st_bins[:-1]],
                "data": st_hist.tolist()
            },
            "velocidad_lt": {
                "labels": [f"{round(b, 4)}" for b in lt_bins[:-1]],
                "data": lt_hist.tolist()
            }
        }
    except Exception as e:
        logger.error(f"Error analizando conducta para {symbol}: {e}")
        raise HTTPException(status_code=500, detail=str(e))
