from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
import logging
# Para traer los datos desde middleware
from middleware.database.dbManager import getCandlesFromDb
from backend.services.correlation_engine import engine
from sqlalchemy.orm import Session
from backend.database.models import SessionLocal, RatioSymbol

router = APIRouter()
logger = logging.getLogger(__name__)

# Dependencia para la base de datos de FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/correlation/{pair_name}")
async def get_pair_correlation(pair_name: str) -> Dict[str, Any]:
    """
    Este endpoint es consumido por PrimeFaces (Java).
    Calcula el análisis de correlación al vuelo tomando 
    el precio de cierre diario histórico.
    """
    logger.info(f"Solicitado el cálculo de correlación para el par: {pair_name}")
    
    try:
        # Extraemos hasta la última vela de 15min/5min de la BD (con las funciones que ya tenías)
        df = await getCandlesFromDb(symbol=pair_name, timeframe="5min", limit=15000)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No se encontraron velas para este par en la tabla 'candles'.")

        # MAGIA PANDAS: Agrupamos todas las operaciones de intradía y nos quedamos
        # con el último 'close' de cada día. 
        # Asi emulamos el comportamiento histórico diario del 'HIST PRICES ATALAIA.xlsm'
        df_daily = df.resample('D').agg({'close': 'last'}).dropna()
        
        # Renombramos 'close' a 'price' que es lo que espera el engine
        df_daily = df_daily.rename(columns={'close': 'price'})
        
        # Mandamos el DataFrame reconstruido a que nuestro motor matemático haga lo suyo
        resultados_matematicos = engine.process_pair(df_daily)
        
        return resultados_matematicos

    except Exception as e:
        logger.error(f"Error procesando {pair_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/catalogo")
def get_catalogo_pares(db: Session = Depends(get_db)):
    """
    Devuelve la lista de pares configurados exclusivamente para este módulo.
    """
    pares = db.query(RatioSymbol).filter(RatioSymbol.is_active == True).all()
    return [{"id": p.id, "pair_name": p.pair_name, "desc": p.description} for p in pares]
