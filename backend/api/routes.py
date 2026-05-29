import os
import sys
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from fastapi import APIRouter, HTTPException, Depends
from typing import Dict, Any
from urllib.parse import unquote
import logging
from middleware.database.dbManager import getCandlesFromDb
from backend.services.correlation_engine import engine
from sqlalchemy.orm import Session
from backend.database.models import SessionLocal, RatioSymbol
import pandas as pd
import numpy as np

router = APIRouter()
logger = logging.getLogger(__name__)

# Dependencia para la base de datos de FastAPI
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/correlation/{pair_name:path}")
async def get_pair_correlation(
    pair_name: str,
    amplitude: float = 1.0,
    freq: float = 0.1,
    phase: float = 0.0,
    offset: float = 0.0,
    r: float = 0.05,
    tYears: float = 30 / 252,
    sigmaWindow: int = 7
) -> Dict[str, Any]:
    """
    Este endpoint es consumido por PrimeFaces (Java).
    Calcula el análisis de correlación al vuelo tomando 
    el precio de cierre diario histórico y aplicando coeficientes dinámicos.
    """
    logger.info(f"Solicitado el cálculo de correlación para el par: {pair_name}")
    # Decodificar por si el cliente envía el par URL-encodificado (ej. AUD%2FUSD → AUD/USD)
    pair_name = unquote(pair_name)
    
    try:
        # Extraemos un histórico amplio (ej. 200000 velas de 5m equivalen a más de 2 años)
        df = await getCandlesFromDb(symbol=pair_name, timeframe="5min", limit=200000)
        
        if df.empty:
            raise HTTPException(status_code=404, detail="No se encontraron velas para este par en la tabla 'candles'.")

        # MAGIA PANDAS: Agrupamos todas las operaciones de intradía y nos quedamos
        # con el último 'close' de cada día. 
        # Asi emulamos el comportamiento histórico diario del 'HIST PRICES ATALAIA.xlsm'
        df_daily = df.resample('D').agg({'close': 'last'}).dropna()
        
        # Renombramos 'close' a 'price' que es lo que espera el engine
        df_daily = df_daily.rename(columns={'close': 'price'})
        
        # Mandamos el DataFrame reconstruido a que nuestro motor matemático haga lo suyo
        resultadosMatematicos = engine.process_pair(
            df_daily,
            amplitude=amplitude,
            freq=freq,
            phase=phase,
            offset=offset,
            r=r,
            tYears=tYears,
            sigmaWindow=sigmaWindow
        )
        
        return resultadosMatematicos

    except Exception as e:
        logger.error(f"Error procesando {pair_name}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/catalogo")
def get_catalogo_pares(db: Session = Depends(get_db)):
    """
    Devuelve la lista de pares configurados exclusivamente para este módulo.
    """
    pares = db.query(RatioSymbol).filter(RatioSymbol.Activo == 1).all()
    return [{"id": p.symbol, "pair_name": p.symbol, "desc": p.symbol, "tipo": p.tipo} for p in pares]


@router.get("/ratio/{pairA:path}")
async def get_ratio_correlation(
    pairA: str,
    pairB: str,
    amplitude: float = 1.0,
    freq: float = 0.1,
    phase: float = 0.0,
    offset: float = 0.0,
    r: float = 0.05,
    tYears: float = 30 / 252,
    sigmaWindow: int = 7,
    tf: str = "1d",
    days: int = 365
) -> Dict[str, Any]:
    """
    Calcula el ratio sintético (Par A / Par B) y aplica el modelo completo.
    Equivalente Python de la hoja 'EURGBPUSD' del Excel ATALA IA MODEL.

    Ejemplo: GET /api/v1/ratio/EUR%2FUSD?pairB=GBP%2FUSD
    Resultado: precio_sintético = close_EURUSD / close_GBPUSD (EUR/GBP implícito)
    """
    logger.info(f"Ratio solicitado: {pairA} / {pairB}")
    # Decodificar por si el cliente envía los pares URL-encodificados (ej. AUD%2FUSD → AUD/USD)
    pairA = unquote(pairA)
    pairB = unquote(pairB)

    try:
        # Asegurar un calentamiento (warm-up) de al menos 30 días para calcular los indicadores (como SMA20) sin fallar el mínimo de 20 filas
        candle_limit = 2000000 if days == 0 else ((days + 30) * 288)

        # Cargar todo el historial posible según el request
        df_a = await getCandlesFromDb(symbol=pairA, timeframe="5min", limit=candle_limit)
        df_b = await getCandlesFromDb(symbol=pairB, timeframe="5min", limit=candle_limit)

        if df_a.empty:
            raise HTTPException(status_code=404, detail=f"Sin velas para Par A: {pairA}")
        if df_b.empty:
            raise HTTPException(status_code=404, detail=f"Sin velas para Par B: {pairB}")

        # Lógica de Agrupación (Resample)
        resample_rule = 'D'
        if tf == "1month":
            resample_rule = 'ME'  # Monthly End
        elif tf == "1week":
            resample_rule = 'W'   # Weekly
        elif tf == "1h":
            resample_rule = '1H'
        elif tf == "30m":
            resample_rule = '30T'
        elif tf == "15m":
            resample_rule = '15T'
        elif tf == "5m":
            resample_rule = None

        if resample_rule:
            df_a_daily = df_a.resample(resample_rule).agg({'close': 'last'}).dropna()
            df_b_daily = df_b.resample(resample_rule).agg({'close': 'last'}).dropna()
        else:
            df_a_daily = df_a
            df_b_daily = df_b

        # Motor de 2 pares: calcula ratio sintético y aplica todo el modelo
        resultado = engine.process_two_pairs(
            df_a=df_a_daily,
            df_b=df_b_daily,
            amplitude=amplitude,
            freq=freq,
            phase=phase,
            offset=offset,
            r=r,
            tYears=tYears,
            sigmaWindow=sigmaWindow
        )

        # Añadir metadata del ratio al response para el frontend
        if resultado.get("success"):
            resultado["pairA"] = pairA
            resultado["pairB"] = pairB
            resultado["ratioLabel"] = f"{pairA} / {pairB}"
            
            # Lógica de la señal de arbitraje basada en el Ciclo Sinusoidal calibrado
            latest_data = resultado.get("latest", {})
            ciclo_st_latest = latest_data.get("cicloStLatest", 0.0)
            
            if ciclo_st_latest > offset:
                resultado["arbitrageSignal"] = f"VENTA {pairA} - COMPRA {pairB}"
                resultado["arbitrageType"] = "SHORT"
            else:
                resultado["arbitrageSignal"] = f"COMPRA {pairA} - VENTA {pairB}"
                resultado["arbitrageType"] = "LONG"

            # Recortar el historial para devolver únicamente la ventana de tiempo solicitada
            if days > 0 and "history" in resultado:
                points_to_keep = days
                if tf == "1month":
                    points_to_keep = max(1, days // 30)
                elif tf == "1week":
                    points_to_keep = max(1, days // 7)
                elif tf == "1h":
                    points_to_keep = days * 24
                elif tf == "30m":
                    points_to_keep = days * 48
                elif tf == "15m":
                    points_to_keep = days * 96
                elif tf == "5m":
                    points_to_keep = days * 288
                
                history_real = resultado["history"][-points_to_keep:]
                
                # --- PROYECCIÓN FUTURA ---
                if history_real:
                    import pandas as pd
                    import numpy as np
                    
                    ultimo_punto = history_real[-1]
                    total_puntos_original = len(resultado["history"])
                    
                    try:
                        dt_start = pd.to_datetime(ultimo_punto["datetime"])
                    except Exception:
                        dt_start = pd.Timestamp.now()
                        
                    history_proyeccion = []
                    num_puntos_proyectar = len(history_real)
                    
                    for k in range(1, num_puntos_proyectar + 1):
                        # Calcular el timestamp futuro sumando el offset correspondiente
                        if tf == "1month":
                            dt_futuro = dt_start + pd.DateOffset(months=k)
                        elif tf == "1week":
                            dt_futuro = dt_start + pd.DateOffset(weeks=k)
                        elif tf == "1d":
                            dt_futuro = dt_start + pd.DateOffset(days=k)
                        elif tf == "1h":
                            dt_futuro = dt_start + pd.DateOffset(hours=k)
                        elif tf == "30m":
                            dt_futuro = dt_start + pd.DateOffset(minutes=30 * k)
                        elif tf == "15m":
                            dt_futuro = dt_start + pd.DateOffset(minutes=15 * k)
                        elif tf == "5m":
                            dt_futuro = dt_start + pd.DateOffset(minutes=5 * k)
                        else:
                            dt_futuro = dt_start + pd.DateOffset(days=k)
                            
                        # El índice secuencial del punto futuro es total_puntos_original + k
                        index_seq_futuro = total_puntos_original + k
                        ciclo_st_futuro = amplitude * np.sin(freq * index_seq_futuro + phase) + offset
                        
                        item_futuro = {
                            "datetime": str(dt_futuro),
                            "price": None,
                            "vol7D": None,
                            "vol60D": None,
                            "sma20": None,
                            "cicloSt": float(ciclo_st_futuro),
                            "bsCall": None,
                            "bsPut": None,
                            "priceA": None,
                            "priceB": None,
                            "isProjection": True
                        }
                        history_proyeccion.append(item_futuro)
                        
                    resultado["history"] = history_real + history_proyeccion
                else:
                    resultado["history"] = history_real

        return resultado

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error calculando ratio {pairA}/{pairB}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/correlations/{base_pair:path}")
async def get_correlations_for_base(base_pair: str, db: Session = Depends(get_db)):
    """
    Calcula los coeficientes de correlación de Pearson para todos los pares del mismo tipo 
    con respecto a un par base (numerador).
    """
    base_pair = unquote(base_pair)
    logger.info(f"Calculando correlaciones para el par base: {base_pair}")
    try:
        # 1. Encontrar el tipo del par base
        base_symbol = db.query(RatioSymbol).filter(RatioSymbol.symbol == base_pair).first()
        if not base_symbol:
            raise HTTPException(status_code=404, detail=f"Símbolo base '{base_pair}' no encontrado en el catálogo.")
        
        tipo_base = base_symbol.tipo
        # 2. Buscar todos los pares activos del mismo tipo
        pares_compatibles = db.query(RatioSymbol).filter(RatioSymbol.Activo == 1, RatioSymbol.tipo == tipo_base).all()
        
        # 3. Cargar las series de tiempo diarias recientes
        series = {}
        for p in pares_compatibles:
            try:
                # Cargar unas 5000 velas de 5m
                df = await getCandlesFromDb(symbol=p.symbol, timeframe="5min", limit=5000)
                if not df.empty:
                    # Agrupar por hora para estabilidad y velocidad
                    df_hourly = df.resample('1H').agg({'close': 'last'}).dropna()
                    series[p.symbol] = df_hourly['close']
            except Exception as e:
                logger.error(f"Error cargando serie temporal para correlación de {p.symbol}: {e}")
                continue
        
        if not series or base_pair not in series:
            return {}
            
        # 4. Combinar series en un único DataFrame para alinear por fechas
        df_combined = pd.DataFrame(series).dropna()
        if df_combined.empty or len(df_combined) < 5:
            # Fallback a alineación directa de 5min sin resample
            series_5m = {}
            for p in pares_compatibles:
                try:
                    df = await getCandlesFromDb(symbol=p.symbol, timeframe="5min", limit=1000)
                    if not df.empty:
                        series_5m[p.symbol] = df['close']
                except Exception as e:
                    continue
            df_combined = pd.DataFrame(series_5m).dropna()
            
        if df_combined.empty:
            return {}
            
        # 5. Calcular matriz de Pearson
        corr_matrix = df_combined.corr(method='pearson')
        
        # Extraer coeficientes de correlación respecto al par base
        correlations = corr_matrix[base_pair].to_dict()
        
        # Estructurar resultado asegurando que todos los pares compatibles tengan un score
        resultado = {}
        for p in pares_compatibles:
            score = correlations.get(p.symbol, 0.0)
            if pd.isna(score):
                score = 0.0
            resultado[p.symbol] = float(score)
            
        return resultado
        
    except Exception as e:
        logger.error(f"Error calculando matriz de correlaciones: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

