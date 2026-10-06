import os
import sys
import logging
from typing import Optional, Dict, Any, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import pymysql
import pandas as pd

# Path setup
service_dir = os.path.dirname(os.path.abspath(__file__))
if service_dir not in sys.path:
    sys.path.insert(0, service_dir)

from core.models import ForecastRequest, ForecastResponse
from core.ou_engine import compute_ratio_forecast

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | forecastService | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("forecastService")

app = FastAPI(
    title="ATALA.ia Ratio Forecast Service",
    description="Microservicio cuantitativo para pronóstico de cruces de ratio basado en Reversión a la Media (Ornstein-Uhlenbeck).",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_connection():
    return pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="M1x&J34ny",
        database="atalaia",
        charset="utf8mb4"
    )

def fetch_candles_for_symbol(symbol: str, timeframe: str = "1h", limit: int = 400) -> pd.DataFrame:
    conn = get_db_connection()
    try:
        tf_lower = str(timeframe).lower()
        if "5m" in tf_lower:
            sql = """
                SELECT timestamp, close as closePrice 
                FROM candles 
                WHERE symbol = %s AND timeframe = "5min"
                ORDER BY timestamp DESC 
                LIMIT %s
            """
            df = pd.read_sql(sql, conn, params=(symbol, limit))
            if not df.empty:
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                df["closePrice"] = pd.to_numeric(df["closePrice"], errors="coerce")
                df = df.sort_values("timestamp").reset_index(drop=True)
                return df
        
        # Para 1h, 4h, 1d o fallback, consultar velas 5min y resamplear
        sql = """
            SELECT timestamp, close as closePrice 
            FROM candles 
            WHERE symbol = %s AND timeframe = "5min"
            ORDER BY timestamp DESC 
            LIMIT %s
        """
        df = pd.read_sql(sql, conn, params=(symbol, limit * 12))
        if not df.empty:
            df["timestamp"] = pd.to_datetime(df["timestamp"])
            df["closePrice"] = pd.to_numeric(df["closePrice"], errors="coerce")
            df = df.set_index("timestamp").sort_index()
            
            rule_map = {"1h": "1h", "4h": "4h", "15min": "15min", "30min": "30min", "1d": "1D"}
            rule = rule_map.get(tf_lower, "1h")
            resampled = df.resample(rule).agg({"closePrice": "last"}).dropna().reset_index()
            return resampled.tail(limit).reset_index(drop=True)

        # Fallback a stockprices si candles estuviera vacio
        sql_sp = """
            SELECT date as timestamp, closePrice 
            FROM stockprices 
            WHERE symbol = %s 
            ORDER BY date DESC 
            LIMIT %s
        """
        df_sp = pd.read_sql(sql_sp, conn, params=(symbol, limit))
        if not df_sp.empty:
            df_sp["timestamp"] = pd.to_datetime(df_sp["timestamp"])
            df_sp["closePrice"] = pd.to_numeric(df_sp["closePrice"], errors="coerce")
            df_sp = df_sp.sort_values("timestamp").reset_index(drop=True)
            return df_sp
        return pd.DataFrame()
    finally:
        conn.close()

@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "ATALA.ia Ratio Forecast Service",
        "port": 8006,
        "description": "Prediccion cuantitativa del proximo cruce de ratios"
    }

@app.get("/api/v1/forecast/ping")
def ping():
    return {"status": "pong", "service": "ForecastService"}

@app.post("/api/v1/forecast/project", response_model=ForecastResponse)
def project_from_history(req: ForecastRequest):
    """
    Proyecta el cruce del ratio directamente a partir del historial provisto por el frontend.
    Tiempo de respuesta ultrarrápido (< 5ms) sin llamadas adicionales a base de datos.
    """
    try:
        raw_history = []
        if req.history:
            for p in req.history:
                if isinstance(p, dict):
                    raw_history.append(p)
                elif hasattr(p, "dict"):
                    raw_history.append(p.dict())
                elif hasattr(p, "model_dump"):
                    raw_history.append(p.model_dump())
        if not raw_history:
            dfA = fetch_candles_for_symbol(req.pairA, req.timeframe, limit=250)
            dfB = fetch_candles_for_symbol(req.pairB, req.timeframe, limit=250)
            if dfA.empty or dfB.empty:
                raise HTTPException(status_code=404, detail="No se encontraron velas para los pares especificados.")
            merged = pd.merge(dfA, dfB, on="timestamp", suffixes=("_A", "_B")).dropna()
            raw_history = [
                {
                    "datetime": row["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                    "priceA": float(row["closePrice_A"]),
                    "priceB": float(row["closePrice_B"])
                }
                for _, row in merged.iterrows()
            ]

        result = compute_ratio_forecast(
            candles_history=raw_history,
            pairA=req.pairA,
            pairB=req.pairB,
            timeframe=req.timeframe,
            idRatio=req.idRatio,
            smaPeriod=req.smaPeriod,
            fixedMinA=req.fixedMinA,
            fixedMaxA=req.fixedMaxA,
            fixedMinB=req.fixedMinB,
            fixedMaxB=req.fixedMaxB
        )
        return ForecastResponse(**result)

    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except Exception as e:
        logger.error(f"Error procesando pronostico: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/forecast/ratio/{idRatio}", response_model=ForecastResponse)
def forecast_by_ratio_id(
    idRatio: int,
    timeframe: Optional[str] = None
):
    """
    Consulta la configuracion del ratio en user_ratios por idRatio,
    obtiene las velas de la BD y calcula el pronostico del siguiente cruce.
    """
    conn = get_db_connection()
    try:
        with conn.cursor(pymysql.cursors.DictCursor) as cursor:
            cursor.execute(
                """
                SELECT id, numerador, denominador, Temporalidad, EMARapida, 
                       fixedMinA, fixedMaxA, fixedMinB, fixedMaxB
                FROM user_ratios 
                WHERE id = %s
                """,
                (idRatio,)
            )
            ratio = cursor.fetchone()
            if not ratio:
                raise HTTPException(status_code=404, detail=f"Ratio id {idRatio} no encontrado en user_ratios.")

        pairA = ratio["numerador"]
        pairB = ratio["denominador"]
        tf = timeframe if timeframe else (ratio.get("Temporalidad") or "1h")
        smaPeriod = int(ratio.get("EMARapida") or 2)
        minA = float(ratio["fixedMinA"]) if ratio["fixedMinA"] is not None else None
        maxA = float(ratio["fixedMaxA"]) if ratio["fixedMaxA"] is not None else None
        minB = float(ratio["fixedMinB"]) if ratio["fixedMinB"] is not None else None
        maxB = float(ratio["fixedMaxB"]) if ratio["fixedMaxB"] is not None else None

        dfA = fetch_candles_for_symbol(pairA, tf, limit=300)
        dfB = fetch_candles_for_symbol(pairB, tf, limit=300)
        if dfA.empty or dfB.empty:
            raise HTTPException(status_code=404, detail=f"No hay velas suficientes para los pares {pairA} y {pairB}.")

        merged = pd.merge(dfA, dfB, on="timestamp", suffixes=("_A", "_B")).dropna()
        raw_history = [
            {
                "datetime": row["timestamp"].strftime("%Y-%m-%d %H:%M:%S"),
                "priceA": float(row["closePrice_A"]),
                "priceB": float(row["closePrice_B"])
            }
            for _, row in merged.iterrows()
        ]

        result = compute_ratio_forecast(
            candles_history=raw_history,
            pairA=pairA,
            pairB=pairB,
            timeframe=tf,
            idRatio=idRatio,
            smaPeriod=smaPeriod,
            fixedMinA=minA,
            fixedMaxA=maxA,
            fixedMinB=minB,
            fixedMaxB=maxB
        )
        return ForecastResponse(**result)

    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8006, reload=False)
