import os
import sys
import logging
from typing import Optional, List
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import pymysql

service_dir = os.path.dirname(os.path.abspath(__file__))
if service_dir not in sys.path:
    sys.path.insert(0, service_dir)

from core.models import NewsEventItem, RatioNewsResponse
from core.calendar_scraper import extract_currencies_from_pair, get_db_connection

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | newsService | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("newsService")

app = FastAPI(
    title="ATALA.ia Economic News Service",
    description="Microservicio independiente para calendario y eventos macroeconómicos relevantes a los Ratios basado en tabla noticias.",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
def root():
    return {
        "status": "ok",
        "service": "ATALA.ia Economic News Service",
        "port": 8007,
        "table": "noticias"
    }

@app.get("/api/v1/news/ping")
def ping():
    return {"status": "pong", "service": "NewsService", "table": "noticias"}

@app.get("/api/v1/news/ratio/{idRatio}", response_model=RatioNewsResponse)
def get_news_for_ratio_id(
    idRatio: int,
    impact: Optional[str] = None,
    limit: int = 40
):
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT id, numerador, denominador FROM user_ratios WHERE id = %s", (idRatio,))
            ratio = cursor.fetchone()
            if not ratio:
                raise HTTPException(status_code=404, detail=f"Ratio id {idRatio} no encontrado en user_ratios.")

            pairA = ratio["numerador"]
            pairB = ratio["denominador"]

        currencies = list(set(extract_currencies_from_pair(pairA) + extract_currencies_from_pair(pairB)))
        pairs = [pairA, pairB]

        events = query_events_from_db(currencies=currencies, pairs=pairs, impact=impact, limit=limit)
        return RatioNewsResponse(
            status="success",
            idRatio=idRatio,
            pairA=pairA,
            pairB=pairB,
            currencies=currencies,
            totalEvents=len(events),
            events=events
        )
    finally:
        conn.close()

@app.get("/api/v1/news/timeline", response_model=RatioNewsResponse)
def get_news_timeline(
    pairA: str,
    pairB: str,
    idRatio: Optional[int] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    impact: Optional[str] = None,
    limit: int = 50
):
    currencies = list(set(extract_currencies_from_pair(pairA) + extract_currencies_from_pair(pairB)))
    pairs = [pairA, pairB]

    events = query_events_from_db(currencies=currencies, pairs=pairs, startDate=startDate, endDate=endDate, impact=impact, limit=limit)
    return RatioNewsResponse(
        status="success",
        idRatio=idRatio,
        pairA=pairA,
        pairB=pairB,
        currencies=currencies,
        totalEvents=len(events),
        events=events
    )

def query_events_from_db(
    currencies: List[str],
    pairs: List[str],
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    impact: Optional[str] = None,
    limit: int = 50
) -> List[NewsEventItem]:
    conn = get_db_connection()
    try:
        with conn.cursor() as cursor:
            all_match_terms = tuple(set(currencies + pairs))
            where_clauses = ["(divisa IN %s OR par IN %s)"]
            params = [all_match_terms, all_match_terms]

            if startDate:
                where_clauses.append("fechaHora >= %s")
                params.append(startDate)
            if endDate:
                where_clauses.append("fechaHora <= %s")
                params.append(endDate)
            if impact and impact.upper() in ["ALTA", "MEDIA", "BAJA"]:
                where_clauses.append("afectacion = %s")
                params.append(impact.upper())

            where_str = " AND ".join(where_clauses)
            sql = f"""
                SELECT id, fechaHora, par, divisa, afectacion, noticia, previo, pronostico, publicado, resumen, fuente
                FROM noticias
                WHERE {where_str}
                ORDER BY fechaHora ASC
                LIMIT %s
            """
            params.append(limit)
            cursor.execute(sql, tuple(params))
            rows = cursor.fetchall()

            items = []
            for r in rows:
                ts_val = r["fechaHora"]
                ts_str = ts_val.strftime("%Y-%m-%d %H:%M:%S") if hasattr(ts_val, "strftime") else str(ts_val)
                af = str(r["afectacion"]).upper()
                imp = "HIGH" if af == "ALTA" else ("MEDIUM" if af == "MEDIA" else "LOW")
                items.append(NewsEventItem(
                    id=r["id"],
                    fechaHora=ts_str,
                    currency=r["divisa"],
                    impact=imp,
                    title=r["noticia"],
                    forecast=r["pronostico"],
                    previous=r["previo"],
                    actual=r["publicado"],
                    summary=r["resumen"],
                    source=r["fuente"] or "CalendarioEconomico"
                ))
            return items
    finally:
        conn.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8007, reload=False)
