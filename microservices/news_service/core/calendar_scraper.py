from typing import List
import json
import logging
import urllib.request
from datetime import datetime
import pymysql

logger = logging.getLogger("newsService")

def get_db_connection():
    return pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="M1x&J34ny",
        database="atalaia",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )

def sync_forexfactory_calendar() -> int:
    """
    Descarga el calendario semanal oficial de ForexFactory y lo guarda en economic_calendar.
    """
    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except Exception as e:
        logger.error(f"Error descargando calendario economico: {e}")
        return 0

    conn = get_db_connection()
    inserted_count = 0
    try:
        with conn.cursor() as cursor:
            for item in data:
                title = str(item.get("title", "")).strip()
                currency = str(item.get("country", "")).strip().upper()
                impact_raw = str(item.get("impact", "")).strip().upper()
                if impact_raw not in ["HIGH", "MEDIUM", "LOW"]:
                    impact = "LOW" if impact_raw != "HOLIDAY" else "NONE"
                else:
                    impact = impact_raw

                dt_str = item.get("date", "")
                if not dt_str:
                    continue

                # Parse ISO date with timezone (e.g. 2026-09-27T19:50:00-04:00)
                try:
                    dt = datetime.fromisoformat(dt_str)
                    clean_ts = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    clean_ts = dt_str.split("+")[0].split("-04:00")[0].replace("T", " ")

                forecast = str(item.get("forecast", "")).strip() or None
                previous = str(item.get("previous", "")).strip() or None

                # Resumen para el tooltip
                summary = f"Evento relevante de {currency}. Impacto: {impact}."
                if forecast or previous:
                    summary += f" Previo: {previous or 'N/D'} | Pronóstico: {forecast or 'N/D'}."

                # Evitar duplicados exactos
                cursor.execute(
                    """
                    SELECT id FROM economic_calendar 
                    WHERE timestamp = %s AND currency = %s AND title = %s
                    LIMIT 1
                    """,
                    (clean_ts, currency, title)
                )
                exists = cursor.fetchone()
                if not exists:
                    cursor.execute(
                        """
                        INSERT INTO economic_calendar 
                        (timestamp, currency, impact, title, forecast, previous, summary, source)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (clean_ts, currency, impact, title, forecast, previous, summary, "ForexFactory")
                    )
                    inserted_count += 1
            conn.commit()
        logger.info(f"Sincronizados {inserted_count} nuevos eventos en economic_calendar.")
        return inserted_count
    finally:
        conn.close()

def extract_currencies_from_pair(pair: str) -> List[str]:
    parts = pair.replace("-", "/").split("/")
    currs = []
    for p in parts:
        cleaned = p.strip().upper()
        if len(cleaned) == 3:
            currs.append(cleaned)
    return currs
