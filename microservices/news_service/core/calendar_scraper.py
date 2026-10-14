from typing import List
import os
import json
import logging
import urllib.request
import time
from datetime import datetime
import pymysql

logger = logging.getLogger("newsService")

CACHE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
os.makedirs(CACHE_DIR, exist_ok=True)
CACHE_FILE = os.path.join(CACHE_DIR, "ff_calendar_cache.json")
CACHE_TTL = 3600 * 2  # 2 horas de caché para evitar rate limit de Cloudflare (429)

def get_db_connection():
    return pymysql.connect(
        host="127.0.0.1",
        user="root",
        password="M1x&J34ny",
        database="atalaia",
        charset="utf8mb4",
        cursorclass=pymysql.cursors.DictCursor
    )

def fetch_calendar_data():
    """
    Descarga el calendario semanal oficial de ForexFactory respetando la caché local para evitar HTTP 429.
    """
    now_ts = time.time()
    if os.path.exists(CACHE_FILE):
        try:
            mtime = os.path.getmtime(CACHE_FILE)
            if (now_ts - mtime) < CACHE_TTL:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list) and len(data) > 0:
                        logger.info(f"Usando calendario en cache local ({len(data)} eventos, antiguedad: {int((now_ts - mtime)/60)} min).")
                        return data
        except Exception as e:
            logger.warning(f"Error leyendo cache de calendario: {e}")

    url = "https://nfs.faireconomy.media/ff_calendar_thisweek.json"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json"
    }
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            if isinstance(data, list) and len(data) > 0:
                with open(CACHE_FILE, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                logger.info(f"Calendario descargado de ForexFactory exitosamente ({len(data)} eventos).")
                return data
    except Exception as e:
        logger.error(f"Error descargando calendario de ForexFactory: {e}")
        # Si falló la descarga pero hay caché previa (aunque sea vieja), úsala
        if os.path.exists(CACHE_FILE):
            try:
                with open(CACHE_FILE, "r", encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
    return []

def sync_forexfactory_calendar() -> int:
    """
    Descarga el calendario oficial de ForexFactory y lo guarda en las tablas noticias y economic_calendar.
    """
    data = fetch_calendar_data()
    if not data:
        logger.warning("No se obtuvieron eventos para sincronizar.")
        return 0

    conn = get_db_connection()
    inserted_count = 0
    try:
        with conn.cursor() as cursor:
            for item in data:
                title = str(item.get("title", "")).strip()
                currency = str(item.get("country", "")).strip().upper()
                if not title or not currency:
                    continue

                impact_raw = str(item.get("impact", "")).strip().upper()
                # Omitir eventos de bajo impacto o festivos menores
                if impact_raw not in ["HIGH", "MEDIUM"]:
                    continue

                if impact_raw == "HIGH":
                    afectacion = "ALTA"
                    impact_std = "HIGH"
                else:
                    afectacion = "MEDIA"
                    impact_std = "MEDIUM"

                dt_str = item.get("date", "")
                if not dt_str:
                    continue

                try:
                    dt = datetime.fromisoformat(dt_str)
                    clean_ts = dt.strftime("%Y-%m-%d %H:%M:%S")
                except Exception:
                    clean_ts = dt_str.split("+")[0].split("-04:00")[0].replace("T", " ")

                forecast = str(item.get("forecast", "")).strip() or None
                previous = str(item.get("previous", "")).strip() or None
                actual = str(item.get("actual", "")).strip() or None

                summary = f"Evento relevante de {currency}. Impacto: {afectacion}."
                if forecast or previous:
                    summary += f" Previo: {previous or 'N/D'} | Pronostico: {forecast or 'N/D'}."

                # 1. Insertar en tabla noticias
                cursor.execute(
                    """
                    SELECT id FROM noticias 
                    WHERE fechaHora = %s AND divisa = %s AND noticia = %s
                    LIMIT 1
                    """,
                    (clean_ts, currency, title)
                )
                if not cursor.fetchone():
                    cursor.execute(
                        """
                        INSERT INTO noticias 
                        (fechaHora, par, divisa, afectacion, noticia, previo, pronostico, publicado, resumen, fuente)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (clean_ts, currency, currency, afectacion, title, previous, forecast, actual, summary, "ForexFactory")
                    )
                    inserted_count += 1

                # 2. Insertar en tabla economic_calendar si existe
                try:
                    cursor.execute(
                        """
                        SELECT id FROM economic_calendar 
                        WHERE timestamp = %s AND currency = %s AND title = %s
                        LIMIT 1
                        """,
                        (clean_ts, currency, title)
                    )
                    if not cursor.fetchone():
                        cursor.execute(
                            """
                            INSERT INTO economic_calendar 
                            (timestamp, currency, impact, title, forecast, previous, summary, source)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (clean_ts, currency, impact_std, title, forecast, previous, summary, "ForexFactory")
                        )
                except Exception:
                    pass

            conn.commit()
        logger.info(f"Sincronizacion completada: {inserted_count} nuevos eventos insertados en noticias.")
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
