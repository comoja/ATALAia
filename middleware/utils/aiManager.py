import os
import logging
import requests
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

logger = logging.getLogger("sentinel")

OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

import httpx
import asyncio

async def getMarketSentiment(headlines: list) -> float:
    """
    Analiza una lista de titulares y devuelve un score de sentimiento entre -1.0 y 1.0 usando httpx.
    """
    if not OPENROUTER_API_KEY:
        logger.error("OPENROUTER_API_KEY no configurada en .env")
        return 0.0
    
    if not headlines:
        return 0.0

    prompt = (
        "Eres un analista experto en mercados financieros. Analiza los siguientes titulares "
        "y devuelve un único valor numérico entre -1.0 (muy bajista/miedo) y 1.0 (muy alcista/codicia). "
        "Responde ÚNICAMENTE con el número, sin texto adicional.\n\n"
        "Titulares:\n" + "\n".join([f"- {h}" for h in headlines])
    )

    try:
        payload = {
            "model": "google/gemini-2.0-flash-001",
            "messages": [
                {"role": "user", "content": prompt}
            ],
            "temperature": 0.1
        }
        
        headers = {
            "Authorization": f"Bearer {OPENROUTER_API_KEY}",
            "Content-Type": "application/json",
            "HTTP-Referer": "https://atalaia-system.com",
            "X-Title": "ATALAia Trading System"
        }
        
        async with httpx.AsyncClient() as client:
            response = await client.post(
                OPENROUTER_URL,
                headers=headers,
                json=payload,
                timeout=12.0
            )
            
            if response.status_code == 200:
                result = response.json()
                content = result['choices'][0]['message']['content'].strip()
                clean_content = "".join(c for c in content if c in "0123456789.-")
                try:
                    score = float(clean_content)
                    return max(-1.0, min(1.0, score))
                except:
                    return 0.0
            return 0.0
            
    except Exception as e:
        logger.error(f"Error en getMarketSentiment (httpx): {e}")
        return 0.0

async def validateSignalContext(symbol: str, direction: str, context_data: str) -> dict:
    """
    Usa un LLM para validar un setup de forma asíncrona.
    """
    if not OPENROUTER_API_KEY:
        return {"valid": True, "reason": "AI Validation disabled", "confidence": 1.0}

    prompt = (
        f"Analiza si una operación de {direction} en {symbol} es prudente...\n"
        f"{context_data}\n\n"
        "Responde en formato JSON: {\"valid\": bool, \"reason\": \"string breve\", \"confidence\": float (0-1)}"
    )

    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(
                OPENROUTER_URL,
                headers={
                    "Authorization": f"Bearer {OPENROUTER_API_KEY}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": "google/gemini-2.0-flash-001",
                    "messages": [{"role": "user", "content": prompt}],
                    "response_format": { "type": "json_object" }
                },
                timeout=15.0
            )
            if response.status_code == 200:
                import json
                return json.loads(response.json()['choices'][0]['message']['content'])
            return {"valid": True, "reason": "Error en consulta AI", "confidence": 0.5}
    except Exception as e:
        logger.error(f"Error en validateSignalContext: {e}")
        return {"valid": True, "reason": f"Exception: {e}", "confidence": 0.0}
