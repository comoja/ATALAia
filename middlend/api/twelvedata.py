"""
Asynchronous API client for twelvedata.com using httpx.
"""
import logging
import httpx
import pandas as pd

# Assuming the new project structure allows this import.
# If running as a script, this might need path adjustments.
from middlend.configConstants import TWELVE_DATA_API_URL, TWELVE_DATA_CREDIT_LIMIT, TWELVE_DATA_CREDIT_EMERGENCY_THRESHOLD
# A placeholder for alert functions, which should also be modularized.
# from ..core.communications import sendAlert

logger = logging.getLogger(__name__)

async def checkApiCredits(apiKey: str, accountName: str):
    """
    Checks the current usage of a Twelve Data API key and sends an alert if credits are low.
    """
    url = f"{TWELVE_DATA_API_URL}?apikey={apiKey}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, timeout=10.0)
            response.raise_for_status()  # Raise an exception for bad status codes (4xx or 5xx)
            
            data = response.json()
            usedCredits = data.get('currentUsage', 0)
            remainingCredits = TWELVE_DATA_CREDIT_LIMIT - usedCredits

            if remainingCredits < TWELVE_DATA_CREDIT_EMERGENCY_THRESHOLD:
                message = (
                    f"⚠️ *EMERGENCIA:* Cuenta de API **{accountName}** casi sin créditos. "
                    f"Quedan solo {remainingCredits}."
                )
                # This needs a proper implementation of the alert system
                # await sendAlert(message) 
                logger.critical(f"API Key credits running low for {accountName}! Remaining: {remainingCredits}")

    except httpx.RequestError as e:
        logger.error(f"Error de red al verificar créditos de API para {accountName}: {e}")
    except Exception as e:
        logger.error(f"Error inesperado al verificar créditos de API para {accountName}: {e}")


async def getTimeSeries(symbol: str, interval: str, apiKey: str, nVelas: int = 200, accountName: str = None) -> pd.DataFrame | None:
    """
    Downloads time series data for a given symbol.
    """
    accountInfo = f" [{accountName}]" if accountName else ""
    logger.info(f"Descargando {nVelas} velas para {symbol} en intervalo {interval}{accountInfo}")
    url = f"{TWELVE_DATA_API_URL}/time_series"
    params = {
        "symbol": symbol,
        "interval": interval,
        "outputsize": nVelas,
        "apikey": apiKey
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params=params, timeout=20.0)
            response.raise_for_status()
            
            data = response.json()

        if "values" not in data:
            logger.warning(f"Respuesta de API sin 'values' para {symbol}: {data.get('message', 'No message')}")
            return None

        df = pd.DataFrame(data["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").set_index("datetime")
        
        # Coerce columns to numeric, handling potential errors
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        # Ensure volume column exists and is numeric
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
        else:
            df["volume"] = 0
            
        return df.dropna(subset=["close"])

    except httpx.RequestError as e:
        logger.error(f"Error de red en descarga de time series para {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error crítico en descarga de time series para {symbol}: {e}", exc_info=True)
        return None

async def getComplexData(symbol: str, interval: str, apiKey: str) -> pd.DataFrame | None:
    """
    Fetches complex data (price, rsi, cci, macd) in a single API call.
    """
    logger.info(f"Descargando datos complejos para {symbol} en {interval}")
    url = f"{TWELVE_DATA_API_URL}/complex_data"
    payload = {
        "symbols": [symbol],
        "intervals": [interval],
        "outputsize": 30,
        "apikey": apiKey,
        "methods": [
            {"name": "price"},
            {"name": "rsi", "period": 14},
            {"name": "cci", "period": 20},
            {"name": "macd", "fastPeriod": 12, "slowPeriod": 26, "signalPeriod": 9}
        ]
    }
    
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=payload, timeout=20.0)
            response.raise_for_status()
            data = response.json()

        if not data.get('data'):
            logger.warning(f"Respuesta de API sin 'data' en complexData para {symbol}")
            return None

        apiData = data['data'][0]
        df = pd.DataFrame({
            'precio': [float(x['close']) for x in apiData[0]['values']],
            'rsi': [float(x['rsi']) for x in apiData[1]['values']],
            'cci': [float(x['cci']) for x in apiData[2]['values']],
            'macd': [float(x['macd']) for x in apiData[3]['values']]
        })
        # Invertir para que lo más nuevo esté al final
        return df.iloc[::-1].reset_index(drop=True)

    except httpx.RequestError as e:
        logger.error(f"Error de red en descarga de complexData para {symbol}: {e}")
        return None
    except (KeyError, IndexError, TypeError) as e:
        logger.error(f"Error parseando la respuesta de complexData para {symbol}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error crítico en descarga de complexData para {symbol}: {e}", exc_info=True)
        return None
