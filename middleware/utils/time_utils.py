import pytz
from datetime import datetime, timedelta
import logging
from middleware.config.constants import TIMEZONE, DATA_SOURCE, bypassRestTime

logger = logging.getLogger(__name__)

def get_localized_session_times(session_tz_name, start_h, start_m, end_h, end_m, close_h, close_m):
    """
    Calcula los horarios de una sesión en tiempo real, convirtiendo del huso 
    nativo al huso local (Mexico City) de forma automática para manejar DST.
    """
    local_tz = pytz.timezone(TIMEZONE)
    session_tz = pytz.timezone(session_tz_name)
    
    # Obtener fecha actual en el huso de la sesión
    now_session = datetime.now(session_tz)
    
    # Crear objetos datetime para inicio, fin y cierre en el huso nativo
    start_native = now_session.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    end_native = now_session.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
    close_native = now_session.replace(hour=close_h, minute=close_m, second=0, microsecond=0)
    
    # Convertir a Mexico City
    start_local = start_native.astimezone(local_tz)
    end_local = end_native.astimezone(local_tz)
    close_local = close_native.astimezone(local_tz)
    
    return start_local, end_local, close_local

def is_market_closed(dt=None):
    """
    Determina si el mercado Forex está cerrado basado en el horario de Nueva York.
    Forex abre el domingo a las 17:00 NY y cierra el viernes a las 17:00 NY.
    También hay un descanso diario de 17:00 a 18:00 NY (Rollover).
    """
    ny_tz = pytz.timezone('America/New_York')
    local_tz = pytz.timezone(TIMEZONE) # 'America/Mexico_City'
    
    if dt is None:
        now_ny = datetime.now(ny_tz)
    else:
        # Si dt no tiene zona horaria, asumimos que es local (Mexico City)
        if dt.tzinfo is None:
            now_ny = local_tz.localize(dt).astimezone(ny_tz)
        else:
            # Si es aware (podría venir de ZoneInfo o pytz), astimezone lo maneja bien
            now_ny = dt.astimezone(ny_tz)

    weekday_ny = now_ny.weekday() # 0=Monday, ..., 6=Sunday
    hour_ny = now_ny.hour
    
    # 1. Descanso Diario (Rollover/Settlement): 17:00 - 18:00 NY (todos los días)
    if 17 <= hour_ny < 18:
        # logger.info(f"⏳ [NY] Horario de Rollover (17:00-18:00) - No opera")
        return True

    # 2. Cierre de Fin de Semana (Viernes 17:00 NY - Domingo 18:00 NY / 17:00 CDMX)
    if weekday_ny == 4 and hour_ny >= 17: # Viernes tarde
        return True
    if weekday_ny == 5: # Sábado
        return True
    if weekday_ny == 6 and hour_ny < 18: # Domingo hasta apertura oficial a las 18:00 NY (17:00 CDMX)
        return True

    return False

MEXICAN_IPC_STOCKS = {
    "AC", "ALSEA", "AMX", "BBAJIO", "BIMBO", "CEMEX", "CUERVO", "FEMSA",
    "GAP", "GCC", "GFNORTE", "GMEXICO", "GRUMA", "KIMBER", "KOF", "LIVEPOL",
    "MEGACABLE", "OMA", "ORBIA", "PENOLES", "PINFRA", "TLEVISA", "VESTA", "WALMEX"
}

def is_crypto_symbol(symbol: str) -> bool:
    if not symbol:
        return False
    s = str(symbol).upper().replace("/", "").replace("-", "").replace("_", "").strip()
    return any(c in s for c in ["BTC", "ETH", "SOL", "XRP", "CRYPTO", "DOGE"])

def is_symbol_market_open(symbol: str, dt=None) -> bool:
    """
    Determina si el mercado para un activo específico está abierto actualmente:
    - Criptomonedas (BTC, ETH, etc.): Abiertas 24/7/365.
    - Emisoras Mexicanas (BMV): Lunes a Viernes 08:30 - 15:00 CDMX (09:30 - 16:00 NY).
    - Acciones / ETFs US (NYSE, NASDAQ como EWW, AAPL, SPY): Lunes a Viernes 09:30 - 16:00 NY. Cierra fin de semana completo.
    - Forex / Metales / Otros: Domingo 18:00 NY a Viernes 17:00 NY (con rollover diario 17:00-18:00 NY).
    """
    if not symbol:
        return True
    clean = str(symbol).upper().replace("/", "").replace("_", "").replace("-", "").strip()
    if is_crypto_symbol(clean):
        return True

    ny_tz = pytz.timezone('America/New_York')
    local_tz = pytz.timezone(TIMEZONE)
    if dt is None:
        now_ny = datetime.now(ny_tz)
    else:
        now_ny = dt.astimezone(ny_tz) if dt.tzinfo else local_tz.localize(dt).astimezone(ny_tz)

    weekday = now_ny.weekday()  # 0=Lunes, ..., 6=Domingo
    hour = now_ny.hour
    minute = now_ny.minute

    # Sábado: Cerrado para todo mercado tradicional
    if weekday == 5:
        return False

    # Acciones (BMV o US Stocks / ETFs como EWW)
    is_mexican = clean in MEXICAN_IPC_STOCKS
    is_us_stock = clean in {"EWW", "AAPL", "AMZN", "MSFT", "GOOGL", "TSLA", "SPY", "QQQ", "NVDA"} or (
        not any(fx in clean for fx in ["USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "NZD", "MXN", "SGD", "HKD"])
    )

    if is_mexican or is_us_stock:
        # Domingo completo: CERRADO
        if weekday == 6:
            return False
        # Horario de negociación bursátil regular: 09:30 a 16:00 NY
        if hour < 9 or (hour == 9 and minute < 30) or hour >= 16:
            return False
        return True

    # Forex / Metales / Materias primas estándar
    return not is_market_closed(dt)

def is_ratio_market_open(numerador: str, denominador: str, dt=None) -> bool:
    """
    En una estrategia de Ratio (pares balanceados), AMBOS mercados deben estar
    abiertos simultáneamente para poder evaluar y operar el ciclo.
    Si cualquiera de las dos patas está cerrada, se pausa la señal para evitar desbalance.
    """
    return is_symbol_market_open(numerador, dt) and is_symbol_market_open(denominador, dt)

def isRestTime(dt=None):
    """
    Determina si el mercado está en periodo de descanso o cierre (Weekend/Rollover).
    Es la función central utilizada por Sentinel y DataSymbol para decidir si operar.
    Incluye el descanso obligatorio de madrugada (00:00 - 06:00 MX).
    """
    if bypassRestTime:
        return False
        
    local_tz = pytz.timezone(TIMEZONE)
    if dt is None:
        now_local = datetime.now(local_tz)
    else:
        # Asegurar que dt sea aware en el huso local para la comparación de horas
        if dt.tzinfo is None:
            now_local = local_tz.localize(dt)
        else:
            now_local = dt.astimezone(local_tz)
            
    # 1. Descanso obligatorio de madrugada (00:20 a 05:00 AM Mexico City)
    # Se omite en modo Forex (MT5) a petición del usuario
    if DATA_SOURCE != "forex":
        if (now_local.hour == 0 and now_local.minute >= 20) or (1 <= now_local.hour < 5):
            return True
        
    return is_market_closed(dt)

def get_sleep_minutes(dt=None):
    """
    Calcula cuántos minutos debe dormir el bot si está en periodo de descanso.
    Basado en la lógica: 15 min en madrugada, 5 min en otros descansos.
    """
    local_tz = pytz.timezone(TIMEZONE)
    if dt is None:
        now_local = datetime.now(local_tz)
    else:
        if dt.tzinfo is None:
            now_local = local_tz.localize(dt)
        else:
            now_local = dt.astimezone(local_tz)
            
    # Si estamos en la ventana de madrugada (00:20 - 05:00), dormir más tiempo
    # Se omite en modo Forex (MT5)
    if DATA_SOURCE != "forex":
        if (now_local.hour == 0 and now_local.minute >= 20) or (1 <= now_local.hour < 5):
            return 15
    return 5

def get_seconds_to_next_sync(intervaloMinutos: int) -> float:
    """
    Calcula cuántos segundos faltan para llegar al próximo múltiplo exacto de minutos.
    Ejemplo: Si son las 19:48:20 e intervalo=5, devuelve los segundos hasta las 19:50:00.
    """
    local_tz = pytz.timezone(TIMEZONE)
    now = datetime.now(local_tz)
    
    # Calcular cuántos minutos han pasado desde el inicio de la hora
    minutes_now = now.minute
    minutes_to_next = intervaloMinutos - (minutes_now % intervaloMinutos)
    
    # Crear el objeto datetime del próximo objetivo
    next_sync = now.replace(second=0, microsecond=0) + timedelta(minutes=minutes_to_next)
    
    # Diferencia en segundos
    diff = (next_sync - now).total_seconds()
    
    # Margen de seguridad: Si faltan menos de 15 segundos, saltar al siguiente ciclo 
    # para evitar despertar justo antes de que cambie el minuto
    if diff < 15:
        next_sync += timedelta(minutes=intervaloMinutos)
        diff = (next_sync - now).total_seconds()
        
    return max(0.0, diff)

def get_last_closed_candle(dt, interval, df=None):

    """
    Devuelve la última vela cerrada para un intervalo dado.
    Si se proporciona un DataFrame (df), devuelve la fila completa (Series).
    De lo contrario, devuelve el timestamp (datetime).
    """
    if dt.tzinfo is None:
        localTz = pytz.timezone(TIMEZONE)
        dt = localTz.localize(dt)

    interval = int(interval)
    if interval <= 0:
        raise ValueError("interval debe ser mayor a 0 minutos")

    # Calcular el inicio de la vela actual usando minutos desde medianoche.
    # Esto funciona para 5m/15m/1h/4h/1d; la vela actual siempre sigue abierta.
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    minutes_since_midnight = dt.hour * 60 + dt.minute
    current_candle_minutes = (minutes_since_midnight // interval) * interval
    current_candle = day_start + timedelta(minutes=current_candle_minutes)
    lastCandle = current_candle - timedelta(minutes=interval)
    
    if df is not None:
        try:
            targetCandle = lastCandle
            if df.index.tz is None and targetCandle.tzinfo is not None:
                targetCandle = targetCandle.replace(tzinfo=None)
            elif df.index.tz is not None and targetCandle.tzinfo is None:
                # Convert to index timezone
                targetCandle = targetCandle.replace(tzinfo=pytz.UTC).astimezone(df.index.tz)
            
            # Buscar la vela exacta en el DataFrame
            if targetCandle in df.index:
                return df.loc[targetCandle]
            else:
                # Si no está la exacta, devolver la última disponible que sea <= targetCandle
                availableCandles = df[df.index <= targetCandle]
                if not availableCandles.empty:
                    return availableCandles.iloc[-1]
        except Exception as e:
            logger.error(f"Error buscando vela en DF: {e}")
            
    return lastCandle

def get_seconds_until_market_opens(dt=None) -> float:
    """
    Calcula exactamente cuántos segundos faltan para que termine el periodo de descanso (isRestTime = False)
    y el mercado/sesión abra de nuevo.
    """
    if bypassRestTime:
        return 0.0
        
    localTz = pytz.timezone(TIMEZONE)
    nyTz = pytz.timezone('America/New_York')
    
    if dt is None:
        nowLocal = datetime.now(localTz)
    else:
        if dt.tzinfo is None:
            nowLocal = localTz.localize(dt)
        else:
            nowLocal = dt.astimezone(localTz)
            
    nowNy = nowLocal.astimezone(nyTz)
    
    # Caso 1: Madrugada local (00:20 a 05:00 AM Mexico City)
    # Abre a las 05:00 AM local. Se omite en modo Forex (MT5)
    if DATA_SOURCE != "forex":
        if (nowLocal.hour == 0 and nowLocal.minute >= 20) or (1 <= nowLocal.hour < 5):
            targetLocal = nowLocal.replace(hour=5, minute=0, second=0, microsecond=0)
            diff = (targetLocal - nowLocal).total_seconds()
            if diff > 0:
                return diff
            
    # Caso 2: Cierre fin de semana (Viernes 17:00 NY a Domingo 18:00 NY / 17:00 CDMX)
    # Abre el Domingo a las 18:00 NY (17:00 CDMX)
    weekdayNy = nowNy.weekday()
    hourNy = nowNy.hour
    
    isWeekend = False
    if weekdayNy == 4 and hourNy >= 17: # Viernes tarde
        isWeekend = True
    elif weekdayNy == 5: # Sábado
        isWeekend = True
    elif weekdayNy == 6 and hourNy < 18: # Domingo antes de apertura
        isWeekend = True
        
    if isWeekend:
        # Calcular próximo Domingo a las 18:00 NY
        daysToSunday = (6 - weekdayNy) % 7
        if daysToSunday == 0 and hourNy >= 18:
            daysToSunday = 7
            
        sundayTargetNy = nowNy + timedelta(days=daysToSunday)
        sundayTargetNy = sundayTargetNy.replace(hour=18, minute=0, second=0, microsecond=0)
        
        diff = (sundayTargetNy - nowNy).total_seconds()
        if diff > 0:
            return diff
            
    # Caso 3: Rollover diario (17:00 NY a 18:00 NY)
    # Abre a las 18:00 NY de ese día
    if 17 <= hourNy < 18:
        targetNy = nowNy.replace(hour=18, minute=0, second=0, microsecond=0)
        diff = (targetNy - nowNy).total_seconds()
        if diff > 0:
            return diff
            
    # Fallback de seguridad: 5 minutos
    return 300.0

