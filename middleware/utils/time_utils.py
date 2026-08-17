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
    
    # 1. Descanso Diario (Rollover/Settlement): 17:00 - 18:00 NY
    # Nota: El domingo no hay rollover previo porque es la apertura del mercado.
    if 17 <= hour_ny < 18 and weekday_ny != 6:
        # logger.info(f"⏳ [NY] Horario de Rollover (17:00-18:00) - No opera")
        return True

    # 2. Cierre de Fin de Semana (Viernes 17:00 NY - Domingo 17:00 NY)
    if weekday_ny == 4 and hour_ny >= 17: # Viernes tarde
        return True
    if weekday_ny == 5: # Sábado
        return True
    if weekday_ny == 6 and hour_ny < 17: # Domingo mañana (Apertura a las 17:00 NY)
        return True

    return False

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
            
    # Caso 2: Cierre fin de semana (Viernes 17:00 NY a Domingo 17:00 NY)
    # Abre el Domingo a las 17:00 NY
    weekdayNy = nowNy.weekday()
    hourNy = nowNy.hour
    
    isWeekend = False
    if weekdayNy == 4 and hourNy >= 17: # Viernes tarde
        isWeekend = True
    elif weekdayNy == 5: # Sábado
        isWeekend = True
    elif weekdayNy == 6 and hourNy < 17: # Domingo mañana
        isWeekend = True
        
    if isWeekend:
        # Calcular próximo Domingo a las 17:00 NY
        daysToSunday = (6 - weekdayNy) % 7
        if daysToSunday == 0 and hourNy >= 17:
            # Si ya es domingo después de las 17:00 (no debería entrar aquí)
            daysToSunday = 7
            
        sundayTargetNy = nowNy + timedelta(days=daysToSunday)
        sundayTargetNy = sundayTargetNy.replace(hour=17, minute=0, second=0, microsecond=0)
        
        diff = (sundayTargetNy - nowNy).total_seconds()
        if diff > 0:
            return diff
            
    # Caso 3: Rollover diario (17:00 NY a 18:00 NY)
    # Abre a las 18:00 NY de ese día
    if 17 <= hourNy < 18 and weekdayNy != 6:
        targetNy = nowNy.replace(hour=18, minute=0, second=0, microsecond=0)
        diff = (targetNy - nowNy).total_seconds()
        if diff > 0:
            return diff
            
    # Fallback de seguridad: 5 minutos
    return 300.0

