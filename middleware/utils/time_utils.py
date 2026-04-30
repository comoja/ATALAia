import pytz
from datetime import datetime, timedelta
import logging
from middleware.config.constants import TIMEZONE

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
    """
    return is_market_closed(dt)

def get_last_closed_candle(dt, interval_minutes):
    """
    Devuelve la hora de la última vela cerrada para un intervalo dado.
    Ej: si ahora es 10:07 y el intervalo es 5, devuelve 10:05.
    """
    if dt.tzinfo is None:
        local_tz = pytz.timezone(TIMEZONE)
        dt = local_tz.localize(dt)
    
    # Calcular el inicio de la vela actual
    minute = (dt.minute // interval_minutes) * interval_minutes
    last_candle = dt.replace(minute=minute, second=0, microsecond=0)
    
    # Si la vela calculada es la actual (aún no cierra), retroceder un intervalo
    if last_candle >= dt:
        last_candle = last_candle - timedelta(minutes=interval_minutes)
    
    return last_candle
