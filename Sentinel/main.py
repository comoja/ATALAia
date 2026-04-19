"""
Main entry point for the refactored Trading Bot.
"""
import asyncio
import logging
import sys
import os
import time
import pandas as pd
import pytz
from datetime import datetime

# --- Path Setup ---
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

# --- Module Imports ---
from Sentinel.utils.loggerConfig import setupLoggingSentinel as setupLogging
from Sentinel.core.Sniper import SniperBot
from Sentinel.core.SMA20_200 import SMABot
from Sentinel.core.ImbalanceNY import ImbalanceNYBot
from Sentinel.core.ImbalanceLDN import ImbalanceLDNBot
from Sentinel.core.EMA20200 import EMA20200Bot
from Sentinel.core.Patron4h import Patron4HBot
from Sentinel.core.SesgoBiasHTF import SesgoBiasHTFBot
from Sentinel.core.SilverBullet import SilverBulletBot
from Sentinel.core.ImbalancePMNY import ImbalancePMNYBot
from Sentinel.core.GenericFVG import GenericFVGBot
from Sentinel.core.FVGDiario import FVGDiarioBot
from Sentinel.ml import model as mlModel
from Sentinel.analysis.technical import calculateFeatures
from middleware.utils.momentum import momentum as momentumAnalyzer, _enviar_resumen_inicial

# Flag para enviar resumen solo una vez
_resumen_momentum_enviado = False
from middleware.config import constants as config
from middleware.database import dbManager
from middleware.database.dbManager import get_min_wait_time
from middleware.utils.communications import sendTelegramAlert, alertaInmediata, deleteTelegramMessage   

# --- External Project Imports ---
from middleware.scheduler.autoScheduler import getTiempoEspera, isRestTime
from Sentinel.data.dataLoader import getParametros
from middleware.config import settings
from middleware.config.constants import API_KEYS, FESTIVOS, TIMEZONE

TIMEZONE_LOCAL = pytz.timezone(TIMEZONE)
MAX_CANDLES_PER_CALL = 5000

def resampleData(df: pd.DataFrame, targetInterval: str) -> pd.DataFrame:
    if targetInterval == "5min":
        return df
    
    intervalMap = {
        "15min": "15min",
        "30min": "30min",
        "1h": "1h",
        "2h": "2h",
        "4h": "4h"
    }
   
    rule = intervalMap.get(targetInterval, targetInterval)
    
    dfCopy = df.copy()
    if dfCopy.index.tzinfo is not None:
        dfCopy.index = dfCopy.index.tz_convert("America/Mexico_City")
    
    dfResampled = dfCopy.resample(rule, closed='right', label='right').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }).dropna()
    
    return dfResampled


from middleware.utils.time_utils import get_localized_session_times
from middleware.api import twelvedata as tdApi

INTERVAL = settings.INTERVAL
INTERVALmax = settings.INTERVALmax


async def checkAndCloseTrades():
    """Check open trades and close if SL or TP is hit."""
    try:
        # Solo verificar trades de cuentas que estén actualmente ACTIVAS
        open_trades = dbManager.getOpenTradesForActiveAccounts()
        if not open_trades:
            return
        
        ahora = datetime.now()
        GRACE_PERIOD_SECONDS = getattr(settings, 'GRACE_PERIOD_SECONDS', 120) # Minutos de gracia parametrizados
        
        for trade in open_trades:
            symbol = trade['symbol']
            direction = trade['direction'].upper()
            entryPrice = float(trade['entryPrice'])
            stopLoss = float(trade['stopLoss'])
            takeProfit = float(trade['takeProfit'])
            size = float(trade['size'])
            idTrade = trade['idTrade']
            openTimeStr = trade['openTime']

            # --- NUEVO: Filtro de periodo de gracia (07/04/2026) ---
            try:
                if isinstance(openTimeStr, str):
                    openTime = datetime.strptime(openTimeStr, "%Y-%m-%d %H:%M:%S")
                else:
                    openTime = openTimeStr # Ya es datetime
                
                # Calcular antigüedad en segundos
                age_seconds = (ahora - openTime).total_seconds()
                if age_seconds < GRACE_PERIOD_SECONDS:
                    logger.info(f"[{symbol}] Omitiendo verificación (Trade recién abierto: {age_seconds:.0f}s < {GRACE_PERIOD_SECONDS}s)")
                    continue
            except Exception as e:
                logger.warning(f"Error calculando antigüedad de trade {idTrade}: {e}")
            
            df = await tdApi.getTimeSeries({"symbol": symbol, "interval": "5min", "outputsize": 20})
            if df is None or df.empty:
                continue
            
            latest = df.iloc[-1]
            currentPrice = float(latest['close'])
            high = float(latest['high'])
            low = float(latest['low'])
            
            closed = False
            exitPrice = 0
            reason = ""
            
            if direction == "LARGO":
                if low <= stopLoss:
                    exitPrice = stopLoss
                    reason = "SL"
                    closed = True
                elif high >= takeProfit:
                    exitPrice = takeProfit
                    reason = "TP"
                    closed = True
            else:  # CORTO
                if high >= stopLoss:
                    exitPrice = stopLoss
                    reason = "SL"
                    closed = True
                elif low <= takeProfit:
                    exitPrice = takeProfit
                    reason = "TP"
                    closed = True
            
            if closed:
                capital_anterior = dbManager.getCuentaCapital(trade['idCuenta'])
                pnl_anterior = float(trade.get('pnl', 0) or 0)
                
                from middleware.execution.broker_gateway import gateway
                await gateway.close_trade(idTrade, exitPrice, reason, capital_anterior, pnl_anterior)
                
    except Exception as e:
        logger.error(f"Error en checkAndCloseTrades: {e}")


async def preload_time_series_data(symbolsToScan, apiKey, interval, nVelas):
    """
    Obtiene los datos de time series una sola vez para todos los símbolos.
    Retorna un diccionario: {symbol: dataframe}
    """
    preloaded_data = {}
    for symbolInfo in symbolsToScan:
        symbol = symbolInfo['symbol']
        logger.info(f"Obteniendo datos de 12Data para {symbol} (intervalo base 5min)...")
        df = await tdApi.getTimeSeries({"symbol": symbol, "interval": "5min", "apikey": apiKey, "outputSize": nVelas})
        if df is not None and len(df) >= 100:
            preloaded_data[symbol] = df
        else:
            logger.warning(f"[{symbol}] Datos insuficientes ({len(df) if df is not None else 0} velas).")
    return preloaded_data


async def run_analysis_for_symbols(sniper_bot, sma_bot, imbalance_ny_bot, imbalance_ldn_bot, imbalance_pm_bot, ema20200_bot, patron4_h_bot, sesgo_bias_htf_bot, silver_bullet_bot, generic_fvg_bot, fvg_diario_bot, symbolsToScan, apiKey, interval, nVelas, alertasNyEnviadas, alertasLdnEnviadas, alertasPmNyEnviadas):
    """
    Ejecuta el análisis concurrentemente (vía asyncio.gather) para las estrategias matemáticas,
    optimizando el rendimiento una vez que la data base (5m) se descargó de 12Data.
    """
    MIN_WAIT_SECONDS = get_min_wait_time()
    
    from middleware.utils.communications import alertaInmediata as _alertaInmediata
    
    for idx, symbolInfo in enumerate(symbolsToScan):
        symbol = symbolInfo['symbol']
        start_time = time.time()
        
        # Obtener API key para este símbolo (rota entre cuentas)
        symbolApiKey, _, nombreKey, _, _ = getParametros()
        logger.info(f"Procesando {symbol} ({idx+1}/{len(symbolsToScan)}) con cuenta {nombreKey}...", extra={"color": "orange"})
        
        # Guardar nombreKey en symbolInfo para usar en logs de descarga
        symbolInfo['cuenta'] = nombreKey
        
        # 1. Descargar datos para este símbolo (siempre 5min - intervalo base)
        logger.info(f"Descargando datos de {dbManager.DATA_SOURCE} para {symbol} [{nombreKey}] (5min)...")
        
        params = {
                "symbol": symbol,
                "interval": "5min",
                "apikey": symbolApiKey,
                "outputSize": MAX_CANDLES_PER_CALL
            }
        df = await tdApi.getTimeSeries(params)
        
        if df is None or len(df) < 100:
            logger.warning(f"[{symbol}] Datos insuficientes. Saltando...")
            if idx < len(symbolsToScan) - 1:
                await asyncio.sleep(MIN_WAIT_SECONDS)
            continue
        
        # Limpiar NaN básicos de los datos descargados
        df = df.dropna(subset=['close', 'high', 'low', 'open'])
        logger.info(f"[{symbol}] Tras limpieza inicial: {len(df)} velas válidas")
        
        if len(df) < 200:
            logger.warning(f"[{symbol}] Datos insuficientes tras limpieza. Saltando...")
            if idx < len(symbolsToScan) - 1:
                await asyncio.sleep(MIN_WAIT_SECONDS)
            continue
        
        # Calcular indicadores técnicos base para el análisis (RSI, MACD, etc.)
        df = calculateFeatures(df)

        # Calcular momentum para este símbolo
        logger.info(f"Calculando momentum para {symbol}...", extra={"color": "dark_orange"})
        try:
            estadosMomentum = await momentumAnalyzer(symbol, df)
            symbolInfo['momentum'] = estadosMomentum
        except Exception as e:
            logger.error(f"[{symbol}] Error calculando momentum: {e}")
            symbolInfo['momentum'] = None
        
        # --- RESAMPLEO LOCAL (Optimización: 06/04/2026) ---
        # Generamos todas las temporalidades necesarias en memoria para evitar latencia de DB
        logger.info(f"[{symbol}] Generando resampleos locales (15min, 1h, 1D)...")
        df15m = resampleData(df, "15min")
        df1h = resampleData(df, "1h")
        from Sentinel.analysis.technical import resample_to_interval
        df1d = resample_to_interval(df, "1D")
        
        preloaded_master = {
            '5m': df,
            '15min': df15m,
            '15m': df15m,
            '1h': df1h,
            '4h': resample_to_interval(df, "4h"),
            '1D': df1d,
            symbol: df
        }
        
        logger.info(f"[{symbol}] 5m: {len(df)}v | 15m: {len(df15m)}v | 1h: {len(df1h)}v | 1D: {len(df1d)}v")
        
        # Enviar resumen de momentum al inicio (solo una vez)
        global _resumen_momentum_enviado
        if not _resumen_momentum_enviado:
            _resumen_momentum_enviado = True
            await _enviar_resumen_inicial({symbol: df})
        
        sym_sniper = symbolInfo.copy()
        sym_sniper['intervalo'] = "15min"
        
        sym_sma = symbolInfo.copy()
        sym_sma['intervalo'] = "15min"
        
        sym_ema = symbolInfo.copy()
        sym_ema['intervalo'] = "1h"
        
        sym_p4h = symbolInfo.copy()
        sym_p4h['intervalo'] = "15min"
        
        sym_sesgo = symbolInfo.copy()
        sym_sesgo['intervalo'] = "1h"
        
        sym_sb = symbolInfo.copy()
        sym_sb['intervalo'] = "5min"
        
        sym_fvg = symbolInfo.copy()

        sym_fvg_diario = symbolInfo.copy()

        tasks = [
            sniper_bot.runAnalysisCycle_for_symbol(sym_sniper, {symbol: df15m}, symbolApiKey),
            sma_bot.runAnalysisCycle_for_symbol(sym_sma, {symbol: df15m}, symbolApiKey),
            ema20200_bot.analyze(sym_ema, {symbol: df1h}),
            patron4_h_bot.runAnalysisCycleForSymbol(sym_p4h, {'15m': df15m}, symbolApiKey),
            sesgo_bias_htf_bot.runAnalysisCycleForSymbol(sym_sesgo, {'4h': preloaded_master['4h']}, symbolApiKey),
            silver_bullet_bot.runAnalysisCycleForSymbol(sym_sb, {symbol: df}, symbolApiKey),
            generic_fvg_bot.analyze(sym_fvg, preloaded_master),
            fvg_diario_bot.runAnalysisCycleForSymbol(sym_fvg_diario, preloaded_master, symbolApiKey)
        ]

        # Configurar Imbalances asíncronos
        async def process_imbalance_ny():
            ahoraMX = datetime.now(pytz.timezone(TIMEZONE))
            inicioAperturaNY, finAperturaNY, cierreNY = get_localized_session_times('America/New_York', 8, 0, 9, 0, 14, 0)
            if ahoraMX > finAperturaNY:
                dfIndex = df.index
                if dfIndex.tz is None:
                    dfIndex = dfIndex.tz_localize(TIMEZONE)
                maskApertura = (dfIndex >= inicioAperturaNY) & (dfIndex < finAperturaNY)
                dfApertura = df.loc[maskApertura]
                if len(dfApertura) > 0:
                    precioMaximo = dfApertura['high'].max()
                    precioMinimo = dfApertura['low'].min()
                    horasDesdeFinApertura = (ahoraMX - finAperturaNY).total_seconds() / 3600
                    if horasDesdeFinApertura <= 4 and symbol not in alertasNyEnviadas:
                        textNivel = f"<b>APERTURA NY 🇺🇸 {symbol}</b>\n<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n━━━━━━━━━━━━━━━\n<center><b>Sesión: {inicioAperturaNY.strftime('%H:%M')} - {finAperturaNY.strftime('%H:%M')}</b></center>\n\n  ⬆️ MAX: {precioMaximo:,.4f}\n  ⬇️ MIN: {precioMinimo:,.4f}\n━━━━━━━━━━━━━━━\n"
                        await _alertaInmediata(1, textNivel)
                        alertasNyEnviadas.add(symbol)
                    
                    dfPostApertura = df.loc[dfIndex >= finAperturaNY]
                    sym_imb_ny = symbolInfo.copy()
                    sym_imb_ny.update({'intervalo': "5min", 'precioMaximo': precioMaximo, 'precioMinimo': precioMinimo, 'finAperturaNY': finAperturaNY, 'cierreNY': cierreNY})
                    await imbalance_ny_bot.runAnalysisCycleForSymbol(sym_imb_ny, {symbol: dfPostApertura}, symbolApiKey)

        async def process_imbalance_ldn():
            ahoraMX = datetime.now(pytz.timezone(TIMEZONE))
            inicioAperturaLDN, finAperturaLDN, cierreLDN = get_localized_session_times('Europe/London', 8, 0, 9, 0, 14, 0)
            if ahoraMX > finAperturaLDN:
                dfIndex = df.index
                if dfIndex.tz is None:
                    dfIndex = dfIndex.tz_localize(TIMEZONE)
                maskAperturaLDN = (dfIndex >= inicioAperturaLDN) & (dfIndex < finAperturaLDN)
                dfAperturaLDN = df.loc[maskAperturaLDN]
                if len(dfAperturaLDN) > 0:
                    precioMaximoLDN = dfAperturaLDN['high'].max()
                    precioMinimoLDN = dfAperturaLDN['low'].min()
                    horasDesdeFinAperturaLDN = (ahoraMX - finAperturaLDN).total_seconds() / 3600
                    if horasDesdeFinAperturaLDN <= 7 and symbol not in alertasLdnEnviadas:
                        textNivelLDN = f"<b>APERTURA LNDN 🇬🇧 {symbol}</b>\n<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n━━━━━━━━━━━━━━━\n<b><center>Sesión: {inicioAperturaLDN.strftime('%H:%M')} - {finAperturaLDN.strftime('%H:%M')}</center></b>\n\n  ⬆️ MAX: {precioMaximoLDN:,.4f}\n  ⬇️ MIN: {precioMinimoLDN:,.4f}\n━━━━━━━━━━━━━━━\n"
                        await _alertaInmediata(1, textNivelLDN)
                        alertasLdnEnviadas.add(symbol)
                        
                    dfPostAperturaLDN = df.loc[dfIndex >= finAperturaLDN]
                    sym_imb_ldn = symbolInfo.copy()
                    sym_imb_ldn.update({'intervalo': "5min", 'precioMaximo': precioMaximoLDN, 'precioMinimo': precioMinimoLDN})
                    await imbalance_ldn_bot.runAnalysisCycleForSymbol(sym_imb_ldn, {symbol: dfPostAperturaLDN}, symbolApiKey)

        async def process_imbalance_pm():
            ahoraMX = datetime.now(pytz.timezone(TIMEZONE))
            inicioAperturaPM, finAperturaPM, cierreNYPM = get_localized_session_times('America/New_York', 14, 0, 15, 0, 17, 0)
            if ahoraMX > finAperturaPM:
                dfIndex = df.index
                if dfIndex.tz is None:
                    dfIndex = dfIndex.tz_localize(TIMEZONE)
                maskAperturaPM = (dfIndex >= inicioAperturaPM) & (dfIndex < finAperturaPM)
                dfAperturaPM = df.loc[maskAperturaPM]
                if len(dfAperturaPM) > 0:
                    precioMaximoPM = dfAperturaPM['high'].max()
                    precioMinimoPM = dfAperturaPM['low'].min()
                    horasDesdeFinAperturaPM = (ahoraMX - finAperturaPM).total_seconds() / 3600
                    if horasDesdeFinAperturaPM <= 4 and symbol not in alertasPmNyEnviadas:
                        textNivelPM = f"<b>APERTURA NY PM 🇺🇸 {symbol}</b>\n<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n━━━━━━━━━━━━━━━\n<b><center>Sesión: {inicioAperturaPM.strftime('%H:%M')} - {finAperturaPM.strftime('%H:%M')}</center></b>\n\n  ⬆️ MAX: {precioMaximoPM:,.4f}\n  ⬇️ MIN: {precioMinimoPM:,.4f}\n━━━━━━━━━━━━━━━\n"
                        await _alertaInmediata(1, textNivelPM)
                        alertasPmNyEnviadas.add(symbol)
                        
                    dfPostAperturaPM = df.loc[dfIndex >= finAperturaPM]
                    sym_imb_pm = symbolInfo.copy()
                    sym_imb_pm.update({'intervalo': "5min", 'precioMaximo': precioMaximoPM, 'precioMinimo': precioMinimoPM, 'finAperturaPM': finAperturaPM, 'cierreNYPM': cierreNYPM})
                    await imbalance_pm_bot.runAnalysisCycleForSymbol(sym_imb_pm, {symbol: dfPostAperturaPM}, symbolApiKey)

        tasks.extend([process_imbalance_ny(), process_imbalance_ldn(), process_imbalance_pm()])
        
        # Ejecutar todos los análisis para este par en paralelo
        logger.info(f"[{symbol}] Ejecutando análisis matemáticos y de patrón en paralelo...")
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for r in results:
            if isinstance(r, Exception):
                logger.error(f"[{symbol}] Excepción en ejecución concurrente: {r}")
        
        # 12. Calcular tiempo total y esperar lo necesario para cumplir 3s mínimo entre descargas
        elapsed = time.time() - start_time
        wait_time = max(0, MIN_WAIT_SECONDS - elapsed)
        
        if wait_time > 0:
            logger.info(f"Esperando {wait_time:.1f}s para cumplir límite de 12Data.com (8 llamadas/min)...\n\n")
            await asyncio.sleep(wait_time)
        else:
            logger.info(f"Ciclo completado en {elapsed:.1f}s (sin espera adicional)\n\n")

# 1. Set up logging at the very beginning
setupLogging(enableConsole=True)
logger = logging.getLogger("sentinel")

async def main():
    """
    Main execution function.
    """
    logger.info("===================================================")
    logger.info("====== Inicializando Bot de Trading Sentinel ======")
    logger.info("===================================================")
    
    # --- Model Loading/Training ---
    # Attempt to load the pre-trained model
    model = mlModel.loadModel(config.MODEL_FILE_PATH)

    if model is None:
        logger.critical("Error: No se encontró modelo ML pre-entrenado (.pkl) en la ruta configurada.")
        logger.critical("Por favor, entrena el modelo offline antes de iniciar Sentinel en productivo.")
        logger.critical("Bot detenido. No se puede continuar sin el orquestador predictivo de ML.")
        return

    # --- Bot Initialization ---
    sniper_bot = SniperBot(mlModelInstance=model)
    sma_bot = SMABot()
    imbalance_ny_bot = ImbalanceNYBot()
    imbalance_ldn_bot = ImbalanceLDNBot()
    ema20200_bot = EMA20200Bot()
    patron4_h_bot = Patron4HBot()
    sesgo_bias_htf_bot = SesgoBiasHTFBot()
    silver_bullet_bot = SilverBulletBot()
    imbalance_pm_bot  = ImbalancePMNYBot()
    generic_fvg_bot = GenericFVGBot()
    fvg_diario_bot = FVGDiarioBot()
    
    ALERTS_FILE = os.path.join(rutaRaiz, 'Sentinel', 'logs', 'alerts_flags.json')
    import json

    def load_alert_flags():
        hoy = datetime.now().date().isoformat()
        if os.path.exists(ALERTS_FILE):
            try:
                with open(ALERTS_FILE, 'r') as f:
                    data = json.load(f)
                    if data.get('date') == hoy:
                        return (
                            set(data.get('ny', [])),
                            set(data.get('ldn', [])),
                            set(data.get('pm', []))
                        )
            except Exception as e:
                logger.error(f"Error cargando flags de alerta: {e}")
        return set(), set(), set()

    def save_alert_flags(ny_set, ldn_set, pm_set):
        hoy = datetime.now().date().isoformat()
        try:
            os.makedirs(os.path.dirname(ALERTS_FILE), exist_ok=True)
            with open(ALERTS_FILE, 'w') as f:
                json.dump({
                    'date': hoy,
                    'ny': list(ny_set),
                    'ldn': list(ldn_set),
                    'pm': list(pm_set)
                }, f)
        except Exception as e:
            logger.error(f"Error guardando flags de alerta: {e}")

    alertasNyEnviadas, alertasLdnEnviadas, alertasPmNyEnviadas = load_alert_flags()
    lastAlertDate = datetime.now().date()
    
    def resetAlertFlags():
        nonlocal lastAlertDate, alertasNyEnviadas, alertasLdnEnviadas, alertasPmNyEnviadas
        hoy = datetime.now().date()
        if lastAlertDate != hoy:
            lastAlertDate = hoy
            alertasNyEnviadas.clear()
            alertasLdnEnviadas.clear()
            alertasPmNyEnviadas.clear()
            save_alert_flags(alertasNyEnviadas, alertasLdnEnviadas, alertasPmNyEnviadas)
            logger.info(f"[ALERTAS] Flags de alertas reseteados para fecha: {hoy}")
    
    logger.info("Bot inicializado correctamente. Iniciando bucle principal...")

    # --- Main Loop ---
    wasOperating = True  # Assume we're operating initially
    
    while True:
        try:
            isOperating = not isRestTime()
            
            # Detectar cambio de estado
            if wasOperating and not isOperating:
                # Se acaba de detener (viernes 17:00)
                logger.info("\n\n====== MERCADO CERRADO - Bot detenido ======\n\n")
            elif not wasOperating and isOperating:
                # Se acaba de iniciar (domingo 17:00)
                logger.info("\n\n====== MERCADO ABIERTO - Bot iniciado ======\n\n")
            
            wasOperating = isOperating
            
            if isOperating:
                resetAlertFlags()
                logger.info("Iniciando ciclo de análisis...")
                
                logger.info("Verificando trades abiertos...")
                await checkAndCloseTrades()
                
                await asyncio.sleep(5)
                
                # Obtener parámetros ANTES del análisis
                apiKey, intervaloActual, nombreKey, nVelas, _ = getParametros()
                
                # Obtener símbolos a analizar
                symbolsToScan = dbManager.getSymbols()
                
                # Ejecutar análisis de forma CONCURRENTE (descarga -> gather(bots) -> espera 5s)
                await run_analysis_for_symbols(sniper_bot, sma_bot, imbalance_ny_bot, imbalance_ldn_bot, imbalance_pm_bot, ema20200_bot, patron4_h_bot, sesgo_bias_htf_bot, silver_bullet_bot, generic_fvg_bot, fvg_diario_bot, symbolsToScan, apiKey, INTERVAL, nVelas, alertasNyEnviadas, alertasLdnEnviadas, alertasPmNyEnviadas)
                
                # Persistir estados en memoria de las alertas al finalizar el frame
                save_alert_flags(alertasNyEnviadas, alertasLdnEnviadas, alertasPmNyEnviadas)
                
                # Calcular espera para el PRÓXIMO ciclo (Siempre 5 minutos para mantener reactividad)
                proximaEspera = 5
                
                from middleware.scheduler.autoScheduler import get_next_sync_time
                proximo = get_next_sync_time(proximaEspera)
                hora_ejecucion = proximo.strftime("%H:%M:%S")
                
                logger.info(f"✅ Ciclo completado. Próxima ejecución: {hora_ejecucion} (Frecuencia 5 min)\n")
                await getTiempoEspera(proximaEspera)
            
            else:
                logger.info("Mercado cerrado o en horario de descanso. Durmiendo.")
                _, _, _, _, esperaMin = getParametros()
                await getTiempoEspera(esperaMin)

        except Exception as e:
            logger.critical(f"Ocurrió un error inesperado en el bucle principal: {e}", exc_info=True)
            logger.info("Reiniciando bucle después de 60 segundos de espera...")
            await asyncio.sleep(60)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido manualmente. ¡Adiós!")
        sys.exit(0)
    except Exception as e:
        logger.critical(f"Ocurrió un error fatal fuera del bucle principal: {e}", exc_info=True)
        sys.exit(1)
