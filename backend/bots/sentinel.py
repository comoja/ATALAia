import sys
import os
import asyncio
import requests
import pandas as pd
import numpy as np
import talib as ta
import pytz
from sklearn.ensemble import RandomForestClassifier
from datetime import datetime
import time
import warnings
import logging

# Esto detecta la carpeta 'backend' y la registra en Python
ruta_raiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if ruta_raiz not in sys.path:
    sys.path.insert(0, ruta_raiz)



from database import dbManager
from scheduler.autoScheduler import getTiempoEspera, isRestTime
from core.comm import enviarAlerta, alertaInmediata
from utils.momentum import momentum

from data.dataLoader import getParametros, nombre_key
from config import settings
from config.settings import SYMBOLS, RISK_REWARD, VELAS_HISTORIAL, tiempoEspera, FESTIVOS, INTERVAL, timeZone,INTERVALmax
from core.logger_config import setup_logging

# 1. Configura el sistema de logs antes que nada
setup_logging()

# 2. Crea el logger específico para este archivo
import logging
logger = logging.getLogger(__name__)

# Silencia específicamente las advertencias de sklearn
warnings.filterwarnings("ignore", category=UserWarning, module="sklearn")
# O silencia todas las advertencias (más radical)
warnings.filterwarnings("ignore")

estadosPorSimbolo = {}

# ==============================
# BOT LOGIC
# ==============================
def calcularPosicion(capital, porcentaje_ganancia, distanciaSl, symbols):
    try:
        # 1. Riesgo por trade: Si buscas 10% mensual, arriesgas ~0.33% diario.
        # riesgoDinero es la cantidad máxima en USD a perder si toca el SL.
        riesgoDinero = capital * (porcentaje_ganancia / 100 )
        
        tipo = symbols.get('tipo', '').upper()
        symbol = symbols.get('symbol', '').upper()
        
        if distanciaSl <= 0: return 0.01

        # 2. METALES (XAU/USD) - 1 Lote = 100 Onzas. 1 pip (0.10) = $10 USD
        if tipo == "METALES":
            # distanciaSl en oro suele ser ej: 1.50 (15 pips). 
            # Lotes = Riesgo / (Distancia * 100)
            lotes = riesgoDinero / (distanciaSl * 100)
            
            return max(0.01, round(lotes, 2)), riesgoDinero

        # 3. ÍNDICES (SPX500, NAS100, US30) - 1 Contrato = $1 por punto
        elif tipo == "INDICES":
            # Si el SL es de 10 puntos y arriesgas $50, son 5 contratos (5.0 lotes)
            contratos = riesgoDinero / distanciaSl
            contrato *= 10
            return max(1, round(contratos, 1)), riesgoDinero

        # 4. CRIPTOS (BTC/USD) - 1 Lote = 1 Unidad (BTC)
        elif tipo == "CRIPTO":
            unidades = riesgoDinero / distanciaSl
            return max(0.01, round(unidades, 2)), riesgoDinero

        # 5. FOREX (EUR/USD, GBP/JPY, etc.) 
        else:
            # ERROR ORIGINAL CORREGIDO:
            # En Forex 1 lote estándar (100k) paga $10 USD por cada PIP (0.0001)
            # Necesitamos convertir la distancia del precio a PIPS.
            
            pip_value = 0.01 if "JPY" in symbol else 0.0001
            pips_distancia = distanciaSl / pip_value
            
            lotes =  int((riesgoDinero / pips_distancia) * (10 if pips_distancia<1 else 1)) 
            lotes = 1 if lotes==0 else lotes
            # Limite de seguridad para no sobreapalancar 
            max_lotes = int(capital/lotes )
            lote_final = min(lotes, max_lotes)

            riesgoDinero *= max(1, lote_final)
        
            lote_final *= 1000
            return max(1000, lote_final), riesgoDinero

    except Exception as e:
        logger.error(f"❌ Error en calcularPosicion: {e}")
        return 0


async def verificar_emergencia_creditos(api_key, nombre):
    url = f"https://api.twelvedata.com?apikey={api_key}"
    try:
        res = requests.get(url).json()
        usado = res.get('current_usage', 0)
        restante = 750 - usado
        if restante < 50:
            await enviarAlerta(f"⚠️ *EMERGENCIA:* Cuenta **{nombre}** casi vacía. Quedan solo {restante} créditos.")
            logger.critical(f"¡CUENTA {nombre} AGOTÁNDOSE! Restan {restante}")
    except:
        pass

def verificarCierreTrade(self, df, tradeData):
    """
    Analiza el DataFrame para detectar si el precio tocó SL o TP.
    Calcula el PNL neto si hubo cierre.
    """
    try:
        side = tradeData['direction'].upper()
        entryPrice = tradeData['entryPrice']
        stopLoss = tradeData.get('stopLoss')
        takeProfit = tradeData.get('takeProfit')
        size = tradeData.get('size', 0)
        comisionFija = tradeData.get('commission', 0)

        for timestamp, row in df.iterrows():
            cerro = False
            exitPrice = 0
            
            # Lógica para COMPRA (Long)
            if side == "BUY":
                if stopLoss and row['low'] <= stopLoss:
                    exitPrice = stopLoss
                    cerro = True
                elif takeProfit and row['high'] >= takeProfit:
                    exitPrice = takeProfit
                    cerro = True
            
            # Lógica para VENTA (Short)
            elif side == "SELL":
                if stopLoss and row['high'] >= stopLoss:
                    exitPrice = stopLoss
                    cerro = True
                elif takeProfit and row['low'] <= takeProfit:
                    exitPrice = takeProfit
                    cerro = True

            if cerro:
                # Cálculo de PNL Bruto
                if side == "BUY":
                    pnlBruto = (exitPrice - entryPrice) * size
                else:
                    pnlBruto = (entryPrice - exitPrice) * size
                
                # PNL Neto (restando comisión)
                pnlNeto = pnlBruto - comisionFija
                
                return {
                    "status": "CLOSED",
                    "exitPrice": exitPrice,
                    "closeTime": timestamp,
                    "pnl": pnlNeto,
                    "slippage": 0 # Puedes calcularlo si tienes el precio esperado vs real
                }
        
        return None # Sigue abierto

    except Exception as e:
        logger.error(f"Error en verificarCierreTrade: {e}")
        return None




def verificarNivelesTrade(df, side, entryPrice, stopLoss, takeProfit):
    """
    Analiza el DataFrame para ver si el precio tocó el SL o TP.
    Retorna un diccionario con el resultado o None si sigue abierto.
    """
    try:
        # Solo analizamos desde que el trade entró (asumiendo que el DF ya está filtrado por tiempo)
        for timestamp, row in df.iterrows():
            
            if side.upper() == "BUY":
                # En COMPRA: El Low toca el SL, el High toca el TP
                if stopLoss and row['low'] <= stopLoss:
                    return {"status": "CLOSED", "reason": "SL", "exitPrice": stopLoss, "closeTime": timestamp}
                
                if takeProfit and row['high'] >= takeProfit:
                    return {"status": "CLOSED", "reason": "TP", "exitPrice": takeProfit, "closeTime": timestamp}
            
            elif side.upper() == "SELL":
                # En VENTA: El High toca el SL, el Low toca el TP
                if stopLoss and row['high'] >= stopLoss:
                    return {"status": "CLOSED", "reason": "SL", "exitPrice": stopLoss, "closeTime": timestamp}
                
                if takeProfit and row['low'] <= takeProfit:
                    return {"status": "CLOSED", "reason": "TP", "exitPrice": takeProfit, "closeTime": timestamp}
        
        return None # El trade sigue activo
        
    except Exception as e:
        logger.error(f"Error al verificar niveles: {e}")
        return None

def calcularComisionPorcentaje(precio, tamaño, tarifaPct=0.001):
    # tarifaPct 0.001 es el 0.1%
    return (precio * tamaño) * tarifaPct

    # Ejemplo: BTC a 50,000 con 0.5 unidades
    #comision = calcularComisionPorcentaje(50000, 0.5, 0.001) # Resultado: 25.0

def calcularComisionFija(tamaño, tarifaPorLote=7.0):
    # tarifaPorLote es el costo por 1 unidad completa
    return tamaño * tarifaPorLote

    # Ejemplo: 2 lotes con tarifa de 7 USD
    #comision = calcularComisionFija(2, 7.0) # Resultado: 14.0

def obtenerComisionEstimada(self, symbol, precio, tamaño):
    # Lógica personalizada por activo
    if "USD" in symbol:
        return (precio * tamaño) * 0.0006  # 0.06% para pares mayores
    else:
        return 1.50  # Tarifa plana para el resto

# Luego en tu método verificarCierreTrade:
#comisionTotal = self.obtenerComisionEstimada(tradeData['symbol'], exitPrice, size)
#pnlNeto = pnlBruto - comisionTotal
    
def calcularPeriodosDinamicos( df):
    # 1. Calculamos volatilidad relativa (ATR / Close)
    # Usamos .copy() para evitar advertencias de SettingWithCopy
    df = df.copy()
    df['volatilidad'] = ta.ATR(df.high, df.low, df.close, timeperiod=14) / df.close
    
    # 2. Manejo de nulos (importante para las primeras 100 velas)
    df['volatilidad'] = df['volatilidad'].ffill().bfill()
    
    # 3. Estadísticas de los últimos 100 periodos
    rolling_mean = df['volatilidad'].rolling(window=100)
    avgVol = rolling_mean.mean().iloc[-1]
    stdVol = rolling_mean.std().iloc[-1]
    volActual = df['volatilidad'].iloc[-1]

    # 4. Lógica de selección de periodos
    # Muy volátil (Z-Score > 1)
    if volActual > (avgVol + stdVol): 
        return {
            "cci": 20, 
            "rsi": 21, 
            "macd": (24, 52, 18) # (Fast, Slow, Signal)
        }
    # Muy calmado (Z-Score < -1)
    elif volActual < (avgVol - stdVol): 
        return {
            "cci": 9, 
            "rsi": 7, 
            "macd": (6, 13, 5)
        }
    # Estándar
    else:
        return {
            "cci": 14, 
            "rsi": 14, 
            "macd": (12, 26, 9)
        }

def pendienteRSI(serie_rsi, ventana=3):
    """
    Calcula la pendiente de la línea de regresión del RSI.
    :param serie_rsi: Serie de Pandas con los valores del RSI.
    :param ventana: Cantidad de periodos hacia atrás para medir la inclinación.
    :return: Serie con el valor de la pendiente (m).
    """
    # Creamos un array de índices x (0, 1, 2...) para la regresión
    x = np.arange(ventana)
    
    def obtener_slope(y):
        if len(y) < ventana: return np.nan
        # Aplicamos la fórmula de mínimos cuadrados para obtener la pendiente (m)
        m, b = np.polyfit(x, y, 1)
        return m

    # Aplicamos el cálculo de forma rodante (rolling)
    return serie_rsi.rolling(window=ventana).apply(obtener_slope)

def getPendiente( serie, periodos=3):
    y = serie.iloc[-periodos:].values
    x = np.arange(periodos)
    if len(y) < periodos: return 0
    # Regresión lineal simple para obtener la pendiente (m)
    m, b = np.polyfit(x, y, 1)
    return m


async def download12Data(symbol, n_velas):
    logger.info(f"Descargando datos para {symbol} de la cuenta {nombre_key} con temporalidad {intervalo}")
    url = f"https://api.twelvedata.com/time_series?symbol={symbol}&interval={intervalo}&outputsize={n_velas}&apikey={api_key_activa}"
    global response
    try:
        response = requests.get(url).json()
        if "values" not in response:
            logger.warning(f"Respuesta sin valores para {symbol}: {response.get('message')}")
            return None

        df = pd.DataFrame(response["values"])
        df["datetime"] = pd.to_datetime(df["datetime"])
        df = df.sort_values("datetime").set_index("datetime")
        try:
            global estadosPorSimbolo
            estadosPorSimbolo = await momentum(symbol, df)
        except Exception as e:
            logger.error(f"Error procesando {symbol}: {e}")
        
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        
        if "volume" in df.columns:
            df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
        else:
            df["volume"] = pd.Series(0, index=df.index)
            
        return df.dropna(subset=["close"])
    except Exception as e:
        logger.error(f"Error crítico en descarga {symbol}: {e}", exc_info=True)
        return None

def getSubdatos(symbol,interval):        
    # Solicitamos Precio + RSI + CCI + MACD en una sola llamada técnica
    url = f"https://api.twelvedata.com/complex_data?apikey={api_key_activa}"
    payload = {
        "symbols": [symbol],
        "intervals": [interval],
        "outputsize": 30,
        "methods": [
            {"name": "price"},
            {"name": "rsi", "period": 14},
            {"name": "cci", "period": 20},
            {"name": "macd", "fast_period": 12, "slow_period": 26, "signal_period": 9}
        ]
    }
    
    response = requests.post(url, json=payload).json()
    
    # Estructuramos los datos en un DataFrame
    try:
        data = response['data'][0]
        df = pd.DataFrame({
            'precio': [float(x['close']) for x in data[0]['values']],
            'rsi':    [float(x['rsi']) for x in data[1]['values']],
            'cci':    [float(x['cci']) for x in data[2]['values']],
            'macd':   [float(x['macd']) for x in data[3]['values']]
        })
        return df.iloc[::-1].reset_index(drop=True) # Invertir para que lo más nuevo esté al final
    except Exception as e:
        logger.error(f"Error obteniendo datos: {e}")
        return None
    
async def analyzeSymbol(symbols, n_velas):
    cuentas = dbManager.getAccount()
    global ultimoEstado
    ultimoEstado = None
    
    df = await download12Data(symbols['symbol'], n_velas )

    if df is None or len(df) < 100:
        logger.info( "velas descargadas: " + str(n_velas)+ " tamaño del df "+ str(len(df))  +"\n" )
        logger.warning(f"⚠️ {symbols['symbol']}: Datos insuficientes.")
        return

    try:
        """
        resultado = verificarNivelesTrade(df, "BUY", 50000, 49500, 51000)

        if resultado:
            # Preparamos la data para tu base de datos
            datosParaActualizar = {
                "idCuenta": 1,
                "symbol": "BTC/USD",
                "direction": "BUY",
                "exitPrice": resultado["exitPrice"],
                "closeTime": resultado["closeTime"],
                "pnl": (resultado["exitPrice"] - 50000) # Ejemplo simple de PNL
            }
            """
        # dbManager.gestionarTrade(datosParaActualizar)
       
        # --- Cálculo de Indicadores Base ---
        ema20 = df["close"].ewm(span=20, adjust=False).mean()
        ema50 = df["close"].ewm(span=50, adjust=False).mean()
        df["ema20"], df["ema50"] = ema20, ema50
        df["ema_dist"] = (df["close"] - ema20) / df["close"]
        df["ema_trend"] = (ema20 - ema50) / df["close"]
        df["slope_ema50"] = ema50.pct_change(12)
        df["atr"] = ta.ATR(df["high"], df["low"], df["close"], 14)
        
        # --- Indicadores Dinámicos ---
        periodo = calcularPeriodosDinamicos(df)
        df["rsi"] = ta.RSI(df["close"], periodo['rsi'])
        df['cci'] = ta.CCI(df['high'].values, df['low'].values, df['close'].values, timeperiod=periodo['cci'])
        
        # --- Volumen y Momentum ---
        df["vol_sma"] = df["volume"].rolling(window=24).mean()
        df["vol_ratio"] = np.where(df["vol_sma"] > 0, df["volume"] / df["vol_sma"], 1.0)
        df["vol_regime"] = df["atr"] / df["atr"].rolling(60).mean()
        
        for i in range(1, 6): 
            df[f"lag{i}"] = df["close"].pct_change(i)
            
        macd, macdsignal, macdhist = ta.MACD(df['close'], fastperiod=periodo['macd'][0], slowperiod=periodo['macd'][1], signalperiod=periodo['macd'][2])
        df["macd"], df["macd_sig"], df["macd_hist"] = macd, macdsignal, macdhist
        df["macd_norm"] = df["macd_hist"] / df["close"]
        
        # --- Pendientes ---
        df["pendiente_rsi"] = pendienteRSI(df["rsi"], ventana=3)
        df["pendiente_cci"] = pendienteRSI(df["cci"], ventana=3)

        # --- Configuración de Target para ML ---
        atr_avg = df["atr"].rolling(60).mean()
        vol_relative_val = (df["atr"] / atr_avg).iloc[-1]
        horizonte = 12 if vol_relative_val < 0.8 else 5 if vol_relative_val > 1.2 else 8
        df["target"] = (df["close"].shift(-horizonte) > (df["close"] + df["atr"] * 0.5)).astype(int)

        # --- Limpieza de Datos (CORREGIDO: Coma añadida en "cci") ---
        features = ["rsi", "atr", "ema_dist", "ema_trend", "slope_ema50", "vol_ratio", "cci",
                    "lag1", "lag2", "lag3", "vol_regime", "macd_hist", "macd_norm", "pendiente_rsi"]
        
        df_clean = df.replace([np.inf, -np.inf], np.nan).dropna(subset=features + ["target"]).copy()
        
        if len(df_clean) < 100:
            logger.info( " tamaño del df "+ str(len(df_clean))  +"\n" )
            logger.warning(f"⚠️ {symbols['symbol']}: Datos insuficientes tras limpieza.")
            return

        # --- Valores Actuales ---
        close = df_clean["close"].iloc[-1]
        rsi_val = df_clean["rsi"].iloc[-1]
        atr_val = df_clean["atr"].iloc[-1]
        hist_val = df_clean["macd_hist"].iloc[-1]
        hist_anterior = df_clean["macd_hist"].iloc[-2]
        pendiente_rsi_val = df_clean["pendiente_rsi"].iloc[-1]
        vol_ratio = df_clean["vol_ratio"].iloc[-1]
        ema50_last = df_clean["ema50"].iloc[-1]
        cci_val = df_clean["cci"].iloc[-1]
        pendiente_cci_val = df_clean["pendiente_cci"].iloc[-1]
        prev_hist_val = df_clean["macd_hist"].iloc[-2] 
        # --- Modelo Random Forest ---
        X = df_clean[features]
        y = df_clean["target"]
        model = RandomForestClassifier(n_estimators=150, max_depth=7, random_state=42, n_jobs=-1)
        model.fit(X.iloc[:-12], y.iloc[:-12])
        
        proba_val = model.predict_proba(X.iloc[-1:])[0][1]

        # --- Gestión de Riesgo Base ---
        atr_multiplier = 1.15 if (proba_val >= 0.65 or proba_val <= 0.35) else 1.5
        distancia_sl = atr_val * atr_multiplier
        vol_porcentaje = (atr_val / close) * 100
        fuerza_vol = "ALTA 🔥" if vol_porcentaje > 0.15 else "BAJA ❄️" if vol_porcentaje < 0.05 else "NORMAL ⚡"
        vela_engulfing = ta.CDLENGULFING(df_clean['open'], df_clean['high'], df_clean['low'], df_clean['close']).iloc[-1]
        vela_hammer = ta.CDLHAMMER(df_clean['open'], df_clean['high'], df_clean['low'], df_clean['close']).iloc[-1]
        vela_star = ta.CDLSHOOTINGSTAR(df_clean['open'], df_clean['high'], df_clean['low'], df_clean['close']).iloc[-1]
        

        # 1. Alertas de Sobrecompra/Venta
        msg_rsi = ""
        

        # --- 2. DECISIÓN EXPERTA (VERSIÓN SNIPER) ---
        direction = None
        confianza = 0
        if  vol_porcentaje < 0.05:
            logger.info(f" la volatilidad es {fuerza_vol}  se descarta señal" )
            mensaje = (
                f"Procesando simbolo {symbols['symbol']} \n"  
                f" la volatilidad es {fuerza_vol}  se descarta señal"
            )
            alertaInmediata(4, mensaje)
            return
        
        # Umbrales de Probabilidad Ajustados (Más exigentes)
        # Antes: 0.65 / 0.35 | Ahora: 0.72 / 0.28
        umbralLargo = 0.72
        umbralCorto = 0.28

        # --- LÓGICA DE FILTRADO TÉCNICO (EL VETO) ---
        # Solo permitimos LARGO si el precio NO está cayendo con fuerza
        tecnicoApoyaLargo = (pendiente_cci_val > 2 and pendiente_rsi_val > -0.2)
        
        # Solo permitimos CORTO si el precio NO está subiendo con fuerza
        tecnicoApoyaCorto = (pendiente_cci_val < -2 and pendiente_rsi_val < 0.2)

        # --- CONFLUENCIA MAESTRA ---
        esLargoExperto = (
            proba_val >= umbralLargo and 
            hist_val > hist_anterior and 
            tecnicoApoyaLargo and
            rsi_val < 75 # Evitamos comprar en el techo absoluto
        )

        esCortoExperto = (
            proba_val <= umbralCorto and 
            hist_val < hist_anterior and 
            tecnicoApoyaCorto and
            rsi_val > 25 # Evitamos vender en el piso absoluto
        )

        # --- ASIGNACIÓN CON BONO DE CONFLUENCIA ---

        # Identificamos el estado del MACD para los bonos
        esCruceAlcista = (prev_hist_val <= 0 and hist_val > 0)
        esCruceBajista = (prev_hist_val >= 0 and hist_val < 0)
        impulsoCreciendo = (hist_val > prev_hist_val)
        impulsoBajando = (hist_val < prev_hist_val)

        # Aseguramos que proba_val sea un número simple (float)
        probActual = float(proba_val[0]) if hasattr(proba_val, "__len__") else float(proba_val)

        if esLargoExperto:
            direction = "LARGO"
            # Bono: +12 si cruza cero, +5 si el histograma solo está creciendo
            bono = 12 if esCruceAlcista else 5 if impulsoCreciendo else 0
            confianza = (probActual * 100) + bono
            
        elif esCortoExperto:
            direction = "CORTO"
            # Bono: +12 si cruza cero, +5 si el histograma solo está bajando
            bono = 12 if esCruceBajista else 5 if impulsoBajando else 0
            confianza = ((1 - probActual) * 100) + bono
        else:
            logger.info(f" Si no hay confluencia, {proba_val >= umbralLargo} and {hist_val > hist_anterior} and {tecnicoApoyaLargo} and {rsi_val < 75}  saltamos al siguiente símbolo")            
            return

        # --- LÓGICA DE VELAS EN SCALPING ---
        # Bono extra por patrón de vela a favor (+10%)
        if direction == "LARGO" and (vela_engulfing > 0 or vela_hammer > 0):
            confianza *= 1.10
        elif direction == "CORTO" and (vela_engulfing < 0 or vela_star < 0):
            confianza *= 1.10
        else:
            confianza *= 0.50
        
        # --- 3. FILTRO DE TENDENCIA DINÁMICO (EMA 50) ---
        # Si no hay una confianza brutal (>90%), prohibido ir contra la EMA50

        if confianza < 90:
            if (direction == "LARGO" and close < ema50_last) or (direction == "CORTO" and close > ema50_last):
                logger.info(f"⚠️ {symbols['symbol']} Filtrado: Intento de contratendencia con confianza baja.")
                return
        # Resumen de señal de vela
        msg_vela = ""
        if vela_engulfing != 0:
            msg_vela = " ENVOLVENTE"
        if vela_hammer != 0:
            msg_vela += " MARTILLO"
        if vela_star != 0:
            msg_vela += " ESTRELLA"
        
        if (vela_engulfing > 0 or vela_hammer > 0):
            msg_vela += " ALCISTA 🟢"
        elif (vela_engulfing < 0 or vela_star < 0):
            msg_vela += " BAJISTA 🔴"
        else:
            msg_vela += " LATERAL ⚪"


        # FILTRO AGRESIVO: Si hay un patrón de vela fuerte EN CONTRA, cancelamos (Veto)
        if direction == "LARGO" and (vela_engulfing < 0 or vela_star < 0):
            logger.info(f"⚠️ {symbols['symbol']} Veto: Patrón de vela bajista {msg_vela} detectado en señal larga.")
            return
        if direction == "CORTO" and (vela_engulfing > 0 or vela_hammer > 0):
            logger.info(f"⚠️ {symbols['symbol']} Veto: Patrón de vela alcista {msg_vela} detectado en señal corta.")
            return
        if vela_star != 0:
            logger.info(f"⚠️ {symbols['symbol']} Veto: Patrón de vela ESTRELLA {msg_vela} detectado en señal {direction}.")
            return
        
        # --- Bucle de Cuentas y Decisiones ---
        for cuenta in cuentas:
            # --- DETECCIÓN DE ACCIÓN DEL PRECIO (Scalping) ---
            # Usamos TA-Lib para detectar patrones clave
            
            trade = {
                "idCuenta": cuenta['idCuenta'],
                "symbol": symbols['symbol']
            }
            if rsi_val >= 68:
                msg_rsi = "🟩🟩🟩 <b>SOBRECOMPRA</b> 🟩🟩🟩\n"
            elif rsi_val <= 32:
                msg_rsi = "🟥🟥🟥 <b>SOBREVENTA</b> 🟥🟥🟥\n"        
            if msg_rsi != "":
                msg_rsi += (
                    f"━━━━━━━━━━━━━━━━\n"
                    f"<center>{symbols['symbol']} ({intervalo})</center>\n"
                    f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
                    f"━━━━━━━━━━━━━━━━\n"
                    f"• RSI: {rsi_val:.2f} | "
                    f"Pend.: {pendiente_rsi_val:.2f} {'🟢' if pendiente_rsi_val > 0 else '🔴'}\n"
                    f"• CCI: {cci_val:.2f} | "
                    f"Pend.: {pendiente_cci_val:.2f} {'🟢' if pendiente_cci_val > 0 else '🔴'}\n"
                    f"• MACD: {'ALCISTA 🟢' if hist_val > 0 else 'BAJISTA 🔴'}\n"
                    f"• Volatilidad: {vol_porcentaje:.3f}% ({fuerza_vol})\n"
                    f"• Vela: {msg_vela}\n"
                    f"━━━━━━━━━━━━━━━━\n"
                )
            if cuenta['idCuenta'] == 1:
                await enviarAlerta(cuenta['idCuenta'], cuenta['TokenMsg'], msg_rsi)
            
            # 4. Parámetros de Operación
            miCapital = float(cuenta['Capital'])
            lote_sugerido , riesgoDinero = calcularPosicion(miCapital,cuenta['ganancia'], distancia_sl, symbols) 
            if lote_sugerido is None: continue

            # Ratio Dinámico
            ratioBase = 2.2 if confianza > 85 else 2.0
            bonoVolumen = 0.3 if vol_ratio > 1.5 else 0.15 if vol_ratio > 1.2 else 0.0
            bonoImpulso = 0.2 if abs(pendiente_cci_val) > 15 else 0.0
            ratioDinamico = min(ratioBase + bonoVolumen + bonoImpulso, 2.2)
            
            
            # Niveles Finales
            logger.info(f"close {close} distanciasl {distancia_sl} ratio {ratioDinamico}")

            sl = (close - distancia_sl if direction == "LARGO" else close + distancia_sl ) 
            tp = (close + (distancia_sl * ratioDinamico ) if direction == "LARGO" else close - (distancia_sl * ratioDinamico)) 
            
            punto_be = close + (distancia_sl * 0.5) if direction == "LARGO" else close - (distancia_sl * 0.5)
            

            # --- Aquí enviarías la orden a tu Exchange/Base de Datos ---
            ulabel = "TAKE PROFIT" if direction == "LARGO" else "STOP LOSS"
            uemoji = "🟢" if direction == "LARGO" else "🔴"
            uvalor = tp if direction == "LARGO" else sl
            upmensaje = f"{uemoji} <b>{ulabel}: {uvalor:,.5f}</b>"
            label = "TAKE PROFIT" if direction == "CORTO" else "STOP LOSS"
            emoji = "🟢" if direction == "CORTO" else "🔴"
            valor = tp if direction == "CORTO" else sl
            dnmensaje = f"{emoji} <b>{label}: {valor:,.5f}</b>"
            
            text = (
                
                f"{'🟩🟩🟩' if direction == 'LARGO' else '🟥🟥🟥'}"
                f" <b>SEÑAL DE { 'COMPRA' if direction == 'LARGO' else 'VENTA' }</b> "
                f"{'🟩🟩🟩' if direction == 'LARGO' else '🟥🟥🟥'}\n"
                f"<center><b>        {symbols['symbol']}</b> ({intervalo})</center>\n"
                f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
                f"━━━━━━━━━━━━━━━\n"
                f"<center>Confianza: <b>{confianza:.1f}%</b></center> \n"
                f"<center>VELA: {msg_vela}</center>"
                f"{'\n<center><b>CON PRECAUCION</b></center> \n' if msg_vela == ' LATERAL ⚪' else '\n'}"
                f"━━━━━━━━━━━━━━━\n"
                f"{upmensaje}\n"
                f"🛡️ Break even: {punto_be:,.6f}\n"
                f"🔹 ENTRADA:   <b>{close:,.6f}</b>\n"
                f"{dnmensaje}\n"
                
                f"━━━━━━━━━━━━━━━\n"
                f"<center><b>DATOS TÉCNICOS:</b></center>\n"
                f"• RSI: {rsi_val:.2f} | "
                f"Pend.: {pendiente_rsi_val:.2f} {'🟢' if pendiente_rsi_val > 0 else '🔴'}\n"
                f"• CCI: {cci_val:.2f} | "
                f"Pend.: {pendiente_cci_val:.2f} {'🟢' if pendiente_cci_val > 0 else '🔴'}\n"
                f"• MACD: {'ALCISTA 🟢' if hist_val > 0 else 'BAJISTA 🔴'}\n"
                f"• Volatilidad: <b>{vol_porcentaje:.3f}%</b> ({fuerza_vol})\n"
                f"• Cantidad (Contratos): <b>{int(lote_sugerido):,}</b>\n"
                f"━━━━━━━━━━━━━━━\n"
                
            )
            trade = {
                "idCuenta": cuenta['idCuenta'],
                "symbol": symbols['symbol'],
                "direction": direction,
                "entryPrice": close,
                "openTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": sl,
                "takeProfit": tp,
                "size": int(lote_sugerido),
                "intervalo": intervalo,
                "closeTime": None,
                "pnl": 0,
                "slippage": 0,
                "exitPrice": None,
                "commission": 0
            }

            
            if cuenta['idCuenta'] != 1:
                dbManager.buscaTrade(trade)
                await enviarAlerta(cuenta['idGrupoMsg'], cuenta['TokenMsg'], text)
                logger.info(f"✅ Alerta SNIPER enviada para {symbols['symbol']}")

    except Exception as e:
        logger.error(f"Error procesando {symbols['symbol']}: {e}", exc_info=True)

# ==============================
# MAIN LOOP
# ==============================

async def iniciar_bot():
    enviado_cierre = False
    
    logger.info(f"Bot operativo con Excepciones de Festivos")

    while True:
        now = datetime.now()
        day = now.weekday()
        hour = now.hour
        minute = now.minute
        fecha_actual = now.strftime("%Y-%m-%d")
        # Actualizamos dinámicamente la configuración para que otros módulos la usen
        
        global api_key_activa, intervalo, nombre_key
        intervalo = INTERVAL
        esperaMin = 15 # Valor por defecto
        now_actual = datetime.now().strftime('%H:%M:%S')
        if not isRestTime():
            logger.info(f"\n\n\n--------------------- Iniciando Escaneo a las {now_actual} -------------------\n\n")

            for s in dbManager.getSymbols():
                key, inter, nom, n_velas,esperaMin = getParametros()
                api_key_activa, intervalo, nombre_key = key, inter, nom
                try:
                    await analyzeSymbol(s, n_velas)
                    await asyncio.sleep(9)
                except Exception as e:
                    logger.error(f"Error en {s['symbol']}: {e}")
            logger.info(f"✅ Ciclo completado. Esperando próxima vela...")
        if intervalo == INTERVALmax:
            intervalo = INTERVAL
            esperaMin = 15
        await getTiempoEspera(esperaMin)
        

# ==============================
# PROGRAM TRIGGER
# ==============================
if __name__ == "__main__":
    try:
        asyncio.run(iniciar_bot())
    except KeyboardInterrupt:
        logger.info("Bot detenido manualmente.\n")
        sys.exit(0)
