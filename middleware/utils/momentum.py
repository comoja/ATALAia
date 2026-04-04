

import sys
import os
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime
import warnings

from middleware.core.communications import alertaInmediata
import logging
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore")


# Diccionario inicializado
estadosPorSimbolo = {} 

def calcularAngulos(df, ventana=14):
    # Asegurar que las columnas sean numéricas para evitar el TypeError
    columnasCalculo = ['close', 'rsi', 'cci', 'macd']
    for col in columnasCalculo:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            df[col] = np.nan # Evita errores si falta una métrica
    
    for col in columnasCalculo:
        minV, maxV = df[col].rolling(ventana).min(), df[col].rolling(ventana).max()
        rango = maxV - minV
        # Evitar división por cero
        dfNorm = 100 * (df[col] - minV) / rango.replace(0, np.nan)
        df[f'ang_{col}'] = np.degrees(np.arctan(dfNorm.diff(1)))
    return df

def obtenerEstado(angR, angP):
    if pd.isna(angR) or pd.isna(angP): return "☁️ SIN DATOS", "Esperando..."
    if angP < -70 and angR > -20: return "💎 GIRO", "🎯 OPORTUNIDAD: Rebote detectado."
    if angR <= -75: return "💸 LIQUIDACIÓN", "🚨 CRÍTICA: Desplome vertical."
    if angR >= 75:  return "🌋 PARÁBOLA", "⚠️ ALERTA: Subida extrema."
    if angR > 30:   return "🚀 ALCISTA", "✅ Tendencia positiva."
    if angR < -30:  return "📉 BAJISTA", "🔻 Presión de venta."
    return "☁️ NEUTRAL", "💤 Sin movimiento claro."

def centrarTexto(texto, ancho=50):
    espacios = (ancho - len(texto)) // 2
    return " " * max(0, espacios) + texto

async def momentum(symbol, df, intervalo=None):   
    global estadosPorSimbolo 
    # 1. Procesar datos
    df = calcularAngulos(df)
    last = df.iloc[-1]
    
    closePrice = last.get('close', 0)
    # 2. Obtener estado actual (usamos 'ang_close')
    estadoActual, notaMensaje = obtenerEstado(last.get('ang_rsi'), last.get('ang_close'))
    # 3. FILTRO POR SÍMBOLO
    estadoPrevio = estadosPorSimbolo.get(symbol)
    
    if estadoActual != estadoPrevio:
        def obtenerIcono(angulo): 
            if pd.isna(angulo): return "⚪"
            return "🧊" if angulo <= -75 else ("🔥" if angulo >= 75 else ("📈" if angulo > 0 else "📉"))
        
        intervalText = f"({intervalo})" if intervalo else ""
        mensajeFinal = (
            f"<b><center>MOMENTUM {symbol} {intervalText}</center></b>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>PRECIO:</b> ${closePrice:,.2f}\n"
            f"<b>ESTADO:</b> {estadoActual}\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>RSI:</b>  {obtenerIcono(last.get('ang_rsi'))} {last.get('ang_rsi', 0):>6.1f}° ({last.get('rsi', 0):.1f})\n"
            f"<b>CCI:</b>  {obtenerIcono(last.get('ang_cci'))} {last.get('ang_cci', 0):>6.1f}° ({last.get('cci', 0):.1f})\n"
            f"<b>MACD:</b> {obtenerIcono(last.get('ang_macd'))} {last.get('ang_macd', 0):>6.1f}° ({last.get('macd', 0):.2f})\n"
            f"━━━━━━━━━━━━━━━━\n"
            f"<b>NOTA:</b> <i>{notaMensaje}</i>"
        )

        # 4. Enviar alerta
        esCritico = estadoActual in ["💸 LIQUIDACIÓN", "💎 GIRO", "🌋 PARÁBOLA"]
        
        esLateral = False
        cambioPorcentual = 0
        if len(df) >= 2:
            precioActual = float(last.get('close', 0))
            precioAnterior = float(df.iloc[-2].get('close', 0))
            if precioAnterior > 0:
                cambioPorcentual = abs((precioActual - precioAnterior) / precioAnterior * 100)
                esLateral = cambioPorcentual < 0.5
        
        if esLateral:
            logger.info(f"[{symbol}] Filtrado MOMENTUM: Movimiento lateral ({cambioPorcentual:.2f}%)")
        elif estadoActual not in ["☁️ SIN DATOS", "☁️ NEUTRAL"]:
            await alertaInmediata(1, mensajeFinal, esCritico)
            
        # 5. Actualizar el diccionario
        estadosPorSimbolo[symbol] = estadoActual

    return estadosPorSimbolo
