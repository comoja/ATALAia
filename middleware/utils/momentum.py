

import sys
import os
import asyncio
import pandas as pd
import numpy as np
from datetime import datetime
import warnings

from middleware.utils.communications import alertaInmediata
from middleware.database import dbManager
import logging
logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore")


# Diccionario en memoria (se carga desde DB al iniciar)
estadosPorSimbolo = {} 

def _cargar_estados_desde_db():
    """Carga los estados guardados desde ConnectionPool microservicio al iniciar."""
    global estadosPorSimbolo
    try:
        res = dbManager._call_connection_pool("GET", "/momentum-estados")
        if res and isinstance(res, list):
            for row in res:
                if isinstance(row, dict) and "symbol" in row and "estado" in row:
                    estadosPorSimbolo[row['symbol']] = row['estado']
            logger.info(f"[Momentum] Estados cargados desde ConnectionPool: {len(estadosPorSimbolo)} símbolos")
            return estadosPorSimbolo
        return {}
    except Exception as e:
        logger.error(f"[Momentum] Error cargando estados desde ConnectionPool: {e}")
        return {}

def _guardar_estado_en_db(symbol: str, estado: str):
    """Guarda el estado del símbolo en ConnectionPool microservicio."""
    try:
        payload = {
            "symbol": symbol,
            "estado": estado
        }
        dbManager._call_connection_pool("POST", "/momentum-estados", json_data=payload)
    except Exception as e:
        logger.error(f"[Momentum] Error guardando estado en ConnectionPool: {e}")

async def _enviar_resumen_inicial(df_dict: dict):
    """Envía resumen de estados actuales al iniciar (a cuenta 1 - Sentinel)."""
    mensaje = (
        f"<b><center>MOMENTUM - RESUMEN</center></b>\n"
        f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M')}</center>\n"
        f"━━━━━━━━━━━━━━━━\n"
    )
    for symbol, df in df_dict.items():
        if df is None or len(df) < 15:
            continue
        df_calc = calcularAngulos(df.tail(15))
        last = df_calc.iloc[-1]
        estado, _ = obtenerEstado(last.get('ang_rsi'), last.get('ang_close'))
        
        # Usar icono según el estado real
        estado_icono_map = {
            "🚀 ALCISTA": "📈",
            "📉 BAJISTA": "📉",
            "💸 LIQUIDACIÓN": "💸",
            "💎 GIRO": "💎",
            "🌋 PARÁBOLA": "🌋",
            "☁️ NEUTRAL": "➡️",
            "☁️ SIN DATOS": "❓"
        }
        icono = estado_icono_map.get(estado, "⚡")
        mensaje += f"<b>{symbol}</b>: {estado} (${last.get('close', 0):,.2f})\n"
    
    mensaje += "━━━━━━━━━━━━━━━━\n"
    
    try:
        await alertaInmediata(1, mensaje, False)
        logger.info("[Momentum] Resumen inicial enviado a cuenta 1")
    except Exception as e:
        logger.error(f"[Momentum] Error enviando resumen: {e}")

def _guardar_estado_en_db(symbol: str, estado: str):
    """Guarda el estado del símbolo en ConnectionPool microservicio."""
    try:
        payload = {
            "symbol": symbol,
            "estado": estado
        }
        dbManager._call_connection_pool("POST", "/momentum-estados", json_data=payload)
    except Exception as e:
        logger.error(f"[Momentum] Error guardando estado en ConnectionPool: {e}")

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

MOMENTUM_BONUS_POSITIVE = ["🚀 ALCISTA", "💎 GIRO"]
MOMENTUM_BONUS_NEGATIVE = ["📉 BAJISTA"]
MOMENTUM_PENALTY = ["💸 LIQUIDACIÓN", "🌋 PARÁBOLA"]

def getMomentumBonus(momentum_estado: str, direction: str) -> tuple:
    """
    Calcula el bonus/penalización de confianza basado en el momentum.
    Args:
        momentum_estado: Estado actual del símbolo (ej: '🚀 ALCISTA')
        direction: Dirección de la signal ('LARGO' o 'CORTO')
    Returns:
        (bonus: int, aligned: bool) - bonus de confianza y si está alineado
    """
    bonus = 0
    aligned = False
    
    if direction == "LARGO":
        if momentum_estado in MOMENTUM_BONUS_POSITIVE:
            bonus = 10
            aligned = True
    elif direction == "CORTO":
        if momentum_estado in MOMENTUM_BONUS_NEGATIVE:
            bonus = 10
            aligned = True
    
    if momentum_estado in MOMENTUM_PENALTY:
        bonus = -5
    
    return bonus, aligned

def centrarTexto(texto, ancho=50):
    espacios = (ancho - len(texto)) // 2
    return " " * max(0, espacios) + texto

async def momentum(symbol, df, intervalo=None):   
    global estadosPorSimbolo 
    
    # Cargar estados desde DB si está vacío (al reiniciar Sentinel)
    if not estadosPorSimbolo:
        _cargar_estados_desde_db()
    
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
            
        # 5. Actualizar el diccionario y guardar en DB
        estadosPorSimbolo[symbol] = estadoActual
        _guardar_estado_en_db(symbol, estadoActual)

    return estadosPorSimbolo
