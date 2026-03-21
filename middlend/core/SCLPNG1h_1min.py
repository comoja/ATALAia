"""
SCLPNG1h_1min Trading Strategy Bot
Strategy based on liquidity sweeps in 1H with FVG confirmation in 1min.
"""
import logging
from datetime import datetime, time
from typing import Dict, Any
import pandas as pd
import numpy as np
import asyncio
import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middlend.api import twelvedata
from middlend.analysis import technical, risk
from middlend.core.communications import sendTelegramAlert, alertaInmediata
from middlend import configConstants as config
from middlend.database import dbManager
from middlend.scheduler.autoScheduler import getTiempoEspera, isRestTime
from middlend.data.dataLoader import getParametros

logger = logging.getLogger(__name__)


class SCLPNGBot:
    def __init__(self):
        self.accounts = []
        self.lastMessageIds = {}

    def esta_en_horario_operativo(self, hora_actual: datetime) -> bool:
        hora = hora_actual.hour
        minuto = hora_actual.minute
        hora_minuto = (hora, minuto)
        
        london = (9, 0) <= hora_minuto <= (11, 0)
        new_york = (14, 0) <= hora_minuto <= (16, 30)
        
        return london or new_york

    def detectar_fvg(self, velas_m1: pd.DataFrame) -> str:
        if len(velas_m1) < 3:
            return None
        
        low_i_2 = velas_m1['low'].iloc[-3]
        high_i_1 = velas_m1['high'].iloc[-2]
        low_i = velas_m1['low'].iloc[-1]
        
        high_i_2 = velas_m1['high'].iloc[-3]
        low_i_1 = velas_m1['low'].iloc[-2]
        high_i = velas_m1['high'].iloc[-1]
        
        if low_i_2 > high_i_1:
            return "Bearish_FVG"
        
        if high_i_2 < low_i_1:
            return "Bullish_FVG"
        
        return None

    def obtener_max_min_dia_anterior(self, datos_h1: pd.DataFrame) -> tuple:
        maximo_h1 = datos_h1['high'].max()
        minimo_h1 = datos_h1['low'].min()
        return maximo_h1, minimo_h1

    async def _get_and_prepare_data_h1(self, symbolInfo: Dict, apiKey: str, nVelas: int, raw_df: pd.DataFrame = None) -> pd.DataFrame | None:
        symbol = symbolInfo['symbol']
        
        if raw_df is not None and len(raw_df) >= 50:
            df = raw_df.copy()
        else:
            df = await twelvedata.getTimeSeries(symbol, "1h", apiKey, nVelas)
            if df is None or len(df) < 50:
                logger.warning(f"[{symbol}] Datos insuficientes para análisis SCLPNG 1H ({len(df) if df is not None else 0} velas).")
                return None
        
        return df

    async def _get_and_prepare_data_m1(self, symbolInfo: Dict, apiKey: str, nVelas: int = 100) -> pd.DataFrame | None:
        symbol = symbolInfo['symbol']
        
        df = await twelvedata.getTimeSeries(symbol, "1min", apiKey, nVelas)
        if df is None or len(df) < 10:
            logger.warning(f"[{symbol}] Datos insuficientes para análisis SCLPNG 1min ({len(df) if df is not None else 0} velas).")
            return None
        
        return df

    async def _get_signal(self, datos_h1: pd.DataFrame, datos_m1: pd.DataFrame, symbol: str, hora_actual: datetime) -> Dict[str, Any] | None:
        if not self.esta_en_horario_operativo(hora_actual):
            logger.debug(f"[{symbol}] Fuera de horario operativo (Londres 9-11 o NY 14-16:30).")
            return None
        
        maximo_h1, minimo_h1 = self.obtener_max_min_dia_anterior(datos_h1)
        
        precio_actual = datos_m1['close'].iloc[-1]
        
        maximo_m1 = datos_m1['high'].tail(3).max()
        minimo_m1 = datos_m1['low'].tail(3).min()
        
        fvg = self.detectar_fvg(datos_m1)
        
        direction = None
        setup_type = None
        stop_loss = None
        take_profit = None
        confianza = 0
        
        if precio_actual > maximo_h1:
            if fvg == "Bearish_FVG":
                direction = "CORTO"
                setup_type = "LIQUIDATION_SELL"
                stop_loss = maximo_m1 * 1.002
                distancia_sl = precio_actual - stop_loss
                take_profit = precio_actual - distancia_sl * 2
                confianza = 75
        
        elif precio_actual < minimo_h1:
            if fvg == "Bullish_FVG":
                direction = "LARGO"
                setup_type = "LIQUIDATION_BUY"
                stop_loss = minimo_m1 * 0.998
                distancia_sl = stop_loss - precio_actual
                take_profit = precio_actual + distancia_sl * 2
                confianza = 75
        
        if direction is None:
            logger.debug(f"[{symbol}] No se detectó ningún setup válido SCLPNG.")
            return None
        
        return {
            "strategy": "SCLPNG1h_1min",
            "direction": direction,
            "confidence": confianza,
            "entryPrice": precio_actual,
            "slDistance": abs(precio_actual - stop_loss),
            "stopLoss": stop_loss,
            "takeProfit": take_profit,
            "setup": setup_type,
            "maximo_h1": maximo_h1,
            "minimo_h1": minimo_h1,
            "fvg": fvg,
            "symbolInfo": symbol
        }

    async def _execute_trades(self, signal: Dict, symbolInfo):
        if not signal:
            return

        for account in self.accounts:
            posSize, riskUsd = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=signal['slDistance'],
                symbolInfo=symbolInfo
            )
            if posSize is None:
                logger.warning(f"[{account['idCuenta']}] No se pudo calcular el tamaño de posición para {symbolInfo['symbol']}.")
                continue
            
            direction = signal['direction']
            entryPrice = signal['entryPrice']
            slDist = signal['slDistance']
            
            slPrice = entryPrice - slDist if direction == "LARGO" else entryPrice + slDist
            
            ratioBase = 2.0
            tpPrice = entryPrice + (slDist * ratioBase) if direction == "LARGO" else entryPrice - (slDist * ratioBase)
            
            trade = {
                "idCuenta": account['idCuenta'],
                "symbol": symbolInfo['symbol'],
                "direction": direction,
                "entryPrice": entryPrice,
                "openTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": slPrice,
                "takeProfit": tpPrice,
                "size": posSize,
                "intervalo": symbolInfo.get('intervalo', ''),
                "status": "OPEN",
            }
            
            if account['idCuenta'] != 1:
                dbManager.buscaTrade(trade)
                
                message = self._format_alert_message(signal, trade)
                
                msgId = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
                if msgId:
                    self.lastMessageIds[symbolInfo['symbol']] = msgId
                    
                logger.info(f"✅ Alerta SCLPNG1h_1min enviada para {symbolInfo['symbol']} a la cuenta {account['idCuenta']}")

    def _format_alert_message(self, signal: Dict, trade: Dict) -> str:
        direction = signal['direction']
        directionStr = "COMPRA" if direction == "LARGO" else "VENTA"
        colorHeader = "🟩" if direction == "LARGO" else "🟥"
        
        close = signal['entryPrice']
        tp = trade['takeProfit']
        sl = trade['stopLoss']
        confianza = signal['confidence']
        setup = signal['setup']
        fvg = signal.get('fvg', 'N/A')
        maximo_h1 = signal.get('maximo_h1', 0)
        minimo_h1 = signal.get('minimo_h1', 0)
        
        text = (
            f"{colorHeader}{colorHeader}{colorHeader} "
            f"<b>SEÑAL DE {directionStr}</b> "
            f"{colorHeader}{colorHeader}{colorHeader}\n"
            f"<center><i>Estrategia: SCLPNG1h_1min</i></center>\n"
            f"<center><b>{trade['symbol']}</b> (1H breakout + 1min FVG)</center>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center>Setup: <b>{setup}</b></center>\n"
            f"<center>Confianza: <b>{confianza}%</b></center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔹 ENTRADA:   <b>{close:,.6f}</b>\n"
            f"🔴 SL: <b>{sl:,.6f}</b>\n"
            f"🟢 TP: <b>{tp:,.6f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"• 1H High: {maximo_h1:.6f}\n"
            f"• 1H Low: {minimo_h1:.6f}\n"
            f"• FVG: <b>{fvg}</b>\n"
            f"• Cantidad: <b>{trade['size']:.2f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
        )
        return text

    async def runAnalysisCycle(self, preloaded_data: Dict = None):
        self.accounts = dbManager.getAccount()
        if not self.accounts:
            logger.error("No se encontraron cuentas en la base de datos.")
            return

        logger.info("Iniciando ciclo SCLPNG1h_1min...")
        
        symbolsToScan = dbManager.getSymbols()
        apiKey, _, _, nVelas, _ = getParametros()
        
        for symbolInfo in symbolsToScan:
            symbol = symbolInfo['symbol']
            logger.debug(f"Analizando {symbol} con SCLPNG1h_1min...")
            
            hora_actual = datetime.now()
            
            datos_h1 = await self._get_and_prepare_data_h1(symbolInfo, apiKey, nVelas, preloaded_data.get(symbol) if preloaded_data else None)
            if datos_h1 is None:
                continue
            
            datos_m1 = await self._get_and_prepare_data_m1(symbolInfo, apiKey)
            if datos_m1 is None:
                continue
            
            signal = await self._get_signal(datos_h1, datos_m1, symbol, hora_actual)
            if signal:
                logger.info(f"[{symbol}] Señal SCLPNG1h_1min: {signal['direction']} ({signal['confidence']}% confianza)")
                await self._execute_trades(signal, symbolInfo)
            else:
                logger.debug(f"[{symbol}] Sin señal SCLPNG1h_1min.")
            await asyncio.sleep(5)
        logger.info("✅ Ciclo SCLPNG1h_1min completado.")

    async def runAnalysisCycle_for_symbol(self, symbolInfo: Dict, preloaded_data: Dict = None, apiKey: str = None):
        if not self.accounts:
            return
        
        symbol = symbolInfo['symbol']
        interval = symbolInfo.get('intervalo', '15min')
        if apiKey is None:
            apiKey, _, _, nVelas, _ = getParametros()
        else:
            _, _, _, nVelas, _ = getParametros()
        
        logger.debug(f"Analizando {symbol} con SCLPNG1h_1min en intervalo {interval}...")
        
        hora_actual = datetime.now()
        
        raw_df = preloaded_data.get(symbol) if preloaded_data else None
        datos_h1 = await self._get_and_prepare_data_h1(symbolInfo, apiKey, nVelas, raw_df)
        if datos_h1 is None:
            return
        
        datos_m1 = await self._get_and_prepare_data_m1(symbolInfo, apiKey)
        if datos_m1 is None:
            return
        
        signal = await self._get_signal(datos_h1, datos_m1, symbol, hora_actual)
        if signal:
            logger.info(f"[{symbol}] Señal SCLPNG1h_1min: {signal['direction']} ({signal['confidence']}% confianza)")
            await self._execute_trades(signal, symbolInfo)
        else:
            logger.debug(f"[{symbol}] Sin señal SCLPNG1h_1min en intervalo {interval}.")
