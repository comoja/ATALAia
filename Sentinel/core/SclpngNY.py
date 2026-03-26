"""
SclpngNY Trading Strategy Bot
Strategy based on liquidity sweeps with FVG confirmation in 5min.
Only for XAU/USD symbol.
"""
import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd
import numpy as np
import asyncio
import os
import sys
import pytz

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.api import twelvedata
from Sentinel.analysis import technical, risk
from middleware.utils.communications import sendTelegramAlert, alertaInmediata
from middleware.utils.alertBuilder import buildSclpngNYAlertMessage
from middleware.database import dbManager
from Sentinel.data.dataLoader import getParametros
from middleware.config.constants import TIMEZONE

logger = logging.getLogger(__name__)


class SCLPNGBot:
    SUPPORTED_SYMBOLS = ['XAU/USD', 'XAUUSD']
    MEXICO_TZ = pytz.timezone(TIMEZONE)
    
    def __init__(self):
        self.accounts = []
        self.lastMessageIds = {}
        self.sessionStarted = False
        self.sessionEnded = False
        self.currentSessionLevels = None
        self.velaCorte = None
        self.signalGenerada = False
        self.timestamp_signal1 = None
        self.timestamp_signal2 = None
        self.signal1_enviada = False
        self.signal2_enviada = False
    
    @staticmethod
    def isNyDST(date) -> bool:
        if hasattr(date, 'tzinfo') and date.tzinfo is not None:
            date = date.replace(tzinfo=None)
        year = date.year
        dstStart = pd.Timestamp(year, 3, 8)
        dstEnd = pd.Timestamp(year, 11, 1)
        
        while dstStart.weekday() != 6:
            dstStart += pd.Timedelta(days=1)
        while dstEnd.weekday() != 6:
            dstEnd += pd.Timedelta(days=1)
        
        return dstStart <= pd.Timestamp(date) < dstEnd
    
    def getMexicoTime(self) -> datetime:
        return datetime.now(self.MEXICO_TZ)
    
    def detectarFvg(self, datos5min: pd.DataFrame, idx: int, direction: str) -> dict:
        if idx >= len(datos5min) - 3:
            return None
        
        if direction == 'SHORT':
            lowN = datos5min['low'].iloc[idx]
            highN2 = datos5min['high'].iloc[idx + 2]
            if lowN > highN2:
                return {
                    'type': 'Bearish_FVG',
                    'start': highN2,
                    'end': lowN,
                    'mid': (highN2 + lowN) / 2,
                    'size': lowN - highN2,
                    'idx': idx
                }
        else:
            highN = datos5min['high'].iloc[idx]
            lowN2 = datos5min['low'].iloc[idx + 2]
            if highN < lowN2:
                return {
                    'type': 'Bullish_FVG',
                    'start': highN,
                    'end': lowN2,
                    'mid': (highN + lowN2) / 2,
                    'size': lowN2 - highN,
                    'idx': idx
                }
        return None
    
    def findFvgAfterImpulse(self, datos5min: pd.DataFrame, startIdx: int, direction: str) -> dict:
        fvgs = []
        for i in range(startIdx, min(startIdx + 10, len(datos5min) - 3)):
            fvg = self.detectarFvg(datos5min, i, direction)
            if fvg:
                fvgs.append(fvg)
        return fvgs[-1] if fvgs else None
    
    def findVelaCorte(self, datos5min: pd.DataFrame, precioMaximo: float, precioMinimo: float) -> dict:
        for i in range(len(datos5min)):
            vela = datos5min.iloc[i]
            openPrice = vela['open']
            closePrice = vela['close']
            highPrice = vela['high']
            lowPrice = vela['low']
            
            bodyTop = max(openPrice, closePrice)
            bodyBottom = min(openPrice, closePrice)
            
            if bodyTop > precioMaximo and bodyBottom > precioMaximo:
                return {
                    'type': 'SHORT',
                    'idx': i,
                    'vela': vela,
                    'precioRuptura': bodyTop
                }
            
            if bodyBottom < precioMinimo and bodyTop < precioMinimo:
                return {
                    'type': 'LONG',
                    'idx': i,
                    'vela': vela,
                    'precioRuptura': bodyBottom
                }
        
        return None
    
    def findFvgEnRango(self, datos5min: pd.DataFrame, startIdx: int, direction: str, precioMaximo: float, precioMinimo: float) -> dict:
        for i in range(startIdx, min(startIdx + 20, len(datos5min) - 3)):
            vela = datos5min.iloc[i]
            highPrice = vela['high']
            lowPrice = vela['low']
            
            if direction == 'SHORT':
                if highPrice <= precioMaximo and lowPrice >= precioMinimo:
                    fvg = self.detectarFvg(datos5min, i, 'SHORT')
                    if fvg:
                        return fvg
            else:
                if highPrice <= precioMaximo and lowPrice >= precioMinimo:
                    fvg = self.detectarFvg(datos5min, i, 'LONG')
                    if fvg:
                        return fvg
        
        return None
    
    def findAllFvgEnRango(self, datos5min: pd.DataFrame, startIdx: int, direction: str, precioMaximo: float, precioMinimo: float, maxFvg: int = 2) -> list:
        fvgs = []
        for i in range(startIdx, min(startIdx + 50, len(datos5min) - 3)):
            vela = datos5min.iloc[i]
            highPrice = vela['high']
            lowPrice = vela['low']
            
            if direction == 'SHORT':
                if highPrice <= precioMaximo and lowPrice >= precioMinimo:
                    fvg = self.detectarFvg(datos5min, i, 'SHORT')
                    if fvg:
                        fvgs.append(fvg)
                        if len(fvgs) >= maxFvg:
                            break
            else:
                if highPrice <= precioMaximo and lowPrice >= precioMinimo:
                    fvg = self.detectarFvg(datos5min, i, 'LONG')
                    if fvg:
                        fvgs.append(fvg)
                        if len(fvgs) >= maxFvg:
                            break
        
        return fvgs
    
    async def _getSignals(self, datos5min: pd.DataFrame, symbolInfo: Dict) -> list:
        symbol = symbolInfo['symbol']
        """
        if symbol.upper() not in self.SUPPORTED_SYMBOLS:
            logger.debug(f"[{symbol}] SCLPNG solo para XAU/USD")
            return []
        """
        precioMaximo = symbolInfo.get('precioMaximo')
        precioMinimo = symbolInfo.get('precioMinimo')
        
        if precioMaximo is None or precioMinimo is None:
            logger.debug(f"[{symbol}] No hay niveles de precio definidos")
            return []
        
        precioActual = datos5min['close'].iloc[-1]
        logger.info(f"[SclpngNY] Precio actual: {precioActual}, Max: {precioMaximo}, Min: {precioMinimo}")
        
        if self.signalGenerada:
            logger.info(f"[SclpngNY] Señales ya generadas anteriormente")
            return []
        
        if self.velaCorte is None:
            velaCorte = self.findVelaCorte(datos5min, precioMaximo, precioMinimo)
            if velaCorte is None:
                logger.info(f"[SclpngNY] No hay vela de corte todavía")
                return []
            
            self.velaCorte = velaCorte
            logger.info(f"[SclpngNY] Vela de corte detectada: {velaCorte['type']} en idx {velaCorte['idx']}")
        
        velaCorte = self.velaCorte
        direction = velaCorte['type']
        
        startSearch = velaCorte['idx'] + 1
        
        fvgs = self.findAllFvgEnRango(datos5min, startSearch, direction, precioMaximo, precioMinimo, maxFvg=2)
        
        if not fvgs:
            logger.info(f"[SclpngNY] No se encontraron FVGs en rango después de vela de corte")
            return []
        
        logger.info(f"[SclpngNY] FVGs encontrados: {len(fvgs)}")
        
        signals = []
        
        ahora = self.getMexicoTime()
        
        for idx, fvg in enumerate(fvgs):
            logger.info(f"[SclpngNY] FVG {idx+1}: {fvg['type']}, idx: {fvg['idx']}")
            
            # Verificar 30 minutos para cada señal de trade (no solo detección FVG)
            if idx == 0:  # Señal 1
                if self.signal1_enviada and self.timestamp_signal1:
                    minutos_desde_signal1 = (ahora - self.timestamp_signal1).total_seconds() / 60
                    if minutos_desde_signal1 > 30:
                        logger.info(f"[SclpngNY] Señal 1 (trade) ya enviada hace más de 30 min ({minutos_desde_signal1:.1f}), omitiendo...")
                        continue
            elif idx == 1:  # Señal 2
                if self.signal2_enviada and self.timestamp_signal2:
                    minutos_desde_signal2 = (ahora - self.timestamp_signal2).total_seconds() / 60
                    if minutos_desde_signal2 > 30:
                        logger.info(f"[SclpngNY] Señal 2 (trade) ya enviada hace más de 30 min ({minutos_desde_signal2:.1f}), omitiendo...")
                        continue
            
            entryPrice = fvg['mid']
            
            if direction == 'SHORT':
                setupType = "LIQUIDATION_SELL"
                stopLoss = datos5min['high'].iloc[fvg['idx']] * 1.001
                distanciaSl = entryPrice - stopLoss
                takeProfit = entryPrice - distanciaSl * 2
                signalDirection = "CORTO"
            else:
                setupType = "LIQUIDATION_BUY"
                stopLoss = datos5min['low'].iloc[fvg['idx']] * 0.999
                distanciaSl = stopLoss - entryPrice
                takeProfit = entryPrice + distanciaSl * 2
                signalDirection = "LARGO"
            
            signals.append({
                "strategy": "SclpngNY",
                "direction": signalDirection,
                "confidence": 75,
                "entryPrice": entryPrice,
                "slDistance": abs(entryPrice - stopLoss),
                "stopLoss": stopLoss,
                "takeProfit": takeProfit,
                "setup": setupType,
                "precioMaximo": precioMaximo,
                "precioMinimo": precioMinimo,
                "fvg": fvg['type'],
                "fvgNum": idx + 1,
                "velaCorteType": direction,
                "symbolInfo": symbolInfo
            })
        
        self.signalGenerada = True
        
        return signals

    async def _executeTrades(self, signal: Dict, symbolInfo):
        if not signal:
            return

        if not self.accounts:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                logger.warning("[SclpngNY] No hay cuentas disponibles")
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
                "openTime": self.getMexicoTime().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": slPrice,
                "takeProfit": tpPrice,
                "size": posSize,
                "intervalo": symbolInfo.get('intervalo', ''),
                "status": "OPEN",
            }
            
            if account['idCuenta'] != 1:
                dbManager.buscaTrade(trade)
                
                message = buildSclpngNYAlertMessage(signal, trade)
                
                msgId = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
                if msgId:
                    self.lastMessageIds[symbolInfo['symbol']] = msgId
                    
                logger.info(f"✅ Alerta SclpngNY enviada para {symbolInfo['symbol']} a la cuenta {account['idCuenta']}")
        
        # Marcar señal como enviada y guardar timestamp (se cuenta cuando se ejecuta el trade)
        fvg_num = signal.get('fvgNum', 0)
        ahora = self.getMexicoTime()
        if fvg_num == 1:
            self.signal1_enviada = True
            self.timestamp_signal1 = ahora
            logger.info(f"[SclpngNY] Marcando señal 1 como enviada a las {ahora}")
        elif fvg_num == 2:
            self.signal2_enviada = True
            self.timestamp_signal2 = ahora
            logger.info(f"[SclpngNY] Marcando señal 2 como enviada a las {ahora}")

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None):
        logger.info(f"[SclpngNY] ===== INICIANDO CICLO SCLPNG =====")
        
        symbol = symbolInfo['symbol']
        
        datos5min = preloadedData.get(symbol) if preloadedData else None
        if datos5min is None or len(datos5min) < 10:
            logger.warning(f"[SclpngNY] Datos insuficientes para {symbol}")
            return
        
        logger.info(f"[SclpngNY] Velas recibidas: {len(datos5min)}, desde: {datos5min.index[0]} hasta: {datos5min.index[-1]}")
        
        signals = await self._getSignals(datos5min, symbolInfo)
        
        if signals:
            logger.info(f"[{symbol}] Señales encontradas: {len(signals)}")
            for signal in signals:
                logger.info(f"[{symbol}] Señal {signal['fvgNum']}: {signal['direction']} ({signal['confidence']}% confianza)")
                await self._executeTrades(signal, symbolInfo)
        else:
            logger.info(f"[{symbol}] Sin señales SclpngNY en este ciclo")
