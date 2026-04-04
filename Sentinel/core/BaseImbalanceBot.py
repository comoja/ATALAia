import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd
import numpy as np
import talib as ta
import asyncio
import pytz

from middleware.database import dbManager
from Sentinel.analysis import risk
from middleware.utils.communications import sendTelegramAlert
from middleware.utils.alertBuilder import buildImbalanceLDNAlertMessage, buildImbalanceNYAlertMessage
from middleware.config.constants import TIMEZONE

logger = logging.getLogger(__name__)

def getAssetConfig(symbol: str) -> dict:
    symbolData = dbManager.getSymbol(symbol)
    assetType = symbolData.get('tipo', 'MONEDA') if symbolData else 'MONEDA'
    
    typeConfig = dbManager.getSymbolTypeConfig(assetType)
    
    if typeConfig:
        return {
            "sl": float(typeConfig.get('sl_atr', 1.5)),
            "tp": float(typeConfig.get('tp_atr', 3.0))
        }
    
    return {"sl": 1.5, "tp": 3.0}

class BaseImbalanceBot:
    MEXICO_TZ = pytz.timezone(TIMEZONE)
    
    def __init__(self, strategy_name: str):
        self.strategy_name = strategy_name
        self.accounts = []
        self.lastMessageIds = {}
        
        self.velaCorte = None
        self.signalGenerada = False
        self.timestamp_signal1 = None
        self.timestamp_signal2 = None
        self.signal1_enviada = False
        self.signal2_enviada = False
        
        strategyConfig = dbManager.getStrategyConfig(self.strategy_name)
        self.maxMinutosFvg = strategyConfig.get('max_minutos_fvg', 20) if strategyConfig else 20
        self.maxMinutosSignal = strategyConfig.get('max_minutos_signal', 20) if strategyConfig else 20
    
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
    
    def findVelaCorte(self, datos5min: pd.DataFrame, precioMaximo: float, precioMinimo: float) -> dict:
        for i in range(len(datos5min)):
            vela = datos5min.iloc[i]
            openPrice = vela['open']
            closePrice = vela['close']
            
            bodyTop = max(openPrice, closePrice)
            bodyBottom = min(openPrice, closePrice)
            
            if bodyTop > precioMaximo and bodyBottom > precioMaximo:
                return {
                    'type': 'LONG',
                    'idx': i,
                    'vela': vela,
                    'precioRuptura': bodyTop
                }
            
            if bodyBottom < precioMinimo and bodyTop < precioMinimo:
                return {
                    'type': 'SHORT',
                    'idx': i,
                    'vela': vela,
                    'precioRuptura': bodyBottom
                }
        return None
    
    def findFvgEnRango(self, datos5min: pd.DataFrame, startIdx: int, direction: str, precioMaximo: float, precioMinimo: float, maxFvg: int = 2) -> list:
        fvgs = []
        for i in range(startIdx, min(startIdx + 50, len(datos5min) - 3)):
            vela = datos5min.iloc[i]
            highPrice = vela['high']
            lowPrice = vela['low']
            
            dentroRango = highPrice <= precioMaximo and lowPrice >= precioMinimo
            
            if dentroRango:
                fvg = self.detectarFvg(datos5min, i, direction)
                if fvg:
                    fvg['dentroRango'] = True
                    fvgs.append(fvg)
                    if len(fvgs) >= maxFvg:
                        break
        return fvgs
    
    def findFvgFueraRango(self, datos5min: pd.DataFrame, startIdx: int, direction: str, precioMaximo: float, precioMinimo: float, maxFvg: int = 2) -> list:
        fvgs = []
        for i in range(startIdx, min(startIdx + 50, len(datos5min) - 3)):
            vela = datos5min.iloc[i]
            highPrice = vela['high']
            lowPrice = vela['low']
            
            fueraRango = highPrice > precioMaximo or lowPrice < precioMinimo
            
            if fueraRango:
                fvg = self.detectarFvg(datos5min, i, direction)
                if fvg:
                    fvg['dentroRango'] = False
                    fvgs.append(fvg)
                    if len(fvgs) >= maxFvg:
                        break
        return fvgs
    
    async def _getSignals(self, datos5min: pd.DataFrame, symbolInfo: Dict) -> list:
        symbol = symbolInfo['symbol']
        precioMaximo = symbolInfo.get('precioMaximo')
        precioMinimo = symbolInfo.get('precioMinimo')
        
        if precioMaximo is None or precioMinimo is None:
            logger.debug(f"[{symbol}] No hay niveles de precio definidos")
            return []
        
        precioActual = datos5min['close'].iloc[-1]
        logger.info(f"[{self.strategy_name}] Precio actual: {precioActual}, Max: {precioMaximo}, Min: {precioMinimo}")
        
        if self.signalGenerada:
            logger.info(f"[{self.strategy_name}] Señales ya generadas anteriormente")
            return []
        
        if self.velaCorte is None:
            velaCorte = self.findVelaCorte(datos5min, precioMaximo, precioMinimo)
            if velaCorte is None:
                logger.info(f"[{self.strategy_name}] No hay vela de corte todavía")
                return []
            
            self.velaCorte = velaCorte
            logger.info(f"[{self.strategy_name}] Vela de corte detectada: {velaCorte['type']} en idx {velaCorte['idx']}")
        
        velaCorte = self.velaCorte
        direction = velaCorte['type']
        
        startSearch = velaCorte['idx'] + 1
        
        fvgs_dentro = self.findFvgEnRango(datos5min, startSearch, direction, precioMaximo, precioMinimo, maxFvg=2)
        fvgs_fuera = self.findFvgFueraRango(datos5min, startSearch, direction, precioMaximo, precioMinimo, maxFvg=2)
        
        fvgs = fvgs_dentro + fvgs_fuera
        
        if not fvgs:
            logger.info(f"[{self.strategy_name}] No se encontraron FVGs después de vela de corte")
            return []
        
        logger.info(f"[{self.strategy_name}] FVGs encontrados: {len(fvgs)} (dentro: {len(fvgs_dentro)}, fuera: {len(fvgs_fuera)})")
        
        signals = []
        ahora = self.getMexicoTime().replace(tzinfo=None)
        
        for idx, fvg in enumerate(fvgs):
            logger.info(f"[{self.strategy_name}] FVG {idx+1}: {fvg['type']}, idx: {fvg['idx']}")
            
            fvg_time = pd.Timestamp(datos5min.index[fvg['idx']]).tz_localize(None)
            fvgTimeStr = fvg_time.strftime("%H:%M")
            
            minutos_desde_fvg = (ahora - fvg_time).total_seconds() / 60
            if minutos_desde_fvg > self.maxMinutosFvg:
                logger.info(f"[{self.strategy_name}] FVG {idx+1} tiene {minutos_desde_fvg:.1f} min, omitiendo...")
                continue
            
            if idx == 0 and self.signal1_enviada and self.timestamp_signal1:
                if (ahora - self.timestamp_signal1).total_seconds() / 60 > self.maxMinutosSignal:
                    continue
            elif idx == 1 and self.signal2_enviada and self.timestamp_signal2:
                if (ahora - self.timestamp_signal2).total_seconds() / 60 > self.maxMinutosSignal:
                    continue
            
            entryPrice = fvg['mid']
            atr = ta.ATR(datos5min['high'], datos5min['low'], datos5min['close'], 14).iloc[-1]
            
            if pd.isna(atr) or pd.isna(entryPrice):
                logger.warning(f"[{self.strategy_name}] ATR o entryPrice NaN para {symbol}, omitiendo señal...")
                continue
            
            assetConfig = getAssetConfig(symbol)
            slMultiplier = assetConfig["sl"]
            tpMultiplier = assetConfig["tp"]
            
            # Ajuste de Stop Loss estructural + padding
            # El padding de ATR otorga respiro para evitar cazar stops
            padding_pips = atr * slMultiplier
            
            if direction == 'SHORT':
                setupType = "LIQUIDATION_SELL"
                # Stop loss arriba de la formación del FVG + padding
                zona_high = datos5min['high'].iloc[fvg['idx']:fvg['idx']+2].max() if (fvg['idx']+2 < len(datos5min)) else datos5min['high'].iloc[fvg['idx']]
                stopLoss = zona_high + padding_pips
                
                distanciaSl = entryPrice - stopLoss
                takeProfit = entryPrice - abs(distanciaSl) * tpMultiplier
                signalDirection = "CORTO"
            else:
                setupType = "LIQUIDATION_BUY"
                # Stop loss debajo de la formación del FVG - padding
                zona_low = datos5min['low'].iloc[fvg['idx']:fvg['idx']+2].min() if (fvg['idx']+2 < len(datos5min)) else datos5min['low'].iloc[fvg['idx']]
                stopLoss = zona_low - padding_pips
                
                distanciaSl = stopLoss - entryPrice
                takeProfit = entryPrice + abs(distanciaSl) * tpMultiplier
                signalDirection = "LARGO"
                
            signals.append({
                "strategy": self.strategy_name,
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
                "fvgTime": fvgTimeStr,
                "dentroRango": fvg.get('dentroRango', True),
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
                logger.warning(f"[{self.strategy_name}] No hay cuentas disponibles")
                return

        for account in self.accounts:
            if not dbManager.isEstrategiaHabilitadaParaCuenta(account['idCuenta'], self.strategy_name):
                logger.info(f"[{self.strategy_name}] Estrategia deshabilitada para cuenta {account['idCuenta']}, omitiendo...")
                continue
            
            posSize, riskUsd, marginUsed = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=signal['slDistance'],
                symbolInfo=symbolInfo,
                entryPrice=signal.get('entryPrice')
            )
            
            direction = signal['direction']
            entryPrice = signal['entryPrice']
            slDist = signal['slDistance']
            fvgNum = signal.get('fvgNum', 0)
            
            slPrice = signal['stopLoss']
            
            ratioBase = 2.0
            tpPrice = entryPrice + (slDist * ratioBase) if direction == "LARGO" else entryPrice - (slDist * ratioBase)
            
            if posSize is None:
                posSize = 0
                marginUsed = 0
                logger.warning(f"[{account['idCuenta']}] Trade no ejecutada: {symbolInfo['symbol']} - size=0 (margen/riesgo excede capital)")
            
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
                "strategy": self.strategy_name,
                "fvgNum": fvgNum,
                "margin_used": marginUsed,
            }
            
            if account['idCuenta'] != 1:
                dbManager.buscaTrade(trade)
                
                # Dynamic alert building based on strategy name
                if self.strategy_name == 'ImbalanceLDN':
                    message = buildImbalanceLDNAlertMessage(signal, trade)
                else:
                    message = buildImbalanceNYAlertMessage(signal, trade)
                
                msgId = await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
                if msgId:
                    self.lastMessageIds[symbolInfo['symbol']] = msgId
                    
                logger.info(f"✅ Alerta {self.strategy_name} enviada para {symbolInfo['symbol']} a la cuenta {account['idCuenta']} | Size: {posSize}")
        
        fvg_num = signal.get('fvgNum', 0)
        ahora = self.getMexicoTime()
        if fvg_num == 1:
            self.signal1_enviada = True
            self.timestamp_signal1 = ahora
            logger.info(f"[{self.strategy_name}] Marcando señal 1 como enviada a las {ahora}")
        elif fvg_num == 2:
            self.signal2_enviada = True
            self.timestamp_signal2 = ahora
            logger.info(f"[{self.strategy_name}] Marcando señal 2 como enviada a las {ahora}")

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None):
        logger.info(f"[{self.strategy_name}] ===== INICIANDO CICLO IMBALANCE =====")
        
        symbol = symbolInfo['symbol']
        datos5min = preloadedData.get(symbol) if preloadedData else None
        
        if datos5min is None or len(datos5min) < 1:
            logger.warning(f"[{self.strategy_name}] Datos ({len(datos5min) if datos5min is not None else 0} velas) insuficientes para {symbol}")
            return
        
        logger.info(f"[{self.strategy_name}] Velas recibidas: {len(datos5min)}, desde: {datos5min.index[0]} hasta: {datos5min.index[-1]}")
        
        signals = await self._getSignals(datos5min, symbolInfo)
        
        if signals:
            logger.info(f"[{symbol}] Señales encontradas: {len(signals)}")
            for signal in signals:
                logger.info(f"[{symbol}] Señal {signal['fvgNum']}: {signal['direction']} ({signal['confidence']}% confianza)")
                await self._executeTrades(signal, symbolInfo)
        else:
            logger.info(f"[{symbol}] Sin señales {self.strategy_name} en este ciclo")
