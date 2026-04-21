import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any
import pandas as pd
import numpy as np
import talib as ta
import asyncio
import pytz

from middleware.database import dbManager
from Sentinel.analysis import risk
from Sentinel.analysis.technical import check_tp_exhaustion
from middleware.utils.communications import sendTelegramAlert
from middleware.utils import momentum

from middleware.utils.alertBuilder import buildImbalanceLDNAlertMessage, buildImbalanceNYAlertMessage, adjustTPForMinRR, getPipMultiplier, calculateRR
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle

logger = logging.getLogger("sentinel")

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
        
        if direction == 'CORTO':
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
            highPrice = vela['high']
            lowPrice = vela['low']
            
            cuerpo = abs(closePrice - openPrice)
            rango = highPrice - lowPrice
            if rango == 0: continue
            
            bodyTop = max(openPrice, closePrice)
            bodyBottom = min(openPrice, closePrice)
            
            if bodyTop > precioMaximo and bodyBottom > precioMaximo:
                # Filtro agresivo: Vela fuerte (cuerpo > 60%) y cierre cerca del máximo (mecha superior muy pequeña)
                if closePrice > openPrice and (cuerpo / rango) > 0.6 and ((highPrice - closePrice) / rango) <= 0.25:
                    return {
                        'type': 'LARGO',
                        'idx': i,
                        'vela': vela,
                        'precioRuptura': bodyTop
                    }
            
            if bodyBottom < precioMinimo and bodyTop < precioMinimo:
                # Filtro agresivo: Vela fuerte (cuerpo > 60%) y cierre cerca del mínimo (mecha inferior muy pequeña)
                if closePrice < openPrice and (cuerpo / rango) > 0.6 and ((closePrice - lowPrice) / rango) <= 0.25:
                    return {
                        'type': 'CORTO',
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
            logger.info(f"[{symbol}] No hay niveles de precio definidos")
            return []
        
        # ADX Filter: Verificar mercado con tendencia
        adx = ta.ADX(datos5min['high'], datos5min['low'], datos5min['close'], timeperiod=14)
        adx_val = float(adx.dropna().iloc[-1]) if len(adx.dropna()) > 0 else 25.0
        if adx_val < 20:
            logger.info(f"[{self.strategy_name}][{symbol}] Mercado lateral (ADX={adx_val:.1f} < 20), sin señales")
            return []
        
        precioActual = datos5min['close'].iloc[-1]
        logger.info(f"[{self.strategy_name}] Precio actual: {precioActual}, Max: {precioMaximo}, Min: {precioMinimo}")
        
        if self.signalGenerada:
            logger.debug(f"[{self.strategy_name}] Señales ya generadas anteriormente")
            return []
        
        # Verificar en DB si ya existe trade abierto para este símbolo
        existing_trade = dbManager.getOpenTradeBySymbol(symbol)
        if existing_trade:
            logger.info(f"[{self.strategy_name}] Trade ya abierto para {symbol} - omitiendo")
            self.signalGenerada = True
            return []
        
        if self.velaCorte is None:
            velaCorte = self.findVelaCorte(datos5min, precioMaximo, precioMinimo)
            if velaCorte is None:
                logger.debug(f"[{self.strategy_name}] No hay vela de corte todavía")
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
        now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
        last_closed = get_last_closed_candle(now_cdmx, interval=5)
        last_closed_str = last_closed.strftime("%Y-%m-%d %H:%M:%S")
        
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
                    logger.info(f"[{self.strategy_name}] FVG 1 rechazado: Señal 1 expirada (>{self.maxMinutosSignal} min)")
                    continue
            elif idx == 1 and self.signal2_enviada and self.timestamp_signal2:
                if (ahora - self.timestamp_signal2).total_seconds() / 60 > self.maxMinutosSignal:
                    logger.info(f"[{self.strategy_name}] FVG 2 rechazado: Señal 2 expirada (>{self.maxMinutosSignal} min)")
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
            
            # Niveles estructurales para SL y TP lógicos (sensibilidad aumentada)
            from Sentinel.analysis import technical
            levels = technical.get_structural_levels(datos5min, lookback=30)
            
            if direction == 'CORTO':
                lowN = datos5min['low'].iloc[idx]
                setupType = "LIQUIDATION_SELL"
                # Stop loss arriba de la formación del FVG o el máximo reciente
                zona_high_fvg = datos5min['high'].iloc[fvg['idx']:fvg['idx']+2].max() if (fvg['idx']+2 < len(datos5min)) else datos5min['high'].iloc[fvg['idx']]
                stop_ref = max(zona_high_fvg, levels['swing_high'])
                stopLoss = stop_ref + padding_pips
                
                distanciaSl = entryPrice - stopLoss
                tp_structural = levels['low_zone']
                
                # Priorizar TP estructural si cumple RR
                tp_final = adjustTPForMinRR(entryPrice, stopLoss, tp_structural, "CORTO", minRR=1.5)
                takeProfit = tp_final
                signalDirection = "CORTO"
            else:
                setupType = "LIQUIDATION_BUY"
                # Stop loss debajo de la formación del FVG o el mínimo reciente
                zona_low_fvg = datos5min['low'].iloc[fvg['idx']:fvg['idx']+2].min() if (fvg['idx']+2 < len(datos5min)) else datos5min['low'].iloc[fvg['idx']]
                stop_ref = min(zona_low_fvg, levels['swing_low'])
                stopLoss = stop_ref - padding_pips
                
                distanciaSl = stopLoss - entryPrice
                tp_structural = levels['high_zone']
                
                # Priorizar TP estructural si cumple RR
                tp_final = adjustTPForMinRR(entryPrice, stopLoss, tp_structural, "LARGO", minRR=1.5)
                takeProfit = tp_final
                signalDirection = "LARGO"
            
            rr_actual = calculateRR(entryPrice, stopLoss, takeProfit)
            multiplier = getPipMultiplier(symbol)
            
            distancia_sl = abs(entryPrice - stopLoss)
            distancia_tp = abs(takeProfit - entryPrice)
            min_distance_pips = 6.0
            min_distance_absolute = min_distance_pips / multiplier
            
            if distancia_sl < min_distance_absolute:
                logger.info(f"[{self.strategy_name}] FVG {idx+1} rechazada: distancia SL muy pequeña ({distancia_sl * multiplier:.1f} pips < {min_distance_pips} pips)")
                continue
            
            if distancia_tp < min_distance_absolute:
                logger.info(f"[{self.strategy_name}] FVG {idx+1} rechazada: distancia TP muy pequeña ({distancia_tp * multiplier:.1f} pips < {min_distance_pips} pips)")
                continue
            
            atr_min_distance = atr * 0.3
            if distancia_sl < atr_min_distance:
                logger.info(f"[{self.strategy_name}] FVG {idx+1} rechazada: distancia SL ({distancia_sl:.5f}) < 0.3*ATR ({atr_min_distance:.5f})")
                continue
            
            if distancia_tp < atr_min_distance:
                logger.info(f"[{self.strategy_name}] FVG {idx+1} rechazada: distancia TP ({distancia_tp:.5f}) < 0.3*ATR ({atr_min_distance:.5f})")
                continue
            
            # ── FILTRO: Verificar si el precio ya recorrió >60% hacia el TP ──
            vela_origen_idx = len(datos5min) - 5  # Usar vela actual como origen
            is_valid, recorrido_pct, mensaje = check_tp_exhaustion(datos5min, vela_origen_idx, entryPrice, takeProfit, stopLoss, signalDirection, threshold=0.60, timeframe="5min")
            if not is_valid:
                logger.info(f"[{self.strategy_name}][{symbol}] FVG {idx+1} descartada: Exhaustion - {mensaje}")
                continue
            
            # --- SEMÁFORO DE ENTRADA (Price Action) ---
            fvg_mid = entryPrice # En esta clase base, entryPrice ya es fvg['mid']
            total_path = abs(takeProfit - fvg_mid)
            if total_path == 0: total_path = 0.001
            
            # Calcular progreso: (Precio Actual - Mid) / Distancia Total al TP
            if direction == 'LARGO':
                progress_pct = (precioActual - fvg_mid) / total_path
            else:
                progress_pct = (fvg_mid - precioActual) / total_path
                
            if progress_pct >= 1.0:
                status_msg = "META ALCANZADA 🚨"
            elif progress_pct > 0.5:
                status_msg = "ALEJÁNDOSE ⚠️"
            else:
                status_msg = "EN ZONA ✅"

            # Momentum Filter: Usar momentum pre-calculado desde main.py
            momentum_estado = symbolInfo.get('momentum', '☁️ SIN DATOS') if symbolInfo else '☁️ SIN DATOS'
            momentum_bonus, _ = momentum.getMomentumBonus(momentum_estado, signalDirection)
            
            logger.info(f"[{self.strategy_name}][{symbol}] Momentum: {momentum_estado} → {'+' if momentum_bonus > 0 else ''}{momentum_bonus}% confianza")
            
            signals.append({
                "symbol": symbol,
                "direction": signalDirection,
                "entryPrice": entryPrice,
                "stopLoss": stopLoss,
                "takeProfit": takeProfit,
                "slDistance": distancia_sl,
                "riesgo_pips": round(abs(entryPrice - stopLoss) * multiplier, 1),
                "rr_ratio": round(rr_actual, 2),
                "setup": setupType,
                "strategy": self.strategy_name,
                "fvg": fvg['type'],
                "fvgNum": idx + 1,
                "fvgTime": fvgTimeStr,
                "dentroRango": fvg.get('dentroRango', True),
                "velaCorteType": direction,
                "symbolInfo": symbolInfo,
                "confidence": 75 + momentum_bonus,
                "momentum": momentum_estado,
                "candle_time": last_closed_str,
                "status": status_msg
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
            # Excluir cuenta maestra de señales (SENTINEL)
            if account['idCuenta'] == 1: continue
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
            
            if posSize is None or posSize == 0:
                logger.warning(f"[{self.strategy_name}] Size=0 para {symbolInfo['symbol']} - riesgo ${riskUsd:.2f} < $5 mínimo")
                continue
            
            signal['profit'] = riskUsd
            
            direction = signal['direction']
            entryPrice = signal['entryPrice']
            slDist = signal['slDistance']
            fvgNum = signal.get('fvgNum', 0)
            
            slPrice = signal['stopLoss']
            
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
                "strategy": self.strategy_name,
                "fvgNum": fvgNum,
                "margin_used": marginUsed,
            }
            
            # Ejecución centralizada vía Gateway (DB + Telegram + Broker)
            from middleware.execution.broker_gateway import gateway
            success, msgId = await gateway.execute_trade(trade, signal, account, self.strategy_name, df=datos5min)
            
            if success and msgId:
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

        logger.info(f"[{self.strategy_name}] ◀ SALIENDO análisis para {symbol}")
