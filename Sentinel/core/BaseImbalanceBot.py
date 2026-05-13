import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Dict, Any, Optional
import pandas as pd
import numpy as np
import talib as ta
import asyncio
import pytz

from middleware.database import dbManager
from Sentinel.analysis.technical import check_tp_exhaustion, check_signal_health
from middleware.utils import momentum

from middleware.utils.alertBuilder import adjustTPForMinRR, getPipMultiplier, calculateRR, calculateBEPrice
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle
from Sentinel.core.models import Signal

logger = logging.getLogger("sentinel")

async def getAssetConfig(symbol: str) -> dict:
    import asyncio
    symbolData = await asyncio.to_thread(dbManager.getSymbol, symbol)
    assetType = symbolData.get('tipo', 'MONEDA') if symbolData else 'MONEDA'
    
    typeConfig = await asyncio.to_thread(dbManager.getSymbolTypeConfig, assetType)
    
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
                if closePrice > openPrice and (cuerpo / rango) > 0.6 and ((highPrice - closePrice) / rango) <= 0.25:
                    return {
                        'type': 'LARGO',
                        'idx': i,
                        'vela': vela,
                        'precioRuptura': bodyTop
                    }
            
            if bodyBottom < precioMinimo and bodyTop < precioMinimo:
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
    
    async def _getSignals(self, datos5min: pd.DataFrame, symbolInfo: Dict) -> list[Signal]:
        symbol = symbolInfo['symbol']
        precioMaximo = symbolInfo.get('precioMaximo')
        precioMinimo = symbolInfo.get('precioMinimo')
        
        if precioMaximo is None or precioMinimo is None:
            return []
        
        strat_config = dbManager.getStrategyConfig(self.strategy_name) or {}
        
        adx = ta.ADX(datos5min['high'], datos5min['low'], datos5min['close'], timeperiod=14)
        adx_val = float(adx.dropna().iloc[-1]) if len(adx.dropna()) > 0 else 25.0
        if adx_val < 20:
            return []
        
        if self.signalGenerada:
            return []
        
        if self.velaCorte is None:
            velaCorte = self.findVelaCorte(datos5min, precioMaximo, precioMinimo)
            if velaCorte is None:
                return []
            self.velaCorte = velaCorte
        
        velaCorte = self.velaCorte
        direction = velaCorte['type']
        startSearch = velaCorte['idx'] + 1
        
        fvgs_dentro = self.findFvgEnRango(datos5min, startSearch, direction, precioMaximo, precioMinimo, maxFvg=2)
        fvgs_fuera = self.findFvgFueraRango(datos5min, startSearch, direction, precioMaximo, precioMinimo, maxFvg=2)
        
        fvgs = fvgs_dentro + fvgs_fuera
        if not fvgs:
            return []
        
        signals = []
        ahora = self.getMexicoTime().replace(tzinfo=None)
        now_cdmx = datetime.now(ZoneInfo(TIMEZONE))
        last_v = get_last_closed_candle(now_cdmx, interval=5, df=datos5min)
        last_closed_ts = last_v.name if hasattr(last_v, 'name') else last_v
        last_closed_str = last_closed_ts.strftime("%Y-%m-%d %H:%M:%S")

        
        for idx, fvg in enumerate(fvgs):
            fvg_time = pd.Timestamp(datos5min.index[fvg['idx']]).tz_localize(None)
            minutos_desde_fvg = (ahora - fvg_time).total_seconds() / 60
            if minutos_desde_fvg > self.maxMinutosFvg:
                continue
            
            # Validar mitigación ICT (50% rule): descartar si el precio ya cerró
            # dentro del gap superando el punto medio
            from Sentinel.analysis import technical as _technical
            if _technical._is_fvg_mitigated(datos5min, fvg['idx'], fvg):
                logger.debug(f"[{symbol}] {self.strategy_name}: FVG mitigado (50% rule) - descartando")
                continue
            
            entryPrice = fvg['mid']
            atr = ta.ATR(datos5min['high'], datos5min['low'], datos5min['close'], 14).iloc[-1]
            
            if pd.isna(atr) or pd.isna(entryPrice):
                continue
            
            assetConfig = await getAssetConfig(symbol)
            slMultiplier = assetConfig["sl"]
            
            padding_pips = atr * slMultiplier
            from Sentinel.analysis import technical
            levels = technical.get_structural_levels(datos5min, lookback=30)
            
            if direction == 'CORTO':
                setupType = "LIQUIDATION_SELL"
                zona_high_fvg = datos5min['high'].iloc[fvg['idx']:fvg['idx']+2].max() if (fvg['idx']+2 < len(datos5min)) else datos5min['high'].iloc[fvg['idx']]
                stop_ref = max(zona_high_fvg, levels['swing_high'])
                stopLoss = stop_ref + padding_pips
                tp_structural = levels['low_zone']
                min_rr_val = float(dbManager.getStrategyConfig(self.strategy_name).get('min_rr', 1.5)) if dbManager.getStrategyConfig(self.strategy_name) else 1.5
                tp_final = adjustTPForMinRR(entryPrice, stopLoss, tp_structural, "CORTO", minRR=min_rr_val)
                signalDirection = "CORTO"
            else:
                setupType = "LIQUIDATION_BUY"
                zona_low_fvg = datos5min['low'].iloc[fvg['idx']:fvg['idx']+2].min() if (fvg['idx']+2 < len(datos5min)) else datos5min['low'].iloc[fvg['idx']]
                stop_ref = min(zona_low_fvg, levels['swing_low'])
                stopLoss = stop_ref - padding_pips
                tp_structural = levels['high_zone']
                min_rr_val = float(dbManager.getStrategyConfig(self.strategy_name).get('min_rr', 1.5)) if dbManager.getStrategyConfig(self.strategy_name) else 1.5
                tp_final = adjustTPForMinRR(entryPrice, stopLoss, tp_structural, "LARGO", minRR=min_rr_val)
                signalDirection = "LARGO"
            
            distancia_sl = abs(entryPrice - stopLoss)
            multiplier = getPipMultiplier(symbol)
            
            # Exhaustion Filter
            vela_origen_idx = len(datos5min) - 5
            is_valid, _, mensaje = check_tp_exhaustion(datos5min, vela_origen_idx, entryPrice, tp_final, stopLoss, signalDirection, threshold=0.60, timeframe="5min")
            if not is_valid:
                continue
            
            # Signal Health Filter (precio actual vs SL y progreso)
            current_price = float(datos5min['close'].iloc[-1])
            is_valid, _, mensaje = check_signal_health(entryPrice, tp_final, stopLoss, signalDirection, current_price, threshold=0.65, candle_time=last_closed_str)
            if not is_valid:
                continue
            
            momentum_estado = symbolInfo.get('momentum', '☁️ SIN DATOS')
            momentum_bonus, _ = momentum.getMomentumBonus(momentum_estado, signalDirection)
            
            # --- FILTRO HTF: Solo operar a favor de la tendencia mensual/semanal ---
            monthly_trend = symbolInfo.get('weekly_trend', 'NEUTRAL')
            if monthly_trend == "BAJISTA" and signalDirection == "LARGO":
                logger.info(f"[{symbol}] {self.strategy_name}: Señal LARGO bloqueada - Tendencia HTF BAJISTA")
                continue
            elif monthly_trend == "ALCISTA" and signalDirection == "CORTO":
                logger.info(f"[{symbol}] {self.strategy_name}: Señal CORTO bloqueada - Tendencia HTF ALCISTA")
                continue

            base_confidence = 75 + momentum_bonus
            
            # --- Cálculo de Tamaño de Posición Real (Centralizado) ---
            from Sentinel.analysis import risk
            refCapital = symbolInfo.get('refCapital', 10000.0)
            refRiskPct = symbolInfo.get('refRiskPct', 1.0)
            
            # Usar precio actual como entrada real para el cálculo de riesgo
            currentPrice = float(datos5min['close'].iloc[-1])
            realEntry = currentPrice
            realRiskDist = abs(realEntry - stopLoss)
            
            size, riskUsdActual, marginUsed = risk.calculatePositionSize(
                refCapital, refRiskPct, realRiskDist, symbolInfo, entryPrice=realEntry
            )
            
            if size is None or size <= 0:
                logger.info(f"[{symbol}] {self.strategy_name}: Tamaño de posición inválido o margen insuficiente - descartando")
                continue
                
            minUsdProfit = float(strat_config.get('min_usd_profit', 10.0))
            rrRatio = round(abs(tp_final - realEntry) / realRiskDist, 2) if realRiskDist > 0 else 0
            expectedProfit = riskUsdActual * rrRatio
            
            if expectedProfit < minUsdProfit:
                logger.info(f"[{symbol}] {self.strategy_name}: Beneficio Est. ${expectedProfit:.2f} < ${minUsdProfit:.2f} - descartando")
                continue

            min_confidence = float(strat_config.get('min_confidence', 70))
            if base_confidence < min_confidence:
                logger.info(f"[{symbol}] {self.strategy_name}: confidence={base_confidence} < min_confidence={min_confidence} - descartando")
                continue
            
            # Calcular Break Even inteligente
            be_trigger = calculateBEPrice(entryPrice, stopLoss, tp_final, signalDirection)
            
            signals.append(Signal(
                strategy=self.strategy_name,
                symbol=symbol,
                direction=signalDirection,
                entry_price=realEntry,
                stop_loss=stopLoss,
                take_profit=tp_final,
                sl_distance=realRiskDist,
                confidence=base_confidence,
                setup=setupType,
                status="EN ZONA ✅",
                candleTime=last_closed_str,
                intervalo="5min",
                riesgo_pips=round(realRiskDist * multiplier, 1),
                rr_ratio=rrRatio,
                break_even=calculateBEPrice(realEntry, stopLoss, tp_final, signalDirection),
                size=size,
                metadata={
                    "riskUsd": round(riskUsdActual, 2),
                    "expectedProfit": round(expectedProfit, 2),
                    "marginUsed": round(marginUsed, 2),
                    "fvg": fvg['type'],
                    "fvgTime": fvg_time.strftime("%Y-%m-%d %H:%M:%S")
                }
            ))

        
        self.signalGenerada = True
        return signals

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> list[Signal]:
        symbol = symbolInfo['symbol']
        master = preloadedData.get(symbol) if preloadedData else None
        
        # Punto 3: Master Dictionary integration
        if isinstance(master, dict):
            datos5min = master.get('5min')
        else:
            datos5min = master
        
        if datos5min is None or len(datos5min) < 1:
            return []

        
        signals = await self._getSignals(datos5min, symbolInfo)
        
        for sig in signals:
            fvg_num = sig.metadata.get('fvgNum', 0)
            ahora = self.getMexicoTime()
            if fvg_num == 1:
                self.signal1_enviada = True
                self.timestamp_signal1 = ahora
            elif fvg_num == 2:
                self.signal2_enviada = True
                self.timestamp_signal2 = ahora
                
        return signals
