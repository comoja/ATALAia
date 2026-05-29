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
        self._sent_signals = {}
        self.timestamp_signal1 = None
        self.timestamp_signal2 = None
        self.signal1_enviada = False
        self.signal2_enviada = False
        
        strategyConfig = dbManager.getStrategyConfig(self.strategy_name)
        self.maxMinutosFvg = strategyConfig.get('max_minutos_fvg', 20) if strategyConfig else 20
        self.maxMinutosSignal = strategyConfig.get('max_minutos_signal', 20) if strategyConfig else 20
    
    def getMexicoTime(self) -> datetime:
        return datetime.now(self.MEXICO_TZ)
    
    # detectarFvg(), findFvgEnRango() y findFvgFueraRango() fueron eliminados.
    # _getSignals() usa Sentinel.analysis.technical.detect_fvgs() como fuente
    # centralizada con EMA200, Market Structure Shift y clasificación de probabilidad.
    
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
    
    async def _getSignals(self, datos5min: pd.DataFrame, symbolInfo: Dict) -> list[Signal]:
        symbol = symbolInfo['symbol']
        precioMaximo = symbolInfo.get('precioMaximo')
        precioMinimo = symbolInfo.get('precioMinimo')
        
        if precioMaximo is None or precioMinimo is None:
            return []
        
        strat_config = dbManager.getStrategyConfig(self.strategy_name) or {}
        
        if self.velaCorte is None:
            velaCorte = self.findVelaCorte(datos5min, precioMaximo, precioMinimo)
            if velaCorte is None:
                return []
            self.velaCorte = velaCorte
        
        velaCorte = self.velaCorte
        direction = velaCorte['type']
        startSearch = velaCorte['idx'] + 1
        
        # Centralizar lógica: Usar detect_fvgs de alta probabilidad y descartar trampas de mecha (Rechazo/Baja Probabilidad)
        from Sentinel.analysis import technical as _technical
        raw_fvgs = _technical.detect_fvgs(datos5min, apply_high_prob_filters=True)
        
        fvgs = []
        for f in raw_fvgs:
            if f.get('classification') == 'Rechazo/Baja Probabilidad':
                logger.info(f"[{symbol}] FVG {f['idx']} ({f['type']}) de Rechazo/Baja Probabilidad (trampa) - omitido en Imbalance")
                continue
            
            fvg_direction = 'LARGO' if f['type'] == 'Bullish_FVG' else 'CORTO'
            if fvg_direction != direction:
                continue
                
            # Solo procesar FVGs generados a partir de velaCorte
            if f['idx'] < startSearch:
                continue
                
            # Construir objeto compatible con la lógica subsiguiente
            f_compat = {
                'type': f['type'],
                'start': f['bottom'] if fvg_direction == 'LARGO' else f['top'],
                'end': f['top'] if fvg_direction == 'LARGO' else f['bottom'],
                'mid': f['mid'],
                'size': f['size'],
                'idx': f['idx']
            }
            
            # Clasificar si está dentro o fuera del rango de la sesión
            fvg_candle_idx = f['idx']
            vela = datos5min.iloc[fvg_candle_idx]
            highPrice = vela['high']
            lowPrice = vela['low']
            dentroRango = highPrice <= precioMaximo and lowPrice >= precioMinimo
            f_compat['dentroRango'] = dentroRango
            
            fvgs.append(f_compat)
            if len(fvgs) >= 4: # tope de FVGs a procesar
                break
                
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
            
            signalKey = f"{symbol}_5min_{fvg_time.strftime('%Y-%m-%d %H:%M:%S')}"
            if signalKey in self._sent_signals:
                continue
                
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
                # Cap de TP: máximo 3.0 ATR desde la entrada (estrategia 5min)
                from Sentinel.analysis.technical import capTpByAtr
                tp_structural = capTpByAtr(tp_structural, entryPrice, float(atr), "CORTO", maxAtrMult=3.0)
                min_rr_val = float(dbManager.getStrategyConfig(self.strategy_name).get('min_rr', 1.5)) if dbManager.getStrategyConfig(self.strategy_name) else 1.5
                tp_final = adjustTPForMinRR(entryPrice, stopLoss, tp_structural, "CORTO", minRR=min_rr_val)
                signalDirection = "CORTO"
            else:
                setupType = "LIQUIDATION_BUY"
                zona_low_fvg = datos5min['low'].iloc[fvg['idx']:fvg['idx']+2].min() if (fvg['idx']+2 < len(datos5min)) else datos5min['low'].iloc[fvg['idx']]
                stop_ref = min(zona_low_fvg, levels['swing_low'])
                stopLoss = stop_ref - padding_pips
                tp_structural = levels['high_zone']
                # Cap de TP: máximo 3.0 ATR desde la entrada (estrategia 5min)
                from Sentinel.analysis.technical import capTpByAtr
                tp_structural = capTpByAtr(tp_structural, entryPrice, float(atr), "LARGO", maxAtrMult=3.0)
                min_rr_val = float(dbManager.getStrategyConfig(self.strategy_name).get('min_rr', 1.5)) if dbManager.getStrategyConfig(self.strategy_name) else 1.5
                tp_final = adjustTPForMinRR(entryPrice, stopLoss, tp_structural, "LARGO", minRR=min_rr_val)
                signalDirection = "LARGO"
            
            distancia_sl = abs(entryPrice - stopLoss)
            multiplier = getPipMultiplier(symbol)
            
            # Exhaustion Filter
            vela_origen_idx = len(datos5min) - 5
            is_valid, _, mensaje = check_tp_exhaustion(datos5min, vela_origen_idx, entryPrice, tp_final, stopLoss, signalDirection, threshold=0.75, timeframe="5min")
            if not is_valid:
                continue
            
            # Signal Health Filter (precio actual vs SL y progreso)
            current_price = float(datos5min['close'].iloc[-1])
            is_valid, _, mensaje = check_signal_health(entryPrice, tp_final, stopLoss, signalDirection, current_price, threshold=0.65, candle_time=last_closed_str)
            if not is_valid:
                continue
            
            momentum_estado = symbolInfo.get('momentum', '☁️ SIN DATOS')
            momentum_bonus, _ = momentum.getMomentumBonus(momentum_estado, signalDirection)
            
            # --- FILTRO HTF: Solo operar a favor de la tendencia macro ---
            weeklyTrend = str(symbolInfo.get('weekly_trend', 'NEUTRAL')).upper()
            if signalDirection == "LARGO" and ("BAJISTA" in weeklyTrend or "LIQUIDACION" in weeklyTrend):
                logger.info(f"[{symbol}] {self.strategy_name}: Señal LARGO bloqueada - Tendencia macro BAJISTA ({weeklyTrend})")
                continue
            elif signalDirection == "CORTO" and ("ALCISTA" in weeklyTrend or "GIRO" in weeklyTrend):
                logger.info(f"[{symbol}] {self.strategy_name}: Señal CORTO bloqueada - Tendencia macro ALCISTA ({weeklyTrend})")
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
            
            self._sent_signals[signalKey] = True

        return signals

    async def runAnalysisCycleForSymbol(self, symbolInfo: Dict, preloadedData: Dict = None, apiKey: str = None) -> list[Signal]:
        symbol = symbolInfo['symbol']
        master = preloadedData.get(symbol) if preloadedData else None
        logger.info(f"[{symbol}] {self.strategy_name} ")
        # Punto 3: Master Dictionary integration
        if isinstance(master, dict):
            datos5min = master.get('5min')
        else:
            datos5min = master
        
        if datos5min is None or len(datos5min) < 1:
            logger.info(f"[{symbol}] Datos insuficientes en {self.strategy_name}")
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
