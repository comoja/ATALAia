import asyncio
import logging
import pandas as pd
from datetime import datetime
import pytz
import talib as ta

from Sentinel.core.models import Signal
from Sentinel.analysis import technical
from middleware.database import dbManager
from dataSymbol.mainOrchestrator import get_last_closed_candle
from middleware.utils.alertBuilder import getPipMultiplier, calculateBEPrice, adjustTPForMinRR

logger = logging.getLogger('sentinel')

class GenericFVGBot:
    """
    Bot evolucionado a SMC (Toni Maura/ICT) que busca re-tests de FVGs
    tras un Market Structure Shift (MSS) con objetivos estructurales.
    """
    def __init__(self, intervals=['5min', '15min', '1h', '4h']):
        self.strategy_name = "GenericFVG"
        self.intervals = intervals
        self._sent_signals = {}
        
    def _now_mx(self):
        return datetime.now(pytz.timezone('America/Mexico_City'))

    async def runAnalysisCycleForSymbol(self, symbolInfo, preloaded_data):
        """
        Analiza un símbolo en múltiples intervalos usando datos pre-cargados.
        """
        symbol = symbolInfo['symbol']
        logger.info(f"Analizando {symbol} en intervalos {self.intervals}")
        
        signals = []
        strat_config = dbManager.getStrategyConfig(self.strategy_name) or {}
        min_confidence = float(strat_config.get('min_confidence', 80))
        min_rr_val = float(strat_config.get('min_rr', 1.5))
        
        multiplier = getPipMultiplier(symbol)
        preloaded_master = preloaded_data.get(symbol)
        if preloaded_master is None:
            return []
            
        logger.info(f"Analizando {symbol} (Toni Maura SMC) en {self.intervals}")
        for interval in self.intervals:
            df = preloaded_master.get(interval)
            if df is None or len(df) < 200:
                continue
            
            # 1. Detectar FVGs
            fvgs = technical.detect_fvgs(df)
            if not fvgs:
                continue
            
            # Tomar el más reciente
            latestFvg = fvgs[-1]
            fvgDirection = "LARGO" if latestFvg['type'] == 'Bullish_FVG' else "CORTO"

            # --- FILTRO MTF: Verificar HTF Liquidity Sweep (regla video) ---
            # Solo operar el FVG si hay un sweep de liquidez real en el HTF
            # (precio superó PDH/PDL y cerró de vuelta dentro del rango)
            htfLevels = technical.get_prev_day_high_low(df)
            htfHigh   = htfLevels.get('pdh')
            htfLow    = htfLevels.get('pdl')
            if htfHigh and htfLow:
                htfSweep = technical.detectLiquiditySweep(df, htfHigh=htfHigh, htfLow=htfLow, lookback=20)
                if not htfSweep:
                    # Sin sweep HTF confirmado no hay 'intención real' detrás de la entrada
                    logger.debug(f"[{symbol}] {interval}: Sin HTF liquidity sweep confirmado - saltando")
                    continue
                # Validar alineación: el FVG debe ser en dirección opuesta al sweep
                sweepType = htfSweep.get('type', '')
                if sweepType == 'MANIPULATION_UP' and fvgDirection != 'CORTO':
                    logger.debug(f"[{symbol}] {interval}: FVG no alineado al bias HTF (sweep UP → solo CORTO)")
                    continue
                if sweepType == 'MANIPULATION_DOWN' and fvgDirection != 'LARGO':
                    logger.debug(f"[{symbol}] {interval}: FVG no alineado al bias HTF (sweep DOWN → solo LARGO)")
                    continue

            latest_fvg = latestFvg
            signal_direction = fvgDirection
            
            # --- FILTRO MAURA 1: MSS (Market Structure Shift) ---
            if not technical.detect_mss(df, signal_direction, lookback=15):
                continue

            # Evitar señales duplicadas
            signal_key = f"{symbol}_{interval}_{latest_fvg['timestamp']}"
            if signal_key in self._sent_signals:
                continue
            
            # 2. Calcular niveles SMC
            # --- FILTRO SEGURIDAD: Antigüedad del FVG (Máx 24h) ---
            fvg_time = pd.to_datetime(latest_fvg['timestamp'])
            ahora = self._now_mx().replace(tzinfo=None)
            fvg_time_naive = fvg_time.replace(tzinfo=None)
            age_hours = (ahora - fvg_time_naive).total_seconds() / 3600
            
            if age_hours > 24:
                # logger.debug(f"[{symbol}] {interval}: FVG demasiado antiguo ({age_hours:.1f}h) - saltando")
                continue

            # --- Cálculo de Niveles Centralizado (Maura SMC) ---
            atr = ta.ATR(df['high'], df['low'], df['close'], 14).iloc[-1]
            current_price = float(df['close'].iloc[-1])
            setup_fvg = technical.calculate_fvg_setup(latest_fvg, current_price, atr)
            
            entry_price = setup_fvg['entry']
            sl = setup_fvg['sl']
            signal_direction = setup_fvg['direction']
            sl_dist = setup_fvg['sl_dist']
            levels = technical.get_structural_levels(df, lookback=60)
            # Usar high_zone/low_zone (percentil 90/10) en lugar de swing_high/low absoluto
            # para evitar TPs que apuntan al máximo histórico de las últimas 15h
            if signal_direction == "LARGO":
                tp_ref = levels['high_zone']
            else:
                tp_ref = levels['low_zone']

            # Cap de TP por ATR: máximo 3.0 ATR desde el precio actual (estrategia multi-TF)
            from Sentinel.analysis.technical import capTpByAtr
            tp_ref = capTpByAtr(tp_ref, current_price, float(atr), signal_direction, maxAtrMult=3.0)
            
            tp1 = adjustTPForMinRR(entry_price, sl, tp_ref, signal_direction, minRR=min_rr_val)
            
            risk_dist = abs(entry_price - sl)
            reward_dist = abs(tp1 - entry_price)
            rr_ratio = reward_dist / risk_dist if risk_dist > 0 else 0
            
            # --- FILTRO SEGURIDAD: RR Máximo (Evitar errores de data) ---
            if rr_ratio > 15:
                # logger.info(f"[{symbol}] {interval}: RR={rr_ratio:.2f} irreal - descartando")
                continue

            # --- Health Check ---
            current_price = float(df['close'].iloc[-1])
            fvg_time = latest_fvg.get('candle_time', '')
            fvg_time_str = fvg_time.strftime("%Y-%m-%d %H:%M:%S") if fvg_time else ""
            
            is_healthy, progress_pct, reason = technical.check_signal_health(
                entry_price, tp1, sl, 
                "LARGO" if latest_fvg['type'] == 'Bullish_FVG' else "CORTO",
                current_price,
                threshold=3.5, # Permitir hasta 350% (re-tests lejanos)
                candle_time=fvg_time_str
            )
            
            if not is_healthy:
                # Si el precio ya tocó SL no insistir (ya logueado en technical)
                if current_price >= sl if latest_fvg['type'] == 'Bearish_FVG' else current_price <= sl:
                    continue
                
                # Si ya avanzó demasiado (ej. 350% del tamaño del gap), descartar
                if progress_pct > 3.5:
                    continue
            
            # 3. Verificar que precio actual no haya invalidado el SL
            if latest_fvg['type'] == 'Bullish_FVG':
                if current_price <= sl:
                    logger.info(f"[{symbol}] {interval}: Precio tocó SL (price={current_price:.5f}, sl={sl:.5f}) - descartando")
                    continue
            else:
                if current_price >= sl:
                    logger.info(f"[{symbol}] {interval}: Precio tocó SL (price={current_price:.5f}, sl={sl:.5f}) - descartando")
                    continue
            
            # --- FILTRO: Ganancia Mínima Est. ---
            multiplier = getPipMultiplier(symbol)
            # --- Cálculo de Tamaño de Posición Real (Centralizado) ---
            from Sentinel.analysis import risk
            refCapital = symbolInfo.get('refCapital', 10000.0)
            refRiskPct = symbolInfo.get('refRiskPct', 1.0)
            
            # Usar precio actual como entrada real para el cálculo de riesgo
            realEntry = current_price
            realRiskDist = abs(realEntry - sl)
            
            size, riskUsdActual, marginUsed = risk.calculatePositionSize(
                refCapital, refRiskPct, realRiskDist, symbolInfo, entryPrice=realEntry
            )
            
            if size is None or size <= 0:
                logger.info(f"[{symbol}] {interval}: Tamaño de posición inválido o margen insuficiente - descartando")
                continue
                
            minUsdProfit = float(strat_config.get('min_usd_profit', 10.0))
            rrVal = round(abs(tp1 - realEntry) / realRiskDist, 2) if realRiskDist > 0 else 0
            expectedProfit = (abs(tp1 - realEntry) * multiplier * (size / 100000.0 if "JPY" not in symbol else size / 100.0)) # Simplificado
            # Mejor usar el riskUsdActual * rrVal
            expectedProfit = riskUsdActual * rrVal
            
            if expectedProfit < minUsdProfit:
                logger.info(f"[{symbol}] {interval}: Beneficio Est. ${expectedProfit:.2f} < ${minUsdProfit:.2f} - descartando")
                continue

            # 4. Filtrar por confianza mínima
            base_confidence = 85
            if base_confidence < min_confidence:
                logger.info(f"[{symbol}] {interval}: confidence={base_confidence} < min_confidence={min_confidence} - descartando")
                continue
            
            # 5. Filtrar por tendencia HTF (21 días con filtro de neutralidad)
            monthly_trend = symbolInfo.get('weekly_trend', 'NEUTRAL')
            signal_direction = "LARGO" if latest_fvg['type'] == 'Bullish_FVG' else "CORTO"
            
            if monthly_trend == "BAJISTA" and signal_direction == "LARGO":
                logger.info(f"[{symbol}] {interval}: Señal LARGO descartada - tendencia mensual BAJISTA")
                continue
            elif monthly_trend == "ALCISTA" and signal_direction == "CORTO":
                logger.info(f"[{symbol}] {interval}: Señal CORTO descartada - tendencia mensual ALCISTA")
                continue
            
            # Marcar como enviada en RAM
            self._sent_signals[signal_key] = True
            
            # Crear objeto Signal
            # Usar la hora de la vela origen para trazabilidad
            minutes = 5
            if 'min' in interval: minutes = int(interval.replace('min', ''))
            elif 'h' in interval: minutes = int(interval.replace('h', '')) * 60
            elif '4h' in interval: minutes = 240
            elif '1d' in interval: minutes = 1440
            
            last_v = get_last_closed_candle(self._now_mx(), minutes, df=df)
            candle_time_obj = last_v.name if hasattr(last_v, 'name') else last_v
            
            origin_candle_time = latest_fvg.get('candle_time', candle_time_obj)
            
            # Calcular Break Even inteligente
            be_trigger = calculateBEPrice(entry_price, sl, tp1, signal_direction)

            sig = Signal(
                strategy=self.strategy_name,
                symbol=symbol,
                direction=signal_direction,
                entry_price=realEntry,
                stop_loss=sl,
                take_profit=tp1,
                sl_distance=realRiskDist,
                confidence=base_confidence,
                setup=f"FVG {interval}",
                status="EN ZONA ✅",
                candleTime=candle_time_obj.strftime("%Y-%m-%d %H:%M:%S"),
                intervalo=interval,
                riesgo_pips=round(realRiskDist * multiplier, 1),
                rr_ratio=rrVal,
                break_even=calculateBEPrice(realEntry, sl, tp1, signal_direction),
                size=size,
                metadata={
                    "risk_usd": round(riskUsdActual, 2),
                    "expected_profit": round(expectedProfit, 2),
                    "margin_used": round(marginUsed, 2),
                    "fvg": latest_fvg.get('type', 'N/A'),
                    "fvgTime": latest_fvg.get('timestamp', 'N/A')
                }
            )
            signals.append(sig)
            
        return signals
