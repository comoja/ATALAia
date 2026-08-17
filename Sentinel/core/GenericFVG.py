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
from middleware.config.constants import MODEL_FEATURES
from Sentinel.ml.model import loadModel, predictProba

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
        self.ml_model = loadModel()
        
    def _now_mx(self):
        return datetime.now(pytz.timezone('America/Mexico_City'))

    async def runAnalysisCycleForSymbol(self, symbolInfo, preloadedData):
        """
        Analiza un símbolo en múltiples intervalos usando datos pre-cargados.
        """
        symbol = symbolInfo['symbol']
        logger.info(f"Analizando {symbol} en intervalos {self.intervals}")
        
        signals = []
        stratConfig = dbManager.getSymbolStrategyConfig(self.strategy_name, symbol) or {}
        minConfidence = float(stratConfig.get('minConfidence') or stratConfig.get('min_confidence') or 80.0)
        minRrVal = float(stratConfig.get('minRr') or stratConfig.get('min_rr') or 1.5)
        
        multiplier = getPipMultiplier(symbol)
        preloadedMaster = preloadedData.get(symbol)
        if preloadedMaster is None:
            logger.info(f"[{symbol}] Datos insuficientes")
            return []
            
        logger.info(f"Analizando {symbol} (Toni Maura SMC) en {self.intervals}")
        for interval in self.intervals:
            df = preloadedMaster.get(interval)
            
            # Adaptar el requerimiento mínimo de velas según el intervalo para optimizar datos
            requiredCandles = 80 if interval == '4h' else 40 if interval == '1d' else 200
            if df is None or len(df) < requiredCandles:
                logger.info(f"[{symbol}] Datos insuficientes en {interval}")
                continue
            
            fvgMinPct = float(stratConfig.get('fvgMinPct') or stratConfig.get('fvg_min_pct') or 0.0001)
            useImpulseMacdFilter = bool(int(stratConfig.get('useImpulseMacdFilter', 0)))
            macdSlow = int(stratConfig.get('macdSlow', 34))
            macdSignal = int(stratConfig.get('macdSignal', 9))
            
            # 1. Detectar FVGs con filtros de alta probabilidad activos (tendencia EMA 200 y MSS de Vela 2)
            fvgs = technical.detect_fvgs(df, apply_high_prob_filters=True, min_gap_pct=fvgMinPct, use_impulse_macd_filter=useImpulseMacdFilter, macd_slow=macdSlow, macd_signal=macdSignal)
            if not fvgs:
                logger.info(f"[{symbol}] No se detecto FVG de alta probabilidad en {interval}")
                continue
            
            # Tomar el más reciente
            latestFvg = fvgs[-1]
            
            # Filtrar trampas / rechazos de mecha
            classification = latestFvg.get('classification', 'Alta Probabilidad')
            if classification == 'Rechazo/Baja Probabilidad':
                logger.info(f"[{symbol}] {interval}: FVG de Rechazo/Baja Probabilidad detectado (trampa de liquidez) - descartando")
                continue
                
            fvgDirection = "LARGO" if latestFvg['type'] == 'Bullish_FVG' else "CORTO"

            # --- FILTRO MTF: Verificar HTF Liquidity Sweep (regla video, flexibilizada) ---
            # Solo operar el FVG si hay un sweep de liquidez real en el HTF.
            # Se ha ampliado el lookback dinámicamente y se hace opcional mediante base de datos.
            requireHtfSweep = stratConfig.get('requireHtfSweep') or stratConfig.get('require_htf_sweep') or False
            htfLevels = technical.get_prev_day_high_low(df)
            htfHigh   = htfLevels.get('pdh')
            htfLow    = htfLevels.get('pdl')
            
            if htfHigh and htfLow:
                # Lookback dinámico según el timeframe: mayor lookback para timeframes pequeños
                sweepLookback = 150 if interval in ['5min', '15min'] else 50 if interval == '1h' else 30
                htfSweep = technical.detectLiquiditySweep(df, htfHigh=htfHigh, htfLow=htfLow, lookback=sweepLookback)
                
                if htfSweep:
                    # Validar alineaición: el FVG debe ser en dirección opuesta al sweep
                    sweepType = htfSweep.get('type', '')
                    if sweepType == 'MANIPULATION_UP' and fvgDirection != 'CORTO':
                        logger.info(f"[{symbol}] {interval}: FVG no alineado al bias HTF (sweep UP → solo CORTO)")
                        continue
                    if sweepType == 'MANIPULATION_DOWN' and fvgDirection != 'LARGO':
                        logger.info(f"[{symbol}] {interval}: FVG no alineado al bias HTF (sweep DOWN → solo LARGO)")
                        continue
                else:
                    if requireHtfSweep:
                        logger.info(f"[{symbol}] {interval}: Sin HTF liquidity sweep confirmado (requerido) - saltando")
                        continue
                    else:
                        logger.debug(f"[{symbol}] {interval}: Sin sweep HTF confirmado, pero se continúa según configuración")

            latestFvgRef = latestFvg
            signalDirection = fvgDirection
            
            # --- FILTRO DE TENDENCIA MACRO UNIFICADO (Body-to-Body / SMC Alignment) ---
            # Solo tomar compras si la tendencia macro del instrumento es alcista y ventas si es bajista
            weeklyTrend = str(symbolInfo.get('weekly_trend', 'NEUTRAL')).upper()
            if signalDirection == "LARGO" and ("BAJISTA" in weeklyTrend or "LIQUIDACION" in weeklyTrend):
                logger.info(f"[{symbol}] {interval}: FVG LARGO descartado porque la tendencia macro es bajista ({weeklyTrend})")
                continue
            if signalDirection == "CORTO" and ("ALCISTA" in weeklyTrend or "GIRO" in weeklyTrend):
                logger.info(f"[{symbol}] {interval}: FVG CORTO descartado porque la tendencia macro es alcista ({weeklyTrend})")
                continue
            
            # --- FILTRO MAURA 1: MSS (Market Structure Shift) Comentado por contradicción lógica en retest ---
            # El MSS ya se valida a nivel de vela de impulso (Vela 2) en detect_fvgs.
            # Exigirlo en la vela de retest bloquea todas las señales porque el precio está retrocediendo (no rompiendo máximos).
            # if not technical.detect_mss(df, signalDirection, lookback=15):
            #     logger.info(f"[{symbol}] {interval}: No se detecto MSS")
            #     continue

            # Evitar señales duplicadas
            signalKey = f"{symbol}_{interval}_{latestFvgRef['timestamp']}"
            if signalKey in self._sent_signals:
                logger.info(f"[{symbol}] {interval}: Señal duplicada")
                continue
            
            # 2. Calcular niveles SMC
            # --- FILTRO SEGURIDAD: Antigüedad del FVG por velas (Máx 10 velas) ---
            fvgIdx = latestFvgRef.get('idx', len(df) - 1)
            fvgAgeCandles = len(df) - 1 - fvgIdx
            
            if fvgAgeCandles > 30:
                logger.info(f"[{symbol}] {interval}: FVG demasiado antiguo ({fvgAgeCandles} velas > 30) - saltando")
                continue

            # --- Cálculo de Niveles Centralizado (Maura SMC) ---
            atr = ta.ATR(df['high'], df['low'], df['close'], 14).iloc[-1]
            currentPrice = float(df['close'].iloc[-1])
            setupFvg = technical.calculate_fvg_setup(latestFvgRef, currentPrice, atr)
            
            entryPrice = setupFvg['entry']
            sl = setupFvg['sl']
            signalDirection = setupFvg['direction']
            slDist = setupFvg['sl_dist']
            # Lógica SMC/ICT Estricta: El Take Profit de alta probabilidad es el primer alto/bajo anterior (Swing High/Low local)
            # con respecto a la 3ª vela del FVG (inclusive), previniendo distorsiones por la acción del precio posterior.
            fvgIdxPrior = latestFvgRef.get('idx', len(df) - 1)
            dfPrior = df.iloc[:fvgIdxPrior + 1]
            levels = technical.get_structural_levels(dfPrior, lookback=20)
            if signalDirection == "LARGO":
                tpRef = levels['swing_high_body']  # Primer máximo del cuerpo de vela local anterior
            else:
                tpRef = levels['swing_low_body']   # Primer mínimo del cuerpo de vela local anterior

            # Cap de TP por ATR: máximo 3.0 ATR desde el precio actual (estrategia multi-TF)
            from Sentinel.analysis.technical import capTpByAtr
            tpRef = capTpByAtr(tpRef, currentPrice, float(atr), signalDirection, maxAtrMult=3.0)
            
            tp1 = adjustTPForMinRR(entryPrice, sl, tpRef, signalDirection, minRR=minRrVal)
            
            riskDist = abs(entryPrice - sl)
            rewardDist = abs(tp1 - entryPrice)
            rrRatio = rewardDist / riskDist if riskDist > 0 else 0
            
            # --- FILTRO SEGURIDAD: RR Máximo (Evitar errores de data) ---
            if rrRatio > 15:
                # logger.info(f"[{symbol}] {interval}: RR={rrRatio:.2f} irreal - descartando")
                continue

            # --- Health Check ---
            currentPrice = float(df['close'].iloc[-1])
            fvgTime = latestFvgRef.get('candle_time', '')
            fvgTimeStr = fvgTime.strftime("%Y-%m-%d %H:%M:%S") if fvgTime else ""
            
            isHealthy, progressPct, reason = technical.check_signal_health(
                entryPrice, tp1, sl, 
                "LARGO" if latestFvgRef['type'] == 'Bullish_FVG' else "CORTO",
                currentPrice,
                threshold=3.5, # Permitir hasta 350% (re-tests lejanos)
                candle_time=fvgTimeStr
            )
            
            if not isHealthy:
                # Si el precio ya tocó SL no insistir (ya logueado en technical)
                if currentPrice >= sl if latestFvgRef['type'] == 'Bearish_FVG' else currentPrice <= sl:
                    continue
                
                # Si ya avanzó demasiado (ej. 350% del tamaño del gap), descartar
                if progressPct > 3.5:
                    continue
            
            # 3. Verificar que precio actual no haya invalidado el SL
            if latestFvgRef['type'] == 'Bullish_FVG':
                if currentPrice <= sl:
                    logger.info(f"[{symbol}] {interval}: Precio tocó SL (price={currentPrice:.5f}, sl={sl:.5f}) - descartando")
                    continue
            else:
                if currentPrice >= sl:
                    logger.info(f"[{symbol}] {interval}: Precio tocó SL (price={currentPrice:.5f}, sl={sl:.5f}) - descartando")
                    continue
            
            # --- FILTRO: Ganancia Mínima Est. ---
            multiplier = getPipMultiplier(symbol)
            # --- Cálculo de Tamaño de Posición Real (Centralizado) ---
            from Sentinel.analysis import risk
            refCapital = symbolInfo.get('refCapital', 10000.0)
            refRiskPct = symbolInfo.get('refRiskPct', 1.0)
            
            # Usar precio actual como entrada real para el cálculo de riesgo
            realEntry = currentPrice
            realRiskDist = abs(realEntry - sl)
            
            size, riskUsdActual, marginUsed = risk.calculatePositionSize(
                refCapital, refRiskPct, realRiskDist, symbolInfo, entryPrice=realEntry
            )
            
            if size is None or size <= 0:
                logger.info(f"[{symbol}] {interval}: Tamaño de posición inválido o margen insuficiente - descartando")
                continue
                
            minUsdProfit = float(stratConfig.get('minUsdProfit') or stratConfig.get('min_usd_profit') or 10.0)
            rrVal = round(abs(tp1 - realEntry) / realRiskDist, 2) if realRiskDist > 0 else 0
            
            # --- FILTRO SEGURIDAD: Evitar entradas tardías con RR real pésimo ---
            # Si el precio actual ya avanzó tanto que el RR real (basado en precio de mercado)
            # es menor al 70% del RR mínimo exigido, se descarta.
            minRealRr = minRrVal * 0.70
            if rrVal < minRealRr:
                logger.info(f"[{symbol}] {interval}: Descartando señal por RR real insuficiente ({rrVal:.2f} < {minRealRr:.2f}) debido a entrada tardía")
                continue
                
            # Mejor usar el riskUsdActual * rrVal
            expectedProfit = riskUsdActual * rrVal
            
            if expectedProfit < minUsdProfit:
                logger.info(f"[{symbol}] {interval}: Beneficio Est. ${expectedProfit:.2f} < ${minUsdProfit:.2f} - descartando")
                continue
 
            # 4. Filtrar por confianza mínima
            baseConfidence = 85
            if baseConfidence < minConfidence:
                logger.info(f"[{symbol}] {interval}: confidence={baseConfidence} < minConfidence={minConfidence} - descartando")
                continue
            
            # --- FILTRO 5: Inteligencia Predictiva (Machine Learning) ---
            probaML = 0.0
            minMlProb = float(stratConfig.get('minMlProb') or stratConfig.get('min_ml_prob') or 0.55)
            if self.ml_model is not None and all(f in df.columns for f in MODEL_FEATURES):
                X_feats = df[MODEL_FEATURES].dropna()
                if not X_feats.empty:
                    probaML = predictProba(self.ml_model, X_feats) or 0.0
                    if probaML < minMlProb:
                        logger.info(f"[{symbol}] {interval}: FVG descartado por predicción ML baja ({probaML*100:.1f}% < {minMlProb*100:.1f}%)")
                        continue
            
            # 6. Filtrar por tendencia HTF (Ya validada al inicio del ciclo de forma unificada)
            signalDirection = "LARGO" if latestFvgRef['type'] == 'Bullish_FVG' else "CORTO"
            
            # Marcar como enviada en RAM
            self._sent_signals[signalKey] = True
            
            # Crear objeto Signal
            # Usar la hora de la vela origen para trazabilidad
            minutes = 5
            if 'min' in interval: minutes = int(interval.replace('min', ''))
            elif 'h' in interval: minutes = int(interval.replace('h', '')) * 60
            elif '4h' in interval: minutes = 240
            elif '1d' in interval: minutes = 1440
            
            lastV = get_last_closed_candle(self._now_mx(), minutes, df=df)
            candleTimeObj = lastV.name if hasattr(lastV, 'name') else lastV
            
            originCandleTime = latestFvgRef.get('candle_time', candleTimeObj)
            
            # Calcular Break Even inteligente
            beTrigger = calculateBEPrice(entryPrice, sl, tp1, signalDirection)
 
            sig = Signal(
                strategy=self.strategy_name,
                symbol=symbol,
                direction=signalDirection,
                entry_price=realEntry,
                stop_loss=sl,
                take_profit=tp1,
                sl_distance=realRiskDist,
                confidence=baseConfidence,
                setup=f"FVG {interval}",
                status="EN ZONA ✅",
                candleTime=candleTimeObj.strftime("%Y-%m-%d %H:%M:%S"),
                intervalo=interval,
                riesgo_pips=round(realRiskDist * multiplier, 1),
                rr_ratio=rrVal,
                break_even=calculateBEPrice(realEntry, sl, tp1, signalDirection),
                size=size,
                metadata={
                    "risk_usd": round(riskUsdActual, 2),
                    "expected_profit": round(expectedProfit, 2),
                    "margin_used": round(marginUsed, 2),
                    "fvg": latestFvgRef.get('type', 'N/A'),
                    "fvgTime": latestFvgRef.get('timestamp', 'N/A'),
                    "ml_probability": round(probaML, 3)
                }
            )
            signals.append(sig)
            
        return signals

