"""
Core Trading Bot Class
"""
import logging
from datetime import datetime
from typing import Dict, Any
import pandas as pd

# --- Module Imports ---
# Assuming the new project structure allows these imports.
# This might need path adjustments (e.g., setting up as a package).
import sys, os
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middlend.api import twelvedata
from middlend.analysis import technical, risk
from middlend.ml import model as mlModel
from middlend.core.communications import sendTelegramAlert
from middlend import configConstants as config

# --- External Project Imports ---
# These are dependencies on the original `backend` structure.
# This is not ideal, but necessary for compatibility without refactoring the whole project.
from backend.database import dbManager
from backend.scheduler.autoScheduler import getTiempoEspera, isRestTime
from backend.data.dataLoader import getParametros

logger = logging.getLogger(__name__)

class TradingBot:
    def __init__(self, mlModelInstance):
        self.model = mlModelInstance
        self.accounts = []

    async def _get_and_prepare_data(self, symbolInfo: Dict, apiKey: str, nVelas: int, interval: str) -> pd.DataFrame | None:
        """Fetches, prepares, and enriches data with technical features."""
        symbol = symbolInfo['symbol']
        
        # 1. Download data
        df = await twelvedata.getTimeSeries(symbol, interval, apiKey, nVelas)
        if df is None or len(df) < 100:
            logger.warning(f"[{symbol}] Datos insuficientes para análisis ({len(df) if df is not None else 0} velas).")
            return None
        
        # 2. Calculate features
        dfFeatured = technical.calculateFeatures(df)
        
        # 3. Define ML target (needed for data cleaning consistency)
        dfFinal = mlModel.defineMlTarget(dfFeatured)
        
        return dfFinal

    def _get_signal(self, df: pd.DataFrame, symbol: str) -> Dict[str, Any] | None:
        """Analyzes the data to generate a trading signal dictionary."""
        
        X, _ = mlModel.cleanDataForModel(df)
        if len(X) < 100:
            logger.warning(f"[{symbol}] Datos insuficientes tras limpieza ({len(X)} filas).")
            return None

        # --- Get Current Values ---
        latest = X.iloc[-1]
        latestFullData = df.iloc[-1]
        
        close = latestFullData["close"]
        volPercent = (latest["atr"] / close) * 100
        
        # --- FILTERS (VETO) ---
        if volPercent < config.MIN_VOLATILITY_PERCENT:
            logger.info(f"[{symbol}] Volatilidad ({volPercent:.3f}%) demasiado baja. Señal descartada.")
            return None
        
        # --- PREDICTION ---
        proba = mlModel.predictProba(self.model, X)
        if proba is None:
            return None

        # --- STRATEGY LOGIC (SNIPER) ---
        direction = None
        confianza = 0
        
        histVal = latestFullData["macdHist"]
        prevHistVal = df["macdHist"].iloc[-2]
        
        # Technical confirmation
        techConfLong = (latestFullData["pendienteCci"] > 2 and latestFullData["pendienteRsi"] > -0.2)
        techConfShort = (latestFullData["pendienteCci"] < -2 and latestFullData["pendienteRsi"] < 0.2)
        
        isLongCandidate = (
            proba >= config.PROBA_THRESHOLD_LONG and
            histVal > prevHistVal and
            techConfLong and
            latest["rsi"] < config.RSI_OVERBOUGHT_THRESHOLD
        )
        isShortCandidate = (
            proba <= config.PROBA_THRESHOLD_SHORT and
            histVal < prevHistVal and
            techConfShort and
            latest["rsi"] > config.RSI_SOLD_THRESHOLD
        )
        
        if isLongCandidate:
            direction = "LARGO"
            isCross = (prevHistVal <= 0 and histVal > 0)
            bonus = 12 if isCross else 5
            confianza = (proba * 100) + bonus
        elif isShortCandidate:
            direction = "CORTO"
            isCross = (prevHistVal >= 0 and histVal < 0)
            bonus = 12 if isCross else 5
            confianza = ((1 - proba) * 100) + bonus
        else:
            return None # No signal
            
        # --- Apply Bonuses/Penalties ---
        # Candle patterns
        cdlEngulfing = latestFullData["cdlEngulfing"]
        cdlHammer = latestFullData["cdlHammer"]
        cdlShootingStar = latestFullData["cdlShootingStar"]

        if (direction == "LARGO" and (cdlEngulfing > 0 or cdlHammer > 0)) or (direction == "CORTO" and (cdlEngulfing < 0 or cdlShootingStar < 0)):
            confianza *= 1.10
        else:
            confianza *= 0.50 # Penalty if no confirming candle

        # --- FINAL FILTERS ---
        if confianza < config.CONTRARIAN_CONFIDENCE_THRESHOLD:
            isAgainstTrend = (direction == "LARGO" and close < latestFullData["ema50"]) or (direction == "CORTO" and close > latestFullData["ema50"])
            if isAgainstTrend:
                logger.info(f"[{symbol}] Filtrado: Intento de contratendencia con confianza baja ({confianza:.1f}%).")
                return None

        return {
            "direction": direction,
            "confidence": confianza,
            "entryPrice": close,
            "slDistance": latest["atr"] * (config.ATR_MULTIPLIER_HIGH_CONFIDENCE if proba >= 0.65 or proba <= 0.35 else config.ATR_MULTIPLIER_DEFAULT),
            "latestMetrics": latestFullData.toDict(),
            "symbolInfo": symbol
        }
    
    async def _execute_trades(self, signal: Dict, symbolInfo):
        """Processes a valid signal, calculates risk, and sends alerts for all accounts."""
        if not signal:
            return

        for account in self.accounts:
            # --- Risk and Position Sizing ---
            posSize, riskUsd = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=signal['slDistance'],
                symbolInfo=symbolInfo
            )
            if posSize is None:
                logger.warning(f"[{account['idCuenta']}] No se pudo calcular el tamaño de posición para {symbolInfo['symbol']}.")
                continue
            
            # --- Define SL/TP ---
            direction = signal['direction']
            entryPrice = signal['entryPrice']
            slDist = signal['slDistance']

            slPrice = entryPrice - slDist if direction == "LARGO" else entryPrice + slDist
            
            # Dynamic RR
            ratioBase = config.HIGH_CONFIDENCE_RISK_REWARD_RATIO if signal['confidence'] > 85 else config.BASE_RISK_REWARD_RATIO
            
            tpPrice = entryPrice + (slDist * ratioBase) if direction == "LARGO" else entryPrice - (slDist * ratioBase)
            
            # --- Create Trade Object ---
            trade = {
                "idCuenta": account['idCuenta'],
                "symbol": symbolInfo['symbol'],
                "direction": direction,
                "entryPrice": entryPrice,
                "openTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "stopLoss": slPrice,
                "takeProfit": tpPrice,
                "size": posSize,
                "intervalo": symbolInfo['intervalo'], # Assumes this info is passed
                "status": "OPEN",
                # ... other fields for DB
            }
            
            # --- Persist and Alert ---
            if account['idCuenta'] != 1: # Original logic to exclude account 1
                dbManager.buscaTrade(trade) # Assumes this function saves the trade
                
                # Format and send alert
                message = self._format_alert_message(signal, trade)
                await sendTelegramAlert(account['TokenMsg'], account['idGrupoMsg'], message)
                logger.info(f"✅ Alerta SNIPER enviada para {symbolInfo['symbol']} a la cuenta {account['idCuenta']}")
    
    def _format_alert_message(self, signal: Dict, trade: Dict) -> str:
        """Formats the beautiful Telegram message from the original script."""
        # This is a simplified version of the original message string building
        directionStr = "COMPRA" if signal['direction'] == "LARGO" else "VENTA"
        colorHeader = "🟩" if signal['direction'] == "LARGO" else "🟥"
        
        text = (
            f"{colorHeader*3} <b>SEÑAL DE {directionStr}</b> {colorHeader*3}"
            f"<center><b>{trade['symbol']}</b> ({trade['intervalo']})</center>"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>"
            f"━━━━━━━━━━━━━━━"
            f"<center>Confianza: <b>{signal['confidence']:.1f}%</b></center>"
            f"━━━━━━━━━━━━━━━"
            f"🟢 TAKE PROFIT: {trade['takeProfit']:,.5f}"
            f"🔹 ENTRADA:   <b>{trade['entryPrice']:,.5f}</b>"
            f"🔴 STOP LOSS: {trade['stopLoss']:,.5f}"
            f"━━━━━━━━━━━━━━━"
            f"<center>Cantidad: <b>{trade['size']:.2f}</b></center>"
        )
        return text

    async def runAnalysisCycle(self):
        """The main operational loop of the bot."""
        self.accounts = dbManager.getAccount()
        if not self.accounts:
            logger.error("No se encontraron cuentas en la base de datos. El bot no puede operar.")
            return

        logger.info("Iniciando ciclo de análisis...")
        
        symbolsToScan = dbManager.getSymbols()
        
        for symbolInfo in symbolsToScan:
            # These parameters are now fetched per symbol, as in the original logic
            apiKey, interval, _, nVelas, waitMin = getParametros()
            symbolInfo['intervalo'] = interval # Augment symbolInfo

            data = await self._get_and_prepare_data(symbolInfo, apiKey, nVelas, interval)
            if data is None:
                continue
                
            signal = self._get_signal(data, symbolInfo['symbol'])
            if signal:
                await self._execute_trades(signal, symbolInfo)
        
        logger.info("✅ Ciclo de análisis completado.")
