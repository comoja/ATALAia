"""
Module for risk management, including position sizing and trade monitoring.
"""
import logging
import pandas as pd
from typing import Dict, Any

logger = logging.getLogger(__name__)

def calculatePositionSize(capital: float, riskPercentage: float, slDistance: float, symbolInfo: Dict[str, Any]) -> tuple[float, float] | tuple[None, None]:
    """
    Calculates the appropriate position size based on risk parameters.

    Returns:
        A tuple of (positionSize, riskInCurrency) or (None, None) on error.
        
    Note:
        - METALS: returns units (min 1)
        - FOREX: returns thousands of units (min 1000)
        - INDICES/CRYPTO: returns units (min 1)
    """
    try:
        if slDistance <= 0:
            logger.warning("La distancia del stop loss es cero o negativa. No se puede calcular el tamaño de posición.")
            return None, None

        riskInCurrency = capital * (riskPercentage / 100)
        
        symbolType = symbolInfo.get('tipo', 'FOREX').upper()
        symbolName = symbolInfo.get('symbol', '').upper()
        
        # --- METALS (e.g., XAU/USD) ---
        if symbolType == "METALES":
            # XAU/USD: 1 lot = 100 ounces, 1 pip (0.10) = $10 per ounce = $1000 per pip per lot
            # To get units (ounces): risk = units * pips * $10
            pips = slDistance / 0.10
            if pips <= 0:
                return None, None
            units = riskInCurrency / (pips * 10)
            return max(1.0, round(units, 2)), riskInCurrency

        # --- INDICES (e.g., US30, SP500) ---
        elif symbolType == "INDICES":
            # 1 Contract = $1 per point typically
            contracts = riskInCurrency / slDistance
            return max(1.0, round(contracts, 1)), riskInCurrency

        # --- CRYPTO (e.g., BTC/USD) ---
        elif symbolType == "CRIPTO":
            # 1 unit of the crypto
            units = riskInCurrency / slDistance
            return max(0.01, round(units, 4)), riskInCurrency

        # --- FOREX (e.g., EUR/USD) ---
        else:
            pipValue = 0.01 if "JPY" in symbolName else 0.0001
            pipsDistance = slDistance / pipValue
            
            if pipsDistance == 0:
                return None, None

            # Value per pip for 1 standard lot (100,000 units) = $10
            valuePerPip = 10
            
            # Standard lots needed
            lots = riskInCurrency / (pipsDistance * valuePerPip)
            
            # Convert to thousands of units (broker format)
            thousandsOfUnits = lots * 100
            
            # Minimum 1000 (micro lot)
            return max(1000, int(round(thousandsOfUnits))), riskInCurrency

    except Exception as e:
        logger.error(f"Error en calculatePositionSize: {e}", exc_info=True)
        return None, None


def checkTradeClosure(dfNewCandles: pd.DataFrame, tradeData: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Analyzes new candles to see if an open trade hit its SL or TP.
    (Original logic from `verificarNivelesTrade`)

    Returns:
        A dictionary with closure details if closed, otherwise None.
    """
    try:
        side = tradeData['direction'].upper()
        stopLoss = tradeData.get('stopLoss')
        takeProfit = tradeData.get('takeProfit')

        for timestamp, row in dfNewCandles.iterrows():
            if side == "BUY":
                if stopLoss and row['low'] <= stopLoss:
                    return {"status": "CLOSED", "reason": "SL", "exitPrice": stopLoss, "closeTime": timestamp}
                if takeProfit and row['high'] >= takeProfit:
                    return {"status": "CLOSED", "reason": "TP", "exitPrice": takeProfit, "closeTime": timestamp}
            
            elif side == "SELL":
                if stopLoss and row['high'] >= stopLoss:
                    return {"status": "CLOSED", "reason": "SL", "exitPrice": stopLoss, "closeTime": timestamp}
                if takeProfit and row['low'] <= takeProfit:
                    return {"status": "CLOSED", "reason": "TP", "exitPrice": takeProfit, "closeTime": timestamp}
        
        return None  # Trade remains open

    except Exception as e:
        logger.error(f"Error al verificar cierre de trade: {e}", exc_info=True)
        return None

def calculatePnl(tradeData: Dict[str, Any], closureData: Dict[str, Any]) -> float:
    """
    Calculates the net Profit and Loss for a closed trade.
    """
    try:
        side = tradeData['direction'].upper()
        entryPrice = tradeData['entryPrice']
        size = tradeData['size']
        exitPrice = closureData['exitPrice']
        
        # Simplified commission logic for now
        commission = tradeData.get('commission', 0) 

        if side == "BUY":
            grossPnl = (exitPrice - entryPrice) * size
        else: # SELL
            grossPnl = (entryPrice - exitPrice) * size
        
        netPnl = grossPnl - commission
        return netPnl

    except Exception as e:
        logger.error(f"Error al calcular PNL: {e}", exc_info=True)
        return 0.0
