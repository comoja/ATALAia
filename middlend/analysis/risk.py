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
    (Original logic from `calcularPosicion`)

    Returns:
        A tuple of (positionSize, riskInCurrency) or (None, None) on error.
    """
    try:
        if slDistance <= 0:
            logger.warning("Stop loss distance is zero or negative. Cannot calculate position size.")
            return None, None

        # Maximum amount of capital to risk on this single trade
        riskInCurrency = capital * (riskPercentage / 100)
        
        symbolType = symbolInfo.get('tipo', 'FOREX').upper()
        symbolName = symbolInfo.get('symbol', '').upper()
        
        # --- METALS (e.g., XAU/USD) ---
        if symbolType == "METALES":
            # 1 Lot = 100 Ounces. 1 pip (0.10) movement = $10 USD.
            # Assuming slDistance is in price points, and 1 point = 10 pips.
            lots = riskInCurrency / (slDistance * 100)
            return max(0.01, round(lots, 2)), riskInCurrency

        # --- INDICES (e.g., US30) ---
        elif symbolType == "INDICES":
            # 1 Contract = $1 per point.
            contracts = riskInCurrency / slDistance
            # The original code had `contrato *= 10`, which seems arbitrary.
            # Sticking to a clearer 1 contract = 1 lot definition for now.
            return max(1.0, round(contracts, 1)), riskInCurrency

        # --- CRYPTO (e.g., BTC/USD) ---
        elif symbolType == "CRIPTO":
            # 1 Lot = 1 unit of the crypto (e.g., 1 BTC).
            units = riskInCurrency / slDistance
            return max(0.01, round(units, 2)), riskInCurrency

        # --- FOREX (e.g., EUR/USD) ---
        else: # Default to Forex logic
            # For a standard lot (100,000 units), value of a pip is ~$10.
            pipValue = 0.01 if "JPY" in symbolName else 0.0001
            pipsDistance = slDistance / pipValue
            
            if pipsDistance == 0: return None, None

            # Value per pip for a standard lot
            valuePerPip = 10 
            
            # lots = (Risk amount) / (pips to SL * value per pip)
            lots = riskInCurrency / (pipsDistance * valuePerPip)
            
            # The original logic had complex multipliers `* (10 if ...)` and `* 1000`.
            # This is simplified to standard lot calculation. The final unit might need
            # adjustment based on the broker's contract size specification.
            # For now, returning standard lots.
            return max(0.01, round(lots, 2)), riskInCurrency

    except Exception as e:
        logger.error(f"❌ Error in calculatePositionSize: {e}", exc_info=True)
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
        logger.error(f"Error checking trade closure: {e}", exc_info=True)
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
        logger.error(f"Error calculating PNL: {e}", exc_info=True)
        return 0.0
