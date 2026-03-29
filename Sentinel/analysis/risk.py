"""
Module for risk management, including position sizing and trade monitoring.
"""
import logging
import pandas as pd
from typing import Dict, Any
from middleware.database import dbManager

logger = logging.getLogger(__name__)

def calculatePositionSize(capital: float, riskPercentage: float, slDistance: float, symbolInfo: Dict[str, Any], entryPrice: float = None) -> tuple[float, float, float] | tuple[None, None, float]:
    """
    Calculates the appropriate position size based on risk parameters.

    Returns:
        A tuple of (positionSize, riskInCurrency, margin_used) or (None, None, 0) on error.
        
    Note:
        - METALS: returns units (min 1)
        - FOREX: returns thousands of units (min 1000)
        - INDICES/CRYPTO: returns units (min 1)
    """
    try:
        if slDistance <= 0:
            logger.warning("La distancia del stop loss es cero o negativa. No se puede calcular el tamaño de posición.")
            return None, None, 0

        riskInCurrency = capital * (riskPercentage / 100)
        
        if riskInCurrency > capital:
            logger.warning(f"[{symbolName}] Riesgo {riskInCurrency:.2f} > capital {capital:.2f} - ajustar ganancia en BD")
            return None, None, 0
        
        symbolType = symbolInfo.get('tipo', 'FOREX').upper()
        symbolName = symbolInfo.get('symbol', '').upper()
        
        symbolMargin = symbolInfo.get('margen')
        symbolMinLots = symbolInfo.get('min_lots')
        
        margin_multiplier = float(symbolMargin) if symbolMargin else 0.0333
        
        min_units = {
            "METALES": 1,
            "INDICES": 1,
            "CRIPTO": 0.01,
            "MONEDA": 1000
        }
        
        if symbolMinLots is not None:
            min_lots_val = int(symbolMinLots)
            min_units = {
                "METALES": min_lots_val,
                "INDICES": min_lots_val,
                "CRIPTO": min_lots_val,
                "MONEDA": min_lots_val
            }
        
        min_margin_required = {
            "METALES": min_units["METALES"] * margin_multiplier,
            "INDICES": min_units["INDICES"] * margin_multiplier,
            "CRIPTO": min_units["CRIPTO"] * margin_multiplier,
            "MONEDA": min_units["MONEDA"] * margin_multiplier / 100
        }
        
        min_margin = min_margin_required.get(symbolType, margin_multiplier)
        
        if min_margin > capital:
            logger.warning(f"[{symbolName}] Capital insuficiente para margen mínimo: {min_margin:.2f} > {capital}")
            return None, None, 0
        
        def adjustForMargin(size, margin_mult, capital_available, risk_curr):
            required = size * margin_mult
            if required <= capital_available:
                return size, risk_curr
            
            max_size = capital_available / margin_mult
            min_size = min_units.get(symbolType, 1.0)
            
            if max_size < min_size:
                return None, 0
            
            adjusted_risk = (max_size / size) * risk_curr if size > 0 else risk_curr
            
            if adjusted_risk > capital_available:
                logger.warning(f"[{symbolName}] Riesgo real {adjusted_risk:.2f} > capital {capital_available:.2f} - ajustar ganancia en BD")
                return None, 0
            
            logger.info(f"[{symbolName}] Auto-ajustado: size {size:.2f}→{max_size:.2f}, riesgo {risk_curr:.2f}→{adjusted_risk:.2f} (margen: {required:.2f} > {capital_available:.2f})")
            return max_size, adjusted_risk
        
        # --- METALS (e.g., XAU/USD) ---
        if symbolType == "METALES":
            pips = slDistance / 0.10
            if pips <= 0:
                return None, None, 0
            units = riskInCurrency / (pips * 10)
            units = max(min_units["METALES"], round(units, 2))
            
            units, riskInCurrency = adjustForMargin(units, margin_multiplier, capital, riskInCurrency)
            if units is None:
                return None, None, 0
            
            margin_used = margin_multiplier * min_units["METALES"] * entryPrice * units
            return units, riskInCurrency, margin_used

        # --- INDICES (e.g., US30, SP500) ---
        elif symbolType == "INDICES":
            contracts = riskInCurrency / slDistance
            contracts = max(1.0, round(contracts, 1))
            
            contracts, riskInCurrency = adjustForMargin(contracts, margin_multiplier, capital, riskInCurrency)
            if contracts is None:
                return None, None, 0
            
            margin_used = margin_multiplier * min_units["INDICES"] * entryPrice * contracts
            return contracts, riskInCurrency, margin_used

        # --- CRYPTO (e.g., BTC/USD) ---
        elif symbolType == "CRIPTO":
            units = riskInCurrency / slDistance
            units = max(0.01, round(units, 4))
            
            units, riskInCurrency = adjustForMargin(units, margin_multiplier, capital, riskInCurrency)
            if units is None:
                return None, None, 0
            
            margin_used = margin_multiplier * min_units["CRIPTO"] * entryPrice * units
            return units, riskInCurrency, margin_used

        # --- FOREX (e.g., EUR/USD) ---
        else:
            pipValue = 0.01 if "JPY" in symbolName else 0.0001
            pipsDistance = slDistance / pipValue
            
            if pipsDistance == 0:
                return None, None, 0

            valuePerPip = 10
            lots = riskInCurrency / (pipsDistance * valuePerPip)
            thousandsOfUnits = lots * 100
            thousandsOfUnits = float(max(1000, int(round(thousandsOfUnits))))
            
            def adjustForex(size, margin_mult, capital_avail, risk_curr):
                required = (size / 100) * margin_mult
                if required <= capital_avail:
                    return size, risk_curr
                max_size = (capital_avail / margin_mult) * 100
                min_forex = min_units.get("MONEDA", 1000)
                if max_size < min_forex:
                    return None, 0
                adjusted_risk = (max_size / size) * risk_curr if size > 0 else risk_curr
                
                if adjusted_risk > capital_avail:
                    logger.warning(f"[{symbolName}] Riesgo real {adjusted_risk:.2f} > capital {capital_avail:.2f} - ajustar ganancia en BD")
                    return None, 0
                
                logger.info(f"[{symbolName}] Auto-ajustado FOREX: size {size:.0f}→{max_size:.0f}, riesgo {risk_curr:.2f}→{adjusted_risk:.2f}")
                return max_size, adjusted_risk
            
            thousandsOfUnits, riskInCurrency = adjustForex(thousandsOfUnits, margin_multiplier, capital, riskInCurrency)
            if thousandsOfUnits is None:
                return None, None, 0
            
            margin_used = margin_multiplier * min_units["MONEDA"] * entryPrice * (thousandsOfUnits / 1000)
            
            return int(thousandsOfUnits), riskInCurrency, margin_used

    except Exception as e:
        logger.error(f"Error en calculatePositionSize: {e}", exc_info=True)
        return None, None, 0


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
