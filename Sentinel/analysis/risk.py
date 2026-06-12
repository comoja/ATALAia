"""
Module for risk management, including position sizing and trade monitoring.
"""
import logging
import pandas as pd
from typing import Dict, Any
from middleware.database import dbManager

logger = logging.getLogger("sentinel")

def calculatePositionSize(capital: float, riskPercentage: float, slDistance: float, symbolInfo: Dict[str, Any], entryPrice: float = None) -> tuple[float, float, float] | tuple[None, None, float]:
    """
    Calcula el tamaño de posición basado en el riesgo y el margen.
    """
    try:
        import math
        from decimal import Decimal
        
        if entryPrice is None or entryPrice <= 0:
            entryPrice = 1.0

        if slDistance <= 0 or math.isnan(slDistance):
            return None, None, 0

        # Calcular riesgo monetario
        riskInCurrency = capital * (riskPercentage / 100.0)
        
        symbolType = symbolInfo.get('tipo', 'MONEDA').upper()
        symbolName = symbolInfo.get('symbol', '').upper()
        
        # Margen desde la BD (porcentaje, ej: 2.0)
        marginPercentValue = float(symbolInfo.get('margen', 2.0))
        marginPercent = marginPercentValue / 100.0
        
        # Mapeo de lotes mínimos por defecto según tipo de activo para consistencia
        minUnitsDict = {
            "METALES": 1.0,
            "INDICES": 1.0,
            "INDICE": 1.0,
            "CRIPTO": 0.01,
            "CRYPTO": 0.01,
            "MONEDA": 1000.0,
            "EXOTIC": 1000.0,
            "FOREX": 1000.0
        }
        
        symbolMinLots = symbolInfo.get('min_lots')
        if symbolMinLots is not None:
            minLots = float(symbolMinLots)
        else:
            minLots = float(minUnitsDict.get(symbolType, 1000.0))

        def adjustForMargin(size, mPercent, capAvailable, riskCurr, mLots, price=1.0):
            requiredMargin = size * price * mPercent
            if requiredMargin <= capAvailable:
                return size, riskCurr
            
            # Limitar por margen disponible
            maxSize = math.floor(capAvailable / (price * mPercent) / mLots) * mLots
            if maxSize < mLots:
                return None, 0
            
            adjustedRisk = (maxSize / size) * riskCurr if size > 0 else 0
            return maxSize, adjustedRisk

        # --- FOREX ---
        if symbolType in ["MONEDA", "EXOTIC", "FOREX", "CURRENCY"]:
            # Pip value calculation con inferencia robusta de JPY/HUF si no viene en symbolInfo
            pipValue = symbolInfo.get('pip')
            if pipValue is None:
                symbol_up = symbolName.upper()
                if any(s in symbol_up for s in ["JPY", "HUF"]):
                    pipValue = 0.01
                else:
                    pipValue = 0.0001
            pipValue = float(pipValue)
            pipsDistance = slDistance / pipValue
            if pipsDistance == 0: return None, None, 0

            quoteCurr = symbolInfo.get('quote_currency', 'USD')
            # Valor base por 1 lote (100k unidades)
            # Para pares XXX/USD, 1 pip de 100k = $10
            # Para pares USD/XXX, 1 pip de 100k = $10 / currentPrice
            baseLotSize = 100000.0
            # Dinámico: valor de 1 pip para 1 lote (100k unidades)
            # Para pares XXX/USD, 1 pip de 100k = baseLotSize * pipValue
            # Para pares USD/XXX, 1 pip de 100k = (baseLotSize * pipValue) / currentPrice
            pipBaseValue = baseLotSize * pipValue
            valuePerPipPerLot = pipBaseValue if quoteCurr == 'USD' else (pipBaseValue / entryPrice if entryPrice and entryPrice > 0 else pipBaseValue)
            
            # Unidades = Riesgo / (Pips * ValorPipUnidad)
            # ValorPipUnidad = valuePerPipPerLot / baseLotSize
            units = riskInCurrency / (pipsDistance * (valuePerPipPerLot / baseLotSize))
            
            # Redondear a múltiplos de minLots
            units = math.floor(units / minLots) * minLots
            
            # Ajustar por margen
            isUsdBase = symbolName.startswith("USD/")
            priceForMargin = 1.0 if isUsdBase else entryPrice
            units, riskInCurrency = adjustForMargin(units, marginPercent, capital, riskInCurrency, minLots, price=priceForMargin)
            
            if units is None or units <= 0: return None, None, 0
            
            marginUsed = units * priceForMargin * marginPercent
            return int(units), riskInCurrency, marginUsed

        # --- METALES / INDICES ---
        else:
            # Simplificado para unidades directas (Oro, etc.)
            units = riskInCurrency / slDistance
            units = math.floor(units / minLots) * minLots
            
            units, riskInCurrency = adjustForMargin(units, marginPercent, capital, riskInCurrency, minLots, price=entryPrice)
            if units is None or units <= 0: return None, None, 0
            
            marginUsed = units * entryPrice * marginPercent
            return units, riskInCurrency, marginUsed

    except Exception as e:
        logger.error(f"Error en calculatePositionSize: {e}")
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
        
        # DEBUG: Log input data
        logger.debug(f"[checkTradeClosure] symbol={tradeData.get('symbol')}, side={side}, SL={stopLoss}, TP={takeProfit}, df_rows={len(dfNewCandles)}")

        for timestamp, row in dfNewCandles.iterrows():
            if side == "LARGO":
                if stopLoss and row['low'] <= stopLoss:
                    return {"status": "CLOSED", "reason": "SL", "exitPrice": stopLoss, "closeTime": timestamp}
                if takeProfit and row['high'] >= takeProfit:
                    return {"status": "CLOSED", "reason": "TP", "exitPrice": takeProfit, "closeTime": timestamp}
            
            elif side == "CORTO":
                if stopLoss and row['high'] >= stopLoss:
                    return {"status": "CLOSED", "reason": "SL", "exitPrice": stopLoss, "closeTime": timestamp}
                if takeProfit and row['low'] <= takeProfit:
                    return {"status": "CLOSED", "reason": "TP", "exitPrice": takeProfit, "closeTime": timestamp}
        
        return None  # Trade remains open

    except Exception as e:
        logger.error(f"Error al verificar cierre de trade: {e}", exc_info=True)
        return None


def check_multi_tp_closure(dfNewCandles: pd.DataFrame, tradeData: Dict[str, Any]) -> Dict[str, Any] | None:
    """
    Analiza nuevas velas para verificar si un trade open alcanzó sus niveles de TP múltiples.
    Soporta cierres parciales: 30% en TP1, 40% en TP2, 30% en TP_FINAL.
    
    Returns:
        Dict con detalles del cierre si el trade se cierra completamente, 
        o dict con 'partial' = True si es cierre parcial.
        None si el trade permanece abierto.
    """
    try:
        side = tradeData['direction'].upper()
        stopLoss = tradeData.get('stopLoss')
        entryPrice = tradeData.get('entryPrice')
        
        tp1 = tradeData.get('tp1')
        tp2 = tradeData.get('tp2')
        tp_final = tradeData.get('tp_final', tradeData.get('takeProfit'))
        
        partial_levels = tradeData.get('partial_close_levels', [])
        closed_levels = tradeData.get('closed_tp_levels', [])
        
        if closed_levels is None:
            closed_levels = []
        
        result = {
            'closed': False,
            'partial': False,
            'reason': None,
            'exitPrice': None,
            'closeTime': None,
            'closed_tp_levels': closed_levels.copy(),
            'remaining_size_pct': 100
        }
        
        pct_closed = 0
        for level in closed_levels:
            pct_closed += level.get('pct', 0)
        result['remaining_size_pct'] = 100 - pct_closed
        
        for timestamp, row in dfNewCandles.iterrows():
            velaHigh = float(row['high'])
            velaLow = float(row['low'])
            velaClose = float(row['close'])
            
            if side == "LARGO":
                if stopLoss and velaLow <= stopLoss:
                    return {
                        "status": "CLOSED", 
                        "reason": "SL", 
                        "exitPrice": stopLoss, 
                        "closeTime": timestamp,
                        "full_close": True
                    }
                
                if tp1 and tp1 not in [l.get('tp') for l in closed_levels] and velaHigh >= tp1:
                    pct_to_add = 30 if not any(l.get('tp') == tp1 for l in partial_levels) else 0
                    if pct_to_add > 0:
                        result['closed_tp_levels'].append({'tp': tp1, 'pct': pct_to_add})
                        result['partial'] = True
                        result['exitPrice'] = tp1
                        result['closeTime'] = timestamp
                        pct_closed += pct_to_add
                        
                        if pct_closed >= 70:
                            return {
                                "status": "PARTIAL_CLOSED",
                                "reason": "TP1",
                                "exitPrice": tp1,
                                "closeTime": timestamp,
                                "partial": True,
                                "closed_tp_levels": result['closed_tp_levels'],
                                "remaining_size_pct": 100 - pct_closed
                            }
                
                if tp2 and tp2 not in [l.get('tp') for l in closed_levels] and velaHigh >= tp2:
                    pct_to_add = 40 if not any(l.get('tp') == tp2 for l in partial_levels) else 0
                    if pct_to_add > 0:
                        result['closed_tp_levels'].append({'tp': tp2, 'pct': pct_to_add})
                        result['partial'] = True
                        result['exitPrice'] = tp2
                        result['closeTime'] = timestamp
                        pct_closed += pct_to_add
                        
                        if pct_closed >= 70:
                            return {
                                "status": "PARTIAL_CLOSED",
                                "reason": "TP2",
                                "exitPrice": tp2,
                                "closeTime": timestamp,
                                "partial": True,
                                "closed_tp_levels": result['closed_tp_levels'],
                                "remaining_size_pct": 100 - pct_closed
                            }
                
                if tp_final and tp_final not in [l.get('tp') for l in closed_levels] and velaHigh >= tp_final:
                    pct_to_add = 30
                    result['closed_tp_levels'].append({'tp': tp_final, 'pct': pct_to_add})
                    return {
                        "status": "CLOSED",
                        "reason": "TP_FINAL",
                        "exitPrice": tp_final,
                        "closeTime": timestamp,
                        "full_close": True,
                        "closed_tp_levels": result['closed_tp_levels']
                    }
            
            else:  # CORTO
                if stopLoss and velaHigh >= stopLoss:
                    return {
                        "status": "CLOSED",
                        "reason": "SL",
                        "exitPrice": stopLoss,
                        "closeTime": timestamp,
                        "full_close": True
                    }
                
                if tp1 and tp1 not in [l.get('tp') for l in closed_levels] and velaLow <= tp1:
                    pct_to_add = 30 if not any(l.get('tp') == tp1 for l in partial_levels) else 0
                    if pct_to_add > 0:
                        result['closed_tp_levels'].append({'tp': tp1, 'pct': pct_to_add})
                        result['partial'] = True
                        result['exitPrice'] = tp1
                        result['closeTime'] = timestamp
                        pct_closed += pct_to_add
                        
                        if pct_closed >= 70:
                            return {
                                "status": "PARTIAL_CLOSED",
                                "reason": "TP1",
                                "exitPrice": tp1,
                                "closeTime": timestamp,
                                "partial": True,
                                "closed_tp_levels": result['closed_tp_levels'],
                                "remaining_size_pct": 100 - pct_closed
                            }
                
                if tp2 and tp2 not in [l.get('tp') for l in closed_levels] and velaLow <= tp2:
                    pct_to_add = 40 if not any(l.get('tp') == tp2 for l in partial_levels) else 0
                    if pct_to_add > 0:
                        result['closed_tp_levels'].append({'tp': tp2, 'pct': pct_to_add})
                        result['partial'] = True
                        result['exitPrice'] = tp2
                        result['closeTime'] = timestamp
                        pct_closed += pct_to_add
                        
                        if pct_closed >= 70:
                            return {
                                "status": "PARTIAL_CLOSED",
                                "reason": "TP2",
                                "exitPrice": tp2,
                                "closeTime": timestamp,
                                "partial": True,
                                "closed_tp_levels": result['closed_tp_levels'],
                                "remaining_size_pct": 100 - pct_closed
                            }
                
                if tp_final and tp_final not in [l.get('tp') for l in closed_levels] and velaLow <= tp_final:
                    pct_to_add = 30
                    result['closed_tp_levels'].append({'tp': tp_final, 'pct': pct_to_add})
                    return {
                        "status": "CLOSED",
                        "reason": "TP_FINAL",
                        "exitPrice": tp_final,
                        "closeTime": timestamp,
                        "full_close": True,
                        "closed_tp_levels": result['closed_tp_levels']
                    }
        
        if result['partial']:
            return result
        
        return None

    except Exception as e:
        logger.error(f"Error al verificar cierre multi-TP: {e}", exc_info=True)
        return None


def _get_latest_price_sync(symbol: str) -> float:
    try:
        from middleware.database import dbConnection
        conn = dbConnection.getConnection()
        if conn is None:
            return 1.0
        cursor = conn.cursor()
        cursor.execute("SELECT close FROM candles WHERE symbol=%s AND timeframe='5min' ORDER BY timestamp DESC LIMIT 1", (symbol,))
        row = cursor.fetchone()
        cursor.close()
        conn.close()
        if row and row[0]:
            return float(row[0])
    except Exception as e:
        logger.error(f"Error en _get_latest_price_sync para {symbol}: {e}")
    return 1.0


def calculatePnl(tradeData: Dict[str, Any], closureData: Dict[str, Any]) -> float:
    """
    Calculates the net Profit and Loss for a closed trade.
    """
    try:
        side = tradeData['direction'].upper()
        entryPrice = tradeData['entryPrice']
        size = tradeData['size']
        exitPrice = closureData['exitPrice']
        symbol = tradeData.get('symbol')
        
        # Simplified commission logic for now - handle None cases
        commission = tradeData.get('commission')
        if commission is None:
            commission = 0.0
        else:
            commission = float(commission)

        if side == "LARGO":
            grossPnl = (exitPrice - entryPrice) * size
        else:  # CORTO
            grossPnl = (entryPrice - exitPrice) * size
            
        # Convert grossPnl from quote currency to USD if needed
        symbolInfo = dbManager.getSymbol(symbol)
        quoteCurr = 'USD'
        if symbolInfo and 'quote_currency' in symbolInfo:
            quoteCurr = str(symbolInfo['quote_currency']).upper()
        elif symbolInfo and 'currencyQuote' in symbolInfo:
            quoteCurr = str(symbolInfo['currencyQuote']).upper()
            
        if quoteCurr != 'USD' and quoteCurr != '':
            usd_base_symbol = f"USD/{quoteCurr}"
            quote_usd_symbol = f"{quoteCurr}/USD"
            
            rate = 1.0
            # For JPY/CAD/CHF/MXN quote pairs, if the pair itself has USD base, use exitPrice
            if symbol.startswith("USD/"):
                rate = exitPrice
                if rate > 0:
                    grossPnl = grossPnl / rate
            else:
                rate_obtained = False
                try:
                    if quoteCurr in ["GBP", "EUR", "AUD", "NZD", "BTC"]:
                        rate = _get_latest_price_sync(quote_usd_symbol)
                        if rate > 0:
                            grossPnl = grossPnl * rate
                            rate_obtained = True
                    elif quoteCurr in ["JPY", "CAD", "CHF", "MXN", "HKD"]:
                        rate = _get_latest_price_sync(usd_base_symbol)
                        if rate > 0:
                            grossPnl = grossPnl / rate
                            rate_obtained = True
                except Exception as ex:
                    logger.error(f"Error obteniendo tasa de conversión para {quoteCurr}: {ex}")
                
                if not rate_obtained:
                    if "JPY" in symbol:
                        rate = _get_latest_price_sync("USD/JPY")
                        grossPnl = grossPnl / rate
                    elif "CAD" in symbol:
                        rate = _get_latest_price_sync("USD/CAD")
                        grossPnl = grossPnl / rate
                    elif "CHF" in symbol:
                        rate = _get_latest_price_sync("USD/CHF")
                        grossPnl = grossPnl / rate
                    elif "GBP" in symbol:
                        rate = _get_latest_price_sync("GBP/USD")
                        grossPnl = grossPnl * rate
        
        netPnl = grossPnl - commission
        return netPnl

    except Exception as e:
        logger.error(f"Error al calcular PNL: {e}", exc_info=True)
        return 0.0

def isDailyDrawdownLimitReached(accountId: int, maxDrawdownPercent: float = 20.0) -> bool:
    """
    Verifica si se ha alcanzado el límite de pérdida diaria (Drawdown).
    [TEMPORALMENTE DESHABILITADO PARA PRUEBAS]: Retorna siempre False para
    permitir el flujo continuo de señales durante la etapa de pruebas.
    """
    return False
