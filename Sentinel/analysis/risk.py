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
    Calculates the appropriate position size based on risk parameters.

    Returns:
        A tuple of (positionSize, riskInCurrency, margin_used) or (None, None, 0) on error.
        
    Note:
        - METALS: returns units (min 1)
        - FOREX: returns thousands of units (min 1000)
        - INDICE/CRYPTO: returns units (min 1)
    """
    try:
        import math
        
        if slDistance <= 0 or math.isnan(slDistance):
            logger.warning("La distancia del stop loss es cero, negativa o NaN. No se puede calcular el tamaño de posición.")
            return None, None, 0

        if math.isnan(riskPercentage) or math.isnan(capital):
            logger.warning(f"Capital o riesgo NaN: capital={capital}, riskPercentage={riskPercentage}")
            return None, None, 0

        riskInCurrency = capital * (riskPercentage / 100)
        
        symbolType = symbolInfo.get('tipo', 'FOREX').upper()
        symbolName = symbolInfo.get('symbol', '').upper()

        if riskInCurrency > capital:
            logger.warning(f"[{symbolName}] Riesgo {riskInCurrency:.2f} > capital {capital:.2f} - ajustar ganancia en BD")
            return None, None, 0
        
        MIN_RISK_USD = 5.0
        if riskInCurrency < MIN_RISK_USD:
            logger.warning(f"[{symbolName}] Riesgo {riskInCurrency:.2f} < ${MIN_RISK_USD} USD mínimo - omitir señal")
            return None, None, 0
        
        # Priorizar 'margen' de la BD (tabla SentinelSymbol), si no usar el genérico
        margin_percent = float(symbolInfo.get('margen')) / 100.0 if symbolInfo.get('margen') is not None else 0.02
        symbolMinLots = symbolInfo.get('min_lots')
        
        min_units = {
            "METALES": 1,
            "INDICE": 1,
            "CRYPTO": 0.01,
            "MONEDA": 1000,
            "EXOTIC": 1000
        }
        
        if symbolMinLots is not None:
            min_lots_val = int(symbolMinLots)
            min_units = {k: min_lots_val for k in min_units.keys()}
        
        def adjustForMargin(size, m_percent, capital_available, risk_curr, min_lots, price=1.0):
            # El margen se calcula sobre el valor nominal en la moneda de la cuenta: Unidades * Precio * Margen%
            # En Forex USD/XXX, el precio es 1.0 si la cuenta es USD.
            # En Oro XAU/USD, el precio es el precio del Oro.
            required = size * price * m_percent
                
            if required <= capital_available:
                return size, risk_curr
            
            # Recalcular tamaño máximo basado en margen
            max_size = math.floor(capital_available / (price * m_percent) / min_lots) * min_lots
                
            if max_size < min_lots:
                return None, 0
            
            adjusted_risk = (max_size / size) * risk_curr if size > 0 else risk_curr
            return max_size, adjusted_risk

        # --- METALS (e.g., XAU/USD) ---
        if symbolType == "METALES":
            units = riskInCurrency / slDistance
            min_lots = min_units.get("METALES", 1)
            if min_lots > 0:
                units = math.floor(units / min_lots) * min_lots
            
            units, riskInCurrency = adjustForMargin(units, margin_percent, capital, riskInCurrency, min_lots, price=entryPrice)
            if units is None: return None, None, 0
            
            margin_used = units * entryPrice * margin_percent
            return units, riskInCurrency, margin_used

        # --- INDICE (e.g., US30, SP500) ---
        elif symbolType == "INDICE":
            contracts = riskInCurrency / slDistance
            min_lots = min_units.get("INDICE", 1)
            if min_lots > 0:
                contracts = math.floor(contracts / min_lots) * min_lots
            
            contracts, riskInCurrency = adjustForMargin(contracts, margin_percent, capital, riskInCurrency, min_lots, price=entryPrice)
            if contracts is None: return None, None, 0
            
            margin_used = contracts * entryPrice * margin_percent
            return contracts, riskInCurrency, margin_used

        # --- CRYPTO (e.g., BTC/USD) ---
        elif symbolType == "CRYPTO":
            units = riskInCurrency / slDistance
            min_lots = min_units.get("CRYPTO", 0.01)
            if min_lots > 0:
                units = math.floor(units / min_lots) * min_lots
            
            units, riskInCurrency = adjustForMargin(units, margin_percent, capital, riskInCurrency, min_lots, price=entryPrice)
            if units is None: return None, None, 0
            
            margin_used = units * entryPrice * margin_percent
            return units, riskInCurrency, margin_used

        # --- FOREX (e.g., EUR/USD, USD/MXN) ---
        else:
            symbolData = dbManager.getSymbol(symbolName)
            pipValue = float(symbolData['pip']) if symbolData and symbolData.get('pip') else (0.01 if "JPY" in symbolName else 0.0001)
            pipsDistance = slDistance / pipValue
            
            if pipsDistance == 0: return None, None, 0

            quote_curr = symbolInfo.get('quote_currency', 'USD')
            base_pip_value = 1000.0 if quote_curr == 'JPY' else 10.0 # Valor por 1 lote (100k)
            
            if quote_curr == 'USD':
                valuePerPip = base_pip_value
            elif symbolName.startswith("USD/"):
                valuePerPip = base_pip_value / entryPrice
            else:
                valuePerPip = base_pip_value

            lots = riskInCurrency / (pipsDistance * valuePerPip)
            units = lots * 100000
            
            min_lots = min_units.get("MONEDA", 1000)
            units = math.floor(units / min_lots) * min_lots
            
            # Ajuste de Margen para Forex
            is_usd_base = symbolName.startswith("USD/")
            units, riskInCurrency = adjustForMargin(
                units, margin_percent, capital, riskInCurrency, min_lots, 
                price=entryPrice if not is_usd_base else 1.0
            )
            if units is None: return None, None, 0
            
            margin_used = units * (entryPrice if not is_usd_base else 1.0) * margin_percent
            
            return int(units), riskInCurrency, margin_used


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


def calculatePnl(tradeData: Dict[str, Any], closureData: Dict[str, Any]) -> float:
    """
    Calculates the net Profit and Loss for a closed trade.
    """
    try:
        side = tradeData['direction'].upper()
        entryPrice = tradeData['entryPrice']
        size = tradeData['size']
        exitPrice = closureData['exitPrice']
        
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
        
        netPnl = grossPnl - commission
        return netPnl

    except Exception as e:
        logger.error(f"Error al calcular PNL: {e}", exc_info=True)
        return 0.0

def is_daily_drawdown_limit_reached(accountId: int, maxDrawdownPercent: float = 2.0) -> bool:
    """
    Verifica si se ha alcanzado el límite de pérdida diaria (Drawdown).
    Compara la pérdida acumulada de trades cerrados hoy contra el capital INICIAL del día.
    """
    try:
        from datetime import date
        today = date.today().strftime("%Y-%m-%d")
        
        # Obtener trades cerrados hoy desde DB
        trades_hoy = dbManager.getTradesClosedToday(accountId, today)
        if not trades_hoy:
            return False
            
        total_pnl = sum(float(t.get('pnl', 0)) for t in trades_hoy)
        
        # Si el PnL neto es positivo o cero, no hay drawdown que bloquee
        if total_pnl >= 0:
            return False
            
        # Obtener capital actual y trades abiertos para calcular capital inicial
        account = dbManager.getAccountById(accountId)
        if not account:
            return False
        
        current_capital = float(account['Capital'])
        
        # Calcular capital inicial del día: capital actual + pérdidas de hoy
        initial_capital = current_capital + abs(total_pnl)
        
        # Evitar división por cero
        if initial_capital <= 0:
            return True  # Bloquear si no hay capital
        
        pérdida_percent = (abs(total_pnl) / initial_capital) * 100
        
        # if pérdida_percent >= maxDrawdownPercent:
        #     logger.warning(f"⚠️ BLOQUEO DE SEGURIDAD: Drawdown Diario alcanzado ({pérdida_percent:.2f}%). Capital inicial: ${initial_capital:.2f}, Pérdida: ${abs(total_pnl):.2f}")
        #     return True
            
        return False
    except Exception as e:
        logger.error(f"Error al verificar drawdown diario: {e}")
        return False
