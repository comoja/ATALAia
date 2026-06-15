from datetime import datetime
from middleware.database import dbManager

def getPipMultiplier(symbol: str) -> float:
    """Obtiene el pip multiplier desde la BD (SentinelSymbol.pip) o usa fallback."""
    try:
        symbolData = dbManager.getSymbol(symbol)
        if symbolData and 'pip' in symbolData and symbolData['pip'] is not None:
            pip_val = float(symbolData['pip'])
            if pip_val > 0:
                return 1.0 / pip_val
    except Exception:
        pass
    
    symbol_up = symbol.upper()
    if "XAU" in symbol_up or "GOLD" in symbol_up:
        return 1.0
    if any(pair in symbol_up for pair in ["JPY", "HUF"]):
        return 100.0
    if any(crypto in symbol_up for crypto in ["BTC", "ETH", "SOL", "BNB"]):
        return 1.0
    if "MXN" in symbol_up:
        return 10000.0
    return 10000.0

def calculateRR(entry: float, sl: float, tp: float) -> float:
    """Calculates Risk:Reward ratio."""
    riesgo = abs(entry - sl)
    if riesgo == 0: return 0.0
    return round(abs(tp - entry) / riesgo, 2)

def adjustTPForMinRR(entry: float, sl: float, tp: float, direction: str, minRR: float = 1.5) -> float:
    """Ensures TP meets a minimum RR ratio and is in the correct direction."""
    riesgo = abs(entry - sl)
    if riesgo == 0: return tp
    rr = abs(tp - entry) / riesgo
    is_largo = direction.upper() in ["LARGO"]
    # Validar que el TP esté en la dirección correcta
    tp_wrong = (is_largo and tp <= entry) or (not is_largo and tp >= entry)
    if rr < minRR or tp_wrong:
        if is_largo:
            return entry + (riesgo * minRR)
        else:
            return entry - (riesgo * minRR)
    return tp

def calculateBEPrice(entry: float, sl: float, tp: float, direction: str) -> float:
    """
    Calcula el precio de activación de Break Even.
    - Por defecto intenta un Ratio 1:1.
    - Si el TP está más cerca que el 1:1 (RR < 1), pone el BE al 50% del camino al TP.
    """
    riesgo = abs(entry - sl)
    recompensa = abs(tp - entry)
    
    if riesgo == 0: return entry

    # Si la recompensa es menor al riesgo (RR < 1), usamos la mitad de la recompensa
    if recompensa < riesgo:
        trigger_dist = recompensa * 0.5
    else:
        # Estándar: 1:1 RR
        trigger_dist = riesgo
        
    if direction.upper() == "LARGO":
        return entry + trigger_dist
    else:
        return entry - trigger_dist

def safe_float(val, default=0.0) -> float:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def buildAlertMessage(
    signal: dict,
    trade: dict,
    strategyName: str,
    extraFields: dict = None
) -> str:
    # Normalización de dirección
    raw_dir = signal.get('direction', signal.get('direccion', 'LARGO'))
    direction = str(raw_dir).strip().upper() if raw_dir else "LARGO"
    
    # Mapeo estricto: LARGO = COMPRA, CORTO = VENTA
    directionStr = "COMPRA" if direction == "LARGO" else "VENTA"
    is_adjustment = signal.get('is_adjustment', False)
    
    if is_adjustment:
        title_base = "SEÑAL DE AJUSTE"
        colorHeaderIni = "🟩" if direction == "LARGO" else "🟥"
        colorHeader = "🟧"  # Naranja para ajustes, diferente de compra/venta
    else:
        title_base = f"SEÑAL DE {directionStr}"
        colorHeader = "🟩" if direction == "LARGO" else "🟥"
    
    close = signal.get('entryPrice', signal.get('entrada', 0))
    tp = trade.get('takeProfit', signal.get('take_profit', 0))
    sl = trade.get('stopLoss', signal.get('stop_loss', 0))
    be = signal.get('break_even', trade.get('break_even'))
    
    confianza = signal.get('confidence', signal.get('confianza', 0))
    setup = signal.get('setup', signal.get('tipo_entrada', 'N/A'))

    header = f"{colorHeader}{colorHeader}{colorHeader} <b>{title_base}</b> {colorHeader}{colorHeader}{colorHeader}" if not is_adjustment else f"{colorHeaderIni}{colorHeader}{colorHeader} <b>{title_base}</b> {colorHeader}{colorHeader}{colorHeaderIni}"

    text = (
        f"{header}\n"
        f"<i><b><center>{strategyName}</center></b></i>\n"
        f"<center>👤  <b>{trade.get('accountName', 'N/A')}</b></center>\n"
        f"<b><center>{trade['symbol']} ({trade.get('intervalo', 'N/A')})</center></b>\n"
        f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
        f"━━━━━━━━━━━━━━━\n"
    )
    
    if is_adjustment:
        text += f"<center>⚠️ <b>Revisar operación abierta</b> ⚠️</center>\n"
        text += f"<center><i>Nuevos niveles detectados</i></center>\n<center><i>ajustar TP/SL o cerrar</i></center>\n"
        text += f"━━━━━━━━━━━━━━━\n"
    
    text += (
        f"<center>Setup: <b>{setup}</b></center>\n"
        f"<center>Confianza: <b>{confianza:,.2f}%</b></center>\n"
    )
    
    sentiment = signal.get('marketSentiment', 0.0)
    sentiment_emoji = "⚪️"
    if sentiment > 0.2: sentiment_emoji = "🟢"
    elif sentiment < -0.2: sentiment_emoji = "🔴"
    
    text += f"<center>🤖 AI Sent: <b>{sentiment_emoji} {sentiment:.2f}</b></center>\n"
    
    imminent = signal.get('imminentNews')
    if imminent:
        text += f"<center><b>{imminent}</b></center>\n"
        
    # Calcular distancia de Stop Loss dinámico y Trail
    slDistPrice = abs(close - sl)
    symbolMultiplier = getPipMultiplier(trade['symbol'])
    slDistPips = slDistPrice * symbolMultiplier
    
    metadata = signal.get('metadata', {})
    atrMult = metadata.get('supertrendMultiplier', metadata.get('atr_multiplier', None))
    
    try:
        atrMultVal = float(atrMult) if atrMult is not None else None
    except (ValueError, TypeError):
        atrMultVal = None
        
    if atrMultVal is not None:
        trailInfo = f"{slDistPips:.1f} pips ({atrMultVal:.1f}x ATR)"
    else:
        trailInfo = f"{slDistPips:.1f} pips"

    text += f"━━━━━━━━━━━━━━━\n"

    if direction == "LARGO":
        text += (
            f"🟢 TAKE PROFIT: <b>{tp:,.5f}</b>\n"
        )
        if be:
            text += f"🟠 BREAK EVEN:  <b>{be:,.5f}</b>\n"
        text += (
            f"🔹 ENTRADA:     <b>{close:,.5f}</b>\n"
            f"🔴 STOP LOSS TRAIL: <b>{sl:,.5f} ({trailInfo})</b>\n"
        )
    else:
        text += (
            f"🔴 STOP LOSS TRAIL: <b>{sl:,.5f} ({trailInfo})</b>\n"
            f"🔹 ENTRADA:     <b>{close:,.5f}</b>\n"
        )
        if be:
            text += f"🟠 BREAK EVEN:  <b>{be:,.5f}</b>\n"
        text += (
            f"🟢 TAKE PROFIT: <b>{tp:,.5f}</b>\n"
        )

    # Calcular multiplo y formatear cantidad para evitar exceder el margen de 0.25%
    sizeVal = float(trade['size'])
    qtyStr = f"{sizeVal:,.2f}"
    
    try:
        symbolInfo = dbManager.getSymbol(trade['symbol'])
        if symbolInfo:
            symbolTipo = symbolInfo.get('tipo', 'FOREX').upper()
            symbolName = symbolInfo.get('symbol', '').upper()
            
            dbMultiplo = symbolInfo.get('multiplo')
            multiploVal = None
            if dbMultiplo is not None:
                try:
                    multiploVal = int(dbMultiplo)
                except (ValueError, TypeError):
                    pass
            
            if multiploVal is None:
                if symbolTipo == "MONEDA" or symbolTipo == "EXOTIC":
                    multiploVal = 10000
                elif symbolTipo == "METALES" or "XAU" in symbolName or "GOLD" in symbolName:
                    multiploVal = 450
                elif symbolTipo == "CRYPTO" or "BTC" in symbolName:
                    multiploVal = 2
            
            if multiploVal and sizeVal > multiploVal:
                import math
                nOrders = math.ceil(sizeVal / multiploVal)
                if nOrders > 1:
                    symbolMinLots = symbolInfo.get('min_lots')
                    minLots = 1000.0
                    if symbolMinLots is not None:
                        minLots = float(symbolMinLots)
                    else:
                        minUnitsDict = {
                            "METALES": 1.0,
                            "INDICE": 1.0,
                            "CRYPTO": 0.01,
                            "MONEDA": 1000.0,
                            "EXOTIC": 1000.0
                        }
                        minLots = float(minUnitsDict.get(symbolTipo, 1000.0))
                    
                    eachSize = math.floor((sizeVal / nOrders) / minLots) * minLots
                    if eachSize < minLots:
                        eachSize = minLots
                    
                    if eachSize > 0:
                        qtyStr = f"{sizeVal:,.2f} ({nOrders} x {eachSize:,.2f})"
    except Exception as e:
        import logging
        logging.getLogger("execution").error(f"Error calculando multiplos en buildAlertMessage: {e}")
        
    text += f"     CANTIDAD:  <b>{qtyStr}</b>\n"
    if trade.get('margin_used'):
        text += f"     MARGEN EST: <b>${trade['margin_used']:,.2f} USD</b>\n"
    text += f"━━━━━━━━━━━━━━━\n"

    
    if extraFields:
        for key, value in extraFields.items():
            if "Ratio" in key:
                text += f"• {key}: <b>{safe_float(value):.2f}</b>\n"
            elif "Pips" in key:
                text += f"• {key}: <b>{safe_float(value):,.1f}</b>\n"
            elif isinstance(value, float):
                text += f"• {key}: <b>{value:,.4f}</b>\n"
            else:
                text += f"• {key}: <b>{value}</b>\n"
    
    text += f"━━━━━━━━━━━━━━━\n"
    
    return text


def buildImbalanceNYAlertMessage(signal: dict, trade: dict) -> str:
    fvgNum = signal.get('fvgNum', '')
    fvgText = f" #{fvgNum}" if fvgNum else ""
    
    dentroRango = signal.get('dentroRango', True)
    rangoText = "Dentro" if dentroRango else "Fuera"
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'FVG': signal.get('fvg', 'N/A'),
        'Hora FVG': str(signal.get('vela_origen', 'N/A')).split('.')[0],
        'Hora FVG': str(signal.get('fvgTime', signal.get('vela_origen', 'N/A'))).split('.')[0],
        'Sesión': f"{signal.get('precioMaximo', 0):,.4f} - {signal.get('precioMinimo', 0):,.4f}"
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="NY SESSION IMBALANCE",
        extraFields=extraFields
    )


def buildImbalanceLDNAlertMessage(signal: dict, trade: dict) -> str:
    fvgNum = signal.get('fvgNum', '')
    fvgText = f" #{fvgNum}" if fvgNum else ""
    
    dentroRango = signal.get('dentroRango', True)
    rangoText = "Dentro" if dentroRango else "Fuera"
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'FVG': signal.get('fvg', 'N/A'),
        'Hora FVG': str(signal.get('vela_origen', 'N/A')).split('.')[0],
        'Hora FVG': str(signal.get('fvgTime', signal.get('vela_origen', 'N/A'))).split('.')[0],
        'Sesión': f"{signal.get('precioMaximo', 0):,.4f} - {signal.get('precioMinimo', 0):,.4f}"
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="LONDON SESSION IMBALANCE",
        extraFields=extraFields
    )


def buildSMAAlertMessage(signal: dict, trade: dict) -> str:
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'SMA20': f"{signal.get('sma20', 0):,.4f}",
        'SMA200': f"{signal.get('sma200', 0):,.4f}",
        'Tendencia': signal.get('tendencia', 'N/A'),
        'Momentum': momentum
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="TENDENCIA SMA 20-200",
        extraFields=extraFields
    )


def buildPatron4HAlertMessage(signal: dict, trade: dict) -> str:
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    tp1 = signal.get('tp1', signal.get('takeProfit'))
    tp2 = signal.get('tp2', signal.get('takeProfit2'))
    tp_final = signal.get('tp_final', signal.get('takeProfit3'))
    
    riesgoUsd = safe_float(signal.get('riskUsd', signal.get('profit')))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'Vela Origen': signal.get('velaOrigen', signal.get('candleTime', 'N/A')),
        'Momentum': momentum
    }
    
    if tp1: extraFields['TP1'] = round(tp1, 5)
    if tp2: extraFields['TP2'] = round(tp2, 5)
    if tp_final: extraFields['TP3'] = round(tp_final, 5)
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="PATTERN 4H HTF",
        extraFields=extraFields
    )



def buildEMAAlertMessage(signal: dict, trade: dict) -> str:
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'Slope': f"{signal.get('slope', 0):.2f}",
        'Separation': f"{signal.get('separation', 0):.4f}",
        'Prob ML': f"{signal.get('confidence', 0):.2f}%",
        'Momentum': momentum
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="TENDENCIA EMA 20-200 ML",
        extraFields=extraFields
    )


def buildSniperAlertMessage(signal: dict, trade: dict) -> str:
    latest = signal.get('latestMetrics', {})
    close = signal.get('entryPrice', 0)
    currentAtr = latest.get('atr', 0)
    vol_porcentaje = (currentAtr / close) * 100 if close > 0 else 0
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'RSI': f"{latest.get('rsi', 0):.2f} ({'🟢' if latest.get('pendienteRsi', 0) > 0 else '🔴'})",
        'MACD': 'ALCISTA 🟢' if latest.get('macdHist', 0) > 0 else 'BAJISTA 🔴',
        'Volatilidad': f"{vol_porcentaje:.3f}%",
        'Momentum': momentum
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="ML SNIPER TECHNICAL",
        extraFields=extraFields
    )


def buildSesgoBiasHTFAlertMessage(signal: dict, trade: dict) -> str:
    biases = signal.get('biases', {})
    zone = signal.get('zone', {})
    
    bias_str = f"H4: {biases.get('H4', 'N/A')} | D: {biases.get('D', 'N/A')} | W: {biases.get('W', 'N/A')} | M: {biases.get('M', 'N/A')}"
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'Bias HTF': bias_str,
        'Zona': f"{zone.get('type', 'N/A')} ({zone.get('fib_50', 0):.5f})",
        'Modelo': signal.get('tipo_entrada', 'N/A'),
        'MSS': 'Sí' if signal.get('mss') else 'No',
        'PO3': signal.get('po3_type', 'N/A'),
        'TF Confirmación': signal.get('timeframe_confirmacion', 'D'),
        'TF Entrada': signal.get('timeframe_entrada', 'H4')
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="BIAS HTF ANALYSIS",
        extraFields=extraFields
    )

def buildSilverBulletAlertMessage(signal: dict, trade: dict) -> str:
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    
    riesgoUsd = safe_float(signal.get('riskUsd', signal.get('profit')))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'Ventana': signal.get('window', 'N/A'),
        'FVG': signal.get('fvg', 'N/A'),
        'Hora FVG': str(signal.get('fvgTime', signal.get('vela_origen', 'N/A'))).split('.')[0],
        'ADX': signal.get('adx', 0),
        'Momentum': momentum
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="SILVER BULLET ICT",
        extraFields=extraFields
    )


def buildImbalancePMNYAlertMessage(signal: dict, trade: dict) -> str:
    fvgNum = signal.get('fvgNum', '')
    fvgText = f" #{fvgNum}" if fvgNum else ""
    
    dentroRango = signal.get('dentroRango', True)
    rangoText = "Dentro" if dentroRango else "Fuera"
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'FVG': signal.get('fvg', 'N/A'),
        'Hora FVG': str(signal.get('fvgTime', signal.get('vela_origen', 'N/A'))).split('.')[0],
        'Sesión PM': f"{signal.get('precioMaximo', 0):,.4f} - {signal.get('precioMinimo', 0):,.4f}",
        'Momentum': momentum
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="PM SESSION IMBALANCE",
        extraFields=extraFields
    )
def buildGenericFVGAlertMessage(signal: dict, trade: dict) -> str:
    """Standardized message for Generic FVG signals matching the user's template."""
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    
    riesgoUsd = safe_float(signal.get('riskUsd', signal.get('profit')))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'FVG': signal.get('fvg', 'N/A'),
        'Hora FVG': str(signal.get('fvgTime', signal.get('vela_origen', 'N/A'))).split('.')[0],
        'Confirmación': 'Price Action',
        'TF Señal': trade.get('intervalo', 'N/A'),
        'Momentum': momentum
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="FVG GENERICO",
        extraFields=extraFields
    )


def buildFVGDiarioAlertMessage(signal: dict, trade: dict) -> str:
    """Mensaje para estrategia FVGDiario - Manipulación + Daily Bias"""
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'Daily Bias': signal.get('daily_bias', 'N/A'),
        'PDH': f"{signal.get('pdh', 0):,.5f}",
        'PDL': f"{signal.get('pdl', 0):,.5f}",
        'Manipulación': signal.get('manipulation_type', 'N/A'),
        'FVG': signal.get('fvg_type', 'N/A'),
        'Hora FVG': str(signal.get('vela_origen', 'N/A')).split('.')[0],
        'Liq. Opuesta': f"{signal.get('opposite_liquidity', 0):,.5f}",
        'TF Entrada': trade.get('intervalo', '15min'),
        'Momentum': momentum
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="FVG DIARIO + MANIPULACIÓN",
        extraFields=extraFields
    )

def buildSpeedBotAlertMessage(signal: dict, trade: dict) -> str:
    """Mensaje para estrategia SpeedBot - Impulso Institucional"""
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    metadata = signal.get('metadata', {})
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'IMPULSO ⚡️'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'ATR Mult.': f"{metadata.get('atr_multiplier', 0):.2f}x",
        'Setup': signal.get('setup', 'N/A'),
        'Momentum': momentum
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="SPEED / DISPLACEMENT BOT",
        extraFields=extraFields
    )

def buildBreakoutNYAlertMessage(signal: dict, trade: dict) -> str:
    """Mensaje para estrategia BreakoutNY - rango apertura NY."""
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'RUPTURA CONFIRMADA'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'Rango Alto': f"{signal.get('range_high', 0):,.5f}",
        'Rango Bajo': f"{signal.get('range_low', 0):,.5f}",
        'Hora Rango': str(signal.get('range_time', 'N/A')),
        'Cierre Ruptura': f"{signal.get('breakout_close', 0):,.5f}",
    }

    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="BREAKOUT NY",
        extraFields=extraFields
    )

def buildIchimokuAlertMessage(signal: dict, trade: dict) -> str:
    """Mensaje para estrategia Ichimoku + Bollinger Bands."""
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'Tenkan-sen': f"{signal.get('tenkan', 0):.5f}",
        'Kijun-sen': f"{signal.get('kijun', 0):.5f}",
        'BB Middle': f"{signal.get('bb_middle', 0):.5f}",
        'Momentum': momentum
    }

    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="ICHIMOKU + BB + MACD",
        extraFields=extraFields
    )

def buildReversionMediaAlertMessage(signal: dict, trade: dict) -> str:
    """Mensaje estructurado para la estrategia Reversión a la Media (LRC + RSI)"""
    metadata = signal.get('metadata', {})
    
    # Calcular beneficios de forma segura
    riesgo_usd = safe_float(signal.get('profit'))
    if riesgo_usd == 0:
        riesgo_usd = safe_float(metadata.get('risk_usd', 0))
        
    rr_ratio = safe_float(signal.get('rr_ratio'))
    expected_profit = riesgo_usd * rr_ratio
    
    extraFields = {
        'Estado': signal.get('status', 'EN ZONA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rr_ratio,
        'Riesgo Máx:': f"${riesgo_usd:.2f} USD",
        'Beneficio Est:': f"${expected_profit:.2f} USD",
        'RSI (14)': f"{metadata.get('rsi', 0):.2f}",
        'Pendiente LRC': f"{metadata.get('lrc_slope', 0):.6f}",
        'Volumen VSA': f"{metadata.get('volume_ratio', 0):.2f}x (vs Promedio 20v)",
        'TP/SL Estructural': 'Confirmado 🛡️',
        'TF Entrada': trade.get('intervalo', '1h')
    }

    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="REVERSIÓN A LA MEDIA",
        extraFields=extraFields
    )

def buildQTrendAlertMessage(signal: dict, trade: dict) -> str:
    """Mensaje para la estrategia QTrend (SuperTrend + Q-Trend EMA)."""
    metadata = signal.get('metadata', {})
    momentumState = metadata.get('momentum', '☁️ NEUTRAL')
    
    supertrendPeriod = metadata.get('supertrendPeriod', 10)
    supertrendMult = metadata.get('supertrendMultiplier', 3.0)
    qtrendFast = metadata.get('qtrendFast', 9)
    qtrendSlow = metadata.get('qtrendSlow', 21)
    tpPercent = metadata.get('tpPercent', 0.025)
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), riesgoUsd * rrRatio)
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD",
        'Beneficio Est:': f"${expectedProfit:.2f} USD",
        'SuperTrend': f"{supertrendPeriod}p / {supertrendMult}x",
        'Q-Trend (EMA)': f"{qtrendFast} / {qtrendSlow}",
        'TP %': f"{tpPercent * 100:.2f}%",
        'Momentum': momentumState
    }

    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="QTREND SUPERTREND",
        extraFields=extraFields
    )


def buildBreakoutProbabilityAlertMessage(signal: dict, trade: dict) -> str:
    """Mensaje para la estrategia BreakoutProbability - Rupturas cuantitativas con Impulse MACD."""
    metadata = signal.get('metadata', {})
    
    breakoutProbability = metadata.get('breakout_probability', signal.get('confidence', 50.0))
    impulseMacd = metadata.get('impulse_macd', 0.0)
    channelMax = metadata.get('channel_max', 0.0)
    channelMin = metadata.get('channel_min', 0.0)
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), (riesgoUsd or 0) * (rrRatio or 0))
    
    extraFields = {
        'Estado': signal.get('status', 'RUPTURA ⚡️'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD" if riesgoUsd is not None else "N/A",
        'Beneficio Est:': f"${expectedProfit:.2f} USD" if expectedProfit is not None else "N/A",
        'Prob. Ruptura': f"<b>{breakoutProbability:.1f}%</b>",
        'Impulse MACD': f"{impulseMacd:.5f}",
        'Canal Máx': f"{channelMax:,.5f}",
        'Canal Mín': f"{channelMin:,.5f}",
        'TF': trade.get('intervalo', '15min')
    }

    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="BREAKOUT PROBABILITY",
        extraFields=extraFields
    )

def buildPremiumConfluenceAlertMessage(signal: dict, trade: dict) -> str:
    """Mensaje para la estrategia PremiumConfluence."""
    metadata = signal.get('metadata', {})
    
    riesgoUsd = safe_float(signal.get('profit'))
    rrRatio = safe_float(signal.get('rr_ratio'))
    expectedProfit = safe_float(signal.get('expectedProfit'), (riesgoUsd or 0) * (rrRatio or 0))
    momentumState = metadata.get('momentum', '☁️ NEUTRAL')
    
    extraFields = {
        'Estado': signal.get('status', 'EN ZONA ✅'),
        'Riesgo Pips': safe_float(signal.get('riesgo_pips')),
        'RR Ratio': rrRatio,
        'Riesgo Máx:': f"${riesgoUsd:.2f} USD" if riesgoUsd is not None else "N/A",
        'Beneficio Est:': f"${expectedProfit:.2f} USD" if expectedProfit is not None else "N/A",
        'SuperTrend': f"{metadata.get('supertrendPeriod', 10)}p / {metadata.get('supertrendMultiplier', 3.0)}x",
        'HA Periodos': f"{metadata.get('haPeriod1', 10)} / {metadata.get('haPeriod2', 10)}",
        'Momentum': momentumState,
        'TF': trade.get('intervalo', '15min')
    }

    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="PREMIUM CONFLUENCE",
        extraFields=extraFields
    )
