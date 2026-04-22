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
    """Ensures TP meets a minimum RR ratio."""
    riesgo = abs(entry - sl)
    if riesgo == 0: return tp
    rr = abs(tp - entry) / riesgo
    if rr < minRR:
        if direction.upper() in [ "LARGO"]:
            return entry + (riesgo * minRR)
        else:
            return entry - (riesgo * minRR)
    return tp

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
    colorHeader = "🟩" if direction == "LARGO" else "🟥"
    
    close = signal.get('entryPrice', signal.get('entrada', 0))
    tp = trade.get('takeProfit', signal.get('take_profit', 0))
    sl = trade.get('stopLoss', signal.get('stop_loss', 0))
    confianza = signal.get('confidence', signal.get('confianza', 0))
    setup = signal.get('setup', signal.get('tipo_entrada', 'N/A'))

    if direction == "LARGO":
        text = (
            f"{colorHeader}{colorHeader}{colorHeader} "
            f"<b>SEÑAL DE {directionStr}</b> "
            f"{colorHeader}{colorHeader}{colorHeader}\n"
            f"<i><b><center>{strategyName}</center></b></i>\n"
            f"<b><center>{trade['symbol']} ({trade.get('intervalo', 'N/A')})</center></b>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center>Setup: <b>{setup}</b></center>\n"
            f"<center>Confianza: <b>{confianza}%</b></center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🟢 TAKE PROFIT: <b>{tp:,.5f}</b>\n"            
            f"🔹 ENTRADA:     <b>{close:,.5f}</b>\n"
            f"🔴 STOP LOSS:   <b>{sl:,.5f}</b>\n"
            f"     CANTIDAD:  <b>{trade['size']:,.2f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
        )
    else:
        text = (
            f"{colorHeader}{colorHeader}{colorHeader} "
            f"<b>SEÑAL DE {directionStr}</b> "
            f"{colorHeader}{colorHeader}{colorHeader}\n"
            f"<i><b><center>{strategyName}</center></b></i>\n"
            f"<b><center>{trade['symbol']} ({trade.get('intervalo', 'N/A')})</center></b>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center>Setup: <b>{setup}</b></center>\n"
            f"<center>Confianza: <b>{confianza}%</b></center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔴 STOP LOSS:   <b>{sl:,.5f}</b>\n"
            f"🔹 ENTRADA:     <b>{close:,.5f}</b>\n"
            f"🟢 TAKE PROFIT: <b>{tp:,.5f}</b>\n"
            f"     CANTIDAD:  <b>{trade['size']:,.2f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
        )
    
    if extraFields:
        for key, value in extraFields.items():
            if "Ratio" in key:
                text += f"• {key}: <b>{value:.2f}</b>\n"
            elif "Pips" in key:
                text += f"• {key}: <b>{value:,.1f}</b>\n"
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
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Riesgo Máx:': f"${signal.get('profit', 0):.2f} USD",
        'FVG': signal.get('fvg', 'N/A'),
        'Hora FVG': signal.get('fvgTime', 'N/A'),
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
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Riesgo Máx:': f"${signal.get('profit', 0):.2f} USD",
        'FVG': signal.get('fvg', 'N/A'),
        'Hora FVG': signal.get('fvgTime', 'N/A'),
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
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Riesgo Máx:': f"${signal.get('profit', 0):.2f} USD",
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
    
    tp1 = signal.get('tp1')
    tp2 = signal.get('tp2')
    tp_final = signal.get('tp_final')
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0) or 0,
        'RR Ratio': signal.get('rr_ratio', 0) or 0,
        'Riesgo Máx:': f"${(signal.get('profit') or 0):.2f} USD",
        'Confirmación': signal.get('timeframe_confirmacion', 'N/A'),
        'TF Señal': signal.get('timeframe_entrada', '15M'),
        'Vela Origen': signal.get('vela_origen', 'N/A'),
        'Momentum': momentum,
        'TP1': round(tp1, 5) if tp1 and tp1 > 0 else 0.0,
        'TP2': round(tp2, 5) if tp2 and tp2 > 0 else 0.0,
        'TP3': round(tp_final, 5) if tp_final and tp_final > 0 else 0.0
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="PATTERN 4H HTF",
        extraFields=extraFields
    )


def buildEMAAlertMessage(signal: dict, trade: dict) -> str:
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Riesgo Máx:': f"${signal.get('profit', 0):.2f} USD",
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
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Riesgo Máx:': f"${signal.get('profit', 0):.2f} USD",
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
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Riesgo Máx:': f"${signal.get('profit', 0):.2f} USD",
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
    ote_text = "✅ CONFIRMADA" if signal.get('ote_ok') else "⚠️ FUERA DE ZONA"
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Riesgo Máx:': f"${signal.get('profit', 0):.2f} USD",
        'Ventana': signal.get('window_label', 'N/A'),
        'FVG': signal.get('fvg', 'N/A'),
        'OTE': ote_text,
        'Sweep': signal.get('sweep_type', 'N/A'),
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
    momentum = signal.get('momentum', '☁️ NEUTRAL')
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Riesgo Máx:': f"${signal.get('profit', 0):.2f} USD",
        'FVG': signal.get('fvg', 'N/A'),
        'Hora FVG': signal.get('fvgTime', 'N/A'),
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
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Riesgo Máx:': f"${signal.get('profit', 0):.2f} USD",
        'FVG': signal.get('fvg', 'N/A'),
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
    extraFields = {
        'Estado': signal.get('status', 'ACTIVA ✅'),
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Riesgo Máx:': f"${signal.get('profit', 0):.2f} USD",
        'Daily Bias': signal.get('daily_bias', 'N/A'),
        'PDH': f"{signal.get('pdh', 0):,.5f}",
        'PDL': f"{signal.get('pdl', 0):,.5f}",
        'Manipulación': signal.get('manipulation_type', 'N/A'),
        'FVG': signal.get('fvg_type', 'N/A'),
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
