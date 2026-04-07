"""
Alert Message Builder - Centralized signal message formatting
"""
from datetime import datetime


def buildAlertMessage(
    signal: dict,
    trade: dict,
    strategyName: str,
    extraFields: dict = None
) -> str:
    direction = signal['direction']
    directionStr = "COMPRA" if direction == "LARGO" else "VENTA"
    colorHeader = "🟩" if direction == "LARGO" else "🟥"
    
    close = signal['entryPrice']
    tp = trade['takeProfit']
    sl = trade['stopLoss']
    confianza = signal['confidence']
    setup = signal.get('setup', 'N/A')

    
    if direction == "LARGO":
        text = (
            f"{colorHeader}{colorHeader}{colorHeader} "
            f"<b>SEÑAL DE {directionStr}</b> "
            f"{colorHeader}{colorHeader}{colorHeader}\n"
            f"<i><b><center>ESTRATEGIA: {strategyName}</center></b></i>\n"
            f"<b><center>{trade['symbol']} ({trade.get('intervalo', 'N/A')})</center></b>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center>Setup: <b>{setup}</b></center>\n"
            f"<center>Confianza: <b>{confianza}%</b></center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🟢 TAKE PROFIT: <b>{tp:,.5f}</b>\n"
            f"🔹 ENTRADA:     <b>{close:,.5f}</b>\n"
            f"🔴 STOP LOSS:   <b>{sl:,.5f}</b>\n"
            f"     CANTIDAD:  <b>{trade['size']:,.0f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
        )
    else:
        text = (
            f"{colorHeader}{colorHeader}{colorHeader} "
            f"<b>SEÑAL DE {directionStr}</b> "
            f"{colorHeader}{colorHeader}{colorHeader}\n"
            f"<i><b><center>ESTRATEGIA: {strategyName}</center></b></i>\n"
            f"<b><center>{trade['symbol']} ({trade.get('intervalo', 'N/A')})</center></b>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center>Setup: <b>{setup}</b></center>\n"
            f"<center>Confianza: <b>{confianza}%</b></center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔴 STOP LOSS:   <b>{sl:,.5f}</b>\n"
            f"🔹 ENTRADA:     <b>{close:,.5f}</b>\n"
            f"🟢 TAKE PROFIT: <b>{tp:,.5f}</b>\n"
            f"     CANTIDAD:  <b>{trade['size']:,.0f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
        )
    
    if extraFields:
        for key, value in extraFields.items():
            if isinstance(value, float):
                text += f"• {key}: <b>{value:,.6f}</b>\n"
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
        'MAX': signal.get('precioMaximo', 0),
        'MIN': signal.get('precioMinimo', 0),
        f'FVG{fvgText}': signal.get('fvg', 'N/A'),
        'Hora FVG': signal.get('fvgTime', 'N/A'),
        'Zona': rangoText
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="ImbalanceNY",
        extraFields=extraFields
    )


def buildImbalanceLDNAlertMessage(signal: dict, trade: dict) -> str:
    fvgNum = signal.get('fvgNum', '')
    fvgText = f" #{fvgNum}" if fvgNum else ""
    
    dentroRango = signal.get('dentroRango', True)
    rangoText = "Dentro" if dentroRango else "Fuera"
    
    extraFields = {
        'MAX': signal.get('precioMaximo', 0),
        'MIN': signal.get('precioMinimo', 0),
        f'FVG{fvgText}': signal.get('fvg', 'N/A'),
        'Hora FVG': signal.get('fvgTime', 'N/A'),
        'Zona': rangoText
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="ImbalanceLDN",
        extraFields=extraFields
    )


def buildSMAAlertMessage(signal: dict, trade: dict) -> str:
    extraFields = {
        'SMA20': signal.get('sma20', 0),
        'SMA200': signal.get('sma200', 0),
        'ATR': signal.get('atr', 0),
        'Tendencia': signal.get('tendencia', 'N/A')
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="SMA20-200",
        extraFields=extraFields
    )


def buildPatron4HAlertMessage(signal: dict, trade: dict) -> str:
    extraFields = {
        'Riesgo Pips': signal.get('riesgo_pips', 0),
        'RR Ratio': signal.get('rr_ratio', 0),
        'Confirmación': signal.get('timeframe_confirmacion', 'N/A'),
        'TF Señal': signal.get('timeframe_entrada', '15M')
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="PATRÓN 4H",
        extraFields=extraFields
    )


def buildEMAAlertMessage(signal: dict, trade: dict) -> str:
    extraFields = {
        'Slope': f"{signal.get('slope', 0):.2f}",
        'Separation': f"{signal.get('separation', 0):.4f}",
        'Prob ML': f"{signal.get('confidence', 0):.2f}%"
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="EMA 20-200 ML",
        extraFields=extraFields
    )


def buildSniperAlertMessage(signal: dict, trade: dict) -> str:
    latest = signal.get('latestMetrics', {})
    close = signal.get('entryPrice', 0)
    currentAtr = latest.get('atr', 0)
    vol_porcentaje = (currentAtr / close) * 100 if close > 0 else 0
    
    extraFields = {
        'RSI': f"{latest.get('rsi', 0):.2f} ({'🟢' if latest.get('pendienteRsi', 0) > 0 else '🔴'})",
        'CCI': f"{latest.get('cci', 0):.2f} ({'🟢' if latest.get('pendienteCci', 0) > 0 else '🔴'})",
        'MACD': 'ALCISTA 🟢' if latest.get('macdHist', 0) > 0 else 'BAJISTA 🔴',
        'Volatilidad': f"{vol_porcentaje:.3f}%",
        'Break even': f"{close:,.6f}"
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="ML SNIPER",
        extraFields=extraFields
    )