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
            f"<center><i>ESTRATEGIA: {strategyName}</i></center>\n"
            f"<center><b>{trade['symbol']}</b> ({trade.get('intervalo', 'N/A')})</center>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center>Setup: <b>{setup}</b></center>\n"
            f"<center>Confianza: <b>{confianza}%</b></center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🟢 TAKE PROFIT: <b>{tp:,.6f}</b>\n"
            f"🔹 ENTRADA:     <b>{close:,.6f}</b>\n"
            f"🔴 STOP LOSS:   <b>{sl:,.6f}</b>\n"
            f"   CANTIDAD:  <b>{trade['size']:,.2f}</b>\n"
            f"━━━━━━━━━━━━━━━\n"
        )
    else:
        text = (
            f"{colorHeader}{colorHeader}{colorHeader} "
            f"<b>SEÑAL DE {directionStr}</b> "
            f"{colorHeader}{colorHeader}{colorHeader}\n"
            f"<center><i>ESTRATEGIA: {strategyName}</i></center>\n"
            f"<center><b>{trade['symbol']}</b> ({trade.get('intervalo', 'N/A')})</center>\n"
            f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"<center>Setup: <b>{setup}</b></center>\n"
            f"<center>Confianza: <b>{confianza}%</b></center>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🔴 STOP LOSS:   <b>{sl:,.6f}</b>\n"
            f"🔹 ENTRADA:     <b>{close:,.6f}</b>\n"
            f"🟢 TAKE PROFIT: <b>{tp:,.6f}</b>\n"
            f"   CANTIDAD:  <b>{trade['size']:,.2f}</b>\n"
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


def buildSclpngNYAlertMessage(signal: dict, trade: dict) -> str:
    fvgNum = signal.get('fvgNum', '')
    fvgText = f" #{fvgNum}" if fvgNum else ""
    
    extraFields = {
        'MAX': signal.get('precioMaximo', 0),
        'MIN': signal.get('precioMinimo', 0),
        f'FVG{fvgText}': signal.get('fvg', 'N/A')
    }
    
    return buildAlertMessage(
        signal=signal,
        trade=trade,
        strategyName="SclpngNY",
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