import io
import os
import math
from PIL import Image, ImageDraw, ImageFont
from middleware.utils.alertBuilder import getPipMultiplier
from middleware.database import dbManager

def generateSignalCard(
    strategyName: str,
    signal: dict,
    trade: dict
) -> io.BytesIO:
    """
    Generates a dynamic colored card image for the signal.
    Background is green for buy and red for sell.
    """
    scale = 1.4

    def loadFont(fontName: str, size: int):
        paths = [
            f"/System/Library/Fonts/Supplemental/{fontName}",
            f"/System/Library/Fonts/{fontName}",
            fontName
        ]
        for path in paths:
            if os.path.exists(path):
                try:
                    return ImageFont.truetype(path, size)
                except Exception:
                    pass
        return ImageFont.load_default()

    fontRegular = loadFont("Monaco.ttf", int(16 * scale))
    fontBold = loadFont("Monaco.ttf", int(18 * scale))
    fontTitle = loadFont("Monaco.ttf", int(26 * scale))
    fontHeader = loadFont("Monaco.ttf", int(14 * scale))
    fontPriceLabel = loadFont("Monaco.ttf", int(14 * scale))
    fontPriceVal = loadFont("Monaco.ttf", int(24 * scale))

    rawDir = signal.get('direction', signal.get('direccion', 'LARGO'))
    direction = str(rawDir).strip().upper() if rawDir else "LARGO"
    isLargo = direction in ["LARGO", "COMPRA"]
    
    symbol = trade.get('symbol', 'N/A')
    intervalo = trade.get('intervalo', 'N/A')
    accountName = trade.get('accountName', 'N/A')
    setup = signal.get('setup', signal.get('tipo_entrada', 'N/A'))
    confidence = signal.get('confidence', signal.get('confianza', 0.0))
    sentiment = signal.get('marketSentiment', 0.0)
    
    entryPrice = signal.get('entryPrice', signal.get('entrada', 0.0))
    stopLoss = trade.get('stopLoss', signal.get('stop_loss', 0.0))
    takeProfit = trade.get('takeProfit', signal.get('take_profit', 0.0))
    
    # Calculate Trail Info
    slDistPrice = abs(entryPrice - stopLoss)
    symbolMultiplier = getPipMultiplier(symbol)
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
        
    # Calculate Quantity String
    sizeVal = float(trade.get('size', 0.0))
    qtyStr = f"{sizeVal:,.2f}"
    
    try:
        symbolInfo = dbManager.getSymbol(symbol)
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
                if symbolTipo in ["MONEDA", "EXOTIC"]:
                    multiploVal = 10000
                elif symbolTipo == "METALES" or "XAU" in symbolName or "GOLD" in symbolName:
                    multiploVal = 450
                elif symbolTipo == "CRYPTO" or "BTC" in symbolName:
                    multiploVal = 2
            
            if multiploVal and sizeVal > multiploVal:
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
    except Exception:
        pass

    marginUsed = float(trade.get('margin_used', 0.0))
    
    isAdjustment = signal.get('is_adjustment', False)

    # Palette
    if isAdjustment:
        bgColor = (215, 240, 215) if isLargo else (255, 220, 220) # More saturated soft background for adjustments
    else:
        bgColor = (244, 249, 244) if isLargo else (254, 244, 244) # Very soft pastel background for normal signals
        
    accentColor = (46, 125, 50) if isLargo else (198, 40, 40) # Green / Red theme
    
    # Header color: Orange/Amber if it's an adjustment, otherwise Green/Red
    if isAdjustment:
        headerColor = (230, 81, 0) # Dark Orange (E65100)
    else:
        headerColor = (34, 112, 63) if isLargo else (186, 45, 45) # Dark Green / Dark Red
        
    textColor = (30, 41, 59) # Slate 800
    labelColor = (100, 116, 139) # Slate 500
    whiteColor = (255, 255, 255)
    cardBgColor = (255, 255, 255)
    borderColor = (226, 232, 240)
    
    cardWidth = int(500 * scale)
    cardHeight = int(730 * scale) # Increased height from 710 to 730 to accommodate split detail line
    
    image = Image.new("RGB", (cardWidth, cardHeight), bgColor)
    draw = ImageDraw.Draw(image)
    
    # Extract candle/signal timestamp or fallback to current time
    dateTimeStr = trade.get('candleTime') or signal.get('candleTime') or signal.get('candle_time')
    if not dateTimeStr:
        import datetime
        dateTimeStr = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 1. Header Banner
    draw.rectangle([0, 0, cardWidth, int(90 * scale)], fill=headerColor)
    
    if isAdjustment:
        headerTitle = "AJUSTE DE COMPRA" if isLargo else "AJUSTE DE VENTA"
        headerSubtext = f"{strategyName.upper()} - MODIFICACIÓN | {dateTimeStr}"
    else:
        headerTitle = "SEÑAL DE COMPRA" if isLargo else "SEÑAL DE VENTA"
        headerSubtext = f"{strategyName.upper()} | {dateTimeStr}"
        
    draw.text((int(25 * scale), int(15 * scale)), headerTitle, fill=whiteColor, font=fontTitle)
    draw.text((int(25 * scale), int(55 * scale)), headerSubtext, fill=(255, 224, 178) if isAdjustment else ((200, 230, 201) if isLargo else (255, 205, 210)), font=fontHeader)
    
    # 2. Main Content Card (white rounded rectangle)
    cardLeft = int(20 * scale)
    cardTop = int(110 * scale)
    cardRight = int(480 * scale)
    cardBottom = int(710 * scale) # Increased bottom from 690 to 710
    draw.rounded_rectangle(
        [cardLeft, cardTop, cardRight, cardBottom],
        radius=int(12 * scale),
        fill=cardBgColor,
        outline=borderColor,
        width=int(1 * scale)
    )
    
    # 3. Row 1: Symbol & Account
    draw.text((int(40 * scale), int(135 * scale)), "ACTIVO", fill=labelColor, font=fontRegular)
    draw.text((int(40 * scale), int(155 * scale)), f"{symbol} ({intervalo})", fill=textColor, font=fontBold)
    
    draw.text((int(260 * scale), int(135 * scale)), "CUENTA", fill=labelColor, font=fontRegular)
    draw.text((int(260 * scale), int(155 * scale)), accountName, fill=textColor, font=fontBold)
    
    # Divider
    draw.line([(int(40 * scale), int(195 * scale)), (int(460 * scale), int(195 * scale))], fill=borderColor, width=int(1 * scale))
    
    # 4. Row 2: Setup & Confidence
    draw.text((int(40 * scale), int(215 * scale)), "SETUP", fill=labelColor, font=fontRegular)
    draw.text((int(40 * scale), int(235 * scale)), setup, fill=textColor, font=fontBold)
    
    draw.text((int(260 * scale), int(215 * scale)), "CONFIANZA", fill=labelColor, font=fontRegular)
    draw.text((int(260 * scale), int(235 * scale)), f"{confidence:.2f}%", fill=accentColor, font=fontBold)
    
    # Divider
    draw.line([(int(40 * scale), int(275 * scale)), (int(460 * scale), int(275 * scale))], fill=borderColor, width=int(1 * scale))
    
    # 5. Price Levels Box (Highlighted)
    levelBoxTop = int(295 * scale)
    levelBoxHeight = int(180 * scale)
    draw.rounded_rectangle(
        [int(40 * scale), levelBoxTop, int(460 * scale), levelBoxTop + levelBoxHeight],
        radius=int(8 * scale),
        fill=(245, 247, 250), # Light gray highlight
        outline=borderColor,
        width=int(1 * scale)
    )
    
    # Draw Levels
    # Entrada
    draw.text((int(60 * scale), levelBoxTop + int(20 * scale)), "🔹 ENTRADA", fill=labelColor, font=fontPriceLabel)
    draw.text((int(240 * scale), levelBoxTop + int(15 * scale)), f"{entryPrice:,.5f}", fill=textColor, font=fontPriceVal)
    
    # SL
    draw.text((int(60 * scale), levelBoxTop + int(70 * scale)), "🔴 STOP LOSS", fill=labelColor, font=fontPriceLabel)
    draw.text((int(240 * scale), levelBoxTop + int(65 * scale)), f"{stopLoss:,.5f}", fill=(186, 45, 45), font=fontPriceVal)
    draw.text((int(60 * scale), levelBoxTop + int(90 * scale)), f"({trailInfo})", fill=labelColor, font=fontRegular)
    
    # TP
    draw.text((int(60 * scale), levelBoxTop + int(130 * scale)), "🟢 TAKE PROFIT", fill=labelColor, font=fontPriceLabel)
    draw.text((int(240 * scale), levelBoxTop + int(125 * scale)), f"{takeProfit:,.5f}", fill=(34, 112, 63), font=fontPriceVal)
    
    # 6. Row 3: Sizing & Margin
    draw.text((int(40 * scale), int(495 * scale)), "CANTIDAD (SIZE)", fill=labelColor, font=fontRegular)
    if "(" in qtyStr:
        mainQty, splitDetail = qtyStr.split(" (", 1)
        splitDetail = "(" + splitDetail
        draw.text((int(40 * scale), int(515 * scale)), mainQty, fill=textColor, font=fontBold)
        draw.text((int(40 * scale), int(538 * scale)), splitDetail, fill=labelColor, font=fontRegular)
    else:
        draw.text((int(40 * scale), int(515 * scale)), qtyStr, fill=textColor, font=fontBold)
    
    draw.text((int(260 * scale), int(495 * scale)), "MARGEN EST", fill=labelColor, font=fontRegular)
    marginStr = f"${marginUsed:,.2f} USD" if marginUsed else "N/A"
    draw.text((int(260 * scale), int(515 * scale)), marginStr, fill=textColor, font=fontBold)
    
    # Row 4: Risk & Benefit (Expected Profit) - Shifted down by 20 units
    riesgoUsd = float(signal.get('profit', 0.0))
    rrRatio = float(signal.get('rr_ratio', 0.0))
    expectedProfit = float(signal.get('expectedProfit', 0.0))
    beneficioUsd = expectedProfit if expectedProfit > 0 else (riesgoUsd * rrRatio)
    
    draw.text((int(40 * scale), int(575 * scale)), "RIESGO MÁX", fill=labelColor, font=fontRegular)
    draw.text((int(40 * scale), int(595 * scale)), f"${riesgoUsd:,.2f} USD" if riesgoUsd else "N/A", fill=(186, 45, 45), font=fontBold)
    
    draw.text((int(260 * scale), int(575 * scale)), "BENEFICIO EST", fill=labelColor, font=fontRegular)
    draw.text((int(260 * scale), int(595 * scale)), f"${beneficioUsd:,.2f} USD" if beneficioUsd else "N/A", fill=(34, 112, 63), font=fontBold)
    
    # Divider - Shifted down by 20 units
    draw.line([(int(40 * scale), int(635 * scale)), (int(460 * scale), int(635 * scale))], fill=borderColor, width=int(1 * scale))
    
    # 7. Sentiment - Shifted down by 20 units
    sentimentEmoji = "🟢" if sentiment > 0.2 else ("🔴" if sentiment < -0.2 else "⚪️")
    sentimentText = f"{sentimentEmoji} {sentiment:.2f}"
    draw.text((int(40 * scale), int(650 * scale)), f"AI Market Sentiment: {sentimentText}", fill=textColor, font=fontRegular)

    imageBuffer = io.BytesIO()
    image.save(imageBuffer, format="PNG")
    imageBuffer.seek(0)
    return imageBuffer
