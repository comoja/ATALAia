"""
Script de prueba para detectar_rebote_sma_doble
"""
import asyncio
import pandas as pd
import pytz
import os
import sys
from datetime import datetime

rutaProyecto = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaProyecto not in sys.path:
    sys.path.insert(0, rutaProyecto)

from Sentinel.api import twelvedata
from Sentinel.core.SMA20_200 import SMABot
from middleware.utils.communications import sendTelegramAlert
from Sentinel.database import dbManager
from Sentinel.data.dataLoader import getParametros
import talib as ta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def format_telegram_message(signal, symbol, interval):
    direction = signal['direction']
    directionStr = "COMPRA" if direction == "LARGO" else "VENTA"
    colorHeader = "🟢" if direction == "LARGO" else "🔴"
    
    close = signal['entryPrice']
    tp = signal['takeProfit']
    sl = signal['stopLoss']
    confianza = signal['confidence']
    setup = signal['setup']
    tendencia = signal['tendencia']
    
    text = (
        f"{colorHeader}{colorHeader}{colorHeader} "
        f"<b>SEÑAL SMA20-200 DE {directionStr}</b> "
        f"{colorHeader}{colorHeader}{colorHeader}\n"
        f"<center><i>Estrategia: SMA20-200</i></center>\n"
        f"<center><b>{symbol}</b> ({interval})</center>\n"
        f"<center>{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</center>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<center>Setup: <b>{setup}</b></center>\n"
        f"<center>Tendencia: <b>{tendencia}</b></center>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<b>Entrada:</b> <code>{close:.5f}</code>\n"
        f"<b>Stop Loss:</b> <code>{sl:.5f}</code>\n"
        f"<b>Take Profit:</b> <code>{tp:.5f}</code>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"<center>Confianza: <b>{confianza}%</b></center>\n"
    )
    return text


async def test_doble_toque():
    symbol = "EUR/USD"
    interval = "15min"
    nVelas = 500
    apiKey, _, _, _, _ = getParametros()
    
    cdmx_tz = pytz.timezone("America/Mexico_City")
    ahora_cdmx = datetime.now(cdmx_tz)
    
    print(f"\n{'='*60}")
    print(f"PRUEBA: Doble Toque SMA20")
    print(f"{'='*60}")
    print(f"Símbolo: {symbol}")
    print(f"Intervalo: {interval}")
    print(f"Hora CDMX actual: {ahora_cdmx.strftime('%Y-%m-%d %H:%M:%S %Z')}")
    print(f"{'='*60}\n")
    
    df = await twelvedata.getTimeSeries(symbol, interval, apiKey, nVelas=nVelas)
    
    if df is None or len(df) < 100:
        print("❌ Error: No se pudieron obtener datos")
        return
    
    df["sma20"] = ta.SMA(df["close"], timeperiod=20)
    df["sma200"] = ta.SMA(df["close"], timeperiod=200)
    df["atr"] = ta.ATR(df["high"], df["low"], df["close"], 14)
    df = df.dropna()
    
    if df.index.tzinfo is None:
        df.index = df.index.tz_localize(cdmx_tz)
    
    vela_actual_utc = df.index[-1]
    
    print(f"Datos cargados: {len(df)} velas")
    print(f"Última vela CDMX: {vela_actual_utc.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"SMA20 actual: {df['sma20'].iloc[-1]:.2f}")
    print(f"Precio actual: {df['close'].iloc[-1]:.2f}")
    print(f"Distancia: {abs(df['close'].iloc[-1] - df['sma20'].iloc[-1])/df['sma20'].iloc[-1]*100:.3f}%\n")
    
    sma_bot = SMABot()
    
    signal = await sma_bot._get_signal(df, symbol, interval)
    
    print(f"\n{'='*60}")
    if signal:
        print(f"✅ SEÑAL {signal['direction']} DETECTADA")
        print(f"{'='*60}")
        print(f"📊 ENTRADA:     {signal['entryPrice']:.5f}")
        print(f"🛡️  STOP LOSS:  {signal['stopLoss']:.5f}")
        print(f"🎯 TAKE PROFIT: {signal['takeProfit']:.5f}")
        print(f"📏 Dist SL:     {signal['slDistance']:.5f}")
        print(f"📈 Tendencia:   {signal['tendencia']}")
        print(f"🔧 Setup:       {signal['setup']}")
        print(f"{'='*60}")
        
        cuentas = dbManager.getAccount()
        message = format_telegram_message(signal, symbol, interval)
        
        print(f"\nEnviando alertas a {len(cuentas)} cuentas...")
        for cuenta in cuentas:
            if cuenta['idCuenta'] == 1:
                continue
            if cuenta.get('TokenMsg') and cuenta.get('idGrupoMsg'):
                msgId = await sendTelegramAlert(
                    cuenta['TokenMsg'],
                    cuenta['idGrupoMsg'],
                    message
                )
                if msgId:
                    print(f"  ✅ Enviado a cuenta {cuenta['idCuenta']}")
                else:
                    print(f"  ❌ Error enviando a cuenta {cuenta['idCuenta']}")
    else:
        print(f"❌ SIN SEÑAL")
    print(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(test_doble_toque())
