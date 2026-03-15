import logging
logger = logging.getLogger(__name__)

from datetime import datetime
import time
import warnings
import asyncio
from telegram import Bot
import data.dataLoader as dataLoader
import database.dbManager as dbManager

#from config.settings import  TOKEN, IDGRUPO

warnings.filterwarnings("ignore")

 # # # ## COMUNICACIÓN CON TELEGRAM Y CONTROL DE RATE LIMITS #
async def enviar_alerta(id, token, mensaje, prioridad=True):
    bot = Bot(token=token)
    import httpx
    
    try:
        #await bot.send_message(chat_id=id, text=mensaje, parse_mode='HTML', disable_notification=not prioridad)
        await bot.send_message(chat_id=id, text=mensaje, parse_mode='Markdown')        
    except Exception as e:
        try:
            await bot.send_message(chat_id=id, text=mensaje, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"❌ Fallo al enviar mensaje a Telegram: {id}: {e}")
            pass


def alertaInmediata(id, mensaje):
    cuentas = dbManager.getAccount(id)
    for cuenta in cuentas:
        token = cuenta['TokenMsg']
    enviar_alerta(id, token, mensaje,True)
        
