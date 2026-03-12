import logging
logger = logging.getLogger(__name__)

from datetime import datetime
import time
import warnings
import asyncio
from telegram import Bot
import data.dataLoader as dataLoader
#from config.settings import  TOKEN, IDGRUPO

warnings.filterwarnings("ignore")

 # # # ## COMUNICACIÓN CON TELEGRAM Y CONTROL DE RATE LIMITS #
async def enviar_alerta(id, token, mensaje):
    bot = Bot(token=token)
    import httpx
    
    try:
        await bot.send_message(chat_id=id, text=mensaje, parse_mode='Markdown')
        logger.info(f"Mensaje enviado a Telegram:\n {mensaje}")
    except Exception as e:
            try:
                await bot.send_message(chat_id=id, text=mensaje, parse_mode='Markdown')
                logger.info(f"Mensaje enviado a Telegram 2o intento:\n {mensaje}")
            except Exception as e:
                    logger.error(f"❌ Fallo al enviar mensaje a Telegram: {id}: {e}")
                    pass