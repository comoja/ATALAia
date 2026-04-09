"""
Module for handling external communications, like Telegram alerts.
"""
import logging
import re
import asyncio
from telegram import Bot
from telegram.error import TelegramError, RetryAfter
from middleware.database import dbManager


logger = logging.getLogger(__name__)

def _clean_html_for_telegram(text: str) -> str:
    """
    Cleans and formats HTML-like text for Telegram.
    - Processes a custom <center> tag for text alignment.
    - Strips unsupported HTML tags.
    """
    if not isinstance(text, str):
        return ""

    def centerTextReplacer(match):
        content = match.group(1)
        maxWidth = 40  # Assumed width for centering
        spacesNeeded = (maxWidth - len(content)) // 2
        return " " * spacesNeeded + content if spacesNeeded > 0 else content

    text = re.sub(r'<center>(.*?)</center>', centerTextReplacer, text, flags=re.DOTALL)
    
    # List of tags supported by Telegram
    allowedTags = {'b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike', 'del', 'a', 'code', 'pre'}
    
    def stripUnsupportedTags(match):
        fullTag = match.group(0)
        tagName = match.group(1).lower().strip('/')
        return fullTag if tagName in allowedTags else ""

    return re.sub(r'<(/?\w+).*?>', stripUnsupportedTags, text)

async def alertaInmediata(id, mensaje, prioridad=True):
    cuentas = dbManager.getAccount(id)
    
    if cuentas:
        cuenta = cuentas[0]
        await sendTelegramAlert(cuenta['TokenMsg'],cuenta['idGrupoMsg'], message=mensaje) 

async def sendTelegramAlert(token: str, chatId: str, message: str, highPriority: bool = True):
    """
    Sends a message to a Telegram chat with intelligent retry on flood control.
    Returns message_id on success, None on failure.
    """
    if not message or not token or not chatId:
        logger.warning("Alerta de Telegram omitida por falta de mensaje, token o chatId.")
        return None

    bot = Bot(token=token)
    cleanedMessage = _clean_html_for_telegram(message)
    
    max_retries = 3
    for attempt in range(max_retries):
        try:
            sent_message = await bot.send_message(
                chat_id=chatId, 
                text=cleanedMessage, 
                parse_mode='HTML', 
                disable_notification=not highPriority
            )
            if attempt > 0:
                logger.info(f"✅ Alerta de Telegram enviada {cleanedMessage} a {chatId} tras {attempt} reintentos.")
            else:
                logger.debug(f"Alerta de Telegram enviada {cleanedMessage} a chatId {chatId}")
            return sent_message.message_id

        except RetryAfter as e:
            wait_time = e.retry_after + 1
            logger.warning(f"⚠️ Flood control excedido. Esperando {wait_time}s antes del reintento {attempt + 1}/{max_retries}...")
            await asyncio.sleep(wait_time)
            
        except TelegramError as e:
            logger.error(f"Error en intento {attempt + 1} al enviar a {chatId}: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2)
            else:
                logger.critical(f"❌ Error fatal de Telegram tras {max_retries} intentos: {e}")
                
    return None

async def deleteTelegramMessage(token: str, chatId: str, messageId: int):
    """
    Deletes a message from a Telegram chat.
    """
    if not token or not chatId or not messageId:
        logger.warning("No se puede eliminar mensaje: falta token, chatId o messageId.")
        return False
    
    bot = Bot(token=token)
    
    try:
        await bot.delete_message(chat_id=chatId, message_id=messageId)
        logger.info(f"Mensaje {messageId} eliminado del chat {chatId}")
        return True
    except TelegramError as e:
        logger.error(f"Error al eliminar mensaje {messageId} de {chatId}: {e}")
        return False
