"""
Module for handling external communications, like Telegram alerts.
"""
import logging
import re
import asyncio
from telegram import Bot
from telegram.error import TelegramError
from Sentinel.database import dbManager


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
    Sends a message to a Telegram chat using a specific bot token.
    Retries once on failure.
    Returns message_id on success, None on failure.
    """
    if not message or not token or not chatId:
        logger.warning("Alerta de Telegram omitida por falta de mensaje, token o chatId.")
        return None

    bot = Bot(token=token)
    cleanedMessage = _clean_html_for_telegram(message)
    
    try:
        sent_message = await bot.send_message(
            chat_id=chatId, 
            text=cleanedMessage, 
            parse_mode='HTML', 
            disable_notification=not highPriority
        )
        logger.debug(f"Alerta de Telegram enviada a chatId {chatId}")
        return sent_message.message_id
    except TelegramError as e:
        logger.error(f"Error al enviar mensaje de Telegram a {chatId} en el primer intento: {e}")
        # Retry once after a short delay
        await asyncio.sleep(1)
        try:
            sent_message = await bot.send_message(
                chat_id=chatId, 
                text=cleanedMessage, 
                parse_mode='HTML', 
                disable_notification=not highPriority
            )
            logger.info(f"Mensaje de Telegram enviado exitosamente a {chatId} en el segundo intento.")
            return sent_message.message_id
        except TelegramError as eRetry:
            logger.critical(f"❌ Error al enviar mensaje de Telegram a {chatId} después de dos intentos: {eRetry}")
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

