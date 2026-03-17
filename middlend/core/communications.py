"""
Module for handling external communications, like Telegram alerts.
"""
import logging
import re
import asyncio
from telegram import Bot
from telegram.error import TelegramError

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
        maxWidth = 45  # Assumed width for centering
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


async def sendTelegramAlert(token: str, chatId: str, message: str, highPriority: bool = True):
    """
    Sends a message to a Telegram chat using a specific bot token.
    Retries once on failure.
    """
    if not message or not token or not chatId:
        logger.warning("Telegram alert skipped due to missing message, token, or chatId.")
        return

    bot = Bot(token=token)
    cleanedMessage = _clean_html_for_telegram(message)
    
    try:
        await bot.sendMessage(
            chatId=chatId, 
            text=cleanedMessage, 
            parseMode='HTML', 
            disableNotification=not highPriority
        )
        logger.debug(f"Telegram alert sent to chatId {chatId}")
    except TelegramError as e:
        logger.error(f"Failed to send Telegram message to {chatId} on first attempt: {e}")
        # Retry once after a short delay
        await asyncio.sleep(1)
        try:
            await bot.sendMessage(
                chatId=chatId, 
                text=cleanedMessage, 
                parseMode='HTML', 
                disableNotification=not highPriority
            )
            logger.info(f"Successfully sent Telegram message to {chatId} on second attempt.")
        except TelegramError as eRetry:
            logger.critical(f"❌ Failed to send Telegram message to {chatId} after two attempts: {eRetry}")

