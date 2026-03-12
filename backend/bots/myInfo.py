from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes


# Reemplaza 'TU_TOKEN_AQUI' con el token que te dio BotFather
TOKEN = '8709556193:AAGbqWrLlbr6WVp3fPjYmctDS09dvc9QvA8'

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envía un mensaje cuando se emite el comando /start."""
    user_id = update.effective_user.id
    await update.message.reply_text(f'Hola! Tu ID de usuario (User ID) es: {user_id}')

async def get_my_id(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Envía el ID de usuario cuando se emite el comando /myid."""
    user_id = update.effective_user.id
    await update.message.reply_text(f'Tu ID de usuario (User ID) es: {user_id}')

def main() -> None:
    """Inicia el bot."""
    # Crea la Application y pásale el token de tu bot.
    application = Application.builder().token(TOKEN).build()

    # Añade handlers para los comandos
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("myid", get_my_id))

    # Inicia el bot
    application.run_polling(poll_interval=3.0)

if __name__ == '__main__':
    main()