import logging
logger = logging.getLogger(__name__)

from datetime import datetime
import time
import warnings
import asyncio
from telegram import Bot
import data.dataLoader as dataLoader
import database.dbManager as dbManager
import re

#from config.settings import  TOKEN, IDGRUPO

warnings.filterwarnings("ignore")

def centraTexto(match, ancho_max=45):
        contenido = match.group(1)
        # Calculamos espacios necesarios: (Ancho total - largo del texto) / 2
        espacios_necesarios = (ancho_max - len(contenido)) // 2
        
        if espacios_necesarios > 0:
            # Usamos el espacio especial "punto de código" o espacios normales
            return " " * espacios_necesarios + contenido
        return contenido

def limpiarHtml(texto):
    texto = re.sub(r'<center>(.*?)</center>', centraTexto, texto, flags=re.DOTALL)
    # Lista de etiquetas permitidas por Telegram
    tags_permitidas = ['b', 'strong', 'i', 'em', 'u', 'ins', 's', 'strike', 'del', 'a', 'code', 'pre']
    
    # Expresión regular para encontrar etiquetas: <tag> o </tag>
    # Borra cualquier etiqueta que NO esté en la lista de permitidas
    def borrar_tag(match):
        tag_completa = match.group(0)
        nombre_tag = match.group(1).lower().strip('/')
        return tag_completa if nombre_tag in tags_permitidas else ""

    return re.sub(r'<(/?\w+).*?>', borrar_tag, texto)

async def alertaInmediata(id, mensaje, prioridad=True):
    cuentas = dbManager.getAccount(id)
    
    if cuentas:
        cuenta = cuentas[0]
        await enviarAlerta(cuenta['idGrupoMsg'], cuenta['TokenMsg'], mensaje=mensaje) 


async def enviarAlerta(id, token, mensaje, prioridad=True):
    bot = Bot(token=token)
    import httpx
    if mensaje is None or mensaje == "": return
    try:
        await bot.send_message(chat_id=id, text=limpiarHtml(mensaje), parse_mode='HTML', disable_notification=not prioridad)
        #await bot.send_message(chat_id=id, text=mensaje, parse_mode='Markdown')
    except Exception as e:
        try:
            await bot.send_message(chat_id=id, text=limpiarHtml(mensaje), parse_mode='HTML', disable_notification=not prioridad)
            #await bot.send_message(chat_id=id, text=mensaje, parse_mode='Markdown')
        except Exception as e:
            logger.error(f"❌ Fallo al enviar mensaje a Telegram: {id}: {e}")
            pass



        
