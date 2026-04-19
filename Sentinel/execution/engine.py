import logging
from datetime import datetime
import pytz

import pandas as pd
from typing import Dict, Tuple, Optional

from middleware.database import dbManager
from Sentinel.analysis import risk
from middleware.execution.broker_gateway import gateway
from middleware.config.constants import TIMEZONE

logger = logging.getLogger(__name__)

# Cache de cuentas para evitar llamadas extremas a la BD
# En sistemas más puristas esto expiraría, pero aquí se regenera
# si está vacío. Dependiendo de los requerimientos, los bots lo leen directo.
_accounts_cache = None

def get_mexico_time() -> datetime:
    """Devuelve la fecha/hora actual localizada a México."""
    return datetime.now(pytz.timezone(TIMEZONE))

def clear_accounts_cache():
    """Limpia el caché de cuentas para forzar una recarga."""
    global _accounts_cache
    _accounts_cache = None


async def execute_signal(
    signal: Dict, 
    symbolInfo: Dict, 
    strategy_name: str, 
    df: Optional[pd.DataFrame] = None
) -> Tuple[bool, Optional[str]]:
    """
    Desacopla la lógica de riesgo e iteración de cuentas de las capas analíticas (Bots).
    
    Args:
        signal (Dict): Diccionario con los datos matemáticos puros de la señal.
                       Debe contener idealmente: 'direction', 'entryPrice', 'stopLoss', 
                       'takeProfit', 'slDistance'.
        symbolInfo (Dict): Detalles nativos del activo desde la BD.
        strategy_name (str): Nombre estandarizado de la estrategia.
        df (pd.DataFrame, optional): DataFrame para construir visualizaciones del Gateway.

    Returns:
        Tuple[bool, Optional[str]]: (Se_ejecuto_alguna_orden, Message_Id_devuelto_por_Telegram)
    """
    if not signal:
        return False, None

    global _accounts_cache
    if not _accounts_cache:
        _accounts_cache = dbManager.getAccount()
        if not _accounts_cache:
            logger.warning(f"[ExecutionEngine] No hay cuentas disponibles en BD para operar {strategy_name}.")
            return False, None

    success_any = False
    first_msg_id = None

    for account in _accounts_cache:
        # Excluir cuenta maestra de señales (SENTINEL)
        if account['idCuenta'] == 1: 
            continue
            
        if not dbManager.isEstrategiaHabilitadaParaCuenta(account['idCuenta'], strategy_name):
            logger.debug(f"[ExecutionEngine] Estrategia {strategy_name} deshabilitada para la cuenta {account['idCuenta']}.")
            continue

        # Dependiendo del bot, traen "slDistance", si no, la deducimos de la propia orden:
        entry_price = signal.get('entrada', signal.get('entryPrice'))
        sl_price = signal.get('stop_loss', signal.get('stopLoss'))
        sl_dist = signal.get('slDistance', abs(entry_price - sl_price) if entry_price and sl_price else 0)

        posSize, riskUsd, marginUsed = risk.calculatePositionSize(
            capital=float(account['Capital']),
            riskPercentage=float(account['ganancia']),
            slDistance=sl_dist,
            symbolInfo=symbolInfo,
            entryPrice=entry_price
        )

        if posSize is None or posSize == 0:
            logger.warning(f"[ExecutionEngine] Size=0 para {symbolInfo['symbol']} cuenta {account['idCuenta']} - Excede límite/capital")
            continue

        # Retroalimento el riesgo en dólares de cada iteracion en el dict clonado (referencia local)
        signal['profit'] = riskUsd
        
        # Obtener o derivar campos comunes
        interval = symbolInfo.get('intervalo', signal.get('intervalo', ''))
        tp_price = signal.get('take_profit', signal.get('takeProfit'))

        trade_data = {
            "idCuenta": account['idCuenta'],
            "symbol": symbolInfo['symbol'],
            "direction": signal.get('direccion', signal.get('direction', 'UNKNOWN')),
            "entryPrice": entry_price,
            "openTime": get_mexico_time().strftime("%Y-%m-%d %H:%M:%S"),
            "stopLoss": sl_price,
            "takeProfit": tp_price,
            "size": posSize,
            "intervalo": interval,
            "status": "OPEN",
            "strategy": strategy_name,
            "margin_used": marginUsed,
        }
        
        # Add-ons opcionales para estrategias específicas
        if 'fvgNum' in signal:
            trade_data['fvgNum'] = signal['fvgNum']

        # Disparo aislado y estandarizado
        success, msgId = await gateway.execute_trade(trade_data, signal, account, strategy_name, df=df)
        
        if success:
            success_any = True
            if not first_msg_id and msgId:
                first_msg_id = msgId
                
            logger.info(f"✅ Alerta {strategy_name} despachada con éxito para {symbolInfo['symbol']} a la cuenta {account['idCuenta']} | Lotes: {posSize}")

    return success_any, first_msg_id
