import logging
import pandas as pd
from typing import Dict, List, Optional
from datetime import datetime

from middleware.database import dbManager
from middleware.execution.broker_gateway import gateway
from Sentinel.ml import model as ml_model

logger = logging.getLogger("sentinel")

class TradeManager:
    """
    Gestor dinámico de posiciones abiertas.
    Se encarga de evaluar operaciones en vivo para:
    1. Mover Trailing Stops y asegurar Break-Even.
    2. Ejecutar Salidas Inteligentes (Smart Exits) impulsadas por Machine Learning.
    """

    def __init__(self, aggressiveness: float = 0.5):
        self.aggressiveness = aggressiveness # 0.5 = neutral, >0.5 = exits faster

    async def manageOpenPositions(self, master_data: Dict[str, pd.DataFrame]):
        """
        Ciclo principal de revisión de trades abiertos.
        Debe ser invocado por el main loop de Sentinel en cada iteración.
        master_data: Diccionario con DataFrames por símbolo y timeframe, precalculado.
        """
        try:
            # 1. Recuperar todas las posiciones OPEN
            open_trades = dbManager.getOpenTrades() # Necesitaríamos este método
        except Exception as e:
            logger.error(f"Error fetching open trades: {e}")
            return

        if not open_trades:
            return

        # 2. Cargar modelos ML si no están cargados (para Smart Exits)
        # Solo se cargan una vez si se mantienen en memoria, pero para simplificar
        # asumiremos que ml_model.loadModel() tiene su propio caché o lo cargamos:
        clf_model = ml_model.loadModel()
        reg_model = ml_model.loadRegModel()

        for trade in open_trades:
            symbol = trade['symbol']
            
            # Buscar el DataFrame más reciente de corto plazo (ej. 5m o 15m)
            # para calcular ATR y pasar al modelo ML.
            df_sym = master_data.get(symbol, {}).get('5min')
            if df_sym is None or df_sym.empty:
                df_sym = master_data.get(symbol, {}).get('15min')
            
            if df_sym is None or df_sym.empty:
                continue
                
            current_price = df_sym['close'].iloc[-1]
            atr = df_sym['atr'].iloc[-1] if 'atr' in df_sym.columns else None
            
            if not atr:
                # Fallback genérico si no hay ATR (0.2%)
                atr = current_price * 0.002
                
            direction = trade['direction'].upper()
            entry_price = float(trade['entryPrice'])
            current_sl = float(trade['stopLoss'])
            
            # Ganancia de la operación en términos de unidades de precio
            gain_in_price = (current_price - entry_price) if direction == "LARGO" else (entry_price - current_price)
            
            # --- 1. LÓGICA DE TRAILING STOP ESCALONADO ---
            new_sl = None
            if direction == "LARGO":
                if gain_in_price >= (atr * 2.5):
                    # Nivel 3: Agresivo (ganancia >= 2.5 ATR -> trailing 0.5 ATR)
                    new_sl = current_price - (atr * 0.5)
                elif gain_in_price >= (atr * 1.5):
                    # Nivel 2: Normal (ganancia >= 1.5 ATR -> trailing 1.0 ATR)
                    new_sl = current_price - (atr * 1.0)
                elif gain_in_price >= (atr * 1.0):
                    # Nivel 1: Break-Even Temprano (ganancia >= 1.0 ATR -> SL a Entry)
                    new_sl = entry_price
                
                if new_sl is not None and new_sl > current_sl:
                    self._updateSL(trade, new_sl)
                    current_sl = new_sl
                    
            elif direction == "CORTO":
                if gain_in_price >= (atr * 2.5):
                    new_sl = current_price + (atr * 0.5)
                elif gain_in_price >= (atr * 1.5):
                    new_sl = current_price + (atr * 1.0)
                elif gain_in_price >= (atr * 1.0):
                    new_sl = entry_price
                    
                if new_sl is not None and new_sl < current_sl:
                    self._updateSL(trade, new_sl)
                    current_sl = new_sl
                        
            # --- 2. LÓGICA DE SMART EXIT (MACHINE LEARNING) ---
            if clf_model is not None and len(df_sym) >= 100:
                # Limpiar datos para que coincidan con los features del modelo
                # Necesitamos extraer el último vector de características
                try:
                    features_cols = ml_model.MODEL_FEATURES
                    # Si no están calculados todos los indicadores, los pasamos por alto o los re-calculamos.
                    # Asumimos que main.py ya calculó la mayoría.
                    X_latest = df_sym[features_cols].tail(1)
                    
                    if not X_latest.isnull().values.any():
                        # Probabilidad de ir LARGO (1)
                        prob_up = ml_model.predictProba(clf_model, X_latest)
                        
                        if prob_up is not None:
                            # Sensibilidad Dinámica: Cortar pérdidas rápido si la IA anticipa un cambio de tendencia
                            # Si vamos LARGO y la probabilidad de subir cae por debajo del 35%
                            if direction == "LARGO" and prob_up <= 0.35:
                                await self._triggerSmartExit(trade, current_price, f"ML SmartExit: Prob Up colapsó a {prob_up:.2f} (Alerta de Reversa)")
                            # Si vamos CORTO y la probabilidad de subir sube por encima del 65%
                            elif direction == "CORTO" and prob_up >= 0.65:
                                await self._triggerSmartExit(trade, current_price, f"ML SmartExit: Prob Up explotó a {prob_up:.2f} (Alerta de Reversa)")
                                
                except Exception as ml_err:
                    logger.debug(f"Smart Exit ML omitido por falta de características para {symbol}: {ml_err}")


    def _updateSL(self, trade: dict, new_sl: float):
        """Actualiza el SL vía gateway y base de datos."""
        trade_id = trade['idTrade']
        ticket_id = trade.get('ticketId')
        symbol = trade['symbol']
        
        logger.info(f"🔄 Trailing Stop dinámico activado para {symbol} (Trade {trade_id}). Nuevo SL: {new_sl:.5f}")
        
        # 1. Update MT5 via gateway si es live
        if ticket_id:
            mt5Symbol = gateway.findMt5Symbol(symbol)
            success = gateway.updateLiveSLTP(int(ticket_id), new_sl, mt5Symbol)
            if not success:
                logger.warning(f"No se pudo actualizar Trailing Stop en Broker para {symbol}")
        
        # 2. Update DataBase
        try:
            dbManager.updateTradeLevels(trade_id, float(new_sl), float(trade['takeProfit']))
        except Exception as e:
            logger.error(f"Error actualizando SL en DB para {trade_id}: {e}")

    async def _triggerSmartExit(self, trade: dict, current_price: float, reason: str):
        """Cierra el trade anticipadamente para evitar colapsos predichos por ML."""
        logger.warning(f"🤖 [SMART EXIT] Cerrando posición abierta {trade['symbol']} | Motivo: {reason}")
        await gateway.close_trade(
            id_trade=trade['idTrade'],
            exit_price=current_price,
            reason=reason
        )
