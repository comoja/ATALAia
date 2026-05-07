import logging
import math
from typing import Dict, Any, List, Optional
from datetime import datetime
from Sentinel.core.models import Signal
from Sentinel.analysis import risk
from middleware.database import dbManager
from middleware.execution.broker_gateway import gateway
from middleware.config import constants as config

logger = logging.getLogger("sentinel")

class ExecutionEngine:
    """
    Centralized engine for processing trading signals, managing risk,
    and executing trades across multiple accounts.
    """

    def __init__(self):
        self.accounts = []
        self._refresh_accounts()

    def _refresh_accounts(self):
        """Fetch active accounts from the database."""
        try:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                logger.warning("[ExecutionEngine] No se encontraron cuentas activas en la BD.")
        except Exception as e:
            logger.error(f"[ExecutionEngine] Error al obtener cuentas: {e}")

    async def process_signals(self, signals: List[Signal]):
        """
        Processes a list of signals sequentially.
        """
        for signal in signals:
            await self.process_signal(signal)

    async def process_signal(self, signal: Signal, symbol_info: Optional[Dict[str, Any]] = None, df_context: Optional[Any] = None) -> bool:
        """
        Processes a signal: validates, calculates risk per account, and executes.
        """
        if not signal:
            return False

        symbol = signal.symbol
        strategy_name = signal.strategy

        # 1. Ensure we have symbol_info
        if symbol_info is None:
            symbol_info = dbManager.getSymbol(symbol)
            if not symbol_info:
                logger.error(f"[ExecutionEngine] No se encontró información para el símbolo {symbol}. Omitiendo señal.")
                return False

        # 2. Refresh accounts to ensure we have latest balances/status
        self._refresh_accounts()
        if not self.accounts:
            return False

        # Obtenemos todos los trades abiertos para este símbolo una vez
        try:
            all_open_trades = dbManager.getOpenTradesBySymbol(symbol)
        except Exception as e:
            all_open_trades = []
            logger.error(f"[ExecutionEngine] Error verificando trades abiertos para {symbol}: {e}")

        executed_any = False
        
        # 3. Iterate over accounts
        for account in self.accounts:
            account_id = account['idCuenta']
            
            # Exclude master account (ID 1: SENTINEL) from signals if needed
            if account_id == 1:
                continue

            # Check if this strategy is enabled for this specific account
            if not dbManager.isEstrategiaHabilitadaParaCuenta(account_id, strategy_name):
                logger.debug(f"[ExecutionEngine] Estrategia {strategy_name} no habilitada para cuenta {account_id}")
                continue

            # --- NUEVA LÓGICA DE AJUSTE ---
            # Buscar si ESTA cuenta tiene un trade abierto (sin cerrar) para este setup
            existing_trade = next((t for t in all_open_trades if t['idCuenta'] == account_id and t['strategy'] == strategy_name and t.get('setup') == signal.setup and t.get('closeTime') is None), None)
            
            is_adjustment = False
            id_trade_to_update = None
            if existing_trade:
                diff_tp = abs(existing_trade['takeProfit'] - signal.take_profit) / signal.take_profit > 0.0001
                diff_sl = abs(existing_trade['stopLoss'] - signal.stop_loss) / signal.stop_loss > 0.0001
                
                if diff_tp or diff_sl:
                    is_adjustment = True
                    id_trade_to_update = existing_trade['idTrade']
                    logger.info(f"[ExecutionEngine] [{symbol}] AJUSTE detectado para cuenta {account_id}")
                else:
                    logger.debug(f"[ExecutionEngine] [{symbol}] Setup ya abierto en cuenta {account_id} - omitiendo.")
                    continue

            # 4. Calculate Risk and Position Size (Apply risk_factor)
            try:
                base_risk = float(account['ganancia'])
                effective_risk = base_risk * signal.risk_factor
                
                # Aplicar Cap de Riesgo Máximo
                if effective_risk > config.MAX_RISK_PER_TRADE:
                    logger.info(f"[ExecutionEngine] [{symbol}] Riesgo {effective_risk}% excedía el máximo. Ajustado a {config.MAX_RISK_PER_TRADE}%")
                    effective_risk = config.MAX_RISK_PER_TRADE
                
                pos_size, risk_usd, margin_used = risk.calculatePositionSize(
                    capital=float(account['Capital']),
                    riskPercentage=effective_risk,
                    slDistance=signal.sl_distance,
                    symbolInfo=symbol_info,
                    entryPrice=signal.entry_price
                )
            except Exception as e:
                logger.error(f"[ExecutionEngine] Error calculando riesgo para cuenta {account_id}: {e}")
                continue

            if pos_size is None or pos_size == 0:
                risk_str = f"${risk_usd:.2f}" if risk_usd else "None"
                logger.warning(f"[ExecutionEngine] [{symbol}] Size=0 para cuenta {account_id} - riesgo {risk_str} insuficiente o margen excedido.")
                continue

            # --- NUEVA VALIDACIÓN Y AJUSTE: Riesgo Por Operación ---
            riesgo_por_operacion_pct = float(account.get('riesgoPorOperacion', 1.0)) # Default 1.0% si no existe
            limite_operacion_usd = float(account['Capital']) * (riesgo_por_operacion_pct / 100.0)
            
            if (margin_used + risk_usd) > limite_operacion_usd:
                total_current = margin_used + risk_usd
                if total_current > 0:
                    reduction_factor = limite_operacion_usd / total_current
                    new_size_raw = pos_size * reduction_factor
                    
                    symbol_type = symbol_info.get('tipo', 'FOREX').upper()
                    symbol_min_lots = symbol_info.get('min_lots')
                    min_units_dict = {
                        "METALES": 1,
                        "INDICE": 1,
                        "CRYPTO": 0.01,
                        "MONEDA": 1000,
                        "EXOTIC": 1000
                    }
                    if symbol_min_lots is not None:
                        min_lots = float(symbol_min_lots)
                    else:
                        min_lots = float(min_units_dict.get(symbol_type, 1000 if symbol_type not in min_units_dict else 1))
                    
                    original_pos_size = pos_size
                    pos_size = math.floor(new_size_raw / min_lots) * min_lots
                    
                    if symbol_type in ['MONEDA', 'EXOTIC']:
                        pos_size = int(pos_size)
                    
                    if pos_size < min_lots:
                        logger.warning(
                            f"[ExecutionEngine] [{symbol}] Cuenta {account_id}: Tras reducir por límite de riesgo (máx {limite_operacion_usd:.2f} USD), "
                            f"el tamaño ({new_size_raw}) es menor al lote mínimo ({min_lots}). Se omite señal."
                        )
                        continue
                    
                    # Recalcular margin y risk proporcionales al nuevo tamaño
                    margin_used = margin_used * (pos_size / original_pos_size)
                    risk_usd = risk_usd * (pos_size / original_pos_size)
                    
                    logger.info(
                        f"[ExecutionEngine] [{symbol}] Cuenta {account_id}: Tamaño ajustado a {pos_size} para no exceder "
                        f"límite de {riesgo_por_operacion_pct:.2f}% ({limite_operacion_usd:.2f} USD). Nuevo PNL+Margen: {(margin_used + risk_usd):.2f}"
                    )

            # 5. Prepare Trade Data
            # Update signal with calculated profit for this account
            signal_dict = signal.to_dict()
            signal_dict['profit'] = risk_usd
            signal_dict['is_adjustment'] = is_adjustment

            trade_data = {
                "idTrade": id_trade_to_update,
                "idCuenta": account_id,
                "symbol": symbol,
                "direction": signal.direction,
                "entryPrice": signal.entry_price,
                "stopLoss": signal.stop_loss,
                "takeProfit": signal.take_profit,
                "takeProfit2": signal.take_profit2,
                "takeProfit3": signal.take_profit3,
                "size": pos_size,
                "margin_used": margin_used,
                "intervalo": signal.intervalo,
                "strategy": strategy_name,
                "setup": signal.setup,
                "openTime": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "status": "OPEN",
                "candleTime": signal.candleTime
            }

            # 6. Execute via Gateway
            try:
                success, msg_id = await gateway.execute_trade(
                    trade_data, 
                    signal_dict, 
                    account, 
                    strategy_name, 
                    df=df_context
                )
                
                if success:
                    prefix = "🔄 Ajuste" if is_adjustment else "✅ Ejecución"
                    logger.info(f"{prefix} {strategy_name} para {symbol} en cuenta {account_id} (Msg: {msg_id})")
                    executed_any = True
                else:
                    logger.warning(f"⚠️ Ejecución {strategy_name} para {symbol} en cuenta {account_id}: {msg_id}")
            except Exception as e:
                logger.error(f"Error crítico en ejecución {strategy_name} para {symbol} en cuenta {account_id}: {e}")

        return executed_any
