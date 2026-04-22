import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
from Sentinel.core.models import Signal
from Sentinel.analysis import risk
from middleware.database import dbManager
from middleware.execution.broker_gateway import gateway

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

        # 2. Check if EXACT trade is already open (Same Symbol, Same Strategy, Same Setup)
        try:
            # Obtenemos todos los trades abiertos para este símbolo
            open_trades = dbManager.getOpenTradesBySymbol(symbol)
            for ot in open_trades:
                if ot['strategy'] == strategy_name and ot.get('setup') == signal.setup:
                    logger.info(f"[ExecutionEngine] [{symbol}] {strategy_name} ({signal.setup}) ya abierto - omitiendo.")
                    return False
        except Exception as e:
            logger.error(f"[ExecutionEngine] Error verificando trades abiertos para {symbol}: {e}")

        # 2. Refresh accounts to ensure we have latest balances/status
        self._refresh_accounts()
        if not self.accounts:
            return False

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

            # 4. Calculate Risk and Position Size (Apply risk_factor)
            try:
                base_risk = float(account['ganancia'])
                effective_risk = base_risk * signal.risk_factor
                
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
                logger.warning(f"[ExecutionEngine] [{symbol}] Size=0 para cuenta {account_id} - riesgo ${risk_usd:.2f} insuficiente o margen excedido.")
                continue

            # 5. Prepare Trade Data
            # Update signal with calculated profit for this account
            signal_dict = signal.to_dict()
            signal_dict['profit'] = risk_usd

            trade_data = {
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
                "status": "OPEN"
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
                    logger.info(f"✅ [ExecutionEngine] Señal {strategy_name} ejecutada para {symbol} en cuenta {account_id} (Msg: {msg_id})")
                    executed_any = True
                else:
                    logger.error(f"❌ [ExecutionEngine] Falló ejecución para {symbol} en cuenta {account_id}: {msg_id}")
            except Exception as e:
                logger.error(f"[ExecutionEngine] Error crítico en ejecución para cuenta {account_id}: {e}")

        return executed_any
