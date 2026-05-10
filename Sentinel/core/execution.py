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
        self.refreshAccounts()

    def refreshAccounts(self):
        """Fetch active accounts from the database."""
        try:
            self.accounts = dbManager.getAccount()
            if not self.accounts:
                logger.warning("[ExecutionEngine] No se encontraron cuentas activas en la BD.")
        except Exception as e:
            logger.error(f"[ExecutionEngine] Error al obtener cuentas: {e}")

    async def processSignals(self, signals: List[Signal]):
        """
        Processes a list of signals sequentially.
        """
        for signal in signals:
            await self.processSignal(signal)

    async def processSignal(self, signal: Signal, symbolInfo: Optional[Dict[str, Any]] = None, dfContext: Optional[Any] = None) -> bool:
        """
        Processes a signal: validates, calculates risk per account, and executes.
        """
        if not signal:
            return False

        symbol = signal.symbol
        strategyName = signal.strategy

        # 1. Ensure we have symbolInfo
        if symbolInfo is None:
            symbolInfo = dbManager.getSymbol(symbol)
            if not symbolInfo:
                logger.error(f"[ExecutionEngine] No se encontró información para el símbolo {symbol}. Omitiendo señal.")
                return False

        # 2. Refresh accounts to ensure we have latest balances/status
        self.refreshAccounts()
        if not self.accounts:
            return False

        # Obtenemos todos los trades abiertos para este símbolo una vez
        try:
            allOpenTrades = dbManager.getOpenTradesBySymbol(symbol)
        except Exception as e:
            allOpenTrades = []
            logger.error(f"[ExecutionEngine] Error verificando trades abiertos para {symbol}: {e}")

        executedAny = False
        
        # 3. Iterate over accounts
        for account in self.accounts:
            accountId = account['idCuenta']
            
            # Exclude master account (ID 1: SENTINEL) from signals if needed
            if accountId == 1:
                continue

            if not dbManager.isEstrategiaHabilitadaParaCuenta(accountId, strategyName):
                logger.debug(f"[ExecutionEngine] Estrategia {strategyName} no habilitada para cuenta {accountId}")
                continue

            # Check if this symbol type is allowed for this account
            symbolTipo = symbolInfo.get('tipo', 'FOREX')
            if not dbManager.isTipoHabilitadoParaCuenta(accountId, symbolTipo):
                logger.debug(f"[ExecutionEngine] Tipo {symbolTipo} no habilitado para cuenta {accountId}")
                continue

            # --- NUEVA LÓGICA DE AJUSTE ---
            # Buscar si ESTA cuenta tiene un trade abierto (sin cerrar) para este setup
            existingTrade = next((t for t in allOpenTrades if t['idCuenta'] == accountId and t['strategy'] == strategyName and t.get('setup') == signal.setup and t.get('closeTime') is None), None)
            
            isAdjustment = False
            idTradeToUpdate = None
            if existingTrade:
                diffTp = abs(existingTrade['takeProfit'] - signal.take_profit) / signal.take_profit > 0.0001
                diffSl = abs(existingTrade['stopLoss'] - signal.stop_loss) / signal.stop_loss > 0.0001
                
                if diffTp or diffSl:
                    isAdjustment = True
                    idTradeToUpdate = existingTrade['idTrade']
                    logger.info(f"[ExecutionEngine] [{symbol}] AJUSTE detectado para cuenta {accountId}")
                else:
                    logger.debug(f"[ExecutionEngine] [{symbol}] Setup ya abierto en cuenta {accountId} - omitiendo.")
                    continue

            # 4. Calculate Risk and Position Size (Apply risk_factor)
            try:
                baseRisk = float(account['ganancia'])
                effectiveRisk = baseRisk * signal.risk_factor
                
                # Aplicar Cap de Riesgo Máximo
                if effectiveRisk > config.MAX_RISK_PER_TRADE:
                    logger.info(f"[ExecutionEngine] [{symbol}] Riesgo {effectiveRisk}% excedía el máximo. Ajustado a {config.MAX_RISK_PER_TRADE}%")
                    effectiveRisk = config.MAX_RISK_PER_TRADE
                
                posSize, riskUsd, marginUsed = risk.calculatePositionSize(
                    capital=float(account['Capital']),
                    riskPercentage=effectiveRisk,
                    slDistance=signal.sl_distance,
                    symbolInfo=symbolInfo,
                    entryPrice=signal.entry_price
                )
            except Exception as e:
                logger.error(f"[ExecutionEngine] Error calculando riesgo para cuenta {accountId}: {e}")
                continue

            if posSize is None or posSize == 0:
                riskStr = f"${riskUsd:.2f}" if riskUsd else "None"
                logger.warning(f"[ExecutionEngine] [{symbol}] Size=0 para cuenta {accountId} - riesgo {riskStr} insuficiente o margen excedido.")
                continue

            # --- NUEVA VALIDACIÓN Y AJUSTE: Riesgo Por Operación ---
            riesgoPorOperacionPct = float(account.get('riesgoPorOperacion', 1.0)) # Default 1.0% si no existe
            limiteOperacionUsd = float(account['Capital']) * (riesgoPorOperacionPct / 100.0)
            
            if (marginUsed + riskUsd) > limiteOperacionUsd:
                totalCurrent = marginUsed + riskUsd
                if totalCurrent > 0:
                    reductionFactor = limiteOperacionUsd / totalCurrent
                    newSizeRaw = posSize * reductionFactor
                    
                    symbolType = symbolInfo.get('tipo', 'FOREX').upper()
                    symbolMinLots = symbolInfo.get('min_lots')
                    minUnitsDict = {
                        "METALES": 1,
                        "INDICE": 1,
                        "CRYPTO": 0.01,
                        "MONEDA": 1000,
                        "EXOTIC": 1000
                    }
                    if symbolMinLots is not None:
                        minLots = float(symbolMinLots)
                    else:
                        minLots = float(minUnitsDict.get(symbolType, 1000 if symbolType not in minUnitsDict else 1))
                    
                    originalPosSize = posSize
                    posSize = math.floor(newSizeRaw / minLots) * minLots
                    
                    if symbolType in ['MONEDA', 'EXOTIC']:
                        posSize = int(posSize)
                    
                    if posSize < minLots:
                        logger.warning(
                            f"[ExecutionEngine] [{symbol}] Cuenta {accountId}: Tras reducir por límite de riesgo (máx {limiteOperacionUsd:.2f} USD), "
                            f"el tamaño ({newSizeRaw}) es menor al lote mínimo ({minLots}). Se omite señal."
                        )
                        continue
                    
                    # Recalcular margin y risk proporcionales al nuevo tamaño
                    marginUsed = marginUsed * (posSize / originalPosSize)
                    riskUsd = riskUsd * (posSize / originalPosSize)
                    
                    logger.info(
                        f"[ExecutionEngine] [{symbol}] Cuenta {accountId}: Tamaño ajustado a {posSize} para no exceder "
                        f"límite de {riesgoPorOperacionPct:.2f}% ({limiteOperacionUsd:.2f} USD). Nuevo PNL+Margen: {(marginUsed + riskUsd):.2f}"
                    )

            # 5. Prepare Trade Data
            # Update signal with calculated profit for this account
            signalDict = signal.to_dict()
            signalDict['profit'] = riskUsd
            signalDict['is_adjustment'] = isAdjustment

            tradeData = {
                "idTrade": idTradeToUpdate,
                "idCuenta": accountId,
                "symbol": symbol,
                "direction": signal.direction,
                "entryPrice": signal.entry_price,
                "stopLoss": signal.stop_loss,
                "takeProfit": signal.take_profit,
                "takeProfit2": signal.take_profit2,
                "takeProfit3": signal.take_profit3,
                "size": posSize,
                "margin_used": marginUsed,
                "intervalo": signal.intervalo,
                "strategy": strategyName,
                "setup": signal.setup,
                "openTime": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                "status": "OPEN",
                "candleTime": signal.candleTime
            }

            # 6. Execute via Gateway
            try:
                success, msgId = await gateway.execute_trade(
                    tradeData, 
                    signalDict, 
                    account, 
                    strategyName, 
                    df=dfContext
                )
                
                if success:
                    prefix = "🔄 Ajuste" if isAdjustment else "✅ Ejecución"
                    logger.info(f"{prefix} {strategyName} para {symbol} en cuenta {accountId} (Msg: {msgId})")
                    executedAny = True
                else:
                    logger.warning(f"⚠️ Ejecución {strategyName} para {symbol} en cuenta {accountId}: {msgId}")
            except Exception as e:
                logger.error(f"Error crítico en ejecución {strategyName} para {symbol} en cuenta {accountId}: {e}")

        return executedAny
