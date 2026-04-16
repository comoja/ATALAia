import logging
import pandas as pd
import pytz
from datetime import datetime
from typing import Dict, List, Any
from zoneinfo import ZoneInfo

import os
import sys

# Path setup
rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from middleware.database import dbManager
from Sentinel.analysis import technical, risk
from middleware.execution.broker_gateway import gateway
from middleware.config.constants import TIMEZONE
from dataSymbol.mainOrchestrator import get_last_closed_candle

logger = logging.getLogger(__name__)

class GenericFVGBot:
    """
    Bot especializado en la detección de FVGs (Fair Value Gaps) en múltiples temporalidades
    de forma continua durante todo el día.
    """

    def __init__(self):
        self.intervals = ['15min', '1h', '4h']
        self._sent_signals = {}
        self.accounts = None
        self.strategy_name = "GenericFVG"
        
        logger.info(f"GenericFVGBot iniciado para intervalos: {self.intervals}")
       

    def getMexicoTime(self) -> datetime:
        return datetime.now(pytz.timezone(TIMEZONE))

    async def analyze(self, symbolInfo: Dict, df5m: pd.DataFrame):
        """Analiza un símbolo en todas las temporalidades configuradas."""
        logger.info(f"Analizando {symbolInfo['symbol']} en intervalos {self.intervals}")
        
        symbol = symbolInfo['symbol']
        
        if df5m is None or len(df5m) < 20:
            return

        for interval in self.intervals:
            latest_fvg = technical.detect_fvg_closed(
                df_source=df5m,
                interval=interval,
                min_gap_pct=0.0005,
                min_adx=20
            )
            
            if not latest_fvg:
                continue
            
            # Control de duplicados usando el timestamp del FVG
            signal_key = f"{symbol}_{interval}_{latest_fvg['timestamp']}"
            if signal_key in self._sent_signals:
                continue
            
            # Resamplear para obtener datos de precio (necesario para SL/TP)
            df = technical.resample_to_interval(df5m, interval)
            if len(df) < 3:
                continue
            
            # El idx del FVG es respecto al df resampleado
            # Ajustar indices basados en el df resampleado
            last_closed_idx = len(df) - 2
            if latest_fvg['idx'] > last_closed_idx:
                continue
            
            v1_idx = latest_fvg['idx'] - 1
            if v1_idx < 0: 
                continue
            
            if v1_idx >= len(df):
                continue
            
            vela1 = df.iloc[v1_idx]
            structural = technical.get_structural_levels(df, lookback=30)
            
            entry_price = float(df['close'].iloc[-2])
            sl = 0
            tp1 = 0
            
            if latest_fvg['type'] == 'Bullish_FVG':
                sl = float(vela1['low'])
                tp1 = structural['swing_high']
            else:
                sl = float(vela1['high'])
                tp1 = structural['swing_low']
            
            risk_dist = abs(entry_price - sl)
            if risk_dist == 0: 
                continue
            
            if latest_fvg['type'] == 'Bullish_FVG':
                if tp1 <= entry_price: tp1 = entry_price + (risk_dist * 1.5)
            else:
                if tp1 >= entry_price: tp1 = entry_price - (risk_dist * 1.5)

            min_lots = float(symbolInfo.get('min_lots', 1.0))

            tp2 = entry_price + (risk_dist * 2) if latest_fvg['type'] == 'Bullish_FVG' else entry_price - (risk_dist * 2)

            fvg_mid = float(latest_fvg['mid'])
            total_path = abs(tp1 - fvg_mid)
            if total_path == 0: total_path = 0.001
            
            if latest_fvg['type'] == 'Bullish_FVG':
                progress_pct = (entry_price - fvg_mid) / total_path
            else:
                progress_pct = (fvg_mid - entry_price) / total_path
                
            if progress_pct >= 1.0:
                status_msg = "META ALCANZADA 🚨"
            elif progress_pct > 0.5:
                status_msg = "ALEJÁNDOSE ⚠️"
            else:
                status_msg = "EN ZONA ✅"

            # Preparar Dicc de Señal para Gateway
            signal_data = {
                "strategy": self.strategy_name,
                "symbol": symbol,
                "direction": "LARGO" if latest_fvg['type'] == 'Bullish_FVG' else "CORTO",
                "entryPrice": entry_price,
                "stopLoss": sl,
                "takeProfit": tp1,
                "slDistance": risk_dist,
                "riesgo_pips": round(risk_dist * technical.get_pip_multiplier(symbol), 1),
                "rr_ratio": round(abs(tp1 - entry_price) / risk_dist, 2),
                "fvg": latest_fvg['type'],
                "candle_time": latest_fvg['timestamp'],
                "confidence": 85,
                "setup": f"FVG {interval}",
                "status": status_msg
            }

            # Preparar Dicc de Trade para Gateway/DB
            trade_data = {
                "symbol": symbol,
                "direction": signal_data['direction'],
                "entryPrice": entry_price,
                "stopLoss": sl,
                "takeProfit": tp1,
                "intervalo": interval,
                "strategy": self.strategy_name,
                "size": min_lots
            }

            # Ejecutar vía Gateway (DB + Telegram + Broker) para cada cuenta válida
            if not self.accounts:
                self.accounts = dbManager.getAccount()
            
            for account in self.accounts:
                # Excluir cuenta maestra de señales (SENTINEL)
                if account['idCuenta'] == 1: 
                    continue
                
                posSize, riskUsd, marginUsed = risk.calculatePositionSize(
                    capital=float(account['Capital']),
                    riskPercentage=float(account['ganancia']),
                    slDistance=risk_dist,
                    symbolInfo=symbolInfo,
                    entryPrice=entry_price
                )
                
                if posSize is None or posSize == 0:
                    logger.warning(f"[GenericFVG] Size=0 para {symbol} - riesgo ${riskUsd:.2f} < $5 mínimo")
                    continue
                
                signal_data['profit'] = riskUsd
                trade_data['idCuenta'] = account['idCuenta']
                trade_data['size'] = posSize
                trade_data['margin_used'] = marginUsed

                success, msg_id = await gateway.execute_trade(trade_data, signal_data, account, self.strategy_name, df=df)
                if success:
                    self._sent_signals[signal_key] = True
                    logger.info(f"✅ Señal estandarizada enviada para {symbol} [{interval}] a cuenta {account['idCuenta']}")

