"""
Estrategia FVG Diario - Manipulación de Liquidez + Daily Bias

Esta estrategia identifica escenarios de alta probabilidad siguiendo:
1. Daily Bias: Identificar tendencia diaria (alcista/bajista)
2. Manipulación: Detectar cuando el precio supera PDH/PDL
3. Confirmación: FVG en 15m/1h tras el rompimiento
4. Entry: Retest del FVG formado

Autor: Sentinel Trading System
"""
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any

from middleware.database import dbManager
from middleware.execution.broker_gateway import gateway
from Sentinel.analysis import technical, risk
from Sentinel.data.dataLoader import getParametros
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)


class FVGDiarioBot:
    """
    Estrategia de trading basada en manipulación de liquidez del día anterior y continuación del Daily Bias.
    """
    
    def __init__(self, config: Dict = None):
        self.strategy_name = "FVGDiario"
        
        # Configuración por defecto
        self.config = {
            # Temporalidades a analizar
            "timeframes": ["15min", "1H", "4H", "1D"],
            
            # Parámetros de Daily Bias
            "daily_bullish_threshold": 0.0,  # close > open = bullish
            "min_adx_filter": 20.0,  # Filtrar mercados laterales
            
            # Parámetros de manipulación
            "manipulation_sessions": ["asian", "london", "ny"],  # Sesiones a monitorear
            
            # Parámetros de FVG
            "min_fvg_pips": 5,  # Mínimo 5 pips para FVG válido
            
            # Gestión de riesgo
            "min_rr_ratio": 2.0,  # Mínimo 1:2 R:R
            "max_rr_ratio": 4.0,  # Objetivo 1:4
            
            # Niveles a visualizar
            "show_pdh_pdl": True,
            
            # Alertas
            "alert_on_manipulation": True,
            "alert_on_fvg": True,
        }
        
        if config:
            self.config.update(config)
        
        self.accounts = []
        self._sent_signals = {}
        
        logger.info(f"🤖 {self.strategy_name} iniciado")
    
    async def start(self):
        """Inicia el análisis de la estrategia."""
        self.accounts = dbManager.getAccount()
        
        if not self.accounts:
            logger.warning(f"[{self.strategy_name}] No hay cuentas activas")
            return
        
        symbols = dbManager.getSymbols()
        
        for symbol_data in symbols:
            if not symbol_data.get('Activo', 1) == 1:
                continue
            
            await self.analyze_symbol(symbol_data)
    
    async def analyze_symbol(self, symbolData: Dict):
        """Analiza un símbolo en busca de señales."""
        symbol = symbolData['symbol']
        
        try:
            # Obtener datos diarios para Daily Bias
            df_daily = self._get_daily_data(symbol)
            if df_daily is None or len(df_daily) < 2:
                return
            
            # Identificar Daily Bias
            daily_bias = self._get_daily_bias(df_daily)
            
            # Obtener PDH y PDL del día anterior
            pdh, pdl = self._get_pdh_pdl(df_daily)
            
            # Obtener nivel de liquidez opuesto (para TP)
            opposite_liquidity = pdl if daily_bias == "BULLISH" else pdh
            
            # Detectar manipulación en datos intradía
            df_intraday = self._get_intraday_data(symbol)
            if df_intraday is None or len(df_intraday) < 20:
                return
            
            # Buscar manipulación
            manipulation = self._detect_manipulation(
                df_intraday, pdh, pdl, daily_bias
            )
            
            if not manipulation:
                return
            
            # Verificar cambio de estructura tras manipulación
            if not self._check_market_structure_shift(
                df_intraday, manipulation, daily_bias
            ):
                return
            
            # Buscar FVG tras manipulación
            fvg = self._find_fvg_after_manipulation(
                df_intraday, manipulation, daily_bias
            )
            
            if not fvg:
                return
            
            # Generar señal
            await self._generate_signal(
                symbolData, daily_bias, pdh, pdl,
                manipulation, fvg, opposite_liquidity
            )
            
        except Exception as e:
            logger.error(f"[{self.strategy_name}] Error análisis {symbol}: {e}")
    
    def _get_daily_bias(self, df_daily: pd.DataFrame) -> str:
        """
        Determina el Daily Bias basándose en el día anterior completo.
        
        Returns:
            "BULLISH" si close > open (vela alcista)
            "BEARISH" si close < open (vela bajista)
            "NEUTRAL" si no hay diferencia clara
        """
        if df_daily is None or len(df_daily) < 2:
            return "NEUTRAL"
        
        # Usar el día ANTERIOR completo (iloc[-2]), no el actual
        last_candle = df_daily.iloc[-2]
        close = last_candle['close']
        open_price = last_candle['open']
        
        # Calcular cuerpo de la vela
        body = close - open_price
        total_range = last_candle['high'] - last_candle['low']
        
        if total_range == 0:
            return "NEUTRAL"
        
        # Considerar bullish si el cuerpo es más del 50% del rango
        if body / total_range >= 0.5 and body > 0:
            return "BULLISH"
        elif body / total_range >= 0.5 and body < 0:
            return "BEARISH"
        
        return "NEUTRAL"
    
    def _get_pdh_pdl(self, df_daily: pd.DataFrame) -> tuple:
        """
        Obtiene el Previous Day High (PDH) y Previous Day Low (PDL).
        
        Returns:
            (pdh, pdl) - Valores del día anterior
        """
        if df_daily is None or len(df_daily) < 2:
            return None, None
        
        # Día anterior (índice -2 porque el último es el día actual)
        prev_day = df_daily.iloc[-2]
        
        pdh = float(prev_day['high'])
        pdl = float(prev_day['low'])
        
        return pdh, pdl
    
    def _detect_manipulation(
        self,
        df: pd.DataFrame,
        pdh: float,
        pdl: float,
        daily_bias: str
    ) -> Optional[Dict]:
        """
        Detecta manipulación del nivel diario.
        
        La manipulación ocurre cuando el precio excede brevemente el PDH/PDL
        y luego se reversa.
        
        Returns:
            Dict con info de manipulación o None
        """
        if df is None or len(df) < 10:
            return None
        
        recent = df.tail(10)
        
        if daily_bias == "BEARISH":
            # Buscar manipulación en PDH (buscar liquidez de venta)
            # El precio debe haber superado el PDH brevemente
            for i in range(len(recent) - 1, -1, -1):
                if recent.iloc[i]['high'] >= pdh:
                    # Limpiar el stop de liquidez
                    return {
                        "type": "MANIPULATION_UP",
                        "level": pdh,
                        "index": i,
                        "timestamp": str(recent.index[i]),
                        "price": float(recent.iloc[i]['high'])
                    }
        
        elif daily_bias == "BULLISH":
            # Buscar manipulación en PDL (buscar liquidez de compra)
            for i in range(len(recent) - 1, -1, -1):
                if recent.iloc[i]['low'] <= pdl:
                    return {
                        "type": "MANIPULATION_DOWN",
                        "level": pdl,
                        "index": i,
                        "timestamp": str(recent.index[i]),
                        "price": float(recent.iloc[i]['low'])
                    }
        
        return None
    
    def _check_market_structure_shift(
        self,
        df: pd.DataFrame,
        manipulation: Dict,
        daily_bias: str
    ) -> bool:
        """
        Verifica si hay un Market Structure Shift (MSS) tras la manipulación.
        
        Un MSS ocurre cuando:
        - Para bearish: el precio hace lower low tras manipular PDH
        - Para bullish: el precio hace higher high tras manipular PDL
        
        Returns:
            True si hay MSS
        """
        if df is None or manipulation is None:
            return False
        
        manip_idx = manipulation['index']
        
        # Necesitamos al menos 2 velas después de la manipulación
        if manip_idx + 2 >= len(df):
            return False
        
        post_manip = df.iloc[manip_idx + 1:]
        
        if len(post_manip) < 2:
            return False
        
        if daily_bias == "BEARISH":
            # Buscar lower low (precio rompe bajo tras manipular)
            recent_lows = post_manip['low'].values
            for low in recent_lows:
                if low < manipulation['level']:
                    return True
        
        elif daily_bias == "BULLISH":
            # Buscar higher high (precio rompe arriba tras manipular)
            recent_highs = post_manip['high'].values
            for high in recent_highs:
                if high > manipulation['level']:
                    return True
        
        return False
    
    def _find_fvg_after_manipulation(
        self,
        df: pd.DataFrame,
        manipulation: Dict,
        daily_bias: str
    ) -> Optional[Dict]:
        """
        Busca un Fair Value Gap Formación después de la manipulación.
        
        El FVG debe formarse tras el rompimiento de la estructura.
        Usa detect_fvg_closed para solo usar velas terminadas.
        
        Returns:
            Dict con info del FVG o None
        """
        if df is None or manipulation is None:
            return None
        
        manip_idx = manipulation['index']
        
        start_idx = max(0, manip_idx + 2)
        end_idx = min(len(df), start_idx + 5)
        
        if end_idx - start_idx < 3:
            return None
        
        df_after = df.iloc[start_idx:end_idx]
        
        latest_fvg = technical.detect_fvg_closed(
            df_source=df_after,
            interval='15min',
            min_gap_pct=0.0005,
            min_adx=20,
            lookback=10
        )
        
        if not latest_fvg:
            return None
        
        if daily_bias == "BULLISH" and latest_fvg.get("type") == "Bullish_FVG":
            return latest_fvg
        elif daily_bias == "BEARISH" and latest_fvg.get("type") == "Bearish_FVG":
            return latest_fvg
        
        return None
    
    async def _generate_signal(
        self,
        symbolData: Dict,
        daily_bias: str,
        pdh: float,
        pdl: float,
        manipulation: Dict,
        fvg: Dict,
        opposite_liquidity: float
    ):
        """Genera y envía la señal a las cuentas."""
        symbol = symbolData['symbol']
        
        # Verificar si la señal ya fue enviada
        signal_key = f"{symbol}_{fvg['timestamp']}"
        if signal_key in self._sent_signals:
            return
        
        # Calcular niveles de entrada, SL y TP
        entry_price = float(fvg['mid'])
        
        if daily_bias == "BULLISH":
            # Entrada en el FVG bullish, SL debajo
            stop_loss = float(fvg['bottom'])
            take_profit = entry_price + (entry_price - stop_loss) * self.config['min_rr_ratio']
            direction = "LARGO"
        else:
            # Entrada en el FVG bearish, SL encima
            stop_loss = float(fvg['top'])
            take_profit = entry_price - (stop_loss - entry_price) * self.config['min_rr_ratio']
            direction = "CORTO"
        
        # Calcular distancia de SL
        sl_distance = abs(entry_price - stop_loss)
        rr_ratio = round(
            abs(take_profit - entry_price) / sl_distance, 2
        )
        
        # Preparar señal
        signal = {
            "strategy": self.strategy_name,
            "symbol": symbol,
            "direction": direction,
            "entryPrice": entry_price,
            "stopLoss": stop_loss,
            "takeProfit": take_profit,
            "slDistance": sl_distance,
            "riesgo_pips": round(
                sl_distance * technical.get_pip_multiplier(symbol), 1
            ),
            "rr_ratio": rr_ratio,
            "daily_bias": daily_bias,
            "pdh": pdh,
            "pdl": pdl,
            "manipulation_type": manipulation['type'],
            "fvg_type": fvg['type'],
            "opposite_liquidity": opposite_liquidity,
            "confidence": 75,
            "setup": "FVG Diario + Manipulación",
            "status": "EN ZONA ✅"
        }
        
        # Preparar trade
        trade = {
            "symbol": symbol,
            "direction": direction,
            "entryPrice": entry_price,
            "stopLoss": stop_loss,
            "takeProfit": take_profit,
            "intervalo": "15min",
            "strategy": self.strategy_name,
            "size": 1.0,  # Se calculará por cuenta
            "openTime": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Enviar a cada cuenta
        if not self.accounts:
            self.accounts = dbManager.getAccount()
        
        for account in self.accounts:
            # Excluir cuenta maestra
            if account['idCuenta'] == 1:
                continue
            
            # Verificar estrategia habilitada
            if not dbManager.isEstrategiaHabilitadaParaCuenta(
                account['idCuenta'], self.strategy_name
            ):
                continue
            
            # Calcular tamaño de posición
            posSize, riskUsd, marginUsed = risk.calculatePositionSize(
                capital=float(account['Capital']),
                riskPercentage=float(account['ganancia']),
                slDistance=sl_distance,
                symbolInfo=symbolData,
                entryPrice=entry_price
            )
            
            if posSize is None or posSize == 0:
                logger.warning(
                    f"[{self.strategy_name}] Size=0 para {symbol} - "
                    f"riesgo ${riskUsd:.2f} < $5 mínimo"
                )
                continue
            
            signal['profit'] = riskUsd
            
            trade['idCuenta'] = account['idCuenta']
            trade['size'] = posSize
            trade['margin_used'] = marginUsed
            
            # Enviar vía gateway
            success, msgId = await gateway.execute_trade(
                trade, signal, account, self.strategy_name
            )
            
            if success and msgId:
                self._sent_signals[signal_key] = True
                logger.info(
                    f"✅ {self.strategy_name} enviado para {symbol} "
                    f"[{direction}] cuenta {account['idCuenta']}"
                )
    
    def _get_daily_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Obtiene datos diarios del símbolo."""
        try:
            tz = 'America/Mexico_City'
            df = getParametros(symbol, '1D', 5, tz)
            if df is not None and len(df) > 0:
                df = df.tail(5)
            return df
        except Exception as e:
            logger.error(f"Error obteniendo datos diarios para {symbol}: {e}")
            return None
    
    def _get_intraday_data(self, symbol: str) -> Optional[pd.DataFrame]:
        """Obtiene datos intradía (15min) del símbolo."""
        try:
            from middleware.config.constants import TIMEZONE
            tz = TIMEZONE
            df = getParametros(symbol, '15min', 50, tz)
            if df is not None and len(df) > 0:
                df = df.tail(50)
            return df
        except Exception as e:
            logger.error(f"Error obteniendo datos 15min para {symbol}: {e}")
            return None
    
    def get_mexico_time(self) -> datetime:
        """Obtiene la hora actual en timezone México."""
        from datetime import datetime, timezone
        import pytz
        
        mexico_tz = pytz.timezone('America/Mexico_City')
        return datetime.now(mexico_tz)


async def main():
    """Función principal para ejecutar la estrategia."""
    strategy = FVGDiarioBot()
    await strategy.start()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())