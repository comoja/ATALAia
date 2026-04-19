import pandas as pd
import numpy as np
from scipy.stats import norm
import logging

logger = logging.getLogger(__name__)

class CorrelationEngine:
    """
    Motor matemático que transforma las fórmulas de 'ATALA IA MODEL.xlsm' a Python.
    """
    
    def __init__(self):
        # Constantes de Días de Operación para anualizar (SQRT(252))
        self.TRADING_DAYS_YEAR = 252

    def calculate_log_returns(self, df: pd.DataFrame, price_col: str = 'price') -> pd.DataFrame:
        """
        Equivalente a Excel: =LN(Z5/Z6)
        """
        # Usamos np.log para sacar el logaritmo natural de la división actual entre el anterior (o con pct_change directo)
        # Como en Excel Z6 es fila anterior cronológicamente (dependiendo del orden), asumimos df está ordenado cronológicamente antiguo->reciente
        df['log_return'] = np.log(df[price_col] / df[price_col].shift(1))
        return df
        
    def calculate_volatility(self, df: pd.DataFrame, window: int = 7) -> pd.DataFrame:
        """
        Equivalente a Excel: ="7D-VOL": =STDEV(AJ5:AJ9)*SQRT(252)
        Calcula volatilidad anualizada en una ventana móvil.
        """
        col_name = f'vol_{window}D'
        df[col_name] = df['log_return'].rolling(window=window).std() * np.sqrt(self.TRADING_DAYS_YEAR)
        return df

    def calculate_moving_average(self, df: pd.DataFrame, price_col: str, window: int = 20) -> pd.DataFrame:
        """
        Equivalente a Excel: =AVERAGE(AI5:AI24) (Strike AVG)
        """
        col_name = f'sma_{window}'
        df[col_name] = df[price_col].rolling(window=window).mean()
        return df

    def compute_cycles(self, df: pd.DataFrame, index_col: str, 
                       amplitude: float, freq: float, phase: float, offset: float) -> pd.DataFrame:
        """
        Equivalente a Excel (CICLO ST): =$U$13*(SIN($U$10*BP5+($U$11)))+($U$12)
        """
        # BP5 representa un índice secuencial (ej. 1, 2, 3...)
        df['ciclo_st'] = amplitude * np.sin(freq * df[index_col] + phase) + offset
        return df

    def black_scholes(self, S, K, T, r, sigma, option_type="call"):
        """
        Macro personalizada en Excel: =+blackscholes($BS$1,AI5,AZ5,BA5,CATALOGO!$C$7,0,$BS$2)
        S: Precio subyacente actual (Spot price)
        K: Precio de ejercicio (Strike price)
        T: Tiempo hasta vencimiento en años
        r: Tasa de interés libre de riesgo
        sigma: Volatilidad
        """
        # Evitar ceros en la raíz de tiempo
        if T <= 0 or sigma <= 0:
            return max(0.0, S - K) if option_type == 'call' else max(0.0, K - S)
            
        d1 = (np.log(S / K) + (r + 0.5 * sigma ** 2) * T) / (sigma * np.sqrt(T))
        d2 = d1 - sigma * np.sqrt(T)
        
        if option_type == "call":
            price = S * norm.cdf(d1) - K * np.exp(-r * T) * norm.cdf(d2)
        else: # put
            price = K * np.exp(-r * T) * norm.cdf(-d2) - S * norm.cdf(-d1)
            
        return price

    def process_pair(self, df: pd.DataFrame) -> dict:
        """
        Función principal que orquesta todos los cálculos equivalentes a la hoja 'EURGBPUSD'.
        """
        logger.info("Iniciando procesamiento de Par Computacional (Pandas)")
        df = df.copy()
        
        if len(df) < 20: 
            return {"error": "No hay suficientes datos. Min: 20"}
            
        df = self.calculate_log_returns(df, 'price')
        df = self.calculate_volatility(df, 7)
        df = self.calculate_volatility(df, 60) # Equivalente a 2M
        df = self.calculate_moving_average(df, 'price', 20)
        
        # Eliminar NaNs generados por los shifts y rollings
        df = df.dropna()
        
        # Retorno de ultimos cálculos para la interfaz
        latest = df.iloc[-1]
        
        return {
            "current_price": float(latest['price']),
            "log_return_latest": float(latest['log_return']),
            "vol_7D_annualized": float(latest['vol_7D']),
            "strike_avg_20": float(latest['sma_20']),
            "success": True
        }

# Instancia global (singleton) para usos de API
engine = CorrelationEngine()
