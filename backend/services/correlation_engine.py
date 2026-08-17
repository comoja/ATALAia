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
        df[col_name] = df['log_return'].rolling(window=window, min_periods=1).std() * np.sqrt(self.TRADING_DAYS_YEAR)
        return df

    def calculate_moving_average(self, df: pd.DataFrame, price_col: str, window: int = 20) -> pd.DataFrame:
        """
        Equivalente a Excel: =AVERAGE(AI5:AI24) (Strike AVG)
        """
        col_name = f'sma_{window}'
        df[col_name] = df[price_col].rolling(window=window, min_periods=1).mean()
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

    def process_pair(self, df: pd.DataFrame, amplitude: float = 1.0, freq: float = 0.1, phase: float = 0.0, offset: float = 0.0, r: float = 0.05, tYears: float = 30 / 252, sigmaWindow: int = 7, smaPeriod: int = 20, emaSlowPeriod: int = 50) -> dict:
        """
        Función principal que orquesta todos los cálculos equivalentes a la hoja 'EURGBPUSD'
        utilizando parámetros dinámicos y fórmulas financieras precisas.
        """
        logger.info("Iniciando procesamiento de Par Computacional con calibración en RAM")
        df = df.copy()
        
        if len(df) < 20: 
            return {"error": "No hay suficientes datos. Min: 20", "success": False}
            
        # Asegurar orden cronológico ascendente
        df = df.sort_index()

        df = self.calculate_log_returns(df, 'price')
        df = self.calculate_volatility(df, 7)
        df = self.calculate_volatility(df, 60) # Equivalente a 2M
        df = self.calculate_moving_average(df, 'price', smaPeriod)
        df = self.calculate_moving_average(df, 'price', emaSlowPeriod)
        
        sma_fast_col = f'sma_{smaPeriod}'
        sma_slow_col = f'sma_{emaSlowPeriod}'
        
        # Eliminar NaNs generados por los shifts y rollings
        df = df.dropna()
        
        if df.empty:
            return {"error": "Datos insuficientes tras aplicar ventanas móviles.", "success": False}

        # Generar el ciclo senoidal utilizando el precio como input
        df['indexSeq'] = np.arange(1, len(df) + 1)
        df = self.compute_cycles(df, 'price', amplitude, freq, phase, offset)

        # Calcular las primas teóricas de opciones Call y Put de Black-Scholes para cada día
        volCol = f'vol_{sigmaWindow}D'
        if volCol not in df.columns:
            volCol = 'vol_7D' # Fallback seguro
            
        df['bsCall'] = df.apply(lambda row: self.black_scholes(row['price'], row[sma_fast_col], tYears, r, row[volCol], "call"), axis=1)
        df['bsPut']  = df.apply(lambda row: self.black_scholes(row['price'], row[sma_fast_col], tYears, r, row[volCol], "put"), axis=1)

        # Si existen ambos pares (close_a y close_b), calcular iMACD sobre la Media de Precios Normalizados [0, 1]
        if 'close_a' in df.columns and 'close_b' in df.columns:
            min_a, max_a = df['close_a'].min(), df['close_a'].max()
            min_b, max_b = df['close_b'].min(), df['close_b'].max()
            range_a = (max_a - min_a) if (max_a - min_a) != 0 else 1.0
            range_b = (max_b - min_b) if (max_b - min_b) != 0 else 1.0
            norm_a = (df['close_a'] - min_a) / range_a
            norm_b = (df['close_b'] - min_b) / range_b
            price_target = (norm_a + norm_b) / 2.0
        else:
            price_target = df['price']

        # Calcular iMACD (12, 26, 9) sobre la serie objetivo (Media de Precios Normalizados)
        ema_12 = price_target.ewm(span=12, adjust=False).mean()
        ema_26 = price_target.ewm(span=26, adjust=False).mean()
        df['macd_line'] = ema_12 - ema_26
        df['macd_signal'] = df['macd_line'].ewm(span=9, adjust=False).mean()
        df['macd_hist'] = df['macd_line'] - df['macd_signal']

        # Retorno de ultimos cálculos y el historial completo para visualización
        latestRow = df.iloc[-1]
        
        latestData = {
            "currentPrice": float(latestRow['price']),
            "logReturnLatest": float(latestRow['log_return']),
            "vol7DAnnualized": float(latestRow['vol_7D']),
            "vol60DAnnualized": float(latestRow['vol_60D']),
            "strikeAvg20": float(latestRow[sma_fast_col]),
            "smaSlow": float(latestRow[sma_slow_col]),
            "cicloStLatest": float(latestRow['ciclo_st']),
            "bsCallLatest": float(latestRow['bsCall']),
            "bsPutLatest": float(latestRow['bsPut']),
            "macdLineLatest": float(latestRow['macd_line']),
            "macdSignalLatest": float(latestRow['macd_signal']),
            "macdHistLatest": float(latestRow['macd_hist'])
        }

        # Estructurar historial completo formateado para gráficas en PrimeFaces
        historyData = []
        for idx, row in df.iterrows():
            item = {
                "datetime": str(idx),
                "price": float(row['price']),
                "vol7D": float(row['vol_7D']),
                "vol60D": float(row['vol_60D']),
                "sma20": float(row[sma_fast_col]),
                "smaSlow": float(row[sma_slow_col]),
                "cicloSt": float(row['ciclo_st']),
                "bsCall": float(row['bsCall']),
                "bsPut": float(row['bsPut']),
                "macdLine": float(row['macd_line']),
                "macdSignal": float(row['macd_signal']),
                "macdHist": float(row['macd_hist'])
            }
            if 'close_a' in row:
                item["priceA"] = float(row['close_a'])
            if 'close_b' in row:
                item["priceB"] = float(row['close_b'])
            historyData.append(item)

        return {
            "success": True,
            "latest": latestData,
            "history": historyData
        }

    def compute_ratio_series(self, df_a: pd.DataFrame, df_b: pd.DataFrame) -> pd.DataFrame:
        """
        Calcula el precio sintético (ratio) dividiendo el Par A entre el Par B.
        Equivalente a la hoja 'EURGBPUSD' del Excel:
            precio_sintético = close_EURUSD / close_GBPUSD  → genera EUR/GBP sintético

        Args:
            df_a: DataFrame del Par Numerador con columna 'close' y DatetimeIndex
            df_b: DataFrame del Par Denominador con columna 'close' y DatetimeIndex
        Returns:
            DataFrame con columna 'price' = close_a / close_b, alineado por fecha
        """
        # Renombrar para merge sin ambigüedad
        df_a = df_a[['close']].rename(columns={'close': 'close_a'})
        df_b = df_b[['close']].rename(columns={'close': 'close_b'})

        # Inner join por fecha (solo días con datos en AMBOS pares)
        merged = df_a.join(df_b, how='inner')

        if merged.empty:
            raise ValueError("No hay fechas en común entre los dos pares seleccionados.")

        merged['price'] = merged['close_a'] / merged['close_b']

        return merged[['price', 'close_a', 'close_b']]

    def process_two_pairs(
        self,
        df_a: pd.DataFrame,
        df_b: pd.DataFrame,
        amplitude: float = 1.0,
        freq: float = 0.1,
        phase: float = 0.0,
        offset: float = 0.0,
        r: float = 0.05,
        tYears: float = 30 / 252,
        sigmaWindow: int = 7,
        smaPeriod: int = 20,
        emaSlowPeriod: int = 50
    ) -> dict:
        """
        Flujo completo de correlación cruzada de dos pares.
        1. Calcula el ratio sintético (Par A / Par B)
        2. Aplica todos los indicadores del CorrelationEngine sobre ese ratio
        Esto es el equivalente Python de la hoja EURGBPUSD del Excel.

        Args:
            df_a: DataFrame del Par Numerador (ej. EUR/USD) con columna 'close'
            df_b: DataFrame del Par Denominador (ej. GBP/USD) con columna 'close'
            ... parámetros de calibración idénticos a process_pair()
        """
        logger.info("Iniciando correlación cruzada de 2 pares (ratio sintético).")
        try:
            df_ratio = self.compute_ratio_series(df_a, df_b)
        except ValueError as e:
            return {"error": str(e), "success": False}

        # Reutilizamos process_pair() sobre el ratio
        return self.process_pair(
            df=df_ratio,
            amplitude=amplitude,
            freq=freq,
            phase=phase,
            offset=offset,
            r=r,
            tYears=tYears,
            sigmaWindow=sigmaWindow,
            smaPeriod=smaPeriod,
            emaSlowPeriod=emaSlowPeriod
        )


# Instancia global (singleton) para usos de API
engine = CorrelationEngine()

