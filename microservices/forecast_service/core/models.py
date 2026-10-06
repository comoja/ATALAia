from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field

class CandlePoint(BaseModel):
    datetime: Optional[str] = None
    timestamp: Optional[str] = None
    priceA: Optional[float] = None
    priceB: Optional[float] = None

    class Config:
        extra = "allow"

class ForecastRequest(BaseModel):
    idRatio: Optional[int] = Field(None, description="Identificador único del ratio para trazabilidad humana")
    pairA: str = Field(..., description="Símbolo del numerador / Par A")
    pairB: str = Field(..., description="Símbolo del denominador / Par B")
    timeframe: str = Field("1h", description="Temporalidad de análisis (5min, 15min, 1h, 4h, 1d)")
    smaPeriod: int = Field(2, description="Periodo de suavizado EMA RIDE")
    fixedMinA: Optional[float] = Field(None, description="Mínimo de calibración para Par A")
    fixedMaxA: Optional[float] = Field(None, description="Máximo de calibración para Par A")
    fixedMinB: Optional[float] = Field(None, description="Mínimo de calibración para Par B")
    fixedMaxB: Optional[float] = Field(None, description="Máximo de calibración para Par B")
    history: Optional[List[Dict[str, Any]]] = Field(None, description="Historial de precios opcional para cálculo directo en memoria")

    class Config:
        extra = "allow"

class ProjectedPoint(BaseModel):
    timestamp: str
    projectedNormA: float
    projectedNormB: float
    projectedSpread: float
    projectedZScore: float
    isCrossPoint: bool = False

class ForecastResponse(BaseModel):
    status: str = "success"
    idRatio: Optional[int] = Field(None, description="ID del ratio evaluado")
    pairA: str
    pairB: str
    timeframe: str
    currentTimestamp: str
    currentNormA: float
    currentNormB: float
    currentSpread: float
    currentZScore: float
    targetEquilibrium: float
    halfLifeBars: float
    halfLifeHours: float
    estimatedBarsToCross: int
    estimatedCrossTime: str
    estimatedCrossEquilibrium: float
    direction: str
    confidencePct: float
    model: str = "Ornstein-Uhlenbeck + Angular Momentum"
    projectedPoints: List[ProjectedPoint] = []
