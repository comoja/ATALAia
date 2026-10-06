from typing import List, Optional
from pydantic import BaseModel, Field

class NewsEventItem(BaseModel):
    id: Optional[int] = None
    fechaHora: str = Field(..., description="Timestamp del evento economico en formato YYYY-MM-DD HH:mm:ss")
    currency: str
    impact: str = Field("HIGH", description="HIGH, MEDIUM, LOW")
    title: str
    forecast: Optional[str] = None
    previous: Optional[str] = None
    actual: Optional[str] = None
    summary: Optional[str] = None
    source: str = "CalendarioEconomico"

class RatioNewsResponse(BaseModel):
    status: str = "success"
    idRatio: Optional[int] = Field(None, description="ID del ratio")
    pairA: str
    pairB: str
    currencies: List[str]
    totalEvents: int
    events: List[NewsEventItem]
