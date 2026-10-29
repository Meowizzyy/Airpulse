from datetime import datetime
from pydantic import BaseModel, Field
from uuid import UUID

class AirQualityObservation(BaseModel):
    city: str = Field(min_length=1)
    
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    observed_at: datetime
    pm2_5: float = Field(ge=0)
    pm10: float = Field(ge=0)
    carbon_monoxide: float = Field(ge=0)
    nitrogen_dioxide: float = Field(ge=0)
    sulphur_dioxide: float = Field(ge=0)
    ozone: float = Field(ge=0)
    european_aqi: int = Field(ge=0) 


class AirQualityEvent(BaseModel):
    event_id : UUID 
    event_type : str = Field(min_length=1)
    event_version : int = Field(ge=1)
    source : str = Field(min_length=3)
    ingested_at : datetime
    payload : AirQualityObservation


