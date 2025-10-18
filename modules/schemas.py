from pydantic import BaseModel
from typing import Optional

class StravaActivity(BaseModel):
    id: int
    name: Optional[str] = None
    athlete_id: Optional[int] = None
    type: Optional[str] = None
    created_at: Optional[str] = None # passing straight to db in it's orginal form "2025-10-18T12:00:00Z" to avoid any python datetime oddities
    distance: Optional[float] = None
    duration_seconds: Optional[int] = None
    elevation_high: Optional[float] = None
    elevation_low: Optional[float] = None
    avg_speed: Optional[float] = None
    max_speed: Optional[float] = None
    average_heartrate: Optional[float] = None
    max_heartrate: Optional[float] = None
    calories_burned: Optional[float] = None