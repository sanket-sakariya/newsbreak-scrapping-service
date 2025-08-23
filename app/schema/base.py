from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class BaseSchema(BaseModel):
    """Base schema with common fields"""
    class Config:
        from_attributes = True
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }

class ResponseSchema(BaseModel):
    """Base response schema"""
    success: bool
    data: Optional[dict] = None
    message: Optional[str] = None
    error: Optional[str] = None
