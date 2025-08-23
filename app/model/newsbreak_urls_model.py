from sqlalchemy import Column, Text, DateTime, Integer
from .base import BaseModel

class UrlsModel(BaseModel):
    __tablename__ = "urls"
    
    url = Column(Text, primary_key=True)
    last_scraped = Column(DateTime, nullable=True)
    scrape_count = Column(Integer, default=0)
