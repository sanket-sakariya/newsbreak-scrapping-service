from pydantic import BaseModel, HttpUrl
from typing import List, Optional
from .base import BaseSchema

class StartScrapingRequest(BaseModel):
    initial_urls: List[HttpUrl]
    batch_size: int = 100
    worker_count: int = 5

class StopScrapingRequest(BaseModel):
    job_id: Optional[str] = None
    force: bool = False

class AddUrlsRequest(BaseModel):
    urls: List[HttpUrl]

class NewsbreakDataSchema(BaseModel):
    id: str
    likeCount: int
    commentCount: int
    source_id: int
    city_id: int
    title: str
    origin_url: str
    share_count: int
    first_text_category_id: int
    second_text_category_id: int
    third_text_category_id: int
    first_text_category_value: float
    second_text_category_value: float
    third_text_category_value: float
    nf_entities_id: int
    nf_entities_value: float
    nf_tags_id: int
    is_active: bool
    status: str
    created_at: str
    updated_at: str

class NewsbreakUrlSchema(BaseModel):
    url: str
    created_at: Optional[str] = None
    last_scraped: Optional[str] = None
    scrape_count: Optional[int] = 0

class ScrapingStatusSchema(BaseModel):
    status: str
    statistics: dict
