from pydantic import BaseModel
from typing import Optional

class LogQueryParams(BaseModel):
    level: Optional[str] = None
    service: Optional[str] = None
    start_time: Optional[str] = None
    end_time: Optional[str] = None

# class SearchQuery(BaseModel):
#     query: str
#     top_k: int = 5
