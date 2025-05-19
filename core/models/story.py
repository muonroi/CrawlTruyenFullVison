from typing import Optional, List

from pydantic import BaseModel

class Genre(BaseModel):
    name: str
    url: str

class Story(BaseModel):
    title: str
    url: str
    author: Optional[str] = None
    categories: List[Genre] = []
    total_chapters_on_site: Optional[int] = None
    cover: Optional[str] = None
    description: Optional[str] = None
    status: Optional[str] = None

class Chapter(BaseModel):
    title: str
    url: str