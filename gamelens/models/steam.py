from typing import Optional

from pydantic import BaseModel, Field


class SteamAppListItem(BaseModel):
    appid: int = Field(..., description="Steam application ID")
    name: str = Field(..., description="Game name")
    last_modified: Optional[int] = Field(None, description="Unix timestamp of last modification")
    price_change_number: Optional[int] = Field(None, description="Price revision number")
