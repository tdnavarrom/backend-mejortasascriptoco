from typing import Any

from pydantic import BaseModel


class PriceResponse(BaseModel):
    coin: str
    prices: list[dict[str, Any]]
