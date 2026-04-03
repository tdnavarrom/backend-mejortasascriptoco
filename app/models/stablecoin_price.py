from typing import Optional

from sqlmodel import Field, SQLModel


class StablecoinPrice(SQLModel, table=True):
    __tablename__ = "stablecoin_prices"

    exchange: str = Field(primary_key=True)
    coin: str = Field(primary_key=True)
    buy_cop: float
    sell_cop: float
    buy_usd: float = 0
    sell_usd: float = 0
    spread: float = 0
    direct_cop: bool = True
    usd_bridge: str = ""
    last_updated: Optional[str] = None
