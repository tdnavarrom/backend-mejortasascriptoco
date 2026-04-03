from typing import Optional

from sqlalchemy import Column, Text
from sqlmodel import Field, SQLModel


class PlatformInfo(SQLModel, table=True):
    __tablename__ = "platform_info"

    id: str = Field(primary_key=True, index=True)
    name: str
    category: str
    logo_url: str
    funding: str
    trading: str
    withdraw: str
    deposit_networks: str
    withdraw_networks: str
    manual_prices: str = Field(sa_column=Column(Text))
    is_manual: bool
    is_active: bool
    last_updated: Optional[str] = None
