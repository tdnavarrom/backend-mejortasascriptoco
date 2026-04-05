from __future__ import annotations

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
    website_url: str = ""
    referral_url: str = ""
    referral_code: str = ""
    cta_label: str = ""
    deposit_networks: str
    withdraw_networks: str
    funding_en: str = ""
    trading_en: str = ""
    withdraw_en: str = ""
    deposit_networks_en: str = ""
    withdraw_networks_en: str = ""
    manual_prices: str = Field(sa_column=Column(Text))
    is_manual: bool
    is_active: bool
    last_updated: Optional[str] = None
