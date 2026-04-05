from __future__ import annotations

from datetime import datetime
from typing import Optional

from sqlmodel import Field, SQLModel


class PlatformReferralClick(SQLModel, table=True):
    __tablename__ = "platform_referral_click"

    id: Optional[int] = Field(default=None, primary_key=True)
    platform_id: str = Field(index=True)
    destination_url: str
    referral_code: str = ""
    user_agent: str = ""
    created_at: datetime = Field(default_factory=datetime.utcnow, index=True)
