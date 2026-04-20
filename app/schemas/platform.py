from typing import Dict

from pydantic import BaseModel


class PlatformUpdate(BaseModel):
    id: str
    name: str
    category: str
    logo_url: str
    logo_dark_url: str = ""
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
    manual_prices: Dict
    is_manual: bool
    is_active: bool
