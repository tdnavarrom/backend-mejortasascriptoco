from typing import Dict

from pydantic import BaseModel


class PlatformUpdate(BaseModel):
    id: str
    name: str
    category: str
    logo_url: str
    funding: str
    trading: str
    withdraw: str
    deposit_networks: str
    withdraw_networks: str
    manual_prices: Dict
    is_manual: bool
    is_active: bool
