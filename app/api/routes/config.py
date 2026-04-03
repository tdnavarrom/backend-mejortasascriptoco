from fastapi import APIRouter

from app.core.config import settings

router = APIRouter()


@router.get("/config")
def get_config():
    return {"crypto": settings.crypto_coins, "stablecoins": settings.stable_coins}
