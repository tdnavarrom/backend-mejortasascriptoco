from fastapi import APIRouter

from app.api.routes import auth, config, cron, health, platforms, prices

api_router = APIRouter(prefix="/api")
api_router.include_router(auth.router, tags=["auth"])
api_router.include_router(config.router, tags=["config"])
api_router.include_router(cron.router, tags=["cron"])
api_router.include_router(health.router, tags=["health"])
api_router.include_router(platforms.router, tags=["platforms"])
api_router.include_router(prices.router, tags=["prices"])
