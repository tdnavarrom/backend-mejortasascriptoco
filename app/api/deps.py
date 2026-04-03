from fastapi import Header, HTTPException

from app.core.config import settings


def verify_admin(token: str = Header(None)):
    if token != settings.admin_token:
        raise HTTPException(status_code=401, detail="No autorizado.")
