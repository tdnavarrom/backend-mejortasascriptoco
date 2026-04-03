from fastapi import APIRouter, HTTPException

from app.core.config import settings
from app.schemas.auth import LoginRequest

router = APIRouter()


@router.post("/login")
def login(req: LoginRequest):
    if req.username == settings.admin_user and req.password == settings.admin_pass:
        return {"token": settings.admin_token}
    raise HTTPException(status_code=401, detail="Credenciales incorrectas")
