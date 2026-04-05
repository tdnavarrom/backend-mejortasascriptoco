from fastapi import APIRouter, HTTPException, Request

from app.core.config import settings
from app.i18n import INVALID_CREDENTIALS, get_lang, t
from app.schemas.auth import LoginRequest

router = APIRouter()


@router.post("/login")
def login(req: LoginRequest, request: Request):
    if req.username == settings.admin_user and req.password == settings.admin_pass:
        return {"token": settings.admin_token}
    lang = get_lang(request)
    raise HTTPException(status_code=401, detail=t(INVALID_CREDENTIALS, lang))
