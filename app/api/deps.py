from fastapi import Header, HTTPException, Request

from app.core.config import settings
from app.i18n import UNAUTHORIZED, get_lang, t


def verify_admin(request: Request, token: str = Header(None)):
    if token != settings.admin_token:
        lang = get_lang(request)
        raise HTTPException(status_code=401, detail=t(UNAUTHORIZED, lang))
