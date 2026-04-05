"""
app/i18n.py — Minimal backend localization.

Language is detected from the standard Accept-Language HTTP header.
The frontend sends this header automatically via the browser, and our
custom Admin fetch calls send it explicitly using the user's chosen lang.

Usage:
    from app.i18n import get_lang, t

    lang = get_lang(request)
    raise HTTPException(status_code=401, detail=t("unauthorized", lang))
"""

from fastapi import Request

# Translation key constants — import these instead of using raw strings.
INVALID_CREDENTIALS = "invalid_credentials"
UNAUTHORIZED = "unauthorized"
PLATFORM_NOT_FOUND = "platform_not_found"
REFERRAL_NOT_CONFIGURED = "referral_not_configured"

TRANSLATIONS: dict[str, dict[str, str]] = {
    "es": {
        "invalid_credentials": "Credenciales incorrectas",
        "unauthorized": "No autorizado.",
        "platform_not_found": "Plataforma no encontrada",
        "referral_not_configured": "Enlace de referido no configurado",
    },
    "en": {
        "invalid_credentials": "Invalid credentials",
        "unauthorized": "Unauthorized.",
        "platform_not_found": "Platform not found",
        "referral_not_configured": "Referral link not configured",
    },
}


def get_lang(request: Request) -> str:
    """
    Resolve the preferred language from the request.

    Priority:
      1. Accept-Language header value of exactly 'en' or 'es'
      2. First language tag in a full Accept-Language header (e.g. 'en-US,en;q=0.9')
      3. Default: 'es'
    """
    header = request.headers.get("Accept-Language", "")
    if not header:
        return "es"

    # Walk through comma-separated language tags, pick first recognised one
    for part in header.split(","):
        tag = part.strip().split(";")[0].strip().split("-")[0].lower()
        if tag in TRANSLATIONS:
            return tag

    return "es"


def t(key: str, lang: str) -> str:
    """Return the translation for *key* in *lang*, falling back to Spanish."""
    return TRANSLATIONS.get(lang, TRANSLATIONS["es"]).get(key, key)
