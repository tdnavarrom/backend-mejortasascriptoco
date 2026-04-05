import logging

from fastapi import APIRouter, Header, HTTPException, Request, status
from fastapi.responses import PlainTextResponse

from app.core.config import settings
from app.fetcher import run_fetcher

router = APIRouter()
logger = logging.getLogger(__name__)


def get_request_ip(request: Request, x_forwarded_for: str | None) -> str:
    if x_forwarded_for:
        forwarded_ips = [ip.strip() for ip in x_forwarded_for.split(",") if ip.strip()]
        if forwarded_ips:
            return forwarded_ips[0]
    if request.client and request.client.host:
        return request.client.host
    return "unknown"


@router.get("/cron/fetcher")
async def run_fetcher_from_cron(
    request: Request,
    authorization: str | None = Header(default=None),
    x_forwarded_for: str | None = Header(default=None),
):
    expected_secret = settings.cron_secret
    request_ip = get_request_ip(request, x_forwarded_for)

    if not expected_secret:
        logger.error("Cron fetcher rejected: CRON_SECRET is not configured.")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="CRON_SECRET is not configured.",
        )

    if authorization != f"Bearer {expected_secret}":
        logger.warning("Cron fetcher rejected: invalid authorization from ip=%s", request_ip)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
        )

    allowed_ips = settings.cron_allowed_ips
    if allowed_ips and request_ip not in allowed_ips:
        logger.warning(
            "Cron fetcher rejected: ip=%s not in CRON_ALLOWED_IPS",
            request_ip,
        )
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Forbidden",
        )

    logger.info("Cron fetcher started from ip=%s", request_ip)
    try:
        await run_fetcher()
    except Exception:
        logger.exception("Cron fetcher failed from ip=%s", request_ip)
        raise

    logger.info("Cron fetcher completed successfully from ip=%s", request_ip)
    return PlainTextResponse("ok")
