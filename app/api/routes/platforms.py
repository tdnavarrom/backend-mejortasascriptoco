import datetime
import json

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse
from sqlmodel import Session, select

from app.api.deps import verify_admin
from app.db.session import get_session
from app.i18n import PLATFORM_NOT_FOUND, REFERRAL_NOT_CONFIGURED, get_lang, t
from app.models import PlatformInfo, PlatformReferralClick
from app.schemas.platform import PlatformUpdate

router = APIRouter()


_LOCALIZABLE_FIELDS = ("funding", "trading", "withdraw", "deposit_networks", "withdraw_networks")


@router.get("/platforms")
def get_platforms(request: Request, all: bool = False, session: Session = Depends(get_session)):
    statement = select(PlatformInfo) if all else select(PlatformInfo).where(PlatformInfo.is_active == True)
    result = session.exec(statement)
    lang = get_lang(request)

    platforms = {}
    for platform in result:
        item = platform.model_dump()
        if isinstance(item["manual_prices"], str):
            item["manual_prices"] = json.loads(item["manual_prices"])

        if lang == "en":
            for field in _LOCALIZABLE_FIELDS:
                en_val = (item.get(f"{field}_en") or "").strip()
                if en_val:
                    item[field] = en_val

        platforms[item["id"]] = item
    return platforms


@router.post("/admin/platforms", dependencies=[Depends(verify_admin)])
def save_platform(data: PlatformUpdate, session: Session = Depends(get_session)):
    platform = PlatformInfo(
        id=data.id.lower(),
        name=data.name,
        category=data.category,
        logo_url=data.logo_url,
        funding=data.funding,
        trading=data.trading,
        withdraw=data.withdraw,
        website_url=data.website_url,
        referral_url=data.referral_url,
        referral_code=data.referral_code,
        cta_label=data.cta_label,
        deposit_networks=data.deposit_networks,
        withdraw_networks=data.withdraw_networks,
        funding_en=data.funding_en,
        trading_en=data.trading_en,
        withdraw_en=data.withdraw_en,
        deposit_networks_en=data.deposit_networks_en,
        withdraw_networks_en=data.withdraw_networks_en,
        manual_prices=json.dumps(data.manual_prices),
        is_manual=data.is_manual,
        is_active=data.is_active,
        last_updated=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    )
    session.merge(platform)
    session.commit()
    return {"status": "success"}


@router.delete("/admin/platforms/{platform_id}", dependencies=[Depends(verify_admin)])
def delete_platform(platform_id: str, session: Session = Depends(get_session)):
    platform = session.get(PlatformInfo, platform_id.lower())
    if platform:
        session.delete(platform)
        session.commit()
    return {"status": "success"}


@router.get("/r/{platform_id}")
def redirect_referral(platform_id: str, request: Request, session: Session = Depends(get_session)):
    platform = session.get(PlatformInfo, platform_id.lower())
    lang = get_lang(request)
    if not platform or not platform.is_active:
        raise HTTPException(status_code=404, detail=t(PLATFORM_NOT_FOUND, lang))

    destination_url = platform.referral_url or platform.website_url
    if not destination_url:
        raise HTTPException(status_code=404, detail=t(REFERRAL_NOT_CONFIGURED, lang))

    click = PlatformReferralClick(
        platform_id=platform.id,
        destination_url=destination_url,
        referral_code=platform.referral_code or "",
        user_agent=request.headers.get("user-agent", ""),
    )
    session.add(click)
    session.commit()

    return RedirectResponse(destination_url, status_code=302)
