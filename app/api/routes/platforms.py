import datetime
import json

from fastapi import APIRouter, Depends
from sqlmodel import Session, select

from app.api.deps import verify_admin
from app.db.session import get_session
from app.models import PlatformInfo
from app.schemas.platform import PlatformUpdate

router = APIRouter()


@router.get("/platforms")
def get_platforms(all: bool = False, session: Session = Depends(get_session)):
    statement = select(PlatformInfo) if all else select(PlatformInfo).where(PlatformInfo.is_active == True)
    result = session.exec(statement)

    platforms = {}
    for platform in result:
        item = platform.model_dump()
        if isinstance(item["manual_prices"], str):
            item["manual_prices"] = json.loads(item["manual_prices"])
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
        deposit_networks=data.deposit_networks,
        withdraw_networks=data.withdraw_networks,
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
