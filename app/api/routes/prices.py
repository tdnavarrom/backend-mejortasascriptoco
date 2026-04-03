from fastapi import APIRouter, Depends
from sqlmodel import Session

from app.db.session import get_session
from app.schemas.price import PriceResponse
from app.services.pricing import build_price_response

router = APIRouter()


@router.get("/prices/{coin}", response_model=PriceResponse)
def get_prices(coin: str, session: Session = Depends(get_session)):
    return build_price_response(session, coin)
