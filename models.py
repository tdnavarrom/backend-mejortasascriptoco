from sqlalchemy import Column, Integer, String, Float, Boolean
from .database import Base

class PrecioCripto(Base):
    __tablename__ = "precios"

    id = Column(Integer, primary_key=True, index=True)
    exchange = Column(String) # Postgres requiere especificar que es String
    coin = Column(String)
    buy_price = Column(Float)
    sell_price = Column(Float)