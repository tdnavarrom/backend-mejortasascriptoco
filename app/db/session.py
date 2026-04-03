from sqlalchemy import text
from sqlmodel import Session, SQLModel, create_engine

from app import models  # noqa: F401
from app.core.config import settings

engine = create_engine(settings.database_url, pool_pre_ping=True)


def get_session():
    with Session(engine) as session:
        yield session


def create_db_and_tables():
    SQLModel.metadata.create_all(engine)
    with engine.begin() as connection:
        connection.execute(text("ALTER TABLE crypto_prices ADD COLUMN IF NOT EXISTS last_updated VARCHAR"))
        connection.execute(text("ALTER TABLE stablecoin_prices ADD COLUMN IF NOT EXISTS last_updated VARCHAR"))
