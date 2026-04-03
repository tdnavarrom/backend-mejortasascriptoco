import os
from typing import Optional

from dotenv import load_dotenv

load_dotenv()


class Settings:
    db_user: Optional[str] = os.getenv("DB_USER")
    db_password: Optional[str] = os.getenv("DB_PASSWORD")
    db_host: Optional[str] = os.getenv("DB_HOST")
    db_port: str = os.getenv("DB_PORT", "5432")
    db_name: str = os.getenv("DB_NAME", "postgres")

    admin_user: str = os.getenv("ADMIN_USER", "m4cc1")
    admin_pass: str = os.getenv("ADMIN_PASS", "TDNMunera_06*")
    admin_token: str = os.getenv("ADMIN_TOKEN", "crypto_spread_secret_token_2026")

    crypto_coins = ["btc", "bch", "eth", "sol", "ltc"]
    stable_coins = ["usdt", "usdc", "euroc"]

    @property
    def database_url(self) -> str:
        return f"postgresql://{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"


settings = Settings()
