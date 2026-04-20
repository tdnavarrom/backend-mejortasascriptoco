"""
seed_platforms.py — Inserts the initial platform_info rows into the DB.

Run once (or any time you need to reset platform data):
  .venv/bin/python3 seed_platforms.py

Uses session.merge() so it's safe to run multiple times — existing rows are
updated rather than duplicated.
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from app.db.session import create_db_and_tables, engine
from app.models import PlatformInfo
from sqlmodel import Session

EXCHANGE_COINS = ("btc", "bch", "eth", "sol", "ltc", "usdt", "usdc", "euroc")
ND_PRICE = "N.D. - no tienen APIs abiertas"
WENIA_LOGO_LIGHT = (
    "data:image/svg+xml;utf8,"
    "<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 142 143' fill='none'>"
    "<path fill='%23030370' d='M131.335 0H120.236C115.034 0 110.583 3.78422 109.73 8.9657L96.8247 86.9789C96.2467 90.4284 93.2841 92.9609 89.8158 92.9609H78.2691H61.91H52.1553C48.687 92.9609 45.7244 90.4284 45.1608 86.9789L38.1663 44.7121C37.2992 39.5451 32.8626 35.7464 27.6601 35.7464H10.6507C4.76898 35.7609 0 40.564 0 46.4732V82.2195C0 88.1433 4.76898 92.9464 10.6507 92.9464H40.1317C43.6 92.9464 46.5626 95.4789 47.1406 98.9138L52.9501 134.034C53.8172 139.201 58.2538 143 63.4563 143H78.5437C83.7462 143 88.1828 139.216 89.0499 134.034L94.8594 98.9138C95.423 95.4643 98.4 92.9464 101.868 92.9464H131.349C137.231 92.9464 142 88.1433 142 82.2195V10.7414C141.986 4.80305 137.217 0 131.335 0Z'/>"
    "</svg>"
)
WENIA_LOGO_DARK = WENIA_LOGO_LIGHT.replace("%23030370", "%23FFFFFF")


def build_nd_manual_prices(coins, currency="COP"):
    return json.dumps(
        {
            coin: {
                "active": True,
                "buy": ND_PRICE,
                "sell": ND_PRICE,
                "currency": currency,
            }
            for coin in coins
        }
    )


PLATFORMS = [
    # ── Exchanges (automatic pricing via fetcher) ──────────────────────────
    PlatformInfo(
        id="binance",
        name="Binance",
        category="exchange",
        logo_url="https://bin.bnbstatic.com/static/images/common/favicon.ico",
        logo_dark_url="https://bin.bnbstatic.com/static/images/common/favicon.ico",
        funding="P2P en COP: gratis en Binance",
        trading="Spot: 0,10% maker / 0,10% taker; 0,075% con BNB",
        withdraw="P2P: gratis; retiro cripto: variable por red",
        funding_en="P2P in COP: free on Binance",
        trading_en="Spot: 0.10% maker / 0.10% taker; 0.075% with BNB",
        withdraw_en="P2P: free; crypto withdrawal: variable by network",
        website_url="https://www.binance.com/",
        referral_url="https://www.binance.com/referral/earn-together/refer2earn-usdc/claim?hl=en&ref=GRO_28502_I1B4G&utm_source=referral_entrance",
        referral_code="GRO_28502_I1B4G",
        cta_label="Referido",
        deposit_networks="",
        withdraw_networks="",
        deposit_networks_en="",
        withdraw_networks_en="",
        manual_prices=json.dumps({}),
        is_manual=False,
        is_active=True,
    ),
    PlatformInfo(
        id="bitso",
        name="Bitso",
        category="exchange",
        logo_url="/logos/bitso-wordmark.svg",
        logo_dark_url="/logos/bitso-wordmark.svg",
        funding="Recarga en COP vía Bancolombia o Nequi",
        trading="Maker/taker variable por volumen; conversión con precio final visible",
        withdraw="Retiro bancario a cuenta propia",
        funding_en="COP deposit via Bancolombia or Nequi",
        trading_en="Variable maker/taker by volume; conversion with visible final price",
        withdraw_en="Bank withdrawal to own account",
        deposit_networks="",
        withdraw_networks="",
        deposit_networks_en="",
        withdraw_networks_en="",
        manual_prices=json.dumps({}),
        is_manual=False,
        is_active=True,
    ),
    PlatformInfo(
        id="buda",
        name="Buda",
        category="exchange",
        logo_url="https://static.cdnlogo.com/logos/b/26/buda.svg",
        logo_dark_url="https://blog.buda.com/content/images/2025/04/buda-logo-white-1.svg",
        funding="Depósito por PSE: $1.297 COP",
        trading="Buda Pro: maker/taker variable",
        withdraw="Retiro bancario en COP: $1.740 COP + 0,1%",
        funding_en="PSE deposit: $1,297 COP",
        trading_en="Buda Pro: variable maker/taker",
        withdraw_en="Bank withdrawal in COP: $1,740 COP + 0.1%",
        deposit_networks="",
        withdraw_networks="",
        deposit_networks_en="",
        withdraw_networks_en="",
        manual_prices=json.dumps({}),
        is_manual=False,
        is_active=True,
    ),
    PlatformInfo(
        id="cryptomkt",
        name="CryptoMKT",
        category="exchange",
        logo_url="https://www.cryptomkt.com/static/global/assets/images/cmkt-blue-footer.png",
        logo_dark_url="https://www.cryptomkt.com/static/global/assets/images/cmkt-blanc.png",
        funding="Depósito en COP vía SafetyPay/PSE: 2% a 3%",
        trading="Simple: precio final visible; Pro: variable según mercado",
        withdraw="Retiro bancario disponible; costo no publicado",
        funding_en="COP deposit via SafetyPay/PSE: 2%–3%",
        trading_en="Simple: visible final price; Pro: variable by market",
        withdraw_en="Bank withdrawal available; fee not published",
        deposit_networks="",
        withdraw_networks="",
        deposit_networks_en="",
        withdraw_networks_en="",
        manual_prices=json.dumps({}),
        is_manual=False,
        is_active=True,
    ),
    PlatformInfo(
        id="lulox",
        name="LuloX",
        category="exchange",
        logo_url="https://tawk.link/65295746eb150b3fb9a11be6/kb/logo/4WfzNB9oM3.png",
        logo_dark_url="https://tawk.link/65295746eb150b3fb9a11be6/kb/logo/4WfzNB9oM3.png",
        funding="Depósito: N.D. en sitio oficial",
        trading="Trading: N.D. en sitio oficial",
        withdraw="Retiro: N.D. en sitio oficial",
        funding_en="Deposit: N/A on official site",
        trading_en="Trading: N/A on official site",
        withdraw_en="Withdrawal: N/A on official site",
        deposit_networks="",
        withdraw_networks="",
        deposit_networks_en="",
        withdraw_networks_en="",
        manual_prices=build_nd_manual_prices(EXCHANGE_COINS),
        is_manual=True,
        is_active=True,
    ),
    PlatformInfo(
        id="wenia",
        name="Wenia",
        category="exchange",
        logo_url=WENIA_LOGO_LIGHT,
        logo_dark_url=WENIA_LOGO_DARK,
        funding="Depósito con Bancolombia, Nequi o tarjeta",
        trading="Conversión cripto desde 0,1%",
        withdraw="Retiro a Bancolombia/Nequi; envío cripto a wallets Wenia o externas",
        funding_en="Deposit via Bancolombia, Nequi or card",
        trading_en="Crypto conversion from 0.1%",
        withdraw_en="Withdrawal to Bancolombia/Nequi; crypto send to Wenia or external wallets",
        deposit_networks="",
        withdraw_networks="",
        deposit_networks_en="",
        withdraw_networks_en="",
        manual_prices=build_nd_manual_prices(EXCHANGE_COINS),
        is_manual=True,
        is_active=True,
    ),
    # ── Fintechs (automatic pricing via fetcher) ───────────────────────────
    PlatformInfo(
        id="global66",
        name="Global66",
        category="fintech",
        logo_url="https://www.global66.com/blog/wp-content/uploads/2022/03/logo_desktop.svg",
        logo_dark_url="https://www.global66.com/blog/wp-content/uploads/2022/03/logo_desktop.svg",
        funding="Depósito ACH: gratis; Wire/SWIFT: 10 USD; PSE: gratis",
        trading="Cambio con tasa transparente; comisión fija no publicada",
        withdraw="Transferencia internacional: variable según destino",
        funding_en="ACH deposit: free; Wire/SWIFT: 10 USD; PSE: free",
        trading_en="Exchange at transparent rate; fixed fee not disclosed",
        withdraw_en="International transfer: variable by destination",
        deposit_networks="Red propia",
        withdraw_networks="TRC-20, ERC-20",
        deposit_networks_en="Own network",
        withdraw_networks_en="TRC-20, ERC-20",
        manual_prices=json.dumps({}),
        is_manual=False,
        is_active=True,
    ),
    PlatformInfo(
        id="plenti",
        name="Plenti",
        category="fintech",
        logo_url="https://cdn.prod.website-files.com/6697e29d92e2b75be213df4c/669a8987dc5b503c4b7694d7_Logo-main.svg",
        logo_dark_url="https://cdn.prod.website-files.com/6697e29d92e2b75be213df4c/669a8987bfcc824265f6195c_Logo-white.svg",
        funding="Depósito desde ACH/Wire: desde 2,5 USD; PSE disponible",
        trading="Cambio con tasa competitiva; fee fijo no público",
        withdraw="Transferencia a brokers/plataformas: 2,99 USD",
        funding_en="ACH/Wire deposit: from $2.50 USD; PSE available",
        trading_en="Exchange at competitive rate; fixed fee not public",
        withdraw_en="Transfer to brokers/platforms: $2.99 USD",
        deposit_networks="TRC-20",
        withdraw_networks="TRC-20",
        deposit_networks_en="TRC-20",
        withdraw_networks_en="TRC-20",
        manual_prices=json.dumps({}),
        is_manual=False,
        is_active=True,
    ),
    PlatformInfo(
        id="dolarapp",
        name="ArqFinance",
        category="fintech",
        logo_url="https://www.arqfinance.com/favicon.svg",
        logo_dark_url="https://www.arqfinance.com/_astro/logo.D534Oqrz_Z1xxVCU.svg",
        funding="Depósito por PSE: gratis; USD/EUR vía cuenta global: 3 USD/EUR",
        trading="Cambio desde pesos: sin comisión",
        withdraw="Transferencia USD/EUR: 3 USD/EUR; retiro local en COP: gratis",
        funding_en="PSE deposit: free; USD/EUR via global account: $3 USD/EUR",
        trading_en="Exchange from pesos: no fee",
        withdraw_en="USD/EUR transfer: $3 USD/EUR; local COP withdrawal: free",
        deposit_networks="TRC-20, ERC-20",
        withdraw_networks="TRC-20, ERC-20",
        deposit_networks_en="TRC-20, ERC-20",
        withdraw_networks_en="TRC-20, ERC-20",
        manual_prices=json.dumps({}),
        is_manual=False,
        is_active=True,
    ),
    PlatformInfo(
        id="littio",
        name="Littio",
        category="fintech",
        logo_url="https://framerusercontent.com/images/mqmwc7ucueZ7kZOWhBS56Wb0vo.png",
        logo_dark_url="https://framerusercontent.com/images/mqmwc7ucueZ7kZOWhBS56Wb0vo.png",
        funding="Depósito desde USA/Europa: 2,99 o 0,6%; PSE en EUR/COP: gratis",
        trading="Cambio entre cuentas: sin comisión",
        withdraw="Transferencia a Europa: 2,99 EUR o 0,6%; retiro en Colombia en EUR/COP: gratis",
        funding_en="Deposit from USA/Europe: 2.99 or 0.6%; PSE in EUR/COP: free",
        trading_en="Exchange between accounts: no fee",
        withdraw_en="Transfer to Europe: 2.99 EUR or 0.6%; Colombia withdrawal in EUR/COP: free",
        website_url="https://apps.apple.com/co/app/littio-tu-cuenta-en-usdc/id1613160324",
        referral_url="https://apps.apple.com/co/app/littio-tu-cuenta-en-usdc/id1613160324",
        referral_code="X58X",
        cta_label="Referido",
        deposit_networks="N.D. - no tienen APIs abiertas",
        withdraw_networks="N.D. - no tienen APIs abiertas",
        deposit_networks_en="N/A - no public APIs",
        withdraw_networks_en="N/A - no public APIs",
        manual_prices=json.dumps(
            {
                "usd": {
                    "active": True,
                    "buy": ND_PRICE,
                    "sell": ND_PRICE,
                    "currency": "USD",
                },
                "eur": {
                    "active": True,
                    "buy": ND_PRICE,
                    "sell": ND_PRICE,
                    "currency": "EUR",
                },
            }
        ),
        is_manual=True,
        is_active=True,
    ),
]


def seed():
    create_db_and_tables()
    with Session(engine) as session:
        for platform in PLATFORMS:
            session.merge(platform)  # insert or update — safe to run multiple times
        session.commit()
    print(f"✅ Seeded {len(PLATFORMS)} platforms into platform_info.")


if __name__ == "__main__":
    seed()
