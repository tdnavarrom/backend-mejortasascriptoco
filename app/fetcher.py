"""
fetcher.py — Price collection engine for MejorTasaCripto.co

This module is the core of the data pipeline. It runs every 15 minutes via
GitHub Actions and does the following:
  1. Queries 7 crypto/fintech platforms for COP (Colombian Peso) prices.
  2. Converts USD-quoted prices to COP using a "bridge rate" (USDT/COP or USDC/COP).
  3. Saves the results into PostgreSQL via an upsert (insert or update on conflict).

Supported exchanges: Binance, CryptoMKT, Bitso, Buda
Supported fintechs:  Global66, Plenti, DolarApp

Entry point: run_fetcher() → called by `python -m app.fetcher` or GitHub Actions.
"""

import asyncio
import datetime
from dataclasses import asdict, dataclass, replace
from typing import Any, Iterable, Optional

import httpx
from dotenv import load_dotenv
from sqlalchemy.dialects.postgresql import insert
from sqlmodel import Session

from app.core.config import settings
from app.db.session import create_db_and_tables, engine
from app.models import CryptoPrice, StablecoinPrice

load_dotenv()  # Load DB credentials and tokens from .env file

# ── Coin lists ────────────────────────────────────────────────────────────────
# These come from config.py. TARGET_COINS is everything we track.
CRYPTO_COINS = tuple(settings.crypto_coins)   # e.g. ("btc", "eth", "sol", ...)
STABLE_COINS = tuple(settings.stable_coins)   # e.g. ("usdt", "usdc", "euroc")
TARGET_COINS = CRYPTO_COINS + STABLE_COINS
TARGET_COIN_SET = set(TARGET_COINS)            # set for O(1) membership checks

# ── Bridge-rate config ────────────────────────────────────────────────────────
# A "bridge rate" is the USDT/COP (or USDC/COP) exchange rate used to convert
# USD-quoted prices into COP. e.g. if BTC costs 67,000 USDT and 1 USDT = 4,150 COP,
# then BTC costs 67,000 × 4,150 = 278,050,000 COP.
BITSO_BRIDGES = ("usdt", "usdc", "usd")       # stable coins Bitso uses as COP bridge
DEFAULT_BRIDGE_RATE = 3900.0                   # COP per USD fallback if all APIs fail

# ── Special symbol mappings ───────────────────────────────────────────────────
# Some coins are listed under a different ticker on a specific exchange.
# e.g. EUROC (Euro Coin) is tracked as EURUSDT on Binance (no direct EUR pair).
BINANCE_SYMBOLS = {"euroc": "EURUSDT"}

# ── Quote currency priority ───────────────────────────────────────────────────
# When a coin is listed against multiple quote currencies, we pick the best one
# in priority order. COP is always preferred (no conversion needed); then stable coins.
BITSO_QUOTE_PRIORITY    = ("cop", "usdt", "usd", "usds")
BINANCE_QUOTE_PRIORITY  = ("COP", "USDT", "USDC", "FDUSD", "USD", "USD1", "BUSD", "TUSD")
CRYPTOMKT_QUOTE_PRIORITY = ("COP", "USDT", "USDC", "USD")

# All quote currencies that are considered "1 USD ≈ 1 unit", used to derive
# a bridge rate multiplier for COP conversion.
USD_LIKE_QUOTES = {"USDT", "USDC", "FDUSD", "USD", "USD1", "BUSD", "TUSD"}


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass(frozen=True)
class BridgeRate:
    """
    Holds the buy and sell COP price for a stable coin (USDT, USDC, or USD).
    Used as a multiplier to convert USD-quoted asset prices into COP.

    Example: BridgeRate(coin="usdt", buy=4160.0, sell=4140.0)
    - buy=4160 means the exchange charges 4,160 COP to sell you 1 USDT
    - sell=4140 means the exchange pays you 4,140 COP to buy 1 USDT from you
    """
    coin: str
    buy: float
    sell: float


@dataclass(frozen=True)
class PriceRecord:
    """
    One price entry for a (exchange, coin) pair, ready to be saved to the DB.

    Fields:
      exchange     — e.g. "binance", "bitso"
      coin         — e.g. "btc", "usdt"
      buy_cop      — COP price to BUY the coin (what the exchange charges you)
      sell_cop     — COP price to SELL the coin (what the exchange pays you)
      buy_usd      — USD price to buy (0 if unavailable)
      sell_usd     — USD price to sell (0 if unavailable)
      spread       — % difference between buy and sell: ((buy-sell)/buy)*100
      direct_cop   — True if the exchange quoted the coin directly in COP,
                     False if it was quoted in USD and we converted via a bridge
      usd_bridge   — Which stable coin was used as bridge (e.g. "usdt"), empty if direct
      last_updated — UTC timestamp string set in collect_prices()
    """
    exchange: str
    coin: str
    buy_cop: float
    sell_cop: float
    buy_usd: float = 0.0
    sell_usd: float = 0.0
    spread: float = 0.0
    direct_cop: bool = True
    usd_bridge: str = ""
    last_updated: str = ""

    def as_db_dict(self) -> dict[str, Any]:
        """Convert this dataclass to a plain dict for SQL insertion."""
        return asdict(self)


# ── Pure helper functions ─────────────────────────────────────────────────────

def safe_float(value: Any, default: float = 0.0) -> float:
    """
    Safely convert any value to float, returning `default` on failure.
    API responses often return prices as strings ("67000.50") or None.
    """
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def calculate_spread(buy_cop: float, sell_cop: float) -> float:
    """
    Spread = how much the exchange marks up the price, as a percentage.
    Formula: ((buy - sell) / buy) * 100

    Example: buy=4160, sell=4100 → spread = ((4160-4100)/4160)*100 = 1.44%
    A high spread means the exchange takes a bigger cut.
    """
    if buy_cop <= 0:
        return 0.0
    return round(((buy_cop - sell_cop) / buy_cop) * 100, 2)


def build_price_record(
    *,
    exchange: str,
    coin: str,
    buy_cop: float,
    sell_cop: float,
    buy_usd: float = 0.0,
    sell_usd: float = 0.0,
    direct_cop: bool = True,
    usd_bridge: str = "",
    last_updated: str = "",
) -> PriceRecord:
    """
    Normalize and assemble a PriceRecord.
    - Lowercases exchange and coin names for consistency.
    - Rounds prices to 8 decimal places (standard crypto precision).
    - Automatically calculates the spread.
    All arguments must be passed as keyword-only (the * enforces this).
    """
    normalized_coin = coin.lower()
    return PriceRecord(
        exchange=exchange.lower(),
        coin=normalized_coin,
        buy_cop=round(buy_cop, 8),
        sell_cop=round(sell_cop, 8),
        buy_usd=round(buy_usd, 8),
        sell_usd=round(sell_usd, 8),
        spread=calculate_spread(buy_cop, sell_cop),
        direct_cop=direct_cop,
        usd_bridge=usd_bridge.lower(),
        last_updated=last_updated,
    )


# ── Database helpers ──────────────────────────────────────────────────────────

def get_price_model(coin: str):
    """
    Return the correct SQLModel table class for a given coin.
    Stablecoins go to StablecoinPrice; volatile coins go to CryptoPrice.
    """
    return StablecoinPrice if coin.lower() in STABLE_COINS else CryptoPrice


def upsert_price(session: Session, record: PriceRecord) -> None:
    """
    Insert a price row, or update it if (exchange, coin) already exists.
    This is a PostgreSQL-specific "INSERT ... ON CONFLICT DO UPDATE" (upsert).

    The primary key is the pair (exchange, coin), e.g. ("binance", "btc").
    All other columns (buy_cop, sell_cop, spread, last_updated, …) are overwritten.
    """
    model = get_price_model(record.coin)
    payload = record.as_db_dict()

    statement = insert(model).values(payload)

    # Build the SET clause: update every column except the primary key columns
    update_fields = {
        column: payload[column]
        for column in payload
        if column not in {"exchange", "coin"}  # these are the PK, never update them
    }
    statement = statement.on_conflict_do_update(
        index_elements=["exchange", "coin"],  # conflict target = primary key
        set_=update_fields,
    )
    session.exec(statement)


def upsert_prices(session: Session, records: Iterable[PriceRecord]) -> int:
    """Run upsert_price for every record and return how many were processed."""
    count = 0
    for record in records:
        upsert_price(session, record)
        count += 1
    return count


# ── HTTP helper ───────────────────────────────────────────────────────────────

async def request_json(
    client: httpx.AsyncClient,
    url: str,
    *,
    method: str = "GET",
    exchange: str,
    **kwargs: Any,
) -> Optional[Any]:
    """
    Make an async HTTP request and return the parsed JSON body.
    Returns None on any error (network failure, non-2xx status, bad JSON)
    so that individual exchange failures don't crash the whole run.

    Extra keyword args (**kwargs) are passed through to httpx, e.g. `json=` for POST bodies.
    """
    try:
        response = await client.request(method, url, **kwargs)
        response.raise_for_status()  # raises if status >= 400
        return response.json()
    except Exception as exc:
        print(f"⚠️ {exchange}: request failed for {url}: {exc}")
        return None


# ── Available market discovery ────────────────────────────────────────────────
# Before fetching prices, we ask each exchange what markets it actually supports.
# This avoids 404 errors and lets us skip unsupported coins gracefully.

async def fetch_bitso_available_books(client: httpx.AsyncClient) -> set[str]:
    """
    Fetch all trading pairs ("books") that Bitso currently supports.

    API: GET https://api.bitso.com/v3/available_books/

    Example response:
    {
      "success": true,
      "payload": [
        {"book": "btc_cop", "minimum_amount": "0.000125", ...},
        {"book": "usdt_cop", "minimum_amount": "1.00", ...},
        {"book": "eth_mxn", ...},
        ...
      ]
    }

    Returns a set of lowercase book names, e.g. {"btc_cop", "usdt_cop", "eth_mxn", ...}
    Book names follow the pattern "{base}_{quote}", e.g. "btc_cop" = BTC quoted in COP.
    """
    data = await request_json(
        client,
        "https://api.bitso.com/v3/available_books/",
        exchange="bitso",
    )
    if not data:
        return set()
    return {item.get("book", "").lower() for item in data.get("payload", [])}


async def fetch_buda_available_markets(client: httpx.AsyncClient) -> set[str]:
    """
    Fetch all trading markets that Buda currently supports.

    API: GET https://www.buda.com/api/v2/markets

    Example response:
    {
      "markets": [
        {"id": "BTC-COP", "name": "btc-cop", "base_currency": "BTC", "quote_currency": "COP"},
        {"id": "ETH-COP", "name": "eth-cop", ...},
        {"id": "USDT-COP", "name": "usdt-cop", ...},
        ...
      ]
    }

    Returns a set of lowercase market names, e.g. {"btc-cop", "eth-cop", "usdt-cop", ...}
    Buda uses dashes: "btc-cop" (vs Bitso which uses underscores: "btc_cop").
    """
    data = await request_json(
        client,
        "https://www.buda.com/api/v2/markets",
        exchange="buda",
    )
    if not data:
        return set()
    return {item.get("name", "").lower() for item in data.get("markets", [])}


# ── Bridge rate fetching ──────────────────────────────────────────────────────
# Bridge rates are the USDT/COP (or USDC/COP) prices fetched from each exchange.
# They are used later to convert any USD-quoted price into COP.

async def fetch_bitso_bridge_rates(client: httpx.AsyncClient) -> dict[str, BridgeRate]:
    """
    Fetch the COP price for each stable coin on Bitso (USDT, USDC, USD).
    These become the bridge rates used to convert BTC/USDT → BTC/COP, etc.

    API: GET https://api.bitso.com/v3/ticker/?book=usdt_cop

    Example response:
    {
      "success": true,
      "payload": {
        "book": "usdt_cop",
        "ask": "4160.50",   ← exchange sells USDT to you for 4,160.50 COP  (you pay this to BUY)
        "bid": "4139.80",   ← exchange buys USDT from you for 4,139.80 COP (you get this when SELLING)
        "last": "4150.00",
        "volume": "1234567.89"
      }
    }

    Returns a dict like: {"usdt": BridgeRate(coin="usdt", buy=4160.5, sell=4139.8), ...}
    """
    available_books = await fetch_bitso_available_books(client)
    bridges: dict[str, BridgeRate] = {}

    for bridge in BITSO_BRIDGES:  # tries "usdt", "usdc", "usd" in order
        book = f"{bridge}_cop"
        # Skip if this stable coin isn't traded on Bitso
        if available_books and book not in available_books:
            continue

        data = await request_json(
            client,
            f"https://api.bitso.com/v3/ticker/?book={book}",
            exchange="bitso",
        )
        if not data or not data.get("success"):
            continue

        payload = data.get("payload", {})
        buy_cop = safe_float(payload.get("ask"))   # ask = price to BUY from exchange
        sell_cop = safe_float(payload.get("bid"))  # bid = price exchange BUYS from you
        if buy_cop <= 0 or sell_cop <= 0:
            continue

        bridges[bridge] = BridgeRate(coin=bridge, buy=buy_cop, sell=sell_cop)

    return bridges


async def fetch_buda_bridge_rates(client: httpx.AsyncClient) -> dict[str, BridgeRate]:
    """
    Fetch the COP price for each stable coin on Buda (USDT, USDC, EUROC).

    API: GET https://www.buda.com/api/v2/markets/usdt-cop/ticker

    Example response:
    {
      "ticker": {
        "market_id": "USDT-COP",
        "last_price": ["4150.00", "COP"],
        "min_ask": ["4161.00", "COP"],   ← lowest ask = cheapest price to BUY USDT
        "max_bid": ["4140.00", "COP"],   ← highest bid = best price when SELLING USDT
        "volume": ["98765.43", "USDT"]
      }
    }

    Note: prices come as [value, currency] arrays, so we take index [0].
    Returns a dict like: {"usdt": BridgeRate(coin="usdt", buy=4161.0, sell=4140.0), ...}
    """
    available_markets = await fetch_buda_available_markets(client)
    bridges: dict[str, BridgeRate] = {}

    for bridge in STABLE_COINS:
        market = f"{bridge}-cop"
        if available_markets and market not in available_markets:
            continue

        data = await request_json(
            client,
            f"https://www.buda.com/api/v2/markets/{market}/ticker",
            exchange="buda",
        )
        ticker = (data or {}).get("ticker", {})
        min_ask = ticker.get("min_ask") or []  # e.g. ["4161.00", "COP"]
        max_bid = ticker.get("max_bid") or []  # e.g. ["4140.00", "COP"]
        buy_cop = safe_float(min_ask[0] if min_ask else None)
        sell_cop = safe_float(max_bid[0] if max_bid else None)
        if buy_cop <= 0 or sell_cop <= 0:
            continue

        bridges[bridge] = BridgeRate(coin=bridge, buy=buy_cop, sell=sell_cop)

    return bridges


# ── Bridge rate selection logic ───────────────────────────────────────────────

def select_reference_bridge(
    bitso_bridges: dict[str, BridgeRate],
    buda_bridges: dict[str, BridgeRate],
) -> BridgeRate:
    """
    Pick the single best bridge rate to use as the global COP/USD reference.
    Prefers Bitso first (higher liquidity for COP pairs), then Buda.
    Prefers USDT over USDC over USD as the stable coin.

    Falls back to DEFAULT_BRIDGE_RATE (3,900 COP/USD) if no exchange responded.
    This fallback is a hardcoded estimate and will be wrong if the real rate diverges.
    """
    for bridge_name in ("usdt", "usdc", "usd"):
        if bridge_name in bitso_bridges:
            return bitso_bridges[bridge_name]
        if bridge_name in buda_bridges:
            return buda_bridges[bridge_name]
    return BridgeRate(coin="usd", buy=DEFAULT_BRIDGE_RATE, sell=DEFAULT_BRIDGE_RATE)


def get_usd_like_bridge(quote: str, reference_bridge: BridgeRate) -> Optional[BridgeRate]:
    """
    Return the BridgeRate to use for a given quote currency on Binance/CryptoMKT.

    - "COP" quote → bridge rate of 1.0 (no conversion needed; price is already in COP)
    - Any USD-like stable coin (USDT, USDC, FDUSD, …) → use the reference bridge rate
    - Anything else (e.g. BTC, ETH as quote) → return None (we can't convert this)

    Example: coin=BTC, quote=USDT, reference_bridge.buy=4160
      → bridge = BridgeRate(coin="usdt", buy=4160, sell=4140)
      → buy_cop = ask_price_usdt × 4160
    """
    normalized_quote = quote.lower()
    if normalized_quote == "cop":
        # Already in COP — multiply by 1.0, no-op
        return BridgeRate(coin="cop", buy=1.0, sell=1.0)
    if normalized_quote in {item.lower() for item in USD_LIKE_QUOTES}:
        # USD-equivalent: use the reference bridge to convert USD → COP
        return BridgeRate(coin=normalized_quote, buy=reference_bridge.buy, sell=reference_bridge.sell)
    return None  # unknown quote currency; skip this pair


def _resolve_bridge_prices(
    ask_price: float,
    bid_price: float,
    quote: str,
    reference_bridge: BridgeRate,
    bridge: Optional[BridgeRate],
) -> Optional[tuple[float, float, float, float, bool, str]]:
    """
    Convert raw ask/bid prices (in `quote` currency) into COP and USD values.
    Used by both Binance and CryptoMKT, which share the same uppercase quote format.

    Returns: (buy_cop, sell_cop, buy_usd, sell_usd, direct_cop, usd_bridge)
    Returns None if we can't resolve the price (unknown quote currency).

    How the math works:
      Case A — Direct COP quote (e.g. BTCCOP):
        buy_cop  = ask_price             e.g. 278,000,000 COP
        sell_cop = bid_price             e.g. 276,000,000 COP
        buy_usd  = buy_cop / bridge.buy  e.g. 278,000,000 / 4160 ≈ 66,827 USD

      Case B — USD-quoted (e.g. BTCUSDT):
        buy_cop  = ask_price × bridge.buy   e.g. 67,000 × 4160 = 278,720,000 COP
        sell_cop = bid_price × bridge.sell  e.g. 66,980 × 4140 = 277,297,200 COP
        buy_usd  = ask_price                e.g. 67,000 USD
    """
    if quote == "COP":
        buy_cop = ask_price
        sell_cop = bid_price
        buy_usd = buy_cop / reference_bridge.buy if reference_bridge.buy else 0.0
        sell_usd = sell_cop / reference_bridge.sell if reference_bridge.sell else 0.0
        return buy_cop, sell_cop, buy_usd, sell_usd, True, ""
    if not bridge:
        return None  # can't convert; skip this pair
    buy_cop = ask_price * bridge.buy
    sell_cop = bid_price * bridge.sell
    buy_usd = ask_price if quote in USD_LIKE_QUOTES else 0.0
    sell_usd = bid_price if quote in USD_LIKE_QUOTES else 0.0
    return buy_cop, sell_cop, buy_usd, sell_usd, False, bridge.coin


def get_bitso_quote_bridge(quote: str, bitso_bridges: dict[str, BridgeRate]) -> Optional[BridgeRate]:
    """
    Return the BridgeRate for a given quote currency specifically for Bitso.
    Bitso uses lowercase quotes ("cop", "usdt", "usd", "usds").

    "usds" is an alias Bitso uses for USD-pegged stable dollars; map it to "usd".
    Returns None if the stable coin isn't in our fetched bridge rates.
    """
    normalized_quote = quote.lower()
    if normalized_quote == "cop":
        return BridgeRate(coin="cop", buy=1.0, sell=1.0)
    if normalized_quote == "usds":
        return bitso_bridges.get("usd")  # treat USDS as USD
    return bitso_bridges.get(normalized_quote)


# ── Symbol/book selection ─────────────────────────────────────────────────────
# Each exchange has its own naming convention for trading pairs.
# These helpers pick the best available pair for a given coin.

def select_bitso_book(
    coin: str,
    available_books: set[str],
    bitso_bridges: dict[str, BridgeRate],
) -> Optional[tuple[str, str]]:
    """
    Find the best Bitso trading book for `coin` by checking BITSO_QUOTE_PRIORITY.
    Returns (book_name, quote_currency) or None if no suitable book is found.

    Example: coin="btc", available_books={"btc_cop", "btc_usdt"}
      → tries "btc_cop" first → found! → returns ("btc_cop", "cop")
    Example: coin="eth", available_books={"eth_usdt"}
      → "eth_cop" not found → tries "eth_usdt" → found and bridge exists
      → returns ("eth_usdt", "usdt")
    """
    for quote in BITSO_QUOTE_PRIORITY:
        book = f"{coin}_{quote}"
        if book not in available_books:
            continue
        # Only use this book if we have a way to convert its quote to COP
        if quote == "cop" or get_bitso_quote_bridge(quote, bitso_bridges):
            return book, quote
    return None


def select_binance_symbol(
    coin: str,
    market_data: dict[str, Any],
) -> Optional[tuple[str, str]]:
    """
    Find the best Binance trading symbol for `coin` by checking BINANCE_QUOTE_PRIORITY.
    Returns (symbol, quote_currency) or None if not found.

    Binance symbols are uppercase with no separator: "BTCUSDT", "BTCCOP", etc.

    Example: coin="btc" → tries "BTCCOP", "BTCUSDT", ... → first match wins.
    Special case: coin="euroc" → Binance has no EUROCCOP, so we use BINANCE_SYMBOLS
      to map it to "EURUSDT" instead.
    """
    for quote in BINANCE_QUOTE_PRIORITY:
        symbol = f"{coin.upper()}{quote}"
        if symbol in market_data:
            return symbol, quote
    # Check the special alias mapping (e.g. euroc → EURUSDT)
    if coin in BINANCE_SYMBOLS and BINANCE_SYMBOLS[coin] in market_data:
        symbol = BINANCE_SYMBOLS[coin]
        for quote in BINANCE_QUOTE_PRIORITY:
            if symbol.endswith(quote):
                return symbol, quote
    return None


def select_cryptomkt_symbol(
    coin: str,
    market_data: dict[str, Any],
) -> Optional[tuple[str, str]]:
    """
    Find the best CryptoMKT symbol for `coin` by checking CRYPTOMKT_QUOTE_PRIORITY.
    CryptoMKT uses the same uppercase no-separator format as Binance: "BTCCOP", "BTCUSDT".

    Returns (symbol, quote_currency) or None if not found.
    """
    for quote in CRYPTOMKT_QUOTE_PRIORITY:
        symbol = f"{coin.upper()}{quote}"
        if symbol in market_data:
            return symbol, quote
    return None


def get_ticker_prices(ticker: dict[str, Any]) -> tuple[float, float]:
    """
    Extract ask and bid prices from a CryptoMKT ticker dict.
    Falls back to `last` price if ask or bid is missing/zero — this can
    happen for thinly traded markets where the order book is empty.

    Example ticker:
    {
      "ask": "278000000",
      "bid": "276000000",
      "last": "277000000",
      "open": "270000000",
      ...
    }
    """
    ask_price = safe_float(ticker.get("ask"))
    bid_price = safe_float(ticker.get("bid"))
    last_price = safe_float(ticker.get("last"))

    # If ask/bid are missing, use the last traded price as a best-effort fallback
    if ask_price <= 0 and last_price > 0:
        ask_price = last_price
    if bid_price <= 0 and last_price > 0:
        bid_price = last_price

    return ask_price, bid_price


# ── Exchange price fetchers ───────────────────────────────────────────────────

async def fetch_binance_prices(
    client: httpx.AsyncClient,
    reference_bridge: BridgeRate,
) -> list[PriceRecord]:
    """
    Fetch prices for all TARGET_COINS from Binance in a single API call.

    API: GET https://api.binance.com/api/v3/ticker/bookTicker
    Returns the best bid/ask for every symbol in one shot (no pagination).

    Example response (truncated):
    [
      {"symbol": "BTCUSDT",  "bidPrice": "67000.00", "askPrice": "67010.00", "bidQty": "0.5",  "askQty": "1.2"},
      {"symbol": "BTCCOP",   "bidPrice": "276500000","askPrice": "278000000","bidQty": "0.01", "askQty": "0.02"},
      {"symbol": "ETHUSDT",  "bidPrice": "3500.00",  "askPrice": "3502.00",  "bidQty": "2.0",  "askQty": "3.5"},
      {"symbol": "SOLUSDT",  "bidPrice": "180.00",   "askPrice": "180.10",   ...},
      {"symbol": "EURUSDT",  "bidPrice": "1.08",     "askPrice": "1.082",    ...},
      ...
    ]

    We index this by symbol → dict for O(1) lookup, then loop over TARGET_COINS.
    """
    data = await request_json(
        client,
        "https://api.binance.com/api/v3/ticker/bookTicker",
        exchange="binance",
    )
    if not data:
        return []

    # Build a lookup dict: {"BTCUSDT": {symbol, bidPrice, askPrice, ...}, ...}
    market_data = {item.get("symbol"): item for item in data}
    records: list[PriceRecord] = []

    for coin in TARGET_COINS:
        selection = select_binance_symbol(coin, market_data)
        if not selection:
            continue
        symbol, quote = selection
        ticker = market_data.get(symbol)
        if not ticker:
            continue

        ask_price = safe_float(ticker.get("askPrice"))  # price you pay to BUY
        bid_price = safe_float(ticker.get("bidPrice"))  # price you get when SELLING
        if ask_price <= 0 or bid_price <= 0:
            continue

        bridge = get_usd_like_bridge(quote, reference_bridge)
        prices = _resolve_bridge_prices(ask_price, bid_price, quote, reference_bridge, bridge)
        if prices is None:
            continue
        buy_cop, sell_cop, buy_usd, sell_usd, direct_cop, usd_bridge = prices

        records.append(
            build_price_record(
                exchange="binance",
                coin=coin,
                buy_cop=buy_cop,
                sell_cop=sell_cop,
                buy_usd=buy_usd,
                sell_usd=sell_usd,
                direct_cop=direct_cop,
                usd_bridge=usd_bridge,
            )
        )

    return records


async def fetch_cryptomkt_prices(
    client: httpx.AsyncClient,
    reference_bridge: BridgeRate,
) -> list[PriceRecord]:
    """
    Fetch prices for all TARGET_COINS from CryptoMKT in a single API call.

    API: GET https://api.exchange.cryptomkt.com/api/3/public/ticker
    Returns a dict keyed by symbol (unlike Binance which returns a list).

    Example response (truncated):
    {
      "BTCCOP":  {"ask": "278500000", "bid": "276000000", "last": "277000000", "open": "270000000", ...},
      "BTCUSDT": {"ask": "67100.00",  "bid": "67050.00",  "last": "67080.00",  ...},
      "ETHCOP":  {"ask": "14500000",  "bid": "14300000",  "last": "14400000",  ...},
      "USDTCOP": {"ask": "4165.00",   "bid": "4142.00",   "last": "4155.00",   ...},
      ...
    }

    CryptoMKT sometimes leaves ask/bid empty on illiquid pairs, so we fall back
    to the `last` price via get_ticker_prices().
    """
    data = await request_json(
        client,
        "https://api.exchange.cryptomkt.com/api/3/public/ticker",
        exchange="cryptomkt",
    )
    if not data:
        return []

    records: list[PriceRecord] = []

    for coin in TARGET_COINS:
        selection = select_cryptomkt_symbol(coin, data)
        if not selection:
            continue

        symbol, quote = selection
        ticker = data.get(symbol, {})
        ask_price, bid_price = get_ticker_prices(ticker)
        if ask_price <= 0 or bid_price <= 0:
            continue

        bridge = get_usd_like_bridge(quote, reference_bridge)
        prices = _resolve_bridge_prices(ask_price, bid_price, quote, reference_bridge, bridge)
        if prices is None:
            continue
        buy_cop, sell_cop, buy_usd, sell_usd, direct_cop, usd_bridge = prices

        records.append(
            build_price_record(
                exchange="cryptomkt",
                coin=coin,
                buy_cop=buy_cop,
                sell_cop=sell_cop,
                buy_usd=buy_usd,
                sell_usd=sell_usd,
                direct_cop=direct_cop,
                usd_bridge=usd_bridge,
            )
        )

    return records


async def fetch_bitso_prices(
    client: httpx.AsyncClient,
    available_books: set[str],
    bitso_bridges: dict[str, BridgeRate],
) -> list[PriceRecord]:
    """
    Fetch prices for all TARGET_COINS from Bitso.

    Unlike Binance/CryptoMKT, Bitso has no "all tickers" endpoint — each book
    (trading pair) requires its own API call. We gather all requests in parallel
    to avoid waiting for them sequentially.

    API: GET https://api.bitso.com/v3/ticker/?book={book}
    e.g. book="btc_cop", "eth_usdt"

    Example response:
    {
      "success": true,
      "payload": {
        "book": "btc_cop",
        "ask": "278000000",   ← price to BUY BTC (exchange sells to you)
        "bid": "276000000",   ← price to SELL BTC (exchange buys from you)
        "last": "277000000",
        "volume": "12.34567"
      }
    }

    Step 1: Determine which books exist and have bridge support → coin_selections
    Step 2: Fetch all tickers in parallel with asyncio.gather
    Step 3: Process each response and build PriceRecord
    """
    # Step 1: Build list of (coin, book, quote) for all coins we can fetch
    coin_selections = [
        (coin, book, quote)
        for coin in TARGET_COINS
        if (sel := select_bitso_book(coin, available_books, bitso_bridges))
        for book, quote in [sel]  # unpack the Optional[tuple] result
    ]
    if not coin_selections:
        return []

    # Step 2: Fire all ticker requests concurrently (instead of one at a time)
    ticker_responses = await asyncio.gather(*[
        request_json(client, f"https://api.bitso.com/v3/ticker/?book={book}", exchange="bitso")
        for _, book, _ in coin_selections
    ])

    # Step 3: Process each (selection, response) pair
    records: list[PriceRecord] = []
    for (coin, _book, quote), data in zip(coin_selections, ticker_responses):
        if not data or not data.get("success"):
            continue

        payload = data.get("payload", {})
        ask_price = safe_float(payload.get("ask"))
        bid_price = safe_float(payload.get("bid"))
        if ask_price <= 0 or bid_price <= 0:
            continue

        bridge = get_bitso_quote_bridge(quote, bitso_bridges)

        if quote == "cop":
            # Direct COP quote — no conversion needed
            buy_cop = ask_price
            sell_cop = bid_price
            # Derive the USD equivalent using our reference bridge
            ref = (
                bitso_bridges.get("usdt")
                or bitso_bridges.get("usd")
                or BridgeRate(coin="usd", buy=DEFAULT_BRIDGE_RATE, sell=DEFAULT_BRIDGE_RATE)
            )
            buy_usd = buy_cop / ref.buy if ref.buy else 0.0
            sell_usd = sell_cop / ref.sell if ref.sell else 0.0
            direct_cop = True
            usd_bridge = ""
        else:
            # USD-quoted pair — multiply by the stable coin's COP price
            if not bridge:
                continue  # no bridge available, can't convert
            buy_cop = ask_price * bridge.buy
            sell_cop = bid_price * bridge.sell
            buy_usd = ask_price if quote in {"usd", "usds", "usdt", "usdc"} else 0.0
            sell_usd = bid_price if quote in {"usd", "usds", "usdt", "usdc"} else 0.0
            direct_cop = False
            usd_bridge = bridge.coin

        records.append(
            build_price_record(
                exchange="bitso",
                coin=coin,
                buy_cop=buy_cop,
                sell_cop=sell_cop,
                buy_usd=buy_usd,
                sell_usd=sell_usd,
                direct_cop=direct_cop,
                usd_bridge=usd_bridge,
            )
        )

    return records


async def fetch_buda_prices(
    client: httpx.AsyncClient,
    available_markets: set[str],
    reference_bridge: BridgeRate,
) -> list[PriceRecord]:
    """
    Fetch prices for all TARGET_COINS from Buda.
    Buda only has direct COP pairs (e.g. BTC-COP), so no bridge conversion needed
    for the COP prices. We still derive USD values using the reference bridge.

    API: GET https://www.buda.com/api/v2/markets/{market}/ticker
    e.g. market="btc-cop"

    Example response:
    {
      "ticker": {
        "market_id": "BTC-COP",
        "last_price": ["277000000.0", "COP"],
        "min_ask":    ["278500000.0", "COP"],   ← lowest sell offer = price to BUY
        "max_bid":    ["276000000.0", "COP"],   ← highest buy offer = price when SELLING
        "volume":     ["5.43210000", "BTC"],
        "open":       ["270000000.0", "COP"]
      }
    }

    Prices come as [value, currency] arrays; we take index [0] for the number.
    Note: Buda also has one request per market, but the number of markets we
    track is small so we keep it sequential for simplicity.
    """
    records: list[PriceRecord] = []

    for coin in TARGET_COINS:
        market = f"{coin}-cop"
        # Skip coins not available on Buda
        if available_markets and market not in available_markets:
            continue

        data = await request_json(
            client,
            f"https://www.buda.com/api/v2/markets/{market}/ticker",
            exchange="buda",
        )
        ticker = (data or {}).get("ticker", {})
        min_ask = ticker.get("min_ask") or []  # e.g. ["278500000.0", "COP"]
        max_bid = ticker.get("max_bid") or []  # e.g. ["276000000.0", "COP"]
        buy_cop = safe_float(min_ask[0] if min_ask else None)
        sell_cop = safe_float(max_bid[0] if max_bid else None)
        if buy_cop <= 0 or sell_cop <= 0:
            continue

        # Derive USD values from the reference bridge (COP ÷ COP/USD = USD)
        buy_usd = buy_cop / reference_bridge.buy if reference_bridge.buy else 0.0
        sell_usd = sell_cop / reference_bridge.sell if reference_bridge.sell else 0.0

        records.append(
            build_price_record(
                exchange="buda",
                coin=coin,
                buy_cop=buy_cop,
                sell_cop=sell_cop,
                buy_usd=buy_usd,
                sell_usd=sell_usd,
            )
        )

    return records


async def fetch_global66_prices(client: httpx.AsyncClient) -> list[PriceRecord]:
    """
    Fetch USD/COP and EUR/COP rates from Global66 (a Colombian fintech/remittance app).
    Global66 doesn't have a public crypto API — it's a currency exchange platform.
    We use their quote API by simulating a transfer and reading the output amount.

    API: GET https://api.global66.com/quote/public?...
    Parameters:
      originRoute=287   → Colombia (COP source)
      originRoute=291   → United States (USD source)
      originRoute=286   → Europe (EUR source)
      destinationRoute  → opposite of origin
      amount            → input amount in the origin currency
      way=origin        → amount refers to what we're sending (not what arrives)
      product=EXCHANGE  → currency exchange product (not remittance)

    Example response for sell_quote (sending 1,000 USD to Colombia):
    {
      "quoteData": {
        "originAmount": 1000,
        "destinationAmount": 4150000.0,   ← you get 4,150,000 COP for 1,000 USD
        "exchangeRate": 4150.0,
        "fee": 0,
        ...
      }
    }

    Rate derivation:
      sell_cop  = destinationAmount / 1000 = 4,150,000 / 1,000 = 4,150 COP per USD
                  (this is what users RECEIVE when selling USD → they get COP)
      buy_cop   = 1,000,000 / destinationAmount
                  (inverting: if 1M COP buys X USD, then 1 USD costs 1M/X COP)

    We apply these rates to both USDT and USDC, since Global66 doesn't deal in
    actual crypto — it's a proxy for the USD/COP exchange rate they offer.
    """
    # Fire all 4 quote requests in parallel (USD buy/sell + EUR buy/sell)
    sell_quote, buy_quote, eur_sell_quote, eur_buy_quote = await asyncio.gather(
        # Sell side: how much COP do you get for 1,000 USD?
        request_json(client, "https://api.global66.com/quote/public?originRoute=287&destinationRoute=291&amount=1000&way=origin&product=EXCHANGE", exchange="global66"),
        # Buy side: how much USD do you get for 1,000,000 COP?
        request_json(client, "https://api.global66.com/quote/public?originRoute=291&destinationRoute=287&amount=1000000&way=origin&product=EXCHANGE", exchange="global66"),
        # EUR sell side: how much COP do you get for 1,000 EUR?
        request_json(client, "https://api.global66.com/quote/public?originRoute=286&destinationRoute=291&amount=1000&way=origin&product=EXCHANGE", exchange="global66"),
        # EUR buy side: how much EUR do you get for 1,000,000 COP?
        request_json(client, "https://api.global66.com/quote/public?originRoute=291&destinationRoute=286&amount=1000000&way=origin&product=EXCHANGE", exchange="global66"),
    )

    records: list[PriceRecord] = []

    # ── USD rates ──
    sell_amount = safe_float(((sell_quote or {}).get("quoteData") or {}).get("destinationAmount"))
    buy_amount  = safe_float(((buy_quote or {}).get("quoteData") or {}).get("destinationAmount"))
    if sell_amount > 0 and buy_amount > 0:
        sell_cop = sell_amount / 1000        # COP received per 1 USD sold
        buy_cop  = 1_000_000 / buy_amount    # COP paid per 1 USD bought
        # Apply these rates to both USDT and USDC (Global66 treats them all as "USD")
        records.extend(
            build_price_record(
                exchange="global66",
                coin=coin,
                buy_cop=buy_cop,
                sell_cop=sell_cop,
                buy_usd=1.0,   # 1 USDT ≈ 1 USD by definition
                sell_usd=1.0,
                usd_bridge="usd",
            )
            for coin in ("usdt", "usdc")
            if coin in TARGET_COIN_SET
        )

    # ── EUR rates ──
    eur_sell_amount = safe_float(((eur_sell_quote or {}).get("quoteData") or {}).get("destinationAmount"))
    eur_buy_amount  = safe_float(((eur_buy_quote or {}).get("quoteData") or {}).get("destinationAmount"))
    if "euroc" in TARGET_COIN_SET and eur_sell_amount > 0 and eur_buy_amount > 0:
        eur_sell_cop = eur_sell_amount / 1000
        eur_buy_cop  = 1_000_000 / eur_buy_amount
        records.append(
            build_price_record(
                exchange="global66",
                coin="euroc",
                buy_cop=eur_buy_cop,
                sell_cop=eur_sell_cop,
                buy_usd=0.0,    # EUR is not USD; leave these as 0
                sell_usd=0.0,
                usd_bridge="eur",
            )
        )

    return records


async def fetch_plenti_prices(client: httpx.AsyncClient) -> list[PriceRecord]:
    """
    Fetch USD/COP and EUR/COP rates from Plenti (a Colombian fintech).
    Similar to Global66, Plenti offers currency accounts — not direct crypto.
    We call their currency converter and reverse-engineer the buy/sell rates.

    API: POST https://prod.somosplenti.com/currency-converter/convert
    Body: {"fromCurrency": "USD", "toCurrency": "COP"}

    Example response (USD → COP):
    {"exchangeRate": "4,150.00", "fromCurrency": "USD", "toCurrency": "COP", ...}
      → sell_rate = 4,150.00 (you receive 4,150 COP per USD you sell)

    Example response (COP → USD):
    {"exchangeRate": "0.000238", "fromCurrency": "COP", "toCurrency": "USD", ...}
      → buy_rate = 0.000238 COP/USD
      → buy_cop = 1 / 0.000238 ≈ 4,201 COP (cost to buy 1 USD)

    Note: Plenti returns rates with commas as thousands separators ("4,150.00"),
    so we strip commas before parsing.

    All 4 requests (USD buy/sell + EUR buy/sell) run in parallel.
    """
    _url = "https://prod.somosplenti.com/currency-converter/convert"
    usd_to_cop, cop_to_usd, eur_to_cop, cop_to_eur = await asyncio.gather(
        request_json(client, _url, method="POST", exchange="plenti", json={"fromCurrency": "USD", "toCurrency": "COP"}),
        request_json(client, _url, method="POST", exchange="plenti", json={"fromCurrency": "COP", "toCurrency": "USD"}),
        request_json(client, _url, method="POST", exchange="plenti", json={"fromCurrency": "EUR", "toCurrency": "COP"}),
        request_json(client, _url, method="POST", exchange="plenti", json={"fromCurrency": "COP", "toCurrency": "EUR"}),
    )

    records: list[PriceRecord] = []

    # ── USD rates ──
    # Strip commas from the rate string before converting to float
    sell_rate = safe_float((((usd_to_cop or {}).get("exchangeRate", "")) or "").replace(",", ""))
    buy_rate  = safe_float((((cop_to_usd or {}).get("exchangeRate", "")) or "").replace(",", ""))
    if sell_rate > 0 and buy_rate > 0:
        buy_cop  = 1 / buy_rate   # COP needed to buy 1 USD (invert the COP→USD rate)
        sell_cop = sell_rate      # COP received when selling 1 USD
        records.extend(
            build_price_record(
                exchange="plenti",
                coin=coin,
                buy_cop=buy_cop,
                sell_cop=sell_cop,
                buy_usd=1.0,
                sell_usd=1.0,
                usd_bridge="usd",
            )
            for coin in ("usdt", "usdc")
            if coin in TARGET_COIN_SET
        )

    # ── EUR rates ──
    eur_sell_rate = safe_float((((eur_to_cop or {}).get("exchangeRate", "")) or "").replace(",", ""))
    eur_buy_rate  = safe_float((((cop_to_eur or {}).get("exchangeRate", "")) or "").replace(",", ""))
    if "euroc" in TARGET_COIN_SET and eur_sell_rate > 0 and eur_buy_rate > 0:
        records.append(
            build_price_record(
                exchange="plenti",
                coin="euroc",
                buy_cop=1 / eur_buy_rate,   # invert COP→EUR rate to get EUR→COP cost
                sell_cop=eur_sell_rate,
                buy_usd=0.0,
                sell_usd=0.0,
                usd_bridge="eur",
            )
        )

    return records


async def fetch_dolarapp_prices(client: httpx.AsyncClient) -> list[PriceRecord]:
    """
    Fetch USD/COP and EUR/COP rates from DolarApp (a Colombian dollar account app)
    via the co.dolarapi.com aggregator — an unofficial public API.

    API: GET https://co.dolarapi.com/v1/cotizaciones

    Example response:
    [
      {
        "moneda": "USD",
        "casa": "dolarapp",
        "compra": 4140.0,   ← "compra" = exchange BUYS from you → you SELL USD → receive 4,140 COP
        "venta": 4160.0,    ← "venta"  = exchange SELLS to you → you BUY USD → pay 4,160 COP
        "fechaActualizacion": "2024-04-03T15:00:00Z"
      },
      {
        "moneda": "EUR",
        "casa": "dolarapp",
        "compra": 4500.0,
        "venta": 4520.0,
        ...
      }
    ]

    ⚠️ Note the Spanish naming convention:
      "compra" (the exchange "buys" from you) = what YOU receive when SELLING → sell_cop
      "venta"  (the exchange "sells" to you)  = what YOU pay when BUYING     → buy_cop
    """
    data = await request_json(
        client,
        "https://co.dolarapi.com/v1/cotizaciones",
        exchange="dolarapp",
    )
    if not data:
        return []

    records: list[PriceRecord] = []

    for quote in data:
        currency = str(quote.get("moneda", "")).upper()
        buy_cop  = safe_float(quote.get("venta"))   # "venta" = they sell to you = your buy price
        sell_cop = safe_float(quote.get("compra"))  # "compra" = they buy from you = your sell price
        if buy_cop <= 0 or sell_cop <= 0:
            continue

        if currency == "USD":
            # Apply the USD rate to both USDT and USDC
            records.extend(
                build_price_record(
                    exchange="dolarapp",
                    coin=coin,
                    buy_cop=buy_cop,
                    sell_cop=sell_cop,
                    buy_usd=1.0,
                    sell_usd=1.0,
                    usd_bridge="usd",
                )
                for coin in ("usdt", "usdc")
                if coin in TARGET_COIN_SET
            )
        elif currency == "EUR" and "euroc" in TARGET_COIN_SET:
            records.append(
                build_price_record(
                    exchange="dolarapp",
                    coin="euroc",
                    buy_cop=buy_cop,
                    sell_cop=sell_cop,
                    buy_usd=1.0,
                    sell_usd=1.0,
                    usd_bridge="eur",
                )
            )

    return records


# ── Orchestration ─────────────────────────────────────────────────────────────

async def collect_prices() -> list[PriceRecord]:
    """
    Orchestrate a full price collection run across all providers.

    Execution order:
      1. Fetch available books/markets from Bitso and Buda (needed for next steps).
      2. Fetch bridge rates (USDT/COP, USDC/COP) from Bitso and Buda.
      3. Select the best available bridge rate as the global reference.
      4. Fetch prices from all 7 providers in PARALLEL (asyncio.gather).
      5. Stamp every record with the same `last_updated` timestamp.

    Steps 1-3 are sequential because each step depends on the previous.
    Step 4 is fully parallel — all exchanges are queried at the same time.

    Returns a flat list of PriceRecord objects ready for database insertion.
    """
    refreshed_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    async with httpx.AsyncClient(timeout=10.0) as client:
        # Sequential setup: need available books before fetching bridge rates
        bitso_books   = await fetch_bitso_available_books(client)
        buda_markets  = await fetch_buda_available_markets(client)
        bitso_bridges = await fetch_bitso_bridge_rates(client)
        buda_bridges  = await fetch_buda_bridge_rates(client)

        # Choose the best stable coin rate to use as our COP/USD reference
        reference_bridge = select_reference_bridge(bitso_bridges, buda_bridges)

        # Fetch all providers simultaneously — this is the main performance win
        provider_results = await asyncio.gather(
            fetch_binance_prices(client, reference_bridge),
            fetch_cryptomkt_prices(client, reference_bridge),
            fetch_bitso_prices(client, bitso_books, bitso_bridges),
            fetch_buda_prices(client, buda_markets, reference_bridge),
            fetch_global66_prices(client),
            fetch_plenti_prices(client),
            fetch_dolarapp_prices(client),
        )

    # Flatten the list of lists into a single list
    records = [record for provider_records in provider_results for record in provider_records]

    # Stamp all records with the same timestamp using dataclasses.replace()
    # (avoids rebuilding each record from scratch)
    records = [replace(r, last_updated=refreshed_at) for r in records]

    print(f"ℹ️ Reference bridge: {reference_bridge.coin.upper()} buy={reference_bridge.buy:.2f} sell={reference_bridge.sell:.2f}")
    print(f"ℹ️ Collected {len(records)} price rows.")
    return records


async def run_fetcher() -> None:
    """
    Entry point for the full fetch-and-save pipeline.
    Called by GitHub Actions every 15 minutes (see .github/workflows/fetcher.yml).

    Steps:
      1. Ensure DB tables exist (idempotent — safe to call on every run).
      2. Collect prices from all providers.
      3. Upsert all records into PostgreSQL and commit.
    """
    print("🚀 Iniciando recolección de datos para Postgres...")
    create_db_and_tables()  # creates tables if they don't exist yet

    records = await collect_prices()
    with Session(engine) as session:
        upserted = upsert_prices(session, records)
        session.commit()  # commit all upserts in one transaction

    print(f"✅ Sincronización completada. {upserted} filas insertadas/actualizadas.")


if __name__ == "__main__":
    asyncio.run(run_fetcher())
