"""
pricing.py — Build the price response served to the frontend.

This module assembles the data returned by GET /api/prices/{coin}.
It combines two sources:
  1. Prices fetched automatically by the fetcher (stored in CryptoPrice / StablecoinPrice tables).
  2. Prices entered manually in the admin panel (stored in PlatformInfo.manual_prices as JSON).

Manual prices exist for platforms that don't have a public API (e.g. some fintechs).
"""

import json
from typing import Optional

from sqlalchemy.exc import SQLAlchemyError
from sqlmodel import Session, select

from app.core.config import settings
from app.models import CryptoPrice, PlatformInfo, StablecoinPrice


def parse_price(value) -> Optional[float]:
    """
    Safely convert a value to float. Returns None if conversion fails.

    Manual prices in the DB are sometimes stored as strings ("4150.5") or
    might be missing entirely (None). This avoids crashing on bad data.

    Examples:
      parse_price("4150.5") → 4150.5
      parse_price(4150)     → 4150.0
      parse_price(None)     → None
      parse_price("n/a")    → None
    """
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def build_price_response(session: Session, coin: str):
    """
    Build the full price list for a given coin, combining DB prices and manual overrides.

    Returns a dict like:
    {
      "coin": "btc",
      "prices": [
        {
          "exchange": "binance",
          "buy_cop": 278720000.0,
          "sell_cop": 277297200.0,
          "buy_usd": 67000.0,
          "sell_usd": 66980.0,
          "spread": 0.51,
          "direct_cop": false,
          "usd_bridge": "usdt",
          "last_updated": "2024-04-03 15:00:00 UTC"
        },
        {
          "exchange": "bitso",
          ...
        },
        ...
      ]
    }

    Only active platforms (is_active=True) are included.
    If a platform has a manual price that's active, it's appended at the end
    (unless a fetched price already exists for that exchange — no duplicates).
    """
    normalized_coin = coin.lower()
    results = []

    # ── Step 1: Load all active platforms ────────────────────────────────────
    # We need the full platform list to:
    #   a) filter fetched prices (only include active exchanges)
    #   b) inject manual prices for platforms that don't have API prices
    platforms = [
        platform.model_dump()
        for platform in session.exec(select(PlatformInfo).where(PlatformInfo.is_active))
    ]
    # Build a set of active exchange IDs for fast filtering below
    active_ids = [platform["id"] for platform in platforms]

    # ── Step 2: Load automatically fetched prices from the DB ─────────────────
    # Stablecoins (usdt, usdc, euroc) are in a separate table from volatile coins (btc, eth, …)
    price_model = StablecoinPrice if normalized_coin in settings.stable_coins else CryptoPrice
    try:
        prices = session.exec(select(price_model).where(price_model.coin == normalized_coin))
        for price in prices:
            row = price.model_dump()
            # Only include this price if the exchange is currently active
            if row["exchange"] in active_ids:
                results.append(row)
    except SQLAlchemyError as exc:
        # Log DB errors but return whatever partial results we have
        print(f"DB error querying {normalized_coin} prices: {exc}")

    # ── Step 3: Inject manual prices ─────────────────────────────────────────
    # Some platforms (like fintechs) don't have public APIs.
    # An admin enters their rates manually in the admin panel.
    # Those rates are stored in PlatformInfo.manual_prices as a JSON blob.
    #
    # Example manual_prices structure stored in the DB:
    # {
    #   "usd": {
    #     "active": true,
    #     "currency": "USD",   ← means the buy/sell values below are in USD, not COP
    #     "buy": 1.005,        ← cost in USD to buy 1 USD-equivalent stablecoin
    #     "sell": 0.995        ← USD received when selling
    #   },
    #   "eur": {
    #     "active": true,
    #     "currency": "EUR",
    #     "buy": 1.01,
    #     "sell": 0.99
    #   }
    # }
    for platform in platforms:
        # Skip platforms that use automatic fetching (not manual entry)
        if not platform["is_manual"]:
            continue

        # manual_prices may be stored as a raw JSON string or already parsed as a dict
        manual_prices = platform["manual_prices"]
        if isinstance(manual_prices, str):
            manual_prices = json.loads(manual_prices)

        # Look up the entry for this specific coin
        target_data = manual_prices.get(normalized_coin)

        # Fintechs store rates under "usd" / "eur" keys, not "usdt" / "usdc" / "euroc".
        # If we didn't find a direct match, try the currency aliases.
        if not target_data and platform["category"] == "fintech":
            if normalized_coin in ["usdc", "usdt"]:
                target_data = manual_prices.get("usd")
            elif normalized_coin == "euroc":
                target_data = manual_prices.get("eur")

        # Skip if this coin has no manual entry, or if the entry is marked inactive
        if not target_data or not target_data.get("active"):
            continue

        raw_buy = target_data.get("buy")
        raw_sell = target_data.get("sell")
        buy_val = parse_price(raw_buy)
        sell_val = parse_price(raw_sell)

        is_usd = target_data.get("currency") == "USD"

        # Allow manual placeholder strings such as "N.D." to surface in the UI
        # instead of silently dropping the platform from the response.
        if buy_val is None or sell_val is None:
            if not any(result["exchange"] == platform["id"] for result in results):
                results.append(
                    {
                        "exchange": platform["id"],
                        "buy_cop": raw_buy,
                        "sell_cop": raw_sell,
                        "buy_usd": raw_buy if is_usd else raw_buy,
                        "sell_usd": raw_sell if is_usd else raw_sell,
                        "spread": 0,
                        "direct_cop": not is_usd,
                        "usd_bridge": "",
                        "last_updated": platform.get("last_updated"),
                    }
                )
            continue

        if is_usd:
            # The manual entry stores a USD rate (not COP).
            # We need to convert it to COP using the platform's own stable coin rate.
            #
            # Example: platform manually entered buy=1.005 USD for USDT.
            # Their internal USD/COP rate (from "usdc" or "usd" entry) is buy=4160, sell=4140.
            # Final COP buy price = 1.005 × 4160 = 4,180.8 COP
            internal_stable = (
                manual_prices.get("usdc")
                or manual_prices.get("usd")
                or manual_prices.get("usdt")
            )
            if internal_stable and internal_stable.get("active"):
                trm_i_buy  = parse_price(internal_stable.get("buy"))
                trm_i_sell = parse_price(internal_stable.get("sell"))
                if trm_i_buy is not None and trm_i_sell is not None:
                    buy_val  = buy_val  * trm_i_buy    # USD price × COP/USD = COP price
                    sell_val = sell_val * trm_i_sell

        # Only append if this exchange doesn't already have a fetched price in results
        # (fetched prices take priority over manual entries)
        if not any(result["exchange"] == platform["id"] for result in results):
            results.append(
                {
                    "exchange":    platform["id"],
                    "buy_cop":     buy_val,
                    "sell_cop":    sell_val,
                    "buy_usd":     raw_buy if is_usd else 0,
                    "sell_usd":    raw_sell if is_usd else 0,
                    "spread":      0,         # spread not calculated for manual entries
                    "direct_cop":  not is_usd,
                    "usd_bridge":  "USDC" if is_usd else "",
                    "last_updated": platform.get("last_updated"),
                }
            )

    return {"coin": normalized_coin, "prices": results}
