import os
import asyncio
import httpx
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Cargar configuración desde el .env
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

# Configuraciones de monedas
CRYPTO_COINS = ["btc", "bch", "eth", "sol", "ltc"]
STABLE_COINS = ["usdt", "usdc", "euroc"]
TARGET_COINS = CRYPTO_COINS + STABLE_COINS
BRIDGES = ["usdt", "usdc", "usd"]

def save_price_postgres(connection, table_name, data):
    query = text(f"""
        INSERT INTO {table_name} 
        (exchange, coin, buy_cop, sell_cop, buy_usd, sell_usd, spread, direct_cop, usd_bridge)
        VALUES (:exchange, :coin, :buy_cop, :sell_cop, :buy_usd, :sell_usd, :spread, :direct_cop, :usd_bridge)
        ON CONFLICT (exchange, coin) 
        DO UPDATE SET 
            buy_cop = EXCLUDED.buy_cop,
            sell_cop = EXCLUDED.sell_cop,
            buy_usd = EXCLUDED.buy_usd,
            sell_usd = EXCLUDED.sell_usd,
            spread = EXCLUDED.spread,
            direct_cop = EXCLUDED.direct_cop,
            usd_bridge = EXCLUDED.usd_bridge;
    """)
    
    # Forzamos que direct_cop sea un booleano real de Python
    is_direct = bool(data.get("direct_cop", True))

    params = {
        "exchange": data.get("exchange"),
        "coin": data.get("coin"),
        "buy_cop": float(data.get("buy_cop", 0)),
        "sell_cop": float(data.get("sell_cop", 0)),
        "buy_usd": float(data.get("buy_usd", 0)),
        "sell_usd": float(data.get("sell_usd", 0)),
        "spread": float(data.get("spread", 0)),
        "direct_cop": is_direct,
        "usd_bridge": str(data.get("usd_bridge", ""))
    }
    
    try:
        # Usamos savepoints para que si falla un insert no se muera toda la transacción
        connection.execute(text("SAVEPOINT sp1"))
        connection.execute(query, params)
        connection.execute(text("RELEASE SAVEPOINT sp1"))
    except Exception as e:
        connection.execute(text("ROLLBACK TO SAVEPOINT sp1"))
        print(f"⚠️ No se pudo guardar {data.get('exchange')}-{data.get('coin')}: {e}")

async def get_best_bridge_rate(client, exchange):
    bridges_rates = {}
    try:
        if exchange == "buda":
            res = await client.get("https://www.buda.com/api/v2/markets/usdc-cop/ticker")
            if res.status_code == 200:
                t = res.json()["ticker"]
                if t["min_ask"]:
                    bridges_rates["usdc"] = {"buy": float(t["min_ask"][0]), "sell": float(t["max_bid"][0])}
        elif exchange == "bitso":
            for b in ["usdt", "usd", "usdc"]:
                res = await client.get(f"https://api.bitso.com/v3/ticker/?book={b}_cop")
                if res.status_code == 200:
                    data = res.json()
                    if data.get("success"):
                        t = data["payload"]
                        bridges_rates[b] = {"buy": float(t["ask"]), "sell": float(t["bid"])}
    except Exception as e:
        print(f"⚠️ Error en bridge {exchange}: {e}")
    return bridges_rates

async def run_fetcher():
    print("🚀 Iniciando recolección de datos para Supabase...")
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        with engine.begin() as conn: # Inicia transacción
            
            # Obtener puentes de Bitso y Buda
            buda_bridges = await get_best_bridge_rate(client, "buda")
            bitso_bridges = await get_best_bridge_rate(client, "bitso")

            # --- LÓGICA GLOBAL66 ---
            try:
                res_sell_usd = await client.get("https://api.global66.com/quote/public?originRoute=287&destinationRoute=291&amount=1000&way=origin&product=EXCHANGE")
                res_buy_usd = await client.get("https://api.global66.com/quote/public?originRoute=291&destinationRoute=287&amount=1000000&way=origin&product=EXCHANGE")
                
                if res_sell_usd.status_code == 200 and res_buy_usd.status_code == 200:
                    sell_cop = res_sell_usd.json()["quoteData"]["destinationAmount"] / 1000
                    buy_cop = 1000000 / res_buy_usd.json()["quoteData"]["destinationAmount"]
                    for c in ["usdt", "usdc"]:
                        save_price_postgres(conn, "crypto_prices", {"exchange": "global66", "coin": c, "buy_cop": buy_cop, "sell_cop": sell_cop, "buy_usd": 1.0, "sell_usd": 1.0})
            except Exception as e: print(f"🚨 Global66 Error: {e}")

            # --- LÓGICA PLENTI ---
            try:
                res_usd_cop = await client.post("https://prod.somosplenti.com/currency-converter/convert", json={"fromCurrency":"USD","toCurrency":"COP"})
                res_cop_usd = await client.post("https://prod.somosplenti.com/currency-converter/convert", json={"fromCurrency":"COP","toCurrency":"USD"})
                if res_usd_cop.status_code == 200 and res_cop_usd.status_code == 200:
                    sell_cop = float(res_usd_cop.json()["exchangeRate"].replace(",", ""))
                    buy_cop = 1 / float(res_cop_usd.json()["exchangeRate"].replace(",", ""))
                    for c in ["usdt", "usdc"]:
                        save_price_postgres(conn, "crypto_prices", {"exchange": "plenti", "coin": c, "buy_cop": buy_cop, "sell_cop": sell_cop, "buy_usd": 1.0, "sell_usd": 1.0})
            except Exception as e: print(f"🚨 Plenti Error: {e}")

            # --- LÓGICA DOLARAPP (DolarAPI) ---
            try:
                res_dolarapi = await client.get("https://co.dolarapi.com/v1/cotizaciones")
                if res_dolarapi.status_code == 200:
                    for cot in res_dolarapi.json():
                        moneda = cot.get("moneda", "").upper()
                        buy_v = float(cot.get("venta", 0))
                        sell_v = float(cot.get("compra", 0))
                        if buy_v > 0:
                            spr = round(((buy_v - sell_v) / buy_v) * 100, 2)
                            if moneda == "USD":
                                for c in ["usdt", "usdc"]:
                                    save_price_postgres(conn, "crypto_prices", {"exchange": "dolarapp", "coin": c, "buy_cop": buy_v, "sell_cop": sell_v, "buy_usd": 1.0, "sell_usd": 1.0, "spread": spr, "usd_bridge": "usd"})
            except Exception as e: print(f"🚨 DolarAPI Error: {e}")

            # --- LÓGICA BINANCE ---
            try:
                res_bin = await client.get("https://api.binance.com/api/v3/ticker/bookTicker")
                if res_bin.status_code == 200:
                    bin_data = {item['symbol']: item for item in res_bin.json()}
                    b_bridge = bitso_bridges.get("usdt", {"buy": 3900, "sell": 3900})
                    for coin in TARGET_COINS:
                        symbol = "EURCUSDT" if coin == "euroc" else f"{coin.upper()}USDT"
                        if symbol in bin_data:
                            b_usd = float(bin_data[symbol]["askPrice"])
                            s_usd = float(bin_data[symbol]["bidPrice"])
                            b_cop = b_usd * b_bridge["buy"]
                            s_cop = s_usd * b_bridge["sell"]
                            spr = round(((b_cop - s_cop) / b_cop) * 100, 2)
                            save_price_postgres(conn, "crypto_prices", {"exchange": "binance", "coin": coin, "buy_cop": b_cop, "sell_cop": s_cop, "buy_usd": b_usd, "sell_usd": s_usd, "spread": spr, "direct_cop": False, "usd_bridge": "usdt"})
            except Exception as e: print(f"🚨 Binance Error: {e}")

            # --- LÓGICA BITSO ---
            for coin in TARGET_COINS:
                try:
                    res = await client.get(f"https://api.bitso.com/v3/ticker/?book={coin}_cop")
                    if res.status_code == 200 and res.json().get("success"):
                        t = res.json()["payload"]
                        b_cop, s_cop = float(t["ask"]), float(t["bid"])
                        br_r = bitso_bridges.get("usdt", {"buy": 3900, "sell": 3900})
                        save_price_postgres(conn, "crypto_prices", {"exchange": "bitso", "coin": coin, "buy_cop": b_cop, "sell_cop": s_cop, "buy_usd": b_cop/br_r["buy"], "sell_usd": s_cop/br_r["sell"], "spread": round(((b_cop-s_cop)/b_cop)*100, 2)})
                except: continue

    print("✅ Sincronización con Supabase completada.")

if __name__ == "__main__":
    asyncio.run(run_fetcher())