import sqlite3
import httpx
import asyncio

DB_FILE = "crypto_spread.db"

# Configuraciones
CRYPTO_COINS = ["btc", "bch", "eth", "sol", "ltc"]
STABLE_COINS = ["usdt", "usdc", "euroc"]
TARGET_COINS = CRYPTO_COINS + STABLE_COINS
BRIDGES = ["usdt", "usdc", "usd"]

def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crypto_prices (
            exchange TEXT, coin TEXT, buy_cop REAL, sell_cop REAL, 
            buy_usd REAL, sell_usd REAL, spread REAL, direct_cop BOOLEAN, usd_bridge TEXT,
            PRIMARY KEY (exchange, coin)
        )
    ''')
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS stablecoin_prices (
            exchange TEXT, coin TEXT, buy_cop REAL, sell_cop REAL, 
            buy_usd REAL, sell_usd REAL,
            PRIMARY KEY (exchange, coin)
        )
    ''')
    conn.commit()
    conn.close()

def save_price(cursor, exchange, coin, buy_cop, sell_cop, buy_usd, sell_usd, spread=0, direct_cop=True, usd_bridge=""):
    if coin in STABLE_COINS:
        cursor.execute("INSERT OR REPLACE INTO stablecoin_prices VALUES (?, ?, ?, ?, ?, ?)",
                       (exchange, coin, buy_cop, sell_cop, buy_usd, sell_usd))
    else:
        cursor.execute("INSERT OR REPLACE INTO crypto_prices VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (exchange, coin, buy_cop, sell_cop, buy_usd, sell_usd, spread, direct_cop, usd_bridge))

async def get_best_bridge_rate(client, exchange):
    bridges_rates = {}
    if exchange == "buda":
        res = await client.get("https://www.buda.com/api/v2/markets/usdc-cop/ticker")
        if res.status_code == 200 and res.json()["ticker"]["min_ask"]:
            t = res.json()["ticker"]
            bridges_rates["usdc"] = {"buy": float(t["min_ask"][0]), "sell": float(t["max_bid"][0])}
            
    elif exchange == "bitso":
        for b in ["usdt", "usd", "usdc"]:
            res = await client.get(f"https://api.bitso.com/v3/ticker/?book={b}_cop")
            if res.status_code == 200 and res.json().get("success"):
                t = res.json()["payload"]
                bridges_rates[b] = {"buy": float(t["ask"]), "sell": float(t["bid"])}
    
    return bridges_rates

async def run_fetcher():
    init_db()
    print("🚀 Iniciando el Motor Recolector (Exchanges y Multimonedas)...")
    
    while True:
        try:
            async with httpx.AsyncClient() as client:
                conn = sqlite3.connect(DB_FILE)
                cursor = conn.cursor()

                buda_bridges = await get_best_bridge_rate(client, "buda")
                bitso_bridges = await get_best_bridge_rate(client, "bitso")

                # ==========================================
                # LÓGICA PARA GLOBAL66 (Vía API Privada de Cotización)
                # Rutas: 287 = USD, 286 = EUR, 291 = COP
                # ==========================================
                try:
                    # --- 1. USD a COP (Precio al que VENDES tus USD al exchange) ---
                    res_sell_usd = await client.get("https://api.global66.com/quote/public?originRoute=287&destinationRoute=291&amount=1000&way=origin&product=EXCHANGE")
                    
                    # --- 2. COP a USD (Precio al que COMPRAS USD al exchange) ---
                    res_buy_usd = await client.get("https://api.global66.com/quote/public?originRoute=291&destinationRoute=287&amount=1000000&way=origin&product=EXCHANGE")
                    
                    if res_sell_usd.status_code == 200 and res_buy_usd.status_code == 200:
                        # Venta: Me dan X pesos por 1000 USD -> Divido por 1000
                        sell_cop = res_sell_usd.json()["quoteData"]["destinationAmount"] / 1000
                        # Compra: Pago 1 millón de pesos por X USD -> Divido 1M por los USD recibidos
                        buy_cop = 1000000 / res_buy_usd.json()["quoteData"]["destinationAmount"]
                        
                        save_price(cursor, "global66", "usdt", buy_cop, sell_cop, 1.0, 1.0)
                        save_price(cursor, "global66", "usdc", buy_cop, sell_cop, 1.0, 1.0)
                        
                    # --- 3. EUR a COP (Precio al que VENDES tus EUR al exchange) ---
                    res_sell_eur = await client.get("https://api.global66.com/quote/public?originRoute=286&destinationRoute=291&amount=1000&way=origin&product=EXCHANGE")
                    
                    # --- 4. COP a EUR (Precio al que COMPRAS EUR al exchange) ---
                    res_buy_eur = await client.get("https://api.global66.com/quote/public?originRoute=291&destinationRoute=286&amount=1000000&way=origin&product=EXCHANGE")
                    
                    if res_sell_eur.status_code == 200 and res_buy_eur.status_code == 200:
                        sell_cop_eur = res_sell_eur.json()["quoteData"]["destinationAmount"] / 1000
                        buy_cop_eur = 1000000 / res_buy_eur.json()["quoteData"]["destinationAmount"]
                        
                        save_price(cursor, "global66", "euroc", buy_cop_eur, sell_cop_eur, 1.0, 1.0)

                except Exception as e:
                    print(f"🚨 Error consultando Global66: {e}")

                # ==========================================
                # LÓGICA PARA PLENTI
                # ==========================================
                try:
                    res_usd_cop = await client.post("https://prod.somosplenti.com/currency-converter/convert", json={"fromCurrency":"USD","toCurrency":"COP"})
                    res_cop_usd = await client.post("https://prod.somosplenti.com/currency-converter/convert", json={"fromCurrency":"COP","toCurrency":"USD"})
                    
                    if res_usd_cop.status_code == 200 and res_cop_usd.status_code == 200:
                        rate_usd_cop = float(res_usd_cop.json()["exchangeRate"].replace(",", ""))
                        rate_cop_usd = float(res_cop_usd.json()["exchangeRate"].replace(",", ""))
                        sell_cop = rate_usd_cop
                        buy_cop = 1 / rate_cop_usd
                        save_price(cursor, "plenti", "usdc", buy_cop, sell_cop, 1.0, 1.0)
                        save_price(cursor, "plenti", "usdt", buy_cop, sell_cop, 1.0, 1.0)

                    res_eur_cop = await client.post("https://prod.somosplenti.com/currency-converter/convert", json={"fromCurrency":"EUR","toCurrency":"COP"})
                    res_cop_eur = await client.post("https://prod.somosplenti.com/currency-converter/convert", json={"fromCurrency":"COP","toCurrency":"EUR"})
                    
                    if res_eur_cop.status_code == 200 and res_cop_eur.status_code == 200:
                        rate_eur_cop = float(res_eur_cop.json()["exchangeRate"].replace(",", ""))
                        rate_cop_eur = float(res_cop_eur.json()["exchangeRate"].replace(",", ""))
                        sell_cop_eur = rate_eur_cop
                        buy_cop_eur = 1 / rate_cop_eur
                        save_price(cursor, "plenti", "euroc", buy_cop_eur, sell_cop_eur, 1.0, 1.0)
                except Exception as e:
                    print(f"🚨 Error consultando Plenti: {e}")

                # ==========================================
                # LÓGICA PARA DOLARAPP
                # ==========================================
                try:
                    res_dolarapi = await client.get("https://co.dolarapi.com/v1/cotizaciones")
                    if res_dolarapi.status_code == 200:
                        cotizaciones = res_dolarapi.json()
                        if isinstance(cotizaciones, list):
                            for cotizacion in cotizaciones:
                                moneda = cotizacion.get("moneda", "").upper()
                                buy_cop = float(cotizacion.get("venta", 0))   
                                sell_cop = float(cotizacion.get("compra", 0)) 
                                if buy_cop > 0 and sell_cop > 0:
                                    spread = round(((buy_cop - sell_cop) / buy_cop) * 100, 2)
                                    if moneda == "USD":
                                        save_price(cursor, "dolarapp", "usdt", buy_cop, sell_cop, 1.0, 1.0, spread, True, "usd")
                                        save_price(cursor, "dolarapp", "usdc", buy_cop, sell_cop, 1.0, 1.0, spread, True, "usd")
                                    elif moneda == "EUR":
                                        save_price(cursor, "dolarapp", "euroc", buy_cop, sell_cop, 1.0, 1.0, spread, True, "eur")
                except Exception as e:
                    print(f"🚨 Error consultando DolarAPI: {e}")

                # ==========================================
                # LÓGICA PARA BINANCE (Spot API)
                # ==========================================
                try:
                    res_binance = await client.get("https://api.binance.com/api/v3/ticker/bookTicker")
                    if res_binance.status_code == 200:
                        binance_data = {item['symbol']: item for item in res_binance.json()}
                        binance_bridge_rate = bitso_bridges.get("usdt", {"buy": 3900, "sell": 3900})
                        for coin in TARGET_COINS:
                            buy_usd, sell_usd = 0, 0
                            if coin == "usdt":
                                buy_usd, sell_usd = 1.0, 1.0 
                            else:
                                symbol = "EURCUSDT" if coin == "euroc" else f"{coin.upper()}USDT"
                                if symbol in binance_data:
                                    b_data = binance_data[symbol]
                                    buy_usd = float(b_data["askPrice"])
                                    sell_usd = float(b_data["bidPrice"])
                                else:
                                    continue
                            if buy_usd == 0 or sell_usd == 0: continue
                            buy_cop = buy_usd * binance_bridge_rate["buy"]
                            sell_cop = sell_usd * binance_bridge_rate["sell"]
                            spread = round(((buy_cop - sell_cop) / buy_cop) * 100, 2)
                            save_price(cursor, "binance", coin, buy_cop, sell_cop, buy_usd, sell_usd, spread, False, "usdt")
                except Exception as e:
                    pass

                # ==========================================
                # LÓGICA PARA CRYPTOMARKET (API v3)
                # ==========================================
                try:
                    res_cm = await client.get("https://api.exchange.cryptomkt.com/api/3/public/ticker")
                    if res_cm.status_code == 200:
                        cm_data = res_cm.json()
                        cm_bridges = {}
                        for b in ["USDT", "USDC", "USD"]:
                            pair = f"{b}COP"
                            if pair in cm_data and cm_data[pair].get("ask") and cm_data[pair].get("bid"):
                                cm_bridges[b.lower()] = {"buy": float(cm_data[pair]["ask"]), "sell": float(cm_data[pair]["bid"])}
                        
                        backup_bridge = bitso_bridges.get("usdt", {"buy": 3900, "sell": 3900})
                        for coin in TARGET_COINS:
                            direct_pair = f"{coin.upper()}COP"
                            if direct_pair in cm_data and cm_data[direct_pair].get("ask") and cm_data[direct_pair].get("bid"):
                                buy_cop = float(cm_data[direct_pair]["ask"])
                                sell_cop = float(cm_data[direct_pair]["bid"])
                                bridge_used = "usdt" if "usdt" in cm_bridges else "usd"
                                b_rate = cm_bridges.get(bridge_used, backup_bridge)
                                buy_usd = buy_cop / b_rate["buy"]
                                sell_usd = sell_cop / b_rate["sell"]
                                spread = round(((buy_cop - sell_cop) / buy_cop) * 100, 2)
                                save_price(cursor, "cryptomarket", coin, buy_cop, sell_cop, buy_usd, sell_usd, spread, True, bridge_used)
                            else:
                                for bridge in BRIDGES:
                                    bridge_pair = f"{coin.upper()}{bridge.upper()}"
                                    if bridge_pair in cm_data and cm_data[bridge_pair].get("ask") and cm_data[bridge_pair].get("bid"):
                                        buy_usd = float(cm_data[bridge_pair]["ask"])
                                        sell_usd = float(cm_data[bridge_pair]["bid"])
                                        b_rate = cm_bridges.get(bridge, backup_bridge)
                                        buy_cop = buy_usd * b_rate["buy"]
                                        sell_cop = sell_usd * b_rate["sell"]
                                        spread = round(((buy_cop - sell_cop) / buy_cop) * 100, 2)
                                        save_price(cursor, "cryptomarket", coin, buy_cop, sell_cop, buy_usd, sell_usd, spread, False, bridge)
                                        break
                except Exception as e:
                    pass

                # ==========================================
                # LÓGICA PARA BITSO
                # ==========================================
                for coin in TARGET_COINS:
                    try:
                        direct_bitso = f"{coin}_cop"
                        res_dir_bitso = await client.get(f"https://api.bitso.com/v3/ticker/?book={direct_bitso}")
                        if res_dir_bitso.status_code == 200 and res_dir_bitso.json().get("success"):
                            t = res_dir_bitso.json()["payload"]
                            buy_cop, sell_cop = float(t["ask"]), float(t["bid"])
                            bridge_used = "usdt" if "usdt" in bitso_bridges else "usd"
                            bridge_rate = bitso_bridges.get(bridge_used, {"buy": 3900, "sell": 3900})
                            buy_usd = buy_cop / bridge_rate["buy"]
                            sell_usd = sell_cop / bridge_rate["sell"]
                            spread = round(((buy_cop - sell_cop) / buy_cop) * 100, 2)
                            save_price(cursor, "bitso", coin, buy_cop, sell_cop, buy_usd, sell_usd, spread, True, bridge_used)
                        else:
                            for bridge in BRIDGES:
                                res_bridge = await client.get(f"https://api.bitso.com/v3/ticker/?book={coin}_{bridge}")
                                if res_bridge.status_code == 200 and res_bridge.json().get("success"):
                                    t = res_bridge.json()["payload"]
                                    buy_usd, sell_usd = float(t["ask"]), float(t["bid"])
                                    b_rate = bitso_bridges.get(bridge, {"buy": 3900, "sell": 3900})
                                    buy_cop = buy_usd * b_rate["buy"]
                                    sell_cop = sell_usd * b_rate["sell"]
                                    spread = round(((buy_cop - sell_cop) / buy_cop) * 100, 2)
                                    save_price(cursor, "bitso", coin, buy_cop, sell_cop, buy_usd, sell_usd, spread, False, bridge)
                                    break
                    except Exception as e:
                        pass

                # ==========================================
                # LÓGICA PARA BUDA
                # ==========================================
                for coin in TARGET_COINS:
                    try:
                        direct_buda = f"{coin}-cop"
                        res_dir_buda = await client.get(f"https://www.buda.com/api/v2/markets/{direct_buda}/ticker")
                        if res_dir_buda.status_code == 200 and res_dir_buda.json().get("ticker") and res_dir_buda.json()["ticker"]["min_ask"]:
                            t = res_dir_buda.json()["ticker"]
                            buy_cop, sell_cop = float(t["min_ask"][0]), float(t["max_bid"][0])
                            bridge_used = "usdc"
                            bridge_rate = buda_bridges.get(bridge_used, {"buy": 3900, "sell": 3900})
                            buy_usd = buy_cop / bridge_rate["buy"]
                            sell_usd = sell_cop / bridge_rate["sell"]
                            spread = round(((buy_cop - sell_cop) / buy_cop) * 100, 2)
                            save_price(cursor, "buda", coin, buy_cop, sell_cop, buy_usd, sell_usd, spread, True, bridge_used)
                    except Exception as e:
                        pass
                
                conn.commit()
                conn.close()
                print("✅ Bases de datos (Criptos y Stablecoins) actualizadas.")
                
        except Exception as e:
            print(f"🚨 Error general en Fetcher: {e}")
        
        await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(run_fetcher())