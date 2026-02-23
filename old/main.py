from fastapi import FastAPI, Form, File, UploadFile, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional # <--- ¡ESTA ES LA LÍNEA QUE FALTABA!
import sqlite3
import datetime
import json
import smtplib
from email.message import EmailMessage


DB_FILE = "crypto_spread.db"
POSTGRES_URI = 'postgresql://postgres:TDNMunera_06*@db.emxfimcwvsikzmlshuqh.supabase.co:5432/postgres'

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
   allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CRYPTO_COINS = ["btc", "bch", "eth", "sol", "ltc"]
STABLE_COINS = ["usdt", "usdc", "euroc"]

# ==========================================
# 🔐 CONFIGURACIÓN DE SEGURIDAD
# ==========================================
ADMIN_USER = "m4cc1"
ADMIN_PASS = "TDNMunera_06*"
ADMIN_TOKEN = "crypto_spread_secret_token_2026"

class LoginRequest(BaseModel):
    username: str
    password: str

@app.post("/api/login")
def login(req: LoginRequest):
    if req.username == ADMIN_USER and req.password == ADMIN_PASS:
        return {"token": ADMIN_TOKEN}
    raise HTTPException(status_code=401, detail="Credenciales incorrectas")

def verify_admin(token: str = Header(None)):
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="No autorizado.")

# ==========================================
# BASE DE DATOS
# ==========================================
def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS platform_info (
            id TEXT PRIMARY KEY, name TEXT, category TEXT, logo_url TEXT,
            funding TEXT, trading TEXT, withdraw TEXT, deposit_networks TEXT,
            withdraw_networks TEXT, manual_prices TEXT, 
            is_manual BOOLEAN, is_active BOOLEAN, last_updated TEXT
        )
    ''')
    conn.commit()
    conn.close()

init_db()

class PlatformUpdate(BaseModel):
    id: str
    name: str
    category: str
    logo_url: str
    funding: str
    trading: str
    withdraw: str
    deposit_networks: str
    withdraw_networks: str
    manual_prices: dict
    is_manual: bool
    is_active: bool

# ==========================================
# RUTAS DE LA API
# ==========================================
@app.get("/api/config")
def get_config(): return {"crypto": CRYPTO_COINS, "stablecoins": STABLE_COINS}

@app.get("/api/platforms")
def get_platforms(all: bool = False):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM platform_info" if all else "SELECT * FROM platform_info WHERE is_active = 1")
    
    platforms = {}
    for r in cursor.fetchall():
        p = dict(r)
        p["manual_prices"] = json.loads(p["manual_prices"]) if p.get("manual_prices") else {}
        platforms[p["id"]] = p
    conn.close()
    return platforms

@app.get("/api/prices/{coin}")
def get_prices(coin: str):
    coin = coin.lower()
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    results = []
    
    cursor.execute("SELECT * FROM platform_info WHERE is_active = 1")
    platforms = cursor.fetchall()
    active_ids = [p["id"] for p in platforms]
    
    # 1. Obtener precios automáticos
    if coin in STABLE_COINS:
        cursor.execute("SELECT exchange, buy_cop, sell_cop, buy_usd, sell_usd FROM stablecoin_prices WHERE coin = ?", (coin,))
        for r in cursor.fetchall():
            if r["exchange"] in active_ids:
                results.append({"exchange": r["exchange"], "buy_cop": r["buy_cop"], "sell_cop": r["sell_cop"], "buy_usd": r["buy_usd"], "sell_usd": r["sell_usd"], "spread": 0, "direct_cop": True, "usd_bridge": ""})
    else:
        cursor.execute("SELECT exchange, buy_cop, sell_cop, buy_usd, sell_usd, spread, direct_cop, usd_bridge FROM crypto_prices WHERE coin = ?", (coin,))
        for r in cursor.fetchall():
            if r["exchange"] in active_ids:
                results.append(dict(r))
                
    # 2. INYECTAR PRECIOS MANUALES (Con lógica de conversión USD -> COP)
    for p in platforms:
        if p["is_manual"]:
            m_prices = json.loads(p["manual_prices"]) if p["manual_prices"] else {}
            
            if p["category"] == "fintech":
                if coin in ["usdc", "usdt"] and m_prices.get("usd", {}).get("active"):
                    results.append({"exchange": p["id"], "buy_cop": m_prices["usd"]["buy"], "sell_cop": m_prices["usd"]["sell"], "buy_usd": 1.0, "sell_usd": 1.0, "spread": 0, "direct_cop": True, "usd_bridge": ""})
                elif coin == "euroc" and m_prices.get("eur", {}).get("active"):
                    results.append({"exchange": p["id"], "buy_cop": m_prices["eur"]["buy"], "sell_cop": m_prices["eur"]["sell"], "buy_usd": 1.0, "sell_usd": 1.0, "spread": 0, "direct_cop": True, "usd_bridge": ""})
            
            elif p["category"] == "exchange":
                if coin in m_prices and m_prices[coin].get("active"):
                    buy_val = m_prices[coin].get("buy", "N.D.")
                    sell_val = m_prices[coin].get("sell", "N.D.")
                    currency = m_prices[coin].get("currency", "COP")
                    
                    buy_cop, sell_cop = buy_val, sell_val
                    buy_usd, sell_usd = 1.0, 1.0
                    direct_cop = True
                    usd_bridge = ""

                    # Si el admin lo ingresó en USD, lo convertimos a COP usando la stablecoin del mismo exchange
                    if currency == "USD" and coin not in STABLE_COINS:
                        direct_cop = False
                        usd_bridge = "usd" # Por defecto si no tiene stablecoins activas
                        bridge_buy, bridge_sell = 1.0, 1.0
                        
                        # Prioridad 1: USDC
                        if m_prices.get("usdc", {}).get("active"):
                            usd_bridge = "usdc"
                            bridge_buy = m_prices["usdc"].get("buy", 1.0)
                            bridge_sell = m_prices["usdc"].get("sell", 1.0)
                        # Prioridad 2: USDT
                        elif m_prices.get("usdt", {}).get("active"):
                            usd_bridge = "usdt"
                            bridge_buy = m_prices["usdt"].get("buy", 1.0)
                            bridge_sell = m_prices["usdt"].get("sell", 1.0)

                        try:
                            # Multiplicamos los dólares por el precio de la stablecoin en COP
                            buy_cop = float(buy_val) * float(bridge_buy) if (buy_val != "N.D." and bridge_buy != "N.D.") else "N.D."
                            sell_cop = float(sell_val) * float(bridge_sell) if (sell_val != "N.D." and bridge_sell != "N.D.") else "N.D."
                            buy_usd = float(buy_val) if buy_val != "N.D." else "N.D."
                            sell_usd = float(sell_val) if sell_val != "N.D." else "N.D."
                        except:
                            buy_cop, sell_cop = "N.D.", "N.D."

                    results.append({
                        "exchange": p["id"], 
                        "buy_cop": buy_cop, "sell_cop": sell_cop, 
                        "buy_usd": buy_usd, "sell_usd": sell_usd, 
                        "spread": 0, "direct_cop": direct_cop, "usd_bridge": usd_bridge
                    })

    conn.close()
    return {"coin": coin, "prices": results}

@app.post("/api/admin/platforms", dependencies=[Depends(verify_admin)])
def save_platform(data: PlatformUpdate):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute("""
        INSERT OR REPLACE INTO platform_info 
        (id, name, category, logo_url, funding, trading, withdraw, deposit_networks, withdraw_networks, manual_prices, is_manual, is_active, last_updated)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (data.id.lower(), data.name, data.category, data.logo_url, data.funding, data.trading, data.withdraw, 
          data.deposit_networks, data.withdraw_networks, json.dumps(data.manual_prices), data.is_manual, data.is_active, now))
    conn.commit()
    conn.close()
    return {"status": "success"}

@app.delete("/api/admin/platforms/{platform_id}", dependencies=[Depends(verify_admin)])
def delete_platform(platform_id: str):
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("DELETE FROM platform_info WHERE id = ?", (platform_id.lower(),))
    conn.commit()
    conn.close()
    return {"status": "success"}