from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional, List, Dict
import datetime
import json
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

# ==========================================
# 🔌 CONFIGURACIÓN DE POSTGRES (SUPABASE)
# ==========================================
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "postgres")

# Importante: Algunos despliegues de Render/Supabase requieren sslmode=require
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

engine = create_engine(DATABASE_URL, pool_pre_ping=True) # pool_pre_ping evita conexiones muertas
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # En producción, cámbialo por tu dominio de Vercel
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

CRYPTO_COINS = ["btc", "bch", "eth", "sol", "ltc"]
STABLE_COINS = ["usdt", "usdc", "euroc"]

# ==========================================
# 🔐 CONFIGURACIÓN DE SEGURIDAD
# ==========================================
ADMIN_USER = os.getenv("ADMIN_USER", "m4cc1")
ADMIN_PASS = os.getenv("ADMIN_PASS", "TDNMunera_06*")
ADMIN_TOKEN = os.getenv("ADMIN_TOKEN", "crypto_spread_secret_token_2026")

class LoginRequest(BaseModel):
    username: str
    password: str

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

@app.post("/api/login")
def login(req: LoginRequest):
    if req.username == ADMIN_USER and req.password == ADMIN_PASS:
        return {"token": ADMIN_TOKEN}
    raise HTTPException(status_code=401, detail="Credenciales incorrectas")

def verify_admin(token: str = Header(None)):
    if token != ADMIN_TOKEN:
        raise HTTPException(status_code=401, detail="No autorizado.")

# ==========================================
# 🛣️ RUTAS DE LA API
# ==========================================

@app.get("/api/config")
def get_config(): 
    return {"crypto": CRYPTO_COINS, "stablecoins": STABLE_COINS}

@app.get("/api/platforms")
def get_platforms(all: bool = False):
    with engine.connect() as conn:
        query = "SELECT * FROM platform_info" if all else "SELECT * FROM platform_info WHERE is_active = true"
        result = conn.execute(text(query))
        
        platforms = {}
        for r in result:
            p = dict(r._mapping)
            if isinstance(p["manual_prices"], str):
                p["manual_prices"] = json.loads(p["manual_prices"])
            platforms[p["id"]] = p
        return platforms

@app.get("/api/prices/{coin}")
def get_prices(coin: str):
    coin = coin.lower()
    results = []
    
    # Función para intentar convertir a float, si falla devuelve el original (ej: "N.D.")
    def parse_price(value):
        try:
            return float(value)
        except (ValueError, TypeError):
            return value

    with engine.connect() as conn:
        # 1. Obtener plataformas activas
        active_query = text("SELECT * FROM platform_info WHERE is_active = true")
        platforms = [dict(r._mapping) for r in conn.execute(active_query)]
        active_ids = [p["id"] for p in platforms]
        
        # 2. Precios Automáticos
        table = "stablecoin_prices" if coin in STABLE_COINS else "crypto_prices"
        try:
            price_query = text(f"SELECT * FROM {table} WHERE coin = :coin")
            prices = conn.execute(price_query, {"coin": coin})
            for r in prices:
                row = dict(r._mapping)
                if row["exchange"] in active_ids:
                    results.append(row)
        except Exception as e:
            print(f"Error: {e}")

        # 3. Inyectar Manuales (Incluyendo Lulox con N.D.)
        for p in platforms:
            if p["is_manual"]:
                m_prices = p["manual_prices"]
                if isinstance(m_prices, str): m_prices = json.loads(m_prices)
                
                target_data = m_prices.get(coin)
                if not target_data and p["category"] == "fintech":
                    if coin in ["usdc", "usdt"]: target_data = m_prices.get("usd")
                    elif coin == "euroc": target_data = m_prices.get("eur")

                if target_data and target_data.get("active"):
                    # Obtenemos el valor tal cual (puede ser 4000 o "N.D.")
                    buy_val = parse_price(target_data.get("buy"))
                    sell_val = parse_price(target_data.get("sell"))
                    
                    is_usd = target_data.get("currency") == "USD"
                    
                    # Lógica de conversión SOLO si son números
                    if is_usd and isinstance(buy_val, (int, float)):
                        internal_stable = m_prices.get("usdc") or m_prices.get("usd") or m_prices.get("usdt")
                        if internal_stable and internal_stable.get("active"):
                            trm_i_buy = parse_price(internal_stable.get("buy"))
                            trm_i_sell = parse_price(internal_stable.get("sell"))
                            
                            if isinstance(trm_i_buy, (int, float)):
                                buy_val = buy_val * trm_i_buy
                                sell_val = sell_val * trm_i_sell

                    # Evitar duplicados y añadir a resultados
                    if not any(res["exchange"] == p["id"] for res in results):
                        results.append({
                            "exchange": p["id"], 
                            "buy_cop": buy_val, 
                            "sell_cop": sell_val, 
                            "buy_usd": target_data.get("buy") if is_usd else 0,
                            "sell_usd": target_data.get("sell") if is_usd else 0, 
                            "spread": 0, # No se puede calcular spread con "N.D."
                            "direct_cop": not is_usd,
                            "usd_bridge": "USDC" if is_usd else ""
                        })
    
    return {"coin": coin, "prices": results}

@app.post("/api/admin/platforms", dependencies=[Depends(verify_admin)])
def save_platform(data: PlatformUpdate):
    with engine.begin() as conn:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        query = text("""
            INSERT INTO platform_info 
            (id, name, category, logo_url, funding, trading, withdraw, deposit_networks, withdraw_networks, manual_prices, is_manual, is_active, last_updated)
            VALUES (:id, :name, :category, :logo_url, :funding, :trading, :withdraw, :dn, :wn, :mp, :im, :ia, :lu)
            ON CONFLICT (id) DO UPDATE SET
            name=EXCLUDED.name, category=EXCLUDED.category, logo_url=EXCLUDED.logo_url,
            funding=EXCLUDED.funding, trading=EXCLUDED.trading, withdraw=EXCLUDED.withdraw,
            deposit_networks=EXCLUDED.deposit_networks, withdraw_networks=EXCLUDED.withdraw_networks,
            manual_prices=EXCLUDED.manual_prices, is_manual=EXCLUDED.is_manual,
            is_active=EXCLUDED.is_active, last_updated=EXCLUDED.last_updated
        """)
        conn.execute(query, {
            "id": data.id.lower(), "name": data.name, "category": data.category,
            "logo_url": data.logo_url, "funding": data.funding, "trading": data.trading,
            "withdraw": data.withdraw, "dn": data.deposit_networks, "wn": data.withdraw_networks,
            "mp": json.dumps(data.manual_prices), "im": data.is_manual, "ia": data.is_active, "lu": now
        })
    return {"status": "success"}

@app.delete("/api/admin/platforms/{platform_id}", dependencies=[Depends(verify_admin)])
def delete_platform(platform_id: str):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM platform_info WHERE id = :id"), {"id": platform_id.lower()})
    return {"status": "success"}