from fastapi import FastAPI, HTTPException, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
import datetime
import json
import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

# ==========================================
# 🔌 CONFIGURACIÓN DE POSTGRES (SUPABASE)
# ==========================================
# Priorizamos la variable de entorno de Render, si no, usamos tu URI
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://postgres:TDNMunera_06*@db.emxfimcwvsikzmlshuqh.supabase.co:5432/postgres")

# Corrección necesaria para compatibilidad con SQLAlchemy
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True)

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
ADMIN_TOKEN = "crypto_spread_secret_token_2026"

# Modelos Pydantic
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
            # Postgres puede devolver dict o string según el driver
            if isinstance(p["manual_prices"], str):
                p["manual_prices"] = json.loads(p["manual_prices"])
            platforms[p["id"]] = p
        return platforms

@app.get("/api/prices/{coin}")
def get_prices(coin: str):
    coin = coin.lower()
    results = []
    
    with engine.connect() as conn:
        # 1. Obtener plataformas activas
        active_query = text("SELECT * FROM platform_info WHERE is_active = true")
        platforms = [dict(r._mapping) for r in conn.execute(active_query)]
        active_ids = [p["id"] for p in platforms]
        
        # 2. Precios Automáticos
        table = "stablecoin_prices" if coin in STABLE_COINS else "crypto_prices"
        # Usamos :coin (parámetro nombrado de Postgres) en vez de ?
        price_query = text(f"SELECT * FROM {table} WHERE coin = :coin")
        prices = conn.execute(price_query, {"coin": coin})
        
        for r in prices:
            row = dict(r._mapping)
            if row["exchange"] in active_ids:
                results.append(row)
                
        # 3. Inyectar Manuales
        for p in platforms:
            if p["is_manual"]:
                m_prices = p["manual_prices"]
                if isinstance(m_prices, str): m_prices = json.loads(m_prices)
                
                # Ejemplo lógica USDC/USDT para Fintech
                if p["category"] == "fintech" and coin in ["usdc", "usdt"]:
                    if m_prices.get("usd", {}).get("active"):
                        results.append({
                            "exchange": p["id"], "buy_cop": m_prices["usd"]["buy"], 
                            "sell_cop": m_prices["usd"]["sell"], "buy_usd": 1.0, 
                            "sell_usd": 1.0, "spread": 0, "direct_cop": True, "usd_bridge": ""
                        })
    
    return {"coin": coin, "prices": results}

@app.post("/api/admin/platforms", dependencies=[Depends(verify_admin)])
def save_platform(data: PlatformUpdate):
    with engine.begin() as conn:
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Sintaxis UPSERT para PostgreSQL
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