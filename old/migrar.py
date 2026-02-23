import os
from dotenv import load_dotenv
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
load_dotenv()

def migrate():
    # Obtiene la URL armada igual que en el paso anterior
    DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    
    sqlite_conn = sqlite3.connect('crypto_spread.db')
    pg_engine = create_engine(DATABASE_URL)

    # Obtener lista de tablas de tu SQLite
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()

    for table_name in tables:
        name = table_name[0]
        if name == 'sqlite_sequence': continue # Ignorar tablas internas
        
        print(f"Migrando tabla: {name}...")
        # Leer datos de SQLite
        df = pd.read_sql_query(f"SELECT * FROM {name}", sqlite_conn)
        # Escribir en Postgres (Supabase)
        df.to_sql(name, pg_engine, if_exists='replace', index=False)
        print(f"✅ Tabla {name} migrada con éxito.")

    sqlite_conn.close()
    print("\n🚀 ¡Migración completada!")

if __name__ == "__main__":
    migrate()