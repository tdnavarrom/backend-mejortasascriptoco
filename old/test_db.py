import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# Cargar el .env
load_dotenv()

# Construir la URL igual que en tu código
user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT", "5432")
db_name = os.getenv("DB_NAME", "postgres")

# IMPORTANTE: Si la contraseña tiene caracteres especiales (@, #, /), 
# a veces hay que usar un formato específico, pero probemos así primero:
DATABASE_URL = f"postgresql://{user}:{password}@{host}:{port}/{db_name}"

def test_connection():
    try:
        # Crear motor de conexión
        engine = create_engine(DATABASE_URL)
        
        # Intentar una operación simple
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            version = result.fetchone()
            print("------------------------------------------")
            print("✅ ¡CONEXIÓN EXITOSA A SUPABASE!")
            print(f"Versión de Postgres: {version[0]}")
            print("------------------------------------------")
            
    except Exception as e:
        print("------------------------------------------")
        print("❌ ERROR DE CONEXIÓN:")
        print(f"Detalle: {e}")
        print("------------------------------------------")
        print("\n💡 Tips de solución:")
        print("1. Revisa que tu IP no esté bloqueada en Supabase (Settings > Database > Network Restrictions).")
        print("2. Verifica que la contraseña en el .env sea la del PROYECTO, no la de tu cuenta de Supabase.")
        print("3. Asegúrate de tener instalado: pip install psycopg2-binary")

if __name__ == "__main__":
    test_connection()