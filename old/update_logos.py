import sqlite3

DB_FILE = "crypto_spread.db"

# Diccionario con los nuevos logos en formato SVG
nuevos_logos = {
    "buda": "https://blog.buda.com/content/images/2025/04/buda-logo-white-1.svg",
    "bitso": "https://bitso.com/__next/_next/static/media/bitso.5262ce2d.svg",
    "global66": "https://www.global66.com/blog/wp-content/uploads/2022/03/logo_desktop.svg",
    "dolarapp": "https://www.dolarapp.com/_astro/dolarapp-logo.4GTkISB0.svg",
    "plenti": "https://cdn.prod.website-files.com/6697e29d92e2b75be213df4c/669a8987bfcc824265f6195c_Logo-white.svg"
}

def actualizar_logos():
    try:
        # Nos conectamos a tu base de datos actual
        conn = sqlite3.connect(DB_FILE)
        cursor = conn.cursor()

        # Recorremos el diccionario y actualizamos solo la columna 'logo_url'
        for platform_id, logo_url in nuevos_logos.items():
            cursor.execute(
                "UPDATE platform_info SET logo_url = ? WHERE id = ?", 
                (logo_url, platform_id)
            )
            print(f"✅ Logo de {platform_id.upper()} actualizado.")

        # Guardamos los cambios
        conn.commit()
        conn.close()
        print("\n🎉 ¡Todos los logos fueron actualizados con éxito! Tus datos manuales están a salvo.")
        
    except Exception as e:
        print(f"❌ Ocurrió un error: {e}")

if __name__ == "__main__":
    actualizar_logos()