import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Pega a URL que você acabou de destravar no .env
DATABASE_URL = os.getenv('DATABASE_URL')

try:
    print("☁️ Conectando à NUVEM (Neon)...")
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()

    print("🔧 Verificando/Criando coluna 'foto_base64'...")
    cur.execute("ALTER TABLE associados ADD COLUMN IF NOT EXISTS foto_base64 TEXT;")
    
    conn.commit()
    print("✅ SUCESSO! A nuvem agora aceita fotos.")

except Exception as e:
    print(f"❌ Erro: {e}")
finally:
    if 'conn' in locals() and conn: conn.close()