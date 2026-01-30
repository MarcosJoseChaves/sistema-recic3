import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def forcar_remocao():
    print("--- 🔨 INICIANDO REMOÇÃO FORÇADA ---")
    
    url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(url)
    cur = conn.cursor()
    
    try:
        # 1. Tenta remover 'academia' (minúsculo)
        print(" > Tentando DROP COLUMN IF EXISTS academia...")
        cur.execute("ALTER TABLE fluxo_caixa DROP COLUMN IF EXISTS academia;")
        
        # 2. Tenta remover 'Academia' (maiúsculo - por garantia)
        print(" > Tentando DROP COLUMN IF EXISTS \"Academia\"...")
        cur.execute('ALTER TABLE fluxo_caixa DROP COLUMN IF EXISTS "Academia";')

        conn.commit()
        print("✅ SUCESSO! Comandos de remoção executados.")
        
    except Exception as e:
        print(f"❌ ERRO: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    forcar_remocao()