import psycopg2

# --- SUAS CONFIGURAÇÕES DE BANCO LOCAL ---
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "recic3",
    "user": "postgres",
    "password": "postgres"
}

def corrigir_tabela_fluxo():
    print("--- CORRIGINDO TABELA FLUXO_CAIXA ---")
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Comando para adicionar a coluna que falta
        print(" > Adicionando coluna 'associacao'...")
        cur.execute("ALTER TABLE fluxo_caixa ADD COLUMN IF NOT EXISTS associacao VARCHAR(50);")
        
        conn.commit()
        print("✅ SUCESSO! A coluna foi adicionada.")
        
    except Exception as e:
        print(f"❌ ERRO: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    corrigir_tabela_fluxo()