import psycopg2

# --- SUAS CONFIGURAÇÕES DE BANCO LOCAL ---
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "recic3",
    "user": "postgres",
    "password": "postgres"
}

def remover_coluna_fantasma():
    print("--- LIMPANDO BANCO DE DADOS ---")
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # 1. Verifica se a coluna 'academia' existe
        cur.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name='fluxo_caixa' AND column_name='academia';
        """)
        if cur.fetchone():
            print(" > Coluna 'academia' encontrada. Removendo...")
            # Remove a coluna problemática
            cur.execute("ALTER TABLE fluxo_caixa DROP COLUMN academia;")
            conn.commit()
            print("✅ SUCESSO! A coluna 'academia' foi removida.")
        else:
            print("✅ A coluna 'academia' não existe. Nenhuma ação necessária.")
            
    except Exception as e:
        print(f"❌ ERRO: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    remover_coluna_fantasma()