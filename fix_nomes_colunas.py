import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def corrigir_nomes_colunas():
    print("--- 🔧 CORRIGINDO NOMES DAS COLUNAS ---")
    
    url = os.getenv("DATABASE_URL")
    conn = psycopg2.connect(url)
    cur = conn.cursor()
    
    try:
        # 1. Verifica e renomeia 'nome_subgrupo' para 'nome'
        print(" > Verificando tabela 'subgrupos'...")
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='subgrupos' AND column_name='nome_subgrupo'")
        if cur.fetchone():
            print("   Renomeando 'nome_subgrupo' para 'nome'...")
            cur.execute("ALTER TABLE subgrupos RENAME COLUMN nome_subgrupo TO nome;")
        else:
            print("   Coluna 'nome_subgrupo' não encontrada (ou já se chama 'nome').")

        # 2. Verifica e renomeia 'nome_grupo' para 'nome' (na tabela grupos_atividade)
        # Fazemos isso por precaução, pois o app provavelmente espera 'nome' aqui também
        print(" > Verificando tabela 'grupos_atividade'...")
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='grupos_atividade' AND column_name='nome_grupo'")
        if cur.fetchone():
            print("   Renomeando 'nome_grupo' para 'nome'...")
            cur.execute("ALTER TABLE grupos_atividade RENAME COLUMN nome_grupo TO nome;")
        else:
            print("   Coluna 'nome_grupo' não encontrada (ou já se chama 'nome').")

        conn.commit()
        print("✅ SUCESSO! Banco de dados alinhado com o App.")
        
    except Exception as e:
        print(f"❌ ERRO: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    corrigir_nomes_colunas()