import psycopg2
import os
from dotenv import load_dotenv

load_dotenv() # Carrega a senha do Neon do seu .env

def verificar_estrutura():
    print("🔌 Conectando no NEON para checar a estrutura das tabelas...\n")
    
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()

        # 1. Checar tabela FLUXO_CAIXA
        print("--- TABELA: fluxo_caixa ---")
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'fluxo_caixa';")
        colunas_fluxo = [row[0] for row in cur.fetchall()]
        
        # Verificações críticas
        if 'associacao' in colunas_fluxo:
            print("✅ Coluna 'associacao': PRESENTE (Correto)")
        else:
            print("❌ Coluna 'associacao': AUSENTE (Precisa rodar fix_fluxo.py)")

        if 'academia' in colunas_fluxo:
            print("❌ Coluna 'academia': PRESENTE (Precisa rodar fix_coluna_fantasma.py)")
        else:
            print("✅ Coluna 'academia': AUSENTE (Correto)")

        # 2. Checar tabela PRODUTOS_SERVICOS (Catálogo)
        print("\n--- TABELA: produtos_servicos ---")
        cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'produtos_servicos';")
        colunas_prod = [row[0] for row in cur.fetchall()]
        
        if 'id_subgrupo' in colunas_prod:
             print("✅ Coluna 'id_subgrupo': PRESENTE (Correto)")
        else:
             print("⚠️ Coluna 'id_subgrupo': AUSENTE (Talvez precise rodar atualizar_padrao_v2.py)")

        conn.close()

    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")

if __name__ == "__main__":
    verificar_estrutura()