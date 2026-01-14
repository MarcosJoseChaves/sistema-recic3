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

        # 3. Checar tabelas de controle de EPIs
        print("\n--- TABELAS: controle de EPIs ---")
        tabelas_epi = [
            "epi_itens",
            "epi_estoque",
            "epi_entregas",
            "epi_entrega_itens"
        ]
        cur.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
        )
        tabelas_existentes = {row[0] for row in cur.fetchall()}
        tabelas_faltando = [t for t in tabelas_epi if t not in tabelas_existentes]
        for tabela in tabelas_epi:
            if tabela in tabelas_existentes:
                print(f"✅ Tabela '{tabela}': PRESENTE")
            else:
                print(f"❌ Tabela '{tabela}': AUSENTE (será criada agora)")

        if tabelas_faltando:
            print("\n🔧 Criando tabelas de EPIs ausentes...")
            cur.execute("""
                CREATE TABLE IF NOT EXISTS epi_itens (
                    id SERIAL PRIMARY KEY,
                    nome VARCHAR(255) NOT NULL UNIQUE,
                    categoria VARCHAR(100),
                    ca VARCHAR(50),
                    validade_meses INTEGER,
                    data_hora_cadastro TIMESTAMP NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS epi_estoque (
                    id SERIAL PRIMARY KEY,
                    id_item INTEGER NOT NULL REFERENCES epi_itens(id),
                    uvr VARCHAR(10) NOT NULL,
                    associacao VARCHAR(50),
                    unidade VARCHAR(50) NOT NULL,
                    quantidade DECIMAL(10, 3) NOT NULL DEFAULT 0,
                    data_hora_atualizacao TIMESTAMP NOT NULL,
                    UNIQUE (uvr, associacao, id_item)
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS epi_entregas (
                    id SERIAL PRIMARY KEY,
                    id_associado INTEGER NOT NULL REFERENCES associados(id),
                    uvr VARCHAR(10) NOT NULL,
                    associacao VARCHAR(50),
                    data_entrega DATE NOT NULL,
                    observacoes TEXT,
                    usuario_registro VARCHAR(50),
                    data_hora_registro TIMESTAMP NOT NULL
                )
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS epi_entrega_itens (
                    id SERIAL PRIMARY KEY,
                    id_entrega INTEGER NOT NULL REFERENCES epi_entregas(id) ON DELETE CASCADE,
                    id_item INTEGER NOT NULL REFERENCES epi_itens(id),
                    unidade VARCHAR(50) NOT NULL,
                    quantidade DECIMAL(10, 3) NOT NULL,
                    data_validade DATE
                )
            """)
            conn.commit()
            print("✅ Tabelas de EPIs criadas.")

            cur.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';"
            )
            tabelas_existentes = {row[0] for row in cur.fetchall()}
            for tabela in tabelas_epi:
                if tabela in tabelas_existentes:
                    print(f"✅ Tabela '{tabela}': PRESENTE")
                else:
                    print(f"❌ Tabela '{tabela}': AUSENTE (erro na criação)")

        conn.close()

    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")

if __name__ == "__main__":
    
    verificar_estrutura()