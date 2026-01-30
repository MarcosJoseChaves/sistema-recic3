import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')
LOCAL_DB = "dbname=recic3 user=postgres password=postgres host=localhost port=5432"

try:
    if DATABASE_URL:
        print("☁️ Conectando à NUVEM/Definido no .env...")
        conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    else:
        print("🔧 Conectando ao LOCAL (recic3)...")
        conn = psycopg2.connect(LOCAL_DB)

    cur = conn.cursor()

    print("Criando tabela 'epis_catalogo'...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS epis_catalogo (
            id SERIAL PRIMARY KEY,
            grupo VARCHAR(255) NOT NULL,
            epi VARCHAR(255) NOT NULL,
            ca VARCHAR(50),
            tipo_protecao VARCHAR(255),
            funcao VARCHAR(100),
            tempo_troca VARCHAR(50),
            data_hora_cadastro TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (grupo, epi, ca, funcao)
        )
    """)

    conn.commit()
    print("✅ SUCESSO! Tabela de EPIs criada.")

except Exception as e:
    print(f"❌ Erro: {e}")
finally:
    if 'conn' in locals() and conn:
        conn.close()