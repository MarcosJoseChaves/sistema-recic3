import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# Tenta pegar da nuvem, se falhar (estiver comentado), usa o local
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

    print("Criando tabela 'solicitacoes_alteracao'...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS solicitacoes_alteracao (
            id SERIAL PRIMARY KEY,
            tabela_alvo VARCHAR(50) NOT NULL, -- Ex: 'associados'
            id_registro INTEGER NOT NULL,     -- ID do associado que querem mexer
            tipo_solicitacao VARCHAR(20) NOT NULL, -- 'EDICAO' ou 'EXCLUSAO'
            dados_novos TEXT, -- Guardaremos os dados novos em formato JSON (texto)
            usuario_solicitante VARCHAR(100),
            data_solicitacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status VARCHAR(20) DEFAULT 'PENDENTE', -- PENDENTE, APROVADO, REJEITADO
            motivo_rejeicao TEXT
        );
    """)
    
    conn.commit()
    print("✅ SUCESSO! Tabela de solicitações criada.")

except Exception as e:
    print(f"❌ Erro: {e}")
finally:
    if 'conn' in locals() and conn: conn.close()