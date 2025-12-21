from werkzeug.security import generate_password_hash
import psycopg2
import os
from dotenv import load_dotenv

# Carrega as configurações do arquivo .env
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    print("ERRO: DATABASE_URL não encontrada no arquivo .env")
    exit()

try:
    print("Conectando ao banco de dados na nuvem (Neon)...")
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()

    # --- PASSO DE SEGURANÇA: CRIA A TABELA SE ELA NÃO EXISTIR ---
    print("Verificando/Criando tabela de usuários...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS usuarios (
            id SERIAL PRIMARY KEY,
            username VARCHAR(50) UNIQUE NOT NULL,
            password_hash VARCHAR(255) NOT NULL,
            nome_completo VARCHAR(100),
            role VARCHAR(20) NOT NULL,
            uvr_acesso VARCHAR(50),
            ativo BOOLEAN DEFAULT TRUE
        )
    """)
    conn.commit() # Salva a criação da tabela
    print("Tabela 'usuarios' garantida com sucesso.")
    # ------------------------------------------------------------

    user = 'admin'
    senha_plana = 'admin123' 
    senha_hash = generate_password_hash(senha_plana)

    # Verifica se o usuário já existe
    cur.execute("SELECT id FROM usuarios WHERE username = %s", (user,))
    existente = cur.fetchone()

    if existente:
        print(f"O usuário '{user}' já existe. Atualizando a senha...")
        cur.execute("""
            UPDATE usuarios 
            SET password_hash = %s, role = 'admin', ativo = TRUE, uvr_acesso = NULL
            WHERE username = %s
        """, (senha_hash, user))
        print(f"Senha do usuário '{user}' atualizada para: {senha_plana}")
    else:
        print(f"Criando novo usuário '{user}'...")
        cur.execute("""
            INSERT INTO usuarios (username, password_hash, nome_completo, role, uvr_acesso, ativo)
            VALUES (%s, %s, %s, %s, %s, TRUE)
        """, (user, senha_hash, 'Administrador do Sistema', 'admin', None))
        print(f"Usuário '{user}' criado com sucesso! Senha: {senha_plana}")

    conn.commit()

except Exception as e:
    print(f"ERRO CRÍTICO: {e}")
    if 'conn' in locals() and conn:
        conn.rollback()

finally:
    if 'conn' in locals() and conn:
        conn.close()
        print("Conexão fechada.")