from werkzeug.security import generate_password_hash
import psycopg2
import os
from dotenv import load_dotenv

# Carrega as configurações do arquivo .env (onde está o link do Neon)
load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    print("ERRO: DATABASE_URL não encontrada no arquivo .env")
    exit()

try:
    print("Conectando ao banco na nuvem para criar usuário UVR...")
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()

    # --- DADOS DO USUÁRIO DE TESTE ---
    novo_user = 'uvr01'
    nova_senha = 'user123'
    uvr_vinculada = 'UVR 01' # Esse usuário só vai ver dados da UVR 01
    
    senha_hash = generate_password_hash(nova_senha)

    # Verifica se já existe
    cur.execute("SELECT id FROM usuarios WHERE username = %s", (novo_user,))
    if cur.fetchone():
        print(f"Usuário '{novo_user}' já existe. Atualizando permissões...")
        cur.execute("""
            UPDATE usuarios 
            SET password_hash = %s, role = 'user', uvr_acesso = %s, ativo = TRUE
            WHERE username = %s
        """, (senha_hash, uvr_vinculada, novo_user))
    else:
        print(f"Criando novo usuário '{novo_user}'...")
        cur.execute("""
            INSERT INTO usuarios (username, password_hash, nome_completo, role, uvr_acesso, ativo)
            VALUES (%s, %s, %s, %s, %s, TRUE)
        """, (novo_user, senha_hash, 'Responsável UVR 01', 'user', uvr_vinculada))

    conn.commit()
    print("-" * 30)
    print(f"SUCESSO! Usuário criado/atualizado.")
    print(f"Login: {novo_user}")
    print(f"Senha: {nova_senha}")
    print(f"Acesso restrito a: {uvr_vinculada}")
    print("-" * 30)

except Exception as e:
    print(f"Erro: {e}")
    if 'conn' in locals() and conn:
        conn.rollback()
finally:
    if 'conn' in locals() and conn:
        conn.close()