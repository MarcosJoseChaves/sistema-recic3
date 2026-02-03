from werkzeug.security import generate_password_hash
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL')

if not DATABASE_URL:
    print("ERRO: DATABASE_URL não encontrada no arquivo .env")
    exit()

visitantes = [
    {
        "username": "Visitante01",
        "senha": "visitante123",
        "uvr_acesso": "UVR 01",
        "nome_completo": "Visitante UVR 01",
    },
    {
        "username": "Visitante02",
        "senha": "visitante123",
        "uvr_acesso": "UVR 02",
        "nome_completo": "Visitante UVR 02",
    },
]

try:
    print("Conectando ao banco na nuvem para criar usuários visitantes...")
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    cur = conn.cursor()

    for visitante in visitantes:
        senha_hash = generate_password_hash(visitante["senha"])
        cur.execute("SELECT id FROM usuarios WHERE username = %s", (visitante["username"],))
        if cur.fetchone():
            print(f"Usuário '{visitante['username']}' já existe. Atualizando permissões...")
            cur.execute(
                """
                UPDATE usuarios
                SET password_hash = %s, role = 'visitante', uvr_acesso = %s, ativo = TRUE, nome_completo = %s
                WHERE username = %s
                """,
                (
                    senha_hash,
                    visitante["uvr_acesso"],
                    visitante["nome_completo"],
                    visitante["username"],
                ),
            )
        else:
            print(f"Criando novo usuário '{visitante['username']}'...")
            cur.execute(
                """
                INSERT INTO usuarios (username, password_hash, nome_completo, role, uvr_acesso, ativo)
                VALUES (%s, %s, %s, %s, %s, TRUE)
                """,
                (
                    visitante["username"],
                    senha_hash,
                    visitante["nome_completo"],
                    "visitante",
                    visitante["uvr_acesso"],
                ),
            )

    conn.commit()
    print("-" * 30)
    print("SUCESSO! Usuários visitantes criados/atualizados.")
    for visitante in visitantes:
        print(f"Login: {visitante['username']}")
        print(f"Senha: {visitante['senha']}")
        print(f"Acesso restrito a: {visitante['uvr_acesso']}")
        print("-" * 30)

except Exception as e:
    print(f"Erro: {e}")
    if 'conn' in locals() and conn:
        conn.rollback()
finally:
    if 'conn' in locals() and conn:
        conn.close()