import secrets

import psycopg2
from dotenv import load_dotenv
from flask import Flask, jsonify, redirect, render_template, request, url_for
from flask_login import (
    LoginManager,
    UserMixin,
    login_required,
    login_user,
    logout_user,
)
from werkzeug.security import check_password_hash, generate_password_hash

from configuracao_ambiente import configurar_aplicacao
from logging_operacional import registrar_evento, resposta_erro_interno
from seguranca_csrf import configurar_csrf
from seguranca_rate_limit import aplicar_limites_rotas
from modulos.fiscalizacao_contratos import criar_blueprint_fiscalizacao


# ============================================================
# CARREGAMENTO E CONFIGURAÇÃO
# ============================================================

load_dotenv()

app = Flask(__name__)

configurar_aplicacao(app)
configurar_csrf(app)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = "login"
login_manager.login_message = "Faça login para acessar o sistema."

DATABASE_URL = app.config.get("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL não está configurada.")

# Mantém custo semelhante de verificação mesmo quando usuário não existe.
HASH_SENHA_FICTICIO = generate_password_hash(secrets.token_urlsafe(32))


# ============================================================
# BANCO
# ============================================================

def conectar_banco():
    return psycopg2.connect(DATABASE_URL)


# ============================================================
# USUÁRIO / FLASK-LOGIN
# ============================================================

class User(UserMixin):
    def __init__(self, id, username, role, uvr_acesso):
        self.id = id
        self.username = username
        self.role = role
        self.uvr_acesso = uvr_acesso


@login_manager.user_loader
def load_user(user_id):
    conn = conectar_banco()

    try:
        cur = conn.cursor()

        try:
            cur.execute(
                """
                SELECT id, username, role, uvr_acesso
                FROM usuarios
                WHERE id = %s
                  AND ativo = TRUE
                """,
                (user_id,),
            )

            data = cur.fetchone()

        finally:
            cur.close()

    finally:
        conn.close()

    if data:
        return User(
            id=data[0],
            username=data[1],
            role=data[2],
            uvr_acesso=data[3],
        )

    registrar_evento(
        "inactive_session_rejected",
        nivel="WARNING",
        mensagem="Sessão interna não reconhecida.",
        categoria_seguranca="authentication",
    )

    return None


# ============================================================
# LOGIN
# ============================================================

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""

        conn = conectar_banco()

        try:
            cur = conn.cursor()

            try:
                cur.execute(
                    """
                    SELECT
                        id,
                        username,
                        password_hash,
                        role,
                        uvr_acesso
                    FROM usuarios
                    WHERE username = %s
                      AND ativo = TRUE
                    """,
                    (username,),
                )

                user_data = cur.fetchone()

            finally:
                cur.close()

        finally:
            conn.close()

        hash_para_validar = (
            user_data[2]
            if user_data
            else HASH_SENHA_FICTICIO
        )

        senha_valida = check_password_hash(
            hash_para_validar,
            password,
        )

        if not user_data or not senha_valida:
            registrar_evento(
                "authentication_failed",
                nivel="WARNING",
                mensagem="Falha de autenticação.",
                categoria_seguranca="authentication",
            )

            return render_template(
                "fiscalizacao_login.html",
                erro="Usuário ou senha inválidos.",
            )

        user_obj = User(
            id=user_data[0],
            username=user_data[1],
            role=user_data[3],
            uvr_acesso=user_data[4],
        )

        login_user(user_obj)

        registrar_evento(
            "authentication_succeeded",
            mensagem="Autenticação concluída.",
            categoria_seguranca="authentication",
            actor_id=str(user_data[0]),
            actor_type="internal_user",
        )

        return redirect(url_for("index"))

    return render_template("fiscalizacao_login.html")


@app.post("/logout")
@login_required
def logout():
    logout_user()
    return redirect(url_for("login"))


# ============================================================
# RAIZ DO SISTEMA
# ============================================================

@app.get("/")
@login_required
def index():
    return redirect(
        url_for("fiscalizacao_contratos.painel")
    )


# ============================================================
# HEALTH CHECK
# ============================================================

@app.get("/health")
def health():
    return jsonify(
        {
            "status": "ok",
            "sistema": "fiscalizacao-contratos",
        }
    )


# ============================================================
# ERRO INTERNO
# ============================================================

@app.errorhandler(500)
def tratar_erro_interno(erro):
    return resposta_erro_interno(erro)


# ============================================================
# MÓDULO FISCALIZAÇÃO DE CONTRATOS
# ============================================================

app.register_blueprint(
    criar_blueprint_fiscalizacao(conectar_banco),
    url_prefix="/fiscalizacao-contratos",
)


# Aplica os limites específicos somente depois de todas
# as rotas estarem registradas.
aplicar_limites_rotas(app)


if __name__ == "__main__":
    app.run()

