"""Fábrica explícita de aplicação de teste. Não importa app.py nem abre servidor."""

from pathlib import Path
from uuid import uuid4

from flask import Flask, redirect, render_template, url_for
from flask_login import LoginManager, UserMixin, logout_user
from flask_wtf.csrf import CSRFError
from werkzeug.exceptions import HTTPException

from seguranca_csrf import configurar_csrf
from . import criar_blueprint_esg
from .demonstracao import AVISO_DEMONSTRACAO, RepositorioDemonstrativoESG
from .executivo import montar_visao_executiva
from .repositorio import PersistenciaESGError
from .validacoes import ContratoESGInvalido, exigir_tipo
from modulos.fiscalizacao_contratos.permissions import admin_required


def criar_aplicacao_demonstrativa(repositorio):
    """Somente test_client, com dependência fornecida explicitamente; sem carga implícita."""
    exigir_tipo(repositorio, RepositorioDemonstrativoESG, "repositorio")
    raiz = Path(__file__).resolve().parents[2]
    app = Flask("esg_demonstracao_offline", root_path=str(raiz), template_folder=str(raiz / "templates"))
    app.config.update(TESTING=True, SECRET_KEY=uuid4().hex)
    app.extensions["esg_demonstracao_offline"] = True
    configurar_csrf(app)
    manager = LoginManager(app)
    manager.login_view = "login"

    class AdministradorFicticio(UserMixin):
        id = 1
        role = "admin"

    @manager.user_loader
    def carregar_usuario(identificador):
        return AdministradorFicticio() if identificador == "1" else None

    @app.context_processor
    def aviso():
        return {"aviso_demonstracao": AVISO_DEMONSTRACAO}

    @app.errorhandler(CSRFError)
    def csrf_invalido(_erro):
        return render_template("esg/erro.html", mensagem="Solicitação demonstrativa recusada pela validação CSRF."), 400

    @app.errorhandler(HTTPException)
    def erro_http(erro):
        return render_template("esg/erro.html", mensagem="Solicitação demonstrativa indisponível ou não autorizada."), erro.code

    @app.get("/")
    def index():
        return redirect(url_for("esg.painel"))

    @app.get("/login")
    def login():
        return render_template("esg/demonstracao_login.html")

    @app.post("/logout")
    def logout():
        logout_user()
        return redirect(url_for("login"), code=303)

    app.register_blueprint(criar_blueprint_esg(repositorio), url_prefix="/esg")

    @app.get("/esg/demonstracao/executiva")
    @admin_required
    def executiva_demonstrativa():
        from .routes import ROTULOS

        try:
            visao = montar_visao_executiva(repositorio)
        except (PersistenciaESGError, ContratoESGInvalido):
            return render_template("esg/executiva.html", visao=None,
                                   erro="Não foi possível consultar os dados demonstrativos."), 503
        return render_template("esg/executiva.html", visao=visao, rotulos_esg=ROTULOS)

    return app


def autenticar_administrador_demonstrativo(cliente):
    """Fixture explícita de sessão assinada, sem rota que conceda acesso ou credenciais."""
    if not cliente.application.testing or cliente.application.extensions.get("esg_demonstracao_offline") is not True:
        raise ValueError("Autenticação fictícia restrita à aplicação demonstrativa de teste.")
    with cliente.session_transaction() as sessao:
        sessao.clear()
        sessao["_user_id"] = "1"
        sessao["_fresh"] = True
