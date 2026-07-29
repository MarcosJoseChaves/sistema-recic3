"""Proteção CSRF centralizada para toda a aplicação Flask."""

from flask import render_template
from flask_wtf import CSRFProtect
from flask_wtf.csrf import CSRFError

from logging_operacional import registrar_evento


csrf = CSRFProtect()


def configurar_csrf(app):
    """Habilita CSRF em todos os ambientes e registra uma resposta amigável."""
    app.config["WTF_CSRF_ENABLED"] = True
    csrf.init_app(app)

    @app.errorhandler(CSRFError)
    def tratar_erro_csrf(_erro):
        registrar_evento(
            "csrf_rejected",
            nivel="WARNING",
            mensagem="Solicitação recusada pela validação CSRF.",
            categoria_seguranca="csrf",
            status_code=400,
        )
        return render_template("erro_csrf.html"), 400
