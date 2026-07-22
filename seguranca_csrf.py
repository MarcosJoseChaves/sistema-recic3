"""Proteção CSRF centralizada para toda a aplicação Flask."""

from flask import current_app, render_template
from flask_wtf import CSRFProtect
from flask_wtf.csrf import CSRFError


csrf = CSRFProtect()


def configurar_csrf(app):
    """Habilita CSRF em todos os ambientes e registra uma resposta amigável."""
    app.config["WTF_CSRF_ENABLED"] = True
    csrf.init_app(app)

    @app.errorhandler(CSRFError)
    def tratar_erro_csrf(_erro):
        current_app.logger.warning("Solicitação recusada pela validação CSRF.")
        return render_template("erro_csrf.html"), 400
