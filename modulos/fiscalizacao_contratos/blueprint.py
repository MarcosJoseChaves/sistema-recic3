"""Criação do Blueprint do módulo."""

from flask import Blueprint


def criar_blueprint_fiscalizacao():
    """Cria o Blueprint sem iniciar outro Flask app ou outra conexão de banco."""
    blueprint = Blueprint(
        "fiscalizacao_contratos",
        __name__,
        template_folder="templates",
        static_folder="static",
        static_url_path="static",
    )

    from .routes import registrar_rotas

    registrar_rotas(blueprint)
    return blueprint
