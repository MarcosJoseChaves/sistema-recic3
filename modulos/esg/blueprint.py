"""Criação do Blueprint ESG, sem dependência de banco ou serviços externos."""

from flask import Blueprint


def criar_blueprint_esg():
    """Cria uma instância do módulo para registro no aplicativo principal."""
    blueprint = Blueprint(
        "esg",
        __name__,
        template_folder="templates",
        static_folder="static",
        static_url_path="static",
    )

    from .routes import registrar_rotas

    registrar_rotas(blueprint)
    return blueprint
