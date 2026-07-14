"""Criação do Blueprint do módulo."""

from flask import Blueprint

from .validacoes_contratos import (
    formatar_data_brasileira,
    formatar_data_iso,
    formatar_moeda_brasileira,
    formatar_valor_campo,
)
from .validacoes import formatar_cep, formatar_cnpj


def criar_blueprint_fiscalizacao(conectar_banco):
    """Cria o Blueprint usando a conexão que pertence ao sistema principal."""
    blueprint = Blueprint(
        "fiscalizacao_contratos",
        __name__,
        template_folder="templates",
        static_folder="static",
        static_url_path="static",
    )

    from .routes import registrar_rotas

    blueprint.add_app_template_filter(formatar_cnpj, "fc_cnpj")
    blueprint.add_app_template_filter(formatar_cep, "fc_cep")
    blueprint.add_app_template_filter(formatar_moeda_brasileira, "fc_moeda")
    blueprint.add_app_template_filter(formatar_valor_campo, "fc_valor_campo")
    blueprint.add_app_template_filter(formatar_data_brasileira, "fc_data")
    blueprint.add_app_template_filter(formatar_data_iso, "fc_data_iso")
    registrar_rotas(blueprint, conectar_banco)
    return blueprint
