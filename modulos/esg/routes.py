"""Página inicial do módulo ESG."""

from flask import render_template

# A mesma guarda administrativa já é reutilizada pelo aplicativo principal.
from modulos.fiscalizacao_contratos.permissions import admin_required


def registrar_rotas(blueprint):
    """Registra o painel usando a autenticação e a permissão existentes."""

    @blueprint.route("", methods=["GET"], strict_slashes=False)
    @admin_required
    def painel():
        return render_template("esg/painel.html")
