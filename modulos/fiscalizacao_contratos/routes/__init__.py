"""Rotas iniciais do módulo de Fiscalização de Contratos."""

from flask import render_template

from ..permissions import admin_required


def registrar_rotas(blueprint):
    """Registra somente a página provisória autorizada na Etapa 1."""

    @blueprint.route("", methods=["GET"], strict_slashes=False)
    @admin_required
    def painel():
        return render_template("fiscalizacao_contratos/painel.html")
