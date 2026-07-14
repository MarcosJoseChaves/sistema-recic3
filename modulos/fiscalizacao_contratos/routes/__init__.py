"""Rotas iniciais do módulo de Fiscalização de Contratos."""

from flask import render_template

from ..permissions import admin_required
from .contratos import registrar_rotas_contratos
from .empresas import registrar_rotas_empresas
from .servidores import registrar_rotas_servidores


def registrar_rotas(blueprint, conectar_banco):
    """Registra a página do módulo e as rotas da etapa atual."""

    @blueprint.route("", methods=["GET"], strict_slashes=False)
    @admin_required
    def painel():
        return render_template("fiscalizacao_contratos/painel.html")

    registrar_rotas_empresas(blueprint, conectar_banco)
    registrar_rotas_servidores(blueprint, conectar_banco)
    registrar_rotas_contratos(blueprint, conectar_banco)
