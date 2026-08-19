"""Rotas iniciais do módulo de Fiscalização de Contratos."""

from flask import render_template

from ..permissions import admin_required
from .aditivos import registrar_rotas_aditivos
from .ativos import registrar_rotas_ativos
from .atestes import registrar_rotas_atestes
from .contratos import registrar_rotas_contratos
from .documentos import registrar_rotas_documentos
from .empresas import registrar_rotas_empresas
from .fiscalizacoes import registrar_rotas_fiscalizacoes
from .medicoes import registrar_rotas_medicoes
from .ocorrencias import registrar_rotas_ocorrencias
from .planilhas import registrar_rotas_planilhas
from .servidores import registrar_rotas_servidores
from ..services.fiscalizacoes_service import FiscalizacaoService, FiscalizacaoServiceError
from ..services.medicoes_service import MedicaoService, MedicaoServiceError
from ..services.atestes_service import AtesteService, AtesteServiceError


def registrar_rotas(blueprint, conectar_banco):
    """Registra a página do módulo e as rotas da etapa atual."""

    @blueprint.route("", methods=["GET"], strict_slashes=False)
    @admin_required
    def painel():
        return render_template("fiscalizacao_contratos/painel.html")

    registrar_rotas_empresas(blueprint, conectar_banco)
    registrar_rotas_servidores(blueprint, conectar_banco)
    registrar_rotas_contratos(blueprint, conectar_banco)
    registrar_rotas_aditivos(blueprint, conectar_banco)
    registrar_rotas_documentos(blueprint, conectar_banco)
    registrar_rotas_planilhas(blueprint, conectar_banco)
    registrar_rotas_ativos(blueprint, conectar_banco)
    registrar_rotas_fiscalizacoes(blueprint, conectar_banco)
    registrar_rotas_ocorrencias(blueprint, conectar_banco)
    registrar_rotas_medicoes(blueprint, conectar_banco)
    registrar_rotas_atestes(blueprint, conectar_banco)
