"""Rotas iniciais do módulo de Fiscalização de Contratos."""

from flask import current_app, render_template

from ..permissions import admin_required
from .aditivos import registrar_rotas_aditivos
from .ativos import registrar_rotas_ativos
from .contratos import registrar_rotas_contratos
from .documentos import registrar_rotas_documentos
from .empresas import registrar_rotas_empresas
from .fiscalizacoes import registrar_rotas_fiscalizacoes
from .ocorrencias import registrar_rotas_ocorrencias
from .planilhas import registrar_rotas_planilhas
from .servidores import registrar_rotas_servidores
from ..services.fiscalizacoes_service import FiscalizacaoService, FiscalizacaoServiceError


def registrar_rotas(blueprint, conectar_banco):
    """Registra a página do módulo e as rotas da etapa atual."""

    @blueprint.route("", methods=["GET"], strict_slashes=False)
    @admin_required
    def painel():
        try:
            indicadores = FiscalizacaoService(conectar_banco).indicadores()
        except FiscalizacaoServiceError:
            current_app.logger.exception("Falha ao carregar indicadores de fiscalização")
            indicadores = {
                "ocorrencias_abertas": 0, "ocorrencias_vencidas": 0,
                "graves_criticas": 0, "fiscalizacoes_30_dias": 0,
            }
        return render_template("fiscalizacao_contratos/painel.html", indicadores=indicadores)

    registrar_rotas_empresas(blueprint, conectar_banco)
    registrar_rotas_servidores(blueprint, conectar_banco)
    registrar_rotas_contratos(blueprint, conectar_banco)
    registrar_rotas_aditivos(blueprint, conectar_banco)
    registrar_rotas_documentos(blueprint, conectar_banco)
    registrar_rotas_planilhas(blueprint, conectar_banco)
    registrar_rotas_ativos(blueprint, conectar_banco)
    registrar_rotas_fiscalizacoes(blueprint, conectar_banco)
    registrar_rotas_ocorrencias(blueprint, conectar_banco)
