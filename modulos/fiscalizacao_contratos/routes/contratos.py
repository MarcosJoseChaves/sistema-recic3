"""Rotas administrativas do cadastro de contratos."""

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user

from ..permissions import admin_required
from ..services.aditivos_service import AditivoService, AditivoServiceError
from ..services.contratos_service import (
    ContratoDuplicadoError,
    ContratoNaoEncontradoError,
    ContratoService,
    ContratoServiceError,
    ReferenciaContratoInvalidaError,
)
from ..validacoes_contratos import SITUACOES_CONTRATO, normalizar_e_validar_contrato


def registrar_rotas_contratos(blueprint, conectar_banco):
    """Registra as rotas usando a conexão recebida do sistema principal."""

    def servico():
        return ContratoService(conectar_banco)

    def renderizar_formulario(contrato, responsaveis, modo, status=200):
        try:
            empresas, servidores = servico().opcoes_formulario()
        except ContratoServiceError:
            current_app.logger.exception("Falha ao carregar opções do contrato")
            flash(
                "Não foi possível carregar empresas e servidores. Tente novamente.",
                "danger",
            )
            empresas, servidores = [], []
        return (
            render_template(
                "fiscalizacao_contratos/contratos/form.html",
                contrato=contrato,
                responsaveis=responsaveis,
                empresas=empresas,
                servidores=servidores,
                situacoes=SITUACOES_CONTRATO,
                modo=modo,
            ),
            status,
        )

    @blueprint.route("/contratos", methods=["GET"])
    @admin_required
    def contratos_lista():
        busca = (request.args.get("busca") or "").strip()
        situacao = (request.args.get("situacao") or "").strip()
        if situacao not in ("", *SITUACOES_CONTRATO):
            situacao = ""
        try:
            empresa_id = int(request.args.get("empresa_id") or 0) or None
        except ValueError:
            empresa_id = None
        status_ativo = request.args.get("status_ativo") or "ativos"
        if status_ativo not in ("ativos", "inativos", "todos"):
            status_ativo = "ativos"
        proximos_vencimento = request.args.get("proximos_vencimento") == "1"

        try:
            contrato_service = servico()
            contratos = contrato_service.listar(
                busca=busca,
                situacao=situacao,
                empresa_id=empresa_id,
                status_ativo=status_ativo,
                proximos_vencimento=proximos_vencimento,
            )
            empresas = contrato_service.listar_empresas_filtro()
        except ContratoServiceError:
            current_app.logger.exception("Falha ao listar contratos")
            flash("Não foi possível carregar os contratos. Tente novamente.", "danger")
            contratos, empresas = [], []

        return render_template(
            "fiscalizacao_contratos/contratos/lista.html",
            contratos=contratos,
            empresas=empresas,
            situacoes=SITUACOES_CONTRATO,
            busca=busca,
            situacao=situacao,
            empresa_id=empresa_id,
            status_ativo=status_ativo,
            proximos_vencimento=proximos_vencimento,
        )

    @blueprint.route("/contratos/novo", methods=["GET", "POST"])
    @admin_required
    def contratos_novo():
        if request.method == "GET":
            return renderizar_formulario(
                {"situacao": "Em elaboração"},
                {
                    "gestor_id": None,
                    "fiscal_titular_id": None,
                    "fiscais_substitutos": [],
                    "permitir_multiplas_funcoes": False,
                },
                "novo",
            )

        dados, responsaveis, erros = normalizar_e_validar_contrato(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return renderizar_formulario(dados, responsaveis, "novo", 400)

        try:
            contrato_id = servico().criar(dados, responsaveis, current_user.id)
        except ContratoDuplicadoError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, responsaveis, "novo", 409)
        except ReferenciaContratoInvalidaError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, responsaveis, "novo", 400)
        except ContratoServiceError:
            current_app.logger.exception("Falha ao cadastrar contrato")
            flash("Não foi possível cadastrar o contrato. Tente novamente.", "danger")
            return renderizar_formulario(dados, responsaveis, "novo", 500)

        flash("Contrato cadastrado com sucesso.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.contratos_detalhe", contrato_id=contrato_id)
        )

    @blueprint.route("/contratos/<int:contrato_id>", methods=["GET"])
    @admin_required
    def contratos_detalhe(contrato_id):
        try:
            contrato, responsaveis = servico().obter(contrato_id)
        except ContratoNaoEncontradoError:
            flash("Contrato não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.contratos_lista"))
        except ContratoServiceError:
            current_app.logger.exception("Falha ao carregar contrato")
            flash("Não foi possível carregar o contrato.", "danger")
            return redirect(url_for("fiscalizacao_contratos.contratos_lista"))

        try:
            resumo_aditivos, aditivos = AditivoService(conectar_banco).resumo_contrato(
                contrato_id
            )
        except AditivoServiceError:
            current_app.logger.exception("Falha ao carregar aditivos do contrato")
            flash("Não foi possível carregar os aditivos do contrato.", "danger")
            resumo_aditivos, aditivos = None, []

        responsaveis_ativos = [item for item in responsaveis if item["ativo"]]
        historico_responsaveis = [item for item in responsaveis if not item["ativo"]]
        return render_template(
            "fiscalizacao_contratos/contratos/detalhe.html",
            contrato=contrato,
            responsaveis_ativos=responsaveis_ativos,
            historico_responsaveis=historico_responsaveis,
            resumo_aditivos=resumo_aditivos,
            aditivos=aditivos,
        )

    @blueprint.route("/contratos/<int:contrato_id>/editar", methods=["GET", "POST"])
    @admin_required
    def contratos_editar(contrato_id):
        if request.method == "GET":
            try:
                contrato, vinculos = servico().obter(contrato_id)
            except ContratoNaoEncontradoError:
                flash("Contrato não encontrado.", "warning")
                return redirect(url_for("fiscalizacao_contratos.contratos_lista"))
            except ContratoServiceError:
                current_app.logger.exception("Falha ao carregar contrato para edição")
                flash("Não foi possível carregar o contrato.", "danger")
                return redirect(url_for("fiscalizacao_contratos.contratos_lista"))

            ativos = [item for item in vinculos if item["ativo"]]
            gestor = next(
                (item["servidor_id"] for item in ativos if item["tipo_responsabilidade"] == "Gestor"),
                None,
            )
            fiscal_titular = next(
                (
                    item["servidor_id"]
                    for item in ativos
                    if item["tipo_responsabilidade"] == "Fiscal titular"
                ),
                None,
            )
            substitutos = [
                item["servidor_id"]
                for item in ativos
                if item["tipo_responsabilidade"] == "Fiscal substituto"
            ]
            ids = [item for item in (gestor, fiscal_titular, *substitutos) if item]
            responsaveis = {
                "gestor_id": gestor,
                "fiscal_titular_id": fiscal_titular,
                "fiscais_substitutos": substitutos,
                "permitir_multiplas_funcoes": len(ids) != len(set(ids)),
            }
            return renderizar_formulario(dict(contrato), responsaveis, "editar")

        dados, responsaveis, erros = normalizar_e_validar_contrato(request.form)
        dados["id"] = contrato_id
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return renderizar_formulario(dados, responsaveis, "editar", 400)

        try:
            servico().atualizar(
                contrato_id, dados, responsaveis, current_user.id
            )
        except ContratoDuplicadoError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, responsaveis, "editar", 409)
        except ReferenciaContratoInvalidaError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, responsaveis, "editar", 400)
        except ContratoNaoEncontradoError:
            flash("Contrato não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.contratos_lista"))
        except ContratoServiceError:
            current_app.logger.exception("Falha ao editar contrato")
            flash("Não foi possível atualizar o contrato. Tente novamente.", "danger")
            return renderizar_formulario(dados, responsaveis, "editar", 500)

        flash("Contrato atualizado com sucesso.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.contratos_detalhe", contrato_id=contrato_id)
        )

    @blueprint.route("/contratos/<int:contrato_id>/inativar", methods=["POST"])
    @admin_required
    def contratos_inativar(contrato_id):
        try:
            servico().inativar(contrato_id, current_user.id)
        except ContratoNaoEncontradoError:
            flash("Contrato não encontrado.", "warning")
        except ContratoServiceError:
            current_app.logger.exception("Falha ao inativar contrato")
            flash("Não foi possível inativar o contrato.", "danger")
        else:
            flash("Contrato inativado. O registro foi preservado.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.contratos_lista", status_ativo="todos")
        )

    @blueprint.route("/contratos/<int:contrato_id>/reativar", methods=["POST"])
    @admin_required
    def contratos_reativar(contrato_id):
        try:
            servico().reativar(contrato_id, current_user.id)
        except ContratoNaoEncontradoError:
            flash("Contrato não encontrado.", "warning")
        except ContratoServiceError:
            current_app.logger.exception("Falha ao reativar contrato")
            flash("Não foi possível reativar o contrato.", "danger")
        else:
            flash("Contrato reativado com sucesso.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.contratos_detalhe", contrato_id=contrato_id)
        )
