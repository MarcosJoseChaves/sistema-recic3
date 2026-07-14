"""Rotas administrativas do cadastro de aditivos."""

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user

from ..permissions import admin_required
from ..services.aditivos_service import (
    AditivoDuplicadoError,
    AditivoNaoEncontradoError,
    AditivoService,
    AditivoServiceError,
    ContratoAditivoInvalidoError,
)
from ..validacoes_aditivos import TIPOS_ADITIVO, normalizar_e_validar_aditivo


def registrar_rotas_aditivos(blueprint, conectar_banco):
    """Registra as rotas usando a conexão entregue pelo sistema principal."""

    def servico():
        return AditivoService(conectar_banco)

    def renderizar_formulario(aditivo, modo, status=200):
        try:
            contratos = servico().listar_contratos()
        except AditivoServiceError:
            current_app.logger.exception("Falha ao carregar contratos para o aditivo")
            flash("Não foi possível carregar os contratos. Tente novamente.", "danger")
            contratos = []
        return (
            render_template(
                "fiscalizacao_contratos/aditivos/form.html",
                aditivo=aditivo,
                contratos=contratos,
                tipos_aditivo=TIPOS_ADITIVO,
                modo=modo,
            ),
            status,
        )

    @blueprint.route("/aditivos", methods=["GET"])
    @admin_required
    def aditivos_lista():
        busca = (request.args.get("busca") or "").strip()
        tipo_aditivo = (request.args.get("tipo_aditivo") or "").strip()
        if tipo_aditivo not in ("", *TIPOS_ADITIVO):
            tipo_aditivo = ""
        status_ativo = request.args.get("status_ativo") or "ativos"
        if status_ativo not in ("ativos", "inativos", "todos"):
            status_ativo = "ativos"
        try:
            aditivos = servico().listar(
                busca=busca,
                tipo_aditivo=tipo_aditivo,
                status_ativo=status_ativo,
            )
        except AditivoServiceError:
            current_app.logger.exception("Falha ao listar aditivos")
            flash("Não foi possível carregar os aditivos. Tente novamente.", "danger")
            aditivos = []
        return render_template(
            "fiscalizacao_contratos/aditivos/lista.html",
            aditivos=aditivos,
            tipos_aditivo=TIPOS_ADITIVO,
            busca=busca,
            tipo_aditivo=tipo_aditivo,
            status_ativo=status_ativo,
        )

    @blueprint.route("/aditivos/novo", methods=["GET", "POST"])
    @admin_required
    def aditivos_novo():
        if request.method == "GET":
            contrato_id = request.args.get("contrato_id", type=int)
            return renderizar_formulario({"contrato_id": contrato_id}, "novo")

        dados, erros = normalizar_e_validar_aditivo(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return renderizar_formulario(dados, "novo", 400)
        try:
            aditivo_id = servico().criar(dados, current_user.id)
        except AditivoDuplicadoError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, "novo", 409)
        except ContratoAditivoInvalidoError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, "novo", 400)
        except AditivoServiceError:
            current_app.logger.exception("Falha ao cadastrar aditivo")
            flash("Não foi possível cadastrar o aditivo. Tente novamente.", "danger")
            return renderizar_formulario(dados, "novo", 500)
        flash("Aditivo cadastrado com sucesso.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.aditivos_detalhe", aditivo_id=aditivo_id)
        )

    @blueprint.route("/aditivos/<int:aditivo_id>", methods=["GET"])
    @admin_required
    def aditivos_detalhe(aditivo_id):
        try:
            aditivo = servico().obter(aditivo_id)
        except AditivoNaoEncontradoError:
            flash("Aditivo não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.aditivos_lista"))
        except AditivoServiceError:
            current_app.logger.exception("Falha ao carregar aditivo")
            flash("Não foi possível carregar o aditivo.", "danger")
            return redirect(url_for("fiscalizacao_contratos.aditivos_lista"))
        return render_template(
            "fiscalizacao_contratos/aditivos/detalhe.html", aditivo=aditivo
        )

    @blueprint.route("/aditivos/<int:aditivo_id>/editar", methods=["GET", "POST"])
    @admin_required
    def aditivos_editar(aditivo_id):
        if request.method == "GET":
            try:
                aditivo = dict(servico().obter(aditivo_id))
            except AditivoNaoEncontradoError:
                flash("Aditivo não encontrado.", "warning")
                return redirect(url_for("fiscalizacao_contratos.aditivos_lista"))
            except AditivoServiceError:
                current_app.logger.exception("Falha ao carregar aditivo para edição")
                flash("Não foi possível carregar o aditivo.", "danger")
                return redirect(url_for("fiscalizacao_contratos.aditivos_lista"))
            aditivo["confirmar_valores_simultaneos"] = bool(
                aditivo.get("valor_acrescimo") and aditivo.get("valor_supressao")
            )
            return renderizar_formulario(aditivo, "editar")

        dados, erros = normalizar_e_validar_aditivo(request.form)
        dados["id"] = aditivo_id
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return renderizar_formulario(dados, "editar", 400)
        try:
            servico().atualizar(aditivo_id, dados, current_user.id)
        except AditivoDuplicadoError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, "editar", 409)
        except ContratoAditivoInvalidoError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, "editar", 400)
        except AditivoNaoEncontradoError:
            flash("Aditivo não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.aditivos_lista"))
        except AditivoServiceError:
            current_app.logger.exception("Falha ao editar aditivo")
            flash("Não foi possível atualizar o aditivo. Tente novamente.", "danger")
            return renderizar_formulario(dados, "editar", 500)
        flash("Aditivo atualizado com sucesso.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.aditivos_detalhe", aditivo_id=aditivo_id)
        )

    @blueprint.route("/aditivos/<int:aditivo_id>/inativar", methods=["POST"])
    @admin_required
    def aditivos_inativar(aditivo_id):
        try:
            servico().inativar(aditivo_id, current_user.id)
        except AditivoNaoEncontradoError:
            flash("Aditivo não encontrado.", "warning")
        except AditivoServiceError:
            current_app.logger.exception("Falha ao inativar aditivo")
            flash("Não foi possível inativar o aditivo.", "danger")
        else:
            flash("Aditivo inativado. Seus efeitos foram retirados dos cálculos.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.aditivos_lista", status_ativo="todos")
        )

    @blueprint.route("/aditivos/<int:aditivo_id>/reativar", methods=["POST"])
    @admin_required
    def aditivos_reativar(aditivo_id):
        try:
            servico().reativar(aditivo_id, current_user.id)
        except AditivoNaoEncontradoError:
            flash("Aditivo não encontrado.", "warning")
        except AditivoServiceError:
            current_app.logger.exception("Falha ao reativar aditivo")
            flash("Não foi possível reativar o aditivo.", "danger")
        else:
            flash("Aditivo reativado e incluído novamente nos cálculos.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.aditivos_detalhe", aditivo_id=aditivo_id)
        )
