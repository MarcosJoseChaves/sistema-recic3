"""Rotas administrativas do cadastro de servidores e responsáveis."""

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user

from ..permissions import admin_required
from ..services.servidores_service import (
    MatriculaDuplicadaError,
    ServidorNaoEncontradoError,
    ServidorService,
    ServidorServiceError,
)
from ..validacoes_servidores import normalizar_e_validar_servidor


def registrar_rotas_servidores(blueprint, conectar_banco):
    """Registra as rotas usando a conexão recebida do sistema principal."""

    def servico():
        return ServidorService(conectar_banco)

    @blueprint.route("/servidores", methods=["GET"])
    @admin_required
    def servidores_lista():
        busca = (request.args.get("busca") or "").strip()
        incluir_inativos = request.args.get("incluir_inativos") == "1"
        try:
            servidores = servico().listar(
                busca=busca,
                incluir_inativos=incluir_inativos,
            )
        except ServidorServiceError:
            current_app.logger.exception("Falha ao listar servidores")
            flash("Não foi possível carregar os servidores. Tente novamente.", "danger")
            servidores = []
        return render_template(
            "fiscalizacao_contratos/servidores/lista.html",
            servidores=servidores,
            busca=busca,
            incluir_inativos=incluir_inativos,
        )

    @blueprint.route("/servidores/novo", methods=["GET", "POST"])
    @admin_required
    def servidores_novo():
        if request.method == "GET":
            return render_template(
                "fiscalizacao_contratos/servidores/form.html",
                servidor={},
                modo="novo",
            )

        dados, erros = normalizar_e_validar_servidor(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return render_template(
                "fiscalizacao_contratos/servidores/form.html",
                servidor=dados,
                modo="novo",
            ), 400

        try:
            servidor_id = servico().criar(dados, current_user.id)
        except MatriculaDuplicadaError as erro:
            flash(str(erro), "danger")
            return render_template(
                "fiscalizacao_contratos/servidores/form.html",
                servidor=dados,
                modo="novo",
            ), 409
        except ServidorServiceError:
            current_app.logger.exception("Falha ao cadastrar servidor")
            flash("Não foi possível cadastrar o servidor. Tente novamente.", "danger")
            return render_template(
                "fiscalizacao_contratos/servidores/form.html",
                servidor=dados,
                modo="novo",
            ), 500

        flash("Servidor cadastrado com sucesso.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.servidores_detalhe", servidor_id=servidor_id)
        )

    @blueprint.route("/servidores/<int:servidor_id>", methods=["GET"])
    @admin_required
    def servidores_detalhe(servidor_id):
        try:
            servidor = servico().obter(servidor_id)
        except ServidorNaoEncontradoError:
            flash("Servidor não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.servidores_lista"))
        except ServidorServiceError:
            current_app.logger.exception("Falha ao carregar servidor")
            flash("Não foi possível carregar o servidor.", "danger")
            return redirect(url_for("fiscalizacao_contratos.servidores_lista"))

        return render_template(
            "fiscalizacao_contratos/servidores/detalhe.html",
            servidor=servidor,
        )

    @blueprint.route("/servidores/<int:servidor_id>/editar", methods=["GET", "POST"])
    @admin_required
    def servidores_editar(servidor_id):
        if request.method == "GET":
            try:
                servidor = servico().obter(servidor_id)
            except ServidorNaoEncontradoError:
                flash("Servidor não encontrado.", "warning")
                return redirect(url_for("fiscalizacao_contratos.servidores_lista"))
            except ServidorServiceError:
                current_app.logger.exception("Falha ao carregar servidor para edição")
                flash("Não foi possível carregar o servidor.", "danger")
                return redirect(url_for("fiscalizacao_contratos.servidores_lista"))
            return render_template(
                "fiscalizacao_contratos/servidores/form.html",
                servidor=servidor,
                modo="editar",
            )

        dados, erros = normalizar_e_validar_servidor(request.form)
        dados["id"] = servidor_id
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return render_template(
                "fiscalizacao_contratos/servidores/form.html",
                servidor=dados,
                modo="editar",
            ), 400

        try:
            servico().atualizar(servidor_id, dados, current_user.id)
        except MatriculaDuplicadaError as erro:
            flash(str(erro), "danger")
            return render_template(
                "fiscalizacao_contratos/servidores/form.html",
                servidor=dados,
                modo="editar",
            ), 409
        except ServidorNaoEncontradoError:
            flash("Servidor não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.servidores_lista"))
        except ServidorServiceError:
            current_app.logger.exception("Falha ao editar servidor")
            flash("Não foi possível atualizar o servidor. Tente novamente.", "danger")
            return render_template(
                "fiscalizacao_contratos/servidores/form.html",
                servidor=dados,
                modo="editar",
            ), 500

        flash("Servidor atualizado com sucesso.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.servidores_detalhe", servidor_id=servidor_id)
        )

    @blueprint.route("/servidores/<int:servidor_id>/inativar", methods=["POST"])
    @admin_required
    def servidores_inativar(servidor_id):
        try:
            servico().inativar(servidor_id, current_user.id)
        except ServidorNaoEncontradoError:
            flash("Servidor não encontrado.", "warning")
        except ServidorServiceError:
            current_app.logger.exception("Falha ao inativar servidor")
            flash("Não foi possível inativar o servidor.", "danger")
        else:
            flash("Servidor inativado. O cadastro foi preservado no histórico.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.servidores_lista", incluir_inativos=1)
        )

    @blueprint.route("/servidores/<int:servidor_id>/reativar", methods=["POST"])
    @admin_required
    def servidores_reativar(servidor_id):
        try:
            servico().reativar(servidor_id, current_user.id)
        except ServidorNaoEncontradoError:
            flash("Servidor não encontrado.", "warning")
        except ServidorServiceError:
            current_app.logger.exception("Falha ao reativar servidor")
            flash("Não foi possível reativar o servidor.", "danger")
        else:
            flash("Servidor reativado com sucesso.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.servidores_detalhe", servidor_id=servidor_id)
        )
