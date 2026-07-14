"""Rotas administrativas do cadastro de empresas contratadas."""

from flask import current_app, flash, jsonify, redirect, render_template, request, url_for
from flask_login import current_user

from ..permissions import admin_required
from ..services.consultas_externas import (
    ConsultaExternaError,
    consultar_cep,
    consultar_cnpj,
)
from ..services.empresas_service import (
    EmpresaDuplicadaError,
    EmpresaNaoEncontradaError,
    EmpresaService,
    EmpresaServiceError,
)
from ..validacoes import normalizar_e_validar_empresa


def registrar_rotas_empresas(blueprint, conectar_banco):
    """Registra as rotas usando a conexão recebida do sistema principal."""

    def servico():
        return EmpresaService(conectar_banco)

    @blueprint.route("/empresas", methods=["GET"])
    @admin_required
    def empresas_lista():
        incluir_inativas = request.args.get("incluir_inativas") == "1"
        try:
            empresas = servico().listar(incluir_inativas=incluir_inativas)
        except EmpresaServiceError:
            current_app.logger.exception("Falha ao listar empresas contratadas")
            flash("Não foi possível carregar as empresas. Tente novamente.", "danger")
            empresas = []
        return render_template(
            "fiscalizacao_contratos/empresas/lista.html",
            empresas=empresas,
            incluir_inativas=incluir_inativas,
        )

    @blueprint.route("/empresas/nova", methods=["GET", "POST"])
    @admin_required
    def empresas_nova():
        if request.method == "GET":
            return render_template(
                "fiscalizacao_contratos/empresas/form.html",
                empresa={},
                modo="nova",
            )

        dados, erros = normalizar_e_validar_empresa(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return render_template(
                "fiscalizacao_contratos/empresas/form.html",
                empresa=dados,
                modo="nova",
            ), 400

        try:
            empresa_id = servico().criar(dados, current_user.id)
        except EmpresaDuplicadaError as erro:
            flash(str(erro), "danger")
            return render_template(
                "fiscalizacao_contratos/empresas/form.html",
                empresa=dados,
                modo="nova",
            ), 409
        except EmpresaServiceError:
            current_app.logger.exception("Falha ao cadastrar empresa contratada")
            flash("Não foi possível cadastrar a empresa. Tente novamente.", "danger")
            return render_template(
                "fiscalizacao_contratos/empresas/form.html",
                empresa=dados,
                modo="nova",
            ), 500

        flash("Empresa cadastrada com sucesso.", "success")
        return redirect(url_for("fiscalizacao_contratos.empresas_detalhe", empresa_id=empresa_id))

    @blueprint.route("/empresas/<int:empresa_id>", methods=["GET"])
    @admin_required
    def empresas_detalhe(empresa_id):
        try:
            empresa = servico().obter(empresa_id)
        except EmpresaNaoEncontradaError:
            flash("Empresa não encontrada.", "warning")
            return redirect(url_for("fiscalizacao_contratos.empresas_lista"))
        except EmpresaServiceError:
            current_app.logger.exception("Falha ao carregar empresa contratada")
            flash("Não foi possível carregar a empresa.", "danger")
            return redirect(url_for("fiscalizacao_contratos.empresas_lista"))

        return render_template(
            "fiscalizacao_contratos/empresas/detalhe.html",
            empresa=empresa,
        )

    @blueprint.route("/empresas/<int:empresa_id>/editar", methods=["GET", "POST"])
    @admin_required
    def empresas_editar(empresa_id):
        if request.method == "GET":
            try:
                empresa = servico().obter(empresa_id)
            except EmpresaNaoEncontradaError:
                flash("Empresa não encontrada.", "warning")
                return redirect(url_for("fiscalizacao_contratos.empresas_lista"))
            except EmpresaServiceError:
                current_app.logger.exception("Falha ao carregar empresa para edição")
                flash("Não foi possível carregar a empresa.", "danger")
                return redirect(url_for("fiscalizacao_contratos.empresas_lista"))
            return render_template(
                "fiscalizacao_contratos/empresas/form.html",
                empresa=empresa,
                modo="editar",
            )

        dados, erros = normalizar_e_validar_empresa(request.form)
        dados["id"] = empresa_id
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return render_template(
                "fiscalizacao_contratos/empresas/form.html",
                empresa=dados,
                modo="editar",
            ), 400

        try:
            servico().atualizar(empresa_id, dados, current_user.id)
        except EmpresaDuplicadaError as erro:
            flash(str(erro), "danger")
            return render_template(
                "fiscalizacao_contratos/empresas/form.html",
                empresa=dados,
                modo="editar",
            ), 409
        except EmpresaNaoEncontradaError:
            flash("Empresa não encontrada.", "warning")
            return redirect(url_for("fiscalizacao_contratos.empresas_lista"))
        except EmpresaServiceError:
            current_app.logger.exception("Falha ao editar empresa contratada")
            flash("Não foi possível atualizar a empresa. Tente novamente.", "danger")
            return render_template(
                "fiscalizacao_contratos/empresas/form.html",
                empresa=dados,
                modo="editar",
            ), 500

        flash("Empresa atualizada com sucesso.", "success")
        return redirect(url_for("fiscalizacao_contratos.empresas_detalhe", empresa_id=empresa_id))

    @blueprint.route("/empresas/<int:empresa_id>/inativar", methods=["POST"])
    @admin_required
    def empresas_inativar(empresa_id):
        try:
            servico().inativar(empresa_id, current_user.id)
        except EmpresaNaoEncontradaError:
            flash("Empresa não encontrada.", "warning")
        except EmpresaServiceError:
            current_app.logger.exception("Falha ao inativar empresa contratada")
            flash("Não foi possível inativar a empresa.", "danger")
        else:
            flash("Empresa inativada. O cadastro foi preservado no histórico.", "success")
        return redirect(url_for("fiscalizacao_contratos.empresas_lista", incluir_inativas=1))

    @blueprint.route("/empresas/<int:empresa_id>/reativar", methods=["POST"])
    @admin_required
    def empresas_reativar(empresa_id):
        try:
            servico().reativar(empresa_id, current_user.id)
        except EmpresaNaoEncontradaError:
            flash("Empresa não encontrada.", "warning")
        except EmpresaServiceError:
            current_app.logger.exception("Falha ao reativar empresa contratada")
            flash("Não foi possível reativar a empresa.", "danger")
        else:
            flash("Empresa reativada com sucesso.", "success")
        return redirect(url_for("fiscalizacao_contratos.empresas_detalhe", empresa_id=empresa_id))

    @blueprint.route("/empresas/consultar-cnpj/<string:cnpj>", methods=["GET"])
    @admin_required
    def empresas_consultar_cnpj(cnpj):
        try:
            return jsonify(consultar_cnpj(cnpj))
        except ValueError as erro:
            return jsonify({"erro": str(erro)}), 400
        except ConsultaExternaError as erro:
            return jsonify({"erro": str(erro), "preenchimento_manual": True}), 503

    @blueprint.route("/empresas/consultar-cep/<string:cep>", methods=["GET"])
    @admin_required
    def empresas_consultar_cep(cep):
        try:
            return jsonify(consultar_cep(cep))
        except ValueError as erro:
            return jsonify({"erro": str(erro)}), 400
        except ConsultaExternaError as erro:
            return jsonify({"erro": str(erro), "preenchimento_manual": True}), 503
