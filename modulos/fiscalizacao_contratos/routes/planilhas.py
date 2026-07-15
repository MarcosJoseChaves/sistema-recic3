"""Rotas administrativas das planilhas orçamentárias."""

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user

from ..permissions import admin_required
from ..services.planilhas_service import (
    ItemNaoEncontradoError,
    PlanilhaBloqueadaError,
    PlanilhaDuplicadaError,
    PlanilhaNaoEncontradaError,
    PlanilhaService,
    PlanilhaServiceError,
    ReferenciaPlanilhaInvalidaError,
)
from ..validacoes_planilhas import (
    STATUS_PLANILHA,
    TIPOS_PLANILHA,
    normalizar_e_validar_item,
    normalizar_e_validar_planilha,
)


def registrar_rotas_planilhas(blueprint, conectar_banco):
    def servico():
        return PlanilhaService(conectar_banco)

    def carregar_opcoes(contrato_id=None):
        contratos = servico().listar_contratos()
        aditivos = servico().listar_aditivos(contrato_id) if contrato_id else []
        return contratos, aditivos

    def renderizar_formulario(planilha, modo, status_http=200, origem=None):
        try:
            contratos, aditivos = carregar_opcoes(planilha.get("contrato_id"))
        except PlanilhaServiceError:
            current_app.logger.exception("Falha ao carregar opções da planilha")
            flash("Não foi possível carregar contratos e aditivos.", "danger")
            contratos, aditivos = [], []
        return render_template(
            "fiscalizacao_contratos/planilhas/form.html",
            planilha=planilha,
            contratos=contratos,
            aditivos=aditivos,
            tipos_planilha=TIPOS_PLANILHA,
            modo=modo,
            origem=origem,
        ), status_http

    @blueprint.route("/planilhas", methods=["GET"])
    @admin_required
    def planilhas_lista():
        busca = (request.args.get("busca") or "").strip()
        tipo = (request.args.get("tipo_planilha") or "").strip()
        status = (request.args.get("status") or "").strip()
        vigente = (request.args.get("vigente") or "").strip()
        status_ativo = request.args.get("status_ativo") or "ativos"
        try:
            contrato_id = int(request.args.get("contrato_id") or 0) or None
        except ValueError:
            contrato_id = None
        if tipo not in ("", *TIPOS_PLANILHA):
            tipo = ""
        if status not in ("", *STATUS_PLANILHA):
            status = ""
        if vigente not in ("", "sim", "nao"):
            vigente = ""
        if status_ativo not in ("ativos", "inativos", "todos"):
            status_ativo = "ativos"
        try:
            planilha_service = servico()
            planilhas = planilha_service.listar(
                busca, contrato_id, tipo, status, vigente, status_ativo
            )
            contratos = planilha_service.listar_contratos()
        except PlanilhaServiceError:
            current_app.logger.exception("Falha ao listar planilhas")
            flash("Não foi possível carregar as planilhas.", "danger")
            planilhas, contratos = [], []
        return render_template(
            "fiscalizacao_contratos/planilhas/lista.html",
            planilhas=planilhas, contratos=contratos, tipos_planilha=TIPOS_PLANILHA,
            status_planilha=STATUS_PLANILHA, busca=busca, contrato_id=contrato_id,
            tipo_planilha=tipo, status=status, vigente=vigente,
            status_ativo=status_ativo,
        )

    @blueprint.route("/planilhas/nova", methods=["GET", "POST"])
    @admin_required
    def planilhas_nova():
        if request.method == "GET":
            contrato_id = request.args.get("contrato_id", type=int)
            return renderizar_formulario({
                "contrato_id": contrato_id, "nome": "Planilha Orçamentária Original",
                "versao": 1, "tipo_planilha": "Original",
            }, "nova")
        dados, erros = normalizar_e_validar_planilha(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return renderizar_formulario(dados, "nova", 400)
        try:
            planilha_id = servico().criar(dados, current_user.id)
        except PlanilhaDuplicadaError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, "nova", 409)
        except ReferenciaPlanilhaInvalidaError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, "nova", 400)
        except PlanilhaServiceError:
            current_app.logger.exception("Falha ao cadastrar planilha")
            flash("Não foi possível cadastrar a planilha.", "danger")
            return renderizar_formulario(dados, "nova", 500)
        flash("Planilha criada. Agora inclua os itens antes de consolidar.", "success")
        return redirect(url_for("fiscalizacao_contratos.planilhas_detalhe", planilha_id=planilha_id))

    @blueprint.route("/planilhas/<int:planilha_id>", methods=["GET"])
    @admin_required
    def planilhas_detalhe(planilha_id):
        try:
            planilha, itens = servico().obter(planilha_id)
        except PlanilhaNaoEncontradaError:
            flash("Planilha não encontrada.", "warning")
            return redirect(url_for("fiscalizacao_contratos.planilhas_lista"))
        except PlanilhaServiceError:
            current_app.logger.exception("Falha ao carregar planilha")
            flash("Não foi possível carregar a planilha.", "danger")
            return redirect(url_for("fiscalizacao_contratos.planilhas_lista"))
        return render_template("fiscalizacao_contratos/planilhas/detalhe.html", planilha=planilha, itens=itens)

    @blueprint.route("/planilhas/contratos/<int:contrato_id>/comparar", methods=["GET"])
    @admin_required
    def planilhas_comparar(contrato_id):
        """A comparação é exibida no detalhe administrativo do contrato."""
        return redirect(
            url_for("fiscalizacao_contratos.contratos_detalhe", contrato_id=contrato_id)
            + "#planilhas-orcamentarias"
        )

    @blueprint.route("/planilhas/<int:planilha_id>/editar", methods=["GET", "POST"])
    @admin_required
    def planilhas_editar(planilha_id):
        if request.method == "GET":
            try:
                planilha, _ = servico().obter(planilha_id)
            except PlanilhaNaoEncontradaError:
                flash("Planilha não encontrada.", "warning")
                return redirect(url_for("fiscalizacao_contratos.planilhas_lista"))
            if planilha["status"] != "Em elaboração" or not planilha["ativo"]:
                flash("Somente planilhas ativas em elaboração podem ser editadas.", "warning")
                return redirect(url_for("fiscalizacao_contratos.planilhas_detalhe", planilha_id=planilha_id))
            return renderizar_formulario(dict(planilha), "editar")
        dados, erros = normalizar_e_validar_planilha(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return renderizar_formulario(dados, "editar", 400)
        try:
            servico().atualizar(planilha_id, dados, current_user.id)
        except PlanilhaDuplicadaError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, "editar", 409)
        except (ReferenciaPlanilhaInvalidaError, PlanilhaBloqueadaError) as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, "editar", 400)
        except PlanilhaServiceError:
            current_app.logger.exception("Falha ao editar planilha")
            flash("Não foi possível atualizar a planilha.", "danger")
            return renderizar_formulario(dados, "editar", 500)
        flash("Planilha atualizada.", "success")
        return redirect(url_for("fiscalizacao_contratos.planilhas_detalhe", planilha_id=planilha_id))

    @blueprint.route("/planilhas/<int:planilha_id>/itens/novo", methods=["GET", "POST"])
    @admin_required
    def planilhas_item_novo(planilha_id):
        try:
            planilha, itens = servico().obter(planilha_id)
        except PlanilhaServiceError:
            flash("Planilha não encontrada.", "warning")
            return redirect(url_for("fiscalizacao_contratos.planilhas_lista"))
        if planilha["status"] != "Em elaboração" or not planilha["ativo"]:
            flash("Somente planilhas ativas em elaboração podem receber itens.", "warning")
            return redirect(url_for("fiscalizacao_contratos.planilhas_detalhe", planilha_id=planilha_id))
        if request.method == "GET":
            item = {"ordem": len(itens) + 1, "fator_multiplicador": 1}
            return render_template("fiscalizacao_contratos/planilhas/item_form.html", planilha=planilha, item=item, modo="novo")
        dados, erros = normalizar_e_validar_item(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return render_template("fiscalizacao_contratos/planilhas/item_form.html", planilha=planilha, item=dados, modo="novo"), 400
        try:
            servico().criar_item(planilha_id, dados, current_user.id)
        except PlanilhaBloqueadaError as erro:
            flash(str(erro), "warning")
        except PlanilhaServiceError:
            current_app.logger.exception("Falha ao cadastrar item")
            flash("Não foi possível cadastrar o item.", "danger")
        else:
            flash("Item cadastrado.", "success")
        return redirect(url_for("fiscalizacao_contratos.planilhas_detalhe", planilha_id=planilha_id))

    @blueprint.route("/planilhas/<int:planilha_id>/itens/<int:item_id>/editar", methods=["GET", "POST"])
    @admin_required
    def planilhas_item_editar(planilha_id, item_id):
        try:
            planilha, itens = servico().obter(planilha_id)
            item = next((item for item in itens if item["id"] == item_id), None)
            if not item:
                raise ItemNaoEncontradoError("Item não encontrado.")
        except PlanilhaServiceError:
            flash("Item não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.planilhas_detalhe", planilha_id=planilha_id))
        if planilha["status"] != "Em elaboração" or not planilha["ativo"]:
            flash("Itens de planilhas consolidadas ou inativas não podem ser alterados.", "warning")
            return redirect(url_for("fiscalizacao_contratos.planilhas_detalhe", planilha_id=planilha_id))
        if request.method == "GET":
            return render_template("fiscalizacao_contratos/planilhas/item_form.html", planilha=planilha, item=item, modo="editar")
        dados, erros = normalizar_e_validar_item(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return render_template("fiscalizacao_contratos/planilhas/item_form.html", planilha=planilha, item=dados, modo="editar"), 400
        try:
            servico().atualizar_item(item_id, planilha_id, dados, current_user.id)
        except PlanilhaBloqueadaError as erro:
            flash(str(erro), "warning")
        except PlanilhaServiceError:
            current_app.logger.exception("Falha ao atualizar item")
            flash("Não foi possível atualizar o item.", "danger")
        else:
            flash("Item atualizado.", "success")
        return redirect(url_for("fiscalizacao_contratos.planilhas_detalhe", planilha_id=planilha_id))

    def acao_simples(metodo, planilha_id, mensagem):
        try:
            metodo(planilha_id, current_user.id)
        except PlanilhaBloqueadaError as erro:
            flash(str(erro), "warning")
        except PlanilhaServiceError:
            current_app.logger.exception("Falha em operação da planilha")
            flash("Não foi possível concluir a operação.", "danger")
        else:
            flash(mensagem, "success")
        return redirect(url_for("fiscalizacao_contratos.planilhas_detalhe", planilha_id=planilha_id))

    @blueprint.route("/planilhas/<int:planilha_id>/itens/<int:item_id>/inativar", methods=["POST"])
    @admin_required
    def planilhas_item_inativar(planilha_id, item_id):
        return acao_simples(lambda p, u: servico().alterar_item_ativo(p, item_id, u, False), planilha_id, "Item inativado e retirado dos totais.")

    @blueprint.route("/planilhas/<int:planilha_id>/itens/<int:item_id>/reativar", methods=["POST"])
    @admin_required
    def planilhas_item_reativar(planilha_id, item_id):
        return acao_simples(lambda p, u: servico().alterar_item_ativo(p, item_id, u, True), planilha_id, "Item reativado.")

    @blueprint.route("/planilhas/<int:planilha_id>/consolidar", methods=["POST"])
    @admin_required
    def planilhas_consolidar(planilha_id):
        return acao_simples(servico().consolidar, planilha_id, "Planilha consolidada e bloqueada para edição.")

    @blueprint.route("/planilhas/<int:planilha_id>/definir-vigente", methods=["POST"])
    @admin_required
    def planilhas_definir_vigente(planilha_id):
        return acao_simples(servico().definir_vigente, planilha_id, "Planilha definida como vigente.")

    @blueprint.route("/planilhas/<int:planilha_id>/inativar", methods=["POST"])
    @admin_required
    def planilhas_inativar(planilha_id):
        return acao_simples(lambda p, u: servico().alterar_planilha_ativo(p, u, False), planilha_id, "Planilha inativada sem exclusão.")

    @blueprint.route("/planilhas/<int:planilha_id>/reativar", methods=["POST"])
    @admin_required
    def planilhas_reativar(planilha_id):
        return acao_simples(lambda p, u: servico().alterar_planilha_ativo(p, u, True), planilha_id, "Planilha reativada.")

    @blueprint.route("/planilhas/<int:planilha_id>/nova-versao", methods=["GET", "POST"])
    @admin_required
    def planilhas_nova_versao(planilha_id):
        try:
            origem, _ = servico().obter(planilha_id)
        except PlanilhaServiceError:
            flash("Planilha de origem não encontrada.", "warning")
            return redirect(url_for("fiscalizacao_contratos.planilhas_lista"))
        if request.method == "GET":
            dados = {
                "contrato_id": origem["contrato_id"], "nome": f"{origem['nome']} - nova versão",
                "versao": origem["versao"] + 1, "tipo_planilha": "Reajustada",
                "data_referencia": origem["data_referencia"],
            }
            return renderizar_formulario(dados, "copiar", origem=origem)
        dados, erros = normalizar_e_validar_planilha(request.form, permitir_original=False)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return renderizar_formulario(dados, "copiar", 400, origem)
        try:
            nova_id = servico().criar_versao(planilha_id, dados, current_user.id)
        except (PlanilhaBloqueadaError, ReferenciaPlanilhaInvalidaError) as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, "copiar", 400, origem)
        except PlanilhaServiceError:
            current_app.logger.exception("Falha ao copiar planilha")
            flash("Não foi possível criar a nova versão.", "danger")
            return renderizar_formulario(dados, "copiar", 500, origem)
        flash("Nova versão criada com cópia dos itens ativos.", "success")
        return redirect(url_for("fiscalizacao_contratos.planilhas_detalhe", planilha_id=nova_id))
