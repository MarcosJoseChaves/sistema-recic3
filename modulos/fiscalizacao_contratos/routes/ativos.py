"""Rotas administrativas dos ativos vinculados aos contratos."""

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user

from ..permissions import admin_required
from ..services.ativos_service import (
    AtivoBloqueadoError, AtivoDuplicadoError, AtivoNaoEncontradoError,
    AtivoService, AtivoServiceError, ReferenciaAtivoInvalidaError,
    VinculoDuplicadoError, VinculoNaoEncontradoError,
)
from ..services.ocorrencias_service import OcorrenciaService, OcorrenciaServiceError
from ..validacoes_ativos import (
    NATUREZAS_VINCULO, ORIGENS_ATIVO, SITUACOES_ATIVO, TIPOS_ATIVO,
    normalizar_data_encerramento, normalizar_e_validar_ativo,
    normalizar_e_validar_vinculo,
)


def registrar_rotas_ativos(blueprint, conectar_banco):
    def servico():
        return AtivoService(conectar_banco)

    def opcoes():
        try:
            return servico().opcoes()
        except AtivoServiceError:
            current_app.logger.exception("Falha ao carregar opções de ativos")
            flash("Não foi possível carregar empresas e contratos.", "danger")
            return [], []

    def formulario_ativo(ativo, modo, status=200):
        empresas, _ = opcoes()
        return render_template(
            "fiscalizacao_contratos/ativos/form.html", ativo=ativo, modo=modo,
            empresas=empresas, tipos_ativo=TIPOS_ATIVO, origens_ativo=ORIGENS_ATIVO,
            situacoes_ativo=SITUACOES_ATIVO,
        ), status

    @blueprint.route("/ativos", methods=["GET"])
    @admin_required
    def ativos_lista():
        busca = (request.args.get("busca") or "").strip()
        tipo = (request.args.get("tipo_ativo") or "").strip()
        origem = (request.args.get("origem_ativo") or "").strip()
        situacao = (request.args.get("situacao") or "").strip()
        vinculo = (request.args.get("com_vinculo_ativo") or "").strip()
        status_ativo = request.args.get("status_ativo") or "ativos"
        def inteiro(nome):
            try: return int(request.args.get(nome) or 0) or None
            except ValueError: return None
        empresa_id, contrato_id = inteiro("empresa_id"), inteiro("contrato_id")
        if tipo not in ("", *TIPOS_ATIVO): tipo = ""
        if origem not in ("", *ORIGENS_ATIVO): origem = ""
        if situacao not in ("", *SITUACOES_ATIVO): situacao = ""
        if vinculo not in ("", "sim", "nao"): vinculo = ""
        if status_ativo not in ("ativos", "inativos", "todos"): status_ativo = "ativos"
        try:
            s = servico()
            ativos = s.listar(busca, tipo, origem, situacao, empresa_id, contrato_id, vinculo, status_ativo)
            contadores = s.contadores()
            empresas, contratos = s.opcoes()
        except AtivoServiceError:
            current_app.logger.exception("Falha ao listar ativos contratuais")
            flash("Não foi possível carregar os ativos.", "danger")
            ativos, empresas, contratos = [], [], []
            contadores = {"ativos_cadastrados": 0, "em_operacao": 0, "em_manutencao": 0, "vinculos_ativos": 0}
        return render_template(
            "fiscalizacao_contratos/ativos/lista.html", ativos=ativos,
            contadores=contadores, empresas=empresas, contratos=contratos,
            tipos_ativo=TIPOS_ATIVO, origens_ativo=ORIGENS_ATIVO,
            situacoes_ativo=SITUACOES_ATIVO, busca=busca, tipo_ativo=tipo,
            origem_ativo=origem, situacao=situacao, empresa_id=empresa_id,
            contrato_id=contrato_id, com_vinculo_ativo=vinculo,
            status_ativo=status_ativo,
        )

    @blueprint.route("/ativos/novo", methods=["GET", "POST"])
    @admin_required
    def ativos_novo():
        if request.method == "GET":
            return formulario_ativo({"situacao": "Disponível"}, "novo")
        dados, erros = normalizar_e_validar_ativo(request.form)
        if erros:
            for erro in erros: flash(erro, "danger")
            return formulario_ativo(dados, "novo", 400)
        try:
            ativo_id = servico().criar(dados, current_user.id)
        except (AtivoDuplicadoError, ReferenciaAtivoInvalidaError) as erro:
            flash(str(erro), "danger")
            return formulario_ativo(dados, "novo", 409 if isinstance(erro, AtivoDuplicadoError) else 400)
        except AtivoServiceError:
            current_app.logger.exception("Falha ao cadastrar ativo contratual")
            flash("Não foi possível cadastrar o ativo.", "danger")
            return formulario_ativo(dados, "novo", 500)
        flash("Ativo cadastrado com sucesso.", "success")
        return redirect(url_for("fiscalizacao_contratos.ativos_detalhe", ativo_id=ativo_id))

    @blueprint.route("/ativos/<int:ativo_id>", methods=["GET"])
    @admin_required
    def ativos_detalhe(ativo_id):
        try:
            ativo, vinculos = servico().obter(ativo_id)
        except AtivoNaoEncontradoError:
            flash("Ativo não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.ativos_lista"))
        except AtivoServiceError:
            current_app.logger.exception("Falha ao carregar ativo")
            flash("Não foi possível carregar o ativo.", "danger")
            return redirect(url_for("fiscalizacao_contratos.ativos_lista"))
        try:
            ocorrencias = OcorrenciaService(conectar_banco).listar_do_ativo(ativo_id)
        except OcorrenciaServiceError:
            current_app.logger.exception("Falha ao carregar ocorrências do ativo")
            flash("Não foi possível carregar as ocorrências do ativo.", "danger")
            ocorrencias = []
        return render_template("fiscalizacao_contratos/ativos/detalhe.html", ativo=ativo, vinculos=vinculos, ocorrencias=ocorrencias)

    @blueprint.route("/ativos/<int:ativo_id>/editar", methods=["GET", "POST"])
    @admin_required
    def ativos_editar(ativo_id):
        if request.method == "GET":
            try: ativo, _ = servico().obter(ativo_id)
            except AtivoServiceError:
                flash("Ativo não encontrado.", "warning")
                return redirect(url_for("fiscalizacao_contratos.ativos_lista"))
            return formulario_ativo(dict(ativo), "editar")
        dados, erros = normalizar_e_validar_ativo(request.form)
        if erros:
            for erro in erros: flash(erro, "danger")
            return formulario_ativo(dados, "editar", 400)
        try:
            servico().atualizar(ativo_id, dados, current_user.id)
        except (AtivoDuplicadoError, ReferenciaAtivoInvalidaError) as erro:
            flash(str(erro), "danger")
            return formulario_ativo(dados, "editar", 409 if isinstance(erro, AtivoDuplicadoError) else 400)
        except AtivoServiceError:
            current_app.logger.exception("Falha ao editar ativo")
            flash("Não foi possível atualizar o ativo.", "danger")
            return formulario_ativo(dados, "editar", 500)
        flash("Ativo atualizado com sucesso.", "success")
        return redirect(url_for("fiscalizacao_contratos.ativos_detalhe", ativo_id=ativo_id))

    def alterar_ativo(ativo_id, valor):
        try: servico().alterar_ativo(ativo_id, current_user.id, valor)
        except AtivoBloqueadoError as erro: flash(str(erro), "warning")
        except AtivoServiceError:
            current_app.logger.exception("Falha ao alterar situação do ativo")
            flash("Não foi possível alterar a situação do ativo.", "danger")
        else: flash("Ativo reativado." if valor else "Ativo inativado sem exclusão.", "success")
        return redirect(url_for("fiscalizacao_contratos.ativos_detalhe", ativo_id=ativo_id))

    @blueprint.route("/ativos/<int:ativo_id>/inativar", methods=["POST"])
    @admin_required
    def ativos_inativar(ativo_id): return alterar_ativo(ativo_id, False)

    @blueprint.route("/ativos/<int:ativo_id>/reativar", methods=["POST"])
    @admin_required
    def ativos_reativar(ativo_id): return alterar_ativo(ativo_id, True)

    @blueprint.route("/ativos/<int:ativo_id>/vincular", methods=["GET", "POST"])
    @admin_required
    def ativos_vincular(ativo_id):
        try: ativo, _ = servico().obter(ativo_id)
        except AtivoServiceError:
            flash("Ativo não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.ativos_lista"))
        if not ativo["ativo"] or ativo["situacao"] == "Baixado":
            flash("Este ativo não pode receber novos vínculos.", "warning")
            return redirect(url_for("fiscalizacao_contratos.ativos_detalhe", ativo_id=ativo_id))
        _, contratos = opcoes()
        if request.method == "GET":
            return render_template("fiscalizacao_contratos/ativos/vinculo_form.html", ativo=ativo, vinculo={"ativo_id": ativo_id}, contratos=contratos, naturezas=NATUREZAS_VINCULO)
        formulario = request.form.to_dict(); formulario["ativo_id"] = str(ativo_id)
        dados, erros = normalizar_e_validar_vinculo(formulario)
        if erros:
            for erro in erros: flash(erro, "danger")
            return render_template("fiscalizacao_contratos/ativos/vinculo_form.html", ativo=ativo, vinculo=dados, contratos=contratos, naturezas=NATUREZAS_VINCULO), 400
        try: servico().criar_vinculo(dados, current_user.id)
        except (AtivoBloqueadoError, ReferenciaAtivoInvalidaError, VinculoDuplicadoError) as erro:
            flash(str(erro), "danger")
            return render_template("fiscalizacao_contratos/ativos/vinculo_form.html", ativo=ativo, vinculo=dados, contratos=contratos, naturezas=NATUREZAS_VINCULO), 409 if isinstance(erro, VinculoDuplicadoError) else 400
        except AtivoServiceError:
            current_app.logger.exception("Falha ao vincular ativo")
            flash("Não foi possível criar o vínculo.", "danger")
            return render_template("fiscalizacao_contratos/ativos/vinculo_form.html", ativo=ativo, vinculo=dados, contratos=contratos, naturezas=NATUREZAS_VINCULO), 500
        flash("Ativo vinculado ao contrato.", "success")
        return redirect(url_for("fiscalizacao_contratos.ativos_detalhe", ativo_id=ativo_id))

    @blueprint.route("/ativos/vinculos", methods=["GET"])
    @admin_required
    def ativos_vinculos_lista():
        busca = (request.args.get("busca") or "").strip()
        status = request.args.get("status_ativo") or "todos"
        if status not in ("todos", "ativos", "encerrados"): status = "todos"
        try: vinculos = servico().listar_vinculos(busca, status)
        except AtivoServiceError:
            current_app.logger.exception("Falha ao listar vínculos")
            flash("Não foi possível carregar os vínculos.", "danger")
            vinculos = []
        return render_template("fiscalizacao_contratos/ativos/vinculos_lista.html", vinculos=vinculos, busca=busca, status_ativo=status)

    @blueprint.route("/ativos/vinculos/<int:vinculo_id>/encerrar", methods=["POST"])
    @admin_required
    def ativos_vinculo_encerrar(vinculo_id):
        try: vinculo = servico().obter_vinculo(vinculo_id)
        except VinculoNaoEncontradoError:
            flash("Vínculo não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.ativos_vinculos_lista"))
        fim, erros = normalizar_data_encerramento(request.form.get("data_fim"), vinculo["data_inicio"])
        if erros:
            for erro in erros: flash(erro, "danger")
            return redirect(url_for("fiscalizacao_contratos.ativos_detalhe", ativo_id=vinculo["ativo_id"]))
        try: ativo_id = servico().encerrar_vinculo(vinculo_id, fim, current_user.id)
        except (AtivoBloqueadoError, ReferenciaAtivoInvalidaError) as erro: flash(str(erro), "warning")
        except AtivoServiceError:
            current_app.logger.exception("Falha ao encerrar vínculo")
            flash("Não foi possível encerrar o vínculo.", "danger")
        else:
            flash("Vínculo encerrado e preservado no histórico.", "success")
            return redirect(url_for("fiscalizacao_contratos.ativos_detalhe", ativo_id=ativo_id))
        return redirect(url_for("fiscalizacao_contratos.ativos_detalhe", ativo_id=vinculo["ativo_id"]))
