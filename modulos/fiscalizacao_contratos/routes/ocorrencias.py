"""Rotas administrativas das ocorrências e seus acompanhamentos."""

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user

from ..permissions import admin_required
from ..services.ocorrencias_service import (
    OcorrenciaBloqueadaError, OcorrenciaNaoEncontradaError, OcorrenciaService,
    OcorrenciaServiceError, ReferenciaOcorrenciaInvalidaError,
)
from ..validacoes_ocorrencias import (
    CATEGORIAS_OCORRENCIA, GRAVIDADES_OCORRENCIA, STATUS_OCORRENCIA,
    normalizar_e_validar_acompanhamento, normalizar_e_validar_ocorrencia,
)


def registrar_rotas_ocorrencias(blueprint, conectar_banco):
    def servico(): return OcorrenciaService(conectar_banco)
    def inteiro(valor):
        try: return int(valor) if valor else None
        except (TypeError,ValueError): return None
    def booleano(nome): return str(request.args.get(nome) or "").lower() in ("1","true","on","sim")
    def opcoes():
        try: return servico().opcoes()
        except OcorrenciaServiceError:
            current_app.logger.exception("Falha ao carregar opções de ocorrência"); flash("Não foi possível carregar as opções.","danger")
            return [],[],[],[]
    def formulario(item,modo,status_http=200):
        contratos,servidores,fiscalizacoes,ativos=opcoes()
        return render_template("fiscalizacao_contratos/ocorrencias/form.html",ocorrencia=item,modo=modo,
            contratos=contratos,servidores=servidores,fiscalizacoes=fiscalizacoes,ativos=ativos,
            categorias=CATEGORIAS_OCORRENCIA,gravidades=GRAVIDADES_OCORRENCIA),status_http

    @blueprint.route("/ocorrencias",methods=["GET"])
    @admin_required
    def ocorrencias_lista():
        filtros={"contrato_id":inteiro(request.args.get("contrato_id")),"fiscalizacao_id":inteiro(request.args.get("fiscalizacao_id")),
            "categoria":request.args.get("categoria",""),"gravidade":request.args.get("gravidade",""),"status":request.args.get("status",""),
            "servidor_id":inteiro(request.args.get("servidor_id")),"vencidas":booleano("vencidas"),"notificacao":booleano("notificacao"),
            "status_ativo":request.args.get("status_ativo","ativos")}
        busca=(request.args.get("busca") or "").strip()
        try: itens=servico().listar(busca,filtros); contratos,servidores,fiscalizacoes,_=servico().opcoes()
        except OcorrenciaServiceError: current_app.logger.exception("Falha ao listar ocorrências"); flash("Não foi possível carregar as ocorrências.","danger"); itens=[];contratos=[];servidores=[];fiscalizacoes=[]
        return render_template("fiscalizacao_contratos/ocorrencias/lista.html",ocorrencias=itens,busca=busca,filtros=filtros,
            contratos=contratos,servidores=servidores,fiscalizacoes=fiscalizacoes,categorias=CATEGORIAS_OCORRENCIA,
            gravidades=GRAVIDADES_OCORRENCIA,status_ocorrencia=STATUS_OCORRENCIA)

    @blueprint.route("/ocorrencias/nova",methods=["GET","POST"])
    @admin_required
    def ocorrencias_nova():
        if request.method=="GET": return formulario({"contrato_id":inteiro(request.args.get("contrato_id")),"fiscalizacao_id":inteiro(request.args.get("fiscalizacao_id")),"ativo_contratual_id":inteiro(request.args.get("ativo_id")),"status":"Aberta"},"nova")
        dados,erros=normalizar_e_validar_ocorrencia(request.form)
        if erros:
            for erro in erros: flash(erro,"danger")
            return formulario(dados,"nova",400)
        try: novo=servico().criar(dados,current_user.id)
        except ReferenciaOcorrenciaInvalidaError as erro: flash(str(erro),"danger"); return formulario(dados,"nova",400)
        except OcorrenciaServiceError: current_app.logger.exception("Falha ao cadastrar ocorrência");flash("Não foi possível cadastrar a ocorrência.","danger");return formulario(dados,"nova",500)
        flash("Ocorrência cadastrada.","success");return redirect(url_for("fiscalizacao_contratos.ocorrencias_detalhe",ocorrencia_id=novo))

    @blueprint.route("/ocorrencias/<int:ocorrencia_id>",methods=["GET"])
    @admin_required
    def ocorrencias_detalhe(ocorrencia_id):
        try: item,acompanhamentos=servico().obter(ocorrencia_id)
        except OcorrenciaServiceError: flash("Ocorrência não encontrada.","warning");return redirect(url_for("fiscalizacao_contratos.ocorrencias_lista"))
        return render_template("fiscalizacao_contratos/ocorrencias/detalhe.html",ocorrencia=item,acompanhamentos=acompanhamentos,status_ocorrencia=STATUS_OCORRENCIA)

    @blueprint.route("/ocorrencias/<int:ocorrencia_id>/editar",methods=["GET","POST"])
    @admin_required
    def ocorrencias_editar(ocorrencia_id):
        try: atual,acompanhamentos=servico().obter(ocorrencia_id)
        except OcorrenciaServiceError: flash("Ocorrência não encontrada.","warning");return redirect(url_for("fiscalizacao_contratos.ocorrencias_lista"))
        if acompanhamentos or not atual["ativo"]: flash("Esta ocorrência não pode mais ser editada.","warning");return redirect(url_for("fiscalizacao_contratos.ocorrencias_detalhe",ocorrencia_id=ocorrencia_id))
        if request.method=="GET": return formulario(dict(atual),"editar")
        dados,erros=normalizar_e_validar_ocorrencia(request.form)
        if erros:
            for erro in erros: flash(erro,"danger")
            return formulario(dados,"editar",400)
        try: servico().atualizar(ocorrencia_id,dados,current_user.id)
        except (ReferenciaOcorrenciaInvalidaError,OcorrenciaBloqueadaError) as erro: flash(str(erro),"danger");return formulario(dados,"editar",400)
        except OcorrenciaServiceError: current_app.logger.exception("Falha ao editar ocorrência");flash("Não foi possível atualizar a ocorrência.","danger");return formulario(dados,"editar",500)
        flash("Ocorrência atualizada.","success");return redirect(url_for("fiscalizacao_contratos.ocorrencias_detalhe",ocorrencia_id=ocorrencia_id))

    def alterar_ativo(ocorrencia_id,valor):
        try: servico().alterar_ativo(ocorrencia_id,current_user.id,valor);flash("Ocorrência reativada." if valor else "Ocorrência inativada sem exclusão.","success")
        except OcorrenciaServiceError: current_app.logger.exception("Falha ao alterar ocorrência");flash("Não foi possível alterar a ocorrência.","danger")
        return redirect(url_for("fiscalizacao_contratos.ocorrencias_detalhe",ocorrencia_id=ocorrencia_id))

    @blueprint.route("/ocorrencias/<int:ocorrencia_id>/inativar",methods=["POST"])
    @admin_required
    def ocorrencias_inativar(ocorrencia_id): return alterar_ativo(ocorrencia_id,False)
    @blueprint.route("/ocorrencias/<int:ocorrencia_id>/reativar",methods=["POST"])
    @admin_required
    def ocorrencias_reativar(ocorrencia_id): return alterar_ativo(ocorrencia_id,True)

    @blueprint.route("/ocorrencias/<int:ocorrencia_id>/acompanhamentos/novo",methods=["GET","POST"])
    @admin_required
    def ocorrencias_acompanhamento_novo(ocorrencia_id):
        try: item,_=servico().obter(ocorrencia_id)
        except OcorrenciaServiceError: flash("Ocorrência não encontrada.","warning");return redirect(url_for("fiscalizacao_contratos.ocorrencias_lista"))
        if not item["ativo"]: flash("A ocorrência está inativa.","warning");return redirect(url_for("fiscalizacao_contratos.ocorrencias_detalhe",ocorrencia_id=ocorrencia_id))
        if request.method=="GET": return render_template("fiscalizacao_contratos/ocorrencias/acompanhamento_form.html",ocorrencia=item,acompanhamento={},status_ocorrencia=STATUS_OCORRENCIA)
        dados,erros=normalizar_e_validar_acompanhamento(request.form,item)
        if erros:
            for erro in erros: flash(erro,"danger")
            return render_template("fiscalizacao_contratos/ocorrencias/acompanhamento_form.html",ocorrencia=item,acompanhamento=dados,status_ocorrencia=STATUS_OCORRENCIA),400
        try: servico().adicionar_acompanhamento(ocorrencia_id,dados,current_user.id)
        except OcorrenciaBloqueadaError as erro: flash(str(erro),"danger")
        except OcorrenciaServiceError: current_app.logger.exception("Falha ao adicionar acompanhamento");flash("Não foi possível registrar o acompanhamento.","danger")
        else: flash("Acompanhamento registrado e situação atualizada.","success")
        return redirect(url_for("fiscalizacao_contratos.ocorrencias_detalhe",ocorrencia_id=ocorrencia_id))
