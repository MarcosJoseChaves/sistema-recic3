"""Rotas administrativas das fiscalizações contratuais."""

from datetime import date

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user

from ..permissions import admin_required
from ..services.fiscalizacoes_service import (
    FiscalizacaoBloqueadaError, FiscalizacaoNaoEncontradaError,
    FiscalizacaoService, FiscalizacaoServiceError,
    ReferenciaFiscalizacaoInvalidaError,
)
from ..services.ocorrencias_service import OcorrenciaService, OcorrenciaServiceError
from ..validacoes_fiscalizacoes import (
    RESULTADOS_FISCALIZACAO, STATUS_FISCALIZACAO, TIPOS_FISCALIZACAO,
    normalizar_e_validar_fiscalizacao,
)


def registrar_rotas_fiscalizacoes(blueprint, conectar_banco):
    def servico(): return FiscalizacaoService(conectar_banco)

    def inteiro(valor):
        try: return int(valor) if valor else None
        except (TypeError, ValueError): return None

    def data_filtro(valor):
        try: return date.fromisoformat(valor) if valor else None
        except ValueError: return None

    def opcoes():
        try: return servico().opcoes()
        except FiscalizacaoServiceError:
            current_app.logger.exception("Falha ao carregar opções de fiscalização")
            flash("Não foi possível carregar contratos e servidores.", "danger")
            return [], [], []

    def formulario(item, modo, status_http=200):
        contratos, servidores, _ = opcoes()
        return render_template("fiscalizacao_contratos/fiscalizacoes/form.html",
            fiscalizacao=item, modo=modo, contratos=contratos, servidores=servidores,
            tipos=TIPOS_FISCALIZACAO, resultados=RESULTADOS_FISCALIZACAO), status_http

    @blueprint.route("/fiscalizacoes", methods=["GET"])
    @admin_required
    def fiscalizacoes_lista():
        filtros = {
            "contrato_id": inteiro(request.args.get("contrato_id")),
            "empresa_id": inteiro(request.args.get("empresa_id")),
            "servidor_id": inteiro(request.args.get("servidor_id")),
            "tipo": request.args.get("tipo", ""), "resultado": request.args.get("resultado", ""),
            "status": request.args.get("status", ""), "data_inicio": data_filtro(request.args.get("data_inicio")),
            "data_fim": data_filtro(request.args.get("data_fim")), "status_ativo": request.args.get("status_ativo", "ativos"),
        }
        busca=(request.args.get("busca") or "").strip()
        try: itens=servico().listar(busca,filtros); contratos,servidores,empresas=servico().opcoes()
        except FiscalizacaoServiceError:
            current_app.logger.exception("Falha ao listar fiscalizações"); flash("Não foi possível carregar as fiscalizações.","danger")
            itens=[]; contratos=[]; servidores=[]; empresas=[]
        return render_template("fiscalizacao_contratos/fiscalizacoes/lista.html", fiscalizacoes=itens,
            contratos=contratos,servidores=servidores,empresas=empresas,busca=busca,filtros=filtros,
            tipos=TIPOS_FISCALIZACAO,resultados=RESULTADOS_FISCALIZACAO,status_fiscalizacao=STATUS_FISCALIZACAO)

    @blueprint.route("/fiscalizacoes/nova", methods=["GET","POST"])
    @admin_required
    def fiscalizacoes_nova():
        if request.method=="GET": return formulario({"contrato_id":inteiro(request.args.get("contrato_id")),"status":"Em elaboração"},"nova")
        dados,erros=normalizar_e_validar_fiscalizacao(request.form)
        if erros:
            for erro in erros: flash(erro,"danger")
            return formulario(dados,"nova",400)
        try: novo=servico().criar(dados,current_user.id)
        except ReferenciaFiscalizacaoInvalidaError as erro: flash(str(erro),"danger"); return formulario(dados,"nova",400)
        except FiscalizacaoServiceError:
            current_app.logger.exception("Falha ao criar fiscalização"); flash("Não foi possível cadastrar a fiscalização.","danger"); return formulario(dados,"nova",500)
        flash("Fiscalização cadastrada em elaboração.","success")
        return redirect(url_for("fiscalizacao_contratos.fiscalizacoes_detalhe",fiscalizacao_id=novo))

    @blueprint.route("/fiscalizacoes/<int:fiscalizacao_id>", methods=["GET"])
    @admin_required
    def fiscalizacoes_detalhe(fiscalizacao_id):
        try:
            item=servico().obter(fiscalizacao_id)
            ocorrencias=OcorrenciaService(conectar_banco).listar("",{"fiscalizacao_id":fiscalizacao_id,"status_ativo":"todos"})
        except (FiscalizacaoNaoEncontradaError,OcorrenciaServiceError):
            flash("Fiscalização não encontrada ou indisponível.","warning"); return redirect(url_for("fiscalizacao_contratos.fiscalizacoes_lista"))
        return render_template("fiscalizacao_contratos/fiscalizacoes/detalhe.html",fiscalizacao=item,ocorrencias=ocorrencias)

    @blueprint.route("/fiscalizacoes/<int:fiscalizacao_id>/editar", methods=["GET","POST"])
    @admin_required
    def fiscalizacoes_editar(fiscalizacao_id):
        try: atual=servico().obter(fiscalizacao_id)
        except FiscalizacaoServiceError: flash("Fiscalização não encontrada.","warning"); return redirect(url_for("fiscalizacao_contratos.fiscalizacoes_lista"))
        if atual["status"]!="Em elaboração" or not atual["ativo"]:
            flash("Esta fiscalização não pode mais ser editada.","warning"); return redirect(url_for("fiscalizacao_contratos.fiscalizacoes_detalhe",fiscalizacao_id=fiscalizacao_id))
        if request.method=="GET": return formulario(dict(atual),"editar")
        dados,erros=normalizar_e_validar_fiscalizacao(request.form)
        if erros:
            for erro in erros: flash(erro,"danger")
            return formulario(dados,"editar",400)
        try: servico().atualizar(fiscalizacao_id,dados,current_user.id)
        except (ReferenciaFiscalizacaoInvalidaError,FiscalizacaoBloqueadaError) as erro: flash(str(erro),"danger"); return formulario(dados,"editar",400)
        except FiscalizacaoServiceError: current_app.logger.exception("Falha ao editar fiscalização"); flash("Não foi possível atualizar a fiscalização.","danger"); return formulario(dados,"editar",500)
        flash("Fiscalização atualizada.","success"); return redirect(url_for("fiscalizacao_contratos.fiscalizacoes_detalhe",fiscalizacao_id=fiscalizacao_id))

    def alterar_status(fiscalizacao_id,status):
        try: servico().alterar_status(fiscalizacao_id,status,current_user.id); flash("Fiscalização finalizada." if status=="Finalizada" else "Fiscalização cancelada sem exclusão.","success")
        except FiscalizacaoBloqueadaError as erro: flash(str(erro),"warning")
        except FiscalizacaoServiceError: current_app.logger.exception("Falha ao alterar fiscalização"); flash("Não foi possível alterar a fiscalização.","danger")
        return redirect(url_for("fiscalizacao_contratos.fiscalizacoes_detalhe",fiscalizacao_id=fiscalizacao_id))

    @blueprint.route("/fiscalizacoes/<int:fiscalizacao_id>/finalizar",methods=["POST"])
    @admin_required
    def fiscalizacoes_finalizar(fiscalizacao_id): return alterar_status(fiscalizacao_id,"Finalizada")

    @blueprint.route("/fiscalizacoes/<int:fiscalizacao_id>/cancelar",methods=["POST"])
    @admin_required
    def fiscalizacoes_cancelar(fiscalizacao_id): return alterar_status(fiscalizacao_id,"Cancelada")
