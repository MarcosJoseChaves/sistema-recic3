"""Rotas administrativas dos atestes da execução."""

from datetime import date

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user

from logging_operacional import registrar_evento

from ..permissions import admin_required
from ..services.cloudinary_storage import CloudinaryStorage, CloudinaryStorageError
from ..services.atestes_service import (
    AtesteBloqueadoError, AtesteDuplicadoError, AtesteNaoEncontradoError,
    AtesteService, AtesteServiceError, ReferenciaAtesteInvalidaError,
)
from ..services.documentos_service import DocumentoService, DocumentoServiceError
from ..validacoes_atestes import (
    CATEGORIAS_DOCUMENTO_ATESTE, STATUS_ATESTES, inteiro_positivo,
    normalizar_ateste, normalizar_nota,
)
from ..validacoes_medicoes import competencia_mes
from ..validacoes_documentos import (
    ValidacaoDocumentoError, limite_upload_bytes, validar_arquivo_documento,
)


ERROS_NEGOCIO = (
    AtesteBloqueadoError, AtesteDuplicadoError, AtesteNaoEncontradoError,
    ReferenciaAtesteInvalidaError,
)


def registrar_rotas_atestes(blueprint, conectar_banco):
    def servico(): return AtesteService(conectar_banco)
    def voltar(ateste_id): return redirect(url_for("fiscalizacao_contratos.atestes_detalhe",ateste_id=ateste_id))

    def data_filtro(valor):
        try: return date.fromisoformat(valor) if valor else None
        except (TypeError,ValueError): return None

    def opcoes():
        try: return servico().opcoes()
        except AtesteServiceError:
            current_app.logger.exception("Falha ao carregar opções de ateste")
            flash("Não foi possível carregar medições e servidores.","danger")
            return [],[],[]

    def formulario(ateste,modo,status_http=200):
        medicoes,servidores,_=opcoes()
        return render_template("fiscalizacao_contratos/atestes/form.html",ateste=ateste,modo=modo,medicoes=medicoes,servidores=servidores),status_http

    @blueprint.route("/atestes",methods=["GET"])
    @admin_required
    def atestes_lista():
        filtros={
            "contrato_id":inteiro_positivo(request.args.get("contrato_id")),
            "empresa_id":inteiro_positivo(request.args.get("empresa_id")),
            "competencia":competencia_mes(request.args.get("competencia")),
            "servidor_id":inteiro_positivo(request.args.get("servidor_id")),
            "status":request.args.get("status","") if request.args.get("status","") in ("",*STATUS_ATESTES) else "",
            "ateste_inicio":data_filtro(request.args.get("ateste_inicio")),"ateste_fim":data_filtro(request.args.get("ateste_fim")),
            "encaminhamento_inicio":data_filtro(request.args.get("encaminhamento_inicio")),
            "encaminhamento_fim":data_filtro(request.args.get("encaminhamento_fim")),
            "status_ativo":request.args.get("status_ativo","ativos"),
            "com_diferenca":request.args.get("com_diferenca")=="1","encaminhados":request.args.get("encaminhados")=="1",
        }
        busca=(request.args.get("busca") or "").strip()
        try: atestes=servico().listar(busca,filtros);medicoes,servidores,empresas=servico().opcoes()
        except AtesteServiceError:
            current_app.logger.exception("Falha ao listar atestes");flash("Não foi possível carregar os atestes.","danger")
            atestes,medicoes,servidores,empresas=[],[],[],[]
        return render_template("fiscalizacao_contratos/atestes/lista.html",atestes=atestes,medicoes=medicoes,
            servidores=servidores,empresas=empresas,status_atestes=STATUS_ATESTES,busca=busca,filtros=filtros)

    @blueprint.route("/atestes/novo",methods=["GET","POST"])
    @admin_required
    def atestes_novo():
        if request.method=="GET": return formulario({"medicao_id":inteiro_positivo(request.args.get("medicao_id")),"numero_ateste":1},"novo")
        dados,erros=normalizar_ateste(request.form)
        if erros:
            for erro in erros: flash(erro,"danger")
            return formulario(dados,"novo",400)
        try: identificador=servico().criar(dados,current_user.id)
        except ERROS_NEGOCIO as erro: flash(str(erro),"danger");return formulario(dados,"novo",400)
        except AtesteServiceError: current_app.logger.exception("Falha ao criar ateste");flash("Não foi possível criar o ateste.","danger");return formulario(dados,"novo",500)
        flash("Ateste criado em elaboração.","success");return voltar(identificador)

    @blueprint.route("/atestes/<int:ateste_id>",methods=["GET"])
    @admin_required
    def atestes_detalhe(ateste_id):
        try: ateste,notas,documentos,eventos,total,diferenca=servico().obter(ateste_id)
        except AtesteNaoEncontradoError: flash("Ateste não encontrado.","warning");return redirect(url_for("fiscalizacao_contratos.atestes_lista"))
        except AtesteServiceError: current_app.logger.exception("Falha ao carregar ateste");flash("Não foi possível carregar o ateste.","danger");return redirect(url_for("fiscalizacao_contratos.atestes_lista"))
        return render_template("fiscalizacao_contratos/atestes/detalhe.html",ateste=ateste,notas=notas,
            documentos=documentos,eventos=eventos,total_notas=total,diferenca_notas=diferenca)

    @blueprint.route("/atestes/<int:ateste_id>/editar",methods=["GET","POST"])
    @admin_required
    def atestes_editar(ateste_id):
        try: atual=servico().obter(ateste_id)[0]
        except AtesteServiceError: flash("Ateste não encontrado.","warning");return redirect(url_for("fiscalizacao_contratos.atestes_lista"))
        if atual["status"] not in ("Em elaboração","Devolvido para correção") or not atual["ativo"]:
            flash("Este ateste não pode ser editado.","warning");return voltar(ateste_id)
        if request.method=="GET": return formulario(dict(atual),"editar")
        dados,erros=normalizar_ateste({**request.form,"medicao_id":str(atual["medicao_id"])})
        if erros:
            for erro in erros: flash(erro,"danger")
            return formulario(dados,"editar",400)
        try: servico().atualizar(ateste_id,dados,current_user.id)
        except ERROS_NEGOCIO as erro: flash(str(erro),"danger");return formulario(dados,"editar",400)
        except AtesteServiceError: current_app.logger.exception("Falha ao editar ateste");flash("Não foi possível atualizar o ateste.","danger");return formulario(dados,"editar",500)
        flash("Ateste atualizado.","success");return voltar(ateste_id)

    def formulario_nota(ateste_id,nota,modo,status_http=200):
        try:
            ateste=servico().obter(ateste_id)[0]
            if not ateste["ativo"] or ateste["status"] not in ("Em elaboração","Devolvido para correção","Atestado"):
                flash("Notas fiscais não podem mais ser alteradas.","warning")
                return voltar(ateste_id)
            documentos=DocumentoService(conectar_banco).listar_do_contrato(ateste["contrato_id"])
        except (AtesteServiceError,DocumentoServiceError): flash("Não foi possível carregar o formulário da nota.","danger");return voltar(ateste_id)
        return render_template("fiscalizacao_contratos/atestes/nota_form.html",ateste=ateste,nota=nota,modo=modo,
            documentos=[d for d in documentos if d["ativo"]],limite_mb=limite_upload_bytes()//(1024*1024)),status_http

    def salvar_nota(ateste_id,nota_id=None):
        atual={}
        if nota_id:
            try: atual=next(n for n in servico().obter(ateste_id)[1] if n["id"]==nota_id and n["ativo"])
            except (AtesteServiceError,StopIteration): flash("Nota fiscal não encontrada.","warning");return voltar(ateste_id)
        if request.method=="GET": return formulario_nota(ateste_id,dict(atual),"editar" if nota_id else "nova")
        dados,erros=normalizar_nota(request.form)
        arquivo_enviado=request.files.get("arquivo")
        arquivo=None
        if arquivo_enviado and arquivo_enviado.filename:
            if dados.get("documento_id"): erros.append("Escolha entre um documento existente e um novo arquivo.")
            try: arquivo=validar_arquivo_documento(arquivo_enviado)
            except ValidacaoDocumentoError as erro: erros.append(str(erro))
        if erros:
            for erro in erros: flash(erro,"danger")
            return formulario_nota(ateste_id,dados,"editar" if nota_id else "nova",400)
        try:
            if arquivo:
                servico().salvar_nota_com_upload(ateste_id,dados,arquivo,current_user.id,CloudinaryStorage(),nota_id)
            else: servico().salvar_nota(ateste_id,dados,current_user.id,nota_id)
        except ERROS_NEGOCIO as erro: flash(str(erro),"danger");return formulario_nota(ateste_id,dados,"editar" if nota_id else "nova",400)
        except CloudinaryStorageError:
            registrar_evento(
                "upload_rejected",
                nivel="ERROR",
                mensagem="Falha no armazenamento da nota fiscal.",
                error_type="CloudinaryStorageError",
            )
            flash("Não foi possível enviar o arquivo agora. Tente novamente.","danger")
            return formulario_nota(
                ateste_id,
                dados,
                "editar" if nota_id else "nova",
                500,
            )
        except AtesteServiceError: current_app.logger.exception("Falha ao salvar nota");flash("Não foi possível salvar a nota fiscal.","danger");return formulario_nota(ateste_id,dados,"editar" if nota_id else "nova",500)
        flash("Nota fiscal salva.","success");return voltar(ateste_id)

    @blueprint.route("/atestes/<int:ateste_id>/notas/nova",methods=["GET","POST"])
    @admin_required
    def atestes_nota_nova(ateste_id): return salvar_nota(ateste_id)

    @blueprint.route("/atestes/<int:ateste_id>/notas/<int:nota_id>/editar",methods=["GET","POST"])
    @admin_required
    def atestes_nota_editar(ateste_id,nota_id): return salvar_nota(ateste_id,nota_id)

    @blueprint.route("/atestes/<int:ateste_id>/notas/<int:nota_id>/inativar",methods=["POST"])
    @admin_required
    def atestes_nota_inativar(ateste_id,nota_id):
        return executar(ateste_id,lambda:servico().inativar_nota(ateste_id,nota_id,current_user.id),"Nota fiscal inativada; documento e histórico preservados.")

    @blueprint.route("/atestes/<int:ateste_id>/documentos/vincular",methods=["GET","POST"])
    @admin_required
    def atestes_documento_vincular(ateste_id):
        try:
            ateste=servico().obter(ateste_id)[0]
            if not ateste["ativo"] or ateste["status"] not in ("Em elaboração","Devolvido para correção","Atestado"):
                flash("Documentos não podem mais ser alterados.","warning")
                return voltar(ateste_id)
            documentos=[d for d in DocumentoService(conectar_banco).listar_do_contrato(ateste["contrato_id"]) if d["ativo"]]
        except (AtesteServiceError,DocumentoServiceError): flash("Não foi possível carregar documentos.","danger");return voltar(ateste_id)
        dados={"documento_id":inteiro_positivo(request.form.get("documento_id")),"categoria":(request.form.get("categoria") or "").strip(),"observacoes":(request.form.get("observacoes") or "").strip() or None}
        if request.method=="POST":
            erros=[]
            if not dados["documento_id"]: erros.append("Selecione o documento.")
            if dados["categoria"] not in CATEGORIAS_DOCUMENTO_ATESTE: erros.append("Selecione uma categoria válida.")
            if not erros:
                try: servico().vincular_documento(ateste_id,dados["documento_id"],dados["categoria"],dados["observacoes"],current_user.id)
                except ERROS_NEGOCIO as erro: erros.append(str(erro))
                except AtesteServiceError: current_app.logger.exception("Falha ao vincular documento");erros.append("Não foi possível vincular o documento.")
                else: flash("Documento vinculado sem duplicar o arquivo.","success");return voltar(ateste_id)
            for erro in erros: flash(erro,"danger")
        return render_template("fiscalizacao_contratos/atestes/documento_form.html",ateste=ateste,documentos=documentos,
            categorias=CATEGORIAS_DOCUMENTO_ATESTE,vinculo=dados),400 if request.method=="POST" else 200

    @blueprint.route("/atestes/<int:ateste_id>/documentos/<int:vinculo_id>/inativar",methods=["POST"])
    @admin_required
    def atestes_documento_inativar(ateste_id,vinculo_id):
        return executar(ateste_id,lambda:servico().inativar_documento(ateste_id,vinculo_id,current_user.id),"Vínculo inativado; arquivo original preservado.")

    def executar(ateste_id,operacao,sucesso):
        try: operacao()
        except ERROS_NEGOCIO as erro: flash(str(erro),"warning")
        except AtesteServiceError: current_app.logger.exception("Falha em operação de ateste");flash("Não foi possível concluir a operação.","danger")
        else: flash(sucesso,"success")
        return voltar(ateste_id)

    @blueprint.route("/atestes/<int:ateste_id>/atestar",methods=["POST"])
    @admin_required
    def atestes_atestar(ateste_id): return executar(ateste_id,lambda:servico().atestar(ateste_id,current_user.id),"Execução atestada.")

    def acao(ateste_id,tipo,titulo):
        try: ateste=servico().obter(ateste_id)[0];servidores=opcoes()[1] if tipo=="encaminhar" else []
        except AtesteServiceError: flash("Ateste não encontrado.","warning");return redirect(url_for("fiscalizacao_contratos.atestes_lista"))
        permitidos={"devolver":("Em elaboração","Atestado"),"retornar":("Devolvido para correção",),
            "encaminhar":("Atestado",),"cancelar":("Em elaboração","Devolvido para correção","Atestado")}
        if not ateste["ativo"] or ateste["status"] not in permitidos[tipo]:
            flash("A situação atual não permite esta ação.","warning");return voltar(ateste_id)
        if request.method=="POST":
            justificativa=(request.form.get("justificativa") or "").strip()
            try:
                if tipo=="devolver": servico().devolver(ateste_id,justificativa,current_user.id)
                elif tipo=="retornar": servico().retornar_elaboracao(ateste_id,justificativa,current_user.id)
                elif tipo=="cancelar": servico().cancelar(ateste_id,justificativa,current_user.id)
                else: servico().encaminhar(ateste_id,request.form.get("protocolo"),inteiro_positivo(request.form.get("servidor_encaminhador_id")),current_user.id)
            except ERROS_NEGOCIO as erro: flash(str(erro),"danger")
            except AtesteServiceError: current_app.logger.exception("Falha ao alterar fluxo do ateste");flash("Não foi possível concluir a ação.","danger")
            else: flash(titulo+" concluído.","success");return voltar(ateste_id)
        return render_template("fiscalizacao_contratos/atestes/acao_form.html",ateste=ateste,tipo=tipo,titulo=titulo,servidores=servidores),400 if request.method=="POST" else 200

    @blueprint.route("/atestes/<int:ateste_id>/devolver",methods=["GET","POST"])
    @admin_required
    def atestes_devolver(ateste_id): return acao(ateste_id,"devolver","Devolução para correção")

    @blueprint.route("/atestes/<int:ateste_id>/retornar",methods=["GET","POST"])
    @admin_required
    def atestes_retornar(ateste_id): return acao(ateste_id,"retornar","Retorno para elaboração")

    @blueprint.route("/atestes/<int:ateste_id>/encaminhar",methods=["GET","POST"])
    @admin_required
    def atestes_encaminhar(ateste_id): return acao(ateste_id,"encaminhar","Encaminhamento para pagamento")

    @blueprint.route("/atestes/<int:ateste_id>/cancelar",methods=["GET","POST"])
    @admin_required
    def atestes_cancelar(ateste_id): return acao(ateste_id,"cancelar","Cancelamento")

    @blueprint.route("/atestes/<int:ateste_id>/eventos",methods=["GET"])
    @admin_required
    def atestes_eventos(ateste_id): return redirect(url_for("fiscalizacao_contratos.atestes_detalhe",ateste_id=ateste_id,_anchor="historico"))
