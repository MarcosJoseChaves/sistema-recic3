"""Rotas administrativas das medições contratuais."""

from datetime import date

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user

from ..permissions import admin_required
from ..services.cloudinary_storage import CloudinaryStorage, CloudinaryStorageError
from ..services.atestes_service import AtesteService, AtesteServiceError
from ..services.medicoes_service import (
    MedicaoBloqueadaError,
    MedicaoDuplicadaError,
    MedicaoNaoEncontradaError,
    MedicaoService,
    MedicaoServiceError,
    ReferenciaMedicaoInvalidaError,
)
from ..validacoes_medicoes import (
    CATEGORIAS_DOCUMENTO_MEDICAO,
    STATUS_MEDICAO,
    TIPOS_AJUSTE,
    competencia_mes,
    inteiro_positivo,
    normalizar_e_validar_ajuste,
    normalizar_e_validar_item,
    normalizar_e_validar_medicao,
)
from ..validacoes_documentos import (
    ValidacaoDocumentoError,
    limite_upload_bytes,
    validar_arquivo_documento,
)


ERROS_NEGOCIO = (
    MedicaoBloqueadaError,
    MedicaoDuplicadaError,
    MedicaoNaoEncontradaError,
    ReferenciaMedicaoInvalidaError,
)

TIPO_DOCUMENTO_POR_CATEGORIA_MEDICAO = {
    "Memória de cálculo": "Planilha Orçamentária",
    "Relatório de medição": "Relatório",
    "Evidência da execução": "Comprovante",
    "Nota fiscal": "Comprovante",
    "Planilha": "Planilha Orçamentária",
    "Ordem de serviço": "Ordem de Serviço",
    "Outro": "Outro",
}


def registrar_rotas_medicoes(blueprint, conectar_banco):
    """Registra as rotas usando a conexão entregue pelo sistema principal."""

    def servico():
        return MedicaoService(conectar_banco)

    def inteiro(valor):
        return inteiro_positivo(valor)

    def data_filtro(valor):
        try:
            return date.fromisoformat(valor) if valor else None
        except (TypeError, ValueError):
            return None

    def carregar_opcoes():
        try:
            return servico().opcoes()
        except MedicaoServiceError:
            current_app.logger.exception("Falha ao carregar opções de medição")
            flash("Não foi possível carregar contratos e servidores.", "danger")
            return [], [], []

    def formulario(medicao, modo, status_http=200):
        contratos, servidores, _ = carregar_opcoes()
        return (
            render_template(
                "fiscalizacao_contratos/medicoes/form.html",
                medicao=medicao,
                modo=modo,
                contratos=contratos,
                servidores=servidores,
            ),
            status_http,
        )

    def obter_detalhes(medicao_id):
        return servico().obter(medicao_id)

    def voltar(medicao_id):
        return redirect(
            url_for("fiscalizacao_contratos.medicoes_detalhe", medicao_id=medicao_id)
        )

    def esta_editavel(medicao):
        return (
            medicao["ativo"]
            and medicao["atual"]
            and medicao["status"] in ("Em elaboração", "Devolvida para correção")
        )

    def exigir_editavel_na_tela(medicao):
        if esta_editavel(medicao):
            return True
        flash("Esta versão da medição não pode ser alterada.", "warning")
        return False

    @blueprint.route("/medicoes", methods=["GET"])
    @admin_required
    def medicoes_lista():
        filtros = {
            "contrato_id": inteiro(request.args.get("contrato_id")),
            "empresa_id": inteiro(request.args.get("empresa_id")),
            "competencia": competencia_mes(request.args.get("competencia")),
            "periodo_inicio": data_filtro(request.args.get("periodo_inicio")),
            "periodo_fim": data_filtro(request.args.get("periodo_fim")),
            "servidor_id": inteiro(request.args.get("servidor_id")),
            "status": request.args.get("status", ""),
            "versoes": request.args.get("versoes", "atuais"),
            "status_ativo": request.args.get("status_ativo", "ativos"),
            "com_glosa": request.args.get("com_glosa") == "1",
            "com_desconto": request.args.get("com_desconto") == "1",
        }
        busca = (request.args.get("busca") or "").strip()
        try:
            medicoes = servico().listar(busca, filtros)
            contratos, servidores, empresas = servico().opcoes()
        except MedicaoServiceError:
            current_app.logger.exception("Falha ao listar medições")
            flash("Não foi possível carregar as medições.", "danger")
            medicoes, contratos, servidores, empresas = [], [], [], []
        return render_template(
            "fiscalizacao_contratos/medicoes/lista.html",
            medicoes=medicoes,
            contratos=contratos,
            servidores=servidores,
            empresas=empresas,
            status_medicao=STATUS_MEDICAO,
            busca=busca,
            filtros=filtros,
        )

    @blueprint.route("/medicoes/nova", methods=["GET", "POST"])
    @admin_required
    def medicoes_nova():
        if request.method == "GET":
            return formulario(
                {
                    "contrato_id": inteiro(request.args.get("contrato_id")),
                    "status": "Em elaboração",
                },
                "nova",
            )
        dados, erros = normalizar_e_validar_medicao(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return formulario(dados, "nova", 400)
        try:
            medicao_id = servico().criar(dados, current_user.id)
        except ERROS_NEGOCIO as erro:
            flash(str(erro), "danger")
            return formulario(dados, "nova", 400)
        except MedicaoServiceError:
            current_app.logger.exception("Falha ao cadastrar medição")
            flash("Não foi possível cadastrar a medição.", "danger")
            return formulario(dados, "nova", 500)
        flash("Medição cadastrada em elaboração.", "success")
        return voltar(medicao_id)

    @blueprint.route("/medicoes/<int:medicao_id>", methods=["GET"])
    @admin_required
    def medicoes_detalhe(medicao_id):
        try:
            medicao, itens, ajustes, documentos, eventos, versoes = obter_detalhes(medicao_id)
        except MedicaoServiceError:
            flash("Medição não encontrada ou indisponível.", "warning")
            return redirect(url_for("fiscalizacao_contratos.medicoes_lista"))
        try:
            ateste = AtesteService(conectar_banco).obter_da_medicao(medicao_id)
        except Exception:
            current_app.logger.exception("Falha ao carregar ateste da medição")
            ateste = None
        return render_template(
            "fiscalizacao_contratos/medicoes/detalhe.html",
            medicao=medicao,
            itens=itens,
            ajustes=ajustes,
            documentos=documentos,
            eventos=eventos,
            versoes=versoes,
            ateste=ateste,
        )

    @blueprint.route("/medicoes/<int:medicao_id>/editar", methods=["GET", "POST"])
    @admin_required
    def medicoes_editar(medicao_id):
        try:
            medicao = obter_detalhes(medicao_id)[0]
        except MedicaoServiceError:
            flash("Medição não encontrada.", "warning")
            return redirect(url_for("fiscalizacao_contratos.medicoes_lista"))
        if not exigir_editavel_na_tela(medicao):
            return voltar(medicao_id)
        if request.method == "GET":
            return formulario(dict(medicao), "editar")
        dados, erros = normalizar_e_validar_medicao(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return formulario(dados, "editar", 400)
        try:
            servico().atualizar(medicao_id, dados, current_user.id)
        except ERROS_NEGOCIO as erro:
            flash(str(erro), "danger")
            return formulario(dados, "editar", 400)
        except MedicaoServiceError:
            current_app.logger.exception("Falha ao editar medição")
            flash("Não foi possível atualizar a medição.", "danger")
            return formulario(dados, "editar", 500)
        flash("Medição atualizada.", "success")
        return voltar(medicao_id)

    def formulario_item(medicao_id, item, modo, status_http=200):
        try:
            medicao = obter_detalhes(medicao_id)[0]
            if not exigir_editavel_na_tela(medicao):
                return voltar(medicao_id)
            itens_planilha = servico().opcoes_relacionamentos(medicao["contrato_id"])[0]
        except MedicaoServiceError:
            flash("Não foi possível carregar o formulário do item.", "danger")
            return voltar(medicao_id)
        return (
            render_template(
                "fiscalizacao_contratos/medicoes/item_form.html",
                medicao=medicao,
                item=item,
                modo=modo,
                itens_planilha=itens_planilha,
            ),
            status_http,
        )

    def salvar_item(medicao_id, item_id=None):
        atual = None
        if item_id:
            try:
                atual = next(
                    item for item in obter_detalhes(medicao_id)[1] if item["id"] == item_id
                )
            except (MedicaoServiceError, StopIteration):
                flash("Item não encontrado.", "warning")
                return voltar(medicao_id)
        if request.method == "GET":
            return formulario_item(
                medicao_id,
                dict(atual) if atual else {"ordem": 1},
                "editar" if item_id else "novo",
            )
        dados, erros = normalizar_e_validar_item(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return formulario_item(medicao_id, dados, "editar" if item_id else "novo", 400)
        try:
            if item_id:
                servico().atualizar_item(medicao_id, item_id, dados, current_user.id)
            else:
                servico().criar_item(medicao_id, dados, current_user.id)
        except ERROS_NEGOCIO as erro:
            flash(str(erro), "danger")
            return formulario_item(medicao_id, dados, "editar" if item_id else "novo", 400)
        except MedicaoServiceError:
            current_app.logger.exception("Falha ao salvar item de medição")
            flash("Não foi possível salvar o item.", "danger")
            return formulario_item(medicao_id, dados, "editar" if item_id else "novo", 500)
        flash("Item salvo e totais recalculados.", "success")
        return voltar(medicao_id)

    @blueprint.route("/medicoes/<int:medicao_id>/itens/novo", methods=["GET", "POST"])
    @admin_required
    def medicoes_item_novo(medicao_id):
        return salvar_item(medicao_id)

    @blueprint.route("/medicoes/<int:medicao_id>/itens/<int:item_id>/editar", methods=["GET", "POST"])
    @admin_required
    def medicoes_item_editar(medicao_id, item_id):
        return salvar_item(medicao_id, item_id)

    @blueprint.route("/medicoes/<int:medicao_id>/itens/<int:item_id>/inativar", methods=["POST"])
    @admin_required
    def medicoes_item_inativar(medicao_id, item_id):
        return executar_simples(
            medicao_id,
            lambda: servico().inativar_item(medicao_id, item_id, current_user.id),
            "Item inativado; o registro e o histórico foram preservados.",
        )

    def formulario_ajuste(medicao_id, ajuste, modo, status_http=200):
        try:
            medicao = obter_detalhes(medicao_id)[0]
            if not exigir_editavel_na_tela(medicao):
                return voltar(medicao_id)
            _, fiscalizacoes, ocorrencias, _ = servico().opcoes_relacionamentos(medicao["contrato_id"])
        except MedicaoServiceError:
            flash("Não foi possível carregar o formulário do ajuste.", "danger")
            return voltar(medicao_id)
        return (
            render_template(
                "fiscalizacao_contratos/medicoes/ajuste_form.html",
                medicao=medicao,
                ajuste=ajuste,
                modo=modo,
                tipos_ajuste=TIPOS_AJUSTE,
                fiscalizacoes=fiscalizacoes,
                ocorrencias=ocorrencias,
            ),
            status_http,
        )

    def salvar_ajuste(medicao_id, ajuste_id=None):
        atual = None
        if ajuste_id:
            try:
                atual = next(
                    item for item in obter_detalhes(medicao_id)[2] if item["id"] == ajuste_id
                )
            except (MedicaoServiceError, StopIteration):
                flash("Ajuste não encontrado.", "warning")
                return voltar(medicao_id)
        if request.method == "GET":
            return formulario_ajuste(medicao_id, dict(atual) if atual else {}, "editar" if ajuste_id else "novo")
        dados, erros = normalizar_e_validar_ajuste(request.form)
        if erros:
            for erro in erros:
                flash(erro, "danger")
            return formulario_ajuste(medicao_id, dados, "editar" if ajuste_id else "novo", 400)
        try:
            if ajuste_id:
                servico().atualizar_ajuste(medicao_id, ajuste_id, dados, current_user.id)
            else:
                servico().criar_ajuste(medicao_id, dados, current_user.id)
        except ERROS_NEGOCIO as erro:
            flash(str(erro), "danger")
            return formulario_ajuste(medicao_id, dados, "editar" if ajuste_id else "novo", 400)
        except MedicaoServiceError:
            current_app.logger.exception("Falha ao salvar ajuste de medição")
            flash("Não foi possível salvar o ajuste.", "danger")
            return formulario_ajuste(medicao_id, dados, "editar" if ajuste_id else "novo", 500)
        flash("Ajuste salvo e totais recalculados.", "success")
        return voltar(medicao_id)

    @blueprint.route("/medicoes/<int:medicao_id>/ajustes/novo", methods=["GET", "POST"])
    @admin_required
    def medicoes_ajuste_novo(medicao_id):
        return salvar_ajuste(medicao_id)

    @blueprint.route("/medicoes/<int:medicao_id>/ajustes/<int:ajuste_id>/editar", methods=["GET", "POST"])
    @admin_required
    def medicoes_ajuste_editar(medicao_id, ajuste_id):
        return salvar_ajuste(medicao_id, ajuste_id)

    @blueprint.route("/medicoes/<int:medicao_id>/ajustes/<int:ajuste_id>/inativar", methods=["POST"])
    @admin_required
    def medicoes_ajuste_inativar(medicao_id, ajuste_id):
        return executar_simples(
            medicao_id,
            lambda: servico().inativar_ajuste(medicao_id, ajuste_id, current_user.id),
            "Ajuste inativado; os totais foram recalculados.",
        )

    @blueprint.route("/medicoes/<int:medicao_id>/documentos/vincular", methods=["GET", "POST"])
    @admin_required
    def medicoes_documento_vincular(medicao_id):
        try:
            medicao = obter_detalhes(medicao_id)[0]
            if not exigir_editavel_na_tela(medicao):
                return voltar(medicao_id)
            documentos = servico().opcoes_relacionamentos(medicao["contrato_id"])[3]
        except MedicaoServiceError:
            flash("Não foi possível carregar os documentos do contrato.", "danger")
            return voltar(medicao_id)
        dados = {
            "documento_id": inteiro(request.form.get("documento_id")),
            "categoria": (request.form.get("categoria") or "").strip(),
            "observacoes": (request.form.get("observacoes") or "").strip() or None,
        }
        if request.method == "POST":
            erros = []
            if not dados["documento_id"]:
                erros.append("Selecione o documento.")
            if dados["categoria"] not in CATEGORIAS_DOCUMENTO_MEDICAO:
                erros.append("Selecione uma categoria válida.")
            if not erros:
                try:
                    servico().vincular_documento(
                        medicao_id,
                        dados["documento_id"],
                        dados["categoria"],
                        dados["observacoes"],
                        current_user.id,
                    )
                except ERROS_NEGOCIO as erro:
                    erros.append(str(erro))
                except MedicaoServiceError:
                    current_app.logger.exception("Falha ao vincular documento à medição")
                    erros.append("Não foi possível vincular o documento.")
                else:
                    flash("Documento vinculado sem duplicar o arquivo.", "success")
                    return voltar(medicao_id)
            for erro in erros:
                flash(erro, "danger")
        return (
            render_template(
                "fiscalizacao_contratos/medicoes/documento_form.html",
                medicao=medicao,
                documentos=documentos,
                categorias=CATEGORIAS_DOCUMENTO_MEDICAO,
                vinculo=dados,
            ),
            400 if request.method == "POST" else 200,
        )

    @blueprint.route("/medicoes/<int:medicao_id>/documentos/enviar", methods=["GET", "POST"])
    @admin_required
    def medicoes_documento_enviar(medicao_id):
        try:
            medicao = obter_detalhes(medicao_id)[0]
            if not exigir_editavel_na_tela(medicao):
                return voltar(medicao_id)
        except MedicaoServiceError:
            flash("Não foi possível carregar a medição.", "danger")
            return voltar(medicao_id)

        dados = {
            "titulo": (request.form.get("titulo") or "").strip(),
            "categoria": (request.form.get("categoria") or "").strip(),
            "descricao": (request.form.get("descricao") or "").strip() or None,
            "observacoes": (request.form.get("observacoes") or "").strip() or None,
        }
        if request.method == "POST":
            erros = []
            arquivo = None
            if not dados["titulo"]:
                erros.append("O título do documento é obrigatório.")
            if dados["categoria"] not in CATEGORIAS_DOCUMENTO_MEDICAO:
                erros.append("Selecione uma categoria válida.")
            try:
                arquivo = validar_arquivo_documento(request.files.get("arquivo"))
            except ValidacaoDocumentoError as erro:
                erros.append(str(erro))
            if not erros:
                metadados = {
                    "titulo": dados["titulo"],
                    "descricao": dados["descricao"],
                    "categoria": TIPO_DOCUMENTO_POR_CATEGORIA_MEDICAO[dados["categoria"]],
                }
                try:
                    armazenamento = CloudinaryStorage()
                    servico().enviar_documento(
                        medicao_id, metadados, arquivo, dados["categoria"],
                        dados["observacoes"], current_user.id, armazenamento,
                    )
                except ERROS_NEGOCIO as erro:
                    erros.append(str(erro))
                except CloudinaryStorageError:
                    current_app.logger.exception("Falha no armazenamento do documento da medição")
                    erros.append("Não foi possível enviar o documento agora. Tente novamente.")
                except MedicaoServiceError:
                    current_app.logger.exception("Falha ao registrar documento da medição")
                    erros.append("Não foi possível registrar o documento.")
                else:
                    flash("Documento enviado e vinculado à medição.", "success")
                    return voltar(medicao_id)
            for erro in erros:
                flash(erro, "danger")
        return (
            render_template(
                "fiscalizacao_contratos/medicoes/documento_upload_form.html",
                medicao=medicao,
                categorias=CATEGORIAS_DOCUMENTO_MEDICAO,
                documento=dados,
                limite_mb=limite_upload_bytes() // (1024 * 1024),
            ),
            400 if request.method == "POST" else 200,
        )

    @blueprint.route("/medicoes/<int:medicao_id>/documentos/<int:vinculo_id>/inativar", methods=["POST"])
    @admin_required
    def medicoes_documento_inativar(medicao_id, vinculo_id):
        return executar_simples(
            medicao_id,
            lambda: servico().inativar_documento(medicao_id, vinculo_id, current_user.id),
            "Vínculo inativado. O documento original foi preservado.",
        )

    def executar_simples(medicao_id, operacao, sucesso):
        try:
            operacao()
        except ERROS_NEGOCIO as erro:
            flash(str(erro), "warning")
        except MedicaoServiceError:
            current_app.logger.exception("Falha em operação da medição")
            flash("Não foi possível concluir a operação.", "danger")
        else:
            flash(sucesso, "success")
        return voltar(medicao_id)

    @blueprint.route("/medicoes/<int:medicao_id>/enviar", methods=["POST"])
    @admin_required
    def medicoes_enviar(medicao_id):
        return executar_simples(
            medicao_id,
            lambda: servico().enviar_analise(medicao_id, current_user.id),
            "Medição enviada para análise.",
        )

    def formulario_acao(medicao_id, acao, titulo, aviso, exigir_aprovador=False):
        try:
            medicao = obter_detalhes(medicao_id)[0]
            servidores = carregar_opcoes()[1] if exigir_aprovador else []
        except MedicaoServiceError:
            flash("Medição não encontrada.", "warning")
            return redirect(url_for("fiscalizacao_contratos.medicoes_lista"))
        permitidos = {
            "devolver": ("Em análise",),
            "aprovar": ("Em análise",),
            "cancelar": ("Em elaboração", "Em análise", "Devolvida para correção"),
            "revisao": ("Aprovada",),
        }
        if not medicao["ativo"] or not medicao["atual"] or medicao["status"] not in permitidos[acao]:
            flash("A situação atual não permite esta ação.", "warning")
            return voltar(medicao_id)
        justificativa = (request.form.get("justificativa") or "").strip()
        aprovador_id = inteiro(request.form.get("servidor_aprovador_id"))
        if request.method == "POST":
            try:
                if acao == "devolver":
                    servico().devolver_correcao(medicao_id, justificativa, current_user.id)
                elif acao == "aprovar":
                    servico().aprovar(medicao_id, aprovador_id, current_user.id)
                elif acao == "cancelar":
                    servico().cancelar(medicao_id, justificativa, current_user.id)
                else:
                    novo_id = servico().criar_revisao(medicao_id, justificativa, current_user.id)
                    flash("Nova versão criada; a aprovada foi preservada.", "success")
                    return voltar(novo_id)
            except ERROS_NEGOCIO as erro:
                flash(str(erro), "danger")
            except MedicaoServiceError:
                current_app.logger.exception("Falha ao alterar fluxo da medição")
                flash("Não foi possível concluir a operação.", "danger")
            else:
                flash("Situação da medição atualizada.", "success")
                return voltar(medicao_id)
        return (
            render_template(
                "fiscalizacao_contratos/medicoes/acao_form.html",
                medicao=medicao,
                acao=acao,
                titulo=titulo,
                aviso=aviso,
                justificativa=justificativa,
                exigir_aprovador=exigir_aprovador,
                servidores=servidores,
                servidor_aprovador_id=aprovador_id,
            ),
            400 if request.method == "POST" else 200,
        )

    @blueprint.route("/medicoes/<int:medicao_id>/devolver", methods=["GET", "POST"])
    @admin_required
    def medicoes_devolver(medicao_id):
        return formulario_acao(medicao_id, "devolver", "Devolver para correção", "Informe claramente o que precisa ser corrigido.")

    @blueprint.route("/medicoes/<int:medicao_id>/aprovar", methods=["GET", "POST"])
    @admin_required
    def medicoes_aprovar(medicao_id):
        return formulario_acao(medicao_id, "aprovar", "Aprovar medição", "A aprovação torna esta versão imutável.", True)

    @blueprint.route("/medicoes/<int:medicao_id>/cancelar", methods=["GET", "POST"])
    @admin_required
    def medicoes_cancelar(medicao_id):
        return formulario_acao(medicao_id, "cancelar", "Cancelar medição", "O registro continuará preservado no histórico.")

    @blueprint.route("/medicoes/<int:medicao_id>/revisao", methods=["GET", "POST"])
    @admin_required
    def medicoes_revisao(medicao_id):
        return formulario_acao(medicao_id, "revisao", "Criar revisão", "Será criada uma nova versão; a aprovada permanecerá intacta.")

    @blueprint.route("/medicoes/<int:medicao_id>/eventos", methods=["GET"])
    @admin_required
    def medicoes_eventos(medicao_id):
        return redirect(url_for("fiscalizacao_contratos.medicoes_detalhe", medicao_id=medicao_id) + "#historico")

    @blueprint.route("/medicoes/<int:medicao_id>/versoes", methods=["GET"])
    @admin_required
    def medicoes_versoes(medicao_id):
        return redirect(url_for("fiscalizacao_contratos.medicoes_detalhe", medicao_id=medicao_id) + "#versoes")
