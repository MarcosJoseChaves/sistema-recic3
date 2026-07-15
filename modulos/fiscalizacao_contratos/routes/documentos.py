"""Rotas administrativas de documentos e anexos."""

from flask import current_app, flash, redirect, render_template, request, url_for
from flask_login import current_user

from ..permissions import admin_required
from ..services.cloudinary_storage import CloudinaryStorage, CloudinaryStorageError
from ..services.documentos_service import (
    DocumentoNaoEncontradoError,
    DocumentoReferenciaInvalidaError,
    DocumentoService,
    DocumentoServiceError,
)
from ..validacoes_documentos import (
    CATEGORIAS_DOCUMENTO,
    ValidacaoDocumentoError,
    limite_upload_bytes,
    normalizar_metadados_documento,
    validar_arquivo_documento,
)


def registrar_rotas_documentos(blueprint, conectar_banco):
    def servico():
        return DocumentoService(conectar_banco)

    def renderizar_formulario(documento, status=200):
        try:
            contratos, aditivos = servico().listar_opcoes()
        except DocumentoServiceError:
            current_app.logger.exception("Falha ao carregar opções de documentos")
            flash("Não foi possível carregar contratos e aditivos.", "danger")
            contratos, aditivos = [], []
        return (
            render_template(
                "fiscalizacao_contratos/documentos/form.html",
                documento=documento,
                contratos=contratos,
                aditivos=aditivos,
                categorias=CATEGORIAS_DOCUMENTO,
                limite_mb=limite_upload_bytes() // (1024 * 1024),
            ),
            status,
        )

    @blueprint.route("/documentos", methods=["GET"])
    @admin_required
    def documentos_lista():
        busca = (request.args.get("busca") or "").strip()
        categoria = (request.args.get("categoria") or "").strip()
        if categoria not in ("", *CATEGORIAS_DOCUMENTO):
            categoria = ""
        status_ativo = request.args.get("status_ativo") or "ativos"
        if status_ativo not in ("ativos", "inativos", "todos"):
            status_ativo = "ativos"
        try:
            contrato_id = int(request.args.get("contrato_id") or 0) or None
        except ValueError:
            contrato_id = None
        try:
            documentos = servico().listar(
                busca=busca,
                categoria=categoria,
                contrato_id=contrato_id,
                status_ativo=status_ativo,
            )
            contratos, _ = servico().listar_opcoes()
        except DocumentoServiceError:
            current_app.logger.exception("Falha ao listar documentos")
            flash("Não foi possível carregar os documentos.", "danger")
            documentos, contratos = [], []
        return render_template(
            "fiscalizacao_contratos/documentos/lista.html",
            documentos=documentos,
            contratos=contratos,
            categorias=CATEGORIAS_DOCUMENTO,
            busca=busca,
            categoria=categoria,
            contrato_id=contrato_id,
            status_ativo=status_ativo,
        )

    @blueprint.route("/documentos/novo", methods=["GET", "POST"])
    @admin_required
    def documentos_novo():
        if request.method == "GET":
            return renderizar_formulario(
                {
                    "contrato_id": request.args.get("contrato_id", type=int),
                    "aditivo_id": request.args.get("aditivo_id", type=int),
                }
            )
        try:
            dados = normalizar_metadados_documento(request.form)
            arquivo = validar_arquivo_documento(request.files.get("arquivo"))
        except ValidacaoDocumentoError as erro:
            flash(str(erro), "danger")
            documento = dict(request.form)
            return renderizar_formulario(documento, 400)
        try:
            armazenamento = CloudinaryStorage()
            documento_id = servico().criar(
                dados, arquivo, current_user.id, armazenamento
            )
        except DocumentoReferenciaInvalidaError as erro:
            flash(str(erro), "danger")
            return renderizar_formulario(dados, 400)
        except CloudinaryStorageError:
            current_app.logger.exception("Falha no armazenamento do documento")
            flash("Não foi possível enviar o documento. Tente novamente.", "danger")
            return renderizar_formulario(dados, 502)
        except DocumentoServiceError:
            current_app.logger.exception("Falha ao registrar documento")
            flash("Não foi possível registrar o documento. Tente novamente.", "danger")
            return renderizar_formulario(dados, 500)
        flash("Documento anexado com sucesso.", "success")
        return redirect(
            url_for(
                "fiscalizacao_contratos.documentos_detalhe",
                documento_id=documento_id,
            )
        )

    @blueprint.route("/documentos/<int:documento_id>", methods=["GET"])
    @admin_required
    def documentos_detalhe(documento_id):
        try:
            documento = servico().obter(documento_id)
        except DocumentoNaoEncontradoError:
            flash("Documento não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.documentos_lista"))
        except DocumentoServiceError:
            current_app.logger.exception("Falha ao carregar documento")
            flash("Não foi possível carregar o documento.", "danger")
            return redirect(url_for("fiscalizacao_contratos.documentos_lista"))
        return render_template(
            "fiscalizacao_contratos/documentos/detalhe.html", documento=documento
        )

    @blueprint.route("/documentos/<int:documento_id>/arquivo", methods=["GET"])
    @admin_required
    def documentos_arquivo(documento_id):
        try:
            documento = servico().obter(documento_id)
            armazenamento = CloudinaryStorage()
            url = armazenamento.gerar_url_temporaria(
                documento["armazenamento_chave"],
                documento["extensao"],
                download=request.args.get("download") == "1",
            )
        except DocumentoNaoEncontradoError:
            flash("Documento não encontrado.", "warning")
            return redirect(url_for("fiscalizacao_contratos.documentos_lista"))
        except (DocumentoServiceError, CloudinaryStorageError):
            current_app.logger.exception("Falha ao gerar acesso temporário")
            flash("Não foi possível abrir o documento agora.", "danger")
            return redirect(url_for("fiscalizacao_contratos.documentos_lista"))
        return redirect(url)

    @blueprint.route("/documentos/<int:documento_id>/inativar", methods=["POST"])
    @admin_required
    def documentos_inativar(documento_id):
        try:
            servico().inativar(documento_id, current_user.id)
        except DocumentoNaoEncontradoError:
            flash("Documento não encontrado.", "warning")
        except DocumentoServiceError:
            current_app.logger.exception("Falha ao inativar documento")
            flash("Não foi possível inativar o documento.", "danger")
        else:
            flash("Documento inativado. O arquivo e o histórico foram preservados.", "success")
        return redirect(
            url_for("fiscalizacao_contratos.documentos_lista", status_ativo="todos")
        )

    @blueprint.route("/documentos/<int:documento_id>/reativar", methods=["POST"])
    @admin_required
    def documentos_reativar(documento_id):
        try:
            servico().reativar(documento_id, current_user.id)
        except DocumentoNaoEncontradoError:
            flash("Documento não encontrado.", "warning")
        except DocumentoServiceError:
            current_app.logger.exception("Falha ao reativar documento")
            flash("Não foi possível reativar o documento.", "danger")
        else:
            flash("Documento reativado com sucesso.", "success")
        return redirect(
            url_for(
                "fiscalizacao_contratos.documentos_detalhe",
                documento_id=documento_id,
            )
        )
