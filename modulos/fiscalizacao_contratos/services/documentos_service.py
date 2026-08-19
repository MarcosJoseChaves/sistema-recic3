"""Operações de banco e consistência dos documentos do módulo."""

import logging

import psycopg2
from psycopg2.extras import RealDictCursor

from .cloudinary_storage import CloudinaryStorageError


LOGGER = logging.getLogger(__name__)


class DocumentoServiceError(Exception):
    """Falha interna tratada pelo módulo."""


class DocumentoNaoEncontradoError(DocumentoServiceError):
    """Documento não encontrado."""


class DocumentoReferenciaInvalidaError(DocumentoServiceError):
    """Contrato ou aditivo incompatível."""


class DocumentoService:
    def __init__(self, conectar_banco):
        self._conectar_banco = conectar_banco

    def listar(
        self,
        busca="",
        categoria="",
        contrato_id=None,
        status_ativo="ativos",
    ):
        conexao = None
        busca = (busca or "").strip()
        padrao = f"%{busca}%"
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT d.*, c.numero_contrato, e.razao_social AS empresa_nome,
                           a.numero_termo AS aditivo_numero
                    FROM fc_documentos d
                    JOIN fc_contratos c ON c.id = d.contrato_id
                    JOIN fc_empresas e ON e.id = c.empresa_id
                    LEFT JOIN fc_aditivos a ON a.id = d.aditivo_id
                    WHERE (
                        %s = '' OR d.titulo ILIKE %s OR d.nome_original ILIKE %s
                        OR c.numero_contrato ILIKE %s OR e.razao_social ILIKE %s
                        OR d.categoria ILIKE %s
                    )
                      AND (%s = '' OR d.categoria = %s)
                      AND (%s::BIGINT IS NULL OR d.contrato_id = %s)
                      AND (
                          %s = 'todos'
                          OR (%s = 'ativos' AND d.ativo = TRUE)
                          OR (%s = 'inativos' AND d.ativo = FALSE)
                      )
                    ORDER BY d.ativo DESC, d.criado_em DESC, d.id DESC
                    """,
                    (
                        busca,
                        padrao,
                        padrao,
                        padrao,
                        padrao,
                        padrao,
                        categoria,
                        categoria,
                        contrato_id,
                        contrato_id,
                        status_ativo,
                        status_ativo,
                        status_ativo,
                    ),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise DocumentoServiceError(
                "Não foi possível carregar os documentos."
            ) from erro
        finally:
            if conexao:
                conexao.close()

    def listar_do_contrato(self, contrato_id):
        return self._listar_relacionados(contrato_id=contrato_id)

    def listar_do_aditivo(self, aditivo_id):
        return self._listar_relacionados(aditivo_id=aditivo_id)

    def _listar_relacionados(self, contrato_id=None, aditivo_id=None):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, contrato_id, aditivo_id, categoria, titulo,
                           nome_original, mime_type, extensao, tamanho_bytes,
                           ativo, criado_em
                    FROM fc_documentos
                    WHERE (%s::BIGINT IS NULL OR contrato_id = %s)
                      AND (%s::BIGINT IS NULL OR aditivo_id = %s)
                    ORDER BY ativo DESC, criado_em DESC, id DESC
                    """,
                    (contrato_id, contrato_id, aditivo_id, aditivo_id),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise DocumentoServiceError(
                "Não foi possível carregar os documentos relacionados."
            ) from erro
        finally:
            if conexao:
                conexao.close()

    def listar_opcoes(self):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT c.id, c.numero_contrato, c.ativo,
                           e.razao_social AS empresa_nome
                    FROM fc_contratos c
                    JOIN fc_empresas e ON e.id = c.empresa_id
                    ORDER BY c.ativo DESC, c.numero_contrato
                    """
                )
                contratos = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT a.id, a.contrato_id, a.numero_termo, a.ativo,
                           c.numero_contrato
                    FROM fc_aditivos a
                    JOIN fc_contratos c ON c.id = a.contrato_id
                    ORDER BY a.ativo DESC, c.numero_contrato, a.numero_termo
                    """
                )
                return contratos, cursor.fetchall()
        except psycopg2.Error as erro:
            raise DocumentoServiceError(
                "Não foi possível carregar contratos e aditivos."
            ) from erro
        finally:
            if conexao:
                conexao.close()

    def obter(self, documento_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT d.*, c.numero_contrato, e.razao_social AS empresa_nome,
                           a.numero_termo AS aditivo_numero
                    FROM fc_documentos d
                    JOIN fc_contratos c ON c.id = d.contrato_id
                    JOIN fc_empresas e ON e.id = c.empresa_id
                    LEFT JOIN fc_aditivos a ON a.id = d.aditivo_id
                    WHERE d.id = %s
                    """,
                    (documento_id,),
                )
                documento = cursor.fetchone()
                if not documento:
                    raise DocumentoNaoEncontradoError("Documento não encontrado.")
                return documento
        except DocumentoNaoEncontradoError:
            raise
        except psycopg2.Error as erro:
            raise DocumentoServiceError(
                "Não foi possível carregar o documento."
            ) from erro
        finally:
            if conexao:
                conexao.close()

    def criar(self, dados, arquivo, usuario_id, armazenamento):
        conexao = None
        enviado = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                self._validar_referencias(
                    cursor, dados["contrato_id"], dados.get("aditivo_id")
                )
                enviado = armazenamento.enviar(
                    arquivo, dados["contrato_id"], dados.get("aditivo_id")
                )
                cursor.execute(
                    """
                    INSERT INTO fc_documentos (
                        contrato_id, aditivo_id, categoria, titulo, descricao,
                        nome_original, armazenamento_provedor,
                        armazenamento_chave, armazenamento_versao, mime_type,
                        extensao, tamanho_bytes, sha256,
                        criado_por_usuario_id, atualizado_por_usuario_id
                    ) VALUES (
                        %(contrato_id)s, %(aditivo_id)s, %(categoria)s,
                        %(titulo)s, %(descricao)s, %(nome_original)s,
                        %(armazenamento_provedor)s, %(armazenamento_chave)s,
                        %(armazenamento_versao)s, %(mime_type)s, %(extensao)s,
                        %(tamanho_bytes)s, %(sha256)s, %(usuario_id)s,
                        %(usuario_id)s
                    ) RETURNING id
                    """,
                    {
                        **dados,
                        **{chave: valor for chave, valor in arquivo.items() if chave != "conteudo"},
                        **enviado,
                        "usuario_id": usuario_id,
                    },
                )
                documento_id = cursor.fetchone()["id"]
            conexao.commit()
            return documento_id
        except (DocumentoReferenciaInvalidaError, CloudinaryStorageError):
            if conexao:
                conexao.rollback()
            raise
        except Exception as erro:
            if conexao:
                conexao.rollback()
            if enviado:
                try:
                    armazenamento.remover(enviado["armazenamento_chave"])
                except CloudinaryStorageError as erro_limpeza:
                    LOGGER.warning(
                        "Falha na limpeza compensatória do documento: tipo_erro=%s",
                        type(erro_limpeza).__name__,
                    )
            raise DocumentoServiceError(
                "Não foi possível registrar o documento."
            ) from erro
        finally:
            if conexao:
                conexao.close()

    def inativar(self, documento_id, usuario_id):
        self._alterar_ativo(documento_id, usuario_id, False)

    def reativar(self, documento_id, usuario_id):
        self._alterar_ativo(documento_id, usuario_id, True)

    def _alterar_ativo(self, documento_id, usuario_id, ativo):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fc_documentos
                    SET ativo = %s, atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s
                    WHERE id = %s
                    """,
                    (ativo, usuario_id, documento_id),
                )
                if cursor.rowcount == 0:
                    raise DocumentoNaoEncontradoError("Documento não encontrado.")
            conexao.commit()
        except DocumentoNaoEncontradoError:
            if conexao:
                conexao.rollback()
            raise
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise DocumentoServiceError(
                "Não foi possível atualizar o documento."
            ) from erro
        finally:
            if conexao:
                conexao.close()

    @staticmethod
    def _validar_referencias(cursor, contrato_id, aditivo_id):
        cursor.execute("SELECT id FROM fc_contratos WHERE id = %s", (contrato_id,))
        if not cursor.fetchone():
            raise DocumentoReferenciaInvalidaError(
                "O contrato selecionado não existe."
            )
        if aditivo_id:
            cursor.execute(
                "SELECT id FROM fc_aditivos WHERE id = %s AND contrato_id = %s",
                (aditivo_id, contrato_id),
            )
            if not cursor.fetchone():
                raise DocumentoReferenciaInvalidaError(
                    "O aditivo selecionado não pertence ao contrato informado."
                )
