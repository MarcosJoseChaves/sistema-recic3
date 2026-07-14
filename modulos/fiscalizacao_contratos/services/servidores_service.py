"""Operações de banco para servidores e responsáveis por contratos."""

import psycopg2
from psycopg2.extras import RealDictCursor


class ServidorServiceError(Exception):
    """Falha interna tratada pelo módulo."""


class MatriculaDuplicadaError(ServidorServiceError):
    """Matrícula já cadastrada."""


class ServidorNaoEncontradoError(ServidorServiceError):
    """Servidor não encontrado."""


class ServidorService:
    def __init__(self, conectar_banco):
        self._conectar_banco = conectar_banco

    def listar(self, busca="", incluir_inativos=False):
        conexao = None
        busca = (busca or "").strip()
        padrao = f"%{busca}%"
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, nome, matricula, cargo, setor, email, telefone,
                           ativo, criado_em, atualizado_em
                    FROM fc_servidores
                    WHERE (%s OR ativo = TRUE)
                      AND (%s = '' OR nome ILIKE %s OR matricula ILIKE %s OR cargo ILIKE %s)
                    ORDER BY ativo DESC, nome
                    """,
                    (incluir_inativos, busca, padrao, padrao, padrao),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise ServidorServiceError("Não foi possível carregar os servidores.") from erro
        finally:
            if conexao:
                conexao.close()

    def obter(self, servidor_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, nome, matricula, cargo, setor, email, telefone,
                           observacoes, ativo, criado_em, atualizado_em,
                           criado_por_usuario_id, atualizado_por_usuario_id
                    FROM fc_servidores
                    WHERE id = %s
                    """,
                    (servidor_id,),
                )
                servidor = cursor.fetchone()
                if not servidor:
                    raise ServidorNaoEncontradoError("Servidor não encontrado.")
                return servidor
        except ServidorNaoEncontradoError:
            raise
        except psycopg2.Error as erro:
            raise ServidorServiceError("Não foi possível carregar o servidor.") from erro
        finally:
            if conexao:
                conexao.close()

    def criar(self, dados, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fc_servidores (
                        nome, matricula, cargo, setor, email, telefone, observacoes,
                        criado_por_usuario_id, atualizado_por_usuario_id
                    ) VALUES (
                        %(nome)s, %(matricula)s, %(cargo)s, %(setor)s, %(email)s,
                        %(telefone)s, %(observacoes)s, %(usuario_id)s, %(usuario_id)s
                    )
                    RETURNING id
                    """,
                    {**dados, "usuario_id": usuario_id},
                )
                servidor_id = cursor.fetchone()[0]
            conexao.commit()
            return servidor_id
        except psycopg2.IntegrityError as erro:
            if conexao:
                conexao.rollback()
            if erro.pgcode == "23505":
                raise MatriculaDuplicadaError(
                    "Já existe um servidor cadastrado com esta matrícula."
                ) from erro
            raise ServidorServiceError("Não foi possível cadastrar o servidor.") from erro
        except psycopg2.Error as erro:
            if conexao:
                conexao.rollback()
            raise ServidorServiceError("Não foi possível cadastrar o servidor.") from erro
        finally:
            if conexao:
                conexao.close()

    def atualizar(self, servidor_id, dados, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fc_servidores
                    SET nome = %(nome)s,
                        matricula = %(matricula)s,
                        cargo = %(cargo)s,
                        setor = %(setor)s,
                        email = %(email)s,
                        telefone = %(telefone)s,
                        observacoes = %(observacoes)s,
                        atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %(usuario_id)s
                    WHERE id = %(servidor_id)s
                    """,
                    {**dados, "usuario_id": usuario_id, "servidor_id": servidor_id},
                )
                if cursor.rowcount == 0:
                    raise ServidorNaoEncontradoError("Servidor não encontrado.")
            conexao.commit()
        except ServidorNaoEncontradoError:
            if conexao:
                conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao:
                conexao.rollback()
            if erro.pgcode == "23505":
                raise MatriculaDuplicadaError(
                    "Já existe um servidor cadastrado com esta matrícula."
                ) from erro
            raise ServidorServiceError("Não foi possível atualizar o servidor.") from erro
        except psycopg2.Error as erro:
            if conexao:
                conexao.rollback()
            raise ServidorServiceError("Não foi possível atualizar o servidor.") from erro
        finally:
            if conexao:
                conexao.close()

    def inativar(self, servidor_id, usuario_id):
        self._alterar_situacao(servidor_id, usuario_id, ativo=False)

    def reativar(self, servidor_id, usuario_id):
        self._alterar_situacao(servidor_id, usuario_id, ativo=True)

    def _alterar_situacao(self, servidor_id, usuario_id, *, ativo):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fc_servidores
                    SET ativo = %s,
                        atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s
                    WHERE id = %s
                    """,
                    (ativo, usuario_id, servidor_id),
                )
                if cursor.rowcount == 0:
                    raise ServidorNaoEncontradoError("Servidor não encontrado.")
            conexao.commit()
        except ServidorNaoEncontradoError:
            if conexao:
                conexao.rollback()
            raise
        except psycopg2.Error as erro:
            if conexao:
                conexao.rollback()
            acao = "reativar" if ativo else "inativar"
            raise ServidorServiceError(f"Não foi possível {acao} o servidor.") from erro
        finally:
            if conexao:
                conexao.close()
