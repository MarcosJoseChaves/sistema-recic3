"""Operações de banco para empresas contratadas."""

import psycopg2
from psycopg2.extras import RealDictCursor


class EmpresaServiceError(Exception):
    """Erro interno que pode ser mostrado ao usuário de forma segura."""


class EmpresaDuplicadaError(EmpresaServiceError):
    """CNPJ já cadastrado."""


class EmpresaNaoEncontradaError(EmpresaServiceError):
    """Empresa não encontrada."""


class EmpresaService:
    def __init__(self, conectar_banco):
        self._conectar_banco = conectar_banco

    def listar(self, incluir_inativas=False):
        conn = None
        try:
            conn = self._conectar_banco()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, cnpj, razao_social, nome_fantasia, cidade, uf, telefone,
                           email, ativo, criado_em, atualizado_em
                    FROM fc_empresas
                    WHERE (%s OR ativo = TRUE)
                    ORDER BY ativo DESC, razao_social
                    """,
                    (incluir_inativas,),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise EmpresaServiceError("Não foi possível carregar as empresas.") from erro
        finally:
            if conn:
                conn.close()

    def obter(self, empresa_id):
        conn = None
        try:
            conn = self._conectar_banco()
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, cnpj, razao_social, nome_fantasia, cep, logradouro, numero,
                           bairro, cidade, uf, telefone, email, ativo, criado_em, atualizado_em,
                           criado_por_usuario_id, atualizado_por_usuario_id
                    FROM fc_empresas
                    WHERE id = %s
                    """,
                    (empresa_id,),
                )
                empresa = cursor.fetchone()
                if not empresa:
                    raise EmpresaNaoEncontradaError("Empresa não encontrada.")
                return empresa
        except EmpresaNaoEncontradaError:
            raise
        except psycopg2.Error as erro:
            raise EmpresaServiceError("Não foi possível carregar a empresa.") from erro
        finally:
            if conn:
                conn.close()

    def criar(self, dados, usuario_id):
        conn = None
        try:
            conn = self._conectar_banco()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO fc_empresas (
                        cnpj, razao_social, nome_fantasia, cep, logradouro, numero, bairro,
                        cidade, uf, telefone, email, criado_por_usuario_id,
                        atualizado_por_usuario_id
                    ) VALUES (
                        %(cnpj)s, %(razao_social)s, %(nome_fantasia)s, %(cep)s,
                        %(logradouro)s, %(numero)s, %(bairro)s, %(cidade)s, %(uf)s,
                        %(telefone)s, %(email)s, %(usuario_id)s, %(usuario_id)s
                    )
                    RETURNING id
                    """,
                    {**dados, "usuario_id": usuario_id},
                )
                empresa_id = cursor.fetchone()[0]
            conn.commit()
            return empresa_id
        except psycopg2.IntegrityError as erro:
            if conn:
                conn.rollback()
            if erro.pgcode == "23505":
                raise EmpresaDuplicadaError("Já existe uma empresa cadastrada com este CNPJ.") from erro
            raise EmpresaServiceError("Não foi possível cadastrar a empresa.") from erro
        except psycopg2.Error as erro:
            if conn:
                conn.rollback()
            raise EmpresaServiceError("Não foi possível cadastrar a empresa.") from erro
        finally:
            if conn:
                conn.close()

    def atualizar(self, empresa_id, dados, usuario_id):
        conn = None
        try:
            conn = self._conectar_banco()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fc_empresas
                    SET cnpj = %(cnpj)s,
                        razao_social = %(razao_social)s,
                        nome_fantasia = %(nome_fantasia)s,
                        cep = %(cep)s,
                        logradouro = %(logradouro)s,
                        numero = %(numero)s,
                        bairro = %(bairro)s,
                        cidade = %(cidade)s,
                        uf = %(uf)s,
                        telefone = %(telefone)s,
                        email = %(email)s,
                        atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %(usuario_id)s
                    WHERE id = %(empresa_id)s
                    """,
                    {**dados, "usuario_id": usuario_id, "empresa_id": empresa_id},
                )
                if cursor.rowcount == 0:
                    raise EmpresaNaoEncontradaError("Empresa não encontrada.")
            conn.commit()
        except EmpresaNaoEncontradaError:
            if conn:
                conn.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conn:
                conn.rollback()
            if erro.pgcode == "23505":
                raise EmpresaDuplicadaError("Já existe uma empresa cadastrada com este CNPJ.") from erro
            raise EmpresaServiceError("Não foi possível atualizar a empresa.") from erro
        except psycopg2.Error as erro:
            if conn:
                conn.rollback()
            raise EmpresaServiceError("Não foi possível atualizar a empresa.") from erro
        finally:
            if conn:
                conn.close()

    def inativar(self, empresa_id, usuario_id):
        conn = None
        try:
            conn = self._conectar_banco()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fc_empresas
                    SET ativo = FALSE,
                        atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s
                    WHERE id = %s
                    """,
                    (usuario_id, empresa_id),
                )
                if cursor.rowcount == 0:
                    raise EmpresaNaoEncontradaError("Empresa não encontrada.")
            conn.commit()
        except EmpresaNaoEncontradaError:
            if conn:
                conn.rollback()
            raise
        except psycopg2.Error as erro:
            if conn:
                conn.rollback()
            raise EmpresaServiceError("Não foi possível inativar a empresa.") from erro
        finally:
            if conn:
                conn.close()

    def reativar(self, empresa_id, usuario_id):
        conn = None
        try:
            conn = self._conectar_banco()
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fc_empresas
                    SET ativo = TRUE,
                        atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s
                    WHERE id = %s
                    """,
                    (usuario_id, empresa_id),
                )
                if cursor.rowcount == 0:
                    raise EmpresaNaoEncontradaError("Empresa não encontrada.")
            conn.commit()
        except EmpresaNaoEncontradaError:
            if conn:
                conn.rollback()
            raise
        except psycopg2.Error as erro:
            if conn:
                conn.rollback()
            raise EmpresaServiceError("Não foi possível reativar a empresa.") from erro
        finally:
            if conn:
                conn.close()
