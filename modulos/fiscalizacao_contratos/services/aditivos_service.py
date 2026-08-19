"""Operações de banco e cálculos do histórico de aditivos."""

from datetime import timedelta
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor


class AditivoServiceError(Exception):
    """Falha interna tratada pelo módulo."""


class AditivoDuplicadoError(AditivoServiceError):
    """Número de termo já usado no mesmo contrato."""


class AditivoNaoEncontradoError(AditivoServiceError):
    """Aditivo não encontrado."""


class ContratoAditivoInvalidoError(AditivoServiceError):
    """Contrato inexistente ou vigência incompatível."""


class AditivoService:
    def __init__(self, conectar_banco):
        self._conectar_banco = conectar_banco

    def listar(self, busca="", tipo_aditivo="", status_ativo="ativos", contrato_id=None):
        conexao = None
        busca = (busca or "").strip()
        padrao = f"%{busca}%"
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT a.*, c.numero_contrato, c.processo_administrativo,
                           e.razao_social AS empresa_nome
                    FROM fc_aditivos a
                    JOIN fc_contratos c ON c.id = a.contrato_id
                    JOIN fc_empresas e ON e.id = c.empresa_id
                    WHERE (
                        %s = '' OR c.numero_contrato ILIKE %s
                        OR COALESCE(c.processo_administrativo, '') ILIKE %s
                        OR e.razao_social ILIKE %s OR a.numero_termo ILIKE %s
                        OR a.tipo_aditivo ILIKE %s
                    )
                      AND (%s = '' OR a.tipo_aditivo = %s)
                      AND (%s::BIGINT IS NULL OR a.contrato_id = %s)
                      AND (
                          %s = 'todos'
                          OR (%s = 'ativos' AND a.ativo = TRUE)
                          OR (%s = 'inativos' AND a.ativo = FALSE)
                      )
                    ORDER BY a.ativo DESC, a.data_assinatura DESC, a.id DESC
                    """,
                    (
                        busca,
                        padrao,
                        padrao,
                        padrao,
                        padrao,
                        padrao,
                        tipo_aditivo,
                        tipo_aditivo,
                        contrato_id,
                        contrato_id,
                        status_ativo,
                        status_ativo,
                        status_ativo,
                    ),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise AditivoServiceError("Não foi possível carregar os aditivos.") from erro
        finally:
            if conexao:
                conexao.close()

    def listar_contratos(self):
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
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise AditivoServiceError("Não foi possível carregar os contratos.") from erro
        finally:
            if conexao:
                conexao.close()

    def obter(self, aditivo_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT a.*, c.numero_contrato, c.valor_original,
                           c.vigencia_inicio, c.vigencia_fim,
                           e.razao_social AS empresa_nome
                    FROM fc_aditivos a
                    JOIN fc_contratos c ON c.id = a.contrato_id
                    JOIN fc_empresas e ON e.id = c.empresa_id
                    WHERE a.id = %s
                    """,
                    (aditivo_id,),
                )
                aditivo = cursor.fetchone()
                if not aditivo:
                    raise AditivoNaoEncontradoError("Aditivo não encontrado.")
                return aditivo
        except AditivoNaoEncontradoError:
            raise
        except psycopg2.Error as erro:
            raise AditivoServiceError("Não foi possível carregar o aditivo.") from erro
        finally:
            if conexao:
                conexao.close()

    def resumo_contrato(self, contrato_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                contrato = self._obter_contrato(cursor, contrato_id)
                aditivos = self._listar_do_contrato(cursor, contrato_id)
                return self._calcular_resumo(contrato, aditivos), aditivos
        except ContratoAditivoInvalidoError:
            raise
        except psycopg2.Error as erro:
            raise AditivoServiceError("Não foi possível calcular os aditivos.") from erro
        finally:
            if conexao:
                conexao.close()

    def criar(self, dados, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                contrato = self._obter_contrato(cursor, dados["contrato_id"])
                aditivos = self._listar_do_contrato(cursor, dados["contrato_id"])
                self._validar_nova_vigencia(dados, contrato, aditivos)
                cursor.execute(
                    """
                    INSERT INTO fc_aditivos (
                        contrato_id, numero_termo, tipo_aditivo, data_assinatura,
                        data_inicio_efeitos, dias_acrescidos, nova_vigencia_fim,
                        valor_acrescimo, valor_supressao, percentual_alteracao,
                        descricao_alteracao, justificativa, observacoes,
                        criado_por_usuario_id, atualizado_por_usuario_id
                    ) VALUES (
                        %(contrato_id)s, %(numero_termo)s, %(tipo_aditivo)s,
                        %(data_assinatura)s, %(data_inicio_efeitos)s,
                        %(dias_acrescidos)s, %(nova_vigencia_fim)s,
                        %(valor_acrescimo)s, %(valor_supressao)s,
                        %(percentual_alteracao)s, %(descricao_alteracao)s,
                        %(justificativa)s, %(observacoes)s, %(usuario_id)s,
                        %(usuario_id)s
                    ) RETURNING id
                    """,
                    {**dados, "usuario_id": usuario_id},
                )
                aditivo_id = cursor.fetchone()["id"]
            conexao.commit()
            return aditivo_id
        except ContratoAditivoInvalidoError:
            if conexao:
                conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao:
                conexao.rollback()
            self._tratar_integridade(erro, "cadastrar")
        except psycopg2.Error as erro:
            if conexao:
                conexao.rollback()
            raise AditivoServiceError("Não foi possível cadastrar o aditivo.") from erro
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise AditivoServiceError("Não foi possível cadastrar o aditivo.") from erro
        finally:
            if conexao:
                conexao.close()

    def atualizar(self, aditivo_id, dados, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                self._obter_aditivo_simples(cursor, aditivo_id)
                contrato = self._obter_contrato(cursor, dados["contrato_id"])
                aditivos = self._listar_do_contrato(
                    cursor, dados["contrato_id"], excluir_id=aditivo_id
                )
                self._validar_nova_vigencia(dados, contrato, aditivos)
                cursor.execute(
                    """
                    UPDATE fc_aditivos
                    SET contrato_id = %(contrato_id)s,
                        numero_termo = %(numero_termo)s,
                        tipo_aditivo = %(tipo_aditivo)s,
                        data_assinatura = %(data_assinatura)s,
                        data_inicio_efeitos = %(data_inicio_efeitos)s,
                        dias_acrescidos = %(dias_acrescidos)s,
                        nova_vigencia_fim = %(nova_vigencia_fim)s,
                        valor_acrescimo = %(valor_acrescimo)s,
                        valor_supressao = %(valor_supressao)s,
                        percentual_alteracao = %(percentual_alteracao)s,
                        descricao_alteracao = %(descricao_alteracao)s,
                        justificativa = %(justificativa)s,
                        observacoes = %(observacoes)s,
                        atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %(usuario_id)s
                    WHERE id = %(aditivo_id)s
                    """,
                    {**dados, "usuario_id": usuario_id, "aditivo_id": aditivo_id},
                )
            conexao.commit()
        except (AditivoNaoEncontradoError, ContratoAditivoInvalidoError):
            if conexao:
                conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao:
                conexao.rollback()
            self._tratar_integridade(erro, "atualizar")
        except psycopg2.Error as erro:
            if conexao:
                conexao.rollback()
            raise AditivoServiceError("Não foi possível atualizar o aditivo.") from erro
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise AditivoServiceError("Não foi possível atualizar o aditivo.") from erro
        finally:
            if conexao:
                conexao.close()

    def inativar(self, aditivo_id, usuario_id):
        self._alterar_ativo(aditivo_id, usuario_id, False)

    def reativar(self, aditivo_id, usuario_id):
        self._alterar_ativo(aditivo_id, usuario_id, True)

    def _alterar_ativo(self, aditivo_id, usuario_id, ativo):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fc_aditivos
                    SET ativo = %s, atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s
                    WHERE id = %s
                    """,
                    (ativo, usuario_id, aditivo_id),
                )
                if cursor.rowcount == 0:
                    raise AditivoNaoEncontradoError("Aditivo não encontrado.")
            conexao.commit()
        except AditivoNaoEncontradoError:
            if conexao:
                conexao.rollback()
            raise
        except Exception as erro:
            if conexao:
                conexao.rollback()
            acao = "reativar" if ativo else "inativar"
            raise AditivoServiceError(f"Não foi possível {acao} o aditivo.") from erro
        finally:
            if conexao:
                conexao.close()

    @staticmethod
    def _obter_contrato(cursor, contrato_id):
        cursor.execute(
            """
            SELECT c.id, c.numero_contrato, c.valor_original, c.vigencia_inicio,
                   c.vigencia_fim, c.ativo, e.razao_social AS empresa_nome
            FROM fc_contratos c
            JOIN fc_empresas e ON e.id = c.empresa_id
            WHERE c.id = %s
            """,
            (contrato_id,),
        )
        contrato = cursor.fetchone()
        if not contrato:
            raise ContratoAditivoInvalidoError("O contrato selecionado não existe.")
        return contrato

    @staticmethod
    def _obter_aditivo_simples(cursor, aditivo_id):
        cursor.execute("SELECT id FROM fc_aditivos WHERE id = %s", (aditivo_id,))
        if not cursor.fetchone():
            raise AditivoNaoEncontradoError("Aditivo não encontrado.")

    @staticmethod
    def _listar_do_contrato(cursor, contrato_id, excluir_id=None):
        cursor.execute(
            """
            SELECT id, contrato_id, numero_termo, tipo_aditivo, data_assinatura,
                   data_inicio_efeitos, dias_acrescidos, nova_vigencia_fim,
                   valor_acrescimo, valor_supressao, percentual_alteracao,
                   descricao_alteracao, justificativa, observacoes, ativo,
                   criado_em, atualizado_em
            FROM fc_aditivos
            WHERE contrato_id = %s AND (%s::BIGINT IS NULL OR id <> %s)
            ORDER BY COALESCE(data_inicio_efeitos, data_assinatura), id
            """,
            (contrato_id, excluir_id, excluir_id),
        )
        return cursor.fetchall()

    @classmethod
    def _calcular_resumo(cls, contrato, aditivos):
        ativos = [item for item in aditivos if item["ativo"]]
        total_acrescimos = sum(
            (Decimal(str(item["valor_acrescimo"] or 0)) for item in ativos),
            Decimal("0.00"),
        )
        total_supressoes = sum(
            (Decimal(str(item["valor_supressao"] or 0)) for item in ativos),
            Decimal("0.00"),
        )
        vigencia_atual = cls._calcular_vigencia_atual(contrato["vigencia_fim"], ativos)
        valor_original = Decimal(str(contrato["valor_original"] or 0))
        return {
            "valor_original": valor_original,
            "total_acrescimos": total_acrescimos,
            "total_supressoes": total_supressoes,
            "valor_atualizado": valor_original + total_acrescimos - total_supressoes,
            "vigencia_inicio_original": contrato["vigencia_inicio"],
            "vigencia_fim_original": contrato["vigencia_fim"],
            "vigencia_fim_atual": vigencia_atual,
            "quantidade_aditivos_ativos": len(ativos),
        }

    @staticmethod
    def _calcular_vigencia_atual(vigencia_original, aditivos):
        vigencia = vigencia_original
        ordenados = sorted(
            aditivos,
            key=lambda item: (
                item["data_inicio_efeitos"] or item["data_assinatura"],
                item["id"],
            ),
        )
        for item in ordenados:
            if item["nova_vigencia_fim"]:
                vigencia = item["nova_vigencia_fim"]
            elif item["dias_acrescidos"] is not None and vigencia:
                vigencia += timedelta(days=item["dias_acrescidos"])
        return vigencia

    @classmethod
    def _validar_nova_vigencia(cls, dados, contrato, aditivos):
        if not dados.get("nova_vigencia_fim"):
            return
        atuais = [item for item in aditivos if item["ativo"]]
        vigencia_atual = cls._calcular_vigencia_atual(contrato["vigencia_fim"], atuais)
        if vigencia_atual and dados["nova_vigencia_fim"] < vigencia_atual:
            raise ContratoAditivoInvalidoError(
                "A nova vigência não pode ser anterior à vigência atual do contrato."
            )

    @staticmethod
    def _tratar_integridade(erro, acao):
        diagnostico = getattr(erro, "diag", None)
        if getattr(diagnostico, "constraint_name", None) == "uq_fc_aditivos_contrato_numero":
            raise AditivoDuplicadoError(
                "Já existe um aditivo com este número de termo para o contrato."
            ) from erro
        raise AditivoServiceError(f"Não foi possível {acao} o aditivo.") from erro
