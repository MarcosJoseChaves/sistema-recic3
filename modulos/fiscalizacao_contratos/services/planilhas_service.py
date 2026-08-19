"""Persistência e regras das planilhas orçamentárias versionadas."""

from collections import OrderedDict
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor

from ..validacoes_planilhas import calcular_total_item


class PlanilhaServiceError(Exception):
    """Falha interna apresentada ao usuário de forma amigável."""


class PlanilhaNaoEncontradaError(PlanilhaServiceError):
    pass


class PlanilhaDuplicadaError(PlanilhaServiceError):
    pass


class ReferenciaPlanilhaInvalidaError(PlanilhaServiceError):
    pass


class PlanilhaBloqueadaError(PlanilhaServiceError):
    pass


class ItemNaoEncontradoError(PlanilhaServiceError):
    pass


class PlanilhaService:
    def __init__(self, conectar_banco):
        self._conectar_banco = conectar_banco

    def listar(
        self,
        busca="",
        contrato_id=None,
        tipo_planilha="",
        status="",
        vigente="",
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
                    SELECT p.*, c.numero_contrato, c.processo_administrativo,
                           c.valor_original, e.razao_social AS empresa_nome,
                           COALESCE(t.total_geral, 0) AS total_geral,
                           COALESCE(t.quantidade_itens, 0) AS quantidade_itens
                    FROM fc_planilhas_orcamentarias p
                    JOIN fc_contratos c ON c.id = p.contrato_id
                    JOIN fc_empresas e ON e.id = c.empresa_id
                    LEFT JOIN (
                        SELECT planilha_id,
                               SUM(quantidade * valor_unitario * fator_multiplicador)
                                   FILTER (WHERE ativo = TRUE) AS total_geral,
                               COUNT(*) FILTER (WHERE ativo = TRUE) AS quantidade_itens
                        FROM fc_planilha_itens GROUP BY planilha_id
                    ) t ON t.planilha_id = p.id
                    WHERE (
                        %s = '' OR p.nome ILIKE %s OR c.numero_contrato ILIKE %s
                        OR COALESCE(c.processo_administrativo, '') ILIKE %s
                        OR e.razao_social ILIKE %s
                        OR COALESCE(p.descricao_referencia, '') ILIKE %s
                    )
                      AND (%s::BIGINT IS NULL OR p.contrato_id = %s)
                      AND (%s = '' OR p.tipo_planilha = %s)
                      AND (%s = '' OR p.status = %s)
                      AND (%s = '' OR p.vigente = (%s = 'sim'))
                      AND (
                          %s = 'todos'
                          OR (%s = 'ativos' AND p.ativo = TRUE)
                          OR (%s = 'inativos' AND p.ativo = FALSE)
                      )
                    ORDER BY p.ativo DESC, c.numero_contrato, p.versao DESC
                    """,
                    (
                        busca, padrao, padrao, padrao, padrao, padrao,
                        contrato_id, contrato_id, tipo_planilha, tipo_planilha,
                        status, status, vigente, vigente,
                        status_ativo, status_ativo, status_ativo,
                    ),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise PlanilhaServiceError("Não foi possível carregar as planilhas.") from erro
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
                    SELECT c.id, c.numero_contrato, c.processo_administrativo,
                           c.valor_original, c.ativo, e.razao_social AS empresa_nome
                    FROM fc_contratos c
                    JOIN fc_empresas e ON e.id = c.empresa_id
                    ORDER BY c.ativo DESC, c.numero_contrato
                    """
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise PlanilhaServiceError("Não foi possível carregar os contratos.") from erro
        finally:
            if conexao:
                conexao.close()

    def listar_aditivos(self, contrato_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, contrato_id, numero_termo, tipo_aditivo, ativo
                    FROM fc_aditivos WHERE contrato_id = %s
                    ORDER BY ativo DESC, data_assinatura DESC, id DESC
                    """,
                    (contrato_id,),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise PlanilhaServiceError("Não foi possível carregar os aditivos.") from erro
        finally:
            if conexao:
                conexao.close()

    def obter(self, planilha_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                planilha = self._obter_planilha_cursor(cursor, planilha_id)
                cursor.execute(
                    """
                    SELECT * FROM fc_planilha_itens
                    WHERE planilha_id = %s
                    ORDER BY ativo DESC, ordem, id
                    """,
                    (planilha_id,),
                )
                itens = cursor.fetchall()
                resumo = self.calcular_resumo(itens)
                planilha["total_geral"] = resumo["total_geral"]
                planilha["quantidade_itens"] = resumo["quantidade_itens"]
                planilha["subtotais"] = resumo["subtotais"]
                return planilha, itens
        except PlanilhaNaoEncontradaError:
            raise
        except psycopg2.Error as erro:
            raise PlanilhaServiceError("Não foi possível carregar a planilha.") from erro
        finally:
            if conexao:
                conexao.close()

    def listar_do_contrato(self, contrato_id):
        return self.listar(contrato_id=contrato_id, status_ativo="todos")

    def criar(self, dados, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                self._validar_referencias(cursor, dados)
                cursor.execute(
                    """
                    INSERT INTO fc_planilhas_orcamentarias (
                        contrato_id, aditivo_id, nome, versao, tipo_planilha,
                        data_referencia, descricao_referencia,
                        criado_por_usuario_id, atualizado_por_usuario_id
                    ) VALUES (
                        %(contrato_id)s, %(aditivo_id)s, %(nome)s, %(versao)s,
                        %(tipo_planilha)s, %(data_referencia)s,
                        %(descricao_referencia)s, %(usuario_id)s, %(usuario_id)s
                    ) RETURNING id
                    """,
                    {**dados, "usuario_id": usuario_id},
                )
                planilha_id = cursor.fetchone()["id"]
            conexao.commit()
            return planilha_id
        except (ReferenciaPlanilhaInvalidaError, PlanilhaDuplicadaError):
            if conexao:
                conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao:
                conexao.rollback()
            self._tratar_integridade(erro)
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise PlanilhaServiceError("Não foi possível cadastrar a planilha.") from erro
        finally:
            if conexao:
                conexao.close()

    def atualizar(self, planilha_id, dados, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                atual = self._obter_planilha_cursor(cursor, planilha_id, bloquear=True)
                self._exigir_editavel(atual)
                self._validar_referencias(cursor, dados)
                cursor.execute(
                    """
                    UPDATE fc_planilhas_orcamentarias
                    SET contrato_id = %(contrato_id)s, aditivo_id = %(aditivo_id)s,
                        nome = %(nome)s, versao = %(versao)s,
                        tipo_planilha = %(tipo_planilha)s,
                        data_referencia = %(data_referencia)s,
                        descricao_referencia = %(descricao_referencia)s,
                        atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %(usuario_id)s
                    WHERE id = %(planilha_id)s
                    """,
                    {**dados, "usuario_id": usuario_id, "planilha_id": planilha_id},
                )
            conexao.commit()
        except (PlanilhaNaoEncontradaError, PlanilhaBloqueadaError, ReferenciaPlanilhaInvalidaError):
            if conexao:
                conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao:
                conexao.rollback()
            self._tratar_integridade(erro)
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise PlanilhaServiceError("Não foi possível atualizar a planilha.") from erro
        finally:
            if conexao:
                conexao.close()

    def criar_item(self, planilha_id, dados, usuario_id):
        return self._salvar_item(None, planilha_id, dados, usuario_id)

    def atualizar_item(self, item_id, planilha_id, dados, usuario_id):
        return self._salvar_item(item_id, planilha_id, dados, usuario_id)

    def _salvar_item(self, item_id, planilha_id, dados, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                planilha = self._obter_planilha_cursor(cursor, planilha_id, bloquear=True)
                self._exigir_editavel(planilha)
                if item_id is None:
                    cursor.execute(
                        """
                        INSERT INTO fc_planilha_itens (
                            planilha_id, ordem, grupo, codigo_item, descricao, unidade,
                            quantidade, valor_unitario, fator_multiplicador, observacoes,
                            criado_por_usuario_id, atualizado_por_usuario_id
                        ) VALUES (
                            %(planilha_id)s, %(ordem)s, %(grupo)s, %(codigo_item)s,
                            %(descricao)s, %(unidade)s, %(quantidade)s,
                            %(valor_unitario)s, %(fator_multiplicador)s,
                            %(observacoes)s, %(usuario_id)s, %(usuario_id)s
                        ) RETURNING id
                        """,
                        {**dados, "planilha_id": planilha_id, "usuario_id": usuario_id},
                    )
                    salvo_id = cursor.fetchone()["id"]
                else:
                    cursor.execute(
                        """
                        UPDATE fc_planilha_itens
                        SET ordem = %(ordem)s, grupo = %(grupo)s,
                            codigo_item = %(codigo_item)s, descricao = %(descricao)s,
                            unidade = %(unidade)s, quantidade = %(quantidade)s,
                            valor_unitario = %(valor_unitario)s,
                            fator_multiplicador = %(fator_multiplicador)s,
                            observacoes = %(observacoes)s,
                            atualizado_em = CURRENT_TIMESTAMP,
                            atualizado_por_usuario_id = %(usuario_id)s
                        WHERE id = %(item_id)s AND planilha_id = %(planilha_id)s
                        """,
                        {**dados, "item_id": item_id, "planilha_id": planilha_id, "usuario_id": usuario_id},
                    )
                    if cursor.rowcount == 0:
                        raise ItemNaoEncontradoError("Item não encontrado.")
                    salvo_id = item_id
            conexao.commit()
            return salvo_id
        except (PlanilhaNaoEncontradaError, PlanilhaBloqueadaError, ItemNaoEncontradoError):
            if conexao:
                conexao.rollback()
            raise
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise PlanilhaServiceError("Não foi possível salvar o item.") from erro
        finally:
            if conexao:
                conexao.close()

    def alterar_item_ativo(self, planilha_id, item_id, usuario_id, ativo):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                planilha = self._obter_planilha_cursor(cursor, planilha_id, bloquear=True)
                self._exigir_editavel(planilha)
                cursor.execute(
                    """
                    UPDATE fc_planilha_itens
                    SET ativo = %s, atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s
                    WHERE id = %s AND planilha_id = %s
                    """,
                    (ativo, usuario_id, item_id, planilha_id),
                )
                if cursor.rowcount == 0:
                    raise ItemNaoEncontradoError("Item não encontrado.")
            conexao.commit()
        except (PlanilhaNaoEncontradaError, PlanilhaBloqueadaError, ItemNaoEncontradoError):
            if conexao:
                conexao.rollback()
            raise
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise PlanilhaServiceError("Não foi possível alterar o item.") from erro
        finally:
            if conexao:
                conexao.close()

    def consolidar(self, planilha_id, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                planilha = self._obter_planilha_cursor(cursor, planilha_id, bloquear=True)
                self._exigir_elaboracao(planilha)
                if not planilha["ativo"]:
                    raise PlanilhaBloqueadaError("Uma planilha inativa não pode ser consolidada.")
                cursor.execute(
                    """
                    SELECT * FROM fc_planilha_itens
                    WHERE planilha_id = %s AND ativo = TRUE
                    ORDER BY ordem, id FOR UPDATE
                    """,
                    (planilha_id,),
                )
                itens = cursor.fetchall()
                if not itens:
                    raise PlanilhaBloqueadaError("Inclua pelo menos um item ativo antes de consolidar.")
                self.calcular_resumo(itens)
                cursor.execute(
                    """
                    UPDATE fc_planilhas_orcamentarias
                    SET status = 'Consolidada', atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s WHERE id = %s
                    """,
                    (usuario_id, planilha_id),
                )
            conexao.commit()
        except (PlanilhaNaoEncontradaError, PlanilhaBloqueadaError):
            if conexao:
                conexao.rollback()
            raise
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise PlanilhaServiceError("Não foi possível consolidar a planilha.") from erro
        finally:
            if conexao:
                conexao.close()

    def definir_vigente(self, planilha_id, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                planilha = self._obter_planilha_cursor(cursor, planilha_id, bloquear=True)
                if not planilha["ativo"] or planilha["status"] != "Consolidada":
                    raise PlanilhaBloqueadaError("Somente uma planilha ativa e consolidada pode ser vigente.")
                cursor.execute(
                    """
                    UPDATE fc_planilhas_orcamentarias
                    SET vigente = FALSE, atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s
                    WHERE contrato_id = %s AND vigente = TRUE AND id <> %s
                    """,
                    (usuario_id, planilha["contrato_id"], planilha_id),
                )
                cursor.execute(
                    """
                    UPDATE fc_planilhas_orcamentarias
                    SET vigente = TRUE, atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s WHERE id = %s
                    """,
                    (usuario_id, planilha_id),
                )
            conexao.commit()
        except (PlanilhaNaoEncontradaError, PlanilhaBloqueadaError):
            if conexao:
                conexao.rollback()
            raise
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise PlanilhaServiceError("Não foi possível definir a planilha vigente.") from erro
        finally:
            if conexao:
                conexao.close()

    def criar_versao(self, origem_id, dados, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                origem = self._obter_planilha_cursor(cursor, origem_id, bloquear=True)
                if not origem["ativo"] or origem["status"] != "Consolidada":
                    raise PlanilhaBloqueadaError("A origem deve ser uma planilha ativa e consolidada.")
                dados = {**dados, "contrato_id": origem["contrato_id"]}
                self._validar_referencias(cursor, dados)
                cursor.execute(
                    "SELECT id FROM fc_contratos WHERE id = %s FOR UPDATE",
                    (origem["contrato_id"],),
                )
                cursor.execute(
                    "SELECT COALESCE(MAX(versao), 0) + 1 AS proxima FROM fc_planilhas_orcamentarias WHERE contrato_id = %s",
                    (origem["contrato_id"],),
                )
                proxima = cursor.fetchone()["proxima"]
                cursor.execute(
                    """
                    INSERT INTO fc_planilhas_orcamentarias (
                        contrato_id, aditivo_id, nome, versao, tipo_planilha,
                        data_referencia, descricao_referencia, status, vigente,
                        criado_por_usuario_id, atualizado_por_usuario_id
                    ) VALUES (
                        %(contrato_id)s, %(aditivo_id)s, %(nome)s, %(versao)s,
                        %(tipo_planilha)s, %(data_referencia)s,
                        %(descricao_referencia)s, 'Em elaboração', FALSE,
                        %(usuario_id)s, %(usuario_id)s
                    ) RETURNING id
                    """,
                    {**dados, "versao": proxima, "usuario_id": usuario_id},
                )
                nova_id = cursor.fetchone()["id"]
                cursor.execute(
                    """
                    INSERT INTO fc_planilha_itens (
                        planilha_id, ordem, grupo, codigo_item, descricao, unidade,
                        quantidade, valor_unitario, fator_multiplicador, observacoes,
                        criado_por_usuario_id, atualizado_por_usuario_id
                    )
                    SELECT %s, ordem, grupo, codigo_item, descricao, unidade,
                           quantidade, valor_unitario, fator_multiplicador, observacoes,
                           %s, %s
                    FROM fc_planilha_itens
                    WHERE planilha_id = %s AND ativo = TRUE
                    ORDER BY ordem, id
                    """,
                    (nova_id, usuario_id, usuario_id, origem_id),
                )
            conexao.commit()
            return nova_id
        except (PlanilhaNaoEncontradaError, PlanilhaBloqueadaError, ReferenciaPlanilhaInvalidaError):
            if conexao:
                conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao:
                conexao.rollback()
            self._tratar_integridade(erro)
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise PlanilhaServiceError("Não foi possível criar a nova versão.") from erro
        finally:
            if conexao:
                conexao.close()

    def alterar_planilha_ativo(self, planilha_id, usuario_id, ativo):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                planilha = self._obter_planilha_cursor(cursor, planilha_id, bloquear=True)
                if not ativo and planilha["vigente"]:
                    raise PlanilhaBloqueadaError("Defina outra planilha vigente antes de inativar esta versão.")
                cursor.execute(
                    """
                    UPDATE fc_planilhas_orcamentarias
                    SET ativo = %s, atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s WHERE id = %s
                    """,
                    (ativo, usuario_id, planilha_id),
                )
            conexao.commit()
        except (PlanilhaNaoEncontradaError, PlanilhaBloqueadaError):
            if conexao:
                conexao.rollback()
            raise
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise PlanilhaServiceError("Não foi possível alterar a situação da planilha.") from erro
        finally:
            if conexao:
                conexao.close()

    def comparar_contrato(self, contrato_id, valor_atualizado=None):
        planilhas = self.listar(contrato_id=contrato_id, status_ativo="todos")
        original = next((p for p in planilhas if p["tipo_planilha"] == "Original"), None)
        vigente = next((p for p in planilhas if p["vigente"]), None)
        total_original = Decimal(str(original["total_geral"] or 0)) if original else None
        total_vigente = Decimal(str(vigente["total_geral"] or 0)) if vigente else None
        valor_contrato = Decimal(str(original["valor_original"] or 0)) if original else None
        atualizado = Decimal(str(valor_atualizado)) if valor_atualizado is not None else None
        diferenca = total_vigente - total_original if None not in (total_vigente, total_original) else None
        percentual = None
        if diferenca is not None and total_original not in (None, Decimal("0")):
            percentual = diferenca * Decimal("100") / total_original
        return {
            "planilhas": planilhas,
            "original": original,
            "vigente": vigente,
            "total_original": total_original,
            "total_vigente": total_vigente,
            "diferenca_original_vigente": diferenca,
            "percentual_original_vigente": percentual,
            "diferenca_contrato_original": total_original - valor_contrato if None not in (total_original, valor_contrato) else None,
            "diferenca_atualizado_vigente": total_vigente - atualizado if None not in (total_vigente, atualizado) else None,
        }

    @staticmethod
    def calcular_resumo(itens):
        subtotais = OrderedDict()
        quantidade = 0
        for item in itens:
            if not item.get("ativo", True):
                continue
            total = calcular_total_item(item)
            item["total_item"] = total
            grupo = item.get("grupo") or "Sem grupo"
            subtotais[grupo] = subtotais.get(grupo, Decimal("0")) + total
            quantidade += 1
        return {
            "subtotais": subtotais,
            "total_geral": sum(subtotais.values(), Decimal("0")),
            "quantidade_itens": quantidade,
        }

    @staticmethod
    def _exigir_elaboracao(planilha):
        if planilha["status"] != "Em elaboração":
            raise PlanilhaBloqueadaError("Planilhas consolidadas não podem ser alteradas.")

    @classmethod
    def _exigir_editavel(cls, planilha):
        cls._exigir_elaboracao(planilha)
        if not planilha["ativo"]:
            raise PlanilhaBloqueadaError("Reative a planilha antes de alterá-la.")

    @staticmethod
    def _obter_planilha_cursor(cursor, planilha_id, bloquear=False):
        sufixo = " FOR UPDATE OF p" if bloquear else ""
        cursor.execute(
            """
            SELECT p.*, c.numero_contrato, c.processo_administrativo,
                   c.valor_original, e.razao_social AS empresa_nome,
                   a.numero_termo AS aditivo_numero
            FROM fc_planilhas_orcamentarias p
            JOIN fc_contratos c ON c.id = p.contrato_id
            JOIN fc_empresas e ON e.id = c.empresa_id
            LEFT JOIN fc_aditivos a ON a.id = p.aditivo_id
            WHERE p.id = %s
            """ + sufixo,
            (planilha_id,),
        )
        planilha = cursor.fetchone()
        if not planilha:
            raise PlanilhaNaoEncontradaError("Planilha não encontrada.")
        return planilha

    @staticmethod
    def _validar_referencias(cursor, dados):
        cursor.execute("SELECT id, ativo FROM fc_contratos WHERE id = %s", (dados["contrato_id"],))
        if not cursor.fetchone():
            raise ReferenciaPlanilhaInvalidaError("O contrato selecionado não existe.")
        if dados.get("aditivo_id") is not None:
            cursor.execute(
                "SELECT id FROM fc_aditivos WHERE id = %s AND contrato_id = %s",
                (dados["aditivo_id"], dados["contrato_id"]),
            )
            if not cursor.fetchone():
                raise ReferenciaPlanilhaInvalidaError("O aditivo selecionado não pertence ao contrato.")

    @staticmethod
    def _tratar_integridade(erro):
        nome = getattr(getattr(erro, "diag", None), "constraint_name", "")
        if nome == "uq_fc_planilhas_original_contrato":
            raise PlanilhaDuplicadaError("Já existe uma planilha Original para este contrato.") from erro
        if nome == "uq_fc_planilhas_contrato_versao":
            raise PlanilhaDuplicadaError("Já existe uma planilha com esta versão para o contrato.") from erro
        raise PlanilhaServiceError("Não foi possível salvar a planilha.") from erro
