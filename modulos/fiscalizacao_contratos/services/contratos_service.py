"""Operações de banco para contratos e seus responsáveis."""

import psycopg2
from psycopg2.extras import RealDictCursor


class ContratoServiceError(Exception):
    """Falha interna tratada pelo módulo."""


class ContratoDuplicadoError(ContratoServiceError):
    """Número de contrato já cadastrado."""


class ContratoNaoEncontradoError(ContratoServiceError):
    """Contrato não encontrado."""


class ReferenciaContratoInvalidaError(ContratoServiceError):
    """Empresa ou servidor não pode ser usado no contrato."""


class ContratoService:
    def __init__(self, conectar_banco):
        self._conectar_banco = conectar_banco

    def listar(
        self,
        busca="",
        situacao="",
        empresa_id=None,
        status_ativo="ativos",
        proximos_vencimento=False,
    ):
        conexao = None
        busca = (busca or "").strip()
        padrao = f"%{busca}%"
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT c.id, c.numero_contrato, c.processo_administrativo,
                           c.objeto, c.valor_original, c.vigencia_inicio,
                           c.vigencia_fim, c.situacao, c.ativo,
                           e.id AS empresa_id, e.razao_social AS empresa_nome,
                           (
                               c.ativo = TRUE
                               AND c.vigencia_fim BETWEEN CURRENT_DATE
                                   AND CURRENT_DATE + INTERVAL '60 days'
                           ) AS vence_em_60_dias
                    FROM fc_contratos c
                    JOIN fc_empresas e ON e.id = c.empresa_id
                    WHERE (
                        %s = '' OR c.numero_contrato ILIKE %s
                        OR COALESCE(c.processo_administrativo, '') ILIKE %s
                        OR c.objeto ILIKE %s OR e.razao_social ILIKE %s
                    )
                      AND (%s = '' OR c.situacao = %s)
                      AND (%s::BIGINT IS NULL OR c.empresa_id = %s)
                      AND (
                          %s = 'todos'
                          OR (%s = 'ativos' AND c.ativo = TRUE)
                          OR (%s = 'inativos' AND c.ativo = FALSE)
                      )
                      AND (
                          %s = FALSE
                          OR (
                              c.ativo = TRUE
                              AND c.vigencia_fim BETWEEN CURRENT_DATE
                                  AND CURRENT_DATE + INTERVAL '60 days'
                          )
                      )
                    ORDER BY c.ativo DESC, c.vigencia_fim NULLS LAST, c.numero_contrato
                    """,
                    (
                        busca,
                        padrao,
                        padrao,
                        padrao,
                        padrao,
                        situacao,
                        situacao,
                        empresa_id,
                        empresa_id,
                        status_ativo,
                        status_ativo,
                        status_ativo,
                        proximos_vencimento,
                    ),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise ContratoServiceError("Não foi possível carregar os contratos.") from erro
        finally:
            if conexao:
                conexao.close()

    def listar_empresas_filtro(self):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, razao_social, ativo
                    FROM fc_empresas
                    ORDER BY ativo DESC, razao_social
                    """
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise ContratoServiceError("Não foi possível carregar as empresas.") from erro
        finally:
            if conexao:
                conexao.close()

    def opcoes_formulario(self):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT id, razao_social
                    FROM fc_empresas
                    WHERE ativo = TRUE
                    ORDER BY razao_social
                    """
                )
                empresas = cursor.fetchall()
                cursor.execute(
                    """
                    SELECT id, nome, matricula, cargo
                    FROM fc_servidores
                    WHERE ativo = TRUE
                    ORDER BY nome
                    """
                )
                servidores = cursor.fetchall()
                return empresas, servidores
        except psycopg2.Error as erro:
            raise ContratoServiceError(
                "Não foi possível carregar empresas e servidores."
            ) from erro
        finally:
            if conexao:
                conexao.close()

    def obter(self, contrato_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT c.*, e.razao_social AS empresa_nome, e.ativo AS empresa_ativa,
                           (
                               c.ativo = TRUE
                               AND c.vigencia_fim BETWEEN CURRENT_DATE
                                   AND CURRENT_DATE + INTERVAL '60 days'
                           ) AS vence_em_60_dias
                    FROM fc_contratos c
                    JOIN fc_empresas e ON e.id = c.empresa_id
                    WHERE c.id = %s
                    """,
                    (contrato_id,),
                )
                contrato = cursor.fetchone()
                if not contrato:
                    raise ContratoNaoEncontradoError("Contrato não encontrado.")

                cursor.execute(
                    """
                    SELECT r.id, r.servidor_id, r.tipo_responsabilidade, r.titular,
                           r.data_inicio, r.data_fim, r.ativo, r.criado_em,
                           r.atualizado_em, s.nome AS servidor_nome,
                           s.matricula AS servidor_matricula, s.cargo AS servidor_cargo
                    FROM fc_contrato_responsaveis r
                    JOIN fc_servidores s ON s.id = r.servidor_id
                    WHERE r.contrato_id = %s
                    ORDER BY r.ativo DESC,
                             CASE r.tipo_responsabilidade
                                 WHEN 'Gestor' THEN 1
                                 WHEN 'Fiscal titular' THEN 2
                                 ELSE 3
                             END,
                             r.data_inicio DESC, s.nome
                    """,
                    (contrato_id,),
                )
                return contrato, cursor.fetchall()
        except ContratoNaoEncontradoError:
            raise
        except psycopg2.Error as erro:
            raise ContratoServiceError("Não foi possível carregar o contrato.") from erro
        finally:
            if conexao:
                conexao.close()

    def criar(self, dados, responsaveis, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor() as cursor:
                self._validar_referencias(cursor, dados, responsaveis)
                cursor.execute(
                    """
                    INSERT INTO fc_contratos (
                        numero_contrato, processo_administrativo, objeto, empresa_id,
                        valor_original, data_assinatura, vigencia_inicio, vigencia_fim,
                        situacao, observacoes, criado_por_usuario_id,
                        atualizado_por_usuario_id
                    ) VALUES (
                        %(numero_contrato)s, %(processo_administrativo)s, %(objeto)s,
                        %(empresa_id)s, %(valor_original)s, %(data_assinatura)s,
                        %(vigencia_inicio)s, %(vigencia_fim)s, %(situacao)s,
                        %(observacoes)s, %(usuario_id)s, %(usuario_id)s
                    )
                    RETURNING id
                    """,
                    {**dados, "usuario_id": usuario_id},
                )
                contrato_id = cursor.fetchone()[0]
                for servidor_id, tipo, titular in self._vinculos_desejados(responsaveis):
                    self._inserir_vinculo(
                        cursor,
                        contrato_id,
                        servidor_id,
                        tipo,
                        titular,
                        usuario_id,
                    )
            conexao.commit()
            return contrato_id
        except ReferenciaContratoInvalidaError:
            if conexao:
                conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao:
                conexao.rollback()
            if self._nome_restricao(erro) == "uq_fc_contratos_numero":
                raise ContratoDuplicadoError(
                    "Já existe um contrato cadastrado com este número."
                ) from erro
            raise ContratoServiceError("Não foi possível cadastrar o contrato.") from erro
        except psycopg2.Error as erro:
            if conexao:
                conexao.rollback()
            raise ContratoServiceError("Não foi possível cadastrar o contrato.") from erro
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise ContratoServiceError("Não foi possível cadastrar o contrato.") from erro
        finally:
            if conexao:
                conexao.close()

    def atualizar(self, contrato_id, dados, responsaveis, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor() as cursor:
                self._validar_referencias(cursor, dados, responsaveis)
                cursor.execute(
                    """
                    UPDATE fc_contratos
                    SET numero_contrato = %(numero_contrato)s,
                        processo_administrativo = %(processo_administrativo)s,
                        objeto = %(objeto)s,
                        empresa_id = %(empresa_id)s,
                        valor_original = %(valor_original)s,
                        data_assinatura = %(data_assinatura)s,
                        vigencia_inicio = %(vigencia_inicio)s,
                        vigencia_fim = %(vigencia_fim)s,
                        situacao = %(situacao)s,
                        observacoes = %(observacoes)s,
                        atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %(usuario_id)s
                    WHERE id = %(contrato_id)s
                    """,
                    {**dados, "usuario_id": usuario_id, "contrato_id": contrato_id},
                )
                if cursor.rowcount == 0:
                    raise ContratoNaoEncontradoError("Contrato não encontrado.")
                self._sincronizar_responsaveis(
                    cursor, contrato_id, responsaveis, usuario_id
                )
            conexao.commit()
        except (ContratoNaoEncontradoError, ReferenciaContratoInvalidaError):
            if conexao:
                conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao:
                conexao.rollback()
            if self._nome_restricao(erro) == "uq_fc_contratos_numero":
                raise ContratoDuplicadoError(
                    "Já existe um contrato cadastrado com este número."
                ) from erro
            raise ContratoServiceError("Não foi possível atualizar o contrato.") from erro
        except psycopg2.Error as erro:
            if conexao:
                conexao.rollback()
            raise ContratoServiceError("Não foi possível atualizar o contrato.") from erro
        except Exception as erro:
            if conexao:
                conexao.rollback()
            raise ContratoServiceError("Não foi possível atualizar o contrato.") from erro
        finally:
            if conexao:
                conexao.close()

    def inativar(self, contrato_id, usuario_id):
        self._alterar_situacao_ativa(contrato_id, usuario_id, ativo=False)

    def reativar(self, contrato_id, usuario_id):
        self._alterar_situacao_ativa(contrato_id, usuario_id, ativo=True)

    def _alterar_situacao_ativa(self, contrato_id, usuario_id, *, ativo):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor() as cursor:
                cursor.execute(
                    """
                    UPDATE fc_contratos
                    SET ativo = %s,
                        atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s
                    WHERE id = %s
                    """,
                    (ativo, usuario_id, contrato_id),
                )
                if cursor.rowcount == 0:
                    raise ContratoNaoEncontradoError("Contrato não encontrado.")
            conexao.commit()
        except ContratoNaoEncontradoError:
            if conexao:
                conexao.rollback()
            raise
        except psycopg2.Error as erro:
            if conexao:
                conexao.rollback()
            acao = "reativar" if ativo else "inativar"
            raise ContratoServiceError(f"Não foi possível {acao} o contrato.") from erro
        except Exception as erro:
            if conexao:
                conexao.rollback()
            acao = "reativar" if ativo else "inativar"
            raise ContratoServiceError(f"Não foi possível {acao} o contrato.") from erro
        finally:
            if conexao:
                conexao.close()

    @staticmethod
    def _validar_referencias(cursor, dados, responsaveis):
        cursor.execute("SELECT ativo FROM fc_empresas WHERE id = %s", (dados["empresa_id"],))
        empresa = cursor.fetchone()
        if not empresa or not empresa[0]:
            raise ReferenciaContratoInvalidaError(
                "A empresa selecionada não existe ou está inativa."
            )

        ids_servidores = {
            responsaveis["gestor_id"],
            responsaveis["fiscal_titular_id"],
            *responsaveis["fiscais_substitutos"],
        }
        cursor.execute(
            "SELECT id, ativo FROM fc_servidores WHERE id = ANY(%s)",
            (list(ids_servidores),),
        )
        encontrados = {servidor_id: ativo for servidor_id, ativo in cursor.fetchall()}
        if any(not encontrados.get(servidor_id) for servidor_id in ids_servidores):
            raise ReferenciaContratoInvalidaError(
                "Um dos responsáveis selecionados não existe ou está inativo."
            )

    @staticmethod
    def _vinculos_desejados(responsaveis):
        vinculos = [
            (responsaveis["gestor_id"], "Gestor", True),
            (responsaveis["fiscal_titular_id"], "Fiscal titular", True),
        ]
        vinculos.extend(
            (servidor_id, "Fiscal substituto", False)
            for servidor_id in responsaveis["fiscais_substitutos"]
        )
        return vinculos

    @staticmethod
    def _inserir_vinculo(
        cursor, contrato_id, servidor_id, tipo, titular, usuario_id
    ):
        cursor.execute(
            """
            INSERT INTO fc_contrato_responsaveis (
                contrato_id, servidor_id, tipo_responsabilidade, titular,
                data_inicio, criado_por_usuario_id, atualizado_por_usuario_id
            ) VALUES (%s, %s, %s, %s, CURRENT_DATE, %s, %s)
            """,
            (contrato_id, servidor_id, tipo, titular, usuario_id, usuario_id),
        )

    def _sincronizar_responsaveis(
        self, cursor, contrato_id, responsaveis, usuario_id
    ):
        cursor.execute(
            """
            SELECT id, servidor_id, tipo_responsabilidade
            FROM fc_contrato_responsaveis
            WHERE contrato_id = %s AND ativo = TRUE
            """,
            (contrato_id,),
        )
        atuais = {
            (servidor_id, tipo): vinculo_id
            for vinculo_id, servidor_id, tipo in cursor.fetchall()
        }
        desejados_com_titular = self._vinculos_desejados(responsaveis)
        desejados = {
            (servidor_id, tipo): titular
            for servidor_id, tipo, titular in desejados_com_titular
        }

        for chave, vinculo_id in atuais.items():
            if chave not in desejados:
                cursor.execute(
                    """
                    UPDATE fc_contrato_responsaveis
                    SET ativo = FALSE,
                        data_fim = CURRENT_DATE,
                        atualizado_em = CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id = %s
                    WHERE id = %s
                    """,
                    (usuario_id, vinculo_id),
                )

        for (servidor_id, tipo), titular in desejados.items():
            if (servidor_id, tipo) not in atuais:
                self._inserir_vinculo(
                    cursor,
                    contrato_id,
                    servidor_id,
                    tipo,
                    titular,
                    usuario_id,
                )

    @staticmethod
    def _nome_restricao(erro):
        diagnostico = getattr(erro, "diag", None)
        return getattr(diagnostico, "constraint_name", None)
