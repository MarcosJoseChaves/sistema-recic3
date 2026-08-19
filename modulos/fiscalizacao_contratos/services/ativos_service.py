"""Persistência e regras dos ativos utilizados na execução contratual."""

from datetime import date
from decimal import Decimal

import psycopg2
from psycopg2.extras import RealDictCursor

from ..validacoes_ativos import NATUREZAS_VINCULO


class AtivoServiceError(Exception):
    """Falha interna tratada sem expor SQL ao usuário."""


class AtivoNaoEncontradoError(AtivoServiceError):
    pass


class AtivoDuplicadoError(AtivoServiceError):
    pass


class ReferenciaAtivoInvalidaError(AtivoServiceError):
    pass


class AtivoBloqueadoError(AtivoServiceError):
    pass


class VinculoNaoEncontradoError(AtivoServiceError):
    pass


class VinculoDuplicadoError(AtivoServiceError):
    pass


class AtivoService:
    def __init__(self, conectar_banco):
        self._conectar_banco = conectar_banco

    def listar(
        self, busca="", tipo_ativo="", origem_ativo="", situacao="",
        empresa_id=None, contrato_id=None, com_vinculo_ativo="", status_ativo="ativos",
    ):
        conexao = None
        busca = (busca or "").strip()
        padrao = f"%{busca}%"
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT a.*, e.razao_social AS empresa_proprietaria_nome,
                           COALESCE(v.quantidade_vinculos_ativos, 0) AS quantidade_vinculos_ativos
                    FROM fc_ativos_contratuais a
                    LEFT JOIN fc_empresas e ON e.id = a.empresa_proprietaria_id
                    LEFT JOIN (
                        SELECT ativo_id, COUNT(*) AS quantidade_vinculos_ativos
                        FROM fc_ativo_vinculos WHERE ativo = TRUE GROUP BY ativo_id
                    ) v ON v.ativo_id = a.id
                    WHERE (
                        %s = '' OR a.codigo_interno ILIKE %s OR a.descricao ILIKE %s
                        OR COALESCE(a.marca, '') ILIKE %s OR COALESCE(a.modelo, '') ILIKE %s
                        OR COALESCE(a.placa, '') ILIKE %s OR COALESCE(a.renavam, '') ILIKE %s
                        OR COALESCE(a.chassi, '') ILIKE %s OR COALESCE(a.numero_serie, '') ILIKE %s
                        OR COALESCE(a.numero_patrimonio, '') ILIKE %s
                        OR COALESCE(e.razao_social, '') ILIKE %s
                        OR EXISTS (
                            SELECT 1 FROM fc_ativo_vinculos vx
                            JOIN fc_contratos cx ON cx.id = vx.contrato_id
                            WHERE vx.ativo_id = a.id AND cx.numero_contrato ILIKE %s
                        )
                    )
                      AND (%s = '' OR a.tipo_ativo = %s)
                      AND (%s = '' OR a.origem_ativo = %s)
                      AND (%s = '' OR a.situacao = %s)
                      AND (%s::BIGINT IS NULL OR a.empresa_proprietaria_id = %s)
                      AND (%s::BIGINT IS NULL OR EXISTS (
                          SELECT 1 FROM fc_ativo_vinculos vc
                          WHERE vc.ativo_id = a.id AND vc.contrato_id = %s
                      ))
                      AND (%s = '' OR (%s = 'sim' AND COALESCE(v.quantidade_vinculos_ativos, 0) > 0)
                                      OR (%s = 'nao' AND COALESCE(v.quantidade_vinculos_ativos, 0) = 0))
                      AND (%s = 'todos' OR (%s = 'ativos' AND a.ativo = TRUE)
                                           OR (%s = 'inativos' AND a.ativo = FALSE))
                    ORDER BY a.ativo DESC, a.codigo_interno
                    """,
                    (
                        busca, padrao, padrao, padrao, padrao, padrao, padrao,
                        padrao, padrao, padrao, padrao, padrao,
                        tipo_ativo, tipo_ativo, origem_ativo, origem_ativo,
                        situacao, situacao, empresa_id, empresa_id,
                        contrato_id, contrato_id, com_vinculo_ativo,
                        com_vinculo_ativo, com_vinculo_ativo,
                        status_ativo, status_ativo, status_ativo,
                    ),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise AtivoServiceError("Não foi possível carregar os ativos.") from erro
        finally:
            if conexao:
                conexao.close()

    def contadores(self):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT COUNT(*) FILTER (WHERE ativo = TRUE) AS ativos_cadastrados,
                           COUNT(*) FILTER (WHERE ativo = TRUE AND situacao = 'Em operação') AS em_operacao,
                           COUNT(*) FILTER (WHERE ativo = TRUE AND situacao = 'Em manutenção') AS em_manutencao,
                           (SELECT COUNT(*) FROM fc_ativo_vinculos WHERE ativo = TRUE) AS vinculos_ativos
                    FROM fc_ativos_contratuais
                    """
                )
                return cursor.fetchone()
        except psycopg2.Error as erro:
            raise AtivoServiceError("Não foi possível calcular os indicadores.") from erro
        finally:
            if conexao:
                conexao.close()

    def opcoes(self):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT id, razao_social, ativo FROM fc_empresas ORDER BY ativo DESC, razao_social")
                empresas = cursor.fetchall()
                cursor.execute("SELECT id, numero_contrato, ativo FROM fc_contratos ORDER BY ativo DESC, numero_contrato")
                contratos = cursor.fetchall()
                return empresas, contratos
        except psycopg2.Error as erro:
            raise AtivoServiceError("Não foi possível carregar as opções.") from erro
        finally:
            if conexao:
                conexao.close()

    def obter(self, ativo_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                ativo = self._obter_ativo(cursor, ativo_id)
                cursor.execute(
                    """
                    SELECT v.*, c.numero_contrato, c.objeto AS contrato_objeto,
                           e.razao_social AS empresa_contratada
                    FROM fc_ativo_vinculos v
                    JOIN fc_contratos c ON c.id = v.contrato_id
                    JOIN fc_empresas e ON e.id = c.empresa_id
                    WHERE v.ativo_id = %s
                    ORDER BY v.ativo DESC, v.data_inicio DESC, v.id DESC
                    """,
                    (ativo_id,),
                )
                return ativo, cursor.fetchall()
        except AtivoNaoEncontradoError:
            raise
        except psycopg2.Error as erro:
            raise AtivoServiceError("Não foi possível carregar o ativo.") from erro
        finally:
            if conexao:
                conexao.close()

    def criar(self, dados, usuario_id):
        conexao = None
        try:
            self._validar_dados_ativo(dados)
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                self._validar_empresa(cursor, dados.get("empresa_proprietaria_id"))
                cursor.execute(
                    """
                    INSERT INTO fc_ativos_contratuais (
                        codigo_interno, tipo_ativo, descricao, marca, modelo,
                        ano_fabricacao, placa, renavam, chassi, numero_serie,
                        numero_patrimonio, origem_ativo, empresa_proprietaria_id,
                        capacidade, unidade_capacidade, situacao, observacoes,
                        criado_por_usuario_id, atualizado_por_usuario_id
                    ) VALUES (
                        %(codigo_interno)s, %(tipo_ativo)s, %(descricao)s, %(marca)s,
                        %(modelo)s, %(ano_fabricacao)s, %(placa)s, %(renavam)s,
                        %(chassi)s, %(numero_serie)s, %(numero_patrimonio)s,
                        %(origem_ativo)s, %(empresa_proprietaria_id)s, %(capacidade)s,
                        %(unidade_capacidade)s, %(situacao)s, %(observacoes)s,
                        %(usuario_id)s, %(usuario_id)s
                    ) RETURNING id
                    """,
                    {**dados, "usuario_id": usuario_id},
                )
                ativo_id = cursor.fetchone()["id"]
            conexao.commit()
            return ativo_id
        except ReferenciaAtivoInvalidaError:
            if conexao: conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao: conexao.rollback()
            self._tratar_integridade(erro)
        except Exception as erro:
            if conexao: conexao.rollback()
            raise AtivoServiceError("Não foi possível cadastrar o ativo.") from erro
        finally:
            if conexao: conexao.close()

    def atualizar(self, ativo_id, dados, usuario_id):
        conexao = None
        try:
            self._validar_dados_ativo(dados)
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                self._obter_ativo(cursor, ativo_id, bloquear=True)
                self._validar_empresa(cursor, dados.get("empresa_proprietaria_id"))
                cursor.execute(
                    """
                    UPDATE fc_ativos_contratuais SET
                        codigo_interno=%(codigo_interno)s, tipo_ativo=%(tipo_ativo)s,
                        descricao=%(descricao)s, marca=%(marca)s, modelo=%(modelo)s,
                        ano_fabricacao=%(ano_fabricacao)s, placa=%(placa)s,
                        renavam=%(renavam)s, chassi=%(chassi)s,
                        numero_serie=%(numero_serie)s,
                        numero_patrimonio=%(numero_patrimonio)s,
                        origem_ativo=%(origem_ativo)s,
                        empresa_proprietaria_id=%(empresa_proprietaria_id)s,
                        capacidade=%(capacidade)s,
                        unidade_capacidade=%(unidade_capacidade)s,
                        situacao=%(situacao)s, observacoes=%(observacoes)s,
                        atualizado_em=CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id=%(usuario_id)s
                    WHERE id=%(ativo_id)s
                    """,
                    {**dados, "usuario_id": usuario_id, "ativo_id": ativo_id},
                )
            conexao.commit()
        except (AtivoNaoEncontradoError, ReferenciaAtivoInvalidaError):
            if conexao: conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao: conexao.rollback()
            self._tratar_integridade(erro)
        except Exception as erro:
            if conexao: conexao.rollback()
            raise AtivoServiceError("Não foi possível atualizar o ativo.") from erro
        finally:
            if conexao: conexao.close()

    def alterar_ativo(self, ativo_id, usuario_id, ativo):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                self._obter_ativo(cursor, ativo_id, bloquear=True)
                if not ativo:
                    cursor.execute("SELECT COUNT(*) AS quantidade FROM fc_ativo_vinculos WHERE ativo_id=%s AND ativo=TRUE", (ativo_id,))
                    if cursor.fetchone()["quantidade"]:
                        raise AtivoBloqueadoError("Encerre os vínculos ativos antes de inativar o ativo.")
                cursor.execute(
                    """UPDATE fc_ativos_contratuais
                       SET ativo=%s, atualizado_em=CURRENT_TIMESTAMP,
                           atualizado_por_usuario_id=%s WHERE id=%s""",
                    (ativo, usuario_id, ativo_id),
                )
            conexao.commit()
        except (AtivoNaoEncontradoError, AtivoBloqueadoError):
            if conexao: conexao.rollback()
            raise
        except Exception as erro:
            if conexao: conexao.rollback()
            raise AtivoServiceError("Não foi possível alterar a situação do ativo.") from erro
        finally:
            if conexao: conexao.close()

    def criar_vinculo(self, dados, usuario_id):
        conexao = None
        try:
            self._validar_dados_vinculo(dados)
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                ativo = self._obter_ativo(cursor, dados["ativo_id"], bloquear=True)
                if not ativo["ativo"]:
                    raise AtivoBloqueadoError("Ativos inativos não podem receber novos vínculos.")
                if ativo["situacao"] == "Baixado":
                    raise AtivoBloqueadoError("Ativos baixados não podem receber novos vínculos.")
                cursor.execute("SELECT id, ativo FROM fc_contratos WHERE id=%s FOR UPDATE", (dados["contrato_id"],))
                contrato = cursor.fetchone()
                if not contrato:
                    raise ReferenciaAtivoInvalidaError("O contrato selecionado não existe.")
                if not contrato["ativo"]:
                    raise ReferenciaAtivoInvalidaError("O contrato selecionado está inativo.")
                cursor.execute(
                    """
                    INSERT INTO fc_ativo_vinculos (
                        ativo_id, contrato_id, natureza_vinculo, data_inicio,
                        data_fim, principal, observacoes,
                        criado_por_usuario_id, atualizado_por_usuario_id
                    ) VALUES (
                        %(ativo_id)s, %(contrato_id)s, %(natureza_vinculo)s,
                        %(data_inicio)s, %(data_fim)s, %(principal)s,
                        %(observacoes)s, %(usuario_id)s, %(usuario_id)s
                    ) RETURNING id
                    """,
                    {**dados, "usuario_id": usuario_id},
                )
                vinculo_id = cursor.fetchone()["id"]
            conexao.commit()
            return vinculo_id
        except (AtivoNaoEncontradoError, AtivoBloqueadoError, ReferenciaAtivoInvalidaError):
            if conexao: conexao.rollback()
            raise
        except psycopg2.IntegrityError as erro:
            if conexao: conexao.rollback()
            self._tratar_integridade(erro)
        except Exception as erro:
            if conexao: conexao.rollback()
            raise AtivoServiceError("Não foi possível criar o vínculo.") from erro
        finally:
            if conexao: conexao.close()

    def obter_vinculo(self, vinculo_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT * FROM fc_ativo_vinculos WHERE id=%s", (vinculo_id,))
                vinculo = cursor.fetchone()
                if not vinculo:
                    raise VinculoNaoEncontradoError("Vínculo não encontrado.")
                return vinculo
        except VinculoNaoEncontradoError:
            raise
        except psycopg2.Error as erro:
            raise AtivoServiceError("Não foi possível carregar o vínculo.") from erro
        finally:
            if conexao: conexao.close()

    def encerrar_vinculo(self, vinculo_id, data_fim, usuario_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT * FROM fc_ativo_vinculos WHERE id=%s FOR UPDATE", (vinculo_id,))
                vinculo = cursor.fetchone()
                if not vinculo:
                    raise VinculoNaoEncontradoError("Vínculo não encontrado.")
                if not vinculo["ativo"]:
                    raise AtivoBloqueadoError("Este vínculo já foi encerrado e permanece no histórico.")
                if data_fim < vinculo["data_inicio"]:
                    raise ReferenciaAtivoInvalidaError("A data final não pode ser anterior à data inicial.")
                cursor.execute(
                    """UPDATE fc_ativo_vinculos
                       SET data_fim=%s, ativo=FALSE,
                           atualizado_em=CURRENT_TIMESTAMP,
                           atualizado_por_usuario_id=%s WHERE id=%s""",
                    (data_fim, usuario_id, vinculo_id),
                )
            conexao.commit()
            return vinculo["ativo_id"]
        except (VinculoNaoEncontradoError, AtivoBloqueadoError, ReferenciaAtivoInvalidaError):
            if conexao: conexao.rollback()
            raise
        except Exception as erro:
            if conexao: conexao.rollback()
            raise AtivoServiceError("Não foi possível encerrar o vínculo.") from erro
        finally:
            if conexao: conexao.close()

    def listar_vinculos(self, busca="", status_ativo="todos"):
        conexao = None
        padrao = f"%{(busca or '').strip()}%"
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT v.*, a.codigo_interno, a.descricao AS ativo_descricao,
                           c.numero_contrato, e.razao_social AS empresa_contratada
                    FROM fc_ativo_vinculos v
                    JOIN fc_ativos_contratuais a ON a.id=v.ativo_id
                    JOIN fc_contratos c ON c.id=v.contrato_id
                    JOIN fc_empresas e ON e.id=c.empresa_id
                    WHERE (%s='' OR a.codigo_interno ILIKE %s OR a.descricao ILIKE %s
                                      OR c.numero_contrato ILIKE %s)
                      AND (%s='todos' OR (%s='ativos' AND v.ativo=TRUE)
                                           OR (%s='encerrados' AND v.ativo=FALSE))
                    ORDER BY v.ativo DESC, v.data_inicio DESC, v.id DESC
                    """,
                    ((busca or '').strip(), padrao, padrao, padrao,
                     status_ativo, status_ativo, status_ativo),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise AtivoServiceError("Não foi possível carregar os vínculos.") from erro
        finally:
            if conexao: conexao.close()

    def listar_do_contrato(self, contrato_id):
        conexao = None
        try:
            conexao = self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute(
                    """
                    SELECT v.*, a.codigo_interno, a.tipo_ativo,
                           a.descricao AS ativo_descricao, a.placa,
                           a.numero_patrimonio, a.situacao, a.ativo AS ativo_cadastral
                    FROM fc_ativo_vinculos v
                    JOIN fc_ativos_contratuais a ON a.id=v.ativo_id
                    WHERE v.contrato_id=%s
                    ORDER BY v.ativo DESC, v.data_inicio DESC, v.id DESC
                    """,
                    (contrato_id,),
                )
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise AtivoServiceError("Não foi possível carregar os ativos do contrato.") from erro
        finally:
            if conexao: conexao.close()

    @staticmethod
    def _validar_empresa(cursor, empresa_id):
        if empresa_id is None:
            return
        cursor.execute("SELECT id, ativo FROM fc_empresas WHERE id=%s", (empresa_id,))
        empresa = cursor.fetchone()
        if not empresa:
            raise ReferenciaAtivoInvalidaError("A empresa proprietária não existe.")
        if not empresa["ativo"]:
            raise ReferenciaAtivoInvalidaError("A empresa proprietária está inativa.")

    @staticmethod
    def _validar_dados_ativo(dados):
        ano = dados.get("ano_fabricacao")
        if ano is not None and (
            not isinstance(ano, int) or isinstance(ano, bool)
            or ano < 1900 or ano > date.today().year + 1
        ):
            raise ReferenciaAtivoInvalidaError(
                f"O ano de fabricação deve estar entre 1900 e {date.today().year + 1}."
            )
        capacidade = dados.get("capacidade")
        if capacidade is not None and (
            not isinstance(capacidade, Decimal) or capacidade < 0
        ):
            raise ReferenciaAtivoInvalidaError("A capacidade deve ser um valor decimal não negativo.")

    @staticmethod
    def _validar_dados_vinculo(dados):
        if dados.get("natureza_vinculo") not in NATUREZAS_VINCULO:
            raise ReferenciaAtivoInvalidaError("A natureza do vínculo é inválida.")
        if not isinstance(dados.get("data_inicio"), date):
            raise ReferenciaAtivoInvalidaError("A data inicial do vínculo é inválida.")
        if dados.get("data_fim") is not None:
            raise ReferenciaAtivoInvalidaError(
                "Um novo vínculo ativo não pode possuir data final."
            )

    @staticmethod
    def _obter_ativo(cursor, ativo_id, bloquear=False):
        sufixo = " FOR UPDATE OF a" if bloquear else ""
        cursor.execute(
            """
            SELECT a.*, e.razao_social AS empresa_proprietaria_nome
            FROM fc_ativos_contratuais a
            LEFT JOIN fc_empresas e ON e.id=a.empresa_proprietaria_id
            WHERE a.id=%s
            """ + sufixo,
            (ativo_id,),
        )
        ativo = cursor.fetchone()
        if not ativo:
            raise AtivoNaoEncontradoError("Ativo não encontrado.")
        return ativo

    @staticmethod
    def _tratar_integridade(erro):
        nome = getattr(getattr(erro, "diag", None), "constraint_name", "")
        mensagens = {
            "uq_fc_ativos_codigo_normalizado": "Já existe um ativo com este código interno.",
            "uq_fc_ativos_placa_normalizada": "Já existe um ativo com esta placa.",
            "uq_fc_ativos_chassi_normalizado": "Já existe um ativo com este chassi.",
            "uq_fc_ativos_patrimonio_normalizado": "Já existe um ativo com este número de patrimônio.",
        }
        if nome in mensagens:
            raise AtivoDuplicadoError(mensagens[nome]) from erro
        if nome == "uq_fc_ativo_vinculo_ativo":
            raise VinculoDuplicadoError("Este ativo já possui vínculo ativo com o contrato.") from erro
        raise AtivoServiceError("Não foi possível salvar o registro.") from erro
