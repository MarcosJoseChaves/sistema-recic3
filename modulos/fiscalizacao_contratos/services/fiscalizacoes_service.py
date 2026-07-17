"""Persistência e regras das fiscalizações contratuais."""

from datetime import date

import psycopg2
from psycopg2.extras import RealDictCursor

from ..validacoes_fiscalizacoes import RESULTADOS_FISCALIZACAO, TIPOS_FISCALIZACAO


class FiscalizacaoServiceError(Exception):
    """Erro tratado sem expor detalhes do PostgreSQL ao usuário."""


class FiscalizacaoNaoEncontradaError(FiscalizacaoServiceError): pass
class FiscalizacaoBloqueadaError(FiscalizacaoServiceError): pass
class ReferenciaFiscalizacaoInvalidaError(FiscalizacaoServiceError): pass


class FiscalizacaoService:
    def __init__(self, conectar_banco):
        self._conectar_banco = conectar_banco

    def listar(self, busca="", filtros=None):
        filtros = filtros or {}
        padrao = f"%{(busca or '').strip()}%"
        status_ativo = filtros.get("status_ativo", "ativos")
        parametros = [padrao] * 6 + [
            filtros.get("contrato_id"), filtros.get("empresa_id"),
            filtros.get("servidor_id"), filtros.get("tipo"),
            filtros.get("resultado"), filtros.get("status"),
            filtros.get("data_inicio"), filtros.get("data_fim"), status_ativo,
        ]
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""
                        SELECT f.*, c.numero_contrato, c.processo_administrativo,
                               e.razao_social AS empresa_nome, s.nome AS servidor_nome
                        FROM fc_fiscalizacoes f
                        JOIN fc_contratos c ON c.id=f.contrato_id
                        JOIN fc_empresas e ON e.id=c.empresa_id
                        JOIN fc_servidores s ON s.id=f.servidor_responsavel_id
                        WHERE (%s='' OR c.numero_contrato ILIKE %s
                            OR COALESCE(c.processo_administrativo,'') ILIKE %s
                            OR e.razao_social ILIKE %s OR s.nome ILIKE %s
                            OR f.objeto_verificado ILIKE %s
                            OR COALESCE(f.local_fiscalizacao,'') ILIKE %s)
                          AND (%s IS NULL OR f.contrato_id=%s)
                          AND (%s IS NULL OR c.empresa_id=%s)
                          AND (%s IS NULL OR f.servidor_responsavel_id=%s)
                          AND (%s='' OR f.tipo_fiscalizacao=%s)
                          AND (%s='' OR f.resultado=%s)
                          AND (%s='' OR f.status=%s)
                          AND (%s IS NULL OR f.data_fiscalizacao >= %s)
                          AND (%s IS NULL OR f.data_fiscalizacao <= %s)
                          AND (%s='todos' OR (%s='ativos' AND f.ativo=TRUE)
                              OR (%s='inativos' AND f.ativo=FALSE))
                        ORDER BY f.data_fiscalizacao DESC, f.id DESC
                    """, (
                        padrao, padrao, padrao, padrao, padrao, padrao, padrao,
                        filtros.get("contrato_id"), filtros.get("contrato_id"),
                        filtros.get("empresa_id"), filtros.get("empresa_id"),
                        filtros.get("servidor_id"), filtros.get("servidor_id"),
                        filtros.get("tipo", ""), filtros.get("tipo", ""),
                        filtros.get("resultado", ""), filtros.get("resultado", ""),
                        filtros.get("status", ""), filtros.get("status", ""),
                        filtros.get("data_inicio"), filtros.get("data_inicio"),
                        filtros.get("data_fim"), filtros.get("data_fim"),
                        status_ativo, status_ativo, status_ativo,
                    ))
                    return cursor.fetchall()
        except psycopg2.Error as erro:
            raise FiscalizacaoServiceError("Falha ao consultar fiscalizações.") from erro

    def opcoes(self):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT c.id,c.numero_contrato,c.empresa_id,e.razao_social AS empresa_nome
                        FROM fc_contratos c JOIN fc_empresas e ON e.id=c.empresa_id
                        WHERE c.ativo=TRUE ORDER BY c.numero_contrato""")
                    contratos = cursor.fetchall()
                    cursor.execute("SELECT id,nome,matricula FROM fc_servidores WHERE ativo=TRUE ORDER BY nome")
                    servidores = cursor.fetchall()
                    cursor.execute("SELECT id,razao_social FROM fc_empresas WHERE ativo=TRUE ORDER BY razao_social")
                    empresas = cursor.fetchall()
                    return contratos, servidores, empresas
        except psycopg2.Error as erro:
            raise FiscalizacaoServiceError("Falha ao carregar opções.") from erro

    def obter(self, fiscalizacao_id):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT f.*,c.numero_contrato,c.processo_administrativo,
                        e.razao_social AS empresa_nome,s.nome AS servidor_nome,s.matricula
                        FROM fc_fiscalizacoes f JOIN fc_contratos c ON c.id=f.contrato_id
                        JOIN fc_empresas e ON e.id=c.empresa_id
                        JOIN fc_servidores s ON s.id=f.servidor_responsavel_id WHERE f.id=%s""", (fiscalizacao_id,))
                    item = cursor.fetchone()
                    if not item: raise FiscalizacaoNaoEncontradaError("Fiscalização não encontrada.")
                    return item
        except FiscalizacaoNaoEncontradaError: raise
        except psycopg2.Error as erro:
            raise FiscalizacaoServiceError("Falha ao consultar fiscalização.") from erro

    def _validar_referencias(self, cursor, dados, exigir_contrato_ativo):
        cursor.execute("SELECT ativo FROM fc_contratos WHERE id=%s", (dados["contrato_id"],))
        contrato = cursor.fetchone()
        if not contrato: raise ReferenciaFiscalizacaoInvalidaError("Contrato não encontrado.")
        if exigir_contrato_ativo and not contrato["ativo"]: raise ReferenciaFiscalizacaoInvalidaError("O contrato está inativo.")
        cursor.execute("SELECT ativo FROM fc_servidores WHERE id=%s", (dados["servidor_responsavel_id"],))
        servidor = cursor.fetchone()
        if not servidor: raise ReferenciaFiscalizacaoInvalidaError("Servidor responsável não encontrado.")
        if not servidor["ativo"]: raise ReferenciaFiscalizacaoInvalidaError("O servidor responsável está inativo.")

    def criar(self, dados, usuario_id):
        conexao = self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                self._validar_referencias(cursor, dados, True)
                cursor.execute("""INSERT INTO fc_fiscalizacoes
                    (contrato_id,servidor_responsavel_id,data_fiscalizacao,hora_inicio,hora_fim,
                     tipo_fiscalizacao,local_fiscalizacao,objeto_verificado,resultado,status,
                     observacoes,criado_por_usuario_id,atualizado_por_usuario_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'Em elaboração',%s,%s,%s) RETURNING id""",
                    (dados["contrato_id"],dados["servidor_responsavel_id"],dados["data_fiscalizacao"],
                     dados["hora_inicio"],dados["hora_fim"],dados["tipo_fiscalizacao"],
                     dados["local_fiscalizacao"],dados["objeto_verificado"],dados["resultado"],
                     dados["observacoes"],usuario_id,usuario_id))
                novo_id = cursor.fetchone()["id"]
            conexao.commit(); return novo_id
        except (ReferenciaFiscalizacaoInvalidaError, psycopg2.Error) as erro:
            conexao.rollback()
            if isinstance(erro, ReferenciaFiscalizacaoInvalidaError): raise
            raise FiscalizacaoServiceError("Falha ao cadastrar fiscalização.") from erro
        finally: conexao.close()

    def atualizar(self, fiscalizacao_id, dados, usuario_id):
        conexao = self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT status,ativo FROM fc_fiscalizacoes WHERE id=%s FOR UPDATE", (fiscalizacao_id,))
                atual = cursor.fetchone()
                if not atual: raise FiscalizacaoNaoEncontradaError("Fiscalização não encontrada.")
                if atual["status"] != "Em elaboração" or not atual["ativo"]:
                    raise FiscalizacaoBloqueadaError("Somente fiscalizações ativas em elaboração podem ser editadas.")
                self._validar_referencias(cursor, dados, False)
                cursor.execute("""UPDATE fc_fiscalizacoes SET contrato_id=%s,
                    servidor_responsavel_id=%s,data_fiscalizacao=%s,hora_inicio=%s,hora_fim=%s,
                    tipo_fiscalizacao=%s,local_fiscalizacao=%s,objeto_verificado=%s,resultado=%s,
                    observacoes=%s,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s
                    WHERE id=%s""", (dados["contrato_id"],dados["servidor_responsavel_id"],
                    dados["data_fiscalizacao"],dados["hora_inicio"],dados["hora_fim"],
                    dados["tipo_fiscalizacao"],dados["local_fiscalizacao"],dados["objeto_verificado"],
                    dados["resultado"],dados["observacoes"],usuario_id,fiscalizacao_id))
            conexao.commit()
        except (FiscalizacaoNaoEncontradaError, FiscalizacaoBloqueadaError, ReferenciaFiscalizacaoInvalidaError, psycopg2.Error) as erro:
            conexao.rollback()
            if not isinstance(erro, psycopg2.Error): raise
            raise FiscalizacaoServiceError("Falha ao atualizar fiscalização.") from erro
        finally: conexao.close()

    def alterar_status(self, fiscalizacao_id, novo_status, usuario_id):
        if novo_status not in ("Finalizada", "Cancelada"):
            raise FiscalizacaoBloqueadaError("Status de fiscalização inválido.")
        conexao = self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT * FROM fc_fiscalizacoes WHERE id=%s FOR UPDATE", (fiscalizacao_id,))
                atual = cursor.fetchone()
                if not atual: raise FiscalizacaoNaoEncontradaError("Fiscalização não encontrada.")
                if not atual["ativo"] or atual["status"] != "Em elaboração":
                    raise FiscalizacaoBloqueadaError("Esta fiscalização não pode ser finalizada ou cancelada.")
                obrigatorios = (atual["contrato_id"], atual["servidor_responsavel_id"], atual["data_fiscalizacao"], atual["tipo_fiscalizacao"], atual["objeto_verificado"], atual["resultado"])
                if novo_status == "Finalizada" and not all(obrigatorios):
                    raise FiscalizacaoBloqueadaError("Complete os dados obrigatórios antes de finalizar.")
                cursor.execute("UPDATE fc_fiscalizacoes SET status=%s,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s", (novo_status,usuario_id,fiscalizacao_id))
            conexao.commit()
        except (FiscalizacaoNaoEncontradaError, FiscalizacaoBloqueadaError, psycopg2.Error) as erro:
            conexao.rollback()
            if not isinstance(erro, psycopg2.Error): raise
            raise FiscalizacaoServiceError("Falha ao alterar fiscalização.") from erro
        finally: conexao.close()

    def listar_do_contrato(self, contrato_id, limite=10):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT f.*,s.nome AS servidor_nome FROM fc_fiscalizacoes f
                        JOIN fc_servidores s ON s.id=f.servidor_responsavel_id
                        WHERE f.contrato_id=%s ORDER BY f.data_fiscalizacao DESC,f.id DESC LIMIT %s""", (contrato_id,limite))
                    return cursor.fetchall()
        except psycopg2.Error as erro: raise FiscalizacaoServiceError("Falha ao consultar fiscalizações.") from erro

    def indicadores(self):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT
                        COUNT(*) FILTER (WHERE o.ativo AND o.status IN ('Aberta','Em acompanhamento')) AS ocorrencias_abertas,
                        COUNT(*) FILTER (WHERE o.ativo AND o.status IN ('Aberta','Em acompanhamento') AND o.prazo_correcao<CURRENT_DATE) AS ocorrencias_vencidas,
                        COUNT(*) FILTER (WHERE o.ativo AND o.status IN ('Aberta','Em acompanhamento') AND o.gravidade IN ('Grave','Crítica')) AS graves_criticas,
                        (SELECT COUNT(*) FROM fc_fiscalizacoes f WHERE f.ativo AND f.data_fiscalizacao>=CURRENT_DATE-30) AS fiscalizacoes_30_dias
                        FROM fc_ocorrencias o""")
                    return cursor.fetchone()
        except psycopg2.Error as erro: raise FiscalizacaoServiceError("Falha ao consultar indicadores.") from erro
