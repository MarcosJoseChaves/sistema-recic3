"""Persistência, histórico e regras das ocorrências contratuais."""

import psycopg2
from psycopg2.extras import RealDictCursor


class OcorrenciaServiceError(Exception): pass
class OcorrenciaNaoEncontradaError(OcorrenciaServiceError): pass
class OcorrenciaBloqueadaError(OcorrenciaServiceError): pass
class ReferenciaOcorrenciaInvalidaError(OcorrenciaServiceError): pass


class OcorrenciaService:
    def __init__(self, conectar_banco): self._conectar_banco = conectar_banco

    def listar(self, busca="", filtros=None):
        filtros=filtros or {}; padrao=f"%{(busca or '').strip()}%"; status_ativo=filtros.get("status_ativo","ativos")
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT o.*,c.numero_contrato,e.razao_social AS empresa_nome,
                        s.nome AS servidor_nome,a.codigo_interno AS ativo_codigo,
                        (o.ativo AND o.status IN ('Aberta','Em acompanhamento')
                         AND o.prazo_correcao<CURRENT_DATE) AS vencida
                        FROM fc_ocorrencias o JOIN fc_contratos c ON c.id=o.contrato_id
                        JOIN fc_empresas e ON e.id=c.empresa_id JOIN fc_servidores s ON s.id=o.servidor_responsavel_id
                        LEFT JOIN fc_ativos_contratuais a ON a.id=o.ativo_contratual_id
                        WHERE (%s='' OR o.titulo ILIKE %s OR o.descricao ILIKE %s OR c.numero_contrato ILIKE %s
                          OR e.razao_social ILIKE %s OR COALESCE(o.numero_notificacao,'') ILIKE %s
                          OR COALESCE(a.codigo_interno,'') ILIKE %s OR s.nome ILIKE %s)
                        AND (%s IS NULL OR o.contrato_id=%s) AND (%s IS NULL OR o.fiscalizacao_id=%s)
                        AND (%s='' OR o.categoria=%s) AND (%s='' OR o.gravidade=%s)
                        AND (%s='' OR o.status=%s) AND (%s IS NULL OR o.servidor_responsavel_id=%s)
                        AND (%s=FALSE OR (o.prazo_correcao<CURRENT_DATE AND o.status IN ('Aberta','Em acompanhamento') AND o.ativo))
                        AND (%s=FALSE OR o.exige_notificacao=TRUE)
                        AND (%s='todos' OR (%s='ativos' AND o.ativo) OR (%s='inativos' AND NOT o.ativo))
                        ORDER BY vencida DESC,o.data_identificacao DESC,o.id DESC""", (
                        padrao,padrao,padrao,padrao,padrao,padrao,padrao,padrao,
                        filtros.get("contrato_id"),filtros.get("contrato_id"),filtros.get("fiscalizacao_id"),filtros.get("fiscalizacao_id"),
                        filtros.get("categoria",""),filtros.get("categoria",""),filtros.get("gravidade",""),filtros.get("gravidade",""),
                        filtros.get("status",""),filtros.get("status",""),filtros.get("servidor_id"),filtros.get("servidor_id"),
                        filtros.get("vencidas",False),filtros.get("notificacao",False),status_ativo,status_ativo,status_ativo))
                    return cursor.fetchall()
        except psycopg2.Error as erro: raise OcorrenciaServiceError("Falha ao consultar ocorrências.") from erro

    def opcoes(self, contrato_id=None):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("SELECT id,numero_contrato FROM fc_contratos ORDER BY numero_contrato"); contratos=cursor.fetchall()
                    cursor.execute("SELECT id,nome,matricula FROM fc_servidores WHERE ativo ORDER BY nome"); servidores=cursor.fetchall()
                    cursor.execute("SELECT id,contrato_id,data_fiscalizacao,objeto_verificado FROM fc_fiscalizacoes WHERE ativo ORDER BY data_fiscalizacao DESC"); fiscalizacoes=cursor.fetchall()
                    cursor.execute("""SELECT DISTINCT a.id,a.codigo_interno,a.descricao,v.contrato_id FROM fc_ativos_contratuais a
                        JOIN fc_ativo_vinculos v ON v.ativo_id=a.id ORDER BY a.codigo_interno"""); ativos=cursor.fetchall()
                    return contratos,servidores,fiscalizacoes,ativos
        except psycopg2.Error as erro: raise OcorrenciaServiceError("Falha ao carregar opções.") from erro

    def obter(self, ocorrencia_id):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT o.*,c.numero_contrato,e.razao_social AS empresa_nome,s.nome AS servidor_nome,
                        f.data_fiscalizacao,a.codigo_interno AS ativo_codigo,a.descricao AS ativo_descricao,
                        (o.ativo AND o.status IN ('Aberta','Em acompanhamento') AND o.prazo_correcao<CURRENT_DATE) AS vencida
                        FROM fc_ocorrencias o JOIN fc_contratos c ON c.id=o.contrato_id
                        JOIN fc_empresas e ON e.id=c.empresa_id JOIN fc_servidores s ON s.id=o.servidor_responsavel_id
                        LEFT JOIN fc_fiscalizacoes f ON f.id=o.fiscalizacao_id
                        LEFT JOIN fc_ativos_contratuais a ON a.id=o.ativo_contratual_id WHERE o.id=%s""",(ocorrencia_id,))
                    item=cursor.fetchone()
                    if not item: raise OcorrenciaNaoEncontradaError("Ocorrência não encontrada.")
                    cursor.execute("SELECT * FROM fc_ocorrencia_acompanhamentos WHERE ocorrencia_id=%s ORDER BY data_acompanhamento,id",(ocorrencia_id,))
                    return item,cursor.fetchall()
        except OcorrenciaNaoEncontradaError: raise
        except psycopg2.Error as erro: raise OcorrenciaServiceError("Falha ao consultar ocorrência.") from erro

    def _validar_referencias(self,cursor,dados):
        cursor.execute("SELECT id FROM fc_contratos WHERE id=%s",(dados["contrato_id"],))
        if not cursor.fetchone(): raise ReferenciaOcorrenciaInvalidaError("Contrato não encontrado.")
        cursor.execute("SELECT ativo FROM fc_servidores WHERE id=%s",(dados["servidor_responsavel_id"],)); servidor=cursor.fetchone()
        if not servidor: raise ReferenciaOcorrenciaInvalidaError("Servidor responsável não encontrado.")
        if not servidor["ativo"]: raise ReferenciaOcorrenciaInvalidaError("O servidor responsável está inativo.")
        if dados.get("fiscalizacao_id"):
            cursor.execute("SELECT contrato_id FROM fc_fiscalizacoes WHERE id=%s",(dados["fiscalizacao_id"],)); fiscalizacao=cursor.fetchone()
            if not fiscalizacao or fiscalizacao["contrato_id"] != dados["contrato_id"]: raise ReferenciaOcorrenciaInvalidaError("A fiscalização não pertence ao contrato informado.")
        if dados.get("ativo_contratual_id"):
            cursor.execute("SELECT 1 FROM fc_ativo_vinculos WHERE ativo_id=%s AND contrato_id=%s",(dados["ativo_contratual_id"],dados["contrato_id"]))
            if not cursor.fetchone(): raise ReferenciaOcorrenciaInvalidaError("O ativo não possui vínculo atual ou histórico com este contrato.")

    def criar(self,dados,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                self._validar_referencias(cursor,dados)
                cursor.execute("""INSERT INTO fc_ocorrencias
                    (contrato_id,fiscalizacao_id,ativo_contratual_id,servidor_responsavel_id,titulo,categoria,
                     gravidade,descricao,data_identificacao,prazo_correcao,status,exige_notificacao,
                     numero_notificacao,data_regularizacao,conclusao,criado_por_usuario_id,atualizado_por_usuario_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Aberta',%s,%s,NULL,%s,%s,%s) RETURNING id""",
                    (dados["contrato_id"],dados.get("fiscalizacao_id"),dados.get("ativo_contratual_id"),dados["servidor_responsavel_id"],dados["titulo"],dados["categoria"],dados["gravidade"],dados["descricao"],dados["data_identificacao"],dados.get("prazo_correcao"),dados["exige_notificacao"],dados.get("numero_notificacao"),dados.get("conclusao"),usuario_id,usuario_id))
                novo=cursor.fetchone()["id"]
            conexao.commit(); return novo
        except (ReferenciaOcorrenciaInvalidaError,psycopg2.Error) as erro:
            conexao.rollback()
            if isinstance(erro,ReferenciaOcorrenciaInvalidaError): raise
            raise OcorrenciaServiceError("Falha ao cadastrar ocorrência.") from erro
        finally: conexao.close()

    def atualizar(self,ocorrencia_id,dados,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""SELECT o.ativo,COUNT(a.id) AS acompanhamentos FROM fc_ocorrencias o
                    LEFT JOIN fc_ocorrencia_acompanhamentos a ON a.ocorrencia_id=o.id
                    WHERE o.id=%s GROUP BY o.id""",(ocorrencia_id,)); atual=cursor.fetchone()
                if not atual: raise OcorrenciaNaoEncontradaError("Ocorrência não encontrada.")
                if not atual["ativo"] or atual["acompanhamentos"]: raise OcorrenciaBloqueadaError("Somente ocorrências ativas sem acompanhamento podem ser editadas.")
                self._validar_referencias(cursor,dados)
                cursor.execute("""UPDATE fc_ocorrencias SET contrato_id=%s,fiscalizacao_id=%s,ativo_contratual_id=%s,
                    servidor_responsavel_id=%s,titulo=%s,categoria=%s,gravidade=%s,descricao=%s,data_identificacao=%s,
                    prazo_correcao=%s,exige_notificacao=%s,numero_notificacao=%s,conclusao=%s,
                    atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s""",
                    (dados["contrato_id"],dados.get("fiscalizacao_id"),dados.get("ativo_contratual_id"),dados["servidor_responsavel_id"],dados["titulo"],dados["categoria"],dados["gravidade"],dados["descricao"],dados["data_identificacao"],dados.get("prazo_correcao"),dados["exige_notificacao"],dados.get("numero_notificacao"),dados.get("conclusao"),usuario_id,ocorrencia_id))
            conexao.commit()
        except (OcorrenciaNaoEncontradaError,OcorrenciaBloqueadaError,ReferenciaOcorrenciaInvalidaError,psycopg2.Error) as erro:
            conexao.rollback()
            if not isinstance(erro,psycopg2.Error): raise
            raise OcorrenciaServiceError("Falha ao atualizar ocorrência.") from erro
        finally: conexao.close()

    def alterar_ativo(self,ocorrencia_id,usuario_id,ativo):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor() as cursor:
                cursor.execute("UPDATE fc_ocorrencias SET ativo=%s,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s",(ativo,usuario_id,ocorrencia_id))
                if cursor.rowcount!=1: raise OcorrenciaNaoEncontradaError("Ocorrência não encontrada.")
            conexao.commit()
        except (OcorrenciaNaoEncontradaError,psycopg2.Error) as erro:
            conexao.rollback()
            if isinstance(erro,OcorrenciaNaoEncontradaError): raise
            raise OcorrenciaServiceError("Falha ao alterar ocorrência.") from erro
        finally: conexao.close()

    def adicionar_acompanhamento(self,ocorrencia_id,dados,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT * FROM fc_ocorrencias WHERE id=%s FOR UPDATE",(ocorrencia_id,)); atual=cursor.fetchone()
                if not atual: raise OcorrenciaNaoEncontradaError("Ocorrência não encontrada.")
                if not atual["ativo"]: raise OcorrenciaBloqueadaError("A ocorrência está inativa.")
                cursor.execute("""INSERT INTO fc_ocorrencia_acompanhamentos
                    (ocorrencia_id,data_acompanhamento,status_anterior,status_novo,descricao,
                     providencia_contratada,observacoes,criado_por_usuario_id)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",(ocorrencia_id,dados["data_acompanhamento"],atual["status"],dados["status_novo"],dados["descricao"],dados.get("providencia_contratada"),dados.get("observacoes"),usuario_id))
                data_reg = dados.get("data_regularizacao") if dados["status_novo"] in ("Regularizada","Não regularizada") else None
                cursor.execute("""UPDATE fc_ocorrencias SET status=%s,data_regularizacao=%s,
                    atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s""",
                    (dados["status_novo"],data_reg,usuario_id,ocorrencia_id))
            conexao.commit()
        except (OcorrenciaNaoEncontradaError,OcorrenciaBloqueadaError,psycopg2.Error) as erro:
            conexao.rollback()
            if not isinstance(erro,psycopg2.Error): raise
            raise OcorrenciaServiceError("Falha ao registrar acompanhamento.") from erro
        finally: conexao.close()

    def listar_do_contrato(self,contrato_id): return self.listar("",{"contrato_id":contrato_id,"status_ativo":"todos"})

    def listar_do_ativo(self,ativo_id):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT o.*,c.numero_contrato,(o.ativo AND o.status IN ('Aberta','Em acompanhamento')
                        AND o.prazo_correcao<CURRENT_DATE) AS vencida FROM fc_ocorrencias o
                        JOIN fc_contratos c ON c.id=o.contrato_id WHERE o.ativo_contratual_id=%s
                        ORDER BY o.data_identificacao DESC,o.id DESC""",(ativo_id,)); return cursor.fetchall()
        except psycopg2.Error as erro: raise OcorrenciaServiceError("Falha ao consultar ocorrências do ativo.") from erro
