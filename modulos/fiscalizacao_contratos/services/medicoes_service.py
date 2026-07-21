"""Persistência transacional das medições contratuais."""

from decimal import Decimal, ROUND_HALF_UP

import psycopg2
from psycopg2.extras import RealDictCursor

from ..validacoes_medicoes import (
    CATEGORIAS_DOCUMENTO_MEDICAO,
    CENTAVOS,
    TIPOS_AJUSTE,
    calcular_valor_item,
)


class MedicaoServiceError(Exception): pass
class MedicaoNaoEncontradaError(MedicaoServiceError): pass
class MedicaoBloqueadaError(MedicaoServiceError): pass
class ReferenciaMedicaoInvalidaError(MedicaoServiceError): pass
class MedicaoDuplicadaError(MedicaoServiceError): pass


EDITAVEIS = ("Em elaboração", "Devolvida para correção")


class MedicaoService:
    def __init__(self, conectar_banco):
        self._conectar_banco = conectar_banco

    def listar(self, busca="", filtros=None):
        filtros = filtros or {}
        padrao = f"%{(busca or '').strip()}%"
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT m.*,c.numero_contrato,c.processo_administrativo,
                        e.razao_social AS empresa_nome,s.nome AS fiscal_nome,
                        (m.total_acrescimos-m.total_descontos-m.total_glosas) AS total_ajustes
                        FROM fc_medicoes m JOIN fc_contratos c ON c.id=m.contrato_id
                        JOIN fc_empresas e ON e.id=c.empresa_id
                        JOIN fc_servidores s ON s.id=m.servidor_fiscal_id
                        WHERE (%s='' OR CAST(m.numero_medicao AS TEXT) ILIKE %s
                          OR c.numero_contrato ILIKE %s OR COALESCE(c.processo_administrativo,'') ILIKE %s
                          OR e.razao_social ILIKE %s OR s.nome ILIKE %s
                          OR COALESCE(m.observacoes,'') ILIKE %s)
                        AND (%s IS NULL OR m.contrato_id=%s)
                        AND (%s IS NULL OR c.empresa_id=%s)
                        AND (%s IS NULL OR m.competencia=%s)
                        AND (%s IS NULL OR m.periodo_inicio>=%s)
                        AND (%s IS NULL OR m.periodo_fim<=%s)
                        AND (%s IS NULL OR m.servidor_fiscal_id=%s)
                        AND (%s='' OR m.status=%s)
                        AND (%s='todas' OR (%s='atuais' AND m.atual) OR (%s='historicas' AND NOT m.atual))
                        AND (%s='todos' OR (%s='ativos' AND m.ativo) OR (%s='inativos' AND NOT m.ativo))
                        AND (%s=FALSE OR m.total_glosas>0) AND (%s=FALSE OR m.total_descontos>0)
                        ORDER BY m.competencia DESC,m.numero_medicao DESC,m.versao DESC""",(
                        padrao,padrao,padrao,padrao,padrao,padrao,padrao,
                        filtros.get("contrato_id"),filtros.get("contrato_id"),filtros.get("empresa_id"),filtros.get("empresa_id"),
                        filtros.get("competencia"),filtros.get("competencia"),filtros.get("periodo_inicio"),filtros.get("periodo_inicio"),
                        filtros.get("periodo_fim"),filtros.get("periodo_fim"),filtros.get("servidor_id"),filtros.get("servidor_id"),
                        filtros.get("status",""),filtros.get("status",""),filtros.get("versoes","atuais"),filtros.get("versoes","atuais"),
                        filtros.get("versoes","atuais"),filtros.get("status_ativo","ativos"),filtros.get("status_ativo","ativos"),
                        filtros.get("status_ativo","ativos"),filtros.get("com_glosa",False),filtros.get("com_desconto",False)))
                    return cursor.fetchall()
        except psycopg2.Error as erro:
            raise MedicaoServiceError("Falha ao consultar medições.") from erro

    def opcoes(self):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT c.id,c.numero_contrato,c.empresa_id,e.razao_social AS empresa_nome
                        FROM fc_contratos c JOIN fc_empresas e ON e.id=c.empresa_id
                        WHERE c.ativo=TRUE ORDER BY c.numero_contrato""")
                    contratos=cursor.fetchall()
                    cursor.execute("SELECT id,nome,matricula FROM fc_servidores WHERE ativo=TRUE ORDER BY nome")
                    servidores=cursor.fetchall()
                    cursor.execute("SELECT id,razao_social FROM fc_empresas WHERE ativo=TRUE ORDER BY razao_social")
                    return contratos,servidores,cursor.fetchall()
        except psycopg2.Error as erro:
            raise MedicaoServiceError("Falha ao carregar opções de medição.") from erro

    def opcoes_relacionamentos(self, contrato_id):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT pi.id,pi.codigo_item,pi.descricao,pi.unidade,pi.quantidade,
                        pi.valor_unitario,p.nome AS planilha_nome,p.versao
                        FROM fc_planilha_itens pi JOIN fc_planilhas_orcamentarias p ON p.id=pi.planilha_id
                        WHERE p.contrato_id=%s AND p.ativo AND pi.ativo
                        ORDER BY p.vigente DESC,p.versao DESC,pi.ordem,pi.id""",(contrato_id,))
                    itens=cursor.fetchall()
                    cursor.execute("SELECT id,data_fiscalizacao,objeto_verificado FROM fc_fiscalizacoes WHERE contrato_id=%s AND ativo ORDER BY data_fiscalizacao DESC",(contrato_id,))
                    fiscalizacoes=cursor.fetchall()
                    cursor.execute("SELECT id,titulo,status FROM fc_ocorrencias WHERE contrato_id=%s AND ativo ORDER BY data_identificacao DESC",(contrato_id,))
                    ocorrencias=cursor.fetchall()
                    cursor.execute("SELECT id,titulo,nome_original,categoria FROM fc_documentos WHERE contrato_id=%s AND ativo ORDER BY criado_em DESC",(contrato_id,))
                    return itens,fiscalizacoes,ocorrencias,cursor.fetchall()
        except psycopg2.Error as erro:
            raise MedicaoServiceError("Falha ao carregar relacionamentos da medição.") from erro

    def obter(self, medicao_id):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT m.*,c.numero_contrato,c.processo_administrativo,
                        e.razao_social AS empresa_nome,s.nome AS fiscal_nome,ap.nome AS aprovador_nome
                        FROM fc_medicoes m JOIN fc_contratos c ON c.id=m.contrato_id
                        JOIN fc_empresas e ON e.id=c.empresa_id
                        JOIN fc_servidores s ON s.id=m.servidor_fiscal_id
                        LEFT JOIN fc_servidores ap ON ap.id=m.servidor_aprovador_id WHERE m.id=%s""",(medicao_id,))
                    medicao=cursor.fetchone()
                    if not medicao: raise MedicaoNaoEncontradaError("Medição não encontrada.")
                    cursor.execute("SELECT * FROM fc_medicao_itens WHERE medicao_id=%s ORDER BY ativo DESC,ordem,id",(medicao_id,));itens=cursor.fetchall()
                    cursor.execute("SELECT * FROM fc_medicao_ajustes WHERE medicao_id=%s ORDER BY ativo DESC,tipo_ajuste,id",(medicao_id,));ajustes=cursor.fetchall()
                    cursor.execute("""SELECT md.*,d.titulo,d.nome_original,d.categoria AS documento_categoria
                        FROM fc_medicao_documentos md JOIN fc_documentos d ON d.id=md.documento_id
                        WHERE md.medicao_id=%s ORDER BY md.ativo DESC,md.criado_em DESC""",(medicao_id,));documentos=cursor.fetchall()
                    cursor.execute("""SELECT ev.*,u.username AS usuario_nome FROM fc_medicao_eventos ev
                        JOIN usuarios u ON u.id=ev.criado_por_usuario_id WHERE ev.medicao_id=%s
                        ORDER BY ev.criado_em DESC,ev.id DESC""",(medicao_id,));eventos=cursor.fetchall()
                    cursor.execute("""SELECT id,versao,status,atual,ativo,valor_liquido,medicao_origem_id
                        FROM fc_medicoes WHERE contrato_id=%s AND numero_medicao=%s ORDER BY versao DESC""",
                        (medicao["contrato_id"],medicao["numero_medicao"]));versoes=cursor.fetchall()
                    return medicao,itens,ajustes,documentos,eventos,versoes
        except MedicaoNaoEncontradaError: raise
        except psycopg2.Error as erro:
            raise MedicaoServiceError("Falha ao consultar medição.") from erro

    def _obter_bloqueada(self,cursor,medicao_id):
        cursor.execute("SELECT * FROM fc_medicoes WHERE id=%s FOR UPDATE",(medicao_id,))
        medicao=cursor.fetchone()
        if not medicao: raise MedicaoNaoEncontradaError("Medição não encontrada.")
        return medicao

    @staticmethod
    def _exigir_editavel(medicao):
        if not medicao["ativo"] or not medicao["atual"] or medicao["status"] not in EDITAVEIS:
            raise MedicaoBloqueadaError("Esta versão da medição não pode mais ser alterada.")

    @staticmethod
    def _validar_referencias(cursor,dados):
        cursor.execute("SELECT ativo FROM fc_contratos WHERE id=%s",(dados["contrato_id"],));contrato=cursor.fetchone()
        if not contrato: raise ReferenciaMedicaoInvalidaError("Contrato não encontrado.")
        if not contrato["ativo"]: raise ReferenciaMedicaoInvalidaError("O contrato está inativo.")
        cursor.execute("SELECT ativo FROM fc_servidores WHERE id=%s",(dados["servidor_fiscal_id"],));servidor=cursor.fetchone()
        if not servidor: raise ReferenciaMedicaoInvalidaError("Fiscal responsável não encontrado.")
        if not servidor["ativo"]: raise ReferenciaMedicaoInvalidaError("O fiscal responsável está inativo.")

    @staticmethod
    def _evento(cursor,medicao_id,tipo,status_anterior,status_novo,usuario_id,justificativa=None):
        cursor.execute("""INSERT INTO fc_medicao_eventos
            (medicao_id,tipo_evento,status_anterior,status_novo,justificativa,
             valor_bruto,total_acrescimos,total_descontos,total_glosas,valor_liquido,criado_por_usuario_id)
            SELECT id,%s,%s,%s,%s,valor_bruto,total_acrescimos,total_descontos,total_glosas,
                   valor_liquido,%s FROM fc_medicoes WHERE id=%s""",
            (tipo,status_anterior,status_novo,justificativa,usuario_id,medicao_id))

    @staticmethod
    def _recalcular(cursor,medicao_id,usuario_id):
        cursor.execute("""SELECT
            COALESCE((SELECT SUM(valor_medido) FROM fc_medicao_itens WHERE medicao_id=%s AND ativo),0) AS bruto,
            COALESCE((SELECT SUM(valor) FROM fc_medicao_ajustes WHERE medicao_id=%s AND ativo AND tipo_ajuste='Acréscimo'),0) AS acrescimos,
            COALESCE((SELECT SUM(valor) FROM fc_medicao_ajustes WHERE medicao_id=%s AND ativo AND tipo_ajuste='Desconto'),0) AS descontos,
            COALESCE((SELECT SUM(valor) FROM fc_medicao_ajustes WHERE medicao_id=%s AND ativo AND tipo_ajuste='Glosa'),0) AS glosas""",
            (medicao_id,medicao_id,medicao_id,medicao_id))
        totais=cursor.fetchone()
        valores=[Decimal(str(totais[chave])).quantize(CENTAVOS,rounding=ROUND_HALF_UP) for chave in ("bruto","acrescimos","descontos","glosas")]
        liquido=(valores[0]+valores[1]-valores[2]-valores[3]).quantize(CENTAVOS,rounding=ROUND_HALF_UP)
        if liquido < 0: raise MedicaoBloqueadaError("O valor líquido da medição não pode ser negativo.")
        cursor.execute("""UPDATE fc_medicoes SET valor_bruto=%s,total_acrescimos=%s,total_descontos=%s,
            total_glosas=%s,valor_liquido=%s,atualizado_em=CURRENT_TIMESTAMP,
            atualizado_por_usuario_id=%s WHERE id=%s""",(*valores,liquido,usuario_id,medicao_id))
        return (*valores,liquido)

    def criar(self,dados,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                self._validar_referencias(cursor,dados)
                cursor.execute("""INSERT INTO fc_medicoes
                    (contrato_id,numero_medicao,competencia,periodo_inicio,periodo_fim,versao,atual,
                     servidor_fiscal_id,data_apresentacao,status,valor_bruto,total_acrescimos,
                     total_descontos,total_glosas,valor_liquido,observacoes,criado_por_usuario_id,
                     atualizado_por_usuario_id)
                    VALUES (%s,%s,%s,%s,%s,1,TRUE,%s,%s,'Em elaboração',0,0,0,0,0,%s,%s,%s)
                    RETURNING id""",(dados["contrato_id"],dados["numero_medicao"],dados["competencia"],
                    dados["periodo_inicio"],dados["periodo_fim"],dados["servidor_fiscal_id"],
                    dados.get("data_apresentacao"),dados.get("observacoes"),usuario_id,usuario_id))
                novo=cursor.fetchone()["id"]
                self._evento(cursor,novo,"Criação",None,"Em elaboração",usuario_id)
            conexao.commit();return novo
        except (ReferenciaMedicaoInvalidaError,psycopg2.IntegrityError,psycopg2.Error) as erro:
            conexao.rollback()
            if isinstance(erro,ReferenciaMedicaoInvalidaError): raise
            if isinstance(erro,psycopg2.IntegrityError): raise MedicaoDuplicadaError("Já existe medição atual ou numeração incompatível para este contrato.") from erro
            raise MedicaoServiceError("Falha ao cadastrar medição.") from erro
        finally: conexao.close()

    def atualizar(self,medicao_id,dados,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                atual=self._obter_bloqueada(cursor,medicao_id);self._exigir_editavel(atual);self._validar_referencias(cursor,dados)
                cursor.execute("""UPDATE fc_medicoes SET contrato_id=%s,numero_medicao=%s,competencia=%s,
                    periodo_inicio=%s,periodo_fim=%s,servidor_fiscal_id=%s,data_apresentacao=%s,
                    observacoes=%s,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s""",
                    (dados["contrato_id"],dados["numero_medicao"],dados["competencia"],dados["periodo_inicio"],
                    dados["periodo_fim"],dados["servidor_fiscal_id"],dados.get("data_apresentacao"),
                    dados.get("observacoes"),usuario_id,medicao_id))
            conexao.commit()
        except (MedicaoNaoEncontradaError,MedicaoBloqueadaError,ReferenciaMedicaoInvalidaError,psycopg2.Error) as erro:
            conexao.rollback()
            if not isinstance(erro,psycopg2.Error): raise
            if isinstance(erro,psycopg2.IntegrityError):
                raise MedicaoDuplicadaError(
                    "Já existe medição atual ou numeração incompatível para este contrato."
                ) from erro
            raise MedicaoServiceError("Falha ao atualizar medição.") from erro
        finally: conexao.close()

    def _dados_item(self,cursor,medicao,dados,item_existente=None):
        if item_existente and item_existente.get("planilha_item_id"):
            dados={**dados,
                "planilha_item_id":item_existente["planilha_item_id"],
                "codigo_item":item_existente["codigo_item"],
                "descricao":item_existente["descricao"],
                "unidade":item_existente["unidade"],
                "quantidade_prevista":item_existente["quantidade_prevista"],
                "preco_unitario":item_existente["preco_unitario"]}
        elif dados.get("planilha_item_id"):
            cursor.execute("""SELECT pi.codigo_item,pi.descricao,pi.unidade,pi.quantidade,pi.valor_unitario
                FROM fc_planilha_itens pi JOIN fc_planilhas_orcamentarias p ON p.id=pi.planilha_id
                WHERE pi.id=%s AND pi.ativo AND p.ativo AND p.contrato_id=%s""",
                (dados["planilha_item_id"],medicao["contrato_id"]))
            origem=cursor.fetchone()
            if not origem: raise ReferenciaMedicaoInvalidaError("O item da planilha não pertence ao contrato da medição.")
            dados={**dados,"codigo_item":origem["codigo_item"],"descricao":origem["descricao"],
                "unidade":origem["unidade"],"quantidade_prevista":origem["quantidade"],"preco_unitario":origem["valor_unitario"]}
        if not dados.get("descricao") or not dados.get("unidade"):
            raise ReferenciaMedicaoInvalidaError("Descrição e unidade são obrigatórias.")
        if dados["quantidade_medida"] < 0 or dados["preco_unitario"] < 0:
            raise ReferenciaMedicaoInvalidaError("Quantidade e preço não podem ser negativos.")
        if dados.get("quantidade_prevista") is not None and dados["quantidade_medida"] > dados["quantidade_prevista"] and not dados.get("justificativa_excedente"):
            raise ReferenciaMedicaoInvalidaError("Justifique a quantidade acima da prevista.")
        return dados,calcular_valor_item(dados["quantidade_medida"],dados["preco_unitario"])

    def criar_item(self,medicao_id,dados,usuario_id):
        return self._salvar_item(medicao_id,None,dados,usuario_id)

    def atualizar_item(self,medicao_id,item_id,dados,usuario_id):
        return self._salvar_item(medicao_id,item_id,dados,usuario_id)

    def _salvar_item(self,medicao_id,item_id,dados,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                medicao=self._obter_bloqueada(cursor,medicao_id);self._exigir_editavel(medicao)
                item_existente=None
                if item_id:
                    cursor.execute("SELECT * FROM fc_medicao_itens WHERE id=%s AND medicao_id=%s AND ativo FOR UPDATE",(item_id,medicao_id))
                    item_existente=cursor.fetchone()
                    if not item_existente: raise MedicaoNaoEncontradaError("Item não encontrado.")
                dados,valor=self._dados_item(cursor,medicao,dados,item_existente)
                if item_id:
                    cursor.execute("""UPDATE fc_medicao_itens SET planilha_item_id=%s,ordem=%s,codigo_item=%s,
                        descricao=%s,unidade=%s,quantidade_prevista=%s,quantidade_medida=%s,preco_unitario=%s,
                        valor_medido=%s,justificativa_excedente=%s,observacoes=%s,atualizado_em=CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id=%s WHERE id=%s AND medicao_id=%s AND ativo""",
                        (dados.get("planilha_item_id"),dados["ordem"],dados.get("codigo_item"),dados["descricao"],dados["unidade"],
                        dados.get("quantidade_prevista"),dados["quantidade_medida"],dados["preco_unitario"],valor,
                        dados.get("justificativa_excedente"),dados.get("observacoes"),usuario_id,item_id,medicao_id))
                else:
                    cursor.execute("""INSERT INTO fc_medicao_itens
                        (medicao_id,planilha_item_id,ordem,codigo_item,descricao,unidade,quantidade_prevista,
                         quantidade_medida,preco_unitario,valor_medido,justificativa_excedente,observacoes,
                         criado_por_usuario_id,atualizado_por_usuario_id)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (medicao_id,dados.get("planilha_item_id"),dados["ordem"],dados.get("codigo_item"),dados["descricao"],
                        dados["unidade"],dados.get("quantidade_prevista"),dados["quantidade_medida"],dados["preco_unitario"],
                        valor,dados.get("justificativa_excedente"),dados.get("observacoes"),usuario_id,usuario_id))
                self._recalcular(cursor,medicao_id,usuario_id)
            conexao.commit();return valor
        except (MedicaoNaoEncontradaError,MedicaoBloqueadaError,ReferenciaMedicaoInvalidaError,psycopg2.IntegrityError,psycopg2.Error) as erro:
            conexao.rollback()
            if isinstance(erro,(MedicaoNaoEncontradaError,MedicaoBloqueadaError,ReferenciaMedicaoInvalidaError)): raise
            if isinstance(erro,psycopg2.IntegrityError): raise MedicaoDuplicadaError("Este item da planilha já está ativo na medição.") from erro
            raise MedicaoServiceError("Falha ao salvar item da medição.") from erro
        finally: conexao.close()

    def inativar_item(self,medicao_id,item_id,usuario_id):
        self._inativar_e_recalcular("fc_medicao_itens",medicao_id,item_id,usuario_id)

    def _validar_ajuste(self,cursor,medicao,dados):
        if dados.get("tipo_ajuste") not in TIPOS_AJUSTE:
            raise ReferenciaMedicaoInvalidaError("Selecione um tipo de ajuste válido.")
        if not isinstance(dados.get("valor"),Decimal) or dados["valor"]<=0:
            raise ReferenciaMedicaoInvalidaError("O valor do ajuste deve ser maior que zero.")
        if dados.get("fiscalizacao_id"):
            cursor.execute("SELECT contrato_id FROM fc_fiscalizacoes WHERE id=%s", (dados["fiscalizacao_id"],))
            fiscalizacao = cursor.fetchone()
            if not fiscalizacao or fiscalizacao["contrato_id"] != medicao["contrato_id"]:
                raise ReferenciaMedicaoInvalidaError("A fiscalização não pertence ao contrato da medição.")
        if dados.get("ocorrencia_id"):
            cursor.execute("SELECT contrato_id FROM fc_ocorrencias WHERE id=%s", (dados["ocorrencia_id"],))
            ocorrencia = cursor.fetchone()
            if not ocorrencia or ocorrencia["contrato_id"] != medicao["contrato_id"]:
                raise ReferenciaMedicaoInvalidaError("A ocorrência não pertence ao contrato da medição.")

    def criar_ajuste(self,medicao_id,dados,usuario_id): return self._salvar_ajuste(medicao_id,None,dados,usuario_id)
    def atualizar_ajuste(self,medicao_id,ajuste_id,dados,usuario_id): return self._salvar_ajuste(medicao_id,ajuste_id,dados,usuario_id)

    def _salvar_ajuste(self,medicao_id,ajuste_id,dados,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                medicao=self._obter_bloqueada(cursor,medicao_id);self._exigir_editavel(medicao);self._validar_ajuste(cursor,medicao,dados)
                if ajuste_id:
                    cursor.execute("""UPDATE fc_medicao_ajustes SET tipo_ajuste=%s,descricao=%s,valor=%s,
                        fiscalizacao_id=%s,ocorrencia_id=%s,observacoes=%s,atualizado_em=CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id=%s WHERE id=%s AND medicao_id=%s AND ativo""",
                        (dados["tipo_ajuste"],dados["descricao"],dados["valor"],dados.get("fiscalizacao_id"),
                        dados.get("ocorrencia_id"),dados.get("observacoes"),usuario_id,ajuste_id,medicao_id))
                    if cursor.rowcount!=1: raise MedicaoNaoEncontradaError("Ajuste não encontrado.")
                else:
                    cursor.execute("""INSERT INTO fc_medicao_ajustes
                        (medicao_id,tipo_ajuste,descricao,valor,fiscalizacao_id,ocorrencia_id,observacoes,
                         criado_por_usuario_id,atualizado_por_usuario_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (medicao_id,dados["tipo_ajuste"],dados["descricao"],dados["valor"],dados.get("fiscalizacao_id"),
                        dados.get("ocorrencia_id"),dados.get("observacoes"),usuario_id,usuario_id))
                self._recalcular(cursor,medicao_id,usuario_id)
            conexao.commit()
        except (MedicaoNaoEncontradaError,MedicaoBloqueadaError,ReferenciaMedicaoInvalidaError,psycopg2.Error) as erro:
            conexao.rollback()
            if not isinstance(erro,psycopg2.Error): raise
            raise MedicaoServiceError("Falha ao salvar ajuste da medição.") from erro
        finally: conexao.close()

    def inativar_ajuste(self,medicao_id,ajuste_id,usuario_id):
        self._inativar_e_recalcular("fc_medicao_ajustes",medicao_id,ajuste_id,usuario_id)

    def _inativar_e_recalcular(self,tabela,medicao_id,registro_id,usuario_id):
        if tabela not in ("fc_medicao_itens","fc_medicao_ajustes"): raise MedicaoServiceError("Operação inválida.")
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                medicao=self._obter_bloqueada(cursor,medicao_id);self._exigir_editavel(medicao)
                consulta = "UPDATE fc_medicao_itens SET ativo=FALSE,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s AND medicao_id=%s AND ativo" if tabela=="fc_medicao_itens" else "UPDATE fc_medicao_ajustes SET ativo=FALSE,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s AND medicao_id=%s AND ativo"
                cursor.execute(consulta,(usuario_id,registro_id,medicao_id))
                if cursor.rowcount!=1: raise MedicaoNaoEncontradaError("Registro não encontrado.")
                self._recalcular(cursor,medicao_id,usuario_id)
            conexao.commit()
        except (MedicaoNaoEncontradaError,MedicaoBloqueadaError,psycopg2.Error) as erro:
            conexao.rollback()
            if not isinstance(erro,psycopg2.Error): raise
            raise MedicaoServiceError("Falha ao inativar registro da medição.") from erro
        finally: conexao.close()

    def vincular_documento(self,medicao_id,documento_id,categoria,observacoes,usuario_id):
        if categoria not in CATEGORIAS_DOCUMENTO_MEDICAO:
            raise ReferenciaMedicaoInvalidaError("Selecione uma categoria de documento válida.")
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                medicao=self._obter_bloqueada(cursor,medicao_id);self._exigir_editavel(medicao)
                cursor.execute("SELECT contrato_id FROM fc_documentos WHERE id=%s AND ativo",(documento_id,));documento=cursor.fetchone()
                if not documento or documento["contrato_id"]!=medicao["contrato_id"]: raise ReferenciaMedicaoInvalidaError("O documento não pertence ao contrato da medição.")
                cursor.execute("""INSERT INTO fc_medicao_documentos
                    (medicao_id,documento_id,categoria,observacoes,criado_por_usuario_id,atualizado_por_usuario_id)
                    VALUES (%s,%s,%s,%s,%s,%s)""",(medicao_id,documento_id,categoria,observacoes,usuario_id,usuario_id))
            conexao.commit()
        except (MedicaoBloqueadaError,ReferenciaMedicaoInvalidaError,psycopg2.IntegrityError,psycopg2.Error) as erro:
            conexao.rollback()
            if isinstance(erro,(MedicaoBloqueadaError,ReferenciaMedicaoInvalidaError)): raise
            if isinstance(erro,psycopg2.IntegrityError): raise MedicaoDuplicadaError("Este documento já está vinculado à medição.") from erro
            raise MedicaoServiceError("Falha ao vincular documento.") from erro
        finally: conexao.close()

    def inativar_documento(self,medicao_id,vinculo_id,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                medicao=self._obter_bloqueada(cursor,medicao_id);self._exigir_editavel(medicao)
                cursor.execute("""UPDATE fc_medicao_documentos SET ativo=FALSE,atualizado_em=CURRENT_TIMESTAMP,
                    atualizado_por_usuario_id=%s WHERE id=%s AND medicao_id=%s AND ativo""",(usuario_id,vinculo_id,medicao_id))
                if cursor.rowcount!=1: raise MedicaoNaoEncontradaError("Vínculo de documento não encontrado.")
            conexao.commit()
        except (MedicaoNaoEncontradaError,MedicaoBloqueadaError,psycopg2.Error) as erro:
            conexao.rollback()
            if not isinstance(erro,psycopg2.Error): raise
            raise MedicaoServiceError("Falha ao inativar vínculo de documento.") from erro
        finally: conexao.close()

    def enviar_analise(self,medicao_id,usuario_id): self._fluxo(medicao_id,"enviar",usuario_id)
    def devolver_correcao(self,medicao_id,justificativa,usuario_id): self._fluxo(medicao_id,"devolver",usuario_id,justificativa=justificativa)
    def aprovar(self,medicao_id,aprovador_id,usuario_id): self._fluxo(medicao_id,"aprovar",usuario_id,aprovador_id=aprovador_id)
    def cancelar(self,medicao_id,justificativa,usuario_id): self._fluxo(medicao_id,"cancelar",usuario_id,justificativa=justificativa)

    def _fluxo(self,medicao_id,acao,usuario_id,justificativa=None,aprovador_id=None):
        justificativa=str(justificativa or "").strip() or None
        if acao in ("devolver","cancelar") and not justificativa: raise MedicaoBloqueadaError("Informe a justificativa.")
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                medicao=self._obter_bloqueada(cursor,medicao_id)
                if not medicao["ativo"] or not medicao["atual"]: raise MedicaoBloqueadaError("Somente a versão atual e ativa pode mudar de status.")
                mapa={"enviar":(EDITAVEIS,"Em análise","Envio para análise"),"devolver":(("Em análise",),"Devolvida para correção","Devolução para correção"),"aprovar":(("Em análise",),"Aprovada","Aprovação"),"cancelar":(("Em elaboração","Em análise","Devolvida para correção"),"Cancelada","Cancelamento")}
                permitidos,novo_status,tipo=mapa[acao]
                if medicao["status"] not in permitidos: raise MedicaoBloqueadaError("A situação atual não permite esta ação.")
                if acao in ("enviar","aprovar"):
                    cursor.execute("SELECT COUNT(*) AS quantidade FROM fc_medicao_itens WHERE medicao_id=%s AND ativo",(medicao_id,))
                    if cursor.fetchone()["quantidade"]<1: raise MedicaoBloqueadaError("Inclua pelo menos um item ativo.")
                self._recalcular(cursor,medicao_id,usuario_id)
                if acao=="enviar":
                    cursor.execute("SELECT ativo FROM fc_servidores WHERE id=%s",(medicao["servidor_fiscal_id"],));fiscal=cursor.fetchone()
                    if not fiscal or not fiscal["ativo"]: raise MedicaoBloqueadaError("O fiscal responsável está inativo.")
                if acao=="aprovar":
                    cursor.execute("SELECT ativo FROM fc_servidores WHERE id=%s",(aprovador_id,));aprovador=cursor.fetchone()
                    if not aprovador or not aprovador["ativo"]: raise ReferenciaMedicaoInvalidaError("Selecione um aprovador ativo.")
                    cursor.execute("""UPDATE fc_medicoes SET status='Aprovada',aprovado_em=CURRENT_TIMESTAMP,
                        servidor_aprovador_id=%s,aprovado_por_usuario_id=%s,atualizado_em=CURRENT_TIMESTAMP,
                        atualizado_por_usuario_id=%s WHERE id=%s""",(aprovador_id,usuario_id,usuario_id,medicao_id))
                elif acao=="cancelar":
                    cursor.execute("""UPDATE fc_medicoes SET status=%s,atual=FALSE,aprovado_em=NULL,
                        servidor_aprovador_id=NULL,aprovado_por_usuario_id=NULL,
                        atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s""",
                        (novo_status,usuario_id,medicao_id))
                else:
                    cursor.execute("""UPDATE fc_medicoes SET status=%s,aprovado_em=NULL,servidor_aprovador_id=NULL,
                        aprovado_por_usuario_id=NULL,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s""",
                        (novo_status,usuario_id,medicao_id))
                self._evento(cursor,medicao_id,tipo,medicao["status"],novo_status,usuario_id,justificativa)
            conexao.commit()
        except (MedicaoNaoEncontradaError,MedicaoBloqueadaError,ReferenciaMedicaoInvalidaError,psycopg2.Error) as erro:
            conexao.rollback()
            if not isinstance(erro,psycopg2.Error): raise
            raise MedicaoServiceError("Falha ao alterar situação da medição.") from erro
        finally: conexao.close()

    def criar_revisao(self,medicao_id,justificativa,usuario_id):
        justificativa=str(justificativa or "").strip()
        if not justificativa: raise MedicaoBloqueadaError("Informe a justificativa da revisão.")
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                origem=self._obter_bloqueada(cursor,medicao_id)
                if origem["status"]!="Aprovada" or not origem["ativo"] or not origem["atual"]: raise MedicaoBloqueadaError("Somente uma medição aprovada, ativa e atual pode gerar revisão.")
                cursor.execute("UPDATE fc_medicoes SET atual=FALSE,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s",(usuario_id,medicao_id))
                cursor.execute("""INSERT INTO fc_medicoes
                    (contrato_id,numero_medicao,competencia,periodo_inicio,periodo_fim,versao,medicao_origem_id,
                     atual,servidor_fiscal_id,data_apresentacao,status,valor_bruto,total_acrescimos,total_descontos,
                     total_glosas,valor_liquido,observacoes,criado_por_usuario_id,atualizado_por_usuario_id)
                    SELECT contrato_id,numero_medicao,competencia,periodo_inicio,periodo_fim,versao+1,id,TRUE,
                     servidor_fiscal_id,data_apresentacao,'Em elaboração',0,0,0,0,0,observacoes,%s,%s
                    FROM fc_medicoes WHERE id=%s RETURNING id""",(usuario_id,usuario_id,medicao_id))
                nova=cursor.fetchone()["id"]
                cursor.execute("""INSERT INTO fc_medicao_itens
                    (medicao_id,planilha_item_id,ordem,codigo_item,descricao,unidade,quantidade_prevista,
                     quantidade_medida,preco_unitario,valor_medido,justificativa_excedente,observacoes,
                     criado_por_usuario_id,atualizado_por_usuario_id)
                    SELECT %s,planilha_item_id,ordem,codigo_item,descricao,unidade,quantidade_prevista,
                     quantidade_medida,preco_unitario,valor_medido,justificativa_excedente,observacoes,%s,%s
                    FROM fc_medicao_itens WHERE medicao_id=%s AND ativo""",(nova,usuario_id,usuario_id,medicao_id))
                cursor.execute("""INSERT INTO fc_medicao_ajustes
                    (medicao_id,tipo_ajuste,descricao,valor,fiscalizacao_id,ocorrencia_id,observacoes,
                     criado_por_usuario_id,atualizado_por_usuario_id)
                    SELECT %s,tipo_ajuste,descricao,valor,fiscalizacao_id,ocorrencia_id,observacoes,%s,%s
                    FROM fc_medicao_ajustes WHERE medicao_id=%s AND ativo""",(nova,usuario_id,usuario_id,medicao_id))
                cursor.execute("""INSERT INTO fc_medicao_documentos
                    (medicao_id,documento_id,categoria,observacoes,criado_por_usuario_id,atualizado_por_usuario_id)
                    SELECT %s,documento_id,categoria,observacoes,%s,%s FROM fc_medicao_documentos
                    WHERE medicao_id=%s AND ativo""",(nova,usuario_id,usuario_id,medicao_id))
                self._recalcular(cursor,nova,usuario_id)
                self._evento(cursor,medicao_id,"Substituição por revisão","Aprovada","Aprovada",usuario_id,justificativa)
                self._evento(cursor,nova,"Revisão criada","Aprovada","Em elaboração",usuario_id,justificativa)
            conexao.commit();return nova
        except (MedicaoNaoEncontradaError,MedicaoBloqueadaError,psycopg2.Error) as erro:
            conexao.rollback()
            if not isinstance(erro,psycopg2.Error): raise
            if isinstance(erro,psycopg2.IntegrityError):
                raise MedicaoDuplicadaError(
                    "Já existe uma revisão posterior ou outra versão atual para esta competência."
                ) from erro
            raise MedicaoServiceError("Falha ao criar revisão da medição.") from erro
        finally: conexao.close()

    def listar_do_contrato(self,contrato_id,limite=10):
        itens=self.listar("",{"contrato_id":contrato_id,"versoes":"todas","status_ativo":"todos"})
        atuais=[m for m in itens if m["atual"] and m["ativo"]]
        resumo={"total":len(itens),"elaboracao":sum(m["status"]=="Em elaboração" for m in atuais),
            "analise":sum(m["status"]=="Em análise" for m in atuais),"devolvidas":sum(m["status"]=="Devolvida para correção" for m in atuais),
            "aprovadas":sum(m["status"]=="Aprovada" for m in itens),
            "valor_aprovado":sum((Decimal(str(m["valor_liquido"])) for m in itens if m["status"]=="Aprovada"),Decimal("0")),
            "total_glosas":sum((Decimal(str(m["total_glosas"])) for m in itens),Decimal("0"))}
        return itens[:limite],resumo

    def indicadores(self):
        try:
            with self._conectar_banco() as conexao:
                with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                    cursor.execute("""SELECT
                        COUNT(*) FILTER (WHERE ativo AND atual AND status='Em elaboração') AS elaboracao,
                        COUNT(*) FILTER (WHERE ativo AND atual AND status='Em análise') AS analise,
                        COUNT(*) FILTER (WHERE ativo AND atual AND status='Devolvida para correção') AS devolvidas,
                        COUNT(*) FILTER (WHERE status='Aprovada' AND DATE_TRUNC('month',aprovado_em)=DATE_TRUNC('month',CURRENT_DATE)) AS aprovadas_mes,
                        COALESCE(SUM(valor_liquido) FILTER (WHERE status='Aprovada' AND DATE_TRUNC('month',aprovado_em)=DATE_TRUNC('month',CURRENT_DATE)),0) AS liquido_aprovado_mes,
                        COALESCE(SUM(total_glosas) FILTER (WHERE status='Aprovada' AND DATE_TRUNC('month',aprovado_em)=DATE_TRUNC('month',CURRENT_DATE)),0) AS glosas_mes
                        FROM fc_medicoes""")
                    return cursor.fetchone()
        except psycopg2.Error as erro: raise MedicaoServiceError("Falha ao consultar indicadores de medições.") from erro
