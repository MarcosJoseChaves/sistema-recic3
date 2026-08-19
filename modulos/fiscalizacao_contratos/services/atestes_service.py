"""Persistência transacional dos atestes da execução contratual."""

import logging
from decimal import Decimal, ROUND_HALF_UP

import psycopg2
from psycopg2.extras import RealDictCursor

from .cloudinary_storage import CloudinaryStorageError
from ..validacoes_atestes import CATEGORIAS_DOCUMENTO_ATESTE, CENTAVOS


class AtesteServiceError(Exception): pass
class AtesteNaoEncontradoError(AtesteServiceError): pass
class AtesteBloqueadoError(AtesteServiceError): pass
class AtesteDuplicadoError(AtesteServiceError): pass
class ReferenciaAtesteInvalidaError(AtesteServiceError): pass


EDITAVEIS = ("Em elaboração", "Devolvido para correção")
ALTERAVEIS_COMPLEMENTOS = (*EDITAVEIS, "Atestado")
LOGGER = logging.getLogger(__name__)


class AtesteService:
    def __init__(self, conectar_banco):
        self._conectar_banco = conectar_banco

    def listar(self, busca="", filtros=None):
        filtros = filtros or {}
        padrao = f"%{str(busca or '').strip()}%"
        conexao = self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""SELECT a.*,m.numero_medicao,m.competencia,m.versao AS medicao_versao,
                    m.valor_bruto,m.total_acrescimos,m.total_descontos,m.total_glosas,m.valor_liquido,
                    c.id AS contrato_id,c.numero_contrato,c.processo_administrativo,
                    e.id AS empresa_id,e.razao_social AS empresa_nome,s.nome AS atestador_nome,
                    COALESCE(n.total_notas,0) AS total_notas,
                    COALESCE(n.total_notas,0)-a.valor_atestado AS diferenca_notas
                    FROM fc_atestes a JOIN fc_medicoes m ON m.id=a.medicao_id
                    JOIN fc_contratos c ON c.id=m.contrato_id JOIN fc_empresas e ON e.id=c.empresa_id
                    JOIN fc_servidores s ON s.id=a.servidor_atestador_id
                    LEFT JOIN (SELECT ateste_id,SUM(valor_nota) AS total_notas
                        FROM fc_ateste_notas_fiscais WHERE ativo GROUP BY ateste_id) n ON n.ateste_id=a.id
                    WHERE (%s='' OR CAST(a.numero_ateste AS TEXT) ILIKE %s OR CAST(m.numero_medicao AS TEXT) ILIKE %s
                        OR c.numero_contrato ILIKE %s OR COALESCE(c.processo_administrativo,'') ILIKE %s
                        OR e.razao_social ILIKE %s OR s.nome ILIKE %s
                        OR COALESCE(a.protocolo_encaminhamento,'') ILIKE %s
                        OR EXISTS (SELECT 1 FROM fc_ateste_notas_fiscais nf WHERE nf.ateste_id=a.id AND nf.ativo
                            AND (nf.numero_nota ILIKE %s OR COALESCE(nf.chave_acesso,'') ILIKE %s)))
                    AND (%s::BIGINT IS NULL OR c.id=%s) AND (%s::BIGINT IS NULL OR e.id=%s)
                    AND (%s::DATE IS NULL OR m.competencia=%s) AND (%s::BIGINT IS NULL OR a.servidor_atestador_id=%s)
                    AND (%s='' OR a.status=%s)
                    AND (%s::DATE IS NULL OR a.data_ateste>=%s) AND (%s::DATE IS NULL OR a.data_ateste<=%s)
                    AND (%s::DATE IS NULL OR a.encaminhado_em::DATE>=%s)
                    AND (%s::DATE IS NULL OR a.encaminhado_em::DATE<=%s)
                    AND (%s='todos' OR (%s='ativos' AND a.ativo) OR (%s='cancelados' AND a.status='Cancelado'))
                    AND (%s=FALSE OR COALESCE(n.total_notas,0)<>a.valor_atestado)
                    AND (%s=FALSE OR a.status='Encaminhado para pagamento')
                    ORDER BY a.criado_em DESC,a.id DESC""",(
                    padrao,padrao,padrao,padrao,padrao,padrao,padrao,padrao,padrao,padrao,
                    filtros.get("contrato_id"),filtros.get("contrato_id"),filtros.get("empresa_id"),filtros.get("empresa_id"),
                    filtros.get("competencia"),filtros.get("competencia"),filtros.get("servidor_id"),filtros.get("servidor_id"),
                    filtros.get("status",""),filtros.get("status",""),filtros.get("ateste_inicio"),filtros.get("ateste_inicio"),
                    filtros.get("ateste_fim"),filtros.get("ateste_fim"),filtros.get("encaminhamento_inicio"),filtros.get("encaminhamento_inicio"),
                    filtros.get("encaminhamento_fim"),filtros.get("encaminhamento_fim"),filtros.get("status_ativo","ativos"),
                    filtros.get("status_ativo","ativos"),filtros.get("status_ativo","ativos"),filtros.get("com_diferenca",False),
                    filtros.get("encaminhados",False),
                ))
                return cursor.fetchall()
        except psycopg2.Error as erro:
            raise AtesteServiceError("Falha ao consultar atestes.") from erro
        finally: conexao.close()

    def opcoes(self):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""SELECT m.id,m.numero_medicao,m.competencia,m.versao,m.valor_liquido,
                    c.id AS contrato_id,c.numero_contrato,e.razao_social AS empresa_nome
                    FROM fc_medicoes m JOIN fc_contratos c ON c.id=m.contrato_id
                    JOIN fc_empresas e ON e.id=c.empresa_id
                    WHERE m.ativo AND m.atual AND m.status='Aprovada' ORDER BY m.competencia DESC""")
                medicoes=cursor.fetchall()
                cursor.execute("SELECT id,nome,matricula FROM fc_servidores WHERE ativo ORDER BY nome")
                servidores=cursor.fetchall()
                cursor.execute("SELECT id,razao_social FROM fc_empresas WHERE ativo ORDER BY razao_social")
                return medicoes,servidores,cursor.fetchall()
        except psycopg2.Error as erro: raise AtesteServiceError("Falha ao carregar opções.") from erro
        finally: conexao.close()

    def obter(self,ateste_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""SELECT a.*,m.numero_medicao,m.competencia,m.versao AS medicao_versao,
                    m.valor_bruto,m.total_acrescimos,m.total_descontos,m.total_glosas,m.valor_liquido,
                    c.id AS contrato_id,c.numero_contrato,c.processo_administrativo,e.razao_social AS empresa_nome,
                    s.nome AS atestador_nome,se.nome AS encaminhador_nome,u.username AS criador_nome
                    FROM fc_atestes a JOIN fc_medicoes m ON m.id=a.medicao_id
                    JOIN fc_contratos c ON c.id=m.contrato_id JOIN fc_empresas e ON e.id=c.empresa_id
                    JOIN fc_servidores s ON s.id=a.servidor_atestador_id
                    LEFT JOIN fc_servidores se ON se.id=a.servidor_encaminhador_id
                    JOIN usuarios u ON u.id=a.criado_por_usuario_id WHERE a.id=%s""",(ateste_id,))
                ateste=cursor.fetchone()
                if not ateste: raise AtesteNaoEncontradoError("Ateste não encontrado.")
                cursor.execute("""SELECT n.*,d.titulo AS documento_titulo,d.nome_original
                    FROM fc_ateste_notas_fiscais n LEFT JOIN fc_documentos d ON d.id=n.documento_id
                    WHERE n.ateste_id=%s ORDER BY n.ativo DESC,n.data_emissao,n.id""",(ateste_id,))
                notas=cursor.fetchall()
                cursor.execute("""SELECT ad.*,d.titulo,d.nome_original,d.extensao
                    FROM fc_ateste_documentos ad JOIN fc_documentos d ON d.id=ad.documento_id
                    WHERE ad.ateste_id=%s ORDER BY ad.ativo DESC,ad.criado_em DESC""",(ateste_id,))
                documentos=cursor.fetchall()
                cursor.execute("""SELECT ev.*,u.username AS usuario_nome FROM fc_ateste_eventos ev
                    JOIN usuarios u ON u.id=ev.criado_por_usuario_id WHERE ev.ateste_id=%s
                    ORDER BY ev.criado_em DESC,ev.id DESC""",(ateste_id,))
                eventos=cursor.fetchall()
                total=sum((Decimal(str(n["valor_nota"])) for n in notas if n["ativo"]),Decimal("0")).quantize(CENTAVOS,rounding=ROUND_HALF_UP)
                diferenca=(total-Decimal(str(ateste["valor_atestado"]))).quantize(CENTAVOS,rounding=ROUND_HALF_UP)
                return ateste,notas,documentos,eventos,total,diferenca
        except AtesteNaoEncontradoError: raise
        except psycopg2.Error as erro: raise AtesteServiceError("Falha ao carregar ateste.") from erro
        finally: conexao.close()

    def obter_da_medicao(self,medicao_id):
        itens=self.listar("",{"status_ativo":"todos"})
        return next((a for a in itens if a["medicao_id"]==medicao_id and a["ativo"] and a["status"]!="Cancelado"),None)

    def listar_do_contrato(self,contrato_id,limite=10):
        itens=self.listar("",{"contrato_id":contrato_id,"status_ativo":"todos"})
        ativos=[a for a in itens if a["ativo"]]
        resumo={"total":len(itens),"elaboracao":sum(a["status"]=="Em elaboração" for a in ativos),
            "devolvidos":sum(a["status"]=="Devolvido para correção" for a in ativos),
            "atestados":sum(a["status"]=="Atestado" for a in ativos),
            "encaminhados":sum(a["status"]=="Encaminhado para pagamento" for a in ativos),
            "valor_encaminhado":sum((Decimal(str(a["valor_atestado"])) for a in ativos if a["status"]=="Encaminhado para pagamento"),Decimal("0"))}
        return itens[:limite],resumo

    def indicadores(self):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""SELECT COUNT(*) FILTER (WHERE ativo AND status='Em elaboração') AS elaboracao,
                    COUNT(*) FILTER (WHERE ativo AND status='Devolvido para correção') AS devolvidos,
                    COUNT(*) FILTER (WHERE ativo AND status='Atestado') AS aguardando,
                    COUNT(*) FILTER (WHERE ativo AND status='Encaminhado para pagamento'
                        AND DATE_TRUNC('month',encaminhado_em)=DATE_TRUNC('month',CURRENT_DATE)) AS encaminhados_mes,
                    COALESCE(SUM(valor_atestado) FILTER (WHERE ativo AND status='Encaminhado para pagamento'
                        AND DATE_TRUNC('month',encaminhado_em)=DATE_TRUNC('month',CURRENT_DATE)),0) AS valor_encaminhado_mes
                    FROM fc_atestes""")
                return cursor.fetchone()
        except psycopg2.Error as erro: raise AtesteServiceError("Falha ao carregar indicadores.") from erro
        finally: conexao.close()

    def _medicao_valida(self,cursor,medicao_id,bloquear=True):
        sufixo=" FOR UPDATE" if bloquear else ""
        cursor.execute("""SELECT id,contrato_id,valor_liquido,status,ativo,atual
            FROM fc_medicoes WHERE id=%s"""+sufixo,(medicao_id,))
        medicao=cursor.fetchone()
        if not medicao or not medicao["ativo"] or not medicao["atual"] or medicao["status"]!="Aprovada":
            raise ReferenciaAtesteInvalidaError("Somente uma medição aprovada, ativa e atual pode receber ateste.")
        return medicao

    @staticmethod
    def _servidor_ativo(cursor,servidor_id,mensagem="Selecione um servidor ativo."):
        cursor.execute("SELECT id FROM fc_servidores WHERE id=%s AND ativo",(servidor_id,))
        if not cursor.fetchone(): raise ReferenciaAtesteInvalidaError(mensagem)

    def _obter_bloqueado(self,cursor,ateste_id):
        cursor.execute("SELECT * FROM fc_atestes WHERE id=%s FOR UPDATE",(ateste_id,))
        ateste=cursor.fetchone()
        if not ateste: raise AtesteNaoEncontradoError("Ateste não encontrado.")
        return ateste

    @staticmethod
    def _total_notas(cursor,ateste_id):
        cursor.execute("""SELECT COUNT(*) AS quantidade,COALESCE(SUM(valor_nota),0) AS total,
            COUNT(*) FILTER (WHERE n.documento_id IS NULL OR d.id IS NULL) AS sem_documento
            FROM fc_ateste_notas_fiscais n
            JOIN fc_atestes a ON a.id=n.ateste_id
            JOIN fc_medicoes m ON m.id=a.medicao_id
            LEFT JOIN fc_documentos d ON d.id=n.documento_id
                AND d.ativo AND d.contrato_id=m.contrato_id
            WHERE n.ateste_id=%s AND n.ativo""",(ateste_id,))
        dados=cursor.fetchone()
        return dados["quantidade"],Decimal(str(dados["total"])).quantize(CENTAVOS,rounding=ROUND_HALF_UP),dados["sem_documento"]

    def _evento(self,cursor,ateste,tipo,anterior,novo,usuario_id,justificativa=None):
        _,total,_=self._total_notas(cursor,ateste["id"])
        cursor.execute("""INSERT INTO fc_ateste_eventos
            (ateste_id,tipo_evento,status_anterior,status_novo,justificativa,valor_atestado,total_notas,criado_por_usuario_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (ateste["id"],tipo,anterior,novo,justificativa,ateste["valor_atestado"],total,usuario_id))

    def criar(self,dados,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                medicao=self._medicao_valida(cursor,dados["medicao_id"])
                self._servidor_ativo(cursor,dados["servidor_atestador_id"],"O servidor atestador deve existir e estar ativo.")
                cursor.execute("SELECT id FROM fc_atestes WHERE medicao_id=%s AND ativo AND status<>'Cancelado'",(medicao["id"],))
                if cursor.fetchone(): raise AtesteDuplicadoError("Esta medição já possui um ateste ativo.")
                valor_atestado=Decimal(str(medicao["valor_liquido"])).quantize(CENTAVOS,rounding=ROUND_HALF_UP)
                cursor.execute("""INSERT INTO fc_atestes
                    (medicao_id,numero_ateste,servidor_atestador_id,status,parecer,observacoes,valor_atestado,
                     criado_por_usuario_id,atualizado_por_usuario_id)
                    VALUES (%s,%s,%s,'Em elaboração',%s,%s,%s,%s,%s) RETURNING *""",
                    (medicao["id"],dados["numero_ateste"],dados["servidor_atestador_id"],dados.get("parecer"),
                     dados.get("observacoes"),valor_atestado,usuario_id,usuario_id))
                ateste=cursor.fetchone();self._evento(cursor,ateste,"Criação",None,"Em elaboração",usuario_id)
            conexao.commit();return ateste["id"]
        except Exception as erro:
            conexao.rollback()
            if isinstance(erro,(ReferenciaAtesteInvalidaError,AtesteDuplicadoError)): raise
            if isinstance(erro,psycopg2.IntegrityError): raise AtesteDuplicadoError("Já existe ateste ou número igual para esta medição.") from erro
            raise AtesteServiceError("Falha ao criar ateste.") from erro
        finally: conexao.close()

    def atualizar(self,ateste_id,dados,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                ateste=self._obter_bloqueado(cursor,ateste_id)
                if not ateste["ativo"] or ateste["status"] not in EDITAVEIS: raise AtesteBloqueadoError("Este ateste não pode ser editado.")
                self._servidor_ativo(cursor,dados["servidor_atestador_id"],"O servidor atestador deve estar ativo.")
                cursor.execute("""UPDATE fc_atestes SET numero_ateste=%s,servidor_atestador_id=%s,parecer=%s,
                    observacoes=%s,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s""",
                    (dados["numero_ateste"],dados["servidor_atestador_id"],dados.get("parecer"),dados.get("observacoes"),usuario_id,ateste_id))
            conexao.commit()
        except Exception as erro:
            conexao.rollback()
            if isinstance(erro,(AtesteNaoEncontradoError,AtesteBloqueadoError,ReferenciaAtesteInvalidaError)): raise
            if isinstance(erro,psycopg2.IntegrityError): raise AtesteDuplicadoError("Número de ateste duplicado.") from erro
            raise AtesteServiceError("Falha ao atualizar ateste.") from erro
        finally: conexao.close()

    def _exigir_complementos_alteraveis(self,ateste):
        if not ateste["ativo"] or ateste["status"] not in ALTERAVEIS_COMPLEMENTOS:
            raise AtesteBloqueadoError("Notas e documentos não podem mais ser alterados.")

    def salvar_nota(self,ateste_id,dados,usuario_id,nota_id=None):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                ateste=self._obter_bloqueado(cursor,ateste_id);self._exigir_complementos_alteraveis(ateste)
                self._validar_documento_nota(cursor,ateste,dados.get("documento_id"))
                self._gravar_nota(cursor,ateste_id,dados,usuario_id,nota_id)
            conexao.commit()
        except Exception as erro:
            conexao.rollback()
            if isinstance(erro,(AtesteNaoEncontradoError,AtesteBloqueadoError,ReferenciaAtesteInvalidaError)): raise
            if isinstance(erro,psycopg2.IntegrityError): raise AtesteDuplicadoError("Esta nota fiscal já está ativa neste ateste.") from erro
            raise AtesteServiceError("Falha ao salvar nota fiscal.") from erro
        finally: conexao.close()

    @staticmethod
    def _validar_documento_nota(cursor,ateste,documento_id):
        if not documento_id: return
        cursor.execute("""SELECT d.id FROM fc_documentos d JOIN fc_medicoes m ON m.id=%s
            WHERE d.id=%s AND d.contrato_id=m.contrato_id AND d.ativo""",(ateste["medicao_id"],documento_id))
        if not cursor.fetchone():
            raise ReferenciaAtesteInvalidaError("O documento da nota deve estar ativo e pertencer ao mesmo contrato.")

    @staticmethod
    def _gravar_nota(cursor,ateste_id,dados,usuario_id,nota_id=None):
        if nota_id:
            cursor.execute("""UPDATE fc_ateste_notas_fiscais SET numero_nota=%s,serie=%s,data_emissao=%s,
                valor_nota=%s,chave_acesso=%s,documento_id=%s,observacoes=%s,
                atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s
                WHERE id=%s AND ateste_id=%s AND ativo""",(
                dados["numero_nota"],dados.get("serie"),dados["data_emissao"],dados["valor_nota"],dados.get("chave_acesso"),
                dados.get("documento_id"),dados.get("observacoes"),usuario_id,nota_id,ateste_id))
            if cursor.rowcount!=1: raise AtesteNaoEncontradoError("Nota fiscal não encontrada.")
        else:
            cursor.execute("""INSERT INTO fc_ateste_notas_fiscais
                (ateste_id,numero_nota,serie,data_emissao,valor_nota,chave_acesso,documento_id,observacoes,
                 criado_por_usuario_id,atualizado_por_usuario_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",(
                ateste_id,dados["numero_nota"],dados.get("serie"),dados["data_emissao"],dados["valor_nota"],
                dados.get("chave_acesso"),dados.get("documento_id"),dados.get("observacoes"),usuario_id,usuario_id))

    def salvar_nota_com_upload(self,ateste_id,dados,arquivo,usuario_id,armazenamento,nota_id=None):
        """Cria o documento privado e o vincula à nota na mesma transação."""
        if dados.get("documento_id"):
            raise ReferenciaAtesteInvalidaError(
                "Escolha entre um documento existente e um novo arquivo."
            )
        conexao=None;enviado=None
        try:
            conexao=self._conectar_banco()
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                ateste=self._obter_bloqueado(cursor,ateste_id);self._exigir_complementos_alteraveis(ateste)
                cursor.execute("SELECT contrato_id FROM fc_medicoes WHERE id=%s",(ateste["medicao_id"],))
                medicao=cursor.fetchone()
                if not medicao: raise ReferenciaAtesteInvalidaError("A medição do ateste não foi encontrada.")
                enviado=armazenamento.enviar(arquivo,medicao["contrato_id"],None)
                cursor.execute("""INSERT INTO fc_documentos (
                    contrato_id,aditivo_id,categoria,titulo,descricao,nome_original,
                    armazenamento_provedor,armazenamento_chave,armazenamento_versao,
                    mime_type,extensao,tamanho_bytes,sha256,
                    criado_por_usuario_id,atualizado_por_usuario_id
                ) VALUES (%s,NULL,'Comprovante',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                RETURNING id""",(
                    medicao["contrato_id"],f"Nota fiscal {dados['numero_nota']}",
                    f"Arquivo enviado durante o lançamento do ateste {ateste['numero_ateste']}.",
                    arquivo["nome_original"],enviado["armazenamento_provedor"],enviado["armazenamento_chave"],
                    enviado.get("armazenamento_versao"),arquivo["mime_type"],arquivo["extensao"],
                    arquivo["tamanho_bytes"],arquivo["sha256"],usuario_id,usuario_id,
                ))
                documento_id=cursor.fetchone()["id"]
                self._gravar_nota(cursor,ateste_id,{**dados,"documento_id":documento_id},usuario_id,nota_id)
            conexao.commit();return documento_id
        except (AtesteNaoEncontradoError,AtesteBloqueadoError,ReferenciaAtesteInvalidaError,CloudinaryStorageError):
            if conexao: conexao.rollback()
            if enviado: self._remover_upload_nota(armazenamento,enviado)
            raise
        except Exception as erro:
            if conexao: conexao.rollback()
            if enviado: self._remover_upload_nota(armazenamento,enviado)
            if isinstance(erro,psycopg2.IntegrityError):
                raise AtesteDuplicadoError("Esta nota fiscal já está ativa neste ateste.") from erro
            raise AtesteServiceError("Falha ao enviar o arquivo da nota fiscal.") from erro
        finally:
            if conexao: conexao.close()

    @staticmethod
    def _remover_upload_nota(armazenamento,enviado):
        try: armazenamento.remover(enviado["armazenamento_chave"])
        except CloudinaryStorageError as erro:
            LOGGER.warning("Falha na limpeza compensatória do arquivo da nota: tipo_erro=%s",type(erro).__name__)

    def inativar_nota(self,ateste_id,nota_id,usuario_id):
        self._inativar_vinculo("nota",ateste_id,nota_id,usuario_id)

    def vincular_documento(self,ateste_id,documento_id,categoria,observacoes,usuario_id):
        if categoria not in CATEGORIAS_DOCUMENTO_ATESTE: raise ReferenciaAtesteInvalidaError("Selecione uma categoria válida.")
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                ateste=self._obter_bloqueado(cursor,ateste_id);self._exigir_complementos_alteraveis(ateste)
                cursor.execute("""SELECT d.id FROM fc_documentos d JOIN fc_medicoes m ON m.id=%s
                    WHERE d.id=%s AND d.contrato_id=m.contrato_id AND d.ativo""",(ateste["medicao_id"],documento_id))
                if not cursor.fetchone(): raise ReferenciaAtesteInvalidaError("O documento deve estar ativo e pertencer ao mesmo contrato.")
                cursor.execute("""INSERT INTO fc_ateste_documentos
                    (ateste_id,documento_id,categoria,observacoes,criado_por_usuario_id,atualizado_por_usuario_id)
                    VALUES (%s,%s,%s,%s,%s,%s)""",(ateste_id,documento_id,categoria,observacoes,usuario_id,usuario_id))
            conexao.commit()
        except Exception as erro:
            conexao.rollback()
            if isinstance(erro,(AtesteNaoEncontradoError,AtesteBloqueadoError,ReferenciaAtesteInvalidaError)): raise
            if isinstance(erro,psycopg2.IntegrityError): raise AtesteDuplicadoError("Este documento já está vinculado ao ateste.") from erro
            raise AtesteServiceError("Falha ao vincular documento.") from erro
        finally: conexao.close()

    def inativar_documento(self,ateste_id,vinculo_id,usuario_id):
        self._inativar_vinculo("documento",ateste_id,vinculo_id,usuario_id)

    def _inativar_vinculo(self,tipo,ateste_id,registro_id,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                ateste=self._obter_bloqueado(cursor,ateste_id);self._exigir_complementos_alteraveis(ateste)
                consulta=("UPDATE fc_ateste_notas_fiscais SET ativo=FALSE,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s AND ateste_id=%s AND ativo"
                    if tipo=="nota" else "UPDATE fc_ateste_documentos SET ativo=FALSE,atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s AND ateste_id=%s AND ativo")
                cursor.execute(consulta,(usuario_id,registro_id,ateste_id))
                if cursor.rowcount!=1: raise AtesteNaoEncontradoError("Registro não encontrado.")
            conexao.commit()
        except Exception as erro:
            conexao.rollback()
            if isinstance(erro,(AtesteNaoEncontradoError,AtesteBloqueadoError)): raise
            raise AtesteServiceError("Falha ao inativar registro.") from erro
        finally: conexao.close()

    def atestar(self,ateste_id,usuario_id):
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                ateste=self._obter_bloqueado(cursor,ateste_id)
                if not ateste["ativo"] or ateste["status"] not in EDITAVEIS: raise AtesteBloqueadoError("Este ateste não pode ser concluído.")
                if not str(ateste.get("parecer") or "").strip(): raise AtesteBloqueadoError("Informe o parecer antes de atestar.")
                self._servidor_ativo(cursor,ateste["servidor_atestador_id"],"O servidor atestador deve continuar ativo.")
                medicao=self._medicao_valida(cursor,ateste["medicao_id"])
                quantidade,_,_=self._total_notas(cursor,ateste_id)
                cursor.execute("SELECT COUNT(*) AS quantidade FROM fc_ateste_documentos WHERE ateste_id=%s AND ativo",(ateste_id,));docs=cursor.fetchone()["quantidade"]
                if not quantidade and not docs: raise AtesteBloqueadoError("Inclua ao menos uma nota fiscal ou documento comprobatório.")
                anterior=ateste["status"];ateste["valor_atestado"]=Decimal(str(medicao["valor_liquido"])).quantize(CENTAVOS,rounding=ROUND_HALF_UP)
                cursor.execute("""UPDATE fc_atestes SET status='Atestado',data_ateste=CURRENT_DATE,valor_atestado=%s,
                    atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s""",(ateste["valor_atestado"],usuario_id,ateste_id))
                self._evento(cursor,ateste,"Ateste",anterior,"Atestado",usuario_id)
            conexao.commit()
        except Exception as erro:
            conexao.rollback()
            if isinstance(erro,(AtesteNaoEncontradoError,AtesteBloqueadoError,ReferenciaAtesteInvalidaError)): raise
            raise AtesteServiceError("Falha ao concluir ateste.") from erro
        finally: conexao.close()

    def devolver(self,ateste_id,justificativa,usuario_id):
        self._mudar_status(ateste_id,"Devolução para correção","Devolvido para correção",justificativa,usuario_id,("Em elaboração","Atestado"),limpar_data=True)

    def retornar_elaboracao(self,ateste_id,justificativa,usuario_id):
        self._mudar_status(ateste_id,"Retorno para elaboração","Em elaboração",justificativa,usuario_id,("Devolvido para correção",))

    def cancelar(self,ateste_id,justificativa,usuario_id):
        self._mudar_status(ateste_id,"Cancelamento","Cancelado",justificativa,usuario_id,("Em elaboração","Devolvido para correção","Atestado"))

    def _mudar_status(self,ateste_id,tipo,novo,justificativa,usuario_id,permitidos,limpar_data=False):
        justificativa=str(justificativa or "").strip()
        if not justificativa: raise AtesteBloqueadoError("Informe a justificativa.")
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                ateste=self._obter_bloqueado(cursor,ateste_id)
                if not ateste["ativo"] or ateste["status"] not in permitidos: raise AtesteBloqueadoError("A situação atual não permite esta ação.")
                data_sql=",data_ateste=NULL" if limpar_data else ""
                cursor.execute("UPDATE fc_atestes SET status=%s"+data_sql+",atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s",(novo,usuario_id,ateste_id))
                self._evento(cursor,ateste,tipo,ateste["status"],novo,usuario_id,justificativa)
            conexao.commit()
        except Exception as erro:
            conexao.rollback()
            if isinstance(erro,(AtesteNaoEncontradoError,AtesteBloqueadoError)): raise
            raise AtesteServiceError("Falha ao alterar situação do ateste.") from erro
        finally: conexao.close()

    def encaminhar(self,ateste_id,protocolo,servidor_id,usuario_id):
        protocolo=str(protocolo or "").strip()
        if not protocolo: raise AtesteBloqueadoError("Informe o protocolo de encaminhamento.")
        conexao=self._conectar_banco()
        try:
            with conexao.cursor(cursor_factory=RealDictCursor) as cursor:
                ateste=self._obter_bloqueado(cursor,ateste_id)
                if not ateste["ativo"] or ateste["status"]!="Atestado": raise AtesteBloqueadoError("Somente um ateste concluído pode ser encaminhado.")
                self._medicao_valida(cursor,ateste["medicao_id"])
                self._servidor_ativo(cursor,servidor_id,"O servidor encaminhador deve existir e estar ativo.")
                quantidade,total,sem_documento=self._total_notas(cursor,ateste_id)
                if not quantidade: raise AtesteBloqueadoError("Inclua ao menos uma nota fiscal ativa.")
                if sem_documento: raise AtesteBloqueadoError("Todas as notas fiscais precisam de arquivo vinculado.")
                valor=Decimal(str(ateste["valor_atestado"])).quantize(CENTAVOS,rounding=ROUND_HALF_UP)
                if total!=valor: raise AtesteBloqueadoError("A soma das notas fiscais deve ser igual ao valor atestado.")
                cursor.execute("""UPDATE fc_atestes SET status='Encaminhado para pagamento',protocolo_encaminhamento=%s,
                    encaminhado_em=CURRENT_TIMESTAMP,servidor_encaminhador_id=%s,encaminhado_por_usuario_id=%s,
                    atualizado_em=CURRENT_TIMESTAMP,atualizado_por_usuario_id=%s WHERE id=%s""",
                    (protocolo,servidor_id,usuario_id,usuario_id,ateste_id))
                self._evento(cursor,ateste,"Encaminhamento para pagamento","Atestado","Encaminhado para pagamento",usuario_id)
            conexao.commit()
        except Exception as erro:
            conexao.rollback()
            if isinstance(erro,(AtesteNaoEncontradoError,AtesteBloqueadoError,ReferenciaAtesteInvalidaError)): raise
            raise AtesteServiceError("Falha ao encaminhar ateste.") from erro
        finally: conexao.close()
