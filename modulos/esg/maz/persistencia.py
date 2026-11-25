"""Adaptador K01 explícito. Sem conexão implícita ou aprovação institucional."""
from contextlib import contextmanager
from dataclasses import dataclass, replace
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib
import json
from uuid import UUID, uuid4
from ..contratos import FonteDocumental, Perimetro, Periodo
from ..validacoes import ContratoESGInvalido
from .modelos import ComponenteMassa, UnidadeMassa, OrigemValor, QualidadeInformacao, ReconciliacaoFisica
from .trcs import calcular_trcs, avaliar_metodologia, ResultadoTRCS, AvaliacaoTRCS

class PersistenciaMAZError(Exception): pass
class ConflitoMAZ(PersistenciaMAZError): pass
class IntegridadeMAZ(PersistenciaMAZError): pass
class ConfirmacaoIncertaMAZ(PersistenciaMAZError): pass

CONTRATO_BYTES='MAZ-SPEC-UTF8-JSON-v1'

def validar_especificacao(raw, digest):
    if type(raw) is not bytes or type(digest) is not str:
        raise ContratoESGInvalido('Bytes e SHA-256 explícitos são obrigatórios.')
    def unique(pairs):
        out={}
        for k,v in pairs:
            if k in out: raise ContratoESGInvalido('Chave JSON duplicada.')
            out[k]=v
        return out
    def invalid(_): raise ContratoESGInvalido('Número JSON não finito.')
    try:
        text=raw.decode('utf-8')
        doc=json.loads(text,object_pairs_hook=unique,parse_float=Decimal,parse_constant=invalid)
    except (UnicodeError,ValueError) as e: raise ContratoESGInvalido('JSON UTF-8 inválido.') from e
    if not isinstance(doc,dict) or hashlib.sha256(raw).hexdigest()!=digest:
        raise ContratoESGInvalido('Especificação e SHA-256 incompatíveis.')
    if doc.get('formula')!='TRCS_BRUTA_100_REJEITOS_ENTRADA' or doc.get('contrato_numerico')!='R2-kg-1e9-6':
        raise ContratoESGInvalido('Especificação K01 incompatível.')
    return text

def _escalar(valor, casas):
    """Deslocamento decimal exato, independente do contexto ativo."""
    t=valor.as_tuple()
    return Decimal((t.sign,t.digits,t.exponent+casas))

def normalizar(componente):
    if not isinstance(componente,ComponenteMassa): raise ContratoESGInvalido('Componente R1 obrigatório.')
    v=componente.massa
    kg=_escalar(v,3) if v is not None and componente.unidade is UnidadeMassa.TONELADA else v
    if kg is not None:
        t=kg.as_tuple();digits=list(t.digits);exponent=t.exponent
        while digits and digits[-1]==0:
            digits.pop();exponent+=1
        if kg>Decimal('1000000000') or (digits and exponent < -6):
            raise ContratoESGInvalido('Massa excede magnitude ou escala homologada.')
    return replace(componente,massa=kg,unidade=UnidadeMassa.KG)

@dataclass(frozen=True)
class EvidenciaMAZ:
    fonte: FonteDocumental
    versao: int
    tipo: str

    def __post_init__(self):
        if not isinstance(self.fonte,FonteDocumental) or type(self.versao)is not int or self.versao<1 or not isinstance(self.tipo,str) or not self.tipo.strip():
            raise ContratoESGInvalido('Evidência versionada inválida.')

@dataclass(frozen=True)
class MedicaoMAZ:
    serie_id: UUID
    revisao_id: UUID
    numero: int
    metodologia: str
    entrada_original: ComponenteMassa
    rejeitos_original: ComponenteMassa
    resultado: ResultadoTRCS
    avaliacao: AvaliacaoTRCS
    evidencias: tuple
    especificacao_bytes: bytes
    hash_especificacao: str
    motivo: str
    anterior: UUID | None
    versoes_fontes: tuple
    auditorias: tuple
    proveniencia: tuple

class AdaptadorMAZ:
    """Fábrica injetada deve devolver conexão dedicada runtime, ociosa, autocommit=False."""
    def __init__(self, conectar):
        if not callable(conectar): raise ContratoESGInvalido('Fábrica de conexão obrigatória.')
        self._conectar=conectar

    @contextmanager
    def _tx(self, leitura=False):
        c=None;owned=False;commit=False
        try:
            c=self._conectar()
            if c.closed or c.autocommit or c.get_transaction_status()!=0:
                raise PersistenciaMAZError('Conexão dedicada ociosa obrigatória.')
            owned=True
            c.set_session(readonly=leitura,isolation_level='REPEATABLE READ' if leitura else 'READ COMMITTED')
            with c.cursor() as q: yield q
            commit=True;c.commit()
        except Exception as e:
            if owned:
                try:c.rollback()
                except Exception:pass
            if commit:raise ConfirmacaoIncertaMAZ('Confirmação incerta; consultar antes de repetir.') from None
            if isinstance(e,(PersistenciaMAZError,ContratoESGInvalido)):raise
            if getattr(e,'pgcode',None) in ('23505','40001','40P01'):
                raise ConflitoMAZ('Conflito; nenhuma repetição automática.') from None
            raise IntegridadeMAZ('Dados ou vínculos incompatíveis; transação revertida.') from None
        finally:
            if owned:
                try:c.close()
                except Exception:pass

    @staticmethod
    def _contexto(ator,request):
        if type(ator)is not int or ator<=0 or not isinstance(request,UUID):
            raise ContratoESGInvalido('Ator positivo e request_id UUID obrigatórios.')

    def _put(self,q,t,fields,ator,request):
        q.execute("INSERT INTO public.auditoria_tecnica(request_id,modulo,acao,resultado,tipo_ator,usuario_id,origem) VALUES (%s,'ESG','REGISTRAR','SUCESSO','USUARIO',%s,'ADAPTADOR_MAZ') RETURNING id",(str(request),ator))
        fields={**fields,'criado_por_usuario_id':ator,'request_id':str(request),'auditoria_id':q.fetchone()[0]}
        # Identifiers originate exclusively from this module's fixed statements.
        if t=='esg_fonte_versoes':fields.setdefault('motivo','Cadastro adaptador MAZ')
        cols=','.join(fields);values=','.join(['%s']*len(fields))
        q.execute('INSERT INTO public.'+t+' ('+cols+') VALUES ('+values+')',tuple(fields.values()))

    def _ensure(self,q,t,keys,fields,ator,request):
        q.execute('SELECT '+(','.join(fields) if fields else '1')+' FROM public.'+t+' WHERE '+' AND '.join(k+'=%s' for k in keys),tuple(keys.values()))
        row=q.fetchone()
        if row is not None:
            actual=tuple(bytes(x) if isinstance(x,memoryview) else x for x in row) if fields else ()
            if actual!=tuple(fields.values()):raise IntegridadeMAZ('Identidade existente com conteúdo conflitante.')
            return
        self._put(q,t,{**keys,**fields},ator,request)

    def _fonte(self,q,fonte,versao,ator,request,anterior=None):
        if not isinstance(fonte,FonteDocumental) or type(versao)is not int or versao<1:
            raise ContratoESGInvalido('Fonte versionada inválida.')
        if versao>1 and anterior is None:
            q.execute('SELECT anterior FROM public.esg_fonte_versoes WHERE fonte_id=%s AND versao=%s',(fonte.identificador,versao));existente=q.fetchone()
            if existente:anterior=existente[0]
        self._ensure(q,'esg_fontes_documentais',{'identificador':fonte.identificador},{},ator,request)
        self._ensure(q,'esg_fonte_versoes',{'fonte_id':fonte.identificador,'versao':versao},
                     {'referencia':fonte.referencia_documental,'data_documento':fonte.data_documento,'anterior':anterior},ator,request)

    def cadastrar_fonte(self,fonte,versao,*,ator,request_id,anterior=None):
        self._contexto(ator,request_id)
        with self._tx() as q:self._fonte(q,fonte,versao,ator,request_id,anterior)

    def cadastrar_metodologia(self,versao,raw,digest,fonte,*,ator,request_id,versao_fonte=1):
        text=validar_especificacao(raw,digest);self._contexto(ator,request_id)
        with self._tx() as q:
            self._ensure(q,'esg_definicoes',{'codigo':'K01'},{'nome':'TRCS','dimensao':'ambiental'},ator,request_id)
            self._ensure(q,'esg_unidades',{'simbolo':'kg'},{'descricao':'Quilograma'},ator,request_id)
            self._ensure(q,'esg_aliases',{'alias':'A1'},{'indicador_codigo':'K01'},ator,request_id)
            self._fonte(q,fonte,versao_fonte,ator,request_id)
            q.execute('SELECT especificacao_bytes,hash_especificacao,fonte_id,fonte_versao FROM public.esg_metodologias WHERE indicador_codigo=%s AND versao=%s',('K01',versao));row=q.fetchone()
            if row:
                if (bytes(row[0]),*row[1:])!=(raw,digest,fonte.identificador,versao_fonte):raise IntegridadeMAZ('Versão metodológica conflitante.')
                return
            # JSON is passed as its exact validated text, never reserialized from Decimal.
            self._put(q,'esg_metodologias',{'indicador_codigo':'K01','versao':versao,'especificacao':text,'hash_especificacao':digest,'especificacao_bytes':raw,'especificacao_serializacao':CONTRATO_BYTES,'fonte_id':fonte.identificador,'fonte_versao':versao_fonte},ator,request_id)

    def registrar(self,entrada,rejeitos,metodologia,*,ator,request_id,motivo,
                  versao_entrada=1,versao_rejeitos=1,evidencias=(),reconciliacao=None,
                  versao_reconciliacao=1,serie_id=None,revisao_esperada=None,precisao=50):
        self._contexto(ator,request_id)
        if not isinstance(motivo,str) or not motivo.strip():raise ContratoESGInvalido('Motivo obrigatório.')
        en=normalizar(entrada);re=normalizar(rejeitos)
        resultado=calcular_trcs(re,en,versao_metodologica=metodologia,precisao=precisao)
        avaliacao=avaliar_metodologia(resultado,reconciliacao=reconciliacao)
        if any(c.qualidade is QualidadeInformacao.VALIDADO for c in (entrada,rejeitos)):
            raise ContratoESGInvalido('Validação documental de componentes ainda não implementada pelo adaptador.')
        if (serie_id is None)!=(revisao_esperada is None):raise ContratoESGInvalido('Sucessora exige série e revisão esperada.')
        sid=serie_id or uuid4();rid=uuid4();numero=1
        with self._tx() as q:
            self._ensure(q,'esg_perimetros',{'codigo':en.perimetro.codigo},{'descricao':en.perimetro.descricao},ator,request_id)
            for c,v in ((entrada,versao_entrada),(rejeitos,versao_rejeitos)):
                # Existing later source versions are resolved, not silently invented.
                q.execute('SELECT anterior FROM public.esg_fonte_versoes WHERE fonte_id=%s AND versao=%s',(c.fonte.identificador,v));old=q.fetchone()
                self._fonte(q,c.fonte,v,ator,request_id,old[0] if old else None)
            if serie_id is None:
                self._put(q,'esg_series',{'id':str(sid),'indicador_codigo':'K01','ano':en.periodo.ano,'mes':en.periodo.mes,'perimetro_codigo':en.perimetro.codigo},ator,request_id)
            else:
                q.execute('SELECT s.ano,s.mes,s.perimetro_codigo,c.revisao_id,c.numero,r.metodologia_versao FROM public.esg_series s JOIN public.esg_correntes c ON c.serie_id=s.id JOIN public.esg_revisoes r ON r.id=c.revisao_id WHERE s.id=%s',(str(sid),));row=q.fetchone()
                if not row or str(row[3])!=str(revisao_esperada):raise ConflitoMAZ('Revisão corrente mudou.')
                if row[:3]!=(en.periodo.ano,en.periodo.mes,en.perimetro.codigo) or row[5]!=metodologia:raise IntegridadeMAZ('Escopo ou metodologia da série incompatível.')
                numero=row[4]+1
            self._put(q,'esg_revisoes',{'id':str(rid),'serie_id':str(sid),'indicador_codigo':'K01','metodologia_versao':metodologia,'numero':numero,'anterior':str(revisao_esperada) if revisao_esperada else None,'numero_anterior':numero-1 if numero>1 else None,'motivo':motivo},ator,request_id)
            for papel,orig,kg,v in (('entrada',entrada,en,versao_entrada),('rejeitos',rejeitos,re,versao_rejeitos)):
                self._put(q,'esg_registros',{'revisao_id':str(rid),'serie_id':str(sid),'indicador_codigo':'K01','metodologia_versao':metodologia,'papel':papel,'valor':kg.massa,'valor_original':orig.massa,'valor_original_texto':str(orig.massa) if orig.massa is not None else None,'unidade_original':orig.unidade.value if orig.massa is not None else None,'regra_conversao':('t-kg-x1000-v1' if orig.unidade is UnidadeMassa.TONELADA else 'kg-identidade-v1') if orig.massa is not None else None,'unidade':'kg','fonte_id':orig.fonte.identificador,'fonte_versao':v,'origem':orig.origem.value,'qualidade':orig.qualidade.value,'disponibilidade':'informado' if kg.massa is not None else 'indisponivel'},ator,request_id)
            self._put(q,'esg_resultados_trcs',{'revisao_id':str(rid),'percentual':resultado.percentual,'razao_numerador':_escalar(re.massa,8) if resultado.percentual is not None else None,'razao_denominador':_escalar(en.massa,6) if resultado.percentual is not None else None,'precisao':precisao,'disponibilidade':resultado.disponibilidade.value},ator,request_id)
            fields={'id':str(uuid4()),'revisao_id':str(rid),'validade_metodologica':'pendente','pendencias':json.dumps(avaliacao.pendencias,ensure_ascii=False)}
            if reconciliacao:
                self._fonte(q,reconciliacao.fonte,versao_reconciliacao,ator,request_id)
                fields.update(validade_metodologica='referencia_apresentada',reconciliacao_fonte_id=reconciliacao.fonte.identificador,reconciliacao_fonte_versao=versao_reconciliacao,reconciliacao_ano=reconciliacao.periodo.ano,reconciliacao_mes=reconciliacao.periodo.mes,reconciliacao_perimetro=reconciliacao.perimetro.codigo)
            self._put(q,'esg_avaliacoes',fields,ator,request_id)
            for ev in evidencias:
                self._fonte(q,ev.fonte,ev.versao,ator,request_id)
                self._put(q,'esg_evidencias',{'id':str(uuid4()),'revisao_id':str(rid),'fonte_id':ev.fonte.identificador,'fonte_versao':ev.versao,'tipo':ev.tipo},ator,request_id)
            if numero==1:self._put(q,'esg_correntes',{'serie_id':str(sid),'revisao_id':str(rid),'numero':1},ator,request_id)
            else:
                q.execute("INSERT INTO public.auditoria_tecnica(request_id,modulo,acao,resultado,tipo_ator,usuario_id,origem) VALUES (%s,'ESG','REVISAR','SUCESSO','USUARIO',%s,'ADAPTADOR_MAZ') RETURNING id",(str(request_id),ator));audit=q.fetchone()[0]
                q.execute('UPDATE public.esg_correntes SET revisao_id=%s,numero=%s,criado_por_usuario_id=%s,request_id=%s,auditoria_id=%s WHERE serie_id=%s AND revisao_id=%s AND numero=%s',(str(rid),numero,ator,str(request_id),audit,str(sid),str(revisao_esperada),numero-1))
                if q.rowcount!=1:raise ConflitoMAZ('Compare-and-swap perdeu a disputa.')
            q.execute('SET CONSTRAINTS ALL IMMEDIATE')
        return rid

    @staticmethod
    def _dict(q,table,where,args):
        q.execute('SELECT * FROM public.'+table+' WHERE '+where,args)
        names=[d[0] for d in q.description]
        return [dict(zip(names,row)) for row in q.fetchall()]

    def obter(self,revisao_id):
        with self._tx(leitura=True) as q:
            rev=self._dict(q,'esg_revisoes','id=%s',(str(revisao_id),))
            if len(rev)!=1:raise IntegridadeMAZ('Revisão não encontrada.')
            rev=rev[0];series=self._dict(q,'esg_series','id=%s',(rev['serie_id'],))[0]
            per=self._dict(q,'esg_perimetros','codigo=%s',(series['perimetro_codigo'],))[0];periodo=Periodo(series['ano'],series['mes']);perimetro=Perimetro(per['codigo'],per['descricao'])
            def fonte(fid,version):
                f=self._dict(q,'esg_fonte_versoes','fonte_id=%s AND versao=%s',(fid,version))[0]
                return FonteDocumental(fid,f['referencia'],f['data_documento'])
            method=self._dict(q,'esg_metodologias','indicador_codigo=%s AND versao=%s',('K01',rev['metodologia_versao']))[0]
            raw=bytes(method['especificacao_bytes']);validar_especificacao(raw,method['hash_especificacao'])
            q.execute('SELECT especificacao::text FROM public.esg_metodologias WHERE indicador_codigo=%s AND versao=%s',('K01',rev['metodologia_versao']));documento=json.loads(q.fetchone()[0],parse_float=Decimal)
            if method['especificacao_serializacao']!=CONTRATO_BYTES or json.loads(raw,parse_float=Decimal)!=documento:raise IntegridadeMAZ('Metodologia persistida incoerente.')
            records=sorted(self._dict(q,'esg_registros','revisao_id=%s',(str(revisao_id),)),key=lambda x:x['papel']);parts={}
            for x in records:
                c=ComponenteMassa(x['valor_original'],UnidadeMassa(x['unidade_original'] or 'kg'),periodo,perimetro,fonte(x['fonte_id'],x['fonte_versao']),OrigemValor(x['origem']),QualidadeInformacao(x['qualidade']))
                n=normalizar(c)
                if n.massa!=x['valor'] or x['metodologia_versao']!=rev['metodologia_versao'] or x['serie_id']!=rev['serie_id'] or x['unidade']!='kg':raise IntegridadeMAZ('Componente persistido incoerente.')
                if c.massa is not None:
                    regra='t-kg-x1000-v1' if c.unidade is UnidadeMassa.TONELADA else 'kg-identidade-v1'
                    if Decimal(x['valor_original_texto'])!=c.massa or x['regra_conversao']!=regra or x['disponibilidade']!='informado':raise IntegridadeMAZ('Proveniência original incoerente.')
                elif any(x[k] is not None for k in ('valor_original_texto','unidade_original','regra_conversao')) or x['disponibilidade']=='informado':raise IntegridadeMAZ('Ausência persistida incoerente.')
                parts[x['papel']]=c
            if set(parts)!={'entrada','rejeitos'}:raise IntegridadeMAZ('Componentes incompletos.')
            stored=self._dict(q,'esg_resultados_trcs','revisao_id=%s',(str(revisao_id),))[0]
            result=calcular_trcs(normalizar(parts['rejeitos']),normalizar(parts['entrada']),versao_metodologica=rev['metodologia_versao'],precisao=stored['precisao'])
            if stored['percentual']!=result.percentual or stored['disponibilidade']!=result.disponibilidade.value or stored['publicavel'] or stored['aprovacao']!='nao_autorizada' or stored['qualidade']!='provisorio' or stored['origem']!='calculado' or stored['arredondamento']!='ROUND_HALF_EVEN' or rev['situacao']!='rascunho':raise IntegridadeMAZ('Resultado persistido contradiz R1.')
            if result.percentual_exato is None and (stored['razao_numerador'] is not None or stored['razao_denominador'] is not None):raise IntegridadeMAZ('Razão ausente incoerente.')
            if result.percentual_exato is not None and Fraction(stored['razao_numerador'])/Fraction(stored['razao_denominador'])!=result.percentual_exato:raise IntegridadeMAZ('Razão persistida incoerente.')
            a=self._dict(q,'esg_avaliacoes','revisao_id=%s',(str(revisao_id),))
            if len(a)!=1:raise IntegridadeMAZ('Avaliação ausente/ambígua.')
            a=a[0];recon=None
            if a['reconciliacao_fonte_id']:recon=ReconciliacaoFisica(Periodo(a['reconciliacao_ano'],a['reconciliacao_mes']),Perimetro(a['reconciliacao_perimetro'],perimetro.descricao),fonte(a['reconciliacao_fonte_id'],a['reconciliacao_fonte_versao']))
            assessment=avaliar_metodologia(result,reconciliacao=recon)
            if a['validade_metodologica']!=('referencia_apresentada' if recon else 'pendente') or list(assessment.pendencias)!=a['pendencias'] or a['publicavel'] or a['aprovacao']!='nao_autorizada':raise IntegridadeMAZ('Pendências persistidas incoerentes.')
            ev=tuple(EvidenciaMAZ(fonte(x['fonte_id'],x['fonte_versao']),x['fonte_versao'],x['tipo']) for x in sorted(self._dict(q,'esg_evidencias','revisao_id=%s',(str(revisao_id),)),key=lambda x:(x['fonte_id'],x['fonte_versao'],x['tipo'],str(x['id']))))
            auditorias=[]
            for fato in (rev,*records,stored,a):
                q.execute('SELECT usuario_id,request_id,modulo FROM public.auditoria_tecnica WHERE id=%s',(fato['auditoria_id'],));aud=q.fetchone()
                if not aud or (aud[0],str(aud[1]),aud[2])!=(fato['criado_por_usuario_id'],str(fato['request_id']),'ESG'):raise IntegridadeMAZ('Auditoria persistida incoerente.')
                auditorias.append((fato['auditoria_id'],aud[0],UUID(str(aud[1]))))
            auditorias.sort(key=lambda item:item[0])
            return MedicaoMAZ(UUID(str(rev['serie_id'])),UUID(str(revisao_id)),rev['numero'],rev['metodologia_versao'],parts['entrada'],parts['rejeitos'],result,assessment,ev,raw,method['hash_especificacao'],rev['motivo'],UUID(str(rev['anterior'])) if rev['anterior'] else None,tuple((x['papel'],x['fonte_versao']) for x in records),tuple(auditorias),tuple((x['papel'],x['valor_original_texto'],x['unidade_original'],x['regra_conversao']) for x in records))

    def listar(self,indicador='K01',serie_id=None):
        if indicador not in ('K01','A1'):raise ContratoESGInvalido('Apenas K01/A1 suportado.')
        with self._tx(leitura=True) as q:
            q.execute('SELECT r.id FROM public.esg_revisoes r WHERE r.indicador_codigo=%s'+(' AND r.serie_id=%s' if serie_id else '')+' ORDER BY r.serie_id,r.numero',('K01',str(serie_id)) if serie_id else ('K01',))
            return tuple(UUID(str(x[0])) for x in q.fetchall())

    def corrente(self,serie_id):
        with self._tx(leitura=True) as q:
            q.execute('SELECT revisao_id FROM public.esg_correntes WHERE serie_id=%s',(str(serie_id),));row=q.fetchone()
            if not row:raise IntegridadeMAZ('Série sem corrente.')
            return UUID(str(row[0]))

    def obter_fonte(self,identificador,versao=1):
        with self._tx(leitura=True) as q:
            rows=self._dict(q,'esg_fonte_versoes','fonte_id=%s AND versao=%s',(identificador,versao))
            if len(rows)!=1:raise IntegridadeMAZ('Fonte versionada não encontrada.')
            row=rows[0]
            return FonteDocumental(identificador,row['referencia'],row['data_documento'])

    def obter_metodologia(self,versao):
        with self._tx(leitura=True) as q:
            rows=self._dict(q,'esg_metodologias','indicador_codigo=%s AND versao=%s',('K01',versao))
            if len(rows)!=1:raise IntegridadeMAZ('Metodologia não encontrada.')
            row=rows[0];raw=bytes(row['especificacao_bytes'])
            validar_especificacao(raw,row['hash_especificacao'])
            q.execute('SELECT especificacao::text FROM public.esg_metodologias WHERE indicador_codigo=%s AND versao=%s',('K01',versao))
            if row['especificacao_serializacao']!=CONTRATO_BYTES or json.loads(raw,parse_float=Decimal)!=json.loads(q.fetchone()[0],parse_float=Decimal):raise IntegridadeMAZ('JSON persistido incompatível.')
            return raw,row['hash_especificacao']

    def obter_definicao(self,alias='K01'):
        if alias not in ('K01','A1'):raise ContratoESGInvalido('Apenas K01/A1 suportado.')
        with self._tx(leitura=True) as q:
            rows=self._dict(q,'esg_definicoes','codigo=%s',('K01',))
            if len(rows)!=1:raise IntegridadeMAZ('Definição K01 não encontrada.')
            return {k:rows[0][k] for k in ('codigo','nome','dimensao')}

    def por_request(self,request_id):
        """Consulta para confirmação incerta; não executa retry ou nova escrita."""
        if not isinstance(request_id,UUID):raise ContratoESGInvalido('request_id UUID obrigatório.')
        with self._tx(leitura=True) as q:
            q.execute('SELECT id FROM public.esg_revisoes WHERE request_id=%s ORDER BY id',(str(request_id),))
            return tuple(UUID(str(x[0])) for x in q.fetchall())
