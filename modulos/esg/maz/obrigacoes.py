"""Acompanhamento documental; não determina cumprimento jurídico."""
from dataclasses import dataclass,field
from datetime import date
from uuid import UUID,uuid4
from ..contratos import FonteDocumental,Periodo
from ..validacoes import ContratoESGInvalido,validar_codigo
from .persistencia import AdaptadorMAZ,IntegridadeMAZ,ConflitoMAZ

def _texto(value,nome,obrigatorio=False):
    if value is None and not obrigatorio:return
    if not isinstance(value,str) or not value.strip():raise ContratoESGInvalido(nome+' inválido.')

def _versao(v):
    if type(v)is not int or v<1:raise ContratoESGInvalido('Versão documental positiva obrigatória.')

@dataclass(frozen=True)
class DocumentoObrigacao:
    fonte: FonteDocumental
    versao: int
    descricao: str
    tipo: str='evidencia'
    estado: str='apresentado'
    avaliacao: str='nao_avaliada'
    documento_privado_id: int|None=None
    sha256: str|None=None
    def __post_init__(self):
        if not isinstance(self.fonte,FonteDocumental):raise ContratoESGInvalido('Fonte obrigatória.')
        _versao(self.versao);_texto(self.descricao,'Descrição',True)
        if self.tipo not in ('evidencia','analise_tecnica','decisao_documental') or self.estado not in ('apresentado','analisado') or self.avaliacao not in ('nao_avaliada','suficiente','insuficiente'):raise ContratoESGInvalido('Classificação documental inválida.')
        if self.avaliacao!='nao_avaliada' and (self.tipo!='analise_tecnica' or self.estado!='analisado'):raise ContratoESGInvalido('Suficiência exige avaliação técnica analisada.')
        if self.documento_privado_id is not None and (type(self.documento_privado_id)is not int or self.documento_privado_id<=0):raise ContratoESGInvalido('Documento privado inválido.')
        if self.sha256 is not None:
            import re
            if not isinstance(self.sha256,str) or not re.fullmatch('[0-9a-f]{64}',self.sha256):raise ContratoESGInvalido('SHA-256 documental inválido.')

@dataclass(frozen=True)
class Obrigacao:
    identificador: str
    titulo: str
    fonte: FonteDocumental
    versao_fonte: int=1
    instrumento_ref: str|None=None
    texto: str|None=None
    competencia: Periodo|None=None
    prazo: date|None=None
    responsavel_proposto: str|None=None
    responsavel_designado: str|None=None
    designacao_fonte: FonteDocumental|None=None
    designacao_versao: int=1
    situacao: str='desconhecida'
    documentos: tuple=()
    pendencias: tuple=()
    riscos: tuple=()
    encaminhamentos: tuple=()
    def __post_init__(self):
        validar_codigo(self.identificador,'obrigacao.id');_texto(self.titulo,'Título',True)
        if not isinstance(self.fonte,FonteDocumental):raise ContratoESGInvalido('Fonte obrigatória.')
        _versao(self.versao_fonte);_versao(self.designacao_versao)
        for n in ('instrumento_ref','texto','responsavel_proposto','responsavel_designado'):_texto(getattr(self,n),n)
        if self.competencia is not None and not isinstance(self.competencia,Periodo):raise ContratoESGInvalido('Competência inválida.')
        if self.prazo is not None and type(self.prazo)is not date:raise ContratoESGInvalido('Prazo deve ser data conhecida, ou None.')
        if self.situacao not in ('desconhecida','em_acompanhamento','pendente','em_analise'):raise ContratoESGInvalido('Situação não admite cumprimento automático.')
        if (self.responsavel_designado is None)!=(self.designacao_fonte is None):raise ContratoESGInvalido('Designação formal exige responsável e fonte.')
        if self.designacao_fonte is not None and not isinstance(self.designacao_fonte,FonteDocumental):raise ContratoESGInvalido('Fonte de designação inválida.')
        if type(self.documentos)is not tuple or not all(isinstance(x,DocumentoObrigacao) for x in self.documentos):raise ContratoESGInvalido('Documentos devem ser tupla de referências.')
        for n in ('pendencias','riscos','encaminhamentos'):
            if type(getattr(self,n))is not tuple:raise ContratoESGInvalido(n+' deve ser tupla.')
            for t in getattr(self,n):_texto(t,n,True)

@dataclass(frozen=True)
class CasoObrigacao:
    caso: Obrigacao
    revisao_id: UUID
    numero: int
    anterior: UUID|None
    justificativa: str
    criado_em: object
    ator: int
    request_id: UUID
    auditoria_id: int
    cumprimento_juridico: str=field(default='nao_determinado',init=False)
    autorizacao_operacional: bool=field(default=False,init=False)

class AdaptadorObrigacoes(AdaptadorMAZ):
    """Reutiliza conexão/transação/autoria/fontes do adaptador MAZ homologado."""
    def salvar(self,caso,*,ator,request_id,justificativa,revisao_esperada=None):
        if not isinstance(caso,Obrigacao):raise ContratoESGInvalido('Obrigação tipada obrigatória.')
        self._contexto(ator,request_id);_texto(justificativa,'Justificativa',True)
        if revisao_esperada is not None and not isinstance(revisao_esperada,UUID):raise ContratoESGInvalido('Revisão esperada deve ser UUID.')
        rid=uuid4()
        with self._tx() as q:
            q.execute('SELECT revisao_id,numero FROM public.esg_obrigacao_correntes WHERE obrigacao_id=%s',(caso.identificador,));current=q.fetchone()
            if current:
                if str(current[0])!=str(revisao_esperada):raise ConflitoMAZ('Caso foi alterado; reler corrente.')
                numero=current[1]+1
            else:
                if revisao_esperada is not None:raise ConflitoMAZ('Caso/revisão não encontrado.')
                numero=1;self._put(q,'esg_obrigacoes',{'id':caso.identificador},ator,request_id)
            self._fonte(q,caso.fonte,caso.versao_fonte,ator,request_id)
            if caso.designacao_fonte:self._fonte(q,caso.designacao_fonte,caso.designacao_versao,ator,request_id)
            values={'id':str(rid),'obrigacao_id':caso.identificador,'numero':numero,'anterior':str(revisao_esperada) if revisao_esperada else None,'titulo':caso.titulo,'instrumento_ref':caso.instrumento_ref,'fonte_id':caso.fonte.identificador,'fonte_versao':caso.versao_fonte,'texto':caso.texto,'ano':caso.competencia.ano if caso.competencia else None,'mes':caso.competencia.mes if caso.competencia else None,'prazo':caso.prazo,'prazo_conhecido':caso.prazo is not None,'responsavel_proposto':caso.responsavel_proposto,'responsavel_designado':caso.responsavel_designado,'designacao_fonte_id':caso.designacao_fonte.identificador if caso.designacao_fonte else None,'designacao_fonte_versao':caso.designacao_versao if caso.designacao_fonte else None,'situacao':caso.situacao,'pendencias':list(caso.pendencias),'riscos':list(caso.riscos),'encaminhamentos':list(caso.encaminhamentos),'justificativa':justificativa}
            self._put(q,'esg_obrigacao_revisoes',values,ator,request_id)
            for doc in caso.documentos:
                self._fonte(q,doc.fonte,doc.versao,ator,request_id)
                self._put(q,'esg_obrigacao_documentos',{'id':str(uuid4()),'revisao_id':str(rid),'fonte_id':doc.fonte.identificador,'fonte_versao':doc.versao,'documento_privado_id':doc.documento_privado_id,'sha256':doc.sha256,'tipo':doc.tipo,'descricao':doc.descricao,'estado':doc.estado,'avaliacao':doc.avaliacao},ator,request_id)
            if numero==1:self._put(q,'esg_obrigacao_correntes',{'obrigacao_id':caso.identificador,'revisao_id':str(rid),'numero':1},ator,request_id)
            else:
                q.execute("INSERT INTO public.auditoria_tecnica(request_id,modulo,acao,resultado,tipo_ator,usuario_id,origem) VALUES (%s,'ESG','REVISAR','SUCESSO','USUARIO',%s,'OBRIGACOES_MAZ') RETURNING id",(str(request_id),ator));aid=q.fetchone()[0]
                q.execute('UPDATE public.esg_obrigacao_correntes SET revisao_id=%s,numero=%s,criado_por_usuario_id=%s,request_id=%s,auditoria_id=%s WHERE obrigacao_id=%s AND revisao_id=%s AND numero=%s',(str(rid),numero,ator,str(request_id),aid,caso.identificador,str(revisao_esperada),numero-1))
                if q.rowcount!=1:raise ConflitoMAZ('Compare-and-swap perdido.')
            q.execute('SET CONSTRAINTS ALL IMMEDIATE')
        return rid

    def obter(self,identificador,revisao_id=None):
        with self._tx(leitura=True) as q:
            if revisao_id is None:
                q.execute('SELECT revisao_id FROM public.esg_obrigacao_correntes WHERE obrigacao_id=%s',(identificador,));row=q.fetchone()
                if not row:raise IntegridadeMAZ('Caso não encontrado.')
                revisao_id=row[0]
            records=self._dict(q,'esg_obrigacao_revisoes','id=%s AND obrigacao_id=%s',(str(revisao_id),identificador))
            if len(records)!=1:raise IntegridadeMAZ('Revisão não encontrada.')
            r=records[0]
            if r['cumprimento_juridico']!='nao_determinado' or r['autorizacao_operacional']:raise IntegridadeMAZ('Estado institucional incompatível.')
            def fonte(fid,v):
                f=self._dict(q,'esg_fonte_versoes','fonte_id=%s AND versao=%s',(fid,v))
                if len(f)!=1:raise IntegridadeMAZ('Fonte ausente.')
                return FonteDocumental(fid,f[0]['referencia'],f[0]['data_documento'])
            docs=[]
            for d in sorted(self._dict(q,'esg_obrigacao_documentos','revisao_id=%s',(str(revisao_id),)),key=lambda x:(x['fonte_id'],x['fonte_versao'],x['tipo'],str(x['id']))):
                if d['eficacia_juridica']:raise IntegridadeMAZ('Eficácia jurídica não atribuível automaticamente.')
                docs.append(DocumentoObrigacao(fonte(d['fonte_id'],d['fonte_versao']),d['fonte_versao'],d['descricao'],d['tipo'],d['estado'],d['avaliacao'],d['documento_privado_id'],d['sha256']))
            caso=Obrigacao(identificador,r['titulo'],fonte(r['fonte_id'],r['fonte_versao']),r['fonte_versao'],r['instrumento_ref'],r['texto'],Periodo(r['ano'],r['mes']) if r['ano'] else None,r['prazo'],r['responsavel_proposto'],r['responsavel_designado'],fonte(r['designacao_fonte_id'],r['designacao_fonte_versao']) if r['designacao_fonte_id'] else None,r['designacao_fonte_versao'] or 1,r['situacao'],tuple(docs),tuple(r['pendencias']),tuple(r['riscos']),tuple(r['encaminhamentos']))
            return CasoObrigacao(caso,UUID(str(r['id'])),r['numero'],UUID(str(r['anterior'])) if r['anterior'] else None,r['justificativa'],r['criado_em'],r['criado_por_usuario_id'],UUID(str(r['request_id'])),r['auditoria_id'])

    def listar(self):
        with self._tx(leitura=True) as q:
            q.execute('SELECT r.obrigacao_id,r.titulo,r.situacao,r.prazo,r.prazo_conhecido,r.numero FROM public.esg_obrigacao_revisoes r JOIN public.esg_obrigacao_correntes c ON c.revisao_id=r.id ORDER BY r.obrigacao_id')
            return tuple(dict(zip(('identificador','titulo','situacao','prazo','prazo_conhecido','numero'),row)) for row in q.fetchall())

    def historico(self,identificador):
        with self._tx(leitura=True) as q:
            q.execute('SELECT id,numero,anterior,criado_em,criado_por_usuario_id,justificativa,situacao FROM public.esg_obrigacao_revisoes WHERE obrigacao_id=%s ORDER BY numero',(identificador,))
            return tuple(dict(zip(('id','numero','anterior','criado_em','ator','justificativa','situacao'),row)) for row in q.fetchall())

    def g1(self):
        casos=self.listar()
        return {'escopo':'somente_casos_cadastrados','total_cadastrado':len(casos),'situacoes':tuple((x['identificador'],x['situacao']) for x in casos),'plano_15_itens_comprovado':False,'meta_institucional':None}

    def g2(self,identificador,revisao_id=None):
        return self.g2_registro(self.obter(identificador,revisao_id))

    @staticmethod
    def g2_registro(r):
        """Calcula somente o snapshot consultado, sem reler a revisão corrente."""
        c=r.caso;identificador=c.identificador
        campos={n:('indisponivel' if getattr(c,n) is None else 'preenchido') for n in ('instrumento_ref','texto','competencia','prazo','responsavel_proposto','responsavel_designado')}
        campos.update(titulo='preenchido',fonte='preenchido',situacao='indisponivel' if c.situacao=='desconhecida' else 'preenchido',documentos='preenchido' if c.documentos else 'nao_informado',pendencias='preenchido' if c.pendencias else 'nao_informado',riscos='preenchido' if c.riscos else 'nao_informado',encaminhamentos='preenchido' if c.encaminhamentos else 'nao_informado')
        return {'escopo':'campos_do_caso','identificador':identificador,'revisao_id':str(r.revisao_id),'numero':r.numero,'indisponiveis':sum(v=='indisponivel' for v in campos.values()),'nao_informados':sum(v=='nao_informado' for v in campos.values()),'tratados':sum(v!='nao_informado' for v in campos.values()),'campos':campos,'preenchidos':sum(v=='preenchido' for v in campos.values()),'total_campos':len(campos),'cumprimento_juridico':'nao_determinado','meta_institucional':None}
