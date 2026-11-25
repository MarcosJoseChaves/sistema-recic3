"""Integração real. O harness injeta FACTORY/ATOR; sem DSN ou conexão implícita."""
import hashlib,threading
from dataclasses import replace
from decimal import Decimal
from unittest import TestCase
from uuid import uuid4
from modulos.esg.contratos import FonteDocumental,Periodo,Perimetro
from modulos.esg.maz.modelos import ComponenteMassa,UnidadeMassa,OrigemValor
from modulos.esg.maz.trcs import ResultadoTRCS,AvaliacaoTRCS
from modulos.esg.maz.persistencia import (AdaptadorMAZ,EvidenciaMAZ,ConflitoMAZ,IntegridadeMAZ,ContratoESGInvalido,ConfirmacaoIncertaMAZ)
FACTORY=None
ATOR=None
REPORT={}
class TestAdaptadorPostgreSQL(TestCase):
    @classmethod
    def setUpClass(cls):
        if FACTORY is None:raise RuntimeError('Real runtime factory must be explicitly injected')
        cls.ad=AdaptadorMAZ(FACTORY);cls.token='c'+uuid4().hex;cls.fonte=FonteDocumental(cls.token,'Fonte exclusivamente sintetica 03C')
        cls.raw=b'{"formula":"TRCS_BRUTA_100_REJEITOS_ENTRADA","contrato_numerico":"R2-kg-1e9-6","extra":1.123456789012345678901234567890}'
        cls.digest=hashlib.sha256(cls.raw).hexdigest();cls.version=cls.token
        cls.ad.cadastrar_metodologia(cls.version,cls.raw,cls.digest,cls.fonte,ator=ATOR,request_id=uuid4())
    def setUp(self):self.per=Perimetro('p'+uuid4().hex,'Perimetro 03C sintetico')
    def c(self,v,unit=UnidadeMassa.KG):return ComponenteMassa(None if v is None else Decimal(v),unit,Periodo(2051,3),self.per,self.fonte,OrigemValor.DECLARADO)
    def save(self,ad=None,**kw):
        return (ad or self.ad).registrar(kw.pop('entrada',self.c('1',UnidadeMassa.TONELADA)),kw.pop('rejeitos',self.c('0.0025',UnidadeMassa.TONELADA)),self.version,ator=ATOR,request_id=kw.pop('request_id',uuid4()),motivo='Caso sintetico adaptador 03C',**kw)
    def rows(self,text,args=()):
        c=FACTORY()
        try:
            with c.cursor() as q:q.execute(text,args);return q.fetchall()
        finally:c.rollback();c.close()
    def test_fluxo_integral_reabertura(self):
        rid=self.save(evidencias=(EvidenciaMAZ(self.fonte,1,'Expedicao sintetica'),));m=self.ad.obter(rid)
        self.assertIsInstance(m.resultado,ResultadoTRCS);self.assertIsInstance(m.avaliacao,AvaliacaoTRCS)
        self.assertEqual(m.resultado.percentual,Decimal('.25'));self.assertEqual(m.entrada_original.massa,Decimal(1))
        self.assertEqual(m.especificacao_bytes,self.raw);self.assertEqual(len(m.evidencias),1)
        self.assertFalse(m.avaliacao.publicavel_oficialmente);self.assertEqual(self.ad.corrente(m.serie_id),rid)
        self.assertEqual(AdaptadorMAZ(FACTORY).obter(rid),m)
        REPORT['example']={'revision':str(rid),'series':str(m.serie_id),'percent':str(m.resultado.percentual),'exact':str(m.resultado.percentual_exato),'method':m.metodologia,'audit_ids':[a[0] for a in m.auditorias]}
    def test_fonte_conflitante(self):
        with self.assertRaises(IntegridadeMAZ):self.ad.cadastrar_fonte(FonteDocumental(self.fonte.identificador,'Referencia diferente'),1,ator=ATOR,request_id=uuid4())
    def test_fonte_sucessora_recuperavel(self):
        f=FonteDocumental('f'+uuid4().hex,'Versao 1');self.ad.cadastrar_fonte(f,1,ator=ATOR,request_id=uuid4());f2=replace(f,referencia_documental='Versao 2');self.ad.cadastrar_fonte(f2,2,ator=ATOR,request_id=uuid4(),anterior=1)
        self.assertEqual(self.ad.obter_fonte(f.identificador,1),f);self.assertEqual(self.ad.obter_fonte(f.identificador,2),f2)
    def test_hash_metodologico(self):
        with self.assertRaises(ContratoESGInvalido):self.ad.cadastrar_metodologia('bad',self.raw,'0'*64,self.fonte,ator=ATOR,request_id=uuid4())
        self.assertEqual(self.ad.obter_metodologia(self.version),(self.raw,self.digest))
    def test_ausencia(self):
        m=self.ad.obter(self.save(rejeitos=self.c(None)));self.assertIsNone(m.resultado.percentual);self.assertIsNone(m.rejeitos_original.massa)
    def test_zero(self):self.assertEqual(self.ad.obter(self.save(rejeitos=self.c('0'))).resultado.percentual,Decimal(0))
    def test_excesso(self):
        for v in ('1e10','0.0000001'):
            with self.assertRaises(ContratoESGInvalido):self.save(rejeitos=self.c(v))
    def test_escopo(self):
        for c in (replace(self.c('1'),periodo=Periodo(2052)),replace(self.c('1'),perimetro=Perimetro('outro','Outro'))):
            with self.assertRaises(ContratoESGInvalido):self.save(rejeitos=c)
    def test_rollback_parcial_auditoria(self):
        class Falha(AdaptadorMAZ):
            def _put(self,q,t,*a,**kw):
                super()._put(q,t,*a,**kw)
                if t=='esg_evidencias':raise RuntimeError('synthetic after partial graph')
        before=self.rows('SELECT count(*) FROM auditoria_tecnica');revs=self.rows('SELECT count(*) FROM esg_revisoes')
        with self.assertRaises(IntegridadeMAZ):self.save(ad=Falha(FACTORY),evidencias=(EvidenciaMAZ(self.fonte,1,'Teste'),))
        self.assertEqual(self.rows('SELECT count(*) FROM auditoria_tecnica'),before);self.assertEqual(self.rows('SELECT count(*) FROM esg_revisoes'),revs)
    def test_revisao_e_conflito(self):
        first=self.save();old=self.ad.obter(first);second=self.save(serie_id=old.serie_id,revisao_esperada=first)
        self.assertEqual(self.ad.obter(first),old);self.assertEqual(self.ad.obter(second).anterior,first)
        self.assertEqual(self.ad.listar(serie_id=old.serie_id),(first,second))
        with self.assertRaises(ConflitoMAZ):self.save(serie_id=old.serie_id,revisao_esperada=first)
        REPORT['revisions']=[str(first),str(second)]
    def test_alias(self):self.assertEqual(self.ad.listar('A1'),self.ad.listar('K01'));self.assertEqual(self.ad.obter_definicao('A1')['codigo'],'K01')
    def test_autoria(self):
        request=uuid4();m=self.ad.obter(self.save(request_id=request));self.assertTrue(all(actor==ATOR and rid==request for _,actor,rid in m.auditorias))
    def test_sem_promocao(self):
        m=self.ad.obter(self.save());self.assertFalse(m.avaliacao.publicavel_oficialmente)
        with self.assertRaises(TypeError):self.save(publicavel=True)
    def test_corrupcao_simulada_na_leitura(self):
        rid=self.save()
        class Corrompida(AdaptadorMAZ):
            @staticmethod
            def _dict(q,t,w,args):
                r=AdaptadorMAZ._dict(q,t,w,args)
                if t=='esg_resultados_trcs':r[0]['percentual']=Decimal('99')
                return r
        with self.assertRaises(IntegridadeMAZ):Corrompida(FACTORY).obter(rid)
    def test_concorrencia_real(self):
        rid=self.save();old=self.ad.obter(rid);barrier=threading.Barrier(2);out=[]
        class Racing(AdaptadorMAZ):
            def _put(self,q,t,fields,*args):
                if t=='esg_revisoes' and fields['numero']==2:barrier.wait(4)
                return super()._put(q,t,fields,*args)
        def worker():
            try:out.append(('ok',self.save(ad=Racing(FACTORY),serie_id=old.serie_id,revisao_esperada=rid)))
            except ConflitoMAZ:out.append(('conflict',None))
            except Exception as e:out.append(('error',type(e).__name__))
        threads=[threading.Thread(target=worker) for _ in range(2)]
        for t in threads:t.start()
        for t in threads:t.join(10)
        self.assertFalse(any(t.is_alive() for t in threads));self.assertEqual(sorted(x[0] for x in out),['conflict','ok'])
        self.assertEqual(len(self.ad.listar(serie_id=old.serie_id)),2);self.assertEqual(self.ad.obter(rid),old)
        REPORT['race']=[(x,str(y)) for x,y in out]
    def test_contexto_hostil_gravacao(self):
        from decimal import localcontext,Inexact,Rounded
        with localcontext() as ctx:
            ctx.prec=2;ctx.traps[Inexact]=True;ctx.traps[Rounded]=True
            rid=self.save(entrada=self.c('1.234567891',UnidadeMassa.TONELADA),rejeitos=self.c('0.000000001',UnidadeMassa.TONELADA))
            m=self.ad.obter(rid)
        self.assertEqual(m.resultado.entrada.massa,Decimal('1234.567891'))
        self.assertEqual(m.resultado.rejeitos.massa,Decimal('0.000001'))
    def test_reconciliacao_compativel(self):
        from modulos.esg.maz.modelos import ReconciliacaoFisica
        recon=ReconciliacaoFisica(Periodo(2051,3),self.per,self.fonte)
        m=self.ad.obter(self.save(reconciliacao=recon))
        self.assertEqual(m.avaliacao.reconciliacao,recon)
        self.assertTrue(any('conteúdo ainda não verificado' in p for p in m.avaliacao.pendencias))
    def test_reconciliacao_incompativel(self):
        from modulos.esg.maz.modelos import ReconciliacaoFisica
        with self.assertRaises(ContratoESGInvalido):self.save(reconciliacao=ReconciliacaoFisica(Periodo(2052),self.per,self.fonte))
    def test_versao_metodologica_inalteravel(self):
        rid=self.save();m=self.ad.obter(rid)
        with self.assertRaises(IntegridadeMAZ):self.ad.registrar(self.c('1'),self.c('2'),self.version+'x',ator=ATOR,request_id=uuid4(),motivo='Nao pode mudar versao',serie_id=m.serie_id,revisao_esperada=rid)
    def test_commit_incerto_real_sem_retry(self):
        counter=[]
        class Connection:
            def __init__(self,c):self.c=c
            def __getattr__(self,n):return getattr(self.c,n)
            def commit(self):self.c.commit();counter.append(1);raise OSError('simulated lost acknowledgement')
        request=uuid4()
        with self.assertRaises(ConfirmacaoIncertaMAZ):self.save(ad=AdaptadorMAZ(lambda:Connection(FACTORY())),request_id=request)
        self.assertEqual(counter,[1])
        rows=self.rows('SELECT id FROM esg_revisoes WHERE request_id=%s',(str(request),))
        self.assertEqual(len(rows),1);self.assertEqual(self.ad.obter(rows[0][0]).numero,1);self.assertEqual(self.ad.por_request(request),(self.ad.obter(rows[0][0]).revisao_id,))
    def test_identidade_runtime_e_ledger_protegido(self):
        row=self.rows('SELECT current_user,session_user,rolsuper FROM pg_roles WHERE rolname=current_user')[0]
        self.assertEqual(row,('maz_runtime_03b4','maz_runtime_03b4',False))
    def test_qualidade_validada_sem_adaptador_documental(self):
        from modulos.esg.maz.modelos import QualidadeInformacao
        with self.assertRaises(ContratoESGInvalido):self.save(rejeitos=replace(self.c('1'),qualidade=QualidadeInformacao.VALIDADO))

    def test_search_path_hostil_factory(self):
        def factory():
            c=FACTORY()
            with c.cursor() as q:q.execute('SET search_path=pg_catalog')
            c.commit()
            return c
        ad=AdaptadorMAZ(factory)
        rid=self.save(ad=ad)
        self.assertEqual(ad.obter(rid).resultado.percentual,Decimal('.25'))
        original=ad.obter(rid)
        new=self.save(ad=ad,serie_id=original.serie_id,revisao_esperada=rid)
        self.assertEqual(ad.corrente(original.serie_id),new)
        self.assertEqual(ad.obter(rid),original)

    def test_obter_deterministico_com_cursor_invertido(self):
        evidencias=tuple(EvidenciaMAZ(self.fonte,1,tipo) for tipo in ('Zeta','Alfa','Meio'))
        rid=self.save(evidencias=evidencias)
        esperado=self.ad.obter(rid)
        class OrdemInversa(AdaptadorMAZ):
            @staticmethod
            def _dict(q,table,where,args):
                return list(reversed(AdaptadorMAZ._dict(q,table,where,args)))
        recuperado=OrdemInversa(FACTORY).obter(rid)
        self.assertEqual(recuperado,esperado)
        self.assertEqual(tuple(ev.tipo for ev in recuperado.evidencias),('Alfa','Meio','Zeta'))
        self.assertEqual(tuple(papel for papel,_ in recuperado.versoes_fontes),('entrada','rejeitos'))
        self.assertEqual(tuple(x[0] for x in recuperado.proveniencia),('entrada','rejeitos'))
        self.assertEqual(tuple(a[0] for a in recuperado.auditorias),tuple(sorted(a[0] for a in recuperado.auditorias)))
