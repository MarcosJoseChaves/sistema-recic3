"""Casos sintéticos; fábrica runtime injetada pelo harness externo."""
from unittest import TestCase
from dataclasses import replace
from datetime import date
from uuid import uuid4
import threading
from modulos.esg.contratos import FonteDocumental,Periodo
from modulos.esg.maz.obrigacoes import Obrigacao,DocumentoObrigacao,AdaptadorObrigacoes
from modulos.esg.maz.persistencia import ContratoESGInvalido,IntegridadeMAZ,ConflitoMAZ
FACTORY=None;ATOR=None;DOC=None;REPORT={}
class TestObrigacoesContrato(TestCase):
 def case(self):return Obrigacao('O14','Caso sintético Obrigação 14',FonteDocumental('f','Fonte sintética'))
 def test_desconhecidos(self):
  c=self.case();self.assertIsNone(c.texto);self.assertIsNone(c.prazo);self.assertIsNone(c.responsavel_designado);self.assertEqual(c.situacao,'desconhecida')
 def test_proposta_sem_designacao(self):self.assertIsNone(replace(self.case(),responsavel_proposto='Equipe sintética').responsavel_designado)
 def test_designacao_sem_fonte(self):
  with self.assertRaises(ContratoESGInvalido):replace(self.case(),responsavel_designado='Equipe sintética')
 def test_cumprimento_nao_e_situacao(self):
  with self.assertRaises(ContratoESGInvalido):replace(self.case(),situacao='cumprida')
 def test_suficiencia_nao_e_apresentacao(self):
  with self.assertRaises(ContratoESGInvalido):DocumentoObrigacao(self.case().fonte,1,'Protocolo sintético',avaliacao='suficiente')
 def test_tipo_prazo(self):
  with self.assertRaises(ContratoESGInvalido):replace(self.case(),prazo='2026-12-08')
 def test_versao_obrigatoria(self):
  with self.assertRaises(ContratoESGInvalido):replace(self.case(),versao_fonte=0)
 def test_fabrica_nao_chamada_por_construtor(self):AdaptadorObrigacoes(lambda:(_ for _ in ()).throw(AssertionError('no IO')))
class TestObrigacoesPostgreSQL(TestCase):
 @classmethod
 def setUpClass(cls):
  if FACTORY is None:raise RuntimeError('Runtime factory must be injected')
  cls.ad=AdaptadorObrigacoes(FACTORY)
 def case(self):return Obrigacao('O14s'+uuid4().hex,'Obrigação 14 — pátio de compostagem [SINTÉTICO; conferir instrumento]',FonteDocumental('f'+uuid4().hex,'Fonte exclusivamente sintética; instrumento original não conferido'),pendencias=('Conferir instrumento original.',),riscos=('Prazo formal desconhecido.',),encaminhamentos=('Obter referência documental autorizada.',))
 def save(self,c,**kw):return (kw.pop('ad',None) or self.ad).salvar(c,ator=ATOR,request_id=kw.pop('request_id',uuid4()),justificativa='Ensaio sintético MAZ04',**kw)
 def rows(self,s,args=()):
  c=FACTORY()
  try:
   with c.cursor() as q:q.execute(s,args);return q.fetchall()
  finally:c.rollback();c.close()
 def test_fluxo_completo_reabertura(self):
  c=self.case();rid=self.save(c);result=AdaptadorObrigacoes(FACTORY).obter(c.identificador);self.assertEqual(result.caso,c);self.assertEqual(result.revisao_id,rid);self.assertEqual(result.cumprimento_juridico,'nao_determinado');self.assertFalse(result.autorizacao_operacional)
  REPORT['example']={'id':c.identificador,'revision':str(rid),'unknown_fields':['texto','instrumento_ref','prazo','responsavel_designado','competencia'],'G2':self.ad.g2(c.identificador),'G1':self.ad.g1()}
 def test_fontes_conflitantes(self):
  c=self.case();self.save(c)
  with self.assertRaises(IntegridadeMAZ):self.save(replace(c,fonte=replace(c.fonte,referencia_documental='Outra identidade')),revisao_esperada=self.ad.obter(c.identificador).revisao_id)
 def test_fonte_versao(self):
  c=self.case();first=self.save(c);f2=replace(c.fonte,referencia_documental='Fonte sintética v2');self.ad.cadastrar_fonte(f2,2,ator=ATOR,request_id=uuid4(),anterior=1);new=self.save(replace(c,fonte=f2,versao_fonte=2),revisao_esperada=first);self.assertEqual(self.ad.obter(c.identificador,new).caso.versao_fonte,2);self.assertEqual(self.ad.obter(c.identificador,first).caso.versao_fonte,1)
 def test_evidencias_manifestacoes_sem_cumprimento(self):
  c=self.case();docs=(DocumentoObrigacao(c.fonte,1,'Protocolo apresentado; não é licença'),DocumentoObrigacao(c.fonte,1,'Avaliação técnica sintética','analise_tecnica','analisado','insuficiente'),DocumentoObrigacao(c.fonte,1,'Referência de decisão documental sintética','decisao_documental','analisado'))
  rid=self.save(replace(c,documentos=docs));r=self.ad.obter(c.identificador,rid);self.assertEqual(len(r.caso.documentos),3);self.assertEqual(r.caso.situacao,'desconhecida');self.assertEqual(r.cumprimento_juridico,'nao_determinado')
 def test_documento_privado_existente(self):
  c=self.case();doc=DocumentoObrigacao(c.fonte,1,'Documento sintético existente',documento_privado_id=DOC[0],sha256=DOC[1]);self.save(replace(c,documentos=(doc,)));self.assertEqual(self.ad.obter(c.identificador).caso.documentos[0].documento_privado_id,DOC[0])
 def test_hash_documento_divergente(self):
  c=self.case();doc=DocumentoObrigacao(c.fonte,1,'Documento sintético existente',documento_privado_id=DOC[0],sha256='b'*64)
  with self.assertRaises(IntegridadeMAZ):self.save(replace(c,documentos=(doc,)))
 def test_revisao_historico_conflito(self):
  c=self.case();first=self.save(c);old=self.ad.obter(c.identificador);second=self.save(replace(c,situacao='em_acompanhamento'),revisao_esperada=first);self.assertEqual(self.ad.obter(c.identificador,first),old);self.assertEqual(self.ad.obter(c.identificador).numero,2);self.assertEqual(len(self.ad.historico(c.identificador)),2)
  with self.assertRaises(ConflitoMAZ):self.save(c,revisao_esperada=first)
 def test_conflito_concorrente(self):
  c=self.case();first=self.save(c);barrier=threading.Barrier(2);out=[]
  class Race(AdaptadorObrigacoes):
   def _put(self,q,t,values,*a):
    if t=='esg_obrigacao_revisoes' and values['numero']==2:barrier.wait(4)
    return super()._put(q,t,values,*a)
  def work():
   try:out.append(('ok',str(self.save(c,ad=Race(FACTORY),revisao_esperada=first))))
   except ConflitoMAZ:out.append(('conflict',None))
   except Exception as e:out.append(('error',type(e).__name__))
  ts=[threading.Thread(target=work) for _ in range(2)]
  for t in ts:t.start()
  for t in ts:t.join(10)
  self.assertFalse(any(t.is_alive() for t in ts));self.assertEqual(sorted(x[0] for x in out),['conflict','ok']);REPORT['concurrency']=out
 def test_rollback_auditoria(self):
  c=self.case();before=self.rows('SELECT count(*) FROM auditoria_tecnica')
  class Broken(AdaptadorObrigacoes):
   def _put(self,q,t,*a):
    super()._put(q,t,*a)
    if t=='esg_obrigacao_documentos':raise RuntimeError('synthetic partial failure')
  with self.assertRaises(IntegridadeMAZ):self.save(replace(c,documentos=(DocumentoObrigacao(c.fonte,1,'Sintético'),)),ad=Broken(FACTORY))
  self.assertEqual(self.rows('SELECT count(*) FROM auditoria_tecnica'),before);self.assertFalse(self.rows('SELECT id FROM esg_obrigacoes WHERE id=%s',(c.identificador,)))
 def test_autoria(self):
  c=self.case();req=uuid4();self.save(c,request_id=req);r=self.ad.obter(c.identificador);self.assertEqual(r.ator,ATOR);self.assertEqual(r.request_id,req);self.assertEqual(self.rows('SELECT usuario_id,request_id FROM auditoria_tecnica WHERE id=%s',(r.auditoria_id,))[0],(ATOR,str(req)))
 def test_g1_g2_indisponivel(self):
  c=self.case();self.save(c);g=self.ad.g2(c.identificador);self.assertEqual(g['campos']['prazo'],'indisponivel');self.assertEqual(g['campos']['texto'],'indisponivel');self.assertLess(g['preenchidos'],g['total_campos']);self.assertFalse(self.ad.g1()['plano_15_itens_comprovado']);self.assertIsNone(g['meta_institucional'])
 def test_responsavel_proposto_designado(self):
  c=replace(self.case(),responsavel_proposto='Equipe sintética');first=self.save(c);self.assertIsNone(self.ad.obter(c.identificador).caso.responsavel_designado);self.save(replace(c,responsavel_designado='Outra equipe sintética',designacao_fonte=c.fonte),revisao_esperada=first);self.assertEqual(self.ad.obter(c.identificador).caso.responsavel_proposto,'Equipe sintética')
 def test_runtime_sem_truncate_nem_update(self):
  for s in ('TRUNCATE esg_obrigacao_revisoes','UPDATE esg_obrigacao_revisoes SET cumprimento_juridico=\'cumprido\''):
   c=FACTORY()
   try:
    with self.assertRaises(Exception) as e:
     with c.cursor() as q:q.execute(s)
    self.assertEqual(e.exception.pgcode,'42501')
   finally:c.rollback();c.close()
 def test_banco_nega_cumprimento_juridico(self):
  class Forged(AdaptadorObrigacoes):
   def _put(self,q,t,values,*args):
    if t=='esg_obrigacao_revisoes':values={**values,'cumprimento_juridico':'cumprido'}
    return super()._put(q,t,values,*args)
  with self.assertRaises(IntegridadeMAZ):self.save(self.case(),ad=Forged(FACTORY))
 def test_banco_nega_autorizacao_operacional(self):
  class Forged(AdaptadorObrigacoes):
   def _put(self,q,t,values,*args):
    if t=='esg_obrigacao_revisoes':values={**values,'autorizacao_operacional':True}
    return super()._put(q,t,values,*args)
  with self.assertRaises(IntegridadeMAZ):self.save(self.case(),ad=Forged(FACTORY))
 def test_campos_conhecidos_sinteticos(self):
  c=self.case();c=replace(c,instrumento_ref='Instrumento fictício de teste',texto='Texto exclusivamente sintético; sem validade jurídica.',competencia=Periodo(2051,3),prazo=date(2051,4,30),responsavel_proposto='Equipe sintética proposta',responsavel_designado='Equipe sintética designada',designacao_fonte=c.fonte,situacao='pendente')
  self.save(c);r=self.ad.obter(c.identificador);self.assertEqual(r.caso,c);self.assertEqual(self.ad.g2(c.identificador)['campos']['prazo'],'preenchido');self.assertFalse(r.autorizacao_operacional)
