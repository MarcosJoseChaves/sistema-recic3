"""Integração Flask/PostgreSQL; dependências explícitas do harness local."""
from unittest import TestCase
from uuid import uuid4
import re
APP=None;BUILDER=None;PASSWORD=None;TRCS=None;OBL=None;ACTOR=None;FACTORY=None
class TestWebMAZ(TestCase):
 def setUp(self):
  if APP is None:self.skipTest('Requer injecao explicita pelo executor PostgreSQL isolado MAZ05A.')
  self.client=APP.test_client()
 def token(self,path='/login'):
  r=self.client.get(path);self.assertEqual(r.status_code,200);return re.search(r'name="csrf_token" value="([^"]+)"',r.get_data(as_text=True)).group(1)
 def login(self):
  r=self.client.post('/login',data={'csrf_token':self.token(),'password':PASSWORD});self.assertEqual(r.status_code,303)
 def measure(self):
  self.login();return self.client.post('/esg/maz/trcs/nova',data={'csrf_token':self.token('/esg/maz/trcs/nova'),'ano':'2053','mes':'4','entrada':'100','rejeitos':'25','unidade':'kg','fonte':'Fonte sintética de teste web','perimetro':'Recorte web sintético','motivo':'Teste web','ator':'999999'})
 def obligation(self):
  self.login();name='web'+uuid4().hex
  r=self.client.post('/esg/maz/obrigacoes/nova',data={'csrf_token':self.token('/esg/maz/obrigacoes/nova'),'identificador':name,'titulo':'Obrigação 14 sintética web','fonte':'Instrumento não conferido','justificativa':'Teste web','situacao':'desconhecida','pendencias':'Conferir fonte original.','documento':'Protocolo sintético; não é licença','tipo_documento':'evidencia','estado_documento':'apresentado','avaliacao':'nao_avaliada','ator':'999999'})
  self.assertEqual(r.status_code,303);return name,r
 def test_acesso_anonimo(self):
  for p in ('/esg/maz/','/esg/maz/catalogo','/esg/maz/trcs','/esg/maz/obrigacoes','/esg/maz/materialidade'):self.assertEqual(self.client.get(p).status_code,302)
 def test_autorizacao_consultas(self):
  with self.client.session_transaction() as s:s['_user_id']='readonly';s['_fresh']=True
  self.assertEqual(self.client.get('/esg/maz/').status_code,403);self.assertEqual(self.client.get('/esg/maz/trcs').status_code,403)
 def test_csrf_login_e_gravacao(self):
  self.assertEqual(self.client.post('/login',data={'password':PASSWORD}).status_code,400);self.login();self.assertEqual(self.client.post('/esg/maz/trcs/nova',data={}).status_code,400)
 def test_navegacao_catalogo_materialidade(self):
  self.login()
  for p in ('/esg/maz/','/esg/maz/catalogo','/esg/maz/materialidade','/esg/maz/trcs','/esg/maz/obrigacoes','/esg'):
   r=self.client.get(p);self.assertEqual(r.status_code,200,p)
  html=self.client.get('/esg/maz/catalogo').get_data(as_text=True)
  for c in ('K01','K04c','K09d','K11'):self.assertIn(c,html)
 def test_trcs_persistencia_e_ator(self):
  r=self.measure();self.assertEqual(r.status_code,303);rid=r.location.rsplit('/',1)[1];m=TRCS.obter(rid);self.assertEqual(str(m.resultado.percentual),'25');self.assertTrue(all(a[1]==ACTOR for a in m.auditorias));self.assertEqual(self.client.get(r.location).status_code,200)
 def test_trcs_revisao_conflito_historico(self):
  r=self.measure();rid=r.location.rsplit('/',1)[1];path='/esg/maz/trcs/'+rid+'/revisar';data={'csrf_token':self.token(path),'esperada':rid,'entrada':'100','rejeitos':'20','unidade':'kg','motivo':'Revisão sintética'}
  new=self.client.post(path,data=data);self.assertEqual(new.status_code,303);self.assertEqual(self.client.post(path,data=data).status_code,409);self.assertEqual(self.client.get(r.location).status_code,200)
 def test_obrigacao_campos_desconhecidos(self):
  name,r=self.obligation();m=OBL.obter(name);self.assertIsNone(m.caso.prazo);self.assertEqual(m.ator,ACTOR);self.assertEqual(m.cumprimento_juridico,'nao_determinado');self.assertFalse(m.autorizacao_operacional);self.assertIn('Explicitamente indisponível',self.client.get(r.location).get_data(as_text=True))
 def test_obrigacao_revisao_conflito(self):
  name,r=self.obligation();old=OBL.obter(name);path=r.location+'/revisar';data={'csrf_token':self.token(path),'esperada':str(old.revisao_id),'titulo':old.caso.titulo,'situacao':'em_acompanhamento','justificativa':'Revisão web sintética'}
  self.assertEqual(self.client.post(path,data=data).status_code,303);self.assertEqual(self.client.post(path,data=data).status_code,409);self.assertEqual(OBL.obter(name,old.revisao_id),old)
 def test_historico_sem_escrita_direta(self):
  name,r=self.obligation();old=OBL.obter(name);url=r.location+'/revisoes/'+str(old.revisao_id);self.assertEqual(self.client.get(url).status_code,200);self.assertEqual(self.client.post(url,data={'csrf_token':self.token(r.location)}).status_code,405)
 def test_invalidacao_decimal(self):
  self.login();r=self.client.post('/esg/maz/trcs/nova',data={'csrf_token':self.token('/esg/maz/trcs/nova'),'ano':'2053','entrada':'abc','rejeitos':'1','fonte':'Fonte sintética','motivo':'Teste','perimetro':'Teste'});self.assertEqual(r.status_code,400)
 def test_ausencia_config_sem_fallback(self):
  app=BUILDER(None,None);client=app.test_client();t=re.search(r'name="csrf_token" value="([^"]+)"',client.get('/login').get_data(as_text=True)).group(1);client.post('/login',data={'csrf_token':t,'password':PASSWORD});self.assertEqual(client.get('/esg/maz/').status_code,503)
 def test_confirmacao_incerta_sem_retry(self):
  from modulos.esg.maz.persistencia import ConfirmacaoIncertaMAZ
  class Uncertain:
   calls=0
   def registrar(self,*a,**kw):self.calls+=1;raise ConfirmacaoIncertaMAZ('synthetic')
  u=Uncertain();app=BUILDER(u,OBL);client=app.test_client();html=client.get('/login').get_data(as_text=True);t=re.search(r'name="csrf_token" value="([^"]+)"',html).group(1);client.post('/login',data={'csrf_token':t,'password':PASSWORD});html=client.get('/esg/maz/trcs/nova').get_data(as_text=True);t=re.search(r'name="csrf_token" value="([^"]+)"',html).group(1);r=client.post('/esg/maz/trcs/nova',data={'csrf_token':t,'ano':'2053','entrada':'100','rejeitos':'10','fonte':'Sintética','motivo':'Teste','perimetro':'Sintético'});self.assertEqual(r.status_code,503);self.assertEqual(u.calls,1)
 def test_privacidade_html(self):
  self.login();html=self.client.get('/esg/maz/').get_data(as_text=True);self.assertNotIn('C:\\',html);self.assertNotIn('documento_privado_id',html);self.assertNotIn('password_hash',html)
 def test_banco_segregado(self):
  c=FACTORY()
  try:
   with c.cursor() as q:q.execute('SELECT current_database(),current_user');r=q.fetchone()
   self.assertEqual(r,('maz_demo_20261125','maz_demo_runtime'))
  finally:c.rollback();c.close()
 def test_autorizacao_gravacao(self):
  with self.client.session_transaction() as s:s['_user_id']='readonly';s['_fresh']=True
  html=self.client.get('/login').get_data(as_text=True);token=re.search(r'name="csrf_token" value="([^"]+)"',html).group(1)
  for path in ('/esg/maz/trcs/nova','/esg/maz/obrigacoes/nova'):
   self.assertEqual(self.client.post(path,data={'csrf_token':token}).status_code,403)
 def test_login_invalido(self):
  r=self.client.post('/login',data={'csrf_token':self.token(),'password':'codigo-sintetico-incorreto'});self.assertEqual(r.status_code,200);self.assertEqual(self.client.get('/esg/maz/').status_code,302)
 def test_trcs_zero_preservado_na_tela(self):
  self.login();r=self.client.post('/esg/maz/trcs/nova',data={'csrf_token':self.token('/esg/maz/trcs/nova'),'ano':'2053','entrada':'100','rejeitos':'0','fonte':'Fonte sintética zero','perimetro':'Recorte sintético zero','motivo':'Verificar valor zero'})
  self.assertEqual(r.status_code,303);m=TRCS.obter(r.location.rsplit('/',1)[1]);self.assertEqual(m.resultado.percentual,0);html=self.client.get(r.location).get_data(as_text=True);self.assertIn('TRCS · 0,00%',html)
 def test_unidades_independentes_na_revisao(self):
  self.login();r=self.client.post('/esg/maz/trcs/nova',data={'csrf_token':self.token('/esg/maz/trcs/nova'),'ano':'2053','entrada':'1','rejeitos':'250','unidade_entrada':'t','unidade_rejeitos':'kg','fonte':'Fonte sintética com unidades mistas','perimetro':'Recorte sintético','motivo':'Unidades independentes'})
  self.assertEqual(r.status_code,303);rid=r.location.rsplit('/',1)[1];path=r.location+'/revisar';m=TRCS.obter(rid);self.assertEqual(m.resultado.percentual,25)
  r=self.client.post(path,data={'csrf_token':self.token(path),'esperada':rid,'entrada':'1','rejeitos':'250','unidade_entrada':'t','unidade_rejeitos':'kg','motivo':'Preservar as unidades originais'})
  self.assertEqual(r.status_code,303);m=TRCS.obter(r.location.rsplit('/',1)[1]);self.assertEqual(m.resultado.percentual,25);self.assertEqual(m.entrada_original.unidade.value,'t');self.assertEqual(m.rejeitos_original.unidade.value,'kg')

 def test_blueprint_injetado_fora_da_fabrica_demo(self):
  from flask import Flask
  from flask_login import LoginManager
  from seguranca_csrf import configurar_csrf
  from modulos.esg import criar_blueprint_esg
  from modulos.esg.maz_web import criar_blueprint_maz
  app=Flask('maz_injection_test');app.secret_key=APP.secret_key;configurar_csrf(app)
  lm=LoginManager(app);lm.user_loader(APP.login_manager._user_callback)
  app.register_blueprint(criar_blueprint_esg(),url_prefix='/esg');app.register_blueprint(criar_blueprint_maz(TRCS,OBL),url_prefix='/esg/maz')
  client=app.test_client()
  with client.session_transaction() as session:session['_user_id']=str(ACTOR);session['_fresh']=True
  r=client.get('/esg/maz/');self.assertEqual(r.status_code,200);self.assertNotIn('Encerrar sessão sintética',r.get_data(as_text=True))

 def revision(self,response,**overrides):
  rid=response.location.rsplit('/',1)[1];path=response.location+'/revisar'
  data={'csrf_token':self.token(path),'esperada':rid,'entrada':'100','rejeitos':'20','motivo':'Ensaio automatizado MAZ05C'}
  data.update(overrides);return path,data,self.client.post(path,data=data)

 def test_hv001_g2_historico_humano(self):
  self.login();history=OBL.historico('O14-demo')
  for number,filled,docs,responsible in ((2,7,2,None),(3,8,3,'Equipe técnica ambiental [SINTÉTICO]')):
   h=next(h for h in history if h['numero']==number);r=OBL.obter('O14-demo',h['id']);g=OBL.g2('O14-demo',h['id'])
   self.assertEqual((g['preenchidos'],g['total_campos'],len(r.caso.documentos),r.caso.responsavel_proposto),(filled,13,docs,responsible))
   html=self.client.get('/esg/maz/obrigacoes/O14-demo/revisoes/'+str(h['id'])).get_data(as_text=True)
   self.assertIn(str(filled)+'/13',html);self.assertIn('Revisão '+str(number),html)
   self.assertEqual('HV-OBR-001' in html,number==3)
   self.assertEqual('Equipe técnica ambiental [SINTÉTICO]' in html,number==3)

 def test_hv001_g2_calcula_snapshot_sem_reconsulta(self):
  from modulos.esg.maz.obrigacoes import AdaptadorObrigacoes
  old=OBL.obter('O14-demo',next(h['id'] for h in OBL.historico('O14-demo') if h['numero']==2))
  self.assertEqual(AdaptadorObrigacoes.g2_registro(old)['preenchidos'],7)
  self.assertEqual(OBL.g2('O14-demo')['preenchidos'],8)

 def test_hv002_rota_historica_nao_aceita_id_corrente_adulterado(self):
  r=self.measure();path,data,new=self.revision(r);self.assertEqual(new.status_code,303)
  current=new.location.rsplit('/',1)[1];data['esperada']=current
  self.assertEqual(self.client.post(path,data=data).status_code,409)
  self.assertEqual(str(TRCS.corrente(TRCS.obter(current).serie_id)),current)
  self.assertEqual(len(TRCS.listar(serie_id=TRCS.obter(current).serie_id)),2)

 def test_hv002_rota_corrente_nao_aceita_id_de_outra_serie(self):
  r=self.measure();other=self.measure();before=len(TRCS.listar())
  path,data,response=self.revision(r,esperada=other.location.rsplit('/',1)[1])
  self.assertEqual(response.status_code,409);self.assertEqual(len(TRCS.listar()),before)

 def test_hv002_get_historico_e_post_direto(self):
  r=self.measure();path,data,new=self.revision(r);self.assertEqual(new.status_code,303)
  self.assertEqual(self.client.get(path).status_code,409)
  self.assertEqual(self.client.post(path,data=data).status_code,409)
  self.assertNotIn('Criar nova revisão',self.client.get(r.location).get_data(as_text=True))
  self.assertIn('Criar nova revisão',self.client.get(new.location).get_data(as_text=True))

 def test_hv002_adaptador_rejeita_revisao_desatualizada_atomicamente(self):
  from modulos.esg.maz.persistencia import ConflitoMAZ
  from dataclasses import replace
  r=self.measure();old=TRCS.obter(r.location.rsplit('/',1)[1]);_,_,new=self.revision(r)
  c=FACTORY()
  try:
   with c.cursor() as q:q.execute('SELECT count(*) FROM esg_fontes_documentais');before=q.fetchone()[0]
  finally:c.rollback();c.close()
  f=replace(old.entrada_original.fonte,identificador='rollback'+uuid4().hex)
  with self.assertRaises(ConflitoMAZ):
   TRCS.registrar(replace(old.entrada_original,fonte=f),replace(old.rejeitos_original,fonte=f),old.metodologia,ator=ACTOR,request_id=uuid4(),motivo='Ensaio de conflito',serie_id=old.serie_id,revisao_esperada=old.revisao_id)
  c=FACTORY()
  try:
   with c.cursor() as q:q.execute('SELECT count(*) FROM esg_fontes_documentais');self.assertEqual(q.fetchone()[0],before)
  finally:c.rollback();c.close()
  self.assertEqual(len(TRCS.listar(serie_id=old.serie_id)),2)

 def test_hv003_trcs_fonte_nova_e_historico_preservado(self):
  r=self.measure();old=TRCS.obter(r.location.rsplit('/',1)[1]);_,_,new=self.revision(r,fonte='HV-002 fonte retificada sintética',fonte_evidencia='HV-002 evidência independente',evidencia='HV-002')
  self.assertEqual(new.status_code,303);m=TRCS.obter(new.location.rsplit('/',1)[1])
  self.assertEqual(m.entrada_original.fonte.referencia_documental,'HV-002 fonte retificada sintética')
  self.assertTrue(any(e.fonte.referencia_documental=='HV-002 evidência independente' for e in m.evidencias))
  self.assertTrue(all(e in m.evidencias for e in old.evidencias));self.assertEqual(TRCS.obter(old.revisao_id),old)
  self.assertNotEqual(m.entrada_original.fonte.identificador,old.entrada_original.fonte.identificador)

 def test_hv003_obrigacao_documento_fonte_independente(self):
  name,r=self.obligation();old=OBL.obter(name);path=r.location+'/revisar'
  data={'csrf_token':self.token(path),'esperada':str(old.revisao_id),'titulo':old.caso.titulo,'situacao':'em_acompanhamento','justificativa':'Ensaio 05C','documento':'HV-002 retificação','fonte_documento':'Fonte HV-002 independente'}
  self.assertEqual(self.client.post(path,data=data).status_code,303);current=OBL.obter(name)
  d=next(d for d in current.caso.documentos if d.descricao=='HV-002 retificação')
  self.assertEqual(d.fonte.referencia_documental,'Fonte HV-002 independente');self.assertNotEqual(d.fonte.identificador,current.caso.fonte.identificador)
  self.assertTrue(all(d in current.caso.documentos for d in old.caso.documentos));self.assertEqual(OBL.obter(name,old.revisao_id),old)

 def test_hv004_revisao_atual_por_uuid_e_historico(self):
  self.login();current=OBL.obter('O14-demo');base='/esg/maz/obrigacoes/O14-demo'
  html=self.client.get(base+'/revisoes/'+str(current.revisao_id)).get_data(as_text=True)
  self.assertIn('Vigente · revisão 3',html);self.assertIn('Criar nova revisão',html)
  old=next(h for h in OBL.historico('O14-demo') if h['numero']==2)
  html=self.client.get(base+'/revisoes/'+str(old['id'])).get_data(as_text=True)
  self.assertIn('Histórica · revisão 2',html);self.assertNotIn('Criar nova revisão',html)
  self.assertIn('autoria registrada',html);self.assertIn(old['justificativa'],html)

 def test_hv005_painel_referencia_fixa_e_selecao_explicita(self):
  r=self.measure();self.assertEqual(r.status_code,303)
  html=self.client.get('/esg/maz/').get_data(as_text=True)
  self.assertIn('20,00%',html);self.assertIn('2055-11',html);self.assertIn('Homologação visual - UVR teste',html)
  m=TRCS.obter(r.location.rsplit('/',1)[1]);html=self.client.get('/esg/maz/?escopo=todos&serie='+str(m.serie_id)).get_data(as_text=True)
  self.assertIn('25,00%',html);self.assertIn(str(m.serie_id),html)
  self.assertEqual(self.client.get('/esg/maz/?serie='+str(uuid4())).status_code,400)

 def test_hv005_sem_referencia_sem_escolha_arbitraria(self):
  self.login();anchor=APP.config['MAZ_PAINEL_TRCS_REVISAO_REFERENCIA']
  try:
   APP.config['MAZ_PAINEL_TRCS_REVISAO_REFERENCIA']=None
   html=self.client.get('/esg/maz/').get_data(as_text=True);self.assertIn('Nenhuma série da seleção disponível',html)
  finally:APP.config['MAZ_PAINEL_TRCS_REVISAO_REFERENCIA']=anchor

 def test_hv006_filtro_reproduzivel_sem_exclusao(self):
  name,r=self.obligation();self.assertNotIn(name,self.client.get('/esg/maz/obrigacoes').get_data(as_text=True))
  self.assertIn(name,self.client.get('/esg/maz/obrigacoes?escopo=todos&q='+name).get_data(as_text=True));self.assertEqual(OBL.obter(name).caso.identificador,name)
  r=self.measure();m=TRCS.obter(r.location.rsplit('/',1)[1])
  self.assertNotIn(str(m.revisao_id),self.client.get('/esg/maz/trcs').get_data(as_text=True))
  self.assertIn(str(m.revisao_id),self.client.get('/esg/maz/trcs?escopo=todos').get_data(as_text=True))

 def test_hv007_competencia_mensal_anual_indisponivel(self):
  name,r=self.obligation();old=OBL.obter(name);path=r.location+'/revisar'
  for year,month,expected in (('2056','7','2056-07'),('2057','','2057'),('','','Explicitamente indisponível')):
   data={'csrf_token':self.token(path),'esperada':str(old.revisao_id),'titulo':old.caso.titulo,'situacao':'desconhecida','justificativa':'Ensaio competência','competencia_ano':year,'competencia_mes':month}
   self.assertEqual(self.client.post(path,data=data).status_code,303);new=OBL.obter(name)
   self.assertEqual(new.caso.competencia.competencia if new.caso.competencia else 'Explicitamente indisponível',expected)
   self.assertIn(expected,self.client.get(r.location).get_data(as_text=True));self.assertEqual(OBL.obter(name,old.revisao_id),old);old=new

 def test_hv007_competencia_invalida_nao_grava(self):
  name,r=self.obligation();old=OBL.obter(name);path=r.location+'/revisar'
  for year,month in (('','2'),('2055','13'),('abc','1')):
   data={'csrf_token':self.token(path),'esperada':str(old.revisao_id),'titulo':old.caso.titulo,'justificativa':'Ensaio inválido','competencia_ano':year,'competencia_mes':month}
   self.assertEqual(self.client.post(path,data=data).status_code,400);self.assertEqual(OBL.obter(name),old)

 def test_g2_grafico_denominadores(self):
  self.login();html=self.client.get('/esg/maz/obrigacoes/O14-demo').get_data(as_text=True);g=OBL.g2('O14-demo')
  self.assertIn('8/13',html);self.assertIn(str(g['tratados'])+'/13',html);self.assertIn('Completude efetiva',html);self.assertIn('Campos tratados',html)
  self.assertEqual(g['preenchidos']+g['indisponiveis']+g['nao_informados'],g['total_campos'])

 def test_catalogo_pesquisa_dimensao_disponibilidade(self):
  self.login();html=self.client.get('/esg/maz/catalogo?q=TRCS&dimensao=ambiental&disponibilidade=motor').get_data(as_text=True)
  self.assertIn('1/16 códigos',html);self.assertIn('Consultar medições',html)
  html=self.client.get('/esg/maz/catalogo?dimensao=social').get_data(as_text=True);self.assertIn('4/16 códigos',html)
  html=self.client.get('/esg/maz/catalogo?disponibilidade=indisponivel').get_data(as_text=True);self.assertIn('15/16 códigos',html)

 def test_assets_locais_e_rotulos(self):
  self.login()
  for path in ('css/maz_demo.css','js/maz_demo.js','favicon.svg'):
   self.assertEqual(self.client.get('/esg/maz/assets/'+path).status_code,200)
  html=self.client.get('/esg/maz/obrigacoes/O14-demo').get_data(as_text=True)
  for label in ('Análise técnica','Não avaliada','Em acompanhamento','Instrumento de origem','aria-current="page"'):self.assertIn(label,html)
  for raw in ('>analise_tecnica<','>nao_avaliada<','>em_acompanhamento<','>instrumento_ref<','https://','http://'):self.assertNotIn(raw,html)

 def test_hv002_cas_concorrente_uma_unica_sucessora(self):
  import threading
  from modulos.esg.maz.persistencia import AdaptadorMAZ,ConflitoMAZ
  r=self.measure();old=TRCS.obter(r.location.rsplit('/',1)[1]);barrier=threading.Barrier(2);out=[]
  class Race(AdaptadorMAZ):
   def _put(self,q,table,values,*args):
    if table=='esg_revisoes' and values['numero']==2:barrier.wait(4)
    return super()._put(q,table,values,*args)
  def work():
   try:
    rid=Race(FACTORY).registrar(old.entrada_original,old.rejeitos_original,old.metodologia,ator=ACTOR,request_id=uuid4(),motivo='Ensaio concorrente 05C',serie_id=old.serie_id,revisao_esperada=old.revisao_id)
    out.append(('ok',rid))
   except ConflitoMAZ:out.append(('conflict',None))
   except Exception as exc:out.append((type(exc).__name__,None))
  threads=[threading.Thread(target=work) for _ in range(2)]
  for t in threads:t.start()
  for t in threads:t.join(12)
  self.assertFalse(any(t.is_alive() for t in threads));self.assertEqual(sorted(k for k,_ in out),['conflict','ok'])
  self.assertEqual(len(TRCS.listar(serie_id=old.serie_id)),2)
  self.assertEqual(TRCS.corrente(old.serie_id),next(rid for k,rid in out if k=='ok'))

 def test_hv003_preserva_fontes_distintas_das_massas(self):
  from dataclasses import replace
  r=self.measure();old=TRCS.obter(r.location.rsplit('/',1)[1]);f=replace(old.rejeitos_original.fonte,identificador='reject'+uuid4().hex,referencia_documental='Fonte independente de rejeitos')
  rid=TRCS.registrar(old.entrada_original,replace(old.rejeitos_original,fonte=f),old.metodologia,ator=ACTOR,request_id=uuid4(),motivo='Ensaio de fontes distintas',serie_id=old.serie_id,revisao_esperada=old.revisao_id)
  path='/esg/maz/trcs/'+str(rid)+'/revisar';data={'csrf_token':self.token(path),'esperada':str(rid),'entrada':'100','rejeitos':'19','motivo':'Preservar fontes distintas'}
  new=self.client.post(path,data=data);self.assertEqual(new.status_code,303);m=TRCS.obter(new.location.rsplit('/',1)[1])
  self.assertEqual(m.entrada_original.fonte,old.entrada_original.fonte);self.assertEqual(m.rejeitos_original.fonte,f)
