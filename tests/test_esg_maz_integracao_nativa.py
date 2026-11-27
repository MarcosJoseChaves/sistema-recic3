import ast,os,re,unittest
from unittest.mock import patch,Mock
from pathlib import Path
from flask import Flask
from flask_login import LoginManager,UserMixin
from flask_wtf.csrf import generate_csrf
from configuracao_ambiente import configurar_aplicacao
from seguranca_csrf import configurar_csrf
from modulos.esg.integracao_maz import configurar_maz_nativo
from modulos.esg import criar_blueprint_esg
from modulos.fiscalizacao_contratos import criar_blueprint_fiscalizacao
from modulos.esg.maz_web import criar_demo_maz
ROOT=Path(__file__).resolve().parents[1]
class Principal(UserMixin):
 id=1
 role='admin'
class ComposicaoNativa(unittest.TestCase):
 def setUp(self):
  self.db=Mock(side_effect=AssertionError('DB forbidden'))
  self.patches=[patch('psycopg2.connect',self.db),patch('dotenv.load_dotenv',side_effect=AssertionError('dotenv forbidden')),patch('modulos.esg.maz_web.criar_demo_maz',side_effect=AssertionError('demo forbidden')),patch('modulos.esg.maz.persistencia.AdaptadorMAZ.registrar',side_effect=AssertionError('write forbidden')),patch('modulos.esg.maz.obrigacoes.AdaptadorObrigacoes.salvar',side_effect=AssertionError('write forbidden'))]
  for p in self.patches:p.start()
 def tearDown(self):
  self.db.assert_not_called()
  for p in reversed(self.patches):p.stop()
 def app(self,flag=None,role='admin',compose=True):
  app=Flask(__name__,root_path=str(ROOT),template_folder='templates');env={'APP_ENV':'testing','SECRET_KEY':'exclusive-unit-test-key'}
  if flag is not None:env['ESG_MAZ_NATIVE_ENABLED']=flag
  with patch.dict(os.environ,env,clear=True):configurar_aplicacao(app)
  configurar_csrf(app);lm=LoginManager(app)
  @lm.user_loader
  def load(value):
   u=Principal();u.role=role;return u if value=='1' else None
  app.add_url_rule('/','index',lambda:'existing index')
  app.add_url_rule('/login','login',lambda:'existing login')
  app.add_url_rule('/logout','logout',lambda:'existing logout',methods=['POST'])
  app.add_url_rule('/test-token','test_token',lambda:generate_csrf())
  app.register_blueprint(criar_blueprint_esg(),url_prefix='/esg')
  app.register_blueprint(criar_blueprint_fiscalizacao(self.db),url_prefix='/fiscalizacao-contratos')
  if compose:configurar_maz_nativo(app)
  return app
 def client(self,a):
  c=a.test_client()
  with c.session_transaction() as s:s['_user_id']='1';s['_fresh']=True
  return c
 def test_01_ausente(self):self.assertFalse(self.app().config['ESG_MAZ_NATIVE_ENABLED']);self.assertEqual(self.app().test_client().get('/esg/maz/').status_code,404)
 def test_02_disabled(self):
  for v in ['false','0','off','no',' ']:self.assertNotIn('maz',self.app(v).blueprints)
 def test_03_invalid(self):
  for v in ['arbitrary','2','enabled']:
   with self.assertRaises(RuntimeError):self.app(v)
 def test_04_enabled_none_dependencies(self):
  from modulos.esg.maz_web import criar_blueprint_maz
  with patch('modulos.esg.maz_web.criar_blueprint_maz',wraps=criar_blueprint_maz) as factory:
   a=self.app('true');factory.assert_called_once_with(trcs=None,obrigacoes=None,metodologia=None);self.assertIn('maz',a.blueprints)
 def test_05_import_and_composition_no_db(self):
  import importlib
  import modulos.esg.integracao_maz as module
  importlib.reload(module);self.app('true');self.db.assert_not_called()
 def test_06_no_duplicate_login(self):
  a=self.app('true');rules=[r.rule for r in a.url_map.iter_rules()];self.assertEqual(rules.count('/login'),1);self.assertEqual(rules.count('/logout'),1);self.assertNotIn('/sair',rules)
 def test_07_v1_unchanged(self):
  off,on=self.app('false'),self.app('true')
  for path in ['/esg','/esg/indicadores']:
   x,y=self.client(off).get(path),self.client(on).get(path);self.assertEqual(x.status_code,y.status_code);self.assertEqual(re.sub(rb'name="csrf_token"[^>]*',b'name="csrf_token"',re.sub(rb'nonce="[^"]+"',b'nonce="request-specific"',x.data)),re.sub(rb'name="csrf_token"[^>]*',b'name="csrf_token"',re.sub(rb'nonce="[^"]+"',b'nonce="request-specific"',y.data)))
 def test_08_fc_composition_preserved(self):
  off,on=self.app('false'),self.app('true');rules=lambda a:{(r.rule,tuple(sorted(r.methods))) for r in a.url_map.iter_rules() if r.endpoint.startswith('fiscalizacao_contratos.')};self.assertTrue(rules(off));self.assertEqual(rules(off),rules(on))
 def test_09_anonymous(self):self.assertEqual(self.app('true').test_client().get('/esg/maz/').status_code,401)
 def test_10_nonadmin(self):self.assertEqual(self.client(self.app('true',role='user')).get('/esg/maz/').status_code,403)
 def test_11_all_native_routes_no_writes_valid_csrf(self):
  a=self.app('true');c=self.client(a);token=c.get('/test-token').get_data(as_text=True)
  for r in a.url_map.iter_rules():
   if not r.endpoint.startswith('maz.'):continue
   path=re.sub(r'<uuid:[^>]+>','00000000-0000-0000-0000-000000000001',r.rule);path=re.sub(r'<[^>]+>','untrusted',path)
   for method in r.methods & {'GET','POST'}:
    with self.subTest(path=path,method=method):self.assertEqual(c.open(path,method=method,data={'csrf_token':token}).status_code,503)
 def test_12_no_csrf_bypass(self):self.assertEqual(self.client(self.app('true')).post('/esg/maz/trcs/nova').status_code,400)
 def test_13_no_fallback_or_config_injection(self):
  a=self.app('true');spy=Mock(side_effect=AssertionError('injection forbidden'));a.config.update(MAZ_DEMO_OBRIGACOES=spy,MAZ_DEMO_TRCS_REFERENCIAS=spy,MAZ_NATIVE_ADAPTER=spy)
  r=self.client(a).get('/esg/maz/');self.assertEqual(r.status_code,503);spy.assert_not_called();self.assertNotIn(b'O14-demo',r.data)
 def test_14_http_cannot_enable(self):
  a=self.app('false');c=self.client(a)
  with c.session_transaction() as s:s['ESG_MAZ_NATIVE_ENABLED']=True
  self.assertEqual(c.get('/esg/maz/?ESG_MAZ_NATIVE_ENABLED=true').status_code,404)
 def test_15_invalid_config_or_adapter_arguments(self):
  for value in ['true',1,None]:
   a=Flask(__name__);a.config['ESG_MAZ_NATIVE_ENABLED']=value
   with self.assertRaises(RuntimeError):configurar_maz_nativo(a)
  with self.assertRaises(TypeError):configurar_maz_nativo(Flask(__name__),trcs=Mock())
 def test_16_demonstrative_factory_preserved(self):
  from werkzeug.security import generate_password_hash
  # Original function bound before the spy; explicit demonstration fixture only.
  a=criar_demo_maz(None,None,1,generate_password_hash('test-only'),'test-only-session');c=a.test_client();html=c.get('/login').get_data(as_text=True);t=re.search('name="csrf_token" value="([^"]+)"',html).group(1)
  self.assertEqual(c.post('/login',data={'csrf_token':t,'password':'test-only'}).status_code,303);self.assertEqual(c.get('/esg/maz/catalogo').status_code,200);self.assertEqual(c.get('/esg/maz/').status_code,503)
 def test_17_main_composition_ast_controlled(self):
  tree=ast.parse((ROOT/'app.py').read_text('utf8'));a=self.app('true',compose=False);before=set(a.view_functions);nodes=[n for n in tree.body if isinstance(n,ast.Expr) and isinstance(n.value,ast.Call) and isinstance(n.value.func,ast.Name) and n.value.func.id=='configurar_maz_nativo'];self.assertEqual(len(nodes),1)
  exec(compile(ast.Module(body=nodes,type_ignores=[]),'controlled-composition','exec'),{'app':a,'configurar_maz_nativo':configurar_maz_nativo});self.assertTrue(before<=set(a.view_functions));self.assertIn('maz',a.blueprints)
if __name__=='__main__':unittest.main(verbosity=2)
