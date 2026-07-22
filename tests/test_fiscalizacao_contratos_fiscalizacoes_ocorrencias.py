"""Testes da Etapa 2H sem PostgreSQL, Cloudinary ou arquivos reais."""

import importlib
import os
import sys
import unittest
from copy import deepcopy
from datetime import date, datetime, time
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg2

from modulos.fiscalizacao_contratos.services.fiscalizacoes_service import (
    FiscalizacaoBloqueadaError, FiscalizacaoService, FiscalizacaoServiceError,
    ReferenciaFiscalizacaoInvalidaError,
)
from modulos.fiscalizacao_contratos.services.ocorrencias_service import (
    OcorrenciaBloqueadaError, OcorrenciaService, OcorrenciaServiceError,
    ReferenciaOcorrenciaInvalidaError,
)
from modulos.fiscalizacao_contratos.validacoes_fiscalizacoes import normalizar_e_validar_fiscalizacao
from modulos.fiscalizacao_contratos.validacoes_ocorrencias import (
    normalizar_e_validar_acompanhamento, normalizar_e_validar_ocorrencia,
    situacao_prazo,
)


CONEXAO_FALSA = MagicMock(name="conexao_falsa_etapa_2h")
PATCH_CONEXAO = patch("psycopg2.connect", return_value=CONEXAO_FALSA)
MOCK_CONNECT = PATCH_CONEXAO.start()
sys.modules.pop("app", None)
with patch.dict(os.environ, {"APP_ENV": "testing", "SECRET_KEY": "teste-ficticio", "DATABASE_URL": ""}, clear=True), patch("dotenv.load_dotenv", return_value=False):
    APP_MODULE = importlib.import_module("app")
MOCK_CONNECT.reset_mock()
MOCK_CONNECT.side_effect = AssertionError("Nenhuma conexão real é permitida")


class FiscalizacaoServiceFake:
    def __init__(self):
        self.contratos={1:{"id":1,"numero_contrato":"CT-001/2026","empresa_id":1,"empresa_nome":"Empresa","ativo":True},2:{"id":2,"numero_contrato":"CT-002/2026","empresa_id":1,"empresa_nome":"Empresa","ativo":False}}
        self.servidores={1:{"id":1,"nome":"Fiscal Ativo","matricula":"F-1","ativo":True},2:{"id":2,"nome":"Fiscal Inativo","matricula":"F-2","ativo":False}}
        self.empresas=[{"id":1,"razao_social":"Empresa"}]
        self.itens={1:self._item(1)}; self.eventos={1:[]}; self.proximo=2; self.ultimo_filtro=None
    def _item(self,i,**extra):
        base={"id":i,"contrato_id":1,"servidor_responsavel_id":1,"data_fiscalizacao":date(2026,7,20),"hora_inicio":time(8),"hora_fim":time(9),"tipo_fiscalizacao":"Rotina","local_fiscalizacao":"Garagem","objeto_verificado":"Veículos e equipe","resultado":"Conforme","status":"Em elaboração","observacoes":None,"ativo":True,"numero_contrato":"CT-001/2026","processo_administrativo":"PA-1","empresa_nome":"Empresa","servidor_nome":"Fiscal Ativo","matricula":"F-1"};base.update(extra);return base
    def indicadores(self): return {"ocorrencias_abertas":2,"ocorrencias_vencidas":1,"graves_criticas":1,"fiscalizacoes_30_dias":3}
    def opcoes(self): return list(self.contratos.values()),[s for s in self.servidores.values() if s["ativo"]],self.empresas
    def listar(self,busca="",filtros=None): self.ultimo_filtro=(busca,filtros or {});return list(self.itens.values())
    def obter(self,i): return self.itens[i]
    def _refs(self,dados):
        c=self.contratos.get(dados["contrato_id"]);s=self.servidores.get(dados["servidor_responsavel_id"])
        if not c: raise ReferenciaFiscalizacaoInvalidaError("Contrato não encontrado.")
        if not c["ativo"]: raise ReferenciaFiscalizacaoInvalidaError("O contrato está inativo.")
        if not s: raise ReferenciaFiscalizacaoInvalidaError("Servidor responsável não encontrado.")
        if not s["ativo"]: raise ReferenciaFiscalizacaoInvalidaError("O servidor responsável está inativo.")
    def criar(self,dados,usuario):
        self._refs(dados);i=self.proximo;self.proximo+=1;self.itens[i]=self._item(i,**{**dados,"status":"Em elaboração"});self.eventos[i]=[];return i
    def atualizar(self,i,dados,usuario):
        if self.itens[i]["status"]!="Em elaboração": raise FiscalizacaoBloqueadaError("Bloqueada")
        self._refs(dados);self.itens[i].update(dados)
    def alterar_status(self,i,status,usuario,justificativa=None):
        item=self.itens[i]
        if item["status"]!="Em elaboração" or not item["ativo"]: raise FiscalizacaoBloqueadaError("Bloqueada")
        if status=="Cancelada" and not str(justificativa or "").strip(): raise FiscalizacaoBloqueadaError("Informe a justificativa")
        self.eventos.setdefault(i,[]).append({"id":len(self.eventos.setdefault(i,[]))+1,"tipo_evento":"Finalização" if status=="Finalizada" else "Cancelamento","status_anterior":item["status"],"status_novo":status,"justificativa":justificativa,"criado_em":datetime(2026,7,20,10),"usuario_nome":"admin","criado_por_usuario_id":usuario})
        item["status"]=status
    def reabrir_fiscalizacao(self,i,justificativa,usuario):
        item=self.itens[i]
        if item["status"]!="Finalizada" or not item["ativo"] or not str(justificativa or "").strip(): raise FiscalizacaoBloqueadaError("Bloqueada")
        self.eventos.setdefault(i,[]).append({"id":len(self.eventos.setdefault(i,[]))+1,"tipo_evento":"Reabertura","status_anterior":"Finalizada","status_novo":"Em elaboração","justificativa":justificativa.strip(),"criado_em":datetime(2026,7,20,11),"usuario_nome":"admin","criado_por_usuario_id":usuario})
        item["status"]="Em elaboração"
    def listar_eventos_fiscalizacao(self,i): return list(reversed(self.eventos.setdefault(i,[])))
    def listar_do_contrato(self,contrato_id,limite=10): return [x for x in self.itens.values() if x["contrato_id"]==contrato_id]


class OcorrenciaServiceFake:
    def __init__(self,fiscalizacoes):
        self.fiscalizacoes=fiscalizacoes
        self.contratos={1:{"id":1,"numero_contrato":"CT-001/2026"},2:{"id":2,"numero_contrato":"CT-002/2026"}}
        self.servidores={1:{"id":1,"nome":"Fiscal Ativo","matricula":"F-1","ativo":True},2:{"id":2,"nome":"Fiscal Inativo","matricula":"F-2","ativo":False}}
        self.ativos={1:{"id":1,"codigo_interno":"AT-001","descricao":"Caminhão","contrato_id":1},2:{"id":2,"codigo_interno":"AT-002","descricao":"Máquina","contrato_id":2}}
        self.itens={1:self._item(1)};self.acompanhamentos={1:[]};self.proximo=2;self.ultimo_filtro=None
    def _item(self,i,**extra):
        base={"id":i,"contrato_id":1,"fiscalizacao_id":None,"ativo_contratual_id":1,"servidor_responsavel_id":1,"titulo":"Falha operacional","categoria":"Qualidade","gravidade":"Grave","descricao":"Serviço fora do padrão","data_identificacao":date(2026,7,1),"prazo_correcao":date(2026,7,10),"status":"Aberta","exige_notificacao":False,"numero_notificacao":None,"data_regularizacao":None,"conclusao":None,"ativo":True,"numero_contrato":"CT-001/2026","empresa_nome":"Empresa","servidor_nome":"Fiscal Ativo","ativo_codigo":"AT-001","ativo_descricao":"Caminhão","data_fiscalizacao":None,"vencida":True};base.update(extra);return base
    def opcoes(self,contrato_id=None): return list(self.contratos.values()),[s for s in self.servidores.values() if s["ativo"]],list(self.fiscalizacoes.itens.values()),list(self.ativos.values())
    def listar(self,busca="",filtros=None):
        self.ultimo_filtro=(busca,filtros or {});itens=list(self.itens.values())
        if filtros and filtros.get("fiscalizacao_id"): itens=[x for x in itens if x["fiscalizacao_id"]==filtros["fiscalizacao_id"]]
        return itens
    def obter(self,i): return self.itens[i],self.acompanhamentos.setdefault(i,[])
    def _refs(self,dados):
        if dados["contrato_id"] not in self.contratos: raise ReferenciaOcorrenciaInvalidaError("Contrato não encontrado.")
        s=self.servidores.get(dados["servidor_responsavel_id"])
        if not s: raise ReferenciaOcorrenciaInvalidaError("Servidor responsável não encontrado.")
        if not s["ativo"]: raise ReferenciaOcorrenciaInvalidaError("O servidor responsável está inativo.")
        if dados.get("fiscalizacao_id") and self.fiscalizacoes.itens.get(dados["fiscalizacao_id"],{}).get("contrato_id")!=dados["contrato_id"]: raise ReferenciaOcorrenciaInvalidaError("A fiscalização não pertence ao contrato informado.")
        if dados.get("ativo_contratual_id") and self.ativos.get(dados["ativo_contratual_id"],{}).get("contrato_id")!=dados["contrato_id"]: raise ReferenciaOcorrenciaInvalidaError("O ativo não possui vínculo com este contrato.")
    def criar(self,dados,usuario):
        self._refs(dados);i=self.proximo;self.proximo+=1;self.itens[i]=self._item(i,**{**dados,"status":"Aberta","vencida":False});self.acompanhamentos[i]=[];return i
    def atualizar(self,i,dados,usuario):
        if self.acompanhamentos[i] or not self.itens[i]["ativo"]: raise OcorrenciaBloqueadaError("Bloqueada")
        self._refs(dados);self.itens[i].update(dados)
    def alterar_ativo(self,i,usuario,ativo): self.itens[i]["ativo"]=ativo
    def adicionar_acompanhamento(self,i,dados,usuario):
        item=self.itens[i]
        if not item["ativo"]: raise OcorrenciaBloqueadaError("Inativa")
        self.acompanhamentos[i].append({"id":len(self.acompanhamentos[i])+1,"ocorrencia_id":i,"status_anterior":item["status"],**dados})
        item["status"]=dados["status_novo"];item["data_regularizacao"]=dados.get("data_regularizacao") if dados["status_novo"] in ("Regularizada","Não regularizada") else None
    def listar_do_contrato(self,i): return [x for x in self.itens.values() if x["contrato_id"]==i]
    def listar_do_ativo(self,i): return [x for x in self.itens.values() if x["ativo_contratual_id"]==i]


class TestFiscalizacoesOcorrencias(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.app=APP_MODULE.app;cls.app.config.update(TESTING=True);cls.loader=APP_MODULE.login_manager._user_callback
    @classmethod
    def tearDownClass(cls): APP_MODULE.login_manager._user_callback=cls.loader
    def setUp(self):
        self.client=self.app.test_client();APP_MODULE.login_manager._user_callback=self._usuario
        self.fiscal=FiscalizacaoServiceFake();self.ocorr=OcorrenciaServiceFake(self.fiscal)
        self.patchers=[
            patch("modulos.fiscalizacao_contratos.routes.fiscalizacoes.FiscalizacaoService",return_value=self.fiscal),
            patch("modulos.fiscalizacao_contratos.routes.fiscalizacoes.OcorrenciaService",return_value=self.ocorr),
            patch("modulos.fiscalizacao_contratos.routes.ocorrencias.OcorrenciaService",return_value=self.ocorr),
            patch("modulos.fiscalizacao_contratos.routes.FiscalizacaoService",return_value=self.fiscal),
        ]
        for p in self.patchers:p.start()
    def tearDown(self):
        for p in reversed(self.patchers):p.stop()
    @staticmethod
    def _usuario(uid): return {"1":APP_MODULE.User(1,"admin","admin",None),"2":APP_MODULE.User(2,"comum","usuario",None)}.get(str(uid))
    def autenticar(self,uid):
        with self.client.session_transaction() as s:s["_user_id"]=str(uid);s["_fresh"]=True
    @staticmethod
    def dados_fiscal(**extra):
        d={"contrato_id":"1","servidor_responsavel_id":"1","data_fiscalizacao":"2026-07-20","hora_inicio":"08:00","hora_fim":"09:00","tipo_fiscalizacao":"Rotina","local_fiscalizacao":"Garagem","objeto_verificado":"Veículos","resultado":"Conforme","observacoes":""};d.update(extra);return d
    @staticmethod
    def dados_ocorr(**extra):
        d={"contrato_id":"1","fiscalizacao_id":"","ativo_contratual_id":"1","servidor_responsavel_id":"1","titulo":"Nova falha","categoria":"Qualidade","gravidade":"Média","descricao":"Descrição da falha","data_identificacao":"2026-07-20","prazo_correcao":"2026-07-25","exige_notificacao":"","numero_notificacao":"","conclusao":""};d.update(extra);return d
    def test_admin_acessa_listagens_e_templates_principais(self):
        self.autenticar(1)
        for caminho in ("/fiscalizacao-contratos/fiscalizacoes","/fiscalizacao-contratos/fiscalizacoes/nova","/fiscalizacao-contratos/fiscalizacoes/1","/fiscalizacao-contratos/ocorrencias","/fiscalizacao-contratos/ocorrencias/nova","/fiscalizacao-contratos/ocorrencias/1","/fiscalizacao-contratos/ocorrencias/1/acompanhamentos/novo"):
            self.assertEqual(self.client.get(caminho).status_code,200,caminho)
        painel=self.client.get("/fiscalizacao-contratos").data
        self.assertIn("Ocorrências".encode(),painel)
        self.assertNotIn("Ocorrências vencidas".encode(),painel)
    def test_visitante_e_comum_bloqueados_em_todas_as_rotas(self):
        caminhos_get=["/fiscalizacao-contratos/fiscalizacoes","/fiscalizacao-contratos/fiscalizacoes/nova","/fiscalizacao-contratos/fiscalizacoes/1","/fiscalizacao-contratos/fiscalizacoes/1/editar","/fiscalizacao-contratos/fiscalizacoes/1/cancelar","/fiscalizacao-contratos/fiscalizacoes/1/reabrir","/fiscalizacao-contratos/ocorrencias","/fiscalizacao-contratos/ocorrencias/nova","/fiscalizacao-contratos/ocorrencias/1","/fiscalizacao-contratos/ocorrencias/1/editar","/fiscalizacao-contratos/ocorrencias/1/acompanhamentos/novo"]
        caminhos_post=["/fiscalizacao-contratos/fiscalizacoes/nova","/fiscalizacao-contratos/fiscalizacoes/1/editar","/fiscalizacao-contratos/fiscalizacoes/1/finalizar","/fiscalizacao-contratos/fiscalizacoes/1/cancelar","/fiscalizacao-contratos/fiscalizacoes/1/reabrir","/fiscalizacao-contratos/ocorrencias/nova","/fiscalizacao-contratos/ocorrencias/1/editar","/fiscalizacao-contratos/ocorrencias/1/inativar","/fiscalizacao-contratos/ocorrencias/1/reativar","/fiscalizacao-contratos/ocorrencias/1/acompanhamentos/novo"]
        self.assertTrue(all(self.client.get(x).status_code==302 for x in caminhos_get));self.autenticar(2);self.assertTrue(all(self.client.get(x).status_code==403 for x in caminhos_get));self.assertTrue(all(self.client.post(x).status_code==403 for x in caminhos_post))
    def test_cria_fiscalizacao_com_status_inicial(self):
        self.autenticar(1);r=self.client.post("/fiscalizacao-contratos/fiscalizacoes/nova",data=self.dados_fiscal());self.assertEqual(r.status_code,302);self.assertEqual(self.fiscal.itens[2]["status"],"Em elaboração")
    def test_referencias_invalidas_da_fiscalizacao(self):
        self.autenticar(1)
        for campo,valor in (("contrato_id","999"),("contrato_id","2"),("servidor_responsavel_id","999"),("servidor_responsavel_id","2")):
            self.assertEqual(self.client.post("/fiscalizacao-contratos/fiscalizacoes/nova",data=self.dados_fiscal(**{campo:valor})).status_code,400)
    def test_validacoes_de_data_hora_tipo_resultado_e_objeto(self):
        casos=[{"data_fiscalizacao":""},{"hora_inicio":"10:00","hora_fim":"09:00"},{"tipo_fiscalizacao":"Inválida"},{"resultado":"Inválido"},{"objeto_verificado":"  "}]
        for alteracao in casos:
            _,erros=normalizar_e_validar_fiscalizacao(self.dados_fiscal(**alteracao));self.assertTrue(erros)
    def test_edita_somente_em_elaboracao_e_revalida_contrato(self):
        self.autenticar(1);self.assertEqual(self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/editar",data=self.dados_fiscal(local_fiscalizacao="Pátio")).status_code,302);self.fiscal.itens[1]["status"]="Finalizada";self.assertEqual(self.client.get("/fiscalizacao-contratos/fiscalizacoes/1/editar").status_code,302)
    def test_finaliza_e_cancela_sem_apagar(self):
        self.autenticar(1);self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/finalizar");self.assertEqual(self.fiscal.itens[1]["status"],"Finalizada");self.fiscal.itens[2]=self.fiscal._item(2);self.client.post("/fiscalizacao-contratos/fiscalizacoes/2/cancelar",data={"justificativa":"Preenchimento indevido"});self.assertEqual(self.fiscal.itens[2]["status"],"Cancelada");self.assertIn(2,self.fiscal.itens)
    def test_finalizada_ou_cancelada_nao_muda_novamente(self):
        self.fiscal.itens[1]["status"]="Finalizada";self.autenticar(1);self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/cancelar");self.assertEqual(self.fiscal.itens[1]["status"],"Finalizada")

    def test_botao_reabrir_aparece_somente_finalizada_ativa(self):
        self.autenticar(1)
        self.fiscal.itens[1]["status"]="Finalizada"
        self.assertIn("Reabrir fiscalização".encode(),self.client.get("/fiscalizacao-contratos/fiscalizacoes/1").data)
        self.fiscal.itens[1]["status"]="Em elaboração"
        self.assertNotIn("Reabrir fiscalização".encode(),self.client.get("/fiscalizacao-contratos/fiscalizacoes/1").data)
        self.fiscal.itens[1]["status"]="Cancelada"
        self.assertNotIn("Reabrir fiscalização".encode(),self.client.get("/fiscalizacao-contratos/fiscalizacoes/1").data)

    def test_reabertura_exige_justificativa_e_bloqueia_status_invalido(self):
        self.autenticar(1);self.fiscal.itens[1]["status"]="Finalizada"
        self.assertEqual(self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/reabrir",data={"justificativa":"   "}).status_code,400)
        self.assertEqual(self.fiscal.itens[1]["status"],"Finalizada")
        self.fiscal.itens[1]["status"]="Em elaboração"
        self.assertEqual(self.client.get("/fiscalizacao-contratos/fiscalizacoes/1/reabrir").status_code,302)
        self.fiscal.itens[1]["status"]="Cancelada"
        self.assertEqual(self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/reabrir",data={"justificativa":"Erro"}).status_code,302)

    def test_reabertura_preserva_vinculos_e_permite_editar_e_finalizar_novamente(self):
        self.autenticar(1);self.fiscal.itens[1]["status"]="Finalizada";self.ocorr.itens[1]["fiscalizacao_id"]=1
        ocorrencias=deepcopy(self.ocorr.itens);acompanhamentos=deepcopy(self.ocorr.acompanhamentos)
        self.assertEqual(self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/reabrir",data={"justificativa":"Complementar informação"}).status_code,302)
        self.assertEqual(self.fiscal.itens[1]["status"],"Em elaboração");self.assertEqual(ocorrencias,self.ocorr.itens);self.assertEqual(acompanhamentos,self.ocorr.acompanhamentos)
        self.assertEqual(self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/editar",data=self.dados_fiscal(local_fiscalizacao="Pátio")).status_code,302)
        self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/finalizar")
        self.assertEqual([e["tipo_evento"] for e in self.fiscal.eventos[1]],["Reabertura","Finalização"])

    def test_varias_reaberturas_preservam_historico_em_ordem_recente(self):
        self.autenticar(1);self.fiscal.itens[1]["status"]="Finalizada"
        self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/reabrir",data={"justificativa":"Primeira complementação"})
        self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/finalizar")
        self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/reabrir",data={"justificativa":"Segunda complementação"})
        pagina=self.client.get("/fiscalizacao-contratos/fiscalizacoes/1").data.decode()
        self.assertEqual(len(self.fiscal.eventos[1]),3);self.assertLess(pagina.index("Segunda complementação"),pagina.index("Primeira complementação"))

    def test_finalizacao_e_cancelamento_geram_eventos(self):
        self.autenticar(1);self.client.post("/fiscalizacao-contratos/fiscalizacoes/1/finalizar")
        evento=self.fiscal.eventos[1][0];self.assertEqual((evento["tipo_evento"],evento["status_anterior"],evento["status_novo"]),("Finalização","Em elaboração","Finalizada"));self.assertEqual(evento["criado_por_usuario_id"],1);self.assertTrue(evento["criado_em"])
        self.fiscal.itens[2]=self.fiscal._item(2);self.client.post("/fiscalizacao-contratos/fiscalizacoes/2/cancelar",data={"justificativa":"Registro incorreto"})
        self.assertEqual(self.fiscal.eventos[2][0]["tipo_evento"],"Cancelamento");self.assertEqual(self.fiscal.eventos[2][0]["justificativa"],"Registro incorreto")
    def test_cria_ocorrencia_independente_e_em_fiscalizacao(self):
        self.autenticar(1);self.assertEqual(self.client.post("/fiscalizacao-contratos/ocorrencias/nova",data=self.dados_ocorr()).status_code,302);self.fiscal.itens[2]=self.fiscal._item(2);r=self.client.post("/fiscalizacao-contratos/ocorrencias/nova",data=self.dados_ocorr(fiscalizacao_id="2"));self.assertEqual(r.status_code,302);self.assertEqual(self.ocorr.itens[3]["status"],"Aberta")
    def test_fiscalizacao_e_ativo_de_outro_contrato_sao_rejeitados(self):
        self.autenticar(1);self.fiscal.itens[2]=self.fiscal._item(2,contrato_id=2)
        self.assertEqual(self.client.post("/fiscalizacao-contratos/ocorrencias/nova",data=self.dados_ocorr(fiscalizacao_id="2")).status_code,400);self.assertEqual(self.client.post("/fiscalizacao-contratos/ocorrencias/nova",data=self.dados_ocorr(ativo_contratual_id="2")).status_code,400)
    def test_servidor_inexistente_ou_inativo_na_ocorrencia(self):
        self.autenticar(1);self.assertEqual(self.client.post("/fiscalizacao-contratos/ocorrencias/nova",data=self.dados_ocorr(servidor_responsavel_id="999")).status_code,400);self.assertEqual(self.client.post("/fiscalizacao-contratos/ocorrencias/nova",data=self.dados_ocorr(servidor_responsavel_id="2")).status_code,400)
    def test_validacoes_da_ocorrencia(self):
        casos=[{"titulo":""},{"descricao":""},{"categoria":"Inválida"},{"gravidade":"Inválida"},{"data_identificacao":""},{"prazo_correcao":"2026-07-19"},{"exige_notificacao":"on","numero_notificacao":""}]
        for alteracao in casos:
            _,erros=normalizar_e_validar_ocorrencia(self.dados_ocorr(**alteracao));self.assertTrue(erros)
    def test_edicao_bloqueada_depois_de_acompanhamento(self):
        self.autenticar(1);self.assertEqual(self.client.post("/fiscalizacao-contratos/ocorrencias/1/editar",data=self.dados_ocorr()).status_code,302);self.ocorr.acompanhamentos[1].append({"id":1});self.assertEqual(self.client.get("/fiscalizacao-contratos/ocorrencias/1/editar").status_code,302)
    def test_inativacao_preserva_e_reativacao_nao_altera_historico(self):
        self.ocorr.acompanhamentos[1]=[{"id":1,"status_anterior":"Aberta","status_novo":"Em acompanhamento"}];historico=list(self.ocorr.acompanhamentos[1]);self.autenticar(1);self.client.post("/fiscalizacao-contratos/ocorrencias/1/inativar");self.assertFalse(self.ocorr.itens[1]["ativo"]);self.client.post("/fiscalizacao-contratos/ocorrencias/1/reativar");self.assertTrue(self.ocorr.itens[1]["ativo"]);self.assertEqual(historico,self.ocorr.acompanhamentos[1])
    def test_situacoes_de_prazo(self):
        hoje=date(2026,7,20);base={"ativo":True,"status":"Aberta","prazo_correcao":date(2026,7,21)}
        self.assertEqual(situacao_prazo(base,hoje),"Dentro do prazo");self.assertEqual(situacao_prazo({**base,"prazo_correcao":date(2026,7,19)},hoje),"Vencida");self.assertEqual(situacao_prazo({**base,"prazo_correcao":hoje},hoje),"Dentro do prazo");self.assertEqual(situacao_prazo({**base,"prazo_correcao":None},hoje),"Sem prazo");self.assertEqual(situacao_prazo({**base,"ativo":False,"prazo_correcao":date(2026,7,19)},hoje),"Dentro do prazo");self.assertEqual(situacao_prazo({**base,"status":"Cancelada","prazo_correcao":date(2026,7,19)},hoje),"Dentro do prazo");self.assertEqual(situacao_prazo({**base,"status":"Regularizada"},hoje),"Regularizada");self.assertEqual(situacao_prazo({**base,"status":"Não regularizada"},hoje),"Não regularizada")
    def test_acompanhamento_registra_status_anterior_e_atualiza_atual(self):
        self.autenticar(1);dados={"data_acompanhamento":"2026-07-20","status_novo":"Em acompanhamento","descricao":"Cobrança realizada","providencia_contratada":"Corrigir","observacoes":"","data_regularizacao":""};self.assertEqual(self.client.post("/fiscalizacao-contratos/ocorrencias/1/acompanhamentos/novo",data=dados).status_code,302);self.assertEqual(self.ocorr.acompanhamentos[1][0]["status_anterior"],"Aberta");self.assertEqual(self.ocorr.itens[1]["status"],"Em acompanhamento")
    def test_acompanhamento_valida_data_regularizacao_cancelamento_e_saida(self):
        item=self.ocorr.itens[1]
        _,e1=normalizar_e_validar_acompanhamento({"data_acompanhamento":"2026-07-20","status_novo":"Regularizada","descricao":"ok"},item);self.assertTrue(e1)
        _,e2=normalizar_e_validar_acompanhamento({"data_acompanhamento":"2026-06-30","status_novo":"Em acompanhamento","descricao":"ok"},item);self.assertTrue(e2)
        _,e3=normalizar_e_validar_acompanhamento({"data_acompanhamento":"2026-07-20","status_novo":"Cancelada","descricao":""},item);self.assertTrue(e3)
        item["status"]="Regularizada";_,e4=normalizar_e_validar_acompanhamento({"data_acompanhamento":"2026-07-20","status_novo":"Aberta","descricao":"reaberta"},item);self.assertTrue(e4)
    def test_pesquisas_e_filtros_chegam_aos_servicos(self):
        self.autenticar(1);self.client.get("/fiscalizacao-contratos/fiscalizacoes?busca=Empresa&tipo=Rotina&status=Finalizada&contrato_id=1");self.assertEqual(self.fiscal.ultimo_filtro[0],"Empresa");self.assertEqual(self.fiscal.ultimo_filtro[1]["tipo"],"Rotina");self.client.get("/fiscalizacao-contratos/ocorrencias?busca=Falha&gravidade=Grave&vencidas=1&notificacao=1");self.assertEqual(self.ocorr.ultimo_filtro[0],"Falha");self.assertTrue(self.ocorr.ultimo_filtro[1]["vencidas"])
    def test_rotas_e_cartoes_estao_registrados(self):
        rotas={r.rule for r in self.app.url_map.iter_rules()};self.assertIn("/fiscalizacao-contratos/fiscalizacoes",rotas);self.assertIn("/fiscalizacao-contratos/ocorrencias",rotas);self.autenticar(1);pagina=self.client.get("/fiscalizacao-contratos").data;self.assertIn("Fiscalizações".encode(),pagina);self.assertIn("Ocorrências".encode(),pagina)
    def test_fiscalizacoes_e_ocorrencias_aparecem_no_contrato(self):
        self.autenticar(1);contrato={"id":1,"numero_contrato":"CT-001/2026","valor_original":1,"ativo":True,"situacao":"Vigente","vence_em_60_dias":False,"objeto":"Objeto","empresa_nome":"Empresa","processo_administrativo":None,"data_assinatura":None,"vigencia_inicio":None,"vigencia_fim":None,"observacoes":None,"criado_em":None,"atualizado_em":None}
        with patch("modulos.fiscalizacao_contratos.routes.contratos.ContratoService") as c,patch("modulos.fiscalizacao_contratos.routes.contratos.AditivoService") as ad,patch("modulos.fiscalizacao_contratos.routes.contratos.DocumentoService") as doc,patch("modulos.fiscalizacao_contratos.routes.contratos.PlanilhaService") as pl,patch("modulos.fiscalizacao_contratos.routes.contratos.AtivoService") as at,patch("modulos.fiscalizacao_contratos.routes.contratos.FiscalizacaoService",return_value=self.fiscal),patch("modulos.fiscalizacao_contratos.routes.contratos.OcorrenciaService",return_value=self.ocorr):
            c.return_value.obter.return_value=(contrato,[]);ad.return_value.resumo_contrato.return_value=(None,[]);doc.return_value.listar_do_contrato.return_value=[];pl.return_value.comparar_contrato.return_value={"planilhas":[]};at.return_value.listar_do_contrato.return_value=[];r=self.client.get("/fiscalizacao-contratos/contratos/1")
        self.assertEqual(r.status_code,200);self.assertIn("Falha operacional".encode(),r.data);self.assertIn("Fiscal Ativo".encode(),r.data)
    def test_ocorrencias_aparecem_no_ativo(self):
        self.autenticar(1);ativo={"id":1,"codigo_interno":"AT-001","tipo_ativo":"Veículo","descricao":"Caminhão","marca":None,"modelo":None,"ano_fabricacao":2025,"placa":None,"renavam":None,"chassi":None,"numero_serie":None,"numero_patrimonio":None,"origem_ativo":"Contratada","empresa_proprietaria_nome":"Empresa","capacidade":None,"unidade_capacidade":None,"situacao":"Disponível","observacoes":None,"ativo":True}
        with patch("modulos.fiscalizacao_contratos.routes.ativos.AtivoService") as at,patch("modulos.fiscalizacao_contratos.routes.ativos.OcorrenciaService",return_value=self.ocorr):at.return_value.obter.return_value=(ativo,[]);r=self.client.get("/fiscalizacao-contratos/ativos/1")
        self.assertEqual(r.status_code,200);self.assertIn("Falha operacional".encode(),r.data);self.assertIn(b"CT-001/2026",r.data)
    def test_migracao_008_aditiva_composta_e_nao_automatica(self):
        caminho=Path(__file__).parents[1]/"modulos/fiscalizacao_contratos/migrations/008_criar_fc_fiscalizacoes_ocorrencias.sql";sql=caminho.read_text(encoding="utf-8").upper()
        for tabela in ("FC_FISCALIZACOES","FC_OCORRENCIAS","FC_OCORRENCIA_ACOMPANHAMENTOS"):self.assertIn("CREATE TABLE IF NOT EXISTS "+tabela,sql)
        self.assertIn("UQ_FC_FISCALIZACOES_ID_CONTRATO",sql);self.assertIn("(FISCALIZACAO_ID, CONTRATO_ID)",sql)
        for comando in ("DROP","TRUNCATE","DELETE","UPDATE","INSERT","ALTER TABLE"):self.assertNotIn(comando,sql)
    def test_codigo_nao_usa_delete_e_rotas_exigem_admin(self):
        raiz=Path(__file__).parents[1]/"modulos/fiscalizacao_contratos";fontes="\n".join((raiz/p).read_text(encoding="utf-8") for p in ("services/fiscalizacoes_service.py","services/ocorrencias_service.py"));self.assertNotIn("DELETE FROM",fontes.upper());rotas="\n".join((raiz/p).read_text(encoding="utf-8") for p in ("routes/fiscalizacoes.py","routes/ocorrencias.py"));self.assertEqual(rotas.count("@blueprint.route"),rotas.count("@admin_required"))
    def test_migracao_009_e_aditiva_e_preserva_eventos(self):
        caminho=Path(__file__).parents[1]/"modulos/fiscalizacao_contratos/migrations/009_criar_fc_fiscalizacao_eventos.sql";sql=caminho.read_text(encoding="utf-8").upper()
        self.assertIn("CREATE TABLE IF NOT EXISTS FC_FISCALIZACAO_EVENTOS",sql);self.assertIn("TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP",sql);self.assertIn("CRIADO_POR_USUARIO_ID INTEGER NOT NULL",sql);self.assertIn("REABERTURA",sql);self.assertIn("FINALIZAÇÃO",sql);self.assertIn("CANCELAMENTO",sql)
        for comando in ("DROP","TRUNCATE","DELETE","UPDATE","INSERT","ALTER TABLE"):self.assertNotIn(comando,sql)
    def test_barreira_global_permanece_sem_uso_real(self): MOCK_CONNECT.assert_not_called()


class TestTransacoesEtapa2H(unittest.TestCase):
    @staticmethod
    def _fiscalizacao(status="Finalizada",ativo=True):
        return {"id":1,"ativo":ativo,"status":status,"contrato_id":1,"servidor_responsavel_id":1,"data_fiscalizacao":date.today(),"tipo_fiscalizacao":"Rotina","objeto_verificado":"Objeto","resultado":"Conforme"}

    def test_edicao_trava_ocorrencia_e_bloqueia_quando_ha_acompanhamento(self):
        conexao=MagicMock();cursor=conexao.cursor.return_value.__enter__.return_value
        cursor.fetchone.side_effect=[{"ativo":True},{"possui_acompanhamento":True}]
        with self.assertRaises(OcorrenciaBloqueadaError):
            OcorrenciaService(lambda:conexao).atualizar(1,{},1)
        self.assertIn("FOR UPDATE",cursor.execute.call_args_list[0].args[0])
        conexao.rollback.assert_called_once();conexao.commit.assert_not_called()

    def test_finalizacao_executa_rollback_em_falha(self):
        conexao=MagicMock();cursor=conexao.cursor.return_value.__enter__.return_value;cursor.fetchone.return_value={"id":1,"ativo":True,"status":"Em elaboração","contrato_id":1,"servidor_responsavel_id":1,"data_fiscalizacao":date.today(),"tipo_fiscalizacao":"Rotina","objeto_verificado":"Objeto","resultado":"Conforme"};cursor.execute.side_effect=[None,psycopg2.OperationalError("falha simulada")]
        with self.assertRaises(Exception):FiscalizacaoService(lambda:conexao).alterar_status(1,"Finalizada",1)
        conexao.rollback.assert_called_once();conexao.commit.assert_not_called()

    def test_reabertura_grava_evento_e_status_na_mesma_transacao(self):
        conexao=MagicMock();cursor=conexao.cursor.return_value.__enter__.return_value;cursor.fetchone.return_value=self._fiscalizacao()
        FiscalizacaoService(lambda:conexao).reabrir_fiscalizacao(1,"Complementar dados",7)
        self.assertIn("FOR UPDATE",cursor.execute.call_args_list[0].args[0]);self.assertIn("INSERT INTO fc_fiscalizacao_eventos",cursor.execute.call_args_list[1].args[0]);self.assertIn("UPDATE fc_fiscalizacoes",cursor.execute.call_args_list[2].args[0])
        self.assertEqual(cursor.execute.call_args_list[1].args[1],(1,"Complementar dados",7));conexao.commit.assert_called_once();conexao.rollback.assert_not_called()

    def test_reabertura_bloqueia_elaboracao_cancelada_inativa_e_justificativa_vazia(self):
        for status,ativo in (("Em elaboração",True),("Cancelada",True),("Finalizada",False)):
            conexao=MagicMock();conexao.cursor.return_value.__enter__.return_value.fetchone.return_value=self._fiscalizacao(status,ativo)
            with self.assertRaises(FiscalizacaoBloqueadaError):FiscalizacaoService(lambda:conexao).reabrir_fiscalizacao(1,"Motivo",1)
            conexao.rollback.assert_called_once();conexao.commit.assert_not_called()
        conectar=MagicMock()
        with self.assertRaises(FiscalizacaoBloqueadaError):FiscalizacaoService(conectar).reabrir_fiscalizacao(1,"   ",1)
        conectar.assert_not_called()

    def test_falha_ao_inserir_evento_de_reabertura_executa_rollback(self):
        conexao=MagicMock();cursor=conexao.cursor.return_value.__enter__.return_value;cursor.fetchone.return_value=self._fiscalizacao();cursor.execute.side_effect=[None,psycopg2.OperationalError("falha simulada")]
        with self.assertRaises(FiscalizacaoServiceError):FiscalizacaoService(lambda:conexao).reabrir_fiscalizacao(1,"Motivo",1)
        conexao.rollback.assert_called_once();conexao.commit.assert_not_called();self.assertEqual(cursor.execute.call_count,2)

    def test_falha_ao_atualizar_reabertura_desfaz_evento(self):
        conexao=MagicMock();cursor=conexao.cursor.return_value.__enter__.return_value;cursor.fetchone.return_value=self._fiscalizacao();cursor.execute.side_effect=[None,None,psycopg2.OperationalError("falha simulada")]
        with self.assertRaises(FiscalizacaoServiceError):FiscalizacaoService(lambda:conexao).reabrir_fiscalizacao(1,"Motivo",1)
        conexao.rollback.assert_called_once();conexao.commit.assert_not_called()

    def test_finalizacao_e_cancelamento_inserem_evento_antes_da_atualizacao(self):
        for novo,justificativa,tipo in (("Finalizada",None,"Finalização"),("Cancelada","Registro incorreto","Cancelamento")):
            conexao=MagicMock();cursor=conexao.cursor.return_value.__enter__.return_value;cursor.fetchone.return_value=self._fiscalizacao("Em elaboração")
            FiscalizacaoService(lambda:conexao).alterar_status(1,novo,9,justificativa)
            self.assertIn("INSERT INTO fc_fiscalizacao_eventos",cursor.execute.call_args_list[1].args[0]);self.assertEqual(cursor.execute.call_args_list[1].args[1],(1,tipo,"Em elaboração",novo,justificativa,9));self.assertIn("UPDATE fc_fiscalizacoes",cursor.execute.call_args_list[2].args[0]);conexao.commit.assert_called_once()

    def test_cancelamento_exige_justificativa_sem_abrir_conexao(self):
        conectar=MagicMock()
        with self.assertRaises(FiscalizacaoBloqueadaError):FiscalizacaoService(conectar).alterar_status(1,"Cancelada",1,"  ")
        conectar.assert_not_called()

    def test_historico_consulta_usuario_e_ordena_mais_recente(self):
        conexao=MagicMock();cursor=conexao.__enter__.return_value.cursor.return_value.__enter__.return_value;cursor.fetchall.return_value=[]
        FiscalizacaoService(lambda:conexao).listar_eventos_fiscalizacao(1)
        consulta=cursor.execute.call_args.args[0];self.assertIn("JOIN usuarios",consulta);self.assertIn("ORDER BY ev.criado_em DESC,ev.id DESC",consulta);self.assertEqual(cursor.execute.call_args.args[1],(1,))
    def _conexao_acompanhamento(self,falha_na_chamada):
        conexao=MagicMock();cursor=conexao.cursor.return_value.__enter__.return_value;cursor.fetchone.return_value={"id":1,"ativo":True,"status":"Aberta","data_identificacao":date(2026,7,1),"data_regularizacao":None};efeitos=[None,None,None];efeitos[falha_na_chamada-1]=psycopg2.OperationalError("falha simulada");cursor.execute.side_effect=efeitos;return conexao
    def test_falha_no_historico_executa_rollback(self):
        c=self._conexao_acompanhamento(2);dados={"data_acompanhamento":date(2026,7,20),"status_novo":"Em acompanhamento","descricao":"ok","providencia_contratada":None,"observacoes":None,"data_regularizacao":None}
        with self.assertRaises(OcorrenciaServiceError):OcorrenciaService(lambda:c).adicionar_acompanhamento(1,dados,1)
        c.rollback.assert_called_once();c.commit.assert_not_called()
    def test_falha_na_atualizacao_executa_rollback(self):
        c=self._conexao_acompanhamento(3);dados={"data_acompanhamento":date(2026,7,20),"status_novo":"Em acompanhamento","descricao":"ok","providencia_contratada":None,"observacoes":None,"data_regularizacao":None}
        with self.assertRaises(OcorrenciaServiceError):OcorrenciaService(lambda:c).adicionar_acompanhamento(1,dados,1)
        c.rollback.assert_called_once();c.commit.assert_not_called()
    def test_historico_e_status_sao_confirmados_juntos(self):
        c=MagicMock();cursor=c.cursor.return_value.__enter__.return_value;cursor.fetchone.return_value={"id":1,"ativo":True,"status":"Aberta","data_identificacao":date(2026,7,1),"data_regularizacao":None};dados={"data_acompanhamento":date(2026,7,20),"status_novo":"Em acompanhamento","descricao":"ok","providencia_contratada":None,"observacoes":None,"data_regularizacao":None};OcorrenciaService(lambda:c).adicionar_acompanhamento(1,dados,1);c.commit.assert_called_once();c.rollback.assert_not_called();self.assertIn("INSERT INTO fc_ocorrencia_acompanhamentos",cursor.execute.call_args_list[1].args[0]);self.assertIn("UPDATE fc_ocorrencias",cursor.execute.call_args_list[2].args[0])

    def test_reabertura_preserva_data_e_justificativa_no_historico(self):
        c=MagicMock();cursor=c.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value={"id":1,"ativo":True,"status":"Regularizada","data_identificacao":date(2026,7,1),"data_regularizacao":date(2026,7,10)}
        dados={"data_acompanhamento":date(2026,7,20),"status_novo":"Aberta","descricao":"Falha voltou a ocorrer","providencia_contratada":"Nova correção","observacoes":"Problema reapareceu após a conferência.","data_regularizacao":None,"confirmar_saida_regularizada":True}
        OcorrenciaService(lambda:c).adicionar_acompanhamento(1,dados,1)
        parametros_historico=cursor.execute.call_args_list[1].args[1]
        self.assertEqual(parametros_historico[2],"Regularizada")
        self.assertEqual(parametros_historico[4],"Falha voltou a ocorrer")
        self.assertIn("Regularização anterior: 10/07/2026",parametros_historico[6])
        self.assertIn("Problema reapareceu",parametros_historico[6])
        self.assertIsNone(cursor.execute.call_args_list[2].args[1][1])


def tearDownModule(): PATCH_CONEXAO.stop()
