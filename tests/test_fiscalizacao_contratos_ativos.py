"""Testes da Etapa 2G sem PostgreSQL, Cloudinary ou arquivos reais."""

import importlib
import os
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import psycopg2

from modulos.fiscalizacao_contratos.services.ativos_service import (
    AtivoBloqueadoError, AtivoDuplicadoError, AtivoService, AtivoServiceError,
    ReferenciaAtivoInvalidaError, VinculoDuplicadoError,
)
from modulos.fiscalizacao_contratos.validacoes_ativos import (
    normalizar_e_validar_ativo, normalizar_e_validar_vinculo,
)


CONEXAO_FALSA = MagicMock(name="conexao_falsa_ativos")
PATCH_CONEXAO = patch("psycopg2.connect", return_value=CONEXAO_FALSA)
MOCK_CONNECT = PATCH_CONEXAO.start()
sys.modules.pop("app", None)
with patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False), patch("dotenv.load_dotenv", return_value=False):
    APP_MODULE = importlib.import_module("app")
MOCK_CONNECT.side_effect = AssertionError("Nenhuma conexão real é permitida")


class AtivoServiceFake:
    def __init__(self):
        self.empresas = {
            1: {"id": 1, "razao_social": "Empresa Ativa", "ativo": True},
            2: {"id": 2, "razao_social": "Empresa Inativa", "ativo": False},
        }
        self.contratos = {
            1: {"id": 1, "numero_contrato": "CT-001/2026", "ativo": True},
            2: {"id": 2, "numero_contrato": "CT-002/2026", "ativo": False},
        }
        self.ativos = {1: self._ativo(1)}
        self.vinculos = {}
        self.proximo_ativo = 2
        self.proximo_vinculo = 1
        self.ultimo_filtro = None

    def _ativo(self, aid, **extra):
        base = {
            "id": aid, "codigo_interno": f"AT-{aid:03d}", "tipo_ativo": "Veículo",
            "descricao": "Caminhão coletor", "marca": "Marca", "modelo": "Modelo",
            "ano_fabricacao": 2025, "placa": f"ABC1D2{aid}", "renavam": "123456789",
            "chassi": f"CHASSI{aid}", "numero_serie": f"SERIE-{aid}",
            "numero_patrimonio": f"PAT-{aid}", "origem_ativo": "Contratada",
            "empresa_proprietaria_id": 1, "empresa_proprietaria_nome": "Empresa Ativa",
            "capacidade": Decimal("12.50000000"), "unidade_capacidade": "toneladas",
            "situacao": "Disponível", "observacoes": None, "ativo": True,
            "criado_em": datetime(2026, 7, 17), "atualizado_em": datetime(2026, 7, 17),
        }
        base.update(extra); return base

    def opcoes(self): return list(self.empresas.values()), list(self.contratos.values())

    def listar(self, busca="", tipo_ativo="", origem_ativo="", situacao="", empresa_id=None, contrato_id=None, com_vinculo_ativo="", status_ativo="ativos"):
        self.ultimo_filtro = (busca, tipo_ativo, origem_ativo, situacao, empresa_id, contrato_id, com_vinculo_ativo, status_ativo)
        resultado = []
        for a in self.ativos.values():
            links = [v for v in self.vinculos.values() if v["ativo_id"] == a["id"]]
            texto = " ".join(str(a.get(k) or "") for k in ("codigo_interno","descricao","marca","modelo","placa","renavam","chassi","numero_serie","numero_patrimonio","empresa_proprietaria_nome")).casefold()
            if busca and busca.casefold() not in texto and not any(busca.casefold() in self.contratos[v["contrato_id"]]["numero_contrato"].casefold() for v in links): continue
            if tipo_ativo and a["tipo_ativo"] != tipo_ativo: continue
            if origem_ativo and a["origem_ativo"] != origem_ativo: continue
            if situacao and a["situacao"] != situacao: continue
            if empresa_id and a["empresa_proprietaria_id"] != empresa_id: continue
            if contrato_id and not any(v["contrato_id"] == contrato_id for v in links): continue
            ativos_links = [v for v in links if v["ativo"]]
            if com_vinculo_ativo == "sim" and not ativos_links: continue
            if com_vinculo_ativo == "nao" and ativos_links: continue
            if status_ativo == "ativos" and not a["ativo"]: continue
            if status_ativo == "inativos" and a["ativo"]: continue
            resultado.append({**a, "quantidade_vinculos_ativos": len(ativos_links)})
        return resultado

    def contadores(self):
        ativos = [a for a in self.ativos.values() if a["ativo"]]
        return {"ativos_cadastrados": len(ativos), "em_operacao": sum(a["situacao"]=="Em operação" for a in ativos), "em_manutencao": sum(a["situacao"]=="Em manutenção" for a in ativos), "vinculos_ativos": sum(v["ativo"] for v in self.vinculos.values())}

    def obter(self, aid):
        a = self.ativos[aid]
        links = []
        for v in self.vinculos.values():
            if v["ativo_id"] == aid:
                c = self.contratos[v["contrato_id"]]
                links.append({**v, "numero_contrato": c["numero_contrato"], "empresa_contratada": "Empresa contratada", "contrato_objeto": "Objeto"})
        return a, sorted(links, key=lambda v: (not v["ativo"], v["id"]))

    def _validar_empresa(self, dados):
        eid = dados.get("empresa_proprietaria_id")
        if eid and eid not in self.empresas: raise ReferenciaAtivoInvalidaError("A empresa proprietária não existe.")
        if eid and not self.empresas[eid]["ativo"]: raise ReferenciaAtivoInvalidaError("A empresa proprietária está inativa.")

    def _duplicado(self, dados, excluir=None):
        for a in self.ativos.values():
            if a["id"] == excluir: continue
            for campo, mensagem in (("codigo_interno","código interno"),("placa","placa"),("chassi","chassi"),("numero_patrimonio","número de patrimônio")):
                if dados.get(campo) and str(a.get(campo) or "").casefold() == str(dados[campo]).casefold():
                    raise AtivoDuplicadoError(f"Já existe um ativo com este {mensagem}.")

    def criar(self, dados, usuario_id):
        self._validar_empresa(dados); self._duplicado(dados)
        aid = self.proximo_ativo; self.proximo_ativo += 1
        empresa = self.empresas.get(dados.get("empresa_proprietaria_id"))
        self.ativos[aid] = {**self._ativo(aid), **dados, "id": aid, "ativo": True, "empresa_proprietaria_nome": empresa["razao_social"] if empresa else None}
        return aid

    def atualizar(self, aid, dados, usuario_id):
        self._validar_empresa(dados); self._duplicado(dados, aid); self.ativos[aid].update(dados)

    def alterar_ativo(self, aid, usuario_id, ativo):
        if not ativo and any(v["ativo_id"] == aid and v["ativo"] for v in self.vinculos.values()): raise AtivoBloqueadoError("Encerre os vínculos ativos antes de inativar o ativo.")
        self.ativos[aid]["ativo"] = ativo

    def criar_vinculo(self, dados, usuario_id):
        a = self.ativos[dados["ativo_id"]]
        if not a["ativo"]: raise AtivoBloqueadoError("Ativos inativos não podem receber novos vínculos.")
        if a["situacao"] == "Baixado": raise AtivoBloqueadoError("Ativos baixados não podem receber novos vínculos.")
        if dados["contrato_id"] not in self.contratos: raise ReferenciaAtivoInvalidaError("O contrato selecionado não existe.")
        if not self.contratos[dados["contrato_id"]]["ativo"]: raise ReferenciaAtivoInvalidaError("O contrato selecionado está inativo.")
        if any(v["ativo"] and v["ativo_id"]==dados["ativo_id"] and v["contrato_id"]==dados["contrato_id"] for v in self.vinculos.values()): raise VinculoDuplicadoError("Este ativo já possui vínculo ativo com o contrato.")
        vid = self.proximo_vinculo; self.proximo_vinculo += 1
        self.vinculos[vid] = {"id": vid, **dados, "ativo": True, "data_fim": dados.get("data_fim")}
        return vid

    def obter_vinculo(self, vid): return self.vinculos[vid]

    def encerrar_vinculo(self, vid, fim, usuario_id):
        v = self.vinculos[vid]
        if not v["ativo"]: raise AtivoBloqueadoError("Este vínculo já foi encerrado.")
        if fim < v["data_inicio"]: raise ReferenciaAtivoInvalidaError("A data final não pode ser anterior à data inicial.")
        v.update(data_fim=fim, ativo=False, atualizado_em=datetime.now()); return v["ativo_id"]

    def listar_vinculos(self, busca="", status_ativo="todos"):
        resultado=[]
        for v in self.vinculos.values():
            a=self.ativos[v["ativo_id"]]; c=self.contratos[v["contrato_id"]]
            if busca and busca.casefold() not in (a["codigo_interno"]+a["descricao"]+c["numero_contrato"]).casefold(): continue
            if status_ativo=="ativos" and not v["ativo"]: continue
            if status_ativo=="encerrados" and v["ativo"]: continue
            resultado.append({**v,"codigo_interno":a["codigo_interno"],"ativo_descricao":a["descricao"],"numero_contrato":c["numero_contrato"],"empresa_contratada":"Empresa"})
        return resultado

    def listar_do_contrato(self, cid):
        return [{**v,"codigo_interno":self.ativos[v["ativo_id"]]["codigo_interno"],"tipo_ativo":self.ativos[v["ativo_id"]]["tipo_ativo"],"ativo_descricao":self.ativos[v["ativo_id"]]["descricao"],"placa":self.ativos[v["ativo_id"]]["placa"],"numero_patrimonio":self.ativos[v["ativo_id"]]["numero_patrimonio"],"situacao":self.ativos[v["ativo_id"]]["situacao"],"ativo_cadastral":self.ativos[v["ativo_id"]]["ativo"]} for v in self.vinculos.values() if v["contrato_id"]==cid]


class TestFiscalizacaoContratosAtivos(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app=APP_MODULE.app; cls.app.config.update(TESTING=True); cls.loader=APP_MODULE.login_manager._user_callback
    @classmethod
    def tearDownClass(cls): APP_MODULE.login_manager._user_callback=cls.loader
    def setUp(self):
        self.client=self.app.test_client(); self.servico=AtivoServiceFake(); APP_MODULE.login_manager._user_callback=self._usuario
        self.patcher=patch("modulos.fiscalizacao_contratos.routes.ativos.AtivoService", return_value=self.servico); self.patcher.start()
    def tearDown(self): self.patcher.stop()
    @staticmethod
    def _usuario(uid): return {"1":APP_MODULE.User(1,"admin","admin",None),"2":APP_MODULE.User(2,"comum","usuario",None)}.get(str(uid))
    def autenticar(self, uid):
        with self.client.session_transaction() as s: s["_user_id"]=str(uid); s["_fresh"]=True
    @staticmethod
    def dados_ativo(**extra):
        dados={"codigo_interno":" at-002 ","tipo_ativo":"Veículo","descricao":" Caminhão ","marca":"Marca","modelo":"Modelo","ano_fabricacao":"2025","placa":" abc 1d23 ","renavam":"123 456","chassi":" chassi-2 ","numero_serie":" serie 2 ","numero_patrimonio":" pat-2 ","origem_ativo":"Contratada","empresa_proprietaria_id":"1","capacidade":"12,500000","unidade_capacidade":"toneladas","situacao":"Disponível","observacoes":"teste"}; dados.update(extra); return dados
    @staticmethod
    def dados_vinculo(**extra):
        dados={"contrato_id":"1","natureza_vinculo":"Operacional","data_inicio":"2026-07-17","data_fim":"","principal":"1","observacoes":"teste"}; dados.update(extra); return dados

    def test_admin_acessa_listagem_e_cartao(self):
        self.autenticar(1); r=self.client.get("/fiscalizacao-contratos/ativos"); self.assertEqual(r.status_code,200); self.assertIn(b"AT-001",r.data); self.assertIn("Ativos Contratuais".encode(),self.client.get("/fiscalizacao-contratos").data)
    def test_visitante_e_comum_sao_bloqueados_em_todas_as_rotas(self):
        self.assertIn("/login",self.client.get("/fiscalizacao-contratos/ativos").headers["Location"]); self.autenticar(2)
        gets=["/fiscalizacao-contratos/ativos","/fiscalizacao-contratos/ativos/novo","/fiscalizacao-contratos/ativos/1","/fiscalizacao-contratos/ativos/1/editar","/fiscalizacao-contratos/ativos/1/vincular","/fiscalizacao-contratos/ativos/vinculos"]
        posts=["/fiscalizacao-contratos/ativos/novo","/fiscalizacao-contratos/ativos/1/editar","/fiscalizacao-contratos/ativos/1/inativar","/fiscalizacao-contratos/ativos/1/reativar","/fiscalizacao-contratos/ativos/1/vincular","/fiscalizacao-contratos/ativos/vinculos/1/encerrar"]
        self.assertTrue(all(self.client.get(x).status_code==403 for x in gets)); self.assertTrue(all(self.client.post(x).status_code==403 for x in posts))
    def test_cria_ativo_e_normaliza_campos_com_decimal(self):
        self.autenticar(1); r=self.client.post("/fiscalizacao-contratos/ativos/novo",data=self.dados_ativo()); self.assertEqual(r.status_code,302); a=self.servico.ativos[2]; self.assertEqual(a["codigo_interno"],"AT-002"); self.assertEqual(a["placa"],"ABC1D23"); self.assertEqual(a["chassi"],"CHASSI2"); self.assertEqual(a["capacidade"],Decimal("12.500000")); self.assertIsInstance(a["capacidade"],Decimal)
    def test_identificacoes_compostas_so_por_separadores_ficam_vazias(self):
        dados, erros = normalizar_e_validar_ativo(
            self.dados_ativo(placa="---", chassi="///", renavam="...", numero_patrimonio="   "),
            2026,
        )
        self.assertFalse(erros)
        self.assertIsNone(dados["placa"]); self.assertIsNone(dados["chassi"])
        self.assertIsNone(dados["renavam"]); self.assertIsNone(dados["numero_patrimonio"])
    def test_campos_obrigatorios_e_enumeracoes_invalidas(self):
        for campo,valor in (("codigo_interno"," "),("descricao",""),("tipo_ativo","Inválido"),("origem_ativo","Inválida"),("situacao","Inválida")):
            d=self.dados_ativo(**{campo:valor}); _,erros=normalizar_e_validar_ativo(d,2026); self.assertTrue(erros)
    def test_ano_e_capacidade_invalidos(self):
        for campo,valor in (("ano_fabricacao","1899"),("ano_fabricacao","2028"),("capacidade","-1")):
            d=self.dados_ativo(**{campo:valor}); _,erros=normalizar_e_validar_ativo(d,2026); self.assertTrue(erros)
    def test_duplicidades_amigaveis(self):
        self.autenticar(1)
        for campo,valor in (("codigo_interno","at-001"),("placa","abc1d21"),("chassi","chassi1"),("numero_patrimonio","PAT-1")):
            r=self.client.post("/fiscalizacao-contratos/ativos/novo",data=self.dados_ativo(**{campo:valor})); self.assertEqual(r.status_code,409)
    def test_empresa_inexistente_e_inativa_sao_rejeitadas(self):
        self.autenticar(1); self.assertEqual(self.client.post("/fiscalizacao-contratos/ativos/novo",data=self.dados_ativo(empresa_proprietaria_id="999")).status_code,400); self.assertEqual(self.client.post("/fiscalizacao-contratos/ativos/novo",data=self.dados_ativo(empresa_proprietaria_id="2")).status_code,400)
    def test_edicao_funciona(self):
        self.autenticar(1); d=self.dados_ativo(codigo_interno="AT-001",placa="XYZ9Z99",chassi="NOVOCHASSI",numero_patrimonio="NOVOPAT",descricao="Atualizado"); r=self.client.post("/fiscalizacao-contratos/ativos/1/editar",data=d); self.assertEqual(r.status_code,302); self.assertEqual(self.servico.ativos[1]["descricao"],"Atualizado")
        self.assertEqual(self.servico.ativos[1]["codigo_interno"], "AT-001")
    def test_inativacao_preserva_e_reativacao_funciona(self):
        self.autenticar(1); self.client.post("/fiscalizacao-contratos/ativos/1/inativar"); self.assertIn(1,self.servico.ativos); self.assertFalse(self.servico.ativos[1]["ativo"]); self.client.post("/fiscalizacao-contratos/ativos/1/reativar"); self.assertTrue(self.servico.ativos[1]["ativo"])
        self.servico.ativos[1]["situacao"] = "Baixado"
        self.client.post("/fiscalizacao-contratos/ativos/1/inativar")
        self.client.post("/fiscalizacao-contratos/ativos/1/reativar")
        self.assertEqual(self.servico.ativos[1]["situacao"], "Baixado")
    def test_ativo_com_vinculo_nao_pode_ser_inativado(self):
        dados,_=normalizar_e_validar_vinculo({"ativo_id":"1",**self.dados_vinculo()}); self.servico.criar_vinculo(dados,1); self.autenticar(1); self.client.post("/fiscalizacao-contratos/ativos/1/inativar"); self.assertTrue(self.servico.ativos[1]["ativo"])
    def test_ativo_inativo_ou_baixado_nao_pode_ser_vinculado(self):
        self.autenticar(1); self.servico.ativos[1]["ativo"]=False; self.assertEqual(self.client.get("/fiscalizacao-contratos/ativos/1/vincular").status_code,302); self.servico.ativos[1].update(ativo=True,situacao="Baixado"); self.assertEqual(self.client.get("/fiscalizacao-contratos/ativos/1/vincular").status_code,302)
    def test_cria_vinculo_e_rejeita_duplicado(self):
        self.autenticar(1); r=self.client.post("/fiscalizacao-contratos/ativos/1/vincular",data=self.dados_vinculo()); self.assertEqual(r.status_code,302); r2=self.client.post("/fiscalizacao-contratos/ativos/1/vincular",data=self.dados_vinculo()); self.assertEqual(r2.status_code,409)
    def test_mesmo_ativo_pode_vincular_contratos_diferentes(self):
        self.servico.contratos[2]["ativo"] = True
        primeiro,_=normalizar_e_validar_vinculo({"ativo_id":"1",**self.dados_vinculo(contrato_id="1")})
        segundo,_=normalizar_e_validar_vinculo({"ativo_id":"1",**self.dados_vinculo(contrato_id="2")})
        self.servico.criar_vinculo(primeiro,1); self.servico.criar_vinculo(segundo,1)
        self.assertEqual(len(self.servico.vinculos),2)
    def test_contrato_inexistente_e_inativo_sao_rejeitados(self):
        self.autenticar(1); self.assertEqual(self.client.post("/fiscalizacao-contratos/ativos/1/vincular",data=self.dados_vinculo(contrato_id="999")).status_code,400); self.assertEqual(self.client.post("/fiscalizacao-contratos/ativos/1/vincular",data=self.dados_vinculo(contrato_id="2")).status_code,400)
    def test_data_final_anterior_e_rejeitada(self):
        _,erros=normalizar_e_validar_vinculo({"ativo_id":"1",**self.dados_vinculo(data_fim="2026-07-16")}); self.assertTrue(erros)
        _, erros_futura = normalizar_e_validar_vinculo({"ativo_id":"1",**self.dados_vinculo(data_fim="2026-07-18")})
        self.assertTrue(any("não deve possuir data final" in erro for erro in erros_futura))
    def test_encerramento_preserva_periodo_e_novo_vinculo_cria_registro(self):
        dados,_=normalizar_e_validar_vinculo({"ativo_id":"1",**self.dados_vinculo()}); vid=self.servico.criar_vinculo(dados,1); self.autenticar(1); r=self.client.post(f"/fiscalizacao-contratos/ativos/vinculos/{vid}/encerrar",data={"data_fim":"2026-07-18"}); self.assertEqual(r.status_code,302); self.assertIn(vid,self.servico.vinculos); self.assertFalse(self.servico.vinculos[vid]["ativo"]); self.assertEqual(self.servico.vinculos[vid]["data_fim"],date(2026,7,18)); novo=self.servico.criar_vinculo(dados,1); self.assertNotEqual(novo,vid); self.assertEqual(len(self.servico.vinculos),2)
        self.servico.encerrar_vinculo(novo,date(2026,7,19),1)
        self.servico.alterar_ativo(1,1,False); self.servico.alterar_ativo(1,1,True)
        self.assertFalse(self.servico.vinculos[vid]["ativo"])
    def test_encerramento_real_usa_rollback_em_falha(self):
        conexao=MagicMock(); cursor=conexao.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value={"id":1,"ativo_id":1,"data_inicio":date(2026,7,17),"ativo":True}
        cursor.execute.side_effect=[None,psycopg2.OperationalError("falha simulada")]
        with self.assertRaises(AtivoServiceError):
            AtivoService(lambda: conexao).encerrar_vinculo(1,date(2026,7,18),1)
        conexao.rollback.assert_called_once(); conexao.commit.assert_not_called()
    def test_servico_valida_dados_essenciais_sem_depender_da_rota(self):
        with self.assertRaises(ReferenciaAtivoInvalidaError):
            AtivoService._validar_dados_ativo({"ano_fabricacao":1899,"capacidade":Decimal("1")})
        with self.assertRaises(ReferenciaAtivoInvalidaError):
            AtivoService._validar_dados_ativo({"ano_fabricacao":date.today().year+2,"capacidade":Decimal("1")})
        with self.assertRaises(ReferenciaAtivoInvalidaError):
            AtivoService._validar_dados_vinculo({"natureza_vinculo":"Inválida","data_inicio":date.today(),"data_fim":None})
    def test_historico_e_listagem_de_vinculos(self):
        dados,_=normalizar_e_validar_vinculo({"ativo_id":"1",**self.dados_vinculo()}); self.servico.criar_vinculo(dados,1); self.autenticar(1); self.assertIn(b"CT-001/2026",self.client.get("/fiscalizacao-contratos/ativos/1").data); self.assertIn(b"AT-001",self.client.get("/fiscalizacao-contratos/ativos/vinculos").data)
    def test_pesquisa_filtros_e_contadores(self):
        self.autenticar(1); r=self.client.get("/fiscalizacao-contratos/ativos?busca=Caminh%C3%A3o&tipo_ativo=Ve%C3%ADculo&origem_ativo=Contratada&situacao=Dispon%C3%ADvel&empresa_id=1&com_vinculo_ativo=nao&status_ativo=todos"); self.assertEqual(r.status_code,200); self.assertEqual(self.servico.ultimo_filtro[1],"Veículo")
    def test_ativos_aparecem_no_detalhe_do_contrato(self):
        dados,_=normalizar_e_validar_vinculo({"ativo_id":"1",**self.dados_vinculo()}); self.servico.criar_vinculo(dados,1); self.autenticar(1)
        contrato={"id":1,"numero_contrato":"CT-001/2026","valor_original":Decimal("1"),"ativo":True,"situacao":"Vigente","vence_em_60_dias":False,"objeto":"Objeto","empresa_nome":"Empresa","processo_administrativo":None,"data_assinatura":None,"vigencia_inicio":None,"vigencia_fim":None,"observacoes":None,"criado_em":None,"atualizado_em":None}
        with patch("modulos.fiscalizacao_contratos.routes.contratos.ContratoService") as c, patch("modulos.fiscalizacao_contratos.routes.contratos.AditivoService") as ad, patch("modulos.fiscalizacao_contratos.routes.contratos.DocumentoService") as doc, patch("modulos.fiscalizacao_contratos.routes.contratos.PlanilhaService") as pl, patch("modulos.fiscalizacao_contratos.routes.contratos.AtivoService",return_value=self.servico):
            c.return_value.obter.return_value=(contrato,[]); ad.return_value.resumo_contrato.return_value=(None,[]); doc.return_value.listar_do_contrato.return_value=[]; pl.return_value.comparar_contrato.return_value={"planilhas":[]}; r=self.client.get("/fiscalizacao-contratos/contratos/1")
        self.assertEqual(r.status_code,200); self.assertIn(b"AT-001",r.data)
    def test_rotas_antigas_continuam_registradas(self):
        rotas={x.rule for x in self.app.url_map.iter_rules()}; self.assertTrue({"/","/login","/fiscalizacao-contratos/empresas","/fiscalizacao-contratos/contratos","/fiscalizacao-contratos/ativos"}.issubset(rotas))
    def test_migracao_aditiva_e_sem_execucao_automatica(self):
        caminho=os.path.join(os.path.dirname(os.path.dirname(__file__)),"modulos","fiscalizacao_contratos","migrations","007_criar_fc_ativos_contratuais.sql")
        with open(caminho,encoding="utf-8") as arquivo: sql=arquivo.read().upper()
        self.assertIn("CREATE TABLE IF NOT EXISTS FC_ATIVOS_CONTRATUAIS",sql); self.assertIn("CREATE TABLE IF NOT EXISTS FC_ATIVO_VINCULOS",sql)
        self.assertIn("CK_FC_ATIVO_VINCULOS_ESTADO_DATAS", sql)
        for cmd in ("DROP","TRUNCATE","DELETE","UPDATE","INSERT","ALTER TABLE"): self.assertNotIn(cmd,sql)
    def test_servico_nao_usa_delete_e_atualiza_timestamp(self):
        caminho=os.path.join(os.path.dirname(os.path.dirname(__file__)),"modulos","fiscalizacao_contratos","services","ativos_service.py")
        with open(caminho,encoding="utf-8") as arquivo: codigo=arquivo.read().upper()
        self.assertNotIn("DELETE FROM",codigo); self.assertNotIn("FLOAT(",codigo); self.assertIn("ATUALIZADO_EM=CURRENT_TIMESTAMP",codigo)
    def test_nenhum_servico_real_e_acessado(self):
        chamadas=MOCK_CONNECT.call_count; self.autenticar(1); self.client.get("/fiscalizacao-contratos/ativos"); self.assertEqual(MOCK_CONNECT.call_count,chamadas)


def tearDownModule(): PATCH_CONEXAO.stop()
if __name__=="__main__": unittest.main()
