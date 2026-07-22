"""Testes seguros da Etapa 2J, sem PostgreSQL ou Cloudinary reais."""

import importlib
import io
import os
import sys
import unittest
import psycopg2
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

from modulos.fiscalizacao_contratos.services.atestes_service import (
    AtesteBloqueadoError, AtesteDuplicadoError, AtesteService,
    AtesteServiceError, ReferenciaAtesteInvalidaError,
)
from modulos.fiscalizacao_contratos.services.cloudinary_storage import CloudinaryStorageError
from modulos.fiscalizacao_contratos.validacoes_atestes import (
    decimal_monetario, diferenca_notas, normalizar_ateste, normalizar_nota,
)


RAIZ = Path(__file__).resolve().parents[1]
CONEXAO_FALSA = MagicMock(name="conexao_falsa_atestes")
PATCH_CONEXAO = patch("psycopg2.connect", return_value=CONEXAO_FALSA)
MOCK_CONNECT = PATCH_CONEXAO.start()
sys.modules.pop("app", None)
with patch.dict(os.environ,{"DATABASE_URL":""},clear=False),patch("dotenv.load_dotenv",return_value=False):
    APP_MODULE=importlib.import_module("app")
MOCK_CONNECT.reset_mock();MOCK_CONNECT.side_effect=AssertionError("PostgreSQL real bloqueado")


class DocumentoServiceFake:
    def __init__(self,*args,**kwargs): pass
    def listar_do_contrato(self,contrato_id):
        return [d for d in AtesteServiceFake.documentos_catalogo if d["contrato_id"]==contrato_id]


class AtesteServiceFake:
    documentos_catalogo=[
        {"id":1,"contrato_id":1,"titulo":"Nota 1","nome_original":"nota1.pdf","ativo":True},
        {"id":2,"contrato_id":2,"titulo":"Outro","nome_original":"outro.pdf","ativo":True},
        {"id":3,"contrato_id":1,"titulo":"Inativo","nome_original":"inativo.pdf","ativo":False},
    ]

    def __init__(self):
        self.documentos_catalogo=deepcopy(type(self).documentos_catalogo)
        self.medicoes={
            1:{"id":1,"contrato_id":1,"numero_medicao":1,"competencia":date(2026,7,1),"versao":1,"valor_bruto":Decimal("7500.00"),"total_acrescimos":Decimal("5.00"),"total_descontos":Decimal("0.00"),"total_glosas":Decimal("10.00"),"valor_liquido":Decimal("7495.00"),"status":"Aprovada","ativo":True,"atual":True,"numero_contrato":"CT-1","empresa_nome":"Empresa Um"},
            2:{"id":2,"contrato_id":1,"numero_medicao":2,"competencia":date(2026,8,1),"versao":1,"valor_liquido":Decimal("100"),"status":"Em análise","ativo":True,"atual":True,"numero_contrato":"CT-1","empresa_nome":"Empresa Um"},
            3:{"id":3,"contrato_id":1,"numero_medicao":1,"competencia":date(2026,7,1),"versao":0,"valor_liquido":Decimal("100"),"status":"Aprovada","ativo":True,"atual":False,"numero_contrato":"CT-1","empresa_nome":"Empresa Um"},
            4:{"id":4,"contrato_id":1,"numero_medicao":4,"competencia":date(2026,9,1),"versao":1,"valor_liquido":Decimal("100"),"status":"Aprovada","ativo":False,"atual":True,"numero_contrato":"CT-1","empresa_nome":"Empresa Um"},
        }
        self.servidores={1:{"id":1,"nome":"Atestador","matricula":"A1","ativo":True},2:{"id":2,"nome":"Inativo","matricula":"I1","ativo":False},3:{"id":3,"nome":"Encaminhador","matricula":"E1","ativo":True}}
        self.atestes={};self.notas={};self.documentos={};self.eventos={};self.proximo=1;self.proximo_registro=1
        self.ultimo_filtro=None;self.rollback_simulado=False;self.ultimo_upload=None

    def opcoes(self):
        return [m for m in self.medicoes.values() if m["ativo"] and m["atual"] and m["status"]=="Aprovada"],[s for s in self.servidores.values() if s["ativo"]],[{"id":1,"razao_social":"Empresa Um"}]
    def _medicao(self,identificador):
        m=self.medicoes.get(identificador)
        if not m or not m["ativo"] or not m["atual"] or m["status"]!="Aprovada": raise ReferenciaAtesteInvalidaError("Somente uma medição aprovada, ativa e atual pode receber ateste.")
        return m
    def _servidor(self,identificador):
        s=self.servidores.get(identificador)
        if not s or not s["ativo"]: raise ReferenciaAtesteInvalidaError("Servidor deve estar ativo.")
        return s
    def _evento(self,ateste_id,tipo,anterior,novo,justificativa=None):
        total=sum((n["valor_nota"] for n in self.notas[ateste_id] if n["ativo"]),Decimal("0"))
        self.eventos[ateste_id].append({"id":self.proximo_registro,"tipo_evento":tipo,"status_anterior":anterior,"status_novo":novo,"justificativa":justificativa,"valor_atestado":self.atestes[ateste_id]["valor_atestado"],"total_notas":total,"criado_em":datetime(2026,7,21,12,self.proximo_registro%60),"usuario_nome":"admin"});self.proximo_registro+=1
    def criar(self,dados,usuario_id):
        m=self._medicao(dados["medicao_id"]);self._servidor(dados["servidor_atestador_id"])
        if any(a["medicao_id"]==m["id"] and a["ativo"] and a["status"]!="Cancelado" for a in self.atestes.values()): raise AtesteDuplicadoError("Esta medição já possui um ateste ativo.")
        i=self.proximo;self.proximo+=1
        self.atestes[i]={"id":i,"medicao_id":m["id"],"numero_ateste":dados["numero_ateste"],"servidor_atestador_id":dados["servidor_atestador_id"],"atestador_nome":self.servidores[dados["servidor_atestador_id"]]["nome"],"data_ateste":None,"status":"Em elaboração","parecer":dados.get("parecer"),"observacoes":dados.get("observacoes"),"valor_atestado":Decimal(str(m["valor_liquido"])),"protocolo_encaminhamento":None,"encaminhado_em":None,"encaminhador_nome":None,"ativo":True,"criado_em":datetime(2026,7,21,10),"atualizado_em":datetime(2026,7,21,10),"criador_nome":"admin",**{k:m[k] for k in ("contrato_id","numero_medicao","competencia","valor_bruto","total_acrescimos","total_descontos","total_glosas","valor_liquido")},"medicao_versao":m["versao"],"numero_contrato":m["numero_contrato"],"empresa_nome":m["empresa_nome"],"processo_administrativo":"PA-1"}
        self.notas[i]=[];self.documentos[i]=[];self.eventos[i]=[];self._evento(i,"Criação",None,"Em elaboração");return i
    def atualizar(self,i,dados,usuario_id):
        a=self.atestes[i]
        if a["status"] not in ("Em elaboração","Devolvido para correção"): raise AtesteBloqueadoError("bloqueado")
        self._servidor(dados["servidor_atestador_id"]);a.update(numero_ateste=dados["numero_ateste"],servidor_atestador_id=dados["servidor_atestador_id"],parecer=dados.get("parecer"),observacoes=dados.get("observacoes"))
    def obter(self,i):
        a=self.atestes[i];notas=self.notas[i];total=sum((n["valor_nota"] for n in notas if n["ativo"]),Decimal("0")).quantize(Decimal("0.01"));return a,notas,self.documentos[i],list(reversed(self.eventos[i])),total,(total-a["valor_atestado"]).quantize(Decimal("0.01"))
    def obter_da_medicao(self,medicao_id): return next((self._linha(a) for a in self.atestes.values() if a["medicao_id"]==medicao_id and a["status"]!="Cancelado"),None)
    def _linha(self,a):
        total=sum((n["valor_nota"] for n in self.notas[a["id"]] if n["ativo"]),Decimal("0"));return {**a,"total_notas":total,"diferenca_notas":total-a["valor_atestado"]}
    def listar(self,busca="",filtros=None):
        self.ultimo_filtro=(busca,filtros or {});itens=[self._linha(a) for a in self.atestes.values()]
        texto=str(busca or "").lower()
        if texto: itens=[a for a in itens if texto in str(a["numero_ateste"]).lower() or texto in a["numero_contrato"].lower() or any(texto in n["numero_nota"].lower() or texto in str(n.get("chave_acesso") or "").lower() for n in self.notas[a["id"]])]
        if (filtros or {}).get("status"): itens=[a for a in itens if a["status"]==filtros["status"]]
        return itens
    def listar_do_contrato(self,contrato_id,limite=10):
        itens=[self._linha(a) for a in self.atestes.values() if a["contrato_id"]==contrato_id];return itens[:limite],{"total":len(itens),"elaboracao":sum(a["status"]=="Em elaboração" for a in itens),"devolvidos":sum(a["status"]=="Devolvido para correção" for a in itens),"atestados":sum(a["status"]=="Atestado" for a in itens),"encaminhados":sum(a["status"]=="Encaminhado para pagamento" for a in itens),"valor_encaminhado":sum((a["valor_atestado"] for a in itens if a["status"]=="Encaminhado para pagamento"),Decimal("0"))}
    def indicadores(self): return {"elaboracao":sum(a["status"]=="Em elaboração" for a in self.atestes.values()),"devolvidos":sum(a["status"]=="Devolvido para correção" for a in self.atestes.values()),"aguardando":sum(a["status"]=="Atestado" for a in self.atestes.values()),"encaminhados_mes":sum(a["status"]=="Encaminhado para pagamento" for a in self.atestes.values()),"valor_encaminhado_mes":sum((a["valor_atestado"] for a in self.atestes.values() if a["status"]=="Encaminhado para pagamento"),Decimal("0"))}
    def _alteravel(self,i):
        if self.atestes[i]["status"] not in ("Em elaboração","Devolvido para correção","Atestado"): raise AtesteBloqueadoError("bloqueado")
    def salvar_nota(self,i,dados,usuario_id,nota_id=None):
        self._alteravel(i)
        if dados.get("documento_id") and not any(d["id"]==dados["documento_id"] and d["contrato_id"]==self.atestes[i]["contrato_id"] and d["ativo"] for d in self.documentos_catalogo): raise ReferenciaAtesteInvalidaError("Documento inválido")
        if any(n["ativo"] and n["numero_nota"].lower()==dados["numero_nota"].lower() and (n.get("serie") or "").lower()==(dados.get("serie") or "").lower() and n["id"]!=nota_id for n in self.notas[i]): raise AtesteDuplicadoError("Nota duplicada")
        if nota_id: next(n for n in self.notas[i] if n["id"]==nota_id).update(dados)
        else: self.notas[i].append({"id":self.proximo_registro,"ativo":True,"documento_titulo":"Nota" if dados.get("documento_id") else None,"nome_original":"nota.pdf" if dados.get("documento_id") else None,**dados});self.proximo_registro+=1
    def salvar_nota_com_upload(self,i,dados,arquivo,usuario_id,armazenamento,nota_id=None):
        self.ultimo_upload=armazenamento.enviar(arquivo,self.atestes[i]["contrato_id"],None);documento_id=1000
        self.documentos_catalogo.append({"id":documento_id,"contrato_id":self.atestes[i]["contrato_id"],"titulo":f"Nota fiscal {dados['numero_nota']}","nome_original":arquivo["nome_original"],"ativo":True})
        self.salvar_nota(i,{**dados,"documento_id":documento_id},usuario_id,nota_id);return documento_id
    def inativar_nota(self,i,nota_id,usuario_id): self._alteravel(i);next(n for n in self.notas[i] if n["id"]==nota_id)["ativo"]=False
    def vincular_documento(self,i,documento_id,categoria,observacoes,usuario_id):
        self._alteravel(i)
        doc=next((d for d in self.documentos_catalogo if d["id"]==documento_id and d["contrato_id"]==self.atestes[i]["contrato_id"] and d["ativo"]),None)
        if not doc: raise ReferenciaAtesteInvalidaError("Documento inválido")
        if any(d["documento_id"]==documento_id and d["ativo"] for d in self.documentos[i]): raise AtesteDuplicadoError("Documento duplicado")
        self.documentos[i].append({"id":self.proximo_registro,"documento_id":documento_id,"categoria":categoria,"observacoes":observacoes,"ativo":True,"titulo":doc["titulo"],"nome_original":doc["nome_original"]});self.proximo_registro+=1
    def inativar_documento(self,i,vinculo_id,usuario_id): self._alteravel(i);next(d for d in self.documentos[i] if d["id"]==vinculo_id)["ativo"]=False
    def atestar(self,i,usuario_id):
        a=self.atestes[i]
        if a["status"] not in ("Em elaboração","Devolvido para correção") or not a.get("parecer"): raise AtesteBloqueadoError("Parecer obrigatório")
        self._servidor(a["servidor_atestador_id"]);m=self._medicao(a["medicao_id"])
        if not any(n["ativo"] for n in self.notas[i]) and not any(d["ativo"] for d in self.documentos[i]): raise AtesteBloqueadoError("Comprovação obrigatória")
        anterior=a["status"];a.update(status="Atestado",data_ateste=date(2026,7,21),valor_atestado=m["valor_liquido"]);self._evento(i,"Ateste",anterior,"Atestado")
    def devolver(self,i,justificativa,usuario_id): self._fluxo(i,"Devolvido para correção","Devolução para correção",justificativa,("Em elaboração","Atestado"),True)
    def retornar_elaboracao(self,i,justificativa,usuario_id): self._fluxo(i,"Em elaboração","Retorno para elaboração",justificativa,("Devolvido para correção",))
    def cancelar(self,i,justificativa,usuario_id): self._fluxo(i,"Cancelado","Cancelamento",justificativa,("Em elaboração","Devolvido para correção","Atestado"))
    def _fluxo(self,i,novo,tipo,justificativa,permitidos,limpar=False):
        a=self.atestes[i]
        if a["status"] not in permitidos or not str(justificativa or "").strip(): raise AtesteBloqueadoError("Justificativa obrigatória")
        anterior=a["status"];a["status"]=novo
        if limpar:a["data_ateste"]=None
        self._evento(i,tipo,anterior,novo,justificativa)
    def encaminhar(self,i,protocolo,servidor_id,usuario_id):
        a=self.atestes[i]
        if a["status"]!="Atestado": raise AtesteBloqueadoError("Status inválido")
        if not str(protocolo or "").strip(): raise AtesteBloqueadoError("Protocolo obrigatório")
        self._servidor(servidor_id);notas=[n for n in self.notas[i] if n["ativo"]]
        if not notas: raise AtesteBloqueadoError("Nota obrigatória")
        documentos_validos={d["id"] for d in self.documentos_catalogo if d["ativo"] and d["contrato_id"]==a["contrato_id"]}
        if any(n.get("documento_id") not in documentos_validos for n in notas): raise AtesteBloqueadoError("Arquivo obrigatório")
        total=sum((n["valor_nota"] for n in notas),Decimal("0")).quantize(Decimal("0.01"))
        if total!=a["valor_atestado"].quantize(Decimal("0.01")): raise AtesteBloqueadoError("Soma diferente")
        a.update(status="Encaminhado para pagamento",protocolo_encaminhamento=protocolo,encaminhado_em=datetime(2026,7,21,14),encaminhador_nome=self.servidores[servidor_id]["nome"]);self._evento(i,"Encaminhamento para pagamento","Atestado",a["status"])


class TestFiscalizacaoContratosAtestes(unittest.TestCase):
    @classmethod
    def setUpClass(cls): cls.app=APP_MODULE.app;cls.app.config.update(TESTING=True);cls.loader=APP_MODULE.login_manager._user_callback
    @classmethod
    def tearDownClass(cls): APP_MODULE.login_manager._user_callback=cls.loader
    def setUp(self):
        self.client=self.app.test_client();APP_MODULE.login_manager._user_callback=self._usuario;self.servico=AtesteServiceFake();self.conexoes=MOCK_CONNECT.call_count
        self.armazenamento=MagicMock();self.armazenamento.enviar.return_value={"armazenamento_provedor":"cloudinary","armazenamento_chave":"privado/uuid.pdf","armazenamento_versao":"1"}
        painel_f=MagicMock();painel_f.indicadores.return_value={"ocorrencias_abertas":0,"ocorrencias_vencidas":0,"graves_criticas":0,"fiscalizacoes_30_dias":0}
        painel_m=MagicMock();painel_m.indicadores.return_value={"elaboracao":0,"analise":0,"devolvidas":0,"aprovadas_mes":0,"liquido_aprovado_mes":0,"glosas_mes":0}
        self.patchers=[patch("modulos.fiscalizacao_contratos.routes.atestes.AtesteService",return_value=self.servico),patch("modulos.fiscalizacao_contratos.routes.atestes.DocumentoService",DocumentoServiceFake),patch("modulos.fiscalizacao_contratos.routes.atestes.CloudinaryStorage",return_value=self.armazenamento),patch("modulos.fiscalizacao_contratos.routes.AtesteService",return_value=self.servico),patch("modulos.fiscalizacao_contratos.routes.medicoes.AtesteService",return_value=self.servico),patch("modulos.fiscalizacao_contratos.routes.contratos.AtesteService",return_value=self.servico),patch("modulos.fiscalizacao_contratos.routes.FiscalizacaoService",return_value=painel_f),patch("modulos.fiscalizacao_contratos.routes.MedicaoService",return_value=painel_m)]
        [p.start() for p in self.patchers]
    def tearDown(self):
        [p.stop() for p in reversed(self.patchers)];self.assertEqual(MOCK_CONNECT.call_count,self.conexoes)
    @staticmethod
    def _usuario(uid): return {"1":APP_MODULE.User(1,"admin","admin",None),"2":APP_MODULE.User(2,"comum","usuario",None)}.get(str(uid))
    def autenticar(self,uid):
        with self.client.session_transaction() as s:s["_user_id"]=str(uid);s["_fresh"]=True
    @staticmethod
    def dados(**extra):
        d={"medicao_id":"1","numero_ateste":"1","servidor_atestador_id":"1","parecer":"Execução regular","observacoes":"Conferido"};d.update(extra);return d
    @staticmethod
    def nota(**extra):
        d={"numero_nota":"NF-1","serie":"A","data_emissao":"2026-07-20","valor_nota":"7.495,00","chave_acesso":" CHAVE ","documento_id":"1","observacoes":""};d.update(extra);return d
    def criar(self,**extra): return self.servico.criar({"medicao_id":1,"numero_ateste":1,"servidor_atestador_id":1,"parecer":"Execução regular","observacoes":None,**extra},1)

    def test_admin_acessa_lista_novo_detalhe_e_painel(self):
        self.autenticar(1);i=self.criar()
        for url in ("/fiscalizacao-contratos/atestes","/fiscalizacao-contratos/atestes/novo",f"/fiscalizacao-contratos/atestes/{i}"): self.assertEqual(self.client.get(url).status_code,200,url)
        self.assertIn("Atestes".encode(),self.client.get("/fiscalizacao-contratos").data)
    def test_visitante_e_comum_bloqueados_em_todas_as_rotas(self):
        gets=["/fiscalizacao-contratos/atestes","/fiscalizacao-contratos/atestes/novo","/fiscalizacao-contratos/atestes/1","/fiscalizacao-contratos/atestes/1/editar","/fiscalizacao-contratos/atestes/1/notas/nova","/fiscalizacao-contratos/atestes/1/documentos/vincular","/fiscalizacao-contratos/atestes/1/devolver","/fiscalizacao-contratos/atestes/1/retornar","/fiscalizacao-contratos/atestes/1/encaminhar","/fiscalizacao-contratos/atestes/1/cancelar","/fiscalizacao-contratos/atestes/1/eventos"]
        posts=["/fiscalizacao-contratos/atestes/novo","/fiscalizacao-contratos/atestes/1/editar","/fiscalizacao-contratos/atestes/1/notas/nova","/fiscalizacao-contratos/atestes/1/notas/1/editar","/fiscalizacao-contratos/atestes/1/notas/1/inativar","/fiscalizacao-contratos/atestes/1/documentos/vincular","/fiscalizacao-contratos/atestes/1/documentos/1/inativar","/fiscalizacao-contratos/atestes/1/atestar","/fiscalizacao-contratos/atestes/1/devolver","/fiscalizacao-contratos/atestes/1/retornar","/fiscalizacao-contratos/atestes/1/encaminhar","/fiscalizacao-contratos/atestes/1/cancelar"]
        self.assertTrue(all(self.client.get(u).status_code==302 for u in gets));self.autenticar(2);self.assertTrue(all(self.client.get(u).status_code==403 for u in gets));self.assertTrue(all(self.client.post(u).status_code==403 for u in posts))
    def test_criacao_copia_valor_ignora_formulario_e_registra_evento(self):
        self.autenticar(1);r=self.client.post("/fiscalizacao-contratos/atestes/novo",data=self.dados(valor_atestado="1",status="Encaminhado para pagamento",data_ateste="2020-01-01",protocolo_encaminhamento="FORJADO"));self.assertEqual(r.status_code,302);a=self.servico.atestes[1];self.assertEqual(a["valor_atestado"],Decimal("7495.00"));self.assertEqual((a["status"],a["data_ateste"],a["protocolo_encaminhamento"]),("Em elaboração",None,None));self.assertEqual(self.servico.eventos[1][0]["tipo_evento"],"Criação")
    def test_medicao_inexistente_nao_aprovada_historica_e_inativa_rejeitadas(self):
        for mid in (999,2,3,4):
            with self.assertRaises(ReferenciaAtesteInvalidaError): self.servico.criar({"medicao_id":mid,"numero_ateste":1,"servidor_atestador_id":1},1)
    def test_segundo_ateste_ativo_rejeitado_e_cancelado_libera(self):
        i=self.criar()
        with self.assertRaises(AtesteDuplicadoError): self.criar(numero_ateste=2)
        self.servico.cancelar(i,"Cadastro incorreto",1);self.assertEqual(self.criar(numero_ateste=2),2)
    def test_servidor_inexistente_ou_inativo_rejeitado(self):
        for sid in (2,999):
            with self.assertRaises(ReferenciaAtesteInvalidaError): self.criar(servidor_atestador_id=sid)
    def test_validacao_numero_ateste_e_campos(self):
        for dados in (self.dados(numero_ateste="0"),self.dados(medicao_id=""),self.dados(servidor_atestador_id="")):
            _,erros=normalizar_ateste(dados);self.assertTrue(erros)
    def test_nota_normaliza_decimal_chave_e_rejeita_campos_invalidos(self):
        dados,erros=normalizar_nota(self.nota());self.assertFalse(erros);self.assertEqual(dados["valor_nota"],Decimal("7495.00"));self.assertEqual(dados["chave_acesso"],"CHAVE")
        for alteracao in ({"numero_nota":"   "},{"data_emissao":""},{"valor_nota":"0"},{"valor_nota":"-1"}): self.assertTrue(normalizar_nota(self.nota(**alteracao))[1])
    def test_nota_criar_editar_duplicar_e_inativar_sem_apagar(self):
        i=self.criar();d=normalizar_nota(self.nota())[0];self.servico.salvar_nota(i,d,1);nota=self.servico.notas[i][0];self.servico.salvar_nota(i,{**d,"valor_nota":Decimal("7000")},1,nota["id"]);self.assertEqual(nota["valor_nota"],Decimal("7000"))
        with self.assertRaises(AtesteDuplicadoError): self.servico.salvar_nota(i,d,1)
        self.servico.inativar_nota(i,nota["id"],1);self.assertFalse(nota["ativo"]);self.assertEqual(len(self.servico.notas[i]),1)
    def test_formulario_da_nota_permite_upload_imediato_e_vincula_documento(self):
        self.autenticar(1);i=self.criar();pagina=self.client.get(f"/fiscalizacao-contratos/atestes/{i}/notas/nova").data.decode();self.assertIn('enctype="multipart/form-data"',pagina);self.assertIn('name="arquivo"',pagina)
        dados={**self.nota(documento_id=""),"arquivo":(io.BytesIO(b"%PDF-1.4\nconteudo\n%%EOF"),"nota-fiscal.pdf")};resposta=self.client.post(f"/fiscalizacao-contratos/atestes/{i}/notas/nova",data=dados,content_type="multipart/form-data")
        self.assertEqual(resposta.status_code,302);self.assertEqual(self.servico.notas[i][0]["documento_id"],1000);self.armazenamento.enviar.assert_called_once()
    def test_formulario_rejeita_documento_existente_e_upload_ao_mesmo_tempo(self):
        self.autenticar(1);i=self.criar();dados={**self.nota(documento_id="1"),"arquivo":(io.BytesIO(b"%PDF-1.4\n%%EOF"),"nota.pdf")};resposta=self.client.post(f"/fiscalizacao-contratos/atestes/{i}/notas/nova",data=dados,content_type="multipart/form-data");self.assertEqual(resposta.status_code,400);self.armazenamento.enviar.assert_not_called()
    def test_documento_existente_ou_nenhum_documento_nao_faz_upload(self):
        self.autenticar(1);i=self.criar()
        for numero,documento in (("NF-1","1"),("NF-2","")):
            resposta=self.client.post(f"/fiscalizacao-contratos/atestes/{i}/notas/nova",data=self.nota(numero_nota=numero,documento_id=documento));self.assertEqual(resposta.status_code,302)
        self.armazenamento.enviar.assert_not_called()
    def test_documento_de_outro_contrato_ou_inativo_rejeitado(self):
        i=self.criar();d=normalizar_nota(self.nota())[0]
        for doc in (2,3):
            with self.assertRaises(ReferenciaAtesteInvalidaError): self.servico.salvar_nota(i,{**d,"documento_id":doc},1)
    def test_documento_complementar_vincula_duplica_e_inativa_somente_vinculo(self):
        i=self.criar();self.servico.vincular_documento(i,1,"Comprovante",None,1);v=self.servico.documentos[i][0]
        with self.assertRaises(AtesteDuplicadoError): self.servico.vincular_documento(i,1,"Comprovante",None,1)
        self.servico.inativar_documento(i,v["id"],1);self.assertFalse(v["ativo"]);self.assertTrue(self.servico.documentos_catalogo[0]["ativo"])
    def test_atestar_exige_parecer_servidor_ativo_e_comprovacao(self):
        i=self.criar(parecer=None)
        with self.assertRaises(AtesteBloqueadoError): self.servico.atestar(i,1)
        self.servico.atestes[i]["parecer"]="Regular"
        with self.assertRaises(AtesteBloqueadoError): self.servico.atestar(i,1)
        self.servico.vincular_documento(i,1,"Comprovante",None,1);self.servico.servidores[1]["ativo"]=False
        with self.assertRaises(ReferenciaAtesteInvalidaError): self.servico.atestar(i,1)
    def test_atestar_copia_valor_data_status_e_evento(self):
        i=self.criar();self.servico.vincular_documento(i,1,"Comprovante",None,1);self.servico.atestes[i]["valor_atestado"]=Decimal("1");self.servico.atestar(i,1);a=self.servico.atestes[i];self.assertEqual((a["status"],a["valor_atestado"]),("Atestado",Decimal("7495.00")));self.assertIsNotNone(a["data_ateste"]);self.assertEqual(self.servico.eventos[i][-1]["tipo_evento"],"Ateste")
    def test_devolucao_exige_justificativa_limpa_data_e_preserva_registros(self):
        i=self.criar();self.servico.vincular_documento(i,1,"Comprovante",None,1);self.servico.atestar(i,1);quantidades=(len(self.servico.notas[i]),len(self.servico.documentos[i]))
        with self.assertRaises(AtesteBloqueadoError): self.servico.devolver(i,"",1)
        self.servico.devolver(i,"Corrigir parecer",1);self.assertEqual(self.servico.atestes[i]["status"],"Devolvido para correção");self.assertIsNone(self.servico.atestes[i]["data_ateste"]);self.assertEqual(quantidades,(len(self.servico.notas[i]),len(self.servico.documentos[i])))
    def test_retorno_elaboracao_exige_justificativa_e_preserva_historico(self):
        i=self.criar();self.servico.devolver(i,"Corrigir",1)
        with self.assertRaises(AtesteBloqueadoError): self.servico.retornar_elaboracao(i,"",1)
        self.servico.retornar_elaboracao(i,"Dados corrigidos",1);self.assertEqual(self.servico.atestes[i]["status"],"Em elaboração");self.servico.vincular_documento(i,1,"Comprovante",None,1);self.servico.atestar(i,1);self.servico.devolver(i,"Rever",1);self.servico.atestar(i,1);self.assertEqual(sum(e["tipo_evento"]=="Ateste" for e in self.servico.eventos[i]),2)
    def preparar_atestado(self,total="7495.00",documento=True):
        i=self.criar();d=normalizar_nota(self.nota(valor_nota=total,documento_id="1" if documento else ""))[0];self.servico.salvar_nota(i,d,1);self.servico.atestar(i,1);return i
    def test_encaminhamento_exige_atestado_nota_arquivo_total_protocolo_e_servidor(self):
        i=self.criar()
        with self.assertRaises(AtesteBloqueadoError): self.servico.encaminhar(i,"P",3,1)
        self.servico.vincular_documento(i,1,"Comprovante",None,1);self.servico.atestar(i,1)
        with self.assertRaises(AtesteBloqueadoError): self.servico.encaminhar(i,"P",3,1)
        self.servico.salvar_nota(i,normalizar_nota(self.nota(valor_nota="7000",documento_id=""))[0],1)
        with self.assertRaises(AtesteBloqueadoError): self.servico.encaminhar(i,"P",3,1)
        self.servico.notas[i][0]["documento_id"]=1
        with self.assertRaises(AtesteBloqueadoError): self.servico.encaminhar(i,"P",3,1)
        self.servico.notas[i][0]["valor_nota"]=Decimal("7495")
        for protocolo,servidor in (("",3),("P",2),("P",999)):
            with self.assertRaises((AtesteBloqueadoError,ReferenciaAtesteInvalidaError)): self.servico.encaminhar(i,protocolo,servidor,1)
    def test_soma_varias_notas_encaminha_audita_e_torna_imutavel(self):
        i=self.criar();self.servico.salvar_nota(i,normalizar_nota(self.nota(numero_nota="1",valor_nota="7000"))[0],1);self.servico.salvar_nota(i,normalizar_nota(self.nota(numero_nota="2",valor_nota="495"))[0],1);self.servico.atestar(i,1);self.servico.encaminhar(i,"PROTO-1",3,1);a=self.servico.atestes[i];self.assertEqual(a["status"],"Encaminhado para pagamento");self.assertIsNotNone(a["encaminhado_em"]);self.assertEqual(self.servico.eventos[i][-1]["tipo_evento"],"Encaminhamento para pagamento")
        with self.assertRaises(AtesteBloqueadoError): self.servico.salvar_nota(i,normalizar_nota(self.nota(numero_nota="3"))[0],1)
        with self.assertRaises(AtesteBloqueadoError): self.servico.cancelar(i,"Não pode",1)
    def test_encaminhamento_rejeita_centavo_inativo_e_documento_que_deixou_de_ser_valido(self):
        i=self.criar();self.servico.salvar_nota(i,normalizar_nota(self.nota(numero_nota="1",valor_nota="7000"))[0],1);self.servico.salvar_nota(i,normalizar_nota(self.nota(numero_nota="2",valor_nota="494,99"))[0],1);self.servico.salvar_nota(i,normalizar_nota(self.nota(numero_nota="3",valor_nota="0,01"))[0],1);self.servico.inativar_nota(i,self.servico.notas[i][2]["id"],1);self.servico.atestar(i,1)
        with self.assertRaises(AtesteBloqueadoError): self.servico.encaminhar(i,"PROTO",3,1)
        self.servico.notas[i][1]["valor_nota"]=Decimal("495.00");self.servico.documentos_catalogo[0]["ativo"]=False
        try:
            with self.assertRaises(AtesteBloqueadoError): self.servico.encaminhar(i,"PROTO",3,1)
        finally:self.servico.documentos_catalogo[0]["ativo"]=True
    def test_cancelamento_exige_justificativa_preserva_e_libera(self):
        i=self.preparar_atestado();antes=(len(self.servico.notas[i]),len(self.servico.documentos[i]),len(self.servico.eventos[i]))
        with self.assertRaises(AtesteBloqueadoError): self.servico.cancelar(i,"",1)
        self.servico.cancelar(i,"Documento incorreto",1);self.assertEqual(self.servico.atestes[i]["status"],"Cancelado");self.assertEqual(antes[:2],(len(self.servico.notas[i]),len(self.servico.documentos[i])));self.assertTrue(self.servico.atestes[i]["ativo"]);self.assertEqual(self.criar(numero_ateste=2),2)
    def test_pesquisa_filtros_indicadores_e_ordem_eventos(self):
        i=self.criar();self.servico.salvar_nota(i,normalizar_nota(self.nota())[0],1);self.assertEqual(len(self.servico.listar("NF-1",{})),1);self.assertEqual(len(self.servico.listar("",{"status":"Em elaboração"})),1);self.assertEqual(self.servico.indicadores()["elaboracao"],1);self.assertEqual([e["tipo_evento"] for e in self.servico.obter(i)[3]],["Criação"])
    def test_tela_detalhe_distingue_encaminhamento_de_pagamento(self):
        self.autenticar(1);i=self.criar();r=self.client.get(f"/fiscalizacao-contratos/atestes/{i}");texto=r.data.decode();self.assertIn("Pagamento ainda não registrado neste módulo",texto);self.assertNotIn("Valor pago",texto);self.assertIn("não significa liquidação, pagamento ou quitação",texto)
    def test_decimal_sem_float_e_diferenca(self):
        self.assertEqual(decimal_monetario("7.495,00"),Decimal("7495.00"));self.assertEqual(diferenca_notas(Decimal("7495"),Decimal("7495")),Decimal("0.00"))
    def test_migracao_aditiva_indices_restricoes_e_sem_execucao_automatica(self):
        sql=(RAIZ/"modulos/fiscalizacao_contratos/migrations/011_criar_fc_atestes.sql").read_text(encoding="utf-8");upper=sql.upper()
        for tabela in ("fc_atestes","fc_ateste_notas_fiscais","fc_ateste_documentos","fc_ateste_eventos"): self.assertIn("CREATE TABLE IF NOT EXISTS "+tabela.upper(),upper)
        for termo in ("DROP ","TRUNCATE ","DELETE ","UPDATE ","INSERT ","ALTER TABLE"): self.assertNotIn(termo,upper)
        self.assertIn("WHERE ativo = TRUE AND status <> 'Cancelado'",sql);self.assertIn("uq_fc_ateste_nota_ativa",sql);self.assertIn("uq_fc_ateste_documento_ativo",sql)
        self.assertNotIn("atualizado_por_usuario_id INTEGER NOT NULL",sql)
    def test_codigo_sem_delete_float_credenciais_e_cloudinary(self):
        textos="\n".join((RAIZ/p).read_text(encoding="utf-8") for p in ("modulos/fiscalizacao_contratos/services/atestes_service.py","modulos/fiscalizacao_contratos/routes/atestes.py","modulos/fiscalizacao_contratos/validacoes_atestes.py"));self.assertNotIn("DELETE ",textos.upper());self.assertNotIn("float(",textos);self.assertNotIn("DATABASE_URL",textos);self.assertNotIn("destroy(",textos);self.assertIn("CloudinaryStorage",textos)
    def test_todas_rotas_atestes_tem_admin_required(self):
        import ast
        arvore=ast.parse((RAIZ/"modulos/fiscalizacao_contratos/routes/atestes.py").read_text(encoding="utf-8"));rotas=[]
        for no in ast.walk(arvore):
            if isinstance(no,(ast.FunctionDef,ast.AsyncFunctionDef)) and any(isinstance(d,ast.Call) and isinstance(d.func,ast.Attribute) and d.func.attr=="route" for d in no.decorator_list): rotas.append(no);self.assertTrue(any(isinstance(d,ast.Name) and d.id=="admin_required" for d in no.decorator_list),no.name)
        self.assertGreaterEqual(len(rotas),15)
    def test_sql_parametrizado_e_sem_dados_concatenados(self):
        texto=(RAIZ/"modulos/fiscalizacao_contratos/services/atestes_service.py").read_text(encoding="utf-8");self.assertNotIn("execute(f",texto);self.assertNotIn(".format(",texto);self.assertIn("cursor.execute(consulta,",texto);self.assertIn("d.ativo AND d.contrato_id=m.contrato_id",texto);self.assertGreaterEqual(texto.count("criado_por_usuario_id,atualizado_por_usuario_id"),3)
    def test_integracao_presente_em_medicao_contrato_e_painel(self):
        for arquivo,trecho in (("templates/fiscalizacao_contratos/medicoes/detalhe.html","Criar ateste"),("templates/fiscalizacao_contratos/contratos/detalhe.html","Atestes da execução"),("templates/fiscalizacao_contratos/painel.html","Atestados aguardando encaminhamento")):
            self.assertIn(trecho,(RAIZ/"modulos/fiscalizacao_contratos"/arquivo).read_text(encoding="utf-8"))


class CursorAtesteTransacaoFake:
    def __init__(self,falhar_evento=False,possui_ateste=False,falhar_integridade=False): self.falhar_evento=falhar_evento;self.possui_ateste=possui_ateste;self.falhar_integridade=falhar_integridade;self.ultima="";self.rowcount=1
    def __enter__(self): return self
    def __exit__(self,*args): return False
    def execute(self,sql,parametros=None):
        self.ultima=" ".join(str(sql).split())
        if self.falhar_integridade and "INSERT INTO fc_atestes" in self.ultima: raise psycopg2.IntegrityError("conflito simulado")
        if self.falhar_evento and "INSERT INTO fc_ateste_eventos" in self.ultima: raise Exception("falha simulada")
    def fetchone(self):
        if "FROM fc_medicoes WHERE id=" in self.ultima: return {"id":1,"contrato_id":1,"valor_liquido":Decimal("100"),"status":"Aprovada","ativo":True,"atual":True}
        if "FROM fc_servidores" in self.ultima:return {"id":1}
        if "SELECT id FROM fc_atestes" in self.ultima:return None
        if "RETURNING *" in self.ultima:return {"id":10,"medicao_id":1,"valor_atestado":Decimal("100"),"status":"Em elaboração"}
        if "COUNT(*) AS quantidade" in self.ultima:return {"quantidade":0,"total":Decimal("0"),"sem_documento":0}
        if "SELECT EXISTS" in self.ultima:return {"possui_ateste":self.possui_ateste}
        return None


class ConexaoAtesteFake:
    def __init__(self,cursor,falhar_commit=False):self.cursor_obj=cursor;self.commits=0;self.rollbacks=0;self.falhar_commit=falhar_commit
    def cursor(self,**kwargs):return self.cursor_obj
    def commit(self):
        if self.falhar_commit:raise Exception("falha simulada no commit")
        self.commits+=1
    def rollback(self):self.rollbacks+=1
    def close(self):pass


class CursorUploadNotaFake:
    def __init__(self,falha_em=None):self.ultima="";self.rowcount=1;self.falha_em=falha_em
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def execute(self,sql,parametros=None):
        self.ultima=" ".join(str(sql).split())
        if self.falha_em=="documento" and "INSERT INTO fc_documentos" in self.ultima:raise Exception("falha simulada no documento")
        if self.falha_em=="nota" and "INSERT INTO fc_ateste_notas_fiscais" in self.ultima:raise Exception("falha simulada na nota")
        if self.falha_em=="atualizacao" and "UPDATE fc_ateste_notas_fiscais" in self.ultima:raise Exception("falha simulada na atualização")
    def fetchone(self):
        if "SELECT * FROM fc_atestes" in self.ultima:return {"id":1,"medicao_id":1,"numero_ateste":1,"ativo":True,"status":"Em elaboração"}
        if "SELECT contrato_id FROM fc_medicoes" in self.ultima:return {"contrato_id":7}
        if "RETURNING id" in self.ultima:return {"id":99}
        return None


class ArmazenamentoNotaFake:
    def __init__(self,falhar_limpeza=False):self.removidos=[];self.envios=0;self.falhar_limpeza=falhar_limpeza
    def enviar(self,arquivo,contrato_id,aditivo_id=None):self.envios+=1;return {"armazenamento_provedor":"cloudinary","armazenamento_chave":"contratos/7/uuid.pdf","armazenamento_versao":"1"}
    def remover(self,chave):
        self.removidos.append(chave)
        if self.falhar_limpeza:raise CloudinaryStorageError("falha simulada na limpeza")


class TestTransacoesAtestes(unittest.TestCase):
    def test_criacao_evento_mesma_transacao_e_rollback_integral(self):
        for falha,commits,rollbacks in ((False,1,0),(True,0,1)):
            c=ConexaoAtesteFake(CursorAtesteTransacaoFake(falha));s=AtesteService(lambda:c);dados={"medicao_id":1,"numero_ateste":1,"servidor_atestador_id":1,"parecer":None,"observacoes":None}
            if falha:
                with self.assertRaises(Exception):s.criar(dados,7)
            else:self.assertEqual(s.criar(dados,7),10)
            self.assertEqual((c.commits,c.rollbacks),(commits,rollbacks))
    def test_revisao_medicao_consulta_ateste_com_parametro(self):
        texto=(RAIZ/"modulos/fiscalizacao_contratos/services/medicoes_service.py").read_text(encoding="utf-8");self.assertIn("FROM fc_atestes",texto);self.assertIn("AND ativo",texto);self.assertIn("status<>'Cancelado'",texto);self.assertIn("(medicao_id,)",texto);self.assertIn("Esta medição possui um ateste ativo",texto)
    def test_conflito_concorrente_do_indice_retorna_erro_amigavel_e_rollback(self):
        c=ConexaoAtesteFake(CursorAtesteTransacaoFake(falhar_integridade=True));s=AtesteService(lambda:c);dados={"medicao_id":1,"numero_ateste":1,"servidor_atestador_id":1,"parecer":None,"observacoes":None}
        with self.assertRaisesRegex(AtesteDuplicadoError,"Já existe ateste"):s.criar(dados,7)
        self.assertEqual((c.commits,c.rollbacks),(0,1))
    def test_upload_da_nota_faz_commit_ou_remove_arquivo_se_banco_falhar(self):
        dados=normalizar_nota({"numero_nota":"NF-1","serie":"A","data_emissao":"2026-07-22","valor_nota":"10,00","documento_id":""})[0];arquivo={"nome_original":"nota.pdf","mime_type":"application/pdf","extensao":"pdf","tamanho_bytes":10,"sha256":"a"*64,"conteudo":b"%PDF-"}
        cenarios=((None,None,False),("documento",None,False),("nota",None,False),("atualizacao",10,False),(None,None,True))
        for falha_em,nota_id,falhar_commit in cenarios:
            conexao=ConexaoAtesteFake(CursorUploadNotaFake(falha_em),falhar_commit);armazenamento=ArmazenamentoNotaFake();servico=AtesteService(lambda:conexao)
            if falha_em or falhar_commit:
                with self.assertRaises(AtesteServiceError):servico.salvar_nota_com_upload(1,dados,arquivo,7,armazenamento,nota_id)
                self.assertEqual((conexao.commits,conexao.rollbacks,armazenamento.removidos),(0,1,["contratos/7/uuid.pdf"]))
            else:
                self.assertEqual(servico.salvar_nota_com_upload(1,dados,arquivo,7,armazenamento,nota_id),99)
                self.assertEqual((conexao.commits,conexao.rollbacks,armazenamento.removidos),(1,0,[]))
    def test_opcoes_mutuamente_exclusivas_tambem_no_servico(self):
        dados=normalizar_nota({"numero_nota":"NF-1","serie":"A","data_emissao":"2026-07-22","valor_nota":"10,00","documento_id":"1"})[0];armazenamento=ArmazenamentoNotaFake();servico=AtesteService(lambda:(_ for _ in ()).throw(AssertionError("banco não deveria ser aberto")))
        with self.assertRaisesRegex(ReferenciaAtesteInvalidaError,"Escolha entre"):servico.salvar_nota_com_upload(1,dados,{"nome_original":"nota.pdf"},7,armazenamento)
        self.assertEqual(armazenamento.envios,0)
    def test_falha_na_limpeza_nao_encobre_erro_principal(self):
        dados=normalizar_nota({"numero_nota":"NF-1","serie":"A","data_emissao":"2026-07-22","valor_nota":"10,00","documento_id":""})[0];arquivo={"nome_original":"nota.pdf","mime_type":"application/pdf","extensao":"pdf","tamanho_bytes":10,"sha256":"a"*64,"conteudo":b"%PDF-"};conexao=ConexaoAtesteFake(CursorUploadNotaFake("nota"));armazenamento=ArmazenamentoNotaFake(True);servico=AtesteService(lambda:conexao)
        with self.assertLogs("modulos.fiscalizacao_contratos.services.atestes_service",level="WARNING"),self.assertRaisesRegex(AtesteServiceError,"Falha ao enviar"):servico.salvar_nota_com_upload(1,dados,arquivo,7,armazenamento)
        self.assertEqual(conexao.rollbacks,1)


if __name__=="__main__": unittest.main()
