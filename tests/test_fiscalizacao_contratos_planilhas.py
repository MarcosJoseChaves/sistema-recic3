"""Testes da Etapa 2F com serviços falsos e nenhuma conexão real."""

import importlib
import os
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import psycopg2
from jinja2 import Undefined

from modulos.fiscalizacao_contratos.services.planilhas_service import (
    PlanilhaBloqueadaError,
    PlanilhaDuplicadaError,
    PlanilhaService,
    PlanilhaServiceError,
    ReferenciaPlanilhaInvalidaError,
)
from modulos.fiscalizacao_contratos.validacoes_planilhas import (
    calcular_total_item,
    formatar_decimal_brasileiro,
    normalizar_e_validar_item,
    normalizar_e_validar_planilha,
)


CONEXAO_FALSA = MagicMock(name="conexao_falsa_planilhas")
PATCH_CONEXAO = patch("psycopg2.connect", return_value=CONEXAO_FALSA)
MOCK_CONNECT = PATCH_CONEXAO.start()
sys.modules.pop("app", None)
with patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False), patch("dotenv.load_dotenv", return_value=False):
    APP_MODULE = importlib.import_module("app")
MOCK_CONNECT.side_effect = AssertionError("Nenhuma conexão real é permitida nos testes")


class PlanilhaServiceFake:
    def __init__(self):
        self.contratos = {
            1: {"id": 1, "numero_contrato": "CT-001/2026", "processo_administrativo": "PA-10", "empresa_nome": "Empresa Um", "valor_original": Decimal("100000"), "ativo": True},
            2: {"id": 2, "numero_contrato": "CT-002/2026", "processo_administrativo": "PA-20", "empresa_nome": "Empresa Dois", "valor_original": Decimal("50000"), "ativo": True},
        }
        self.aditivos = {1: {"id": 1, "contrato_id": 1, "numero_termo": "TA-01", "tipo_aditivo": "Reajuste", "ativo": True}, 2: {"id": 2, "contrato_id": 2, "numero_termo": "TA-02", "tipo_aditivo": "Prazo", "ativo": True}}
        self.planilhas = {1: self._planilha(1, 1, 1, "Original", "Em elaboração")}
        self.itens = {1: [self._item(1, 1, ativo=True), self._item(2, 1, descricao="Item inativo", ativo=False)]}
        self.proxima_planilha = 2
        self.proximo_item = 3
        self.ultimo_filtro = None
        self.rollback_simulado = False

    def _planilha(self, pid, contrato_id, versao, tipo, status, vigente=False):
        c = self.contratos[contrato_id]
        return {"id": pid, "contrato_id": contrato_id, "aditivo_id": None, "nome": "Planilha Orçamentária Original" if tipo == "Original" else f"Planilha {tipo}", "versao": versao, "tipo_planilha": tipo, "data_referencia": date(2026, 7, 1), "descricao_referencia": "Referência de teste", "status": status, "vigente": vigente, "ativo": True, "criado_em": datetime(2026, 7, 15), "atualizado_em": datetime(2026, 7, 15), **c}

    @staticmethod
    def _item(iid, planilha_id, descricao="Postos de trabalho", ativo=True):
        return {"id": iid, "planilha_id": planilha_id, "ordem": iid, "grupo": "Mão de obra", "codigo_item": f"MO-{iid}", "descricao": descricao, "unidade": "posto", "quantidade": Decimal("6"), "valor_unitario": Decimal("2000"), "fator_multiplicador": Decimal("12"), "observacoes": None, "ativo": ativo}

    def listar_contratos(self):
        return list(self.contratos.values())

    def listar_aditivos(self, contrato_id):
        return [a for a in self.aditivos.values() if a["contrato_id"] == contrato_id]

    def listar(self, busca="", contrato_id=None, tipo_planilha="", status="", vigente="", status_ativo="ativos"):
        self.ultimo_filtro = (busca, contrato_id, tipo_planilha, status, vigente, status_ativo)
        resultado = []
        for p in self.planilhas.values():
            c = self.contratos[p["contrato_id"]]
            texto = " ".join((p["nome"], c["numero_contrato"], c["processo_administrativo"], c["empresa_nome"], p["descricao_referencia"])).casefold()
            if busca and busca.casefold() not in texto: continue
            if contrato_id and p["contrato_id"] != contrato_id: continue
            if tipo_planilha and p["tipo_planilha"] != tipo_planilha: continue
            if status and p["status"] != status: continue
            if vigente and p["vigente"] != (vigente == "sim"): continue
            if status_ativo == "ativos" and not p["ativo"]: continue
            if status_ativo == "inativos" and p["ativo"]: continue
            resumo = PlanilhaService.calcular_resumo(self.itens.get(p["id"], []))
            resultado.append({**p, **resumo})
        return resultado

    def obter(self, planilha_id):
        p = dict(self.planilhas[planilha_id])
        itens = [dict(i) for i in self.itens.get(planilha_id, [])]
        p.update(PlanilhaService.calcular_resumo(itens))
        p["aditivo_numero"] = self.aditivos.get(p.get("aditivo_id"), {}).get("numero_termo")
        return p, itens

    def criar(self, dados, usuario_id):
        if dados["contrato_id"] not in self.contratos:
            raise ReferenciaPlanilhaInvalidaError("O contrato selecionado não existe.")
        if dados.get("aditivo_id") and self.aditivos.get(dados["aditivo_id"], {}).get("contrato_id") != dados["contrato_id"]:
            raise ReferenciaPlanilhaInvalidaError("O aditivo selecionado não pertence ao contrato.")
        if dados["tipo_planilha"] == "Original" and any(p["contrato_id"] == dados["contrato_id"] and p["tipo_planilha"] == "Original" for p in self.planilhas.values()):
            raise PlanilhaDuplicadaError("Já existe uma planilha Original para este contrato.")
        if any(p["contrato_id"] == dados["contrato_id"] and p["versao"] == dados["versao"] for p in self.planilhas.values()):
            raise PlanilhaDuplicadaError("Já existe uma planilha com esta versão para o contrato.")
        pid = self.proxima_planilha; self.proxima_planilha += 1
        self.planilhas[pid] = {**self._planilha(pid, dados["contrato_id"], dados["versao"], dados["tipo_planilha"], "Em elaboração"), **dados}
        self.itens[pid] = []
        return pid

    def atualizar(self, pid, dados, usuario_id):
        if self.planilhas[pid]["status"] != "Em elaboração": raise PlanilhaBloqueadaError("Planilhas consolidadas não podem ser alteradas.")
        self.planilhas[pid].update(dados); self.planilhas[pid]["atualizado_em"] = datetime.now()

    def criar_item(self, pid, dados, usuario_id):
        if self.planilhas[pid]["status"] != "Em elaboração": raise PlanilhaBloqueadaError()
        iid = self.proximo_item; self.proximo_item += 1
        self.itens.setdefault(pid, []).append({"id": iid, "planilha_id": pid, "ativo": True, **dados})
        return iid

    def atualizar_item(self, iid, pid, dados, usuario_id):
        if self.planilhas[pid]["status"] != "Em elaboração": raise PlanilhaBloqueadaError()
        next(i for i in self.itens[pid] if i["id"] == iid).update(dados)

    def alterar_item_ativo(self, pid, iid, usuario_id, ativo):
        if self.planilhas[pid]["status"] != "Em elaboração": raise PlanilhaBloqueadaError()
        next(i for i in self.itens[pid] if i["id"] == iid)["ativo"] = ativo

    def consolidar(self, pid, usuario_id):
        p = self.planilhas[pid]
        if p["status"] != "Em elaboração" or not p["ativo"]: raise PlanilhaBloqueadaError("Planilha não pode ser consolidada.")
        if not any(i["ativo"] for i in self.itens.get(pid, [])): raise PlanilhaBloqueadaError("Inclua pelo menos um item ativo antes de consolidar.")
        p["status"] = "Consolidada"; p["atualizado_em"] = datetime.now()

    def definir_vigente(self, pid, usuario_id):
        p = self.planilhas[pid]
        if p["status"] != "Consolidada" or not p["ativo"]: raise PlanilhaBloqueadaError("Somente uma planilha ativa e consolidada pode ser vigente.")
        for outra in self.planilhas.values():
            if outra["contrato_id"] == p["contrato_id"]: outra["vigente"] = False
        p["vigente"] = True

    def criar_versao(self, origem_id, dados, usuario_id):
        origem = self.planilhas[origem_id]
        if origem["status"] != "Consolidada": raise PlanilhaBloqueadaError("A origem deve ser uma planilha ativa e consolidada.")
        versao = max(p["versao"] for p in self.planilhas.values() if p["contrato_id"] == origem["contrato_id"]) + 1
        pid = self.proxima_planilha; self.proxima_planilha += 1
        self.planilhas[pid] = {**self._planilha(pid, origem["contrato_id"], versao, dados["tipo_planilha"], "Em elaboração"), **dados, "contrato_id": origem["contrato_id"], "versao": versao, "vigente": False}
        self.itens[pid] = []
        try:
            for item in self.itens[origem_id]:
                if item["ativo"]:
                    novo = dict(item); novo["id"] = self.proximo_item; novo["planilha_id"] = pid; self.proximo_item += 1; self.itens[pid].append(novo)
        except Exception:
            self.rollback_simulado = True; self.planilhas.pop(pid, None); self.itens.pop(pid, None); raise
        return pid

    def alterar_planilha_ativo(self, pid, usuario_id, ativo):
        if not ativo and self.planilhas[pid]["vigente"]: raise PlanilhaBloqueadaError("Defina outra planilha vigente antes de inativar esta versão.")
        self.planilhas[pid]["ativo"] = ativo

    def comparar_contrato(self, contrato_id, valor_atualizado=None):
        return PlanilhaService.comparar_contrato(self, contrato_id, valor_atualizado)

    def listar_do_contrato(self, contrato_id):
        return self.listar(contrato_id=contrato_id, status_ativo="todos")


class TestFiscalizacaoContratosPlanilhas(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = APP_MODULE.app; cls.app.config.update(TESTING=True)
        cls.loader_original = APP_MODULE.login_manager._user_callback

    @classmethod
    def tearDownClass(cls):
        APP_MODULE.login_manager._user_callback = cls.loader_original

    def setUp(self):
        self.client = self.app.test_client(); self.servico = PlanilhaServiceFake()
        APP_MODULE.login_manager._user_callback = self._usuario
        self.patcher = patch("modulos.fiscalizacao_contratos.routes.planilhas.PlanilhaService", return_value=self.servico); self.patcher.start()

    def tearDown(self): self.patcher.stop()

    @staticmethod
    def _usuario(uid):
        return {"1": APP_MODULE.User(1, "admin", "admin", None), "2": APP_MODULE.User(2, "comum", "usuario", None)}.get(str(uid))

    def autenticar(self, uid):
        with self.client.session_transaction() as s: s["_user_id"] = str(uid); s["_fresh"] = True

    @staticmethod
    def dados_planilha(contrato="2", versao="1", tipo="Original"):
        return {"contrato_id": contrato, "aditivo_id": "", "nome": "  Planilha de teste  ", "versao": versao, "tipo_planilha": tipo, "data_referencia": "2026-07-15", "descricao_referencia": " Base julho "}

    @staticmethod
    def dados_item():
        return {"ordem": "3", "grupo": " Materiais ", "codigo_item": " MAT-1 ", "descricao": " Item novo ", "unidade": " unidade ", "quantidade": "1.250,500000", "valor_unitario": "R$ 2.345,6789", "fator_multiplicador": "1,5", "observacoes": " teste "}

    def test_administrador_acessa_listagem_e_cartao(self):
        self.autenticar(1); self.assertEqual(self.client.get("/fiscalizacao-contratos/planilhas").status_code, 200); self.assertIn("Planilhas Orçamentárias".encode(), self.client.get("/fiscalizacao-contratos").data)

    def test_visitante_e_usuario_comum_sao_bloqueados(self):
        self.assertIn("/login", self.client.get("/fiscalizacao-contratos/planilhas").headers["Location"])
        self.autenticar(2)
        rotas_get = [
            "/fiscalizacao-contratos/planilhas",
            "/fiscalizacao-contratos/planilhas/nova",
            "/fiscalizacao-contratos/planilhas/1",
            "/fiscalizacao-contratos/planilhas/1/editar",
            "/fiscalizacao-contratos/planilhas/1/itens/novo",
            "/fiscalizacao-contratos/planilhas/1/itens/1/editar",
            "/fiscalizacao-contratos/planilhas/1/nova-versao",
            "/fiscalizacao-contratos/planilhas/contratos/1/comparar",
        ]
        rotas_post = [
            "/fiscalizacao-contratos/planilhas/1/itens/1/inativar",
            "/fiscalizacao-contratos/planilhas/1/itens/1/reativar",
            "/fiscalizacao-contratos/planilhas/1/consolidar",
            "/fiscalizacao-contratos/planilhas/1/definir-vigente",
            "/fiscalizacao-contratos/planilhas/1/inativar",
            "/fiscalizacao-contratos/planilhas/1/reativar",
        ]
        self.assertTrue(all(self.client.get(r).status_code == 403 for r in rotas_get))
        self.assertTrue(all(self.client.post(r).status_code == 403 for r in rotas_post))

    def test_administrador_cria_planilha_original(self):
        self.autenticar(1); r = self.client.post("/fiscalizacao-contratos/planilhas/nova", data=self.dados_planilha()); self.assertEqual(r.status_code, 302); self.assertEqual(self.servico.planilhas[2]["nome"], "Planilha de teste")

    def test_segunda_original_e_versao_duplicada_sao_rejeitadas(self):
        self.autenticar(1); original = self.client.post("/fiscalizacao-contratos/planilhas/nova", data=self.dados_planilha("1", "2")); versao = self.client.post("/fiscalizacao-contratos/planilhas/nova", data=self.dados_planilha("1", "1", "Outra")); self.assertEqual((original.status_code, versao.status_code), (409, 409))

    def test_contrato_inexistente_e_aditivo_de_outro_contrato_sao_rejeitados(self):
        self.autenticar(1); a = self.dados_planilha("999"); b = self.dados_planilha("1", "2", "Aditivada"); b["aditivo_id"] = "2"; self.assertEqual(self.client.post("/fiscalizacao-contratos/planilhas/nova", data=a).status_code, 400); self.assertEqual(self.client.post("/fiscalizacao-contratos/planilhas/nova", data=b).status_code, 400)

    def test_validacao_nome_versao_data_e_tipo(self):
        for campo, valor in (("nome", "   "), ("versao", "0"), ("data_referencia", ""), ("tipo_planilha", "Inválida")):
            dados = self.dados_planilha(); dados[campo] = valor; _, erros = normalizar_e_validar_planilha(dados); self.assertTrue(erros)

    def test_cria_item_e_normaliza_decimais_brasileiros(self):
        dados, erros = normalizar_e_validar_item(self.dados_item()); self.assertFalse(erros); self.assertEqual(dados["quantidade"], Decimal("1250.500000")); self.assertEqual(dados["valor_unitario"], Decimal("2345.6789")); self.assertIsInstance(dados["fator_multiplicador"], Decimal)

    def test_administrador_cadastra_edita_inativa_e_reativa_item_pelas_rotas(self):
        self.autenticar(1)
        cadastro = self.client.post("/fiscalizacao-contratos/planilhas/1/itens/novo", data=self.dados_item())
        self.assertEqual(cadastro.status_code, 302)
        item_id = self.servico.itens[1][-1]["id"]
        dados = self.dados_item(); dados["descricao"] = "Item atualizado"
        edicao = self.client.post(f"/fiscalizacao-contratos/planilhas/1/itens/{item_id}/editar", data=dados)
        self.assertEqual(edicao.status_code, 302)
        self.assertEqual(self.servico.itens[1][-1]["descricao"], "Item atualizado")
        self.client.post(f"/fiscalizacao-contratos/planilhas/1/itens/{item_id}/inativar")
        self.assertFalse(self.servico.itens[1][-1]["ativo"])
        self.client.post(f"/fiscalizacao-contratos/planilhas/1/itens/{item_id}/reativar")
        self.assertTrue(self.servico.itens[1][-1]["ativo"])

    def test_formulario_novo_renderiza_campos_iniciais(self):
        self.autenticar(1)
        resposta = self.client.get("/fiscalizacao-contratos/planilhas/1/itens/novo")
        self.assertEqual(resposta.status_code, 200)
        self.assertIn(b'id="quantidade" name="quantidade" required value=""', resposta.data)
        self.assertIn(b'id="valor_unitario" name="valor_unitario" required value=""', resposta.data)
        self.assertIn(b'id="fator_multiplicador" name="fator_multiplicador" required value="1"', resposta.data)

    def test_formatador_decimal_e_seguro_para_template(self):
        class ValorInesperado:
            def __str__(self):
                return "valor inesperado"

        self.assertEqual(formatar_decimal_brasileiro(None), "")
        self.assertEqual(formatar_decimal_brasileiro(Undefined(name="ausente")), "")
        self.assertEqual(formatar_decimal_brasileiro(""), "")
        self.assertEqual(formatar_decimal_brasileiro(Decimal("0")), "0")
        self.assertEqual(formatar_decimal_brasileiro(Decimal("1.5")), "1,5")
        self.assertEqual(formatar_decimal_brasileiro(Decimal("2345.6789")), "2.345,6789")
        self.assertEqual(formatar_decimal_brasileiro(ValorInesperado()), "")

    def test_edicao_item_renderiza_valores_existentes(self):
        self.autenticar(1)
        resposta = self.client.get("/fiscalizacao-contratos/planilhas/1/itens/1/editar")
        self.assertEqual(resposta.status_code, 200)
        self.assertIn(b'id="quantidade" name="quantidade" required value="6"', resposta.data)
        self.assertIn(b'id="valor_unitario" name="valor_unitario" required value="2.000"', resposta.data)
        self.assertIn(b'id="fator_multiplicador" name="fator_multiplicador" required value="12"', resposta.data)

    def test_descricao_e_unidade_obrigatorias(self):
        for campo in ("descricao", "unidade"):
            dados = self.dados_item(); dados[campo] = " "; _, erros = normalizar_e_validar_item(dados); self.assertTrue(erros)

    def test_valores_negativos_e_fator_zero_sao_rejeitados(self):
        for campo, valor in (("quantidade", "-1"), ("valor_unitario", "-0,01"), ("fator_multiplicador", "0")):
            dados = self.dados_item(); dados[campo] = valor; _, erros = normalizar_e_validar_item(dados); self.assertTrue(erros)

    def test_total_item_usa_decimal(self):
        total = calcular_total_item({"quantidade": Decimal("6"), "valor_unitario": Decimal("2000"), "fator_multiplicador": Decimal("12")}); self.assertEqual(total, Decimal("144000")); self.assertIsInstance(total, Decimal)

    def test_total_geral_e_subtotal_usam_somente_ativos(self):
        itens = [self.servico._item(1, 1), self.servico._item(2, 1, ativo=False), {**self.servico._item(3, 1), "grupo": "Materiais", "quantidade": Decimal("1"), "valor_unitario": Decimal("10"), "fator_multiplicador": Decimal("2")}]
        resumo = PlanilhaService.calcular_resumo(itens); self.assertEqual(resumo["total_geral"], Decimal("144020")); self.assertEqual(resumo["subtotais"]["Mão de obra"], Decimal("144000"))

    def test_planilha_em_elaboracao_pode_ser_editada(self):
        self.autenticar(1); dados = self.dados_planilha("1", "1", "Original"); dados["nome"] = "Novo nome"; self.assertEqual(self.client.post("/fiscalizacao-contratos/planilhas/1/editar", data=dados).status_code, 302)

    def test_planilha_consolidada_e_itens_ficam_bloqueados(self):
        self.servico.planilhas[1]["status"] = "Consolidada"; self.autenticar(1); self.assertEqual(self.client.get("/fiscalizacao-contratos/planilhas/1/editar").status_code, 302); self.assertEqual(self.client.get("/fiscalizacao-contratos/planilhas/1/itens/novo").status_code, 302); self.assertEqual(self.client.get("/fiscalizacao-contratos/planilhas/1/itens/1/editar").status_code, 302)
        with self.assertRaises(PlanilhaBloqueadaError):
            PlanilhaService._exigir_editavel(self.servico.planilhas[1])

    def test_planilha_sem_itens_ativos_nao_consolida(self):
        self.servico.itens[1][0]["ativo"] = False; self.autenticar(1); self.client.post("/fiscalizacao-contratos/planilhas/1/consolidar"); self.assertEqual(self.servico.planilhas[1]["status"], "Em elaboração")

    def test_consolidacao_bloqueia_sem_tornar_vigente(self):
        self.autenticar(1); self.client.post("/fiscalizacao-contratos/planilhas/1/consolidar"); self.assertEqual(self.servico.planilhas[1]["status"], "Consolidada"); self.assertFalse(self.servico.planilhas[1]["vigente"])

    def test_somente_consolidada_pode_ser_vigente(self):
        self.autenticar(1); self.client.post("/fiscalizacao-contratos/planilhas/1/definir-vigente"); self.assertFalse(self.servico.planilhas[1]["vigente"]); self.servico.consolidar(1, 1); self.client.post("/fiscalizacao-contratos/planilhas/1/definir-vigente"); self.assertTrue(self.servico.planilhas[1]["vigente"])

    def test_troca_vigente_mantem_uma_por_contrato(self):
        self.servico.planilhas[1].update(status="Consolidada", vigente=True); self.servico.planilhas[2] = self.servico._planilha(2, 1, 2, "Reajustada", "Consolidada"); self.servico.definir_vigente(2, 1); self.assertFalse(self.servico.planilhas[1]["vigente"]); self.assertTrue(self.servico.planilhas[2]["vigente"])
        conexao = MagicMock()
        cursor = conexao.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = self.servico.planilhas[2]
        cursor.execute.side_effect = [None, None, psycopg2.OperationalError("falha simulada")]
        with self.assertRaises(PlanilhaServiceError):
            PlanilhaService(lambda: conexao).definir_vigente(2, 1)
        conexao.rollback.assert_called_once()
        conexao.commit.assert_not_called()

    def test_nova_versao_copia_apenas_ativos_com_novos_ids(self):
        self.servico.planilhas[1]["status"] = "Consolidada"; dados, _ = normalizar_e_validar_planilha(self.dados_planilha("1", "2", "Reajustada"), permitir_original=False); nova = self.servico.criar_versao(1, dados, 1); self.assertEqual(len(self.servico.itens[nova]), 1); self.assertNotEqual(self.servico.itens[nova][0]["id"], 1); self.assertEqual(self.servico.planilhas[nova]["status"], "Em elaboração"); self.assertFalse(self.servico.planilhas[nova]["vigente"])

    def test_copia_nao_altera_vigente_anterior(self):
        self.servico.planilhas[1].update(status="Consolidada", vigente=True); dados, _ = normalizar_e_validar_planilha(self.dados_planilha("1", "2", "Outra"), permitir_original=False); self.servico.criar_versao(1, dados, 1); self.assertTrue(self.servico.planilhas[1]["vigente"])

    def test_falha_real_na_copia_executa_rollback(self):
        conexao = MagicMock(); cursor = conexao.cursor.return_value.__enter__.return_value
        origem = self.servico._planilha(1, 1, 1, "Original", "Consolidada")
        cursor.fetchone.side_effect = [origem, {"id": 1, "ativo": True}, {"proxima": 2}, {"id": 2}]
        cursor.execute.side_effect = [None, None, None, None, None, psycopg2.OperationalError("falha simulada")]
        dados, _ = normalizar_e_validar_planilha(self.dados_planilha("1", "2", "Outra"), permitir_original=False)
        with self.assertRaises(PlanilhaServiceError): PlanilhaService(lambda: conexao).criar_versao(1, dados, 1)
        conexao.rollback.assert_called_once(); conexao.commit.assert_not_called()

        class ColisaoVersao(psycopg2.IntegrityError):
            @property
            def diag(self):
                return SimpleNamespace(constraint_name="uq_fc_planilhas_contrato_versao")

        conexao_colisao = MagicMock()
        cursor_colisao = conexao_colisao.cursor.return_value.__enter__.return_value
        cursor_colisao.fetchone.side_effect = [
            origem,
            {"id": 1, "ativo": True},
            {"proxima": 2},
        ]
        cursor_colisao.execute.side_effect = [
            None, None, None, None, ColisaoVersao("colisão simulada")
        ]
        with self.assertRaises(PlanilhaDuplicadaError):
            PlanilhaService(lambda: conexao_colisao).criar_versao(1, dados, 1)
        conexao_colisao.rollback.assert_called_once()
        conexao_colisao.commit.assert_not_called()

    def test_planilha_vigente_nao_pode_ser_inativada(self):
        self.servico.planilhas[1].update(status="Consolidada", vigente=True)
        with self.assertRaises(PlanilhaBloqueadaError):
            self.servico.alterar_planilha_ativo(1, 1, False)

    def test_inativacao_preserva_e_reativacao_funciona(self):
        self.servico.alterar_planilha_ativo(1, 1, False); self.assertIn(1, self.servico.planilhas); self.assertFalse(self.servico.planilhas[1]["ativo"]); self.servico.alterar_planilha_ativo(1, 1, True); self.assertTrue(self.servico.planilhas[1]["ativo"])

    def test_item_inativado_e_reativado_sem_exclusao(self):
        self.servico.alterar_item_ativo(1, 1, 1, False); self.assertEqual(len(self.servico.itens[1]), 2); self.servico.alterar_item_ativo(1, 1, 1, True); self.assertTrue(self.servico.itens[1][0]["ativo"])

    def test_comparacao_e_zero_sem_divisao(self):
        self.servico.planilhas[1].update(status="Consolidada", vigente=True); resumo = self.servico.comparar_contrato(1, Decimal("110000")); self.assertEqual(resumo["total_original"], Decimal("144000")); self.servico.itens[1][0]["quantidade"] = Decimal("0"); resumo_zero = self.servico.comparar_contrato(1, Decimal("0")); self.assertIsNone(resumo_zero["percentual_original_vigente"])

    def test_valores_do_contrato_e_aditivos_nao_sao_alterados(self):
        valor = self.servico.contratos[1]["valor_original"]; aditivos = dict(self.servico.aditivos); self.servico.obter(1); self.assertEqual(self.servico.contratos[1]["valor_original"], valor); self.assertEqual(self.servico.aditivos, aditivos)

    def test_pesquisa_e_filtros_funcionam(self):
        self.autenticar(1); r = self.client.get("/fiscalizacao-contratos/planilhas?busca=Empresa&contrato_id=1&tipo_planilha=Original&status=Em+elabora%C3%A7%C3%A3o&vigente=nao&status_ativo=todos"); self.assertEqual(r.status_code, 200); self.assertEqual(self.servico.ultimo_filtro[1], 1)

    def test_rotas_antigas_e_novas_estao_registradas(self):
        rotas = {r.rule for r in self.app.url_map.iter_rules()}; self.assertTrue({"/", "/login", "/fiscalizacao-contratos/empresas", "/fiscalizacao-contratos/contratos", "/fiscalizacao-contratos/planilhas"}.issubset(rotas))

    def test_planilhas_aparecem_no_detalhe_do_contrato(self):
        self.autenticar(1)
        contrato = {**self.servico.contratos[1], "objeto": "Objeto", "situacao": "Vigente", "vence_em_60_dias": False, "data_assinatura": None, "vigencia_inicio": None, "vigencia_fim": None, "observacoes": None, "criado_em": None, "atualizado_em": None}
        with (
            patch("modulos.fiscalizacao_contratos.routes.contratos.ContratoService") as contrato_cls,
            patch("modulos.fiscalizacao_contratos.routes.contratos.AditivoService") as aditivo_cls,
            patch("modulos.fiscalizacao_contratos.routes.contratos.DocumentoService") as documento_cls,
            patch("modulos.fiscalizacao_contratos.routes.contratos.PlanilhaService", return_value=self.servico),
        ):
            contrato_cls.return_value.obter.return_value = (contrato, [])
            aditivo_cls.return_value.resumo_contrato.return_value = (None, [])
            documento_cls.return_value.listar_do_contrato.return_value = []
            resposta = self.client.get("/fiscalizacao-contratos/contratos/1")
        self.assertEqual(resposta.status_code, 200)
        self.assertIn("Planilha Orçamentária Original".encode(), resposta.data)

    def test_migracao_aditiva_idempotente_e_nao_automatica(self):
        caminho = os.path.join(os.path.dirname(os.path.dirname(__file__)), "modulos", "fiscalizacao_contratos", "migrations", "006_criar_fc_planilhas_orcamentarias.sql")
        with open(caminho, encoding="utf-8") as f: sql = f.read().upper()
        self.assertIn("CREATE TABLE IF NOT EXISTS FC_PLANILHAS_ORCAMENTARIAS", sql); self.assertIn("CREATE TABLE IF NOT EXISTS FC_PLANILHA_ITENS", sql)
        for comando in ("DROP", "TRUNCATE", "DELETE", "UPDATE", "INSERT", "ALTER TABLE"): self.assertNotIn(comando, sql)

    def test_servico_nao_usa_delete_float_nem_altera_contrato_aditivo(self):
        caminho = os.path.join(os.path.dirname(os.path.dirname(__file__)), "modulos", "fiscalizacao_contratos", "services", "planilhas_service.py")
        with open(caminho, encoding="utf-8") as f: codigo = f.read().upper()
        self.assertNotIn("DELETE FROM", codigo); self.assertNotIn("FLOAT(", codigo); self.assertNotIn("UPDATE FC_CONTRATOS", codigo); self.assertNotIn("UPDATE FC_ADITIVOS", codigo)

    def test_nenhum_banco_real_e_acessado(self):
        chamadas = MOCK_CONNECT.call_count; self.autenticar(1); self.client.get("/fiscalizacao-contratos/planilhas"); self.assertEqual(MOCK_CONNECT.call_count, chamadas)


def tearDownModule(): PATCH_CONEXAO.stop()


if __name__ == "__main__": unittest.main()
