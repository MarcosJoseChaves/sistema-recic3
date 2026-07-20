"""Testes da Etapa 2C sem conexão real e sem SQL real."""

import importlib
import os
import sys
import unittest
from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import MagicMock, patch

from modulos.fiscalizacao_contratos.services.contratos_service import (
    ContratoDuplicadoError,
    ContratoService,
    ContratoServiceError,
    ReferenciaContratoInvalidaError,
)
from modulos.fiscalizacao_contratos.validacoes_contratos import (
    converter_valor_brasileiro,
    formatar_data_brasileira,
    formatar_moeda_brasileira,
)


CONEXAO_FALSA_IMPORTACAO = MagicMock(name="conexao_falsa_contratos")
CURSOR_FALSO_IMPORTACAO = CONEXAO_FALSA_IMPORTACAO.cursor.return_value
CURSOR_FALSO_IMPORTACAO.fetchall.return_value = []
CURSOR_FALSO_IMPORTACAO.fetchone.return_value = None

PATCH_CONEXAO = patch("psycopg2.connect", return_value=CONEXAO_FALSA_IMPORTACAO)
MOCK_CONNECT = PATCH_CONEXAO.start()

sys.modules.pop("app", None)
with (
    patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False),
    patch("dotenv.load_dotenv", return_value=False),
):
    APP_MODULE = importlib.import_module("app")

MOCK_CONNECT.side_effect = AssertionError("Nenhuma conexão real é permitida nos testes")


class ContratoServiceFake:
    def __init__(self):
        hoje = date.today()
        agora = datetime(2026, 7, 14, 9, 0)
        self.empresas = {
            1: {"id": 1, "razao_social": "Empresa Ativa Ltda", "ativo": True},
            2: {"id": 2, "razao_social": "Empresa Inativa Ltda", "ativo": False},
        }
        self.servidores = {
            1: {"id": 1, "nome": "Gestor Ativo", "matricula": "G-001", "cargo": "Gestor", "ativo": True},
            2: {"id": 2, "nome": "Fiscal Ativo", "matricula": "F-001", "cargo": "Fiscal", "ativo": True},
            3: {"id": 3, "nome": "Fiscal Substituto", "matricula": "F-002", "cargo": "Fiscal", "ativo": True},
            4: {"id": 4, "nome": "Servidor Inativo", "matricula": "I-001", "cargo": "Fiscal", "ativo": False},
        }
        self.contratos = {
            1: {
                "id": 1,
                "numero_contrato": "CT-001/2026",
                "processo_administrativo": "PA-100/2026",
                "objeto": "Serviços de manutenção predial",
                "empresa_id": 1,
                "empresa_nome": "Empresa Ativa Ltda",
                "empresa_ativa": True,
                "valor_original": Decimal("125450.75"),
                "data_assinatura": hoje - timedelta(days=30),
                "vigencia_inicio": hoje - timedelta(days=20),
                "vigencia_fim": hoje + timedelta(days=30),
                "situacao": "Vigente",
                "observacoes": None,
                "ativo": True,
                "criado_em": agora,
                "atualizado_em": agora,
                "criado_por_usuario_id": 1,
                "atualizado_por_usuario_id": 1,
                "vence_em_60_dias": True,
            }
        }
        self.responsaveis = {
            1: [
                self._vinculo(1, 1, 1, "Gestor", True, hoje - timedelta(days=20)),
                self._vinculo(2, 1, 2, "Fiscal titular", True, hoje - timedelta(days=20)),
            ]
        }
        self.proximo_contrato_id = 2
        self.proximo_vinculo_id = 3
        self.criar_chamadas = 0
        self.ultimos_filtros = None

    def _vinculo(self, vinculo_id, contrato_id, servidor_id, tipo, titular, inicio):
        servidor = self.servidores[servidor_id]
        return {
            "id": vinculo_id,
            "contrato_id": contrato_id,
            "servidor_id": servidor_id,
            "tipo_responsabilidade": tipo,
            "titular": titular,
            "data_inicio": inicio,
            "data_fim": None,
            "ativo": True,
            "criado_em": datetime(2026, 7, 14, 9, 0),
            "atualizado_em": datetime(2026, 7, 14, 9, 0),
            "servidor_nome": servidor["nome"],
            "servidor_matricula": servidor["matricula"],
            "servidor_cargo": servidor["cargo"],
        }

    def listar(self, **filtros):
        self.ultimos_filtros = filtros
        busca = filtros.get("busca", "").casefold()
        resultado = []
        for contrato in self.contratos.values():
            campos = (
                contrato["numero_contrato"],
                contrato["processo_administrativo"] or "",
                contrato["objeto"],
                contrato["empresa_nome"],
            )
            if busca and not any(busca in campo.casefold() for campo in campos):
                continue
            if filtros.get("situacao") and contrato["situacao"] != filtros["situacao"]:
                continue
            if filtros.get("empresa_id") and contrato["empresa_id"] != filtros["empresa_id"]:
                continue
            status = filtros.get("status_ativo", "ativos")
            if status == "ativos" and not contrato["ativo"]:
                continue
            if status == "inativos" and contrato["ativo"]:
                continue
            if filtros.get("proximos_vencimento") and not contrato["vence_em_60_dias"]:
                continue
            resultado.append(contrato)
        return resultado

    def listar_empresas_filtro(self):
        return list(self.empresas.values())

    def opcoes_formulario(self):
        empresas = [item for item in self.empresas.values() if item["ativo"]]
        servidores = [item for item in self.servidores.values() if item["ativo"]]
        return empresas, servidores

    def obter(self, contrato_id):
        return self.contratos[contrato_id], self.responsaveis.get(contrato_id, [])

    def _validar_referencias(self, dados, responsaveis):
        empresa = self.empresas.get(dados["empresa_id"])
        if not empresa or not empresa["ativo"]:
            raise ReferenciaContratoInvalidaError(
                "A empresa selecionada não existe ou está inativa."
            )
        ids = {
            responsaveis["gestor_id"],
            responsaveis["fiscal_titular_id"],
            *responsaveis["fiscais_substitutos"],
        }
        if any(not self.servidores.get(item) or not self.servidores[item]["ativo"] for item in ids):
            raise ReferenciaContratoInvalidaError(
                "Um dos responsáveis selecionados não existe ou está inativo."
            )

    @staticmethod
    def _desejados(responsaveis):
        resultado = [
            (responsaveis["gestor_id"], "Gestor", True),
            (responsaveis["fiscal_titular_id"], "Fiscal titular", True),
        ]
        resultado.extend(
            (item, "Fiscal substituto", False)
            for item in responsaveis["fiscais_substitutos"]
        )
        return resultado

    def criar(self, dados, responsaveis, usuario_id):
        self.criar_chamadas += 1
        if any(item["numero_contrato"] == dados["numero_contrato"] for item in self.contratos.values()):
            raise ContratoDuplicadoError(
                "Já existe um contrato cadastrado com este número."
            )
        self._validar_referencias(dados, responsaveis)
        contrato_id = self.proximo_contrato_id
        self.proximo_contrato_id += 1
        agora = datetime(2026, 7, 14, 10, 0)
        empresa = self.empresas[dados["empresa_id"]]
        self.contratos[contrato_id] = {
            "id": contrato_id,
            **dados,
            "empresa_nome": empresa["razao_social"],
            "empresa_ativa": empresa["ativo"],
            "ativo": True,
            "criado_em": agora,
            "atualizado_em": agora,
            "criado_por_usuario_id": usuario_id,
            "atualizado_por_usuario_id": usuario_id,
            "vence_em_60_dias": False,
        }
        self.responsaveis[contrato_id] = []
        for servidor_id, tipo, titular in self._desejados(responsaveis):
            self.responsaveis[contrato_id].append(
                self._vinculo(
                    self.proximo_vinculo_id,
                    contrato_id,
                    servidor_id,
                    tipo,
                    titular,
                    date.today(),
                )
            )
            self.proximo_vinculo_id += 1
        return contrato_id

    def atualizar(self, contrato_id, dados, responsaveis, usuario_id):
        self._validar_referencias(dados, responsaveis)
        for item in self.contratos.values():
            if item["id"] != contrato_id and item["numero_contrato"] == dados["numero_contrato"]:
                raise ContratoDuplicadoError(
                    "Já existe um contrato cadastrado com este número."
                )
        contrato = self.contratos[contrato_id]
        contrato.update(dados)
        contrato["empresa_nome"] = self.empresas[dados["empresa_id"]]["razao_social"]
        contrato["atualizado_em"] = datetime(2026, 7, 14, 11, 0)
        contrato["atualizado_por_usuario_id"] = usuario_id

        atuais = {
            (item["servidor_id"], item["tipo_responsabilidade"]): item
            for item in self.responsaveis[contrato_id]
            if item["ativo"]
        }
        desejados = {
            (servidor_id, tipo): titular
            for servidor_id, tipo, titular in self._desejados(responsaveis)
        }
        for chave, vinculo in atuais.items():
            if chave not in desejados:
                vinculo["ativo"] = False
                vinculo["data_fim"] = date.today()
                vinculo["atualizado_em"] = datetime(2026, 7, 14, 11, 0)
        for (servidor_id, tipo), titular in desejados.items():
            if (servidor_id, tipo) not in atuais:
                self.responsaveis[contrato_id].append(
                    self._vinculo(
                        self.proximo_vinculo_id,
                        contrato_id,
                        servidor_id,
                        tipo,
                        titular,
                        date.today(),
                    )
                )
                self.proximo_vinculo_id += 1

    def inativar(self, contrato_id, usuario_id):
        self.contratos[contrato_id]["ativo"] = False
        self.contratos[contrato_id]["atualizado_em"] = datetime(2026, 7, 14, 12, 0)
        self.contratos[contrato_id]["atualizado_por_usuario_id"] = usuario_id

    def reativar(self, contrato_id, usuario_id):
        self.contratos[contrato_id]["ativo"] = True
        self.contratos[contrato_id]["atualizado_em"] = datetime(2026, 7, 14, 13, 0)
        self.contratos[contrato_id]["atualizado_por_usuario_id"] = usuario_id


class TestFiscalizacaoContratosContratos(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.flask_app = APP_MODULE.app
        cls.flask_app.config.update(TESTING=True)
        cls.user_loader_original = APP_MODULE.login_manager._user_callback

    @classmethod
    def tearDownClass(cls):
        APP_MODULE.login_manager._user_callback = cls.user_loader_original

    def setUp(self):
        self.client = self.flask_app.test_client()
        self.servico = ContratoServiceFake()
        APP_MODULE.login_manager._user_callback = self._carregar_usuario_falso
        self.patcher_servico = patch(
            "modulos.fiscalizacao_contratos.routes.contratos.ContratoService",
            return_value=self.servico,
        )
        self.patcher_servico.start()
        self.patcher_aditivos = patch(
            "modulos.fiscalizacao_contratos.routes.contratos.AditivoService"
        )
        aditivo_service = self.patcher_aditivos.start().return_value
        contrato = self.servico.contratos[1]
        aditivo_service.resumo_contrato.return_value = (
            {
                "valor_original": contrato["valor_original"],
                "total_acrescimos": Decimal("0.00"),
                "total_supressoes": Decimal("0.00"),
                "valor_atualizado": contrato["valor_original"],
                "vigencia_inicio_original": contrato["vigencia_inicio"],
                "vigencia_fim_original": contrato["vigencia_fim"],
                "vigencia_fim_atual": contrato["vigencia_fim"],
                "quantidade_aditivos_ativos": 0,
            },
            [],
        )
        self.patcher_documentos = patch(
            "modulos.fiscalizacao_contratos.routes.contratos.DocumentoService"
        )
        self.patcher_documentos.start().return_value.listar_do_contrato.return_value = []
        self.patcher_planilhas = patch(
            "modulos.fiscalizacao_contratos.routes.contratos.PlanilhaService"
        )
        self.patcher_planilhas.start().return_value.comparar_contrato.return_value = {
            "planilhas": [], "original": None, "vigente": None,
            "total_original": None, "total_vigente": None,
            "diferenca_original_vigente": None,
            "percentual_original_vigente": None,
            "diferenca_contrato_original": None,
            "diferenca_atualizado_vigente": None,
        }
        self.patcher_ativos = patch(
            "modulos.fiscalizacao_contratos.routes.contratos.AtivoService"
        )
        self.patcher_ativos.start().return_value.listar_do_contrato.return_value = []
        self.patcher_fiscalizacoes = patch("modulos.fiscalizacao_contratos.routes.contratos.FiscalizacaoService")
        self.patcher_fiscalizacoes.start().return_value.listar_do_contrato.return_value = []
        self.patcher_ocorrencias = patch("modulos.fiscalizacao_contratos.routes.contratos.OcorrenciaService")
        self.patcher_ocorrencias.start().return_value.listar_do_contrato.return_value = []

    def tearDown(self):
        self.patcher_ocorrencias.stop()
        self.patcher_fiscalizacoes.stop()
        self.patcher_ativos.stop()
        self.patcher_planilhas.stop()
        self.patcher_documentos.stop()
        self.patcher_aditivos.stop()
        self.patcher_servico.stop()

    @staticmethod
    def _carregar_usuario_falso(user_id):
        perfis = {
            "2": APP_MODULE.User(2, "usuario_comum", "usuario", "UVR 01"),
            "1": APP_MODULE.User(1, "administrador", "admin", None),
        }
        return perfis.get(str(user_id))

    def autenticar_como(self, user_id):
        with self.client.session_transaction() as sessao:
            sessao["_user_id"] = str(user_id)
            sessao["_fresh"] = True

    @staticmethod
    def dados_validos(numero="CT-002/2026", substitutos=(3,)):
        dados = {
            "numero_contrato": numero,
            "processo_administrativo": "PA-200/2026",
            "objeto": "Serviços continuados de apoio administrativo",
            "empresa_id": "1",
            "valor_original": "R$ 125.450,75",
            "data_assinatura": "2026-07-01",
            "vigencia_inicio": "2026-07-01",
            "vigencia_fim": "2027-06-30",
            "situacao": "Vigente",
            "gestor_id": "1",
            "fiscal_titular_id": "2",
            "observacoes": "Contrato de teste.",
        }
        if substitutos:
            dados["fiscais_substitutos"] = [str(item) for item in substitutos]
        return dados

    def test_administrador_acessa_listagem(self):
        self.autenticar_como(1)
        resposta = self.client.get("/fiscalizacao-contratos/contratos")
        self.assertEqual(resposta.status_code, 200)
        self.assertIn(b"CT-001/2026", resposta.data)

    def test_administrador_cadastra_contrato(self):
        self.autenticar_como(1)
        resposta = self.client.post(
            "/fiscalizacao-contratos/contratos/novo", data=self.dados_validos()
        )
        self.assertEqual(resposta.status_code, 302)
        self.assertIn(2, self.servico.contratos)
        self.assertEqual(self.servico.contratos[2]["valor_original"], Decimal("125450.75"))

    def test_administrador_visualiza_e_edita_contrato(self):
        self.autenticar_como(1)
        detalhe = self.client.get("/fiscalizacao-contratos/contratos/1")
        formulario = self.client.get("/fiscalizacao-contratos/contratos/1/editar")
        dados = self.dados_validos(numero="CT-001/2026", substitutos=())
        dados["objeto"] = "Objeto atualizado"
        edicao = self.client.post(
            "/fiscalizacao-contratos/contratos/1/editar", data=dados
        )
        self.assertEqual(detalhe.status_code, 200)
        self.assertEqual(formulario.status_code, 200)
        self.assertEqual(edicao.status_code, 302)
        self.assertEqual(self.servico.contratos[1]["objeto"], "Objeto atualizado")

    def test_numero_duplicado_e_rejeitado(self):
        self.autenticar_como(1)
        resposta = self.client.post(
            "/fiscalizacao-contratos/contratos/novo",
            data=self.dados_validos(numero="CT-001/2026"),
        )
        self.assertEqual(resposta.status_code, 409)
        self.assertIn("Já existe um contrato".encode("utf-8"), resposta.data)

    def test_objeto_vazio_e_rejeitado(self):
        self.autenticar_como(1)
        dados = self.dados_validos()
        dados["objeto"] = "   "
        resposta = self.client.post("/fiscalizacao-contratos/contratos/novo", data=dados)
        self.assertEqual(resposta.status_code, 400)
        self.assertEqual(self.servico.criar_chamadas, 0)
        self.assertIn("objeto do contrato é obrigatório".encode("utf-8"), resposta.data)

    def test_empresa_inexistente_ou_inativa_e_rejeitada(self):
        self.autenticar_como(1)
        for empresa_id in ("2", "999"):
            with self.subTest(empresa_id=empresa_id):
                dados = self.dados_validos()
                dados["empresa_id"] = empresa_id
                resposta = self.client.post(
                    "/fiscalizacao-contratos/contratos/novo", data=dados
                )
                self.assertEqual(resposta.status_code, 400)
                self.assertIn("não existe ou está inativa".encode("utf-8"), resposta.data)

    def test_valor_negativo_e_rejeitado(self):
        self.autenticar_como(1)
        dados = self.dados_validos()
        dados["valor_original"] = "R$ -1,00"
        resposta = self.client.post("/fiscalizacao-contratos/contratos/novo", data=dados)
        self.assertEqual(resposta.status_code, 400)
        self.assertIn("não pode ser negativo".encode("utf-8"), resposta.data)

    def test_vigencia_final_anterior_ao_inicio_e_rejeitada(self):
        self.autenticar_como(1)
        dados = self.dados_validos()
        dados["vigencia_fim"] = "2026-06-30"
        resposta = self.client.post("/fiscalizacao-contratos/contratos/novo", data=dados)
        self.assertEqual(resposta.status_code, 400)
        self.assertIn("não pode ser anterior".encode("utf-8"), resposta.data)

    def test_gestor_obrigatorio(self):
        self.autenticar_como(1)
        dados = self.dados_validos()
        dados["gestor_id"] = ""
        resposta = self.client.post("/fiscalizacao-contratos/contratos/novo", data=dados)
        self.assertEqual(resposta.status_code, 400)
        self.assertIn("gestor do contrato".encode("utf-8"), resposta.data)

    def test_fiscal_titular_obrigatorio(self):
        self.autenticar_como(1)
        dados = self.dados_validos()
        dados["fiscal_titular_id"] = ""
        resposta = self.client.post("/fiscalizacao-contratos/contratos/novo", data=dados)
        self.assertEqual(resposta.status_code, 400)
        self.assertIn("fiscal titular".encode("utf-8"), resposta.data)

    def test_servidor_inexistente_ou_inativo_e_rejeitado(self):
        self.autenticar_como(1)
        for gestor_id in ("4", "999"):
            with self.subTest(gestor_id=gestor_id):
                dados = self.dados_validos()
                dados["gestor_id"] = gestor_id
                resposta = self.client.post(
                    "/fiscalizacao-contratos/contratos/novo", data=dados
                )
                self.assertEqual(resposta.status_code, 400)
                self.assertIn("não existe ou está inativo".encode("utf-8"), resposta.data)

    def test_fiscal_substituto_opcional_funciona(self):
        self.autenticar_como(1)
        resposta = self.client.post(
            "/fiscalizacao-contratos/contratos/novo",
            data=self.dados_validos(substitutos=()),
        )
        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(len(self.servico.responsaveis[2]), 2)

    def test_responsabilidades_sao_gravadas_corretamente(self):
        self.autenticar_como(1)
        resposta = self.client.post(
            "/fiscalizacao-contratos/contratos/novo", data=self.dados_validos()
        )
        tipos = [item["tipo_responsabilidade"] for item in self.servico.responsaveis[2]]
        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(tipos, ["Gestor", "Fiscal titular", "Fiscal substituto"])

    def test_substituicao_preserva_historico(self):
        self.autenticar_como(1)
        dados = self.dados_validos(numero="CT-001/2026", substitutos=())
        dados["fiscal_titular_id"] = "3"
        resposta = self.client.post(
            "/fiscalizacao-contratos/contratos/1/editar", data=dados
        )
        vinculos = self.servico.responsaveis[1]
        anterior = next(item for item in vinculos if item["servidor_id"] == 2)
        novo = next(
            item
            for item in vinculos
            if item["servidor_id"] == 3
            and item["tipo_responsabilidade"] == "Fiscal titular"
        )
        self.assertEqual(resposta.status_code, 302)
        self.assertFalse(anterior["ativo"])
        self.assertIsNotNone(anterior["data_fim"])
        self.assertTrue(novo["ativo"])

    def test_inativacao_nao_apaga_contrato(self):
        self.autenticar_como(1)
        resposta = self.client.post("/fiscalizacao-contratos/contratos/1/inativar")
        self.assertEqual(resposta.status_code, 302)
        self.assertIn(1, self.servico.contratos)
        self.assertFalse(self.servico.contratos[1]["ativo"])

    def test_reativacao_funciona(self):
        self.autenticar_como(1)
        self.servico.contratos[1]["ativo"] = False
        resposta = self.client.post("/fiscalizacao-contratos/contratos/1/reativar")
        self.assertEqual(resposta.status_code, 302)
        self.assertTrue(self.servico.contratos[1]["ativo"])

    def test_pesquisa_e_filtros_funcionam(self):
        self.autenticar_como(1)
        resposta = self.client.get(
            "/fiscalizacao-contratos/contratos?busca=CT-001&situacao=Vigente"
            "&empresa_id=1&status_ativo=ativos&proximos_vencimento=1"
        )
        self.assertEqual(resposta.status_code, 200)
        self.assertIn(b"CT-001/2026", resposta.data)
        self.assertEqual(self.servico.ultimos_filtros["situacao"], "Vigente")
        self.assertEqual(self.servico.ultimos_filtros["empresa_id"], 1)
        self.assertTrue(self.servico.ultimos_filtros["proximos_vencimento"])

    def test_alerta_de_vencimento_em_60_dias(self):
        self.autenticar_como(1)
        lista = self.client.get("/fiscalizacao-contratos/contratos")
        detalhe = self.client.get("/fiscalizacao-contratos/contratos/1")
        self.assertIn("Vence em até 60 dias".encode("utf-8"), lista.data)
        self.assertIn("vence nos próximos 60 dias".encode("utf-8"), detalhe.data)

    def test_multiplas_funcoes_exigem_confirmacao_explicita(self):
        self.autenticar_como(1)
        dados = self.dados_validos(substitutos=())
        dados["fiscal_titular_id"] = "1"
        sem_confirmacao = self.client.post(
            "/fiscalizacao-contratos/contratos/novo", data=dados
        )
        dados["permitir_multiplas_funcoes"] = "1"
        com_confirmacao = self.client.post(
            "/fiscalizacao-contratos/contratos/novo", data=dados
        )
        self.assertEqual(sem_confirmacao.status_code, 400)
        self.assertEqual(com_confirmacao.status_code, 302)

    def test_visitante_e_enviado_ao_login(self):
        resposta = self.client.get("/fiscalizacao-contratos/contratos")
        self.assertEqual(resposta.status_code, 302)
        self.assertIn("/login", resposta.headers["Location"])

    def test_usuario_comum_recebe_403_em_todas_as_operacoes(self):
        self.autenticar_como(2)
        respostas = (
            self.client.get("/fiscalizacao-contratos/contratos"),
            self.client.get("/fiscalizacao-contratos/contratos/novo"),
            self.client.get("/fiscalizacao-contratos/contratos/1"),
            self.client.get("/fiscalizacao-contratos/contratos/1/editar"),
            self.client.post("/fiscalizacao-contratos/contratos/1/inativar"),
            self.client.post("/fiscalizacao-contratos/contratos/1/reativar"),
        )
        self.assertTrue(all(resposta.status_code == 403 for resposta in respostas))

    def test_rotas_antigas_permanecem_funcionando(self):
        rotas = {regra.rule for regra in self.flask_app.url_map.iter_rules()}
        for rota in (
            "/",
            "/login",
            "/logout",
            "/fiscalizacao-contratos/empresas",
            "/fiscalizacao-contratos/servidores",
            "/fiscalizacao-contratos/contratos",
        ):
            self.assertIn(rota, rotas)

    def test_nenhum_banco_real_e_acessado(self):
        chamadas_antes = MOCK_CONNECT.call_count
        self.autenticar_como(1)
        resposta = self.client.get("/fiscalizacao-contratos/contratos")
        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(MOCK_CONNECT.call_count, chamadas_antes)

    def test_valor_e_data_sao_formatados_no_padrao_brasileiro(self):
        self.assertEqual(converter_valor_brasileiro("R$ 125.450,75"), Decimal("125450.75"))
        self.assertEqual(formatar_moeda_brasileira(Decimal("125450.75")), "R$ 125.450,75")
        self.assertEqual(formatar_data_brasileira(date(2026, 7, 14)), "14/07/2026")

    def test_servico_preserva_historico_sem_delete(self):
        conexao = MagicMock()
        cursor = conexao.cursor.return_value.__enter__.return_value
        cursor.rowcount = 1
        cursor.fetchone.return_value = (True,)
        cursor.fetchall.side_effect = [
            [(1, True), (2, True), (3, True)],
            [(10, 1, "Gestor"), (11, 2, "Fiscal titular")],
        ]
        servico = ContratoService(lambda: conexao)
        dados = {
            "numero_contrato": "CT-001/2026",
            "processo_administrativo": None,
            "objeto": "Objeto",
            "empresa_id": 1,
            "valor_original": Decimal("10.00"),
            "data_assinatura": None,
            "vigencia_inicio": None,
            "vigencia_fim": None,
            "situacao": "Vigente",
            "observacoes": None,
        }
        responsaveis = {
            "gestor_id": 1,
            "fiscal_titular_id": 3,
            "fiscais_substitutos": [],
            "permitir_multiplas_funcoes": False,
        }

        servico.atualizar(1, dados, responsaveis, 1)

        comandos = [chamada.args[0].upper() for chamada in cursor.execute.call_args_list]
        sql = " ".join(comandos)
        self.assertIn("ATIVO = FALSE", sql)
        self.assertIn("DATA_FIM = CURRENT_DATE", sql)
        self.assertIn("INSERT INTO FC_CONTRATO_RESPONSAVEIS", sql)
        self.assertIn("ATUALIZADO_EM = CURRENT_TIMESTAMP", sql)
        self.assertNotIn("DELETE", sql)
        conexao.commit.assert_called_once()

        conexao_com_erro = MagicMock()
        cursor_com_erro = conexao_com_erro.cursor.return_value.__enter__.return_value
        cursor_com_erro.execute.side_effect = RuntimeError("falha inesperada simulada")
        servico_com_erro = ContratoService(lambda: conexao_com_erro)

        with self.assertRaises(ContratoServiceError):
            servico_com_erro.atualizar(1, dados, responsaveis, 1)

        conexao_com_erro.rollback.assert_called_once()
        conexao_com_erro.commit.assert_not_called()

    def test_migracao_e_idempotente_aditiva_e_nao_automatica(self):
        caminho = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "modulos",
            "fiscalizacao_contratos",
            "migrations",
            "003_criar_fc_contratos.sql",
        )
        with open(caminho, encoding="utf-8") as arquivo:
            sql = arquivo.read().upper()
        self.assertIn("CREATE TABLE IF NOT EXISTS FC_CONTRATOS", sql)
        self.assertIn("CREATE TABLE IF NOT EXISTS FC_CONTRATO_RESPONSAVEIS", sql)
        self.assertIn("CREATE INDEX IF NOT EXISTS", sql)
        for comando in ("DROP", "TRUNCATE", "DELETE", "UPDATE", "INSERT", "ALTER TABLE"):
            self.assertNotIn(comando, sql)


def tearDownModule():
    PATCH_CONEXAO.stop()


if __name__ == "__main__":
    unittest.main()
