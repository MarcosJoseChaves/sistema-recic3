"""Testes da Etapa 2A sem conexão real e sem SQL real."""

import importlib
import os
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from modulos.fiscalizacao_contratos.services.consultas_externas import ConsultaExternaError
from modulos.fiscalizacao_contratos.services.empresas_service import (
    EmpresaDuplicadaError,
    EmpresaService,
)
from modulos.fiscalizacao_contratos.validacoes import normalizar_e_validar_empresa


CONEXAO_FALSA_IMPORTACAO = MagicMock(name="conexao_falsa_somente_importacao")
CURSOR_FALSO_IMPORTACAO = CONEXAO_FALSA_IMPORTACAO.cursor.return_value
CURSOR_FALSO_IMPORTACAO.fetchall.return_value = []
CURSOR_FALSO_IMPORTACAO.fetchone.return_value = None

PATCH_CONEXAO = patch("psycopg2.connect", return_value=CONEXAO_FALSA_IMPORTACAO)
MOCK_CONNECT = PATCH_CONEXAO.start()

sys.modules.pop("app", None)
with (
    patch.dict(os.environ, {"APP_ENV": "testing", "SECRET_KEY": "teste-ficticio", "DATABASE_URL": ""}, clear=True),
    patch("dotenv.load_dotenv", return_value=False),
):
    APP_MODULE = importlib.import_module("app")

# Depois da importação segura, qualquer nova tentativa de conexão falha o teste.
MOCK_CONNECT.side_effect = AssertionError("Nenhuma conexão real é permitida nos testes")


class EmpresaServiceFake:
    def __init__(self):
        agora = datetime(2026, 7, 14, 9, 0)
        self.empresas = {
            1: {
                "id": 1,
                "cnpj": "11222333000181",
                "razao_social": "Empresa Inicial Ltda",
                "nome_fantasia": "Empresa Inicial",
                "cep": "01001000",
                "logradouro": "Praça da Sé",
                "numero": "100",
                "bairro": "Sé",
                "cidade": "São Paulo",
                "uf": "SP",
                "telefone": "(11) 3333-4444",
                "email": "contato@empresa.test",
                "ativo": True,
                "criado_em": agora,
                "atualizado_em": agora,
                "criado_por_usuario_id": 1,
                "atualizado_por_usuario_id": 1,
            }
        }
        self.proximo_id = 2
        self.criar_chamadas = 0

    def listar(self, incluir_inativas=False):
        return [
            empresa
            for empresa in self.empresas.values()
            if incluir_inativas or empresa["ativo"]
        ]

    def obter(self, empresa_id):
        return self.empresas[empresa_id]

    def criar(self, dados, usuario_id):
        self.criar_chamadas += 1
        if any(empresa["cnpj"] == dados["cnpj"] for empresa in self.empresas.values()):
            raise EmpresaDuplicadaError("Já existe uma empresa cadastrada com este CNPJ.")
        empresa_id = self.proximo_id
        self.proximo_id += 1
        agora = datetime(2026, 7, 14, 10, 0)
        self.empresas[empresa_id] = {
            "id": empresa_id,
            **dados,
            "ativo": True,
            "criado_em": agora,
            "atualizado_em": agora,
            "criado_por_usuario_id": usuario_id,
            "atualizado_por_usuario_id": usuario_id,
        }
        return empresa_id

    def atualizar(self, empresa_id, dados, usuario_id):
        self.empresas[empresa_id].update(dados)
        self.empresas[empresa_id]["atualizado_em"] = datetime(2026, 7, 14, 11, 0)
        self.empresas[empresa_id]["atualizado_por_usuario_id"] = usuario_id

    def inativar(self, empresa_id, usuario_id):
        self.empresas[empresa_id]["ativo"] = False
        self.empresas[empresa_id]["atualizado_por_usuario_id"] = usuario_id

    def reativar(self, empresa_id, usuario_id):
        self.empresas[empresa_id]["ativo"] = True
        self.empresas[empresa_id]["atualizado_por_usuario_id"] = usuario_id


class TestFiscalizacaoContratosEmpresas(unittest.TestCase):
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
        self.servico = EmpresaServiceFake()
        APP_MODULE.login_manager._user_callback = self._carregar_usuario_falso
        self.patcher_servico = patch(
            "modulos.fiscalizacao_contratos.routes.empresas.EmpresaService",
            return_value=self.servico,
        )
        self.patcher_servico.start()

    def tearDown(self):
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
    def dados_validos(cnpj="04252011000110"):
        return {
            "cnpj": cnpj,
            "razao_social": "Nova Empresa Contratada Ltda",
            "nome_fantasia": "Nova Contratada",
            "cep": "01310100",
            "logradouro": "Avenida Paulista",
            "numero": "1000",
            "bairro": "Bela Vista",
            "cidade": "São Paulo",
            "uf": "SP",
            "telefone": "(11) 3000-4000",
            "email": "nova@empresa.test",
        }

    def test_administrador_acessa_listagem_cadastro_detalhes_e_edicao(self):
        self.autenticar_como(1)

        respostas = (
            self.client.get("/fiscalizacao-contratos/empresas"),
            self.client.get("/fiscalizacao-contratos/empresas/nova"),
            self.client.get("/fiscalizacao-contratos/empresas/1"),
            self.client.get("/fiscalizacao-contratos/empresas/1/editar"),
        )

        self.assertTrue(all(resposta.status_code == 200 for resposta in respostas))

    def test_administrador_cadastra_empresa(self):
        self.autenticar_como(1)

        resposta = self.client.post(
            "/fiscalizacao-contratos/empresas/nova",
            data=self.dados_validos(),
        )

        self.assertEqual(resposta.status_code, 302)
        self.assertIn(2, self.servico.empresas)

    def test_administrador_edita_empresa(self):
        self.autenticar_como(1)
        dados = self.dados_validos(cnpj="11222333000181")
        dados["razao_social"] = "Empresa Atualizada Ltda"

        resposta = self.client.post(
            "/fiscalizacao-contratos/empresas/1/editar",
            data=dados,
        )

        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(self.servico.empresas[1]["razao_social"], "Empresa Atualizada Ltda")

    def test_visitante_e_encaminhado_ao_login(self):
        resposta = self.client.get("/fiscalizacao-contratos/empresas")

        self.assertEqual(resposta.status_code, 302)
        self.assertIn("/login", resposta.headers["Location"])

    def test_usuario_comum_recebe_403(self):
        self.autenticar_como(2)

        resposta = self.client.get("/fiscalizacao-contratos/empresas")

        self.assertEqual(resposta.status_code, 403)

    def test_cnpj_invalido_e_rejeitado_antes_do_servico(self):
        self.autenticar_como(1)
        dados = self.dados_validos(cnpj="123")

        resposta = self.client.post("/fiscalizacao-contratos/empresas/nova", data=dados)

        self.assertEqual(resposta.status_code, 400)
        self.assertEqual(self.servico.criar_chamadas, 0)
        self.assertIn("CNPJ válido".encode("utf-8"), resposta.data)

    def test_cnpj_duplicado_recebe_mensagem_amigavel(self):
        self.autenticar_como(1)
        dados = self.dados_validos(cnpj="11.222.333/0001-81")

        resposta = self.client.post("/fiscalizacao-contratos/empresas/nova", data=dados)

        self.assertEqual(resposta.status_code, 409)
        self.assertIn("Já existe uma empresa".encode("utf-8"), resposta.data)

    def test_normaliza_cnpj_cep_e_uf(self):
        dados = self.dados_validos(cnpj="04.252.011/0001-10")
        dados["cep"] = "01310-100"
        dados["uf"] = "sp"

        normalizados, erros = normalizar_e_validar_empresa(dados)

        self.assertEqual(erros, [])
        self.assertEqual(normalizados["cnpj"], "04252011000110")
        self.assertEqual(normalizados["cep"], "01310100")
        self.assertEqual(normalizados["uf"], "SP")

    def test_inativacao_preserva_registro(self):
        self.autenticar_como(1)

        resposta = self.client.post("/fiscalizacao-contratos/empresas/1/inativar")

        self.assertEqual(resposta.status_code, 302)
        self.assertIn(1, self.servico.empresas)
        self.assertFalse(self.servico.empresas[1]["ativo"])

    def test_reativacao_retorna_empresa_ao_estado_ativo(self):
        self.autenticar_como(1)
        self.servico.empresas[1]["ativo"] = False

        resposta = self.client.post("/fiscalizacao-contratos/empresas/1/reativar")

        self.assertEqual(resposta.status_code, 302)
        self.assertTrue(self.servico.empresas[1]["ativo"])

    def test_falha_externa_permite_preenchimento_manual(self):
        self.autenticar_como(1)

        with patch(
            "modulos.fiscalizacao_contratos.routes.empresas.consultar_cnpj",
            side_effect=ConsultaExternaError("Serviço indisponível."),
        ):
            resposta = self.client.get(
                "/fiscalizacao-contratos/empresas/consultar-cnpj/11222333000181"
            )

        self.assertEqual(resposta.status_code, 503)
        self.assertTrue(resposta.get_json()["preenchimento_manual"])

    def test_rotas_antigas_continuam_registradas(self):
        rotas = {regra.rule for regra in self.flask_app.url_map.iter_rules()}

        for rota_antiga in ("/", "/login", "/logout", "/cadastrar", "/buscar_associados"):
            self.assertIn(rota_antiga, rotas)

    def test_nenhuma_conexao_real_foi_aberta(self):
        chamadas_antes = MOCK_CONNECT.call_count
        self.autenticar_como(1)

        resposta = self.client.get("/fiscalizacao-contratos/empresas")

        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(MOCK_CONNECT.call_count, chamadas_antes)

    def test_servico_usa_atualizacao_para_inativar_e_reativar(self):
        conexao = MagicMock()
        cursor = conexao.cursor.return_value.__enter__.return_value
        cursor.rowcount = 1
        servico = EmpresaService(lambda: conexao)

        servico.inativar(1, 1)
        sql_inativar = cursor.execute.call_args.args[0].upper()
        servico.reativar(1, 1)
        sql_reativar = cursor.execute.call_args.args[0].upper()

        self.assertIn("ATIVO = FALSE", sql_inativar)
        self.assertIn("ATIVO = TRUE", sql_reativar)
        self.assertIn("CURRENT_TIMESTAMP", sql_inativar)
        self.assertIn("CURRENT_TIMESTAMP", sql_reativar)
        self.assertNotIn("DELETE", sql_inativar + sql_reativar)

    def test_migracao_e_idempotente_e_nao_destrutiva(self):
        caminho = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "modulos",
            "fiscalizacao_contratos",
            "migrations",
            "001_criar_fc_empresas.sql",
        )
        with open(caminho, encoding="utf-8") as arquivo:
            sql = arquivo.read().upper()

        self.assertIn("CREATE TABLE IF NOT EXISTS FC_EMPRESAS", sql)
        self.assertIn("CREATE INDEX IF NOT EXISTS", sql)
        for comando_proibido in ("DROP", "TRUNCATE", "DELETE"):
            self.assertNotIn(comando_proibido, sql)


def tearDownModule():
    PATCH_CONEXAO.stop()


if __name__ == "__main__":
    unittest.main()
