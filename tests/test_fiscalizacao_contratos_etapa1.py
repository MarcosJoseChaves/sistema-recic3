"""Testes seguros da Etapa 1, sem qualquer conexão real com banco."""

import importlib
import os
import sys
import unittest
from tests.csrf_helpers import ClienteComCSRF
from unittest.mock import MagicMock, patch


def importar_app_sem_banco():
    """Importa app.py substituindo a conexão PostgreSQL por um objeto falso."""
    conexao_falsa = MagicMock(name="conexao_postgresql_falsa")
    cursor_falso = conexao_falsa.cursor.return_value
    cursor_falso.fetchall.return_value = []
    cursor_falso.fetchone.return_value = None

    sys.modules.pop("app", None)

    with (
        patch.dict(os.environ, {"APP_ENV": "testing", "SECRET_KEY": "teste-ficticio", "DATABASE_URL": ""}, clear=True),
        patch("dotenv.load_dotenv", return_value=False),
        patch("psycopg2.connect", return_value=conexao_falsa) as conectar_mock,
    ):
        modulo_app = importlib.import_module("app")

    return modulo_app, conectar_mock


APP_MODULE, CONECTAR_MOCK = importar_app_sem_banco()


class TestFiscalizacaoContratosEtapa1(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.flask_app = APP_MODULE.app
        cls.flask_app.config.update(TESTING=True)
        cls.user_loader_original = APP_MODULE.login_manager._user_callback

    @classmethod
    def tearDownClass(cls):
        APP_MODULE.login_manager._user_callback = cls.user_loader_original

    def setUp(self):
        self.client = ClienteComCSRF(self.flask_app.test_client())
        APP_MODULE.login_manager._user_callback = self._carregar_usuario_falso

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

    def test_visitante_e_redirecionado_ao_login_existente(self):
        resposta = self.client.get("/fiscalizacao-contratos")

        self.assertEqual(resposta.status_code, 302)
        self.assertIn("/login", resposta.headers["Location"])

    def test_usuario_comum_recebe_acesso_negado(self):
        self.autenticar_como(2)

        with patch.object(APP_MODULE, "conectar_banco", side_effect=AssertionError("Banco não deve ser acessado")):
            resposta = self.client.get("/fiscalizacao-contratos")

        self.assertEqual(resposta.status_code, 403)

    def test_administrador_acessa_o_modulo(self):
        self.autenticar_como(1)

        with (
            patch("modulos.fiscalizacao_contratos.routes.FiscalizacaoService") as fiscalizacao_cls,
            patch("modulos.fiscalizacao_contratos.routes.MedicaoService") as medicao_cls,
            patch("modulos.fiscalizacao_contratos.routes.AtesteService") as ateste_cls,
            patch.object(APP_MODULE, "conectar_banco", side_effect=AssertionError("Banco não deve ser acessado")),
        ):
            resposta = self.client.get("/fiscalizacao-contratos")

        self.assertEqual(resposta.status_code, 200)
        self.assertIn("Fiscalização de Contratos".encode("utf-8"), resposta.data)
        self.assertIn("Gestão integrada de contratos, fiscalizações, medições e documentos.".encode("utf-8"), resposta.data)
        self.assertIn("Painel principal".encode("utf-8"), resposta.data)
        self.assertNotIn("Painel do módulo".encode("utf-8"), resposta.data)
        fiscalizacao_cls.assert_not_called()
        medicao_cls.assert_not_called()
        ateste_cls.assert_not_called()

    def test_painel_exibe_os_onze_modulos_sem_indicadores(self):
        self.autenticar_como(1)

        with patch.object(APP_MODULE, "conectar_banco", side_effect=AssertionError("Banco não deve ser acessado")):
            resposta = self.client.get("/fiscalizacao-contratos")

        texto = resposta.data.decode("utf-8")
        destinos = {
            "Empresas": "/fiscalizacao-contratos/empresas",
            "Servidores e Responsáveis": "/fiscalizacao-contratos/servidores",
            "Contratos": "/fiscalizacao-contratos/contratos",
            "Aditivos": "/fiscalizacao-contratos/aditivos",
            "Documentos": "/fiscalizacao-contratos/documentos",
            "Planilhas Orçamentárias": "/fiscalizacao-contratos/planilhas",
            "Ativos Contratuais": "/fiscalizacao-contratos/ativos",
            "Fiscalizações": "/fiscalizacao-contratos/fiscalizacoes",
            "Ocorrências": "/fiscalizacao-contratos/ocorrencias",
            "Medições": "/fiscalizacao-contratos/medicoes",
            "Atestes": "/fiscalizacao-contratos/atestes",
        }
        self.assertEqual(resposta.status_code, 200)
        for titulo, destino in destinos.items():
            self.assertIn(titulo, texto)
            self.assertIn(f'href="{destino}"', texto)
        for indicador in (
            "Ocorrências abertas", "Ocorrências vencidas", "Fiscalizações em 30 dias",
            "Atestes em elaboração", "Valor encaminhado no mês", "Medições em elaboração",
            "Líquido aprovado no mês", "Glosas no mês",
        ):
            self.assertNotIn(indicador, texto)

    def test_rotas_antigas_continuam_registradas(self):
        rotas = {regra.rule for regra in self.flask_app.url_map.iter_rules()}

        for rota_antiga in ("/", "/login", "/logout", "/cadastrar", "/buscar_associados"):
            self.assertIn(rota_antiga, rotas)

        self.assertIn("/fiscalizacao-contratos", rotas)

    def test_sistema_importa_sem_acessar_banco(self):
        self.assertEqual(self.flask_app.name, "app")
        self.assertIsNotNone(self.client)
        CONECTAR_MOCK.assert_not_called()


if __name__ == "__main__":
    unittest.main()
