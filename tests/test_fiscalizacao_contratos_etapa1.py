"""Testes seguros da Etapa 1, sem qualquer conexão real com banco."""

import importlib
import os
import sys
import unittest
from unittest.mock import MagicMock, patch


def importar_app_sem_banco():
    """Importa app.py substituindo a conexão PostgreSQL por um objeto falso."""
    conexao_falsa = MagicMock(name="conexao_postgresql_falsa")
    cursor_falso = conexao_falsa.cursor.return_value
    cursor_falso.fetchall.return_value = []
    cursor_falso.fetchone.return_value = None

    sys.modules.pop("app", None)

    with (
        patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False),
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
        self.client = self.flask_app.test_client()
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

        with patch.object(APP_MODULE, "conectar_banco", side_effect=AssertionError("Banco não deve ser acessado")):
            resposta = self.client.get("/fiscalizacao-contratos")

        self.assertEqual(resposta.status_code, 200)
        self.assertIn("Fiscalização de Contratos".encode("utf-8"), resposta.data)
        self.assertIn("Módulo em construção".encode("utf-8"), resposta.data)

    def test_rotas_antigas_continuam_registradas(self):
        rotas = {regra.rule for regra in self.flask_app.url_map.iter_rules()}

        for rota_antiga in ("/", "/login", "/logout", "/cadastrar", "/buscar_associados"):
            self.assertIn(rota_antiga, rotas)

        self.assertIn("/fiscalizacao-contratos", rotas)

    def test_sistema_importa_e_cria_cliente_de_teste(self):
        self.assertEqual(self.flask_app.name, "app")
        self.assertIsNotNone(self.client)
        self.assertTrue(CONECTAR_MOCK.called)
        self.assertIs(CONECTAR_MOCK.return_value, CONECTAR_MOCK.return_value)


if __name__ == "__main__":
    unittest.main()
