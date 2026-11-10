"""Testes ESG isolados: sem .env, banco, rede, migrations ou serviços reais."""

import builtins
from contextlib import ExitStack
import importlib
import io
import os
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit


class TestESG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # As barreiras cobrem a importação do app e todas as requisições locais.
        barreiras = ExitStack()
        cls.addClassCleanup(barreiras.close)
        barreiras.enter_context(patch.dict(os.environ, {
            "APP_ENV": "testing",
            "SECRET_KEY": "esg-chave-exclusiva-de-teste",
            "DATABASE_URL": "",
            "RATELIMIT_STORAGE_URI": "memory://",
        }, clear=True))

        cls.bloqueios = {}
        for alvo in (
            "socket.socket.connect",
            "socket.socket.connect_ex",
            "socket.socket.bind",
            "socket.create_connection",
            "socket.getaddrinfo",
            "psycopg2.connect",
            "requests.sessions.Session.request",
            "cloudinary.uploader.upload",
            "cloudinary.uploader.destroy",
            "migrations_control.runner.MigrationRunner.executar",
            "migrations_control.runner.executar_cadeia_controlada",
            "migrations_control.bootstrap.executar_bootstrap_controlado",
        ):
            cls.bloqueios[alvo] = barreiras.enter_context(patch(
                alvo, side_effect=AssertionError(f"Recurso proibido no teste: {alvo}")
            ))

        cls.dotenv_mock = barreiras.enter_context(
            patch("dotenv.load_dotenv", return_value=False)
        )

        def proteger_abertura(abrir):
            def protegido(arquivo, *args, **kwargs):
                if isinstance(arquivo, (str, bytes, os.PathLike)):
                    caminho = Path(os.fsdecode(arquivo))
                    if (
                        caminho.name.lower().startswith(".env")
                        or caminho.suffix.lower() in {".db", ".sqlite", ".sqlite3", ".log"}
                    ):
                        raise AssertionError("Leitura de arquivo operacional proibida")
                return abrir(arquivo, *args, **kwargs)
            return protegido

        barreiras.enter_context(patch("builtins.open", proteger_abertura(builtins.open)))
        barreiras.enter_context(patch("io.open", proteger_abertura(io.open)))

        # Preserva qualquer módulo carregado por outras suítes e evita mocks globais.
        anterior = sys.modules.pop("app", None)

        def restaurar_app():
            sys.modules.pop("app", None)
            if anterior is not None:
                sys.modules["app"] = anterior

        cls.addClassCleanup(restaurar_app)
        cls.modulo = importlib.import_module("app")
        cls.app = cls.modulo.app

    def setUp(self):
        self.client = self.app.test_client()
        # Só o carregamento de usuários é substituído. LoginManager, User,
        # sessão assinada e a guarda administrativa são os do RECIC3.
        usuarios = {
            "1": self.modulo.User(1, "admin_teste", "admin", None),
            "2": self.modulo.User(2, "comum_teste", "usuario", "UVR 01"),
            "3": self.modulo.User(3, "sem_perfil_teste", None, None),
        }
        self.loader = self.enterContext(patch.object(
            self.modulo.login_manager, "_user_callback", side_effect=usuarios.get
        ))
        self.banco = self.enterContext(patch.object(
            self.modulo, "conectar_banco",
            side_effect=AssertionError("O painel ESG não deve acessar banco"),
        ))

    def tearDown(self):
        self.banco.assert_not_called()
        for alvo, bloqueio in self.bloqueios.items():
            with self.subTest(recurso=alvo):
                bloqueio.assert_not_called()

    def autenticar(self, user_id):
        with self.client.session_transaction() as sessao:
            sessao["_user_id"] = str(user_id)
            sessao["_fresh"] = True

    def test_blueprint_e_resolucao_da_rota(self):
        from flask import url_for

        self.assertIn("esg", self.app.blueprints)
        mapa = self.app.url_map.bind("localhost")
        for caminho in ("/esg", "/esg/"):
            self.assertEqual(mapa.match(caminho, method="GET"), ("esg.painel", {}))
        with self.app.test_request_context():
            self.assertEqual(url_for("esg.painel"), "/esg")
        self.assertEqual(self.modulo.login_manager.login_view, "login")
        self.assertIsNotNone(self.modulo.login_manager._user_callback)

    def test_visitante_encaminhado_ao_login_existente(self):
        for caminho in ("/esg", "/esg/"):
            with self.subTest(caminho=caminho):
                resposta = self.client.get(caminho)
                self.assertEqual(resposta.status_code, 302)
                destino = urlsplit(resposta.headers["Location"])
                self.assertEqual(destino.path, "/login")
                self.assertEqual(parse_qs(destino.query)["next"], [caminho])

    def test_usuario_sem_permissao_recebe_403(self):
        for user_id in (2, 3):
            for caminho in ("/esg", "/esg/"):
                with self.subTest(usuario=user_id, caminho=caminho):
                    self.autenticar(user_id)
                    resposta = self.client.get(caminho)
                    self.assertEqual(resposta.status_code, 403)
                    self.assertNotIn(b"Indicadores ESG", resposta.data)

    def test_sessao_nao_reconhecida_encaminhada_ao_login(self):
        self.autenticar(999)
        resposta = self.client.get("/esg")
        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(urlsplit(resposta.headers["Location"]).path, "/login")

    def test_administrador_autorizado(self):
        self.autenticar(1)
        for caminho in ("/esg", "/esg/"):
            with self.subTest(caminho=caminho):
                resposta = self.client.get(caminho)
                self.assertEqual(resposta.status_code, 200)
                self.assertIn("Marechal Aterro Zero", resposta.get_data(as_text=True))
                self.assertIn("no-store", resposta.headers["Cache-Control"])

    def test_tela_sem_indicadores_ou_dados_aprovados(self):
        self.autenticar(1)
        resposta = self.client.get("/esg")
        texto = resposta.get_data(as_text=True)
        self.assertEqual(resposta.status_code, 200)
        for mensagem in (
            "Indicadores ESG", "Nenhum indicador cadastrado",
            "Dados ainda não aprovados", "Aguardando dados aprovados",
            "Ambiental", "Social", "Governança",
        ):
            self.assertIn(mensagem, texto)
        self.assertNotRegex(texto, r"\d[\d.,]*\s*(%|kg|toneladas|tCO2)")
        self.assertNotIn("<table", texto)

    def test_recursos_locais_e_logout_com_csrf(self):
        self.autenticar(1)
        texto = self.client.get("/esg").get_data(as_text=True)
        self.assertIn('href="/esg/static/css/modulo.css"', texto)
        self.assertNotIn("https://", texto)
        self.assertNotIn("<script", texto)
        self.assertIn('method="POST" action="/logout"', texto)
        self.assertRegex(texto, r'name="csrf_token" value="[^"]+"')
        resposta = self.client.get("/esg/static/css/modulo.css")
        try:
            self.assertEqual(resposta.status_code, 200)
            self.assertEqual(resposta.mimetype, "text/css")
        finally:
            resposta.close()

    def test_menu_visivel_somente_para_administrador(self):
        for user_id, visivel in ((2, False), (1, True)):
            with self.subTest(usuario=user_id):
                self.autenticar(user_id)
                resposta = self.client.get("/")
                self.assertEqual(resposta.status_code, 200)
                self.assertEqual(b'href="/esg"' in resposta.data, visivel)

    def test_importacao_segura_e_protecoes_centrais_preservadas(self):
        self.dotenv_mock.assert_called_once_with()
        self.assertIsNone(self.app.config["DATABASE_URL"])
        self.assertEqual(self.app.config["APP_ENV"], "testing")
        self.assertEqual(self.app.config["RATELIMIT_STORAGE_URI"], "memory://")
        self.assertTrue(self.app.config["WTF_CSRF_ENABLED"])
        for bloqueio in self.bloqueios.values():
            bloqueio.assert_not_called()


if __name__ == "__main__":
    unittest.main()
