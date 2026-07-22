"""Testes da proteção CSRF sem PostgreSQL ou Cloudinary reais."""

import base64
import importlib
import io
import os
import re
import sys
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from flask import Flask, jsonify, render_template_string
from werkzeug.security import generate_password_hash

from configuracao_ambiente import configurar_aplicacao
from seguranca_csrf import configurar_csrf, csrf


SEGREDO_TESTE = "segredo-csrf-exclusivamente-ficticio"
PADRAO_TOKEN = re.compile(
    rb'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)', re.IGNORECASE
)
PDF_VALIDO = b"%PDF-1.4\n1 0 obj<</Type/Catalog>>endobj\n%%EOF"
CATEGORIAS_CRITICAS = {
    "empresa": "/fiscalizacao-contratos/empresas/1/inativar",
    "servidor": "/fiscalizacao-contratos/servidores/1/inativar",
    "contrato": "/fiscalizacao-contratos/contratos/1/inativar",
    "aditivo": "/fiscalizacao-contratos/aditivos/1/inativar",
    "documento": "/fiscalizacao-contratos/documentos/1/inativar",
    "planilha": "/fiscalizacao-contratos/planilhas/1/inativar",
    "ativo": "/fiscalizacao-contratos/ativos/1/inativar",
    "fiscalizacao": "/fiscalizacao-contratos/fiscalizacoes/1/finalizar",
    "ocorrencia": "/fiscalizacao-contratos/ocorrencias/1/inativar",
    "medicao": "/fiscalizacao-contratos/medicoes/1/cancelar",
    "ateste": "/fiscalizacao-contratos/atestes/1/cancelar",
}


def importar_app_sem_servicos_reais():
    sys.modules.pop("app", None)
    conexao = MagicMock(name="conexao_falsa")
    conexao.cursor.return_value.fetchall.return_value = []
    conexao.cursor.return_value.fetchone.return_value = None
    with (
        patch.dict(
            os.environ,
            {"APP_ENV": "testing", "SECRET_KEY": SEGREDO_TESTE, "DATABASE_URL": ""},
            clear=True,
        ),
        patch("dotenv.load_dotenv", return_value=False),
        patch(
            "psycopg2.connect",
            side_effect=AssertionError("teste tentou acessar PostgreSQL real"),
        ),
        patch(
            "cloudinary.uploader.upload",
            side_effect=AssertionError("teste tentou enviar ao Cloudinary real"),
        ),
        patch(
            "cloudinary.uploader.destroy",
            side_effect=AssertionError("teste tentou remover do Cloudinary real"),
        ),
    ):
        return importlib.import_module("app")


APP_MODULE = importar_app_sem_servicos_reais()


def obter_token(cliente, caminho="/login", headers=None):
    resposta = cliente.get(caminho, headers=headers or {})
    encontrado = PADRAO_TOKEN.search(resposta.data)
    if not encontrado:
        raise AssertionError("Token CSRF não encontrado no formulário.")
    return encontrado.group(1).decode(), resposta


def cabecalho_basic(usuario="porteiro", senha="senha-ficticia"):
    valor = base64.b64encode(f"{usuario}:{senha}".encode()).decode()
    return {"Authorization": f"Basic {valor}"}


class TestProtecaoCsrfAplicacao(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = APP_MODULE.app
        cls.carregador_original = APP_MODULE.login_manager._user_callback

    @classmethod
    def tearDownClass(cls):
        APP_MODULE.login_manager._user_callback = cls.carregador_original

    def setUp(self):
        self.client = self.app.test_client()
        APP_MODULE.login_manager._user_callback = self._usuario

    @staticmethod
    def _usuario(user_id):
        usuarios = {
            "1": APP_MODULE.User(1, "administrador", "admin", None),
            "2": APP_MODULE.User(2, "usuario", "usuario", "UVR 01"),
        }
        return usuarios.get(str(user_id))

    def autenticar(self, user_id):
        with self.client.session_transaction() as sessao:
            sessao["_user_id"] = str(user_id)
            sessao["_fresh"] = True

    def test_01_formulario_get_contem_token_e_token_nao_esta_na_url(self):
        token, resposta = obter_token(self.client)
        self.assertEqual(resposta.status_code, 200)
        self.assertTrue(token)
        self.assertNotIn(token, resposta.request.url)

    def test_02_post_sem_token_retorna_400(self):
        resposta = self.client.post("/login", data={"username": "x", "password": "x"})
        self.assertEqual(resposta.status_code, 400)

    def test_03_post_com_token_invalido_retorna_400(self):
        obter_token(self.client)
        resposta = self.client.post(
            "/login",
            data={"username": "x", "password": "x", "csrf_token": "invalido"},
        )
        self.assertEqual(resposta.status_code, 400)

    def test_04_token_de_outra_sessao_retorna_400(self):
        token, _ = obter_token(self.client)
        outro_cliente = self.app.test_client()
        resposta = outro_cliente.post(
            "/login", data={"username": "x", "password": "x", "csrf_token": token}
        )
        self.assertEqual(resposta.status_code, 400)

    def test_05_login_com_token_valido_e_credenciais_erradas_mantem_fluxo(self):
        token, _ = obter_token(self.client)
        conexao = MagicMock()
        conexao.cursor.return_value.fetchone.return_value = None
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.post(
                "/login",
                data={"username": "incorreto", "password": "x", "csrf_token": token},
            )
        self.assertEqual(resposta.status_code, 200)
        self.assertIn("encontrado".encode(), resposta.data)

    def test_06_login_com_token_valido_e_credenciais_corretas_autentica(self):
        token, _ = obter_token(self.client)
        conexao = MagicMock()
        conexao.cursor.return_value.fetchone.return_value = (
            1,
            "administrador",
            generate_password_hash("senha-correta"),
            "admin",
            None,
        )
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.post(
                "/login",
                data={
                    "username": "administrador",
                    "password": "senha-correta",
                    "csrf_token": token,
                },
            )
        self.assertEqual(resposta.status_code, 302)
        with self.client.session_transaction() as sessao:
            self.assertEqual(sessao.get("_user_id"), "1")

    def test_07_logout_por_get_nao_altera_sessao(self):
        self.autenticar(1)
        resposta = self.client.get("/logout")
        self.assertEqual(resposta.status_code, 405)
        with self.client.session_transaction() as sessao:
            self.assertEqual(sessao.get("_user_id"), "1")

    def test_08_logout_post_sem_token_nao_encerra_sessao(self):
        self.autenticar(1)
        resposta = self.client.post("/logout")
        self.assertEqual(resposta.status_code, 400)
        with self.client.session_transaction() as sessao:
            self.assertEqual(sessao.get("_user_id"), "1")

    def test_09_logout_post_com_token_encerra_sessao(self):
        self.autenticar(1)
        token, _ = obter_token(self.client)
        resposta = self.client.post("/logout", data={"csrf_token": token})
        self.assertEqual(resposta.status_code, 302)
        with self.client.session_transaction() as sessao:
            self.assertIsNone(sessao.get("_user_id"))

    def test_09a_logout_invalido_preserva_sessao_e_visitante_tem_fluxo_seguro(self):
        self.autenticar(1)
        obter_token(self.client)
        resposta = self.client.post("/logout", data={"csrf_token": "invalido"})
        self.assertEqual(resposta.status_code, 400)
        with self.client.session_transaction() as sessao:
            self.assertEqual(sessao.get("_user_id"), "1")
            sessao.pop("_user_id", None)
            sessao.pop("_fresh", None)
        token, _ = obter_token(self.client)
        resposta = self.client.post("/logout", data={"csrf_token": token})
        self.assertEqual(resposta.status_code, 302)
        self.assertIn("/login", resposta.headers["Location"])

    def test_10_token_valido_nao_substitui_login(self):
        token, _ = obter_token(self.client)
        resposta = self.client.post(
            "/fiscalizacao-contratos/empresas/1/inativar",
            data={"csrf_token": token},
        )
        self.assertEqual(resposta.status_code, 302)
        self.assertIn("/login", resposta.headers["Location"])

    def test_11_usuario_comum_com_token_valido_continua_recebendo_403(self):
        self.autenticar(2)
        token, _ = obter_token(self.client)
        resposta = self.client.post(
            "/fiscalizacao-contratos/empresas/1/inativar",
            data={"csrf_token": token},
        )
        self.assertEqual(resposta.status_code, 403)

    def test_12_json_sem_token_e_rejeitado(self):
        self.autenticar(1)
        resposta = self.client.post("/api/subgrupos", json={"acao": "novo"})
        self.assertEqual(resposta.status_code, 400)

    def test_13_json_com_token_invalido_e_rejeitado(self):
        self.autenticar(1)
        obter_token(self.client)
        resposta = self.client.post(
            "/api/subgrupos",
            json={"acao": "novo"},
            headers={"X-CSRFToken": "invalido"},
        )
        self.assertEqual(resposta.status_code, 400)

    def test_14_json_com_cabecalho_valido_chega_a_regra_de_negocio(self):
        self.autenticar(1)
        token, _ = obter_token(self.client)
        conexao = MagicMock()
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.post(
                "/api/subgrupos",
                json={"acao": "novo", "nome": "", "atividade_pai": ""},
                headers={"X-CSRFToken": token},
            )
        self.assertEqual(resposta.status_code, 400)
        self.assertIn("obrigat".encode(), resposta.data)

    def test_15_json_com_token_valido_sem_login_nao_acessa_negocio(self):
        token, _ = obter_token(self.client)
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=AssertionError("negócio acessado")
        ):
            resposta = self.client.post(
                "/api/subgrupos", json={"acao": "novo"}, headers={"X-CSRFToken": token}
            )
        self.assertEqual(resposta.status_code, 302)

    def test_16_upload_sem_token_nao_acessa_cloudinary(self):
        self.autenticar(1)
        with (
            patch(
                "modulos.fiscalizacao_contratos.routes.documentos.CloudinaryStorage"
            ) as armazenamento,
            patch.object(
                APP_MODULE,
                "conectar_banco",
                side_effect=AssertionError("CSRF permitiu acesso ao banco"),
            ) as conectar,
        ):
            resposta = self.client.post(
                "/fiscalizacao-contratos/documentos/novo",
                data={"arquivo": (io.BytesIO(PDF_VALIDO), "nota.pdf")},
                content_type="multipart/form-data",
            )
        self.assertEqual(resposta.status_code, 400)
        armazenamento.assert_not_called()
        conectar.assert_not_called()

    def test_17_upload_com_token_valido_mantem_fluxo(self):
        self.autenticar(1)
        token, _ = obter_token(self.client)
        with (
            patch(
                "modulos.fiscalizacao_contratos.routes.documentos.CloudinaryStorage"
            ) as armazenamento,
            patch(
                "modulos.fiscalizacao_contratos.routes.documentos.DocumentoService.criar",
                return_value=9,
            ) as criar,
        ):
            resposta = self.client.post(
                "/fiscalizacao-contratos/documentos/novo",
                data={
                    "csrf_token": token,
                    "contrato_id": "1",
                    "aditivo_id": "",
                    "categoria": "Contrato",
                    "titulo": "Comprovante",
                    "descricao": "Teste isolado",
                    "arquivo": (io.BytesIO(PDF_VALIDO), "nota.pdf"),
                },
                content_type="multipart/form-data",
            )
        self.assertEqual(resposta.status_code, 302)
        armazenamento.assert_called_once()
        criar.assert_called_once()

    def test_18_erro_csrf_e_generico_e_nao_revela_detalhes(self):
        resposta = self.client.post(
            "/login", data={"username": "segredo", "password": "segredo"}
        )
        corpo = resposta.get_data(as_text=True)
        self.assertEqual(resposta.status_code, 400)
        self.assertIn("Atualize a página", corpo)
        for segredo in (SEGREDO_TESTE, "csrf_token", "_user_id", "segredo"):
            self.assertNotIn(segredo, corpo)

    def test_19_health_permanece_publico_sem_token(self):
        self.assertEqual(self.client.get("/health").status_code, 200)

    def test_20_gets_de_consulta_continuam_funcionando(self):
        self.assertEqual(self.client.get("/login?origem=teste").status_code, 200)
        resposta = self.client.get("/fiscalizacao-contratos/empresas?busca=x")
        self.assertEqual(resposta.status_code, 302)

    def test_21_operacao_de_alteracao_nao_e_aceita_por_get(self):
        self.autenticar(1)
        resposta = self.client.get("/fiscalizacao-contratos/empresas/1/inativar")
        self.assertEqual(resposta.status_code, 405)

    def test_22_csrf_fica_ativo_e_sem_excecoes(self):
        self.assertTrue(self.app.config["WTF_CSRF_ENABLED"])
        self.assertEqual(csrf._exempt_views, set())

    def test_23_tokens_vazios_com_espacos_e_malformados_sao_rejeitados(self):
        obter_token(self.client)
        for token in ("", "   ", "token-malformado"):
            with self.subTest(token=repr(token)):
                resposta = self.client.post(
                    "/login",
                    data={"username": "x", "password": "x", "csrf_token": token},
                )
                self.assertEqual(resposta.status_code, 400)

    def test_24_sessao_sem_cookie_ou_com_cookie_invalido_e_rejeitada(self):
        token, _ = obter_token(self.client)
        for cookie in (None, "session=cookie-invalido"):
            headers = {"Cookie": cookie} if cookie else {}
            cliente = self.app.test_client(use_cookies=False)
            with self.subTest(cookie=bool(cookie)):
                resposta = cliente.post(
                    "/login",
                    data={"username": "x", "password": "x", "csrf_token": token},
                    headers=headers,
                )
                self.assertEqual(resposta.status_code, 400)

    def test_25_cabecalho_duplicado_ou_malformado_e_rejeitado(self):
        token, _ = obter_token(self.client)
        respostas = (
            self.client.post(
                "/login",
                data={"username": "x", "password": "x"},
                headers=[("X-CSRFToken", token), ("X-CSRFToken", "outro")],
            ),
            self.client.post(
                "/login",
                data={"username": "x", "password": "x"},
                headers={"X-CSRFToken": "   "},
            ),
        )
        self.assertTrue(all(resposta.status_code == 400 for resposta in respostas))

    def test_26_token_invalido_no_formulario_prevalece_sobre_cabecalho_valido(self):
        token, _ = obter_token(self.client)
        resposta = self.client.post(
            "/login",
            data={"username": "x", "password": "x", "csrf_token": "invalido"},
            headers={"X-CSRFToken": token},
        )
        self.assertEqual(resposta.status_code, 400)

    def test_27_token_expira_apos_o_limite_padrao_de_uma_hora(self):
        token, _ = obter_token(self.client)
        self.assertEqual(self.app.config["WTF_CSRF_TIME_LIMIT"], 3600)
        with patch("itsdangerous.timed.time.time", return_value=time.time() + 3601):
            resposta = self.client.post(
                "/login",
                data={"username": "x", "password": "x", "csrf_token": token},
            )
        self.assertEqual(resposta.status_code, 400)

    def test_28_abrir_multiplos_formularios_nao_invalida_token_anterior(self):
        primeiro, _ = obter_token(self.client)
        segundo, _ = obter_token(self.client)
        self.assertTrue(primeiro)
        self.assertTrue(segundo)
        conexao = MagicMock()
        conexao.cursor.return_value.fetchone.return_value = None
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.post(
                "/login",
                data={"username": "incorreto", "password": "x", "csrf_token": primeiro},
            )
        self.assertEqual(resposta.status_code, 200)

    def test_29_categorias_criticas_bloqueiam_admin_sem_token_ou_com_token_invalido(self):
        self.autenticar(1)
        obter_token(self.client)
        for categoria, caminho in CATEGORIAS_CRITICAS.items():
            with self.subTest(categoria=categoria, caso="ausente"):
                self.assertEqual(self.client.post(caminho).status_code, 400)
            with self.subTest(categoria=categoria, caso="invalido"):
                self.assertEqual(
                    self.client.post(caminho, data={"csrf_token": "invalido"}).status_code,
                    400,
                )

    def test_30_categorias_criticas_nao_trocam_token_por_login_ou_admin(self):
        token, _ = obter_token(self.client)
        for categoria, caminho in CATEGORIAS_CRITICAS.items():
            with self.subTest(categoria=categoria, perfil="visitante"):
                self.assertEqual(
                    self.client.post(caminho, data={"csrf_token": token}).status_code,
                    302,
                )
        self.autenticar(2)
        token, _ = obter_token(self.client)
        for categoria, caminho in CATEGORIAS_CRITICAS.items():
            with self.subTest(categoria=categoria, perfil="usuario"):
                self.assertEqual(
                    self.client.post(caminho, data={"csrf_token": token}).status_code,
                    403,
                )

    def test_31_cinco_formularios_multipart_tem_token_no_proprio_formulario(self):
        raiz = Path(__file__).resolve().parents[1]
        encontrados = []
        for caminho in raiz.rglob("*.html"):
            if "_referencia_fiscaliza" in caminho.parts:
                continue
            conteudo = caminho.read_text(encoding="utf-8")
            for formulario in re.findall(r"<form\b[^>]*>.*?</form\s*>", conteudo, re.I | re.S):
                abertura = re.match(r"<form\b[^>]*>", formulario, re.I | re.S).group(0)
                if re.search(r'enctype\s*=\s*["\']multipart/form-data', abertura, re.I):
                    encontrados.append(caminho)
                    self.assertEqual(len(re.findall(r'name=["\']csrf_token["\']', formulario)), 1)
        self.assertEqual(len(encontrados), 5)

    def test_32_alteracao_de_senha_e_painel_principal_exibem_tokens_reais(self):
        self.autenticar(1)
        for caminho in ("/alterar_senha", "/"):
            with self.subTest(caminho=caminho):
                resposta = self.client.get(caminho)
                self.assertEqual(resposta.status_code, 200)
                self.assertIsNotNone(PADRAO_TOKEN.search(resposta.data))


class TestCsrfComBarreiraHomologacao(unittest.TestCase):
    def setUp(self):
        variaveis = {
            "APP_ENV": "homologation",
            "SECRET_KEY": SEGREDO_TESTE,
            "DATABASE_URL": "postgresql://usuario-ficticio@host/banco",
            "HOMOLOGATION_GATE_ENABLED": "true",
            "HOMOLOGATION_GATE_USER": "porteiro",
            "HOMOLOGATION_GATE_PASSWORD": "senha-ficticia",
        }
        with patch.dict(os.environ, variaveis, clear=True):
            self.app = Flask(
                __name__, template_folder=str(Path(__file__).resolve().parents[1] / "templates")
            )
            configurar_aplicacao(self.app)
            configurar_csrf(self.app)

        @self.app.get("/formulario")
        def formulario():
            return render_template_string(
                '<form method="post"><input name="csrf_token" value="{{ csrf_token() }}"></form>'
            )

        @self.app.post("/escrita")
        def escrita():
            return jsonify(ok=True)

        self.client = self.app.test_client()
        self.basic = cabecalho_basic()
        self.basic["Referer"] = "https://localhost/formulario"
        resposta = self.client.get(
            "/formulario", headers=self.basic, base_url="https://localhost"
        )
        encontrado = PADRAO_TOKEN.search(resposta.data)
        self.assertIsNotNone(encontrado)
        self.token = encontrado.group(1).decode()

    def test_23_basic_correta_nao_substitui_csrf(self):
        resposta = self.client.post(
            "/escrita", headers=self.basic, base_url="https://localhost"
        )
        self.assertEqual(resposta.status_code, 400)
        self.assertEqual(resposta.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(resposta.headers["X-Frame-Options"], "SAMEORIGIN")

    def test_24_csrf_correto_nao_substitui_basic(self):
        resposta = self.client.post(
            "/escrita",
            data={"csrf_token": self.token},
            base_url="https://localhost",
            headers={"Referer": "https://localhost/formulario"},
        )
        self.assertEqual(resposta.status_code, 401)
        self.assertEqual(resposta.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(resposta.headers["X-Frame-Options"], "SAMEORIGIN")
        self.assertNotIn("porteiro", resposta.get_data(as_text=True))
        self.assertNotIn(self.token, resposta.headers.get("WWW-Authenticate", ""))

    def test_25_basic_e_csrf_validos_permiten_fluxo(self):
        resposta = self.client.post(
            "/escrita",
            data={"csrf_token": self.token},
            headers=self.basic,
            base_url="https://localhost",
        )
        self.assertEqual(resposta.status_code, 200)


class TestCsrfTodosAmbientes(unittest.TestCase):
    def test_csrf_ativo_em_todos_os_ambientes(self):
        for ambiente in ("development", "testing", "homologation", "production"):
            variaveis = {"APP_ENV": ambiente, "SECRET_KEY": SEGREDO_TESTE}
            if ambiente in {"homologation", "production"}:
                variaveis["DATABASE_URL"] = "postgresql://usuario-ficticio@host/banco"
            with self.subTest(ambiente=ambiente), patch.dict(
                os.environ, variaveis, clear=True
            ):
                app = Flask(__name__)
                configurar_aplicacao(app)
                configurar_csrf(app)
                self.assertTrue(app.config["WTF_CSRF_ENABLED"])


if __name__ == "__main__":
    unittest.main()
