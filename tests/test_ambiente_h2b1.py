"""Testes da reprodutibilidade, hosts confiáveis e Gunicorn seguro."""

import os
import runpy
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

from flask import Flask, jsonify, request
from werkzeug.middleware.proxy_fix import ProxyFix

from configuracao_ambiente import (
    configurar_aplicacao,
    ler_hosts_confiaveis,
    ler_limite_requisicao_mb,
)


RAIZ = Path(__file__).resolve().parents[1]
SEGREDO_TESTE = "segredo-ficticio-h2b1"
BANCO_TESTE = "postgresql://usuario-ficticio@host-ficticio/banco-ficticio"
HOST_TESTE = "homologacao.exemplo.invalid"


def ambiente_online(**valores):
    ambiente = {
        "APP_ENV": "homologation",
        "SECRET_KEY": SEGREDO_TESTE,
        "DATABASE_URL": BANCO_TESTE,
        "TRUSTED_HOSTS": HOST_TESTE,
        "TRUST_PROXY": "true",
        "MAX_REQUEST_MB": "64",
    }
    ambiente.update(valores)
    return ambiente


def criar_app_online(**valores):
    with patch.dict(os.environ, ambiente_online(**valores), clear=True):
        app = Flask(__name__)
        configurar_aplicacao(app)

    @app.get("/")
    def inicio():
        return jsonify(
            ok=True,
            host=request.host,
            origem=request.remote_addr,
            esquema=request.scheme,
        )

    return app


def carregar_gunicorn(**valores):
    ambiente = {
        "APP_ENV": "homologation",
        "PORT": "10000",
    }
    ambiente.update(valores)
    with patch.dict(os.environ, ambiente, clear=True):
        return runpy.run_path(str(RAIZ / "gunicorn.conf.py"))


def criar_app_limites(limite_mb="1"):
    ambiente = {
        "APP_ENV": "testing",
        "SECRET_KEY": SEGREDO_TESTE,
        "MAX_REQUEST_MB": limite_mb,
    }
    with patch.dict(os.environ, ambiente, clear=True):
        app = Flask(__name__)
        configurar_aplicacao(app)

    operacao_html = MagicMock()
    operacao_json = MagicMock()
    operacao_upload = MagicMock()

    @app.post("/receber-html")
    def receber_html():
        request.get_data(cache=False)
        operacao_html()
        return "ok"

    @app.post("/receber-json")
    def receber_json():
        request.get_data(cache=False)
        operacao_json()
        return jsonify(ok=True)

    @app.post("/receber-upload")
    def receber_upload():
        arquivo = request.files.get("arquivo")
        if arquivo:
            arquivo.read()
        operacao_upload()
        return "ok"

    app.config["JSON_ENDPOINT_CLASSIFIER"] = (
        lambda: request.endpoint == "receber_json"
    )
    return app, operacao_html, operacao_json, operacao_upload


class TestReprodutibilidadeH2B1(unittest.TestCase):
    def test_01_python_esta_fixado_com_patch(self):
        self.assertEqual(
            (RAIZ / ".python-version").read_text(encoding="utf-8").strip(),
            "3.12.6",
        )

    def test_02_dependencias_diretas_e_transitivas_estao_fixadas(self):
        linhas = [
            linha.strip()
            for linha in (RAIZ / "requirements.txt").read_text(encoding="utf-8").splitlines()
            if linha.strip() and not linha.lstrip().startswith("#")
        ]
        self.assertGreaterEqual(len(linhas), 20)
        self.assertTrue(all("==" in linha for linha in linhas))
        nomes = [linha.split("==", 1)[0].casefold() for linha in linhas]
        self.assertEqual(len(nomes), len(set(nomes)))
        self.assertIn("gunicorn==26.0.0", linhas)
        self.assertIn("flask==3.1.2", [linha.casefold() for linha in linhas])
        proibidos = (">=", "<=", "~=", " @ ", "file:", "-e ", "${", "c:\\")
        self.assertFalse(
            any(atomo in linha.casefold() for linha in linhas for atomo in proibidos)
        )

    def test_03_requirements_in_identifica_somente_dependencias_diretas(self):
        diretas = {
            linha.split("==", 1)[0].casefold()
            for linha in (RAIZ / "requirements.in").read_text(encoding="utf-8").splitlines()
            if linha.strip() and not linha.lstrip().startswith("#")
        }
        self.assertEqual(
            diretas,
            {
                "cloudinary",
                "flask",
                "flask-login",
                "flask-wtf",
                "gunicorn",
                "psycopg2-binary",
                "python-dotenv",
                "reportlab",
                "requests",
                "werkzeug",
            },
        )

    def test_04_procfile_usa_configuracao_sem_migration(self):
        procfile = (RAIZ / "Procfile").read_text(encoding="utf-8").strip()
        self.assertEqual(
            procfile,
            "web: gunicorn --config gunicorn.conf.py app:app",
        )
        self.assertNotRegex(procfile.casefold(), r"migr|upgrade|psql|flask\s+db")

    def test_05_gunicorn_exige_ambiente_online(self):
        for ambiente in ("", "development", "testing", "outro"):
            with self.subTest(ambiente=ambiente), self.assertRaisesRegex(
                RuntimeError, "APP_ENV"
            ):
                carregar_gunicorn(APP_ENV=ambiente)

    def test_06_gunicorn_exige_porta_valida(self):
        casos = ("", "texto", "0", "65536")
        for porta in casos:
            with self.subTest(porta=porta), self.assertRaisesRegex(
                RuntimeError, "PORT"
            ):
                carregar_gunicorn(PORT=porta)

    def test_07_gunicorn_tem_limites_conservadores(self):
        config = carregar_gunicorn()
        self.assertEqual(config["bind"], "0.0.0.0:10000")
        self.assertEqual(config["workers"], 2)
        self.assertEqual(config["worker_class"], "gthread")
        self.assertEqual(config["threads"], 4)
        self.assertEqual(config["timeout"], 60)
        self.assertEqual(config["graceful_timeout"], 30)
        self.assertFalse(config["preload_app"])
        self.assertFalse(config["reload"])
        self.assertFalse(config["daemon"])
        self.assertEqual(config["max_requests"], 1000)
        self.assertGreater(config["max_requests_jitter"], 0)

    def test_08_gunicorn_rejeita_concorrencia_e_timeout_excessivos(self):
        casos = (
            {"WEB_CONCURRENCY": "9"},
            {"GUNICORN_THREADS": "17"},
            {"GUNICORN_TIMEOUT": "301"},
            {"GUNICORN_GRACEFUL_TIMEOUT": "121"},
        )
        for valores in casos:
            with self.subTest(valores=valores), self.assertRaises(RuntimeError):
                carregar_gunicorn(**valores)

    def test_09_log_do_gunicorn_nao_inclui_query_headers_ou_cookies(self):
        formato = carregar_gunicorn()["access_log_format"]
        self.assertIn("%(U)s", formato)
        for atom in ("%(q)s", "%(r)s", "%({cookie}i)s", "%({authorization}i)s"):
            self.assertNotIn(atom, formato.casefold())

    def test_10_gunicorn_nao_importa_app_nem_executa_migration_na_configuracao(self):
        fonte = (RAIZ / "gunicorn.conf.py").read_text(encoding="utf-8")
        self.assertNotIn("import app", fonte)
        self.assertNotRegex(fonte.casefold(), r"migr|psycopg|cloudinary|subprocess")

        app = (RAIZ / "app.py").read_text(encoding="utf-8")
        bloco_execucao_direta = app.split('if __name__ == "__main__":', 1)[1]
        self.assertNotIn("0.0.0.0", bloco_execucao_direta)
        self.assertIn('host="127.0.0.1"', bloco_execucao_direta)
        self.assertIn('"homologation", "production"', bloco_execucao_direta)
        self.assertNotIn("debug=True", bloco_execucao_direta)


class TestTrustedHostsH2B1(unittest.TestCase):
    def test_11_online_exige_trusted_hosts(self):
        ambiente = ambiente_online()
        ambiente.pop("TRUSTED_HOSTS")
        with patch.dict(os.environ, ambiente, clear=True), self.assertRaisesRegex(
            RuntimeError, "TRUSTED_HOSTS"
        ):
            configurar_aplicacao(Flask(__name__))

    def test_12_online_exige_proxy_explicito(self):
        with patch.dict(
            os.environ,
            ambiente_online(TRUST_PROXY="false"),
            clear=True,
        ), self.assertRaisesRegex(RuntimeError, "TRUST_PROXY"):
            configurar_aplicacao(Flask(__name__))

    def test_13_testing_e_desenvolvimento_usam_apenas_hosts_locais_por_padrao(self):
        for ambiente in ("testing", "development"):
            with self.subTest(ambiente=ambiente), patch.dict(
                os.environ,
                {"APP_ENV": ambiente, "SECRET_KEY": SEGREDO_TESTE},
                clear=True,
            ):
                app = Flask(__name__)
                configurar_aplicacao(app)
            self.assertEqual(
                app.config["TRUSTED_HOSTS"],
                ["localhost", "127.0.0.1", "[::1]"],
            )

    def test_14_hosts_sao_normalizados_e_repetidos_sao_removidos(self):
        with patch.dict(
            os.environ,
            {"TRUSTED_HOSTS": " Exemplo.COM.,exemplo.com,api.exemplo.com "},
            clear=True,
        ):
            hosts = ler_hosts_confiaveis("development")
        self.assertEqual(hosts, ["exemplo.com", "api.exemplo.com"])

    def test_15_wildcard_esquema_caminho_porta_e_credencial_sao_rejeitados(self):
        casos = (
            "*.exemplo.com",
            ".exemplo.com",
            "https://exemplo.com",
            "exemplo.com/caminho",
            "usuario@exemplo.com",
            "exemplo.com:443",
            "exemplo interno.com",
            "exemplo.com\n",
            "exemplo.com\r\n",
            "exemplo.com,,api.exemplo.com",
            "exemplo.com..",
        )
        for valor in casos:
            with self.subTest(valor=valor), patch.dict(
                os.environ,
                {"TRUSTED_HOSTS": valor},
                clear=True,
            ), self.assertRaisesRegex(RuntimeError, "TRUSTED_HOSTS"):
                ler_hosts_confiaveis("development")

    def test_16_lista_vazia_e_rejeitada_online(self):
        for valor in ("", " ", ","):
            with self.subTest(valor=repr(valor)), patch.dict(
                os.environ,
                {"TRUSTED_HOSTS": valor},
                clear=True,
            ), self.assertRaisesRegex(RuntimeError, "TRUSTED_HOSTS"):
                ler_hosts_confiaveis("production")

    def test_17_lista_de_hosts_tem_limite(self):
        valor = ",".join(f"host{indice}.example" for indice in range(21))
        with patch.dict(
            os.environ,
            {"TRUSTED_HOSTS": valor},
            clear=True,
        ), self.assertRaisesRegex(RuntimeError, "hosts demais"):
            ler_hosts_confiaveis("development")

    def test_18_host_permitido_funciona_e_host_forjado_recebe_400(self):
        app = criar_app_online()
        cliente = app.test_client()
        permitido = cliente.get("/", base_url=f"https://{HOST_TESTE}")
        forjado = cliente.get("/", base_url="https://invasor.example")
        self.assertEqual(permitido.status_code, 200)
        self.assertEqual(forjado.status_code, 400)
        self.assertEqual(forjado.headers.get("X-Content-Type-Options"), "nosniff")

    def test_19_host_autorizado_com_porta_e_hosts_exatos(self):
        app = criar_app_online(
            TRUSTED_HOSTS="localhost,127.0.0.1,::1,app.exemplo.invalid"
        )
        cliente = app.test_client()
        casos_permitidos = (
            "http://localhost",
            "http://localhost:5000",
            "http://127.0.0.1",
            "http://127.0.0.1:5000",
            "http://app.exemplo.invalid",
            "http://app.exemplo.invalid:8080",
            "http://[::1]:5000",
        )
        for base_url in casos_permitidos:
            with self.subTest(base_url=base_url):
                self.assertEqual(cliente.get("/", base_url=base_url).status_code, 200)
        for base_url in (
            "http://desconhecido.invalid",
            "http://sub.app.exemplo.invalid",
        ):
            with self.subTest(base_url=base_url):
                self.assertEqual(cliente.get("/", base_url=base_url).status_code, 400)

    def test_20_ponto_final_e_maiusculas_sao_normalizados_sem_aceitar_malformado(self):
        with patch.dict(
            os.environ,
            {"TRUSTED_HOSTS": "LOCALHOST.,App.Exemplo.Invalid"},
            clear=True,
        ):
            self.assertEqual(
                ler_hosts_confiaveis("development"),
                ["localhost", "app.exemplo.invalid"],
            )

    def test_21_proxy_fix_online_confia_somente_origem_e_protocolo(self):
        app = criar_app_online()
        self.assertIsInstance(app.wsgi_app, ProxyFix)
        self.assertEqual(app.wsgi_app.x_for, 1)
        self.assertEqual(app.wsgi_app.x_proto, 1)
        self.assertEqual(app.wsgi_app.x_host, 0)
        self.assertEqual(app.wsgi_app.x_port, 0)
        self.assertEqual(app.wsgi_app.x_prefix, 0)

    def test_22_x_forwarded_host_nao_contorna_host_confiavel(self):
        app = criar_app_online()
        cliente = app.test_client()
        valido = cliente.get(
            "/",
            base_url=f"https://{HOST_TESTE}",
            headers={"X-Forwarded-Host": "invasor.invalid"},
        )
        invalido = cliente.get(
            "/",
            base_url="https://invasor.invalid",
            headers={"X-Forwarded-Host": HOST_TESTE},
        )
        self.assertEqual(valido.status_code, 200)
        self.assertEqual(valido.get_json()["host"], HOST_TESTE)
        self.assertEqual(invalido.status_code, 400)

    def test_23_proxy_desativado_e_multiplos_valores_encaminhados(self):
        with patch.dict(
            os.environ,
            {"APP_ENV": "testing", "SECRET_KEY": SEGREDO_TESTE},
            clear=True,
        ):
            sem_proxy = Flask(__name__)
            configurar_aplicacao(sem_proxy)
        self.assertNotIsInstance(sem_proxy.wsgi_app, ProxyFix)

        app = criar_app_online()
        resposta = app.test_client().get(
            "/",
            base_url=f"http://{HOST_TESTE}",
            headers={
                "X-Forwarded-For": "203.0.113.10, 10.0.0.20",
                "X-Forwarded-Proto": "http, https",
            },
        )
        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(resposta.get_json()["origem"], "10.0.0.20")
        self.assertEqual(resposta.get_json()["esquema"], "https")

    def test_24_limite_global_local_tem_padrao_seguro(self):
        for ambiente in ("development", "testing"):
            with self.subTest(ambiente=ambiente), patch.dict(
                os.environ,
                {},
                clear=True,
            ):
                self.assertEqual(ler_limite_requisicao_mb(ambiente), 64)

    def test_25_limite_global_e_obrigatorio_online(self):
        for ambiente in ("homologation", "production"):
            with self.subTest(ambiente=ambiente), patch.dict(
                os.environ,
                {},
                clear=True,
            ), self.assertRaisesRegex(RuntimeError, "MAX_REQUEST_MB"):
                ler_limite_requisicao_mb(ambiente)

    def test_26_limite_global_rejeita_valores_invalidos(self):
        for valor in ("texto", "1.5", "0", "-1", "129"):
            with self.subTest(valor=valor), patch.dict(
                os.environ,
                {"MAX_REQUEST_MB": valor},
                clear=True,
            ), self.assertRaisesRegex(RuntimeError, "MAX_REQUEST_MB"):
                ler_limite_requisicao_mb("testing")

    def test_27_limite_global_configura_flask_em_bytes(self):
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "testing",
                "SECRET_KEY": SEGREDO_TESTE,
                "MAX_REQUEST_MB": "32",
            },
            clear=True,
        ):
            app = Flask(__name__)
            configurar_aplicacao(app)
        self.assertEqual(app.config["MAX_REQUEST_MB"], 32)
        self.assertEqual(app.config["MAX_CONTENT_LENGTH"], 32 * 1024 * 1024)

    def test_28_conteudo_exatamente_no_limite_e_aceito(self):
        app, operacao_html, _operacao_json, _operacao_upload = criar_app_limites()
        resposta = app.test_client().post(
            "/receber-html",
            data=b"x" * (1024 * 1024),
            content_type="application/octet-stream",
        )
        self.assertEqual(resposta.status_code, 200)
        operacao_html.assert_called_once_with()

    def test_29_erro_413_html_e_amigavel_e_interrompe_operacao(self):
        app, operacao_html, _operacao_json, _operacao_upload = criar_app_limites()
        resposta = app.test_client().post(
            "/receber-html",
            data=b"x" * (1024 * 1024 + 1),
            content_type="application/octet-stream",
        )
        self.assertEqual(resposta.status_code, 413)
        self.assertFalse(resposta.is_json)
        self.assertIn("Conteúdo muito grande", resposta.get_data(as_text=True))
        self.assertNotIn("Traceback", resposta.get_data(as_text=True))
        operacao_html.assert_not_called()

    def test_30_erro_413_json_e_generico_sem_cache(self):
        app, _operacao_html, operacao_json, _operacao_upload = criar_app_limites()
        resposta = app.test_client().post(
            "/receber-json",
            data=b"x" * (1024 * 1024 + 1),
            content_type="application/json",
        )
        self.assertEqual(resposta.status_code, 413)
        self.assertTrue(resposta.is_json)
        self.assertEqual(resposta.get_json(), {"error": "Conteúdo muito grande."})
        self.assertIn("no-store", resposta.headers["Cache-Control"])
        self.assertEqual(resposta.headers["Pragma"], "no-cache")
        operacao_json.assert_not_called()

    def test_31_multipart_legitimo_e_aceito(self):
        app, _operacao_html, _operacao_json, operacao_upload = criar_app_limites()
        resposta = app.test_client().post(
            "/receber-upload",
            data={"arquivo": (BytesIO(b"x" * (512 * 1024)), "arquivo.pdf")},
            content_type="multipart/form-data",
        )
        self.assertEqual(resposta.status_code, 200)
        operacao_upload.assert_called_once_with()

    def test_32_multipart_excessivo_e_bloqueado_antes_da_operacao(self):
        app, _operacao_html, _operacao_json, operacao_upload = criar_app_limites()
        with patch(
            "cloudinary.uploader.upload",
            side_effect=AssertionError("Cloudinary não deveria ser chamado"),
        ) as upload:
            resposta = app.test_client().post(
                "/receber-upload",
                data={"arquivo": (BytesIO(b"x" * (1024 * 1024)), "arquivo.pdf")},
                content_type="multipart/form-data",
            )
        self.assertEqual(resposta.status_code, 413)
        self.assertFalse(resposta.is_json)
        operacao_upload.assert_not_called()
        upload.assert_not_called()

    def test_33_env_example_documenta_nomes_sem_segredos_reais(self):
        exemplo = (RAIZ / ".env.example").read_text(encoding="utf-8")
        for nome in (
            "TRUSTED_HOSTS",
            "PORT",
            "WEB_CONCURRENCY",
            "GUNICORN_THREADS",
            "GUNICORN_TIMEOUT",
            "GUNICORN_GRACEFUL_TIMEOUT",
            "MAX_REQUEST_MB",
        ):
            self.assertIn(f"{nome}=", exemplo)
        self.assertNotIn(HOST_TESTE, exemplo)
        self.assertNotIn(BANCO_TESTE, exemplo)
        self.assertIn("SECRET_KEY=\n", exemplo.replace("\r\n", "\n"))
        self.assertIn("DATABASE_URL=\n", exemplo.replace("\r\n", "\n"))

    def test_34_readme_nao_sugere_migration_automatica(self):
        readme = (RAIZ / "README.md").read_text(encoding="utf-8")
        self.assertIn("não executa migrations", readme)
        self.assertIn("TRUSTED_HOSTS", readme)

    def test_35_execucao_direta_e_bloqueada_online_sem_iniciar_servidor(self):
        ambiente = ambiente_online(
            TRUSTED_HOSTS="localhost",
            DATABASE_URL=BANCO_TESTE,
        )
        with (
            patch.dict(os.environ, ambiente, clear=True),
            patch("dotenv.load_dotenv", return_value=False),
            patch("flask.Flask.run") as executar,
            self.assertRaisesRegex(RuntimeError, "Gunicorn"),
        ):
            runpy.run_path(str(RAIZ / "app.py"), run_name="__main__")
        executar.assert_not_called()

    def test_36_execucao_direta_local_nao_expoe_rede_nem_ativa_debug(self):
        ambiente = {
            "APP_ENV": "development",
            "APP_DEBUG": "false",
            "SECRET_KEY": SEGREDO_TESTE,
            "MAX_REQUEST_MB": "64",
        }
        with (
            patch.dict(os.environ, ambiente, clear=True),
            patch("dotenv.load_dotenv", return_value=False),
            patch("flask.Flask.run") as executar,
        ):
            runpy.run_path(str(RAIZ / "app.py"), run_name="__main__")
        executar.assert_called_once_with(
            host="127.0.0.1",
            port=5000,
            debug=False,
        )


if __name__ == "__main__":
    unittest.main()
