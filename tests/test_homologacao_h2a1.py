"""Testes da inicialização protegida, sem serviços externos reais."""

import ast
import base64
import importlib
import io
import os
import sys
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from flask import Flask, abort, jsonify, request
from werkzeug.middleware.proxy_fix import ProxyFix

from configuracao_ambiente import configurar_aplicacao, identificar_ambiente
from modulos.fiscalizacao_contratos.services.cloudinary_storage import (
    CloudinaryStorage,
    CloudinaryStorageError,
    compor_caminho_cloudinary,
    normalizar_caminho_cloudinary,
)


RAIZ = Path(__file__).resolve().parents[1]
SEGREDO_TESTE = "segredo-exclusivamente-ficticio"
BANCO_TESTE = "postgresql://usuario-ficticio@host-ficticio/banco-ficticio"


def criar_app(ambiente="testing", **variaveis):
    ambiente_variaveis = {
        "APP_ENV": ambiente,
        "SECRET_KEY": SEGREDO_TESTE,
    }
    if ambiente in {"homologation", "production"}:
        ambiente_variaveis["DATABASE_URL"] = BANCO_TESTE
        ambiente_variaveis["TRUSTED_HOSTS"] = "localhost"
        ambiente_variaveis["TRUST_PROXY"] = "true"
        ambiente_variaveis["MAX_REQUEST_MB"] = "64"
        ambiente_variaveis["RATELIMIT_ENABLED"] = "true"
        ambiente_variaveis["RATELIMIT_STORAGE_URI"] = (
            "redis://rate-limit-ficticio.invalid/0"
            if ambiente == "production"
            else "memory://"
        )
        if ambiente == "homologation":
            ambiente_variaveis["RATELIMIT_ALLOW_MEMORY_HOMOLOGATION"] = "true"
    ambiente_variaveis.update(variaveis)

    with patch.dict(os.environ, ambiente_variaveis, clear=True):
        app = Flask(__name__)
        configurar_aplicacao(app)
        if ambiente == "production":
            # Estes testes antigos validam cookies e barreiras, não o backend Redis.
            app.extensions["recic3_rate_limiter"].enabled = False

    @app.get("/privado")
    def privado():
        return jsonify(ok=True)

    @app.get("/health")
    def health():
        return jsonify(status="ok")

    @app.get("/contexto")
    def contexto():
        return jsonify(esquema=request.scheme, host=request.host)

    @app.get("/erro")
    def erro():
        abort(418)

    return app


def cabecalho_basic(usuario, senha):
    credencial = base64.b64encode(f"{usuario}:{senha}".encode()).decode()
    return {"Authorization": f"Basic {credencial}"}


@contextmanager
def importar_app_isolado(**variaveis):
    ambiente = {
        "APP_ENV": "testing",
        "SECRET_KEY": SEGREDO_TESTE,
        "DATABASE_URL": "",
    }
    ambiente.update(variaveis)
    if ambiente.get("APP_ENV") in {"homologation", "production"}:
        ambiente.setdefault("TRUSTED_HOSTS", "localhost")
        ambiente.setdefault("TRUST_PROXY", "true")
        ambiente.setdefault("MAX_REQUEST_MB", "64")
        ambiente.setdefault("RATELIMIT_ENABLED", "true")
        ambiente.setdefault(
            "RATELIMIT_STORAGE_URI",
            (
                "redis://rate-limit-ficticio.invalid/0"
                if ambiente.get("APP_ENV") == "production"
                else "memory://"
            ),
        )
        if ambiente.get("APP_ENV") == "homologation":
            ambiente.setdefault("RATELIMIT_ALLOW_MEMORY_HOMOLOGATION", "true")
    app_anterior = sys.modules.pop("app", None)
    try:
        with (
            patch.dict(os.environ, ambiente, clear=True),
            patch("dotenv.load_dotenv", return_value=False),
            patch(
                "psycopg2.connect",
                side_effect=AssertionError("teste tentou acessar PostgreSQL"),
            ) as conectar,
            patch(
                "cloudinary.uploader.upload",
                side_effect=AssertionError("teste tentou enviar ao Cloudinary"),
            ),
            patch(
                "cloudinary.uploader.destroy",
                side_effect=AssertionError("teste tentou remover do Cloudinary"),
            ),
        ):
            modulo = importlib.import_module("app")
            yield modulo, conectar
    finally:
        sys.modules.pop("app", None)
        if app_anterior is not None:
            sys.modules["app"] = app_anterior


class TestConfiguracaoAmbiente(unittest.TestCase):
    def test_ambiente_ausente_assume_desenvolvimento_documentado(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(identificar_ambiente(), "development")

    def test_ambiente_e_normalizado(self):
        with patch.dict(os.environ, {"APP_ENV": "  HoMoLoGaTiOn  "}, clear=True):
            self.assertEqual(identificar_ambiente(), "homologation")

    def test_ambiente_desconhecido_e_rejeitado(self):
        with patch.dict(os.environ, {"APP_ENV": "outro"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "APP_ENV"):
                identificar_ambiente()

    def test_secret_key_ausente_e_rejeitada(self):
        with patch.dict(os.environ, {"APP_ENV": "development"}, clear=True):
            with self.assertRaisesRegex(RuntimeError, "SECRET_KEY"):
                configurar_aplicacao(Flask(__name__))

    def test_secret_key_com_apenas_espacos_e_rejeitada(self):
        with patch.dict(
            os.environ,
            {"APP_ENV": "testing", "SECRET_KEY": "   "},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "SECRET_KEY"):
                configurar_aplicacao(Flask(__name__))

    def test_producao_sem_secret_key_falha_sem_expor_valor(self):
        with patch.dict(
            os.environ,
            {"APP_ENV": "production", "DATABASE_URL": BANCO_TESTE},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "^SECRET_KEY") as erro:
                configurar_aplicacao(Flask(__name__))
        self.assertNotIn(BANCO_TESTE, str(erro.exception))

    def test_homologacao_sem_secret_key_falha(self):
        with patch.dict(
            os.environ,
            {"APP_ENV": "homologation", "DATABASE_URL": BANCO_TESTE},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "^SECRET_KEY"):
                configurar_aplicacao(Flask(__name__))

    def test_database_url_ausente_e_rejeitada_na_homologacao(self):
        with patch.dict(
            os.environ,
            {"APP_ENV": "homologation", "SECRET_KEY": SEGREDO_TESTE},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "DATABASE_URL"):
                configurar_aplicacao(Flask(__name__))

    def test_database_url_ausente_e_rejeitada_na_producao(self):
        with patch.dict(
            os.environ,
            {"APP_ENV": "production", "SECRET_KEY": SEGREDO_TESTE},
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "DATABASE_URL"):
                configurar_aplicacao(Flask(__name__))

    def test_database_url_com_apenas_espacos_e_rejeitada(self):
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "production",
                "SECRET_KEY": SEGREDO_TESTE,
                "DATABASE_URL": "   ",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "DATABASE_URL"):
                configurar_aplicacao(Flask(__name__))

    def test_testing_nao_exige_banco_e_e_explicito(self):
        app = criar_app("testing")
        self.assertTrue(app.config["TESTING"])
        self.assertIsNone(app.config["DATABASE_URL"])

    def test_cookies_de_desenvolvimento_permanecem_compativeis_com_http(self):
        app = criar_app("development")
        self.assertFalse(app.config["SESSION_COOKIE_SECURE"])
        self.assertTrue(app.config["SESSION_COOKIE_HTTPONLY"])
        self.assertEqual(app.config["SESSION_COOKIE_SAMESITE"], "Lax")
        self.assertFalse(app.config["REMEMBER_COOKIE_SECURE"])
        self.assertTrue(app.config["REMEMBER_COOKIE_HTTPONLY"])
        self.assertEqual(app.config["REMEMBER_COOKIE_SAMESITE"], "Lax")
        self.assertFalse(app.config["DEBUG"])

    def test_debug_local_so_e_ativado_explicitamente(self):
        app = criar_app("development", APP_DEBUG="true")
        self.assertTrue(app.config["DEBUG"])

    def test_flask_env_nao_ativa_debug(self):
        app = criar_app("development", FLASK_ENV="development")
        self.assertFalse(app.config["DEBUG"])

    def test_cookies_online_sao_seguros_e_debug_permanece_desligado(self):
        app = criar_app("homologation", APP_DEBUG="true")
        self.assertTrue(app.config["SESSION_COOKIE_SECURE"])
        self.assertEqual(app.config["PREFERRED_URL_SCHEME"], "https")
        self.assertFalse(app.config["DEBUG"])
        self.assertTrue(app.config["REMEMBER_COOKIE_SECURE"])
        self.assertTrue(app.config["REMEMBER_COOKIE_HTTPONLY"])
        self.assertEqual(app.config["REMEMBER_COOKIE_SAMESITE"], "Lax")

    def test_proxy_nao_e_confiado_por_padrao(self):
        app = criar_app("testing")
        self.assertNotIsInstance(app.wsgi_app, ProxyFix)

    def test_proxy_e_confiado_somente_quando_autorizado(self):
        app = criar_app("testing", TRUST_PROXY="true")
        self.assertIsInstance(app.wsgi_app, ProxyFix)
        self.assertEqual(app.wsgi_app.x_for, 1)
        self.assertEqual(app.wsgi_app.x_proto, 1)
        self.assertEqual(app.wsgi_app.x_host, 0)
        self.assertEqual(app.wsgi_app.x_port, 0)

    def test_proxy_desativado_ignora_cabecalhos_forjados(self):
        resposta = criar_app("testing", TRUST_PROXY="false").test_client().get(
            "/contexto",
            headers={
                "X-Forwarded-Proto": "https",
                "X-Forwarded-Host": "host-forjado.test",
            },
        )
        self.assertEqual(resposta.get_json()["esquema"], "http")
        self.assertNotEqual(resposta.get_json()["host"], "host-forjado.test")

    def test_proxy_ativado_reconhece_https_sem_confiar_em_host(self):
        resposta = criar_app("testing", TRUST_PROXY="true").test_client().get(
            "/contexto",
            headers={
                "X-Forwarded-Proto": "https",
                "X-Forwarded-Host": "host-forjado.test",
            },
        )
        self.assertEqual(resposta.get_json()["esquema"], "https")
        self.assertNotEqual(resposta.get_json()["host"], "host-forjado.test")

    def test_valor_booleano_ambiguo_e_rejeitado(self):
        with self.assertRaisesRegex(RuntimeError, "TRUST_PROXY"):
            criar_app("testing", TRUST_PROXY="talvez")

    def test_cabecalhos_minimos_sao_aplicados(self):
        resposta = criar_app("testing").test_client().get("/privado")
        self.assertEqual(resposta.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(resposta.headers["X-Frame-Options"], "DENY")
        self.assertEqual(
            resposta.headers["Referrer-Policy"],
            "strict-origin-when-cross-origin",
        )
        self.assertNotIn("Strict-Transport-Security", resposta.headers)

    def test_cabecalhos_minimos_tambem_aparecem_em_erros(self):
        resposta = criar_app("testing").test_client().get("/erro")
        self.assertEqual(resposta.status_code, 418)
        self.assertEqual(resposta.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(resposta.headers["X-Frame-Options"], "DENY")

    def test_hsts_e_enviado_em_https_online_sem_preload(self):
        resposta = criar_app("homologation").test_client().get(
            "/privado", base_url="https://exemplo.test"
        )
        self.assertEqual(
            resposta.headers["Strict-Transport-Security"], "max-age=86400"
        )
        self.assertNotIn("preload", resposta.headers["Strict-Transport-Security"])
        self.assertNotIn(
            "includeSubDomains", resposta.headers["Strict-Transport-Security"]
        )

    def test_hsts_funciona_atras_do_proxy_confiavel(self):
        resposta = criar_app(
            "homologation", TRUST_PROXY="true"
        ).test_client().get(
            "/privado", headers={"X-Forwarded-Proto": "https"}
        )
        self.assertEqual(
            resposta.headers["Strict-Transport-Security"], "max-age=86400"
        )

    def test_prefixo_cloudinary_e_exigido_online_quando_configurado(self):
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "homologation",
                "SECRET_KEY": SEGREDO_TESTE,
                "DATABASE_URL": BANCO_TESTE,
                "CLOUDINARY_CLOUD_NAME": "ficticio",
                "CLOUDINARY_API_KEY": "ficticia",
                "CLOUDINARY_API_SECRET": "ficticio",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "CLOUDINARY_FOLDER_PREFIX"):
                configurar_aplicacao(Flask(__name__))

    def test_cloudinary_parcial_e_rejeitado_online(self):
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "production",
                "SECRET_KEY": SEGREDO_TESTE,
                "DATABASE_URL": BANCO_TESTE,
                "CLOUDINARY_CLOUD_NAME": "ficticio",
                "CLOUDINARY_FOLDER_PREFIX": "producao/modulo",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "Cloudinary.*incompleta"):
                configurar_aplicacao(Flask(__name__))

    def test_prefixo_perigoso_e_rejeitado_na_inicializacao_online(self):
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "homologation",
                "SECRET_KEY": SEGREDO_TESTE,
                "DATABASE_URL": BANCO_TESTE,
                "CLOUDINARY_CLOUD_NAME": "ficticio",
                "CLOUDINARY_API_KEY": "ficticia",
                "CLOUDINARY_API_SECRET": "ficticio",
                "CLOUDINARY_FOLDER_PREFIX": "homologacao/../producao",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "CLOUDINARY_FOLDER_PREFIX"):
                configurar_aplicacao(Flask(__name__))


class TestBarreiraHomologacao(unittest.TestCase):
    def criar_app_com_barreira(self, ambiente="homologation", **extras):
        variaveis = {
            "HOMOLOGATION_GATE_ENABLED": "true",
            "HOMOLOGATION_GATE_USER": "homologador",
            "HOMOLOGATION_GATE_PASSWORD": "senha-de-teste",
        }
        variaveis.update(extras)
        return criar_app(ambiente, **variaveis)

    def test_sem_credencial_retorna_401(self):
        resposta = self.criar_app_com_barreira().test_client().get("/privado")
        self.assertEqual(resposta.status_code, 401)
        self.assertIn("Basic", resposta.headers["WWW-Authenticate"])
        self.assertIn("Acesso restrito", resposta.headers["WWW-Authenticate"])

    def test_cabecalho_malformado_retorna_401(self):
        resposta = self.criar_app_com_barreira().test_client().get(
            "/privado", headers={"Authorization": "Basic !!!"}
        )
        self.assertEqual(resposta.status_code, 401)

    def test_credenciais_vazias_retornam_401(self):
        resposta = self.criar_app_com_barreira().test_client().get(
            "/privado", headers=cabecalho_basic("", "")
        )
        self.assertEqual(resposta.status_code, 401)

    def test_senha_parcialmente_correta_retorna_401(self):
        resposta = self.criar_app_com_barreira().test_client().get(
            "/privado", headers=cabecalho_basic("homologador", "senha-de-test")
        )
        self.assertEqual(resposta.status_code, 401)

    def test_credencial_errada_retorna_401_sem_expor_dados(self):
        resposta = self.criar_app_com_barreira().test_client().get(
            "/privado", headers=cabecalho_basic("errado", "incorreta")
        )
        self.assertEqual(resposta.status_code, 401)
        conteudo = resposta.get_data(as_text=True)
        self.assertNotIn("homologador", conteudo)
        self.assertNotIn("senha-de-teste", conteudo)

    def test_credencial_correta_permite_continuar(self):
        resposta = self.criar_app_com_barreira().test_client().get(
            "/privado", headers=cabecalho_basic("homologador", "senha-de-teste")
        )
        self.assertEqual(resposta.status_code, 200)

    def test_health_e_excecao_da_barreira(self):
        resposta = self.criar_app_com_barreira().test_client().get("/health")
        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(resposta.get_json(), {"status": "ok"})

    def test_desenvolvimento_nao_ativa_barreira(self):
        resposta = self.criar_app_com_barreira("development").test_client().get(
            "/privado"
        )
        self.assertEqual(resposta.status_code, 200)

    def test_testing_nao_ativa_barreira(self):
        resposta = self.criar_app_com_barreira("testing").test_client().get(
            "/privado"
        )
        self.assertEqual(resposta.status_code, 200)

    def test_homologacao_com_gate_false_nao_ativa_barreira(self):
        resposta = criar_app(
            "homologation",
            HOMOLOGATION_GATE_ENABLED="false",
            HOMOLOGATION_GATE_USER="homologador",
            HOMOLOGATION_GATE_PASSWORD="senha-de-teste",
        ).test_client().get("/privado")
        self.assertEqual(resposta.status_code, 200)

    def test_producao_nao_ativa_barreira_de_homologacao(self):
        resposta = self.criar_app_com_barreira("production").test_client().get(
            "/privado"
        )
        self.assertEqual(resposta.status_code, 200)

    def test_barreira_ativa_exige_usuario_e_senha(self):
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "homologation",
                "SECRET_KEY": SEGREDO_TESTE,
                "DATABASE_URL": BANCO_TESTE,
                "TRUSTED_HOSTS": "localhost",
                "TRUST_PROXY": "true",
                "MAX_REQUEST_MB": "64",
                "RATELIMIT_ENABLED": "true",
                "RATELIMIT_STORAGE_URI": "memory://",
                "RATELIMIT_ALLOW_MEMORY_HOMOLOGATION": "true",
                "HOMOLOGATION_GATE_ENABLED": "true",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "HOMOLOGATION_GATE_USER"):
                configurar_aplicacao(Flask(__name__))

    def test_barreira_ativa_exige_senha(self):
        with patch.dict(
            os.environ,
            {
                "APP_ENV": "homologation",
                "SECRET_KEY": SEGREDO_TESTE,
                "DATABASE_URL": BANCO_TESTE,
                "TRUSTED_HOSTS": "localhost",
                "TRUST_PROXY": "true",
                "MAX_REQUEST_MB": "64",
                "RATELIMIT_ENABLED": "true",
                "RATELIMIT_STORAGE_URI": "memory://",
                "RATELIMIT_ALLOW_MEMORY_HOMOLOGATION": "true",
                "HOMOLOGATION_GATE_ENABLED": "true",
                "HOMOLOGATION_GATE_USER": "homologador",
                "HOMOLOGATION_GATE_PASSWORD": "   ",
            },
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "HOMOLOGATION_GATE_PASSWORD"):
                configurar_aplicacao(Flask(__name__))


class TestCloudinaryPrefixo(unittest.TestCase):
    CREDENCIAIS = {
        "CLOUDINARY_CLOUD_NAME": "cloud-ficticio",
        "CLOUDINARY_API_KEY": "chave-ficticia",
        "CLOUDINARY_API_SECRET": "segredo-ficticio",
    }

    def test_prefixo_simples(self):
        self.assertEqual(
            normalizar_caminho_cloudinary("homologacao/fiscalizacao-contratos"),
            "homologacao/fiscalizacao-contratos",
        )

    def test_barras_das_extremidades_e_duplas_sao_removidas(self):
        self.assertEqual(
            normalizar_caminho_cloudinary("//homologacao//documentos//"),
            "homologacao/documentos",
        )

    def test_prefixo_e_subpasta_sao_compostos(self):
        self.assertEqual(
            compor_caminho_cloudinary("homologacao/modulo", "contratos/10"),
            "homologacao/modulo/contratos/10",
        )

    def test_segmento_perigoso_e_rejeitado(self):
        with self.assertRaises(CloudinaryStorageError):
            normalizar_caminho_cloudinary("homologacao/../producao")

    def test_segmento_ponto_e_rejeitado(self):
        with self.assertRaises(CloudinaryStorageError):
            normalizar_caminho_cloudinary("homologacao/./documentos")

    def test_barra_invertida_e_rejeitada(self):
        with self.assertRaises(CloudinaryStorageError):
            normalizar_caminho_cloudinary("homologacao\\documentos")

    def test_prefixo_ja_presente_nao_e_duplicado(self):
        self.assertEqual(
            compor_caminho_cloudinary(
                "homologacao/modulo", "homologacao/modulo/contratos/10"
            ),
            "homologacao/modulo/contratos/10",
        )

    def test_prefixo_ausente_e_rejeitado_na_homologacao(self):
        ambiente = {"APP_ENV": "homologation", **self.CREDENCIAIS}
        with patch.dict(os.environ, ambiente, clear=True):
            with self.assertRaises(CloudinaryStorageError):
                CloudinaryStorage()

    def test_prefixo_ausente_e_rejeitado_na_producao(self):
        ambiente = {"APP_ENV": "production", **self.CREDENCIAIS}
        with patch.dict(os.environ, ambiente, clear=True):
            with self.assertRaises(CloudinaryStorageError):
                CloudinaryStorage()

    def test_configuracao_parcial_do_storage_falha_sem_expor_valor(self):
        ambiente = {
            "APP_ENV": "production",
            "CLOUDINARY_CLOUD_NAME": "valor-que-nao-deve-aparecer",
            "CLOUDINARY_FOLDER_PREFIX": "producao/modulo",
        }
        with patch.dict(os.environ, ambiente, clear=True):
            with self.assertRaises(CloudinaryStorageError) as erro:
                CloudinaryStorage()
        self.assertNotIn("valor-que-nao-deve-aparecer", str(erro.exception))

    def test_upload_novo_usa_prefixo_e_subpastas_sem_duplicacao(self):
        ambiente = {
            "APP_ENV": "testing",
            "CLOUDINARY_FOLDER_PREFIX": "teste/modulo",
            **self.CREDENCIAIS,
        }
        with (
            patch.dict(os.environ, ambiente, clear=True),
            patch(
                "cloudinary.uploader.upload",
                side_effect=lambda _arquivo, **opcoes: {
                    "public_id": opcoes["public_id"],
                    "version": 1,
                },
            ) as upload,
        ):
            CloudinaryStorage().enviar(
                {"conteudo": b"arquivo-ficticio", "extensao": "pdf"},
                10,
                20,
            )

        public_id = upload.call_args.kwargs["public_id"]
        self.assertTrue(public_id.startswith("teste/modulo/contratos/10/aditivos/20/"))
        self.assertEqual(public_id.count("teste/modulo"), 1)

    def test_remocao_compensatoria_usa_public_id_retornado(self):
        ambiente = {
            "APP_ENV": "testing",
            "CLOUDINARY_FOLDER_PREFIX": "teste/modulo",
            **self.CREDENCIAIS,
        }
        with (
            patch.dict(os.environ, ambiente, clear=True),
            patch(
                "cloudinary.uploader.upload",
                return_value={"public_id": "teste/modulo/chave-real.pdf", "version": 1},
            ),
            patch("cloudinary.uploader.destroy") as remover,
        ):
            armazenamento = CloudinaryStorage()
            enviado = armazenamento.enviar(
                {"conteudo": b"arquivo-ficticio", "extensao": "pdf"}, 10
            )
            armazenamento.remover(enviado["armazenamento_chave"])

        remover.assert_called_once_with(
            "teste/modulo/chave-real.pdf",
            resource_type="raw",
            type="authenticated",
            invalidate=True,
        )


class TestIntegracaoInicializacao(unittest.TestCase):
    def test_app_nao_chama_rotinas_de_migracao_no_nivel_do_modulo(self):
        arvore = ast.parse((RAIZ / "app.py").read_text(encoding="utf-8"))
        nomes_migracao = {
            "migrar_dados_antigos_produtos",
            "criar_tabelas_se_nao_existir",
        }
        chamadas = [
            no
            for no in arvore.body
            if isinstance(no, ast.Expr)
            and isinstance(no.value, ast.Call)
            and isinstance(no.value.func, ast.Name)
            and no.value.func.id in nomes_migracao
        ]
        self.assertEqual(chamadas, [])

    def test_health_real_nao_acessa_banco_ou_cloudinary(self):
        with importar_app_isolado() as (app_module, conectar):
            conectar.assert_not_called()
            with (
                patch.object(
                    app_module,
                    "conectar_banco",
                    side_effect=AssertionError("health tentou acessar o banco"),
                ),
                patch(
                    "cloudinary.uploader.upload",
                    side_effect=AssertionError("health tentou acessar Cloudinary"),
                ),
            ):
                resposta = app_module.app.test_client().get("/health")

        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(resposta.content_type, "application/json")
        self.assertEqual(resposta.get_json(), {"status": "ok"})

    def test_health_aceita_somente_get(self):
        with importar_app_isolado() as (app_module, conectar):
            cliente = app_module.app.test_client()
            self.assertEqual(cliente.get("/health").status_code, 200)
            for metodo in ("post", "put", "delete"):
                self.assertEqual(getattr(cliente, metodo)("/health").status_code, 405)
            conectar.assert_not_called()

    def test_basic_correta_nao_substitui_login_interno(self):
        with importar_app_isolado(
            APP_ENV="homologation",
            DATABASE_URL=BANCO_TESTE,
            HOMOLOGATION_GATE_ENABLED="true",
            HOMOLOGATION_GATE_USER="homologador",
            HOMOLOGATION_GATE_PASSWORD="senha-de-teste",
        ) as (app_module, conectar):
            resposta = app_module.app.test_client().get(
                "/fiscalizacao-contratos",
                headers=cabecalho_basic("homologador", "senha-de-teste"),
            )
            conectar.assert_not_called()

        self.assertEqual(resposta.status_code, 302)
        self.assertIn("/login", resposta.headers["Location"])

    def test_importar_script_administrativo_nao_importa_app_nem_acessa_banco(self):
        script_anterior = sys.modules.pop("executar_migracao_produtos", None)
        app_anterior = sys.modules.get("app")
        try:
            with patch("psycopg2.connect") as conectar:
                importlib.import_module("executar_migracao_produtos")
            conectar.assert_not_called()
            self.assertIs(sys.modules.get("app"), app_anterior)
        finally:
            sys.modules.pop("executar_migracao_produtos", None)
            if script_anterior is not None:
                sys.modules["executar_migracao_produtos"] = script_anterior

    def test_script_sem_confirmacao_nao_executa_migracao(self):
        import executar_migracao_produtos as script

        migrar = MagicMock()
        app_falso = SimpleNamespace(migrar_dados_antigos_produtos=migrar)
        saida_segura = io.StringIO()
        with (
            patch.object(sys, "argv", ["executar_migracao_produtos.py"]),
            patch.object(sys, "stderr", saida_segura),
            patch.dict(sys.modules, {"app": app_falso}),
            self.assertRaises(SystemExit),
        ):
            script.main()
        migrar.assert_not_called()
        self.assertIn("nenhuma migração foi executada", saida_segura.getvalue())

    def test_procfile_nao_executa_script_administrativo(self):
        procfile = (RAIZ / "Procfile").read_text(encoding="utf-8").strip()
        self.assertEqual(procfile, "web: gunicorn --config gunicorn.conf.py app:app")
        self.assertNotIn("executar_migracao_produtos", procfile)


if __name__ == "__main__":
    unittest.main()
