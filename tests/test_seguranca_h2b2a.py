"""Testes da CSP, cabeçalhos e rate limiting sem serviços externos reais."""

import base64
import os
import re
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from pathlib import Path
from unittest.mock import MagicMock, patch

from flask import Flask, abort, jsonify, render_template_string, request
from flask_login import LoginManager, UserMixin

from configuracao_ambiente import configurar_aplicacao
from seguranca_csrf import configurar_csrf
from seguranca_rate_limit import aplicar_limites_rotas


RAIZ = Path(__file__).resolve().parents[1]
SEGREDO_TESTE = "segredo-ficticio-h2b2a"
BANCO_TESTE = "postgresql://usuario-ficticio@host-ficticio/banco-ficticio"
HOST_TESTE = "homologacao.exemplo.invalid"
PADRAO_NONCE = re.compile(r"<script nonce=\"([A-Za-z0-9_-]+)\">")
PADRAO_TOKEN_CSRF = re.compile(
    rb'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)',
    re.IGNORECASE,
)


def ambiente(ambiente_app="testing", **extras):
    valores = {
        "APP_ENV": ambiente_app,
        "SECRET_KEY": SEGREDO_TESTE,
        "RATELIMIT_ENABLED": "true",
        "RATELIMIT_STORAGE_URI": "memory://",
        "RATELIMIT_DEFAULT": "300 per minute",
        "RATELIMIT_LOGIN": "2 per minute",
        "RATELIMIT_REPORTS": "2 per minute",
        "RATELIMIT_EXTERNAL_LOOKUPS": "2 per minute",
        "RATELIMIT_UPLOADS": "2 per minute",
        "RATELIMIT_MUTATIONS": "2 per minute",
        "RATELIMIT_DOWNLOADS": "2 per minute",
    }
    if ambiente_app in {"homologation", "production"}:
        valores.update(
            {
                "DATABASE_URL": BANCO_TESTE,
                "TRUSTED_HOSTS": HOST_TESTE,
                "TRUST_PROXY": "true",
                "MAX_REQUEST_MB": "64",
                "RATELIMIT_ENABLED": "true",
                "RATELIMIT_STORAGE_URI": (
                    "redis://rate-limit-ficticio.invalid/0"
                    if ambiente_app == "production"
                    else "memory://"
                ),
            }
        )
        if ambiente_app == "homologation":
            valores["RATELIMIT_ALLOW_MEMORY_HOMOLOGATION"] = "true"
    valores.update(extras)
    return valores


def criar_app_minimo(ambiente_app="testing", **extras):
    operacoes = {
        nome: MagicMock(name=nome)
        for nome in (
            "login",
            "senha",
            "json",
            "relatorio",
            "upload",
            "cep",
            "cnpj",
            "mutacao",
        )
    }
    with patch.dict(os.environ, ambiente(ambiente_app, **extras), clear=True):
        app = Flask(
            __name__,
            template_folder=str(RAIZ / "templates"),
            static_folder=str(RAIZ / "static"),
        )
        configurar_aplicacao(app)

    @app.get("/")
    def index():
        return render_template_string(
            '<h1>Página</h1><script nonce="{{ csp_nonce() }}">'
            "window.testeSeguro = true;</script>"
        )

    @app.get("/erro")
    def erro():
        abort(418)

    @app.get("/health")
    def health():
        return jsonify(status="ok")

    def login_view():
        operacoes["login"](request.form.get("username"))
        operacoes["senha"]()
        return "credenciais inválidas", 401

    app.add_url_rule("/login", "login", login_view, methods=["POST"])

    def json_view():
        operacoes["json"]()
        return jsonify(ok=True)

    app.add_url_rule("/json", "responder_solicitacao", json_view, methods=["POST"])

    def relatorio_view():
        operacoes["relatorio"]()
        return "pdf"

    app.add_url_rule(
        "/relatorio",
        "baixar_pdf_relatorio_financeiro",
        relatorio_view,
        methods=["POST"],
    )

    def upload_view():
        operacoes["upload"]()
        return "upload"

    app.add_url_rule(
        "/upload",
        "fiscalizacao_contratos.documentos_novo",
        upload_view,
        methods=["POST"],
    )

    def cep_view():
        operacoes["cep"]()
        return jsonify(ok=True)

    app.add_url_rule("/cep", "buscar_cep", cep_view)

    def cnpj_view():
        operacoes["cnpj"]()
        return jsonify(ok=True)

    app.add_url_rule("/cnpj", "buscar_cnpj", cnpj_view)

    def mutacao_view():
        operacoes["mutacao"]()
        return jsonify(ok=True)

    app.add_url_rule(
        "/mutacao",
        "api_produtos_crud",
        mutacao_view,
        methods=["POST"],
    )

    app.config["JSON_ENDPOINT_CLASSIFIER"] = lambda: request.endpoint in {
        "responder_solicitacao",
        "buscar_cep",
        "buscar_cnpj",
        "api_produtos_crud",
        "health",
    }
    aplicar_limites_rotas(app)
    return app, operacoes


def postagens(cliente, caminho, quantidade, **opcoes):
    return [cliente.post(caminho, **opcoes) for _ in range(quantidade)]


class TestCspECabecalhosH2B2A(unittest.TestCase):
    def test_01_csp_ativa_possui_diretivas_seguras(self):
        app, _ = criar_app_minimo()
        csp = app.test_client().get("/").headers["Content-Security-Policy"]
        for diretiva in (
            "default-src 'self'",
            "base-uri 'self'",
            "object-src 'none'",
            "frame-ancestors 'none'",
            "form-action 'self'",
            "connect-src 'self'",
            "frame-src 'none'",
        ):
            self.assertIn(diretiva, csp)
        self.assertNotIn("unsafe-eval", csp)
        self.assertNotRegex(csp, r"(^|[\s;])\*($|[\s;])")
        self.assertNotIn("http:", csp)

    def test_02_script_src_nao_libera_script_inline_globalmente(self):
        app, _ = criar_app_minimo()
        csp = app.test_client().get("/").headers["Content-Security-Policy"]
        script_src = csp.split("script-src ", 1)[1].split(";", 1)[0]
        self.assertNotIn("unsafe-inline", script_src)
        self.assertIn("nonce-", script_src)

    def test_03_nonce_muda_a_cada_resposta_e_corresponde_ao_template(self):
        app, _ = criar_app_minimo()
        cliente = app.test_client()
        respostas = [cliente.get("/") for _ in range(2)]
        nonces = []
        for resposta in respostas:
            nonce = PADRAO_NONCE.search(resposta.get_data(as_text=True)).group(1)
            self.assertIn(f"'nonce-{nonce}'", resposta.headers["Content-Security-Policy"])
            nonces.append(nonce)
        self.assertNotEqual(nonces[0], nonces[1])

    def test_04_json_nao_expoe_nonce_no_corpo_ou_na_csp(self):
        app, _ = criar_app_minimo()
        resposta = app.test_client().get("/health")
        self.assertNotIn("nonce-", resposta.get_data(as_text=True))
        self.assertNotIn("nonce-", resposta.headers["Content-Security-Policy"])

    def test_05_resposta_de_erro_html_recebe_csp(self):
        app, _ = criar_app_minimo()
        resposta = app.test_client().get("/erro")
        self.assertEqual(resposta.status_code, 418)
        self.assertIn("Content-Security-Policy", resposta.headers)

    def test_06_recursos_externos_sao_exatos_e_cloudinary_nao_vai_para_connect(self):
        app, _ = criar_app_minimo()
        csp = app.test_client().get("/").headers["Content-Security-Policy"]
        self.assertIn("https://cdn.jsdelivr.net", csp)
        self.assertIn("https://cdnjs.cloudflare.com", csp)
        self.assertIn("img-src 'self' data: https://res.cloudinary.com", csp)
        connect = csp.split("connect-src ", 1)[1].split(";", 1)[0]
        self.assertEqual(connect, "'self'")

    def test_07_cabecalhos_adicionais_estao_presentes(self):
        app, _ = criar_app_minimo()
        resposta = app.test_client().get("/")
        self.assertEqual(resposta.headers["X-Content-Type-Options"], "nosniff")
        self.assertEqual(resposta.headers["X-Frame-Options"], "DENY")
        self.assertEqual(
            resposta.headers["Referrer-Policy"],
            "strict-origin-when-cross-origin",
        )
        self.assertEqual(resposta.headers["Cross-Origin-Opener-Policy"], "same-origin")
        self.assertEqual(resposta.headers["Cross-Origin-Resource-Policy"], "same-origin")
        self.assertEqual(resposta.headers["Origin-Agent-Cluster"], "?1")
        self.assertIn("camera=(self)", resposta.headers["Permissions-Policy"])
        self.assertIn("payment=()", resposta.headers["Permissions-Policy"])

    def test_08_hsts_nao_e_enviado_em_http_local(self):
        app, _ = criar_app_minimo()
        self.assertNotIn(
            "Strict-Transport-Security",
            app.test_client().get("/").headers,
        )

    def test_09_hsts_online_somente_em_https_e_e_conservador(self):
        app, _ = criar_app_minimo("homologation")
        cliente = app.test_client()
        http = cliente.get("/", base_url=f"http://{HOST_TESTE}")
        https = cliente.get("/", base_url=f"https://{HOST_TESTE}")
        self.assertNotIn("Strict-Transport-Security", http.headers)
        self.assertEqual(
            https.headers["Strict-Transport-Security"],
            "max-age=86400",
        )
        self.assertNotIn("includeSubDomains", https.headers["Strict-Transport-Security"])
        self.assertNotIn("preload", https.headers["Strict-Transport-Security"])

    def test_10_producao_https_envia_hsts_sem_consultar_storage(self):
        app, _ = criar_app_minimo("production")
        resposta = app.test_client().get(
            "/health",
            base_url=f"https://{HOST_TESTE}",
        )
        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(
            resposta.headers["Strict-Transport-Security"],
            "max-age=86400",
        )

    def test_11_report_only_e_opcional_na_homologacao(self):
        app, _ = criar_app_minimo("homologation", CSP_REPORT_ONLY="true")
        resposta = app.test_client().get(
            "/health",
            base_url=f"https://{HOST_TESTE}",
        )
        self.assertIn("Content-Security-Policy-Report-Only", resposta.headers)
        self.assertNotIn("Content-Security-Policy", resposta.headers)

    def test_12_report_only_e_rejeitado_em_producao(self):
        with self.assertRaisesRegex(RuntimeError, "CSP_REPORT_ONLY"):
            criar_app_minimo("production", CSP_REPORT_ONLY="true")

    def test_13_arquivos_estaticos_continuam_acessiveis(self):
        app, _ = criar_app_minimo(RATELIMIT_DEFAULT="1 per minute")
        cliente = app.test_client()
        for _ in range(3):
            resposta = cliente.get("/static/css/autenticacao.css")
            self.assertEqual(resposta.status_code, 200)
            self.assertIn("text/css", resposta.content_type)

    def test_14_scripts_inline_restantes_possuem_nonce(self):
        arquivos = list((RAIZ / "templates").rglob("*.html"))
        arquivos += list(
            (RAIZ / "modulos" / "fiscalizacao_contratos" / "templates").rglob("*.html")
        )
        sem_nonce = []
        for arquivo in arquivos:
            texto = arquivo.read_text(encoding="utf-8")
            for tag in re.findall(r"<script\b[^>]*>", texto, re.IGNORECASE):
                if " src=" not in tag.casefold() and "nonce=" not in tag.casefold():
                    sem_nonce.append((arquivo.name, tag))
        self.assertEqual(sem_nonce, [])


class TestConfiguracaoRateLimitH2B2A(unittest.TestCase):
    def test_15_rate_limit_ausente_falha_online(self):
        valores = ambiente("homologation")
        valores.pop("RATELIMIT_ENABLED")
        with patch.dict(os.environ, valores, clear=True):
            with self.assertRaisesRegex(RuntimeError, "RATELIMIT_ENABLED"):
                configurar_aplicacao(Flask(__name__))

    def test_16_storage_ausente_falha_online(self):
        valores = ambiente("homologation")
        valores.pop("RATELIMIT_STORAGE_URI")
        with patch.dict(os.environ, valores, clear=True):
            with self.assertRaisesRegex(RuntimeError, "RATELIMIT_STORAGE_URI"):
                configurar_aplicacao(Flask(__name__))

    def test_17_rate_limit_nao_pode_ser_desativado_online(self):
        with self.assertRaisesRegex(RuntimeError, "não pode ser false"):
            criar_app_minimo("homologation", RATELIMIT_ENABLED="false")

    def test_18_producao_recusa_memoria_por_worker(self):
        with self.assertRaisesRegex(RuntimeError, "armazenamento compartilhado"):
            criar_app_minimo("production", RATELIMIT_STORAGE_URI="memory://")

    def test_19_homologacao_exige_decisao_mas_aceita_memoria_explicita(self):
        app, _ = criar_app_minimo(
            "homologation",
            RATELIMIT_STORAGE_URI="memory://",
            RATELIMIT_ALLOW_MEMORY_HOMOLOGATION="true",
        )
        self.assertTrue(app.config["RATELIMIT_STORAGE_IS_MEMORY"])

    def test_20_storage_postgresql_e_rejeitado(self):
        with self.assertRaisesRegex(RuntimeError, "memory.*redis"):
            criar_app_minimo(
                RATELIMIT_STORAGE_URI="postgresql://nao-usar.invalid/banco"
            )

    def test_21_limite_malformado_e_rejeitado(self):
        with self.assertRaisesRegex(RuntimeError, "RATELIMIT_LOGIN"):
            criar_app_minimo(RATELIMIT_LOGIN="5/minute; segundo")

    def test_22_testing_usa_memoria_e_nova_app_reinicia_contadores(self):
        app1, _ = criar_app_minimo(RATELIMIT_LOGIN="1 per minute")
        self.assertEqual(app1.test_client().post("/login").status_code, 401)
        self.assertEqual(app1.test_client().post("/login").status_code, 429)
        app2, _ = criar_app_minimo(RATELIMIT_LOGIN="1 per minute")
        self.assertEqual(app2.test_client().post("/login").status_code, 401)


class TestComportamentoRateLimitH2B2A(unittest.TestCase):
    def test_23_login_dentro_e_acima_do_limite(self):
        app, operacoes = criar_app_minimo()
        respostas = postagens(
            app.test_client(),
            "/login",
            3,
            data={"username": "qualquer", "password": "nao-registrar"},
        )
        self.assertEqual([r.status_code for r in respostas], [401, 401, 429])
        self.assertEqual(operacoes["login"].call_count, 2)

    def test_24_login_nao_separa_chave_por_usuario_informado(self):
        app, operacoes = criar_app_minimo()
        cliente = app.test_client()
        self.assertEqual(
            cliente.post("/login", data={"username": "existente"}).status_code,
            401,
        )
        self.assertEqual(
            cliente.post("/login", data={"username": "inexistente"}).status_code,
            401,
        )
        self.assertEqual(
            cliente.post("/login", data={"username": "inativo"}).status_code,
            429,
        )
        self.assertEqual(operacoes["login"].call_count, 2)

    def test_25_html_429_e_amigavel_sem_executar_operacao(self):
        app, operacoes = criar_app_minimo()
        resposta = postagens(app.test_client(), "/login", 3)[-1]
        self.assertEqual(resposta.status_code, 429)
        self.assertIn("text/html", resposta.content_type)
        self.assertIn("Muitas solicitações", resposta.get_data(as_text=True))
        self.assertNotIn("Traceback", resposta.get_data(as_text=True))
        self.assertEqual(operacoes["login"].call_count, 2)

    def test_26_json_429_e_generico_sem_cache(self):
        app, operacoes = criar_app_minimo()
        resposta = postagens(app.test_client(), "/json", 3, json={"x": 1})[-1]
        self.assertEqual(resposta.status_code, 429)
        self.assertTrue(resposta.is_json)
        self.assertEqual(
            resposta.get_json(),
            {"error": "Muitas solicitações. Aguarde e tente novamente."},
        )
        self.assertIn("no-store", resposta.headers["Cache-Control"])
        self.assertEqual(resposta.headers["Pragma"], "no-cache")
        self.assertEqual(operacoes["json"].call_count, 2)

    def test_27_retry_after_e_informado(self):
        app, _ = criar_app_minimo(RATELIMIT_LOGIN="1 per minute")
        resposta = postagens(app.test_client(), "/login", 2)[-1]
        self.assertRegex(resposta.headers["Retry-After"], r"^[1-9][0-9]*$")

    def test_28_ips_distintos_possuem_limites_independentes(self):
        app, operacoes = criar_app_minimo(RATELIMIT_LOGIN="1 per minute")
        cliente = app.test_client()
        a = cliente.post("/login", environ_base={"REMOTE_ADDR": "192.0.2.10"})
        b = cliente.post("/login", environ_base={"REMOTE_ADDR": "192.0.2.11"})
        bloqueado = cliente.post(
            "/login",
            environ_base={"REMOTE_ADDR": "192.0.2.10"},
        )
        self.assertEqual([a.status_code, b.status_code, bloqueado.status_code], [401, 401, 429])
        self.assertEqual(operacoes["login"].call_count, 2)

    def test_29_basic_auth_nao_contorna_limite(self):
        app, operacoes = criar_app_minimo(RATELIMIT_LOGIN="1 per minute")
        cliente = app.test_client()
        headers_a = {
            "Authorization": "Basic "
            + base64.b64encode(b"porteiro:a").decode()
        }
        headers_b = {
            "Authorization": "Basic "
            + base64.b64encode(b"porteiro:b").decode()
        }
        self.assertEqual(cliente.post("/login", headers=headers_a).status_code, 401)
        self.assertEqual(cliente.post("/login", headers=headers_b).status_code, 429)
        self.assertEqual(operacoes["login"].call_count, 1)

    def test_30_consultas_cep_e_cnpj_sao_limitadas(self):
        app, operacoes = criar_app_minimo()
        cliente = app.test_client()
        respostas = [
            cliente.get("/cep"),
            cliente.get("/cnpj"),
            cliente.get("/cep"),
        ]
        self.assertEqual([r.status_code for r in respostas], [200, 200, 429])
        operacoes["cep"].assert_called_once()
        operacoes["cnpj"].assert_called_once()

    def test_31_relatorio_e_upload_sao_limitados_antes_da_operacao(self):
        app, operacoes = criar_app_minimo()
        cliente = app.test_client()
        for caminho, nome in (("/relatorio", "relatorio"), ("/upload", "upload")):
            with self.subTest(caminho=caminho):
                respostas = postagens(cliente, caminho, 3)
                self.assertEqual([r.status_code for r in respostas], [200, 200, 429])
                self.assertEqual(operacoes[nome].call_count, 2)

    def test_32_endpoint_json_mutavel_e_limitado(self):
        app, operacoes = criar_app_minimo()
        respostas = postagens(app.test_client(), "/mutacao", 3, json={"ok": True})
        self.assertEqual([r.status_code for r in respostas], [200, 200, 429])
        self.assertEqual(operacoes["mutacao"].call_count, 2)

    def test_33_health_nao_e_limitado(self):
        app, _ = criar_app_minimo(RATELIMIT_DEFAULT="1 per minute")
        cliente = app.test_client()
        self.assertEqual(
            [cliente.get("/health").status_code for _ in range(4)],
            [200, 200, 200, 200],
        )

    def test_34_janela_curta_se_recupera_sem_bloqueio_permanente(self):
        app, operacoes = criar_app_minimo(RATELIMIT_LOGIN="1 per second")
        cliente = app.test_client()
        self.assertEqual(cliente.post("/login").status_code, 401)
        self.assertEqual(cliente.post("/login").status_code, 429)
        time.sleep(1.05)
        self.assertEqual(cliente.post("/login").status_code, 401)
        self.assertEqual(operacoes["login"].call_count, 2)

    def test_35_proxy_confia_somente_no_ultimo_valor_encaminhado(self):
        app, operacoes = criar_app_minimo(
            "homologation",
            RATELIMIT_LOGIN="1 per minute",
        )
        cliente = app.test_client()
        base_url = f"https://{HOST_TESTE}"
        primeiro = cliente.post(
            "/login",
            base_url=base_url,
            headers={"X-Forwarded-For": "192.0.2.1, 198.51.100.9"},
        )
        segundo = cliente.post(
            "/login",
            base_url=base_url,
            headers={"X-Forwarded-For": "192.0.2.2, 198.51.100.9"},
        )
        self.assertEqual([primeiro.status_code, segundo.status_code], [401, 429])
        self.assertEqual(operacoes["login"].call_count, 1)

    def test_36_csrf_e_rate_limit_permanecem_independentes(self):
        with patch.dict(
            os.environ,
            ambiente(RATELIMIT_MUTATIONS="1 per minute"),
            clear=True,
        ):
            app = Flask(
                __name__,
                template_folder=str(RAIZ / "templates"),
                static_folder=str(RAIZ / "static"),
            )
            configurar_aplicacao(app)
            configurar_csrf(app)

        operacao = MagicMock()

        @app.get("/")
        def index():
            return render_template_string(
                '<form><input name="csrf_token" value="{{ csrf_token() }}"></form>'
            )

        @app.post("/escrita")
        def escrita():
            operacao()
            return "ok"

        aplicar_limites_rotas(app)
        cliente = app.test_client()
        sem_csrf = cliente.post("/escrita")
        app.extensions["recic3_rate_limiter"].reset()
        token = PADRAO_TOKEN_CSRF.search(cliente.get("/").data).group(1).decode()
        com_csrf = cliente.post("/escrita", data={"csrf_token": token})
        excedido = cliente.post("/escrita", data={"csrf_token": token})
        self.assertEqual(sem_csrf.status_code, 400)
        self.assertEqual(com_csrf.status_code, 200)
        self.assertEqual(excedido.status_code, 429)
        operacao.assert_called_once()

    def test_37_usuario_autenticado_e_admin_usam_chave_interna_sem_dados_do_formulario(self):
        app, operacoes = criar_app_minimo(RATELIMIT_MUTATIONS="1 per minute")
        login_manager = LoginManager(app)

        class Usuario(UserMixin):
            id = "7"
            role = "admin"

        @login_manager.user_loader
        def carregar(_user_id):
            return Usuario()

        cliente = app.test_client()
        with cliente.session_transaction() as sessao:
            sessao["_user_id"] = "7"
            sessao["_fresh"] = True
        self.assertEqual(cliente.post("/mutacao", json={"senha": "nao-usar"}).status_code, 200)
        self.assertEqual(cliente.post("/mutacao", json={"senha": "outra"}).status_code, 429)
        self.assertEqual(operacoes["mutacao"].call_count, 1)

    def test_38_mapeamento_cobre_grupos_sensiveis(self):
        app, _ = criar_app_minimo()
        grupos = {
            dados["grupo"]
            for dados in app.config["RATELIMIT_PROTECTED_ENDPOINTS"].values()
        }
        self.assertTrue(
            {"login", "reports", "external_lookups", "uploads", "mutations"}
            .issubset(grupos)
        )

    def test_39_nonce_independente_em_requisicoes_paralelas_e_sem_cache(self):
        app, _ = criar_app_minimo()

        def obter_nonce(_indice):
            with app.test_client() as cliente:
                resposta = cliente.get("/")
                nonce = PADRAO_NONCE.search(
                    resposta.get_data(as_text=True)
                ).group(1)
                return nonce, resposta.headers["Content-Security-Policy"], resposta

        with ThreadPoolExecutor(max_workers=2) as executor:
            resultados = list(executor.map(obter_nonce, range(2)))

        self.assertNotEqual(resultados[0][0], resultados[1][0])
        for nonce, csp, resposta in resultados:
            self.assertIn(f"'nonce-{nonce}'", csp)
            self.assertIn("no-store", resposta.headers["Cache-Control"])
            self.assertEqual(resposta.headers["Pragma"], "no-cache")

        erros = [app.test_client().get("/erro") for _ in range(2)]
        nonces_erro = [
            re.search(r"'nonce-([^']+)'", r.headers["Content-Security-Policy"]).group(1)
            for r in erros
        ]
        self.assertNotEqual(nonces_erro[0], nonces_erro[1])

    def test_40_inventario_inline_nao_cresce_e_nao_interpola_handler(self):
        arquivos = list((RAIZ / "templates").rglob("*.html"))
        arquivos += list(
            (RAIZ / "modulos" / "fiscalizacao_contratos" / "templates").rglob("*.html")
        )
        texto = "\n".join(arquivo.read_text(encoding="utf-8") for arquivo in arquivos)
        eventos = re.findall(r"\son[a-z]+\s*=", texto, re.IGNORECASE)
        estilos = re.findall(r"\sstyle\s*=", texto, re.IGNORECASE)
        self.assertLessEqual(len(eventos), 60)
        self.assertLessEqual(len(estilos), 44)
        self.assertNotRegex(texto, r"(?i)(?:href|action)\s*=\s*['\"]javascript:")
        for _aspas, handler in re.findall(
            r"\son[a-z]+\s*=\s*([\"'])(.*?)\1",
            texto,
            re.IGNORECASE | re.DOTALL,
        ):
            self.assertNotIn("{{", handler)
            self.assertNotIn("{%", handler)

    def test_41_storage_rejeita_memoria_nao_autorizada_e_uri_malformada(self):
        with self.assertRaisesRegex(RuntimeError, "autorização explícita"):
            criar_app_minimo(
                "homologation",
                RATELIMIT_STORAGE_URI="memory://",
                RATELIMIT_ALLOW_MEMORY_HOMOLOGATION="false",
            )

        for uri in (
            " memory://",
            "memory:// ",
            "memory://\r\n",
            "file:///tmp/limites",
            "redis://",
            "redis://host.invalid/0 fragmento",
        ):
            with self.subTest(uri=repr(uri)), self.assertRaises(RuntimeError):
                criar_app_minimo(RATELIMIT_STORAGE_URI=uri)

    def test_42_rotas_ocultas_online_permanecem_404_sem_virar_429(self):
        with patch.dict(
            os.environ,
            ambiente(
                "homologation",
                RATELIMIT_DEFAULT="1 per minute",
                RATELIMIT_MUTATIONS="1 per minute",
            ),
            clear=True,
        ):
            app = Flask(__name__, template_folder=str(RAIZ / "templates"))
            configurar_aplicacao(app)

        operacao = MagicMock()

        def ocultar_online(view):
            @wraps(view)
            def protegida(*args, **kwargs):
                if app.config["APP_ENV"] in {"homologation", "production"}:
                    abort(404)
                return view(*args, **kwargs)

            return protegida

        @app.post("/registrar_denuncia")
        @ocultar_online
        def registrar_denuncia():
            operacao()
            return "indevido"

        @app.get("/sucesso_denuncia")
        @ocultar_online
        def sucesso_denuncia():
            operacao()
            return "indevido"

        aplicar_limites_rotas(app)
        cliente = app.test_client()
        base_url = f"https://{HOST_TESTE}"
        respostas = [
            cliente.post("/registrar_denuncia", base_url=base_url)
            for _ in range(3)
        ]
        respostas += [
            cliente.get("/sucesso_denuncia", base_url=base_url)
            for _ in range(3)
        ]
        self.assertEqual([resposta.status_code for resposta in respostas], [404] * 6)
        operacao.assert_not_called()
        self.assertNotIn(
            "registrar_denuncia",
            app.config["RATELIMIT_PROTECTED_ENDPOINTS"],
        )

    def test_43_rotas_ocultas_continuam_limitadas_localmente(self):
        with patch.dict(
            os.environ,
            ambiente(RATELIMIT_MUTATIONS="1 per minute"),
            clear=True,
        ):
            app = Flask(__name__, template_folder=str(RAIZ / "templates"))
            configurar_aplicacao(app)

        operacao = MagicMock()

        @app.post("/registrar_denuncia")
        def registrar_denuncia():
            operacao()
            return "ok"

        @app.get("/")
        def index():
            return "início"

        aplicar_limites_rotas(app)
        cliente = app.test_client()
        self.assertEqual(cliente.post("/registrar_denuncia").status_code, 200)
        self.assertEqual(cliente.post("/registrar_denuncia").status_code, 429)
        operacao.assert_called_once()

    def test_44_falhas_da_barreira_basic_recebem_limite_sem_afetar_health(self):
        with patch.dict(
            os.environ,
            ambiente(
                "homologation",
                RATELIMIT_DEFAULT="1 per minute",
                HOMOLOGATION_GATE_ENABLED="true",
                HOMOLOGATION_GATE_USER="porteiro",
                HOMOLOGATION_GATE_PASSWORD="senha-ficticia",
            ),
            clear=True,
        ):
            app = Flask(__name__, template_folder=str(RAIZ / "templates"))
            configurar_aplicacao(app)

        @app.get("/privado")
        def privado():
            return "indevido"

        @app.get("/")
        def index():
            return "início"

        @app.get("/health")
        def health():
            return jsonify(status="ok")

        aplicar_limites_rotas(app)
        cliente = app.test_client()
        base_url = f"https://{HOST_TESTE}"
        self.assertEqual(cliente.get("/privado", base_url=base_url).status_code, 401)
        self.assertEqual(cliente.get("/privado", base_url=base_url).status_code, 429)
        self.assertEqual(
            [cliente.get("/health", base_url=base_url).status_code for _ in range(3)],
            [200, 200, 200],
        )

    def test_45_x_forwarded_for_e_proto_nao_sao_confiados_sem_proxy(self):
        app, operacoes = criar_app_minimo(RATELIMIT_LOGIN="1 per minute")
        cliente = app.test_client()
        cabecalhos = {
            "X-Forwarded-For": "192.0.2.10",
            "X-Forwarded-Proto": "https",
        }
        primeira = cliente.post("/login", headers=cabecalhos)
        cabecalhos["X-Forwarded-For"] = "192.0.2.11"
        segunda = cliente.post("/login", headers=cabecalhos)
        self.assertEqual([primeira.status_code, segunda.status_code], [401, 429])
        self.assertNotIn("Strict-Transport-Security", primeira.headers)
        self.assertEqual(operacoes["login"].call_count, 1)

    def test_46_usuario_autenticado_muda_sessao_sem_forjar_chave(self):
        app, operacoes = criar_app_minimo(RATELIMIT_MUTATIONS="1 per minute")
        login_manager = LoginManager(app)

        class Usuario(UserMixin):
            role = "admin"

            def __init__(self, identificador):
                self.id = identificador

        @login_manager.user_loader
        def carregar(user_id):
            return Usuario(user_id)

        cliente = app.test_client()

        def usar_sessao(user_id):
            with cliente.session_transaction() as sessao:
                sessao["_user_id"] = user_id
                sessao["_fresh"] = True
            return cliente.post("/mutacao", json={"usuario_id": "999"})

        self.assertEqual(usar_sessao("7").status_code, 200)
        self.assertEqual(usar_sessao("8").status_code, 200)
        self.assertEqual(usar_sessao("7").status_code, 429)
        self.assertEqual(operacoes["mutacao"].call_count, 2)

    def test_47_login_bloqueado_nao_consulta_usuario_nem_verifica_senha(self):
        app, operacoes = criar_app_minimo(RATELIMIT_LOGIN="1 per minute")
        cliente = app.test_client()
        self.assertEqual(cliente.post("/login").status_code, 401)
        self.assertEqual(cliente.post("/login").status_code, 429)
        operacoes["login"].assert_called_once()
        operacoes["senha"].assert_called_once()

    def test_48_csp_restringe_data_e_blob_ao_uso_comprovado(self):
        app, _ = criar_app_minimo()
        csp = app.test_client().get("/").headers["Content-Security-Policy"]
        self.assertIn(
            "img-src 'self' data: https://res.cloudinary.com",
            csp,
        )
        self.assertIn("media-src 'self'", csp)
        self.assertNotIn("img-src 'self' data: blob:", csp)
        self.assertNotIn("media-src 'self' blob:", csp)
        self.assertNotIn("font-src 'self' data:", csp)
        cadastro = (RAIZ / "templates" / "cadastro.html").read_text(
            encoding="utf-8"
        )
        self.assertIn("usuario.uvr_acesso", cadastro)
        self.assertIn("| tojson", cadastro)

    def test_49_respostas_429_preservam_csp_sem_detalhes_internos(self):
        app, _ = criar_app_minimo(RATELIMIT_LOGIN="1 per minute")
        resposta = postagens(app.test_client(), "/login", 2)[-1]
        corpo = resposta.get_data(as_text=True)
        self.assertIn("Content-Security-Policy", resposta.headers)
        self.assertNotIn("per minute", corpo)
        self.assertNotIn("127.0.0.1", corpo)
        self.assertNotIn("storage", corpo.casefold())

    def test_50_javascript_extraido_trata_401_e_429_sem_url_externa(self):
        javascript = (RAIZ / "static" / "js" / "cadastro_seguranca.js").read_text(
            encoding="utf-8"
        )
        self.assertIn("statusHttp !== 401", javascript)
        self.assertIn("statusHttp !== 429", javascript)
        self.assertNotIn("http://", javascript)
        self.assertNotIn("https://", javascript)
        self.assertNotRegex(javascript, r"\beval\s*\(|new\s+Function")

    def test_51_limite_compartilhado_nao_e_contornado_por_outro_endpoint(self):
        app, operacoes = criar_app_minimo(RATELIMIT_EXTERNAL_LOOKUPS="1 per minute")
        cliente = app.test_client()
        self.assertEqual(cliente.get("/cep").status_code, 200)
        self.assertEqual(cliente.get("/cnpj").status_code, 429)
        operacoes["cep"].assert_called_once()
        operacoes["cnpj"].assert_not_called()
