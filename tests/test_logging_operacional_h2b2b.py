"""Testes da H2B.2B sem banco, Redis, Cloudinary ou APIs reais."""

import io
import json
import logging
import os
import re
import threading
import unittest
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import patch

import requests
from flask import Flask, Response, abort, jsonify, request

from configuracao_ambiente import configurar_aplicacao
from logging_operacional import (
    HandlerStreamSeguro,
    FormatadorJsonSeguro,
    VALOR_REDACTADO,
    configurar_logging_operacional,
    redigir_dados,
    redigir_texto,
    registrar_evento,
    resposta_erro_interno,
)
from seguranca_csrf import configurar_csrf
from seguranca_rate_limit import aplicar_limites_rotas
from modulos.fiscalizacao_contratos.services.consultas_externas import (
    ConsultaExternaError,
    consultar_cnpj,
)


RAIZ = Path(__file__).resolve().parents[1]
SEGREDO_TESTE = "segredo-ficticio-h2b2b"
LOGGERS_TERCEIROS = ("cloudinary", "redis", "requests", "urllib3", "werkzeug")


def ambiente_testing(**extras):
    valores = {
        "APP_ENV": "testing",
        "SECRET_KEY": SEGREDO_TESTE,
        "LOG_LEVEL": "INFO",
        "LOG_FORMAT": "json",
        "LOG_REQUESTS": "true",
        "LOG_SECURITY_EVENTS": "true",
        "RATELIMIT_ENABLED": "false",
        "RATELIMIT_STORAGE_URI": "memory://",
    }
    valores.update(extras)
    return valores


def ambiente_online(ambiente="production", **extras):
    valores = {
        "APP_ENV": ambiente,
        "SECRET_KEY": SEGREDO_TESTE,
        "DATABASE_URL": "postgresql://usuario-ficticio@host-ficticio/banco",
        "TRUSTED_HOSTS": "homologacao.exemplo.invalid",
        "TRUST_PROXY": "true",
        "MAX_REQUEST_MB": "64",
        "LOG_LEVEL": "INFO",
        "LOG_FORMAT": "json",
        "LOG_REQUESTS": "true",
        "LOG_SECURITY_EVENTS": "true",
        "RATELIMIT_ENABLED": "true",
        "RATELIMIT_STORAGE_URI": "redis://redis-ficticio.invalid/0",
    }
    valores.update(extras)
    return valores


def _eventos(saida):
    resultado = []
    for linha in saida.getvalue().splitlines():
        linha = linha.strip()
        if linha.startswith("{"):
            resultado.append(json.loads(linha))
    return resultado


@contextmanager
def app_capturada(*, ambiente=None):
    stdout = io.StringIO()
    stderr = io.StringIO()
    valores = ambiente or ambiente_testing()
    raiz = logging.getLogger()
    handlers_anteriores = list(raiz.handlers)
    nivel_anterior = raiz.level
    niveis_terceiros = {
        nome: logging.getLogger(nome).level for nome in LOGGERS_TERCEIROS
    }
    try:
        with (
            patch.dict(os.environ, valores, clear=True),
            redirect_stdout(stdout),
            redirect_stderr(stderr),
        ):
            app = Flask(
                __name__,
                template_folder=str(RAIZ / "templates"),
                static_folder=str(RAIZ / "static"),
            )
            configurar_aplicacao(app)
            app.config["PROPAGATE_EXCEPTIONS"] = False
            app.config["JSON_ENDPOINT_CLASSIFIER"] = (
                lambda: request.endpoint == "erro_json"
            )

            @app.get("/")
            def index():
                return "ok"

            @app.route("/dinamica", methods=["GET", "POST"])
            def dinamica():
                registrar_evento(
                    "helper_event",
                    mensagem="Operação auxiliar concluída.",
                    safe_value="ok",
                )
                return jsonify(ok=True)

            @app.get("/health")
            def health():
                return jsonify(status="ok")

            @app.get("/erro-html")
            def erro_html():
                url_com_credencial = (
                    "postgresql://" + "usuario:senha@" + "host/banco"
                )
                raise RuntimeError(
                    "senha=nao-vazar SELECT * FROM usuarios "
                    + url_com_credencial
                )

            @app.get("/erro-json")
            def erro_json():
                raise ValueError("token=nao-vazar tabela_clientes")

            @app.get("/negado/<int:objeto_id>")
            def negado(objeto_id):
                del objeto_id
                abort(403)

            @app.get("/status/<int:codigo>")
            def status_generico(codigo):
                return "erro", codigo

            app.register_error_handler(500, resposta_erro_interno)
            yield app, stdout, stderr
    finally:
        raiz.handlers[:] = handlers_anteriores
        raiz.setLevel(nivel_anterior)
        for nome, nivel in niveis_terceiros.items():
            logging.getLogger(nome).setLevel(nivel)


class LoggingOperacionalH2B2BTests(unittest.TestCase):
    def test_01_log_online_e_json_valido_em_uma_linha(self):
        with app_capturada() as (app, saida, _):
            app.test_client().get("/dinamica")
        linhas = [linha for linha in saida.getvalue().splitlines() if linha.strip()]
        self.assertTrue(linhas)
        self.assertTrue(all(isinstance(json.loads(linha), dict) for linha in linhas))

    def test_02_timestamp_e_utc_iso8601(self):
        with app_capturada() as (app, saida, _):
            app.test_client().get("/dinamica")
        evento = _eventos(saida)[0]
        self.assertTrue(evento["timestamp"].endswith("Z"))
        datetime.fromisoformat(evento["timestamp"].replace("Z", "+00:00"))

    def test_03_nivel_e_evento_estao_presentes(self):
        with app_capturada() as (app, saida, _):
            app.test_client().get("/dinamica")
        for evento in _eventos(saida):
            self.assertIn(evento["level"], {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})
            self.assertTrue(evento["event"])

    def test_04_request_id_presente_e_de_formato_seguro(self):
        with app_capturada() as (app, saida, _):
            resposta = app.test_client().get("/dinamica")
        request_id = resposta.headers["X-Request-ID"]
        self.assertRegex(request_id, r"^[a-f0-9]{32}$")
        self.assertIn(request_id, {item.get("request_id") for item in _eventos(saida)})

    def test_05_request_id_muda_entre_requisicoes(self):
        with app_capturada() as (app, _, _):
            cliente = app.test_client()
            primeiro = cliente.get("/dinamica").headers["X-Request-ID"]
            segundo = cliente.get("/dinamica").headers["X-Request-ID"]
        self.assertNotEqual(primeiro, segundo)

    def test_06_mesmo_request_id_em_helpers_e_conclusao(self):
        with app_capturada() as (app, saida, _):
            resposta = app.test_client().get("/dinamica")
        request_id = resposta.headers["X-Request-ID"]
        relacionados = [
            item for item in _eventos(saida)
            if item.get("request_id") == request_id
        ]
        self.assertEqual(
            {"helper_event", "request_completed"},
            {item["event"] for item in relacionados},
        )

    def test_07_request_id_recebido_do_cliente_nao_e_confiado(self):
        forjado = "a" * 32
        with app_capturada() as (app, _, _):
            resposta = app.test_client().get(
                "/dinamica", headers={"X-Request-ID": forjado}
            )
        self.assertNotEqual(resposta.headers["X-Request-ID"], forjado)

    def test_08_duracao_em_milissegundos_nao_negativa(self):
        with app_capturada() as (app, saida, _):
            app.test_client().get("/dinamica")
        concluido = next(
            item for item in _eventos(saida)
            if item["event"] == "request_completed"
        )
        self.assertGreaterEqual(concluido["duration_ms"], 0)

    def test_09_health_nao_gera_log_comum(self):
        with app_capturada() as (app, saida, _):
            resposta = app.test_client().get("/health")
        self.assertEqual({"status": "ok"}, resposta.get_json())
        self.assertIn("X-Request-ID", resposta.headers)
        self.assertFalse(any(
            item["event"] == "request_completed"
            and item.get("endpoint") == "health"
            for item in _eventos(saida)
        ))

    def test_10_static_nao_gera_log_comum(self):
        with app_capturada() as (app, saida, _):
            app.test_client().get("/static/css/autenticacao.css")
        self.assertFalse(any(
            item["event"] == "request_completed"
            and item.get("endpoint") == "static"
            for item in _eventos(saida)
        ))

    def test_11_corpo_de_formulario_e_json_nao_aparecem(self):
        proibidos = ("senha-super-secreta", "documento-confidencial")
        with app_capturada() as (app, saida, _):
            cliente = app.test_client()
            cliente.post("/dinamica", data={"senha": proibidos[0]})
            cliente.post("/dinamica", json={"documento": proibidos[1]})
        texto = saida.getvalue()
        self.assertTrue(all(valor not in texto for valor in proibidos))

    def test_12_headers_cookies_e_csrf_nao_aparecem(self):
        proibidos = ("Basic dXN1YXJpbzpzZW5oYQ==", "cookie-secreto", "csrf-secreto")
        with app_capturada() as (app, saida, _):
            cliente = app.test_client()
            cliente.set_cookie("sessao_teste", proibidos[1])
            cliente.get(
                "/dinamica",
                headers={
                    "Authorization": proibidos[0],
                    "X-CSRFToken": proibidos[2],
                },
            )
        texto = saida.getvalue()
        self.assertTrue(all(valor not in texto for valor in proibidos))

    def test_13_query_string_nao_e_registrada(self):
        with app_capturada() as (app, saida, _):
            app.test_client().get("/dinamica?" + "token=valor-proibido")
        self.assertNotIn("valor-proibido", saida.getvalue())

    def test_14_redator_recursivo(self):
        dados = redigir_dados({
            "NESTED": [
                {"PASSWORD": "a", "api_key": "b"},
                {"seguro": "ok", "cookie_sessao": "c"},
            ]
        })
        self.assertEqual(VALOR_REDACTADO, dados["NESTED"][0]["PASSWORD"])
        self.assertEqual(VALOR_REDACTADO, dados["NESTED"][0]["api_key"])
        self.assertEqual(VALOR_REDACTADO, dados["NESTED"][1]["cookie_sessao"])
        self.assertEqual("ok", dados["NESTED"][1]["seguro"])

    def test_15_redator_remove_credenciais_de_url(self):
        url = "redis://" + "usuario:senha@" + "redis.exemplo.invalid/0"
        texto = redigir_texto(url)
        self.assertNotIn("usuario:senha", texto)
        self.assertIn("[REDACTED]", texto)

    def test_16_redator_nao_expoe_texto_de_excecao(self):
        dados = redigir_dados(RuntimeError("password=nao-vazar"))
        self.assertEqual({"error_type": "RuntimeError"}, dados)

    def test_17_redator_remove_email_cpf_e_cnpj(self):
        texto = redigir_texto(
            "pessoa@exemplo.invalid 123.456.789-01 12.345.678/0001-99"
        )
        self.assertNotIn("pessoa@", texto)
        self.assertNotIn("123.456", texto)
        self.assertNotIn("12.345", texto)

    def test_18_request_id_nao_e_confundido_com_documento(self):
        request_id = "9ec5efa395464162beedb123456789ab"
        self.assertEqual(request_id, redigir_texto(request_id))

    def test_19_nao_existem_handlers_de_arquivo(self):
        with app_capturada() as (_, __, ___):
            handlers = logging.getLogger().handlers
            self.assertFalse(any(
                isinstance(handler, logging.FileHandler)
                for handler in handlers
            ))

    def test_20_online_rejeita_debug(self):
        with patch.dict(
            os.environ,
            ambiente_online(LOG_LEVEL="DEBUG"),
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "DEBUG"):
                configurar_aplicacao(Flask("debug_inseguro"))

    def test_21_online_rejeita_formato_textual(self):
        with patch.dict(
            os.environ,
            ambiente_online(LOG_FORMAT="text"),
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "json"):
                configurar_aplicacao(Flask("texto_inseguro"))

    def test_22_online_exige_eventos_de_seguranca(self):
        with patch.dict(
            os.environ,
            ambiente_online(LOG_SECURITY_EVENTS="false"),
            clear=True,
        ):
            with self.assertRaisesRegex(RuntimeError, "LOG_SECURITY_EVENTS"):
                configurar_aplicacao(Flask("seguranca_desativada"))

    def test_23_valores_de_log_invalidos_falham_fechado(self):
        casos = (
            {"LOG_LEVEL": "VERBOSE"},
            {"LOG_FORMAT": "xml"},
            {"LOG_REQUESTS": "talvez"},
            {"LOG_SECURITY_EVENTS": "talvez"},
        )
        for extras in casos:
            with self.subTest(extras=extras), patch.dict(
                os.environ, ambiente_testing(**extras), clear=True
            ):
                with self.assertRaises(RuntimeError):
                    configurar_aplicacao(Flask("config_invalida"))

    def test_24_erro_configuracao_nao_expoe_valor_recebido(self):
        stderr = io.StringIO()
        with (
            patch.dict(
                os.environ,
                ambiente_testing(LOG_FORMAT="segredo-nao-vazar"),
                clear=True,
            ),
            redirect_stderr(stderr),
            self.assertRaises(RuntimeError),
        ):
            configurar_aplicacao(Flask("erro_config"))
        evento = json.loads(stderr.getvalue().strip().splitlines()[-1])
        self.assertEqual("application_configuration_error", evento["event"])
        self.assertNotIn("segredo-nao-vazar", stderr.getvalue())

    def test_25_erro_500_html_e_generico(self):
        with app_capturada() as (app, saida, _):
            resposta = app.test_client().get("/erro-html")
        corpo = resposta.get_data(as_text=True)
        self.assertEqual(500, resposta.status_code)
        self.assertIn("Código de referência", corpo)
        self.assertNotIn("senha=nao-vazar", corpo)
        self.assertNotIn("SELECT", corpo)
        self.assertIn("no-store", resposta.headers["Cache-Control"])
        self.assertIn("Content-Security-Policy", resposta.headers)
        self.assertIn("X-Request-ID", resposta.headers)
        self.assertNotIn("senha=nao-vazar", saida.getvalue())

    def test_26_erro_500_json_e_generico(self):
        with app_capturada() as (app, saida, _):
            resposta = app.test_client().get("/erro-json")
        corpo = resposta.get_json()
        self.assertEqual(500, resposta.status_code)
        self.assertEqual(
            "Não foi possível processar a solicitação.",
            corpo["error"],
        )
        self.assertEqual(resposta.headers["X-Request-ID"], corpo["request_id"])
        self.assertNotIn("token=nao-vazar", saida.getvalue())

    def test_27_internal_error_registra_tipo_sem_traceback(self):
        with app_capturada() as (app, saida, _):
            resposta = app.test_client().get("/erro-json")
        evento = next(
            item for item in _eventos(saida)
            if item["event"] == "internal_error"
        )
        self.assertEqual("ValueError", evento["error_type"])
        self.assertEqual(resposta.headers["X-Request-ID"], evento["request_id"])
        self.assertNotIn("Traceback", saida.getvalue())
        self.assertNotIn("tabela_clientes", saida.getvalue())

    def test_28_resposta_500_independe_do_texto_da_excecao(self):
        with app_capturada() as (app, _, _):
            cliente = app.test_client()
            html = cliente.get("/erro-html")
            json_resp = cliente.get("/erro-json")
        self.assertIn("Não foi possível", html.get_data(as_text=True))
        self.assertEqual(
            "Não foi possível processar a solicitação.",
            json_resp.get_json()["error"],
        )

    def test_29_authorization_denied_nao_registra_id_do_objeto(self):
        with app_capturada() as (app, saida, _):
            resposta = app.test_client().get("/negado/987654321")
        self.assertEqual(403, resposta.status_code)
        evento = next(
            item for item in _eventos(saida)
            if item["event"] == "authorization_denied"
        )
        self.assertNotIn("987654321", json.dumps(evento))

    def test_30_basic_auth_failed_e_generico(self):
        valores = ambiente_online(
            "homologation",
            HOMOLOGATION_GATE_ENABLED="true",
            HOMOLOGATION_GATE_USER="porteiro-ficticio",
            HOMOLOGATION_GATE_PASSWORD="senha-ficticia",
            RATELIMIT_STORAGE_URI="memory://",
            RATELIMIT_ALLOW_MEMORY_HOMOLOGATION="true",
        )
        with app_capturada(ambiente=valores) as (app, saida, _):
            @app.get("/privado")
            def privado():
                return "ok"

            resposta = app.test_client().get(
                "/privado",
                base_url="https://homologacao.exemplo.invalid",
                headers={"Authorization": "Basic ZGFkby1wcm9pYmlkbw=="},
            )
        self.assertEqual(401, resposta.status_code)
        evento = next(
            item for item in _eventos(saida)
            if item["event"] == "basic_auth_failed"
        )
        self.assertNotIn("ZGFkby1wcm9pYmlkbw", json.dumps(evento))
        self.assertNotIn("porteiro-ficticio", saida.getvalue())

    def test_31_csrf_rejected_nao_registra_token(self):
        with app_capturada() as (app, saida, _):
            configurar_csrf(app)

            @app.post("/mutacao-csrf")
            def mutacao_csrf():
                return "ok"

            resposta = app.test_client().post(
                "/mutacao-csrf",
                data={"csrf_token": "token-proibido"},
            )
        self.assertEqual(400, resposta.status_code)
        self.assertTrue(any(
            item["event"] == "csrf_rejected" for item in _eventos(saida)
        ))
        self.assertNotIn("token-proibido", saida.getvalue())

    def test_32_rate_limit_exceeded_nao_expoe_chave_interna(self):
        valores = ambiente_testing(
            RATELIMIT_ENABLED="true",
            RATELIMIT_LOGIN="1 per minute",
        )
        with app_capturada(ambiente=valores) as (app, saida, _):
            @app.post("/login")
            def login():
                return "falha", 401

            aplicar_limites_rotas(app)
            cliente = app.test_client()
            cliente.post("/login")
            resposta = cliente.post("/login")
        self.assertEqual(429, resposta.status_code)
        eventos = _eventos(saida)
        self.assertTrue(any(
            item["event"] == "rate_limit_exceeded" for item in eventos
        ))
        self.assertNotIn("grupo:login", saida.getvalue())
        self.assertNotIn("127.0.0.1", saida.getvalue())

    def test_33_eventos_de_integracao_estao_no_codigo_real(self):
        arquivos = {
            "app.py": ("authentication_failed", "authentication_succeeded"),
            "seguranca_csrf.py": ("csrf_rejected",),
            "seguranca_rate_limit.py": ("rate_limit_exceeded",),
            "consultas_externas.py": ("external_service_error",),
            "documentos.py": ("signed_url_generation_failed",),
            "atestes.py": ("upload_rejected",),
        }
        bases = {
            "app.py": RAIZ / "app.py",
            "seguranca_csrf.py": RAIZ / "seguranca_csrf.py",
            "seguranca_rate_limit.py": RAIZ / "seguranca_rate_limit.py",
            "consultas_externas.py": RAIZ / "modulos/fiscalizacao_contratos/services/consultas_externas.py",
            "documentos.py": RAIZ / "modulos/fiscalizacao_contratos/routes/documentos.py",
            "atestes.py": RAIZ / "modulos/fiscalizacao_contratos/routes/atestes.py",
        }
        for nome, eventos in arquivos.items():
            texto = bases[nome].read_text(encoding="utf-8")
            for evento in eventos:
                self.assertIn(evento, texto)
        self.assertIn(
            "inactive_session_rejected",
            bases["app.py"].read_text(encoding="utf-8"),
        )

    def test_34_startup_nao_expoe_configuracoes_sensiveis(self):
        with app_capturada() as (_, saida, _):
            pass
        startup = next(
            item for item in _eventos(saida)
            if item["event"] == "application_startup"
        )
        texto = json.dumps(startup)
        self.assertNotIn(SEGREDO_TESTE, texto)
        self.assertNotIn("DATABASE_URL", texto)
        self.assertNotIn("RATELIMIT_STORAGE_URI", texto)
        self.assertNotIn("TRUSTED_HOSTS", texto)

    def test_35_unicode_e_valores_nao_serializaveis_nao_quebram(self):
        with app_capturada() as (app, saida, _):
            @app.get("/unicode")
            def unicode():
                registrar_evento(
                    "unicode_event",
                    mensagem="Fiscalização concluída.",
                    objeto=object(),
                )
                return "ok"

            app.test_client().get("/unicode")
        evento = next(
            item for item in _eventos(saida)
            if item["event"] == "unicode_event"
        )
        self.assertEqual("Fiscalização concluída.", evento["message"])
        self.assertIsInstance(evento["objeto"], str)

    def test_36_campos_nulos_nao_aumentam_o_log(self):
        with app_capturada() as (app, saida, _):
            @app.get("/nulo")
            def nulo():
                registrar_evento("null_event", opcional=None)
                return "ok"

            app.test_client().get("/nulo")
        evento = next(
            item for item in _eventos(saida)
            if item["event"] == "null_event"
        )
        self.assertNotIn("opcional", evento)

    def test_37_log_requests_pode_ser_desligado_apenas_fora_de_online(self):
        with app_capturada(
            ambiente={
                "APP_ENV": "development",
                "SECRET_KEY": SEGREDO_TESTE,
                "LOG_REQUESTS": "false",
                "RATELIMIT_ENABLED": "false",
                "RATELIMIT_STORAGE_URI": "memory://",
            }
        ) as (app, saida, _):
            app.test_client().get("/dinamica")
        self.assertFalse(any(
            item["event"] == "request_completed" for item in _eventos(saida)
        ))
        self.assertIn("event=helper_event", saida.getvalue())

    def test_38_configuracao_nao_cria_dependencia_externa(self):
        texto = (RAIZ / "logging_operacional.py").read_text(encoding="utf-8")
        self.assertNotRegex(texto, r"(?i)\b(sentry|datadog|newrelic)\b")
        self.assertNotIn("FileHandler(", texto)
        self.assertNotIn("RotatingFileHandler", texto)

    def test_39_gunicorn_continua_em_stdout_stderr_sem_query(self):
        texto = (RAIZ / "gunicorn.conf.py").read_text(encoding="utf-8")
        self.assertIn('accesslog = "-"', texto)
        self.assertIn('errorlog = "-"', texto)
        formato = re.search(r"access_log_format\s*=\s*([^\n]+)", texto).group(1)
        self.assertNotIn("%(q)s", formato)
        self.assertNotIn("Authorization", formato)
        self.assertNotIn("cookie", formato.lower())

    def test_40_erros_http_recebem_request_id_e_cabecalhos(self):
        with app_capturada() as (app, _, _):
            cliente = app.test_client()
            for codigo in (400, 401, 403, 404, 405, 413, 415):
                with self.subTest(codigo=codigo):
                    resposta = cliente.get(f"/status/{codigo}")
                    self.assertEqual(codigo, resposta.status_code)
                    self.assertRegex(
                        resposta.headers["X-Request-ID"],
                        r"^[a-f0-9]{32}$",
                    )
                    self.assertIn(
                        "Content-Security-Policy",
                        resposta.headers,
                    )

    def test_41_json_estrito_trata_nan_infinity_e_tipos_conhecidos(self):
        with app_capturada() as (app, saida, _):
            @app.get("/tipos")
            def tipos():
                registrar_evento(
                    "typed_event",
                    nan=float("nan"),
                    infinito=float("inf"),
                    decimal=Decimal("1.25"),
                    data=date(2026, 7, 29),
                    uuid=uuid.UUID("12345678-1234-5678-1234-567812345678"),
                )
                return "ok"

            app.test_client().get("/tipos")
        linha = next(
            linha for linha in saida.getvalue().splitlines()
            if '"event":"typed_event"' in linha
        )
        self.assertNotRegex(linha, r":(?:NaN|Infinity|-Infinity)(?:,|})")
        evento = json.loads(linha, parse_constant=lambda valor: self.fail(valor))
        self.assertEqual("1.25", evento["decimal"])

    def test_42_campos_extras_nao_sobrescrevem_metadados_centrais(self):
        with app_capturada() as (app, saida, _):
            @app.get("/reservados")
            def reservados():
                logging.getLogger("teste.reservados").info(
                    "mensagem real",
                    extra={
                        "event": "reserved_event",
                        "structured_fields": {
                            "timestamp": "forjado",
                            "level": "CRITICAL",
                            "event": "forjado",
                            "request_id": "forjado",
                            "seguro": "ok",
                        },
                    },
                )
                return "ok"

            resposta = app.test_client().get("/reservados")
        evento = next(
            item for item in _eventos(saida)
            if item["event"] == "reserved_event"
        )
        self.assertNotEqual("forjado", evento["timestamp"])
        self.assertEqual("INFO", evento["level"])
        self.assertEqual(resposta.headers["X-Request-ID"], evento["request_id"])
        self.assertEqual("ok", evento["seguro"])
        conclusao = next(
            item for item in _eventos(saida)
            if item["event"] == "request_completed"
        )
        self.assertNotIn("blueprint", conclusao)
        self.assertNotIn("actor_type", conclusao)

    def test_43_redator_limita_ciclos_profundidade_tamanho_e_nao_muta_original(self):
        original = {
            "PASSWORD": "valor-proibido",
            "itens": list(range(80)),
            "texto": "a" * 5000,
        }
        original["ciclo"] = original
        redigido = redigir_dados(original)
        self.assertEqual("valor-proibido", original["PASSWORD"])
        self.assertEqual(80, len(original["itens"]))
        self.assertEqual(VALOR_REDACTADO, redigido["PASSWORD"])
        self.assertEqual("[CIRCULAR]", redigido["ciclo"])
        self.assertLessEqual(len(redigido["itens"]), 51)
        self.assertIn("[TRUNCATED]", redigido["texto"])

    def test_44_objeto_com_str_perigoso_nao_quebra_nem_vaza(self):
        class ObjetoPerigoso:
            def __str__(self):
                raise RuntimeError("senha-bruta-nao-vazar")

        dados = redigir_dados({"objeto": ObjetoPerigoso()})
        self.assertEqual("<ObjetoPerigoso>", dados["objeto"])

    def test_45_mensagens_complexas_permanecem_em_uma_linha_json(self):
        with app_capturada() as (app, saida, _):
            @app.get("/mensagens")
            def mensagens():
                registrar_evento(
                    "message_event",
                    mensagem='Ação "válida"\nsegunda linha',
                )
                registrar_evento("empty_message", mensagem="")
                registrar_evento("long_message", mensagem="x" * 6000)
                return "ok"

            app.test_client().get("/mensagens")
        eventos = _eventos(saida)
        complexo = next(item for item in eventos if item["event"] == "message_event")
        self.assertIn("Ação", complexo["message"])
        self.assertNotIn("\n", complexo["message"])
        self.assertTrue(any(item["event"] == "empty_message" for item in eventos))
        longo = next(item for item in eventos if item["event"] == "long_message")
        self.assertIn("[TRUNCATED]", longo["message"])

    def test_46_logging_reconfigurado_nao_acumula_handlers(self):
        saida = io.StringIO()
        handlers_anteriores = list(logging.getLogger().handlers)
        nivel_anterior = logging.getLogger().level
        niveis_terceiros = {
            nome: logging.getLogger(nome).level for nome in LOGGERS_TERCEIROS
        }
        try:
            with (
                patch.dict(os.environ, ambiente_testing(), clear=True),
                redirect_stdout(saida),
            ):
                primeiro = Flask("logging_primeiro")
                segundo = Flask("logging_segundo")
                configurar_logging_operacional(primeiro, "testing")
                configurar_logging_operacional(primeiro, "testing")
                configurar_logging_operacional(segundo, "testing")
                handlers = [
                    item for item in logging.getLogger().handlers
                    if getattr(item, "_recic3_operational", False)
                ]
                self.assertEqual(1, len(handlers))
                self.assertTrue(primeiro.logger.propagate)
                self.assertTrue(segundo.logger.propagate)
        finally:
            logging.getLogger().handlers[:] = handlers_anteriores
            logging.getLogger().setLevel(nivel_anterior)
            for nome, nivel in niveis_terceiros.items():
                logging.getLogger(nome).setLevel(nivel)

    def test_47_request_ids_concorrentes_sao_independentes(self):
        with app_capturada() as (app, _, _):
            barreira = threading.Barrier(2)

            @app.get("/concorrente")
            def concorrente():
                barreira.wait(timeout=5)
                return "ok"

            def consultar():
                with app.test_client() as cliente:
                    return cliente.get("/concorrente").headers["X-Request-ID"]

            with ThreadPoolExecutor(max_workers=2) as executor:
                ids = list(executor.map(lambda _: consultar(), range(2)))
        self.assertEqual(2, len(set(ids)))
        self.assertTrue(all(re.fullmatch(r"[a-f0-9]{32}", item) for item in ids))

    def test_48_rejeicao_precoce_de_host_recebe_request_id(self):
        with app_capturada() as (app, _, _):
            app.config["TRUSTED_HOSTS"] = ["permitido.exemplo.invalid"]
            resposta = app.test_client().get(
                "/dinamica",
                base_url="http://host-malicioso.exemplo.invalid",
                headers={"X-Request-ID": "b" * 32},
            )
        self.assertEqual(400, resposta.status_code)
        self.assertRegex(resposta.headers["X-Request-ID"], r"^[a-f0-9]{32}$")
        self.assertNotEqual("b" * 32, resposta.headers["X-Request-ID"])

    def test_49_resposta_streaming_nao_e_materializada_pelo_log(self):
        consumidos = []
        with app_capturada() as (app, _, _):
            @app.after_request
            def observar_antes_da_iteracao(resposta):
                resposta.headers["X-Stream-Consumed-Before-Return"] = (
                    "yes" if consumidos else "no"
                )
                return resposta

            @app.get("/stream")
            def stream():
                def gerar():
                    consumidos.append("iniciado")
                    yield "parte"
                return Response(gerar(), content_type="text/plain")

            resposta = app.test_client().get("/stream", buffered=False)
            self.assertEqual(
                "no",
                resposta.headers["X-Stream-Consumed-Before-Return"],
            )
            self.assertEqual("parte", resposta.get_data(as_text=True))
        self.assertEqual(["iniciado"], consumidos)

    def test_50_excecao_online_omite_mensagem_crua_e_traceback(self):
        for ambiente in ("homologation", "production"):
            with self.subTest(ambiente=ambiente):
                saida = io.StringIO()
                app = Flask(f"exception_{ambiente}")
                raiz = logging.getLogger()
                anteriores = list(raiz.handlers)
                nivel_anterior = raiz.level
                niveis_terceiros = {
                    nome: logging.getLogger(nome).level
                    for nome in LOGGERS_TERCEIROS
                }
                try:
                    with (
                        patch.dict(
                            os.environ,
                            {
                                "LOG_LEVEL": "INFO",
                                "LOG_FORMAT": "json",
                                "LOG_REQUESTS": "true",
                                "LOG_SECURITY_EVENTS": "true",
                            },
                            clear=True,
                        ),
                        redirect_stdout(saida),
                    ):
                        configurar_logging_operacional(app, ambiente)
                        with app.app_context():
                            try:
                                raise RuntimeError(
                                    "valor-bruto-proibido C:\\segredo\\arquivo.py"
                                )
                            except RuntimeError:
                                app.logger.exception(
                                    "mensagem-bruta-proibida SELECT * FROM usuarios"
                                )
                finally:
                    raiz.handlers[:] = anteriores
                    raiz.setLevel(nivel_anterior)
                    for nome, nivel in niveis_terceiros.items():
                        logging.getLogger(nome).setLevel(nivel)
                texto = saida.getvalue()
                evento = _eventos(saida)[0]
                self.assertEqual("RuntimeError", evento["error_type"])
                self.assertEqual("Falha interna registrada.", evento["message"])
                self.assertNotIn("valor-bruto-proibido", texto)
                self.assertNotIn("mensagem-bruta-proibida", texto)
                self.assertNotIn("Traceback", texto)
                self.assertNotIn("SELECT", texto)
                self.assertNotIn("arquivo.py", texto)

    def test_51_excecao_development_exige_debug_para_traceback(self):
        for debug, possui_traceback in ((False, False), (True, True)):
            with self.subTest(debug=debug):
                saida = io.StringIO()
                app = Flask(f"dev_exception_{debug}")
                app.debug = debug
                raiz = logging.getLogger()
                anteriores = list(raiz.handlers)
                nivel_anterior = raiz.level
                niveis_terceiros = {
                    nome: logging.getLogger(nome).level
                    for nome in LOGGERS_TERCEIROS
                }
                try:
                    with (
                        patch.dict(
                            os.environ,
                            {
                                "LOG_LEVEL": "DEBUG" if debug else "INFO",
                                "LOG_FORMAT": "text",
                                "LOG_REQUESTS": "true",
                                "LOG_SECURITY_EVENTS": "true",
                            },
                            clear=True,
                        ),
                        redirect_stdout(saida),
                    ):
                        configurar_logging_operacional(app, "development")
                        with app.app_context():
                            try:
                                raise RuntimeError("senha=valor-proibido")
                            except RuntimeError:
                                app.logger.exception("Falha local controlada.")
                finally:
                    raiz.handlers[:] = anteriores
                    raiz.setLevel(nivel_anterior)
                    for nome, nivel in niveis_terceiros.items():
                        logging.getLogger(nome).setLevel(nivel)
                texto = saida.getvalue()
                self.assertEqual(possui_traceback, "Traceback" in texto)
                self.assertNotIn("valor-proibido", texto)

    def test_52_falha_do_stream_de_log_nao_interrompe_resposta(self):
        class StreamQueFalha:
            def write(self, _valor):
                raise OSError("segredo-nao-vazar")

            def flush(self):
                return None

        with app_capturada() as (app, _, stderr):
            handler = next(
                item for item in logging.getLogger().handlers
                if isinstance(item, HandlerStreamSeguro)
            )
            handler.stream = StreamQueFalha()
            resposta = app.test_client().get("/dinamica")
        self.assertEqual(200, resposta.status_code)
        self.assertIn("logging_failure", stderr.getvalue())
        self.assertNotIn("segredo-nao-vazar", stderr.getvalue())

    def test_53_erro_500_emite_um_unico_internal_error(self):
        with app_capturada() as (app, saida, _):
            app.test_client().get("/erro-json")
        eventos = _eventos(saida)
        self.assertEqual(
            1,
            sum(item["event"] == "internal_error" for item in eventos),
        )
        self.assertEqual(
            1,
            sum(item["event"] == "request_completed" for item in eventos),
        )
        self.assertFalse(any(
            item["event"] == "application_log"
            and item.get("error_type")
            for item in eventos
        ))

    def test_54_testing_ignora_configuracao_pessoal_valida(self):
        valores = ambiente_testing(
            LOG_LEVEL="DEBUG",
            LOG_FORMAT="text",
            LOG_REQUESTS="false",
            LOG_SECURITY_EVENTS="false",
        )
        with app_capturada(ambiente=valores) as (app, saida, _):
            app.test_client().get("/dinamica")
        self.assertEqual("INFO", app.config["LOG_LEVEL"])
        self.assertEqual("json", app.config["LOG_FORMAT"])
        self.assertTrue(app.config["LOG_REQUESTS"])
        self.assertTrue(app.config["LOG_SECURITY_EVENTS"])
        self.assertTrue(_eventos(saida))

    def test_55_redator_reconhece_campos_sensiveis_adicionais(self):
        dados = redigir_dados({
            "PUBLIC_ID": "arquivo",
            "telefone": "999999999",
            "Conta_Bancaria": "123",
            "agencia": "456",
            "email_contato": "pessoa@exemplo.invalid",
            "cnpj": "123",
            "campo_seguro": "ok",
        })
        for chave in (
            "PUBLIC_ID",
            "telefone",
            "Conta_Bancaria",
            "agencia",
            "email_contato",
            "cnpj",
        ):
            self.assertEqual(VALOR_REDACTADO, dados[chave])
        self.assertEqual("ok", dados["campo_seguro"])

    def test_56_erro_de_configuracao_de_startup_e_generico(self):
        saida = io.StringIO()
        with (
            patch.dict(
                os.environ,
                {
                    "APP_ENV": "production",
                    "LOG_FORMAT": "json",
                    "LOG_LEVEL": "INFO",
                    "LOG_REQUESTS": "true",
                    "LOG_SECURITY_EVENTS": "true",
                    "SECRET_KEY": "segredo-ficticio",
                },
                clear=True,
            ),
            redirect_stdout(saida),
            self.assertRaises(RuntimeError),
        ):
            configurar_aplicacao(Flask("startup_invalido"))
        evento = next(
            item for item in _eventos(saida)
            if item["event"] == "application_configuration_error"
        )
        texto = json.dumps(evento)
        self.assertEqual("startup_security", evento["configuration_name"])
        self.assertNotIn("DATABASE_URL", texto)
        self.assertNotIn("segredo-ficticio", texto)

    def test_57_404_e_405_reais_nao_viram_500(self):
        with app_capturada() as (app, saida, _):
            cliente = app.test_client()
            nao_encontrado = cliente.get("/rota-que-nao-existe")
            metodo_invalido = cliente.post("/")
        self.assertEqual(404, nao_encontrado.status_code)
        self.assertEqual(405, metodo_invalido.status_code)
        self.assertIn("X-Request-ID", nao_encontrado.headers)
        self.assertIn("X-Request-ID", metodo_invalido.headers)
        self.assertFalse(any(
            item["event"] == "internal_error" for item in _eventos(saida)
        ))

    def test_58_formato_json_nao_usa_representacao_python(self):
        record = logging.LogRecord(
            "teste",
            logging.INFO,
            __file__,
            1,
            "Ação com aspas \" e acento",
            (),
            None,
        )
        formatado = FormatadorJsonSeguro("production").format(record)
        self.assertIsInstance(json.loads(formatado), dict)
        self.assertNotIn("'event':", formatado)
        self.assertEqual(1, len(formatado.splitlines()))

    def test_59_erro_externo_nao_registra_identificadores_ou_texto_bruto(self):
        proibidos = (
            "04252011000110",
            "01310100",
            "public-id-proibido",
            "https://privado.exemplo.invalid/assinado",
        )
        erro = requests.ConnectionError(
            " ".join(proibidos) + " token-sem-rotulo-proibido"
        )
        with app_capturada() as (app, saida, _):
            with (
                app.app_context(),
                patch(
                    "modulos.fiscalizacao_contratos.services."
                    "consultas_externas.requests.get",
                    side_effect=erro,
                ),
                self.assertRaises(ConsultaExternaError),
            ):
                consultar_cnpj(proibidos[0])
        texto = saida.getvalue()
        self.assertTrue(all(valor not in texto for valor in proibidos))
        self.assertNotIn("token-sem-rotulo-proibido", texto)
        externos = [
            item for item in _eventos(saida)
            if item["event"] == "external_service_error"
        ]
        self.assertEqual(2, len(externos))
        self.assertTrue(all(
            item["error_type"] == "conexao_dns_ou_rede"
            for item in externos
        ))

    def test_60_ambiente_invalido_falha_com_evento_minimo(self):
        stderr = io.StringIO()
        with (
            patch.dict(
                os.environ,
                {"APP_ENV": "ambiente-secreto-invalido"},
                clear=True,
            ),
            redirect_stderr(stderr),
            self.assertRaises(RuntimeError),
        ):
            configurar_aplicacao(Flask("ambiente_invalido"))
        evento = json.loads(stderr.getvalue().strip())
        self.assertEqual("application_configuration_error", evento["event"])
        self.assertEqual("unknown", evento["environment"])
        self.assertNotIn("ambiente-secreto-invalido", stderr.getvalue())

    def test_61_falha_do_redator_durante_erro_500_nao_derruba_handler(self):
        with app_capturada() as (app, _, stderr):
            with patch(
                "logging_operacional.redigir_dados",
                side_effect=RuntimeError("segredo-do-redator-nao-vazar"),
            ):
                resposta = app.test_client().get("/erro-html")
        self.assertEqual(500, resposta.status_code)
        self.assertIn("Não foi possível", resposta.get_data(as_text=True))
        self.assertIn("logging_failure", stderr.getvalue())
        self.assertNotIn("segredo-do-redator-nao-vazar", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
