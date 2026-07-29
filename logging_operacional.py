"""Logs operacionais seguros, estruturados e correlacionados por requisição."""

from __future__ import annotations

import json
import logging
import math
import re
import sys
import time
import uuid
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

from flask import (
    Response,
    current_app,
    g,
    has_app_context,
    has_request_context,
    jsonify,
    render_template,
    request,
)


NIVEIS_VALIDOS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
FORMATOS_VALIDOS = {"json", "text"}
AMBIENTES_ONLINE = {"homologation", "production"}
VALOR_REDACTADO = "[REDACTED]"
VALOR_CIRCULAR = "[CIRCULAR]"
VALOR_TRUNCADO = "[TRUNCATED]"
MAX_PROFUNDIDADE_LOG = 8
MAX_ITENS_LOG = 50
MAX_TEXTO_LOG = 4096
EVENTO_VALIDO = re.compile(r"^[a-z][a-z0-9_]{0,79}$")
CAMPOS_RESERVADOS = {
    "timestamp",
    "level",
    "event",
    "message",
    "environment",
    "request_id",
    "method",
    "endpoint",
}
CHAVES_SENSIVEIS = {
    "password",
    "senha",
    "secret",
    "token",
    "authorization",
    "cookie",
    "csrf",
    "api_key",
    "apikey",
    "cloudinary",
    "database_url",
    "redis",
    "uri",
    "signed_url",
    "public_id",
    "cpf",
    "cnpj",
    "email",
    "telefone",
    "banco",
    "conta",
    "agencia",
}
PADRAO_CREDENCIAL_URL = re.compile(
    r"(?i)\b([a-z][a-z0-9+.-]*://)([^/\s:@]+):([^/\s@]+)@"
)
PADRAO_SEGREDO_TEXTO = re.compile(
    r"(?i)\b(password|senha|secret|token|authorization|cookie|csrf|api[_-]?key|"
    r"database[_-]?url|redis[_-]?(?:url|uri)?|signed[_-]?url)\b"
    r"(\s*[:=]\s*)([^\s,;]+)"
)
PADRAO_BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")
PADRAO_EMAIL = re.compile(
    r"(?i)\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b"
)
PADRAO_DOCUMENTO = re.compile(
    r"(?<![A-Za-z0-9])(?:\d{3}[.\s-]?){3}\d{2}(?![A-Za-z0-9])|"
    r"(?<![A-Za-z0-9])\d{2}[.\s-]?\d{3}[.\s-]?\d{3}[/\s-]?\d{4}[-\s]?"
    r"\d{2}(?![A-Za-z0-9])"
)


def _chave_sensivel(chave: Any) -> bool:
    try:
        texto = str(chave or "")
    except Exception:
        return True
    normalizada = re.sub(r"[^a-z0-9]+", "_", texto.lower()).strip("_")
    tokens = set(normalizada.split("_"))
    for termo in CHAVES_SENSIVEIS:
        if "_" not in termo and termo in tokens:
            return True
        if "_" in termo and (
            normalizada == termo
            or normalizada.startswith(f"{termo}_")
            or normalizada.endswith(f"_{termo}")
            or f"_{termo}_" in f"_{normalizada}_"
        ):
            return True
    return False


def redigir_texto(valor: Any) -> str:
    """Remove padrões sensíveis sem tentar registrar o corpo da requisição."""
    if isinstance(valor, str):
        texto = valor
    else:
        try:
            texto = str(valor)
        except Exception:
            texto = f"<{type(valor).__name__}>"
    texto = PADRAO_CREDENCIAL_URL.sub(r"\1[REDACTED]@", texto)
    texto = PADRAO_SEGREDO_TEXTO.sub(
        lambda achado: f"{achado.group(1)}{achado.group(2)}{VALOR_REDACTADO}",
        texto,
    )
    texto = PADRAO_BEARER.sub(VALOR_REDACTADO, texto)
    texto = PADRAO_EMAIL.sub(VALOR_REDACTADO, texto)
    texto = PADRAO_DOCUMENTO.sub(VALOR_REDACTADO, texto)
    texto = texto.replace("\r", "\\r").replace("\n", "\\n")
    if len(texto) > MAX_TEXTO_LOG:
        return f"{texto[:MAX_TEXTO_LOG]}{VALOR_TRUNCADO}"
    return texto


def _nome_chave_seguro(chave: Any) -> str:
    if isinstance(chave, str):
        return redigir_texto(chave)
    if isinstance(chave, (bool, int, float, Decimal, date, datetime, uuid.UUID)):
        return redigir_texto(chave)
    return f"<{type(chave).__name__}>"


def redigir_dados(
    valor: Any,
    *,
    chave: Any = None,
    _profundidade: int = 0,
    _vistos: set[int] | None = None,
) -> Any:
    """Redige recursivamente estruturas destinadas explicitamente ao log."""
    if chave is not None and _chave_sensivel(chave):
        return VALOR_REDACTADO
    if _profundidade > MAX_PROFUNDIDADE_LOG:
        return VALOR_TRUNCADO
    if _vistos is None:
        _vistos = set()
    if isinstance(valor, (dict, list, tuple, set, frozenset)):
        identificador = id(valor)
        if identificador in _vistos:
            return VALOR_CIRCULAR
        _vistos.add(identificador)
    else:
        identificador = None
    if isinstance(valor, dict):
        resultado = {}
        for indice, (nome, conteudo) in enumerate(valor.items()):
            if indice >= MAX_ITENS_LOG:
                resultado[VALOR_TRUNCADO] = True
                break
            nome_seguro = _nome_chave_seguro(nome)
            resultado[nome_seguro] = redigir_dados(
                conteudo,
                chave=nome,
                _profundidade=_profundidade + 1,
                _vistos=_vistos,
            )
        _vistos.discard(identificador)
        return resultado
    if isinstance(valor, (list, tuple, set, frozenset)):
        resultado = [
            redigir_dados(
                item,
                _profundidade=_profundidade + 1,
                _vistos=_vistos,
            )
            for item in list(valor)[:MAX_ITENS_LOG]
        ]
        if len(valor) > MAX_ITENS_LOG:
            resultado.append(VALOR_TRUNCADO)
        _vistos.discard(identificador)
        return resultado
    if isinstance(valor, BaseException):
        return {"error_type": type(valor).__name__}
    if isinstance(valor, str):
        return redigir_texto(valor)
    if valor is None or isinstance(valor, (bool, int)):
        return valor
    if isinstance(valor, float):
        return valor if math.isfinite(valor) else f"[{str(valor).upper()}]"
    if isinstance(valor, (Decimal, date, datetime, uuid.UUID)):
        return redigir_texto(valor)
    return f"<{type(valor).__name__}>"


def _timestamp_utc() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def obter_request_id() -> str | None:
    if not has_request_context():
        return None
    return getattr(g, "request_id", None)


def garantir_request_id() -> str | None:
    """Cria uma referência mesmo quando a rejeição antecede o before_request."""
    if not has_request_context():
        return None
    request_id = getattr(g, "request_id", None)
    if not request_id:
        request_id = uuid.uuid4().hex
        g.request_id = request_id
    return request_id


def _ator_atual() -> tuple[str | None, str | None]:
    if not has_request_context():
        return None, None
    try:
        # Não força o carregamento da sessão e, portanto, não cria consulta ao
        # banco apenas para enriquecer um log. Usa o usuário somente se a rota
        # ou um decorator já o tiver carregado.
        usuario = getattr(g, "_login_user", None)
        if usuario is not None and usuario.is_authenticated:
            return str(usuario.get_id()), "internal_user"
    except Exception:
        pass
    return None, "anonymous"


def _contexto_requisicao() -> dict[str, Any]:
    if not has_request_context():
        return {}
    contexto: dict[str, Any] = {
        "request_id": garantir_request_id(),
        "method": request.method,
        "endpoint": request.endpoint or "unmatched",
    }
    ator_id, _ator_tipo = _ator_atual()
    if ator_id is not None:
        contexto["actor_id"] = ator_id
    return contexto


def _evento_estavel(valor: Any) -> str:
    try:
        evento = str(valor or "")
    except Exception:
        return "application_log"
    return evento if EVENTO_VALIDO.fullmatch(evento) else "application_log"


def _mensagem_record_segura(record: logging.LogRecord, *, online: bool) -> str:
    if record.exc_info and online:
        return "Falha interna registrada."
    try:
        return redigir_texto(record.getMessage())
    except Exception:
        return "Mensagem de log indisponível."


class FormatadorJsonSeguro(logging.Formatter):
    """Transforma cada evento em uma única linha JSON, sem traceback bruto."""

    def __init__(self, ambiente: str):
        super().__init__()
        self.ambiente = ambiente

    def format(self, record: logging.LogRecord) -> str:
        campos_extras = getattr(record, "structured_fields", {})
        if not isinstance(campos_extras, dict):
            campos_extras = {}
        extras_filtrados = {}
        for chave, valor in campos_extras.items():
            chave_segura = _nome_chave_seguro(chave)
            if chave_segura not in CAMPOS_RESERVADOS:
                extras_filtrados[chave_segura] = valor
        evento = redigir_dados({
            **extras_filtrados,
            **_contexto_requisicao(),
            "timestamp": _timestamp_utc(),
            "level": (
                record.levelname
                if record.levelname in NIVEIS_VALIDOS
                else "INFO"
            ),
            "event": _evento_estavel(
                getattr(record, "event", "application_log")
            ),
            "message": _mensagem_record_segura(
                record,
                online=self.ambiente in AMBIENTES_ONLINE,
            ),
            "environment": self.ambiente,
        })
        if record.exc_info and "error_type" not in evento:
            evento["error_type"] = record.exc_info[0].__name__
        return json.dumps(
            {chave: valor for chave, valor in evento.items() if valor is not None},
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )


class FormatadorTextoSeguro(logging.Formatter):
    """Formato local legível, ainda com redação e correlação."""

    def format(self, record: logging.LogRecord) -> str:
        contexto = _contexto_requisicao()
        partes = [
            _timestamp_utc(),
            record.levelname,
            f"event={_evento_estavel(getattr(record, 'event', 'application_log'))}",
        ]
        if contexto.get("request_id"):
            partes.append(f"request_id={contexto['request_id']}")
        partes.append(
            _mensagem_record_segura(
                record,
                online=bool(record.exc_info and not (
                    has_app_context() and current_app.debug
                )),
            )
        )
        if (
            record.exc_info
            and has_app_context()
            and current_app.debug
        ):
            partes.append(redigir_texto(self.formatException(record.exc_info)))
        return " | ".join(partes)


class HandlerStreamSeguro(logging.StreamHandler):
    """Não deixa falha de formatação ou saída derrubar a requisição."""

    def emit(self, record: logging.LogRecord) -> None:
        try:
            mensagem = self.format(record)
            stream = self.stream
            stream.write(mensagem + self.terminator)
            self.flush()
        except Exception:
            try:
                sys.stderr.write(
                    '{"level":"ERROR","event":"logging_failure",'
                    '"message":"Falha segura na emissão do log."}\n'
                )
            except Exception:
                pass


class FiltroRuidoSensivel(logging.Filter):
    """Evita duplicação e exposição da chave interna de bibliotecas de segurança."""

    def filter(self, record: logging.LogRecord) -> bool:
        nome = record.name.lower().replace("-", "_")
        if nome.startswith(("flask_limiter", "flask_wtf.csrf")):
            return False
        return True


def _ler_booleano_log(nome: str, *, padrao: bool) -> bool:
    import os

    valor = os.getenv(nome)
    if valor is None or not valor.strip():
        return padrao
    normalizado = valor.strip().lower()
    if normalizado in {"1", "true", "yes", "on"}:
        return True
    if normalizado in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{nome} deve usar true ou false.")


def ler_configuracao_logs(ambiente: str) -> dict[str, Any]:
    import os

    if ambiente == "testing":
        nivel_recebido = (os.getenv("LOG_LEVEL") or "INFO").strip().upper()
        formato_recebido = (os.getenv("LOG_FORMAT") or "json").strip().lower()
        if nivel_recebido not in NIVEIS_VALIDOS:
            raise RuntimeError("LOG_LEVEL possui valor inválido.")
        if formato_recebido not in FORMATOS_VALIDOS:
            raise RuntimeError("LOG_FORMAT possui valor inválido.")
        _ler_booleano_log("LOG_REQUESTS", padrao=True)
        _ler_booleano_log("LOG_SECURITY_EVENTS", padrao=True)
        return {
            "level": "INFO",
            "format": "json",
            "requests": True,
            "security_events": True,
        }
    nivel = (os.getenv("LOG_LEVEL") or "INFO").strip().upper()
    formato = (os.getenv("LOG_FORMAT") or (
        "json" if ambiente != "development" else "text"
    )).strip().lower()
    if nivel not in NIVEIS_VALIDOS:
        raise RuntimeError("LOG_LEVEL possui valor inválido.")
    if formato not in FORMATOS_VALIDOS:
        raise RuntimeError("LOG_FORMAT possui valor inválido.")
    if ambiente in AMBIENTES_ONLINE and nivel == "DEBUG":
        raise RuntimeError("LOG_LEVEL DEBUG não é permitido online.")
    if ambiente in AMBIENTES_ONLINE and formato != "json":
        raise RuntimeError("LOG_FORMAT deve ser json online.")
    requisicoes = _ler_booleano_log("LOG_REQUESTS", padrao=True)
    seguranca = _ler_booleano_log("LOG_SECURITY_EVENTS", padrao=True)
    if ambiente in AMBIENTES_ONLINE and not seguranca:
        raise RuntimeError("LOG_SECURITY_EVENTS deve permanecer ativo online.")
    return {
        "level": nivel,
        "format": formato,
        "requests": requisicoes,
        "security_events": seguranca,
    }


def emitir_erro_configuracao_minimo(ambiente: str) -> None:
    registro = {
        "timestamp": _timestamp_utc(),
        "level": "CRITICAL",
        "event": "application_configuration_error",
        "message": "A configuração de logs é inválida.",
        "environment": ambiente,
        "security_category": "configuration",
    }
    try:
        sys.stderr.write(
            json.dumps(registro, ensure_ascii=False, separators=(",", ":")) + "\n"
        )
    except Exception:
        pass


def registrar_evento(
    evento: str,
    *,
    nivel: str = "INFO",
    mensagem: str | None = None,
    categoria_seguranca: str | None = None,
    logger: logging.Logger | None = None,
    **campos: Any,
) -> None:
    """Registra somente metadados previamente escolhidos pelo chamador."""
    try:
        if (
            categoria_seguranca
            and has_app_context()
            and not current_app.config.get("LOG_SECURITY_EVENTS", True)
        ):
            return
        logger_destino = logger
        if logger_destino is None:
            logger_destino = (
                current_app.logger
                if has_app_context()
                else logging.getLogger("sistema_recic3")
            )
        campos_seguros = redigir_dados(campos)
        if categoria_seguranca:
            campos_seguros["security_category"] = categoria_seguranca
        nivel_seguro = nivel if nivel in NIVEIS_VALIDOS else "INFO"
        logger_destino.log(
            getattr(logging, nivel_seguro),
            (
                mensagem
                if mensagem is not None
                else _evento_estavel(evento).replace("_", " ")
            ),
            extra={
                "event": _evento_estavel(evento),
                "structured_fields": campos_seguros,
            },
        )
    except Exception:
        try:
            sys.stderr.write(
                '{"level":"ERROR","event":"logging_failure",'
                '"message":"Falha segura no registro do log."}\n'
            )
        except Exception:
            pass


def resposta_erro_interno(erro: BaseException):
    """Gera resposta 500 pública e registra apenas o tipo técnico da falha."""
    erro_original = getattr(erro, "original_exception", None) or erro
    referencia = garantir_request_id()
    registrar_evento(
        "internal_error",
        nivel="ERROR",
        mensagem="Falha interna ao processar a solicitação.",
        error_type=type(erro_original).__name__,
        status_code=500,
    )
    mensagem = "Não foi possível processar a solicitação."
    classificador_json = current_app.config.get("JSON_ENDPOINT_CLASSIFIER")
    try:
        resposta_json = bool(classificador_json and classificador_json())
    except Exception:
        resposta_json = False
    if resposta_json:
        conteudo = {"error": mensagem}
        if referencia:
            conteudo["request_id"] = referencia
        return jsonify(conteudo), 500
    try:
        return render_template("erro_500.html", request_id=referencia), 500
    except Exception:
        return Response(
            "Não foi possível processar a solicitação.",
            status=500,
            content_type="text/plain; charset=utf-8",
        )


def configurar_logging_operacional(app, ambiente: str) -> dict[str, Any]:
    """Configura stdout e os hooks de correlação, sem arquivos ou serviços externos."""
    try:
        configuracao = ler_configuracao_logs(ambiente)
    except RuntimeError:
        emitir_erro_configuracao_minimo(ambiente)
        raise

    formatador: logging.Formatter
    if configuracao["format"] == "json":
        formatador = FormatadorJsonSeguro(ambiente)
    else:
        formatador = FormatadorTextoSeguro()
    configuracao_existente = app.extensions.get("recic3_operational_logging")
    if configuracao_existente:
        return configuracao_existente

    handler = HandlerStreamSeguro(sys.stdout)
    handler.setFormatter(formatador)
    handler.addFilter(FiltroRuidoSensivel())
    handler.setLevel(getattr(logging, configuracao["level"]))
    handler._recic3_operational = True  # type: ignore[attr-defined]

    raiz = logging.getLogger()
    for existente in list(raiz.handlers):
        raiz.removeHandler(existente)
    raiz.addHandler(handler)
    raiz.setLevel(getattr(logging, configuracao["level"]))
    for nome_terceiro in (
        "cloudinary",
        "redis",
        "requests",
        "urllib3",
        "werkzeug",
    ):
        logging.getLogger(nome_terceiro).setLevel(logging.WARNING)

    app.logger.handlers.clear()
    app.logger.propagate = True
    app.logger.setLevel(getattr(logging, configuracao["level"]))
    app.config.update(
        LOG_LEVEL=configuracao["level"],
        LOG_FORMAT=configuracao["format"],
        LOG_REQUESTS=configuracao["requests"],
        LOG_SECURITY_EVENTS=configuracao["security_events"],
    )
    app.extensions["recic3_operational_logging"] = configuracao

    # O evento central internal_error já representa exceções não tratadas. Esta
    # substituição evita que o Flask emita antes uma cópia genérica com exc_info.
    app.log_exception = lambda _exc_info: None

    @app.before_request
    def iniciar_contexto_operacional():
        garantir_request_id()
        g.request_started_monotonic = time.monotonic()

    @app.after_request
    def finalizar_contexto_operacional(resposta):
        request_id = garantir_request_id()
        if request_id:
            resposta.headers["X-Request-ID"] = request_id
        inicio = getattr(g, "request_started_monotonic", None)
        duracao_ms = max(0.0, (time.monotonic() - inicio) * 1000) if inicio else 0.0
        endpoint = request.endpoint or "unmatched"
        if (
            app.config.get("LOG_REQUESTS", True)
            and endpoint not in {"health", "static", "fiscalizacao_contratos.static"}
        ):
            registrar_evento(
                "request_completed",
                mensagem="Requisição concluída.",
                status_code=resposta.status_code,
                duration_ms=round(duracao_ms, 3),
            )
        if resposta.status_code == 403:
            registrar_evento(
                "authorization_denied",
                nivel="WARNING",
                mensagem="Acesso não autorizado.",
                categoria_seguranca="authorization",
                status_code=403,
            )
        return resposta

    return configuracao


def registrar_inicio_aplicacao(app) -> None:
    """Emite o startup somente depois das configurações centrais concluírem."""
    registrar_evento(
        "application_startup",
        mensagem="Aplicação configurada.",
        logger=app.logger,
        log_format=app.config["LOG_FORMAT"],
        protections={
            "request_logging": app.config["LOG_REQUESTS"],
            "security_events": app.config["LOG_SECURITY_EVENTS"],
            "content_security_policy": True,
            "rate_limit": app.config.get("RATELIMIT_ENABLED", False),
            "trusted_hosts": bool(app.config.get("TRUSTED_HOSTS")),
        },
    )
