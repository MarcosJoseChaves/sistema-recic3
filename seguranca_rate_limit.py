"""Limitação de requisições por cliente, sem contador global improvisado."""

import os
import re
from urllib.parse import urlsplit

from flask import Response, current_app, jsonify, render_template, request
from flask_limiter import Limiter
from flask_login import current_user

from logging_operacional import registrar_evento


AMBIENTES_ONLINE = {"homologation", "production"}
PADRAO_LIMITE = re.compile(
    r"^[1-9][0-9]{0,3}\s+per\s+(second|minute|hour|day)$",
    re.IGNORECASE,
)
LIMITES_PADRAO = {
    "RATELIMIT_DEFAULT": "300 per minute",
    "RATELIMIT_LOGIN": "5 per minute",
    "RATELIMIT_REPORTS": "10 per minute",
    "RATELIMIT_EXTERNAL_LOOKUPS": "30 per minute",
    "RATELIMIT_UPLOADS": "10 per minute",
    "RATELIMIT_MUTATIONS": "60 per minute",
    "RATELIMIT_DOWNLOADS": "30 per minute",
}
ENDPOINTS_CONSULTA_EXTERNA = {
    "buscar_cep",
    "buscar_cnpj",
    "fiscalizacao_contratos.empresas_consultar_cep",
    "fiscalizacao_contratos.empresas_consultar_cnpj",
}
ENDPOINTS_UPLOAD = {
    "cadastrar_associado",
    "editar_associado",
    "cadastrar_patrimonio",
    "editar_patrimonio",
    "fiscalizacao_contratos.documentos_novo",
    "fiscalizacao_contratos.medicoes_documento_enviar",
    "fiscalizacao_contratos.atestes_nota_nova",
    "fiscalizacao_contratos.atestes_nota_editar",
}
ENDPOINTS_DOWNLOAD = {
    "fiscalizacao_contratos.documentos_arquivo",
}
ENDPOINTS_EXPOSTOS_ESPECIAIS = {
    "registrar_denuncia",
    "sucesso_denuncia",
}
PREFIXOS_RELATORIO = (
    "baixar_csv",
    "baixar_pdf",
    "gerar_extrato",
    "gerar_relatorio",
    "imprimir_ficha",
)


def _ler_booleano_rate_limit(nome, *, padrao=None):
    valor = os.getenv(nome)
    if valor is None or not valor.strip():
        if padrao is None:
            raise RuntimeError(f"{nome} é obrigatória neste ambiente.")
        return padrao
    normalizado = valor.strip().lower()
    if normalizado in {"1", "true", "yes", "on"}:
        return True
    if normalizado in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{nome} deve usar true ou false.")


def _ler_limite(nome):
    valor = (os.getenv(nome) or LIMITES_PADRAO[nome]).strip()
    if not PADRAO_LIMITE.fullmatch(valor):
        raise RuntimeError(
            f"{nome} deve usar o formato seguro 'n per minute', sem listas."
        )
    return " ".join(valor.lower().split())


def _ler_storage_uri(ambiente):
    valor_bruto = os.getenv("RATELIMIT_STORAGE_URI")
    if valor_bruto is None or not valor_bruto.strip():
        if ambiente in AMBIENTES_ONLINE:
            raise RuntimeError(
                "RATELIMIT_STORAGE_URI é obrigatória neste ambiente."
            )
        return "memory://"

    if valor_bruto != valor_bruto.strip() or any(
        caractere.isspace() for caractere in valor_bruto
    ):
        raise RuntimeError("RATELIMIT_STORAGE_URI possui formato inválido.")

    valor = valor_bruto
    try:
        partes = urlsplit(valor)
        esquema = partes.scheme.lower()
        hostname = partes.hostname
        _ = partes.port
    except ValueError as erro:
        raise RuntimeError("RATELIMIT_STORAGE_URI possui formato inválido.") from erro

    if esquema not in {"memory", "redis", "rediss"}:
        raise RuntimeError(
            "RATELIMIT_STORAGE_URI deve usar memory://, redis:// ou rediss://."
        )
    if esquema == "memory" and valor != "memory://":
        raise RuntimeError("RATELIMIT_STORAGE_URI memory possui formato inválido.")
    if esquema in {"redis", "rediss"} and (
        not hostname or partes.fragment or not partes.netloc
    ):
        raise RuntimeError("RATELIMIT_STORAGE_URI possui formato inválido.")
    if ambiente == "homologation" and esquema == "memory":
        autorizado = _ler_booleano_rate_limit(
            "RATELIMIT_ALLOW_MEMORY_HOMOLOGATION",
            padrao=False,
        )
        if not autorizado:
            raise RuntimeError(
                "Homologação com memory:// exige autorização explícita."
            )
    if ambiente == "production" and esquema == "memory":
        raise RuntimeError(
            "Produção exige armazenamento compartilhado para o rate limit."
        )
    return valor


def _chave_cliente():
    """Usa somente a origem já normalizada pelo ProxyFix e o ID interno opcional."""
    origem = request.remote_addr or "origem-indefinida"
    if hasattr(current_app, "login_manager") and current_user.is_authenticated:
        return f"ip:{origem}|usuario:{current_user.get_id()}"
    return f"ip:{origem}"


def _rota_isenta():
    return request.endpoint in {
        "health",
        "static",
        "fiscalizacao_contratos.static",
    }


def configurar_rate_limit(app, ambiente):
    """Valida a configuração e registra o limitador antes das demais barreiras."""
    online = ambiente in AMBIENTES_ONLINE
    ativo = _ler_booleano_rate_limit(
        "RATELIMIT_ENABLED",
        padrao=None if online else ambiente == "development",
    )
    if online and not ativo:
        raise RuntimeError("RATELIMIT_ENABLED não pode ser false online.")

    storage_uri = _ler_storage_uri(ambiente)
    limites = {nome: _ler_limite(nome) for nome in LIMITES_PADRAO}
    app.config.update(
        RATELIMIT_ENABLED=ativo,
        RATELIMIT_STORAGE_URI=storage_uri,
        RATELIMIT_DEFAULT=limites["RATELIMIT_DEFAULT"],
        RATELIMIT_LOGIN=limites["RATELIMIT_LOGIN"],
        RATELIMIT_REPORTS=limites["RATELIMIT_REPORTS"],
        RATELIMIT_EXTERNAL_LOOKUPS=limites["RATELIMIT_EXTERNAL_LOOKUPS"],
        RATELIMIT_UPLOADS=limites["RATELIMIT_UPLOADS"],
        RATELIMIT_MUTATIONS=limites["RATELIMIT_MUTATIONS"],
        RATELIMIT_DOWNLOADS=limites["RATELIMIT_DOWNLOADS"],
        RATELIMIT_STORAGE_IS_MEMORY=storage_uri == "memory://",
    )

    limiter = Limiter(
        key_func=_chave_cliente,
        app=app,
        default_limits=[limites["RATELIMIT_DEFAULT"]],
        default_limits_exempt_when=_rota_isenta,
        storage_uri=storage_uri,
        enabled=ativo,
        headers_enabled=True,
        retry_after="delta-seconds",
        strategy="fixed-window",
        swallow_errors=False,
        in_memory_fallback_enabled=False,
        key_prefix="sistema-recic3",
    )
    app.extensions["recic3_rate_limiter"] = limiter

    @app.errorhandler(429)
    def tratar_limite_excedido(_erro):
        registrar_evento(
            "rate_limit_exceeded",
            nivel="WARNING",
            mensagem="Limite de solicitações excedido.",
            categoria_seguranca="rate_limit",
            status_code=429,
        )
        classificador_json = current_app.config.get("JSON_ENDPOINT_CLASSIFIER")
        if classificador_json and classificador_json():
            resposta = jsonify(
                {"error": "Muitas solicitações. Aguarde e tente novamente."}
            )
        else:
            resposta = Response(
                render_template("erro_429.html"),
                content_type="text/html; charset=utf-8",
            )
        resposta.status_code = 429
        resposta.headers["Cache-Control"] = "no-store, private, max-age=0"
        resposta.headers["Pragma"] = "no-cache"
        return resposta

    return limiter


def _grupo_da_rota(endpoint, metodos):
    if endpoint == "login" and "POST" in metodos:
        return "login", {"POST"}
    if endpoint in ENDPOINTS_CONSULTA_EXTERNA:
        return "external_lookups", metodos
    if endpoint in ENDPOINTS_UPLOAD and "POST" in metodos:
        return "uploads", {"POST"}
    if endpoint in ENDPOINTS_DOWNLOAD:
        return "downloads", metodos
    if endpoint.startswith(PREFIXOS_RELATORIO):
        return "reports", metodos
    if endpoint in ENDPOINTS_EXPOSTOS_ESPECIAIS:
        return "mutations", metodos
    mutaveis = metodos.intersection({"POST", "PUT", "PATCH", "DELETE"})
    if mutaveis:
        return "mutations", mutaveis
    return None, set()


def aplicar_limites_rotas(app):
    """Aplica grupos específicos depois que todas as rotas foram registradas."""
    limiter = app.extensions["recic3_rate_limiter"]
    limites = {
        "login": app.config["RATELIMIT_LOGIN"],
        "reports": app.config["RATELIMIT_REPORTS"],
        "external_lookups": app.config["RATELIMIT_EXTERNAL_LOOKUPS"],
        "uploads": app.config["RATELIMIT_UPLOADS"],
        "mutations": app.config["RATELIMIT_MUTATIONS"],
        "downloads": app.config["RATELIMIT_DOWNLOADS"],
    }
    protegidos = {}

    for regra in app.url_map.iter_rules():
        endpoint = regra.endpoint
        if endpoint in {"health", "static", "fiscalizacao_contratos.static"}:
            continue
        if (
            app.config.get("APP_ENV") in AMBIENTES_ONLINE
            and endpoint in ENDPOINTS_EXPOSTOS_ESPECIAIS
        ):
            # Essas rotas são deliberadamente ocultas online. A isenção evita
            # que repetição transforme o 404 em 429 e revele sua existência.
            app.view_functions[endpoint] = limiter.exempt(
                app.view_functions[endpoint]
            )
            continue
        metodos = set(regra.methods).difference({"HEAD", "OPTIONS"})
        grupo, metodos_limitados = _grupo_da_rota(endpoint, metodos)
        if not grupo or endpoint in protegidos:
            continue
        app.view_functions[endpoint] = limiter.shared_limit(
            limites[grupo],
            scope=f"grupo:{grupo}",
            methods=sorted(metodos_limitados),
            override_defaults=False,
        )(app.view_functions[endpoint])
        protegidos[endpoint] = {
            "grupo": grupo,
            "limite": limites[grupo],
            "metodos": tuple(sorted(metodos_limitados)),
        }

    app.config["RATELIMIT_PROTECTED_ENDPOINTS"] = protegidos
    return protegidos
