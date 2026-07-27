"""Configurações de inicialização e segurança por ambiente."""

import hmac
import ipaddress
import os
import re

from flask import Response, jsonify, request
from werkzeug.exceptions import RequestEntityTooLarge
from werkzeug.middleware.proxy_fix import ProxyFix


AMBIENTES_VALIDOS = {"development", "testing", "homologation", "production"}
AMBIENTES_HTTPS = {"homologation", "production"}
HOSTS_LOCAIS_PADRAO = ["localhost", "127.0.0.1", "[::1]"]
MAX_HOSTS_CONFIAVEIS = 20
MAX_REQUEST_MB_PADRAO_LOCAL = 64
MAX_REQUEST_MB_MINIMO = 1
MAX_REQUEST_MB_MAXIMO = 128
PADRAO_ROTULO_HOST = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")


def ler_booleano(nome, padrao=False):
    """Lê uma variável booleana sem aceitar valores ambíguos."""
    valor = os.getenv(nome)
    if valor is None or not valor.strip():
        return padrao
    normalizado = valor.strip().lower()
    if normalizado in {"1", "true", "yes", "on"}:
        return True
    if normalizado in {"0", "false", "no", "off"}:
        return False
    raise RuntimeError(f"{nome} deve usar true ou false.")


def identificar_ambiente():
    """Retorna o ambiente explícito, usando desenvolvimento como padrão local."""
    ambiente = (os.getenv("APP_ENV") or "development").strip().lower()
    if ambiente not in AMBIENTES_VALIDOS:
        permitidos = ", ".join(sorted(AMBIENTES_VALIDOS))
        raise RuntimeError(f"APP_ENV inválido. Valores permitidos: {permitidos}.")
    return ambiente


def _valor_obrigatorio(nome):
    valor = (os.getenv(nome) or "").strip()
    if not valor:
        raise RuntimeError(f"{nome} é obrigatória e não foi configurada.")
    return valor


def _validar_prefixo_cloudinary(prefixo):
    if "\\" in prefixo:
        raise RuntimeError("CLOUDINARY_FOLDER_PREFIX possui formato inválido.")
    segmentos = [segmento.strip() for segmento in prefixo.split("/") if segmento.strip()]
    if any(segmento in {".", ".."} for segmento in segmentos):
        raise RuntimeError("CLOUDINARY_FOLDER_PREFIX possui formato inválido.")


def _normalizar_host_confiavel(valor):
    original = str(valor or "")
    if "\r" in original or "\n" in original:
        raise RuntimeError("TRUSTED_HOSTS contém um host inválido.")
    host = original.strip().lower()
    if not host:
        raise RuntimeError("TRUSTED_HOSTS contém um host vazio.")
    if host.endswith(".."):
        raise RuntimeError("TRUSTED_HOSTS contém um host inválido.")
    host = host[:-1] if host.endswith(".") else host
    if any(caractere in host for caractere in ("*", "/", "\\", "@", "?", "#")):
        raise RuntimeError("TRUSTED_HOSTS contém um host inválido.")
    if "://" in host or host.startswith("."):
        raise RuntimeError("TRUSTED_HOSTS aceita somente hosts exatos.")

    try:
        endereco = ipaddress.ip_address(host)
        return f"[{endereco.compressed}]" if endereco.version == 6 else endereco.compressed
    except ValueError:
        pass

    if ":" in host or len(host) > 253:
        raise RuntimeError("TRUSTED_HOSTS contém um host inválido.")
    rotulos = host.split(".")
    if any(not PADRAO_ROTULO_HOST.fullmatch(rotulo) for rotulo in rotulos):
        raise RuntimeError("TRUSTED_HOSTS contém um host inválido.")
    return host


def ler_hosts_confiaveis(ambiente):
    """Lê hosts exatos; ambientes online não aceitam lista implícita."""

    valor = os.getenv("TRUSTED_HOSTS")
    if valor is None or not valor.strip():
        if ambiente in AMBIENTES_HTTPS:
            raise RuntimeError("TRUSTED_HOSTS é obrigatória neste ambiente.")
        return list(HOSTS_LOCAIS_PADRAO)

    partes = valor.split(",")
    if len(partes) > MAX_HOSTS_CONFIAVEIS:
        raise RuntimeError("TRUSTED_HOSTS possui hosts demais.")

    resultado = []
    for parte in partes:
        host = _normalizar_host_confiavel(parte)
        if host not in resultado:
            resultado.append(host)
    return resultado


def ler_limite_requisicao_mb(ambiente):
    """Lê o limite global em MB sem aceitar valores ambíguos ou excessivos."""

    valor = os.getenv("MAX_REQUEST_MB")
    if valor is None or not valor.strip():
        if ambiente in AMBIENTES_HTTPS:
            raise RuntimeError("MAX_REQUEST_MB é obrigatória neste ambiente.")
        return MAX_REQUEST_MB_PADRAO_LOCAL
    try:
        limite = int(valor.strip())
    except (TypeError, ValueError) as erro:
        raise RuntimeError("MAX_REQUEST_MB deve ser um número inteiro.") from erro
    if not MAX_REQUEST_MB_MINIMO <= limite <= MAX_REQUEST_MB_MAXIMO:
        raise RuntimeError("MAX_REQUEST_MB está fora do intervalo permitido.")
    return limite


def configurar_aplicacao(app):
    """Aplica configurações seguras sem abrir conexões externas."""
    ambiente = identificar_ambiente()
    secret_key = _valor_obrigatorio("SECRET_KEY")
    database_url = (os.getenv("DATABASE_URL") or "").strip() or None

    if ambiente in AMBIENTES_HTTPS and not database_url:
        raise RuntimeError("DATABASE_URL é obrigatória neste ambiente.")

    prefixo_cloudinary = (os.getenv("CLOUDINARY_FOLDER_PREFIX") or "").strip()
    credenciais_cloudinary = [
        (os.getenv(nome) or "").strip()
        for nome in (
            "CLOUDINARY_CLOUD_NAME",
            "CLOUDINARY_API_KEY",
            "CLOUDINARY_API_SECRET",
        )
    ]
    if ambiente in AMBIENTES_HTTPS:
        if any(credenciais_cloudinary) and not all(credenciais_cloudinary):
            raise RuntimeError("A configuração do Cloudinary está incompleta.")
        if all(credenciais_cloudinary):
            if not prefixo_cloudinary.strip("/"):
                raise RuntimeError(
                    "CLOUDINARY_FOLDER_PREFIX é obrigatória quando o Cloudinary está configurado."
                )
            _validar_prefixo_cloudinary(prefixo_cloudinary)

    hosts_confiaveis = ler_hosts_confiaveis(ambiente)
    confiar_proxy = ler_booleano("TRUST_PROXY", padrao=False)
    if ambiente in AMBIENTES_HTTPS and not confiar_proxy:
        raise RuntimeError("TRUST_PROXY deve ser true neste ambiente.")
    limite_requisicao_mb = ler_limite_requisicao_mb(ambiente)

    app.config.update(
        APP_ENV=ambiente,
        SECRET_KEY=secret_key,
        DATABASE_URL=database_url,
        TESTING=ambiente == "testing",
        DEBUG=(
            ambiente == "development" and ler_booleano("APP_DEBUG", padrao=False)
        ),
        PREFERRED_URL_SCHEME="https" if ambiente in AMBIENTES_HTTPS else "http",
        SESSION_COOKIE_SECURE=ambiente in AMBIENTES_HTTPS,
        SESSION_COOKIE_HTTPONLY=True,
        SESSION_COOKIE_SAMESITE="Lax",
        REMEMBER_COOKIE_SECURE=ambiente in AMBIENTES_HTTPS,
        REMEMBER_COOKIE_HTTPONLY=True,
        REMEMBER_COOKIE_SAMESITE="Lax",
        TRUSTED_HOSTS=hosts_confiaveis,
        MAX_REQUEST_MB=limite_requisicao_mb,
        MAX_CONTENT_LENGTH=limite_requisicao_mb * 1024 * 1024,
    )

    if confiar_proxy:
        app.wsgi_app = ProxyFix(
            app.wsgi_app,
            x_for=1,
            x_proto=1,
        )

    barreira_ativa = (
        ambiente == "homologation"
        and ler_booleano("HOMOLOGATION_GATE_ENABLED", padrao=False)
    )
    usuario_barreira = None
    senha_barreira = None
    if barreira_ativa:
        usuario_barreira = _valor_obrigatorio("HOMOLOGATION_GATE_USER")
        senha_barreira = _valor_obrigatorio("HOMOLOGATION_GATE_PASSWORD")

    @app.before_request
    def proteger_homologacao():
        if not barreira_ativa or request.path == "/health":
            return None

        autenticacao = request.authorization
        usuario_recebido = autenticacao.username if autenticacao else ""
        senha_recebida = autenticacao.password if autenticacao else ""
        autorizado = hmac.compare_digest(usuario_recebido or "", usuario_barreira)
        autorizado = hmac.compare_digest(senha_recebida or "", senha_barreira) and autorizado
        if autorizado:
            return None

        return Response(
            "Autenticação necessária.",
            status=401,
            headers={"WWW-Authenticate": 'Basic realm="Acesso restrito"'},
            content_type="text/plain; charset=utf-8",
        )

    @app.errorhandler(RequestEntityTooLarge)
    def tratar_conteudo_excessivo(_erro):
        classificador_json = app.config.get("JSON_ENDPOINT_CLASSIFIER")
        if classificador_json and classificador_json():
            resposta = jsonify({"error": "Conteúdo muito grande."})
        else:
            resposta = Response(
                (
                    "<!doctype html><html lang=\"pt-BR\"><head>"
                    "<meta charset=\"utf-8\"><title>Conteúdo muito grande</title>"
                    "</head><body><main><h1>Conteúdo muito grande</h1>"
                    "<p>O arquivo ou formulário enviado ultrapassa o tamanho permitido.</p>"
                    "<p>Revise o conteúdo e tente novamente.</p></main></body></html>"
                ),
                content_type="text/html; charset=utf-8",
            )
        resposta.status_code = 413
        resposta.headers["Cache-Control"] = "no-store, private, max-age=0"
        resposta.headers["Pragma"] = "no-cache"
        return resposta

    @app.after_request
    def adicionar_cabecalhos_seguranca(resposta):
        classificador_json = app.config.get("JSON_ENDPOINT_CLASSIFIER")
        erros_json = {
            400: "Solicitação inválida.",
            401: "Autenticação necessária.",
            403: "Acesso não autorizado para este recurso.",
            404: "Recurso não encontrado.",
            405: "Método não permitido.",
            413: "Conteúdo muito grande.",
            500: "Não foi possível processar a solicitação.",
        }
        if (
            not resposta.is_json
            and resposta.status_code in erros_json
            and classificador_json
            and classificador_json()
        ):
            cabecalhos_preservados = {
                nome: resposta.headers[nome]
                for nome in ("Allow", "WWW-Authenticate")
                if nome in resposta.headers
            }
            codigo = resposta.status_code
            resposta = jsonify({"error": erros_json[codigo]})
            resposta.status_code = codigo
            resposta.headers.update(cabecalhos_preservados)

        resposta.headers.setdefault("X-Content-Type-Options", "nosniff")
        resposta.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
        resposta.headers.setdefault(
            "Referrer-Policy", "strict-origin-when-cross-origin"
        )
        if ambiente in AMBIENTES_HTTPS and request.is_secure:
            resposta.headers.setdefault(
                "Strict-Transport-Security", "max-age=31536000"
            )
        if resposta.is_json and request.endpoint != "health":
            resposta.headers.setdefault("Cache-Control", "no-store, private, max-age=0")
            resposta.headers.setdefault("Pragma", "no-cache")
        return resposta

    return ambiente
