"""Configurações de inicialização e segurança por ambiente."""

import hmac
import os

from flask import Response, request
from werkzeug.middleware.proxy_fix import ProxyFix


AMBIENTES_VALIDOS = {"development", "testing", "homologation", "production"}
AMBIENTES_HTTPS = {"homologation", "production"}


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
    )

    if ler_booleano("TRUST_PROXY", padrao=False):
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

    @app.after_request
    def adicionar_cabecalhos_seguranca(resposta):
        resposta.headers.setdefault("X-Content-Type-Options", "nosniff")
        resposta.headers.setdefault("X-Frame-Options", "SAMEORIGIN")
        resposta.headers.setdefault(
            "Referrer-Policy", "strict-origin-when-cross-origin"
        )
        if ambiente in AMBIENTES_HTTPS and request.is_secure:
            resposta.headers.setdefault(
                "Strict-Transport-Security", "max-age=31536000"
            )
        return resposta

    return ambiente
