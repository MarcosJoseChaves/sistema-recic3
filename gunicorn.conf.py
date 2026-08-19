"""Configuração explícita e validada para o Gunicorn no ambiente online."""

import os


AMBIENTES_GUNICORN = {"homologation", "production"}


def _inteiro_ambiente(nome, *, padrao=None, minimo, maximo):
    valor = os.getenv(nome)
    if valor is None or not valor.strip():
        if padrao is None:
            raise RuntimeError(f"{nome} é obrigatória para iniciar o Gunicorn.")
        return padrao
    try:
        numero = int(valor)
    except ValueError as erro:
        raise RuntimeError(f"{nome} deve ser um número inteiro.") from erro
    if not minimo <= numero <= maximo:
        raise RuntimeError(f"{nome} está fora do intervalo permitido.")
    return numero


ambiente = (os.getenv("APP_ENV") or "").strip().lower()
if ambiente not in AMBIENTES_GUNICORN:
    raise RuntimeError(
        "Gunicorn exige APP_ENV=homologation ou APP_ENV=production."
    )

porta = _inteiro_ambiente("PORT", minimo=1, maximo=65535)

bind = f"0.0.0.0:{porta}"
workers = _inteiro_ambiente(
    "WEB_CONCURRENCY",
    padrao=2,
    minimo=1,
    maximo=8,
)
worker_class = "gthread"
threads = _inteiro_ambiente(
    "GUNICORN_THREADS",
    padrao=4,
    minimo=1,
    maximo=16,
)
timeout = _inteiro_ambiente(
    "GUNICORN_TIMEOUT",
    padrao=60,
    minimo=30,
    maximo=300,
)
graceful_timeout = _inteiro_ambiente(
    "GUNICORN_GRACEFUL_TIMEOUT",
    padrao=30,
    minimo=10,
    maximo=120,
)
keepalive = 5

preload_app = False
reload = False
daemon = False

max_requests = 1000
max_requests_jitter = 100
limit_request_line = 4094
limit_request_fields = 100
limit_request_field_size = 8190

accesslog = "-"
errorlog = "-"
loglevel = "info"
capture_output = True
access_log_format = '%(t)s %(p)s %(h)s "%(m)s %(U)s" %(s)s %(L)s'
proc_name = "sistema-recic3"
