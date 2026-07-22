"""Apoio para os testes enviarem CSRF real sem desabilitar a proteção."""

import re
from collections.abc import Mapping

from werkzeug.datastructures import MultiDict


PADRAO_TOKEN = re.compile(
    rb'name=["\']csrf_token["\'][^>]*value=["\']([^"\']+)', re.IGNORECASE
)


class ClienteComCSRF:
    """Envolve um test client e inclui um token válido nas escritas usuais."""

    def __init__(self, cliente):
        self._cliente = cliente

    def __getattr__(self, nome):
        return getattr(self._cliente, nome)

    def _obter_token(self):
        resposta = self._cliente.get("/login")
        encontrado = PADRAO_TOKEN.search(resposta.data)
        if not encontrado:
            raise AssertionError("O formulário de login não forneceu token CSRF.")
        return encontrado.group(1).decode()

    def _escrever(self, metodo, *args, **kwargs):
        token = self._obter_token()
        headers = dict(kwargs.pop("headers", {}) or {})
        if "json" in kwargs or "application/json" in (kwargs.get("content_type") or ""):
            headers.setdefault("X-CSRFToken", token)
        else:
            dados = kwargs.get("data")
            if dados is None:
                kwargs["data"] = {"csrf_token": token}
            elif isinstance(dados, MultiDict):
                dados = dados.copy()
                dados.setlistdefault("csrf_token", [token])
                kwargs["data"] = dados
            elif isinstance(dados, Mapping):
                dados = dict(dados)
                dados.setdefault("csrf_token", token)
                kwargs["data"] = dados
            else:
                headers.setdefault("X-CSRFToken", token)
        kwargs["headers"] = headers
        return getattr(self._cliente, metodo)(*args, **kwargs)

    def post(self, *args, **kwargs):
        return self._escrever("post", *args, **kwargs)

    def put(self, *args, **kwargs):
        return self._escrever("put", *args, **kwargs)

    def patch(self, *args, **kwargs):
        return self._escrever("patch", *args, **kwargs)

    def delete(self, *args, **kwargs):
        return self._escrever("delete", *args, **kwargs)
