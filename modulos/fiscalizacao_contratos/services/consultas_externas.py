"""Consultas opcionais para auxiliar o preenchimento manual."""

import logging

import requests

from logging_operacional import registrar_evento

from ..validacoes import somente_numeros, validar_cep, validar_cnpj


logger = logging.getLogger(__name__)


class ConsultaExternaError(Exception):
    """Falha tratada de um serviço externo."""


class _RespostaExternaInvalida(Exception):
    """Resposta que não pode ser usada pelo módulo."""

    def __init__(self, codigo):
        super().__init__("Resposta externa inválida.")
        self.codigo = codigo


def _texto(valor):
    """Converte um valor opcional em texto sem produzir a palavra 'None'."""
    if valor is None:
        return ""
    return str(valor).strip()


def _tipo_resumido_erro(erro):
    if isinstance(erro, requests.exceptions.SSLError):
        return "certificado_ssl"
    if isinstance(erro, requests.exceptions.Timeout):
        return "timeout"
    if isinstance(erro, requests.exceptions.HTTPError):
        return "resposta_http_invalida"
    if isinstance(erro, requests.exceptions.ConnectionError):
        return "conexao_dns_ou_rede"
    if isinstance(erro, _RespostaExternaInvalida):
        return erro.codigo
    return type(erro).__name__


def _status_http(erro):
    resposta = getattr(erro, "response", None)
    return getattr(resposta, "status_code", None) if resposta is not None else None


def _consultar_json(servico, url, *, segunda_opcao=False):
    """Consulta um serviço e registra somente informações técnicas resumidas."""
    logger.info(
        "Consulta externa iniciada: servico=%s segunda_opcao_acionada=%s",
        servico,
        segunda_opcao,
    )

    try:
        resposta = requests.get(url, timeout=8)
        logger.info(
            "Consulta externa respondida: servico=%s status_http=%s "
            "segunda_opcao_acionada=%s",
            servico,
            resposta.status_code,
            segunda_opcao,
        )
        resposta.raise_for_status()
        try:
            dados = resposta.json()
        except ValueError as erro:
            raise _RespostaExternaInvalida("json_invalido") from erro
        if not isinstance(dados, dict):
            raise _RespostaExternaInvalida("formato_json_inesperado")
        return dados
    except (requests.RequestException, _RespostaExternaInvalida) as erro:
        registrar_evento(
            "external_service_error",
            nivel="WARNING",
            mensagem="Falha em consulta externa.",
            service=servico,
            operation="lookup",
            status_code=_status_http(erro),
            error_type=_tipo_resumido_erro(erro),
            fallback_attempt=segunda_opcao,
        )
        raise


def _formatar_telefone(ddd, numero):
    ddd = _texto(ddd)
    numero = _texto(numero)
    if not numero:
        return ""
    return f"({ddd}) {numero}" if ddd else numero


def _telefone_brasilapi(dados):
    for sufixo in ("1", "2"):
        telefone = _formatar_telefone(
            dados.get(f"ddd_telefone_{sufixo}"),
            dados.get(f"telefone_{sufixo}"),
        )
        if telefone:
            return telefone
    return ""


def _telefone_opencnpja(telefones):
    if not isinstance(telefones, list):
        return ""
    for telefone in telefones:
        if isinstance(telefone, dict):
            valor = _formatar_telefone(telefone.get("area"), telefone.get("number"))
        else:
            valor = _texto(telefone)
        if valor:
            return valor
    return ""


def _email_opencnpja(emails):
    if not isinstance(emails, list):
        return ""
    for email in emails:
        if isinstance(email, dict):
            valor = _texto(
                email.get("address") or email.get("email") or email.get("value")
            )
        else:
            valor = _texto(email)
        if valor:
            return valor
    return ""


def _mapear_brasilapi(dados):
    return {
        "razao_social": _texto(dados.get("razao_social")),
        "nome_fantasia": _texto(dados.get("nome_fantasia")),
        "cep": somente_numeros(dados.get("cep"))[:8],
        "logradouro": _texto(dados.get("logradouro")),
        "numero": _texto(dados.get("numero")),
        "bairro": _texto(dados.get("bairro")),
        "cidade": _texto(dados.get("municipio")),
        "uf": _texto(dados.get("uf")).upper(),
        "telefone": _telefone_brasilapi(dados),
        "email": _texto(dados.get("email")),
    }


def _mapear_opencnpja(dados):
    empresa = dados.get("company") if isinstance(dados.get("company"), dict) else {}
    endereco = dados.get("address") if isinstance(dados.get("address"), dict) else {}
    return {
        "razao_social": _texto(empresa.get("name")),
        "nome_fantasia": _texto(dados.get("alias")),
        "cep": somente_numeros(endereco.get("zip"))[:8],
        "logradouro": _texto(endereco.get("street")),
        "numero": _texto(endereco.get("number")),
        "bairro": _texto(endereco.get("district")),
        "cidade": _texto(endereco.get("city")),
        "uf": _texto(endereco.get("state")).upper(),
        "telefone": _telefone_opencnpja(dados.get("phones")),
        "email": _email_opencnpja(dados.get("emails")),
    }


def consultar_cnpj(cnpj):
    cnpj = somente_numeros(cnpj)
    if not validar_cnpj(cnpj):
        raise ValueError("CNPJ inválido.")

    try:
        dados = _consultar_json(
            "BrasilAPI",
            f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}",
        )
        return _mapear_brasilapi(dados)
    except (requests.RequestException, _RespostaExternaInvalida):
        logger.info(
            "Consulta externa acionando alternativa: servico=OpenCNPJA "
            "segunda_opcao_acionada=True"
        )

    try:
        dados = _consultar_json(
            "OpenCNPJA",
            f"https://open.cnpja.com/office/{cnpj}",
            segunda_opcao=True,
        )
        return _mapear_opencnpja(dados)
    except (requests.RequestException, _RespostaExternaInvalida) as erro:
        raise ConsultaExternaError(
            "Não foi possível consultar o CNPJ agora. Preencha os dados manualmente."
        ) from erro


def consultar_cep(cep):
    cep = somente_numeros(cep)
    if not validar_cep(cep):
        raise ValueError("CEP inválido.")

    try:
        dados = _consultar_json(
            "BrasilAPI CEP",
            f"https://brasilapi.com.br/api/cep/v1/{cep}",
        )
    except (requests.RequestException, _RespostaExternaInvalida) as erro:
        raise ConsultaExternaError(
            "Não foi possível consultar o CEP agora. Preencha o endereço manualmente."
        ) from erro

    return {
        "logradouro": _texto(dados.get("street")),
        "bairro": _texto(dados.get("neighborhood")),
        "cidade": _texto(dados.get("city")),
        "uf": _texto(dados.get("state")).upper(),
    }
