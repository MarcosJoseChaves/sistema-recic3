"""Consultas opcionais para auxiliar o preenchimento manual."""

import requests

from ..validacoes import somente_numeros, validar_cep, validar_cnpj


class ConsultaExternaError(Exception):
    """Falha tratada de um serviço externo."""


def consultar_cnpj(cnpj):
    cnpj = somente_numeros(cnpj)
    if not validar_cnpj(cnpj):
        raise ValueError("CNPJ inválido.")

    try:
        resposta = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj}", timeout=8)
        resposta.raise_for_status()
        dados = resposta.json()
    except (requests.RequestException, ValueError) as erro:
        raise ConsultaExternaError(
            "Não foi possível consultar o CNPJ agora. Preencha os dados manualmente."
        ) from erro

    telefone = ""
    if dados.get("ddd_telefone_1") and dados.get("telefone_1"):
        telefone = f"({dados['ddd_telefone_1']}) {dados['telefone_1']}"

    return {
        "razao_social": dados.get("razao_social", ""),
        "nome_fantasia": dados.get("nome_fantasia", ""),
        "cep": somente_numeros(dados.get("cep"))[:8],
        "logradouro": dados.get("logradouro", ""),
        "numero": dados.get("numero", ""),
        "bairro": dados.get("bairro", ""),
        "cidade": dados.get("municipio", ""),
        "uf": dados.get("uf", ""),
        "telefone": telefone,
        "email": dados.get("email", ""),
    }


def consultar_cep(cep):
    cep = somente_numeros(cep)
    if not validar_cep(cep):
        raise ValueError("CEP inválido.")

    try:
        resposta = requests.get(f"https://brasilapi.com.br/api/cep/v1/{cep}", timeout=8)
        resposta.raise_for_status()
        dados = resposta.json()
    except (requests.RequestException, ValueError) as erro:
        raise ConsultaExternaError(
            "Não foi possível consultar o CEP agora. Preencha o endereço manualmente."
        ) from erro

    return {
        "logradouro": dados.get("street", ""),
        "bairro": dados.get("neighborhood", ""),
        "cidade": dados.get("city", ""),
        "uf": dados.get("state", ""),
    }
