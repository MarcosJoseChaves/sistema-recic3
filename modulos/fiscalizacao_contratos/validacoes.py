"""Normalização e validação dos dados do módulo."""

import re


def somente_numeros(valor):
    return re.sub(r"\D", "", valor or "")


def validar_cnpj(cnpj):
    cnpj = somente_numeros(cnpj)
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14:
        return False

    pesos_primeiro = (5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
    soma = sum(int(digito) * peso for digito, peso in zip(cnpj[:12], pesos_primeiro))
    resto = soma % 11
    primeiro = 0 if resto < 2 else 11 - resto
    if int(cnpj[12]) != primeiro:
        return False

    pesos_segundo = (6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2)
    soma = sum(int(digito) * peso for digito, peso in zip(cnpj[:13], pesos_segundo))
    resto = soma % 11
    segundo = 0 if resto < 2 else 11 - resto
    return int(cnpj[13]) == segundo


def validar_cep(cep):
    return len(somente_numeros(cep)) == 8


def validar_email(email):
    if not email:
        return True
    return bool(re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", email))


def normalizar_e_validar_empresa(formulario):
    dados = {
        "cnpj": somente_numeros(formulario.get("cnpj")),
        "razao_social": (formulario.get("razao_social") or "").strip(),
        "nome_fantasia": (formulario.get("nome_fantasia") or "").strip() or None,
        "cep": somente_numeros(formulario.get("cep")),
        "logradouro": (formulario.get("logradouro") or "").strip() or None,
        "numero": (formulario.get("numero") or "").strip() or None,
        "bairro": (formulario.get("bairro") or "").strip() or None,
        "cidade": (formulario.get("cidade") or "").strip() or None,
        "uf": (formulario.get("uf") or "").strip().upper() or None,
        "telefone": (formulario.get("telefone") or "").strip() or None,
        "email": (formulario.get("email") or "").strip().lower() or None,
    }

    erros = []
    if not validar_cnpj(dados["cnpj"]):
        erros.append("Informe um CNPJ válido.")
    if not dados["razao_social"]:
        erros.append("A razão social é obrigatória.")
    if not validar_cep(dados["cep"]):
        erros.append("Informe um CEP válido com 8 dígitos.")
    if dados["uf"] and not re.fullmatch(r"[A-Z]{2}", dados["uf"]):
        erros.append("A UF deve conter duas letras.")
    if not validar_email(dados["email"]):
        erros.append("Informe um e-mail válido.")

    return dados, erros


def formatar_cnpj(cnpj):
    cnpj = somente_numeros(cnpj)
    if len(cnpj) != 14:
        return cnpj
    return f"{cnpj[:2]}.{cnpj[2:5]}.{cnpj[5:8]}/{cnpj[8:12]}-{cnpj[12:]}"


def formatar_cep(cep):
    cep = somente_numeros(cep)
    if len(cep) != 8:
        return cep
    return f"{cep[:5]}-{cep[5:]}"
