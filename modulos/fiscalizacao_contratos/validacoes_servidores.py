"""Normalização e validação do cadastro de servidores."""

import re


def normalizar_e_validar_servidor(formulario):
    """Limpa os campos do formulário e devolve mensagens amigáveis."""
    dados = {
        "nome": (formulario.get("nome") or "").strip(),
        "matricula": re.sub(r"\s+", "", formulario.get("matricula") or ""),
        "cargo": (formulario.get("cargo") or "").strip() or None,
        "setor": (formulario.get("setor") or "").strip() or None,
        "email": (formulario.get("email") or "").strip().lower() or None,
        "telefone": (formulario.get("telefone") or "").strip() or None,
        "observacoes": (formulario.get("observacoes") or "").strip() or None,
    }

    erros = []
    if not dados["nome"]:
        erros.append("O nome é obrigatório.")
    if not dados["matricula"]:
        erros.append("A matrícula é obrigatória.")
    if dados["email"] and not re.fullmatch(
        r"[^\s@]+@[^\s@]+\.[^\s@]+", dados["email"]
    ):
        erros.append("Informe um e-mail válido.")

    return dados, erros
