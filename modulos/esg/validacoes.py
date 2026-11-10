"""Validações dos contratos ESG, sem coerção silenciosa ou acesso externo."""

from datetime import date
from decimal import Decimal
import re


class ContratoESGInvalido(ValueError):
    """Um campo ou vínculo do contrato ESG é inválido."""


def exigir_tipo(valor, tipo, campo):
    """Rejeita coerções e subclasses como bool/int e datetime/date."""
    if type(valor) is not tipo:
        raise ContratoESGInvalido(f"{campo}: tipo esperado {tipo.__name__}.")


def validar_texto(valor, campo):
    exigir_tipo(valor, str, campo)
    if not valor.strip() or valor != valor.strip() or any(ord(c) < 32 for c in valor if c not in "\n\t"):
        raise ContratoESGInvalido(f"{campo}: informe texto não vazio, sem espaços nas extremidades ou controles inválidos.")


def validar_codigo(valor, campo):
    validar_texto(valor, campo)
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_.-]{0,63}", valor):
        raise ContratoESGInvalido(f"{campo}: use até 64 caracteres, iniciando por letra, com letras ASCII, números, ponto, hífen ou sublinhado.")


def validar_ano_mes(ano, mes):
    exigir_tipo(ano, int, "ano")
    if not 1 <= ano <= 9999:
        raise ContratoESGInvalido("ano: informe um ano entre 1 e 9999.")
    if mes is not None:
        exigir_tipo(mes, int, "mes")
        if not 1 <= mes <= 12:
            raise ContratoESGInvalido("mes: informe um mês entre 1 e 12.")


def validar_data(valor, campo):
    exigir_tipo(valor, date, campo)


def validar_valor(valor):
    if valor is not None:
        exigir_tipo(valor, Decimal, "valor")
        if not valor.is_finite():
            raise ContratoESGInvalido("valor: NaN e infinito não são permitidos.")


def validar_aprovacao(situacao, tipo_situacao, fonte_aprovacao, tipo_fonte):
    exigir_tipo(situacao, tipo_situacao, "situacao")
    if fonte_aprovacao is not None:
        exigir_tipo(fonte_aprovacao, tipo_fonte, "fonte_aprovacao")
    if situacao.value == "aprovado" and fonte_aprovacao is None:
        raise ContratoESGInvalido("fonte_aprovacao: aprovação exige referência documental explícita.")
