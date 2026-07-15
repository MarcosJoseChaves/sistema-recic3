"""Normalização, validação e formatação das planilhas orçamentárias."""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation


TIPOS_PLANILHA = (
    "Original",
    "Aditivada",
    "Reajustada",
    "Repactuada",
    "Revisada",
    "Outra",
)
STATUS_PLANILHA = ("Em elaboração", "Consolidada")


def converter_decimal_brasileiro(valor):
    """Converte texto brasileiro em Decimal sem passar por float."""
    texto = str(valor or "").strip().replace("R$", "").replace("%", "").strip()
    if not texto:
        raise InvalidOperation
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    return Decimal(texto)


def _inteiro_positivo(valor):
    try:
        numero = int(str(valor).strip())
    except (TypeError, ValueError):
        return None
    return numero if numero > 0 else None


def _data(valor, erros):
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    try:
        return datetime.strptime(str(valor or "").strip(), "%Y-%m-%d").date()
    except ValueError:
        erros.append("Informe uma data de referência válida.")
        return None


def normalizar_e_validar_planilha(formulario, *, permitir_original=True):
    erros = []
    contrato_id = _inteiro_positivo(formulario.get("contrato_id"))
    aditivo_texto = str(formulario.get("aditivo_id") or "").strip()
    aditivo_id = _inteiro_positivo(aditivo_texto) if aditivo_texto else None
    nome = (formulario.get("nome") or "").strip()
    versao = _inteiro_positivo(formulario.get("versao"))
    tipo = (formulario.get("tipo_planilha") or "").strip()
    data_referencia = _data(formulario.get("data_referencia"), erros)

    if contrato_id is None:
        erros.append("Selecione um contrato.")
    if not nome:
        erros.append("O nome da planilha é obrigatório.")
    if versao is None:
        erros.append("A versão deve ser um número maior que zero.")
    if tipo not in TIPOS_PLANILHA:
        erros.append("Selecione um tipo de planilha válido.")
    if tipo == "Original" and not permitir_original:
        erros.append("Nova versão por cópia não pode ser do tipo Original.")
    if aditivo_texto and aditivo_id is None:
        erros.append("Selecione um aditivo válido.")

    return {
        "contrato_id": contrato_id,
        "aditivo_id": aditivo_id,
        "nome": nome,
        "versao": versao,
        "tipo_planilha": tipo,
        "data_referencia": data_referencia,
        "descricao_referencia": (formulario.get("descricao_referencia") or "").strip() or None,
    }, erros


def normalizar_e_validar_item(formulario):
    erros = []
    ordem = _inteiro_positivo(formulario.get("ordem"))
    descricao = (formulario.get("descricao") or "").strip()
    unidade = (formulario.get("unidade") or "").strip()

    valores = {}
    for campo, rotulo, minimo_exclusivo in (
        ("quantidade", "A quantidade", False),
        ("valor_unitario", "O valor unitário", False),
        ("fator_multiplicador", "O fator multiplicador", True),
    ):
        try:
            numero = converter_decimal_brasileiro(formulario.get(campo))
        except InvalidOperation:
            erros.append(f"{rotulo} deve ser um número válido.")
            numero = Decimal("0")
        if numero < 0 or (minimo_exclusivo and numero == 0):
            comparacao = "maior que zero" if minimo_exclusivo else "maior ou igual a zero"
            erros.append(f"{rotulo} deve ser {comparacao}.")
        valores[campo] = numero

    if ordem is None:
        erros.append("A ordem deve ser um número maior que zero.")
    if not descricao:
        erros.append("A descrição do item é obrigatória.")
    if not unidade:
        erros.append("A unidade do item é obrigatória.")

    return {
        "ordem": ordem,
        "grupo": (formulario.get("grupo") or "").strip() or None,
        "codigo_item": (formulario.get("codigo_item") or "").strip() or None,
        "descricao": descricao,
        "unidade": unidade,
        **valores,
        "observacoes": (formulario.get("observacoes") or "").strip() or None,
    }, erros


def calcular_total_item(item):
    return (
        Decimal(str(item.get("quantidade") or 0))
        * Decimal(str(item.get("valor_unitario") or 0))
        * Decimal(str(item.get("fator_multiplicador") or 0))
    )


def formatar_decimal_brasileiro(valor, casas=8):
    if valor is None or valor == "":
        return ""
    numero = Decimal(str(valor))
    texto = f"{numero:.{casas}f}".rstrip("0").rstrip(".")
    inteiro, _, decimal = texto.partition(".")
    inteiro = f"{int(inteiro):,}".replace(",", ".")
    return inteiro + (f",{decimal}" if decimal else "")


def formatar_percentual_diferenca(valor):
    if valor is None:
        return "Não calculável"
    return f"{formatar_decimal_brasileiro(valor, 4)}%"
