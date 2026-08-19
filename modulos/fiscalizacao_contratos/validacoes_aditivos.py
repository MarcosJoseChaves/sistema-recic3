"""Normalização e validação do cadastro de aditivos contratuais."""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from .validacoes_contratos import converter_valor_brasileiro


TIPOS_ADITIVO = (
    "Prazo",
    "Acréscimo de valor",
    "Supressão de valor",
    "Prazo e valor",
    "Reajuste",
    "Repactuação",
    "Revisão",
    "Alteração de objeto",
    "Alteração quantitativa",
    "Alteração de cronograma",
    "Garantia",
    "Outro",
)

TIPOS_COM_PRAZO = ("Prazo", "Prazo e valor")
TIPOS_COM_VALOR = (
    "Acréscimo de valor",
    "Supressão de valor",
    "Prazo e valor",
    "Reajuste",
    "Repactuação",
    "Revisão",
    "Alteração quantitativa",
)


def _inteiro_positivo(valor):
    try:
        numero = int(valor)
    except (TypeError, ValueError):
        return None
    return numero if numero > 0 else None


def _data(valor, campo, erros, *, obrigatoria=False):
    if not valor:
        if obrigatoria:
            erros.append(f"{campo} é obrigatória.")
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    try:
        return datetime.strptime(str(valor), "%Y-%m-%d").date()
    except ValueError:
        erros.append(f"Informe uma data válida para {campo.lower()}.")
        return None


def _decimal_nao_negativo(valor, campo, erros, *, percentual=False):
    texto = str(valor or "").strip()
    if not texto:
        return None if percentual else Decimal("0.00")
    if percentual:
        texto = texto.replace("%", "").strip()
    try:
        numero = converter_valor_brasileiro(texto)
    except InvalidOperation:
        erros.append(f"Informe um valor válido para {campo}.")
        return None if percentual else Decimal("0.00")
    if numero < 0:
        erros.append(f"{campo.capitalize()} não pode ser negativo.")
    return numero


def normalizar_e_validar_aditivo(formulario):
    """Valida os dados antes de qualquer acesso ao banco."""
    erros = []
    contrato_id = _inteiro_positivo(formulario.get("contrato_id"))
    numero_termo = (formulario.get("numero_termo") or "").strip()
    tipo_aditivo = (formulario.get("tipo_aditivo") or "").strip()
    data_assinatura = _data(
        formulario.get("data_assinatura"), "A data de assinatura", erros, obrigatoria=True
    )
    data_inicio_efeitos = _data(
        formulario.get("data_inicio_efeitos"), "a data de início dos efeitos", erros
    )
    nova_vigencia_fim = _data(
        formulario.get("nova_vigencia_fim"), "a nova vigência final", erros
    )

    dias_texto = str(formulario.get("dias_acrescidos") or "").strip()
    dias_acrescidos = None
    if dias_texto:
        try:
            dias_acrescidos = int(dias_texto)
        except ValueError:
            erros.append("Informe uma quantidade válida de dias acrescidos.")
        else:
            if dias_acrescidos < 0:
                erros.append("Os dias acrescidos não podem ser negativos.")

    valor_acrescimo = _decimal_nao_negativo(
        formulario.get("valor_acrescimo"), "o valor do acréscimo", erros
    )
    valor_supressao = _decimal_nao_negativo(
        formulario.get("valor_supressao"), "o valor da supressão", erros
    )
    percentual = _decimal_nao_negativo(
        formulario.get("percentual_alteracao"),
        "o percentual de alteração",
        erros,
        percentual=True,
    )

    if contrato_id is None:
        erros.append("Selecione um contrato.")
    if not numero_termo:
        erros.append("O número do termo é obrigatório.")
    if tipo_aditivo not in TIPOS_ADITIVO:
        erros.append("Selecione um tipo de aditivo válido.")
    if tipo_aditivo in TIPOS_COM_PRAZO and not dias_acrescidos and not nova_vigencia_fim:
        erros.append("Informe os dias acrescidos ou a nova vigência final para o aditivo de prazo.")
    if tipo_aditivo in TIPOS_COM_VALOR and not (valor_acrescimo or valor_supressao):
        erros.append("Informe um acréscimo ou uma supressão para o aditivo de valor.")

    permitir_simultaneos = formulario.get("confirmar_valores_simultaneos") == "1"
    justificativa = (formulario.get("justificativa") or "").strip() or None
    if valor_acrescimo and valor_supressao and (
        not permitir_simultaneos or not justificativa
    ):
        erros.append(
            "Acréscimo e supressão juntos exigem confirmação e justificativa técnica."
        )

    dados = {
        "contrato_id": contrato_id,
        "numero_termo": numero_termo,
        "tipo_aditivo": tipo_aditivo,
        "data_assinatura": data_assinatura,
        "data_inicio_efeitos": data_inicio_efeitos,
        "dias_acrescidos": dias_acrescidos,
        "nova_vigencia_fim": nova_vigencia_fim,
        "valor_acrescimo": valor_acrescimo,
        "valor_supressao": valor_supressao,
        "percentual_alteracao": percentual,
        "descricao_alteracao": (formulario.get("descricao_alteracao") or "").strip() or None,
        "justificativa": justificativa,
        "observacoes": (formulario.get("observacoes") or "").strip() or None,
        "confirmar_valores_simultaneos": permitir_simultaneos,
    }
    return dados, erros


def formatar_percentual_brasileiro(valor):
    if valor is None or valor == "":
        return "-"
    try:
        numero = Decimal(str(valor))
    except InvalidOperation:
        return str(valor)
    texto = f"{numero:.4f}".rstrip("0").rstrip(".").replace(".", ",")
    return f"{texto}%"


def formatar_percentual_campo(valor):
    formatado = formatar_percentual_brasileiro(valor)
    return "" if formatado == "-" else formatado.removesuffix("%")
