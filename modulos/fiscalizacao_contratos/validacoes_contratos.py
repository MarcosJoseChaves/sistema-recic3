"""Normalização, validação e formatação do cadastro de contratos."""

from datetime import date, datetime
from decimal import Decimal, InvalidOperation


SITUACOES_CONTRATO = (
    "Em elaboração",
    "Vigente",
    "Suspenso",
    "Encerrado",
    "Cancelado",
)


def _inteiro_positivo(valor):
    try:
        numero = int(valor)
    except (TypeError, ValueError):
        return None
    return numero if numero > 0 else None


def _data_opcional(valor, campo, erros):
    if not valor:
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    try:
        return datetime.strptime(str(valor), "%Y-%m-%d").date()
    except ValueError:
        erros.append(f"Informe uma data válida para {campo}.")
        return None


def converter_valor_brasileiro(valor):
    """Converte 'R$ 125.450,75' em Decimal('125450.75')."""
    if isinstance(valor, Decimal):
        return valor.quantize(Decimal("0.01"))
    texto = str(valor or "").strip().replace("R$", "").replace(" ", "")
    if not texto:
        raise InvalidOperation
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto).quantize(Decimal("0.01"))
    except InvalidOperation:
        raise


def normalizar_e_validar_contrato(formulario):
    """Valida o contrato e os responsáveis antes de qualquer acesso ao banco."""
    erros = []
    numero_contrato = (formulario.get("numero_contrato") or "").strip()
    processo = (formulario.get("processo_administrativo") or "").strip() or None
    objeto = (formulario.get("objeto") or "").strip()
    empresa_id = _inteiro_positivo(formulario.get("empresa_id"))
    situacao = (formulario.get("situacao") or "").strip()
    gestor_id = _inteiro_positivo(formulario.get("gestor_id"))
    fiscal_titular_id = _inteiro_positivo(formulario.get("fiscal_titular_id"))
    permitir_multiplas = formulario.get("permitir_multiplas_funcoes") == "1"

    substitutos_recebidos = formulario.getlist("fiscais_substitutos")
    substitutos = [
        servidor_id
        for servidor_id in (_inteiro_positivo(valor) for valor in substitutos_recebidos)
        if servidor_id is not None
    ]

    if not numero_contrato:
        erros.append("O número do contrato é obrigatório.")
    if not objeto:
        erros.append("O objeto do contrato é obrigatório.")
    if empresa_id is None:
        erros.append("Selecione uma empresa.")

    valor_original = None
    try:
        valor_original = converter_valor_brasileiro(formulario.get("valor_original"))
        if valor_original < 0:
            erros.append("O valor original não pode ser negativo.")
    except InvalidOperation:
        erros.append("Informe um valor original válido.")

    data_assinatura = _data_opcional(
        formulario.get("data_assinatura"), "a data de assinatura", erros
    )
    vigencia_inicio = _data_opcional(
        formulario.get("vigencia_inicio"), "o início da vigência", erros
    )
    vigencia_fim = _data_opcional(
        formulario.get("vigencia_fim"), "o fim da vigência", erros
    )
    if vigencia_inicio and vigencia_fim and vigencia_fim < vigencia_inicio:
        erros.append("O fim da vigência não pode ser anterior ao início.")

    if situacao not in SITUACOES_CONTRATO:
        erros.append("Selecione uma situação válida.")
    if gestor_id is None:
        erros.append("Selecione o gestor do contrato.")
    if fiscal_titular_id is None:
        erros.append("Selecione o fiscal titular do contrato.")
    if len(substitutos) != len(substitutos_recebidos):
        erros.append("Selecione somente fiscais substitutos válidos.")
    if len(substitutos) != len(set(substitutos)):
        erros.append("O mesmo fiscal substituto não pode ser repetido.")

    papeis_por_servidor = {}
    if gestor_id:
        papeis_por_servidor.setdefault(gestor_id, set()).add("Gestor")
    if fiscal_titular_id:
        papeis_por_servidor.setdefault(fiscal_titular_id, set()).add("Fiscal titular")
    for servidor_id in substitutos:
        papeis_por_servidor.setdefault(servidor_id, set()).add("Fiscal substituto")
    if any(len(papeis) > 1 for papeis in papeis_por_servidor.values()) and not permitir_multiplas:
        erros.append(
            "Confirme explicitamente quando um servidor exercer mais de uma função."
        )

    dados = {
        "numero_contrato": numero_contrato,
        "processo_administrativo": processo,
        "objeto": objeto,
        "empresa_id": empresa_id,
        "valor_original": valor_original,
        "data_assinatura": data_assinatura,
        "vigencia_inicio": vigencia_inicio,
        "vigencia_fim": vigencia_fim,
        "situacao": situacao,
        "observacoes": (formulario.get("observacoes") or "").strip() or None,
    }
    responsaveis = {
        "gestor_id": gestor_id,
        "fiscal_titular_id": fiscal_titular_id,
        "fiscais_substitutos": substitutos,
        "permitir_multiplas_funcoes": permitir_multiplas,
    }
    return dados, responsaveis, erros


def formatar_moeda_brasileira(valor):
    if valor is None or valor == "":
        return "-"
    try:
        numero = Decimal(str(valor))
    except InvalidOperation:
        return str(valor)
    formatado = f"{numero:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {formatado}"


def formatar_valor_campo(valor):
    formatado = formatar_moeda_brasileira(valor)
    return "" if formatado == "-" else formatado.replace("R$ ", "")


def formatar_data_brasileira(valor):
    if not valor:
        return "-"
    if isinstance(valor, str):
        try:
            valor = datetime.strptime(valor, "%Y-%m-%d").date()
        except ValueError:
            return valor
    return valor.strftime("%d/%m/%Y")


def formatar_data_iso(valor):
    if not valor:
        return ""
    if isinstance(valor, str):
        return valor
    return valor.strftime("%Y-%m-%d")
