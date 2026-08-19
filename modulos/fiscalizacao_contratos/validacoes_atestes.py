"""Validações e cálculos do fluxo de atestes."""

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


CENTAVOS = Decimal("0.01")
STATUS_ATESTES = (
    "Em elaboração", "Devolvido para correção", "Atestado",
    "Encaminhado para pagamento", "Cancelado",
)
CATEGORIAS_DOCUMENTO_ATESTE = (
    "Nota fiscal", "Relatório de execução", "Certidão", "Comprovante",
    "Declaração", "Ordem de serviço", "Memória de cálculo", "Outro",
)


def inteiro_positivo(valor):
    try:
        numero = int(str(valor or "").strip())
        return numero if numero > 0 else None
    except (TypeError, ValueError):
        return None


def data_iso(valor):
    if isinstance(valor, date):
        return valor
    try:
        return date.fromisoformat(str(valor or "").strip())
    except (TypeError, ValueError):
        return None


def decimal_monetario(valor):
    texto = str(valor or "").strip().replace("R$", "").replace(" ", "")
    if not texto:
        return None
    if "," in texto:
        texto = texto.replace(".", "").replace(",", ".")
    try:
        return Decimal(texto).quantize(CENTAVOS, rounding=ROUND_HALF_UP)
    except (InvalidOperation, TypeError, ValueError):
        return None


def normalizar_ateste(formulario):
    dados = {
        "medicao_id": inteiro_positivo(formulario.get("medicao_id")),
        "numero_ateste": inteiro_positivo(formulario.get("numero_ateste")),
        "servidor_atestador_id": inteiro_positivo(formulario.get("servidor_atestador_id")),
        "parecer": str(formulario.get("parecer") or "").strip() or None,
        "observacoes": str(formulario.get("observacoes") or "").strip() or None,
    }
    erros = []
    if not dados["medicao_id"]: erros.append("Selecione uma medição válida.")
    if not dados["numero_ateste"]: erros.append("Informe um número de ateste inteiro e positivo.")
    if not dados["servidor_atestador_id"]: erros.append("Selecione o servidor atestador.")
    return dados, erros


def normalizar_nota(formulario):
    dados = {
        "numero_nota": str(formulario.get("numero_nota") or "").strip(),
        "serie": str(formulario.get("serie") or "").strip() or None,
        "data_emissao": data_iso(formulario.get("data_emissao")),
        "valor_nota": decimal_monetario(formulario.get("valor_nota")),
        "chave_acesso": str(formulario.get("chave_acesso") or "").strip() or None,
        "documento_id": inteiro_positivo(formulario.get("documento_id")),
        "observacoes": str(formulario.get("observacoes") or "").strip() or None,
    }
    erros = []
    if not dados["numero_nota"]: erros.append("O número da nota fiscal é obrigatório.")
    if not dados["data_emissao"]: erros.append("Informe uma data de emissão válida.")
    if dados["valor_nota"] is None or dados["valor_nota"] <= 0:
        erros.append("O valor da nota fiscal deve ser maior que zero.")
    return dados, erros


def diferenca_notas(valor_atestado, total_notas):
    return (
        Decimal(str(total_notas or 0)).quantize(CENTAVOS, rounding=ROUND_HALF_UP)
        - Decimal(str(valor_atestado or 0)).quantize(CENTAVOS, rounding=ROUND_HALF_UP)
    ).quantize(CENTAVOS, rounding=ROUND_HALF_UP)
