"""Normalização e validação das fiscalizações contratuais."""

from datetime import date, time

TIPOS_FISCALIZACAO = (
    "Rotina", "Extraordinária", "Retorno", "Recebimento de serviço",
    "Conferência documental", "Remota", "Outra",
)
RESULTADOS_FISCALIZACAO = (
    "Conforme", "Conforme com ressalvas", "Não conforme", "Pendente de análise",
)
STATUS_FISCALIZACAO = ("Em elaboração", "Finalizada", "Cancelada")


def _inteiro(valor):
    try:
        numero = int(str(valor or "").strip())
        return numero if numero > 0 else None
    except (TypeError, ValueError):
        return None


def _data(valor):
    if isinstance(valor, date):
        return valor
    try:
        return date.fromisoformat(str(valor or "").strip())
    except (TypeError, ValueError):
        return None


def _hora(valor):
    if isinstance(valor, time):
        return valor.replace(second=0, microsecond=0)
    texto = str(valor or "").strip()
    if not texto:
        return None
    try:
        return time.fromisoformat(texto)
    except ValueError:
        return None


def normalizar_e_validar_fiscalizacao(formulario):
    dados = {
        "contrato_id": _inteiro(formulario.get("contrato_id")),
        "servidor_responsavel_id": _inteiro(formulario.get("servidor_responsavel_id")),
        "data_fiscalizacao": _data(formulario.get("data_fiscalizacao")),
        "hora_inicio": _hora(formulario.get("hora_inicio")),
        "hora_fim": _hora(formulario.get("hora_fim")),
        "tipo_fiscalizacao": str(formulario.get("tipo_fiscalizacao") or "").strip(),
        "local_fiscalizacao": str(formulario.get("local_fiscalizacao") or "").strip() or None,
        "objeto_verificado": str(formulario.get("objeto_verificado") or "").strip(),
        "resultado": str(formulario.get("resultado") or "").strip(),
        "observacoes": str(formulario.get("observacoes") or "").strip() or None,
        "status": "Em elaboração",
    }
    erros = []
    if not dados["contrato_id"]: erros.append("Selecione o contrato.")
    if not dados["servidor_responsavel_id"]: erros.append("Selecione o servidor responsável.")
    if not dados["data_fiscalizacao"]: erros.append("Informe uma data de fiscalização válida.")
    if dados["tipo_fiscalizacao"] not in TIPOS_FISCALIZACAO: erros.append("Selecione um tipo de fiscalização válido.")
    if not dados["objeto_verificado"]: erros.append("Informe o que foi verificado.")
    if dados["resultado"] not in RESULTADOS_FISCALIZACAO: erros.append("Selecione um resultado válido.")
    if formulario.get("hora_inicio") and dados["hora_inicio"] is None: erros.append("Informe uma hora inicial válida.")
    if formulario.get("hora_fim") and dados["hora_fim"] is None: erros.append("Informe uma hora final válida.")
    if dados["hora_inicio"] and dados["hora_fim"] and dados["hora_fim"] < dados["hora_inicio"]:
        erros.append("A hora final não pode ser anterior à hora inicial.")
    return dados, erros


def formatar_hora(valor):
    return valor.strftime("%H:%M") if hasattr(valor, "strftime") else "Não informado"
