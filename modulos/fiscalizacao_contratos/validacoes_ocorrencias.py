"""Normalização e validação das ocorrências e acompanhamentos."""

from datetime import date

CATEGORIAS_OCORRENCIA = (
    "Execução do serviço", "Qualidade", "Prazo", "Mão de obra",
    "Segurança do trabalho", "Veículo ou equipamento", "Documentação",
    "Ambiental", "Trabalhista", "Descumprimento contratual", "Outro",
)
GRAVIDADES_OCORRENCIA = ("Leve", "Média", "Grave", "Crítica")
STATUS_OCORRENCIA = ("Aberta", "Em acompanhamento", "Regularizada", "Não regularizada", "Cancelada")


def _inteiro(valor):
    try:
        numero = int(str(valor or "").strip())
        return numero if numero > 0 else None
    except (TypeError, ValueError): return None


def _data(valor):
    if isinstance(valor, date): return valor
    try: return date.fromisoformat(str(valor or "").strip())
    except (TypeError, ValueError): return None


def normalizar_e_validar_ocorrencia(formulario):
    exige = str(formulario.get("exige_notificacao") or "").lower() in ("1", "true", "on", "sim")
    dados = {
        "contrato_id": _inteiro(formulario.get("contrato_id")),
        "fiscalizacao_id": _inteiro(formulario.get("fiscalizacao_id")),
        "ativo_contratual_id": _inteiro(formulario.get("ativo_contratual_id")),
        "servidor_responsavel_id": _inteiro(formulario.get("servidor_responsavel_id")),
        "titulo": str(formulario.get("titulo") or "").strip(),
        "categoria": str(formulario.get("categoria") or "").strip(),
        "gravidade": str(formulario.get("gravidade") or "").strip(),
        "descricao": str(formulario.get("descricao") or "").strip(),
        "data_identificacao": _data(formulario.get("data_identificacao")),
        "prazo_correcao": _data(formulario.get("prazo_correcao")),
        "exige_notificacao": exige,
        "numero_notificacao": str(formulario.get("numero_notificacao") or "").strip() or None,
        "conclusao": str(formulario.get("conclusao") or "").strip() or None,
        "status": "Aberta", "data_regularizacao": None,
    }
    erros = []
    if not dados["contrato_id"]: erros.append("Selecione o contrato.")
    if not dados["servidor_responsavel_id"]: erros.append("Selecione o servidor responsável.")
    if not dados["titulo"]: erros.append("Informe o título da ocorrência.")
    if not dados["descricao"]: erros.append("Informe a descrição da ocorrência.")
    if dados["categoria"] not in CATEGORIAS_OCORRENCIA: erros.append("Selecione uma categoria válida.")
    if dados["gravidade"] not in GRAVIDADES_OCORRENCIA: erros.append("Selecione uma gravidade válida.")
    if not dados["data_identificacao"]: erros.append("Informe uma data de identificação válida.")
    if formulario.get("prazo_correcao") and not dados["prazo_correcao"]: erros.append("Informe um prazo de correção válido.")
    if dados["data_identificacao"] and dados["prazo_correcao"] and dados["prazo_correcao"] < dados["data_identificacao"]:
        erros.append("O prazo de correção não pode ser anterior à identificação.")
    if exige and not dados["numero_notificacao"]: erros.append("Informe o número da notificação.")
    return dados, erros


def normalizar_e_validar_acompanhamento(formulario, ocorrencia):
    dados = {
        "data_acompanhamento": _data(formulario.get("data_acompanhamento")),
        "status_novo": str(formulario.get("status_novo") or "").strip(),
        "descricao": str(formulario.get("descricao") or "").strip(),
        "providencia_contratada": str(formulario.get("providencia_contratada") or "").strip() or None,
        "observacoes": str(formulario.get("observacoes") or "").strip() or None,
        "data_regularizacao": _data(formulario.get("data_regularizacao")),
        "confirmar_saida_regularizada": str(formulario.get("confirmar_saida_regularizada") or "").lower() in ("1", "true", "on", "sim"),
    }
    erros = []
    if not dados["data_acompanhamento"]: erros.append("Informe a data do acompanhamento.")
    elif dados["data_acompanhamento"] < ocorrencia["data_identificacao"]: erros.append("O acompanhamento não pode ser anterior à identificação.")
    if dados["status_novo"] not in STATUS_OCORRENCIA: erros.append("Selecione um novo status válido.")
    if not dados["descricao"]: erros.append("Descreva o acompanhamento.")
    if dados["status_novo"] == "Regularizada" and not dados["data_regularizacao"]: erros.append("Informe a data da regularização.")
    if dados["data_regularizacao"] and dados["data_acompanhamento"] and dados["data_regularizacao"] > dados["data_acompanhamento"]:
        erros.append("A data da regularização não pode ser posterior ao acompanhamento.")
    if dados["status_novo"] == "Cancelada" and not dados["descricao"]: erros.append("Informe a justificativa do cancelamento.")
    if ocorrencia["status"] == "Regularizada" and dados["status_novo"] != "Regularizada":
        if not dados["confirmar_saida_regularizada"]: erros.append("Confirme explicitamente a saída do status Regularizada.")
        if not dados["observacoes"]: erros.append("Explique por que a ocorrência deixou de estar regularizada.")
    return dados, erros


def situacao_prazo(ocorrencia, hoje=None):
    hoje = hoje or date.today()
    if ocorrencia.get("status") == "Regularizada": return "Regularizada"
    if ocorrencia.get("status") == "Não regularizada": return "Não regularizada"
    prazo = ocorrencia.get("prazo_correcao")
    if not prazo: return "Sem prazo"
    if ocorrencia.get("ativo") and ocorrencia.get("status") in ("Aberta", "Em acompanhamento") and prazo < hoje:
        return "Vencida"
    return "Dentro do prazo"
