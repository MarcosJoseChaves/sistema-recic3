"""Normalização e validação dos ativos contratuais e seus vínculos."""

import re
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from .validacoes_planilhas import converter_decimal_brasileiro, formatar_decimal_brasileiro


TIPOS_ATIVO = (
    "Veículo", "Máquina", "Equipamento", "Implemento",
    "Contentor ou recipiente", "Imóvel ou instalação",
    "Tecnologia ou sistema", "Ferramenta", "Outro",
)
ORIGENS_ATIVO = ("Município", "Contratada", "Locado", "Terceiro")
SITUACOES_ATIVO = (
    "Disponível", "Em operação", "Em manutenção", "Indisponível", "Baixado",
)
NATUREZAS_VINCULO = (
    "Exigido pelo contrato", "Operacional", "Reserva", "Substituto",
    "Cedido pelo Município", "Outro",
)


def _inteiro_positivo(valor):
    try:
        numero = int(str(valor).strip())
    except (TypeError, ValueError):
        return None
    return numero if numero > 0 else None


def _texto(valor):
    return re.sub(r"\s+", " ", str(valor or "").strip()) or None


def _alfanumerico(valor):
    texto = re.sub(r"[^A-Za-z0-9]", "", str(valor or "")).upper()
    return texto or None


def _data(valor, rotulo, erros, obrigatoria=False):
    if not valor:
        if obrigatoria:
            erros.append(f"{rotulo} é obrigatória.")
        return None
    if isinstance(valor, datetime):
        return valor.date()
    if isinstance(valor, date):
        return valor
    try:
        return datetime.strptime(str(valor), "%Y-%m-%d").date()
    except ValueError:
        erros.append(f"Informe uma data válida para {rotulo.lower()}.")
        return None


def normalizar_e_validar_ativo(formulario, ano_atual=None):
    erros = []
    ano_atual = ano_atual or date.today().year
    codigo = _texto(formulario.get("codigo_interno"))
    tipo = (formulario.get("tipo_ativo") or "").strip()
    descricao = _texto(formulario.get("descricao"))
    origem = (formulario.get("origem_ativo") or "").strip()
    situacao = (formulario.get("situacao") or "").strip()
    empresa_texto = str(formulario.get("empresa_proprietaria_id") or "").strip()
    empresa_id = _inteiro_positivo(empresa_texto) if empresa_texto else None

    ano_texto = str(formulario.get("ano_fabricacao") or "").strip()
    ano = None
    if ano_texto:
        try:
            ano = int(ano_texto)
        except ValueError:
            erros.append("O ano de fabricação deve ser um número inteiro.")
        else:
            if ano < 1900 or ano > ano_atual + 1:
                erros.append(f"O ano de fabricação deve estar entre 1900 e {ano_atual + 1}.")

    capacidade_texto = str(formulario.get("capacidade") or "").strip()
    capacidade = None
    if capacidade_texto:
        try:
            capacidade = converter_decimal_brasileiro(capacidade_texto)
        except InvalidOperation:
            erros.append("Informe uma capacidade válida.")
        else:
            if capacidade < 0:
                erros.append("A capacidade não pode ser negativa.")

    if not codigo:
        erros.append("O código interno é obrigatório.")
    if tipo not in TIPOS_ATIVO:
        erros.append("Selecione um tipo de ativo válido.")
    if not descricao:
        erros.append("A descrição é obrigatória.")
    if origem not in ORIGENS_ATIVO:
        erros.append("Selecione uma origem válida.")
    if situacao not in SITUACOES_ATIVO:
        erros.append("Selecione uma situação válida.")
    if empresa_texto and empresa_id is None:
        erros.append("Selecione uma empresa proprietária válida.")

    return {
        "codigo_interno": codigo.upper() if codigo else None,
        "tipo_ativo": tipo,
        "descricao": descricao,
        "marca": _texto(formulario.get("marca")),
        "modelo": _texto(formulario.get("modelo")),
        "ano_fabricacao": ano,
        "placa": _alfanumerico(formulario.get("placa")),
        "renavam": _alfanumerico(formulario.get("renavam")),
        "chassi": _alfanumerico(formulario.get("chassi")),
        "numero_serie": _texto(formulario.get("numero_serie")),
        "numero_patrimonio": _texto(formulario.get("numero_patrimonio")),
        "origem_ativo": origem,
        "empresa_proprietaria_id": empresa_id,
        "capacidade": capacidade,
        "unidade_capacidade": _texto(formulario.get("unidade_capacidade")),
        "situacao": situacao,
        "observacoes": _texto(formulario.get("observacoes")),
    }, erros


def normalizar_e_validar_vinculo(formulario):
    erros = []
    ativo_id = _inteiro_positivo(formulario.get("ativo_id"))
    contrato_id = _inteiro_positivo(formulario.get("contrato_id"))
    natureza = (formulario.get("natureza_vinculo") or "").strip()
    inicio = _data(formulario.get("data_inicio"), "A data inicial", erros, True)
    fim = _data(formulario.get("data_fim"), "a data final", erros)
    if ativo_id is None:
        erros.append("Selecione um ativo.")
    if contrato_id is None:
        erros.append("Selecione um contrato.")
    if natureza not in NATUREZAS_VINCULO:
        erros.append("Selecione uma natureza de vínculo válida.")
    if inicio and fim and fim < inicio:
        erros.append("A data final não pode ser anterior à data inicial.")
    if fim is not None:
        erros.append("Um novo vínculo ativo não deve possuir data final. Encerre-o posteriormente.")
    return {
        "ativo_id": ativo_id,
        "contrato_id": contrato_id,
        "natureza_vinculo": natureza,
        "data_inicio": inicio,
        "data_fim": fim,
        "principal": formulario.get("principal") == "1",
        "observacoes": _texto(formulario.get("observacoes")),
    }, erros


def normalizar_data_encerramento(valor, data_inicio):
    erros = []
    fim = _data(valor, "A data final", erros, True)
    if fim and data_inicio and fim < data_inicio:
        erros.append("A data final não pode ser anterior à data inicial.")
    return fim, erros


def formatar_capacidade(valor):
    return formatar_decimal_brasileiro(valor) if valor is not None else "Não informado"
