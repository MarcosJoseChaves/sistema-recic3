"""Normalização e cálculos seguros das medições contratuais."""

from datetime import date
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP


STATUS_MEDICAO = (
    "Em elaboração", "Em análise", "Devolvida para correção", "Aprovada", "Cancelada",
)
TIPOS_AJUSTE = ("Acréscimo", "Desconto", "Glosa")
CATEGORIAS_DOCUMENTO_MEDICAO = (
    "Memória de cálculo", "Relatório de medição", "Evidência da execução",
    "Nota fiscal", "Planilha", "Ordem de serviço", "Outro",
)
CENTAVOS = Decimal("0.01")


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


def competencia_mes(valor):
    texto = str(valor or "").strip()
    try:
        if len(texto) == 7:
            return date.fromisoformat(texto + "-01")
        resultado = data_iso(texto)
        return resultado.replace(day=1) if resultado and resultado.day == 1 else None
    except ValueError:
        return None


def decimal_brasileiro(valor, casas=None):
    if isinstance(valor, Decimal):
        numero = valor
    else:
        texto = str(valor or "").strip().replace(" ", "")
        if not texto:
            return None
        if "," in texto:
            texto = texto.replace(".", "").replace(",", ".")
        try:
            numero = Decimal(texto)
        except (InvalidOperation, TypeError, ValueError):
            return None
    return numero.quantize(casas, rounding=ROUND_HALF_UP) if casas else numero


def dinheiro(valor):
    return decimal_brasileiro(valor, CENTAVOS)


def calcular_valor_item(quantidade, preco):
    return (Decimal(quantidade) * Decimal(preco)).quantize(CENTAVOS, rounding=ROUND_HALF_UP)


def calcular_totais(itens, ajustes):
    bruto = sum((Decimal(str(i["valor_medido"])) for i in itens if i.get("ativo", True)), Decimal("0"))
    somas = {tipo: Decimal("0") for tipo in TIPOS_AJUSTE}
    for ajuste in ajustes:
        if ajuste.get("ativo", True):
            somas[ajuste["tipo_ajuste"]] += Decimal(str(ajuste["valor"]))
    bruto = bruto.quantize(CENTAVOS, rounding=ROUND_HALF_UP)
    acrescimos = somas["Acréscimo"].quantize(CENTAVOS, rounding=ROUND_HALF_UP)
    descontos = somas["Desconto"].quantize(CENTAVOS, rounding=ROUND_HALF_UP)
    glosas = somas["Glosa"].quantize(CENTAVOS, rounding=ROUND_HALF_UP)
    liquido = (bruto + acrescimos - descontos - glosas).quantize(CENTAVOS, rounding=ROUND_HALF_UP)
    if liquido < 0:
        raise ValueError("O valor líquido da medição não pode ser negativo.")
    return bruto, acrescimos, descontos, glosas, liquido


def normalizar_e_validar_medicao(formulario):
    dados = {
        "contrato_id": inteiro_positivo(formulario.get("contrato_id")),
        "numero_medicao": inteiro_positivo(formulario.get("numero_medicao")),
        "competencia": competencia_mes(formulario.get("competencia")),
        "periodo_inicio": data_iso(formulario.get("periodo_inicio")),
        "periodo_fim": data_iso(formulario.get("periodo_fim")),
        "servidor_fiscal_id": inteiro_positivo(formulario.get("servidor_fiscal_id")),
        "data_apresentacao": data_iso(formulario.get("data_apresentacao")),
        "observacoes": str(formulario.get("observacoes") or "").strip() or None,
    }
    erros = []
    if not dados["contrato_id"]: erros.append("Selecione o contrato.")
    if not dados["numero_medicao"]: erros.append("Informe um número de medição inteiro e positivo.")
    if not dados["competencia"]: erros.append("Informe uma competência válida.")
    if not dados["periodo_inicio"]: erros.append("Informe o início do período.")
    if not dados["periodo_fim"]: erros.append("Informe o fim do período.")
    if dados["periodo_inicio"] and dados["periodo_fim"] and dados["periodo_fim"] < dados["periodo_inicio"]:
        erros.append("O fim do período não pode ser anterior ao início.")
    if not dados["servidor_fiscal_id"]: erros.append("Selecione o fiscal responsável.")
    return dados, erros


def normalizar_e_validar_item(formulario):
    quantidade = decimal_brasileiro(formulario.get("quantidade_medida"))
    preco = decimal_brasileiro(formulario.get("preco_unitario"))
    prevista = decimal_brasileiro(formulario.get("quantidade_prevista"))
    dados = {
        "planilha_item_id": inteiro_positivo(formulario.get("planilha_item_id")),
        "ordem": inteiro_positivo(formulario.get("ordem")),
        "codigo_item": str(formulario.get("codigo_item") or "").strip() or None,
        "descricao": str(formulario.get("descricao") or "").strip(),
        "unidade": str(formulario.get("unidade") or "").strip(),
        "quantidade_prevista": prevista,
        "quantidade_medida": quantidade,
        "preco_unitario": preco,
        "justificativa_excedente": str(formulario.get("justificativa_excedente") or "").strip() or None,
        "observacoes": str(formulario.get("observacoes") or "").strip() or None,
    }
    erros = []
    if not dados["ordem"]: erros.append("Informe uma ordem positiva.")
    if not dados["planilha_item_id"] and not dados["descricao"]: erros.append("Informe a descrição do item.")
    if not dados["planilha_item_id"] and not dados["unidade"]: erros.append("Informe a unidade do item.")
    if quantidade is None or quantidade < 0: erros.append("Informe uma quantidade medida não negativa.")
    if not dados["planilha_item_id"] and (preco is None or preco < 0): erros.append("Informe um preço unitário não negativo.")
    if prevista is not None and prevista < 0: erros.append("A quantidade prevista não pode ser negativa.")
    if quantidade is not None and prevista is not None and quantidade > prevista and not dados["justificativa_excedente"]:
        erros.append("Justifique a quantidade medida acima da prevista.")
    return dados, erros


def normalizar_e_validar_ajuste(formulario):
    dados = {
        "tipo_ajuste": str(formulario.get("tipo_ajuste") or "").strip(),
        "descricao": str(formulario.get("descricao") or "").strip(),
        "valor": dinheiro(formulario.get("valor")),
        "fiscalizacao_id": inteiro_positivo(formulario.get("fiscalizacao_id")),
        "ocorrencia_id": inteiro_positivo(formulario.get("ocorrencia_id")),
        "observacoes": str(formulario.get("observacoes") or "").strip() or None,
    }
    erros = []
    if dados["tipo_ajuste"] not in TIPOS_AJUSTE: erros.append("Selecione um tipo de ajuste válido.")
    if not dados["descricao"]: erros.append("Informe a descrição do ajuste.")
    if dados["valor"] is None or dados["valor"] <= 0: erros.append("O valor do ajuste deve ser maior que zero.")
    return dados, erros
