"""Adaptadores explícitos dos formulários ESG para contratos validados."""

from datetime import date
from decimal import Decimal
import re
from uuid import uuid4

from .contratos import (
    DefinicaoIndicador, DimensaoESG, Evidencia, FonteDocumental, Perimetro,
    Periodo, RegistroPeriodo, SituacaoRegistro, UnidadeMedida,
)
from .validacoes import ContratoESGInvalido, exigir_tipo


CAMPOS_FONTE = {"fonte_id", "fonte_referencia", "fonte_data"}
CAMPOS_DEFINICAO = CAMPOS_FONTE | {
    "codigo", "nome", "dimensao", "descricao", "unidade_simbolo", "unidade_descricao",
    "metodologia", "perimetro_codigo", "perimetro_descricao",
}
CAMPOS_REGISTRO = CAMPOS_FONTE | {
    "competencia", "valor", "data_referencia", "evidencia_tipo", "evidencia_observacoes",
}


def campos_permitidos(dados, permitidos):
    """Recusa campos extras ou repetidos, inclusive tentativas de forjar aprovação."""
    if set(dados) - (set(permitidos) | {"csrf_token"}):
        raise ContratoESGInvalido("O envio contém campos não permitidos. Situação, aprovação, autoria e vínculos são controlados pelo sistema.")
    if hasattr(dados, "getlist") and any(len(dados.getlist(campo)) != 1 for campo in dados):
        raise ContratoESGInvalido("O envio contém campos repetidos.")


def texto(dados, campo):
    valor = dados.get(campo, "")
    exigir_tipo(valor, str, campo)
    return valor.replace("\r\n", "\n").strip()


def data_formulario(valor, campo, opcional=False):
    if opcional and valor == "":
        return None
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", valor):
        raise ContratoESGInvalido(f"{campo}: informe uma data válida no formato AAAA-MM-DD.")
    try:
        return date.fromisoformat(valor)
    except ValueError:
        raise ContratoESGInvalido(f"{campo}: data inválida.") from None


def periodo_formulario(valor):
    if not re.fullmatch(r"[0-9]{4}(?:-[0-9]{2})?", valor):
        raise ContratoESGInvalido("competencia: use AAAA para ano completo ou AAAA-MM para mês.")
    return Periodo(int(valor[:4]), int(valor[5:]) if len(valor) == 7 else None)


def valor_formulario(valor):
    if valor == "":
        return None
    if len(valor) > 256 or not re.fullmatch(r"[+-]?[0-9]+(?:[.,][0-9]+)?", valor):
        raise ContratoESGInvalido("valor: informe número decimal com até 256 caracteres, sem separador de milhar ou notação científica; deixe vazio para valor ausente.")
    return Decimal(valor.replace(",", "."))


def fonte_formulario(dados):
    return FonteDocumental(
        texto(dados, "fonte_id"), texto(dados, "fonte_referencia"),
        data_formulario(texto(dados, "fonte_data"), "fonte_data", opcional=True),
    )


def definicao_formulario(dados):
    campos_permitidos(dados, CAMPOS_DEFINICAO)
    try:
        dimensao = DimensaoESG(texto(dados, "dimensao"))
    except ValueError:
        raise ContratoESGInvalido("dimensao: selecione Ambiental, Social ou Governança.") from None
    return DefinicaoIndicador(
        codigo=texto(dados, "codigo"), nome=texto(dados, "nome"), dimensao=dimensao,
        descricao=texto(dados, "descricao"),
        unidade=UnidadeMedida(texto(dados, "unidade_simbolo"), texto(dados, "unidade_descricao")),
        metodologia=texto(dados, "metodologia"),
        perimetro=Perimetro(texto(dados, "perimetro_codigo"), texto(dados, "perimetro_descricao")),
        fonte_metodologica=fonte_formulario(dados),
    )


def registro_formulario(dados, definicao):
    campos_permitidos(dados, CAMPOS_REGISTRO)
    exigir_tipo(definicao, DefinicaoIndicador, "definicao")
    registro = RegistroPeriodo(
        identificador=uuid4(), indicador_codigo=definicao.codigo,
        periodo=periodo_formulario(texto(dados, "competencia")),
        valor=valor_formulario(texto(dados, "valor")), fonte=fonte_formulario(dados),
        data_referencia=data_formulario(texto(dados, "data_referencia"), "data_referencia"),
        perimetro=definicao.perimetro,
    )
    tipo = texto(dados, "evidencia_tipo")
    observacoes = texto(dados, "evidencia_observacoes")
    if observacoes and not tipo:
        raise ContratoESGInvalido("evidencia_tipo: informe o tipo para registrar observações documentais.")
    evidencias = (Evidencia(uuid4(), registro.identificador, tipo, registro.fonte, observacoes or None),) if tipo else ()
    return registro, evidencias


def filtros_formulario(dados):
    campos_permitidos(dados, {"competencia", "situacao"})
    competencia = texto(dados, "competencia")
    situacao = texto(dados, "situacao")
    periodo = periodo_formulario(competencia) if competencia else None
    try:
        estado = SituacaoRegistro(situacao) if situacao else None
    except ValueError:
        raise ContratoESGInvalido("situacao: selecione uma situação de registro válida.") from None
    return periodo, estado
