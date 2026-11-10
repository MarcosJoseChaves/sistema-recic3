"""Contratos imutáveis ESG. Não persistem, calculam ou aprovam informações."""

from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import Enum
from uuid import UUID

from .validacoes import (
    ContratoESGInvalido,
    exigir_tipo,
    validar_ano_mes,
    validar_aprovacao,
    validar_codigo,
    validar_data,
    validar_texto,
    validar_valor,
)


class DimensaoESG(str, Enum):
    AMBIENTAL = "ambiental"
    SOCIAL = "social"
    GOVERNANCA = "governanca"


class SituacaoIndicador(str, Enum):
    PROPOSTO = "proposto"
    EM_ANALISE = "em_analise"
    APROVADO = "aprovado"
    REJEITADO = "rejeitado"


class SituacaoRegistro(str, Enum):
    RASCUNHO = "rascunho"
    EM_ANALISE = "em_analise"
    APROVADO = "aprovado"
    REJEITADO = "rejeitado"


@dataclass(frozen=True)
class FonteDocumental:
    identificador: str
    referencia_documental: str
    data_documento: date | None = None

    def __post_init__(self):
        validar_codigo(self.identificador, "fonte.identificador")
        validar_texto(self.referencia_documental, "fonte.referencia_documental")
        if self.data_documento is not None:
            validar_data(self.data_documento, "fonte.data_documento")


@dataclass(frozen=True)
class Perimetro:
    codigo: str
    descricao: str

    def __post_init__(self):
        validar_codigo(self.codigo, "perimetro.codigo")
        validar_texto(self.descricao, "perimetro.descricao")


@dataclass(frozen=True)
class UnidadeMedida:
    """Unidade declarada pela fonte; não implica conversão ou limites numéricos."""

    simbolo: str
    descricao: str

    def __post_init__(self):
        validar_texto(self.simbolo, "unidade.simbolo")
        validar_texto(self.descricao, "unidade.descricao")
        if len(self.simbolo) > 32 or any(c.isspace() for c in self.simbolo):
            raise ContratoESGInvalido("unidade.simbolo: use até 32 caracteres sem espaços.")


@dataclass(frozen=True)
class Periodo:
    """Ano civil completo ou competência mensal; mes=None significa anual."""

    ano: int
    mes: int | None = None

    def __post_init__(self):
        validar_ano_mes(self.ano, self.mes)

    @property
    def competencia(self):
        return f"{self.ano:04d}" if self.mes is None else f"{self.ano:04d}-{self.mes:02d}"


@dataclass(frozen=True)
class DefinicaoIndicador:
    codigo: str
    nome: str
    dimensao: DimensaoESG
    descricao: str
    unidade: UnidadeMedida
    metodologia: str
    perimetro: Perimetro
    fonte_metodologica: FonteDocumental
    situacao: SituacaoIndicador = SituacaoIndicador.PROPOSTO
    fonte_aprovacao: FonteDocumental | None = None

    def __post_init__(self):
        validar_codigo(self.codigo, "indicador.codigo")
        for campo in ("nome", "descricao", "metodologia"):
            validar_texto(getattr(self, campo), f"indicador.{campo}")
        exigir_tipo(self.dimensao, DimensaoESG, "indicador.dimensao")
        exigir_tipo(self.unidade, UnidadeMedida, "indicador.unidade")
        exigir_tipo(self.perimetro, Perimetro, "indicador.perimetro")
        exigir_tipo(self.fonte_metodologica, FonteDocumental, "indicador.fonte_metodologica")
        validar_aprovacao(self.situacao, SituacaoIndicador, self.fonte_aprovacao, FonteDocumental)


@dataclass(frozen=True)
class RegistroPeriodo:
    identificador: UUID
    indicador_codigo: str
    periodo: Periodo
    fonte: FonteDocumental
    data_referencia: date
    perimetro: Perimetro
    valor: Decimal | None = None
    situacao: SituacaoRegistro = SituacaoRegistro.RASCUNHO
    fonte_aprovacao: FonteDocumental | None = None

    def __post_init__(self):
        exigir_tipo(self.identificador, UUID, "registro.identificador")
        validar_codigo(self.indicador_codigo, "registro.indicador_codigo")
        exigir_tipo(self.periodo, Periodo, "registro.periodo")
        exigir_tipo(self.fonte, FonteDocumental, "registro.fonte")
        exigir_tipo(self.perimetro, Perimetro, "registro.perimetro")
        validar_data(self.data_referencia, "registro.data_referencia")
        if self.data_referencia.year != self.periodo.ano or (
            self.periodo.mes is not None and self.data_referencia.month != self.periodo.mes
        ):
            raise ContratoESGInvalido("data_referencia: deve pertencer ao período informado; data do documento pertence à fonte.")
        validar_valor(self.valor)
        validar_aprovacao(self.situacao, SituacaoRegistro, self.fonte_aprovacao, FonteDocumental)
        if self.situacao is SituacaoRegistro.APROVADO and self.valor is None:
            raise ContratoESGInvalido("valor: registro sem valor não pode ser aprovado.")


@dataclass(frozen=True)
class Evidencia:
    identificador: UUID
    registro_id: UUID
    tipo: str
    fonte: FonteDocumental
    observacoes: str | None = None

    def __post_init__(self):
        exigir_tipo(self.identificador, UUID, "evidencia.identificador")
        exigir_tipo(self.registro_id, UUID, "evidencia.registro_id")
        validar_texto(self.tipo, "evidencia.tipo")
        exigir_tipo(self.fonte, FonteDocumental, "evidencia.fonte")
        if self.observacoes is not None:
            validar_texto(self.observacoes, "evidencia.observacoes")
