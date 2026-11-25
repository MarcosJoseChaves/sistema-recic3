"""Eixos independentes de proveniência, qualidade, disponibilidade e aprovação."""
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from ..contratos import FonteDocumental, Perimetro, Periodo
from ..validacoes import ContratoESGInvalido, exigir_tipo, validar_valor

class OrigemValor(str, Enum):
    DECLARADO = "declarado"
    ESTIMADO = "estimado"
    CALCULADO = "calculado"

class QualidadeInformacao(str, Enum):
    PROVISORIO = "provisorio"
    VALIDADO = "validado"
    INCONCLUSIVO = "inconclusivo"

class Disponibilidade(str, Enum):
    INFORMADO = "informado"
    INDISPONIVEL = "indisponivel"
    NAO_CALCULAVEL = "nao_calculavel"
    NAO_OBSERVADO = "nao_observado"

class UnidadeMassa(str, Enum):
    KG = "kg"
    TONELADA = "t"

@dataclass(frozen=True)
class ComponenteMassa:
    massa: Decimal | None
    unidade: UnidadeMassa
    periodo: Periodo
    perimetro: Perimetro
    fonte: FonteDocumental
    origem: OrigemValor
    qualidade: QualidadeInformacao = QualidadeInformacao.PROVISORIO

    def __post_init__(self):
        validar_valor(self.massa)
        if self.massa is not None and self.massa < 0:
            raise ContratoESGInvalido("massa: valores negativos não são admissíveis.")
        for campo, tipo in (("unidade", UnidadeMassa), ("periodo", Periodo),
                            ("perimetro", Perimetro), ("fonte", FonteDocumental),
                            ("origem", OrigemValor), ("qualidade", QualidadeInformacao)):
            exigir_tipo(getattr(self, campo), tipo, campo)

@dataclass(frozen=True)
class ReconciliacaoFisica:
    periodo: Periodo
    perimetro: Perimetro
    fonte: FonteDocumental
    # Referência apresentada; não certifica o conteúdo nem calcula ajuste de estoques.
    def __post_init__(self):
        for campo, tipo in (("periodo", Periodo), ("perimetro", Perimetro), ("fonte", FonteDocumental)):
            exigir_tipo(getattr(self, campo), tipo, campo)
