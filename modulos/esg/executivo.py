"""Projeção imutável para leitura; não calcula resultados nem altera contratos."""

from dataclasses import dataclass

from .contratos import DefinicaoIndicador, DimensaoESG, Evidencia, RegistroPeriodo
from .services import SerieIndicador, organizar_registros
from .validacoes import ContratoESGInvalido, exigir_tipo


@dataclass(frozen=True)
class CartaoExecutivo:
    definicao: DefinicaoIndicador
    registro: RegistroPeriodo | None = None
    evidencias: tuple[Evidencia, ...] = ()

    def __post_init__(self):
        exigir_tipo(self.evidencias, tuple, "evidencias")
        organizar_registros(self.definicao, () if self.registro is None else (self.registro,), self.evidencias)

    @property
    def valor(self):
        return self.registro.valor if self.registro is not None else None

    @property
    def fonte(self):
        return self.registro.fonte if self.registro is not None else self.definicao.fonte_metodologica

    @property
    def dado_indisponivel(self):
        return self.valor is None


@dataclass(frozen=True)
class VisaoExecutiva:
    cartoes: tuple[CartaoExecutivo, ...]

    def __post_init__(self):
        exigir_tipo(self.cartoes, tuple, "cartoes")
        for cartao in self.cartoes:
            exigir_tipo(cartao, CartaoExecutivo, "cartao")

    @property
    def dimensoes(self):
        return tuple((dimensao, tuple(c for c in self.cartoes if c.definicao.dimensao is dimensao))
                     for dimensao in DimensaoESG)

    @property
    def sem_dados_suficientes(self):
        return tuple(c for c in self.cartoes if c.dado_indisponivel)


def montar_visao_executiva(repositorio):
    """Usa somente leitores injetados. Um cartão por registro, sem eleger o último.

    Uma definição sem registros gera um cartão sem medição, nunca zero ou período
    presumido. Não soma, calcula percentuais, converte unidades ou une perímetros.
    """
    definicoes = repositorio.listar_definicoes()
    if type(definicoes) not in (tuple, list):
        raise ContratoESGInvalido("Listagem executiva exige contratos em memória.")
    cartoes = []
    codigos = set()
    for definicao in definicoes:
        exigir_tipo(definicao, DefinicaoIndicador, "definicao")
        if definicao.codigo in codigos:
            raise ContratoESGInvalido("Definição duplicada na consulta executiva.")
        codigos.add(definicao.codigo)
        serie = repositorio.listar_registros(definicao.codigo, definicao.perimetro)
        exigir_tipo(serie, SerieIndicador, "serie")
        if serie.definicao != definicao:
            raise ContratoESGInvalido("Definição divergente durante a consulta executiva.")
        serie = organizar_registros(definicao, serie.registros, serie.evidencias)
        if not serie.registros:
            cartoes.append(CartaoExecutivo(definicao))
        for registro in serie.registros:
            evidencias = tuple(e for e in serie.evidencias if e.registro_id == registro.identificador)
            cartoes.append(CartaoExecutivo(definicao, registro, evidencias))
    return VisaoExecutiva(tuple(cartoes))
