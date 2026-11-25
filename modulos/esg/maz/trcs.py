"""Aritmética da TRCS e avaliação metodológica separadas, sem aprovação automática."""
from dataclasses import dataclass
from decimal import (Context, Decimal, DivisionByZero, InvalidOperation, Overflow,
                     ROUND_HALF_EVEN, localcontext)
from fractions import Fraction
from ..contratos import SituacaoRegistro
from ..validacoes import ContratoESGInvalido, exigir_tipo, validar_codigo, validar_valor
from .modelos import (ComponenteMassa, Disponibilidade, OrigemValor,
                      QualidadeInformacao, ReconciliacaoFisica)

@dataclass(frozen=True)
class ResultadoTRCS:
    rejeitos: ComponenteMassa
    entrada: ComponenteMassa
    versao_metodologica: str
    disponibilidade: Disponibilidade
    percentual: Decimal | None
    percentual_exato: Fraction | None
    precisao_decimal: int
    origem: OrigemValor = OrigemValor.CALCULADO
    qualidade: QualidadeInformacao = QualidadeInformacao.PROVISORIO
    situacao: SituacaoRegistro = SituacaoRegistro.RASCUNHO

    def __post_init__(self):
        exigir_tipo(self.disponibilidade, Disponibilidade, "disponibilidade")
        validar_valor(self.percentual)
        if self.percentual_exato is not None:
            exigir_tipo(self.percentual_exato, Fraction, "percentual_exato")
        if (self.origem is not OrigemValor.CALCULADO
                or self.qualidade is not QualidadeInformacao.PROVISORIO
                or self.situacao is not SituacaoRegistro.RASCUNHO):
            raise ContratoESGInvalido("Resultado aritmético admite apenas origem calculada, qualidade provisória e rascunho.")
        esperado = _derivar(self.rejeitos, self.entrada, self.versao_metodologica,
                           self.precisao_decimal)
        if (self.disponibilidade, self.percentual, self.percentual_exato) != esperado:
            raise ContratoESGInvalido("Resultado TRCS contradiz os componentes ou a precisão informada.")


def _verificar_fontes(*fontes):
    por_id = {}
    for fonte in fontes:
        anterior = por_id.setdefault(fonte.identificador, fonte)
        if anterior != fonte:
            raise ContratoESGInvalido("Identidade documental com referências ou datas conflitantes.")


def _derivar(rejeitos, entrada, versao_metodologica, precisao):
    """Única regra aritmética, usada pela fábrica e pelo contrato de saída."""
    for campo, valor in (("rejeitos", rejeitos), ("entrada", entrada)):
        exigir_tipo(valor, ComponenteMassa, campo)
    validar_codigo(versao_metodologica, "versao_metodologica")
    if type(precisao) is not int or not 28 <= precisao <= 200:
        raise ContratoESGInvalido("precisao: inteiro entre 28 e 200.")
    for campo in ("unidade", "periodo", "perimetro"):
        if getattr(rejeitos, campo) != getattr(entrada, campo):
            raise ContratoESGInvalido(f"{campo}: componentes incompatíveis.")
    _verificar_fontes(rejeitos.fonte, entrada.fonte)
    percentual = exato = None
    if rejeitos.massa is None or entrada.massa is None:
        estado = Disponibilidade.INDISPONIVEL
    elif entrada.massa == 0:
        estado = Disponibilidade.NAO_CALCULAVEL
    else:
        estado = Disponibilidade.INFORMADO
        exato = Fraction(rejeitos.massa) * 100 / Fraction(entrada.massa)
        # Todos os parâmetros explícitos: não herda DefaultContext nem o contexto ativo.
        # ROUND_HALF_EVEN é representação numérica, não regra de publicação municipal.
        contexto = Context(prec=precisao, rounding=ROUND_HALF_EVEN,
                           Emin=-999999, Emax=999999, capitals=1, clamp=0,
                           flags=[], traps=[InvalidOperation, DivisionByZero, Overflow])
        with localcontext(contexto):
            percentual = Decimal(exato.numerator) / Decimal(exato.denominator)
    return estado, percentual, exato


def calcular_trcs(rejeitos, entrada, *, versao_metodologica, precisao=50):
    """100 * rejeitos / entrada; não ajusta estoques nem aprova dados."""
    estado, percentual, exato = _derivar(rejeitos, entrada, versao_metodologica, precisao)
    return ResultadoTRCS(rejeitos, entrada, versao_metodologica, estado,
                         percentual, exato, precisao)

@dataclass(frozen=True)
class AvaliacaoTRCS:
    resultado: ResultadoTRCS
    reconciliacao: ReconciliacaoFisica | None
    pendencias: tuple[str, ...]
    publicavel_oficialmente: bool = False

    def __post_init__(self):
        if self.publicavel_oficialmente is not False:
            raise ContratoESGInvalido("Avaliação preliminar não autoriza publicação oficial.")
        exigir_tipo(self.pendencias, tuple, "pendencias")
        if self.pendencias != _pendencias(self.resultado, self.reconciliacao):
            raise ContratoESGInvalido("Pendências devem corresponder à avaliação dos componentes e evidências.")


def _pendencias(resultado, reconciliacao):
    """Deriva impedimentos sem autenticar fontes ou aprovar dados."""
    exigir_tipo(resultado, ResultadoTRCS, "resultado")
    pendencias = ["Metodologia proposta; aprovação institucional não verificada.",
                  "Pesagem e expedição efetivas devem ser verificadas documentalmente."]
    if reconciliacao is None:
        pendencias.append("Reconciliação física e de estoques não apresentada.")
    else:
        exigir_tipo(reconciliacao, ReconciliacaoFisica, "reconciliacao")
        if (reconciliacao.periodo != resultado.entrada.periodo
                or reconciliacao.perimetro != resultado.entrada.perimetro):
            raise ContratoESGInvalido("Reconciliação de outro período ou perímetro.")
        _verificar_fontes(resultado.rejeitos.fonte, resultado.entrada.fonte, reconciliacao.fonte)
        pendencias.append("Referência de reconciliação apresentada; conteúdo ainda não verificado.")
    if resultado.disponibilidade is not Disponibilidade.INFORMADO:
        pendencias.append("Componentes ausentes ou denominador zero impedem o cálculo.")
    if any(c.origem is OrigemValor.ESTIMADO for c in (resultado.rejeitos, resultado.entrada)):
        pendencias.append("Componente estimado exige conferência de qualidade.")
    if (resultado.rejeitos.massa is not None and resultado.entrada.massa is not None
            and resultado.rejeitos.massa > resultado.entrada.massa):
        pendencias.append("Rejeitos superiores à entrada: examinar fronteiras e estoques; sem ajuste presumido.")
    return tuple(pendencias)


def avaliar_metodologia(resultado, *, reconciliacao=None):
    """Avalia impedimentos conhecidos, sem autenticar fontes ou aprovar dados."""
    return AvaliacaoTRCS(resultado, reconciliacao, _pendencias(resultado, reconciliacao))
