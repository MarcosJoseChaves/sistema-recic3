"""Regressões independentes ESG-MAZ-03A-R1; dados exclusivamente sintéticos."""
from dataclasses import replace
from datetime import date
from decimal import (Decimal, DefaultContext, Inexact, Rounded, ROUND_DOWN,
                     localcontext)
from fractions import Fraction
from unittest import TestCase

from modulos.esg.contratos import FonteDocumental, Perimetro, Periodo, SituacaoRegistro
from modulos.esg.validacoes import ContratoESGInvalido
from modulos.esg.maz.catalogo import VERSAO
from modulos.esg.maz.modelos import (ComponenteMassa, Disponibilidade, OrigemValor,
    QualidadeInformacao, ReconciliacaoFisica, UnidadeMassa)
from modulos.esg.maz.trcs import ResultadoTRCS, AvaliacaoTRCS, calcular_trcs, avaliar_metodologia


class TestInvariantesTRCS(TestCase):
    def componente(self, valor, fonte=None):
        return ComponenteMassa(None if valor is None else Decimal(valor), UnidadeMassa.KG,
            Periodo(2042, 3), Perimetro('teste', 'Recorte sintético'),
            fonte or FonteDocumental('doc', 'Documento sintético'), OrigemValor.DECLARADO)

    def resultado(self, n='2', d='8'):
        return calcular_trcs(self.componente(n), self.componente(d), versao_metodologica=VERSAO)

    def test_construcao_direta_percentual_incoerente(self):
        r = self.resultado()
        with self.assertRaises(ContratoESGInvalido):
            ResultadoTRCS(r.rejeitos, r.entrada, VERSAO, Disponibilidade.INFORMADO,
                          Decimal('99'), Fraction(25), 50)

    def test_replace_percentual_e_fracao_incoerentes(self):
        r = self.resultado()
        for campos in ({'percentual': Decimal('99')}, {'percentual_exato': Fraction(99)},
                       {'percentual': 25}, {'percentual': Decimal('NaN')},
                       {'percentual_exato': 25}, {'precisao_decimal': True}):
            with self.subTest(campos=campos), self.assertRaises(ContratoESGInvalido):
                replace(r, **campos)

    def test_replace_disponibilidade_e_componentes(self):
        r = self.resultado()
        for campos in ({'disponibilidade': Disponibilidade.INDISPONIVEL},
                       {'disponibilidade': 'informado'}, {'entrada': self.componente(None)},
                       {'entrada': self.componente('0')}, {'percentual': None},
                       {'entrada': replace(r.entrada, periodo=Periodo(2042, 4))}):
            with self.subTest(campos=campos), self.assertRaises(ContratoESGInvalido):
                replace(r, **campos)

    def test_replace_qualidade_situacao_origem(self):
        r = self.resultado()
        for campos in ({'qualidade': QualidadeInformacao.VALIDADO},
                       {'situacao': SituacaoRegistro.APROVADO},
                       {'situacao': SituacaoRegistro.EM_ANALISE},
                       {'origem': OrigemValor.DECLARADO}, {'origem': 'calculado'}):
            with self.subTest(campos=campos), self.assertRaises(ContratoESGInvalido):
                replace(r, **campos)

    def test_construcao_direta_e_replace_publicacao(self):
        a = avaliar_metodologia(self.resultado())
        with self.assertRaises(ContratoESGInvalido):
            AvaliacaoTRCS(a.resultado, None, a.pendencias, True)
        for campos in ({'publicavel_oficialmente': True}, {'publicavel_oficialmente': 0},
                       {'pendencias': ()}, {'pendencias': list(a.pendencias)}):
            with self.subTest(campos=campos), self.assertRaises(ContratoESGInvalido):
                replace(a, **campos)

    def test_replace_reconciliacao_sem_recalcular_pendencias(self):
        r = self.resultado()
        a = avaliar_metodologia(r)
        doc = ReconciliacaoFisica(r.entrada.periodo, r.entrada.perimetro, r.entrada.fonte)
        with self.assertRaises(ContratoESGInvalido):
            replace(a, reconciliacao=doc)
        with self.assertRaises(ContratoESGInvalido):
            replace(a, reconciliacao=replace(doc, periodo=Periodo(2042, 4)))
        self.assertFalse(avaliar_metodologia(r, reconciliacao=doc).publicavel_oficialmente)

    def test_identidade_fontes_conflitantes(self):
        n = self.componente('2')
        for fonte in (FonteDocumental('doc', 'Outro documento'),
                      FonteDocumental('doc', 'Documento sintético', date(2042, 3, 1))):
            with self.subTest(fonte=fonte), self.assertRaises(ContratoESGInvalido):
                calcular_trcs(n, self.componente('8', fonte), versao_metodologica=VERSAO)
        r = self.resultado()
        doc = ReconciliacaoFisica(r.entrada.periodo, r.entrada.perimetro,
                                 FonteDocumental('doc', 'Outro documento'))
        with self.assertRaises(ContratoESGInvalido):
            avaliar_metodologia(r, reconciliacao=doc)

    def test_contexto_decimal_default_e_atual_adversos(self):
        original = DefaultContext.copy()
        try:
            DefaultContext.traps[Inexact] = True
            DefaultContext.traps[Rounded] = True
            DefaultContext.rounding = ROUND_DOWN
            DefaultContext.Emax = 2
            DefaultContext.Emin = -2
            with localcontext() as ctx:
                ctx.prec = 3
                ctx.traps[Inexact] = True
                ctx.rounding = ROUND_DOWN
                r = self.resultado('2', '3')
                self.assertEqual(r.percentual_exato, Fraction(200, 3))
                self.assertEqual(r.percentual, Decimal('66.' + '6' * 47 + '7'))
                self.assertEqual(ctx.prec, 3)
                self.assertTrue(ctx.traps[Inexact])
            self.assertTrue(DefaultContext.traps[Inexact])
        finally:
            for campo in ('prec', 'rounding', 'Emin', 'Emax', 'capitals', 'clamp'):
                setattr(DefaultContext, campo, getattr(original, campo))
            for sinal in original.traps:
                DefaultContext.traps[sinal] = original.traps[sinal]
                DefaultContext.flags[sinal] = original.flags[sinal]

    def test_fontes_distintas_ou_iguais_consistentes_aceitas(self):
        r = self.resultado()
        self.assertEqual(replace(r), r)
        entrada = replace(r.entrada, fonte=FonteDocumental('outro', 'Outro documento'))
        novo = calcular_trcs(r.rejeitos, entrada, versao_metodologica=VERSAO)
        self.assertEqual(novo.percentual, r.percentual)
        self.assertIs(novo.entrada.fonte, entrada.fonte)

    def test_resultados_indisponiveis_nao_forjam_valores(self):
        for r in (self.resultado(None, '8'), self.resultado('2', '0')):
            self.assertIsNone(r.percentual)
            self.assertEqual(replace(r), r)
            with self.assertRaises(ContratoESGInvalido):
                replace(r, percentual=Decimal('0'), percentual_exato=Fraction(0))
