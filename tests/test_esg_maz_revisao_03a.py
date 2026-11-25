"""Reproduções externas: expectativas de segurança dos contratos; NÃO alteram a baseline."""
from dataclasses import replace
from decimal import Decimal, DefaultContext, ROUND_DOWN, ROUND_UP, Inexact
from fractions import Fraction
from unittest import TestCase
from modulos.esg.contratos import FonteDocumental, Periodo, Perimetro, SituacaoRegistro
from modulos.esg.maz.modelos import ComponenteMassa, UnidadeMassa, OrigemValor, QualidadeInformacao
from modulos.esg.maz.trcs import calcular_trcs, avaliar_metodologia
from modulos.esg.validacoes import ContratoESGInvalido

class TestRevisaoMAZ03A(TestCase):
    def resultado(self):
        def componente(massa):
            return ComponenteMassa(Decimal(massa), UnidadeMassa.KG, Periodo(2048, 2),
                Perimetro("sintetico", "Universo sintético de revisão"),
                FonteDocumental("sintetica", "Fonte sintética, sem documento real"),
                OrigemValor.DECLARADO)
        return calcular_trcs(componente("1"), componente("3"),
                             versao_metodologica="revisao-sintetica-v1")

    def test_resultado_rejeita_percentual_e_razao_contraditorios(self):
        r = self.resultado()
        with self.assertRaises(ContratoESGInvalido):
            replace(r, percentual=Decimal("99"), percentual_exato=Fraction(1, 7))

    def test_resultado_rejeita_validacao_aprovacao_sem_decisao(self):
        with self.assertRaises(ContratoESGInvalido):
            replace(self.resultado(), qualidade=QualidadeInformacao.VALIDADO,
                    situacao=SituacaoRegistro.APROVADO)

    def test_avaliacao_rejeita_publicacao_sem_autorizacao(self):
        a = avaliar_metodologia(self.resultado())
        with self.assertRaises(ContratoESGInvalido):
            replace(a, pendencias=(), publicavel_oficialmente=True)

    def test_avaliador_revalida_resultado_recebido(self):
        # R1: a construção segura pode rejeitar antes do avaliador.
        with self.assertRaises(ContratoESGInvalido):
            forjado = replace(self.resultado(), percentual=Decimal("99"))
            avaliar_metodologia(forjado)

    def test_percentual_independente_de_defaultcontext_rounding(self):
        anterior = DefaultContext.rounding
        try:
            DefaultContext.rounding = ROUND_DOWN
            a = self.resultado()
            DefaultContext.rounding = ROUND_UP
            b = self.resultado()
            self.assertEqual(a.percentual_exato, b.percentual_exato)
            self.assertEqual(a.percentual, b.percentual)
        finally:
            DefaultContext.rounding = anterior

    def test_calculo_nao_herda_trap_inexact_global(self):
        anterior = DefaultContext.traps[Inexact]
        try:
            DefaultContext.traps[Inexact] = True
            r = self.resultado()
            self.assertEqual(r.percentual_exato, Fraction(100, 3))
        finally:
            DefaultContext.traps[Inexact] = anterior

    def test_pendencias_rejeitam_lista_mutavel(self):
        a = avaliar_metodologia(self.resultado())
        with self.assertRaises(ContratoESGInvalido):
            replace(a, pendencias=[])

    def test_fontes_com_identidade_conflitante_sao_rejeitadas(self):
        r = self.resultado()
        entrada = replace(r.entrada, fonte=FonteDocumental(
            r.rejeitos.fonte.identificador, "Outra referência sintética incompatível"))
        with self.assertRaises(ContratoESGInvalido):
            calcular_trcs(r.rejeitos, entrada, versao_metodologica=r.versao_metodologica)
