"""MAZ-02: somente valores sintéticos, sem base acadêmica carregada."""
from dataclasses import FrozenInstanceError, replace
from decimal import Decimal, localcontext
from fractions import Fraction
from importlib import import_module
from unittest import TestCase
from unittest.mock import patch
from modulos.esg.contratos import (FonteDocumental, Perimetro, Periodo,
                                   SituacaoIndicador, SituacaoRegistro)
from modulos.esg.validacoes import ContratoESGInvalido
from modulos.esg.maz.catalogo import criar_catalogo_maz, CODIGOS, VERSAO
from modulos.esg.maz.modelos import (ComponenteMassa, OrigemValor, QualidadeInformacao,
                                   Disponibilidade, ReconciliacaoFisica, UnidadeMassa)
from modulos.esg.maz.trcs import calcular_trcs, avaliar_metodologia

class TestMetodologiaMAZ(TestCase):
    def componente(self, valor, **extra):
        dados = dict(massa=None if valor is None else Decimal(valor), unidade=UnidadeMassa.KG,
                     periodo=Periodo(2042, 3), perimetro=Perimetro("teste", "Recorte sintético"),
                     fonte=FonteDocumental("fonte-teste", "Fonte sintética de teste"),
                     origem=OrigemValor.DECLARADO)
        dados.update(extra)
        return ComponenteMassa(**dados)

    def calcular(self, n="2", d="8", **extras):
        return calcular_trcs(self.componente(n), self.componente(d),
                             versao_metodologica=VERSAO, **extras)

    def test_catalogo_completo_imutavel_e_sem_aprovacao(self):
        catalogo = criar_catalogo_maz()
        self.assertEqual(tuple(d.codigo for d in catalogo.definicoes), CODIGOS)
        self.assertEqual(len(set(CODIGOS)), 16)
        self.assertTrue(all(d.situacao is SituacaoIndicador.PROPOSTO for d in catalogo.definicoes))
        with self.assertRaises(FrozenInstanceError):
            catalogo.versao = "outra"

    def test_catalogo_rejeita_duplicidade_ausencia_e_versao_mista(self):
        c = criar_catalogo_maz()
        for definicoes in (c.definicoes[:-1], c.definicoes[:-1]+(c.definicoes[0],),
                           (replace(c.definicoes[0], versao_metodologica="v2"),)+c.definicoes[1:]):
            with self.subTest(definicoes=definicoes), self.assertRaises(ContratoESGInvalido):
                replace(c, definicoes=definicoes)

    def test_campos_catalogo_e_subcodigos_sem_correspondencia_inventada(self):
        for d in criar_catalogo_maz().definicoes:
            self.assertTrue(d.fontes_verificacao and d.qualidade_minima and d.limitacoes and d.impedimentos)
            self.assertIsNone(d.periodicidade_proposta)
            if d.codigo.startswith("K09"):
                self.assertIn("não confirmada", d.definicao)
                self.assertIsNone(d.unidade)
        with self.assertRaises(ContratoESGInvalido):
            replace(criar_catalogo_maz().definicoes[0], situacao=SituacaoIndicador.APROVADO)

    def test_calculo_preserva_componentes_fontes_e_versao(self):
        n, d = self.componente("2.500"), self.componente("10.000")
        r = calcular_trcs(n, d, versao_metodologica=VERSAO)
        self.assertEqual(r.percentual, Decimal("25"))
        self.assertIs(r.rejeitos, n)
        self.assertIs(r.entrada, d)
        self.assertIs(r.rejeitos.fonte, n.fonte)
        self.assertEqual(r.versao_metodologica, VERSAO)

    def test_divisao_periodica_preserva_fracao_exata_sem_contexto_global(self):
        with localcontext() as ctx:
            ctx.prec = 3
            r = self.calcular("1", "3")
            self.assertEqual(ctx.prec, 3)
        self.assertEqual(r.percentual_exato, Fraction(100, 3))
        self.assertEqual(len(r.percentual.as_tuple().digits), 50)
        self.assertEqual(self.calcular("0.00000001", "0.00000004").percentual, Decimal("25"))

    def test_zero_informado(self):
        self.assertEqual(self.calcular("0", "4").percentual, Decimal("0"))
        self.assertIs(self.calcular("0", "4").disponibilidade, Disponibilidade.INFORMADO)

    def test_ausencia_nao_e_zero(self):
        for n, d in ((None, "8"), ("0", None), (None, None)):
            r = self.calcular(n, d)
            self.assertIsNone(r.percentual)
            self.assertIsNone(r.percentual_exato)
            self.assertIs(r.disponibilidade, Disponibilidade.INDISPONIVEL)

    def test_denominador_zero(self):
        for n in ("0", "2"):
            r = self.calcular(n, "0")
            self.assertIsNone(r.percentual)
            self.assertIs(r.disponibilidade, Disponibilidade.NAO_CALCULAVEL)

    def test_massas_negativas_nao_finitas_e_coercoes_rejeitadas(self):
        for massa in (Decimal("-1"), Decimal("NaN"), Decimal("Infinity"), 1, 1.2, True, "2"):
            with self.subTest(massa=massa), self.assertRaises(ContratoESGInvalido):
                self.componente("1", massa=massa)

    def test_incompatibilidade_unidade_periodo_e_perimetro(self):
        for extra in ({"unidade": UnidadeMassa.TONELADA}, {"periodo": Periodo(2042)},
                      {"periodo": Periodo(2042, 4)}, {"perimetro": Perimetro("outra", "Outro recorte")},
                      {"perimetro": Perimetro("teste", "Descrição divergente")}):
            with self.subTest(extra=extra), self.assertRaises(ContratoESGInvalido):
                calcular_trcs(self.componente("1"), self.componente("4", **extra), versao_metodologica=VERSAO)

    def test_proveniencia_qualidade_e_aprovacao_independentes(self):
        entrada = self.componente("4", origem=OrigemValor.ESTIMADO, qualidade=QualidadeInformacao.VALIDADO)
        r = calcular_trcs(self.componente("1"), entrada, versao_metodologica=VERSAO)
        self.assertIs(r.origem, OrigemValor.CALCULADO)
        self.assertIs(r.qualidade, QualidadeInformacao.PROVISORIO)
        self.assertIs(r.situacao, SituacaoRegistro.RASCUNHO)
        self.assertFalse(avaliar_metodologia(r).publicavel_oficialmente)

    def test_reconciliacao_ausente_identificada_sem_ajustar_resultado(self):
        r = self.calcular()
        a = avaliar_metodologia(r)
        self.assertTrue(any("não apresentada" in p for p in a.pendencias))
        self.assertEqual(r.percentual, Decimal("25"))

    def test_reconciliacao_documentada_nao_aprova_e_rejeita_recorte_distinto(self):
        r = self.calcular()
        doc = ReconciliacaoFisica(r.entrada.periodo, r.entrada.perimetro, r.entrada.fonte)
        self.assertFalse(avaliar_metodologia(r, reconciliacao=doc).publicavel_oficialmente)
        for extra in ({"periodo": Periodo(2041)}, {"perimetro": Perimetro("outra", "Outro")}):
            with self.assertRaises(ContratoESGInvalido):
                avaliar_metodologia(r, reconciliacao=replace(doc, **extra))

    def test_rejeitos_superiores_entrada_nao_geram_ajuste_inventado(self):
        r = self.calcular("6", "4")
        self.assertEqual(r.percentual, Decimal("150"))
        a = avaliar_metodologia(r)
        self.assertFalse(a.publicavel_oficialmente)
        self.assertTrue(any("estoques" in p for p in a.pendencias))

    def test_precisao_e_versao_invalidas(self):
        for p in (0, 27, 201, True, 50.0):
            with self.assertRaises(ContratoESGInvalido):
                self.calcular(precisao=p)
        with self.assertRaises(ContratoESGInvalido):
            calcular_trcs(self.componente("1"), self.componente("2"), versao_metodologica="")

    def test_importacao_sem_carga_e_efeitos_colaterais(self):
        import importlib.util
        from pathlib import Path
        root = Path(__file__).resolve().parents[1] / "modulos/esg/maz"
        # Executa novamente os fontes com imports já resolvidos e I/O negado.
        fontes = {p.name: p.read_text(encoding="utf-8") for p in root.glob("*.py")}
        with patch("builtins.open", side_effect=AssertionError("I/O")), patch("io.open", side_effect=AssertionError("I/O")):
            for nome, fonte in fontes.items():
                namespace = {"__name__": "modulos.esg.maz." + nome[:-3], "__package__": "modulos.esg.maz"}
                exec(compile(fonte, nome, "exec"), namespace)
                self.assertNotIn("CATALOGO", namespace)
