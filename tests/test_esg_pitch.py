"""ESG-04B: executar somente em contêiner isolado autorizado, nunca diretamente no Windows.

Sem app.py, aplicação web, servidor, HTML ou dependências de banco.
As barreiras do ambiente devem preceder inclusive os imports deste módulo.
"""

from dataclasses import replace
from datetime import date
from decimal import Decimal
import unittest

from modulos.esg.contratos import DimensaoESG, SituacaoIndicador, SituacaoRegistro
from modulos.esg.demonstracao import (
    AVISO_DEMONSTRACAO, RepositorioDemonstrativoESG,
    criar_repositorio_com_exemplos, criar_repositorio_executivo_sintetico,
)
from modulos.esg.demonstracao_pitch import criar_repositorio_pitch_fgv
from modulos.esg.executivo import montar_visao_executiva
from modulos.esg.repositorio import IntegridadeESGError


class TestPitchFGVESG(unittest.TestCase):
    def setUp(self):
        self.repo = criar_repositorio_pitch_fgv()
        self.cartoes = {
            c.definicao.codigo: c for c in montar_visao_executiva(self.repo).cartoes
        }

    def test_sete_indicadores_com_nomes_dimensoes_valores_e_unidades(self):
        esperados = {
            "recuperacao": ("Recuperação de materiais", DimensaoESG.AMBIENTAL, Decimal("12"), "t", "Toneladas no exemplo fictício"),
            "destinacao": ("Destinação de rejeitos", DimensaoESG.AMBIENTAL, Decimal("0"), "t", "Toneladas no exemplo fictício"),
            "compostagem": ("Compostagem de orgânicos", DimensaoESG.AMBIENTAL, Decimal("3"), "t", "Toneladas no exemplo fictício"),
            "associacoes": ("Participação das associações", DimensaoESG.SOCIAL, Decimal("2"), "associações", "Associações distintas no exemplo fictício"),
            "inclusao": ("Inclusão produtiva", DimensaoESG.SOCIAL, None, "pessoas", "Pessoas distintas no exemplo fictício"),
            "capacitacoes": ("Capacitações", DimensaoESG.SOCIAL, None, "eventos", "Eventos de capacitação no exemplo fictício"),
            "rastreabilidade": ("Rastreabilidade documental", DimensaoESG.GOVERNANCA, Decimal("4"), "documentos", "Documentos distintos no exemplo fictício"),
        }
        self.assertEqual(set(self.cartoes), {"exec-" + tema for tema in esperados})
        for tema, (nome, dimensao, valor, simbolo, descricao) in esperados.items():
            with self.subTest(tema=tema):
                cartao = self.cartoes["exec-" + tema]
                self.assertEqual(cartao.definicao.nome, nome + " — exemplo fictício")
                self.assertIs(cartao.definicao.dimensao, dimensao)
                self.assertEqual(cartao.valor, valor)
                if valor is not None:
                    self.assertIs(type(cartao.valor), Decimal)
                self.assertEqual(cartao.definicao.unidade.simbolo, simbolo)
                self.assertEqual(cartao.definicao.unidade.descricao, descricao)

    def test_seis_registros_com_competencia_e_referencia_exatas(self):
        registros = [c.registro for c in self.cartoes.values() if c.registro is not None]
        self.assertEqual(len(registros), 6)
        for registro in registros:
            self.assertEqual(registro.periodo.competencia, "2026-10")
            self.assertEqual(registro.periodo.ano, 2026)
            self.assertEqual(registro.periodo.mes, 10)
            self.assertEqual(registro.data_referencia, date(2026, 10, 31))
        self.assertEqual(len({r.identificador for r in registros}), 6)

    def test_zero_indisponivel_e_sem_registro_permanecem_distintos(self):
        zero = self.cartoes["exec-destinacao"]
        indisponivel = self.cartoes["exec-inclusao"]
        sem_registro = self.cartoes["exec-capacitacoes"]
        self.assertIsNotNone(zero.registro)
        self.assertEqual(zero.valor, Decimal("0"))
        self.assertFalse(zero.dado_indisponivel)
        self.assertIsNotNone(indisponivel.registro)
        self.assertIsNone(indisponivel.valor)
        self.assertTrue(indisponivel.dado_indisponivel)
        self.assertIsNone(sem_registro.registro)
        self.assertIsNone(sem_registro.valor)
        serie = self.repo.listar_registros("exec-capacitacoes", sem_registro.definicao.perimetro)
        self.assertEqual(serie.registros, ())
        self.assertEqual(serie.evidencias, ())
        self.assertEqual(
            {c.definicao.codigo for c in montar_visao_executiva(self.repo).sem_dados_suficientes},
            {"exec-inclusao", "exec-capacitacoes"},
        )

    def test_definicoes_propostas_e_registros_rascunhos_sem_aprovacao(self):
        for cartao in self.cartoes.values():
            self.assertIs(cartao.definicao.situacao, SituacaoIndicador.PROPOSTO)
            self.assertIsNone(cartao.definicao.fonte_aprovacao)
            if cartao.registro is not None:
                self.assertIs(cartao.registro.situacao, SituacaoRegistro.RASCUNHO)
                self.assertIsNone(cartao.registro.fonte_aprovacao)

    def test_fontes_perimetros_e_evidencias_explicitamente_ficticios(self):
        fontes = {}
        evidencias_ids = set()
        for codigo, cartao in self.cartoes.items():
            tema = codigo.removeprefix("exec-")
            definicao = cartao.definicao
            self.assertEqual(definicao.perimetro.codigo, "pitch-recorte-" + tema)
            self.assertIn("exemplo fictício", definicao.perimetro.descricao)
            self.assertIn("sem correspondência com resultados municipais", definicao.descricao)
            self.assertIn("NO-GO OPERACIONAL", definicao.descricao)
            self.assertIn("exemplo fictício", definicao.metodologia)
            self.assertEqual(definicao.fonte_metodologica.identificador, "pitch-fonte-" + tema)
            documentos = [definicao.fonte_metodologica]
            if cartao.registro is not None:
                self.assertIs(cartao.registro.perimetro, definicao.perimetro)
                self.assertIs(cartao.registro.fonte, definicao.fonte_metodologica)
                documentos.append(cartao.registro.fonte)
                self.assertEqual(len(cartao.evidencias), 4 if tema == "rastreabilidade" else 1)
            for evidencia in cartao.evidencias:
                self.assertEqual(evidencia.registro_id, cartao.registro.identificador)
                self.assertNotIn(evidencia.identificador, evidencias_ids)
                evidencias_ids.add(evidencia.identificador)
                self.assertIn("exemplo fictício", evidencia.tipo)
                self.assertIn(AVISO_DEMONSTRACAO, evidencia.observacoes)
                documentos.append(evidencia.fonte)
            for fonte in documentos:
                self.assertIn(AVISO_DEMONSTRACAO, fonte.referencia_documental)
                self.assertIn("exemplo fictício", fonte.referencia_documental)
                self.assertIn("sem correspondência com resultados municipais", fonte.referencia_documental)
                self.assertEqual(fonte.data_documento, date(2026, 10, 31))
                self.assertEqual(fontes.setdefault(fonte.identificador, fonte), fonte)
        self.assertEqual(len({c.definicao.perimetro.codigo for c in self.cartoes.values()}), 7)

    def test_governanca_tem_quatro_fontes_documentais_distintas_vinculadas(self):
        cartao = self.cartoes["exec-rastreabilidade"]
        self.assertEqual(cartao.valor, Decimal("4"))
        fontes = [e.fonte for e in cartao.evidencias]
        self.assertEqual(len(fontes), 4)
        self.assertEqual(
            {f.identificador for f in fontes},
            {"pitch-documento-governanca-" + str(n) for n in (1, 2, 3, 4)},
        )
        self.assertEqual(len({f.referencia_documental for f in fontes}), 4)
        self.assertNotIn(cartao.registro.fonte.identificador, {f.identificador for f in fontes})
        for evidencia in cartao.evidencias:
            self.assertEqual(evidencia.registro_id, cartao.registro.identificador)
        self.assertIn("não representa percentual de conformidade", cartao.definicao.metodologia)

    def test_consulta_recusa_perimetro_de_outro_indicador(self):
        recuperacao = self.cartoes["exec-recuperacao"]
        rejeitos = self.cartoes["exec-destinacao"]
        with self.assertRaises(IntegridadeESGError):
            self.repo.listar_registros(recuperacao.definicao.codigo, rejeitos.definicao.perimetro)

    def test_cargas_independentes_e_fixtures_anteriores_preservadas(self):
        exemplos = criar_repositorio_com_exemplos()
        executiva = criar_repositorio_executivo_sintetico()
        antes = (montar_visao_executiva(exemplos), montar_visao_executiva(executiva))
        outra = criar_repositorio_pitch_fgv()
        self.assertIs(type(outra), RepositorioDemonstrativoESG)
        self.assertIsNot(outra, self.repo)
        self.assertEqual((montar_visao_executiva(exemplos), montar_visao_executiva(executiva)), antes)
        self.assertEqual(tuple(c.valor for c in antes[0].cartoes), (Decimal("0"), None, Decimal("12.3400")))
        antiga = {c.definicao.codigo: c for c in antes[1].cartoes}
        self.assertEqual(antiga["exec-recuperacao"].valor, Decimal("12.3400"))
        self.assertEqual(antiga["exec-recuperacao"].definicao.unidade.simbolo, "ud")
        self.assertIsNone(antiga["exec-rastreabilidade"].valor)
        self.assertIsNone(antiga["exec-capacitacoes"].registro)
        self.assertIsNone(antiga["exec-inclusao"].valor)
        self.assertEqual(antiga["exec-inclusao"].registro.periodo.competencia, "2034")
        for cartao in antes[1].cartoes:
            self.assertTrue(cartao.definicao.perimetro.codigo.startswith("exec-recorte-"))
        definicao = outra.listar_definicoes()[0]
        outra.salvar_definicao(replace(definicao, codigo="pitch-apenas-outra-instancia"), usuario_id=1)
        self.assertEqual(len(outra.listar_definicoes()), 8)
        self.assertEqual(len(self.repo.listar_definicoes()), 7)
        self.assertEqual((montar_visao_executiva(exemplos), montar_visao_executiva(executiva)), antes)
