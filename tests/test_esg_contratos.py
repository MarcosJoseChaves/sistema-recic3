"""Contratos ESG em memória: somente biblioteca padrão, sem importar app.py."""

from dataclasses import FrozenInstanceError, replace
from datetime import date, datetime
from decimal import Decimal
import unittest
from unittest.mock import patch
from uuid import UUID

from modulos.esg.contratos import (
    DefinicaoIndicador,
    DimensaoESG,
    Evidencia,
    FonteDocumental,
    Perimetro,
    Periodo,
    RegistroPeriodo,
    SituacaoIndicador,
    SituacaoRegistro,
    UnidadeMedida,
)
from modulos.esg.services import SerieIndicador, organizar_registros
from modulos.esg.validacoes import ContratoESGInvalido


# Material histórico fornecido na tarefa, exclusivamente para testes. Os três
# recortes não são tratados como equivalentes, conciliados ou aprovados.
CASOS_HISTORICOS = (
    ("serie-anual", "Base 2025 — série anual", "10698", "13846", "77.26"),
    ("balanco-detalhado", "Base 2025 — balanço detalhado", "11343.07", "14477.26", "78.35"),
    ("coleta-seletiva", "Coleta seletiva 2025", "1941.26", "5075.45", "38.25"),
)


class TestContratosESG(unittest.TestCase):
    def setUp(self):
        self.fonte = FonteDocumental(
            "mentoria-2026-10-27",
            "Mentoria Marechal Aterro Zero de 27/10/2026, com referência ao Trabalho Final de Estratégias ASG.",
            date(2026, 10, 27),
        )
        self.perimetro = Perimetro("serie-anual", "Base 2025 — série anual")
        self.unidade = UnidadeMedida("t", "Toneladas declaradas na fonte")
        self.definicao = DefinicaoIndicador(
            codigo="historico-teste",
            nome="Caso documental de teste",
            dimensao=DimensaoESG.AMBIENTAL,
            descricao="Transcrição histórica exclusiva de teste.",
            unidade=self.unidade,
            metodologia="Metodologia pendente de validação; sem cálculo nesta etapa.",
            perimetro=self.perimetro,
            fonte_metodologica=self.fonte,
        )
        self.registro = RegistroPeriodo(
            identificador=UUID(int=1),
            indicador_codigo=self.definicao.codigo,
            periodo=Periodo(2025),
            fonte=self.fonte,
            data_referencia=date(2025, 12, 31),
            perimetro=self.perimetro,
        )
        self.evidencia = Evidencia(UUID(int=2), self.registro.identificador, "Referência documental", self.fonte)

    def test_campos_textuais_obrigatorios(self):
        objetos_campos = (
            (self.definicao, ("codigo", "nome", "descricao", "metodologia")),
            (self.fonte, ("identificador", "referencia_documental")),
            (self.perimetro, ("codigo", "descricao")),
            (self.unidade, ("simbolo", "descricao")),
            (self.evidencia, ("tipo",)),
            (self.registro, ("indicador_codigo",)),
        )
        for objeto, campos in objetos_campos:
            for campo in campos:
                for valor in (None, "", "   ", 1, True, [], "texto\x00", " texto "):
                    with self.subTest(contrato=type(objeto).__name__, campo=campo, valor=valor):
                        with self.assertRaises(ContratoESGInvalido):
                            replace(objeto, **{campo: valor})

    def test_codigos_estaveis_e_sem_coercao(self):
        for codigo in ("1codigo", "código", "com espaço", "x" * 65, "codigo/../outro"):
            with self.subTest(codigo=codigo), self.assertRaises(ContratoESGInvalido):
                replace(self.definicao, codigo=codigo)
        self.assertEqual(replace(self.definicao, codigo="ESG.teste_01-v1").codigo, "ESG.teste_01-v1")

    def test_dimensoes_aceitas_e_tipos_invalidos(self):
        for dimensao in DimensaoESG:
            self.assertIs(replace(self.definicao, dimensao=dimensao).dimensao, dimensao)
        for valor in ("ambiental", "outra", None, 1):
            with self.subTest(valor=valor), self.assertRaises(ContratoESGInvalido):
                replace(self.definicao, dimensao=valor)

    def test_unidades_declaradas_sem_conversao(self):
        for simbolo, descricao in (("t", "Toneladas"), ("%", "Percentual documental"), ("kg", "Quilogramas")):
            unidade = UnidadeMedida(simbolo, descricao)
            self.assertEqual(replace(self.definicao, unidade=unidade).unidade, unidade)
        for simbolo in ("", " t", "t ", "t kg", "t\nkg", "t" * 33, 1, None):
            with self.subTest(simbolo=simbolo), self.assertRaises(ContratoESGInvalido):
                UnidadeMedida(simbolo, "Descrição")
        for valor in ("t", None, {}, True):
            with self.subTest(unidade=valor), self.assertRaises(ContratoESGInvalido):
                replace(self.definicao, unidade=valor)

    def test_periodos_anuais_mensais_e_limites(self):
        self.assertEqual(Periodo(2025).competencia, "2025")
        self.assertEqual(Periodo(2025, 2).competencia, "2025-02")
        self.assertEqual(Periodo(1, 1).competencia, "0001-01")
        self.assertEqual(Periodo(9999, 12).competencia, "9999-12")
        self.assertNotEqual(Periodo(2025), Periodo(2025, 1))
        for ano in (0, -1, 10000, "2025", True, 2025.0, None):
            with self.subTest(ano=ano), self.assertRaises(ContratoESGInvalido):
                Periodo(ano)
        for mes in (0, 13, -1, "1", True, 1.0):
            with self.subTest(mes=mes), self.assertRaises(ContratoESGInvalido):
                Periodo(2025, mes)

    def test_data_de_referencia_pertence_ao_periodo(self):
        mensal = replace(self.registro, periodo=Periodo(2024, 2), data_referencia=date(2024, 2, 29))
        self.assertEqual(mensal.periodo.competencia, "2024-02")
        for valor in (date(2024, 12, 31), date(2026, 1, 1), "2025-12-31", datetime(2025, 12, 31), None):
            with self.subTest(valor=valor), self.assertRaises(ContratoESGInvalido):
                replace(self.registro, data_referencia=valor)
        with self.assertRaises(ContratoESGInvalido):
            replace(self.registro, periodo=Periodo(2025, 11))
        # Documento de 2026 pode dar suporte a dados referentes a 2025.
        self.assertNotEqual(self.registro.data_referencia.year, self.fonte.data_documento.year)

    def test_data_documental_opcional_mas_tipagem_estrita(self):
        self.assertIsNone(replace(self.fonte, data_documento=None).data_documento)
        for valor in ("2026-10-27", datetime(2026, 10, 27), 2026):
            with self.subTest(valor=valor), self.assertRaises(ContratoESGInvalido):
                replace(self.fonte, data_documento=valor)

    def test_ausencia_nao_equivale_a_zero(self):
        self.assertIsNone(self.registro.valor)
        zero = replace(self.registro, valor=Decimal("0"))
        self.assertEqual(zero.valor, Decimal("0"))
        self.assertNotEqual(self.registro, zero)
        serie = organizar_registros(self.definicao, [self.registro])
        self.assertIsNone(serie.registros[0].valor)

    def test_valores_decimais_exatos_sem_arredondamento(self):
        for valor in (Decimal("11343.0700"), Decimal("0"), Decimal("-1.25"), Decimal("123.123456789")):
            with self.subTest(valor=valor):
                registro = replace(self.registro, valor=valor)
                self.assertEqual(registro.valor.as_tuple(), valor.as_tuple())

    def test_rejeita_numeros_nao_finitos_e_coercoes(self):
        for valor in (0, 1, True, False, 1.25, "1.25", "1.234,56", [], {}, Decimal("NaN"), Decimal("sNaN"), Decimal("Infinity"), Decimal("-Infinity")):
            with self.subTest(valor=repr(valor)), self.assertRaises(ContratoESGInvalido):
                replace(self.registro, valor=valor)

    def test_tipos_dos_vinculos_e_identificadores(self):
        casos = (
            (self.definicao, "fonte_metodologica", "documento"),
            (self.definicao, "perimetro", "serie-anual"),
            (self.registro, "fonte", None),
            (self.registro, "periodo", "2025"),
            (self.registro, "perimetro", {}),
            (self.registro, "identificador", str(UUID(int=1))),
            (self.evidencia, "fonte", "documento.pdf"),
            (self.evidencia, "identificador", 1),
            (self.evidencia, "registro_id", str(UUID(int=1))),
        )
        for objeto, campo, valor in casos:
            with self.subTest(contrato=type(objeto).__name__, campo=campo), self.assertRaises(ContratoESGInvalido):
                replace(objeto, **{campo: valor})

    def test_observacoes_da_evidencia_opcionais(self):
        self.assertIsNone(self.evidencia.observacoes)
        self.assertEqual(replace(self.evidencia, observacoes="Página a confirmar.").observacoes, "Página a confirmar.")
        for valor in (1, [], " "):
            with self.subTest(valor=valor), self.assertRaises(ContratoESGInvalido):
                replace(self.evidencia, observacoes=valor)

    def test_estados_iniciais_nao_aprovados(self):
        self.assertIs(self.definicao.situacao, SituacaoIndicador.PROPOSTO)
        self.assertIs(self.registro.situacao, SituacaoRegistro.RASCUNHO)
        serie = organizar_registros(self.definicao, [replace(self.registro, valor=Decimal("10698"))])
        self.assertIs(serie.definicao.situacao, SituacaoIndicador.PROPOSTO)
        self.assertIs(serie.registros[0].situacao, SituacaoRegistro.RASCUNHO)

    def test_aprovacao_exige_referencia_documental(self):
        for objeto, status in ((self.definicao, SituacaoIndicador.APROVADO), (self.registro, SituacaoRegistro.APROVADO)):
            with self.subTest(contrato=type(objeto).__name__), self.assertRaises(ContratoESGInvalido):
                replace(objeto, situacao=status)
        with self.assertRaises(ContratoESGInvalido):
            replace(self.registro, situacao=SituacaoRegistro.APROVADO, fonte_aprovacao=self.fonte)
        # Referência inteiramente fictícia: somente formato do contrato, sem ato real.
        aprovacao = FonteDocumental("aprovacao-ficticia-teste", "Decisão fictícia exclusiva de teste.")
        definicao = replace(self.definicao, situacao=SituacaoIndicador.APROVADO, fonte_aprovacao=aprovacao)
        registro = replace(self.registro, valor=Decimal("0"), situacao=SituacaoRegistro.APROVADO, fonte_aprovacao=aprovacao)
        self.assertIs(organizar_registros(definicao, [registro]).registros[0].situacao, SituacaoRegistro.APROVADO)
        with self.assertRaises(ContratoESGInvalido):
            organizar_registros(self.definicao, [registro])

    def test_estados_nao_aprovados_e_estados_invalidos(self):
        for status in (SituacaoIndicador.PROPOSTO, SituacaoIndicador.EM_ANALISE, SituacaoIndicador.REJEITADO):
            self.assertIs(replace(self.definicao, situacao=status).situacao, status)
        for status in (SituacaoRegistro.RASCUNHO, SituacaoRegistro.EM_ANALISE, SituacaoRegistro.REJEITADO):
            self.assertIs(replace(self.registro, situacao=status).situacao, status)
        for objeto, valores in (
            (self.definicao, ("aprovado", True, None, SituacaoRegistro.APROVADO)),
            (self.registro, ("aprovado", True, None, SituacaoIndicador.APROVADO)),
        ):
            for valor in valores:
                with self.subTest(contrato=type(objeto).__name__, valor=valor), self.assertRaises(ContratoESGInvalido):
                    replace(objeto, situacao=valor)
        for objeto in (self.definicao, self.registro):
            with self.assertRaises(ContratoESGInvalido):
                replace(objeto, fonte_aprovacao="aprovado")

    def test_rastreabilidade_da_fonte_e_da_evidencia(self):
        serie = organizar_registros(self.definicao, [self.registro], [self.evidencia])
        self.assertIs(serie.definicao.fonte_metodologica, self.fonte)
        self.assertIs(serie.registros[0].fonte, self.fonte)
        self.assertEqual(serie.evidencias[0].registro_id, serie.registros[0].identificador)
        self.assertIs(serie.evidencias[0].fonte, self.fonte)
        self.assertIn("Trabalho Final de Estratégias ASG", serie.evidencias[0].fonte.referencia_documental)
        with self.assertRaises(ContratoESGInvalido):
            organizar_registros(self.definicao, [self.registro], [replace(self.evidencia, registro_id=UUID(int=999))])

    def test_rejeita_mesmo_identificador_com_fontes_distintas(self):
        divergente = replace(self.fonte, referencia_documental="Outro documento.")
        with self.assertRaises(ContratoESGInvalido):
            organizar_registros(self.definicao, [replace(self.registro, fonte=divergente)])
        with self.assertRaises(ContratoESGInvalido):
            organizar_registros(self.definicao, [self.registro], [replace(self.evidencia, fonte=divergente)])

    def test_rejeita_perimetros_distintos_inclusive_mesmo_codigo(self):
        for perimetro in (Perimetro("balanco", "Balanço detalhado"), replace(self.perimetro, descricao="Outro recorte")):
            with self.subTest(perimetro=perimetro), self.assertRaises(ContratoESGInvalido):
                organizar_registros(self.definicao, [replace(self.registro, perimetro=perimetro)])

    def test_rejeita_registro_de_outro_indicador(self):
        with self.assertRaises(ContratoESGInvalido):
            organizar_registros(self.definicao, [replace(self.registro, indicador_codigo="outro")])

    def test_rejeita_periodos_e_identificadores_duplicados(self):
        duplicado_periodo = replace(self.registro, identificador=UUID(int=3))
        duplicado_id = replace(self.registro, periodo=Periodo(2024), data_referencia=date(2024, 12, 31))
        for registro in (self.registro, duplicado_periodo, duplicado_id):
            with self.subTest(registro=registro), self.assertRaises(ContratoESGInvalido):
                organizar_registros(self.definicao, [self.registro, registro])
        with self.assertRaises(ContratoESGInvalido):
            organizar_registros(self.definicao, [self.registro], [self.evidencia, self.evidencia])

    def test_ordenacao_preserva_periodos_sem_soma_ou_promocao(self):
        mensal = replace(self.registro, identificador=UUID(int=3), periodo=Periodo(2025, 1), data_referencia=date(2025, 1, 31), valor=Decimal("1.23"))
        anterior = replace(self.registro, identificador=UUID(int=4), periodo=Periodo(2024), data_referencia=date(2024, 12, 31), valor=Decimal("2.34"))
        entrada = [mensal, self.registro, anterior]
        serie = organizar_registros(self.definicao, entrada)
        self.assertEqual(serie.registros, (anterior, self.registro, mensal))
        self.assertEqual(entrada, [mensal, self.registro, anterior])
        self.assertEqual([r.valor for r in serie.registros], [Decimal("2.34"), None, Decimal("1.23")])
        self.assertTrue(all(r.situacao is SituacaoRegistro.RASCUNHO for r in serie.registros))
        self.assertEqual(organizar_registros(self.definicao).registros, ())

    def test_organizacao_rejeita_tipos_invalidos_e_iteradores(self):
        for registros, evidencias in ((None, ()), ({}, ()), ([{}], ()), ((), [None])):
            with self.subTest(registros=registros, evidencias=evidencias), self.assertRaises(ContratoESGInvalido):
                organizar_registros(self.definicao, registros, evidencias)

        def iterador_proibido():
            raise AssertionError("Iterador não deve ser consumido")
            yield

        with self.assertRaises(ContratoESGInvalido):
            organizar_registros(self.definicao, iterador_proibido())
        with self.assertRaises(ContratoESGInvalido):
            SerieIndicador(self.definicao, [self.registro])
        with self.assertRaises(ContratoESGInvalido):
            organizar_registros(None)

    def test_contratos_imutaveis(self):
        for objeto, campo in ((self.definicao, "situacao"), (self.registro, "valor"), (self.fonte, "referencia_documental"), (self.perimetro, "codigo"), (self.unidade, "simbolo"), (Periodo(2025), "ano"), (self.evidencia, "registro_id"), (organizar_registros(self.definicao), "registros")):
            with self.subTest(contrato=type(objeto).__name__), self.assertRaises(FrozenInstanceError):
                setattr(objeto, campo, None)

    def test_tres_casos_historicos_exatos_separados_e_nao_aprovados(self):
        series = []
        for caso, (codigo, descricao, massa_a, massa_b, percentual) in enumerate(CASOS_HISTORICOS):
            perimetro = Perimetro(codigo, descricao)
            for indice, valor in enumerate((massa_a, massa_b, percentual), start=1):
                unidade = self.unidade if indice < 3 else UnidadeMedida("%", "Percentual transcrito, sem recálculo")
                definicao = replace(self.definicao, codigo=f"teste-{codigo}-{indice}", perimetro=perimetro, unidade=unidade)
                registro = replace(self.registro, identificador=UUID(int=10 + caso * 3 + indice), indicador_codigo=definicao.codigo, perimetro=perimetro, valor=Decimal(valor))
                evidencia = replace(self.evidencia, identificador=UUID(int=100 + caso * 3 + indice), registro_id=registro.identificador)
                serie = organizar_registros(definicao, [registro], [evidencia])
                series.append(serie)
                self.assertEqual(serie.registros[0].valor.as_tuple(), Decimal(valor).as_tuple())
                self.assertIs(serie.definicao.situacao, SituacaoIndicador.PROPOSTO)
                self.assertIs(serie.registros[0].situacao, SituacaoRegistro.RASCUNHO)
                self.assertIs(serie.registros[0].fonte, self.fonte)
        self.assertEqual(len(series), 9)
        self.assertEqual(len({s.definicao.perimetro for s in series}), 3)
        with self.assertRaises(ContratoESGInvalido):
            organizar_registros(series[0].definicao, [replace(series[3].registros[0], indicador_codigo=series[0].definicao.codigo)])

    def test_contratos_nao_abrem_arquivos_ou_conexoes(self):
        # A importação foi auditada estaticamente: apenas stdlib e módulos puros.
        # A construção/organização também falha se tentar qualquer I/O abaixo.
        with (
            patch("builtins.open", side_effect=AssertionError("Arquivo proibido")),
            patch("io.open", side_effect=AssertionError("Arquivo proibido")),
            patch("socket.socket.connect", side_effect=AssertionError("Rede proibida")),
            patch("socket.create_connection", side_effect=AssertionError("Rede proibida")),
        ):
            fonte = FonteDocumental("fonte-teste", "Referência em memória.")
            definicao = replace(self.definicao, fonte_metodologica=fonte)
            registro = replace(self.registro, fonte=fonte)
            self.assertEqual(organizar_registros(definicao, [registro]).registros, (registro,))


if __name__ == "__main__":
    unittest.main()
