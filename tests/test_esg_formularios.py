"""Conversão de formulários ESG apenas em memória, sem Flask ou repositório."""

from datetime import date
from decimal import Decimal
import unittest
from uuid import UUID

from modulos.esg.contratos import SituacaoIndicador, SituacaoRegistro
from modulos.esg.formularios import (
    definicao_formulario, filtros_formulario, registro_formulario,
)
from modulos.esg.validacoes import ContratoESGInvalido


def dados_definicao():
    return {
        "codigo": "indicador-teste", "nome": "Indicador exclusivo de teste",
        "dimensao": "ambiental", "descricao": "Descrição de teste.",
        "unidade_simbolo": "t", "unidade_descricao": "Toneladas",
        "metodologia": "Metodologia fictícia, não validada.",
        "perimetro_codigo": "recorte-teste", "perimetro_descricao": "Abrangência fictícia.",
        "fonte_id": "fonte-teste", "fonte_referencia": "Documento exclusivo de teste.",
        "fonte_data": "2026-10-27",
    }


def dados_registro():
    return {
        "competencia": "2025", "valor": "", "data_referencia": "2025-12-31",
        "fonte_id": "fonte-registro", "fonte_referencia": "Registro documental de teste.",
        "fonte_data": "", "evidencia_tipo": "", "evidencia_observacoes": "",
    }


class TestFormulariosESG(unittest.TestCase):
    def setUp(self):
        self.definicao = definicao_formulario(dados_definicao())

    def test_definicao_completa_cria_proposta_sem_aprovacao(self):
        self.assertIs(self.definicao.situacao, SituacaoIndicador.PROPOSTO)
        self.assertIsNone(self.definicao.fonte_aprovacao)
        self.assertEqual(self.definicao.fonte_metodologica.data_documento, date(2026, 10, 27))
        self.assertEqual(self.definicao.unidade.simbolo, "t")
        self.assertEqual(self.definicao.perimetro.codigo, "recorte-teste")

    def test_normaliza_espacos_e_quebras_de_linha_dos_textareas(self):
        dados = dados_definicao()
        dados["nome"] = "  Nome de teste  "
        dados["metodologia"] = "Primeira linha\r\nSegunda linha"
        definicao = definicao_formulario(dados)
        self.assertEqual(definicao.nome, "Nome de teste")
        self.assertEqual(definicao.metodologia, "Primeira linha\nSegunda linha")

    def test_campos_obrigatorios_invalidos(self):
        for campo in set(dados_definicao()) - {"fonte_data"}:
            dados = dados_definicao()
            dados[campo] = " "
            with self.subTest(campo=campo), self.assertRaises(ContratoESGInvalido):
                definicao_formulario(dados)
        for campo, valor in (("dimensao", "outra"), ("codigo", "código inválido"),
                             ("unidade_simbolo", "t kg"), ("fonte_data", "2026-02-30"),
                             ("fonte_data", "20261027"), ("descricao", "texto\x00"), ("nome", True)):
            dados = dados_definicao()
            dados[campo] = valor
            with self.subTest(campo=campo, valor=valor), self.assertRaises(ContratoESGInvalido):
                definicao_formulario(dados)

    def test_campos_de_aprovacao_autoria_e_vinculos_sao_recusados(self):
        for campo in ("situacao", "fonte_aprovacao", "aprovado_em", "aprovado_por_usuario_id", "usuario_id", "criado_por_usuario_id", "role"):
            for montar, converter in ((dados_definicao, definicao_formulario),
                                       (dados_registro, lambda d: registro_formulario(d, self.definicao))):
                dados = montar()
                dados[campo] = "aprovado"
                with self.subTest(campo=campo, formulario=montar.__name__), self.assertRaises(ContratoESGInvalido):
                    converter(dados)
        for campo in ("perimetro_codigo", "perimetro", "indicador_codigo", "identificador", "unidade"):
            dados = dados_registro()
            dados[campo] = "outro"
            with self.subTest(campo=campo), self.assertRaises(ContratoESGInvalido):
                registro_formulario(dados, self.definicao)

    def test_valor_ausente_zero_e_precisao_decimal(self):
        for entrada, esperado in (("", None), ("0", Decimal("0")), ("0,00", Decimal("0.00")),
                                  ("1234,567890123456789", Decimal("1234.567890123456789")),
                                  ("-1.2500", Decimal("-1.2500"))):
            dados = dados_registro()
            dados["valor"] = entrada
            registro, evidencias = registro_formulario(dados, self.definicao)
            with self.subTest(entrada=entrada):
                self.assertEqual(registro.valor, esperado)
                if esperado is not None:
                    self.assertIs(type(registro.valor), Decimal)
                    self.assertEqual(registro.valor.as_tuple(), esperado.as_tuple())
                self.assertIs(registro.situacao, SituacaoRegistro.RASCUNHO)
                self.assertIsNone(registro.fonte_aprovacao)
                self.assertIs(registro.perimetro, self.definicao.perimetro)
                self.assertIs(type(registro.identificador), UUID)
                self.assertEqual(evidencias, ())

    def test_periodo_e_data_de_referencia(self):
        dados = dados_registro()
        dados["competencia"] = "2025-12"
        registro, _ = registro_formulario(dados, self.definicao)
        self.assertEqual(registro.periodo.competencia, "2025-12")
        for campo, valor in (("competencia", "2025-13"), ("competencia", "2025-1"),
                             ("competencia", "0000"), ("competencia", "2025-11"),
                             ("data_referencia", "2026-01-01"), ("data_referencia", "2025-02-30"),
                             ("data_referencia", ""), ("data_referencia", "20251231")):
            dados = dados_registro()
            dados[campo] = valor
            with self.subTest(campo=campo, valor=valor), self.assertRaises(ContratoESGInvalido):
                registro_formulario(dados, self.definicao)

    def test_rejeita_valores_ambiguos_nao_finitos_e_notacao_cientifica(self):
        for valor in ("1.234,56", "1,234.56", "1 234", "NaN", "Infinity", "1e3", "1,2,3", ".5", "1.", "9" * 257, 1, True):
            dados = dados_registro()
            dados["valor"] = valor
            with self.subTest(valor=valor), self.assertRaises(ContratoESGInvalido):
                registro_formulario(dados, self.definicao)

    def test_evidencia_usa_mesma_fonte_e_uuid_do_registro(self):
        dados = dados_registro()
        dados.update(evidencia_tipo="Memória documental de teste", evidencia_observacoes="Referência a confirmar.")
        registro, evidencias = registro_formulario(dados, self.definicao)
        self.assertEqual(len(evidencias), 1)
        self.assertEqual(evidencias[0].registro_id, registro.identificador)
        self.assertIs(evidencias[0].fonte, registro.fonte)
        self.assertNotEqual(evidencias[0].identificador, registro.identificador)
        dados["evidencia_tipo"] = ""
        with self.assertRaises(ContratoESGInvalido):
            registro_formulario(dados, self.definicao)

    def test_filtros_apenas_leitura(self):
        self.assertEqual(filtros_formulario({}), (None, None))
        periodo, situacao = filtros_formulario({"competencia": "2025-12", "situacao": "aprovado"})
        self.assertEqual(periodo.competencia, "2025-12")
        self.assertIs(situacao, SituacaoRegistro.APROVADO)
        for dados in ({"situacao": "inexistente"}, {"competencia": "2025-13"}, {"perimetro": "outro"}):
            with self.subTest(dados=dados), self.assertRaises(ContratoESGInvalido):
                filtros_formulario(dados)


if __name__ == "__main__":
    unittest.main()
