"""ESG-03C1: usar somente sob executor offline com barreiras antes dos imports."""

from dataclasses import FrozenInstanceError, replace
from decimal import Decimal
import re
import sys
import unittest
from unittest.mock import Mock, patch

from flask import Flask
from flask_login import UserMixin

from modulos.esg import criar_blueprint_esg
from modulos.esg.contratos import DimensaoESG, SituacaoIndicador, SituacaoRegistro
from modulos.esg.demonstracao import (
    AVISO_DEMONSTRACAO, RepositorioDemonstrativoESG,
    criar_repositorio_com_exemplos, criar_repositorio_executivo_sintetico,
)
from modulos.esg.demonstracao_web import criar_aplicacao_demonstrativa, autenticar_administrador_demonstrativo
from modulos.esg.executivo import CartaoExecutivo, montar_visao_executiva
from modulos.esg.repositorio import PersistenciaESGError
from modulos.esg.services import organizar_registros
from modulos.esg.validacoes import ContratoESGInvalido


EXECUTIVA = "/esg/demonstracao/executiva"


class TestExecutivoESG(unittest.TestCase):
    def setUp(self):
        self.bloqueios = [self.enterContext(patch(alvo, side_effect=AssertionError("I/O real proibido"))) for alvo in (
            "psycopg2.connect", "socket.socket.connect", "socket.socket.bind",
            "socket.create_connection", "socket.getaddrinfo", "dotenv.load_dotenv",
            "modulos.esg.repositorio.RepositorioESG.__init__",
        )]
        self.repo = criar_repositorio_executivo_sintetico()
        self.app = criar_aplicacao_demonstrativa(self.repo)
        self.client = self.app.test_client()
        autenticar_administrador_demonstrativo(self.client)

    def tearDown(self):
        for bloqueio in self.bloqueios:
            bloqueio.assert_not_called()

    def test_ausencia_de_dados_preserva_tres_dimensoes_sem_zero_artificial(self):
        repo = RepositorioDemonstrativoESG()
        visao = montar_visao_executiva(repo)
        self.assertEqual(visao.cartoes, ())
        self.assertEqual(tuple(d for d, _ in visao.dimensoes), tuple(DimensaoESG))
        self.assertTrue(all(not cartoes for _, cartoes in visao.dimensoes))
        app = criar_aplicacao_demonstrativa(repo)
        client = app.test_client()
        autenticar_administrador_demonstrativo(client)
        resposta = client.get(EXECUTIVA)
        self.assertEqual(resposta.status_code, 200)
        texto = resposta.get_data(as_text=True)
        self.assertEqual(texto.count("nenhuma definição demonstrativa nesta dimensão"), 3)
        self.assertIn("Dado indisponível", texto)
        self.assertIn("nenhum cartão carregado", texto)
        self.assertNotIn("Nenhuma ausência de valor identificada", texto)
        self.assertNotIn("0 ud", texto)

    def test_zero_decimal_e_ausencias_sem_coercao_ou_arredondamento(self):
        visao = montar_visao_executiva(self.repo)
        por_codigo = {c.definicao.codigo: c for c in visao.cartoes}
        zero = por_codigo["exec-destinacao"]
        decimal = por_codigo["exec-recuperacao"]
        self.assertFalse(zero.dado_indisponivel)
        self.assertIs(type(zero.valor), Decimal)
        self.assertEqual(zero.valor, Decimal("0"))
        self.assertEqual(decimal.valor.as_tuple(), Decimal("12.3400").as_tuple())
        self.assertIs(decimal.valor, decimal.registro.valor)
        self.assertTrue(por_codigo["exec-compostagem"].dado_indisponivel)
        self.assertIsNone(por_codigo["exec-capacitacoes"].registro)
        self.assertIsNone(por_codigo["exec-capacitacoes"].valor)
        self.assertIsNotNone(por_codigo["exec-compostagem"].registro.periodo)

    def test_periodos_e_perimetros_nao_sao_agregados_ou_trocados(self):
        repo = criar_repositorio_com_exemplos()
        visao = montar_visao_executiva(repo)
        self.assertEqual(len(visao.cartoes), 3)
        self.assertEqual(tuple(c.registro.periodo.competencia for c in visao.cartoes), ("2030", "2030-01", "2030-02"))
        self.assertEqual(tuple(c.valor for c in visao.cartoes), (Decimal("0"), None, Decimal("12.3400")))
        executiva = montar_visao_executiva(self.repo)
        self.assertEqual(len({c.definicao.perimetro.codigo for c in executiva.cartoes}), 7)
        for cartao in executiva.cartoes:
            if cartao.registro:
                self.assertIs(cartao.registro.perimetro, cartao.definicao.perimetro)

    def test_fontes_e_evidencias_preservam_objetos_e_vinculos(self):
        for cartao in montar_visao_executiva(self.repo).cartoes:
            self.assertIn(AVISO_DEMONSTRACAO, cartao.fonte.referencia_documental)
            self.assertEqual(cartao.fonte.data_documento.year, 2035)
            serie = self.repo.listar_registros(cartao.definicao.codigo, cartao.definicao.perimetro)
            esperado = tuple(e for e in serie.evidencias if cartao.registro and e.registro_id == cartao.registro.identificador)
            self.assertEqual(cartao.evidencias, esperado)
            for evidencia in cartao.evidencias:
                self.assertEqual(evidencia.registro_id, cartao.registro.identificador)
                self.assertIs(evidencia.fonte, cartao.registro.fonte)

    def test_estados_iniciais_modelo_imutavel_e_evidencias_nao_mutaveis(self):
        visao = montar_visao_executiva(self.repo)
        for cartao in visao.cartoes:
            self.assertIs(cartao.definicao.situacao, SituacaoIndicador.PROPOSTO)
            self.assertIsNone(cartao.definicao.fonte_aprovacao)
            if cartao.registro:
                self.assertIs(cartao.registro.situacao, SituacaoRegistro.RASCUNHO)
                self.assertIsNone(cartao.registro.fonte_aprovacao)
        with self.assertRaises(FrozenInstanceError):
            visao.cartoes = ()
        with self.assertRaises(FrozenInstanceError):
            visao.cartoes[0].registro = None
        with self.assertRaises(ContratoESGInvalido):
            CartaoExecutivo(visao.cartoes[0].definicao, evidencias=[])

    def test_consulta_usa_somente_interface_de_leitura(self):
        definicao = self.repo.listar_definicoes()[0]
        leitores = Mock(spec=["listar_definicoes", "listar_registros"])
        leitores.listar_definicoes.return_value = (definicao,)
        leitores.listar_registros.return_value = self.repo.listar_registros(definicao.codigo, definicao.perimetro)
        visao = montar_visao_executiva(leitores)
        self.assertEqual(len(visao.cartoes), 1)
        leitores.listar_definicoes.assert_called_once_with()
        leitores.listar_registros.assert_called_once_with(definicao.codigo, definicao.perimetro)

    def test_definicao_divergente_duplicada_ou_serie_ausente_falha_segura(self):
        definicao = self.repo.listar_definicoes()[0]
        leitores = Mock(spec=["listar_definicoes", "listar_registros"])
        leitores.listar_definicoes.return_value = (definicao,)
        for serie in (None, organizar_registros(replace(definicao, nome="Outra definição sintética"))):
            leitores.listar_registros.return_value = serie
            with self.subTest(serie=type(serie).__name__), self.assertRaises(ContratoESGInvalido):
                montar_visao_executiva(leitores)
        leitores.listar_definicoes.return_value = (definicao, definicao)
        leitores.listar_registros.return_value = organizar_registros(definicao)
        with self.assertRaises(ContratoESGInvalido):
            montar_visao_executiva(leitores)

    def test_renderizacao_ficticia_com_tematicas_periodos_fontes_e_detalhes_funcionais(self):
        resposta = self.client.get(EXECUTIVA)
        self.assertEqual(resposta.status_code, 200)
        texto = resposta.get_data(as_text=True)
        for trecho in (AVISO_DEMONSTRACAO, "NO-GO OPERACIONAL", "Visão executiva ESG",
                       "Destinação de resíduos", "Recuperação de materiais", "Compostagem",
                       "Participação das associações", "Inclusão produtiva", "Capacitações",
                       "Rastreabilidade documental", "Indicadores sem dados suficientes", "Fontes documentais",
                       "12.3400", "2034-01", "exec-recorte-destinacao", "exec-fonte-destinacao",
                       "Proposto — sem aprovação institucional", "Rascunho — sem aprovação institucional"):
            self.assertIn(trecho, texto)
        self.assertNotIn("77,26", texto)
        self.assertNotIn("78,35", texto)
        self.assertNotIn("38,25", texto)
        for href in re.findall(r'href="([^"]+)"', texto):
            if href.startswith("/esg/indicadores/"):
                self.assertEqual(self.client.get(href).status_code, 200)
        self.assertNotIn("app", sys.modules)

    def test_get_nao_escreve_e_post_nao_habilita_operacoes(self):
        antes = (self.repo.listar_definicoes(), dict(self.repo.autorias))
        with patch.object(self.repo, "salvar_definicao", side_effect=AssertionError("Escrita proibida")) as definicoes, \
             patch.object(self.repo, "salvar_registro", side_effect=AssertionError("Escrita proibida")) as registros:
            resposta = self.client.get(EXECUTIVA)
            texto = resposta.get_data(as_text=True)
            self.assertEqual(resposta.status_code, 200)
            self.assertNotIn("<form", texto)
            self.assertNotIn("csrf_token", texto)
            self.assertNotIn("Cadastrar", texto)
            self.assertNotIn("registros/novo", texto)
            token = re.search(rb'name="csrf_token" value="([^"]+)"', self.client.get("/login").data).group(1).decode()
            with patch.object(self.repo, "listar_definicoes", side_effect=AssertionError("POST não deve consultar")):
                self.assertEqual(self.client.post(EXECUTIVA, data={"csrf_token": token}).status_code, 405)
            definicoes.assert_not_called()
            registros.assert_not_called()
        self.assertEqual((self.repo.listar_definicoes(), dict(self.repo.autorias)), antes)

    def test_autenticacao_e_papel_administrativo_obrigatorios(self):
        self.assertEqual(self.app.test_client().get(EXECUTIVA).status_code, 302)

        class UsuarioComum(UserMixin):
            id = 2
            role = "usuario"

        with self.client.session_transaction() as sessao:
            sessao["_user_id"] = "2"
        with patch.object(self.app.login_manager, "_user_callback", return_value=UsuarioComum()):
            self.assertEqual(self.client.get(EXECUTIVA).status_code, 403)

    def test_aplicacao_normal_nao_registra_executiva(self):
        app = Flask("exec_normal_fixture")
        app.config.update(TESTING=True, SECRET_KEY="exec-normal-fixture-only")
        app.register_blueprint(criar_blueprint_esg(), url_prefix="/esg")
        self.assertNotIn("executiva_demonstrativa", app.view_functions)
        self.assertEqual(app.test_client().get(EXECUTIVA).status_code, 404)

    def test_falha_de_consulta_nao_expoe_detalhes_ou_finge_ausencia(self):
        for falha in (PersistenciaESGError("segredo-interno"), ContratoESGInvalido("segredo-interno")):
            with self.subTest(falha=type(falha).__name__), patch.object(self.repo, "listar_definicoes", side_effect=falha):
                resposta = self.client.get(EXECUTIVA)
                texto = resposta.get_data(as_text=True)
                self.assertEqual(resposta.status_code, 503)
                self.assertIn(AVISO_DEMONSTRACAO, texto)
                self.assertIn("Não foi possível consultar os dados demonstrativos", texto)
                self.assertNotIn("segredo-interno", texto)
                self.assertNotIn("nenhuma definição demonstrativa", texto)
