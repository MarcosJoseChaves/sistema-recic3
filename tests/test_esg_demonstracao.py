"""ESG-03A: contratos em memória e fluxo HTTP local, sem app.py ou servidor.

Executar somente sob as barreiras externas anteriores aos imports auditadas R3.
As barreiras adicionais desta classe começam em setUp e não as substituem.
"""

from dataclasses import replace
from datetime import date
from decimal import Decimal
import re
import sys
import unittest
from unittest.mock import patch
from uuid import UUID, uuid4

from flask import Flask
from flask_login import LoginManager, UserMixin

from modulos.esg import criar_blueprint_esg
from modulos.esg.contratos import (
    Evidencia, FonteDocumental, Periodo, RegistroPeriodo,
    SituacaoIndicador, SituacaoRegistro,
)
from modulos.esg.demonstracao import (
    AVISO_DEMONSTRACAO, RepositorioDemonstrativoESG, criar_repositorio_com_exemplos,
)
from modulos.esg.demonstracao_web import criar_aplicacao_demonstrativa, autenticar_administrador_demonstrativo
from modulos.esg.formularios import definicao_formulario
from modulos.esg.repositorio import ConflitoESGError, IntegridadeESGError, NaoEncontradoESGError
from modulos.esg.validacoes import ContratoESGInvalido
from seguranca_csrf import configurar_csrf


def proposta_sintetica():
    return {
        "codigo": "demo-nova", "nome": AVISO_DEMONSTRACAO + ": nova proposta",
        "dimensao": "social", "descricao": "Descrição exclusivamente sintética.",
        "unidade_simbolo": "ud", "unidade_descricao": "Unidades demonstrativas fictícias",
        "metodologia": "Metodologia fictícia, não homologada.",
        "perimetro_codigo": "demo-recorte", "perimetro_descricao": "Abrangência inteiramente fictícia",
        "fonte_id": "demo-documento-novo", "fonte_referencia": AVISO_DEMONSTRACAO + ": referência sem emissor institucional.",
        "fonte_data": "",
    }


def registro_sintetico():
    return {
        "competencia": "2031-03", "valor": "7,2500", "data_referencia": "2031-03-31",
        "fonte_id": "demo-registro-novo", "fonte_referencia": AVISO_DEMONSTRACAO + ": fonte do registro sintético.",
        "fonte_data": "", "evidencia_tipo": "Evidência fictícia",
        "evidencia_observacoes": AVISO_DEMONSTRACAO + ": observação documental sintética.",
    }


class TestDemonstracaoESG(unittest.TestCase):
    def setUp(self):
        self.bloqueios = [self.enterContext(patch(alvo, side_effect=AssertionError("I/O real proibido na demonstração"))) for alvo in (
            "psycopg2.connect", "socket.socket.connect", "socket.socket.connect_ex",
            "socket.socket.bind", "socket.create_connection", "socket.getaddrinfo",
            "dotenv.load_dotenv", "modulos.esg.repositorio.RepositorioESG.__init__",
        )]
        self.repo = RepositorioDemonstrativoESG()
        self.definicao = definicao_formulario(proposta_sintetica())

    def tearDown(self):
        for bloqueio in self.bloqueios:
            bloqueio.assert_not_called()

    def registro(self, **extras):
        registro = RegistroPeriodo(uuid4(), self.definicao.codigo, Periodo(2031),
                                   self.definicao.fonte_metodologica, date(2031, 12, 31), self.definicao.perimetro)
        return replace(registro, **extras)

    def test_instancias_vazias_independentes_e_sem_importar_app(self):
        self.assertEqual(self.repo.listar_definicoes(), ())
        self.assertIsNone(self.repo.obter_definicao("ausente"))
        self.repo.salvar_definicao(self.definicao, 1)
        self.assertEqual(RepositorioDemonstrativoESG().listar_definicoes(), ())
        self.assertNotIn("app", sys.modules)

    def test_contratos_preservados_e_autoria_somente_leitura(self):
        self.assertIs(self.repo.salvar_definicao(self.definicao, 7), self.definicao)
        registro = self.registro(valor=Decimal("1.2300"))
        self.assertIs(self.repo.salvar_registro(registro, 8), registro)
        serie = self.repo.listar_registros(self.definicao.codigo, self.definicao.perimetro)
        self.assertIs(serie.registros[0].valor, registro.valor)
        self.assertEqual(serie.registros[0].valor.as_tuple(), Decimal("1.2300").as_tuple())
        self.assertIs(type(registro.identificador), UUID)
        self.assertEqual(self.repo.autorias[("definicao", self.definicao.codigo)], 7)
        self.assertEqual(self.repo.autorias[("registro", registro.identificador)], 8)
        with self.assertRaises(TypeError):
            self.repo.autorias[("registro", registro.identificador)] = 99

    def test_autores_invalidos_e_entradas_nao_tipadas_nao_alteram_estado(self):
        for autor in (True, None, 0, -1, "1", 1.0):
            with self.subTest(autor=autor), self.assertRaises(ContratoESGInvalido):
                self.repo.salvar_definicao(self.definicao, autor)
        with self.assertRaises(ContratoESGInvalido):
            self.repo.salvar_definicao({}, 1)
        self.repo.salvar_definicao(self.definicao, 1)
        for autor in (True, None, 0, -1, "1", 1.0):
            with self.subTest(autor=autor), self.assertRaises(ContratoESGInvalido):
                self.repo.salvar_registro(self.registro(), autor)
        with self.assertRaises(ContratoESGInvalido):
            self.repo.salvar_registro({}, 1)
        self.assertEqual(self.repo.listar_registros(self.definicao.codigo, self.definicao.perimetro).registros, ())

    def test_estados_nao_iniciais_e_aprovacoes_sao_recusados(self):
        fonte = self.definicao.fonte_metodologica
        for estado in (SituacaoIndicador.EM_ANALISE, SituacaoIndicador.REJEITADO, SituacaoIndicador.APROVADO):
            alterada = replace(self.definicao, situacao=estado, fonte_aprovacao=fonte if estado is SituacaoIndicador.APROVADO else None)
            with self.subTest(estado=estado), self.assertRaises(ContratoESGInvalido):
                self.repo.salvar_definicao(alterada, 1)
        with self.assertRaises(ContratoESGInvalido):
            self.repo.salvar_definicao(replace(self.definicao, fonte_aprovacao=fonte), 1)
        self.repo.salvar_definicao(self.definicao, 1)
        for estado in (SituacaoRegistro.EM_ANALISE, SituacaoRegistro.REJEITADO, SituacaoRegistro.APROVADO):
            registro = self.registro(valor=Decimal("0"), situacao=estado, fonte_aprovacao=fonte if estado is SituacaoRegistro.APROVADO else None)
            with self.subTest(estado=estado), self.assertRaises(ContratoESGInvalido):
                self.repo.salvar_registro(registro, 1)
        with self.assertRaises(ContratoESGInvalido):
            self.repo.salvar_registro(self.registro(fonte_aprovacao=fonte), 1)

    def test_codigo_duplicado_e_identidades_divergentes_nao_sobrescrevem(self):
        self.repo.salvar_definicao(self.definicao, 1)
        for alterada in (
            self.definicao,
            replace(self.definicao, codigo="demo-outra", fonte_metodologica=replace(self.definicao.fonte_metodologica, referencia_documental="Referência fictícia diferente")),
            replace(self.definicao, codigo="demo-outra", perimetro=replace(self.definicao.perimetro, descricao="Outro recorte fictício")),
            replace(self.definicao, codigo="demo-outra", unidade=replace(self.definicao.unidade, descricao="Outra unidade fictícia")),
        ):
            with self.subTest(alterada=alterada), self.assertRaises(ConflitoESGError):
                self.repo.salvar_definicao(alterada, 2)
        self.assertEqual(self.repo.listar_definicoes(), (self.definicao,))
        self.assertEqual(len(self.repo.autorias), 1)

    def test_periodo_e_uuid_duplicados_recusados_com_ano_e_mes_distintos(self):
        self.repo.salvar_definicao(self.definicao, 1)
        anual = self.registro(valor=Decimal("0"))
        self.repo.salvar_registro(anual, 1)
        for duplicado in (replace(anual, identificador=uuid4()), replace(anual, periodo=Periodo(2031, 12))):
            with self.subTest(registro=duplicado), self.assertRaises(ConflitoESGError):
                self.repo.salvar_registro(duplicado, 2)
        mensal = replace(anual, identificador=uuid4(), periodo=Periodo(2031, 12), valor=None)
        self.repo.salvar_registro(mensal, 1)
        serie = self.repo.listar_registros(self.definicao.codigo, self.definicao.perimetro)
        self.assertEqual(serie.registros, (anual, mensal))
        self.assertEqual(anual.valor, Decimal("0"))
        self.assertIsNone(mensal.valor)

    def test_perimetros_estritos_e_definicao_ausente(self):
        registro = self.registro()
        with self.assertRaises(NaoEncontradoESGError):
            self.repo.salvar_registro(registro, 1)
        self.assertIsNone(self.repo.listar_registros("ausente", self.definicao.perimetro))
        self.repo.salvar_definicao(self.definicao, 1)
        for perimetro in (replace(self.definicao.perimetro, codigo="outro"), replace(self.definicao.perimetro, descricao="Descrição fictícia divergente")):
            with self.subTest(perimetro=perimetro):
                with self.assertRaises(IntegridadeESGError):
                    self.repo.listar_registros(self.definicao.codigo, perimetro)
                with self.assertRaises(ContratoESGInvalido):
                    self.repo.salvar_registro(replace(registro, perimetro=perimetro), 1)

    def test_evidencia_invalida_ou_fonte_divergente_nao_grava_parcialmente(self):
        self.repo.salvar_definicao(self.definicao, 1)
        registro = self.registro()
        evidencia = Evidencia(uuid4(), registro.identificador, "Exemplo fictício", registro.fonte)
        for evidencias in ((replace(evidencia, registro_id=uuid4()),), (evidencia, evidencia), iter(())):
            with self.subTest(evidencias=type(evidencias).__name__), self.assertRaises(ContratoESGInvalido):
                self.repo.salvar_registro(registro, 1, evidencias)
        fonte_existente = FonteDocumental("demo-outra-fonte", "Referência sintética de outra proposta")
        self.repo.salvar_definicao(replace(self.definicao, codigo="demo-segunda", fonte_metodologica=fonte_existente), 1)
        fonte_nova = FonteDocumental("demo-transitoria", "Fonte sintética que não deve sobreviver à falha")
        evidencia_conflitante = replace(evidencia, fonte=replace(fonte_existente, referencia_documental="Referência sintética incompatível"))
        with self.assertRaises(ConflitoESGError):
            self.repo.salvar_registro(replace(registro, fonte=fonte_nova), 2, (evidencia_conflitante,))
        self.assertEqual(self.repo.listar_registros(self.definicao.codigo, self.definicao.perimetro).registros, ())
        self.assertEqual(len(self.repo.autorias), 2)
        self.repo.salvar_registro(replace(registro, fonte=replace(fonte_nova, referencia_documental="Outra referência sintética permitida após falha")), 1)

    def test_uuid_de_evidencia_globalmente_unico(self):
        self.repo.salvar_definicao(self.definicao, 1)
        primeiro = self.registro()
        evidencia = Evidencia(uuid4(), primeiro.identificador, "Exemplo fictício", primeiro.fonte)
        self.repo.salvar_registro(primeiro, 1, (evidencia,))
        segundo = self.registro(periodo=Periodo(2031, 12))
        with self.assertRaises(ConflitoESGError):
            self.repo.salvar_registro(segundo, 2, (replace(evidencia, registro_id=segundo.identificador),))
        serie = self.repo.listar_registros(self.definicao.codigo, self.definicao.perimetro)
        self.assertEqual(serie.registros, (primeiro,))
        self.assertEqual(serie.evidencias, (evidencia,))

    def test_carga_explicita_sintetica_preserva_zero_ausencia_decimal_e_periodos(self):
        repo = criar_repositorio_com_exemplos()
        definicao, = repo.listar_definicoes()
        serie = repo.listar_registros(definicao.codigo, definicao.perimetro)
        self.assertIn(AVISO_DEMONSTRACAO, definicao.nome)
        self.assertEqual(tuple(r.periodo for r in serie.registros), (Periodo(2030), Periodo(2030, 1), Periodo(2030, 2)))
        self.assertEqual(tuple(r.valor for r in serie.registros), (Decimal("0"), None, Decimal("12.3400")))
        self.assertEqual(serie.registros[2].valor.as_tuple(), Decimal("12.3400").as_tuple())
        self.assertEqual(len(serie.evidencias), 3)
        evidencias = {e.registro_id: e for e in serie.evidencias}
        for registro in serie.registros:
            evidencia = evidencias[registro.identificador]
            self.assertIs(registro.situacao, SituacaoRegistro.RASCUNHO)
            self.assertIsNone(registro.fonte_aprovacao)
            self.assertEqual(evidencia.registro_id, registro.identificador)
            self.assertIn(AVISO_DEMONSTRACAO, evidencia.fonte.referencia_documental)
        self.assertEqual(self.repo.listar_definicoes(), ())

    def client_demo(self, repositorio=None):
        app = criar_aplicacao_demonstrativa(self.repo if repositorio is None else repositorio)
        client = app.test_client()
        autenticar_administrador_demonstrativo(client)
        return app, client

    def post_com_csrf(self, client, caminho, dados):
        login = client.get("/login")
        token = re.search(rb'name="csrf_token" value="([^"]+)"', login.data).group(1).decode()
        return client.post(caminho, data={**dados, "csrf_token": token})

    def pagina(self, resposta, trecho):
        self.assertEqual(resposta.status_code, 200)
        texto = resposta.get_data(as_text=True)
        self.assertIn(AVISO_DEMONSTRACAO, texto)
        self.assertIn(trecho, texto)

    def test_fluxo_end_to_end_com_autoria_csrf_fonte_e_evidencia(self):
        app, client = self.client_demo()
        self.assertTrue(app.config["WTF_CSRF_ENABLED"])
        self.pagina(client.get("/esg"), "Nenhum indicador cadastrado")
        self.pagina(client.get("/esg/indicadores"), "Nenhuma definição cadastrada")
        self.pagina(client.get("/esg/indicadores/novo"), "Cadastrar indicador proposto")
        proposta = self.post_com_csrf(client, "/esg/indicadores/novo", proposta_sintetica())
        self.assertEqual(proposta.status_code, 303)
        detalhe = "/esg/indicadores/demo-nova/detalhe"
        self.assertEqual(proposta.headers["Location"], detalhe)
        self.pagina(client.get(detalhe), "Proposto — sem aprovação institucional")
        self.pagina(client.get("/esg/indicadores/demo-nova/registros/novo"), "Registrar período em rascunho")
        resposta = self.post_com_csrf(client, "/esg/indicadores/demo-nova/registros/novo", registro_sintetico())
        self.assertEqual(resposta.status_code, 303)
        self.pagina(client.get(resposta.headers["Location"]), "7.2500 ud")
        texto = client.get(detalhe).get_data(as_text=True)
        for trecho in ("Rascunho — sem aprovação institucional", "Evidência fictícia", "fonte do registro sintético", "observação documental sintética"):
            self.assertIn(trecho, texto)
        definicao = self.repo.obter_definicao("demo-nova")
        serie = self.repo.listar_registros("demo-nova", definicao.perimetro)
        registro, = serie.registros
        evidencia, = serie.evidencias
        self.assertEqual(registro.valor.as_tuple(), Decimal("7.2500").as_tuple())
        self.assertEqual(evidencia.registro_id, registro.identificador)
        for chave in (("definicao", definicao.codigo), ("registro", registro.identificador), ("evidencia", evidencia.identificador)):
            self.assertEqual(self.repo.autorias[chave], 1)
        self.assertNotIn("app", sys.modules)

    def test_exemplos_renderizados_diferenciam_zero_ausencia_e_decimal(self):
        _, client = self.client_demo(criar_repositorio_com_exemplos())
        self.pagina(client.get("/esg"), "Consulte os cadastros administrativos")
        self.pagina(client.get("/esg/indicadores"), "demo-material")
        resposta = client.get("/esg/indicadores/demo-material/detalhe")
        self.pagina(resposta, "Valor ausente")
        texto = resposta.get_data(as_text=True)
        for trecho in ("<strong>0 ud</strong>", "12.3400 ud", "2030-01", "2030-02", "Referência sintética"):
            self.assertIn(trecho, texto)

    def test_sem_sessao_sem_csrf_e_campos_forjados_nao_gravam(self):
        app = criar_aplicacao_demonstrativa(self.repo)
        client = app.test_client()
        self.assertEqual(client.get("/esg").status_code, 302)
        autenticar_administrador_demonstrativo(client)
        resposta = client.post("/esg/indicadores/novo", data=proposta_sintetica())
        self.assertEqual(resposta.status_code, 400)
        self.assertIn(AVISO_DEMONSTRACAO, resposta.get_data(as_text=True))
        self.assertEqual(self.post_com_csrf(client, "/esg/indicadores/novo", {**proposta_sintetica(), "usuario_id": "99"}).status_code, 400)
        self.assertEqual(self.repo.listar_definicoes(), ())
        with client.session_transaction() as sessao:
            sessao["_user_id"] = "99"
        self.assertEqual(client.get("/esg").status_code, 302)

    def test_paginas_de_login_erro_e_logout_mantem_aviso_e_sessao_controlada(self):
        _, client = self.client_demo()
        self.pagina(client.get("/login"), "Sessão demonstrativa offline")
        resposta = client.get("/esg/indicadores/inexistente/detalhe")
        self.assertEqual(resposta.status_code, 404)
        self.assertIn(AVISO_DEMONSTRACAO, resposta.get_data(as_text=True))
        resposta = self.post_com_csrf(client, "/logout", {})
        self.assertEqual(resposta.status_code, 303)
        self.assertEqual(resposta.headers["Location"], "/login")
        self.assertEqual(client.get("/esg").status_code, 302)

    def test_modo_normal_sem_aviso_sem_carga_e_fixture_restrita(self):
        app = Flask("normal_esg_fixture")
        app.config.update(TESTING=True, SECRET_KEY="normal-fixture-only")
        configurar_csrf(app)
        manager = LoginManager(app)

        class Usuario(UserMixin):
            id = 1
            role = "admin"

        manager.user_loader(lambda identificador: Usuario() if identificador == "1" else None)
        app.add_url_rule("/", "index", lambda: "Fixture")
        app.add_url_rule("/logout", "logout", lambda: "Fixture", methods=["POST"])
        app.register_blueprint(criar_blueprint_esg(), url_prefix="/esg")
        client = app.test_client()
        with self.assertRaises(ValueError):
            autenticar_administrador_demonstrativo(client)
        with client.session_transaction() as sessao:
            sessao["_user_id"] = "1"
            sessao["_fresh"] = True
        resposta = client.get("/esg")
        self.assertEqual(resposta.status_code, 200)
        self.assertNotIn(AVISO_DEMONSTRACAO, resposta.get_data(as_text=True))
        self.assertIn("Persistência ESG não configurada", resposta.get_data(as_text=True))
        with self.assertRaises(ContratoESGInvalido):
            criar_aplicacao_demonstrativa(None)
