"""Interface ESG em Flask test_client, com autenticação/CSRF reais e repo falso."""

import builtins
from contextlib import ExitStack
from dataclasses import replace
from datetime import date
from decimal import Decimal
import io
import os
from pathlib import Path
import re
import unittest
from unittest.mock import Mock, patch
from uuid import UUID

from flask import Flask, render_template_string
from flask_login import LoginManager, UserMixin
from werkzeug.datastructures import MultiDict

from modulos.esg import criar_blueprint_esg
from modulos.esg.contratos import Evidencia, FonteDocumental, Periodo, RegistroPeriodo, SituacaoIndicador, SituacaoRegistro
from modulos.esg.formularios import definicao_formulario
from modulos.esg.repositorio import ConfirmacaoIncertaESGError, ConflitoESGError, PersistenciaESGError
from modulos.esg.services import organizar_registros
from modulos.esg.validacoes import ContratoESGInvalido
from seguranca_csrf import configurar_csrf
from tests.test_esg_formularios import dados_definicao, dados_registro


RAIZ = Path(__file__).resolve().parents[1]
NOVO = "/esg/indicadores/novo"
DETALHE = "/esg/indicadores/indicador-teste/detalhe"
REGISTRO = "/esg/indicadores/indicador-teste/registros/novo"
LISTA = "/esg/indicadores"


class UsuarioFixture(UserMixin):
    def __init__(self, identificador, role):
        self.id = identificador
        self.role = role


class TestInterfaceESG(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        barreiras = ExitStack()
        cls.addClassCleanup(barreiras.close)
        barreiras.enter_context(patch.dict(os.environ, {}, clear=True))
        cls.bloqueios = [barreiras.enter_context(patch(alvo, side_effect=AssertionError("I/O externo proibido"))) for alvo in (
            "socket.socket.connect", "socket.socket.connect_ex", "socket.create_connection",
            "socket.getaddrinfo", "socket.socket.bind", "psycopg2.connect", "dotenv.load_dotenv",
            "modulos.esg.repositorio.RepositorioESG.__init__",
        )]

        def proteger(abrir):
            def guardado(arquivo, *args, **kwargs):
                if isinstance(arquivo, (str, bytes, os.PathLike)):
                    caminho = Path(os.fsdecode(arquivo))
                    if caminho.name.lower().startswith(".env") or caminho.suffix.lower() in {".db", ".sqlite", ".sqlite3", ".log", ".sql"}:
                        raise AssertionError("Arquivo operacional proibido")
                return abrir(arquivo, *args, **kwargs)
            return guardado

        barreiras.enter_context(patch("builtins.open", proteger(builtins.open)))
        barreiras.enter_context(patch("io.open", proteger(io.open)))

    def setUp(self):
        self.definicao = definicao_formulario(dados_definicao())
        self.repo = Mock(spec=["listar_definicoes", "obter_definicao", "listar_registros", "salvar_definicao", "salvar_registro"])
        self.repo.listar_definicoes.return_value = ()
        self.repo.obter_definicao.return_value = self.definicao
        self.repo.listar_registros.return_value = organizar_registros(self.definicao)
        self.app = self.criar_app(self.repo)
        self.client = self.app.test_client()
        self.autenticar()

    def tearDown(self):
        for bloqueio in self.bloqueios:
            bloqueio.assert_not_called()

    def criar_app(self, repositorio):
        app = Flask("esg_interface_fixture", root_path=str(RAIZ), template_folder=str(RAIZ / "templates"))
        app.config.update(TESTING=True, SECRET_KEY="chave-esg-interface-exclusiva-teste")
        configurar_csrf(app)
        manager = LoginManager(app)
        manager.login_view = "login"
        usuarios = {"1": UsuarioFixture(1, "admin"), "2": UsuarioFixture(2, "usuario"), "3": UsuarioFixture(True, "admin")}
        manager.user_loader(usuarios.get)

        @app.get("/login")
        def login():
            return render_template_string('<input type="hidden" name="csrf_token" value="{{ csrf_token() }}">')

        @app.get("/")
        def index():
            return "Fixture de navegação"

        @app.post("/logout")
        def logout():
            return "Fixture de saída"

        app.register_blueprint(criar_blueprint_esg(repositorio), url_prefix="/esg")
        return app

    def autenticar(self, usuario="1", cliente=None):
        with (cliente or self.client).session_transaction() as sessao:
            sessao.clear()
            if usuario is not None:
                sessao["_user_id"] = usuario
                sessao["_fresh"] = True

    def token(self, cliente=None):
        resposta = (cliente or self.client).get("/login")
        return re.search(rb'name="csrf_token" value="([^"]+)"', resposta.data).group(1).decode()

    def post(self, caminho, dados, cliente=None):
        cliente = cliente or self.client
        dados = dados.copy()
        dados["csrf_token"] = self.token(cliente)
        return cliente.post(caminho, data=dados)

    def fixture_registro(self, **extras):
        registro = RegistroPeriodo(UUID(int=1), self.definicao.codigo, Periodo(2025),
                                   self.definicao.fonte_metodologica, date(2025, 12, 31), self.definicao.perimetro)
        return replace(registro, **extras)

    def test_rotas_administrativas_exigem_login_e_permissao_em_get_e_post(self):
        for usuario, esperado in ((None, 302), ("2", 403)):
            self.autenticar(usuario)
            for caminho in ("/esg", LISTA, NOVO, DETALHE, REGISTRO):
                with self.subTest(usuario=usuario, caminho=caminho):
                    resposta = self.client.get(caminho)
                    self.assertEqual(resposta.status_code, esperado)
                    if esperado == 302:
                        self.assertIn("/login", resposta.headers["Location"])
            for caminho, dados in ((NOVO, dados_definicao()), (REGISTRO, dados_registro())):
                with self.subTest(usuario=usuario, post=caminho):
                    self.assertEqual(self.post(caminho, dados).status_code, esperado)
        self.assertEqual(self.repo.mock_calls, [])

    def test_sem_persistencia_nao_consulta_nem_simula_cadastro(self):
        client = self.criar_app(None).test_client()
        self.autenticar(cliente=client)
        for caminho in (LISTA, NOVO, DETALHE, REGISTRO):
            with self.subTest(caminho=caminho):
                resposta = client.get(caminho)
                self.assertEqual(resposta.status_code, 503)
                self.assertIn("Persistência ESG não configurada", resposta.get_data(as_text=True))
        for caminho, dados in ((NOVO, dados_definicao()), (REGISTRO, dados_registro())):
            self.assertEqual(self.post(caminho, dados, client).status_code, 503)
        resposta = client.get("/esg")
        self.assertEqual(resposta.status_code, 200)
        texto = resposta.get_data(as_text=True)
        for mensagem in ("Persistência ESG não configurada", "Consulta de indicadores indisponível", "Nenhum dado foi carregado"):
            self.assertIn(mensagem, texto)
        self.assertNotIn("Nenhum indicador cadastrado", texto)
        self.assertEqual(self.repo.mock_calls, [])

    def test_painel_consulta_vazia_e_com_definicoes(self):
        for definicoes, mensagem in (((), "Nenhum indicador cadastrado"),
                                     ((self.definicao,), "Consulte os cadastros administrativos")):
            self.repo.reset_mock()
            self.repo.listar_definicoes.return_value = definicoes
            with self.subTest(vazio=not definicoes):
                resposta = self.client.get("/esg")
                texto = resposta.get_data(as_text=True)
                self.assertEqual(resposta.status_code, 200)
                self.assertIn("Consulta disponível", texto)
                self.assertIn(mensagem, texto)
                self.assertNotIn("Nenhum dado foi carregado", texto)
                if definicoes:
                    self.assertNotIn("Nenhum indicador cadastrado", texto)
                self.repo.listar_definicoes.assert_called_once_with()
                self.assertEqual(len(self.repo.mock_calls), 1)

    def test_cartoes_de_dimensoes_nao_contradizem_definicoes_cadastradas(self):
        self.repo.listar_definicoes.return_value = (self.definicao,)
        resposta = self.client.get("/esg")
        texto = resposta.get_data(as_text=True)
        self.assertEqual(resposta.status_code, 200)
        self.assertIn("Consulta disponível", texto)
        self.assertIn("Consulte os cadastros administrativos", texto)
        cartoes = re.findall(r'<article class="dimension-card"[^>]*>(.*?)</article>', texto, re.S)
        self.assertEqual(len(cartoes), 3)
        for cartao, rotulo in zip(cartoes, ("Dimensão ambiental", "Dimensão social", "Dimensão de governança")):
            with self.subTest(rotulo=rotulo):
                texto_cartao = re.sub(r"<[^>]*>", "", cartao)
                self.assertIn(rotulo, texto_cartao)
                self.assertNotRegex(texto_cartao.lower(), r"aguardando|nenhum|nenhuma|cadastrad|aprovad|resultado|[0-9]")
        self.assertNotIn("Aguardando definição dos indicadores", texto)
        self.assertNotIn("Nenhum indicador cadastrado", texto)
        self.repo.listar_definicoes.assert_called_once_with()

    def test_painel_falha_de_consulta_nao_presume_cadastros_nem_expoe_detalhes(self):
        for falha in (PersistenciaESGError("segredo-interno-teste"),
                      ContratoESGInvalido("segredo-interno-teste")):
            self.repo.reset_mock()
            self.repo.listar_definicoes.side_effect = falha
            with self.subTest(falha=type(falha).__name__):
                resposta = self.client.get("/esg")
                texto = resposta.get_data(as_text=True)
                self.assertEqual(resposta.status_code, 503)
                self.assertIn("Não foi possível consultar os indicadores", texto)
                self.assertIn("Não foi possível determinar se existem indicadores cadastrados", texto)
                for trecho in ("Nenhum indicador cadastrado", "segredo-interno-teste", "Consulta disponível"):
                    self.assertNotIn(trecho, texto)
                self.repo.listar_definicoes.assert_called_once_with()
                self.assertEqual(len(self.repo.mock_calls), 1)

    def test_novo_caminho_de_detalhe_e_links_preservam_endpoint(self):
        mapa = self.app.url_map.bind("localhost")
        self.assertEqual(mapa.match(DETALHE, method="GET"),
                         ("esg.indicadores_detalhe", {"codigo": "indicador-teste"}))
        self.assertEqual(mapa.match(NOVO, method="GET"), ("esg.indicadores_novo", {}))
        self.assertEqual(self.client.get("/esg/indicadores/indicador-teste").status_code, 404)
        self.repo.listar_definicoes.return_value = (self.definicao,)
        self.assertIn(f'href="{DETALHE}"', self.client.get(LISTA).get_data(as_text=True))
        texto = self.client.get(DETALHE).get_data(as_text=True)
        self.assertIn(f'action="{DETALHE}"', texto)
        self.assertIn(f'href="{REGISTRO}"', texto)
        self.assertIn(f'href="{DETALHE}"', self.client.get(REGISTRO).get_data(as_text=True))

    def test_codigo_novo_pode_ser_cadastrado_e_consultado_sem_colisao(self):
        dados = dados_definicao()
        dados["codigo"] = "novo"
        resposta = self.post(NOVO, dados)
        detalhe = "/esg/indicadores/novo/detalhe"
        self.assertEqual(resposta.status_code, 303)
        self.assertEqual(resposta.headers["Location"], detalhe)
        definicao, autor = self.repo.salvar_definicao.call_args.args
        self.assertEqual(definicao.codigo, "novo")
        self.assertEqual(autor, 1)
        self.assertIs(definicao.situacao, SituacaoIndicador.PROPOSTO)
        self.assertIsNone(definicao.fonte_aprovacao)
        self.repo.obter_definicao.return_value = definicao
        self.repo.listar_registros.return_value = organizar_registros(definicao)
        self.repo.listar_definicoes.return_value = (definicao,)
        resposta = self.client.get(detalhe)
        self.assertEqual(resposta.status_code, 200)
        self.assertIn("Definição do indicador", resposta.get_data(as_text=True))
        self.repo.obter_definicao.assert_called_once_with("novo")
        self.repo.listar_registros.assert_called_once_with("novo", definicao.perimetro)
        self.assertIn(f'href="{detalhe}"', self.client.get(LISTA).get_data(as_text=True))
        cadastro = self.client.get(NOVO)
        self.assertEqual(cadastro.status_code, 200)
        self.assertIn(f'action="{NOVO}"', cadastro.get_data(as_text=True))
        resposta = self.post("/esg/indicadores/novo/registros/novo", dados_registro())
        self.assertEqual(resposta.status_code, 303)
        self.assertEqual(resposta.headers["Location"], detalhe + "?competencia=2025")
        self.assertEqual(self.repo.salvar_registro.call_args.args[0].indicador_codigo, "novo")

    def test_formularios_get_com_csrf_e_sem_campos_de_aprovacao(self):
        for caminho in (NOVO, REGISTRO):
            with self.subTest(caminho=caminho):
                resposta = self.client.get(caminho)
                texto = resposta.get_data(as_text=True)
                self.assertEqual(resposta.status_code, 200)
                self.assertRegex(texto, r'name="csrf_token" value="[^"]+"')
                nomes = re.findall(r'name="([^"]+)"', texto)
                for campo in ("situacao", "aprovado_em", "fonte_aprovacao", "usuario_id", "identificador"):
                    self.assertNotIn(campo, nomes)
                self.assertNotIn("https://", texto)
                self.assertNotIn("<script", texto)
                if caminho == REGISTRO:
                    self.assertNotIn("perimetro_codigo", nomes)
                    self.assertIn("Perímetro fixado pela definição", texto)

    def test_protecao_csrf_recusa_token_ausente_invalido_e_de_outra_sessao(self):
        outro_client = self.app.test_client()
        token_alheio = self.token(outro_client)
        for caminho, dados in ((NOVO, dados_definicao()), (REGISTRO, dados_registro())):
            for token in (None, "token-invalido", token_alheio):
                envio = dados.copy()
                if token is not None:
                    envio["csrf_token"] = token
                with self.subTest(caminho=caminho, token=token):
                    self.assertEqual(self.client.post(caminho, data=envio).status_code, 400)
        self.assertEqual(self.repo.mock_calls, [])
        self.assertTrue(self.app.config["WTF_CSRF_ENABLED"])

    def test_cadastro_valido_proposto_com_autoria_da_sessao(self):
        resposta = self.post(NOVO, dados_definicao())
        self.assertEqual(resposta.status_code, 303)
        self.assertEqual(resposta.headers["Location"], DETALHE)
        definicao, usuario_id = self.repo.salvar_definicao.call_args.args
        self.assertEqual(usuario_id, 1)
        self.assertIs(definicao.situacao, SituacaoIndicador.PROPOSTO)
        self.assertIsNone(definicao.fonte_aprovacao)
        self.assertEqual(definicao.fonte_metodologica.identificador, "fonte-teste")

    def test_cadastro_invalido_reexibe_valores_sem_salvar(self):
        for campo in ("nome", "dimensao", "metodologia", "perimetro_codigo", "fonte_referencia"):
            dados = dados_definicao()
            dados[campo] = ""
            with self.subTest(campo=campo):
                resposta = self.post(NOVO, dados)
                self.assertEqual(resposta.status_code, 400)
                self.assertIn('value="indicador-teste"', resposta.get_data(as_text=True))
        self.repo.salvar_definicao.assert_not_called()

    def test_cadastro_recusa_forjar_aprovacao_autoria_perimetro_e_uuid(self):
        for caminho, montar, campos in ((NOVO, dados_definicao, ("situacao", "fonte_aprovacao_id", "usuario_id")),
                                        (REGISTRO, dados_registro, ("situacao", "aprovado_por_usuario_id", "criado_por_usuario_id", "perimetro_codigo", "indicador_codigo", "identificador"))):
            for campo in campos:
                dados = montar()
                dados[campo] = "aprovado"
                with self.subTest(caminho=caminho, campo=campo):
                    self.assertEqual(self.post(caminho, dados).status_code, 400)
        self.repo.salvar_definicao.assert_not_called()
        self.repo.salvar_registro.assert_not_called()

    def test_recusa_campos_repetidos(self):
        dados = MultiDict(dados_definicao())
        dados.add("codigo", "outro-indicador")
        self.assertEqual(self.post(NOVO, dados).status_code, 400)
        self.repo.salvar_definicao.assert_not_called()

    def test_autor_de_sessao_invalido_nao_grava(self):
        self.autenticar("3")
        self.assertEqual(self.post(NOVO, dados_definicao()).status_code, 403)
        self.assertEqual(self.post(REGISTRO, dados_registro()).status_code, 403)
        self.repo.salvar_definicao.assert_not_called()
        self.repo.salvar_registro.assert_not_called()

    def test_registro_valido_preserva_ausencia_zero_e_precisao(self):
        for valor, esperado in (("", None), ("0", Decimal("0")), ("1,234567890123456789", Decimal("1.234567890123456789"))):
            dados = dados_registro()
            dados["valor"] = valor
            with self.subTest(valor=valor):
                resposta = self.post(REGISTRO, dados)
                self.assertEqual(resposta.status_code, 303)
                self.assertEqual(resposta.headers["Location"], DETALHE + "?competencia=2025")
                registro, usuario_id = self.repo.salvar_registro.call_args.args
                self.assertEqual(registro.valor, esperado)
                if esperado is not None:
                    self.assertIs(type(registro.valor), Decimal)
                self.assertEqual(usuario_id, 1)
                self.assertEqual(registro.indicador_codigo, self.definicao.codigo)
                self.assertIs(registro.perimetro, self.definicao.perimetro)
                self.assertIs(registro.situacao, SituacaoRegistro.RASCUNHO)
                self.assertIsNone(registro.fonte_aprovacao)

    def test_registro_e_evidencia_no_mesmo_cadastro(self):
        dados = dados_registro()
        dados.update(evidencia_tipo="Documento de teste", evidencia_observacoes="Observação fictícia.")
        self.assertEqual(self.post(REGISTRO, dados).status_code, 303)
        registro = self.repo.salvar_registro.call_args.args[0]
        evidencia, = self.repo.salvar_registro.call_args.kwargs["evidencias"]
        self.assertEqual(evidencia.registro_id, registro.identificador)
        self.assertIs(evidencia.fonte, registro.fonte)

    def test_registro_invalido_nao_salva(self):
        for campo, valor in (("competencia", "2025-13"), ("data_referencia", "2026-01-01"),
                             ("valor", "NaN"), ("valor", "1.234,56"), ("fonte_id", ""),
                             ("evidencia_observacoes", "Observação sem tipo")):
            dados = dados_registro()
            dados[campo] = valor
            with self.subTest(campo=campo, valor=valor):
                self.assertEqual(self.post(REGISTRO, dados).status_code, 400)
        self.repo.salvar_registro.assert_not_called()

    def test_listagem_vazia_e_com_estados_unidades_e_perimetros(self):
        self.assertIn("Nenhuma definição cadastrada", self.client.get(LISTA).get_data(as_text=True))
        self.repo.listar_definicoes.return_value = (self.definicao,)
        resposta = self.client.get(LISTA)
        self.assertEqual(resposta.status_code, 200)
        texto = resposta.get_data(as_text=True)
        for trecho in (self.definicao.nome, "Proposto — sem aprovação institucional", "Toneladas", "recorte-teste"):
            self.assertIn(trecho, texto)

    def test_detalhe_sem_registros_e_fontes_metodologicas(self):
        texto = self.client.get(DETALHE).get_data(as_text=True)
        for trecho in ("Nenhum registro encontrado", self.definicao.metodologia,
                       self.definicao.fonte_metodologica.referencia_documental, "Fonte metodológica", "Sem aprovação"):
            if trecho == "Sem aprovação":
                self.assertIn("sem aprovação institucional", texto)
            else:
                self.assertIn(trecho, texto)
        self.repo.listar_registros.assert_called_once_with(self.definicao.codigo, self.definicao.perimetro)

    def test_detalhe_organizado_preserva_textos_extensos_valores_fontes_e_evidencias(self):
        nome = "Indicador sintético com título extenso " * 8
        referencia = "<Documento fictício> " + "referência documental extensa " * 20
        fonte = replace(self.definicao.fonte_metodologica, referencia_documental=referencia.strip())
        definicao = replace(self.definicao, nome=nome.strip(), fonte_metodologica=fonte,
                            metodologia=("Primeira linha sintética.\n" + "Metodologia não homologada. " * 20).strip())
        registro = self.fixture_registro(valor=Decimal("1.2300"), fonte=fonte)
        evidencia = Evidencia(UUID(int=4), registro.identificador, "Evidência sintética", fonte,
                              "Observações extensas sem aprovação. " * 20 + "Fim.")
        self.repo.obter_definicao.return_value = definicao
        self.repo.listar_registros.return_value = organizar_registros(definicao, (registro,), (evidencia,))
        resposta = self.client.get(DETALHE)
        texto = resposta.get_data(as_text=True)
        self.assertEqual(resposta.status_code, 200)
        for trecho in (definicao.nome, definicao.metodologia, definicao.descricao,
                       definicao.unidade.descricao, definicao.perimetro.descricao,
                       str(registro.identificador), str(evidencia.identificador), evidencia.observacoes,
                       "<strong>1.2300 t</strong>", "Proposto — sem aprovação institucional",
                       "Rascunho — sem aprovação institucional", "&lt;Documento fictício&gt;"):
            self.assertIn(trecho, texto)
        self.assertIn('href="#registros-titulo"', texto)
        for identificador in ("definicao-titulo", "metodologia-titulo", "fontes-titulo", "registros-titulo"):
            self.assertIn(f'id="{identificador}"', texto)
        self.assertLess(texto.index('id="definicao-titulo"'), texto.index('id="fontes-titulo"'))
        self.assertLess(texto.index('id="fontes-titulo"'), texto.index('id="registros-titulo"'))
        self.assertNotIn("<Documento fictício>", texto)
        self.repo.salvar_definicao.assert_not_called()
        self.repo.salvar_registro.assert_not_called()

    def test_detalhe_distingue_valor_ausente_zero_e_evidencia_vinculada(self):
        ausente = self.fixture_registro()
        zero = self.fixture_registro(identificador=UUID(int=3), periodo=Periodo(2025, 12), valor=Decimal("0"))
        evidencia = Evidencia(UUID(int=4), zero.identificador, "Evidência exclusiva de zero", zero.fonte, "Página de teste.")
        self.repo.listar_registros.return_value = organizar_registros(self.definicao, (ausente, zero), (evidencia,))
        texto = self.client.get(DETALHE).get_data(as_text=True)
        for trecho in ("Valor ausente", "<strong>0 t</strong>", "Rascunho — sem aprovação institucional", "Página de teste."):
            self.assertIn(trecho, texto)
        texto_filtrado = self.client.get(DETALHE + "?competencia=2025").get_data(as_text=True)
        self.assertIn("Valor ausente", texto_filtrado)
        self.assertNotIn("Evidência exclusiva de zero", texto_filtrado)

    def test_consulta_filtra_periodos_e_situacoes_sem_alterar_aprovacao(self):
        aprovacao = FonteDocumental("aprovacao-teste", "Aprovação fictícia exclusiva de teste.")
        definicao = replace(self.definicao, situacao=SituacaoIndicador.APROVADO, fonte_aprovacao=aprovacao)
        rascunho = self.fixture_registro()
        aprovado = self.fixture_registro(identificador=UUID(int=3), periodo=Periodo(2025, 12), valor=Decimal("7.50"), situacao=SituacaoRegistro.APROVADO, fonte_aprovacao=aprovacao)
        self.repo.obter_definicao.return_value = definicao
        self.repo.listar_registros.return_value = organizar_registros(definicao, (rascunho, aprovado))
        texto = self.client.get(DETALHE + "?competencia=2025-12&situacao=aprovado").get_data(as_text=True)
        self.assertIn("<strong>7.50 t</strong>", texto)
        self.assertIn("Fonte da aprovação do registro", texto)
        self.assertIn(aprovacao.referencia_documental, texto)
        self.assertNotIn("Valor ausente", texto)
        self.assertIn("Nenhum registro encontrado", self.client.get(DETALHE + "?competencia=2025&situacao=aprovado").get_data(as_text=True))
        self.repo.salvar_definicao.assert_not_called()
        self.repo.salvar_registro.assert_not_called()

    def test_filtros_invalidos_retornam_400_sem_carregar_registros(self):
        for filtro in ("competencia=2025-13", "situacao=outro", "perimetro=outro", "competencia=2025&competencia=2024"):
            with self.subTest(filtro=filtro):
                self.assertEqual(self.client.get(DETALHE + "?" + filtro).status_code, 400)
        self.repo.listar_registros.assert_not_called()

    def test_indicador_inexistente_e_codigo_invalido(self):
        self.repo.obter_definicao.return_value = None
        for caminho in (DETALHE, REGISTRO, "/esg/indicadores/codigo%20invalido/detalhe"):
            self.assertEqual(self.client.get(caminho).status_code, 404)
        self.assertEqual(self.post(REGISTRO, dados_registro()).status_code, 404)
        self.repo.salvar_registro.assert_not_called()

    def test_falhas_de_persistencia_nao_expoem_detalhes_ou_fingem_vazio(self):
        for metodo, caminho, dados in (("listar_definicoes", LISTA, None),
                                       ("obter_definicao", DETALHE, None),
                                       ("listar_registros", DETALHE, None),
                                       ("salvar_definicao", NOVO, dados_definicao()),
                                       ("salvar_registro", REGISTRO, dados_registro())):
            getattr(self.repo, metodo).side_effect = PersistenciaESGError("segredo-interno-teste")
            with self.subTest(metodo=metodo):
                resposta = self.client.get(caminho) if dados is None else self.post(caminho, dados)
                self.assertEqual(resposta.status_code, 503)
                self.assertNotIn(b"segredo-interno-teste", resposta.data)
                self.assertNotIn("Nenhuma definição cadastrada", resposta.get_data(as_text=True))
            getattr(self.repo, metodo).side_effect = None

    def test_conflito_e_commit_incerto_nao_repetem_cadastro(self):
        for falha, esperado, mensagem in ((ConflitoESGError("interno"), 409, "Nenhum dado existente foi sobrescrito"),
                                          (ConfirmacaoIncertaESGError("interno"), 503, "Consulte os dados antes de reenviar")):
            self.repo.salvar_definicao.reset_mock()
            self.repo.salvar_definicao.side_effect = falha
            resposta = self.post(NOVO, dados_definicao())
            self.assertEqual(resposta.status_code, esperado)
            self.assertIn(mensagem, resposta.get_data(as_text=True))
            self.repo.salvar_definicao.assert_called_once()

    def test_listagem_com_dados_invalidos_retorna_503_sem_fingir_vazio(self):
        self.repo.listar_definicoes.side_effect = ContratoESGInvalido("segredo-interno-teste")
        resposta = self.client.get(LISTA)
        texto = resposta.get_data(as_text=True)
        self.assertEqual(resposta.status_code, 503)
        self.assertIn("Os dados consultados são incompatíveis com os contratos ESG", texto)
        self.assertNotIn("segredo-interno-teste", texto)
        self.assertNotIn("Nenhuma definição cadastrada", texto)
        self.repo.listar_definicoes.assert_called_once_with()
        self.repo.salvar_definicao.assert_not_called()
        self.repo.salvar_registro.assert_not_called()

    def test_conflito_e_confirmacao_incerta_do_registro_nao_repetem_escrita(self):
        for falha, esperado, mensagem in ((ConflitoESGError("segredo-interno-teste"), 409, "Nenhum dado existente foi sobrescrito"),
                                          (ConfirmacaoIncertaESGError("segredo-interno-teste"), 503, "Consulte os dados antes de reenviar")):
            self.repo.salvar_registro.reset_mock()
            self.repo.salvar_registro.side_effect = falha
            with self.subTest(falha=type(falha).__name__):
                resposta = self.post(REGISTRO, dados_registro())
                texto = resposta.get_data(as_text=True)
                self.assertEqual(resposta.status_code, esperado)
                self.assertIn(mensagem, texto)
                self.assertNotIn("segredo-interno-teste", texto)
                self.assertNotIn("Registro salvo", texto)
                self.assertNotIn("Location", resposta.headers)
                self.repo.salvar_registro.assert_called_once()
                registro, autor = self.repo.salvar_registro.call_args.args
                self.assertEqual(autor, 1)
                self.assertIsNone(registro.valor)
                self.assertIs(registro.perimetro, self.definicao.perimetro)
                self.assertIs(registro.situacao, SituacaoRegistro.RASCUNHO)
                self.assertIsNone(registro.fonte_aprovacao)
        self.repo.salvar_definicao.assert_not_called()

    def test_dados_documentais_e_formulario_sao_escapados(self):
        definicao = replace(self.definicao, nome='<script>alert("x")</script>')
        self.repo.listar_definicoes.return_value = (definicao,)
        texto = self.client.get(LISTA).get_data(as_text=True)
        self.assertIn("&lt;script&gt;", texto)
        self.assertNotIn("<script>", texto)
        dados = dados_definicao()
        dados.update(nome='\"><script>alert(1)</script>', codigo="")
        resposta = self.post(NOVO, dados)
        self.assertEqual(resposta.status_code, 400)
        self.assertNotIn(b"<script>", resposta.data)

    def test_definicao_divergente_entre_consultas_falha_com_seguranca(self):
        outra = replace(self.definicao, perimetro=replace(self.definicao.perimetro, descricao="Outra abrangência"))
        self.repo.listar_registros.return_value = organizar_registros(outra)
        self.assertEqual(self.client.get(DETALHE).status_code, 503)


if __name__ == "__main__":
    unittest.main()
