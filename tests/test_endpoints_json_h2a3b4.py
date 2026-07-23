"""Cobertura da revisão geral dos endpoints JSON/AJAX da H2A.3B.4."""

import ast
import json
import unittest
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch

from test_csrf_h2a2 import APP_MODULE, cabecalho_basic, obter_token


PUBLICOS_ESSENCIAIS = {"/health"}
AUTENTICADOS = {
    "/buscar_cep/01001000",
    "/buscar_cnpj/11222333000181",
    "/get_produtos_servicos",
    "/get_distinct_grupos",
    "/get_distinct_subgrupos",
    "/get_items_for_filters",
    "/get_relatorio_catalog_options",
    "/get_relatorio_tipos_atividade_transacao",
}
ADMINISTRATIVOS = {
    "/api/produtos_crud",
    "/api/subgrupos",
    "/excluir_conta_corrente/1",
    "/excluir_movimentacao/1",
    "/get_detalhes_solicitacao/1",
    "/get_solicitacoes_pendentes",
    "/responder_solicitacao",
    "/fiscalizacao-contratos/empresas/consultar-cep/01001000",
    "/fiscalizacao-contratos/empresas/consultar-cnpj/11222333000181",
}
ESCOPO_OBJETO = {
    "/buscar_associados",
    "/buscar_cadastros",
    "/buscar_contas_correntes_gestao",
    "/buscar_patrimonio",
    "/buscar_transacoes_gestao",
    "/excluir_associado/1",
    "/excluir_cadastro/1",
    "/excluir_patrimonio/1",
    "/excluir_transacao/1",
    "/gerar_extrato_bancario",
    "/gerar_relatorio",
    "/get_associado/1",
    "/get_associados_ativos",
    "/get_cadastro/1",
    "/get_cadastros_ativos",
    "/get_clientes_fornecedores_com_pendencias",
    "/get_conta_corrente_detalhe/1",
    "/get_contas_correntes",
    "/get_movimentacao_detalhes/1",
    "/get_notas_em_aberto",
    "/get_patrimonio_detalhes/1",
    "/get_relatorio_entidades_para_filtro",
    "/get_relatorio_uvrs",
    "/get_resumo_fluxo_caixa",
    "/get_transacao_detalhes/1",
    "/registrar_fluxo_caixa",
}
TODOS = PUBLICOS_ESSENCIAIS | AUTENTICADOS | ADMINISTRATIVOS | ESCOPO_OBJETO

ESCRITA = {
    "/api/produtos_crud",
    "/api/subgrupos",
    "/excluir_associado/1",
    "/excluir_cadastro/1",
    "/excluir_conta_corrente/1",
    "/excluir_movimentacao/1",
    "/excluir_patrimonio/1",
    "/excluir_transacao/1",
    "/registrar_fluxo_caixa",
    "/responder_solicitacao",
}

COBERTOS_B1 = {
    "/gerar_extrato_bancario",
    "/gerar_relatorio",
    "/registrar_fluxo_caixa",
}
COBERTOS_B2 = {
    "/get_associados_ativos",
    "/get_cadastros_ativos",
    "/get_clientes_fornecedores_com_pendencias",
    "/get_contas_correntes",
    "/get_notas_em_aberto",
    "/get_produtos_servicos",
    "/get_relatorio_catalog_options",
    "/get_relatorio_entidades_para_filtro",
    "/get_relatorio_tipos_atividade_transacao",
    "/get_relatorio_uvrs",
    "/get_resumo_fluxo_caixa",
}
COBERTOS_B3 = {
    "/excluir_associado/1",
    "/excluir_cadastro/1",
    "/excluir_patrimonio/1",
    "/excluir_transacao/1",
    "/get_conta_corrente_detalhe/1",
    "/get_movimentacao_detalhes/1",
    "/get_patrimonio_detalhes/1",
    "/get_transacao_detalhes/1",
}

METODOS_ESPECIAIS = {
    "/api/produtos_crud": "DELETE",
    "/gerar_extrato_bancario": "POST",
    "/gerar_relatorio": "POST",
}


class TestEndpointsJsonH2A3B4(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = APP_MODULE.app
        cls.carregador_original = APP_MODULE.login_manager._user_callback

    @classmethod
    def tearDownClass(cls):
        APP_MODULE.login_manager._user_callback = cls.carregador_original

    def setUp(self):
        self.client = self.app.test_client()
        self.app.config["APP_ENV"] = "testing"
        APP_MODULE.login_manager._user_callback = self._usuario

    @staticmethod
    def _usuario(user_id):
        usuarios = {
            "1": APP_MODULE.User(1, "administrador", "admin", None),
            "2": APP_MODULE.User(2, "usuario", "usuario", "UVR 01"),
        }
        return usuarios.get(str(user_id))

    def autenticar(self, user_id):
        with self.client.session_transaction() as sessao:
            sessao["_user_id"] = str(user_id)
            sessao["_fresh"] = True

    def requisitar_com_csrf(self, rota, *, metodo="POST", corpo=None, headers=None):
        token, _ = obter_token(self.client, headers=headers)
        cabecalhos = dict(headers or {})
        cabecalhos["X-CSRFToken"] = token
        return self.client.open(
            rota,
            method=metodo,
            json={} if corpo is None else corpo,
            headers=cabecalhos,
        )

    def test_01_inventario_tem_44_endpoints_sem_duplicidade(self):
        self.assertEqual(len(TODOS), 44)
        self.assertEqual(
            (len(PUBLICOS_ESSENCIAIS), len(AUTENTICADOS),
             len(ADMINISTRATIVOS), len(ESCOPO_OBJETO)),
            (1, 8, 9, 26),
        )

    def test_02_classificacao_tem_34_leituras_e_10_escritas(self):
        self.assertEqual(len(ESCRITA), 10)
        self.assertEqual(len(TODOS - ESCRITA), 34)

    def test_03_etapas_anteriores_cobrem_22_endpoints_distintos(self):
        cobertos = COBERTOS_B1 | COBERTOS_B2 | COBERTOS_B3
        self.assertEqual((len(COBERTOS_B1), len(COBERTOS_B2), len(COBERTOS_B3)), (3, 11, 8))
        self.assertEqual(len(cobertos), 22)
        self.assertTrue(cobertos <= TODOS)

    def test_04_visitante_recebe_401_json_nos_43_protegidos_sem_efeitos(self):
        token, _ = obter_token(self.client)
        with (
            patch.object(
                APP_MODULE,
                "conectar_banco",
                side_effect=AssertionError("visitante tentou acessar banco"),
            ) as banco,
            patch.object(
                APP_MODULE.requests,
                "get",
                side_effect=AssertionError("visitante tentou acessar API"),
            ) as api,
        ):
            for rota in sorted(TODOS - PUBLICOS_ESSENCIAIS):
                with self.subTest(rota=rota):
                    metodo = METODOS_ESPECIAIS.get(
                        rota, "POST" if rota in ESCRITA else "GET"
                    )
                    headers = {"X-CSRFToken": token} if metodo != "GET" else {}
                    resposta = self.client.open(rota, method=metodo, headers=headers)
                    self.assertEqual(resposta.status_code, 401)
                    self.assertTrue(resposta.is_json)
        banco.assert_not_called()
        api.assert_not_called()

    def test_05_basic_sem_login_nao_substitui_sessao(self):
        headers = cabecalho_basic()
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("Basic tentou acessar banco"),
        ) as banco:
            for rota in ("/buscar_associados", "/api/subgrupos"):
                with self.subTest(rota=rota):
                    resposta = self.client.get(rota, headers=headers)
                    self.assertEqual(resposta.status_code, 401)
                    self.assertTrue(resposta.is_json)
        banco.assert_not_called()

    def test_06_usuario_inativo_nao_le_nem_escreve_json(self):
        with self.client.session_transaction() as sessao:
            sessao["_user_id"] = "3"
            sessao["_fresh"] = True
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("inativo tentou acessar banco"),
        ) as banco:
            leitura = self.client.get("/buscar_associados")
            escrita = self.requisitar_com_csrf(
                "/registrar_fluxo_caixa", corpo={"uvr": "UVR 01"}
            )
        self.assertEqual(leitura.status_code, 401)
        self.assertEqual(escrita.status_code, 401)
        banco.assert_not_called()

    def test_07_usuario_comum_recebe_403_nas_nove_apis_administrativas(self):
        self.autenticar(2)
        token, _ = obter_token(self.client)
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("sem permissão tentou acessar banco"),
        ) as banco:
            for rota in sorted(ADMINISTRATIVOS):
                with self.subTest(rota=rota):
                    metodo = METODOS_ESPECIAIS.get(
                        rota, "POST" if rota in ESCRITA else "GET"
                    )
                    headers = {"X-CSRFToken": token} if metodo != "GET" else {}
                    resposta = self.client.open(rota, method=metodo, headers=headers)
                    self.assertEqual(resposta.status_code, 403)
                    self.assertTrue(resposta.is_json)
        banco.assert_not_called()

    def test_08_busca_comum_sem_uvr_falha_fechada(self):
        APP_MODULE.login_manager._user_callback = lambda _user_id: APP_MODULE.User(
            2, "usuario", "usuario", None
        )
        self.autenticar(2)
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("usuário sem UVR tentou acessar banco"),
        ) as banco:
            for rota in (
                "/buscar_associados", "/buscar_cadastros",
                "/buscar_contas_correntes_gestao", "/buscar_transacoes_gestao",
                "/buscar_patrimonio",
            ):
                with self.subTest(rota=rota):
                    resposta = self.client.get(rota)
                    self.assertEqual(resposta.status_code, 403)
                    self.assertTrue(resposta.is_json)
        banco.assert_not_called()

    def test_09_objeto_inexistente_e_alheio_usam_o_mesmo_404(self):
        self.autenticar(2)
        respostas = []
        for _caso in ("inexistente", "alheio"):
            conexao = MagicMock()
            conexao.cursor.return_value.fetchone.return_value = None
            with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
                resposta = self.client.get("/get_associado/91")
            respostas.append((resposta.status_code, resposta.get_json()))
            sql, parametros = conexao.cursor.return_value.execute.call_args.args
            self.assertIn("uvr", sql.lower())
            self.assertEqual(parametros, (91, "UVR 01"))
        self.assertEqual(respostas[0], respostas[1])
        self.assertEqual(respostas[0][0], 404)

    def test_10_json_invalido_e_content_type_incorreto_nao_acessam_banco(self):
        self.autenticar(1)
        token, _ = obter_token(self.client)
        casos = (
            ({"data": "texto", "content_type": "text/plain"}, 415),
            ({"data": "{", "content_type": "application/json"}, 400),
            ({"data": "", "content_type": "application/json"}, 400),
            ({"data": "null", "content_type": "application/json"}, 400),
            ({"data": "[]", "content_type": "application/json"}, 400),
        )
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("JSON inválido tentou acessar banco"),
        ) as banco:
            for argumentos, esperado in casos:
                with self.subTest(argumentos=argumentos):
                    resposta = self.client.post(
                        "/responder_solicitacao",
                        headers={"X-CSRFToken": token},
                        **argumentos,
                    )
                    self.assertEqual(resposta.status_code, esperado)
                    self.assertTrue(resposta.is_json)
        banco.assert_not_called()

    def test_11_campos_forjados_texto_longo_e_lista_excessiva_sao_rejeitados(self):
        self.autenticar(1)
        casos = (
            ("/responder_solicitacao", {"id": 1, "acao": "aprovar", "is_admin": True}),
            ("/api/subgrupos", {"acao": "novo", "nome": "x", "atividade_pai": "a", "uvr_id": 9}),
            ("/api/produtos_crud", {"item": "x", "grupo": "a", "usuario_id": 9}),
            ("/api/subgrupos", {"acao": "novo", "nome": "x" * 5001, "atividade_pai": "a"}),
            (
                "/registrar_fluxo_caixa",
                {"uvr": "UVR 01", "ids_nfs_selecionadas": list(range(201))},
            ),
        )
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("campo forjado tentou acessar banco"),
        ) as banco:
            for rota, corpo in casos:
                with self.subTest(rota=rota):
                    resposta = self.requisitar_com_csrf(rota, corpo=corpo)
                    self.assertEqual(resposta.status_code, 400)
        banco.assert_not_called()

    def test_12_csrf_ausente_e_invalido_bloqueiam_antes_do_negocio(self):
        self.autenticar(1)
        obter_token(self.client)
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("CSRF inválido tentou acessar banco"),
        ) as banco:
            ausente = self.client.post("/api/subgrupos", json={"acao": "novo"})
            invalido = self.client.post(
                "/api/subgrupos",
                json={"acao": "novo"},
                headers={"X-CSRFToken": "inválido"},
            )
        self.assertEqual(ausente.status_code, 400)
        self.assertEqual(invalido.status_code, 400)
        banco.assert_not_called()

    def test_13_metodo_incorreto_retorna_405(self):
        self.autenticar(1)
        resposta = self.client.get("/registrar_fluxo_caixa")
        self.assertEqual(resposta.status_code, 405)
        self.assertTrue(resposta.is_json)

        pagina_html = self.client.get("/logout", headers={"Accept": "application/json"})
        self.assertEqual(pagina_html.status_code, 405)
        self.assertFalse(pagina_html.is_json)

    def test_14_respostas_json_protegidas_nao_sao_armazenadas_em_cache(self):
        self.autenticar(2)
        resposta = self.client.get("/get_distinct_grupos")
        self.assertEqual(resposta.status_code, 200)
        self.assertIn("no-store", resposta.headers.get("Cache-Control", ""))
        self.assertEqual(resposta.headers.get("Pragma"), "no-cache")
        health = self.client.get("/health")
        self.assertNotIn("no-store", health.headers.get("Cache-Control", ""))

    def test_15_nao_existe_cors_permissivo_nem_jsonp(self):
        self.autenticar(2)
        resposta = self.client.get("/get_distinct_grupos")
        self.assertIsNone(resposta.headers.get("Access-Control-Allow-Origin"))
        fonte = Path("app.py").read_text(encoding="utf-8")
        self.assertNotIn("Access-Control-Allow-Origin", fonte)
        self.assertNotRegex(fonte, r"\bjsonp\b")

    def test_16_frontend_ajax_e_mesma_origem_e_escapa_dados(self):
        fonte = Path("templates/cadastro.html").read_text(encoding="utf-8")
        self.assertIn("function escaparHtml", fonte)
        self.assertNotIn("xhr.responseText", fonte)
        self.assertNotIn('console.log("Data recebida do servidor:"', fonte)
        self.assertNotIn("localStorage", fonte)
        self.assertNotRegex(fonte, r"fetch\(\s*['\"]https?://")
        self.assertIn("destino.origin === window.location.origin", fonte)
        self.assertNotIn('value="${nf.id}"', fonte)
        self.assertNotRegex(fonte, r"attr\('onclick'.*data\.id")
        self.assertIn("const idNota = idNumericoSeguro(nf.id)", fonte)

    def test_17_endpoint_sucesso_denuncia_fica_oculto_online_antes_de_efeitos(self):
        for ambiente in ("homologation", "production"):
            with self.subTest(ambiente=ambiente):
                self.app.config["APP_ENV"] = ambiente
                with patch.object(
                    APP_MODULE,
                    "pagina_sucesso_base",
                    side_effect=AssertionError("endpoint online executou a página"),
                ) as pagina:
                    resposta = self.client.get("/sucesso_denuncia")
                self.assertEqual(resposta.status_code, 404)
                self.assertEqual(
                    resposta.headers.get("X-Content-Type-Options"), "nosniff"
                )
                self.assertEqual(resposta.headers.get("X-Frame-Options"), "SAMEORIGIN")
                pagina.assert_not_called()

    def test_18_administrador_tambem_nao_reativa_endpoint_online(self):
        self.autenticar(1)
        for ambiente in ("homologation", "production"):
            with self.subTest(ambiente=ambiente):
                self.app.config["APP_ENV"] = ambiente
                self.assertEqual(
                    self.client.get("/sucesso_denuncia").status_code, 404
                )

    def test_19_endpoint_sucesso_denuncia_exige_login_fora_do_online(self):
        for ambiente in ("development", "testing"):
            with self.subTest(ambiente=ambiente):
                self.app.config["APP_ENV"] = ambiente
                visitante = self.client.get("/sucesso_denuncia")
                self.assertEqual(visitante.status_code, 302)

                self.autenticar(2)
                autenticado = self.client.get("/sucesso_denuncia")
                self.assertEqual(autenticado.status_code, 200)
                with self.client.session_transaction() as sessao:
                    sessao.clear()

    def test_20_erros_internos_json_sao_genericos(self):
        self.autenticar(2)
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=RuntimeError("SQL segredo tabela host"),
        ):
            resposta = self.client.get("/buscar_associados")
        self.assertEqual(resposta.status_code, 500)
        corpo = resposta.get_data(as_text=True)
        self.assertNotIn("SQL segredo", corpo)
        self.assertNotIn("tabela", corpo)
        self.assertNotIn("host", corpo)

    def test_21_inventario_runtime_preserva_177_rotas(self):
        regras = list(self.app.url_map.iter_rules())
        self.assertEqual(len(regras), 177)

    def test_22_fontes_nao_desativam_csrf_nem_expoem_excecao_json(self):
        fonte = Path("app.py").read_text(encoding="utf-8")
        self.assertNotIn("@csrf.exempt", fonte)
        self.assertNotRegex(
            fonte,
            r"jsonify\([^\n]*(?:str|repr)\s*\(\s*(?:e|erro|exception)",
        )

    def test_23_sql_dos_endpoints_nao_concatena_entrada_direta(self):
        fonte = Path("app.py").read_text(encoding="utf-8")
        arvore = ast.parse(fonte)
        suspeitos = []
        for no in ast.walk(arvore):
            if not isinstance(no, ast.Call):
                continue
            if not isinstance(no.func, ast.Attribute) or no.func.attr != "execute":
                continue
            if not no.args:
                continue
            sql = no.args[0]
            if isinstance(sql, ast.BinOp) and isinstance(sql.op, ast.Mod):
                suspeitos.append(no.lineno)
        self.assertEqual(suspeitos, [])

    def test_24_limites_json_estao_documentados_no_codigo(self):
        self.assertEqual(APP_MODULE.JSON_MAX_BYTES, 64 * 1024)
        self.assertEqual(APP_MODULE.JSON_MAX_LIST_ITEMS, 200)
        self.assertEqual(APP_MODULE.JSON_MAX_STRING_LENGTH, 5000)
        self.assertEqual(APP_MODULE.JSON_MAX_DEPTH, 2)

    def test_25_classificador_usa_exatamente_os_44_endpoints_reais(self):
        adaptador = self.app.url_map.bind("localhost")
        endpoints = {
            adaptador.match(
                rota,
                method=METODOS_ESPECIAIS.get(
                    rota, "POST" if rota in ESCRITA else "GET"
                ),
            )[0]
            for rota in TODOS
        }
        self.assertEqual(endpoints, set(APP_MODULE.JSON_ENDPOINTS))
        rotas_funcionais_modulo = [
            regra
            for regra in self.app.url_map.iter_rules()
            if regra.rule.startswith("/fiscalizacao-contratos")
            and regra.endpoint != "fiscalizacao_contratos.static"
        ]
        self.assertEqual(len(rotas_funcionais_modulo), 105)

    def test_26_dez_escritas_exigem_csrf_ausente_ou_invalido(self):
        self.autenticar(1)
        obter_token(self.client)
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("escrita sem CSRF acessou banco"),
        ) as banco:
            for rota in sorted(ESCRITA):
                metodo = METODOS_ESPECIAIS.get(rota, "POST")
                for token in (None, "token-invalido"):
                    with self.subTest(rota=rota, token=token):
                        headers = {"X-CSRFToken": token} if token else {}
                        resposta = self.client.open(
                            rota,
                            method=metodo,
                            json={},
                            headers=headers,
                        )
                        self.assertEqual(resposta.status_code, 400)
                        self.assertTrue(resposta.is_json)
        banco.assert_not_called()

    @staticmethod
    def _corpo_json_com_tamanho(tamanho):
        dados = {f"campo_{indice}": "x" * 4900 for indice in range(13)}
        dados["complemento"] = ""
        corpo = json.dumps(dados, ensure_ascii=False, separators=(",", ":")).encode()
        diferenca = tamanho - len(corpo)
        if diferenca < 0 or diferenca > APP_MODULE.JSON_MAX_STRING_LENGTH:
            raise AssertionError("tamanho de teste incompatível")
        dados["complemento"] = "x" * diferenca
        corpo = json.dumps(dados, ensure_ascii=False, separators=(",", ":")).encode()
        if len(corpo) != tamanho:
            raise AssertionError("corpo de teste não atingiu o tamanho esperado")
        return dados, corpo

    def _validar_corpo_diretamente(
        self,
        corpo,
        campos,
        *,
        content_type="application/json",
        environ_overrides=None,
    ):
        with self.app.test_request_context(
            "/teste-json",
            method="POST",
            data=corpo,
            content_type=content_type,
            environ_overrides=environ_overrides,
        ):
            return APP_MODULE._obter_json_objeto(campos)

    def test_27_corpo_exatamente_no_limite_e_um_byte_acima(self):
        dados, corpo = self._corpo_json_com_tamanho(APP_MODULE.JSON_MAX_BYTES)
        recebido, erro = self._validar_corpo_diretamente(corpo, dados)
        self.assertIsNone(erro)
        self.assertEqual(recebido, dados)

        _dados_maior, corpo_maior = self._corpo_json_com_tamanho(
            APP_MODULE.JSON_MAX_BYTES + 1
        )
        _recebido, erro = self._validar_corpo_diretamente(corpo_maior, dados)
        self.assertEqual(erro[1], 413)
        self.assertTrue(erro[0].is_json)

    def test_28_sem_content_length_nao_contorna_limite(self):
        dados, corpo = self._corpo_json_com_tamanho(APP_MODULE.JSON_MAX_BYTES + 1)
        _recebido, erro = self._validar_corpo_diretamente(
            corpo,
            dados,
            environ_overrides={
                "CONTENT_LENGTH": "",
                "wsgi.input": BytesIO(corpo),
                "wsgi.input_terminated": True,
            },
        )
        self.assertEqual(erro[1], 413)

    def test_29_content_length_menor_forjado_nao_executa_json(self):
        dados = {"acao": "aprovar", "id": 1}
        corpo = json.dumps(dados).encode()
        _recebido, erro = self._validar_corpo_diretamente(
            corpo,
            dados,
            environ_overrides={
                "CONTENT_LENGTH": "5",
                "wsgi.input": BytesIO(corpo),
            },
        )
        self.assertEqual(erro[1], 400)

    def test_30_charset_unicode_e_codificacao_comprimida(self):
        dados = {"texto": "á" * APP_MODULE.JSON_MAX_STRING_LENGTH}
        corpo = json.dumps(dados, ensure_ascii=False).encode()
        recebido, erro = self._validar_corpo_diretamente(
            corpo,
            dados,
            content_type="application/json; charset=utf-8",
        )
        self.assertIsNone(erro)
        self.assertEqual(recebido, dados)

        with self.app.test_request_context(
            "/teste-json",
            method="POST",
            data=b"conteudo",
            content_type="application/json",
            headers={"Content-Encoding": "gzip"},
        ):
            _recebido, erro = APP_MODULE._obter_json_objeto({"texto"})
        self.assertEqual(erro[1], 415)

    def test_31_listas_somadas_e_estruturas_aninhadas_sao_rejeitadas(self):
        casos = (
            {"lista_a": list(range(150)), "lista_b": list(range(51))},
            {"dados": {"nivel": {"interno": "valor"}}},
            {"dados": [["item"]]},
            {"dados": ["x" * (APP_MODULE.JSON_MAX_STRING_LENGTH + 1)]},
        )
        for dados in casos:
            with self.subTest(dados=list(dados)):
                corpo = json.dumps(dados).encode()
                _recebido, erro = self._validar_corpo_diretamente(corpo, dados)
                self.assertEqual(erro[1], 400)

    def test_32_cache_abrange_erros_json_sem_afetar_html(self):
        obter_token(self.client)
        visitante = self.client.get("/buscar_associados")
        self.autenticar(2)
        comum_admin = self.client.get("/api/subgrupos")
        conexao = MagicMock()
        conexao.cursor.return_value.fetchone.return_value = None
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            inexistente = self.client.get("/get_associado/999")
        csrf = self.client.post("/excluir_associado/1")
        metodo = self.client.get("/registrar_fluxo_caixa")

        for resposta in (visitante, comum_admin, inexistente, csrf, metodo):
            with self.subTest(status=resposta.status_code):
                self.assertTrue(resposta.is_json)
                self.assertIn("no-store", resposta.headers.get("Cache-Control", ""))
                self.assertEqual(resposta.headers.get("Pragma"), "no-cache")

        html = self.client.get("/logout", headers={"Accept": "application/json"})
        self.assertFalse(html.is_json)
        self.assertNotIn("no-store", html.headers.get("Cache-Control", ""))

    def test_33_accept_nao_transforma_pagina_html_em_json(self):
        resposta = self.client.get(
            "/fiscalizacao-contratos",
            headers={"Accept": "application/json"},
        )
        self.assertEqual(resposta.status_code, 302)
        self.assertFalse(resposta.is_json)

    def test_34_fluxo_denuncia_permanece_bloqueado_online(self):
        token, _ = obter_token(self.client)
        for ambiente in ("homologation", "production"):
            with self.subTest(ambiente=ambiente):
                self.app.config["APP_ENV"] = ambiente
                for rota, metodo in (
                    ("/registrar_denuncia?APP_ENV=development", "POST"),
                    ("/sucesso_denuncia?APP_ENV=development", "GET"),
                ):
                    resposta = self.client.open(
                        rota,
                        method=metodo,
                        headers={
                            "X-App-Env": "development",
                            **({"X-CSRFToken": token} if metodo == "POST" else {}),
                        },
                    )
                    self.assertEqual(resposta.status_code, 404)
                    self.assertFalse(resposta.is_json)

    def test_35_frontend_orienta_novo_login_sem_token_em_url(self):
        fonte = Path("templates/cadastro.html").read_text(encoding="utf-8")
        self.assertIn("function tratarSessaoExpirada", fonte)
        self.assertIn("window.location.assign('/login')", fonte)
        self.assertNotRegex(fonte, r"/login[^'\"]*(?:csrf|token)")
        self.assertNotIn("sessionStorage", fonte)

    def test_36_limite_json_nao_reduz_limite_global_de_multipart(self):
        self.assertEqual(self.app.config["MAX_CONTENT_LENGTH"], 64 * 1024 * 1024)
        self.assertLess(APP_MODULE.JSON_MAX_BYTES, self.app.config["MAX_CONTENT_LENGTH"])

    def test_37_permissao_administrativa_html_permanece_separada(self):
        visitante = self.client.get("/fiscalizacao-contratos")
        self.assertEqual(visitante.status_code, 302)
        self.assertFalse(visitante.is_json)

        self.autenticar(2)
        comum = self.client.get("/fiscalizacao-contratos")
        self.assertEqual(comum.status_code, 403)
        self.assertFalse(comum.is_json)

        with self.client.session_transaction() as sessao:
            sessao.clear()
        self.autenticar(1)
        administrador = self.client.get("/fiscalizacao-contratos")
        self.assertEqual(administrador.status_code, 200)

        with self.client.session_transaction() as sessao:
            sessao["_user_id"] = "3"
            sessao["_fresh"] = True
        inativo = self.client.get("/fiscalizacao-contratos")
        self.assertEqual(inativo.status_code, 302)

    def test_38_administrador_prossegue_em_endpoint_json(self):
        self.autenticar(1)
        conexao = MagicMock()
        conexao.cursor.return_value.fetchall.return_value = []
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.get("/api/subgrupos")
        self.assertEqual(resposta.status_code, 200)
        self.assertTrue(resposta.is_json)
        conexao.cursor.return_value.execute.assert_called_once()


if __name__ == "__main__":
    unittest.main()
