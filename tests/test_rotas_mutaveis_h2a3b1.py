"""Testes das 11 rotas mutaveis antes publicas e do login uniforme."""

import unittest
from decimal import Decimal
from unittest.mock import MagicMock, patch

from werkzeug.datastructures import MultiDict
from werkzeug.security import generate_password_hash

from test_csrf_h2a2 import APP_MODULE, PADRAO_TOKEN, cabecalho_basic, obter_token


ROTAS_FORMULARIO = (
    "/cadastrar_conta_corrente",
    "/cadastrar_produto_servico",
    "/registrar_denuncia",
    "/registrar_transacao_financeira",
)
ROTAS_JSON = (
    "/baixar_csv_extrato",
    "/baixar_csv_relatorio",
    "/baixar_pdf_extrato",
    "/baixar_pdf_relatorio_financeiro",
    "/gerar_extrato_bancario",
    "/gerar_relatorio",
    "/registrar_fluxo_caixa",
)
TODAS_AS_ROTAS = ROTAS_FORMULARIO + ROTAS_JSON


def conexao_com_uvr(uvr):
    conexao = MagicMock()
    conexao.cursor.return_value.fetchone.return_value = (uvr,)
    return conexao


class TestRotasMutaveisH2A3B1(unittest.TestCase):
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

    def post_com_csrf(self, rota, *, dados=None, json=None, headers=None):
        token, _ = obter_token(self.client, headers=headers)
        cabecalhos = dict(headers or {})
        if rota in ROTAS_JSON:
            cabecalhos["X-CSRFToken"] = token
            return self.client.post(rota, json=json or {}, headers=cabecalhos)
        formulario = dict(dados or {})
        formulario["csrf_token"] = token
        return self.client.post(rota, data=formulario, headers=cabecalhos)

    def test_01_exatamente_as_onze_rotas_estao_na_matriz_de_teste(self):
        self.assertEqual(len(TODAS_AS_ROTAS), 11)
        self.assertEqual(len(set(TODAS_AS_ROTAS)), 11)

    def test_02_post_sem_csrf_rejeita_as_onze_rotas_antes_do_negocio(self):
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("rota bloqueada tentou acessar banco"),
        ) as conectar:
            for rota in TODAS_AS_ROTAS:
                with self.subTest(rota=rota):
                    resposta = self.client.post(
                        rota, json={} if rota in ROTAS_JSON else None
                    )
                    self.assertEqual(resposta.status_code, 400)
        conectar.assert_not_called()

    def test_02a_csrf_invalido_rejeita_as_onze_rotas_antes_do_negocio(self):
        obter_token(self.client)
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("CSRF invalido tentou acessar banco"),
        ) as conectar:
            for rota in TODAS_AS_ROTAS:
                with self.subTest(rota=rota):
                    if rota in ROTAS_JSON:
                        resposta = self.client.post(
                            rota, json={}, headers={"X-CSRFToken": "invalido"}
                        )
                    else:
                        resposta = self.client.post(
                            rota, data={"csrf_token": "invalido"}
                        )
                    self.assertEqual(resposta.status_code, 400)
        conectar.assert_not_called()

    def test_03_visitante_com_csrf_valido_nao_chega_ao_negocio(self):
        with (
            patch.object(
                APP_MODULE,
                "conectar_banco",
                side_effect=AssertionError("visitante tentou acessar banco"),
            ) as conectar,
            patch.object(APP_MODULE, "fetch_report_data") as relatorio,
            patch.object(APP_MODULE, "fetch_extrato_data") as extrato,
        ):
            for rota in TODAS_AS_ROTAS:
                with self.subTest(rota=rota):
                    resposta = self.post_com_csrf(rota)
                    esperado = 401 if rota in ROTAS_JSON else 302
                    self.assertEqual(resposta.status_code, esperado)
                    if rota in ROTAS_JSON:
                        self.assertTrue(resposta.is_json)
                        self.assertIn("error", resposta.get_json())
            conectar.assert_not_called()
            relatorio.assert_not_called()
            extrato.assert_not_called()

    def test_04_basic_correta_sem_login_interno_nao_concede_acesso(self):
        headers = cabecalho_basic()
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("Basic isolada tentou acessar banco"),
        ) as conectar:
            for rota in TODAS_AS_ROTAS:
                with self.subTest(rota=rota):
                    resposta = self.post_com_csrf(rota, headers=headers)
                    esperado = 401 if rota in ROTAS_JSON else 302
                    self.assertEqual(resposta.status_code, esperado)
        conectar.assert_not_called()

    def test_05_metodo_get_nao_executa_as_operacoes(self):
        for rota in TODAS_AS_ROTAS:
            with self.subTest(rota=rota):
                self.assertEqual(self.client.get(rota).status_code, 405)

    def test_06_usuario_comum_nao_executa_operacao_global_ou_uvr_alheia(self):
        self.autenticar(2)
        casos_formulario = {
            "/cadastrar_produto_servico": {},
            "/cadastrar_conta_corrente": {"uvr_conta": "UVR 99"},
            "/registrar_transacao_financeira": {"uvr_transacao": "UVR 99"},
            "/registrar_denuncia": {"uvr_denuncia": "UVR 99"},
        }
        casos_json = {
            "/registrar_fluxo_caixa": {"uvr": "UVR 99"},
            "/gerar_relatorio": {"uvr": "UVR 99"},
            "/baixar_csv_relatorio": {"uvr": "UVR 99"},
            "/baixar_pdf_relatorio_financeiro": {"uvr": "UVR 99"},
        }
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("usuario sem vinculo tentou acessar banco"),
        ) as conectar:
            for rota, dados in casos_formulario.items():
                with self.subTest(rota=rota):
                    self.assertEqual(
                        self.post_com_csrf(rota, dados=dados).status_code, 403
                    )
            for rota, dados in casos_json.items():
                with self.subTest(rota=rota):
                    self.assertEqual(
                        self.post_com_csrf(rota, json=dados).status_code, 403
                    )
        conectar.assert_not_called()

    def test_07_conta_de_outra_uvr_bloqueia_os_tres_extratos(self):
        self.autenticar(2)
        filtros = {
            "id_conta_corrente_extrato": 9,
            "data_inicial_extrato": "2026-01-01",
            "data_final_extrato": "2026-01-31",
        }
        with (
            patch.object(
                APP_MODULE, "conectar_banco", return_value=conexao_com_uvr("UVR 99")
            ),
            patch.object(APP_MODULE, "fetch_extrato_data") as extrato,
        ):
            for rota in (
                "/gerar_extrato_bancario",
                "/baixar_csv_extrato",
                "/baixar_pdf_extrato",
            ):
                with self.subTest(rota=rota):
                    self.assertEqual(
                        self.post_com_csrf(rota, json=filtros).status_code, 403
                    )
            extrato.assert_not_called()

    def test_08_entidade_de_outra_uvr_bloqueia_relatorio(self):
        self.autenticar(2)
        filtros = {
            "uvr": "UVR 01",
            "tipo_entidade": "Cliente",
            "id_entidade": 88,
        }
        with (
            patch.object(
                APP_MODULE, "conectar_banco", return_value=conexao_com_uvr("UVR 99")
            ),
            patch.object(APP_MODULE, "fetch_report_data") as relatorio,
        ):
            resposta = self.post_com_csrf("/gerar_relatorio", json=filtros)
        self.assertEqual(resposta.status_code, 403)
        relatorio.assert_not_called()

    def test_09_usuario_da_uvr_pode_chegar_as_operacoes_de_relatorio(self):
        self.autenticar(2)
        with patch.object(APP_MODULE, "fetch_report_data", return_value=[]) as relatorio:
            respostas = {
                rota: self.post_com_csrf(rota, json={"uvr": "UVR 01"})
                for rota in (
                    "/gerar_relatorio",
                    "/baixar_csv_relatorio",
                    "/baixar_pdf_relatorio_financeiro",
                )
            }
        self.assertEqual(respostas["/gerar_relatorio"].status_code, 200)
        self.assertEqual(respostas["/baixar_csv_relatorio"].status_code, 404)
        self.assertEqual(respostas["/baixar_pdf_relatorio_financeiro"].status_code, 404)
        self.assertEqual(relatorio.call_count, 3)
        for chamada in relatorio.call_args_list:
            self.assertEqual(chamada.args[0]["uvr"], "UVR 01")

    def test_10_usuario_da_uvr_pode_consultar_extrato_da_propria_conta(self):
        self.autenticar(2)
        filtros = {
            "id_conta_corrente_extrato": 7,
            "data_inicial_extrato": "2026-01-01",
            "data_final_extrato": "2026-01-31",
        }
        with (
            patch.object(
                APP_MODULE, "conectar_banco", return_value=conexao_com_uvr("UVR 01")
            ),
            patch.object(APP_MODULE, "fetch_extrato_data", return_value=None) as extrato,
        ):
            respostas = {
                rota: self.post_com_csrf(rota, json=filtros)
                for rota in (
                    "/gerar_extrato_bancario",
                    "/baixar_csv_extrato",
                    "/baixar_pdf_extrato",
                )
            }
        self.assertEqual(respostas["/gerar_extrato_bancario"].status_code, 200)
        self.assertEqual(respostas["/baixar_csv_extrato"].status_code, 404)
        self.assertEqual(respostas["/baixar_pdf_extrato"].status_code, 404)
        self.assertEqual(extrato.call_count, 3)

    def test_11_operacoes_formulario_autorizadas_preservam_fluxo(self):
        self.autenticar(2)
        conta = MagicMock()
        transacao = MagicMock()
        transacao.cursor.return_value.fetchone.return_value = (21,)
        denuncia = MagicMock()
        denuncia.cursor.return_value.fetchone.return_value = None

        dados_conta = {
            "uvr_conta": "UVR 01",
            "banco_conta": "001|Banco Teste",
            "agencia_conta": "1234",
            "conta_corrente_conta": "55-6",
            "data_hora_cadastro_conta": "01/01/2026 10:00:00",
        }
        with patch.object(APP_MODULE, "conectar_banco", return_value=conta):
            resposta_conta = self.post_com_csrf(
                "/cadastrar_conta_corrente", dados=dados_conta
            )
        self.assertEqual(resposta_conta.status_code, 302)
        conta.commit.assert_called_once()

        dados_transacao = MultiDict(
            [
                ("uvr_transacao", "UVR 01"),
                ("data_documento_transacao", "2026-01-01"),
                ("tipo_transacao", "Despesa"),
                ("tipo_atividade_transacao", "Rateio dos Associados"),
                ("data_hora_cadastro_transacao", "01/01/2026 10:00:00"),
                ("nome_fornecedor_prestador_transacao", "Rateio geral"),
                ("produto_servico_descricao[]", "Servico"),
                ("produto_servico_unidade[]", "un"),
                ("produto_servico_quantidade[]", "1"),
                ("produto_servico_valor_unitario[]", "10,00"),
            ]
        )
        token, _ = obter_token(self.client)
        dados_transacao.add("csrf_token", token)
        with patch.object(APP_MODULE, "conectar_banco", return_value=transacao):
            resposta_transacao = self.client.post(
                "/registrar_transacao_financeira", data=dados_transacao
            )
        self.assertEqual(resposta_transacao.status_code, 302)
        transacao.commit.assert_called_once()

        dados_denuncia = {
            "uvr_denuncia": "UVR 01",
            "descricao_denuncia": "Registro de teste",
            "data_registro_denuncia": "01/01/2026 10:00:00",
        }
        with patch.object(APP_MODULE, "conectar_banco", return_value=denuncia):
            resposta_denuncia = self.post_com_csrf(
                "/registrar_denuncia", dados=dados_denuncia
            )
        self.assertEqual(resposta_denuncia.status_code, 302)
        denuncia.commit.assert_called_once()

    def test_12_fluxo_autorizado_valida_todos_os_objetos_antes_do_commit(self):
        self.autenticar(2)
        autorizacao = conexao_com_uvr("UVR 01")
        negocio = MagicMock()
        negocio.cursor.return_value.fetchone.side_effect = [
            (Decimal("100.00"),),
            (30,),
            (Decimal("0"), Decimal("100"), Decimal("100")),
        ]
        dados = {
            "uvr": "UVR 01",
            "id_conta_corrente": 1,
            "ids_nfs_selecionadas": [2],
            "id_cadastro_cf_str": 3,
            "is_associado_rateio": False,
            "nome_cadastro_cf_display": "Fornecedor Teste",
            "tipo_movimentacao": "Pagamento",
            "data_efetiva": "2026-01-10",
            "valor_efetivo": "10.00",
            "data_hora_registro_fluxo": "10/01/2026 10:00:00",
        }
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=[autorizacao, negocio]
        ):
            resposta = self.post_com_csrf("/registrar_fluxo_caixa", json=dados)
        self.assertEqual(resposta.status_code, 200)
        negocio.commit.assert_called_once()
        self.assertEqual(autorizacao.cursor.return_value.execute.call_count, 3)
        sqls_negocio = [
            chamada.args[0].lower()
            for chamada in negocio.cursor.return_value.execute.call_args_list
        ]
        for sql in sqls_negocio:
            if "transacoes_financeiras" in sql or "contas_correntes" in sql:
                self.assertIn("uvr", sql)

    def test_13_administrador_chega_ao_cadastro_global(self):
        self.autenticar(1)
        conexao = MagicMock()
        dados = {
            "tipo_produto_servico": "Despesa",
            "tipo_atividade_produto_servico": "Operacao",
            "item_produto_servico": "Item de teste",
        }
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.post_com_csrf(
                "/cadastrar_produto_servico", dados=dados
            )
        self.assertEqual(resposta.status_code, 302)
        conexao.commit.assert_called_once()

    def test_14_denuncia_fica_oculta_nos_ambientes_online(self):
        self.autenticar(2)
        for ambiente in ("homologation", "production"):
            with self.subTest(ambiente=ambiente):
                self.app.config["APP_ENV"] = ambiente
                with patch.object(
                    APP_MODULE,
                    "conectar_banco",
                    side_effect=AssertionError("denuncia online tentou acessar banco"),
                ) as conectar:
                    resposta = self.post_com_csrf(
                        "/registrar_denuncia", dados={"uvr_denuncia": "UVR 01"}
                    )
                self.assertEqual(resposta.status_code, 404)
                conectar.assert_not_called()

    def test_15_login_inexistente_e_senha_errada_usam_mesma_resposta(self):
        def tentar(registro, usuario, senha):
            cliente = self.app.test_client()
            token, _ = obter_token(cliente)
            conexao = MagicMock()
            conexao.cursor.return_value.fetchone.return_value = registro
            with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
                return cliente.post(
                    "/login",
                    data={"username": usuario, "password": senha, "csrf_token": token},
                )

        inexistente = tentar(None, "nome-inexistente", "segredo-inexistente")
        senha_errada = tentar(
            (
                2,
                "usuario-existente",
                generate_password_hash("senha-correta"),
                "usuario",
                "UVR 01",
            ),
            "usuario-existente",
            "senha-errada",
        )
        self.assertEqual(inexistente.status_code, senha_errada.status_code)
        corpos_normalizados = []
        for resposta in (inexistente, senha_errada):
            corpo = resposta.get_data(as_text=True).lower()
            self.assertIn("ou senha", corpo)
            self.assertNotIn("nao encontrado", corpo)
            self.assertNotIn("senha incorreta", corpo)
            self.assertNotIn("nome-inexistente", corpo)
            self.assertNotIn("segredo-inexistente", corpo)
            self.assertNotIn("senha-errada", corpo)
            corpos_normalizados.append(PADRAO_TOKEN.sub(b"csrf", resposta.data))
        self.assertEqual(corpos_normalizados[0], corpos_normalizados[1])

    def test_16_login_inexistente_verifica_hash_ficticio_uma_vez(self):
        token, _ = obter_token(self.client)
        conexao = MagicMock()
        conexao.cursor.return_value.fetchone.return_value = None
        with (
            patch.object(APP_MODULE, "conectar_banco", return_value=conexao),
            patch.object(APP_MODULE, "check_password_hash", return_value=False) as verificar,
        ):
            resposta = self.client.post(
                "/login",
                data={"username": "ausente", "password": "x", "csrf_token": token},
            )
        self.assertEqual(resposta.status_code, 200)
        verificar.assert_called_once_with(APP_MODULE.HASH_SENHA_FICTICIO, "x")

    def test_17_login_valido_continua_autenticando(self):
        token, _ = obter_token(self.client)
        conexao = MagicMock()
        conexao.cursor.return_value.fetchone.return_value = (
            2,
            "usuario",
            generate_password_hash("senha-correta"),
            "usuario",
            "UVR 01",
        )
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.post(
                "/login",
                data={
                    "username": "usuario",
                    "password": "senha-correta",
                    "csrf_token": token,
                },
            )
        self.assertEqual(resposta.status_code, 302)
        with self.client.session_transaction() as sessao:
            self.assertEqual(sessao.get("_user_id"), "2")

    def test_18_lista_parcialmente_autorizada_bloqueia_todo_o_fluxo(self):
        self.autenticar(2)
        autorizacao = MagicMock()
        autorizacao.cursor.return_value.fetchone.side_effect = [
            ("UVR 01",),
            ("UVR 01",),
            ("UVR 01",),
            ("UVR 99",),
        ]
        dados = {
            "uvr": "UVR 01",
            "id_conta_corrente": 1,
            "ids_nfs_selecionadas": [2, 3, 4],
        }
        with patch.object(
            APP_MODULE, "conectar_banco", return_value=autorizacao
        ) as conectar:
            resposta = self.post_com_csrf("/registrar_fluxo_caixa", json=dados)
        self.assertEqual(resposta.status_code, 403)
        self.assertTrue(resposta.is_json)
        self.assertEqual(conectar.call_count, 1)
        autorizacao.commit.assert_not_called()
        autorizacao.rollback.assert_not_called()

    def test_19_objeto_inexistente_e_erro_de_banco_negam_sem_relatorio(self):
        self.autenticar(2)
        filtros = {
            "id_conta_corrente_extrato": 999,
            "data_inicial_extrato": "2026-01-01",
            "data_final_extrato": "2026-01-31",
        }
        inexistente = conexao_com_uvr(None)
        inexistente.cursor.return_value.fetchone.return_value = None
        with (
            patch.object(APP_MODULE, "conectar_banco", return_value=inexistente),
            patch.object(APP_MODULE, "fetch_extrato_data") as gerar,
        ):
            resposta_inexistente = self.post_com_csrf(
                "/gerar_extrato_bancario", json=filtros
            )
        self.assertEqual(resposta_inexistente.status_code, 403)
        gerar.assert_not_called()
        inexistente.commit.assert_not_called()

        with (
            patch.object(
                APP_MODULE,
                "conectar_banco",
                side_effect=RuntimeError("detalhe interno ficticio"),
            ),
            patch.object(APP_MODULE, "fetch_extrato_data") as gerar,
        ):
            resposta_erro = self.post_com_csrf(
                "/gerar_extrato_bancario", json=filtros
            )
        self.assertEqual(resposta_erro.status_code, 403)
        self.assertNotIn("detalhe interno", resposta_erro.get_data(as_text=True))
        self.assertNotIn("contas_correntes", resposta_erro.get_data(as_text=True))
        gerar.assert_not_called()

    def test_20_consulta_final_do_extrato_repete_escopo_da_uvr(self):
        conexao = MagicMock()
        cursor = conexao.cursor.return_value
        cursor.fetchone.side_effect = [
            ("UVR 01", "Associacao", "Banco", "1", "2", "Conta"),
            (Decimal("0"),),
        ]
        cursor.fetchall.return_value = []
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            dados = APP_MODULE.fetch_extrato_data(
                {
                    "id_conta_corrente_extrato": 1,
                    "data_inicial_extrato": "2026-01-01",
                    "data_final_extrato": "2026-01-31",
                    "_uvr_autorizada": "UVR 01",
                }
            )
        self.assertEqual(dados["conta_info"]["uvr"], "UVR 01")
        self.assertEqual(cursor.execute.call_count, 3)
        for chamada in cursor.execute.call_args_list:
            sql = chamada.args[0]
            parametros = chamada.args[1]
            self.assertIn("uvr", sql.lower())
            self.assertIn("UVR 01", parametros)

    def test_21_consulta_final_do_relatorio_limita_uvr_e_entidade(self):
        self.autenticar(2)
        autorizacao = conexao_com_uvr("UVR 01")
        relatorio = MagicMock()
        cursor = relatorio.cursor.return_value
        cursor.description = []
        cursor.fetchall.return_value = []
        filtros = {
            "uvr": "UVR 01",
            "tipo_entidade": "Cliente",
            "id_entidade": 8,
        }
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=[autorizacao, relatorio]
        ):
            resposta = self.post_com_csrf("/gerar_relatorio", json=filtros)
        self.assertEqual(resposta.status_code, 200)
        sql, parametros = cursor.execute.call_args.args
        self.assertIn("tf.uvr = %s", sql)
        self.assertIn("tf.id_cadastro_origem = %s", sql)
        self.assertIn("UVR 01", parametros)
        self.assertIn(8, parametros)

    def test_22_administrador_tem_bypass_global_explicito_nas_rotas_d(self):
        self.autenticar(1)
        with (
            patch.object(
                APP_MODULE,
                "conectar_banco",
                side_effect=AssertionError("admin nao precisa de consulta preliminar"),
            ) as conectar,
            patch.object(APP_MODULE, "fetch_report_data", return_value=[]) as relatorio,
            patch.object(APP_MODULE, "fetch_extrato_data", return_value=None) as extrato,
        ):
            resposta_relatorio = self.post_com_csrf(
                "/gerar_relatorio", json={"uvr": "UVR 99"}
            )
            resposta_extrato = self.post_com_csrf(
                "/gerar_extrato_bancario",
                json={"id_conta_corrente_extrato": 77},
            )
        self.assertEqual(resposta_relatorio.status_code, 200)
        self.assertEqual(resposta_extrato.status_code, 200)
        conectar.assert_not_called()
        self.assertEqual(relatorio.call_args.args[0]["uvr"], "UVR 99")
        self.assertIsNone(extrato.call_args.args[0]["_uvr_autorizada"])

    def test_23_denuncia_online_bloqueia_inclusive_admin_e_mantem_headers(self):
        self.autenticar(1)
        for ambiente in ("homologation", "production"):
            with self.subTest(ambiente=ambiente):
                self.app.config["APP_ENV"] = ambiente
                with patch.object(
                    APP_MODULE,
                    "conectar_banco",
                    side_effect=AssertionError("admin reativou denuncia online"),
                ) as conectar:
                    resposta = self.post_com_csrf(
                        "/registrar_denuncia",
                        dados={"uvr_denuncia": "UVR 01"},
                    )
                self.assertEqual(resposta.status_code, 404)
                self.assertEqual(resposta.headers.get("X-Content-Type-Options"), "nosniff")
                self.assertEqual(resposta.headers.get("X-Frame-Options"), "DENY")
                conectar.assert_not_called()

    def test_24_denuncia_autorizada_em_development_e_login_inativo_uniforme(self):
        self.autenticar(2)
        self.app.config["APP_ENV"] = "development"
        conexao = MagicMock()
        conexao.cursor.return_value.fetchone.return_value = None
        dados = {
            "uvr_denuncia": "UVR 01",
            "descricao_denuncia": "Registro controlado",
            "data_registro_denuncia": "01/01/2026 10:00:00",
        }
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.post_com_csrf("/registrar_denuncia", dados=dados)
        self.assertEqual(resposta.status_code, 302)
        conexao.commit.assert_called_once()

        cliente = self.app.test_client()
        token, _ = obter_token(cliente)
        login_conn = MagicMock()
        login_conn.cursor.return_value.fetchone.return_value = None
        with patch.object(APP_MODULE, "conectar_banco", return_value=login_conn):
            inativo = cliente.post(
                "/login",
                data={
                    "username": "conta-inativa",
                    "password": "senha-ficticia",
                    "csrf_token": token,
                },
            )
        corpo = PADRAO_TOKEN.sub(b"csrf_token_normalizado", inativo.data)
        self.assertEqual(inativo.status_code, 200)
        self.assertIn(b"ou senha", corpo)
        self.assertNotIn(b"conta-inativa", corpo)
        consulta = login_conn.cursor.return_value.execute.call_args.args[0]
        self.assertIn("ativo = TRUE", consulta)

    def test_25_sessao_de_usuario_inativado_nao_e_recarregada(self):
        conexao = MagicMock()
        conexao.cursor.return_value.fetchone.return_value = None
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            usuario = APP_MODULE.load_user("2")
        self.assertIsNone(usuario)
        consulta = conexao.cursor.return_value.execute.call_args.args[0]
        self.assertIn("ativo = TRUE", consulta)

    def test_26_insert_da_transacao_repete_escopo_da_entidade(self):
        self.autenticar(2)
        autorizacao = conexao_com_uvr("UVR 01")
        negocio = MagicMock()
        negocio.cursor.return_value.fetchone.return_value = (41,)
        dados = MultiDict(
            [
                ("uvr_transacao", "UVR 01"),
                ("data_documento_transacao", "2026-01-01"),
                ("tipo_transacao", "Despesa"),
                ("tipo_atividade_transacao", "Operacao"),
                ("data_hora_cadastro_transacao", "01/01/2026 10:00:00"),
                ("fornecedor_prestador_transacao", "8"),
                ("nome_fornecedor_prestador_transacao", "Fornecedor"),
                ("produto_servico_descricao[]", "Servico"),
                ("produto_servico_unidade[]", "un"),
                ("produto_servico_quantidade[]", "1"),
                ("produto_servico_valor_unitario[]", "10,00"),
            ]
        )
        token, _ = obter_token(self.client)
        dados.add("csrf_token", token)
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=[autorizacao, negocio]
        ):
            resposta = self.client.post(
                "/registrar_transacao_financeira", data=dados
            )
        self.assertEqual(resposta.status_code, 302)
        sql, parametros = negocio.cursor.return_value.execute.call_args_list[0].args
        self.assertIn("FROM cadastros", sql)
        self.assertIn("c.uvr = %s", sql)
        self.assertIn("UVR 01", parametros)
        negocio.commit.assert_called_once()

    def test_27_falha_no_escopo_final_da_transacao_executa_rollback(self):
        self.autenticar(2)
        autorizacao = conexao_com_uvr("UVR 01")
        negocio = MagicMock()
        negocio.cursor.return_value.fetchone.return_value = None
        dados = MultiDict(
            [
                ("uvr_transacao", "UVR 01"),
                ("data_documento_transacao", "2026-01-01"),
                ("tipo_transacao", "Despesa"),
                ("tipo_atividade_transacao", "Operacao"),
                ("data_hora_cadastro_transacao", "01/01/2026 10:00:00"),
                ("fornecedor_prestador_transacao", "8"),
                ("nome_fornecedor_prestador_transacao", "Fornecedor"),
                ("produto_servico_descricao[]", "Servico"),
                ("produto_servico_unidade[]", "un"),
                ("produto_servico_quantidade[]", "1"),
                ("produto_servico_valor_unitario[]", "10,00"),
            ]
        )
        token, _ = obter_token(self.client)
        dados.add("csrf_token", token)
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=[autorizacao, negocio]
        ):
            resposta = self.client.post(
                "/registrar_transacao_financeira", data=dados
            )
        self.assertEqual(resposta.status_code, 403)
        negocio.rollback.assert_called_once()
        negocio.commit.assert_not_called()
        self.assertEqual(negocio.cursor.return_value.execute.call_count, 1)


if __name__ == "__main__":
    unittest.main()
