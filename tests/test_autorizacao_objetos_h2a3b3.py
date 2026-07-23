"""Regressões de autorização por objeto da etapa H2A.3B.3."""

import inspect
import json
import unittest
from datetime import date
from decimal import Decimal
from io import BytesIO
from unittest.mock import MagicMock, patch

from test_csrf_h2a2 import APP_MODULE, cabecalho_basic, obter_token


ROTAS_EDICAO = {
    "/editar_associado": {"id_associado": "91"},
    "/editar_cadastro": {"id_cadastro": "91"},
    "/editar_conta_corrente": {"id_conta": "91"},
    "/editar_patrimonio": {"id_patrimonio": "91"},
    "/editar_transacao": {"id_transacao": "91"},
}
ROTAS_EXCLUSAO = (
    "/excluir_associado/91",
    "/excluir_cadastro/91",
    "/excluir_patrimonio/91",
    "/excluir_transacao/91",
)
ROTAS_JSON = (
    "/get_conta_corrente_detalhe/91",
    "/get_movimentacao_detalhes/91",
    "/get_patrimonio_detalhes/91",
    "/get_transacao_detalhes/91",
)
TODAS_AS_ROTAS = tuple(ROTAS_EDICAO) + ROTAS_EXCLUSAO + ROTAS_JSON


def conexao_com_linhas(*linhas):
    conexao = MagicMock()
    conexao.cursor.return_value.fetchone.side_effect = list(linhas)
    return conexao


class TestAutorizacaoObjetosH2A3B3(unittest.TestCase):
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

    def post_com_csrf(self, rota, dados=None):
        token, _ = obter_token(self.client)
        formulario = dict(dados or {})
        formulario["csrf_token"] = token
        return self.client.post(rota, data=formulario)

    @staticmethod
    def dados_patrimonio():
        return {
            "id_patrimonio": "91",
            "uvr_patrimonio": "UVR 01",
            "tipo_bem": "Equipamento",
            "categoria_bem": "Outro",
            "descricao_bem": "Bem",
            "codigo_patrimonio": "P-1",
            "marca_bem": "",
            "modelo_bem": "",
            "ano_fabricacao": "",
            "serie_chassi": "",
            "situacao_propriedade": "Próprio",
            "entidade_proprietaria": "",
            "orgao_cedente": "",
            "num_termo": "",
            "data_inicio_comodato": "",
            "data_fim_comodato": "",
            "placa": "",
            "renavam": "",
            "combustivel": "",
            "capacidade_carga": "",
            "controle_por": "",
            "medidor_inicial": "",
            "local_instalacao": "",
            "setor_uso": "",
            "nome_responsavel": "",
            "nome_operador": "",
            "status_bem": "Ativo",
            "estado_conservacao": "Bom",
            "alerta_preventiva": "",
            "observacoes_gerais": "",
        }

    def test_01_matriz_possui_exatamente_os_treze_casos_restantes(self):
        self.assertEqual(len(TODAS_AS_ROTAS), 13)
        self.assertEqual(len(set(TODAS_AS_ROTAS)), 13)

    def test_02_visitante_nao_consulta_banco(self):
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("visitante tentou consultar banco"),
        ) as conectar:
            for rota, dados in ROTAS_EDICAO.items():
                with self.subTest(rota=rota):
                    self.assertEqual(self.post_com_csrf(rota, dados).status_code, 302)
            for rota in ROTAS_EXCLUSAO:
                with self.subTest(rota=rota):
                    self.assertEqual(self.post_com_csrf(rota).status_code, 302)
            for rota in ROTAS_JSON:
                with self.subTest(rota=rota):
                    resposta = self.client.get(rota)
                    self.assertEqual(resposta.status_code, 401)
                    self.assertTrue(resposta.is_json)
        conectar.assert_not_called()

    def test_03_usuario_comum_nao_acessa_id_de_outra_uvr(self):
        self.autenticar(2)
        for rota, dados in ROTAS_EDICAO.items():
            with self.subTest(rota=rota):
                conexao = conexao_com_linhas(None)
                with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
                    resposta = self.post_com_csrf(rota, dados)
                self.assertEqual(resposta.status_code, 404)
                conexao.commit.assert_not_called()
        for rota in ROTAS_EXCLUSAO:
            with self.subTest(rota=rota):
                conexao = conexao_com_linhas(None)
                with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
                    resposta = self.post_com_csrf(rota)
                self.assertEqual(resposta.status_code, 404)
                conexao.commit.assert_not_called()
        for rota in ROTAS_JSON:
            with self.subTest(rota=rota):
                conexao = conexao_com_linhas(None)
                with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
                    resposta = self.client.get(rota)
                self.assertEqual(resposta.status_code, 404)

    def test_04_consulta_final_combina_id_e_uvr_em_todos_os_casos(self):
        self.autenticar(2)
        casos = [(rota, dados, "POST") for rota, dados in ROTAS_EDICAO.items()]
        casos += [(rota, {}, "POST") for rota in ROTAS_EXCLUSAO]
        casos += [(rota, {}, "GET") for rota in ROTAS_JSON]
        for rota, dados, metodo in casos:
            with self.subTest(rota=rota):
                conexao = conexao_com_linhas(None)
                with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
                    if metodo == "POST":
                        self.post_com_csrf(rota, dados)
                    else:
                        self.client.get(rota)
                sql, parametros = conexao.cursor.return_value.execute.call_args.args
                self.assertIn("uvr", sql.lower())
                self.assertEqual(parametros[-1], "UVR 01")
                self.assertIn(91, parametros)

    def test_05_id_inexistente_e_id_alheio_tem_mesma_resposta(self):
        self.autenticar(2)
        respostas = []
        for _ in ("inexistente", "alheio"):
            conexao = conexao_com_linhas(None)
            with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
                respostas.append(self.client.get("/get_conta_corrente_detalhe/91"))
        self.assertEqual([r.status_code for r in respostas], [404, 404])
        self.assertEqual([r.get_json() for r in respostas], [r.get_json() for r in respostas[::-1]])

    def test_06_administrador_mantem_acesso_global_explicito(self):
        self.autenticar(1)
        conexao = conexao_com_linhas(
            (91, "UVR 99", "Assoc", "001", "Banco", "1", "2", "Conta")
        )
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.get("/get_conta_corrente_detalhe/91")
        self.assertEqual(resposta.status_code, 200)
        sql, parametros = conexao.cursor.return_value.execute.call_args.args
        self.assertNotIn("trim(uvr)", sql.lower())
        self.assertEqual(parametros, (91,))

    def test_07_uvr_forjada_na_edicao_nao_e_gravada_na_solicitacao(self):
        self.autenticar(2)
        autorizacao = conexao_com_linhas((91,))
        gravacao = MagicMock()
        dados = {
            "id_cadastro": "91",
            "uvr": "UVR 99",
            "campo_forjado": "nao deve entrar",
            "associacao_id": "999",
            "usuario_id": "999",
            "is_admin": "true",
            "criado_por": "999",
            "atualizado_por": "999",
        }
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=[autorizacao, gravacao]
        ):
            resposta = self.post_com_csrf("/editar_cadastro", dados)
        self.assertEqual(resposta.status_code, 200)
        sql, parametros = gravacao.cursor.return_value.execute.call_args.args
        dados_salvos = json.loads(parametros[2])
        self.assertEqual(dados_salvos["uvr"], "UVR 01")
        self.assertNotIn("campo_forjado", dados_salvos)
        for campo in (
            "associacao_id",
            "usuario_id",
            "is_admin",
            "criado_por",
            "atualizado_por",
        ):
            self.assertNotIn(campo, dados_salvos)
        self.assertIn("alvo.id = %s", sql)
        self.assertIn("alvo.uvr", sql)
        self.assertEqual(parametros[-2:], (91, "UVR 01"))
        gravacao.commit.assert_called_once()

    def test_08_falha_de_banco_fecha_acesso_sem_expor_excecao(self):
        self.autenticar(2)
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=RuntimeError("segredo_sql")
        ):
            resposta = self.client.get("/get_conta_corrente_detalhe/91")
        self.assertEqual(resposta.status_code, 500)
        self.assertNotIn(b"segredo_sql", resposta.data)

    def test_09_todas_as_rotas_continuam_com_protecao_de_login(self):
        nomes = (
            "editar_associado", "editar_cadastro", "editar_conta_corrente",
            "editar_patrimonio", "editar_transacao", "excluir_associado",
            "excluir_cadastro", "excluir_patrimonio", "excluir_transacao",
            "get_conta_corrente_detalhe", "get_movimentacao_detalhes",
            "get_patrimonio_detalhes", "get_transacao_detalhes",
        )
        for nome in nomes:
            with self.subTest(nome=nome):
                fonte = inspect.getsource(getattr(APP_MODULE, nome))
                self.assertIn("@login_required", fonte)

    def test_10_usuario_ativo_continua_validado_no_carregamento(self):
        fonte = inspect.getsource(APP_MODULE.load_user)
        self.assertIn("ativo = TRUE", fonte)
        self.assertIn("WHERE id = %s", fonte)

    def test_11_falha_na_gravacao_desfaz_transacao(self):
        self.autenticar(2)
        autorizacao = conexao_com_linhas((91,))
        gravacao = MagicMock()
        gravacao.cursor.return_value.execute.side_effect = RuntimeError("falha simulada")
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=[autorizacao, gravacao]
        ):
            resposta = self.post_com_csrf(
                "/editar_cadastro", {"id_cadastro": "91", "uvr": "UVR 01"}
            )
        self.assertEqual(resposta.status_code, 500)
        gravacao.rollback.assert_called_once()
        gravacao.commit.assert_not_called()
        self.assertNotIn(b"falha simulada", resposta.data)

    def test_12_transacao_relacionada_de_outra_uvr_e_bloqueada(self):
        self.autenticar(2)
        alvo = conexao_com_linhas((91,))
        relacionado = conexao_com_linhas(None)
        dados = {
            "id_transacao": "91",
            "uvr_transacao": "UVR 01",
            "data_documento_transacao": "2026-07-22",
            "tipo_transacao": "Despesa",
            "tipo_atividade_transacao": "Serviço",
            "fornecedor_prestador_transacao": "88",
        }
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=[alvo, relacionado]
        ) as conectar:
            resposta = self.post_com_csrf("/editar_transacao", dados)
        self.assertEqual(resposta.status_code, 404)
        self.assertEqual(conectar.call_count, 2)
        alvo.commit.assert_not_called()
        relacionado.commit.assert_not_called()

    def test_13_admin_nao_confirma_update_de_objeto_inexistente(self):
        self.autenticar(1)
        conexao = MagicMock()
        conexao.cursor.return_value.rowcount = 0
        dados = {
            "id_cadastro": "91", "uvr": "UVR 99", "cnpj": "",
            "razao_social": "Teste", "tipo_atividade": "Outro",
            "tipo_cadastro": "Cliente",
        }
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.post_com_csrf("/editar_cadastro", dados)
        self.assertEqual(resposta.status_code, 404)
        conexao.rollback.assert_called_once()
        conexao.commit.assert_not_called()

    def test_14_post_sem_csrf_e_get_nao_executam_operacoes(self):
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("requisição inválida tentou acessar banco"),
        ) as conectar:
            for rota, dados in ROTAS_EDICAO.items():
                with self.subTest(rota=rota, caso="sem_csrf"):
                    self.assertEqual(self.client.post(rota, data=dados).status_code, 400)
                with self.subTest(rota=rota, caso="get"):
                    self.assertEqual(self.client.get(rota).status_code, 405)
            for rota in ROTAS_EXCLUSAO:
                with self.subTest(rota=rota, caso="sem_csrf"):
                    self.assertEqual(self.client.post(rota).status_code, 400)
                with self.subTest(rota=rota, caso="get"):
                    self.assertEqual(self.client.get(rota).status_code, 405)
        conectar.assert_not_called()

    def test_15_basic_auth_e_token_csrf_nao_substituem_login(self):
        headers = cabecalho_basic()
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("Basic tentou acessar objeto"),
        ) as conectar:
            for rota, dados in ROTAS_EDICAO.items():
                with self.subTest(rota=rota):
                    token, _ = obter_token(self.client, headers=headers)
                    resposta = self.client.post(
                        rota,
                        data={**dados, "csrf_token": token},
                        headers=headers,
                    )
                    self.assertEqual(resposta.status_code, 302)
            for rota in ROTAS_JSON:
                with self.subTest(rota=rota):
                    resposta = self.client.get(rota, headers=headers)
                    self.assertEqual(resposta.status_code, 401)
                    self.assertTrue(resposta.is_json)
        conectar.assert_not_called()

    def test_16_usuario_inativo_nao_recupera_sessao_nem_objeto(self):
        self.autenticar(3)
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("usuário inativo tentou acessar banco"),
        ) as conectar:
            self.assertEqual(
                self.client.get("/get_conta_corrente_detalhe/91").status_code, 401
            )
            self.assertEqual(
                self.post_com_csrf(
                    "/editar_cadastro", {"id_cadastro": "91"}
                ).status_code,
                302,
            )
        conectar.assert_not_called()

    def test_17_mudanca_de_uvr_antes_do_sql_final_impede_solicitacao(self):
        self.autenticar(2)
        autorizacao = conexao_com_linhas((91,))
        gravacao = conexao_com_linhas(None)
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=[autorizacao, gravacao]
        ):
            resposta = self.post_com_csrf(
                "/editar_cadastro", {"id_cadastro": "91", "uvr": "UVR 01"}
            )
        self.assertEqual(resposta.status_code, 404)
        sql, parametros = gravacao.cursor.return_value.execute.call_args.args
        self.assertIn("alvo.id = %s", sql)
        self.assertIn("alvo.uvr", sql)
        self.assertEqual(parametros[-2:], (91, "UVR 01"))
        gravacao.rollback.assert_called_once()
        gravacao.commit.assert_not_called()

    def test_18_relacionamento_e_revalidado_no_sql_final(self):
        self.autenticar(2)
        alvo = conexao_com_linhas((91,))
        relacionado = conexao_com_linhas((88,))
        gravacao = conexao_com_linhas((Decimal("0"), "Aberto"), None)
        dados = {
            "id_transacao": "91",
            "uvr_transacao": "UVR 01",
            "data_documento_transacao": "2026-07-22",
            "tipo_transacao": "Despesa",
            "tipo_atividade_transacao": "Serviço",
            "fornecedor_prestador_transacao": "88",
        }
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=[alvo, relacionado, gravacao],
        ):
            resposta = self.post_com_csrf("/editar_transacao", dados)
        self.assertEqual(resposta.status_code, 404)
        sql, parametros = gravacao.cursor.return_value.execute.call_args.args
        self.assertIn("EXISTS", sql)
        self.assertIn("relacionado.id = %s", sql)
        self.assertEqual(parametros[-4:], (91, "UVR 01", 88, "UVR 01"))
        gravacao.rollback.assert_called_once()
        gravacao.commit.assert_not_called()

    def test_19_falha_no_commit_executa_rollback_e_resposta_generica(self):
        self.autenticar(2)
        autorizacao = conexao_com_linhas((91,))
        gravacao = conexao_com_linhas((701,))
        gravacao.commit.side_effect = RuntimeError("falha_commit_secreta")
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=[autorizacao, gravacao]
        ):
            resposta = self.post_com_csrf(
                "/editar_cadastro", {"id_cadastro": "91", "uvr": "UVR 01"}
            )
        self.assertEqual(resposta.status_code, 500)
        gravacao.rollback.assert_called_once()
        self.assertNotIn(b"falha_commit_secreta", resposta.data)

    def test_20_edicoes_legitimas_do_usuario_comum_sao_preservadas(self):
        self.autenticar(2)
        casos = {
            "/editar_associado": {"id_associado": "91"},
            "/editar_cadastro": {"id_cadastro": "91"},
            "/editar_conta_corrente": {
                "id_conta": "91",
                "banco_conta": "001|Banco",
                "agencia_conta": "1",
                "conta_corrente_conta": "2",
            },
            "/editar_patrimonio": self.dados_patrimonio(),
            "/editar_transacao": {
                "id_transacao": "91",
                "uvr_transacao": "UVR 01",
                "data_documento_transacao": "2026-07-22",
                "tipo_transacao": "Despesa",
                "tipo_atividade_transacao": "Serviço",
            },
        }
        for rota, dados in casos.items():
            with self.subTest(rota=rota):
                autorizacao = conexao_com_linhas((91,))
                gravacao = (
                    conexao_com_linhas((Decimal("0"), "Aberto"), (801,))
                    if rota == "/editar_transacao"
                    else conexao_com_linhas((801,))
                )
                with patch.object(
                    APP_MODULE,
                    "conectar_banco",
                    side_effect=[autorizacao, gravacao],
                ):
                    resposta = self.post_com_csrf(rota, dados)
                self.assertEqual(resposta.status_code, 200)
                gravacao.commit.assert_called_once()

    def test_21_solicitacoes_de_exclusao_legitimas_sao_preservadas(self):
        self.autenticar(2)
        casos = {
            "/excluir_associado/91": [("Associado",), (901,)],
            "/excluir_cadastro/91": [("Cadastro",), (902,)],
            "/excluir_patrimonio/91": [("Patrimônio",), (903,)],
            "/excluir_transacao/91": [
                (Decimal("0"), "NF-1", "Fornecedor"),
                (904,),
            ],
        }
        for rota, linhas in casos.items():
            with self.subTest(rota=rota):
                autorizacao = conexao_com_linhas((91,))
                operacao = conexao_com_linhas(*linhas)
                with patch.object(
                    APP_MODULE,
                    "conectar_banco",
                    side_effect=[autorizacao, operacao],
                ):
                    resposta = self.post_com_csrf(rota)
                self.assertEqual(resposta.status_code, 200)
                operacao.commit.assert_called_once()

    def test_22_consultas_json_legitimas_da_uvr_sao_preservadas(self):
        self.autenticar(2)
        conta = conexao_com_linhas(
            (91, "UVR 01", "Assoc", "001", "Banco", "1", "2", "Conta")
        )
        movimento = conexao_com_linhas(
            (91, date(2026, 7, 22), Decimal("10"), "Pagamento", "", "", "")
        )
        patrimonio = conexao_com_linhas((91, "UVR 01", "Bem"))
        patrimonio.cursor.return_value.description = [
            ("id",),
            ("uvr",),
            ("descricao",),
        ]
        transacao = conexao_com_linhas(
            (
                91,
                "UVR 01",
                "Assoc",
                "Despesa",
                "Serviço",
                "Fornecedor",
                "NF-1",
                date(2026, 7, 22),
                Decimal("10"),
                "Aberto",
                None,
            )
        )
        for rota, conexao in (
            ("/get_conta_corrente_detalhe/91", conta),
            ("/get_movimentacao_detalhes/91", movimento),
            ("/get_patrimonio_detalhes/91", patrimonio),
            ("/get_transacao_detalhes/91", transacao),
        ):
            with self.subTest(rota=rota):
                with patch.object(
                    APP_MODULE, "conectar_banco", return_value=conexao
                ):
                    resposta = self.client.get(rota)
                self.assertEqual(resposta.status_code, 200)

    def test_23_csrf_nao_possui_isencao_nem_desativacao(self):
        fonte = inspect.getsource(APP_MODULE)
        self.assertNotIn("@csrf.exempt", fonte)
        self.assertNotIn("WTF_CSRF_ENABLED = False", fonte)

    def test_24_erro_na_validacao_relacionada_nao_chega_a_gravacao(self):
        self.autenticar(2)
        alvo = conexao_com_linhas((91,))
        dados = {
            "id_transacao": "91",
            "uvr_transacao": "UVR 01",
            "data_documento_transacao": "2026-07-22",
            "tipo_transacao": "Despesa",
            "tipo_atividade_transacao": "Serviço",
            "fornecedor_prestador_transacao": "88",
        }
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=[alvo, RuntimeError("falha_relacionamento")],
        ) as conectar:
            resposta = self.post_com_csrf("/editar_transacao", dados)
        self.assertEqual(resposta.status_code, 404)
        self.assertEqual(conectar.call_count, 2)
        alvo.commit.assert_not_called()

    def test_25_bloqueio_ocorre_antes_de_arquivo_api_ou_documento(self):
        self.autenticar(2)
        conexao = conexao_com_linhas(None)
        token, _ = obter_token(self.client)
        with (
            patch.object(APP_MODULE, "conectar_banco", return_value=conexao),
            patch.object(APP_MODULE.base64, "b64encode") as codificar,
            patch.object(APP_MODULE.requests, "get") as api,
            patch.object(APP_MODULE, "SimpleDocTemplate") as documento,
        ):
            resposta = self.client.post(
                "/editar_associado",
                data={
                    "id_associado": "91",
                    "csrf_token": token,
                    "foto_associado": (BytesIO(b"arquivo"), "foto.jpg"),
                },
                content_type="multipart/form-data",
            )
        self.assertEqual(resposta.status_code, 404)
        codificar.assert_not_called()
        api.assert_not_called()
        documento.assert_not_called()
        conexao.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
