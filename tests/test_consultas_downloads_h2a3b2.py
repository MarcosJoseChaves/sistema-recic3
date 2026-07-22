"""Testes de segurança das consultas e downloads da etapa H2A.3B.2."""

import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

from test_csrf_h2a2 import APP_MODULE, cabecalho_basic


ROTAS_CONSULTA = {
    "/get_associados_ativos?uvr=UVR%2001": "D",
    "/get_cadastros_ativos?uvr=UVR%2001": "D",
    "/get_clientes_fornecedores_com_pendencias?uvr=UVR%2001&tipo_movimentacao=Recebimento": "D",
    "/get_contas_correntes?uvr=UVR%2001": "D",
    "/get_notas_em_aberto?uvr=UVR%2001&id_cadastro_cf=7&tipo_movimentacao=Recebimento&data_inicial=2026-01-01&data_final=2026-01-31": "D",
    "/get_produtos_servicos": "B",
    "/get_relatorio_catalog_options?option_type=grupo": "B",
    "/get_relatorio_entidades_para_filtro?tipo_entidade=Cliente&uvr=UVR%2001": "D",
    "/get_relatorio_tipos_atividade_transacao?tipo_transacao=Receita": "B",
    "/get_relatorio_uvrs": "D",
    "/get_resumo_fluxo_caixa?uvr=UVR%2001&data_inicial=2026-01-01&data_final=2026-01-31": "D",
}

ROTAS_D_COM_FILTRO_UVR = (
    "/get_associados_ativos?uvr=UVR%2099",
    "/get_cadastros_ativos?uvr=UVR%2099",
    "/get_clientes_fornecedores_com_pendencias?uvr=UVR%2099&tipo_movimentacao=Recebimento",
    "/get_contas_correntes?uvr=UVR%2099",
    "/get_notas_em_aberto?uvr=UVR%2099&id_cadastro_cf=7&tipo_movimentacao=Recebimento&data_inicial=2026-01-01&data_final=2026-01-31",
    "/get_relatorio_entidades_para_filtro?tipo_entidade=Cliente&uvr=UVR%2099",
    "/get_resumo_fluxo_caixa?uvr=UVR%2099&data_inicial=2026-01-01&data_final=2026-01-31",
)


def conexao_vazia():
    conexao = MagicMock()
    cursor = conexao.cursor.return_value
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = None
    return conexao


class TestConsultasDownloadsH2A3B2(unittest.TestCase):
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

    def test_01_matriz_contem_exatamente_onze_consultas(self):
        self.assertEqual(len(ROTAS_CONSULTA), 11)
        self.assertEqual(len(set(ROTAS_CONSULTA)), 11)

    def test_02_visitante_recebe_json_401_sem_consultar_banco(self):
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("visitante tentou acessar banco"),
        ) as conectar:
            for rota in ROTAS_CONSULTA:
                with self.subTest(rota=rota):
                    resposta = self.client.get(rota)
                    self.assertEqual(resposta.status_code, 401)
                    self.assertTrue(resposta.is_json)
        conectar.assert_not_called()

    def test_03_basic_sem_login_interno_nao_concede_consulta(self):
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("Basic isolada tentou acessar banco"),
        ) as conectar:
            for rota in ROTAS_CONSULTA:
                with self.subTest(rota=rota):
                    self.assertEqual(
                        self.client.get(rota, headers=cabecalho_basic()).status_code,
                        401,
                    )
        conectar.assert_not_called()

    def test_04_usuario_comum_acessa_consultas_no_proprio_escopo(self):
        self.autenticar(2)
        for rota in ROTAS_CONSULTA:
            with self.subTest(rota=rota):
                with patch.object(
                    APP_MODULE, "conectar_banco", return_value=conexao_vazia()
                ):
                    self.assertEqual(self.client.get(rota).status_code, 200)

    def test_05_filtro_todos_do_usuario_comum_permanece_na_propria_uvr(self):
        self.autenticar(2)
        conexao = conexao_vazia()
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.get("/get_contas_correntes?uvr=todos")
        self.assertEqual(resposta.status_code, 200)
        sql, parametros = conexao.cursor.return_value.execute.call_args.args
        self.assertIn("WHERE uvr = %s", sql)
        self.assertEqual(parametros, ("UVR 01",))

    def test_06_query_string_de_outra_uvr_e_negada_sem_banco(self):
        self.autenticar(2)
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("filtro alheio tentou acessar banco"),
        ) as conectar:
            for rota in ROTAS_D_COM_FILTRO_UVR:
                with self.subTest(rota=rota):
                    self.assertEqual(self.client.get(rota).status_code, 403)
        conectar.assert_not_called()

    def test_07_lista_de_uvrs_do_usuario_comum_nao_consulta_dados_globais(self):
        self.autenticar(2)
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("lista de UVRs tentou acessar banco"),
        ) as conectar:
            resposta = self.client.get("/get_relatorio_uvrs")
        self.assertEqual(resposta.get_json(), ["UVR 01"])
        conectar.assert_not_called()

    def test_08_administrador_possui_politica_global_explicita(self):
        self.autenticar(1)
        conexao = conexao_vazia()
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.get("/get_contas_correntes?uvr=todos")
        self.assertEqual(resposta.status_code, 200)
        sql, parametros = conexao.cursor.return_value.execute.call_args.args
        self.assertNotIn("WHERE uvr = %s", sql)
        self.assertEqual(parametros, ())

    def test_09_erro_de_banco_retorna_mensagem_generica(self):
        self.autenticar(2)
        with patch.object(
            APP_MODULE, "conectar_banco", side_effect=RuntimeError("tabela_secreta")
        ):
            resposta = self.client.get("/get_produtos_servicos")
        self.assertEqual(resposta.status_code, 500)
        self.assertNotIn(b"tabela_secreta", resposta.data)

    def test_10_fichas_exigem_login_antes_de_consultar_banco(self):
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("visitante tentou acessar ficha"),
        ) as conectar:
            for rota in ("/imprimir_ficha_associado/1", "/imprimir_ficha_cadastro/1"):
                with self.subTest(rota=rota):
                    resposta = self.client.get(rota)
                    self.assertEqual(resposta.status_code, 302)
                    self.assertIn("/login", resposta.headers["Location"])
        conectar.assert_not_called()

    def test_11_id_alheio_das_fichas_retorna_404_sem_gerar_pdf(self):
        self.autenticar(2)
        for rota in ("/imprimir_ficha_associado/99", "/imprimir_ficha_cadastro/99"):
            with self.subTest(rota=rota):
                conexao = conexao_vazia()
                with (
                    patch.object(APP_MODULE, "conectar_banco", return_value=conexao),
                    patch.object(APP_MODULE, "SimpleDocTemplate") as gerador,
                ):
                    resposta = self.client.get(rota)
                self.assertEqual(resposta.status_code, 404)
                sql, parametros = conexao.cursor.return_value.execute.call_args.args
                self.assertIn("uvr = %s", sql)
                self.assertEqual(parametros, (99, "UVR 01"))
                gerador.assert_not_called()

    def test_12_ficha_autorizada_mantem_uvr_na_consulta_final(self):
        self.autenticar(2)
        conexao = conexao_vazia()
        conexao.cursor.return_value.fetchone.return_value = (
            "Nome", "000", "RG", date(1990, 1, 1), date(2020, 1, 1),
            "Ativo", "UVR 01", "Associação", "Rua", "1", "Bairro",
            "Cidade", "UF", "00000000", "Telefone", None, "10",
        )
        documento = MagicMock()
        with (
            patch.object(APP_MODULE, "conectar_banco", return_value=conexao),
            patch.object(APP_MODULE, "SimpleDocTemplate", return_value=documento),
        ):
            resposta = self.client.get("/imprimir_ficha_associado/7")
        self.assertEqual(resposta.status_code, 200)
        sql, parametros = conexao.cursor.return_value.execute.call_args.args
        self.assertIn("id = %s AND uvr = %s", sql)
        self.assertEqual(parametros, (7, "UVR 01"))
        documento.build.assert_called_once()

    def test_13_csv_neutraliza_formula_sem_alterar_numero(self):
        for prefixo in ("=", "+", "-", "@"):
            with self.subTest(prefixo=prefixo):
                self.assertEqual(
                    APP_MODULE._texto_csv_seguro(prefixo + "comando"),
                    "'" + prefixo + "comando",
                )
        self.assertEqual(APP_MODULE._texto_csv_seguro("123,45"), "123,45")

    def test_14_texto_pdf_e_escapado(self):
        self.assertEqual(
            APP_MODULE._texto_pdf_seguro("<b>&conteúdo</b>"),
            "&lt;b&gt;&amp;conteúdo&lt;/b&gt;",
        )

    def test_15_usuario_inativo_nao_recupera_sessao_nem_consulta(self):
        self.autenticar(3)
        with patch.object(
            APP_MODULE,
            "conectar_banco",
            side_effect=AssertionError("sessão inativa tentou consultar banco"),
        ) as conectar:
            resposta = self.client.get("/get_produtos_servicos")
        self.assertEqual(resposta.status_code, 401)
        conectar.assert_not_called()

    def test_16_entidade_de_relatorio_repete_uvr_nas_duas_tabelas(self):
        self.autenticar(2)
        conexao = conexao_vazia()
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.get(
                "/get_relatorio_entidades_para_filtro"
                "?tipo_entidade=Cliente&uvr=UVR%2001"
            )
        self.assertEqual(resposta.status_code, 200)
        sql, parametros = conexao.cursor.return_value.execute.call_args.args
        self.assertIn("c.uvr = %s", sql)
        self.assertIn("tf.uvr = %s", sql)
        self.assertEqual(parametros, ("UVR 01", "UVR 01"))

    def test_17_csv_cobre_prefixos_invisiveis_e_preserva_numeros(self):
        casos_perigosos = (
            "=SUM(A1:A2)",
            "+CMD",
            "-2+3",
            "@IMPORT",
            "  =SUM(A1:A2)",
            "\t+CMD",
            "\n-2+3",
            "\ufeff@IMPORT",
        )
        for valor in casos_perigosos:
            with self.subTest(valor=repr(valor)):
                self.assertEqual(APP_MODULE._texto_csv_seguro(valor), "'" + valor)

        self.assertEqual(APP_MODULE._texto_csv_seguro("texto comum"), "texto comum")
        self.assertEqual(APP_MODULE._texto_csv_seguro("'=SUM(A1:A2)"), "'=SUM(A1:A2)")
        self.assertEqual(APP_MODULE._texto_csv_seguro("  '=SUM(A1:A2)"), "'  '=SUM(A1:A2)")
        self.assertEqual(APP_MODULE._texto_csv_seguro(""), "")
        self.assertEqual(APP_MODULE._texto_csv_seguro(None), "")
        for numero in (10, -10, 1.5, Decimal("-12.34")):
            with self.subTest(numero=numero):
                self.assertIs(APP_MODULE._texto_csv_seguro(numero), numero)

    def test_18_pdf_escapa_marcacao_e_nao_duplica_escape(self):
        casos = {
            "<b>Administrador</b>": "&lt;b&gt;Administrador&lt;/b&gt;",
            '<font color="red">Teste</font>': '&lt;font color="red"&gt;Teste&lt;/font&gt;',
            "A & B": "A &amp; B",
            'texto com "aspas"': 'texto com "aspas"',
            "ação com acentuação": "ação com acentuação",
            "texto " * 200: "texto " * 200,
        }
        for original, esperado in casos.items():
            with self.subTest(original=original[:30]):
                protegido = APP_MODULE._texto_pdf_seguro(original)
                self.assertEqual(protegido, esperado)
                self.assertEqual(APP_MODULE._texto_pdf_seguro(protegido), esperado)

    def test_19_filtro_ausente_busca_e_paginacao_nao_removem_escopo(self):
        self.autenticar(2)
        conexao = conexao_vazia()
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.get(
                "/get_cadastros_ativos?page=2&q=forjado&tipo_cadastro_filtro=Cliente"
            )
        self.assertEqual(resposta.status_code, 200)
        sql, parametros = conexao.cursor.return_value.execute.call_args.args
        self.assertIn("uvr = %s", sql)
        self.assertEqual(parametros, ("UVR 01", "Cliente"))

    def test_20_ficha_de_cadastro_propria_usa_id_e_uvr(self):
        self.autenticar(2)
        conexao = conexao_vazia()
        conexao.cursor.return_value.fetchone.return_value = (
            8, "UVR 01", "Associação", datetime(2026, 1, 1), "Empresa",
            "00000000000000", "00000000", "Rua", "1", "Bairro", "Cidade",
            "UF", "Telefone", "Atividade", "Cliente",
        )
        documento = MagicMock()
        with (
            patch.object(APP_MODULE, "conectar_banco", return_value=conexao),
            patch.object(APP_MODULE, "SimpleDocTemplate", return_value=documento),
        ):
            resposta = self.client.get("/imprimir_ficha_cadastro/8")
        self.assertEqual(resposta.status_code, 200)
        sql, parametros = conexao.cursor.return_value.execute.call_args.args
        self.assertIn("id = %s AND uvr = %s", sql)
        self.assertEqual(parametros, (8, "UVR 01"))
        documento.build.assert_called_once()

    def test_21_ficha_alheia_e_inexistente_tem_resposta_identica(self):
        self.autenticar(2)
        respostas = []
        for identificador in (77, 999999):
            with patch.object(
                APP_MODULE, "conectar_banco", return_value=conexao_vazia()
            ):
                respostas.append(self.client.get(f"/imprimir_ficha_cadastro/{identificador}"))
        self.assertEqual(respostas[0].status_code, 404)
        self.assertEqual(respostas[1].status_code, 404)
        self.assertEqual(respostas[0].data, respostas[1].data)

    def test_22_administrador_tem_ramo_explicito_na_ficha(self):
        self.autenticar(1)
        conexao = conexao_vazia()
        conexao.cursor.return_value.fetchone.return_value = None
        with patch.object(APP_MODULE, "conectar_banco", return_value=conexao):
            resposta = self.client.get("/imprimir_ficha_associado/7")
        self.assertEqual(resposta.status_code, 404)
        sql, parametros = conexao.cursor.return_value.execute.call_args.args
        self.assertNotIn("uvr = %s", sql)
        self.assertEqual(parametros, (7,))


if __name__ == "__main__":
    unittest.main()
