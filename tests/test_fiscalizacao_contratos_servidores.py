"""Testes da Etapa 2B sem conexão real e sem SQL real."""

import importlib
import os
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from modulos.fiscalizacao_contratos.services.servidores_service import (
    MatriculaDuplicadaError,
    ServidorService,
)


CONEXAO_FALSA_IMPORTACAO = MagicMock(name="conexao_falsa_servidores")
CURSOR_FALSO_IMPORTACAO = CONEXAO_FALSA_IMPORTACAO.cursor.return_value
CURSOR_FALSO_IMPORTACAO.fetchall.return_value = []
CURSOR_FALSO_IMPORTACAO.fetchone.return_value = None

PATCH_CONEXAO = patch("psycopg2.connect", return_value=CONEXAO_FALSA_IMPORTACAO)
MOCK_CONNECT = PATCH_CONEXAO.start()

sys.modules.pop("app", None)
with (
    patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False),
    patch("dotenv.load_dotenv", return_value=False),
):
    APP_MODULE = importlib.import_module("app")

MOCK_CONNECT.side_effect = AssertionError("Nenhuma conexão real é permitida nos testes")


class ServidorServiceFake:
    def __init__(self):
        agora = datetime(2026, 7, 14, 9, 0)
        self.servidores = {
            1: {
                "id": 1,
                "nome": "João da Silva",
                "matricula": "MAT-001",
                "cargo": "Analista Administrativo",
                "setor": "Contratos",
                "email": "joao@exemplo.test",
                "telefone": "(11) 3333-4444",
                "observacoes": "Servidor titular.",
                "ativo": True,
                "criado_em": agora,
                "atualizado_em": agora,
                "criado_por_usuario_id": 1,
                "atualizado_por_usuario_id": 1,
            },
            2: {
                "id": 2,
                "nome": "Maria Oliveira",
                "matricula": "MAT-002",
                "cargo": "Engenheira",
                "setor": "Obras",
                "email": None,
                "telefone": None,
                "observacoes": None,
                "ativo": True,
                "criado_em": agora,
                "atualizado_em": agora,
                "criado_por_usuario_id": 1,
                "atualizado_por_usuario_id": 1,
            },
        }
        self.proximo_id = 3
        self.criar_chamadas = 0
        self.ultima_busca = None
        self.ultimo_incluir_inativos = None

    def listar(self, busca="", incluir_inativos=False):
        self.ultima_busca = busca
        self.ultimo_incluir_inativos = incluir_inativos
        termo = busca.casefold()
        resultado = []
        for servidor in self.servidores.values():
            campos_pesquisa = (
                servidor["nome"],
                servidor["matricula"],
                servidor["cargo"] or "",
            )
            corresponde = not termo or any(termo in campo.casefold() for campo in campos_pesquisa)
            if (incluir_inativos or servidor["ativo"]) and corresponde:
                resultado.append(servidor)
        return resultado

    def obter(self, servidor_id):
        return self.servidores[servidor_id]

    def criar(self, dados, usuario_id):
        self.criar_chamadas += 1
        if any(item["matricula"] == dados["matricula"] for item in self.servidores.values()):
            raise MatriculaDuplicadaError(
                "Já existe um servidor cadastrado com esta matrícula."
            )
        servidor_id = self.proximo_id
        self.proximo_id += 1
        agora = datetime(2026, 7, 14, 10, 0)
        self.servidores[servidor_id] = {
            "id": servidor_id,
            **dados,
            "ativo": True,
            "criado_em": agora,
            "atualizado_em": agora,
            "criado_por_usuario_id": usuario_id,
            "atualizado_por_usuario_id": usuario_id,
        }
        return servidor_id

    def atualizar(self, servidor_id, dados, usuario_id):
        if any(
            item["matricula"] == dados["matricula"] and item["id"] != servidor_id
            for item in self.servidores.values()
        ):
            raise MatriculaDuplicadaError(
                "Já existe um servidor cadastrado com esta matrícula."
            )
        self.servidores[servidor_id].update(dados)
        self.servidores[servidor_id]["atualizado_em"] = datetime(2026, 7, 14, 11, 0)
        self.servidores[servidor_id]["atualizado_por_usuario_id"] = usuario_id

    def inativar(self, servidor_id, usuario_id):
        self.servidores[servidor_id]["ativo"] = False
        self.servidores[servidor_id]["atualizado_em"] = datetime(2026, 7, 14, 12, 0)
        self.servidores[servidor_id]["atualizado_por_usuario_id"] = usuario_id

    def reativar(self, servidor_id, usuario_id):
        self.servidores[servidor_id]["ativo"] = True
        self.servidores[servidor_id]["atualizado_em"] = datetime(2026, 7, 14, 13, 0)
        self.servidores[servidor_id]["atualizado_por_usuario_id"] = usuario_id


class TestFiscalizacaoContratosServidores(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.flask_app = APP_MODULE.app
        cls.flask_app.config.update(TESTING=True)
        cls.user_loader_original = APP_MODULE.login_manager._user_callback

    @classmethod
    def tearDownClass(cls):
        APP_MODULE.login_manager._user_callback = cls.user_loader_original

    def setUp(self):
        self.client = self.flask_app.test_client()
        self.servico = ServidorServiceFake()
        APP_MODULE.login_manager._user_callback = self._carregar_usuario_falso
        self.patcher_servico = patch(
            "modulos.fiscalizacao_contratos.routes.servidores.ServidorService",
            return_value=self.servico,
        )
        self.patcher_servico.start()

    def tearDown(self):
        self.patcher_servico.stop()

    @staticmethod
    def _carregar_usuario_falso(user_id):
        perfis = {
            "2": APP_MODULE.User(2, "usuario_comum", "usuario", "UVR 01"),
            "1": APP_MODULE.User(1, "administrador", "admin", None),
        }
        return perfis.get(str(user_id))

    def autenticar_como(self, user_id):
        with self.client.session_transaction() as sessao:
            sessao["_user_id"] = str(user_id)
            sessao["_fresh"] = True

    @staticmethod
    def dados_validos(matricula="MAT-003"):
        return {
            "nome": "Carlos Souza",
            "matricula": matricula,
            "cargo": "Fiscal de Contrato",
            "setor": "Fiscalização",
            "email": "carlos@exemplo.test",
            "telefone": "(11) 99999-0000",
            "observacoes": "Disponível para futuras designações.",
        }

    def test_administrador_acessa_listagem(self):
        self.autenticar_como(1)

        resposta = self.client.get("/fiscalizacao-contratos/servidores")

        self.assertEqual(resposta.status_code, 200)
        self.assertIn("João da Silva".encode("utf-8"), resposta.data)

    def test_administrador_cadastra_servidor(self):
        self.autenticar_como(1)
        dados = self.dados_validos()
        dados["matricula"] = " MAT - 003 "

        resposta = self.client.post(
            "/fiscalizacao-contratos/servidores/novo",
            data=dados,
        )

        self.assertEqual(resposta.status_code, 302)
        self.assertIn(3, self.servico.servidores)
        self.assertEqual(self.servico.servidores[3]["matricula"], "MAT-003")

    def test_administrador_visualiza_e_edita_servidor(self):
        self.autenticar_como(1)

        detalhe = self.client.get("/fiscalizacao-contratos/servidores/1")
        formulario = self.client.get("/fiscalizacao-contratos/servidores/1/editar")
        dados = self.dados_validos(matricula="MAT-001")
        dados["nome"] = "João da Silva Atualizado"
        edicao = self.client.post(
            "/fiscalizacao-contratos/servidores/1/editar",
            data=dados,
        )

        self.assertEqual(detalhe.status_code, 200)
        self.assertEqual(formulario.status_code, 200)
        self.assertEqual(edicao.status_code, 302)
        self.assertEqual(self.servico.servidores[1]["nome"], "João da Silva Atualizado")

    def test_matricula_duplicada_e_rejeitada(self):
        self.autenticar_como(1)

        resposta = self.client.post(
            "/fiscalizacao-contratos/servidores/novo",
            data=self.dados_validos(matricula=" MAT - 001 "),
        )

        self.assertEqual(resposta.status_code, 409)
        self.assertIn("Já existe um servidor".encode("utf-8"), resposta.data)

    def test_nome_vazio_e_rejeitado(self):
        self.autenticar_como(1)
        dados = self.dados_validos()
        dados["nome"] = "   "

        resposta = self.client.post("/fiscalizacao-contratos/servidores/novo", data=dados)

        self.assertEqual(resposta.status_code, 400)
        self.assertEqual(self.servico.criar_chamadas, 0)
        self.assertIn("nome é obrigatório".encode("utf-8"), resposta.data)

    def test_matricula_vazia_e_rejeitada(self):
        self.autenticar_como(1)
        dados = self.dados_validos(matricula="   ")

        resposta = self.client.post("/fiscalizacao-contratos/servidores/novo", data=dados)

        self.assertEqual(resposta.status_code, 400)
        self.assertEqual(self.servico.criar_chamadas, 0)
        self.assertIn("matrícula é obrigatória".encode("utf-8"), resposta.data)

    def test_email_invalido_e_rejeitado(self):
        self.autenticar_como(1)
        dados = self.dados_validos()
        dados["email"] = "email-invalido"

        resposta = self.client.post("/fiscalizacao-contratos/servidores/novo", data=dados)

        self.assertEqual(resposta.status_code, 400)
        self.assertEqual(self.servico.criar_chamadas, 0)
        self.assertIn("e-mail válido".encode("utf-8"), resposta.data)

    def test_inativacao_preserva_registro(self):
        self.autenticar_como(1)

        resposta = self.client.post("/fiscalizacao-contratos/servidores/1/inativar")

        self.assertEqual(resposta.status_code, 302)
        self.assertIn(1, self.servico.servidores)
        self.assertFalse(self.servico.servidores[1]["ativo"])

    def test_reativacao_funciona(self):
        self.autenticar_como(1)
        self.servico.servidores[1]["ativo"] = False

        resposta = self.client.post("/fiscalizacao-contratos/servidores/1/reativar")

        self.assertEqual(resposta.status_code, 302)
        self.assertTrue(self.servico.servidores[1]["ativo"])

    def test_pesquisa_por_nome(self):
        self.autenticar_como(1)

        resposta = self.client.get("/fiscalizacao-contratos/servidores?busca=Maria")

        self.assertEqual(resposta.status_code, 200)
        self.assertIn("Maria Oliveira".encode("utf-8"), resposta.data)
        self.assertNotIn("João da Silva".encode("utf-8"), resposta.data)
        self.assertEqual(self.servico.ultima_busca, "Maria")

    def test_pesquisa_por_matricula(self):
        self.autenticar_como(1)

        resposta = self.client.get("/fiscalizacao-contratos/servidores?busca=MAT-001")

        self.assertEqual(resposta.status_code, 200)
        self.assertIn("João da Silva".encode("utf-8"), resposta.data)
        self.assertNotIn("Maria Oliveira".encode("utf-8"), resposta.data)

    def test_pesquisa_por_cargo(self):
        self.autenticar_como(1)

        resposta = self.client.get("/fiscalizacao-contratos/servidores?busca=Engenheira")

        self.assertEqual(resposta.status_code, 200)
        self.assertIn("Maria Oliveira".encode("utf-8"), resposta.data)
        self.assertNotIn("João da Silva".encode("utf-8"), resposta.data)

    def test_opcao_mostra_servidores_inativos(self):
        self.autenticar_como(1)
        self.servico.servidores[2]["ativo"] = False

        ocultos = self.client.get("/fiscalizacao-contratos/servidores")
        exibidos = self.client.get(
            "/fiscalizacao-contratos/servidores?incluir_inativos=1"
        )

        self.assertNotIn("Maria Oliveira".encode("utf-8"), ocultos.data)
        self.assertIn("Maria Oliveira".encode("utf-8"), exibidos.data)
        self.assertTrue(self.servico.ultimo_incluir_inativos)

    def test_cartao_servidores_aparece_no_painel_do_modulo(self):
        self.autenticar_como(1)

        resposta = self.client.get("/fiscalizacao-contratos")

        self.assertEqual(resposta.status_code, 200)
        self.assertIn("Servidores e Responsáveis".encode("utf-8"), resposta.data)
        self.assertIn(b"/fiscalizacao-contratos/servidores", resposta.data)

    def test_usuario_comum_recebe_403(self):
        self.autenticar_como(2)

        resposta = self.client.get("/fiscalizacao-contratos/servidores")

        self.assertEqual(resposta.status_code, 403)

    def test_visitante_e_encaminhado_ao_login(self):
        resposta = self.client.get("/fiscalizacao-contratos/servidores")

        self.assertEqual(resposta.status_code, 302)
        self.assertIn("/login", resposta.headers["Location"])

    def test_rotas_antigas_continuam_funcionando(self):
        rotas = {regra.rule for regra in self.flask_app.url_map.iter_rules()}

        for rota in (
            "/",
            "/login",
            "/logout",
            "/cadastrar",
            "/buscar_associados",
            "/fiscalizacao-contratos/empresas",
            "/fiscalizacao-contratos/servidores",
        ):
            self.assertIn(rota, rotas)

    def test_nenhuma_conexao_real_foi_aberta(self):
        chamadas_antes = MOCK_CONNECT.call_count
        self.autenticar_como(1)

        resposta = self.client.get("/fiscalizacao-contratos/servidores")

        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(MOCK_CONNECT.call_count, chamadas_antes)

    def test_servico_inativa_e_reativa_sem_excluir(self):
        conexao = MagicMock()
        cursor = conexao.cursor.return_value.__enter__.return_value
        cursor.rowcount = 1
        servico = ServidorService(lambda: conexao)

        servico.inativar(1, 1)
        chamada_inativar = cursor.execute.call_args
        sql_inativar = chamada_inativar.args[0].upper()
        servico.reativar(1, 1)
        chamada_reativar = cursor.execute.call_args
        sql_reativar = chamada_reativar.args[0].upper()

        self.assertIn("ATIVO = %S", sql_inativar)
        self.assertIn("ATIVO = %S", sql_reativar)
        self.assertIs(chamada_inativar.args[1][0], False)
        self.assertIs(chamada_reativar.args[1][0], True)
        self.assertIn("CURRENT_TIMESTAMP", sql_inativar)
        self.assertIn("CURRENT_TIMESTAMP", sql_reativar)
        self.assertNotIn("DELETE", sql_inativar + sql_reativar)

    def test_servico_atualiza_horario_na_edicao(self):
        conexao = MagicMock()
        cursor = conexao.cursor.return_value.__enter__.return_value
        cursor.rowcount = 1
        servico = ServidorService(lambda: conexao)

        servico.atualizar(1, self.dados_validos(matricula="MAT-001"), 1)
        sql = cursor.execute.call_args.args[0].upper()

        self.assertIn("UPDATE FC_SERVIDORES", sql)
        self.assertIn("ATUALIZADO_EM = CURRENT_TIMESTAMP", sql)

    def test_migracao_e_idempotente_aditiva_e_nao_automatica(self):
        caminho = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "modulos",
            "fiscalizacao_contratos",
            "migrations",
            "002_criar_fc_servidores.sql",
        )
        with open(caminho, encoding="utf-8") as arquivo:
            sql = arquivo.read().upper()

        self.assertIn("CREATE TABLE IF NOT EXISTS FC_SERVIDORES", sql)
        self.assertIn("CREATE INDEX IF NOT EXISTS", sql)
        self.assertIn("REFERENCES USUARIOS(ID)", sql)
        for comando_proibido in ("DROP", "TRUNCATE", "DELETE", "ALTER TABLE"):
            self.assertNotIn(comando_proibido, sql)


def tearDownModule():
    PATCH_CONEXAO.stop()


if __name__ == "__main__":
    unittest.main()
