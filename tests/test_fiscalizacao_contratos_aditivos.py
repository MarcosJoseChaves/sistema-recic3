"""Testes da Etapa 2D sem conexão real e sem SQL real."""

import importlib
import os
import sys
import unittest
from datetime import date, datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

from modulos.fiscalizacao_contratos.services.aditivos_service import (
    AditivoDuplicadoError,
    AditivoService,
    ContratoAditivoInvalidoError,
)
from modulos.fiscalizacao_contratos.validacoes_aditivos import (
    normalizar_e_validar_aditivo,
)


CONEXAO_FALSA = MagicMock(name="conexao_falsa_aditivos")
CURSOR_FALSO = CONEXAO_FALSA.cursor.return_value
CURSOR_FALSO.fetchall.return_value = []
CURSOR_FALSO.fetchone.return_value = None
PATCH_CONEXAO = patch("psycopg2.connect", return_value=CONEXAO_FALSA)
MOCK_CONNECT = PATCH_CONEXAO.start()

sys.modules.pop("app", None)
with (
    patch.dict(os.environ, {"APP_ENV": "testing", "SECRET_KEY": "teste-ficticio", "DATABASE_URL": ""}, clear=True),
    patch("dotenv.load_dotenv", return_value=False),
):
    APP_MODULE = importlib.import_module("app")

MOCK_CONNECT.side_effect = AssertionError("Nenhuma conexão real é permitida nos testes")


class FormularioFake(dict):
    def getlist(self, chave):
        valor = self.get(chave, [])
        return valor if isinstance(valor, list) else [valor]


class AditivoServiceFake:
    def __init__(self):
        self.contratos = {
            1: {
                "id": 1,
                "numero_contrato": "CT-001/2026",
                "empresa_nome": "Empresa Um Ltda",
                "valor_original": Decimal("100000.00"),
                "vigencia_inicio": date(2026, 1, 1),
                "vigencia_fim": date(2026, 12, 31),
                "ativo": True,
            },
            2: {
                "id": 2,
                "numero_contrato": "CT-002/2026",
                "empresa_nome": "Empresa Dois Ltda",
                "valor_original": Decimal("50000.00"),
                "vigencia_inicio": date(2026, 2, 1),
                "vigencia_fim": date(2027, 1, 31),
                "ativo": True,
            },
        }
        self.aditivos = {
            1: self._aditivo(
                1,
                1,
                "TA-001/2026",
                "Acréscimo de valor",
                valor_acrescimo=Decimal("10000.00"),
            )
        }
        self.proximo_id = 2
        self.criar_chamadas = 0
        self.ultimos_filtros = None

    def _aditivo(
        self,
        aditivo_id,
        contrato_id,
        numero,
        tipo,
        *,
        valor_acrescimo=Decimal("0.00"),
        valor_supressao=Decimal("0.00"),
        dias_acrescidos=None,
        nova_vigencia_fim=None,
    ):
        contrato = self.contratos[contrato_id]
        agora = datetime(2026, 7, 14, 10, 0)
        return {
            "id": aditivo_id,
            "contrato_id": contrato_id,
            "numero_termo": numero,
            "tipo_aditivo": tipo,
            "data_assinatura": date(2026, 7, 10),
            "data_inicio_efeitos": date(2026, 7, 10),
            "dias_acrescidos": dias_acrescidos,
            "nova_vigencia_fim": nova_vigencia_fim,
            "valor_acrescimo": valor_acrescimo,
            "valor_supressao": valor_supressao,
            "percentual_alteracao": None,
            "descricao_alteracao": "Alteração de teste",
            "justificativa": None,
            "observacoes": None,
            "ativo": True,
            "criado_em": agora,
            "atualizado_em": agora,
            "numero_contrato": contrato["numero_contrato"],
            "empresa_nome": contrato["empresa_nome"],
            "valor_original": contrato["valor_original"],
            "vigencia_inicio": contrato["vigencia_inicio"],
            "vigencia_fim": contrato["vigencia_fim"],
        }

    def listar(self, **filtros):
        self.ultimos_filtros = filtros
        busca = filtros.get("busca", "").casefold()
        resultado = []
        for item in self.aditivos.values():
            campos = (
                item["numero_termo"],
                item["numero_contrato"],
                item["empresa_nome"],
                item["tipo_aditivo"],
            )
            if busca and not any(busca in campo.casefold() for campo in campos):
                continue
            if filtros.get("tipo_aditivo") and item["tipo_aditivo"] != filtros["tipo_aditivo"]:
                continue
            status = filtros.get("status_ativo", "ativos")
            if status == "ativos" and not item["ativo"]:
                continue
            if status == "inativos" and item["ativo"]:
                continue
            resultado.append(item)
        return resultado

    def listar_contratos(self):
        return list(self.contratos.values())

    def obter(self, aditivo_id):
        return self.aditivos[aditivo_id]

    def criar(self, dados, usuario_id):
        self.criar_chamadas += 1
        if dados["contrato_id"] not in self.contratos:
            raise ContratoAditivoInvalidoError("O contrato selecionado não existe.")
        if any(
            item["contrato_id"] == dados["contrato_id"]
            and item["numero_termo"] == dados["numero_termo"]
            for item in self.aditivos.values()
        ):
            raise AditivoDuplicadoError(
                "Já existe um aditivo com este número de termo para o contrato."
            )
        aditivo_id = self.proximo_id
        self.proximo_id += 1
        self.aditivos[aditivo_id] = {
            **self._aditivo(
                aditivo_id,
                dados["contrato_id"],
                dados["numero_termo"],
                dados["tipo_aditivo"],
                valor_acrescimo=dados["valor_acrescimo"],
                valor_supressao=dados["valor_supressao"],
                dias_acrescidos=dados["dias_acrescidos"],
                nova_vigencia_fim=dados["nova_vigencia_fim"],
            ),
            **dados,
        }
        return aditivo_id

    def atualizar(self, aditivo_id, dados, usuario_id):
        if dados["contrato_id"] not in self.contratos:
            raise ContratoAditivoInvalidoError("O contrato selecionado não existe.")
        if any(
            item["id"] != aditivo_id
            and item["contrato_id"] == dados["contrato_id"]
            and item["numero_termo"] == dados["numero_termo"]
            for item in self.aditivos.values()
        ):
            raise AditivoDuplicadoError(
                "Já existe um aditivo com este número de termo para o contrato."
            )
        self.aditivos[aditivo_id].update(dados)
        self.aditivos[aditivo_id]["atualizado_em"] = datetime(2026, 7, 14, 11, 0)

    def inativar(self, aditivo_id, usuario_id):
        self.aditivos[aditivo_id]["ativo"] = False
        self.aditivos[aditivo_id]["atualizado_em"] = datetime(2026, 7, 14, 12, 0)

    def reativar(self, aditivo_id, usuario_id):
        self.aditivos[aditivo_id]["ativo"] = True
        self.aditivos[aditivo_id]["atualizado_em"] = datetime(2026, 7, 14, 13, 0)

    def resumo_contrato(self, contrato_id):
        contrato = self.contratos[contrato_id]
        itens = [item for item in self.aditivos.values() if item["contrato_id"] == contrato_id]
        resumo = AditivoService._calcular_resumo(contrato, itens)
        return resumo, itens


class TestFiscalizacaoContratosAditivos(unittest.TestCase):
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
        self.servico = AditivoServiceFake()
        APP_MODULE.login_manager._user_callback = self._carregar_usuario_falso
        self.patcher = patch(
            "modulos.fiscalizacao_contratos.routes.aditivos.AditivoService",
            return_value=self.servico,
        )
        self.patcher.start()
        self.patcher_documentos = patch(
            "modulos.fiscalizacao_contratos.routes.aditivos.DocumentoService"
        )
        self.patcher_documentos.start().return_value.listar_do_aditivo.return_value = []

    def tearDown(self):
        self.patcher_documentos.stop()
        self.patcher.stop()

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
    def dados_validos(numero="TA-002/2026", contrato_id="1", tipo="Acréscimo de valor"):
        return {
            "contrato_id": contrato_id,
            "numero_termo": numero,
            "tipo_aditivo": tipo,
            "data_assinatura": "2026-07-14",
            "data_inicio_efeitos": "2026-07-15",
            "valor_acrescimo": "R$ 5.250,75",
            "valor_supressao": "",
            "percentual_alteracao": "5,25%",
            "descricao_alteracao": "Ampliação do objeto",
            "justificativa": "Necessidade administrativa",
        }

    def test_administrador_acessa_listagem(self):
        self.autenticar_como(1)
        resposta = self.client.get("/fiscalizacao-contratos/aditivos")
        self.assertEqual(resposta.status_code, 200)
        self.assertIn(b"TA-001/2026", resposta.data)

    def test_administrador_cadastra_aditivo(self):
        self.autenticar_como(1)
        resposta = self.client.post("/fiscalizacao-contratos/aditivos/novo", data=self.dados_validos())
        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(self.servico.aditivos[2]["valor_acrescimo"], Decimal("5250.75"))

    def test_administrador_visualiza_e_edita(self):
        self.autenticar_como(1)
        detalhe = self.client.get("/fiscalizacao-contratos/aditivos/1")
        formulario = self.client.get("/fiscalizacao-contratos/aditivos/1/editar")
        dados = self.dados_validos(numero="TA-001/2026")
        dados["descricao_alteracao"] = "Descrição atualizada"
        edicao = self.client.post("/fiscalizacao-contratos/aditivos/1/editar", data=dados)
        self.assertEqual((detalhe.status_code, formulario.status_code, edicao.status_code), (200, 200, 302))
        self.assertEqual(self.servico.aditivos[1]["descricao_alteracao"], "Descrição atualizada")

    def test_contrato_inexistente_e_rejeitado(self):
        self.autenticar_como(1)
        resposta = self.client.post("/fiscalizacao-contratos/aditivos/novo", data=self.dados_validos(contrato_id="999"))
        self.assertEqual(resposta.status_code, 400)
        self.assertIn("contrato selecionado não existe".encode(), resposta.data)

    def test_numero_duplicado_no_mesmo_contrato_e_rejeitado(self):
        self.autenticar_como(1)
        resposta = self.client.post("/fiscalizacao-contratos/aditivos/novo", data=self.dados_validos(numero="TA-001/2026"))
        self.assertEqual(resposta.status_code, 409)
        self.assertIn("Já existe um aditivo".encode(), resposta.data)

    def test_mesmo_numero_em_contratos_diferentes_e_permitido(self):
        self.autenticar_como(1)
        resposta = self.client.post("/fiscalizacao-contratos/aditivos/novo", data=self.dados_validos(numero="TA-001/2026", contrato_id="2"))
        self.assertEqual(resposta.status_code, 302)

    def test_tipo_invalido_e_data_obrigatoria(self):
        for campo, valor, mensagem in (
            ("tipo_aditivo", "Tipo inventado", "tipo de aditivo válido"),
            ("data_assinatura", "", "data de assinatura"),
        ):
            with self.subTest(campo=campo):
                dados = FormularioFake(self.dados_validos())
                dados[campo] = valor
                _, erros = normalizar_e_validar_aditivo(dados)
                self.assertTrue(any(mensagem.casefold() in erro.casefold() for erro in erros))

    def test_valores_e_dias_negativos_sao_rejeitados(self):
        for campo in ("valor_acrescimo", "valor_supressao", "dias_acrescidos"):
            with self.subTest(campo=campo):
                dados = FormularioFake(self.dados_validos())
                dados[campo] = "-1"
                _, erros = normalizar_e_validar_aditivo(dados)
                self.assertTrue(any("negativ" in erro for erro in erros))

    def test_aditivo_de_prazo_exige_prazo(self):
        dados = FormularioFake(self.dados_validos(tipo="Prazo"))
        dados["valor_acrescimo"] = ""
        _, erros = normalizar_e_validar_aditivo(dados)
        self.assertTrue(any("dias acrescidos" in erro for erro in erros))
        dados["dias_acrescidos"] = "0"
        _, erros_com_zero = normalizar_e_validar_aditivo(dados)
        self.assertTrue(any("dias acrescidos" in erro for erro in erros_com_zero))

    def test_aditivo_de_valor_exige_valor(self):
        dados = FormularioFake(self.dados_validos())
        dados["valor_acrescimo"] = ""
        _, erros = normalizar_e_validar_aditivo(dados)
        self.assertTrue(any("acréscimo ou uma supressão" in erro for erro in erros))

    def test_acrescimo_supressao_e_valor_atualizado(self):
        contrato = self.servico.contratos[1]
        itens = [
            self.servico._aditivo(1, 1, "A", "Acréscimo de valor", valor_acrescimo=Decimal("10000")),
            self.servico._aditivo(2, 1, "B", "Supressão de valor", valor_supressao=Decimal("2500")),
        ]
        resumo = AditivoService._calcular_resumo(contrato, itens)
        self.assertEqual(resumo["total_acrescimos"], Decimal("10000"))
        self.assertEqual(resumo["total_supressoes"], Decimal("2500"))
        self.assertEqual(resumo["valor_atualizado"], Decimal("107500"))

    def test_vigencia_atual_considera_aditivos_ativos(self):
        contrato = self.servico.contratos[1]
        itens = [self.servico._aditivo(1, 1, "P", "Prazo", dias_acrescidos=30)]
        resumo = AditivoService._calcular_resumo(contrato, itens)
        self.assertEqual(resumo["vigencia_fim_atual"], date(2027, 1, 30))

    def test_nova_vigencia_anterior_a_atual_e_rejeitada(self):
        contrato = self.servico.contratos[1]
        dados = {"nova_vigencia_fim": date(2026, 12, 1)}
        with self.assertRaises(ContratoAditivoInvalidoError):
            AditivoService._validar_nova_vigencia(dados, contrato, [])

    def test_acrescimo_e_supressao_juntos_exigem_confirmacao_e_justificativa(self):
        dados = FormularioFake(self.dados_validos())
        dados["valor_supressao"] = "1.000,00"
        _, erros = normalizar_e_validar_aditivo(dados)
        self.assertTrue(any("confirmação e justificativa" in erro for erro in erros))
        dados["confirmar_valores_simultaneos"] = "1"
        _, erros_confirmados = normalizar_e_validar_aditivo(dados)
        self.assertFalse(any("confirmação e justificativa" in erro for erro in erros_confirmados))

    def test_inativo_remove_efeito_e_reativacao_restaura(self):
        item = self.servico.aditivos[1]
        contrato = self.servico.contratos[1]
        item["ativo"] = False
        sem_efeito = AditivoService._calcular_resumo(contrato, [item])
        item["ativo"] = True
        com_efeito = AditivoService._calcular_resumo(contrato, [item])
        self.assertEqual(sem_efeito["valor_atualizado"], Decimal("100000"))
        self.assertEqual(com_efeito["valor_atualizado"], Decimal("110000"))

    def test_rotas_inativam_e_reativam_sem_excluir(self):
        self.autenticar_como(1)
        self.client.post("/fiscalizacao-contratos/aditivos/1/inativar")
        self.assertIn(1, self.servico.aditivos)
        self.assertFalse(self.servico.aditivos[1]["ativo"])
        self.client.post("/fiscalizacao-contratos/aditivos/1/reativar")
        self.assertTrue(self.servico.aditivos[1]["ativo"])

    def test_original_do_contrato_nao_e_alterado(self):
        contrato = self.servico.contratos[1]
        valor, fim = contrato["valor_original"], contrato["vigencia_fim"]
        self.servico.resumo_contrato(1)
        self.assertEqual((contrato["valor_original"], contrato["vigencia_fim"]), (valor, fim))

    def test_pesquisa_e_filtros(self):
        self.autenticar_como(1)
        resposta = self.client.get("/fiscalizacao-contratos/aditivos?busca=Empresa&tipo_aditivo=Acr%C3%A9scimo+de+valor&status_ativo=todos")
        self.assertEqual(resposta.status_code, 200)
        self.assertEqual(self.servico.ultimos_filtros["status_ativo"], "todos")

    def test_visitante_e_enviado_ao_login(self):
        resposta = self.client.get("/fiscalizacao-contratos/aditivos")
        self.assertEqual(resposta.status_code, 302)
        self.assertIn("/login", resposta.headers["Location"])

    def test_usuario_comum_recebe_403_em_todas_as_rotas(self):
        self.autenticar_como(2)
        respostas = (
            self.client.get("/fiscalizacao-contratos/aditivos"),
            self.client.get("/fiscalizacao-contratos/aditivos/novo"),
            self.client.get("/fiscalizacao-contratos/aditivos/1"),
            self.client.get("/fiscalizacao-contratos/aditivos/1/editar"),
            self.client.post("/fiscalizacao-contratos/aditivos/1/inativar"),
            self.client.post("/fiscalizacao-contratos/aditivos/1/reativar"),
        )
        self.assertTrue(all(resposta.status_code == 403 for resposta in respostas))

    def test_rotas_antigas_continuam_registradas(self):
        rotas = {regra.rule for regra in self.flask_app.url_map.iter_rules()}
        for rota in ("/", "/login", "/fiscalizacao-contratos/empresas", "/fiscalizacao-contratos/servidores", "/fiscalizacao-contratos/contratos"):
            self.assertIn(rota, rotas)

    def test_nenhum_banco_real_e_acessado(self):
        chamadas = MOCK_CONNECT.call_count
        self.autenticar_como(1)
        self.client.get("/fiscalizacao-contratos/aditivos")
        self.assertEqual(MOCK_CONNECT.call_count, chamadas)

    def test_migracao_e_idempotente_aditiva_e_nao_automatica(self):
        caminho = os.path.join(os.path.dirname(os.path.dirname(__file__)), "modulos", "fiscalizacao_contratos", "migrations", "004_criar_fc_aditivos.sql")
        with open(caminho, encoding="utf-8") as arquivo:
            sql = arquivo.read().upper()
        self.assertIn("CREATE TABLE IF NOT EXISTS FC_ADITIVOS", sql)
        self.assertIn("CREATE INDEX IF NOT EXISTS", sql)
        for comando in ("DROP", "TRUNCATE", "DELETE", "UPDATE", "INSERT", "ALTER TABLE"):
            self.assertNotIn(comando, sql)

    def test_servico_nao_contem_delete_e_atualiza_timestamp(self):
        caminho = os.path.join(os.path.dirname(os.path.dirname(__file__)), "modulos", "fiscalizacao_contratos", "services", "aditivos_service.py")
        with open(caminho, encoding="utf-8") as arquivo:
            codigo = arquivo.read().upper()
        self.assertNotIn("DELETE FROM", codigo)
        self.assertIn("ATUALIZADO_EM = CURRENT_TIMESTAMP", codigo)


def tearDownModule():
    PATCH_CONEXAO.stop()


if __name__ == "__main__":
    unittest.main()
