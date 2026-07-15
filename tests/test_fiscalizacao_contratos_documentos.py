"""Testes da Etapa 2E sem banco ou Cloudinary reais."""

import hashlib
import importlib
import io
import os
import sys
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import psycopg2
from werkzeug.datastructures import FileStorage

from modulos.fiscalizacao_contratos.services.cloudinary_storage import (
    CloudinaryStorage,
    CloudinaryStorageError,
)
from modulos.fiscalizacao_contratos.services.documentos_service import (
    DocumentoNaoEncontradoError,
    DocumentoReferenciaInvalidaError,
    DocumentoService,
    DocumentoServiceError,
)
from modulos.fiscalizacao_contratos.validacoes_documentos import (
    ValidacaoDocumentoError,
    validar_arquivo_documento,
)


CONEXAO_FALSA = MagicMock(name="conexao_falsa_documentos")
CURSOR_FALSO = CONEXAO_FALSA.cursor.return_value
CURSOR_FALSO.fetchall.return_value = []
CURSOR_FALSO.fetchone.return_value = None
PATCH_CONEXAO = patch("psycopg2.connect", return_value=CONEXAO_FALSA)
MOCK_CONNECT = PATCH_CONEXAO.start()

sys.modules.pop("app", None)
with (
    patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False),
    patch("dotenv.load_dotenv", return_value=False),
):
    APP_MODULE = importlib.import_module("app")

MOCK_CONNECT.side_effect = AssertionError("Nenhuma conexão real é permitida nos testes")


PDF_VALIDO = b"%PDF-1.4\n1 0 obj\n<<>>\nendobj\n%%EOF"


class ArmazenamentoFake:
    def __init__(self):
        self.enviados = []
        self.removidos = []
        self.urls = []
        self.falhar_envio = False

    def enviar(self, arquivo, contrato_id, aditivo_id=None):
        if self.falhar_envio:
            raise CloudinaryStorageError("falha simulada")
        chave = f"privado/{contrato_id}/{aditivo_id or 0}/uuid-{len(self.enviados) + 1}.{arquivo['extensao']}"
        self.enviados.append(chave)
        return {
            "armazenamento_provedor": "cloudinary",
            "armazenamento_chave": chave,
            "armazenamento_versao": 123,
        }

    def remover(self, chave):
        self.removidos.append(chave)

    def gerar_url_temporaria(self, chave, extensao, *, download=False):
        self.urls.append((chave, extensao, download))
        return "https://temporaria.exemplo/documento"


class DocumentoServiceFake:
    def __init__(self, armazenamento):
        self.armazenamento = armazenamento
        self.contratos = [
            {"id": 1, "numero_contrato": "CT-001/2026", "empresa_nome": "Empresa Um", "ativo": True},
            {"id": 2, "numero_contrato": "CT-002/2026", "empresa_nome": "Empresa Dois", "ativo": True},
        ]
        self.aditivos = [
            {"id": 1, "contrato_id": 1, "numero_termo": "TA-001", "numero_contrato": "CT-001/2026", "ativo": True},
            {"id": 2, "contrato_id": 2, "numero_termo": "TA-002", "numero_contrato": "CT-002/2026", "ativo": True},
        ]
        self.documentos = {}
        self.proximo_id = 1
        self.ultimos_filtros = None
        self.criar_chamadas = 0

    def listar_opcoes(self):
        return self.contratos, self.aditivos

    def listar(self, **filtros):
        self.ultimos_filtros = filtros
        resultado = list(self.documentos.values())
        status = filtros.get("status_ativo", "ativos")
        if status == "ativos":
            resultado = [item for item in resultado if item["ativo"]]
        elif status == "inativos":
            resultado = [item for item in resultado if not item["ativo"]]
        busca = filtros.get("busca", "").casefold()
        if busca:
            resultado = [item for item in resultado if busca in " ".join((item["titulo"], item["nome_original"], item["numero_contrato"], item["empresa_nome"], item["categoria"])).casefold()]
        if filtros.get("categoria"):
            resultado = [item for item in resultado if item["categoria"] == filtros["categoria"]]
        if filtros.get("contrato_id"):
            resultado = [item for item in resultado if item["contrato_id"] == filtros["contrato_id"]]
        return resultado

    def listar_do_contrato(self, contrato_id):
        return [item for item in self.documentos.values() if item["contrato_id"] == contrato_id]

    def listar_do_aditivo(self, aditivo_id):
        return [item for item in self.documentos.values() if item["aditivo_id"] == aditivo_id]

    def obter(self, documento_id):
        if documento_id not in self.documentos:
            raise DocumentoNaoEncontradoError("Documento não encontrado.")
        return self.documentos[documento_id]

    def criar(self, dados, arquivo, usuario_id, armazenamento):
        self.criar_chamadas += 1
        if dados.get("aditivo_id"):
            aditivo = next((item for item in self.aditivos if item["id"] == dados["aditivo_id"]), None)
            if not aditivo or aditivo["contrato_id"] != dados["contrato_id"]:
                raise DocumentoReferenciaInvalidaError(
                    "O aditivo selecionado não pertence ao contrato informado."
                )
        enviado = armazenamento.enviar(arquivo, dados["contrato_id"], dados.get("aditivo_id"))
        documento_id = self.proximo_id
        self.proximo_id += 1
        contrato = next(item for item in self.contratos if item["id"] == dados["contrato_id"])
        aditivo = next((item for item in self.aditivos if item["id"] == dados.get("aditivo_id")), None)
        self.documentos[documento_id] = {
            "id": documento_id,
            **dados,
            **{chave: valor for chave, valor in arquivo.items() if chave != "conteudo"},
            **enviado,
            "numero_contrato": contrato["numero_contrato"],
            "empresa_nome": contrato["empresa_nome"],
            "aditivo_numero": aditivo["numero_termo"] if aditivo else None,
            "ativo": True,
            "criado_em": datetime(2026, 7, 15, 10, 0),
        }
        return documento_id

    def inativar(self, documento_id, usuario_id):
        self.documentos[documento_id]["ativo"] = False

    def reativar(self, documento_id, usuario_id):
        self.documentos[documento_id]["ativo"] = True


class TestFiscalizacaoContratosDocumentos(unittest.TestCase):
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
        self.armazenamento = ArmazenamentoFake()
        self.servico = DocumentoServiceFake(self.armazenamento)
        APP_MODULE.login_manager._user_callback = self._carregar_usuario_falso
        self.patcher_servico = patch(
            "modulos.fiscalizacao_contratos.routes.documentos.DocumentoService",
            return_value=self.servico,
        )
        self.patcher_storage = patch(
            "modulos.fiscalizacao_contratos.routes.documentos.CloudinaryStorage",
            return_value=self.armazenamento,
        )
        self.mock_storage = self.patcher_storage.start()
        self.patcher_servico.start()

    def tearDown(self):
        self.patcher_servico.stop()
        self.patcher_storage.stop()

    @staticmethod
    def _carregar_usuario_falso(user_id):
        perfis = {
            "1": APP_MODULE.User(1, "administrador", "admin", None),
            "2": APP_MODULE.User(2, "usuario", "usuario", "UVR 01"),
        }
        return perfis.get(str(user_id))

    def autenticar_como(self, user_id):
        with self.client.session_transaction() as sessao:
            sessao["_user_id"] = str(user_id)
            sessao["_fresh"] = True

    @staticmethod
    def dados_upload(contrato_id="1", aditivo_id=""):
        return {
            "contrato_id": contrato_id,
            "aditivo_id": aditivo_id,
            "categoria": "Contrato" if not aditivo_id else "Aditivo",
            "titulo": "Documento de teste",
            "descricao": "Descrição segura",
            "arquivo": (io.BytesIO(PDF_VALIDO), "documento.pdf"),
        }

    def cadastrar_documento(self, **kwargs):
        self.autenticar_como(1)
        return self.client.post(
            "/fiscalizacao-contratos/documentos/novo",
            data=self.dados_upload(**kwargs),
            content_type="multipart/form-data",
        )

    def test_administrador_acessa_listagem(self):
        self.autenticar_como(1)
        self.assertEqual(self.client.get("/fiscalizacao-contratos/documentos").status_code, 200)

    def test_administrador_anexa_documento_ao_contrato(self):
        resposta = self.cadastrar_documento()
        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(self.servico.documentos[1]["contrato_id"], 1)
        self.assertIsNone(self.servico.documentos[1]["aditivo_id"])

    def test_administrador_anexa_documento_ao_aditivo(self):
        resposta = self.cadastrar_documento(aditivo_id="1")
        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(self.servico.documentos[1]["aditivo_id"], 1)

    def test_aditivo_de_outro_contrato_e_rejeitado(self):
        resposta = self.cadastrar_documento(contrato_id="1", aditivo_id="2")
        self.assertEqual(resposta.status_code, 400)
        self.assertFalse(self.servico.documentos)

    def test_arquivo_vazio_e_rejeitado(self):
        arquivo = FileStorage(stream=io.BytesIO(b""), filename="vazio.pdf")
        with self.assertRaises(ValidacaoDocumentoError):
            validar_arquivo_documento(arquivo)

    def test_extensao_proibida_e_rejeitada(self):
        arquivo = FileStorage(stream=io.BytesIO(b"executavel"), filename="programa.exe")
        with self.assertRaises(ValidacaoDocumentoError):
            validar_arquivo_documento(arquivo)

    def test_tamanho_excessivo_e_rejeitado(self):
        arquivo = FileStorage(stream=io.BytesIO(PDF_VALIDO), filename="grande.pdf")
        with self.assertRaises(ValidacaoDocumentoError):
            validar_arquivo_documento(arquivo, limite_bytes=5)

    def test_nome_malicioso_e_neutralizado(self):
        arquivo = FileStorage(stream=io.BytesIO(PDF_VALIDO), filename="../../arquivo.pdf")
        validado = validar_arquivo_documento(arquivo)
        self.assertEqual(validado["nome_original"], "arquivo.pdf")
        self.assertNotIn("..", validado["nome_original"])

    def test_conteudo_nao_correspondente_e_rejeitado(self):
        arquivo = FileStorage(stream=io.BytesIO(b"isto nao e pdf"), filename="falso.pdf")
        with self.assertRaises(ValidacaoDocumentoError):
            validar_arquivo_documento(arquivo)

    def test_sha256_e_metadados_sao_calculados(self):
        arquivo = FileStorage(stream=io.BytesIO(PDF_VALIDO), filename="arquivo.pdf", content_type="text/plain")
        validado = validar_arquivo_documento(arquivo)
        self.assertEqual(validado["sha256"], hashlib.sha256(PDF_VALIDO).hexdigest())
        self.assertEqual(validado["mime_type"], "application/pdf")
        self.assertEqual(validado["tamanho_bytes"], len(PDF_VALIDO))
        self.assertEqual(arquivo.stream.tell(), 0)

    def test_nomes_iguais_recebem_chaves_diferentes(self):
        dados = {"conteudo": PDF_VALIDO, "extensao": "pdf"}
        with (
            patch.dict(os.environ, {"CLOUDINARY_CLOUD_NAME": "x", "CLOUDINARY_API_KEY": "y", "CLOUDINARY_API_SECRET": "z"}),
            patch("cloudinary.uploader.upload", side_effect=lambda _arquivo, **opcoes: {"public_id": opcoes["public_id"], "version": 1}) as upload,
        ):
            storage = CloudinaryStorage()
            primeiro = storage.enviar(dados, 1)
            segundo = storage.enviar(dados, 1)
        self.assertNotEqual(primeiro["armazenamento_chave"], segundo["armazenamento_chave"])
        opcoes = upload.call_args.kwargs
        self.assertEqual(opcoes["resource_type"], "raw")
        self.assertEqual(opcoes["type"], "authenticated")
        self.assertFalse(opcoes["overwrite"])

    def test_url_cloudinary_e_assinada_por_cinco_minutos(self):
        with (
            patch.dict(os.environ, {"CLOUDINARY_CLOUD_NAME": "x", "CLOUDINARY_API_KEY": "y", "CLOUDINARY_API_SECRET": "z"}),
            patch("time.time", return_value=1000),
            patch("cloudinary.utils.private_download_url", return_value="https://temporaria") as gerar,
        ):
            url = CloudinaryStorage().gerar_url_temporaria("chave.pdf", "pdf", download=True)
        self.assertEqual(url, "https://temporaria")
        self.assertEqual(gerar.call_args.kwargs["expires_at"], 1300)
        self.assertEqual(gerar.call_args.kwargs["resource_type"], "raw")
        self.assertEqual(gerar.call_args.kwargs["type"], "authenticated")

    def test_falha_no_armazenamento_nao_cria_registro(self):
        self.armazenamento.falhar_envio = True
        resposta = self.cadastrar_documento()
        self.assertEqual(resposta.status_code, 502)
        self.assertFalse(self.servico.documentos)

    def test_falha_no_banco_remove_arquivo_enviado(self):
        conexao = MagicMock()
        cursor = conexao.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = (1,)
        cursor.execute.side_effect = [None, psycopg2.OperationalError("falha simulada")]
        servico = DocumentoService(lambda: conexao)
        storage = ArmazenamentoFake()
        dados = {"contrato_id": 1, "aditivo_id": None, "categoria": "Contrato", "titulo": "Teste", "descricao": None}
        arquivo = {"nome_original": "a.pdf", "extensao": "pdf", "mime_type": "application/pdf", "tamanho_bytes": len(PDF_VALIDO), "sha256": hashlib.sha256(PDF_VALIDO).hexdigest(), "conteudo": PDF_VALIDO}
        with self.assertRaises(DocumentoServiceError):
            servico.criar(dados, arquivo, 1, storage)
        conexao.rollback.assert_called_once()
        self.assertEqual(storage.removidos, storage.enviados)

        class ArmazenamentoComFalhaNaLimpeza(ArmazenamentoFake):
            def remover(self, chave):
                raise CloudinaryStorageError("falha de limpeza simulada")

        conexao_2 = MagicMock()
        cursor_2 = conexao_2.cursor.return_value.__enter__.return_value
        cursor_2.fetchone.return_value = (1,)
        cursor_2.execute.side_effect = [None, psycopg2.OperationalError("falha principal")]
        with self.assertLogs(
            "modulos.fiscalizacao_contratos.services.documentos_service",
            level="WARNING",
        ) as logs:
            with self.assertRaises(DocumentoServiceError) as erro_principal:
                DocumentoService(lambda: conexao_2).criar(
                    dados, arquivo, 1, ArmazenamentoComFalhaNaLimpeza()
                )
        self.assertIsInstance(erro_principal.exception.__cause__, psycopg2.OperationalError)
        self.assertIn("CloudinaryStorageError", " ".join(logs.output))

    def test_download_exige_administrador_e_url_so_surje_depois(self):
        self.cadastrar_documento()
        self.mock_storage.reset_mock()
        self.client.get("/logout")
        visitante = self.client.get("/fiscalizacao-contratos/documentos/1/arquivo")
        self.assertEqual(visitante.status_code, 302)
        self.assertIn("/login", visitante.headers["Location"])
        self.mock_storage.assert_not_called()
        self.autenticar_como(2)
        comum = self.client.get("/fiscalizacao-contratos/documentos/1/arquivo")
        self.assertEqual(comum.status_code, 403)
        self.mock_storage.assert_not_called()
        self.autenticar_como(1)
        admin = self.client.get("/fiscalizacao-contratos/documentos/1/arquivo?download=1")
        self.assertEqual(admin.status_code, 302)
        self.assertEqual(admin.headers["Location"], "https://temporaria.exemplo/documento")

    def test_documento_inexistente_tem_mensagem_amigavel(self):
        self.autenticar_como(1)
        resposta = self.client.get("/fiscalizacao-contratos/documentos/999")
        self.assertEqual(resposta.status_code, 302)

    def test_inativacao_nao_remove_arquivo_e_reativacao_funciona(self):
        self.cadastrar_documento()
        self.client.post("/fiscalizacao-contratos/documentos/1/inativar")
        self.assertFalse(self.servico.documentos[1]["ativo"])
        self.assertFalse(self.armazenamento.removidos)
        self.client.post("/fiscalizacao-contratos/documentos/1/reativar")
        self.assertTrue(self.servico.documentos[1]["ativo"])

    def test_inativos_ficam_ocultos_por_padrao(self):
        self.cadastrar_documento()
        self.servico.documentos[1]["ativo"] = False
        self.autenticar_como(1)
        resposta = self.client.get("/fiscalizacao-contratos/documentos")
        self.assertNotIn(b"Documento de teste", resposta.data)

    def test_pesquisa_e_filtros_funcionam(self):
        self.cadastrar_documento()
        resposta = self.client.get("/fiscalizacao-contratos/documentos?busca=teste&categoria=Contrato&contrato_id=1&status_ativo=todos")
        self.assertEqual(resposta.status_code, 200)
        self.assertIn(b"Documento de teste", resposta.data)
        self.assertEqual(self.servico.ultimos_filtros["contrato_id"], 1)

    def test_documentos_aparecem_nos_detalhes(self):
        self.cadastrar_documento(aditivo_id="1")
        self.autenticar_como(1)
        contrato = {"id": 1, "numero_contrato": "CT-001/2026", "ativo": True, "situacao": "Vigente", "vence_em_60_dias": False}
        aditivo = self.servico.documentos[1]
        aditivo.update({"numero_termo": "TA-001", "tipo_aditivo": "Aditivo", "data_assinatura": None})
        with (
            patch("modulos.fiscalizacao_contratos.routes.contratos.ContratoService") as contrato_cls,
            patch("modulos.fiscalizacao_contratos.routes.contratos.AditivoService") as aditivo_resumo_cls,
            patch("modulos.fiscalizacao_contratos.routes.contratos.DocumentoService", return_value=self.servico),
            patch("modulos.fiscalizacao_contratos.routes.aditivos.AditivoService") as aditivo_cls,
            patch("modulos.fiscalizacao_contratos.routes.aditivos.DocumentoService", return_value=self.servico),
        ):
            contrato_cls.return_value.obter.return_value = (contrato, [])
            aditivo_resumo_cls.return_value.resumo_contrato.return_value = (None, [])
            aditivo_cls.return_value.obter.return_value = aditivo
            detalhe_contrato = self.client.get("/fiscalizacao-contratos/contratos/1")
            detalhe_aditivo = self.client.get("/fiscalizacao-contratos/aditivos/1")
        self.assertIn(b"Documento de teste", detalhe_contrato.data)
        self.assertIn(b"Documento de teste", detalhe_aditivo.data)

    def test_usuario_comum_recebe_403(self):
        self.autenticar_como(2)
        respostas = (
            self.client.get("/fiscalizacao-contratos/documentos"),
            self.client.get("/fiscalizacao-contratos/documentos/novo"),
            self.client.get("/fiscalizacao-contratos/documentos/1"),
            self.client.get("/fiscalizacao-contratos/documentos/1/arquivo"),
            self.client.post("/fiscalizacao-contratos/documentos/1/inativar"),
            self.client.post("/fiscalizacao-contratos/documentos/1/reativar"),
        )
        self.assertTrue(all(resposta.status_code == 403 for resposta in respostas))

    def test_visitante_e_encaminhado_ao_login(self):
        resposta = self.client.get("/fiscalizacao-contratos/documentos")
        self.assertEqual(resposta.status_code, 302)
        self.assertIn("/login", resposta.headers["Location"])

    def test_rotas_antigas_e_novo_cartao_continuam_funcionando(self):
        rotas = {regra.rule for regra in self.flask_app.url_map.iter_rules()}
        for rota in ("/", "/login", "/fiscalizacao-contratos/contratos", "/fiscalizacao-contratos/aditivos", "/fiscalizacao-contratos/documentos"):
            self.assertIn(rota, rotas)
        self.autenticar_como(1)
        painel = self.client.get("/fiscalizacao-contratos")
        self.assertIn(b"Documentos", painel.data)

    def test_migracao_e_aditiva_e_nao_automatica(self):
        caminho = os.path.join(os.path.dirname(os.path.dirname(__file__)), "modulos", "fiscalizacao_contratos", "migrations", "005_criar_fc_documentos.sql")
        with open(caminho, encoding="utf-8") as arquivo:
            sql = arquivo.read().upper()
        self.assertIn("CREATE TABLE IF NOT EXISTS FC_DOCUMENTOS", sql)
        self.assertIn("CREATE UNIQUE INDEX IF NOT EXISTS", sql)
        for comando in ("DROP", "TRUNCATE", "DELETE", "UPDATE", "INSERT", "ALTER TABLE"):
            self.assertNotIn(comando, sql)

    def test_nenhum_banco_ou_cloudinary_real_e_acessado(self):
        chamadas = MOCK_CONNECT.call_count
        self.autenticar_como(1)
        self.client.get("/fiscalizacao-contratos/documentos")
        self.assertEqual(MOCK_CONNECT.call_count, chamadas)


def tearDownModule():
    PATCH_CONEXAO.stop()


if __name__ == "__main__":
    unittest.main()
