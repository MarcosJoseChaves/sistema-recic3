"""Testes seguros das medições, sem PostgreSQL, Cloudinary ou arquivos reais."""

import ast
import importlib
import io
import os
import sys
import unittest
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock, patch

import psycopg2

from modulos.fiscalizacao_contratos.services.medicoes_service import (
    MedicaoBloqueadaError,
    MedicaoDuplicadaError,
    MedicaoService,
    MedicaoServiceError,
    ReferenciaMedicaoInvalidaError,
)
from modulos.fiscalizacao_contratos.validacoes_medicoes import (
    calcular_totais,
    calcular_valor_item,
    normalizar_e_validar_ajuste,
    normalizar_e_validar_item,
    normalizar_e_validar_medicao,
)


RAIZ = Path(__file__).resolve().parents[1]
CONEXAO_FALSA = MagicMock(name="conexao_falsa_medicoes")
PATCH_CONEXAO = patch("psycopg2.connect", return_value=CONEXAO_FALSA)
MOCK_CONNECT = PATCH_CONEXAO.start()
sys.modules.pop("app", None)
with patch.dict(os.environ, {"DATABASE_URL": ""}, clear=False), patch(
    "dotenv.load_dotenv", return_value=False
):
    APP_MODULE = importlib.import_module("app")
MOCK_CONNECT.reset_mock()
MOCK_CONNECT.side_effect = AssertionError("PostgreSQL real bloqueado nos testes")


class ArmazenamentoMedicaoFake:
    def __init__(self):
        self.enviados = []
        self.removidos = []

    def enviar(self, arquivo, contrato_id, aditivo_id=None):
        self.enviados.append((arquivo, contrato_id, aditivo_id))
        return {
            "armazenamento_provedor": "cloudinary",
            "armazenamento_chave": f"medicoes/{contrato_id}/arquivo-unico",
            "armazenamento_versao": 1,
        }

    def remover(self, chave):
        self.removidos.append(chave)


class MedicaoServiceFake:
    def __init__(self):
        self.contratos = {
            1: {"id": 1, "numero_contrato": "CT-001/2026", "empresa_id": 1, "empresa_nome": "Empresa Um", "ativo": True},
            2: {"id": 2, "numero_contrato": "CT-002/2026", "empresa_id": 2, "empresa_nome": "Empresa Dois", "ativo": False},
        }
        self.servidores = {
            1: {"id": 1, "nome": "Fiscal Ativo", "matricula": "F-1", "ativo": True},
            2: {"id": 2, "nome": "Fiscal Inativo", "matricula": "F-2", "ativo": False},
            3: {"id": 3, "nome": "Aprovador Ativo", "matricula": "A-1", "ativo": True},
        }
        self.empresas = [{"id": 1, "razao_social": "Empresa Um"}, {"id": 2, "razao_social": "Empresa Dois"}]
        self.planilha = {
            10: {"id": 10, "contrato_id": 1, "codigo_item": "P-10", "descricao": "Serviço previsto", "unidade": "m²", "quantidade": Decimal("10"), "valor_unitario": Decimal("5.25"), "planilha_nome": "Original", "versao": 1},
            20: {"id": 20, "contrato_id": 2, "codigo_item": "P-20", "descricao": "Outro contrato", "unidade": "un", "quantidade": Decimal("1"), "valor_unitario": Decimal("9"), "planilha_nome": "Outra", "versao": 1},
        }
        self.fiscalizacoes = [{"id": 1, "contrato_id": 1, "data_fiscalizacao": date(2026, 7, 1), "objeto_verificado": "Execução"}, {"id": 2, "contrato_id": 2, "data_fiscalizacao": date(2026, 7, 2), "objeto_verificado": "Outro"}]
        self.ocorrencias = [{"id": 1, "contrato_id": 1, "titulo": "Falha", "status": "Aberta"}, {"id": 2, "contrato_id": 2, "titulo": "Outra", "status": "Aberta"}]
        self.documentos = [{"id": 1, "contrato_id": 1, "titulo": "Relatório", "nome_original": "relatorio.pdf", "categoria": "Relatório"}, {"id": 2, "contrato_id": 2, "titulo": "Outro", "nome_original": "outro.pdf", "categoria": "Outro"}]
        self.medicoes = {1: self._medicao(1)}
        self.itens = {1: []}
        self.ajustes = {1: []}
        self.vinculos = {1: []}
        self.eventos = {1: [self._evento(1, "Criação", None, "Em elaboração")]}
        self.proxima_medicao = 2
        self.proximo_registro = 1
        self.ultimo_filtro = None
        self.rollback_simulado = False

    def _medicao(self, identificador, **extra):
        base = {
            "id": identificador, "contrato_id": 1, "numero_medicao": 1,
            "competencia": date(2026, 7, 1), "periodo_inicio": date(2026, 7, 1),
            "periodo_fim": date(2026, 7, 31), "versao": 1, "medicao_origem_id": None,
            "atual": True, "servidor_fiscal_id": 1, "data_apresentacao": None,
            "status": "Em elaboração", "valor_bruto": Decimal("0.00"),
            "total_acrescimos": Decimal("0.00"), "total_descontos": Decimal("0.00"),
            "total_glosas": Decimal("0.00"), "valor_liquido": Decimal("0.00"),
            "observacoes": "Medição inicial", "aprovado_em": None,
            "servidor_aprovador_id": None, "ativo": True,
            "criado_em": datetime(2026, 7, 1, 10), "atualizado_em": datetime(2026, 7, 1, 10),
            "numero_contrato": "CT-001/2026", "processo_administrativo": "PA-1",
            "empresa_nome": "Empresa Um", "fiscal_nome": "Fiscal Ativo", "aprovador_nome": None,
        }
        base.update(extra)
        return base

    @staticmethod
    def _evento(identificador, tipo, anterior, novo, justificativa=None):
        return {"id": identificador, "tipo_evento": tipo, "status_anterior": anterior,
                "status_novo": novo, "justificativa": justificativa,
                "valor_bruto": Decimal("0"), "total_acrescimos": Decimal("0"),
                "total_descontos": Decimal("0"), "total_glosas": Decimal("0"),
                "valor_liquido": Decimal("0"), "criado_em": datetime(2026, 7, 1, 10),
                "usuario_nome": "admin"}

    def opcoes(self):
        return list(self.contratos.values()), [s for s in self.servidores.values() if s["ativo"]], self.empresas

    def opcoes_relacionamentos(self, contrato_id):
        return ([x for x in self.planilha.values() if x["contrato_id"] == contrato_id],
                [x for x in self.fiscalizacoes if x["contrato_id"] == contrato_id],
                [x for x in self.ocorrencias if x["contrato_id"] == contrato_id],
                [x for x in self.documentos if x["contrato_id"] == contrato_id])

    def listar(self, busca="", filtros=None):
        self.ultimo_filtro = (busca, filtros or {})
        return list(self.medicoes.values())

    def obter(self, identificador):
        m = self.medicoes[identificador]
        versoes = [x for x in self.medicoes.values() if x["contrato_id"] == m["contrato_id"] and x["numero_medicao"] == m["numero_medicao"]]
        return m, self.itens[identificador], self.ajustes[identificador], self.vinculos[identificador], list(reversed(self.eventos[identificador])), sorted(versoes, key=lambda x: x["versao"], reverse=True)

    def _refs(self, dados):
        contrato = self.contratos.get(dados["contrato_id"])
        fiscal = self.servidores.get(dados["servidor_fiscal_id"])
        if not contrato:
            raise ReferenciaMedicaoInvalidaError("Contrato não encontrado.")
        if not contrato["ativo"]:
            raise ReferenciaMedicaoInvalidaError("O contrato está inativo.")
        if not fiscal:
            raise ReferenciaMedicaoInvalidaError("Fiscal não encontrado.")
        if not fiscal["ativo"]:
            raise ReferenciaMedicaoInvalidaError("O fiscal está inativo.")

    def criar(self, dados, usuario_id):
        self._refs(dados)
        if any(x["contrato_id"] == dados["contrato_id"] and x["competencia"] == dados["competencia"] and x["atual"] and x["ativo"] for x in self.medicoes.values()):
            raise MedicaoDuplicadaError("Já existe medição atual nesta competência.")
        identificador = self.proxima_medicao
        self.proxima_medicao += 1
        self.medicoes[identificador] = self._medicao(identificador, **dados)
        self.itens[identificador], self.ajustes[identificador], self.vinculos[identificador] = [], [], []
        self.eventos[identificador] = [self._evento(1, "Criação", None, "Em elaboração")]
        return identificador

    def _editavel(self, identificador):
        m = self.medicoes[identificador]
        if not m["ativo"] or not m["atual"] or m["status"] not in ("Em elaboração", "Devolvida para correção"):
            raise MedicaoBloqueadaError("Versão bloqueada.")
        return m

    def atualizar(self, identificador, dados, usuario_id):
        self._editavel(identificador)
        self._refs(dados)
        self.medicoes[identificador].update(dados)

    def _recalcular(self, identificador):
        m = self.medicoes[identificador]
        bruto, acrescimos, descontos, glosas, liquido = calcular_totais(self.itens[identificador], self.ajustes[identificador])
        m.update(valor_bruto=bruto, total_acrescimos=acrescimos, total_descontos=descontos, total_glosas=glosas, valor_liquido=liquido)

    def criar_item(self, identificador, dados, usuario_id):
        self._editavel(identificador)
        dados = deepcopy(dados)
        if dados.get("planilha_item_id"):
            origem = self.planilha.get(dados["planilha_item_id"])
            if not origem or origem["contrato_id"] != self.medicoes[identificador]["contrato_id"]:
                raise ReferenciaMedicaoInvalidaError("Item de outro contrato.")
            if any(x.get("planilha_item_id") == origem["id"] and x["ativo"] for x in self.itens[identificador]):
                raise MedicaoDuplicadaError("Item duplicado.")
            dados.update(codigo_item=origem["codigo_item"], descricao=origem["descricao"], unidade=origem["unidade"], quantidade_prevista=origem["quantidade"], preco_unitario=origem["valor_unitario"])
        if dados.get("quantidade_prevista") is not None and dados["quantidade_medida"] > dados["quantidade_prevista"] and not dados.get("justificativa_excedente"):
            raise ReferenciaMedicaoInvalidaError("Justifique o excesso.")
        item = {"id": self.proximo_registro, "ativo": True, **dados}
        self.proximo_registro += 1
        item["valor_medido"] = calcular_valor_item(item["quantidade_medida"], item["preco_unitario"])
        self.itens[identificador].append(item)
        self._recalcular(identificador)
        return item["valor_medido"]

    def atualizar_item(self, identificador, item_id, dados, usuario_id):
        self._editavel(identificador)
        item = next(x for x in self.itens[identificador] if x["id"] == item_id)
        item.update(dados)
        item["valor_medido"] = calcular_valor_item(item["quantidade_medida"], item["preco_unitario"])
        self._recalcular(identificador)

    def inativar_item(self, identificador, item_id, usuario_id):
        self._editavel(identificador)
        next(x for x in self.itens[identificador] if x["id"] == item_id)["ativo"] = False
        self._recalcular(identificador)

    def criar_ajuste(self, identificador, dados, usuario_id):
        self._editavel(identificador)
        for campo, origem in (("fiscalizacao_id", self.fiscalizacoes), ("ocorrencia_id", self.ocorrencias)):
            if dados.get(campo) and not any(x["id"] == dados[campo] and x["contrato_id"] == self.medicoes[identificador]["contrato_id"] for x in origem):
                raise ReferenciaMedicaoInvalidaError("Referência de outro contrato.")
        ajuste = {"id": self.proximo_registro, "ativo": True, **dados}
        self.proximo_registro += 1
        self.ajustes[identificador].append(ajuste)
        try:
            self._recalcular(identificador)
        except ValueError:
            self.ajustes[identificador].pop()
            raise MedicaoBloqueadaError("Valor líquido negativo.")

    def atualizar_ajuste(self, identificador, ajuste_id, dados, usuario_id):
        self._editavel(identificador)
        next(x for x in self.ajustes[identificador] if x["id"] == ajuste_id).update(dados)
        self._recalcular(identificador)

    def inativar_ajuste(self, identificador, ajuste_id, usuario_id):
        self._editavel(identificador)
        next(x for x in self.ajustes[identificador] if x["id"] == ajuste_id)["ativo"] = False
        self._recalcular(identificador)

    def vincular_documento(self, identificador, documento_id, categoria, observacoes, usuario_id):
        self._editavel(identificador)
        if not any(x["id"] == documento_id and x["contrato_id"] == self.medicoes[identificador]["contrato_id"] for x in self.documentos):
            raise ReferenciaMedicaoInvalidaError("Documento de outro contrato.")
        if any(x["documento_id"] == documento_id and x["ativo"] for x in self.vinculos[identificador]):
            raise MedicaoDuplicadaError("Documento duplicado.")
        documento = next(x for x in self.documentos if x["id"] == documento_id)
        self.vinculos[identificador].append({"id": self.proximo_registro, "documento_id": documento_id, "categoria": categoria, "observacoes": observacoes, "ativo": True, **{k: documento[k] for k in ("titulo", "nome_original")}})
        self.proximo_registro += 1

    def enviar_documento(self, identificador, dados, arquivo, categoria, observacoes, usuario_id, armazenamento):
        medicao = self._editavel(identificador)
        armazenamento.enviar(arquivo, medicao["contrato_id"], None)
        documento_id = max((x["id"] for x in self.documentos), default=0) + 1
        documento = {
            "id": documento_id,
            "contrato_id": medicao["contrato_id"],
            "titulo": dados["titulo"],
            "nome_original": arquivo["nome_original"],
            "categoria": dados["categoria"],
        }
        self.documentos.append(documento)
        self.vinculos[identificador].append({
            "id": self.proximo_registro,
            "documento_id": documento_id,
            "categoria": categoria,
            "observacoes": observacoes,
            "ativo": True,
            "titulo": documento["titulo"],
            "nome_original": documento["nome_original"],
        })
        self.proximo_registro += 1
        return documento_id

    def inativar_documento(self, identificador, vinculo_id, usuario_id):
        self._editavel(identificador)
        next(x for x in self.vinculos[identificador] if x["id"] == vinculo_id)["ativo"] = False

    def enviar_analise(self, identificador, usuario_id):
        m = self._editavel(identificador)
        if not any(x["ativo"] for x in self.itens[identificador]):
            raise MedicaoBloqueadaError("Inclua item.")
        if not self.servidores[m["servidor_fiscal_id"]]["ativo"]:
            raise MedicaoBloqueadaError("Fiscal inativo.")
        self._recalcular(identificador)
        m["status"] = "Em análise"
        self.eventos[identificador].append(self._evento(2, "Envio para análise", "Em elaboração", "Em análise"))

    def devolver_correcao(self, identificador, justificativa, usuario_id):
        m = self.medicoes[identificador]
        if m["status"] != "Em análise" or not str(justificativa or "").strip():
            raise MedicaoBloqueadaError("Justificativa obrigatória.")
        m["status"] = "Devolvida para correção"
        self.eventos[identificador].append(self._evento(3, "Devolução para correção", "Em análise", m["status"], justificativa))

    def aprovar(self, identificador, aprovador_id, usuario_id):
        m = self.medicoes[identificador]
        aprovador = self.servidores.get(aprovador_id)
        if m["status"] != "Em análise" or not aprovador or not aprovador["ativo"]:
            raise ReferenciaMedicaoInvalidaError("Aprovador ativo obrigatório.")
        self._recalcular(identificador)
        m.update(status="Aprovada", servidor_aprovador_id=aprovador_id, aprovado_em=datetime(2026, 7, 31, 12), aprovador_nome=aprovador["nome"])
        self.eventos[identificador].append(self._evento(4, "Aprovação", "Em análise", "Aprovada"))

    def cancelar(self, identificador, justificativa, usuario_id):
        m = self.medicoes[identificador]
        if m["status"] == "Aprovada" or not str(justificativa or "").strip():
            raise MedicaoBloqueadaError("Justificativa obrigatória.")
        m["status"] = "Cancelada"
        m["atual"] = False
        self.eventos[identificador].append(self._evento(5, "Cancelamento", "Em elaboração", "Cancelada", justificativa))

    def criar_revisao(self, identificador, justificativa, usuario_id):
        origem = self.medicoes[identificador]
        if origem["status"] != "Aprovada" or not origem["atual"] or not str(justificativa or "").strip():
            raise MedicaoBloqueadaError("Revisão inválida.")
        copia_origem = deepcopy(origem)
        try:
            origem["atual"] = False
            novo_id = self.proxima_medicao
            self.proxima_medicao += 1
            nova = self._medicao(novo_id, **{**origem, "id": novo_id, "versao": origem["versao"] + 1, "medicao_origem_id": identificador, "atual": True, "status": "Em elaboração", "aprovado_em": None, "servidor_aprovador_id": None})
            self.medicoes[novo_id] = nova
            self.itens[novo_id] = [{**deepcopy(x), "id": self.proximo_registro + n} for n, x in enumerate(self.itens[identificador]) if x["ativo"]]
            self.proximo_registro += len(self.itens[novo_id])
            self.ajustes[novo_id] = [{**deepcopy(x), "id": self.proximo_registro + n} for n, x in enumerate(self.ajustes[identificador]) if x["ativo"]]
            self.proximo_registro += len(self.ajustes[novo_id])
            self.vinculos[novo_id] = [{**deepcopy(x), "id": self.proximo_registro + n} for n, x in enumerate(self.vinculos[identificador]) if x["ativo"]]
            self.proximo_registro += len(self.vinculos[novo_id])
            self.eventos[novo_id] = [self._evento(1, "Revisão criada", "Aprovada", "Em elaboração", justificativa)]
            self.eventos[identificador].append(self._evento(6, "Substituição por revisão", "Aprovada", "Aprovada", justificativa))
            self._recalcular(novo_id)
            return novo_id
        except Exception:
            self.rollback_simulado = True
            self.medicoes[identificador] = copia_origem
            raise

    def listar_do_contrato(self, contrato_id, limite=10):
        itens = [x for x in self.medicoes.values() if x["contrato_id"] == contrato_id]
        return itens[:limite], {"total": len(itens), "elaboracao": 1, "analise": 0, "devolvidas": 0, "aprovadas": 0, "valor_aprovado": Decimal("0"), "total_glosas": sum((x["total_glosas"] for x in itens), Decimal("0"))}

    def indicadores(self):
        return {"elaboracao": 1, "analise": 0, "devolvidas": 0, "aprovadas_mes": 0, "liquido_aprovado_mes": Decimal("0"), "glosas_mes": Decimal("0")}


class CursorTransacaoFake:
    def __init__(self, medicao=None, falhar_em=None, falha_excecao=None, totais=None):
        self.medicao = medicao or {
            "id": 1, "contrato_id": 1, "numero_medicao": 1,
            "competencia": date(2026, 7, 1), "versao": 1,
            "servidor_fiscal_id": 1, "status": "Em elaboração",
            "ativo": True, "atual": True,
        }
        self.falhar_em = falhar_em
        self.falha_excecao = falha_excecao or psycopg2.Error("falha simulada")
        self.totais = totais or {"bruto": Decimal("0"), "acrescimos": Decimal("0"), "descontos": Decimal("0"), "glosas": Decimal("0")}
        self.consultas = []
        self._ultimo = ""
        self.rowcount = 1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, consulta, parametros=None):
        texto = " ".join(str(consulta).split())
        self.consultas.append((texto, parametros))
        self._ultimo = texto
        if self.falhar_em and self.falhar_em in texto:
            raise self.falha_excecao

    def fetchone(self):
        if "SELECT * FROM fc_medicoes" in self._ultimo:
            return dict(self.medicao)
        if "SELECT COALESCE((SELECT SUM" in self._ultimo:
            return self.totais
        if "SELECT COUNT(*)" in self._ultimo:
            return {"quantidade": 1}
        if "SELECT ativo FROM fc_contratos" in self._ultimo or "SELECT ativo FROM fc_servidores" in self._ultimo:
            return {"ativo": True}
        if "RETURNING id" in self._ultimo:
            return {"id": 10}
        return None


class ConexaoTransacaoFake:
    def __init__(self, cursor):
        self.cursor_obj = cursor
        self.commits = 0
        self.rollbacks = 0
        self.fechada = False

    def cursor(self, **kwargs):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.fechada = True


class TestFiscalizacaoContratosMedicoes(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = APP_MODULE.app
        cls.app.config.update(TESTING=True)
        cls.loader_original = APP_MODULE.login_manager._user_callback

    @classmethod
    def tearDownClass(cls):
        APP_MODULE.login_manager._user_callback = cls.loader_original

    def setUp(self):
        self.client = self.app.test_client()
        APP_MODULE.login_manager._user_callback = self._usuario
        self.servico = MedicaoServiceFake()
        self.armazenamento = ArmazenamentoMedicaoFake()
        self.conexoes_reais_antes = MOCK_CONNECT.call_count
        fiscalizacao_painel = MagicMock()
        fiscalizacao_painel.indicadores.return_value = {
            "ocorrencias_abertas": 0,
            "ocorrencias_vencidas": 0,
            "graves_criticas": 0,
            "fiscalizacoes_30_dias": 0,
        }
        self.patchers = [
            patch("modulos.fiscalizacao_contratos.routes.medicoes.MedicaoService", return_value=self.servico),
            patch("modulos.fiscalizacao_contratos.routes.MedicaoService", return_value=self.servico),
            patch("modulos.fiscalizacao_contratos.routes.contratos.MedicaoService", return_value=self.servico),
            patch("modulos.fiscalizacao_contratos.routes.FiscalizacaoService", return_value=fiscalizacao_painel),
            patch("modulos.fiscalizacao_contratos.routes.medicoes.CloudinaryStorage", return_value=self.armazenamento),
        ]
        for p in self.patchers:
            p.start()

    def tearDown(self):
        for p in reversed(self.patchers):
            p.stop()

    @staticmethod
    def _usuario(uid):
        return {"1": APP_MODULE.User(1, "admin", "admin", None), "2": APP_MODULE.User(2, "comum", "usuario", None)}.get(str(uid))

    def autenticar(self, uid):
        with self.client.session_transaction() as sessao:
            sessao["_user_id"] = str(uid)
            sessao["_fresh"] = True

    @staticmethod
    def dados_medicao(**extra):
        dados = {"contrato_id": "1", "numero_medicao": "2", "competencia": "2026-08", "periodo_inicio": "2026-08-01", "periodo_fim": "2026-08-31", "servidor_fiscal_id": "1", "data_apresentacao": "", "observacoes": "Execução mensal"}
        dados.update(extra)
        return dados

    @staticmethod
    def dados_item(**extra):
        dados = {"planilha_item_id": "", "ordem": "1", "codigo_item": "M-1", "descricao": "Item manual", "unidade": "un", "quantidade_prevista": "10", "quantidade_medida": "2,5", "preco_unitario": "4,20", "justificativa_excedente": "", "observacoes": ""}
        dados.update(extra)
        return dados

    @staticmethod
    def dados_ajuste(**extra):
        dados = {"tipo_ajuste": "Acréscimo", "descricao": "Complemento", "valor": "2,50", "fiscalizacao_id": "", "ocorrencia_id": "", "observacoes": ""}
        dados.update(extra)
        return dados

    def test_admin_acessa_lista_formulario_detalhe_e_cartao(self):
        self.autenticar(1)
        for caminho in ("/fiscalizacao-contratos/medicoes", "/fiscalizacao-contratos/medicoes/nova", "/fiscalizacao-contratos/medicoes/1"):
            self.assertEqual(self.client.get(caminho).status_code, 200, caminho)
        self.assertIn("Medições".encode(), self.client.get("/fiscalizacao-contratos").data)

    def test_visitante_e_usuario_comum_bloqueados_em_todas_as_rotas(self):
        get = ["/fiscalizacao-contratos/medicoes", "/fiscalizacao-contratos/medicoes/nova", "/fiscalizacao-contratos/medicoes/1", "/fiscalizacao-contratos/medicoes/1/editar", "/fiscalizacao-contratos/medicoes/1/itens/novo", "/fiscalizacao-contratos/medicoes/1/ajustes/novo", "/fiscalizacao-contratos/medicoes/1/documentos/vincular", "/fiscalizacao-contratos/medicoes/1/documentos/enviar", "/fiscalizacao-contratos/medicoes/1/devolver", "/fiscalizacao-contratos/medicoes/1/aprovar", "/fiscalizacao-contratos/medicoes/1/cancelar", "/fiscalizacao-contratos/medicoes/1/revisao", "/fiscalizacao-contratos/medicoes/1/eventos", "/fiscalizacao-contratos/medicoes/1/versoes"]
        post = ["/fiscalizacao-contratos/medicoes/nova", "/fiscalizacao-contratos/medicoes/1/editar", "/fiscalizacao-contratos/medicoes/1/itens/novo", "/fiscalizacao-contratos/medicoes/1/itens/1/editar", "/fiscalizacao-contratos/medicoes/1/itens/1/inativar", "/fiscalizacao-contratos/medicoes/1/ajustes/novo", "/fiscalizacao-contratos/medicoes/1/ajustes/1/editar", "/fiscalizacao-contratos/medicoes/1/ajustes/1/inativar", "/fiscalizacao-contratos/medicoes/1/documentos/vincular", "/fiscalizacao-contratos/medicoes/1/documentos/enviar", "/fiscalizacao-contratos/medicoes/1/documentos/1/inativar", "/fiscalizacao-contratos/medicoes/1/enviar", "/fiscalizacao-contratos/medicoes/1/devolver", "/fiscalizacao-contratos/medicoes/1/aprovar", "/fiscalizacao-contratos/medicoes/1/cancelar", "/fiscalizacao-contratos/medicoes/1/revisao"]
        self.assertTrue(all(self.client.get(x).status_code == 302 for x in get))
        self.autenticar(2)
        self.assertTrue(all(self.client.get(x).status_code == 403 for x in get))
        self.assertTrue(all(self.client.post(x).status_code == 403 for x in post))

    def test_cria_medicao_com_estado_versao_totais_e_evento_iniciais(self):
        self.autenticar(1)
        resposta = self.client.post("/fiscalizacao-contratos/medicoes/nova", data=self.dados_medicao())
        self.assertEqual(resposta.status_code, 302)
        nova = self.servico.medicoes[2]
        self.assertEqual((nova["versao"], nova["status"], nova["valor_liquido"]), (1, "Em elaboração", Decimal("0.00")))
        self.assertEqual(self.servico.eventos[2][0]["tipo_evento"], "Criação")

    def test_rejeita_contrato_e_fiscal_inexistentes_ou_inativos(self):
        self.autenticar(1)
        for campo, valor in (("contrato_id", "999"), ("contrato_id", "2"), ("servidor_fiscal_id", "999"), ("servidor_fiscal_id", "2")):
            self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/nova", data=self.dados_medicao(**{campo: valor})).status_code, 400)

    def test_validacoes_numero_competencia_e_periodo(self):
        for alteracao in ({"numero_medicao": "0"}, {"competencia": "2026-13"}, {"periodo_inicio": ""}, {"periodo_inicio": "2026-08-31", "periodo_fim": "2026-08-01"}):
            _, erros = normalizar_e_validar_medicao(self.dados_medicao(**alteracao))
            self.assertTrue(erros)

    def test_uma_atual_por_contrato_e_competencia(self):
        self.autenticar(1)
        resposta = self.client.post("/fiscalizacao-contratos/medicoes/nova", data=self.dados_medicao(competencia="2026-07"))
        self.assertEqual(resposta.status_code, 400)
        self.assertEqual(sum(x["atual"] for x in self.servico.medicoes.values()), 1)

    def test_item_manual_calcula_com_decimal_sem_confiar_em_total_do_formulario(self):
        self.autenticar(1)
        dados = self.dados_item(valor_medido="999999")
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=dados).status_code, 302)
        item = self.servico.itens[1][0]
        self.assertEqual(item["valor_medido"], Decimal("10.50"))
        self.assertEqual(self.servico.medicoes[1]["valor_bruto"], Decimal("10.50"))

    def test_formula_financeira_com_valores_do_teste_manual(self):
        itens = [{"valor_medido": Decimal("7500.00"), "ativo": True}]
        ajustes = [
            {"tipo_ajuste": "Acréscimo", "valor": Decimal("5.00"), "ativo": True},
            {"tipo_ajuste": "Desconto", "valor": Decimal("0.00"), "ativo": True},
            {"tipo_ajuste": "Glosa", "valor": Decimal("10.00"), "ativo": True},
        ]
        bruto, acrescimos, descontos, glosas, liquido = calcular_totais(itens, ajustes)
        self.assertEqual(
            (bruto, acrescimos, descontos, glosas, liquido),
            (
                Decimal("7500.00"), Decimal("5.00"), Decimal("0.00"),
                Decimal("10.00"), Decimal("7495.00"),
            ),
        )

    def test_detalhe_mostra_composicao_explicita_do_valor_liquido(self):
        self.autenticar(1)
        self.servico.medicoes[1].update(
            valor_bruto=Decimal("7500.00"),
            total_acrescimos=Decimal("5.00"),
            total_descontos=Decimal("0.00"),
            total_glosas=Decimal("10.00"),
            valor_liquido=Decimal("7495.00"),
        )
        resposta = self.client.get("/fiscalizacao-contratos/medicoes/1")
        self.assertEqual(resposta.status_code, 200)
        conteudo = resposta.data.decode("utf-8")
        for texto in (
            "R$ 7.500,00", "(+) Acréscimos", "R$ 5,00", "(-) Glosas",
            "R$ 10,00", "R$ 7.495,00",
        ):
            self.assertIn(texto, conteudo)
        self.assertIn("Aprovação da medição não significa pagamento", conteudo)
        self.assertIn("Reduzem o valor medido", conteudo)

    def test_item_de_planilha_copia_fotografia_e_rejeita_outro_contrato(self):
        self.autenticar(1)
        resposta = self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item(planilha_item_id="10", descricao="", unidade="", preco_unitario=""))
        self.assertEqual(resposta.status_code, 302)
        item = self.servico.itens[1][0]
        self.assertEqual((item["descricao"], item["preco_unitario"]), ("Serviço previsto", Decimal("5.25")))
        self.servico.planilha[10]["descricao"] = "Alterado depois"
        self.assertEqual(item["descricao"], "Serviço previsto")
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item(planilha_item_id="20", descricao="", unidade="", preco_unitario="")).status_code, 400)

    def test_item_duplicado_excesso_e_valores_negativos_sao_rejeitados(self):
        self.autenticar(1)
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item(planilha_item_id="10", descricao="", unidade="", preco_unitario=""))
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item(planilha_item_id="10", descricao="", unidade="", preco_unitario="")).status_code, 400)
        for alteracao in ({"quantidade_medida": "-1"}, {"preco_unitario": "-1"}, {"quantidade_medida": "11", "justificativa_excedente": ""}):
            _, erros = normalizar_e_validar_item(self.dados_item(**alteracao))
            self.assertTrue(erros)
        dados, erros = normalizar_e_validar_item(self.dados_item(quantidade_medida="11", justificativa_excedente="Autorizado"))
        self.assertFalse(erros)
        self.assertEqual(dados["quantidade_medida"], Decimal("11"))

    def test_edicao_e_inativacao_de_item_recalculam_sem_apagar(self):
        self.autenticar(1)
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item())
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/1/editar", data=self.dados_item(quantidade_medida="3"))
        self.assertEqual(self.servico.medicoes[1]["valor_bruto"], Decimal("12.60"))
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/1/inativar")
        self.assertFalse(self.servico.itens[1][0]["ativo"])
        self.assertEqual(self.servico.medicoes[1]["valor_bruto"], Decimal("0.00"))

    def test_acrescimo_desconto_e_glosa_calculam_totais(self):
        self.autenticar(1)
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item(quantidade_medida="10", preco_unitario="10"))
        for tipo, valor in (("Acréscimo", "20"), ("Desconto", "5"), ("Glosa", "10")):
            self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/ajustes/novo", data=self.dados_ajuste(tipo_ajuste=tipo, valor=valor)).status_code, 302)
        m = self.servico.medicoes[1]
        self.assertEqual((m["valor_bruto"], m["total_acrescimos"], m["total_descontos"], m["total_glosas"], m["valor_liquido"]), (Decimal("100.00"), Decimal("20.00"), Decimal("5.00"), Decimal("10.00"), Decimal("105.00")))

    def test_ajuste_zero_referencias_de_outro_contrato_e_liquido_negativo(self):
        _, erros = normalizar_e_validar_ajuste(self.dados_ajuste(valor="0"))
        self.assertTrue(erros)
        self.autenticar(1)
        for campo in ("fiscalizacao_id", "ocorrencia_id"):
            self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/ajustes/novo", data=self.dados_ajuste(**{campo: "2"})).status_code, 400)
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item(quantidade_medida="1", preco_unitario="1"))
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/ajustes/novo", data=self.dados_ajuste(tipo_ajuste="Glosa", valor="2")).status_code, 400)
        self.assertEqual(len(self.servico.ajustes[1]), 0)

    def test_edicao_e_inativacao_de_ajuste_recalculam_e_preservam(self):
        self.autenticar(1)
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item(quantidade_medida="10", preco_unitario="10"))
        self.client.post("/fiscalizacao-contratos/medicoes/1/ajustes/novo", data=self.dados_ajuste(valor="10"))
        self.client.post("/fiscalizacao-contratos/medicoes/1/ajustes/2/editar", data=self.dados_ajuste(valor="15"))
        self.assertEqual(self.servico.medicoes[1]["valor_liquido"], Decimal("115.00"))
        self.client.post("/fiscalizacao-contratos/medicoes/1/ajustes/2/inativar")
        self.assertFalse(self.servico.ajustes[1][0]["ativo"])
        self.assertEqual(self.servico.medicoes[1]["valor_liquido"], Decimal("100.00"))

    def test_inativacao_que_geraria_liquido_negativo_faz_rollback(self):
        totais_negativos = {"bruto": Decimal("0"), "acrescimos": Decimal("0"), "descontos": Decimal("1"), "glosas": Decimal("0")}
        for operacao in ("item", "ajuste"):
            cursor = CursorTransacaoFake(totais=totais_negativos)
            conexao = ConexaoTransacaoFake(cursor)
            servico = MedicaoService(lambda: conexao)
            with self.assertRaises(MedicaoBloqueadaError):
                if operacao == "item":
                    servico.inativar_item(1, 9, 7)
                else:
                    servico.inativar_ajuste(1, 9, 7)
            self.assertEqual((conexao.commits, conexao.rollbacks), (0, 1))

    def test_documento_mesmo_contrato_duplicado_outro_contrato_e_inativacao(self):
        self.autenticar(1)
        dados = {"documento_id": "1", "categoria": "Relatório de medição", "observacoes": "Comprovante"}
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/documentos/vincular", data=dados).status_code, 302)
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/documentos/vincular", data=dados).status_code, 400)
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/documentos/vincular", data={**dados, "documento_id": "2"}).status_code, 400)
        self.client.post("/fiscalizacao-contratos/medicoes/1/documentos/1/inativar")
        self.assertFalse(self.servico.vinculos[1][0]["ativo"])
        self.assertEqual(len(self.servico.documentos), 2)

    def test_admin_pode_abrir_envio_de_documento_na_medicao(self):
        self.autenticar(1)
        resposta = self.client.get(
            "/fiscalizacao-contratos/medicoes/1/documentos/enviar"
        )
        self.assertEqual(resposta.status_code, 200)
        self.assertIn(b'multipart/form-data', resposta.data)
        self.assertIn("Enviar documento comprobatório".encode(), resposta.data)

    def test_upload_cria_documento_e_vincula_a_medicao(self):
        self.autenticar(1)
        resposta = self.client.post(
            "/fiscalizacao-contratos/medicoes/1/documentos/enviar",
            data={
                "titulo": "Boletim da medição",
                "categoria": "Relatório de medição",
                "descricao": "Período de julho",
                "observacoes": "Conferido pelo fiscal",
                "arquivo": (io.BytesIO(b"%PDF-1.4\nconteudo de teste"), "boletim.pdf"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(len(self.armazenamento.enviados), 1)
        self.assertEqual(self.servico.documentos[-1]["categoria"], "Relatório")
        self.assertEqual(self.servico.vinculos[1][-1]["categoria"], "Relatório de medição")
        self.assertEqual(self.servico.vinculos[1][-1]["titulo"], "Boletim da medição")

    def test_upload_invalido_nao_chama_cloudinary(self):
        self.autenticar(1)
        resposta = self.client.post(
            "/fiscalizacao-contratos/medicoes/1/documentos/enviar",
            data={
                "titulo": "Arquivo inválido",
                "categoria": "Relatório de medição",
                "arquivo": (io.BytesIO(b"nao e pdf"), "arquivo.pdf"),
            },
            content_type="multipart/form-data",
        )
        self.assertEqual(resposta.status_code, 400)
        self.assertFalse(self.armazenamento.enviados)

    def test_upload_e_vinculo_reais_sao_atomicos_e_compensam_falha(self):
        arquivo = {
            "nome_original": "boletim.pdf", "mime_type": "application/pdf",
            "extensao": "pdf", "tamanho_bytes": 12, "sha256": "a" * 64,
            "conteudo": b"%PDF-1.4",
        }
        dados = {"titulo": "Boletim", "descricao": None, "categoria": "Relatório"}
        for falha, commits, rollbacks, removidos in (
            (None, 1, 0, 0),
            ("INSERT INTO fc_medicao_documentos", 0, 1, 1),
        ):
            cursor = CursorTransacaoFake(falhar_em=falha)
            conexao = ConexaoTransacaoFake(cursor)
            armazenamento = ArmazenamentoMedicaoFake()
            servico = MedicaoService(lambda: conexao)
            if falha:
                with self.assertRaises(MedicaoServiceError):
                    servico.enviar_documento(
                        1, dados, arquivo, "Relatório de medição", None, 7,
                        armazenamento,
                    )
            else:
                servico.enviar_documento(
                    1, dados, arquivo, "Relatório de medição", None, 7,
                    armazenamento,
                )
            self.assertEqual((conexao.commits, conexao.rollbacks), (commits, rollbacks))
            self.assertEqual(len(armazenamento.removidos), removidos)

    def test_envio_exige_item_e_bloqueia_edicao(self):
        self.autenticar(1)
        self.client.post("/fiscalizacao-contratos/medicoes/1/enviar")
        self.assertEqual(self.servico.medicoes[1]["status"], "Em elaboração")
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item())
        self.client.post("/fiscalizacao-contratos/medicoes/1/enviar")
        self.assertEqual(self.servico.medicoes[1]["status"], "Em análise")
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item()).status_code, 302)

    def test_devolucao_exige_justificativa_e_libera_correcao(self):
        self.autenticar(1)
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item())
        self.client.post("/fiscalizacao-contratos/medicoes/1/enviar")
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/devolver", data={"justificativa": ""}).status_code, 400)
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/devolver", data={"justificativa": "Corrigir quantidade"}).status_code, 302)
        self.assertEqual(self.client.get("/fiscalizacao-contratos/medicoes/1/editar").status_code, 200)

    def test_aprovacao_exige_aprovador_ativo_e_torna_imutavel(self):
        self.autenticar(1)
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item())
        self.client.post("/fiscalizacao-contratos/medicoes/1/enviar")
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/aprovar", data={"servidor_aprovador_id": "2"}).status_code, 400)
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/aprovar", data={"servidor_aprovador_id": "3"}).status_code, 302)
        self.assertEqual(self.servico.medicoes[1]["status"], "Aprovada")
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item()).status_code, 302)

    def test_cancelamento_exige_justificativa_e_preserva_registros(self):
        self.autenticar(1)
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item())
        self.client.post("/fiscalizacao-contratos/medicoes/1/ajustes/novo", data=self.dados_ajuste())
        self.client.post("/fiscalizacao-contratos/medicoes/1/documentos/vincular", data={"documento_id": "1", "categoria": "Relatório de medição", "observacoes": ""})
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/cancelar", data={"justificativa": ""}).status_code, 400)
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/cancelar", data={"justificativa": "Lançamento indevido"}).status_code, 302)
        self.assertEqual(self.servico.medicoes[1]["status"], "Cancelada")
        self.assertFalse(self.servico.medicoes[1]["atual"])
        self.assertTrue(self.servico.medicoes[1]["ativo"])
        self.assertEqual(len(self.servico.itens[1]), 1)
        self.assertEqual(len(self.servico.ajustes[1]), 1)
        self.assertEqual(len(self.servico.vinculos[1]), 1)
        self.assertEqual(self.servico.eventos[1][-1]["tipo_evento"], "Cancelamento")

    def test_cancelamento_libera_competencia_sem_reutilizar_numero(self):
        self.autenticar(1)
        self.client.post("/fiscalizacao-contratos/medicoes/1/cancelar", data={"justificativa": "Substituir lançamento"})
        resposta = self.client.post("/fiscalizacao-contratos/medicoes/nova", data=self.dados_medicao(numero_medicao="2", competencia="2026-07", periodo_inicio="2026-07-01", periodo_fim="2026-07-31"))
        self.assertEqual(resposta.status_code, 302)
        self.assertEqual(self.servico.medicoes[1]["numero_medicao"], 1)
        self.assertEqual(self.servico.medicoes[2]["numero_medicao"], 2)
        self.assertEqual(sum(x["atual"] and x["ativo"] for x in self.servico.medicoes.values()), 1)

    def test_cancelamento_real_e_atomico_com_rollback_de_update_e_evento(self):
        for falha in (None, "UPDATE fc_medicoes SET status", "INSERT INTO fc_medicao_eventos"):
            cursor = CursorTransacaoFake(falhar_em=falha)
            conexao = ConexaoTransacaoFake(cursor)
            servico = MedicaoService(lambda: conexao)
            if falha:
                with self.assertRaises(MedicaoServiceError):
                    servico.cancelar(1, "Justificativa suficiente", 7)
                self.assertEqual((conexao.commits, conexao.rollbacks), (0, 1))
            else:
                servico.cancelar(1, "Justificativa suficiente", 7)
                self.assertEqual((conexao.commits, conexao.rollbacks), (1, 0))
                atualizacao = next(q for q, _ in cursor.consultas if "UPDATE fc_medicoes SET status" in q)
                self.assertIn("atual=FALSE", atualizacao)
                self.assertTrue(any("INSERT INTO fc_medicao_eventos" in q for q, _ in cursor.consultas))

    def test_criacao_trata_concorrencia_e_falha_do_evento_com_rollback(self):
        dados, erros = normalizar_e_validar_medicao(self.dados_medicao())
        self.assertFalse(erros)
        cursor = CursorTransacaoFake(
            falhar_em="INSERT INTO fc_medicoes",
            falha_excecao=psycopg2.IntegrityError("concorrência simulada"),
        )
        conexao = ConexaoTransacaoFake(cursor)
        with self.assertRaises(MedicaoDuplicadaError):
            MedicaoService(lambda: conexao).criar(dados, 7)
        self.assertEqual((conexao.commits, conexao.rollbacks), (0, 1))

        cursor_evento = CursorTransacaoFake(falhar_em="INSERT INTO fc_medicao_eventos")
        conexao_evento = ConexaoTransacaoFake(cursor_evento)
        with self.assertRaises(MedicaoServiceError):
            MedicaoService(lambda: conexao_evento).criar(dados, 7)
        self.assertEqual((conexao_evento.commits, conexao_evento.rollbacks), (0, 1))

    def test_revisao_concorrente_e_conflito_de_indice_fazem_rollback(self):
        aprovada = CursorTransacaoFake().medicao | {"status": "Aprovada", "atual": True, "ativo": True}
        cursor_conflito = CursorTransacaoFake(
            medicao=aprovada,
            falhar_em="INSERT INTO fc_medicoes",
            falha_excecao=psycopg2.IntegrityError("revisão simultânea"),
        )
        conexao_conflito = ConexaoTransacaoFake(cursor_conflito)
        with self.assertRaises(MedicaoDuplicadaError):
            MedicaoService(lambda: conexao_conflito).criar_revisao(1, "Revisão necessária", 7)
        self.assertEqual((conexao_conflito.commits, conexao_conflito.rollbacks), (0, 1))

        historica = aprovada | {"atual": False}
        cursor_historico = CursorTransacaoFake(medicao=historica)
        conexao_historico = ConexaoTransacaoFake(cursor_historico)
        with self.assertRaises(MedicaoBloqueadaError):
            MedicaoService(lambda: conexao_historico).criar_revisao(1, "Segunda solicitação", 7)
        self.assertEqual((conexao_historico.commits, conexao_historico.rollbacks), (0, 1))

    def test_edicao_preserva_fotografia_do_item_de_planilha_sem_nova_consulta(self):
        cursor = MagicMock()
        cursor.execute.side_effect = AssertionError("A planilha não deve ser consultada novamente")
        existente = {"planilha_item_id": 10, "codigo_item": "FOTO-1", "descricao": "Fotografia", "unidade": "un", "quantidade_prevista": Decimal("5"), "preco_unitario": Decimal("2.50")}
        dados = {"planilha_item_id": 10, "codigo_item": "ALTERADO", "descricao": "Alterado", "unidade": "kg", "quantidade_prevista": Decimal("99"), "quantidade_medida": Decimal("3"), "preco_unitario": Decimal("99"), "justificativa_excedente": None}
        normalizados, valor = MedicaoService(lambda: None)._dados_item(cursor, {"contrato_id": 1}, dados, existente)
        self.assertEqual((normalizados["codigo_item"], normalizados["descricao"], normalizados["unidade"], normalizados["quantidade_prevista"], normalizados["preco_unitario"]), ("FOTO-1", "Fotografia", "un", Decimal("5"), Decimal("2.50")))
        self.assertEqual(valor, Decimal("7.50"))
        cursor.execute.assert_not_called()

    def test_arredondamento_monetario_explicito_e_entradas_vazias(self):
        self.assertEqual(calcular_valor_item(Decimal("3"), Decimal("0.335")), Decimal("1.01"))
        dados, erros = normalizar_e_validar_item(self.dados_item(quantidade_medida="", preco_unitario=""))
        self.assertTrue(erros)
        self.assertIsNone(dados["quantidade_medida"])

    def test_acesso_direto_respeita_bloqueios_de_status(self):
        self.autenticar(1)
        self.servico.medicoes[1]["status"] = "Aprovada"
        bloqueadas = [
            "/fiscalizacao-contratos/medicoes/1/editar",
            "/fiscalizacao-contratos/medicoes/1/itens/novo",
            "/fiscalizacao-contratos/medicoes/1/ajustes/novo",
            "/fiscalizacao-contratos/medicoes/1/documentos/vincular",
            "/fiscalizacao-contratos/medicoes/1/cancelar",
        ]
        self.assertTrue(all(self.client.get(caminho).status_code == 302 for caminho in bloqueadas))
        self.assertEqual(self.client.get("/fiscalizacao-contratos/medicoes/1/revisao").status_code, 200)
        self.servico.medicoes[1].update(status="Cancelada", atual=False)
        self.assertEqual(self.client.get("/fiscalizacao-contratos/medicoes/1/revisao").status_code, 302)

    def _aprovar_com_conteudo(self):
        self.autenticar(1)
        self.client.post("/fiscalizacao-contratos/medicoes/1/itens/novo", data=self.dados_item())
        self.client.post("/fiscalizacao-contratos/medicoes/1/ajustes/novo", data=self.dados_ajuste())
        self.client.post("/fiscalizacao-contratos/medicoes/1/documentos/vincular", data={"documento_id": "1", "categoria": "Relatório de medição", "observacoes": ""})
        self.client.post("/fiscalizacao-contratos/medicoes/1/enviar")
        self.client.post("/fiscalizacao-contratos/medicoes/1/aprovar", data={"servidor_aprovador_id": "3"})

    def test_revisao_exige_justificativa_e_copia_com_novos_ids(self):
        self._aprovar_com_conteudo()
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/revisao", data={"justificativa": ""}).status_code, 400)
        ids_anteriores = ([x["id"] for x in self.servico.itens[1]], [x["id"] for x in self.servico.ajustes[1]], [x["id"] for x in self.servico.vinculos[1]])
        self.assertEqual(self.client.post("/fiscalizacao-contratos/medicoes/1/revisao", data={"justificativa": "Correção formal"}).status_code, 302)
        nova = self.servico.medicoes[2]
        self.assertEqual((nova["versao"], nova["medicao_origem_id"], nova["status"], nova["atual"]), (2, 1, "Em elaboração", True))
        self.assertEqual((self.servico.medicoes[1]["status"], self.servico.medicoes[1]["atual"]), ("Aprovada", False))
        self.assertTrue(set(ids_anteriores[0]).isdisjoint(x["id"] for x in self.servico.itens[2]))
        self.assertTrue(set(ids_anteriores[1]).isdisjoint(x["id"] for x in self.servico.ajustes[2]))
        self.assertTrue(set(ids_anteriores[2]).isdisjoint(x["id"] for x in self.servico.vinculos[2]))
        self.assertEqual({self.servico.eventos[1][-1]["tipo_evento"], self.servico.eventos[2][0]["tipo_evento"]}, {"Substituição por revisão", "Revisão criada"})

    def test_varias_revisoes_preservam_historico_e_uma_unica_atual(self):
        self._aprovar_com_conteudo()
        nova = self.servico.criar_revisao(1, "Primeira revisão", 1)
        self.servico.medicoes[nova]["status"] = "Aprovada"
        terceira = self.servico.criar_revisao(nova, "Segunda revisão", 1)
        self.assertEqual([x["versao"] for x in self.servico.medicoes.values()], [1, 2, 3])
        self.assertEqual(sum(x["atual"] for x in self.servico.medicoes.values()), 1)
        self.assertEqual(self.servico.medicoes[terceira]["medicao_origem_id"], nova)

    def test_falha_de_revisao_faz_rollback_simulado(self):
        self._aprovar_com_conteudo()
        original = self.servico._recalcular
        self.servico._recalcular = MagicMock(side_effect=RuntimeError("falha simulada"))
        with self.assertRaises(RuntimeError):
            self.servico.criar_revisao(1, "Revisão", 1)
        self.assertTrue(self.servico.rollback_simulado)
        self.assertTrue(self.servico.medicoes[1]["atual"])
        self.servico._recalcular = original

    def test_pesquisa_filtros_eventos_versoes_e_indicadores(self):
        self.autenticar(1)
        resposta = self.client.get("/fiscalizacao-contratos/medicoes?busca=Empresa&status=Em+elabora%C3%A7%C3%A3o&com_glosa=1&versoes=todas")
        self.assertEqual(resposta.status_code, 200)
        busca, filtros = self.servico.ultimo_filtro
        self.assertEqual(busca, "Empresa")
        self.assertTrue(filtros["com_glosa"])
        self.assertEqual(filtros["versoes"], "todas")
        self.assertEqual(self.client.get("/fiscalizacao-contratos/medicoes/1/eventos").status_code, 302)
        self.assertEqual(self.client.get("/fiscalizacao-contratos/medicoes/1/versoes").status_code, 302)
        painel = self.client.get("/fiscalizacao-contratos").data
        self.assertIn("Líquido aprovado no mês".encode(), painel)

    def test_calculos_decimal_arredondamento_e_sem_float(self):
        self.assertEqual(calcular_valor_item(Decimal("2.555"), Decimal("3.333")), Decimal("8.52"))
        totais = calcular_totais([{"valor_medido": Decimal("10.10"), "ativo": True}], [{"tipo_ajuste": "Acréscimo", "valor": Decimal("1.11"), "ativo": True}, {"tipo_ajuste": "Glosa", "valor": Decimal("0.21"), "ativo": True}])
        self.assertEqual(totais, (Decimal("10.10"), Decimal("1.11"), Decimal("0.00"), Decimal("0.21"), Decimal("11.00")))
        fonte = (RAIZ / "modulos/fiscalizacao_contratos/services/medicoes_service.py").read_text(encoding="utf-8") + (RAIZ / "modulos/fiscalizacao_contratos/validacoes_medicoes.py").read_text(encoding="utf-8")
        self.assertNotIn("float(", fonte)

    def test_migracao_aditiva_com_tabelas_restricoes_e_indices(self):
        sql = (RAIZ / "modulos/fiscalizacao_contratos/migrations/010_criar_fc_medicoes.sql").read_text(encoding="utf-8")
        for tabela in ("fc_medicoes", "fc_medicao_itens", "fc_medicao_ajustes", "fc_medicao_documentos", "fc_medicao_eventos"):
            self.assertIn(f"CREATE TABLE IF NOT EXISTS {tabela}", sql)
        for proibido in ("DROP ", "TRUNCATE ", "DELETE ", "UPDATE ", "INSERT ", "ALTER TABLE"):
            self.assertNotIn(proibido, sql.upper())
        self.assertIn("uq_fc_medicoes_atual_ativa_competencia", sql)
        self.assertIn("uq_fc_medicao_item_planilha_ativo", sql)
        self.assertIn("uq_fc_medicao_documento_ativo", sql)

    def test_servico_nao_usa_delete_sql_dinamico_credencial_ou_app(self):
        caminho = RAIZ / "modulos/fiscalizacao_contratos/services/medicoes_service.py"
        fonte = caminho.read_text(encoding="utf-8")
        arvore = ast.parse(fonte)
        self.assertNotIn("DELETE ", fonte.upper())
        self.assertNotIn("DATABASE_URL", fonte)
        self.assertNotIn("import app", fonte)
        chamadas = [n for n in ast.walk(arvore) if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "execute"]
        self.assertTrue(chamadas)
        for chamada in chamadas:
            self.assertFalse(isinstance(chamada.args[0], ast.JoinedStr), "SQL não pode usar f-string")

    def test_rotas_antigas_e_novas_continuam_registradas(self):
        rotas = {r.rule for r in self.app.url_map.iter_rules()}
        for rota in ("/", "/login", "/fiscalizacao-contratos/contratos", "/fiscalizacao-contratos/fiscalizacoes", "/fiscalizacao-contratos/ocorrencias", "/fiscalizacao-contratos/medicoes", "/fiscalizacao-contratos/medicoes/<int:medicao_id>/revisao"):
            self.assertIn(rota, rotas)

    def test_integracao_com_contrato_e_painel_esta_presente(self):
        contrato = (RAIZ / "modulos/fiscalizacao_contratos/templates/fiscalizacao_contratos/contratos/detalhe.html").read_text(encoding="utf-8")
        painel = (RAIZ / "modulos/fiscalizacao_contratos/templates/fiscalizacao_contratos/painel.html").read_text(encoding="utf-8")
        self.assertIn("medicoes_contrato", contrato)
        self.assertIn("valor_aprovado", contrato)
        self.assertIn("medicoes_nova", contrato)
        for indicador in ("elaboracao", "analise", "devolvidas", "aprovadas_mes", "liquido_aprovado_mes", "glosas_mes"):
            self.assertIn(f"indicadores_medicoes.{indicador}", painel)

    def test_eventos_sao_somente_leitura_e_todas_as_rotas_sao_administrativas(self):
        fonte = (RAIZ / "modulos/fiscalizacao_contratos/routes/medicoes.py").read_text(encoding="utf-8")
        self.assertNotIn("editar_evento", fonte)
        self.assertNotIn("inativar_evento", fonte)
        self.assertNotIn("excluir_evento", fonte)
        arvore = ast.parse(fonte)
        funcoes_rotas = [n for n in ast.walk(arvore) if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and any(isinstance(d, ast.Call) and isinstance(d.func, ast.Attribute) and d.func.attr == "route" for d in n.decorator_list)]
        self.assertGreaterEqual(len(funcoes_rotas), 19)
        for funcao in funcoes_rotas:
            nomes = [d.id for d in funcao.decorator_list if isinstance(d, ast.Name)]
            self.assertIn("admin_required", nomes, funcao.name)

    def test_transacoes_recalculos_e_revisao_usam_bloqueio_e_rollback(self):
        fonte = (RAIZ / "modulos/fiscalizacao_contratos/services/medicoes_service.py").read_text(encoding="utf-8")
        self.assertIn("FOR UPDATE", fonte)
        self.assertIn("conexao.commit()", fonte)
        self.assertIn("conexao.rollback()", fonte)
        self.assertIn("self._recalcular(cursor,medicao_id,usuario_id)", fonte)
        self.assertIn("self._recalcular(cursor,nova,usuario_id)", fonte)
        self.assertIn("UPDATE fc_medicoes SET atual=FALSE", fonte)

    def test_app_py_patrimonio_cloudinary_e_arquivos_reais_nao_sao_tocados(self):
        alterados = {linha[3:] for linha in os.popen("git status --short").read().splitlines() if len(linha) > 3}
        self.assertNotIn("app.py", alterados)
        self.assertFalse(any("patrimonio" in x.lower() for x in alterados))
        self.assertFalse(any("cloudinary" in x.lower() for x in alterados))
        self.assertEqual(MOCK_CONNECT.call_count, self.conexoes_reais_antes)


if __name__ == "__main__":
    unittest.main()
