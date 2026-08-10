"""Testes offline da infraestrutura controlada de migrations H2C.4A.1C."""

from __future__ import annotations

import contextlib
import hashlib
import inspect
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
import migrations_control.validated_sql as validated_sql_module
import tests.test_migrations_control_h2c4a2_postgresql as pg_capture_module
from unittest import mock
from dataclasses import FrozenInstanceError, replace
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID, uuid4

from migrations_control.checksum import (
    calcular_sha256_arquivo,
    calcular_sha256_normalizado,
    normalizar_utf8_lf,
)
from migrations_control.bootstrap import executar_bootstrap_controlado
from migrations_control.cli import main as cli_main
from migrations_control.connection_state import (
    ConnectionState,
    TRANSACTION_STATUS_IDLE,
    TRANSACTION_STATUS_INERROR,
    TRANSACTION_STATUS_INTRANS,
)
from migrations_control.errors import (
    AppliedMigrationHashMismatchError,
    ChecksumMismatchError,
    CircularDependencyError,
    ConnectionClosedError,
    ConnectionNotIdleError,
    ConnectionRestoreError,
    ConnectionStateError,
    DatabaseConnectionError,
    DuplicateJsonKeyError,
    DuplicateMigrationError,
    DuplicateOrderError,
    ExtraMigrationFileError,
    InvalidLedgerError,
    InvalidManifestJsonError,
    ImpossibleLedgerStateError,
    LockError,
    LockReleaseError,
    LockTimeoutError,
    ManifestError,
    MigrationExecutionError,
    MissingDependencyError,
    MissingMigrationFileError,
    NonUtf8FileError,
    UnknownDatabaseError,
    UnknownOperationTypeError,
    UnsafeMigrationPathError,
    UnsupportedManifestVersionError,
    sanitizar_erro,
)
from migrations_control.ledger import (
    concluir_tentativa,
    iniciar_tentativa,
    registrar_m0001_aplicada,
)
from migrations_control.locking import AdvisoryLock, derivar_chave_lock
from migrations_control.manifest import carregar_manifesto
from migrations_control.models import (
    AppliedMigration,
    MigrationExecution,
    PreflightSnapshot,
)
from migrations_control.preflight import (
    LEDGER_OBJECTS,
    LEDGER_SEQUENCE_OBJECTS,
    LEDGER_TABLES,
    classificar_preflight,
    coletar_conteudo_ledger,
    coletar_snapshot,
    validar_conteudo_ledger,
)
from migrations_control.runner import MigrationRunner
from migrations_control.schema_validation import (
    EXPECTED_LEDGER_SCHEMA,
    ColumnSignature,
    ConstraintSignature,
    LedgerSchemaSnapshot,
    QualifiedName,
    _constraint,
    canonicalizar_constraintdef,
    canonicalizar_indexdef,
    canonicalizar_sql,
    coletar_assinatura_ledger,
    normalizar_colunas_constraint,
    normalizar_conkey_constraint,
    tokenizar_sql,
    validar_assinatura_ledger,
)
from migrations_control.validated_sql import (
    ValidatedSql,
    carregar_sql_validado,
    validar_artefato_sql,
)


ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "migrations_control" / "manifesto.json"
SQL = ROOT / "migrations_control" / "sql" / "M0001_criar_ledger.sql"
NOW = datetime(2026, 8, 3, 12, 0, tzinfo=timezone.utc)
REQUEST_ID = UUID("12345678-1234-5678-1234-567812345678")
ARTEFATO_CAMPOS = (
    "operacao_id", "raiz_autorizada", "caminho_autorizado",
    "caminho_resolvido", "checksum_esperado", "checksum_calculado",
    "bytes_normalizados", "texto_sql", "identidade_arquivo",
)


def catalogo_bruto_sintetico():
    """Fixture sintética do contrato; não representa captura PostgreSQL real."""
    captura = {
        "metadados": {
            "formato": "h2c4a2-pg-catalog-raw",
            "formato_versao": 1,
            "postgres_version": "15.18",
            "server_version_num": 150018,
            "container_image": "postgres:15",
            "container_image_digest": "sha256:" + "0" * 64,
            "captured_at_utc": "2026-08-04T12:00:00Z",
            "capture_id": "capture-" + "1" * 32,
            "m0001_checksum": pg_capture_module.CHECKSUM_M0001,
            "manifesto_versao": 1,
        },
        "pg_constraint": [{
            "oid_evidencia": 101,
            "schema": "public",
            "tabela": "schema_migrations",
            "conname": "ck_sintetico",
            "contype": "c",
            "conkey": None,
            "convalidated": True,
            "conislocal": True,
            "coninhcount": 0,
            "connoinherit": False,
            "condeferrable": False,
            "condeferred": False,
            "pg_get_constraintdef": "CHECK ((pg_catalog.btrim(campo) <> ''::text))",
            "pg_get_expr_conbin": "(pg_catalog.btrim(campo) <> ''::text)",
            "colunas_resolvidas": [],
        }],
        "pg_index": [{
            "oid_evidencia": 201,
            "indice_schema": "public",
            "indice_nome": "ix_sintetico",
            "tabela_schema": "public",
            "tabela_nome": "schema_migrations",
            "metodo": "btree",
            "indisunique": False,
            "indisprimary": False,
            "indisexclusion": False,
            "indimmediate": True,
            "indisvalid": True,
            "indisready": True,
            "indislive": True,
            "indisclustered": False,
            "indisreplident": False,
            "indnullsnotdistinct": False,
            "indcheckxmin": False,
            "indnkeyatts": 1,
            "indnatts": 1,
            "indkey": [2],
            "indclass": [3126],
            "indcollation": [100],
            "indoption": [0],
            "indexprs": None,
            "indpred": None,
            "pg_get_indexdef": "CREATE INDEX ix_sintetico ON schema_migrations USING btree (campo)",
            "vinculado_constraint": False,
            "relpersistence": "p",
        }],
        "operator_classes": [{
            "indice_oid_evidencia": 201,
            "posicao": 1,
            "oid_evidencia": 3126,
            "schema": "pg_catalog",
            "nome": "text_ops",
        }],
        "collations": [{
            "indice_oid_evidencia": 201,
            "posicao": 1,
            "oid_evidencia": None,
            "schema": None,
            "nome": None,
        }],
        "sequencias": [{
            "oid_evidencia": 301,
            "schema": "public",
            "nome": "schema_migrations_id_seq",
            "relpersistence": "p",
            "tabela_schema": "public",
            "tabela": "schema_migrations",
            "coluna": "id",
            "numero_coluna": 1,
            "tipo_dependencia": "i",
            "tipo_sequencia": "bigint",
            "inicio": 1,
            "incremento": 1,
            "minimo": 1,
            "maximo": 9223372036854775807,
            "cache": 1,
            "cycle": False,
        }],
        "inventario_public": {
            categoria: [] for categoria in pg_capture_module.INVENTORY_SPECS
        },
        "cobertura_inventario": [],
    }
    captura["cobertura_inventario"] = [
        {
            "categoria": categoria,
            "catalogo": spec["catalogo"],
            "consulta": spec["consulta"],
            "quantidade": 0,
            "vazio": True,
        }
        for categoria, spec in pg_capture_module.INVENTORY_SPECS.items()
    ]
    return captura


class RawCatalogCursorDouble:
    def __init__(self, captura):
        self.captura = captura
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, consulta):
        categoria_inventario = next(
            (
                categoria
                for categoria, spec in pg_capture_module.INVENTORY_SPECS.items()
                if consulta == spec["consulta"]
            ),
            None,
        )
        if categoria_inventario is not None:
            campos = pg_capture_module.INVENTORY_SPECS[categoria_inventario]["campos"]
            self.rows = [
                tuple(item[campo] for campo in campos)
                for item in self.captura["inventario_public"][categoria_inventario]
            ]
            return
        if "FROM pg_catalog.pg_constraint" in consulta:
            colecao = "pg_constraint"
            campos = (
                "oid_evidencia", "schema", "tabela", "conname", "contype",
                "conkey", "convalidated", "conislocal", "coninhcount",
                "connoinherit", "condeferrable", "condeferred",
                "pg_get_constraintdef", "pg_get_expr_conbin", "colunas_resolvidas",
            )
        elif "FROM pg_catalog.pg_index" in consulta and "pg_catalog.pg_opclass" in consulta:
            colecao = "operator_classes"
            campos = (
                "indice_oid_evidencia", "posicao", "oid_evidencia", "schema", "nome",
            )
        elif "FROM pg_catalog.pg_index" in consulta and "pg_catalog.pg_collation" in consulta:
            colecao = "collations"
            campos = (
                "indice_oid_evidencia", "posicao", "oid_evidencia", "schema", "nome",
            )
        elif "FROM pg_catalog.pg_index" in consulta:
            colecao = "pg_index"
            campos = (
                "oid_evidencia", "indice_schema", "indice_nome", "tabela_schema",
                "tabela_nome", "metodo", "indisunique", "indisprimary",
                "indisexclusion", "indimmediate", "indisvalid", "indisready",
                "indislive", "indisclustered", "indisreplident",
                "indnullsnotdistinct", "indcheckxmin", "indnkeyatts", "indnatts",
                "indkey", "indclass", "indcollation", "indoption", "indexprs",
                "indpred", "pg_get_indexdef", "vinculado_constraint",
                "relpersistence",
            )
        elif "FROM pg_catalog.pg_class s" in consulta:
            colecao = "sequencias"
            campos = (
                "oid_evidencia", "schema", "nome", "relpersistence",
                "tabela_schema", "tabela", "coluna", "numero_coluna",
                "tipo_dependencia", "tipo_sequencia", "inicio", "incremento",
                "minimo", "maximo", "cache", "cycle",
            )
        else:
            raise AssertionError("Consulta bruta inesperada no double.")
        self.rows = [tuple(item[campo] for campo in campos) for item in self.captura[colecao]]

    def fetchall(self):
        return list(self.rows)


class RawCatalogConnectionDouble:
    def __init__(self, captura):
        self.captura = captura

    def cursor(self):
        return RawCatalogCursorDouble(self.captura)


def clonar_artefato_por_bypass(artefato, *, classe=ValidatedSql, validar=True, **mudancas):
    """Simula introspecção deliberada; não representa a API suportada."""
    artificial = object.__new__(classe)
    for campo in ARTEFATO_CAMPOS:
        object.__setattr__(
            artificial, campo, mudancas.get(campo, getattr(artefato, campo))
        )
    if validar:
        artificial.__post_init__()
    return artificial


class ManifestFixture:
    def __init__(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name)
        (self.base / "sql").mkdir()
        self.sql = self.base / "sql" / "M0001_criar_ledger.sql"
        self.sql.write_bytes(SQL.read_bytes())
        self.data = json.loads(MANIFEST.read_text(encoding="utf-8"))
        self.manifest = self.base / "manifesto.json"

    def save(self) -> Path:
        self.manifest.write_text(json.dumps(self.data), encoding="utf-8")
        return self.manifest

    def raw(self, texto: str) -> Path:
        self.manifest.write_text(texto, encoding="utf-8")
        return self.manifest

    def close(self) -> None:
        self.temp.cleanup()


class ManifestTests(unittest.TestCase):
    def setUp(self):
        self.fx = ManifestFixture()

    def tearDown(self):
        self.fx.close()

    def test_manifesto_real_valido(self):
        self.assertEqual(["M0000", "M0001"], [x.identificador for x in carregar_manifesto().operacoes])

    def test_json_invalido(self):
        with self.assertRaises(InvalidManifestJsonError):
            carregar_manifesto(self.fx.raw("{"))

    def test_raiz_nao_objeto(self):
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.raw("[]"))

    def test_chave_duplicada_raiz(self):
        texto = MANIFEST.read_text(encoding="utf-8").replace(
            '"versao_formato": 1,', '"versao_formato": 1, "versao_formato": 1,'
        )
        with self.assertRaises(DuplicateJsonKeyError):
            carregar_manifesto(self.fx.raw(texto))

    def test_chave_duplicada_operacao(self):
        texto = MANIFEST.read_text(encoding="utf-8").replace(
            '"identificador": "M0000",',
            '"identificador": "M0000", "identificador": "M0000",', 1,
        )
        with self.assertRaises(DuplicateJsonKeyError):
            carregar_manifesto(self.fx.raw(texto))

    def test_chave_duplicada_objeto_aninhado(self):
        texto = '{"objeto":{"x":1,"x":2}}'
        with self.assertRaises(DuplicateJsonKeyError):
            carregar_manifesto(self.fx.raw(texto))

    def test_versao_invalida(self):
        self.fx.data["versao_formato"] = 2
        with self.assertRaises(UnsupportedManifestVersionError):
            carregar_manifesto(self.fx.save())

    def test_versao_true_rejeitada(self):
        self.fx.data["versao_formato"] = True
        with self.assertRaises(UnsupportedManifestVersionError):
            carregar_manifesto(self.fx.save())

    def test_campo_obrigatorio_ausente(self):
        del self.fx.data["sistema"]
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_campo_desconhecido(self):
        self.fx.data["inesperado"] = 1
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_booleano_como_texto(self):
        self.fx.data["operacoes"][1]["transacional"] = "true"
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_ordem_true_rejeitada(self):
        self.fx.data["operacoes"][1]["ordem_global"] = True
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_ordem_negativa(self):
        self.fx.data["operacoes"][1]["ordem_global"] = -1
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_identificador_invalido(self):
        self.fx.data["operacoes"][1]["identificador"] = "X1"
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_identificador_duplicado(self):
        self.fx.data["operacoes"][1]["identificador"] = "M0000"
        with self.assertRaises(DuplicateMigrationError):
            carregar_manifesto(self.fx.save())

    def test_ordem_duplicada(self):
        self.fx.data["operacoes"][1]["ordem_global"] = 0
        with self.assertRaises(DuplicateOrderError):
            carregar_manifesto(self.fx.save())

    def test_dependencia_ausente(self):
        self.fx.data["operacoes"][1]["dependencias"] = ["M9999"]
        with self.assertRaises(MissingDependencyError):
            carregar_manifesto(self.fx.save())

    def test_dependencia_circular(self):
        self.fx.data["operacoes"][0]["dependencias"] = ["M0001"]
        with self.assertRaises(CircularDependencyError):
            carregar_manifesto(self.fx.save())

    def test_ordem_incompativel(self):
        self.fx.data["operacoes"][0]["ordem_global"] = 1
        self.fx.data["operacoes"][1]["ordem_global"] = 0
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_dependencia_habilitada_para_desabilitada(self):
        self.fx.data["operacoes"][0]["habilitada"] = False
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_m0000_desabilitada(self):
        self.fx.data["operacoes"][0]["habilitada"] = False
        self.fx.data["operacoes"][1]["habilitada"] = False
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_m0001_desabilitada(self):
        self.fx.data["operacoes"][1]["habilitada"] = False
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_tipo_desconhecido(self):
        self.fx.data["operacoes"][1]["tipo"] = "MAGIA"
        with self.assertRaises(UnknownOperationTypeError):
            carregar_manifesto(self.fx.save())

    def test_caminho_absoluto(self):
        self.fx.data["operacoes"][1]["caminho"] = "C:/fora.sql"
        with self.assertRaises(UnsafeMigrationPathError):
            carregar_manifesto(self.fx.save())

    def test_caminho_com_travessia(self):
        self.fx.data["operacoes"][1]["caminho"] = "sql/../fora.sql"
        with self.assertRaises(UnsafeMigrationPathError):
            carregar_manifesto(self.fx.save())

    def test_symlink_escapando_da_pasta_sql(self):
        externo = self.fx.base / "externo.sql"
        externo.write_bytes(self.fx.sql.read_bytes())
        self.fx.sql.unlink()
        try:
            self.fx.sql.symlink_to(externo)
        except OSError:
            self.fx.sql.write_bytes(externo.read_bytes())
            resolver_original = Path.resolve
            def resolver(caminho, *args, **kwargs):
                if caminho == self.fx.sql:
                    return externo
                return resolver_original(caminho, *args, **kwargs)
            with mock.patch.object(Path, "resolve", autospec=True, side_effect=resolver):
                with self.assertRaises(UnsafeMigrationPathError):
                    carregar_manifesto(self.fx.save())
        else:
            with self.assertRaises(UnsafeMigrationPathError):
                carregar_manifesto(self.fx.save())

    def test_extensao_incorreta(self):
        self.fx.data["operacoes"][1]["caminho"] = "sql/M0001.txt"
        with self.assertRaises(UnsafeMigrationPathError):
            carregar_manifesto(self.fx.save())

    def test_arquivo_ausente(self):
        self.fx.sql.unlink()
        with self.assertRaises(MissingMigrationFileError):
            carregar_manifesto(self.fx.save())

    def test_arquivo_extra(self):
        (self.fx.base / "sql" / "extra.sql").write_text("SELECT 1", encoding="utf-8")
        with self.assertRaises(ExtraMigrationFileError):
            carregar_manifesto(self.fx.save())

    def test_checksum_maiusculo(self):
        self.fx.data["operacoes"][1]["checksum"] = "A" * 64
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_checksum_tamanho_incorreto(self):
        self.fx.data["operacoes"][1]["checksum"] = "a" * 63
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_checksum_divergente(self):
        self.fx.data["operacoes"][1]["checksum"] = "0" * 64
        with self.assertRaises(ChecksumMismatchError):
            carregar_manifesto(self.fx.save())

    def test_m0000_com_arquivo(self):
        self.fx.data["operacoes"][0]["caminho"] = "sql/M0001_criar_ledger.sql"
        self.fx.data["operacoes"][0]["checksum"] = calcular_sha256_arquivo(self.fx.sql)
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_m0000_com_ddl(self):
        self.fx.data["operacoes"][0]["possui_ddl"] = True
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_m0001_sem_arquivo(self):
        self.fx.data["operacoes"][1]["caminho"] = None
        self.fx.data["operacoes"][1]["checksum"] = None
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())

    def test_lista_testes_vazia(self):
        self.fx.data["operacoes"][1]["testes_exigidos"] = []
        with self.assertRaises(ManifestError):
            carregar_manifesto(self.fx.save())


class ChecksumTests(unittest.TestCase):
    def test_lf_crlf_cr_equivalentes(self):
        hashes = {calcular_sha256_normalizado(x) for x in (b"a\nb\n", b"a\r\nb\r\n", b"a\rb\r")}
        self.assertEqual(1, len(hashes))

    def test_utf8(self):
        self.assertEqual("ç\n".encode(), normalizar_utf8_lf("ç\r\n".encode()))

    def test_bytes_invalidos(self):
        with self.assertRaises(NonUtf8FileError):
            normalizar_utf8_lf(b"\xff")

    def test_vazio(self):
        self.assertRegex(calcular_sha256_normalizado(b""), r"^[0-9a-f]{64}$")

    def test_conteudo_diferente(self):
        self.assertNotEqual(calcular_sha256_normalizado(b"a"), calcular_sha256_normalizado(b"b"))

    def test_arquivo_nao_alterado(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = Path(pasta) / "x.sql"
            original = b"SELECT 1;\r\n"
            arquivo.write_bytes(original)
            calcular_sha256_arquivo(arquivo)
            self.assertEqual(original, arquivo.read_bytes())

    def test_hash_real_confere(self):
        self.assertEqual(
            "1966113e8d20f4f3aaa2ebc0b6b1f312470ac99835ea97026305c732ab5e0f39",
            calcular_sha256_arquivo(SQL),
        )


class ValidatedSqlTests(unittest.TestCase):
    def carregar(self, caminho, checksum=None, *, raiz=None, autorizado=None, operacao="M0001"):
        caminho = Path(caminho)
        raiz = Path(raiz or caminho.parent).resolve()
        autorizado = Path(autorizado or caminho).absolute()
        return carregar_sql_validado(
            operacao_id=operacao,
            raiz_autorizada=raiz,
            caminho_autorizado=autorizado,
            checksum_esperado=checksum or calcular_sha256_arquivo(caminho),
        )

    def test_arquivo_inalterado(self):
        artefato = self.carregar(SQL)
        self.assertEqual(SQL.read_text(encoding="utf-8"), artefato.texto_sql)
        self.assertEqual("M0001", artefato.operacao_id)

    def test_arquivo_alterado_rejeitado(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = Path(pasta) / "m.sql"
            arquivo.write_text("SELECT 2;\n", encoding="utf-8")
            with self.assertRaises(ChecksumMismatchError):
                self.carregar(arquivo, calcular_sha256_normalizado(b"SELECT 1;\n"))

    def test_bytes_utf8_invalidos(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = Path(pasta) / "m.sql"; arquivo.write_bytes(b"\xff")
            with self.assertRaises(NonUtf8FileError):
                self.carregar(arquivo, "0" * 64)

    def test_arquivo_removido(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = Path(pasta) / "ausente.sql"
            with self.assertRaises(MissingMigrationFileError):
                self.carregar(arquivo, "0" * 64)

    def test_crlf_executavel_normalizado(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = Path(pasta) / "m.sql"; arquivo.write_bytes(b"SELECT 1;\r\n")
            esperado = calcular_sha256_normalizado(arquivo.read_bytes())
            artefato = self.carregar(arquivo, esperado)
            self.assertEqual((b"SELECT 1;\n", "SELECT 1;\n"),
                             (artefato.bytes_normalizados, artefato.texto_sql))

    def test_alteracao_posterior_nao_muda_memoria(self):
        with tempfile.TemporaryDirectory() as pasta:
            arquivo = Path(pasta) / "m.sql"; arquivo.write_text("SELECT 1;\n", encoding="utf-8")
            artefato = self.carregar(arquivo)
            arquivo.write_text("SELECT 2;\n", encoding="utf-8")
            self.assertEqual("SELECT 1;\n", artefato.texto_sql)

    def test_artefato_imutavel(self):
        artefato = self.carregar(SQL)
        with self.assertRaises(FrozenInstanceError):
            artefato.texto_sql = "outro"

    def test_construtor_publico_direto_rejeitado(self):
        with self.assertRaisesRegex(TypeError, "não possui construtor público"):
            ValidatedSql()

    def test_construtor_publico_rejeita_identidade_inventada_e_arquivo_inexistente(self):
        conteudo = b"SELECT 1;\n"
        checksum = calcular_sha256_normalizado(conteudo)
        with tempfile.TemporaryDirectory() as pasta:
            inexistente = Path(pasta).resolve() / "inexistente.sql"
            with self.assertRaisesRegex(TypeError, "não possui construtor público"):
                ValidatedSql(
                    "M0001", inexistente.parent, inexistente, inexistente,
                    checksum, checksum, conteudo, conteudo.decode("utf-8"),
                    (0, 0, len(conteudo), 0),
                )

    def test_fabrica_controlada_cria_artefato_valido(self):
        artefato = self.carregar(SQL)
        self.assertIs(type(artefato), ValidatedSql)
        self.assertTrue(artefato.caminho_autorizado.is_file())
        self.assertEqual(artefato.bytes_normalizados.decode("utf-8"), artefato.texto_sql)

    def test_fabrica_rejeita_arquivo_inexistente_sem_criar_artefato(self):
        with tempfile.TemporaryDirectory() as pasta:
            inexistente = Path(pasta).resolve() / "inexistente.sql"
            with self.assertRaises(MissingMigrationFileError):
                self.carregar(inexistente, "0" * 64)

    def test_buffer_e_texto_divergentes_rejeitados(self):
        artefato = self.carregar(SQL)
        with self.assertRaises(ChecksumMismatchError):
            clonar_artefato_por_bypass(artefato, texto_sql="SELECT 2;\n")

    def test_checksum_calculado_divergente_rejeitado(self):
        artefato = self.carregar(SQL)
        with self.assertRaises(ChecksumMismatchError):
            clonar_artefato_por_bypass(artefato, checksum_calculado="0" * 64)

    def test_checksum_esperado_divergente_rejeitado(self):
        artefato = self.carregar(SQL)
        with self.assertRaises(ChecksumMismatchError):
            clonar_artefato_por_bypass(artefato, checksum_esperado="0" * 64)

    def test_raiz_e_caminho_incompativeis_rejeitados(self):
        artefato = self.carregar(SQL)
        with tempfile.TemporaryDirectory() as pasta:
            with self.assertRaises(UnsafeMigrationPathError):
                clonar_artefato_por_bypass(
                    artefato, raiz_autorizada=Path(pasta).resolve()
                )

    def test_caminho_fora_da_raiz_rejeitado(self):
        with tempfile.TemporaryDirectory() as pasta:
            base = Path(pasta)
            raiz = base / "sql"; raiz.mkdir()
            fora = base / "fora.sql"; fora.write_text("SELECT 1;\n", encoding="utf-8")
            with self.assertRaises(UnsafeMigrationPathError):
                self.carregar(fora, raiz=raiz)

    def test_caminho_canonico_diferente_rejeitado(self):
        with tempfile.TemporaryDirectory() as pasta:
            base = Path(pasta); autorizado = base / "a.sql"; outro = base / "b.sql"
            autorizado.write_text("SELECT 1;\n", encoding="utf-8")
            outro.write_text("SELECT 1;\n", encoding="utf-8")
            artefato = self.carregar(autorizado)
            with self.assertRaises(ChecksumMismatchError):
                validar_artefato_sql(
                    artefato, operacao_id="M0001", raiz_autorizada=base.resolve(),
                    caminho_autorizado=outro.resolve(), checksum_esperado=artefato.checksum_esperado,
                )

    def test_operacao_divergente_rejeitada(self):
        artefato = self.carregar(SQL)
        with self.assertRaises(ChecksumMismatchError):
            validar_artefato_sql(
                artefato, operacao_id="M9999", raiz_autorizada=SQL.parent.resolve(),
                caminho_autorizado=SQL.resolve(), checksum_esperado=artefato.checksum_esperado,
            )

    def test_raiz_autorizada_diferente_rejeitada(self):
        artefato = self.carregar(SQL)
        with tempfile.TemporaryDirectory() as pasta:
            with self.assertRaises((ChecksumMismatchError, UnsafeMigrationPathError)):
                validar_artefato_sql(
                    artefato, operacao_id="M0001", raiz_autorizada=Path(pasta).resolve(),
                    caminho_autorizado=SQL.resolve(), checksum_esperado=artefato.checksum_esperado,
                )

    def test_stat_fstat_divergente_rejeitado(self):
        original = os.fstat
        chamadas = 0
        def fstat_divergente(descritor):
            nonlocal chamadas
            chamadas += 1
            resultado = original(descritor)
            if chamadas == 2:
                return mock.Mock(
                    st_dev=resultado.st_dev, st_ino=resultado.st_ino,
                    st_size=resultado.st_size,
                    st_mtime_ns=resultado.st_mtime_ns + 1,
                )
            return resultado
        with mock.patch("migrations_control.validated_sql.os.fstat", side_effect=fstat_divergente):
            with self.assertRaises(UnsafeMigrationPathError):
                self.carregar(SQL)

    def test_resolucao_final_divergente_rejeitada(self):
        with tempfile.TemporaryDirectory() as pasta:
            raiz = Path(pasta).resolve()
            arquivo = raiz / "m.sql"; arquivo.write_text("SELECT 1;\n", encoding="utf-8")
            outro = raiz / "outro.sql"; outro.write_text("SELECT 1;\n", encoding="utf-8")
            resultados = iter(((raiz, arquivo.resolve()), (raiz, outro.resolve())))
            with mock.patch.object(
                validated_sql_module, "_conferir_caminho", side_effect=lambda *_: next(resultados)
            ):
                with self.assertRaises(UnsafeMigrationPathError):
                    self.carregar(arquivo)

    def test_resolucao_inicial_interna_e_final_fora_da_raiz_rejeitada(self):
        with tempfile.TemporaryDirectory() as pasta:
            base = Path(pasta).resolve()
            raiz = base / "sql"; raiz.mkdir()
            interno = raiz / "m.sql"; interno.write_text("SELECT 1;\n", encoding="utf-8")
            externo = base / "m.sql"; externo.write_text("SELECT 1;\n", encoding="utf-8")
            resultados = iter(((raiz, interno), (raiz, externo)))
            with mock.patch.object(
                validated_sql_module, "_conferir_caminho", side_effect=lambda *_: next(resultados)
            ):
                with self.assertRaises(UnsafeMigrationPathError):
                    self.carregar(interno, raiz=raiz)

    def test_arquivo_homonimo_externo_com_checksum_correto_rejeitado(self):
        with tempfile.TemporaryDirectory() as pasta:
            base = Path(pasta).resolve()
            raiz = base / "sql"; raiz.mkdir()
            externo = base / "M0001_criar_ledger.sql"
            externo.write_bytes(SQL.read_bytes())
            with self.assertRaises(UnsafeMigrationPathError):
                self.carregar(
                    externo, calcular_sha256_arquivo(externo), raiz=raiz,
                    autorizado=externo,
                )

    def test_troca_simulada_de_symlink_durante_leitura_rejeitada(self):
        with tempfile.TemporaryDirectory() as pasta:
            base = Path(pasta).resolve()
            raiz = base / "sql"; raiz.mkdir()
            interno = raiz / "m.sql"; interno.write_text("SELECT 1;\n", encoding="utf-8")
            externo = base / "alvo.sql"; externo.write_text("SELECT 1;\n", encoding="utf-8")
            resultados = iter(((raiz, interno), (raiz, externo)))
            with mock.patch.object(
                validated_sql_module, "_conferir_caminho", side_effect=lambda *_: next(resultados)
            ):
                with self.assertRaises(UnsafeMigrationPathError):
                    self.carregar(interno, raiz=raiz)

    def test_symlink_alterado_antes_da_leitura_e_rejeitado(self):
        with tempfile.TemporaryDirectory() as pasta:
            base = Path(pasta)
            original = base / "original.sql"; original.write_text("SELECT 1;\n", encoding="utf-8")
            alterado = base / "alterado.sql"; alterado.write_text("SELECT 2;\n", encoding="utf-8")
            link = base / "m.sql"
            try:
                link.symlink_to(alterado)
            except OSError:
                resolver_original = Path.resolve
                def resolver(caminho, *args, **kwargs):
                    if caminho == link:
                        return alterado
                    return resolver_original(caminho, *args, **kwargs)
                with mock.patch.object(Path, "resolve", autospec=True, side_effect=resolver):
                    with self.assertRaises(UnsafeMigrationPathError):
                        self.carregar(link, calcular_sha256_arquivo(original))
            else:
                with self.assertRaises(UnsafeMigrationPathError):
                    self.carregar(link, calcular_sha256_arquivo(original))


def registros_validos(manifesto=None, request_id=REQUEST_ID, duracao=5):
    manifesto = manifesto or carregar_manifesto()
    m1 = manifesto.por_id()["M0001"]
    aplicada = AppliedMigration(
        "M0001", 1, m1.modulo, m1.checksum, 1, 1, NOW, duracao, 1
    )
    execucao = MigrationExecution(
        "M0001", 0, "APLICADA", NOW, NOW, duracao, m1.checksum,
        None, None, request_id, "abcdef0123456789", 123, 1,
    )
    return (aplicada,), (execucao,)


def snapshot_novo(objetos=(), public=True):
    return PreflightSnapshot(public, frozenset(objetos))


def snapshot_controlado(manifesto=None):
    aplicadas, execucoes = registros_validos(manifesto)
    return PreflightSnapshot(
        True, LEDGER_OBJECTS, assinatura_ledger=EXPECTED_LEDGER_SCHEMA,
        migrations_aplicadas=aplicadas, execucoes=execucoes,
    )


class SchemaValidationTests(unittest.TestCase):
    def mutar_tabela(self, nome, **mudanca):
        tabelas = [replace(t, **mudanca) if t.nome == nome else t for t in EXPECTED_LEDGER_SCHEMA.tabelas]
        return LedgerSchemaSnapshot(tuple(tabelas), EXPECTED_LEDGER_SCHEMA.sequencias)

    def mutar_sequencia(self, indice=0, **mudanca):
        sequencias = list(EXPECTED_LEDGER_SCHEMA.sequencias)
        sequencias[indice] = replace(sequencias[indice], **mudanca)
        return LedgerSchemaSnapshot(EXPECTED_LEDGER_SCHEMA.tabelas, tuple(sequencias))

    def mutar_indice(self, tabela_indice=0, indice=0, **mudanca):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[tabela_indice]
        indices = list(tabela.indices)
        indices[indice] = replace(indices[indice], **mudanca)
        return self.mutar_tabela(tabela.nome, indices=tuple(indices))

    def test_estrutura_exata(self):
        self.assertTrue(validar_assinatura_ledger(EXPECTED_LEDGER_SCHEMA)[0])
        constraints = tuple(
            constraint
            for tabela in EXPECTED_LEDGER_SCHEMA.tabelas
            for constraint in tabela.constraints
        )
        self.assertTrue(all(
            constraint.no_inherit
            for constraint in constraints
            if constraint.tipo in {"p", "u"}
        ))
        self.assertTrue(all(
            not constraint.no_inherit
            for constraint in constraints
            if constraint.tipo == "c"
        ))
        self.assertFalse(_constraint(
            "fk_exemplo", "f", (1,), ("id",),
            "FOREIGN KEY (id) REFERENCES public.exemplo (id)",
        ).no_inherit)

    def test_tipo_coluna_incorreto(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[1]
        colunas = list(tabela.colunas)
        colunas[0] = replace(colunas[0], tipo="integer")
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, colunas=tuple(colunas)))[0])

    def test_nulabilidade_incorreta(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        colunas = list(tabela.colunas)
        colunas[1] = replace(colunas[1], nullable=True)
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, colunas=tuple(colunas)))[0])

    def test_default_incorreto(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        colunas = list(tabela.colunas)
        colunas[3] = replace(colunas[3], default="2")
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, colunas=tuple(colunas)))[0])

    def test_coluna_extra(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        extra = ColumnSignature(11, "extra", "text", None, None, None, True, None, "", "")
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, colunas=tabela.colunas + (extra,)))[0])

    def test_coluna_ausente(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, colunas=tabela.colunas[:-1]))[0])

    def test_pk_incorreta(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        constraints = tuple(c for c in tabela.constraints if c.tipo != "p")
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, constraints=constraints))[0])
        constraints = tuple(
            replace(c, no_inherit=False) if c.tipo == "p" else c
            for c in tabela.constraints
        )
        self.assertFalse(validar_assinatura_ledger(
            self.mutar_tabela(tabela.nome, constraints=constraints)
        )[0])

    def test_unique_ausente(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        constraints = tuple(c for c in tabela.constraints if c.tipo != "u")
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, constraints=constraints))[0])
        constraints = tuple(
            replace(c, no_inherit=False) if c.tipo == "u" else c
            for c in tabela.constraints
        )
        self.assertFalse(validar_assinatura_ledger(
            self.mutar_tabela(tabela.nome, constraints=constraints)
        )[0])

    def test_check_divergente(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        constraints = list(tabela.constraints)
        constraints[0] = replace(
            constraints[0],
            definicao=canonicalizar_constraintdef("CHECK (false)", "c"),
        )
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, constraints=tuple(constraints)))[0])

    def test_indice_ausente(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[1]
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, indices=tabela.indices[:-1]))[0])

    def test_indice_extra(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, indices=tabela.indices + (tabela.indices[0],)))[0])

    def test_nome_fisico_divergente(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        constraints = list(tabela.constraints)
        constraints[0] = replace(constraints[0], nome="nome_errado")
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, constraints=tuple(constraints)))[0])

    def test_schema_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela("schema_migrations", schema="outro"))[0])

    def test_apenas_uma_tabela(self):
        self.assertFalse(validar_assinatura_ledger(LedgerSchemaSnapshot(
            EXPECTED_LEDGER_SCHEMA.tabelas[:1], EXPECTED_LEDGER_SCHEMA.sequencias
        ))[0])

    def test_canonicalizacao_nao_depende_de_espacos(self):
        self.assertEqual(canonicalizar_sql("CHECK ( a > 0 )"), canonicalizar_sql("check(a>0)"))

    def test_check_postgresql_15_e_forma_normativa_sao_equivalentes(self):
        atual = canonicalizar_constraintdef(
            "CHECK (btrim(migration_id) <> ''::text)", "c"
        )
        normativa = canonicalizar_constraintdef(
            "CHECK ((pg_catalog.btrim(migration_id) <> ''::text))", "c"
        )
        self.assertEqual(normativa, atual)

    def test_canonicalizador_geral_preserva_todos_os_qualificadores(self):
        funcoes = (
            "lower", "upper", "char_length", "octet_length", "coalesce",
            "now", "btrim",
        )
        for funcao in funcoes:
            argumentos = "a, b" if funcao == "coalesce" else ("" if funcao == "now" else "a")
            with self.subTest(funcao=funcao):
                self.assertNotEqual(
                    canonicalizar_sql(f"pg_catalog.{funcao}({argumentos})"),
                    canonicalizar_sql(f"{funcao}({argumentos})"),
                )
        for schema in ("outro_schema", "public"):
            with self.subTest(schema=schema):
                self.assertNotEqual(
                    canonicalizar_sql(f"{schema}.btrim(a)"),
                    canonicalizar_sql("btrim(a)"),
                )

    def test_excecao_btrim_existe_somente_no_contexto_check(self):
        self.assertNotEqual(
            canonicalizar_sql("pg_catalog.btrim(a)"),
            canonicalizar_sql("btrim(a)"),
        )
        self.assertEqual(
            canonicalizar_constraintdef("CHECK (pg_catalog.btrim(a) <> '')", "c"),
            canonicalizar_constraintdef("CHECK (btrim(a) <> '')", "c"),
        )

    def test_check_btrim_qualificado_nos_tres_campos_e_parenteses(self):
        for coluna in ("migration_id", "modulo", "situacao"):
            simples = f"CHECK (btrim({coluna}) <> ''::text)"
            qualificado = f"CHECK (pg_catalog.btrim({coluna}) <> ''::text)"
            redundante = f"CHECK ((((pg_catalog.btrim({coluna}) <> ''::text))))"
            with self.subTest(coluna=coluna):
                esperado = canonicalizar_constraintdef(simples, "c")
                self.assertEqual(esperado, canonicalizar_constraintdef(qualificado, "c"))
                self.assertEqual(esperado, canonicalizar_constraintdef(redundante, "c"))

    def test_check_nao_generaliza_funcao_schema_argumentos_cast_operador_ou_literal(self):
        base = canonicalizar_constraintdef("CHECK (pg_catalog.btrim(a) <> ''::text)", "c")
        divergentes = (
            "CHECK (pg_catalog.lower(a) <> ''::text)",
            "CHECK (lower(a) <> ''::text)",
            "CHECK (pg_catalog.upper(a) <> ''::text)",
            "CHECK (pg_catalog.char_length(a) <> ''::text)",
            "CHECK (pg_catalog.octet_length(a) <> ''::text)",
            "CHECK (pg_catalog.coalesce(a, b) <> ''::text)",
            "CHECK (pg_catalog.now() <> ''::text)",
            "CHECK (outro_schema.btrim(a) <> ''::text)",
            "CHECK (public.btrim(a) <> ''::text)",
            "CHECK (btrim(b) <> ''::text)",
            "CHECK (btrim(a, b) <> ''::text)",
            "CHECK (btrim(a) <> ''::varchar)",
            "CHECK (btrim(a) = ''::text)",
            "CHECK (btrim(a) <> 'x'::text)",
        )
        for definicao in divergentes:
            with self.subTest(definicao=definicao):
                self.assertNotEqual(base, canonicalizar_constraintdef(definicao, "c"))
        self.assertNotEqual(
            canonicalizar_constraintdef("CHECK (pg_catalog.btrim = 1)", "c"),
            canonicalizar_constraintdef("CHECK (btrim = 1)", "c"),
        )
        self.assertNotEqual(
            canonicalizar_constraintdef('CHECK (pg_catalog."btrim"(a) <> \'\')', "c"),
            base,
        )

    def test_qualificadores_permanecem_em_defaults_indices_e_predicados(self):
        self.assertNotEqual(
            canonicalizar_sql("DEFAULT pg_catalog.lower(coluna)"),
            canonicalizar_sql("DEFAULT lower(coluna)"),
        )
        self.assertNotEqual(
            canonicalizar_sql("pg_catalog.lower(coluna)"),
            canonicalizar_sql("lower(coluna)"),
        )
        self.assertNotEqual(
            canonicalizar_sql("pg_catalog.lower(coluna) = 'a'"),
            canonicalizar_sql("lower(coluna) = 'a'"),
        )
        argumentos = dict(
            indice_schema="public", indice_nome="ix_exemplo",
            tabela_schema="public", tabela_nome="tabela_exemplo",
        )
        self.assertNotEqual(
            canonicalizar_indexdef(
                "CREATE INDEX ix_exemplo ON tabela_exemplo USING btree (pg_catalog.lower(coluna))",
                **argumentos,
            ),
            canonicalizar_indexdef(
                "CREATE INDEX ix_exemplo ON tabela_exemplo USING btree (lower(coluna))",
                **argumentos,
            ),
        )

    def test_check_remove_apenas_parenteses_externos_redundantes(self):
        simples = canonicalizar_constraintdef("CHECK (tentativa >= 0)", "c")
        redundante = canonicalizar_constraintdef("CHECK ((((tentativa >= 0))))", "c")
        self.assertEqual(simples, redundante)
        self.assertNotEqual(
            canonicalizar_constraintdef("CHECK ((a + b) * c > 0)", "c"),
            canonicalizar_constraintdef("CHECK (a + (b * c) > 0)", "c"),
        )
        self.assertNotEqual(
            canonicalizar_constraintdef("CHECK (estado = '(ok)'::text)", "c"),
            canonicalizar_constraintdef("CHECK (estado = 'ok'::text)", "c"),
        )

    def test_check_preserva_literal_cast_funcao_operador_e_ordem(self):
        base = canonicalizar_constraintdef(
            "CHECK (pg_catalog.btrim(migration_id) <> ''::text AND tentativa >= 0)", "c"
        )
        divergentes = (
            "CHECK (pg_catalog.btrim(outro) <> ''::text AND tentativa >= 0)",
            "CHECK (pg_catalog.btrim(migration_id) = ''::text AND tentativa >= 0)",
            "CHECK (pg_catalog.btrim(migration_id) <> 'x'::text AND tentativa >= 0)",
            "CHECK (pg_catalog.trim(migration_id) <> ''::text AND tentativa >= 0)",
            "CHECK (tentativa >= 0 AND pg_catalog.btrim(migration_id) <> ''::text)",
            "CHECK (pg_catalog.btrim(migration_id) <> ''::varchar AND tentativa >= 0)",
            "CHECK (pg_catalog.btrim(migration_id) <> ''::text OR tentativa >= 0)",
            "CHECK (pg_catalog.btrim(migration_id) <> ''::text)",
        )
        for definicao in divergentes:
            with self.subTest(definicao=definicao):
                self.assertNotEqual(base, canonicalizar_constraintdef(definicao, "c"))

    def test_check_malformado_ou_sem_expressao_e_rejeitado(self):
        for definicao in ("CHECK", "CHECK ()", "CHECK ((a > 0)", "a > 0"):
            with self.subTest(definicao=definicao):
                with self.assertRaises(ValueError):
                    canonicalizar_constraintdef(definicao, "c")

    def test_colunas_constraint_preservam_ordem_e_quantidade(self):
        self.assertEqual(("a",), normalizar_colunas_constraint([1], ["a"]))
        self.assertEqual(("a", "b"), normalizar_colunas_constraint([1, 2], ["a", "b"]))
        self.assertEqual((), normalizar_colunas_constraint(None, []))
        self.assertIsNone(normalizar_conkey_constraint(None, []))
        self.assertEqual((), normalizar_conkey_constraint([], []))
        self.assertNotEqual(
            normalizar_colunas_constraint([1, 2], ["a", "b"]),
            normalizar_colunas_constraint([2, 1], ["b", "a"]),
        )

    def test_colunas_constraint_rejeitam_catalogo_incompleto_ou_ambiguo(self):
        casos = (
            ([1], []), ([1], [None]), ([1], [""]), ([0], ["a"]),
            ([-1], ["a"]), ([True], ["a"]), ([1, 1], ["a", "a"]),
            ([1, 2], ["a", "a"]), ("1", ["a"]), ([1], "a"),
            (None, ["a"]),
        )
        for attnums, nomes in casos:
            with self.subTest(attnums=attnums, nomes=nomes):
                with self.assertRaises(ValueError):
                    normalizar_colunas_constraint(attnums, nomes)

    def test_oito_checks_possuem_colunas_reais_esperadas(self):
        checks = {
            constraint.nome: constraint.colunas
            for tabela in EXPECTED_LEDGER_SCHEMA.tabelas
            for constraint in tabela.constraints
            if constraint.tipo == "c"
        }
        self.assertEqual({
            "ck_schema_migrations__migration_id_preenchido": ("migration_id",),
            "ck_schema_migrations__modulo_preenchido": ("modulo",),
            "ck_schema_migrations__versao_positivo": ("versao",),
            "ck_schema_migrations__ordem_positivo": ("ordem",),
            "ck_schema_migrations__manifesto_versao_positivo": ("manifesto_versao",),
            "ck_schema_migration_exec__migration_id_preenchido": ("migration_id",),
            "ck_schema_migration_exec__tentativa_positivo": ("tentativa",),
            "ck_schema_migration_exec__situacao_preenchido": ("situacao",),
        }, checks)

    def test_check_com_coluna_vazia_errada_extra_ou_fora_de_ordem_e_rejeitado(self):
        tabela = next(
            item for item in EXPECTED_LEDGER_SCHEMA.tabelas
            if item.nome == "schema_migrations"
        )
        posicao = next(
            indice for indice, item in enumerate(tabela.constraints)
            if item.nome == "ck_schema_migrations__migration_id_preenchido"
        )
        casos = (
            ((), ()),
            ((3,), ("modulo",)),
            ((2, 3), ("migration_id", "modulo")),
            ((3, 2), ("modulo", "migration_id")),
        )
        for conkey, colunas in casos:
            constraints = list(tabela.constraints)
            constraints[posicao] = replace(
                constraints[posicao], conkey=conkey, colunas=colunas,
            )
            with self.subTest(conkey=conkey, colunas=colunas):
                self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(
                    tabela.nome, constraints=tuple(constraints),
                ))[0])

    def test_propriedades_fisicas_de_constraint_permanecem_estritas(self):
        constraint = EXPECTED_LEDGER_SCHEMA.tabelas[0].constraints[0]
        for campo in ("deferrable", "initially_deferred", "validated", "local", "no_inherit"):
            with self.subTest(campo=campo):
                alterada = replace(constraint, **{campo: not getattr(constraint, campo)})
                constraints = list(EXPECTED_LEDGER_SCHEMA.tabelas[0].constraints)
                constraints[0] = alterada
                self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(
                    EXPECTED_LEDGER_SCHEMA.tabelas[0].nome,
                    constraints=tuple(constraints),
                ))[0])
        with self.assertRaises(ValueError):
            replace(constraint, inheritance_count=-1)

    def test_indexdef_public_qualificado_e_omitido_sao_equivalentes(self):
        argumentos = dict(
            indice_schema="public", indice_nome="ix_exemplo",
            tabela_schema="public", tabela_nome="tabela_exemplo",
        )
        qualificado = canonicalizar_indexdef(
            "CREATE INDEX public.ix_exemplo ON public.tabela_exemplo USING btree (id)",
            **argumentos,
        )
        omitido = canonicalizar_indexdef(
            "CREATE INDEX ix_exemplo ON tabela_exemplo USING btree (id)",
            **argumentos,
        )
        self.assertEqual(qualificado, omitido)

    def test_indexdef_nao_relaxa_schema_nome_metodo_chave_ou_predicado(self):
        argumentos = dict(
            indice_schema="public", indice_nome="ix_exemplo",
            tabela_schema="public", tabela_nome="tabela_exemplo",
        )
        base = canonicalizar_indexdef(
            "CREATE UNIQUE INDEX ix_exemplo ON tabela_exemplo USING btree (id) WHERE ativo",
            **argumentos,
        )
        divergentes = (
            "CREATE UNIQUE INDEX outro.ix_exemplo ON tabela_exemplo USING btree (id) WHERE ativo",
            "CREATE UNIQUE INDEX ix_outro ON tabela_exemplo USING btree (id) WHERE ativo",
            "CREATE UNIQUE INDEX ix_exemplo ON outra.tabela_exemplo USING btree (id) WHERE ativo",
            "CREATE UNIQUE INDEX ix_exemplo ON tabela_outra USING btree (id) WHERE ativo",
            "CREATE UNIQUE INDEX ix_exemplo ON tabela_exemplo USING hash (id) WHERE ativo",
            "CREATE UNIQUE INDEX ix_exemplo ON tabela_exemplo USING btree (outro) WHERE ativo",
            "CREATE UNIQUE INDEX ix_exemplo ON tabela_exemplo USING btree (id) WHERE inativo",
            "CREATE INDEX ix_exemplo ON tabela_exemplo USING btree (id) WHERE ativo",
        )
        for definicao in divergentes:
            with self.subTest(definicao=definicao):
                self.assertNotEqual(base, canonicalizar_indexdef(definicao, **argumentos))

    def test_indexdef_nao_depende_de_search_path_conceitual(self):
        base = dict(
            indice_nome="ix_exemplo", tabela_nome="tabela_exemplo",
        )
        sem_qualificador = "CREATE INDEX ix_exemplo ON tabela_exemplo USING btree (id)"
        self.assertEqual(
            canonicalizar_indexdef(
                sem_qualificador, indice_schema="public", tabela_schema="public", **base,
            ),
            canonicalizar_indexdef(
                "CREATE INDEX public.ix_exemplo ON public.tabela_exemplo USING btree (id)",
                indice_schema="public", tabela_schema="public", **base,
            ),
        )
        self.assertNotEqual(
            canonicalizar_indexdef(
                sem_qualificador, indice_schema="outro", tabela_schema="outro", **base,
            ),
            canonicalizar_indexdef(
                "CREATE INDEX outro.ix_exemplo ON outro.tabela_exemplo USING btree (id)",
                indice_schema="outro", tabela_schema="outro", **base,
            ),
        )

    def test_seis_indices_aceitam_representacao_observada_sem_public_textual(self):
        tabelas = []
        total = 0
        for tabela in EXPECTED_LEDGER_SCHEMA.tabelas:
            indices = []
            for indice in tabela.indices:
                total += 1
                bruto = " ".join(indice.definicao)
                indices.append(replace(indice, definicao=canonicalizar_indexdef(
                    bruto,
                    indice_schema=indice.schema, indice_nome=indice.nome,
                    tabela_schema=indice.tabela_schema, tabela_nome=indice.tabela,
                )))
            tabelas.append(replace(tabela, indices=tuple(indices)))
        self.assertEqual(6, total)
        self.assertTrue(validar_assinatura_ledger(LedgerSchemaSnapshot(
            tuple(tabelas), EXPECTED_LEDGER_SCHEMA.sequencias,
        ))[0])

    def test_fixture_sintetica_das_divergencias_observadas_e_aceita(self):
        definicoes_pg15 = {
            "ck_schema_migrations__migration_id_preenchido":
                "CHECK (btrim(migration_id) <> ''::text)",
            "ck_schema_migrations__modulo_preenchido":
                "CHECK (btrim(modulo) <> ''::text)",
            "ck_schema_migrations__versao_positivo": "CHECK (versao > 0)",
            "ck_schema_migrations__ordem_positivo": "CHECK (ordem > 0)",
            "ck_schema_migrations__manifesto_versao_positivo":
                "CHECK (manifesto_versao > 0)",
            "ck_schema_migration_exec__migration_id_preenchido":
                "CHECK (btrim(migration_id) <> ''::text)",
            "ck_schema_migration_exec__tentativa_positivo": "CHECK (tentativa >= 0)",
            "ck_schema_migration_exec__situacao_preenchido":
                "CHECK (btrim(situacao) <> ''::text)",
        }
        tabelas = []
        for tabela in EXPECTED_LEDGER_SCHEMA.tabelas:
            constraints = tuple(
                replace(
                    constraint,
                    definicao=canonicalizar_constraintdef(
                        definicoes_pg15[constraint.nome], constraint.tipo
                    ),
                ) if constraint.tipo == "c" else constraint
                for constraint in tabela.constraints
            )
            indices = tuple(
                replace(indice, definicao=canonicalizar_indexdef(
                    " ".join(indice.definicao),
                    indice_schema=indice.schema, indice_nome=indice.nome,
                    tabela_schema=indice.tabela_schema, tabela_nome=indice.tabela,
                ))
                for indice in tabela.indices
            )
            tabelas.append(replace(
                tabela, constraints=constraints, indices=indices,
            ))
        self.assertTrue(validar_assinatura_ledger(LedgerSchemaSnapshot(
            tuple(tabelas), EXPECTED_LEDGER_SCHEMA.sequencias,
        ))[0])

    def test_sequencia_tabela_errada(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(tabela="outra"))[0])

    def test_sequencia_coluna_errada(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(coluna="outra"))[0])

    def test_sequencia_sem_dependencia(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(tipo_dependencia=None))[0])

    def test_sequencia_dependencia_errada(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(tipo_dependencia="a"))[0])

    def test_sequencia_schema_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(schema="outro"))[0])

    def test_sequencia_incremento_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(incremento=2))[0])

    def test_sequencia_cycle_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(cycle=True))[0])

    def test_sequencia_relkind_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(relkind="r"))[0])

    def test_sequencia_tabela_schema_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(tabela_schema="outro"))[0])

    def test_sequencia_refobjsubid_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(numero_coluna=2))[0])

    def test_sequencia_attidentity_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(identity="a"))[0])

    def test_sequencia_tipo_coluna_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(tipo_coluna="integer"))[0])

    def test_sequencia_tipo_fisico_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(tipo_sequencia="integer"))[0])

    def test_sequencia_inicio_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(inicio=2))[0])

    def test_sequencia_minimo_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(minimo=2))[0])

    def test_sequencia_maximo_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(maximo=100))[0])

    def test_sequencia_cache_errado(self):
        self.assertFalse(validar_assinatura_ledger(self.mutar_sequencia(cache=2))[0])

    def test_sequencia_ausente(self):
        snapshot = LedgerSchemaSnapshot(EXPECTED_LEDGER_SCHEMA.tabelas, EXPECTED_LEDGER_SCHEMA.sequencias[:1])
        self.assertFalse(validar_assinatura_ledger(snapshot)[0])

    def test_sequencia_extra(self):
        snapshot = LedgerSchemaSnapshot(
            EXPECTED_LEDGER_SCHEMA.tabelas,
            EXPECTED_LEDGER_SCHEMA.sequencias + (replace(EXPECTED_LEDGER_SCHEMA.sequencias[0], nome="extra"),),
        )
        self.assertFalse(validar_assinatura_ledger(snapshot)[0])

    def test_indice_desc_rejeitado(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        direcoes = ("DESC",) + indice.direcoes[1:]
        opcoes = (1,) + indice.indoptions[1:]
        self.assertFalse(validar_assinatura_ledger(
            self.mutar_indice(direcoes=direcoes, indoptions=opcoes)
        )[0])

    def test_indice_nulls_first_rejeitado(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        nulos = ("FIRST",) + indice.nulls[1:]
        opcoes = (2,) + indice.indoptions[1:]
        self.assertFalse(validar_assinatura_ledger(
            self.mutar_indice(nulls=nulos, indoptions=opcoes)
        )[0])

    def test_indice_operator_class_rejeitada(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        opclasses = (QualifiedName("public", indice.operator_classes[0].nome),) + indice.operator_classes[1:]
        self.assertFalse(validar_assinatura_ledger(
            self.mutar_indice(operator_classes=opclasses)
        )[0])

    def test_indice_collation_rejeitada(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        collations = (QualifiedName("public", "default"),) + indice.collations[1:]
        self.assertFalse(validar_assinatura_ledger(
            self.mutar_indice(collations=collations)
        )[0])

    def test_indice_definicao_rejeitada(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        indices = list(tabela.indices)
        indices[0] = replace(indices[0], definicao=canonicalizar_sql("CREATE INDEX outro ON public.x (id)"))
        self.assertFalse(validar_assinatura_ledger(self.mutar_tabela(tabela.nome, indices=tuple(indices)))[0])

    def test_quatro_combinacoes_direcao_nulls(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        combinacoes = (
            ("ASC", "LAST", 0), ("ASC", "FIRST", 2),
            ("DESC", "LAST", 1), ("DESC", "FIRST", 3),
        )
        assinaturas = []
        for direcao, nulos, opcao in combinacoes:
            assinaturas.append(replace(
                indice,
                direcoes=(direcao,) + indice.direcoes[1:],
                nulls=(nulos,) + indice.nulls[1:],
                indoptions=(opcao,) + indice.indoptions[1:],
            ))
        self.assertEqual(4, len(set(assinaturas)))
        self.assertEqual([True, False, False, False], [
            validar_assinatura_ledger(self.mutar_indice(
                direcoes=item.direcoes, nulls=item.nulls, indoptions=item.indoptions
            ))[0]
            for item in assinaturas
        ])

    def test_indoption_incoerente_rejeitado_na_construcao(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        with self.assertRaises(ValueError):
            replace(indice, direcoes=("DESC",) + indice.direcoes[1:])

    def test_include_extra_rejeitado(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        snapshot = self.mutar_indice(
            numero_atributos=indice.numero_atributos + 1,
            colunas_include=("extra",),
        )
        self.assertFalse(validar_assinatura_ledger(snapshot)[0])

    def test_indices_esperados_nao_possuem_include(self):
        for tabela in EXPECTED_LEDGER_SCHEMA.tabelas:
            for indice in tabela.indices:
                with self.subTest(indice=indice.nome):
                    self.assertEqual((), indice.colunas_include)
                    self.assertEqual(indice.numero_colunas_chave, indice.numero_atributos)

    def test_include_multiplas_e_ordem_sao_posicionais(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        primeiro = replace(
            indice, numero_atributos=indice.numero_atributos + 2,
            colunas_include=("extra_a", "extra_b"),
        )
        segundo = replace(primeiro, colunas_include=("extra_b", "extra_a"))
        self.assertNotEqual(primeiro, segundo)
        self.assertFalse(validar_assinatura_ledger(self.mutar_indice(
            numero_atributos=primeiro.numero_atributos,
            colunas_include=primeiro.colunas_include,
        ))[0])

    def test_include_nao_recebe_indoption(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        with self.assertRaises(ValueError):
            replace(
                indice, numero_atributos=indice.numero_atributos + 1,
                colunas_include=("extra",), indoptions=indice.indoptions + (0,),
            )

    def assert_campo_indice_divergente_ou_ausente(self, campo):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        snapshot = self.mutar_indice(**{campo: not getattr(indice, campo)})
        self.assertFalse(validar_assinatura_ledger(snapshot)[0])
        with self.assertRaises(ValueError):
            replace(indice, **{campo: None})

    def test_indimmediate_divergente_ou_ausente(self):
        self.assert_campo_indice_divergente_ou_ausente("immediate")

    def test_indisclustered_divergente_ou_ausente(self):
        self.assert_campo_indice_divergente_ou_ausente("clustered")

    def test_indisreplident_divergente_ou_ausente(self):
        self.assert_campo_indice_divergente_ou_ausente("replica_identity")

    def test_indnullsnotdistinct_divergente_ou_ausente(self):
        self.assert_campo_indice_divergente_ou_ausente("nulls_not_distinct")

    def test_indcheckxmin_valor_invertido_ou_ausente(self):
        self.assert_campo_indice_divergente_ou_ausente("check_xmin")

    def test_indcheckxmin_tipo_incorreto_rejeitado(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        for valor in (None, "false", 0, 1):
            with self.subTest(valor=valor):
                with self.assertRaises(ValueError):
                    replace(indice, check_xmin=valor)

    def test_indcheckxmin_divergente_em_um_dos_seis_indices(self):
        total = sum(len(tabela.indices) for tabela in EXPECTED_LEDGER_SCHEMA.tabelas)
        self.assertEqual(6, total)
        for tabela_indice, tabela in enumerate(EXPECTED_LEDGER_SCHEMA.tabelas):
            for indice_posicao, indice in enumerate(tabela.indices):
                with self.subTest(indice=indice.nome):
                    snapshot = self.mutar_indice(
                        tabela_indice=tabela_indice, indice=indice_posicao,
                        check_xmin=not indice.check_xmin,
                    )
                    self.assertFalse(validar_assinatura_ledger(snapshot)[0])

    def test_novos_campos_fisicos_esperados_em_todos_indices(self):
        for tabela in EXPECTED_LEDGER_SCHEMA.tabelas:
            for indice in tabela.indices:
                with self.subTest(indice=indice.nome):
                    self.assertIs(indice.immediate, True)
                    self.assertIs(indice.check_xmin, False)
                    self.assertIs(indice.clustered, False)
                    self.assertIs(indice.replica_identity, False)
                    self.assertIs(indice.nulls_not_distinct, False)

    def test_operator_class_nome_e_schema_posicionais(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        original = indice.operator_classes
        schema_errado = (QualifiedName("public", original[0].nome),) + original[1:]
        nome_errado = (QualifiedName(original[0].schema, "outro_ops"),) + original[1:]
        self.assertFalse(validar_assinatura_ledger(self.mutar_indice(operator_classes=schema_errado))[0])
        self.assertFalse(validar_assinatura_ledger(self.mutar_indice(operator_classes=nome_errado))[0])
        with self.assertRaises(ValueError):
            replace(indice, operator_classes=original[:-1])
        trocadas = tuple(reversed(original))
        self.assertFalse(validar_assinatura_ledger(
            self.mutar_indice(operator_classes=trocadas)
        )[0])

    def test_collation_schema_nome_ausencia_e_posicao(self):
        indice = EXPECTED_LEDGER_SCHEMA.tabelas[0].indices[0]
        original = indice.collations
        schema_errado = (QualifiedName("public", original[0].nome),) + original[1:]
        ausente = (None,) + original[1:]
        extra = original[:-1] + (QualifiedName("pg_catalog", "default"),)
        self.assertFalse(validar_assinatura_ledger(self.mutar_indice(collations=schema_errado))[0])
        self.assertFalse(validar_assinatura_ledger(self.mutar_indice(collations=ausente))[0])
        self.assertFalse(validar_assinatura_ledger(self.mutar_indice(collations=extra))[0])
        self.assertFalse(validar_assinatura_ledger(
            self.mutar_indice(collations=tuple(reversed(original)))
        )[0])

    def test_qualified_name_com_pontos_e_estrutural(self):
        self.assertNotEqual(QualifiedName("a.b", "c"), QualifiedName("a", "b.c"))
        self.assertEqual(QualifiedName("a.b", "c"), QualifiedName("a.b", "c"))
        with self.assertRaises(ValueError):
            QualifiedName("", "nome")
        with self.assertRaises(ValueError):
            QualifiedName("schema", "")


class SqlTokenizationTests(unittest.TestCase):
    def test_not_ativo_diferente_de_notativo(self):
        self.assertNotEqual(canonicalizar_sql("CHECK (NOT ativo)"), canonicalizar_sql("CHECK (notativo)"))

    def test_is_null_diferente_de_identificador(self):
        self.assertNotEqual(canonicalizar_sql("a IS NULL"), canonicalizar_sql("aisnull"))

    def test_operador_composto_nao_e_separado(self):
        self.assertNotEqual(tokenizar_sql("x >= 10"), tokenizar_sql("x > = 10"))
        self.assertNotEqual(tokenizar_sql("x ## y"), tokenizar_sql("x # # y"))

    def test_string_preservada(self):
        self.assertIn("'A  B'", tokenizar_sql("x = 'A  B'"))

    def test_identificador_delimitado_preservado(self):
        self.assertNotEqual(tokenizar_sql('"Campo"'), tokenizar_sql('"campo"'))

    def test_cast_text(self):
        self.assertIn("::", tokenizar_sql("x::text"))

    def test_espacos_e_quebras_inocentes(self):
        self.assertEqual(tokenizar_sql("x\n + 1"), tokenizar_sql(" x+1 "))

    def test_parenteses(self):
        self.assertEqual(("(", "x", ")"), tokenizar_sql("(x)"))

    def test_predicado_parcial(self):
        self.assertIn("where", tokenizar_sql("WHERE ativo IS TRUE"))

    def test_array_e_any(self):
        tokens = tokenizar_sql("situacao = ANY (ARRAY['A', 'B'])")
        self.assertIn("any", tokens); self.assertIn("array", tokens)

    def test_dollar_quote_preservado(self):
        self.assertIn("$$A B$$", tokenizar_sql("x = $$A B$$"))

    def test_comentario_ou_construcao_desconhecida_rejeitada(self):
        with self.assertRaises(ValueError):
            tokenizar_sql("x = 1 -- comentário")
        with self.assertRaises(ValueError):
            tokenizar_sql("`x`")


class RawCatalogContractTests(unittest.TestCase):
    def test_coletor_bruto_preserva_linhas_sinteticas_sem_modelo_esperado(self):
        captura = catalogo_bruto_sintetico()
        conexao = RawCatalogConnectionDouble(captura)
        coletada = pg_capture_module.coletar_catalogo_bruto(
            conexao, captura["metadados"],
        )
        self.assertEqual(captura, coletada)
        self.assertNotIn(
            "EXPECTED_LEDGER_SCHEMA",
            inspect.getsource(pg_capture_module.coletar_catalogo_bruto),
        )

    def test_serializer_preserva_strings_brutas_none_vazio_bool_inteiro_e_ordem(self):
        captura = catalogo_bruto_sintetico()
        captura["pg_index"][0]["indkey"] = [3, 2, 1]
        serializado = pg_capture_module.serializar_catalogo_bruto(captura)
        restaurado = json.loads(serializado.decode("utf-8"))
        self.assertEqual(
            "CHECK ((pg_catalog.btrim(campo) <> ''::text))",
            restaurado["pg_constraint"][0]["pg_get_constraintdef"],
        )
        self.assertEqual(
            "CREATE INDEX ix_sintetico ON schema_migrations USING btree (campo)",
            restaurado["pg_index"][0]["pg_get_indexdef"],
        )
        self.assertIsNone(restaurado["pg_constraint"][0]["conkey"])
        self.assertEqual([], restaurado["pg_constraint"][0]["colunas_resolvidas"])
        self.assertIs(restaurado["pg_index"][0]["indisvalid"], True)
        self.assertIs(type(restaurado["pg_index"][0]["indnkeyatts"]), int)
        self.assertEqual([3, 2, 1], restaurado["pg_index"][0]["indkey"])

    def test_serializer_e_deterministico_e_independente_do_modelo_esperado(self):
        captura = catalogo_bruto_sintetico()
        antes = pg_capture_module.serializar_catalogo_bruto(captura)
        with mock.patch.object(pg_capture_module, "EXPECTED_LEDGER_SCHEMA", object()):
            depois = pg_capture_module.serializar_catalogo_bruto(captura)
        self.assertEqual(antes, depois)
        self.assertNotIn(
            "EXPECTED_LEDGER_SCHEMA",
            inspect.getsource(pg_capture_module.serializar_catalogo_bruto),
        )
        self.assertNotIn(
            "EXPECTED_LEDGER_SCHEMA",
            inspect.getsource(pg_capture_module.coletar_catalogo_bruto),
        )

    def test_serializer_rejeita_campo_obrigatorio_ausente(self):
        captura = catalogo_bruto_sintetico()
        del captura["pg_index"][0]["pg_get_indexdef"]
        with self.assertRaises(ValueError):
            pg_capture_module.serializar_catalogo_bruto(captura)

    def test_serializer_rejeita_credencial_ou_dsn(self):
        captura = catalogo_bruto_sintetico()
        captura["metadados"]["container_image"] = "postgresql:" + "//exemplo-invalido"
        with self.assertRaises(ValueError):
            pg_capture_module.serializar_catalogo_bruto(captura)
        captura = catalogo_bruto_sintetico()
        captura["chave_com_token"] = "invalido"
        with self.assertRaises(ValueError):
            pg_capture_module.serializar_catalogo_bruto(captura)

    def test_captura_desabilitada_nao_escreve_arquivo(self):
        with tempfile.TemporaryDirectory() as diretorio:
            destino = Path(diretorio) / "nao_criado.json"
            self.assertFalse(pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                catalogo_bruto_sintetico(), destino, ambiente={},
            ))
            self.assertFalse(destino.exists())

    def test_captura_rejeita_destino_fora_de_tests_fixtures(self):
        with tempfile.TemporaryDirectory() as diretorio:
            destino = Path(diretorio) / "fora.json"
            ambiente = {
                "H2C4A2_ADMIN_DSN": "entregue-somente-em-memoria",
                "H2C4A2_CAPTURE_RAW_CATALOG": "1",
            }
            with self.assertRaises(ValueError):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino, ambiente=ambiente,
                )
            self.assertFalse(destino.exists())

    def test_serializer_nao_canonicaliza_expressao_ou_indexdef(self):
        captura = catalogo_bruto_sintetico()
        captura["pg_constraint"][0]["pg_get_expr_conbin"] = (
            "((pg_catalog.lower(campo)) = '(Texto)'::text)"
        )
        captura["pg_index"][0]["pg_get_indexdef"] = (
            "CREATE INDEX ix_sintetico ON public.schema_migrations "
            "USING btree (pg_catalog.lower(campo))"
        )
        restaurado = json.loads(
            pg_capture_module.serializar_catalogo_bruto(captura).decode("utf-8")
        )
        self.assertEqual(
            captura["pg_constraint"][0]["pg_get_expr_conbin"],
            restaurado["pg_constraint"][0]["pg_get_expr_conbin"],
        )
        self.assertEqual(
            captura["pg_index"][0]["pg_get_indexdef"],
            restaurado["pg_index"][0]["pg_get_indexdef"],
        )

    def test_interpretador_oficial_e_python_312(self):
        esperado = Path(r"C:\dev\recic4\.venv\Scripts\python.exe")
        self.assertEqual(esperado.resolve(), Path(sys.executable).resolve())
        self.assertEqual((3, 12), sys.version_info[:2])

    def test_sql_bruto_com_identificadores_sensiveis_legitimos_e_aceito(self):
        valores = (
            "CHECK (password = 'x')",
            "CREATE TABLE exemplo (token text)",
            "SELECT host_id FROM tabela",
            "username_type",
            "password_policy",
            "login_attempt",
        )
        for valor in valores:
            captura = catalogo_bruto_sintetico()
            captura["pg_constraint"][0]["pg_get_constraintdef"] = valor
            with self.subTest(valor=valor):
                restaurada = json.loads(
                    pg_capture_module.serializar_catalogo_bruto(captura).decode("utf-8")
                )
                self.assertEqual(
                    valor,
                    restaurada["pg_constraint"][0]["pg_get_constraintdef"],
                )

    def test_credenciais_unicode_nfkc_sao_rejeitadas_sem_mudar_dado_bruto(self):
        valores = (
            "Ａｕｔｈｏｒｉｚａｔｉｏｎ： Ｂｅａｒｅｒ abc",
            "Ａｕｔｈｏｒｉｚａｔｉｏｎ： Ｂａｓｉｃ abc",
            "ｐａｓｓｗｏｒｄ=segredo",
            "ｔｏｋｅｎ=segredo",
        )
        for valor in valores:
            captura = catalogo_bruto_sintetico()
            captura["pg_constraint"][0]["pg_get_constraintdef"] = valor
            with self.subTest(valor=valor), self.assertRaises(ValueError):
                pg_capture_module.serializar_catalogo_bruto(captura)
        with self.assertRaises(ValueError):
            pg_capture_module._validar_valor_json_bruto({
                "Ａｕｔｈｏｒｉｚａｔｉｏｎ": "abc",
            })


class PartialWriteDouble:
    def __init__(self, strategy):
        self.strategy = strategy
        self.bytes_written = bytearray()
        self.calls = 0

    def write(self, remaining):
        self.calls += 1
        value = self.strategy(self.calls, len(remaining))
        if type(value) is int and 0 < value <= len(remaining):
            self.bytes_written.extend(bytes(remaining[:value]))
        return value


class SecureRawCaptureTests(unittest.TestCase):
    @contextlib.contextmanager
    def destino_temporario(self):
        with tempfile.TemporaryDirectory() as diretorio:
            raiz = Path(diretorio) / "repositorio"
            fixtures = raiz / "tests" / "fixtures"
            fixtures.mkdir(parents=True)
            destino = fixtures / "h2c4a2_pg15_18_catalogo_bruto.json"
            with (
                mock.patch.object(pg_capture_module, "REPOSITORY_ROOT", raiz),
                mock.patch.object(pg_capture_module, "FIXTURES_DIR", fixtures),
                mock.patch.object(pg_capture_module, "RAW_CATALOG_PATH", destino),
            ):
                yield destino

    @staticmethod
    def ambiente(**adicionais):
        base = {
            "H2C4A2_ADMIN_DSN": "valor-ficticio-em-memoria",
            "H2C4A2_CAPTURE_RAW_CATALOG": "1",
        }
        base.update(adicionais)
        return base

    @staticmethod
    def adicionar_linha(captura, categoria):
        spec = pg_capture_module.INVENTORY_SPECS[categoria]
        linha = {}
        for campo in spec["campos"]:
            if campo.endswith("oid_evidencia"):
                linha[campo] = 100
            elif campo in pg_capture_module._INVENTORY_BOOL_FIELDS:
                linha[campo] = False
            elif campo == "roles":
                linha[campo] = []
            elif campo == "ordem":
                linha[campo] = 1.0
            else:
                linha[campo] = "valor-sintetico"
        captura["inventario_public"][categoria].append(linha)
        cobertura = next(
            item for item in captura["cobertura_inventario"]
            if item["categoria"] == categoria
        )
        cobertura["quantidade"] = 1
        cobertura["vazio"] = False

    def test_destino_e_obrigatorio(self):
        with self.assertRaises(TypeError):
            pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                catalogo_bruto_sintetico(), ambiente=self.ambiente(),
            )

    def test_gravacao_nova_atomica_e_verificada(self):
        captura = catalogo_bruto_sintetico()
        esperado = pg_capture_module.serializar_catalogo_bruto(captura)
        with self.destino_temporario() as destino:
            self.assertTrue(pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                captura, destino, ambiente=self.ambiente(),
            ))
            self.assertEqual(esperado, destino.read_bytes())
            self.assertEqual([], list(destino.parent.glob("*.tmp")))

    def test_destinos_relativo_alternativo_externo_e_dotdot_rejeitados(self):
        captura = catalogo_bruto_sintetico()
        with self.destino_temporario() as destino:
            casos = (
                Path("tests/fixtures/h2c4a2_pg15_18_catalogo_bruto.json"),
                destino.with_name("alternativo.json"),
                destino.parent.parent / "fora.json",
                destino.parent / "sub" / ".." / destino.name,
            )
            for caso in casos:
                with self.subTest(caso=caso), self.assertRaises(ValueError):
                    pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                        captura, caso, ambiente=self.ambiente(),
                    )

    def test_symlink_de_diretorio_ou_destino_e_rejeitado(self):
        captura = catalogo_bruto_sintetico()
        with self.destino_temporario() as destino:
            original = Path.is_symlink
            for alvo in (destino.parent, destino):
                with mock.patch.object(
                    Path, "is_symlink",
                    lambda caminho, alvo=alvo: caminho == alvo or original(caminho),
                ), self.assertRaises(ValueError):
                    pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                        captura, destino, ambiente=self.ambiente(),
                    )

    def test_colisao_de_temporario_nao_cria_destino(self):
        with self.destino_temporario() as destino:
            colisao = destino.parent / (".h2c4a2-" + "a" * 32 + ".tmp")
            colisao.write_bytes(b"ocupado")
            with mock.patch.object(
                pg_capture_module.secrets, "token_hex", return_value="a" * 32,
            ):
                with self.assertRaises(FileExistsError):
                    pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                        catalogo_bruto_sintetico(), destino,
                        ambiente=self.ambiente(),
                    )
            self.assertFalse(destino.exists())
            self.assertEqual(b"ocupado", colisao.read_bytes())

    def test_falhas_antes_do_replace_removem_temporario(self):
        falhas = (
            ("criacao", "_criar_temporario_exclusivo"),
            ("escrita", "_escrever_todos"),
            ("flush", "_flush_arquivo"),
            ("fsync", "_fsync_arquivo"),
            ("releitura", "_ler_arquivo_seguro"),
        )
        for nome, funcao in falhas:
            with self.subTest(nome=nome), self.destino_temporario() as destino:
                with mock.patch.object(
                    pg_capture_module, funcao, side_effect=OSError("falha sintética"),
                ), self.assertRaises(OSError):
                    pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                        catalogo_bruto_sintetico(), destino,
                        ambiente=self.ambiente(),
                    )
                self.assertFalse(destino.exists())
                self.assertEqual([], list(destino.parent.glob(".h2c4a2-*.tmp")))

    def test_escrita_parcial_e_completada_pelo_loop(self):
        dados = b"0123456789abcdef"
        estrategias = {
            "um_byte": lambda _, restante: 1,
            "dois_bytes": lambda _, restante: min(2, restante),
            "metade": lambda _, restante: max(1, restante // 2),
            "variavel": lambda chamada, restante: min((1, 3, 2, 5)[(chamada - 1) % 4], restante),
            "todos": lambda _, restante: restante,
        }
        for nome, estrategia in estrategias.items():
            double = PartialWriteDouble(estrategia)
            with self.subTest(nome=nome):
                pg_capture_module._escrever_todos(double, dados)
                self.assertEqual(dados, bytes(double.bytes_written))
                self.assertGreaterEqual(double.calls, 1)

    def test_write_zero_negativo_bool_none_excessivo_float_e_string_rejeitados(self):
        invalidos = (0, -1, True, False, None, 9, 1.5, "1")
        for retorno in invalidos:
            double = PartialWriteDouble(lambda _chamada, _restante, retorno=retorno: retorno)
            with self.subTest(retorno=retorno), self.assertRaises(OSError):
                pg_capture_module._escrever_todos(double, b"12345678")
            self.assertEqual(b"", bytes(double.bytes_written))
            self.assertEqual(1, double.calls)

    def test_erro_depois_de_escritas_parciais_remove_temporario(self):
        def falhar_depois_de_parcial(arquivo, dados):
            self.assertEqual(3, arquivo.write(dados[:3]))
            raise OSError("write-principal")

        with self.destino_temporario() as destino:
            with mock.patch.object(
                pg_capture_module, "_escrever_todos", falhar_depois_de_parcial,
            ), self.assertRaisesRegex(OSError, "write-principal"):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino, ambiente=self.ambiente(),
                )
            self.assertFalse(destino.exists())
            self.assertEqual([], list(destino.parent.glob(".h2c4a2-*.tmp")))

    def test_write_falha_e_close_falha_preserva_erro_de_write(self):
        fechar_original = pg_capture_module._fechar_arquivo

        def fechar_e_falhar(arquivo):
            fechar_original(arquivo)
            raise OSError("close-secundario")

        with self.destino_temporario() as destino:
            with (
                mock.patch.object(
                    pg_capture_module, "_escrever_todos",
                    side_effect=OSError("write-principal"),
                ),
                mock.patch.object(pg_capture_module, "_fechar_arquivo", fechar_e_falhar),
                self.assertRaisesRegex(OSError, "write-principal") as contexto,
            ):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino, ambiente=self.ambiente(),
                )
            self.assertTrue(any("fechar" in nota for nota in contexto.exception.__notes__))

    def test_fsync_falha_e_close_falha_preserva_erro_de_fsync(self):
        fechar_original = pg_capture_module._fechar_arquivo

        def fechar_e_falhar(arquivo):
            fechar_original(arquivo)
            raise OSError("close-secundario")

        with self.destino_temporario() as destino:
            with (
                mock.patch.object(
                    pg_capture_module, "_fsync_arquivo",
                    side_effect=OSError("fsync-principal"),
                ),
                mock.patch.object(pg_capture_module, "_fechar_arquivo", fechar_e_falhar),
                self.assertRaisesRegex(OSError, "fsync-principal") as contexto,
            ):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino, ambiente=self.ambiente(),
                )
            self.assertTrue(any("fechar" in nota for nota in contexto.exception.__notes__))

    def test_validacao_falha_e_unlink_falha_preserva_erro_de_validacao(self):
        with self.destino_temporario() as destino:
            with (
                mock.patch.object(
                    pg_capture_module, "validar_bytes_catalogo_bruto",
                    side_effect=[catalogo_bruto_sintetico(), ValueError("validacao-principal")],
                ),
                mock.patch.object(Path, "unlink", side_effect=OSError("unlink-secundario")),
                self.assertRaisesRegex(ValueError, "validacao-principal") as contexto,
            ):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino, ambiente=self.ambiente(),
                )
            self.assertTrue(any("temporário" in nota for nota in contexto.exception.__notes__))

    def test_replace_falha_e_unlink_falha_preserva_erro_de_replace(self):
        with self.destino_temporario() as destino:
            with (
                mock.patch.object(os, "replace", side_effect=OSError("replace-principal")),
                mock.patch.object(Path, "unlink", side_effect=OSError("unlink-secundario")),
                self.assertRaisesRegex(OSError, "replace-principal") as contexto,
            ):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino, ambiente=self.ambiente(),
                )
            self.assertTrue(any("temporário" in nota for nota in contexto.exception.__notes__))

    def test_falha_de_validacao_do_temporario_limpa_arquivo(self):
        original = pg_capture_module.validar_bytes_catalogo_bruto
        chamadas = 0
        def falhar_na_segunda(dados):
            nonlocal chamadas
            chamadas += 1
            if chamadas == 2:
                raise ValueError("falha sintética")
            return original(dados)
        with self.destino_temporario() as destino:
            with mock.patch.object(
                pg_capture_module, "validar_bytes_catalogo_bruto",
                side_effect=falhar_na_segunda,
            ), self.assertRaises(ValueError):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino,
                    ambiente=self.ambiente(),
                )
            self.assertFalse(destino.exists())
            self.assertEqual([], list(destino.parent.glob(".h2c4a2-*.tmp")))

    def test_replace_falha_preserva_evidencia_anterior(self):
        anterior = pg_capture_module.serializar_catalogo_bruto(catalogo_bruto_sintetico())
        with self.destino_temporario() as destino:
            destino.write_bytes(anterior)
            ambiente = self.ambiente(
                H2C4A2_REPLACE_RAW_CATALOG="1",
                H2C4A2_EXPECTED_EXISTING_SHA256=hashlib.sha256(anterior).hexdigest(),
            )
            with mock.patch.object(os, "replace", side_effect=OSError("falha")):
                with self.assertRaises(OSError):
                    pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                        catalogo_bruto_sintetico(), destino, ambiente=ambiente,
                    )
            self.assertEqual(anterior, destino.read_bytes())
            self.assertEqual([], list(destino.parent.glob(".h2c4a2-*.tmp")))

    def test_existente_exige_autorizacao_hash_e_arquivo_valido(self):
        anterior = pg_capture_module.serializar_catalogo_bruto(catalogo_bruto_sintetico())
        with self.destino_temporario() as destino:
            destino.write_bytes(anterior)
            with self.assertRaises(FileExistsError):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino,
                    ambiente=self.ambiente(),
                )
            for hash_invalido in (None, "x" * 64, "0" * 64):
                with self.subTest(hash=hash_invalido), self.assertRaises(ValueError):
                    pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                        catalogo_bruto_sintetico(), destino,
                        ambiente=self.ambiente(
                            H2C4A2_REPLACE_RAW_CATALOG="1",
                            H2C4A2_EXPECTED_EXISTING_SHA256=hash_invalido,
                        ),
                    )
            self.assertEqual(anterior, destino.read_bytes())

    def test_hash_anterior_formatos_invalidos_preservam_destino(self):
        anterior = pg_capture_module.serializar_catalogo_bruto(catalogo_bruto_sintetico())
        invalidos = (
            None,
            hashlib.sha256(anterior).hexdigest().upper(),
            "a" * 63,
            "a" * 65,
            "g" * 64,
            hashlib.sha256(b"outro-conteudo").hexdigest(),
        )
        for checksum in invalidos:
            with self.subTest(checksum=checksum), self.destino_temporario() as destino:
                destino.write_bytes(anterior)
                with self.assertRaises(ValueError):
                    pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                        catalogo_bruto_sintetico(), destino,
                        ambiente=self.ambiente(
                            H2C4A2_REPLACE_RAW_CATALOG="1",
                            H2C4A2_EXPECTED_EXISTING_SHA256=checksum,
                        ),
                    )
                self.assertEqual(anterior, destino.read_bytes())

    def test_anterior_alterado_entre_leituras_e_detectado(self):
        anterior = pg_capture_module.serializar_catalogo_bruto(catalogo_bruto_sintetico())
        alterado = anterior.replace(b'capture-1111', b'capture-2222', 1)
        with self.destino_temporario() as destino:
            destino.write_bytes(anterior)
            original = pg_capture_module._ler_arquivo_seguro
            leituras_destino = 0

            def trocar_antes_da_revalidacao(caminho):
                nonlocal leituras_destino
                if caminho == destino:
                    leituras_destino += 1
                    if leituras_destino == 2:
                        return alterado
                return original(caminho)

            with mock.patch.object(
                pg_capture_module, "_ler_arquivo_seguro",
                side_effect=trocar_antes_da_revalidacao,
            ), self.assertRaises(ValueError):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino,
                    ambiente=self.ambiente(
                        H2C4A2_REPLACE_RAW_CATALOG="1",
                        H2C4A2_EXPECTED_EXISTING_SHA256=hashlib.sha256(anterior).hexdigest(),
                    ),
                )
            self.assertEqual(anterior, destino.read_bytes())

    def test_anterior_json_invalido_duplicado_ou_com_segredo_e_preservado(self):
        casos = (
            b"nao-json",
            b'{"a":1,"a":2}\n',
            b'{"Authorization":"Bearer abc"}\n',
        )
        for anterior in casos:
            with self.subTest(anterior=anterior), self.destino_temporario() as destino:
                destino.write_bytes(anterior)
                ambiente = self.ambiente(
                    H2C4A2_REPLACE_RAW_CATALOG="1",
                    H2C4A2_EXPECTED_EXISTING_SHA256=hashlib.sha256(anterior).hexdigest(),
                )
                with self.assertRaises(ValueError):
                    pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                        catalogo_bruto_sintetico(), destino, ambiente=ambiente,
                    )
                self.assertEqual(anterior, destino.read_bytes())

    def test_substituicao_atomica_completa(self):
        anterior_captura = catalogo_bruto_sintetico()
        anterior = pg_capture_module.serializar_catalogo_bruto(anterior_captura)
        nova = catalogo_bruto_sintetico()
        nova["metadados"]["capture_id"] = "capture-" + "2" * 32
        esperado = pg_capture_module.serializar_catalogo_bruto(nova)
        with self.destino_temporario() as destino:
            destino.write_bytes(anterior)
            self.assertTrue(pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                nova, destino,
                ambiente=self.ambiente(
                    H2C4A2_REPLACE_RAW_CATALOG="1",
                    H2C4A2_EXPECTED_EXISTING_SHA256=hashlib.sha256(anterior).hexdigest(),
                ),
            ))
            self.assertEqual(esperado, destino.read_bytes())

    def test_falha_na_verificacao_final_nao_informa_sucesso(self):
        original = pg_capture_module._ler_arquivo_seguro
        chamadas = 0
        def corromper_verificacao_final(caminho):
            nonlocal chamadas
            chamadas += 1
            dados = original(caminho)
            if chamadas == 2:
                return dados + b"corrompido"
            return dados
        with self.destino_temporario() as destino:
            with mock.patch.object(
                pg_capture_module, "_ler_arquivo_seguro",
                side_effect=corromper_verificacao_final,
            ), self.assertRaises(ValueError):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino,
                    ambiente=self.ambiente(),
                )

    def test_temporario_truncado_acrescido_substituido_ou_nao_regular_e_detectado(self):
        captura = catalogo_bruto_sintetico()
        preparado = pg_capture_module.serializar_catalogo_bruto(captura)
        casos = (
            preparado[:-1],
            preparado + b"x",
            b'{"substituido":true}\n',
            ValueError("temporario-nao-regular"),
        )
        for resultado in casos:
            with self.subTest(resultado=repr(resultado)), self.destino_temporario() as destino:
                efeito = resultado if isinstance(resultado, BaseException) else None
                with mock.patch.object(
                    pg_capture_module, "_ler_arquivo_seguro",
                    return_value=None if efeito else resultado,
                    side_effect=efeito,
                ), self.assertRaises((ValueError, TypeError)):
                    pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                        captura, destino, ambiente=self.ambiente(),
                    )
                self.assertFalse(destino.exists())
                self.assertEqual([], list(destino.parent.glob(".h2c4a2-*.tmp")))

    def test_reparse_point_simulado_e_rejeitado(self):
        with self.destino_temporario() as destino:
            with mock.patch.object(
                pg_capture_module, "_e_reparse_point",
                side_effect=lambda caminho: caminho == destino.parent,
            ), self.assertRaises(ValueError):
                pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    catalogo_bruto_sintetico(), destino, ambiente=self.ambiente(),
                )
            self.assertFalse(destino.exists())

    def test_authorization_basic_bearer_e_chaves_sensiveis_rejeitados(self):
        valores = (
            "Authorization: Bearer abc", "authorization: bearer abc",
            "Authorization: Basic abc", "Bearer abc", "Basic abc",
        )
        for valor in valores:
            captura = catalogo_bruto_sintetico()
            captura["pg_constraint"][0]["pg_get_constraintdef"] = valor
            with self.subTest(valor=valor), self.assertRaises(ValueError):
                pg_capture_module.serializar_catalogo_bruto(captura)
        for chave in ("Authorization", "authorization", "password", "dsn", "host"):
            captura = catalogo_bruto_sintetico()
            captura["metadados"][chave] = "abc"
            with self.subTest(chave=chave), self.assertRaises(ValueError):
                pg_capture_module.serializar_catalogo_bruto(captura)

    def test_metadados_pessoais_imagem_digest_e_capture_id_rejeitados(self):
        casos = (
            ("container_image", "meu-computador/postgres:15"),
            ("container_image", "HOST-PESSOAL"),
            ("container_image", "registry.local/postgres:15"),
            ("container_image", "postgres:15@host-pessoal"),
            ("postgres_version", "15.18 host-pessoal"),
            ("container_image_digest", "sha256:invalido"),
            ("capture_id", "capture-host-pessoal"),
        )
        for campo, valor in casos:
            captura = catalogo_bruto_sintetico()
            captura["metadados"][campo] = valor
            with self.subTest(campo=campo, valor=valor), self.assertRaises(ValueError):
                pg_capture_module.serializar_catalogo_bruto(captura)

    def test_imagem_postgres_15_e_digest_valido_sao_aceitos(self):
        captura = catalogo_bruto_sintetico()
        captura["metadados"]["container_image"] = "postgres:15"
        captura["metadados"]["container_image_digest"] = "sha256:" + "a" * 64
        restaurada = json.loads(
            pg_capture_module.serializar_catalogo_bruto(captura).decode("utf-8")
        )
        self.assertEqual("postgres:15", restaurada["metadados"]["container_image"])
        self.assertEqual(
            "sha256:" + "a" * 64,
            restaurada["metadados"]["container_image_digest"],
        )

    def test_password_em_dsn_e_authorization_unicode_sao_rejeitados(self):
        valores = (
            "postgresql://usuario:senha@host/banco",
            "password=segredo host=servidor",
            "Ａｕｔｈｏｒｉｚａｔｉｏｎ： Ｂｅａｒｅｒ abc",
        )
        for valor in valores:
            captura = catalogo_bruto_sintetico()
            captura["pg_constraint"][0]["pg_get_constraintdef"] = valor
            with self.subTest(valor=valor), self.assertRaises(ValueError):
                pg_capture_module.serializar_catalogo_bruto(captura)

    def test_tupla_none_bool_inteiro_e_ordem(self):
        captura = catalogo_bruto_sintetico()
        captura["pg_constraint"][0]["colunas_resolvidas"] = ()
        captura["pg_index"][0]["indkey"] = (3, 2, 1)
        restaurado = json.loads(
            pg_capture_module.serializar_catalogo_bruto(captura).decode("utf-8")
        )
        self.assertEqual([], restaurado["pg_constraint"][0]["colunas_resolvidas"])
        self.assertIsNone(restaurado["pg_constraint"][0]["conkey"])
        self.assertEqual([3, 2, 1], restaurado["pg_index"][0]["indkey"])
        self.assertIs(type(restaurado["pg_index"][0]["indisvalid"]), bool)
        self.assertIs(type(restaurado["pg_index"][0]["indnkeyatts"]), int)

    def test_inventario_vazio_e_nao_vazio_de_todas_categorias(self):
        captura = catalogo_bruto_sintetico()
        for categoria in pg_capture_module.INVENTORY_SPECS:
            self.adicionar_linha(captura, categoria)
        coletada = pg_capture_module.coletar_catalogo_bruto(
            RawCatalogConnectionDouble(captura), captura["metadados"],
        )
        self.assertEqual(captura, coletada)
        for item in coletada["cobertura_inventario"]:
            self.assertEqual(1, item["quantidade"])
            self.assertFalse(item["vazio"])

    def test_inventario_distingue_funcao_e_procedure(self):
        captura = catalogo_bruto_sintetico()
        self.adicionar_linha(captura, "rotinas")
        funcao = captura["inventario_public"]["rotinas"][0]
        funcao["prokind"] = "f"
        procedure = dict(funcao, oid_evidencia=101, nome="procedure_sintetica", prokind="p")
        captura["inventario_public"]["rotinas"].append(procedure)
        cobertura = next(
            item for item in captura["cobertura_inventario"]
            if item["categoria"] == "rotinas"
        )
        cobertura["quantidade"] = 2
        coletada = pg_capture_module.coletar_catalogo_bruto(
            RawCatalogConnectionDouble(captura), captura["metadados"],
        )
        self.assertEqual(["f", "p"], [
            item["prokind"] for item in coletada["inventario_public"]["rotinas"]
        ])

    def test_categoria_ausente_tipo_incorreto_e_bool_como_inteiro_rejeitados(self):
        captura = catalogo_bruto_sintetico()
        del captura["inventario_public"]["rotinas"]
        with self.assertRaises(ValueError):
            pg_capture_module.serializar_catalogo_bruto(captura)
        captura = catalogo_bruto_sintetico()
        self.adicionar_linha(captura, "relacoes")
        captura["inventario_public"]["relacoes"][0]["relispartition"] = 1
        with self.assertRaises(ValueError):
            pg_capture_module.serializar_catalogo_bruto(captura)

    def test_captura_desabilitada_nao_valida_nem_grava(self):
        with self.destino_temporario() as destino:
            for ambiente in ({}, {"H2C4A2_ADMIN_DSN": "ficticio"}, {
                "H2C4A2_ADMIN_DSN": "ficticio",
                "H2C4A2_CAPTURE_RAW_CATALOG": "0",
            }):
                self.assertFalse(pg_capture_module.gravar_catalogo_bruto_se_habilitado(
                    {}, destino, ambiente=ambiente,
                ))
            self.assertFalse(destino.exists())

    def test_documentacao_declara_matriz_e_limites_de_cobertura(self):
        documento = (ROOT / "tests" / "fixtures" / "README_H2C4A2.md").read_text(
            encoding="utf-8",
        )
        self.assertIn("Matriz explícita de cobertura", documento)
        self.assertIn("pg_statistic_ext", documento)
        self.assertIn("security labels", documento.casefold())
        self.assertNotIn("inventário integral absoluto", documento.casefold())


class CatalogCursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def execute(self, sql, params=()):
        self.connection.executions.append((sql, params))
        if "pg_catalog.count(*)" in sql and "pg_catalog.pg_index" in sql:
            self.rows = [(self.connection.index_catalog_supported,)]
        elif "SELECT EXISTS" in sql:
            self.rows = [(self.connection.public_exists,)]
        elif "c.relkind IN" in sql:
            self.rows = list(self.connection.relations)
        elif "FROM pg_catalog.pg_proc" in sql:
            self.rows = list(self.connection.routines)
        elif "FROM pg_catalog.pg_type" in sql:
            self.rows = list(self.connection.types)
        elif "FROM pg_catalog.pg_trigger" in sql:
            self.rows = list(self.connection.triggers)
        elif "FROM pg_catalog.pg_policy" in sql:
            self.rows = list(self.connection.policies)
        elif "FROM pg_catalog.pg_rewrite" in sql:
            self.rows = list(self.connection.rules)
        elif "FROM pg_catalog.pg_depend" in sql:
            self.rows = list(self.connection.extension_objects)
        elif "SELECT n.nspname, c.relname, c.relkind" in sql:
            tabela = self.connection.tables.get(params[1])
            self.rows = [(tabela.schema, tabela.nome, tabela.relkind)] if tabela else []
        elif "a.attnum, a.attname" in sql:
            tabela = self.connection.tables[params[1]]
            self.rows = [
                (c.posicao, c.nome, c.tipo, c.tamanho, c.precisao, c.escala,
                 c.nullable, " ".join(c.default) if c.default else None,
                 c.identity, c.generation)
                for c in tabela.colunas
            ]
        elif "pg_get_constraintdef" in sql:
            tabela = self.connection.tables[params[1]]
            if params[1] in self.connection.constraint_rows:
                self.rows = list(self.connection.constraint_rows[params[1]])
            else:
                self.rows = [
                    (c.nome, c.tipo,
                     None if c.conkey is None else list(c.conkey),
                     list(c.colunas), " ".join(c.definicao), c.deferrable,
                     c.initially_deferred, c.validated, c.local,
                     c.inheritance_count, c.no_inherit)
                    for c in tabela.constraints
                ]
        elif "FROM pg_catalog.pg_index" in sql:
            tabela = self.connection.tables[params[1]]
            self.rows = [
                (i.schema, i.tabela_schema, i.nome, i.tabela, i.metodo, i.unique,
                 i.primary, i.exclusion, i.immediate, i.check_xmin,
                 i.valid, i.ready, i.live,
                 i.clustered, i.replica_identity, i.nulls_not_distinct,
                 i.numero_colunas_chave, i.numero_atributos,
                 list(i.colunas_chave), list(i.colunas_include),
                 " ".join(i.expressoes) if i.expressoes else None,
                 " ".join(i.predicado) if i.predicado else None,
                 i.vinculado_constraint,
                 [item.schema if item else None for item in i.collations],
                 [item.nome if item else None for item in i.collations],
                 [item.schema for item in i.operator_classes],
                 [item.nome for item in i.operator_classes],
                 list(i.direcoes), list(i.nulls),
                 list(i.indoptions), " ".join(i.definicao))
                for i in tabela.indices
            ]
        elif "FROM pg_catalog.pg_class AS s" in sql and "pg_catalog.pg_sequence" in sql:
            sequencia = self.connection.sequences.get(params[1])
            if params[1] in self.connection.sequence_rows:
                self.rows = list(self.connection.sequence_rows[params[1]])
            else:
                self.rows = [tuple(vars(sequencia).values())] if sequencia else []
        elif "FROM public.schema_migrations" in sql:
            self.rows = list(self.connection.applied_rows)
        elif "FROM public.schema_migration_execucoes" in sql:
            self.rows = list(self.connection.execution_rows)
        else:
            raise AssertionError(f"Consulta inesperada: {sql}")

    def fetchall(self):
        return list(self.rows)

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def close(self):
        self.connection.closed_cursors += 1


class CatalogConnection:
    def __init__(self, **kwargs):
        self.public_exists = kwargs.get("public_exists", True)
        self.relations = kwargs.get("relations", ())
        self.routines = kwargs.get("routines", ())
        self.types = kwargs.get("types", ())
        self.triggers = kwargs.get("triggers", ())
        self.policies = kwargs.get("policies", ())
        self.rules = kwargs.get("rules", ())
        self.extension_objects = kwargs.get("extension_objects", ())
        self.tables = kwargs.get("tables", {})
        self.sequences = kwargs.get("sequences", {})
        self.sequence_rows = kwargs.get("sequence_rows", {})
        self.constraint_rows = kwargs.get("constraint_rows", {})
        self.index_catalog_supported = kwargs.get("index_catalog_supported", True)
        self.applied_rows = kwargs.get("applied_rows", ())
        self.execution_rows = kwargs.get("execution_rows", ())
        self.executions = []
        self.closed_cursors = 0
        self.cursor_calls = 0

    def cursor(self):
        self.cursor_calls += 1
        return CatalogCursor(self)


class PreflightInventoryTests(unittest.TestCase):
    def classificar(self, **kwargs):
        conexao = CatalogConnection(**kwargs)
        snapshot = coletar_snapshot(conexao)
        return classificar_preflight(snapshot, carregar_manifesto()), conexao

    def assert_desconhecido(self, **kwargs):
        resultado, _ = self.classificar(**kwargs)
        self.assertEqual("BANCO_DESCONHECIDO", resultado.classificacao.value)

    def test_public_ausente(self):
        self.assert_desconhecido(public_exists=False)

    def test_funcao(self):
        self.assert_desconhecido(routines=(("f", "funcao", ""),))

    def test_procedure(self):
        self.assert_desconhecido(routines=(("p", "procedimento", ""),))

    def test_aggregate(self):
        self.assert_desconhecido(routines=(("a", "agregado", "integer"),))

    def test_window_function(self):
        self.assert_desconhecido(routines=(("w", "janela", ""),))

    def test_enum(self):
        self.assert_desconhecido(types=(("e", "estado"),))

    def test_domain(self):
        self.assert_desconhecido(types=(("d", "codigo"),))

    def test_composto_explicito(self):
        self.assert_desconhecido(types=(("c", "endereco"),))

    def test_tipo_base(self):
        self.assert_desconhecido(types=(("b", "tipo_base"),))

    def test_range(self):
        self.assert_desconhecido(types=(("r", "faixa"),))

    def test_multirange(self):
        self.assert_desconhecido(types=(("m", "multifaixa"),))

    def test_trigger(self):
        self.assert_desconhecido(triggers=(("gatilho", "tabela"),))

    def test_policy(self):
        self.assert_desconhecido(policies=(("politica", "tabela"),))

    def test_regra(self):
        self.assert_desconhecido(rules=(("regra", "tabela"),))

    def test_objeto_extensao(self):
        self.assert_desconhecido(extension_objects=(("ext", "pg_proc", 10),))

    def test_view(self):
        self.assert_desconhecido(relations=(("v", "visao"),))

    def test_sequence(self):
        self.assert_desconhecido(relations=(("S", "sequencia"),))

    def test_tabela_aplicacao(self):
        self.assert_desconhecido(relations=(("r", "usuarios"),))

    def test_public_realmente_vazio(self):
        resultado, conexao = self.classificar()
        self.assertEqual("BANCO_NOVO", resultado.classificacao.value)
        self.assertTrue(all(sql.lstrip().upper().startswith("SELECT") for sql, _ in conexao.executions))

    def test_filtros_de_objetos_automaticos_e_schemas(self):
        _, conexao = self.classificar()
        sql = " ".join(item[0] for item in conexao.executions)
        self.assertIn("n.nspname = %s", sql)
        self.assertIn("NOT (t.typelem <> 0 AND t.typcategory = 'A')", sql)
        self.assertIn("c.relkind = 'c'", sql)
        self.assertIn("NOT t.tgisinternal", sql)
        self.assertIn("r.rulename <> %s", sql)

    def test_ledger_parcial_primeira_tabela(self):
        self.assert_desconhecido(relations=(("r", "schema_migrations"),))

    def test_ledger_parcial_segunda_tabela(self):
        self.assert_desconhecido(relations=(("r", "schema_migration_execucoes"),))

    def test_coleta_ledger_exato(self):
        aplicadas, execucoes = registros_validos()
        linha_execucao = list(vars(execucoes[0]).values())
        linha_execucao[9] = str(execucoes[0].request_id)
        conexao = CatalogConnection(
            relations=(
                ("r", "schema_migrations"), ("r", "schema_migration_execucoes"),
                ("S", "schema_migrations_id_seq"),
                ("S", "schema_migration_execucoes_id_seq"),
            ),
            tables={t.nome: t for t in EXPECTED_LEDGER_SCHEMA.tabelas},
            sequences={s.nome: s for s in EXPECTED_LEDGER_SCHEMA.sequencias},
            applied_rows=tuple(tuple(vars(x).values()) for x in aplicadas),
            execution_rows=(tuple(linha_execucao),),
        )
        snapshot = coletar_snapshot(conexao)
        self.assertEqual(execucoes, snapshot.execucoes)
        self.assertIs(type(snapshot.execucoes[0].request_id), UUID)
        resultado = classificar_preflight(snapshot, carregar_manifesto())
        self.assertEqual("BANCO_CONTROLADO", resultado.classificacao.value)
        self.assertEqual(conexao.cursor_calls, conexao.closed_cursors)
        consultas = " ".join(sql for sql, _ in conexao.executions)
        for trecho in (
            "pg_catalog.pg_sequence", "pg_catalog.pg_depend", "ix.indoption",
            "ix.indclass", "ix.indcollation", "ix.indnkeyatts", "ix.indnatts",
            "ix.indimmediate", "ix.indisclustered", "ix.indisreplident",
            "ix.indnullsnotdistinct", "pg_catalog.pg_get_indexdef", "con.conkey",
            "con.convalidated", "con.conislocal", "con.coninhcount",
            "con.connoinherit", "LEFT JOIN pg_catalog.pg_attribute AS a",
        ):
            self.assertIn(trecho, consultas)
        linha_invalida = list(linha_execucao)
        linha_invalida[9] = "uuid-invalido"
        with self.assertRaises(InvalidLedgerError):
            coletar_conteudo_ledger(CatalogConnection(
                applied_rows=tuple(tuple(vars(x).values()) for x in aplicadas),
                execution_rows=(tuple(linha_invalida),),
            ))

    def test_conkey_incompleto_no_catalogo_falha_fechado(self):
        tabela = EXPECTED_LEDGER_SCHEMA.tabelas[0]
        constraint = tabela.constraints[0]
        conexao = CatalogConnection(
            tables={item.nome: item for item in EXPECTED_LEDGER_SCHEMA.tabelas},
            sequences={item.nome: item for item in EXPECTED_LEDGER_SCHEMA.sequencias},
            constraint_rows={tabela.nome: [(
                constraint.nome, constraint.tipo, [1], [None],
                " ".join(constraint.definicao), constraint.deferrable,
                constraint.initially_deferred, constraint.validated,
                constraint.local, constraint.inheritance_count,
                constraint.no_inherit,
            )]},
        )
        with self.assertRaises(DatabaseConnectionError):
            coletar_assinatura_ledger(conexao)

    def test_catalogo_sem_indnullsnotdistinct_rejeitado(self):
        aplicadas, execucoes = registros_validos()
        conexao = CatalogConnection(
            relations=(
                ("r", "schema_migrations"), ("r", "schema_migration_execucoes"),
                ("S", "schema_migrations_id_seq"),
                ("S", "schema_migration_execucoes_id_seq"),
            ),
            tables={t.nome: t for t in EXPECTED_LEDGER_SCHEMA.tabelas},
            sequences={s.nome: s for s in EXPECTED_LEDGER_SCHEMA.sequencias},
            applied_rows=tuple(tuple(vars(x).values()) for x in aplicadas),
            execution_rows=tuple(tuple(vars(x).values()) for x in execucoes),
            index_catalog_supported=False,
        )
        with self.assertRaises(DatabaseConnectionError):
            coletar_snapshot(conexao)

    def test_catalogo_sem_indcheckxmin_rejeitado(self):
        conexao = CatalogConnection(index_catalog_supported=False)
        with self.assertRaises(DatabaseConnectionError):
            coletar_assinatura_ledger(conexao)
        consulta, parametros = conexao.executions[0]
        self.assertIn("pg_catalog.pg_index", consulta)
        self.assertEqual(5, parametros[0])
        self.assertIn("indcheckxmin", parametros[1])

    def test_sequencia_com_propriedade_multipla_rejeitada(self):
        sequencias = {s.nome: s for s in EXPECTED_LEDGER_SCHEMA.sequencias}
        primeira = EXPECTED_LEDGER_SCHEMA.sequencias[0]
        conexao = CatalogConnection(
            relations=(
                ("r", "schema_migrations"), ("r", "schema_migration_execucoes"),
                ("S", "schema_migrations_id_seq"),
                ("S", "schema_migration_execucoes_id_seq"),
            ),
            tables={t.nome: t for t in EXPECTED_LEDGER_SCHEMA.tabelas},
            sequences=sequencias,
            sequence_rows={primeira.nome: (
                tuple(vars(primeira).values()), tuple(vars(primeira).values()),
            )},
        )
        with self.assertRaises(DatabaseConnectionError):
            coletar_snapshot(conexao)

    def test_segunda_execucao_com_sequences_e_controlada(self):
        resultado = classificar_preflight(snapshot_controlado(), carregar_manifesto())
        self.assertEqual("BANCO_CONTROLADO", resultado.classificacao.value)
        self.assertTrue(resultado.pode_prosseguir)

    def test_apenas_uma_sequencia_do_ledger(self):
        aplicadas, execucoes = registros_validos()
        objetos = LEDGER_TABLES | frozenset((next(iter(LEDGER_SEQUENCE_OBJECTS)),))
        snapshot = PreflightSnapshot(
            True, objetos,
            assinatura_ledger=LedgerSchemaSnapshot(
                EXPECTED_LEDGER_SCHEMA.tabelas, EXPECTED_LEDGER_SCHEMA.sequencias[:1]
            ),
            migrations_aplicadas=aplicadas, execucoes=execucoes,
        )
        self.assertEqual(
            "BANCO_DESCONHECIDO",
            classificar_preflight(snapshot, carregar_manifesto()).classificacao.value,
        )

    def test_sequencia_extra_bloqueia(self):
        snapshot = replace(
            snapshot_controlado(),
            objetos_encontrados=LEDGER_OBJECTS | frozenset(("relation:S:extra",)),
        )
        self.assertEqual(
            "BANCO_DESCONHECIDO",
            classificar_preflight(snapshot, carregar_manifesto()).classificacao.value,
        )

    def test_sequencia_homonima_sem_propriedade_bloqueia(self):
        sequencias = list(EXPECTED_LEDGER_SCHEMA.sequencias)
        sequencias[0] = replace(sequencias[0], tabela=None, coluna=None, tipo_dependencia=None)
        snapshot = replace(
            snapshot_controlado(),
            assinatura_ledger=LedgerSchemaSnapshot(
                EXPECTED_LEDGER_SCHEMA.tabelas, tuple(sequencias)
            ),
        )
        self.assertEqual(
            "BANCO_DESCONHECIDO",
            classificar_preflight(snapshot, carregar_manifesto()).classificacao.value,
        )


class LedgerHistoryTests(unittest.TestCase):
    def setUp(self):
        self.manifesto = carregar_manifesto()
        self.aplicadas, self.execucoes = registros_validos(self.manifesto)

    def validar(self, aplicadas=None, execucoes=None):
        snapshot = PreflightSnapshot(
            True, LEDGER_TABLES, assinatura_ledger=EXPECTED_LEDGER_SCHEMA,
            migrations_aplicadas=self.aplicadas if aplicadas is None else aplicadas,
            execucoes=self.execucoes if execucoes is None else execucoes,
        )
        return validar_conteudo_ledger(snapshot, self.manifesto)[0]

    def test_m0001_coerente(self):
        self.assertTrue(self.validar())

    def test_aplicada_sem_execucao(self):
        self.assertFalse(self.validar(execucoes=()))

    def test_execucao_sem_aplicada(self):
        self.assertFalse(self.validar(aplicadas=()))

    def test_checksum_divergente(self):
        self.assertFalse(self.validar(aplicadas=(replace(self.aplicadas[0], checksum_sha256="0" * 64),)))

    def test_ordem_divergente(self):
        self.assertFalse(self.validar(aplicadas=(replace(self.aplicadas[0], ordem=2),)))

    def test_modulo_divergente(self):
        self.assertFalse(self.validar(aplicadas=(replace(self.aplicadas[0], modulo="outro"),)))

    def test_migration_desconhecida(self):
        self.assertFalse(self.validar(aplicadas=(replace(self.aplicadas[0], migration_id="M9999"),)))

    def test_duas_execucoes_aplicadas(self):
        outra = replace(self.execucoes[0], tentativa=1)
        self.assertFalse(self.validar(execucoes=self.execucoes + (outra,)))

    def test_m0001_nao_aceita_falha_persistida(self):
        falha = replace(
            self.execucoes[0], tentativa=1, situacao="FALHOU",
            erro_codigo="MIGRATION_FALHOU", erro_sanitizado="Falha segura.",
        )
        self.assertFalse(self.validar(execucoes=(falha,) + self.execucoes))

    def test_tentativa_zero_falhou_e_um_aplicou(self):
        falha = replace(
            self.execucoes[0], situacao="FALHOU", duracao_ms=1,
            erro_codigo="MIGRATION_FALHOU", erro_sanitizado="Falha segura.",
        )
        sucesso = replace(self.execucoes[0], tentativa=1)
        self.assertFalse(self.validar(execucoes=(falha, sucesso)))

    def test_tentativa_zero_iniciada_e_um_aplicou(self):
        iniciada = replace(
            self.execucoes[0], situacao="INICIADA", concluida_em=None,
            duracao_ms=None,
        )
        sucesso = replace(self.execucoes[0], tentativa=1)
        self.assertFalse(self.validar(execucoes=(iniciada, sucesso)))

    def test_somente_tentativa_um_aplicada(self):
        self.assertFalse(self.validar(execucoes=(replace(self.execucoes[0], tentativa=1),)))

    def test_tentativa_zero_checksum_divergente(self):
        ruim = replace(self.execucoes[0], checksum_sha256="0" * 64)
        self.assertFalse(self.validar(execucoes=(ruim,)))

    def test_tentativa_zero_sem_autorregistro(self):
        self.assertFalse(self.validar(aplicadas=(), execucoes=self.execucoes))

    def test_autorregistro_sem_tentativa_zero(self):
        self.assertFalse(self.validar(execucoes=()))

    def test_estado_invalido(self):
        self.assertFalse(self.validar(execucoes=(replace(self.execucoes[0], situacao="QUALQUER"),)))

    def test_duracao_incompativel(self):
        self.assertFalse(self.validar(execucoes=(replace(self.execucoes[0], duracao_ms=-1),)))

    def test_erro_em_aplicada(self):
        ruim = replace(self.execucoes[0], erro_codigo="ERRO", erro_sanitizado="Erro.")
        self.assertFalse(self.validar(execucoes=(ruim,)))

    def test_iniciada_com_conclusao(self):
        ruim = replace(self.execucoes[0], situacao="INICIADA")
        self.assertFalse(self.validar(execucoes=(ruim,)))

    def test_tentativa_duplicada(self):
        self.assertFalse(self.validar(execucoes=self.execucoes + self.execucoes))

    def test_termino_anterior_ao_inicio(self):
        inicio = datetime(2026, 8, 4, tzinfo=timezone.utc)
        ruim = replace(self.execucoes[0], iniciada_em=inicio, concluida_em=NOW)
        self.assertFalse(self.validar(execucoes=(ruim,)))

    def test_request_id_invalido(self):
        self.assertFalse(self.validar(execucoes=(replace(self.execucoes[0], request_id="texto"),)))

    def test_dependencia_nao_aplicada(self):
        m0, m1 = self.manifesto.operacoes
        m2 = replace(m1, identificador="M0002", ordem_global=2, dependencias=("M0001",))
        manifesto = replace(self.manifesto, operacoes=(m0, m1, m2))
        aplicada = replace(
            self.aplicadas[0], migration_id="M0002", ordem=2, modulo=m2.modulo,
            checksum_sha256=m2.checksum,
        )
        execucao = replace(
            self.execucoes[0], migration_id="M0002", tentativa=1,
            checksum_sha256=m2.checksum,
        )
        snapshot = PreflightSnapshot(
            True, LEDGER_TABLES, assinatura_ledger=EXPECTED_LEDGER_SCHEMA,
            migrations_aplicadas=(aplicada,), execucoes=(execucao,),
        )
        valido, motivo = validar_conteudo_ledger(snapshot, manifesto)
        self.assertFalse(valido)
        self.assertIn("Dependência", motivo)


class FakeCursor:
    def __init__(self, connection, numero):
        self.connection = connection
        self.numero = numero
        self.rows = []

    def execute(self, sql, params=None):
        self.connection.executions.append((sql, params, self.connection.autocommit))
        if not self.connection.autocommit:
            self.connection.transaction_status = TRANSACTION_STATUS_INTRANS
        if self.connection.fail_execute_contains and self.connection.fail_execute_contains in sql:
            self.connection.transaction_status = TRANSACTION_STATUS_INERROR
            raise RuntimeError("password=segredo")
        if "pg_try_advisory_lock" in sql:
            if self.connection.fail_lock:
                raise RuntimeError("token=segredo")
            resposta = self.connection.lock_answers.pop(0) if self.connection.lock_answers else True
            self.connection.locked = resposta
            self.rows = [(resposta,)]
        elif "pg_advisory_unlock" in sql:
            if self.connection.unlock_exception:
                raise RuntimeError("authorization: bearer segredo")
            resposta = not self.connection.unlock_false
            if resposta:
                self.connection.locked = False
            self.rows = [(resposta,)]
        elif sql.startswith("UPDATE public.schema_migration_execucoes"):
            self.rows = [(1,)]
        else:
            self.rows = []

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)

    def close(self):
        self.connection.closed_cursors += 1
        if (
            self.connection.fail_cursor_close
            or self.connection.fail_cursor_close_number == self.numero
        ):
            raise RuntimeError("falha ao fechar")


class FakeConnection:
    def __init__(
        self, *, closed=0, autocommit=False, status=TRANSACTION_STATUS_IDLE,
        fail_cursor_number=None, fail_commit=False, fail_rollback=False,
        fail_autocommit_value=None,
    ):
        self.closed = closed
        self._autocommit = autocommit
        self.transaction_status = status
        self.fail_cursor_number = fail_cursor_number
        self.fail_commit = fail_commit
        self.fail_rollback = fail_rollback
        self.fail_autocommit_value = fail_autocommit_value
        self.fail_execute_contains = None
        self.fail_cursor_close = False
        self.fail_cursor_close_number = None
        self.fail_lock = False
        self.unlock_false = False
        self.unlock_exception = False
        self.lock_answers = []
        self.locked = False
        self.cursor_calls = 0
        self.closed_cursors = 0
        self.commits = 0
        self.rollbacks = 0
        self.executions = []
        self.autocommit_changes = []

    @property
    def autocommit(self):
        return self._autocommit

    @autocommit.setter
    def autocommit(self, valor):
        if self.fail_autocommit_value is valor:
            raise RuntimeError("falha autocommit")
        if self.transaction_status != TRANSACTION_STATUS_IDLE:
            raise RuntimeError("transação ativa")
        self._autocommit = valor
        self.autocommit_changes.append(valor)

    def get_transaction_status(self):
        return self.transaction_status

    def cursor(self):
        self.cursor_calls += 1
        if self.fail_cursor_number == self.cursor_calls:
            raise RuntimeError("postgresql://usuario:senha@host/banco")
        return FakeCursor(self, self.cursor_calls)

    def commit(self):
        self.commits += 1
        if self.fail_commit:
            raise RuntimeError("falha commit")
        self.transaction_status = TRANSACTION_STATUS_IDLE

    def rollback(self):
        self.rollbacks += 1
        if self.fail_rollback:
            raise RuntimeError("falha rollback")
        self.transaction_status = TRANSACTION_STATUS_IDLE


def conteudo_do_runner(conexao, *, valido=True):
    manifesto = carregar_manifesto()
    inserts = [(sql, params) for sql, params, _ in conexao.executions if sql.startswith("INSERT INTO public")]
    if len(inserts) < 2:
        return registros_validos(manifesto)
    mparams = inserts[0][1]
    eparams = inserts[1][1]
    aplicada = AppliedMigration(
        mparams[0], mparams[3], mparams[1], mparams[4], mparams[2], mparams[8],
        mparams[5], mparams[6], mparams[7],
    )
    execucao = MigrationExecution(
        eparams[0], eparams[1], eparams[2], eparams[3], eparams[4], eparams[5],
        eparams[6], eparams[7], eparams[8], UUID(eparams[9]),
        eparams[10], eparams[11], eparams[12],
    )
    if not valido:
        execucao = replace(execucao, checksum_sha256="0" * 64)
    return (aplicada,), (execucao,)


def criar_runner(
    conexao, *, snapshot=None, schema=EXPECTED_LEDGER_SCHEMA,
    content_valid=True, events=None, event_logger=None, sql_loader=carregar_sql_validado,
):
    relogio = iter((0.0, 0.005)).__next__
    return MigrationRunner(
        conexao,
        snapshot_factory=lambda _: snapshot or snapshot_novo(),
        schema_factory=lambda _: schema,
        content_factory=lambda _: conteudo_do_runner(conexao, valido=content_valid),
        sql_loader=sql_loader,
        clock=relogio,
        event_logger=event_logger or (
            lambda evento, **campos: (events.append((evento, campos)) if events is not None else None)
        ),
    )


class ExplodingLogger:
    def __init__(self, eventos=None):
        self.eventos = set(eventos or ())

    def __call__(self, evento, **campos):
        if not self.eventos or evento in self.eventos:
            raise RuntimeError("logger indisponível")

    debug = info = warning = error = exception = __call__


class ConnectionStateTests(unittest.TestCase):
    def test_conexao_fechada(self):
        with self.assertRaises(ConnectionClosedError):
            ConnectionState(FakeConnection(closed=1)).validar_entrada()

    def test_conexao_em_transacao(self):
        with self.assertRaises(ConnectionNotIdleError):
            ConnectionState(FakeConnection(status=TRANSACTION_STATUS_INTRANS)).validar_entrada()

    def test_conexao_em_erro(self):
        with self.assertRaises(ConnectionNotIdleError):
            ConnectionState(FakeConnection(status=TRANSACTION_STATUS_INERROR)).validar_entrada()

    def test_autocommit_false_restaurado(self):
        conexao = FakeConnection(autocommit=False)
        estado = ConnectionState(conexao)
        estado.validar_entrada(); estado.preparar_operacoes_sem_transacao(); estado.restaurar()
        self.assertFalse(conexao.autocommit)

    def test_autocommit_true_restaurado(self):
        conexao = FakeConnection(autocommit=True)
        estado = ConnectionState(conexao)
        estado.validar_entrada(); estado.preparar_operacoes_sem_transacao(); estado.restaurar()
        self.assertTrue(conexao.autocommit)

    def test_falha_mudanca_autocommit(self):
        conexao = FakeConnection(fail_autocommit_value=True)
        estado = ConnectionState(conexao); estado.validar_entrada()
        with self.assertRaises(ConnectionStateError):
            estado.preparar_operacoes_sem_transacao()

    def test_restauracao_falha(self):
        conexao = FakeConnection(autocommit=False)
        estado = ConnectionState(conexao); estado.validar_entrada(); estado.preparar_operacoes_sem_transacao()
        conexao.fail_autocommit_value = False
        with self.assertRaises(ConnectionRestoreError):
            estado.restaurar()

    def test_commit_e_rollback_deixam_idle(self):
        conexao = FakeConnection(); estado = ConnectionState(conexao); estado.validar_entrada()
        estado.iniciar_migration(); conexao.transaction_status = TRANSACTION_STATUS_INTRANS
        estado.confirmar_migration(); self.assertEqual(TRANSACTION_STATUS_IDLE, estado.status())
        conexao.transaction_status = TRANSACTION_STATUS_INERROR
        estado.reverter_migration(); self.assertEqual(TRANSACTION_STATUS_IDLE, estado.status())


class LockTests(unittest.TestCase):
    def test_chave_deterministica(self):
        self.assertEqual(-8482190501243477735, derivar_chave_lock())

    def test_aquisicao_e_liberacao(self):
        conexao = FakeConnection(autocommit=True)
        lock = AdvisoryLock(conexao, timeout_segundos=0)
        lock.adquirir(); lock.liberar()
        self.assertFalse(lock.adquirido)
        self.assertEqual(conexao.cursor_calls, conexao.closed_cursors)

    def test_timeout_sem_espera_longa(self):
        conexao = FakeConnection(autocommit=True); conexao.lock_answers = [False]
        with self.assertRaises(LockTimeoutError):
            AdvisoryLock(conexao, timeout_segundos=0).adquirir()

    def test_unlock_false(self):
        conexao = FakeConnection(autocommit=True); lock = AdvisoryLock(conexao)
        lock.adquirir(); conexao.unlock_false = True
        with self.assertRaises(LockError):
            lock.liberar()

    def test_unlock_excecao(self):
        conexao = FakeConnection(autocommit=True); lock = AdvisoryLock(conexao)
        lock.adquirir(); conexao.unlock_exception = True
        with self.assertRaises(LockError):
            lock.liberar()

    def test_falha_criacao_cursor(self):
        with self.assertRaises(LockError):
            AdvisoryLock(FakeConnection(autocommit=True, fail_cursor_number=1)).adquirir()

    def test_mesma_conexao(self):
        conexao = FakeConnection(autocommit=True)
        lock = AdvisoryLock(conexao)
        self.assertIs(conexao, lock.conexao)


class RunnerTests(unittest.TestCase):
    def test_plano_m0000_m0001(self):
        self.assertEqual(["EXECUTOR", "PENDENTE"], [x.estado for x in MigrationRunner().mostrar_plano()])

    def test_operacao_desabilitada_no_plano(self):
        runner = MigrationRunner()
        ops = list(runner.manifesto.operacoes)
        ops[1] = replace(ops[1], habilitada=False)
        runner.manifesto = replace(runner.manifesto, operacoes=tuple(ops))
        self.assertEqual("DESABILITADA", runner.mostrar_plano()[1].estado)

    def test_desabilitada_nao_executada(self):
        conexao = FakeConnection()
        runner = criar_runner(conexao)
        ops = list(runner.manifesto.operacoes); ops[1] = replace(ops[1], habilitada=False)
        runner.manifesto = replace(runner.manifesto, operacoes=tuple(ops))
        with self.assertRaises(UnknownDatabaseError):
            runner.executar()
        self.assertFalse(any("CREATE TABLE" in sql for sql, _, _ in conexao.executions))

    def test_conexao_explicita(self):
        with self.assertRaises(DatabaseConnectionError):
            MigrationRunner().executar()

    def test_trabalho_anterior_nem_confirmado_nem_revertido(self):
        conexao = FakeConnection(status=TRANSACTION_STATUS_INTRANS)
        with self.assertRaises(ConnectionNotIdleError):
            criar_runner(conexao).executar()
        self.assertEqual((0, 0, 0), (conexao.cursor_calls, conexao.commits, conexao.rollbacks))

    def test_m0001_sucesso_autocommit_false(self):
        conexao = FakeConnection(autocommit=False)
        resultado = criar_runner(conexao).executar()
        self.assertEqual(("M0001",), resultado.aplicadas)
        self.assertEqual((1, 0), (conexao.commits, conexao.rollbacks))
        self.assertFalse(conexao.autocommit)
        self.assertEqual(TRANSACTION_STATUS_IDLE, conexao.transaction_status)

    def test_m0001_sucesso_autocommit_true(self):
        conexao = FakeConnection(autocommit=True)
        criar_runner(conexao).executar()
        self.assertTrue(conexao.autocommit)
        self.assertEqual(TRANSACTION_STATUS_IDLE, conexao.transaction_status)

    def test_duracao_real_persistida(self):
        conexao = FakeConnection(); criar_runner(conexao).executar()
        inserts = [(sql, params) for sql, params, _ in conexao.executions if sql.startswith("INSERT INTO public")]
        self.assertEqual(5, inserts[0][1][6])
        self.assertEqual(5, inserts[1][1][5])

    def test_controlado_sem_ddl_restaura_estado(self):
        conexao = FakeConnection(autocommit=False)
        resultado = criar_runner(conexao, snapshot=snapshot_controlado()).executar()
        self.assertEqual(("M0001",), resultado.ignoradas)
        self.assertFalse(conexao.autocommit)
        self.assertEqual(TRANSACTION_STATUS_IDLE, conexao.transaction_status)
        self.assertFalse(any("CREATE TABLE" in sql for sql, _, _ in conexao.executions))

    def test_hash_aplicado_divergente(self):
        snap = snapshot_controlado()
        ruim = replace(snap.migrations_aplicadas[0], checksum_sha256="0" * 64)
        with self.assertRaises(AppliedMigrationHashMismatchError):
            criar_runner(FakeConnection(), snapshot=replace(snap, migrations_aplicadas=(ruim,))).executar()

    def test_banco_desconhecido_sem_ddl(self):
        conexao = FakeConnection()
        with self.assertRaises(UnknownDatabaseError):
            criar_runner(conexao, snapshot=snapshot_novo(("usuarios",))).executar()
        self.assertFalse(any("CREATE TABLE" in sql for sql, _, _ in conexao.executions))

    def test_falha_antes_da_migration_nao_faz_rollback(self):
        conexao = FakeConnection()
        with self.assertRaises(UnknownDatabaseError):
            criar_runner(conexao, snapshot=snapshot_novo(("usuarios",))).executar()
        self.assertEqual(0, conexao.rollbacks)

    def test_estrutura_pos_ddl_invalida_rollback(self):
        conexao = FakeConnection()
        with self.assertRaises(InvalidLedgerError):
            criar_runner(conexao, schema=LedgerSchemaSnapshot(())).executar()
        self.assertEqual(1, conexao.rollbacks)

    def test_autorregistro_falha_rollback(self):
        conexao = FakeConnection(); conexao.fail_execute_contains = "INSERT INTO public.schema_migrations"
        with self.assertRaises(MigrationExecutionError):
            criar_runner(conexao).executar()
        self.assertEqual(1, conexao.rollbacks)

    def test_pos_condicao_conteudo_falha_rollback(self):
        conexao = FakeConnection()
        with self.assertRaises(InvalidLedgerError):
            criar_runner(conexao, content_valid=False).executar()
        self.assertEqual(1, conexao.rollbacks)

    def test_commit_falha_rollback(self):
        conexao = FakeConnection(fail_commit=True)
        with self.assertRaises(ConnectionStateError):
            criar_runner(conexao).executar()
        self.assertEqual(1, conexao.rollbacks)

    def test_rollback_falha_nao_mascara_principal(self):
        conexao = FakeConnection(fail_rollback=True)
        conexao.fail_execute_contains = "CREATE TABLE"
        with self.assertRaises(MigrationExecutionError):
            criar_runner(conexao).executar()

    def test_cursor_migration_falha_sanitizado(self):
        conexao = FakeConnection(fail_cursor_number=2)
        with self.assertRaises(MigrationExecutionError) as contexto:
            criar_runner(conexao).executar()
        self.assertNotIn("senha", str(contexto.exception))

    def test_unlock_false_sem_erro_principal(self):
        conexao = FakeConnection(); conexao.unlock_false = True
        with self.assertRaises(LockError):
            criar_runner(conexao, snapshot=snapshot_controlado()).executar()

    def test_unlock_nao_mascara_erro_principal(self):
        conexao = FakeConnection(); conexao.unlock_false = True
        with self.assertRaises(UnknownDatabaseError):
            criar_runner(conexao, snapshot=snapshot_novo(("usuarios",))).executar()

    def test_log_liberado_somente_no_sucesso(self):
        eventos = []; conexao = FakeConnection(); conexao.unlock_false = True
        with self.assertRaises(LockError):
            criar_runner(conexao, snapshot=snapshot_controlado(), events=eventos).executar()
        self.assertNotIn("migration_lock_liberado", [evento for evento, _ in eventos])

    def test_log_liberado_em_sucesso(self):
        eventos = []; criar_runner(FakeConnection(), snapshot=snapshot_controlado(), events=eventos).executar()
        self.assertIn("migration_lock_liberado", [evento for evento, _ in eventos])

    def test_timeout_impede_preflight_e_ddl(self):
        conexao = FakeConnection(); conexao.lock_answers = [False]
        runner = criar_runner(conexao)
        runner.timeout_lock_segundos = 0
        with self.assertRaises(LockTimeoutError):
            runner.executar()
        self.assertFalse(any("CREATE TABLE" in sql for sql, _, _ in conexao.executions))

    def test_cursores_fechados_no_sucesso(self):
        conexao = FakeConnection(); criar_runner(conexao).executar()
        self.assertEqual(conexao.cursor_calls, conexao.closed_cursors)

    def test_public_qualificado_e_search_path_irrelevante(self):
        conexao = FakeConnection(); criar_runner(conexao).executar()
        sql = " ".join(item[0] for item in conexao.executions)
        self.assertIn("CREATE TABLE public.schema_migrations", sql)
        self.assertIn("INSERT INTO public.schema_migrations", sql)

    def test_historicas_nao_executadas(self):
        conexao = FakeConnection(); criar_runner(conexao).executar()
        sql = " ".join(item[0] for item in conexao.executions)
        self.assertNotRegex(sql, r"H00[1-9]|H01[01]")

    def test_logger_falha_no_sucesso_sem_mudar_resultado(self):
        resultado = criar_runner(FakeConnection(), event_logger=ExplodingLogger()).executar()
        self.assertTrue(resultado.sucesso)

    def test_logger_no_rollback_nao_mascara_migration(self):
        conexao = FakeConnection(fail_rollback=True)
        conexao.fail_execute_contains = "CREATE TABLE"
        with self.assertRaises(MigrationExecutionError):
            criar_runner(
                conexao, event_logger=ExplodingLogger({"migration_rollback_falhou"})
            ).executar()

    def test_logger_no_unlock_nao_mascara_erro_principal(self):
        conexao = FakeConnection(); conexao.unlock_false = True
        with self.assertRaises(UnknownDatabaseError):
            criar_runner(
                conexao, snapshot=snapshot_novo(("usuarios",)),
                event_logger=ExplodingLogger({"migration_lock_liberacao_falhou"}),
            ).executar()

    def test_logger_no_unlock_nao_mascara_lock_release(self):
        conexao = FakeConnection(); conexao.unlock_false = True
        with self.assertRaises(LockReleaseError):
            criar_runner(
                conexao, snapshot=snapshot_controlado(), event_logger=ExplodingLogger()
            ).executar()

    def test_logger_na_restauracao_nao_mascara_principal(self):
        conexao = FakeConnection(autocommit=False, fail_autocommit_value=False)
        with self.assertRaises(UnknownDatabaseError):
            criar_runner(
                conexao, snapshot=snapshot_novo(("usuarios",)),
                event_logger=ExplodingLogger({"migration_conexao_restauracao_falhou"}),
            ).executar()

    def test_restauracao_principal_preservada_com_logger_falhando(self):
        conexao = FakeConnection(autocommit=False, fail_autocommit_value=False)
        with self.assertRaises(ConnectionRestoreError):
            criar_runner(
                conexao, snapshot=snapshot_controlado(), event_logger=ExplodingLogger()
            ).executar()

    def test_logger_no_evento_final_nao_mascara_principal(self):
        with self.assertRaises(UnknownDatabaseError):
            criar_runner(
                FakeConnection(), snapshot=snapshot_novo(("usuarios",)),
                event_logger=ExplodingLogger({"migration_runner_falhou"}),
            ).executar()

    def test_logger_na_falha_commit_preserva_connection_state(self):
        with self.assertRaises(ConnectionStateError):
            criar_runner(
                FakeConnection(fail_commit=True), event_logger=ExplodingLogger()
            ).executar()

    def test_logger_no_fechamento_cursor_nao_mascara_principal(self):
        conexao = FakeConnection(); conexao.fail_execute_contains = "CREATE TABLE"
        conexao.fail_cursor_close_number = 2
        with self.assertRaises(MigrationExecutionError):
            criar_runner(
                conexao, event_logger=ExplodingLogger({"migration_cursor_fechamento_falhou"})
            ).executar()

    def test_logger_no_timeout_nao_mascara_timeout(self):
        conexao = FakeConnection(); conexao.lock_answers = [False]
        runner = criar_runner(conexao, event_logger=ExplodingLogger())
        runner.timeout_lock_segundos = 0
        with self.assertRaises(LockTimeoutError):
            runner.executar()

    def test_sql_alterado_depois_do_manifesto_e_rejeitado(self):
        fx = ManifestFixture()
        try:
            runner = criar_runner(FakeConnection())
            runner.manifesto = carregar_manifesto(fx.save())
            fx.sql.write_text("CREATE TABLE public.outra (id integer);\n", encoding="utf-8")
            with self.assertRaises(ChecksumMismatchError):
                runner.executar()
            self.assertFalse(any(
                sql.lstrip().upper().startswith("CREATE TABLE")
                for sql, _, _ in runner.conexao.executions
            ))
        finally:
            fx.close()

    def test_cursor_executa_exatamente_texto_do_artefato(self):
        texto = SQL.read_text(encoding="utf-8")
        chamadas = []
        artefatos = []
        def loader(**argumentos):
            chamadas.append(argumentos)
            artefato = carregar_sql_validado(**argumentos)
            artefatos.append(artefato)
            return artefato
        conexao = FakeConnection()
        criar_runner(conexao, sql_loader=loader).executar()
        self.assertEqual(1, len(chamadas))
        self.assertTrue(any(sql == texto for sql, _, _ in conexao.executions))
        self.assertEqual(texto.encode("utf-8"), artefatos[0].bytes_normalizados)

    def test_objeto_falso_rejeitado_antes_do_ddl(self):
        conexao = FakeConnection()
        with self.assertRaises(ChecksumMismatchError):
            criar_runner(conexao, sql_loader=lambda **_: object()).executar()
        self.assertFalse(any(
            sql.lstrip().upper().startswith("CREATE TABLE")
            for sql, _, _ in conexao.executions
        ))

    def test_subclasse_de_artefato_rejeitada_antes_do_ddl(self):
        class ArtefatoDerivado(ValidatedSql):
            pass
        conexao = FakeConnection()
        def loader(**argumentos):
            valido = carregar_sql_validado(**argumentos)
            return clonar_artefato_por_bypass(valido, classe=ArtefatoDerivado)
        with self.assertRaises(ChecksumMismatchError):
            criar_runner(conexao, sql_loader=loader).executar()
        self.assertFalse(any(
            sql.lstrip().upper().startswith("CREATE TABLE")
            for sql, _, _ in conexao.executions
        ))

    def test_artefato_de_outra_operacao_rejeitado_antes_do_ddl(self):
        conexao = FakeConnection()
        def loader(**argumentos):
            artefato = carregar_sql_validado(**argumentos)
            return clonar_artefato_por_bypass(artefato, operacao_id="M9999")
        with self.assertRaises(ChecksumMismatchError):
            criar_runner(conexao, sql_loader=loader).executar()
        self.assertFalse(any(
            sql.lstrip().upper().startswith("CREATE TABLE")
            for sql, _, _ in conexao.executions
        ))

    def test_bypass_incompleto_rejeitado_antes_do_ddl(self):
        conexao = FakeConnection()
        artificial = object.__new__(ValidatedSql)
        with self.assertRaises(ChecksumMismatchError):
            criar_runner(conexao, sql_loader=lambda **_: artificial).executar()
        self.assertFalse(any(
            sql.lstrip().upper().startswith("CREATE TABLE")
            for sql, _, _ in conexao.executions
        ))

    def test_artefato_com_caminho_divergente_rejeitado_antes_do_ddl(self):
        conexao = FakeConnection()
        with tempfile.TemporaryDirectory() as pasta:
            outro = Path(pasta) / "M0001_criar_ledger.sql"
            outro.write_bytes(SQL.read_bytes())
            def loader(**argumentos):
                return carregar_sql_validado(
                    operacao_id=argumentos["operacao_id"],
                    raiz_autorizada=outro.parent.resolve(),
                    caminho_autorizado=outro.resolve(),
                    checksum_esperado=argumentos["checksum_esperado"],
                )
            with self.assertRaises(ChecksumMismatchError):
                criar_runner(conexao, sql_loader=loader).executar()
        self.assertFalse(any(
            sql.lstrip().upper().startswith("CREATE TABLE")
            for sql, _, _ in conexao.executions
        ))

    def test_runner_executa_crlf_na_forma_lf_normalizada(self):
        fx = ManifestFixture()
        try:
            fx.sql.write_bytes(fx.sql.read_bytes().replace(b"\n", b"\r\n"))
            runner = criar_runner(FakeConnection())
            runner.manifesto = carregar_manifesto(fx.save())
            runner.executar()
            ddl = next(sql for sql, _, _ in runner.conexao.executions if sql.startswith("CREATE TABLE"))
            self.assertNotIn("\r", ddl)
        finally:
            fx.close()


class BootstrapFastTests(unittest.TestCase):
    def test_entrada_exige_conexao_explicita_sem_consultar_database_url(self):
        with (
            mock.patch.dict(os.environ, {"DATABASE_URL": "nao-deve-ser-usada"}),
            mock.patch("migrations_control.bootstrap.MigrationRunner") as runner_factory,
            self.assertRaises(DatabaseConnectionError),
        ):
            executar_bootstrap_controlado(None)
        runner_factory.assert_not_called()

    def test_entrada_delega_uma_vez_ao_runner_validado(self):
        conexao = object()
        resultado = object()
        runner = mock.Mock()
        runner.executar.return_value = resultado
        logger = mock.Mock()
        with mock.patch(
            "migrations_control.bootstrap.MigrationRunner", return_value=runner,
        ) as runner_factory:
            recebido = executar_bootstrap_controlado(
                conexao, timeout_lock_segundos=7.5, event_logger=logger,
            )
        self.assertIs(resultado, recebido)
        runner_factory.assert_called_once_with(
            conexao, caminho_manifesto=None,
            timeout_lock_segundos=7.5, event_logger=logger,
        )
        runner.executar.assert_called_once_with()

    def test_banco_novo_aplica_somente_m0001_e_resulta_controlado(self):
        conexao = FakeConnection()
        resultado = criar_runner(conexao).executar()
        reconhecido = classificar_preflight(snapshot_controlado(), carregar_manifesto())
        sql = " ".join(item[0] for item in conexao.executions)
        self.assertEqual(("M0001",), resultado.aplicadas)
        self.assertEqual("BANCO_NOVO", resultado.classificacao_preflight)
        self.assertEqual("BANCO_CONTROLADO", reconhecido.classificacao.value)
        self.assertNotRegex(sql, r"H00[1-9]|H01[01]")

    def test_banco_controlado_repetido_e_idempotente(self):
        conexoes = (FakeConnection(), FakeConnection())
        resultados = tuple(
            criar_runner(conexao, snapshot=snapshot_controlado()).executar()
            for conexao in conexoes
        )
        self.assertTrue(all(resultado.aplicadas == () for resultado in resultados))
        self.assertTrue(all(resultado.ignoradas == ("M0001",) for resultado in resultados))
        self.assertTrue(all(not any(
            sql.lstrip().upper().startswith("CREATE TABLE")
            for sql, _, _ in conexao.executions
        ) for conexao in conexoes))

    def test_estado_incompativel_e_recusado_sem_ddl(self):
        conexao = FakeConnection()
        with self.assertRaises(UnknownDatabaseError):
            criar_runner(conexao, snapshot=snapshot_novo(("objeto_inesperado",))).executar()
        self.assertFalse(any(
            sql.lstrip().upper().startswith("CREATE TABLE")
            for sql, _, _ in conexao.executions
        ))

    def test_falha_durante_m0001_faz_rollback_sem_seguir_adiante(self):
        conexao = FakeConnection()
        conexao.fail_execute_contains = "INSERT INTO public.schema_migrations"
        with self.assertRaises(MigrationExecutionError):
            criar_runner(conexao).executar()
        self.assertEqual(1, conexao.rollbacks)
        self.assertNotRegex(" ".join(item[0] for item in conexao.executions), r"H00[1-9]|H01[01]")

    def test_manifesto_do_bootstrap_contem_somente_m0000_m0001(self):
        manifesto = carregar_manifesto()
        self.assertEqual(("M0000", "M0001"), tuple(
            operacao.identificador for operacao in manifesto.operacoes
        ))

    def test_importar_entrada_bootstrap_nao_executa_runner(self):
        codigo = (
            "import os,sys\n"
            "from unittest import mock\n"
            "sys.path.insert(0, os.getcwd())\n"
            "with mock.patch('migrations_control.runner.MigrationRunner.executar', "
            "side_effect=AssertionError('bootstrap automatico')):\n"
            " import migrations_control.bootstrap\n"
            "print('BOOTSTRAP_IMPORT_OK')\n"
        )
        resultado = subprocess.run(
            [sys.executable, "-B", "-c", codigo], cwd=ROOT,
            capture_output=True, text=True, timeout=15, check=False,
            env=dict(os.environ, PYTHONDONTWRITEBYTECODE="1"),
        )
        self.assertEqual(
            (0, "BOOTSTRAP_IMPORT_OK\n", ""),
            (resultado.returncode, resultado.stdout, resultado.stderr),
        )


class LedgerOperationTests(unittest.TestCase):
    def setUp(self):
        self.operacao = carregar_manifesto().por_id()["M0001"]
        self.conexao = FakeConnection(autocommit=False)
        self.cursor = self.conexao.cursor()

    def test_iniciar_parametrizado_public(self):
        iniciar_tentativa(self.cursor, self.operacao, tentativa=1, request_id=REQUEST_ID, iniciada_em=NOW)
        sql, params, _ = self.conexao.executions[-1]
        self.assertTrue(sql.startswith("INSERT INTO public.schema_migration_execucoes"))
        self.assertIn("INICIADA", params)

    def test_request_id_uuid_adaptado_com_mesma_identidade(self):
        registrar_m0001_aplicada(
            self.cursor, self.operacao, request_id=REQUEST_ID,
            iniciada_em=NOW, concluida_em=NOW, duracao_ms=1,
            manifesto_versao=1,
        )
        iniciar_tentativa(
            self.cursor, self.operacao, tentativa=1,
            request_id=REQUEST_ID, iniciada_em=NOW,
        )
        registro_params = self.conexao.executions[-2][1]
        tentativa_params = self.conexao.executions[-1][1]
        self.assertIs(type(REQUEST_ID), UUID)
        self.assertIs(type(registro_params[9]), str)
        self.assertIs(type(tentativa_params[5]), str)
        self.assertEqual(str(REQUEST_ID), registro_params[9])
        self.assertEqual(registro_params[9], tentativa_params[5])

    def test_concluir_sucesso_public(self):
        self.cursor.rows = [(1,)]
        concluir_tentativa(self.cursor, self.operacao, tentativa=1, situacao="APLICADA", concluida_em=NOW, duracao_ms=1)
        self.assertTrue(self.conexao.executions[-1][0].startswith("UPDATE public.schema_migration_execucoes"))

    def test_estado_inventado_rejeitado(self):
        with self.assertRaises(ImpossibleLedgerStateError):
            concluir_tentativa(self.cursor, self.operacao, tentativa=1, situacao="OUTRO", concluida_em=NOW, duracao_ms=1)


class CliAndSecurityTests(unittest.TestCase):
    def cli(self, *args):
        saida = io.StringIO()
        with contextlib.redirect_stdout(saida):
            codigo = cli_main(list(args))
        return codigo, saida.getvalue()

    def test_cli_manifesto(self):
        self.assertEqual(0, self.cli("validar-manifesto")[0])

    def test_cli_checksum(self):
        codigo, saida = self.cli("verificar-checksums")
        self.assertEqual(0, codigo); self.assertEqual(1, json.loads(saida)["checksums_verificados"])

    def test_cli_plano(self):
        codigo, saida = self.cli("mostrar-plano")
        self.assertEqual(0, codigo); self.assertEqual(2, len(json.loads(saida)["operacoes"]))

    def test_cli_preflight_bloqueado(self):
        self.assertEqual(2, self.cli("preflight")[0])

    def test_cli_aplicar_bloqueado(self):
        self.assertEqual(2, self.cli("aplicar")[0])

    def test_sanitizacao_bateria(self):
        segredos = (
            "postgresql://usuario:senha@host/banco", "password=segredo",
            "token=abcdef", "Authorization: Bearer abc", "api_key=123",
            "SELECT 'segredo'\nTraceback caminho C:/pessoal",
        )
        for segredo in segredos:
            with self.subTest(segredo=segredo):
                saida = json.dumps(sanitizar_erro(RuntimeError(segredo)))
                self.assertNotIn(segredo, saida)

    def test_import_isolado_sem_efeitos(self):
        codigo = (
            "import builtins,os,socket,sys\n"
            "sys.path.insert(0, os.getcwd())\n"
            "os.getenv=lambda *a,**k: (_ for _ in ()).throw(RuntimeError('env'))\n"
            "socket.socket=lambda *a,**k: (_ for _ in ()).throw(RuntimeError('rede'))\n"
            "original=builtins.open\n"
            "def seguro(nome,modo='r',*a,**k):\n"
            "    assert not any(x in modo for x in 'wax+'), 'escrita'\n"
            "    return original(nome,modo,*a,**k)\n"
            "builtins.open=seguro\n"
            "import migrations_control\n"
            "print('IMPORT_OK')\n"
        )
        ambiente = dict(os.environ, PYTHONDONTWRITEBYTECODE="1")
        resultado = subprocess.run(
            [sys.executable, "-B", "-c", codigo], cwd=ROOT, env=ambiente,
            capture_output=True, text=True, timeout=15, check=False,
        )
        self.assertEqual((0, "IMPORT_OK\n", ""), (resultado.returncode, resultado.stdout, resultado.stderr))

    def test_sql_somente_estrutura(self):
        sql = SQL.read_text(encoding="utf-8").upper()
        for palavra in ("IF NOT EXISTS", "DROP ", "CASCADE", "GRANT ", "INSERT ", "UPDATE ", "DELETE ", "TRUNCATE ", "ALTER TABLE"):
            self.assertNotIn(palavra, sql)

    def test_sql_public_e_pg_catalog(self):
        sql = SQL.read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE public.schema_migrations", sql)
        self.assertIn("CREATE TABLE public.schema_migration_execucoes", sql)
        self.assertIn("pg_catalog.btrim", sql)

    def test_comandos_ledger_sem_search_path(self):
        fontes = (
            (ROOT / "migrations_control" / "ledger.py").read_text(encoding="utf-8")
            + (ROOT / "migrations_control" / "preflight.py").read_text(encoding="utf-8")
        )
        self.assertNotIn("FROM schema_migrations", fontes)
        self.assertNotIn("INTO schema_migrations", fontes)
        self.assertNotIn("FROM schema_migration_execucoes", fontes)
        self.assertNotIn("INTO schema_migration_execucoes", fontes)

    def test_manifesto_sem_historicas(self):
        self.assertNotRegex(MANIFEST.read_text(encoding="utf-8"), r"H00[1-9]|H01[01]")


if __name__ == "__main__":
    unittest.main()
