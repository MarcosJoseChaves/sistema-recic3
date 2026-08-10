"""H2C.4A.2: validação real e descartável do ledger em PostgreSQL 15+.

Este módulo nunca usa DATABASE_URL. A conexão administrativa deve ser entregue
explicitamente, somente durante a execução, por H2C4A2_ADMIN_DSN.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import math
import os
import re
import secrets
import socket
import stat
import subprocess
import tempfile
import threading
import time
import unicodedata
import unittest
from abc import ABC, abstractmethod
from unittest import mock
from contextlib import contextmanager
from dataclasses import InitVar, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from types import MappingProxyType
from uuid import uuid4

import psycopg2
from psycopg2 import sql
from psycopg2.extensions import (
    TRANSACTION_STATUS_IDLE,
    TRANSACTION_STATUS_INTRANS,
    make_dsn,
    parse_dsn,
)

from migrations_control.errors import (
    ConnectionNotIdleError,
    LockTimeoutError,
    MigrationExecutionError,
    UnknownDatabaseError,
)
from migrations_control.locking import AdvisoryLock, derivar_chave_lock
from migrations_control.manifest import carregar_manifesto
from migrations_control.models import DatabaseClassification
from migrations_control.preflight import (
    LEDGER_OBJECTS,
    classificar_preflight,
    coletar_conteudo_ledger,
    coletar_snapshot,
)
from migrations_control.runner import MigrationRunner
from migrations_control.schema_validation import (
    EXPECTED_LEDGER_SCHEMA,
    coletar_assinatura_ledger,
    validar_assinatura_ledger,
)


CHECKSUM_M0001 = "1966113e8d20f4f3aaa2ebc0b6b1f312470ac99835ea97026305c732ab5e0f39"
POSTGRES_IMAGE = "postgres:15"
POSTGRES_IMAGE_DIGEST = "sha256:74e110c41804365e3915fcc09d5e7a1eff50161aaa94d5da0e58e0cd75ae509c"
POSTGRES_IMMUTABLE_REFERENCE = "postgres@" + POSTGRES_IMAGE_DIGEST
POSTGRES_VERSION = "15.18"
POSTGRES_VERSION_NUM = 150018
DOCKER_CONTEXT = "desktop-linux"
DOCKER_LOCAL_ENDPOINT = "npipe:////./pipe/dockerDesktopLinuxEngine"
E1_HEALTH_TIMEOUT_SECONDS = 60
TPG_PGDATA_PATH = "/var/lib/postgresql/data"
TPG_TMPFS_SPEC = TPG_PGDATA_PATH + ":rw,noexec,nosuid,size=512m"
LOCK_KEY = -8482190501243477735
REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
FIXTURES_DIR = REPOSITORY_ROOT / "tests" / "fixtures"
RAW_CATALOG_PATH = FIXTURES_DIR / "h2c4a2_pg15_18_catalogo_bruto.json"

RAW_CATALOG_FIELDS = frozenset({
    "metadados", "pg_constraint", "pg_index", "operator_classes",
    "collations", "sequencias", "inventario_public", "cobertura_inventario",
})
RAW_METADATA_FIELDS = frozenset({
    "formato", "formato_versao", "postgres_version", "server_version_num",
    "container_image", "container_image_digest", "captured_at_utc",
    "capture_id", "m0001_checksum", "manifesto_versao",
})
RAW_COLLECTION_FIELDS = {
    "pg_constraint": frozenset({
        "oid_evidencia", "schema", "tabela", "conname", "contype",
        "conkey", "convalidated", "conislocal", "coninhcount",
        "connoinherit", "condeferrable", "condeferred",
        "pg_get_constraintdef", "pg_get_expr_conbin", "colunas_resolvidas",
    }),
    "pg_index": frozenset({
        "oid_evidencia", "indice_schema", "indice_nome", "tabela_schema",
        "tabela_nome", "metodo", "indisunique", "indisprimary",
        "indisexclusion", "indimmediate", "indisvalid", "indisready",
        "indislive", "indisclustered", "indisreplident",
        "indnullsnotdistinct", "indcheckxmin", "indnkeyatts", "indnatts",
        "indkey", "indclass", "indcollation", "indoption", "indexprs",
        "indpred", "pg_get_indexdef", "vinculado_constraint",
        "relpersistence",
    }),
    "operator_classes": frozenset({
        "indice_oid_evidencia", "posicao", "oid_evidencia", "schema", "nome",
    }),
    "collations": frozenset({
        "indice_oid_evidencia", "posicao", "oid_evidencia", "schema", "nome",
    }),
    "sequencias": frozenset({
        "oid_evidencia", "schema", "nome", "relpersistence",
        "tabela_schema", "tabela", "coluna", "numero_coluna",
        "tipo_dependencia", "tipo_sequencia", "inicio", "incremento",
        "minimo", "maximo", "cache", "cycle",
    }),
}
_SENSITIVE_KEYS = frozenset({
    "password", "passwd", "pwd", "secret", "client_secret", "token",
    "access_token", "refresh_token", "api_key", "apikey", "authorization",
    "proxy_authorization", "dsn", "database_url", "connection_string",
    "postgres_password", "host", "hostname", "port", "username",
    "user_name", "login",
})
_SEGREDO_VALOR = re.compile(
    r"(?:postgres(?:ql)?://|cloudinary://|(?:^|[\s;,])(?:password|passwd|pwd|token|"
    r"access_token|client_secret|api_key|host|hostname|user|username|dbname|port)"
    r"\s*=\s*(?!['\"])[^\s,;)]+|"
    r"authorization\s*:|\b(?:bearer|basic)\s+[A-Za-z0-9+/_.=-]+|"
    r"C:\\Users\\[^\\\s]+|/home/[^/\s]+|/Users/[^/\s]+)",
    re.IGNORECASE,
)
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_CAPTURE_ID = re.compile(r"capture-[0-9a-f]{32}\Z")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_SAFE_EXCEPTION_CLASS = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_SQLSTATE = re.compile(r"[0-9A-Z]{5}\Z")


class E1Stage(str, Enum):
    """Etapas fechadas do ciclo E1, sem argumentos ou identificadores sensiveis."""

    PRECHECK = "PRECHECK"
    DOCKER_IMAGE = "DOCKER_IMAGE"
    CONTAINER_CREATE = "CONTAINER_CREATE"
    CONTAINER_ISOLATION = "CONTAINER_ISOLATION"
    CONTAINER_HEALTH = "CONTAINER_HEALTH"
    PORT_DISCOVERY = "PORT_DISCOVERY"
    TEMP_CREDENTIALS = "TEMP_CREDENTIALS"
    DB_CONNECT = "DB_CONNECT"
    POSTGRES_VERSION = "POSTGRES_VERSION"
    SERVER_VERSION_NUM = "SERVER_VERSION_NUM"
    POSTGRES_VERSION_VALIDATE = "POSTGRES_VERSION_VALIDATE"
    NEW_DATABASE_CHECK = "NEW_DATABASE_CHECK"
    M0001_CHECKSUM = "M0001_CHECKSUM"
    M0001_READ = "M0001_READ"
    M0001_APPLY = "M0001_APPLY"
    M0001_COMMIT = "M0001_COMMIT"
    CAPTURE_PREPARE = "CAPTURE_PREPARE"
    CATALOG_COLLECT = "CATALOG_COLLECT"
    CATALOG_SERIALIZE = "CATALOG_SERIALIZE"
    CAPTURE_WRITE = "CAPTURE_WRITE"
    CAPTURE_VALIDATE = "CAPTURE_VALIDATE"
    CAPTURE_HASH = "CAPTURE_HASH"
    CLEANUP_CONNECTIONS = "CLEANUP_CONNECTIONS"
    CLEANUP_ENV = "CLEANUP_ENV"
    CLEANUP_CONTAINER = "CLEANUP_CONTAINER"
    CLEANUP_VERIFY_CONTAINER = "CLEANUP_VERIFY_CONTAINER"
    CLEANUP_VERIFY_PORT = "CLEANUP_VERIFY_PORT"
    CLEANUP_VERIFY_VOLUME = "CLEANUP_VERIFY_VOLUME"
    CLEANUP_PHOTOGRAPH = "CLEANUP_PHOTOGRAPH"


class E1ErrorCategory(str, Enum):
    ENVIRONMENT = "ENVIRONMENT"
    DOCKER = "DOCKER"
    CONTAINER = "CONTAINER"
    TIMEOUT = "TIMEOUT"
    DATABASE_CONNECTION = "DATABASE_CONNECTION"
    DATABASE_QUERY = "DATABASE_QUERY"
    VERSION_MISMATCH = "VERSION_MISMATCH"
    CHECKSUM = "CHECKSUM"
    MIGRATION_APPLICATION = "MIGRATION_APPLICATION"
    CAPTURE = "CAPTURE"
    FILESYSTEM = "FILESYSTEM"
    VALIDATION = "VALIDATION"
    CLEANUP = "CLEANUP"
    UNKNOWN = "UNKNOWN"


class E1CheckState(str, Enum):
    TRUE = "TRUE"
    FALSE = "FALSE"
    UNKNOWN = "UNKNOWN"


E1_EXECUTION_STAGES = (
    E1Stage.PRECHECK,
    E1Stage.DOCKER_IMAGE,
    E1Stage.TEMP_CREDENTIALS,
    E1Stage.CONTAINER_CREATE,
    E1Stage.CONTAINER_ISOLATION,
    E1Stage.CONTAINER_HEALTH,
    E1Stage.PORT_DISCOVERY,
    E1Stage.DB_CONNECT,
    E1Stage.POSTGRES_VERSION,
    E1Stage.SERVER_VERSION_NUM,
    E1Stage.POSTGRES_VERSION_VALIDATE,
    E1Stage.NEW_DATABASE_CHECK,
    E1Stage.M0001_CHECKSUM,
    E1Stage.M0001_READ,
    E1Stage.M0001_APPLY,
    E1Stage.M0001_COMMIT,
    E1Stage.CAPTURE_PREPARE,
    E1Stage.CATALOG_COLLECT,
    E1Stage.CATALOG_SERIALIZE,
    E1Stage.CAPTURE_WRITE,
    E1Stage.CAPTURE_VALIDATE,
    E1Stage.CAPTURE_HASH,
)
E1_CLEANUP_STAGES = (
    E1Stage.CLEANUP_CONNECTIONS,
    E1Stage.CLEANUP_ENV,
    E1Stage.CLEANUP_CONTAINER,
    E1Stage.CLEANUP_VERIFY_CONTAINER,
    E1Stage.CLEANUP_VERIFY_PORT,
    E1Stage.CLEANUP_VERIFY_VOLUME,
)
_E1_STAGE_CATEGORIES = MappingProxyType({
    E1Stage.PRECHECK: E1ErrorCategory.ENVIRONMENT,
    E1Stage.DOCKER_IMAGE: E1ErrorCategory.DOCKER,
    E1Stage.CONTAINER_CREATE: E1ErrorCategory.CONTAINER,
    E1Stage.CONTAINER_ISOLATION: E1ErrorCategory.CONTAINER,
    E1Stage.CONTAINER_HEALTH: E1ErrorCategory.TIMEOUT,
    E1Stage.PORT_DISCOVERY: E1ErrorCategory.CONTAINER,
    E1Stage.TEMP_CREDENTIALS: E1ErrorCategory.ENVIRONMENT,
    E1Stage.DB_CONNECT: E1ErrorCategory.DATABASE_CONNECTION,
    E1Stage.POSTGRES_VERSION: E1ErrorCategory.DATABASE_QUERY,
    E1Stage.SERVER_VERSION_NUM: E1ErrorCategory.DATABASE_QUERY,
    E1Stage.POSTGRES_VERSION_VALIDATE: E1ErrorCategory.VERSION_MISMATCH,
    E1Stage.NEW_DATABASE_CHECK: E1ErrorCategory.VALIDATION,
    E1Stage.M0001_CHECKSUM: E1ErrorCategory.CHECKSUM,
    E1Stage.M0001_READ: E1ErrorCategory.FILESYSTEM,
    E1Stage.M0001_APPLY: E1ErrorCategory.MIGRATION_APPLICATION,
    E1Stage.M0001_COMMIT: E1ErrorCategory.MIGRATION_APPLICATION,
    E1Stage.CAPTURE_PREPARE: E1ErrorCategory.CAPTURE,
    E1Stage.CATALOG_COLLECT: E1ErrorCategory.CAPTURE,
    E1Stage.CATALOG_SERIALIZE: E1ErrorCategory.CAPTURE,
    E1Stage.CAPTURE_WRITE: E1ErrorCategory.FILESYSTEM,
    E1Stage.CAPTURE_VALIDATE: E1ErrorCategory.VALIDATION,
    E1Stage.CAPTURE_HASH: E1ErrorCategory.CHECKSUM,
    E1Stage.CLEANUP_PHOTOGRAPH: E1ErrorCategory.CLEANUP,
    **{stage: E1ErrorCategory.CLEANUP for stage in E1_CLEANUP_STAGES},
})


@dataclass
class E1CleanupTelemetry:
    cleanup_started: E1CheckState = E1CheckState.FALSE
    cleanup_connection_closed: E1CheckState = E1CheckState.UNKNOWN
    cleanup_env_cleared: E1CheckState = E1CheckState.UNKNOWN
    cleanup_container_requested: E1CheckState = E1CheckState.UNKNOWN
    cleanup_container_absent: E1CheckState = E1CheckState.UNKNOWN
    cleanup_port_released: E1CheckState = E1CheckState.UNKNOWN
    cleanup_volume_absent: E1CheckState = E1CheckState.UNKNOWN


@dataclass(frozen=True)
class E1SanitizedError:
    stage: E1Stage
    category: E1ErrorCategory
    error_type: str
    sqlstate: str | None = None
    errno: int | None = None
    winerror: int | None = None


@dataclass(frozen=True)
class E1FailureTelemetry:
    code: str
    primary_error: E1SanitizedError
    cleanup: E1CleanupTelemetry
    cleanup_error: E1SanitizedError | None = None


def _estado_e1(resultado):
    if resultado is True:
        return E1CheckState.TRUE
    if resultado is False:
        return E1CheckState.FALSE
    return E1CheckState.UNKNOWN


def _sanitizar_erro_e1(stage, erro):
    def atributo_seguro(nome):
        try:
            return getattr(erro, nome, None)
        except Exception:
            return None

    nome = type(erro).__name__
    if not _SAFE_EXCEPTION_CLASS.fullmatch(nome):
        nome = "Exception"
    pgcode = atributo_seguro("pgcode")
    sqlstate = pgcode if type(pgcode) is str and _SQLSTATE.fullmatch(pgcode) else None
    errno = atributo_seguro("errno")
    winerror = atributo_seguro("winerror")
    return E1SanitizedError(
        stage=stage,
        category=_E1_STAGE_CATEGORIES.get(stage, E1ErrorCategory.UNKNOWN),
        error_type=nome,
        sqlstate=sqlstate,
        errno=errno if type(errno) is int else None,
        winerror=winerror if type(winerror) is int else None,
    )


class E1CleanupCheckFailed(RuntimeError):
    """Marcador fixo para resultado negativo ou nao verificavel no cleanup."""


def sanitizar_telemetria_e1(telemetria):
    """Produz somente o contrato fechado; nunca le mensagens ou argumentos."""
    if type(telemetria) is not E1FailureTelemetry:
        raise TypeError("Telemetria E1 deve usar o contrato fechado.")
    if telemetria.code != "H2C4A2_E1_FAILURE":
        raise ValueError("Codigo de telemetria E1 invalido.")

    def erro_seguro(erro):
        if type(erro) is not E1SanitizedError:
            raise TypeError("Erro E1 deve estar sanitizado.")
        if (
            type(erro.stage) is not E1Stage
            or type(erro.category) is not E1ErrorCategory
            or erro.category is not _E1_STAGE_CATEGORIES[erro.stage]
            or not _SAFE_EXCEPTION_CLASS.fullmatch(erro.error_type)
            or (erro.sqlstate is not None and not _SQLSTATE.fullmatch(erro.sqlstate))
            or (erro.errno is not None and type(erro.errno) is not int)
            or (erro.winerror is not None and type(erro.winerror) is not int)
        ):
            raise ValueError("Campo de erro E1 fora do contrato fechado.")
        return {
            "stage": erro.stage.value,
            "category": erro.category.value,
            "error_type": erro.error_type,
            "sqlstate": erro.sqlstate,
            "errno": erro.errno,
            "winerror": erro.winerror,
        }

    cleanup = telemetria.cleanup
    if type(cleanup) is not E1CleanupTelemetry:
        raise TypeError("Cleanup E1 deve usar o contrato fechado.")
    cleanup_seguro = {}
    for campo in E1CleanupTelemetry.__dataclass_fields__:
        valor = getattr(cleanup, campo)
        if type(valor) is not E1CheckState:
            raise ValueError("Estado de cleanup E1 invalido.")
        cleanup_seguro[campo] = valor.value
    return {
        "code": telemetria.code,
        "primary_error": erro_seguro(telemetria.primary_error),
        "cleanup": cleanup_seguro,
        "cleanup_error": (
            erro_seguro(telemetria.cleanup_error)
            if telemetria.cleanup_error is not None else None
        ),
    }


class H2C4A2E1Failure(RuntimeError):
    """Falha E1 cuja representacao publica contem somente telemetria validada."""

    def __init__(self, telemetria):
        self.telemetria = sanitizar_telemetria_e1(telemetria)
        super().__init__(json.dumps(self.telemetria, sort_keys=True, separators=(",", ":")))


class E1ContractError(RuntimeError):
    """Falha fechada de pos-condicao da orquestracao."""


class TPGStorageContractError(RuntimeError):
    """Falha fechada do armazenamento efêmero dos TPGs."""


def _docker_tpg(*arguments, credential_env=None):
    technical_keys = (
        "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP",
        "USERPROFILE", "APPDATA", "LOCALAPPDATA", "PROGRAMDATA",
        "HOMEDRIVE", "HOMEPATH",
    )
    environment = {
        key: os.environ[key] for key in technical_keys if key in os.environ
    }
    if credential_env is not None:
        if (
            type(credential_env) is not dict
            or set(credential_env) != {"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"}
            or any(type(value) is not str or not value for value in credential_env.values())
        ):
            raise TPGStorageContractError("Credenciais temporárias dos TPGs inválidas.")
        environment.update(credential_env)
    return subprocess.run(
        ("docker", "--context", DOCKER_CONTEXT) + tuple(arguments),
        check=True, capture_output=True, text=True, encoding="utf-8",
        errors="strict", env=environment, shell=False,
    )


def criar_container_tpg_controlado(container_name, credential_env):
    if (
        type(container_name) is not str
        or re.fullmatch(r"h2c4a2-tpg-[0-9a-f]{12,32}", container_name) is None
    ):
        raise TPGStorageContractError("Nome do container TPG inválido.")
    return _docker_tpg(
        "run", "--detach", "--rm", "--name", container_name,
        "--label", "h2c4a2=tpg", "--tmpfs", TPG_TMPFS_SPEC,
        "--publish", "127.0.0.1::5432",
        "--env", "POSTGRES_USER", "--env", "POSTGRES_PASSWORD",
        "--env", "POSTGRES_DB", POSTGRES_IMMUTABLE_REFERENCE,
        credential_env=credential_env,
    )


def validar_isolamento_container_tpg(inspect_payload, expected_image_id):
    try:
        data = json.loads(inspect_payload)
        info = data[0]
        host_config = info["HostConfig"]
        tmpfs = host_config["Tmpfs"]
        tmpfs_options = frozenset(tmpfs[TPG_PGDATA_PATH].split(","))
        ports = info["NetworkSettings"]["Ports"]["5432/tcp"]
        config = info["Config"]
    except (IndexError, KeyError, TypeError, AttributeError, json.JSONDecodeError):
        raise TPGStorageContractError("Inspect TPG inválido.") from None
    size_options = {"size=512m", "size=536870912"}
    if (
        type(data) is not list
        or len(data) != 1
        or type(expected_image_id) is not str
        or re.fullmatch(r"sha256:[0-9a-f]{64}", expected_image_id) is None
        or host_config.get("AutoRemove") is not True
        or type(tmpfs) is not dict
        or set(tmpfs) != {TPG_PGDATA_PATH}
        or not {"rw", "noexec", "nosuid"}.issubset(tmpfs_options)
        or not tmpfs_options.intersection(size_options)
        or host_config.get("Binds") not in (None, [])
        or host_config.get("Mounts") not in (None, [])
        or info.get("Mounts") != []
        or type(ports) is not list
        or len(ports) != 1
        or ports[0].get("HostIp") != "127.0.0.1"
        or info.get("Image") != expected_image_id
        or config.get("Image") != POSTGRES_IMMUTABLE_REFERENCE
        or f"PGDATA={TPG_PGDATA_PATH}" not in config.get("Env", [])
    ):
        raise TPGStorageContractError("Isolamento tmpfs do container TPG não confirmado.")
    return True


def validar_container_tpg_controlado(container_name, expected_image_id):
    if (
        type(container_name) is not str
        or re.fullmatch(r"h2c4a2-tpg-[0-9a-f]{12,32}", container_name) is None
    ):
        raise TPGStorageContractError("Nome do container TPG inválido.")
    result = _docker_tpg("inspect", container_name)
    return validar_isolamento_container_tpg(result.stdout, expected_image_id)


def validar_versao_postgresql_tpg(server_version, server_version_num):
    if type(server_version) is not str or not server_version.split():
        raise TPGStorageContractError("Versão PostgreSQL TPG inválida.")
    normalized_version = server_version.split()[0]
    if (
        normalized_version != POSTGRES_VERSION
        or type(server_version_num) is not int
        or server_version_num != POSTGRES_VERSION_NUM
    ):
        raise TPGStorageContractError("Versão PostgreSQL TPG fora do contrato aprovado.")
    return normalized_version


E1_SUCCESS_CATEGORIES = (
    "relacoes", "rotinas", "tipos", "enums", "domains", "triggers",
    "policies", "rules", "extensions", "collations_public", "conversions",
    "operators", "opclasses_public", "opfamilies_public",
    "text_search_configurations", "text_search_config_mappings",
    "text_search_dictionaries", "text_search_parsers", "text_search_templates",
)


@dataclass(frozen=True, repr=False)
class E1Config:
    repository_root: Path
    destination: Path
    approved_image: str = POSTGRES_IMAGE
    approved_digest: str = POSTGRES_IMAGE_DIGEST
    expected_postgres_version: str = POSTGRES_VERSION
    expected_server_version_num: int = POSTGRES_VERSION_NUM
    expected_m0001_checksum: str = CHECKSUM_M0001
    health_timeout_seconds: int = E1_HEALTH_TIMEOUT_SECONDS

    def __post_init__(self):
        if (
            not isinstance(self.repository_root, Path)
            or not isinstance(self.destination, Path)
            or self.repository_root != REPOSITORY_ROOT
            or self.destination != RAW_CATALOG_PATH
            or self.approved_image != POSTGRES_IMAGE
            or self.approved_digest != POSTGRES_IMAGE_DIGEST
            or self.expected_postgres_version != POSTGRES_VERSION
            or type(self.expected_server_version_num) is not int
            or self.expected_server_version_num != POSTGRES_VERSION_NUM
            or self.expected_m0001_checksum != CHECKSUM_M0001
            or type(self.health_timeout_seconds) is not int
            or self.health_timeout_seconds != E1_HEALTH_TIMEOUT_SECONDS
        ):
            raise ValueError("Configuracao E1 fora do contrato aprovado.")

    def __repr__(self):
        return "E1Config(approved=True)"

    __str__ = __repr__


@dataclass(frozen=True, repr=False)
class E1TestConfig:
    """Configuração confinada a tempdir; nunca aceita o destino operacional."""

    repository_root: Path
    destination: Path
    approved_image: str = POSTGRES_IMAGE
    approved_digest: str = POSTGRES_IMAGE_DIGEST
    expected_postgres_version: str = POSTGRES_VERSION
    expected_server_version_num: int = POSTGRES_VERSION_NUM
    expected_m0001_checksum: str = CHECKSUM_M0001
    health_timeout_seconds: int = E1_HEALTH_TIMEOUT_SECONDS

    def __post_init__(self):
        if (
            not isinstance(self.repository_root, Path)
            or self.repository_root != REPOSITORY_ROOT
            or not isinstance(self.destination, Path)
            or not self.destination.is_absolute()
            or self.destination == RAW_CATALOG_PATH
            or self.destination.name != RAW_CATALOG_PATH.name
            or not self.destination.parent.is_dir()
            or self.destination.parent.is_symlink()
            or self.approved_image != POSTGRES_IMAGE
            or self.approved_digest != POSTGRES_IMAGE_DIGEST
            or self.expected_postgres_version != POSTGRES_VERSION
            or type(self.expected_server_version_num) is not int
            or self.expected_server_version_num != POSTGRES_VERSION_NUM
            or self.expected_m0001_checksum != CHECKSUM_M0001
            or type(self.health_timeout_seconds) is not int
            or self.health_timeout_seconds != E1_HEALTH_TIMEOUT_SECONDS
        ):
            raise ValueError("Configuração de teste E1 fora do contrato confinado.")

    def __repr__(self):
        return "E1TestConfig(test_only=True)"

    __str__ = __repr__


@dataclass(repr=False)
class E1RuntimeState:
    container_name: str | None = None
    temporary_user: str | None = None
    temporary_password: str | None = None
    temporary_database: str | None = None
    port: int | None = None
    dsn: str | None = None
    connection: object | None = None
    postgres_version: str | None = None
    server_version_num: int | None = None
    m0001_checksum: str | None = None
    m0001_sql: str | None = None
    m0001_applied: bool = False
    m0001_committed: bool = False
    metadata: dict | None = None
    catalog: dict | None = None
    serialized: bytes | None = None
    written_path: Path | None = None
    validated_catalog: dict | None = None
    photograph_sha256: str | None = None
    category_counts: dict | None = None
    cleanup_env_cleared: bool = False
    cleanup_container_requested: bool = False
    cleanup_container_absent: bool = False
    cleanup_port_released: bool = False
    cleanup_volume_absent: bool = False
    initial_volumes: frozenset = field(default_factory=frozenset)
    expected_image_id: str | None = None
    capture_created_this_run: bool = False
    capture_written_sha256: str | None = None
    capture_written_capture_id: str | None = None

    def __repr__(self):
        return "E1RuntimeState(redacted=True)"

    __str__ = __repr__


@dataclass(frozen=True)
class E1StepEvidence:
    stage: E1Stage

    def __post_init__(self):
        if type(self.stage) is not E1Stage:
            raise ValueError("Estágio de evidência E1 inválido.")


@dataclass(frozen=True)
class E1CleanupEvidence:
    stage: E1Stage
    state: E1CheckState

    def __post_init__(self):
        if self.stage not in E1_CLEANUP_STAGES or type(self.state) is not E1CheckState:
            raise ValueError("Evidência de cleanup E1 inválida.")


@dataclass(frozen=True)
class M0001AppliedEvidence:
    checksum: str

    def __post_init__(self):
        if self.checksum != CHECKSUM_M0001:
            raise ValueError("Evidência M0001 inválida.")


@dataclass(frozen=True)
class CatalogCollectedEvidence:
    category_counts: tuple

    def __post_init__(self):
        try:
            contagens = dict(self.category_counts)
        except (TypeError, ValueError):
            raise ValueError("Evidência de catálogo inválida.") from None
        if (
            type(self.category_counts) is not tuple
            or len(self.category_counts) != len(contagens)
            or tuple(contagens) != E1_SUCCESS_CATEGORIES
            or any(type(valor) is not int or valor < 0 for valor in contagens.values())
        ):
            raise ValueError("Evidência de catálogo inválida.")


@dataclass(frozen=True)
class SerializedCaptureEvidence:
    photograph_size: int

    def __post_init__(self):
        if type(self.photograph_size) is not int or self.photograph_size <= 0:
            raise ValueError("Evidência serializada inválida.")


@dataclass(frozen=True)
class WrittenCaptureEvidence:
    photograph_size: int

    def __post_init__(self):
        if type(self.photograph_size) is not int or self.photograph_size <= 0:
            raise ValueError("Evidência gravada inválida.")


@dataclass(frozen=True)
class ValidatedCaptureEvidence:
    capture_id: str
    captured_at_utc: str
    category_counts: tuple

    def __post_init__(self):
        if (
            type(self.capture_id) is not str
            or _CAPTURE_ID.fullmatch(self.capture_id) is None
            or type(self.captured_at_utc) is not str
            or not self.captured_at_utc.endswith("Z")
        ):
            raise ValueError("Evidência validada inválida.")
        CatalogCollectedEvidence(self.category_counts)


@dataclass(frozen=True)
class CaptureHashEvidence:
    photograph_sha256: str
    photograph_size: int

    def __post_init__(self):
        if (
            type(self.photograph_sha256) is not str
            or _SHA256.fullmatch(self.photograph_sha256) is None
            or type(self.photograph_size) is not int
            or self.photograph_size <= 0
        ):
            raise ValueError("Evidência de hash inválida.")


@dataclass(frozen=True, repr=False)
class E1FlowOutcome:
    """Resultado interno do fluxo; deliberadamente não é evidência operacional."""

    photograph_relative_path: str
    photograph_sha256: str
    photograph_size: int
    capture_id: str
    captured_at_utc: str
    postgres_version: str
    server_version_num: int
    image_digest: str
    category_counts: tuple
    cleanup: tuple

    def __post_init__(self):
        try:
            categorias = dict(self.category_counts)
            cleanup = dict(self.cleanup)
        except (TypeError, ValueError):
            raise ValueError("Outcome interno E1 fora do contrato fechado.") from None
        if (
            type(self.category_counts) is not tuple
            or len(self.category_counts) != len(E1_SUCCESS_CATEGORIES)
            or len(categorias) != len(E1_SUCCESS_CATEGORIES)
            or type(self.cleanup) is not tuple
            or len(self.cleanup) != len(E1CleanupTelemetry.__dataclass_fields__)
            or len(cleanup) != len(E1CleanupTelemetry.__dataclass_fields__)
            or
            self.photograph_relative_path != "tests/fixtures/h2c4a2_pg15_18_catalogo_bruto.json"
            or type(self.photograph_sha256) is not str
            or _SHA256.fullmatch(self.photograph_sha256) is None
            or type(self.photograph_size) is not int
            or self.photograph_size <= 0
            or type(self.capture_id) is not str
            or _CAPTURE_ID.fullmatch(self.capture_id) is None
            or type(self.captured_at_utc) is not str
            or not self.captured_at_utc.endswith("Z")
            or self.postgres_version != POSTGRES_VERSION
            or type(self.server_version_num) is not int
            or self.server_version_num != POSTGRES_VERSION_NUM
            or self.image_digest != POSTGRES_IMAGE_DIGEST
            or tuple(categorias) != E1_SUCCESS_CATEGORIES
            or any(type(valor) is not int or valor < 0 for valor in categorias.values())
            or tuple(cleanup) != tuple(E1CleanupTelemetry.__dataclass_fields__)
            or any(valor != E1CheckState.TRUE.value for valor in cleanup.values())
        ):
            raise ValueError("Outcome interno E1 fora do contrato fechado.")

    def __repr__(self):
        return "E1FlowOutcome(test_only_or_pending_receipt=True)"

    __str__ = __repr__


_E1_OPERATIONAL_RECEIPT_CAPABILITY = object()


@dataclass(frozen=True, repr=False)
class E1SuccessReceipt:
    capability: InitVar[object]
    code: str
    photograph_relative_path: str
    photograph_sha256: str
    photograph_size: int
    capture_id: str
    captured_at_utc: str
    postgres_version: str
    server_version_num: int
    image_digest: str
    category_counts: tuple
    cleanup: tuple

    def __post_init__(self, capability):
        try:
            categorias = dict(self.category_counts)
            cleanup = dict(self.cleanup)
        except (TypeError, ValueError):
            raise ValueError("Receipt E1 fora do contrato fechado de sucesso.") from None
        if (
            capability is not _E1_OPERATIONAL_RECEIPT_CAPABILITY
            or type(self.category_counts) is not tuple
            or len(self.category_counts) != len(E1_SUCCESS_CATEGORIES)
            or len(categorias) != len(E1_SUCCESS_CATEGORIES)
            or type(self.cleanup) is not tuple
            or len(self.cleanup) != len(E1CleanupTelemetry.__dataclass_fields__)
            or len(cleanup) != len(E1CleanupTelemetry.__dataclass_fields__)
            or self.code != "H2C4A2_E1_SUCCESS"
            or self.photograph_relative_path != "tests/fixtures/h2c4a2_pg15_18_catalogo_bruto.json"
            or type(self.photograph_sha256) is not str
            or not _SHA256.fullmatch(self.photograph_sha256)
            or type(self.photograph_size) is not int
            or self.photograph_size <= 0
            or type(self.capture_id) is not str
            or not _CAPTURE_ID.fullmatch(self.capture_id)
            or type(self.captured_at_utc) is not str
            or not self.captured_at_utc.endswith("Z")
            or self.postgres_version != POSTGRES_VERSION
            or type(self.server_version_num) is not int
            or self.server_version_num != POSTGRES_VERSION_NUM
            or self.image_digest != POSTGRES_IMAGE_DIGEST
            or tuple(categorias) != E1_SUCCESS_CATEGORIES
            or any(type(valor) is not int or valor < 0 for valor in categorias.values())
            or tuple(cleanup) != tuple(E1CleanupTelemetry.__dataclass_fields__)
            or any(valor != E1CheckState.TRUE.value for valor in cleanup.values())
        ):
            raise ValueError("Receipt E1 fora do contrato fechado de sucesso.")

    def __repr__(self):
        return json.dumps({
            "code": self.code,
            "photograph_relative_path": self.photograph_relative_path,
            "photograph_sha256": self.photograph_sha256,
            "photograph_size": self.photograph_size,
            "capture_id": self.capture_id,
            "captured_at_utc": self.captured_at_utc,
            "postgres_version": self.postgres_version,
            "server_version_num": self.server_version_num,
            "image_digest": self.image_digest,
            "category_counts": dict(self.category_counts),
            "cleanup": dict(self.cleanup),
        }, sort_keys=True, separators=(",", ":"))

    __str__ = __repr__


class E1Adapter(ABC):
    """Interface nominal fechada compartilhada pelo adaptador real e pelo fake."""

    def __init__(self, config):
        if type(config) not in (E1Config, E1TestConfig):
            raise TypeError("E1Adapter exige configuração nominal E1.")
        self.config = config
        self.state = E1RuntimeState()

    @abstractmethod
    def precheck(self): ...
    @abstractmethod
    def validar_imagem_docker(self): ...
    @abstractmethod
    def criar_contexto_temporario(self): ...
    @abstractmethod
    def criar_container(self): ...
    @abstractmethod
    def validar_isolamento(self): ...
    @abstractmethod
    def aguardar_health(self): ...
    @abstractmethod
    def descobrir_porta(self): ...
    @abstractmethod
    def conectar_postgresql(self): ...
    @abstractmethod
    def consultar_server_version(self): ...
    @abstractmethod
    def consultar_server_version_num(self): ...
    @abstractmethod
    def validar_postgresql_15_18(self): ...
    @abstractmethod
    def confirmar_banco_novo(self): ...
    @abstractmethod
    def validar_checksum_m0001(self): ...
    @abstractmethod
    def ler_m0001(self): ...
    @abstractmethod
    def aplicar_m0001(self): ...
    @abstractmethod
    def commit_m0001(self): ...
    @abstractmethod
    def preparar_captura(self): ...
    @abstractmethod
    def coletar_catalogo(self): ...
    @abstractmethod
    def serializar_catalogo(self): ...
    @abstractmethod
    def gravar_fotografia(self): ...
    @abstractmethod
    def validar_fotografia(self): ...
    @abstractmethod
    def calcular_hash(self): ...
    @abstractmethod
    def fechar_conexoes(self): ...
    @abstractmethod
    def limpar_ambiente(self): ...
    @abstractmethod
    def remover_container(self): ...
    @abstractmethod
    def confirmar_container_ausente(self): ...
    @abstractmethod
    def confirmar_porta_liberada(self): ...
    @abstractmethod
    def confirmar_volume_ausente(self): ...


def _inventario_spec(catalogo, campos, consulta):
    return {"catalogo": catalogo, "campos": tuple(campos), "consulta": consulta}


INVENTORY_SPECS = {
    "relacoes": _inventario_spec(
        "pg_catalog.pg_class",
        ("oid_evidencia", "schema", "nome", "relkind", "relpersistence",
         "relispartition", "parent_oid_evidencia", "automatico"),
        "SELECT c.oid,n.nspname,c.relname,c.relkind,c.relpersistence,c.relispartition,"
        "inh.inhparent,EXISTS(SELECT 1 FROM pg_catalog.pg_depend d WHERE "
        "d.classid='pg_catalog.pg_class'::pg_catalog.regclass AND d.objid=c.oid "
        "AND d.deptype IN ('a','i','e')) FROM pg_catalog.pg_class c "
        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "LEFT JOIN pg_catalog.pg_inherits inh ON inh.inhrelid=c.oid "
        "WHERE n.nspname='public' ORDER BY c.relkind,c.relname,c.oid "
        "/* H2C4A2_INVENTARIO_RELACOES */",
    ),
    "rotinas": _inventario_spec(
        "pg_catalog.pg_proc",
        ("oid_evidencia", "schema", "nome", "prokind", "argumentos_identidade",
         "tipo_retorno", "linguagem", "security_definer", "volatilidade",
         "paralelismo", "definicao_bruta", "automatico"),
        "SELECT p.oid,n.nspname,p.proname,p.prokind,"
        "pg_catalog.pg_get_function_identity_arguments(p.oid),"
        "pg_catalog.format_type(p.prorettype,NULL),l.lanname,p.prosecdef,"
        "p.provolatile,p.proparallel,CASE WHEN p.prokind IN ('f','p','w') THEN "
        "pg_catalog.pg_get_functiondef(p.oid) ELSE NULL END,EXISTS(SELECT 1 "
        "FROM pg_catalog.pg_depend d WHERE d.classid='pg_catalog.pg_proc'::pg_catalog.regclass "
        "AND d.objid=p.oid AND d.deptype IN ('a','i','e')) FROM pg_catalog.pg_proc p "
        "JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace "
        "JOIN pg_catalog.pg_language l ON l.oid=p.prolang WHERE n.nspname='public' "
        "ORDER BY p.proname,p.prokind,p.oid /* H2C4A2_INVENTARIO_ROTINAS */",
    ),
    "tipos": _inventario_spec(
        "pg_catalog.pg_type",
        ("oid_evidencia", "schema", "nome", "typtype", "typcategory",
         "relacao_oid_evidencia", "elemento_oid_evidencia", "tipo_base_domain",
         "not_null", "default_bruto", "alinhamento", "armazenamento", "automatico"),
        "SELECT t.oid,n.nspname,t.typname,t.typtype,t.typcategory,NULLIF(t.typrelid,0),"
        "NULLIF(t.typelem,0),CASE WHEN t.typbasetype=0 THEN NULL ELSE "
        "pg_catalog.format_type(t.typbasetype,t.typtypmod) END,t.typnotnull,t.typdefault,"
        "t.typalign,t.typstorage,EXISTS(SELECT 1 FROM pg_catalog.pg_depend d WHERE "
        "d.classid='pg_catalog.pg_type'::pg_catalog.regclass AND d.objid=t.oid "
        "AND d.deptype IN ('a','i','e')) FROM pg_catalog.pg_type t "
        "JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace "
        "WHERE n.nspname='public' ORDER BY t.typname,t.oid /* H2C4A2_INVENTARIO_TIPOS */",
    ),
    "enums": _inventario_spec(
        "pg_catalog.pg_enum",
        ("oid_evidencia", "tipo_oid_evidencia", "schema", "tipo_nome", "label", "ordem"),
        "SELECT e.oid,e.enumtypid,n.nspname,t.typname,e.enumlabel,e.enumsortorder "
        "FROM pg_catalog.pg_enum e JOIN pg_catalog.pg_type t ON t.oid=e.enumtypid "
        "JOIN pg_catalog.pg_namespace n ON n.oid=t.typnamespace WHERE n.nspname='public' "
        "ORDER BY t.typname,e.enumsortorder,e.oid /* H2C4A2_INVENTARIO_ENUMS */",
    ),
    "domains": _inventario_spec(
        "pg_catalog.pg_constraint",
        ("oid_evidencia", "tipo_oid_evidencia", "schema", "tipo_nome",
         "constraint_nome", "validated", "definicao_bruta"),
        "SELECT c.oid,c.contypid,n.nspname,t.typname,c.conname,c.convalidated,"
        "pg_catalog.pg_get_constraintdef(c.oid,true) FROM pg_catalog.pg_constraint c "
        "JOIN pg_catalog.pg_type t ON t.oid=c.contypid JOIN pg_catalog.pg_namespace n "
        "ON n.oid=t.typnamespace WHERE n.nspname='public' AND c.contypid<>0 "
        "ORDER BY t.typname,c.conname,c.oid /* H2C4A2_INVENTARIO_DOMAINS */",
    ),
    "triggers": _inventario_spec(
        "pg_catalog.pg_trigger",
        ("oid_evidencia", "tabela_oid_evidencia", "tabela_schema", "tabela_nome",
         "nome", "interno", "habilitacao", "funcao_oid_evidencia", "funcao_schema",
         "funcao_nome", "definicao_bruta"),
        "SELECT tr.oid,tr.tgrelid,n.nspname,c.relname,tr.tgname,tr.tgisinternal,tr.tgenabled,"
        "tr.tgfoid,pn.nspname,p.proname,pg_catalog.pg_get_triggerdef(tr.oid,true) "
        "FROM pg_catalog.pg_trigger tr JOIN pg_catalog.pg_class c ON c.oid=tr.tgrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace JOIN pg_catalog.pg_proc p "
        "ON p.oid=tr.tgfoid JOIN pg_catalog.pg_namespace pn ON pn.oid=p.pronamespace "
        "WHERE n.nspname='public' ORDER BY c.relname,tr.tgname,tr.oid "
        "/* H2C4A2_INVENTARIO_TRIGGERS */",
    ),
    "policies": _inventario_spec(
        "pg_catalog.pg_policy",
        ("oid_evidencia", "tabela_oid_evidencia", "tabela_schema", "tabela_nome",
         "nome", "permissiva", "comando", "roles", "qual_bruta", "with_check_bruto"),
        "SELECT p.oid,p.polrelid,n.nspname,c.relname,p.polname,p.polpermissive,p.polcmd,"
        "p.polroles,pg_catalog.pg_get_expr(p.polqual,p.polrelid,true),"
        "pg_catalog.pg_get_expr(p.polwithcheck,p.polrelid,true) FROM pg_catalog.pg_policy p "
        "JOIN pg_catalog.pg_class c ON c.oid=p.polrelid JOIN pg_catalog.pg_namespace n "
        "ON n.oid=c.relnamespace WHERE n.nspname='public' ORDER BY c.relname,p.polname,p.oid "
        "/* H2C4A2_INVENTARIO_POLICIES */",
    ),
    "rules": _inventario_spec(
        "pg_catalog.pg_rewrite",
        ("oid_evidencia", "relacao_oid_evidencia", "schema", "relacao_nome", "nome",
         "evento", "instead", "definicao_bruta", "automatico"),
        "SELECT r.oid,r.ev_class,n.nspname,c.relname,r.rulename,r.ev_type,r.is_instead,"
        "pg_catalog.pg_get_ruledef(r.oid,true),(r.rulename='_RETURN') FROM pg_catalog.pg_rewrite r "
        "JOIN pg_catalog.pg_class c ON c.oid=r.ev_class JOIN pg_catalog.pg_namespace n "
        "ON n.oid=c.relnamespace WHERE n.nspname='public' ORDER BY c.relname,r.rulename,r.oid "
        "/* H2C4A2_INVENTARIO_RULES */",
    ),
    "extensions": _inventario_spec(
        "pg_catalog.pg_extension", ("oid_evidencia", "schema", "nome", "versao", "relocatable"),
        "SELECT e.oid,n.nspname,e.extname,e.extversion,e.extrelocatable "
        "FROM pg_catalog.pg_extension e JOIN pg_catalog.pg_namespace n ON n.oid=e.extnamespace "
        "WHERE n.nspname='public' ORDER BY e.extname,e.oid /* H2C4A2_INVENTARIO_EXTENSIONS */",
    ),
    "collations_public": _inventario_spec(
        "pg_catalog.pg_collation", ("oid_evidencia", "schema", "nome", "provider", "deterministic", "encoding"),
        "SELECT c.oid,n.nspname,c.collname,c.collprovider,c.collisdeterministic,c.collencoding "
        "FROM pg_catalog.pg_collation c JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace "
        "WHERE n.nspname='public' ORDER BY c.collname,c.oid /* H2C4A2_INVENTARIO_COLLATIONS */",
    ),
    "conversions": _inventario_spec(
        "pg_catalog.pg_conversion", ("oid_evidencia", "schema", "nome", "encoding_origem", "encoding_destino", "funcao_oid_evidencia", "default"),
        "SELECT c.oid,n.nspname,c.conname,c.conforencoding,c.contoencoding,"
        "c.conproc::pg_catalog.oid,c.condefault "
        "FROM pg_catalog.pg_conversion c JOIN pg_catalog.pg_namespace n ON n.oid=c.connamespace "
        "WHERE n.nspname='public' ORDER BY c.conname,c.oid /* H2C4A2_INVENTARIO_CONVERSIONS */",
    ),
    "operators": _inventario_spec(
        "pg_catalog.pg_operator", ("oid_evidencia", "schema", "nome", "tipo", "left_type", "right_type", "result_type", "funcao_oid_evidencia"),
        "SELECT o.oid,n.nspname,o.oprname,o.oprkind,pg_catalog.format_type(o.oprleft,NULL),"
        "pg_catalog.format_type(o.oprright,NULL),pg_catalog.format_type(o.oprresult,NULL),"
        "o.oprcode::pg_catalog.oid "
        "FROM pg_catalog.pg_operator o JOIN pg_catalog.pg_namespace n ON n.oid=o.oprnamespace "
        "WHERE n.nspname='public' ORDER BY o.oprname,o.oid /* H2C4A2_INVENTARIO_OPERATORS */",
    ),
    "opclasses_public": _inventario_spec(
        "pg_catalog.pg_opclass", ("oid_evidencia", "schema", "nome", "metodo", "familia_oid_evidencia", "tipo_entrada", "default"),
        "SELECT o.oid,n.nspname,o.opcname,am.amname,o.opcfamily,"
        "pg_catalog.format_type(o.opcintype,NULL),o.opcdefault FROM pg_catalog.pg_opclass o "
        "JOIN pg_catalog.pg_namespace n ON n.oid=o.opcnamespace JOIN pg_catalog.pg_am am "
        "ON am.oid=o.opcmethod WHERE n.nspname='public' ORDER BY o.opcname,o.oid "
        "/* H2C4A2_INVENTARIO_OPCLASSES */",
    ),
    "opfamilies_public": _inventario_spec(
        "pg_catalog.pg_opfamily", ("oid_evidencia", "schema", "nome", "metodo"),
        "SELECT o.oid,n.nspname,o.opfname,am.amname FROM pg_catalog.pg_opfamily o "
        "JOIN pg_catalog.pg_namespace n ON n.oid=o.opfnamespace JOIN pg_catalog.pg_am am "
        "ON am.oid=o.opfmethod WHERE n.nspname='public' ORDER BY o.opfname,o.oid "
        "/* H2C4A2_INVENTARIO_OPFAMILIES */",
    ),
    "text_search_configurations": _inventario_spec(
        "pg_catalog.pg_ts_config", ("oid_evidencia", "schema", "nome", "parser_oid_evidencia"),
        "SELECT c.oid,n.nspname,c.cfgname,c.cfgparser FROM pg_catalog.pg_ts_config c "
        "JOIN pg_catalog.pg_namespace n ON n.oid=c.cfgnamespace WHERE n.nspname='public' "
        "ORDER BY c.cfgname,c.oid /* H2C4A2_INVENTARIO_TS_CONFIG */",
    ),
    "text_search_config_mappings": _inventario_spec(
        "pg_catalog.pg_ts_config_map",
        ("config_oid_evidencia", "schema", "config_nome", "tipo_lexema", "sequencia",
         "dictionary_oid_evidencia"),
        "SELECT m.mapcfg,n.nspname,c.cfgname,m.maptokentype,m.mapseqno,m.mapdict "
        "FROM pg_catalog.pg_ts_config_map m JOIN pg_catalog.pg_ts_config c ON c.oid=m.mapcfg "
        "JOIN pg_catalog.pg_namespace n ON n.oid=c.cfgnamespace WHERE n.nspname='public' "
        "ORDER BY c.cfgname,m.maptokentype,m.mapseqno "
        "/* H2C4A2_INVENTARIO_TS_CONFIG_MAP */",
    ),
    "text_search_dictionaries": _inventario_spec(
        "pg_catalog.pg_ts_dict", ("oid_evidencia", "schema", "nome", "template_oid_evidencia", "opcoes"),
        "SELECT d.oid,n.nspname,d.dictname,d.dicttemplate,d.dictinitoption FROM pg_catalog.pg_ts_dict d "
        "JOIN pg_catalog.pg_namespace n ON n.oid=d.dictnamespace WHERE n.nspname='public' "
        "ORDER BY d.dictname,d.oid /* H2C4A2_INVENTARIO_TS_DICT */",
    ),
    "text_search_parsers": _inventario_spec(
        "pg_catalog.pg_ts_parser", ("oid_evidencia", "schema", "nome", "start_oid_evidencia", "token_oid_evidencia", "end_oid_evidencia", "headline_oid_evidencia", "lextype_oid_evidencia"),
        "SELECT p.oid,n.nspname,p.prsname,p.prsstart::pg_catalog.oid,"
        "p.prstoken::pg_catalog.oid,p.prsend::pg_catalog.oid,"
        "p.prsheadline::pg_catalog.oid,p.prslextype::pg_catalog.oid "
        "FROM pg_catalog.pg_ts_parser p JOIN pg_catalog.pg_namespace n ON n.oid=p.prsnamespace "
        "WHERE n.nspname='public' ORDER BY p.prsname,p.oid /* H2C4A2_INVENTARIO_TS_PARSER */",
    ),
    "text_search_templates": _inventario_spec(
        "pg_catalog.pg_ts_template", ("oid_evidencia", "schema", "nome", "init_oid_evidencia", "lexize_oid_evidencia"),
        "SELECT t.oid,n.nspname,t.tmplname,NULLIF(t.tmplinit::pg_catalog.oid,0),"
        "t.tmpllexize::pg_catalog.oid "
        "FROM pg_catalog.pg_ts_template t JOIN pg_catalog.pg_namespace n ON n.oid=t.tmplnamespace "
        "WHERE n.nspname='public' ORDER BY t.tmplname,t.oid /* H2C4A2_INVENTARIO_TS_TEMPLATE */",
    ),
}

_INVENTORY_BOOL_FIELDS = frozenset({
    "automatico", "relispartition", "security_definer", "not_null",
    "validated", "interno", "permissiva", "instead", "relocatable",
    "deterministic", "default",
})


def _normalizar_chave_sensivel(chave):
    normalizada = unicodedata.normalize("NFKC", chave).casefold()
    return re.sub(r"[^a-z0-9]+", "_", normalizada).strip("_")


def _contem_segredo_em_valor(valor):
    """Normaliza somente para detecção; o dado bruto original é preservado."""
    normalizado = unicodedata.normalize("NFKC", valor)
    return _SEGREDO_VALOR.search(normalizado) is not None


def _normalizar_json_bruto(valor):
    """Converte tuplas em listas recursivamente, sem tocar nos demais dados."""
    if type(valor) is tuple:
        return [_normalizar_json_bruto(item) for item in valor]
    if type(valor) is list:
        return [_normalizar_json_bruto(item) for item in valor]
    if type(valor) is dict:
        return {chave: _normalizar_json_bruto(item) for chave, item in valor.items()}
    return valor


def _validar_valor_json_bruto(valor, caminho="raiz"):
    if valor is None or type(valor) in (str, int, bool):
        if type(valor) is str and _contem_segredo_em_valor(valor):
            raise ValueError(f"Conteúdo sensível rejeitado em {caminho}.")
        return
    if type(valor) is float:
        if not math.isfinite(valor):
            raise ValueError(f"Número não finito rejeitado em {caminho}.")
        return
    if type(valor) is list:
        for indice, item in enumerate(valor):
            _validar_valor_json_bruto(item, f"{caminho}[{indice}]")
        return
    if type(valor) is dict:
        for chave, item in valor.items():
            if (
                type(chave) is not str
                or _normalizar_chave_sensivel(chave) in _SENSITIVE_KEYS
            ):
                raise ValueError(f"Chave sensível ou inválida em {caminho}.")
            _validar_valor_json_bruto(item, f"{caminho}.{chave}")
        return
    raise ValueError(f"Tipo não serializável na fotografia bruta: {caminho}.")


def validar_catalogo_bruto(captura):
    """Valida somente o contrato bruto; nunca consulta o modelo normativo."""
    captura = _normalizar_json_bruto(captura)
    if type(captura) is not dict or frozenset(captura) != RAW_CATALOG_FIELDS:
        raise ValueError("Campos de topo da fotografia bruta inválidos.")
    metadados = captura["metadados"]
    if type(metadados) is not dict or frozenset(metadados) != RAW_METADATA_FIELDS:
        raise ValueError("Metadados da fotografia bruta incompletos.")
    if (
        metadados["formato"] != "h2c4a2-pg-catalog-raw"
        or type(metadados["formato_versao"]) is not int
        or metadados["formato_versao"] != 1
        or metadados["postgres_version"] != "15.18"
        or type(metadados["server_version_num"]) is not int
        or metadados["server_version_num"] != 150018
        or metadados["container_image"] != "postgres:15"
        or type(metadados["container_image_digest"]) is not str
        or not _DIGEST.fullmatch(metadados["container_image_digest"])
        or type(metadados["capture_id"]) is not str
        or not _CAPTURE_ID.fullmatch(metadados["capture_id"])
        or metadados["m0001_checksum"] != CHECKSUM_M0001
        or type(metadados["manifesto_versao"]) is not int
        or metadados["manifesto_versao"] != 1
    ):
        raise ValueError("Metadados técnicos da fotografia bruta inválidos.")
    if type(metadados["captured_at_utc"]) is not str or not metadados["captured_at_utc"].endswith("Z"):
        raise ValueError("Data da fotografia deve terminar em Z (UTC).")
    try:
        capturado = datetime.fromisoformat(metadados["captured_at_utc"].replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as erro:
        raise ValueError("Data UTC da fotografia bruta inválida.") from erro
    if capturado.tzinfo is None or capturado.utcoffset() != timezone.utc.utcoffset(capturado):
        raise ValueError("Data da fotografia deve usar UTC.")
    for colecao, campos in RAW_COLLECTION_FIELDS.items():
        itens = captura[colecao]
        if type(itens) is not list:
            raise ValueError(f"Coleção bruta inválida: {colecao}.")
        for item in itens:
            if type(item) is not dict or frozenset(item) != campos:
                raise ValueError(f"Linha bruta incompleta: {colecao}.")
    inventario = captura["inventario_public"]
    if type(inventario) is not dict or frozenset(inventario) != frozenset(INVENTORY_SPECS):
        raise ValueError("Categorias obrigatórias do inventário public estão incompletas.")
    for categoria, spec in INVENTORY_SPECS.items():
        linhas = inventario[categoria]
        if type(linhas) is not list:
            raise ValueError(f"Categoria de inventário inválida: {categoria}.")
        for linha in linhas:
            if type(linha) is not dict or frozenset(linha) != frozenset(spec["campos"]):
                raise ValueError(f"Linha de inventário inválida: {categoria}.")
            for chave, valor in linha.items():
                if chave.endswith("oid_evidencia") and valor is not None:
                    if type(valor) is not int or valor <= 0:
                        raise ValueError(f"OID inválido no inventário: {categoria}.")
                if chave in _INVENTORY_BOOL_FIELDS and type(valor) is not bool:
                    raise ValueError(f"Booleano inválido no inventário: {categoria}.")
                if chave == "roles" and type(valor) is not list:
                    raise ValueError("Roles de policy devem preservar o array bruto.")
    cobertura = captura["cobertura_inventario"]
    if type(cobertura) is not list or len(cobertura) != len(INVENTORY_SPECS):
        raise ValueError("Cobertura do inventário public incompleta.")
    for posicao, (categoria, spec) in enumerate(INVENTORY_SPECS.items()):
        item = cobertura[posicao]
        if type(item) is not dict or frozenset(item) != frozenset((
            "categoria", "catalogo", "consulta", "quantidade", "vazio",
        )):
            raise ValueError("Linha de cobertura do inventário inválida.")
        if (
            item["categoria"] != categoria
            or item["catalogo"] != spec["catalogo"]
            or item["consulta"] != spec["consulta"]
            or type(item["quantidade"]) is not int
            or type(item["vazio"]) is not bool
            or item["quantidade"] != len(inventario[categoria])
            or item["vazio"] != (not inventario[categoria])
        ):
            raise ValueError("Cobertura diverge do inventário coletado.")
    _validar_valor_json_bruto(captura)
    return captura


def serializar_catalogo_bruto(captura) -> bytes:
    """Serializa deterministicamente sem canonicalizar ou preencher valores."""
    captura = validar_catalogo_bruto(captura)
    texto = json.dumps(
        captura, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    ) + "\n"
    return texto.encode("utf-8")


def _json_sem_chaves_duplicadas(pares):
    objeto = {}
    for chave, valor in pares:
        if chave in objeto:
            raise ValueError(f"Chave JSON duplicada rejeitada: {chave}.")
        objeto[chave] = valor
    return objeto


def validar_bytes_catalogo_bruto(dados):
    if type(dados) is not bytes:
        raise ValueError("A fotografia deve ser fornecida em bytes.")
    try:
        texto = dados.decode("utf-8", errors="strict")
        captura = json.loads(texto, object_pairs_hook=_json_sem_chaves_duplicadas)
    except (UnicodeDecodeError, json.JSONDecodeError) as erro:
        raise ValueError("Fotografia não contém JSON UTF-8 válido.") from erro
    captura = validar_catalogo_bruto(captura)
    if serializar_catalogo_bruto(captura) != dados:
        raise ValueError("Fotografia não usa a serialização determinística aprovada.")
    return captura


def _consultar_linhas_brutas(conexao, consulta, campos):
    with conexao.cursor() as cursor:
        cursor.execute(consulta)
        return [
            {
                campo: _normalizar_json_bruto(valor)
                for campo, valor in zip(campos, linha)
            }
            for linha in cursor.fetchall()
        ]


def coletar_catalogo_bruto(conexao, metadados):
    """Coleta linhas diretamente de pg_catalog, sem usar expectativas offline."""
    constraints = _consultar_linhas_brutas(
        conexao,
        "SELECT con.oid, n.nspname, c.relname, con.conname, con.contype, "
        "con.conkey, con.convalidated, con.conislocal, con.coninhcount, "
        "con.connoinherit, con.condeferrable, con.condeferred, "
        "pg_catalog.pg_get_constraintdef(con.oid, true), "
        "pg_catalog.pg_get_expr(con.conbin, con.conrelid, true), "
        "CASE WHEN con.conkey IS NULL THEN NULL ELSE ARRAY("
        "SELECT a.attname FROM pg_catalog.unnest(con.conkey) WITH ORDINALITY k(attnum, ordem) "
        "LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=con.conrelid "
        "AND a.attnum=k.attnum AND a.attnum>0 AND NOT a.attisdropped ORDER BY k.ordem) END "
        "FROM pg_catalog.pg_constraint con "
        "JOIN pg_catalog.pg_class c ON c.oid=con.conrelid "
        "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
        "WHERE n.nspname='public' AND c.relname IN "
        "('schema_migrations','schema_migration_execucoes') "
        "ORDER BY n.nspname,c.relname,con.conname",
        (
            "oid_evidencia", "schema", "tabela", "conname", "contype",
            "conkey", "convalidated", "conislocal", "coninhcount",
            "connoinherit", "condeferrable", "condeferred",
            "pg_get_constraintdef", "pg_get_expr_conbin", "colunas_resolvidas",
        ),
    )
    indices = _consultar_linhas_brutas(
        conexao,
        "SELECT ix.indexrelid, ni.nspname, i.relname, nt.nspname, t.relname, "
        "am.amname, ix.indisunique, ix.indisprimary, ix.indisexclusion, "
        "ix.indimmediate, ix.indisvalid, ix.indisready, ix.indislive, "
        "ix.indisclustered, ix.indisreplident, ix.indnullsnotdistinct, "
        "ix.indcheckxmin, ix.indnkeyatts, ix.indnatts, ix.indkey::smallint[], "
        "ix.indclass::oid[], ix.indcollation::oid[], ix.indoption::smallint[], "
        "ix.indexprs::text, ix.indpred::text, "
        "pg_catalog.pg_get_indexdef(ix.indexrelid,0,true), con.oid IS NOT NULL, "
        "i.relpersistence FROM pg_catalog.pg_index ix "
        "JOIN pg_catalog.pg_class i ON i.oid=ix.indexrelid "
        "JOIN pg_catalog.pg_class t ON t.oid=ix.indrelid "
        "JOIN pg_catalog.pg_namespace ni ON ni.oid=i.relnamespace "
        "JOIN pg_catalog.pg_namespace nt ON nt.oid=t.relnamespace "
        "JOIN pg_catalog.pg_am am ON am.oid=i.relam "
        "LEFT JOIN pg_catalog.pg_constraint con ON con.conindid=ix.indexrelid "
        "WHERE nt.nspname='public' AND t.relname IN "
        "('schema_migrations','schema_migration_execucoes') "
        "ORDER BY ni.nspname,i.relname",
        (
            "oid_evidencia", "indice_schema", "indice_nome", "tabela_schema",
            "tabela_nome", "metodo", "indisunique", "indisprimary",
            "indisexclusion", "indimmediate", "indisvalid", "indisready",
            "indislive", "indisclustered", "indisreplident",
            "indnullsnotdistinct", "indcheckxmin", "indnkeyatts", "indnatts",
            "indkey", "indclass", "indcollation", "indoption", "indexprs",
            "indpred", "pg_get_indexdef", "vinculado_constraint",
            "relpersistence",
        ),
    )
    operator_classes = _consultar_linhas_brutas(
        conexao,
        "SELECT ix.indexrelid,k.ordem,o.oid,n.nspname,o.opcname "
        "FROM pg_catalog.pg_index ix "
        "JOIN pg_catalog.pg_class t ON t.oid=ix.indrelid "
        "JOIN pg_catalog.pg_namespace nt ON nt.oid=t.relnamespace "
        "JOIN LATERAL pg_catalog.unnest(ix.indclass::oid[]) WITH ORDINALITY k(oid,ordem) ON true "
        "JOIN pg_catalog.pg_opclass o ON o.oid=k.oid "
        "JOIN pg_catalog.pg_namespace n ON n.oid=o.opcnamespace "
        "WHERE nt.nspname='public' AND t.relname IN "
        "('schema_migrations','schema_migration_execucoes') "
        "AND k.ordem<=ix.indnkeyatts ORDER BY ix.indexrelid,k.ordem",
        ("indice_oid_evidencia", "posicao", "oid_evidencia", "schema", "nome"),
    )
    collations = _consultar_linhas_brutas(
        conexao,
        "SELECT ix.indexrelid,k.ordem,NULLIF(k.oid,0),n.nspname,c.collname "
        "FROM pg_catalog.pg_index ix "
        "JOIN pg_catalog.pg_class t ON t.oid=ix.indrelid "
        "JOIN pg_catalog.pg_namespace nt ON nt.oid=t.relnamespace "
        "JOIN LATERAL pg_catalog.unnest(ix.indcollation::oid[]) WITH ORDINALITY k(oid,ordem) ON true "
        "LEFT JOIN pg_catalog.pg_collation c ON c.oid=k.oid "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid=c.collnamespace "
        "WHERE nt.nspname='public' AND t.relname IN "
        "('schema_migrations','schema_migration_execucoes') "
        "AND k.ordem<=ix.indnkeyatts ORDER BY ix.indexrelid,k.ordem",
        ("indice_oid_evidencia", "posicao", "oid_evidencia", "schema", "nome"),
    )
    sequencias = _consultar_linhas_brutas(
        conexao,
        "SELECT s.oid,ns.nspname,s.relname,s.relpersistence,nt.nspname,t.relname,"
        "a.attname,a.attnum,d.deptype,pg_catalog.format_type(ps.seqtypid,NULL),"
        "ps.seqstart,ps.seqincrement,ps.seqmin,ps.seqmax,ps.seqcache,ps.seqcycle "
        "FROM pg_catalog.pg_class s JOIN pg_catalog.pg_namespace ns ON ns.oid=s.relnamespace "
        "JOIN pg_catalog.pg_sequence ps ON ps.seqrelid=s.oid "
        "LEFT JOIN pg_catalog.pg_depend d ON d.classid="
        "'pg_catalog.pg_class'::pg_catalog.regclass "
        "AND d.objid=s.oid AND d.objsubid=0 AND d.refclassid="
        "'pg_catalog.pg_class'::pg_catalog.regclass AND d.refobjsubid>0 "
        "LEFT JOIN pg_catalog.pg_class t ON t.oid=d.refobjid "
        "LEFT JOIN pg_catalog.pg_namespace nt ON nt.oid=t.relnamespace "
        "LEFT JOIN pg_catalog.pg_attribute a ON a.attrelid=d.refobjid AND a.attnum=d.refobjsubid "
        "WHERE ns.nspname='public' AND s.relkind='S' ORDER BY ns.nspname,s.relname",
        (
            "oid_evidencia", "schema", "nome", "relpersistence",
            "tabela_schema", "tabela", "coluna", "numero_coluna",
            "tipo_dependencia", "tipo_sequencia", "inicio", "incremento",
            "minimo", "maximo", "cache", "cycle",
        ),
    )
    inventario = {}
    cobertura = []
    for categoria, spec in INVENTORY_SPECS.items():
        linhas = _consultar_linhas_brutas(
            conexao, spec["consulta"], spec["campos"],
        )
        inventario[categoria] = linhas
        cobertura.append({
            "categoria": categoria,
            "catalogo": spec["catalogo"],
            "consulta": spec["consulta"],
            "quantidade": len(linhas),
            "vazio": not linhas,
        })
    captura = {
        "metadados": dict(metadados),
        "pg_constraint": constraints,
        "pg_index": indices,
        "operator_classes": operator_classes,
        "collations": collations,
        "sequencias": sequencias,
        "inventario_public": inventario,
        "cobertura_inventario": cobertura,
    }
    return validar_catalogo_bruto(captura)


def _validar_destino_catalogo(destination: Path):
    if not isinstance(destination, Path) or not destination.is_absolute():
        raise ValueError("O destino deve ser um Path absoluto e explícito.")
    if ".." in destination.parts:
        raise ValueError("O destino não pode conter travessia de diretório.")
    raiz = REPOSITORY_ROOT.resolve(strict=True)
    fixtures = FIXTURES_DIR
    if (
        fixtures.parent != REPOSITORY_ROOT / "tests"
        or not fixtures.exists()
        or not fixtures.is_dir()
        or _e_reparse_point(fixtures)
        or _e_reparse_point(REPOSITORY_ROOT / "tests")
        or _e_reparse_point(REPOSITORY_ROOT)
        or fixtures.resolve(strict=True) != raiz / "tests" / "fixtures"
        or destination.parent != fixtures
        or destination.name != RAW_CATALOG_PATH.name
        or destination.suffix != ".json"
        or destination.resolve(strict=False) != RAW_CATALOG_PATH.resolve(strict=False)
        or _e_reparse_point(destination)
    ):
        raise ValueError("Destino da fotografia bruta não autorizado.")
    if destination.exists() and not destination.is_file():
        raise ValueError("Destino existente não é arquivo regular.")
    estado = fixtures.stat()
    if not stat.S_ISDIR(estado.st_mode):
        raise ValueError("Diretório de fixtures não é regular.")
    return (estado.st_dev, estado.st_ino)


def _e_reparse_point(caminho: Path):
    """Detecta symlink/junction quando a plataforma expõe essa informação."""
    if caminho.is_symlink():
        return True
    is_junction = getattr(caminho, "is_junction", None)
    if is_junction is not None and is_junction():
        return True
    try:
        estado = caminho.lstat()
    except FileNotFoundError:
        return False
    atributo = getattr(estado, "st_file_attributes", 0)
    return bool(atributo & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0))


def _ler_arquivo_seguro(caminho: Path) -> bytes:
    flags = os.O_RDONLY | getattr(os, "O_BINARY", 0) | getattr(os, "O_NOFOLLOW", 0)
    descritor = os.open(caminho, flags)
    try:
        estado = os.fstat(descritor)
        if not stat.S_ISREG(estado.st_mode):
            raise ValueError("Arquivo de fotografia não é regular.")
        partes = []
        while True:
            parte = os.read(descritor, 1024 * 1024)
            if not parte:
                break
            partes.append(parte)
        return b"".join(partes)
    finally:
        os.close(descritor)


def _criar_temporario_exclusivo(diretorio: Path):
    flags = (
        os.O_CREAT | os.O_EXCL | os.O_WRONLY | getattr(os, "O_BINARY", 0)
        | getattr(os, "O_NOFOLLOW", 0)
    )
    for _ in range(32):
        caminho = diretorio / f".h2c4a2-{secrets.token_hex(16)}.tmp"
        try:
            return caminho, os.open(caminho, flags, 0o600)
        except FileExistsError:
            continue
    raise FileExistsError("Não foi possível criar temporário exclusivo.")


def _escrever_todos(arquivo, dados):
    restante = memoryview(dados)
    while restante:
        escritos = arquivo.write(restante)
        if (
            type(escritos) is not int
            or escritos <= 0
            or escritos > len(restante)
        ):
            raise OSError("Retorno inválido durante a escrita completa.")
        restante = restante[escritos:]


def _flush_arquivo(arquivo):
    arquivo.flush()


def _fsync_arquivo(arquivo):
    os.fsync(arquivo.fileno())


def _fechar_arquivo(arquivo):
    arquivo.close()


def _escrever_sincronizar_fechar(descritor, dados):
    try:
        arquivo = os.fdopen(descritor, "wb", buffering=0)
    except BaseException as erro_abertura:
        try:
            os.close(descritor)
        except OSError:
            erro_abertura.add_note("Falha secundária ao fechar descritor sem objeto de arquivo.")
        raise
    try:
        _escrever_todos(arquivo, dados)
        _flush_arquivo(arquivo)
        _fsync_arquivo(arquivo)
    except BaseException as erro_principal:
        try:
            _fechar_arquivo(arquivo)
        except BaseException:
            erro_principal.add_note("Falha secundária ao fechar o temporário.")
        raise
    _fechar_arquivo(arquivo)


def _sincronizar_diretorio(diretorio: Path):
    """No POSIX sincroniza o diretório; no Windows não há API portátil igual."""
    if os.name != "posix":
        return
    descritor = os.open(diretorio, os.O_RDONLY)
    try:
        os.fsync(descritor)
    finally:
        os.close(descritor)


def _autorizar_estado_anterior(destination, ambiente):
    if not destination.exists():
        return None
    if ambiente.get("H2C4A2_REPLACE_RAW_CATALOG") != "1":
        raise FileExistsError("Fotografia existente exige autorização específica.")
    checksum_esperado = ambiente.get("H2C4A2_EXPECTED_EXISTING_SHA256")
    if type(checksum_esperado) is not str or not _SHA256.fullmatch(checksum_esperado):
        raise ValueError("Checksum da evidência anterior ausente ou inválido.")
    anterior = _ler_arquivo_seguro(destination)
    validar_bytes_catalogo_bruto(anterior)
    if hashlib.sha256(anterior).hexdigest() != checksum_esperado:
        raise ValueError("Checksum da evidência anterior diverge da autorização.")
    return anterior


def gravar_catalogo_bruto_se_habilitado(
    captura, destination: Path, *, ambiente=None,
):
    """Grava atomicamente somente no modo futuro explicitamente habilitado.

    O temporário usa o mesmo diretório. POSIX oferece O_NOFOLLOW e fsync do
    diretório; no Windows, O_EXCL e os.replace são as melhores garantias
    portáteis disponíveis, sem promessa de durabilidade absoluta.
    """
    ambiente = os.environ if ambiente is None else ambiente
    if not (
        ambiente.get("H2C4A2_ADMIN_DSN")
        and ambiente.get("H2C4A2_CAPTURE_RAW_CATALOG") == "1"
    ):
        return False
    identidade_diretorio = _validar_destino_catalogo(destination)
    dados = serializar_catalogo_bruto(captura)
    validar_bytes_catalogo_bruto(dados)
    checksum_novo = hashlib.sha256(dados).hexdigest()
    anterior = _autorizar_estado_anterior(destination, ambiente)
    temporario = None
    erro_principal = None
    substituido = False
    try:
        temporario, descritor = _criar_temporario_exclusivo(destination.parent)
        _escrever_sincronizar_fechar(descritor, dados)
        temporario_lido = _ler_arquivo_seguro(temporario)
        if (
            temporario_lido != dados
            or hashlib.sha256(temporario_lido).hexdigest() != checksum_novo
        ):
            raise ValueError("Temporário diverge dos bytes preparados.")
        validar_bytes_catalogo_bruto(temporario_lido)
        if _validar_destino_catalogo(destination) != identidade_diretorio:
            raise ValueError("Identidade do diretório mudou durante a gravação.")
        if anterior is None:
            if destination.exists():
                raise FileExistsError("Destino surgiu durante a gravação.")
        else:
            atual = _ler_arquivo_seguro(destination)
            if atual != anterior:
                raise ValueError("Evidência anterior mudou durante a gravação.")
        os.replace(temporario, destination)
        substituido = True
        _sincronizar_diretorio(destination.parent)
        final = _ler_arquivo_seguro(destination)
        if final != dados or hashlib.sha256(final).hexdigest() != checksum_novo:
            raise ValueError("Verificação final da fotografia falhou.")
        validar_bytes_catalogo_bruto(final)
        return True
    except BaseException as erro:
        erro_principal = erro
        if substituido:
            erro.add_note("O replace ocorreu; o estado final foi reportado como não confirmado.")
        raise
    finally:
        if temporario is not None and temporario.exists():
            try:
                temporario.unlink()
            except OSError as erro_limpeza:
                if erro_principal is not None:
                    erro_principal.add_note("Falha secundária ao remover temporário.")
                else:
                    raise erro_limpeza


_E1_REAL_STAGE_METHODS = MappingProxyType({
    E1Stage.PRECHECK: "precheck",
    E1Stage.DOCKER_IMAGE: "validar_imagem_docker",
    E1Stage.TEMP_CREDENTIALS: "criar_contexto_temporario",
    E1Stage.CONTAINER_CREATE: "criar_container",
    E1Stage.CONTAINER_ISOLATION: "validar_isolamento",
    E1Stage.CONTAINER_HEALTH: "aguardar_health",
    E1Stage.PORT_DISCOVERY: "descobrir_porta",
    E1Stage.DB_CONNECT: "conectar_postgresql",
    E1Stage.POSTGRES_VERSION: "consultar_server_version",
    E1Stage.SERVER_VERSION_NUM: "consultar_server_version_num",
    E1Stage.POSTGRES_VERSION_VALIDATE: "validar_postgresql_15_18",
    E1Stage.NEW_DATABASE_CHECK: "confirmar_banco_novo",
    E1Stage.M0001_CHECKSUM: "validar_checksum_m0001",
    E1Stage.M0001_READ: "ler_m0001",
    E1Stage.M0001_APPLY: "aplicar_m0001",
    E1Stage.M0001_COMMIT: "commit_m0001",
    E1Stage.CAPTURE_PREPARE: "preparar_captura",
    E1Stage.CATALOG_COLLECT: "coletar_catalogo",
    E1Stage.CATALOG_SERIALIZE: "serializar_catalogo",
    E1Stage.CAPTURE_WRITE: "gravar_fotografia",
    E1Stage.CAPTURE_VALIDATE: "validar_fotografia",
    E1Stage.CAPTURE_HASH: "calcular_hash",
    E1Stage.CLEANUP_CONNECTIONS: "fechar_conexoes",
    E1Stage.CLEANUP_ENV: "limpar_ambiente",
    E1Stage.CLEANUP_CONTAINER: "remover_container",
    E1Stage.CLEANUP_VERIFY_CONTAINER: "confirmar_container_ausente",
    E1Stage.CLEANUP_VERIFY_PORT: "confirmar_porta_liberada",
    E1Stage.CLEANUP_VERIFY_VOLUME: "confirmar_volume_ausente",
})


def _evidencia_etapa(stage):
    return E1StepEvidence(stage)


def _validar_versao_e1(version, version_num):
    if (
        type(version) is not str
        or version != POSTGRES_VERSION
        or type(version_num) is not int
        or version_num != POSTGRES_VERSION_NUM
    ):
        raise ValueError("Versao PostgreSQL fora do contrato E1.")
    return E1StepEvidence(E1Stage.POSTGRES_VERSION_VALIDATE)


class E1RealAdapter(E1Adapter):
    """Implementacao concreta; seus efeitos ocorrem somente pela entrada publica."""

    @staticmethod
    def _docker(*argumentos, check=True, credential_env=None):
        chaves_tecnicas = (
            "PATH", "PATHEXT", "SYSTEMROOT", "WINDIR", "COMSPEC", "TEMP", "TMP",
            "USERPROFILE", "APPDATA", "LOCALAPPDATA", "PROGRAMDATA",
            "HOMEDRIVE", "HOMEPATH",
        )
        ambiente = {
            chave: os.environ[chave] for chave in chaves_tecnicas if chave in os.environ
        }
        if credential_env is not None:
            if (
                type(credential_env) is not dict
                or set(credential_env) != {"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"}
                or any(type(valor) is not str or not valor for valor in credential_env.values())
            ):
                raise E1ContractError("Ambiente filho Docker fora do contrato fechado.")
            ambiente.update(credential_env)
        return subprocess.run(
            ("docker", "--context", DOCKER_CONTEXT) + tuple(argumentos),
            check=check, capture_output=True, text=True, encoding="utf-8",
            errors="strict", env=ambiente, shell=False,
        )

    def _scalar(self, comando):
        with self.state.connection.cursor() as cursor:
            cursor.execute(comando)
            linha = cursor.fetchone()
        if not linha or len(linha) != 1:
            raise E1ContractError("Consulta escalar E1 sem resultado unico.")
        return linha[0]

    def precheck(self):
        operacional = type(self.config) is E1Config
        guardas_invalidas = operacional and (
            os.environ.get("H2C4A2_E1_AUTHORIZED") != "1"
            or os.environ.get("H2C4A2_CAPTURE_RAW_CATALOG") != "1"
            or "DOCKER_HOST" in os.environ
            or "DOCKER_CONTEXT" in os.environ
        )
        if guardas_invalidas or self.config.destination.exists():
            raise E1ContractError("Guardas explicitas da E1 nao satisfeitas.")
        return _evidencia_etapa(E1Stage.PRECHECK)

    def validar_imagem_docker(self):
        contexto = json.loads(self._docker("context", "inspect", DOCKER_CONTEXT).stdout)
        try:
            endpoint = contexto[0]["Endpoints"]["docker"]["Host"]
        except (IndexError, KeyError, TypeError):
            raise E1ContractError("Contexto Docker local nao comprovado.") from None
        if (
            type(contexto) is not list or len(contexto) != 1
            or type(endpoint) is not str
            or endpoint.lower() != DOCKER_LOCAL_ENDPOINT.lower()
            or endpoint.lower().startswith(("tcp://", "ssh://"))
        ):
            raise E1ContractError("Contexto Docker local nao comprovado.")
        informacao = json.loads(
            self._docker("image", "inspect", POSTGRES_IMMUTABLE_REFERENCE).stdout
        )
        image_id = informacao[0].get("Id") if type(informacao) is list and len(informacao) == 1 else None
        if (
            type(informacao) is not list or len(informacao) != 1
            or type(image_id) is not str
            or re.fullmatch(r"sha256:[0-9a-f]{64}", image_id) is None
            or not any(
                item.endswith("@" + self.config.approved_digest)
                for item in informacao[0].get("RepoDigests", [])
            )
            or informacao[0].get("Os") != "linux"
            or informacao[0].get("Architecture") != "amd64"
        ):
            raise E1ContractError("Imagem Docker fora do digest aprovado.")
        self.state.expected_image_id = image_id
        volumes = self._docker("volume", "ls", "--format", "{{.Name}}").stdout.splitlines()
        self.state.initial_volumes = frozenset(volumes)
        return _evidencia_etapa(E1Stage.DOCKER_IMAGE)

    def criar_contexto_temporario(self):
        token = secrets.token_hex(16)
        self.state.container_name = "sistema-recic3-h2c4a2-e1-" + token
        self.state.temporary_user = "e1u_" + secrets.token_hex(12)
        self.state.temporary_password = secrets.token_urlsafe(32)
        self.state.temporary_database = "e1db_" + secrets.token_hex(12)
        return _evidencia_etapa(E1Stage.TEMP_CREDENTIALS)

    def criar_container(self):
        credenciais = {
            "POSTGRES_USER": self.state.temporary_user,
            "POSTGRES_PASSWORD": self.state.temporary_password,
            "POSTGRES_DB": self.state.temporary_database,
        }
        self._docker(
            "run", "--detach", "--rm", "--name", self.state.container_name,
            "--label", "h2c4a2=e1", "--tmpfs",
            "/var/lib/postgresql/data:rw,noexec,nosuid,size=512m",
            "--publish", "127.0.0.1::5432",
            "--env", "POSTGRES_USER",
            "--env", "POSTGRES_PASSWORD",
            "--env", "POSTGRES_DB",
            "--health-cmd", "pg_isready -U $POSTGRES_USER -d $POSTGRES_DB",
            "--health-interval", "1s", "--health-timeout", "3s",
            "--health-retries", "30", POSTGRES_IMMUTABLE_REFERENCE,
            credential_env=credenciais,
        )
        return _evidencia_etapa(E1Stage.CONTAINER_CREATE)

    def validar_isolamento(self):
        dados = json.loads(self._docker("inspect", self.state.container_name).stdout)
        if type(dados) is not list or len(dados) != 1:
            raise E1ContractError("Inspect do container E1 invalido.")
        info = dados[0]
        tmpfs = info.get("HostConfig", {}).get("Tmpfs", {})
        portas = info.get("NetworkSettings", {}).get("Ports", {}).get("5432/tcp")
        mounts = info.get("Mounts", [])
        if (
            not info.get("HostConfig", {}).get("AutoRemove")
            or "/var/lib/postgresql/data" not in tmpfs
            or type(portas) is not list or len(portas) != 1
            or portas[0].get("HostIp") != "127.0.0.1"
            or type(mounts) is not list or bool(mounts)
            or info.get("Image") != self.state.expected_image_id
            or info.get("Config", {}).get("Image") != POSTGRES_IMMUTABLE_REFERENCE
        ):
            raise E1ContractError("Isolamento do container E1 nao confirmado.")
        return _evidencia_etapa(E1Stage.CONTAINER_ISOLATION)

    def aguardar_health(self):
        limite = time.monotonic() + self.config.health_timeout_seconds
        while time.monotonic() < limite:
            estado = self._docker(
                "inspect", "--format", "{{.State.Health.Status}}",
                self.state.container_name,
            ).stdout.strip()
            if estado == "healthy":
                return _evidencia_etapa(E1Stage.CONTAINER_HEALTH)
            if estado == "unhealthy":
                break
            time.sleep(1)
        raise TimeoutError("Health do container E1 nao confirmado.")

    def descobrir_porta(self):
        saida = self._docker("port", self.state.container_name, "5432/tcp").stdout.strip()
        correspondencia = re.fullmatch(r"127\.0\.0\.1:(\d+)", saida)
        if not correspondencia:
            raise E1ContractError("Porta loopback E1 nao confirmada.")
        porta = int(correspondencia.group(1))
        if not 1 <= porta <= 65535:
            raise E1ContractError("Porta E1 fora do intervalo valido.")
        self.state.port = porta
        return _evidencia_etapa(E1Stage.PORT_DISCOVERY)

    def conectar_postgresql(self):
        self.state.dsn = make_dsn(
            host="127.0.0.1", port=self.state.port,
            user=self.state.temporary_user, password=self.state.temporary_password,
            dbname=self.state.temporary_database,
        )
        os.environ["H2C4A2_ADMIN_DSN"] = self.state.dsn
        self.state.connection = psycopg2.connect(self.state.dsn)
        if self.state.connection is None or self.state.connection.closed:
            raise E1ContractError("Conexao PostgreSQL E1 nao confirmada.")
        return _evidencia_etapa(E1Stage.DB_CONNECT)

    def consultar_server_version(self):
        self.state.postgres_version = str(self._scalar("SHOW server_version")).split()[0]
        return _evidencia_etapa(E1Stage.POSTGRES_VERSION)

    def consultar_server_version_num(self):
        self.state.server_version_num = self._scalar(
            "SELECT pg_catalog.current_setting('server_version_num')::pg_catalog.int4"
        )
        return _evidencia_etapa(E1Stage.SERVER_VERSION_NUM)

    def validar_postgresql_15_18(self):
        return _validar_versao_e1(
            self.state.postgres_version, self.state.server_version_num,
        )

    def confirmar_banco_novo(self):
        resultado = classificar_preflight(
            coletar_snapshot(self.state.connection), carregar_manifesto(),
        )
        if (
            resultado.classificacao is not DatabaseClassification.BANCO_NOVO
            or not resultado.pode_prosseguir
        ):
            raise E1ContractError("Banco E1 nao foi classificado como novo.")
        return _evidencia_etapa(E1Stage.NEW_DATABASE_CHECK)

    def validar_checksum_m0001(self):
        caminho = self.config.repository_root / "migrations_control" / "sql" / "M0001_criar_ledger.sql"
        dados = caminho.read_bytes()
        normalizados = dados.replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        checksum = hashlib.sha256(normalizados).hexdigest()
        if checksum != self.config.expected_m0001_checksum:
            raise E1ContractError("Checksum da M0001 divergente.")
        self.state.m0001_checksum = checksum
        return _evidencia_etapa(E1Stage.M0001_CHECKSUM)

    def ler_m0001(self):
        caminho = self.config.repository_root / "migrations_control" / "sql" / "M0001_criar_ledger.sql"
        dados = caminho.read_bytes().replace(b"\r\n", b"\n").replace(b"\r", b"\n")
        if hashlib.sha256(dados).hexdigest() != self.state.m0001_checksum:
            raise E1ContractError("M0001 mudou entre checksum e leitura.")
        self.state.m0001_sql = dados.decode("utf-8", errors="strict")
        return _evidencia_etapa(E1Stage.M0001_READ)

    def aplicar_m0001(self):
        with self.state.connection.cursor() as cursor:
            cursor.execute(self.state.m0001_sql)
        self.state.m0001_applied = True
        return M0001AppliedEvidence(self.state.m0001_checksum)

    def commit_m0001(self):
        if not self.state.m0001_applied:
            raise E1ContractError("M0001 nao aplicada antes do commit.")
        self.state.connection.commit()
        self.state.m0001_committed = True
        return _evidencia_etapa(E1Stage.M0001_COMMIT)

    def preparar_captura(self):
        agora = datetime.now(timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z")
        self.state.metadata = {
            "formato": "h2c4a2-pg-catalog-raw", "formato_versao": 1,
            "postgres_version": self.state.postgres_version,
            "server_version_num": self.state.server_version_num,
            "container_image": self.config.approved_image,
            "container_image_digest": self.config.approved_digest,
            "captured_at_utc": agora, "capture_id": "capture-" + uuid4().hex,
            "m0001_checksum": self.state.m0001_checksum, "manifesto_versao": 1,
        }
        return _evidencia_etapa(E1Stage.CAPTURE_PREPARE)

    def coletar_catalogo(self):
        self.state.catalog = coletar_catalogo_bruto(self.state.connection, self.state.metadata)
        self.state.category_counts = {
            categoria: len(self.state.catalog["inventario_public"][categoria])
            for categoria in E1_SUCCESS_CATEGORIES
        }
        return CatalogCollectedEvidence(tuple(self.state.category_counts.items()))

    def serializar_catalogo(self):
        self.state.serialized = serializar_catalogo_bruto(self.state.catalog)
        return SerializedCaptureEvidence(len(self.state.serialized))

    def gravar_fotografia(self):
        existia_antes = self.config.destination.exists()
        try:
            gravada = gravar_catalogo_bruto_se_habilitado(
                self.state.catalog, self.config.destination, ambiente=os.environ,
            )
        finally:
            if not existia_antes and self.config.destination.is_file():
                dados = _ler_arquivo_seguro(self.config.destination)
                if type(self.state.serialized) is bytes and dados == self.state.serialized:
                    self.state.capture_created_this_run = True
                    self.state.capture_written_sha256 = hashlib.sha256(dados).hexdigest()
                    self.state.capture_written_capture_id = self.state.metadata["capture_id"]
                    self.state.written_path = self.config.destination
        if gravada is not True:
            raise E1ContractError("Gravacao da fotografia nao confirmada.")
        self.state.written_path = self.config.destination
        return WrittenCaptureEvidence(self.config.destination.stat().st_size)

    def validar_fotografia(self):
        dados = _ler_arquivo_seguro(self.config.destination)
        self.state.validated_catalog = validar_bytes_catalogo_bruto(dados)
        metadados = self.state.validated_catalog["metadados"]
        contagens = tuple(
            (categoria, len(self.state.validated_catalog["inventario_public"][categoria]))
            for categoria in E1_SUCCESS_CATEGORIES
        )
        return ValidatedCaptureEvidence(
            metadados["capture_id"], metadados["captured_at_utc"], contagens,
        )

    def calcular_hash(self):
        dados = _ler_arquivo_seguro(self.config.destination)
        self.state.photograph_sha256 = hashlib.sha256(dados).hexdigest()
        return CaptureHashEvidence(self.state.photograph_sha256, len(dados))

    def fechar_conexoes(self):
        conexao = self.state.connection
        if conexao is not None and not conexao.closed:
            conexao.close()
        if conexao is not None and not conexao.closed:
            return E1CleanupEvidence(E1Stage.CLEANUP_CONNECTIONS, E1CheckState.FALSE)
        self.state.connection = None
        return E1CleanupEvidence(E1Stage.CLEANUP_CONNECTIONS, E1CheckState.TRUE)

    def limpar_ambiente(self):
        for chave in (
            "H2C4A2_ADMIN_DSN", "H2C4A2_CAPTURE_RAW_CATALOG",
            "H2C4A2_E1_AUTHORIZED", "H2C4A2_REPLACE_RAW_CATALOG",
            "H2C4A2_EXPECTED_EXISTING_SHA256",
        ):
            os.environ.pop(chave, None)
        self.state.dsn = None
        self.state.temporary_user = None
        self.state.temporary_password = None
        self.state.temporary_database = None
        self.state.cleanup_env_cleared = not any(
            chave in os.environ for chave in (
                "H2C4A2_ADMIN_DSN", "H2C4A2_CAPTURE_RAW_CATALOG",
                "H2C4A2_E1_AUTHORIZED", "H2C4A2_REPLACE_RAW_CATALOG",
                "H2C4A2_EXPECTED_EXISTING_SHA256",
            )
        )
        return E1CleanupEvidence(
            E1Stage.CLEANUP_ENV,
            E1CheckState.TRUE if self.state.cleanup_env_cleared else E1CheckState.FALSE,
        )

    def remover_container(self):
        if self.state.container_name:
            self._docker("rm", "--force", self.state.container_name, check=False)
        self.state.cleanup_container_requested = True
        return E1CleanupEvidence(E1Stage.CLEANUP_CONTAINER, E1CheckState.TRUE)

    def confirmar_container_ausente(self):
        if self.state.container_name:
            resultado = self._docker(
                "ps", "-aq", "--filter", "name=^/" + self.state.container_name + "$",
                check=False,
            )
            if resultado.returncode != 0:
                raise E1ContractError("Docker nao confirmou ausencia do container.")
            linhas = resultado.stdout.splitlines()
            if any(re.fullmatch(r"[0-9a-f]{12,64}", linha) is None for linha in linhas):
                raise E1ContractError("Resposta Docker de ausencia malformada.")
            ausente = not linhas
        else:
            ausente = True
        self.state.cleanup_container_absent = ausente
        return E1CleanupEvidence(
            E1Stage.CLEANUP_VERIFY_CONTAINER,
            E1CheckState.TRUE if ausente else E1CheckState.FALSE,
        )

    def confirmar_porta_liberada(self):
        if self.state.port is None:
            liberada = True
        else:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as cliente:
                cliente.settimeout(0.2)
                liberada = cliente.connect_ex(("127.0.0.1", self.state.port)) != 0
        self.state.cleanup_port_released = liberada
        return E1CleanupEvidence(
            E1Stage.CLEANUP_VERIFY_PORT,
            E1CheckState.TRUE if liberada else E1CheckState.FALSE,
        )

    def confirmar_volume_ausente(self):
        if self.state.container_name is None and not self.state.initial_volumes:
            ausente = True
        else:
            atuais = frozenset(
                self._docker("volume", "ls", "--format", "{{.Name}}").stdout.splitlines()
            )
            ausente = atuais == self.state.initial_volumes
        self.state.cleanup_volume_absent = ausente
        return E1CleanupEvidence(
            E1Stage.CLEANUP_VERIFY_VOLUME,
            E1CheckState.TRUE if ausente else E1CheckState.FALSE,
        )


def _validar_pos_condicao_e1(stage, adapter, evidencia):
    estado = adapter.state
    if stage is E1Stage.M0001_APPLY:
        valido = (
            type(evidencia) is M0001AppliedEvidence
            and evidencia.checksum == adapter.config.expected_m0001_checksum
            and estado.m0001_applied is True
        )
    elif stage is E1Stage.CATALOG_COLLECT:
        valido = (
            type(evidencia) is CatalogCollectedEvidence
            and tuple(dict(evidencia.category_counts)) == E1_SUCCESS_CATEGORIES
            and type(estado.catalog) is dict
        )
    elif stage is E1Stage.CATALOG_SERIALIZE:
        valido = (
            type(evidencia) is SerializedCaptureEvidence
            and type(evidencia.photograph_size) is int
            and evidencia.photograph_size > 0
            and type(estado.serialized) is bytes
            and len(estado.serialized) == evidencia.photograph_size
        )
    elif stage is E1Stage.CAPTURE_WRITE:
        valido = (
            type(evidencia) is WrittenCaptureEvidence
            and type(evidencia.photograph_size) is int
            and evidencia.photograph_size > 0
            and estado.written_path == adapter.config.destination
            and adapter.config.destination.is_file()
            and adapter.config.destination.stat().st_size == evidencia.photograph_size
        )
    elif stage is E1Stage.CAPTURE_VALIDATE:
        valido = (
            type(evidencia) is ValidatedCaptureEvidence
            and _CAPTURE_ID.fullmatch(evidencia.capture_id) is not None
            and tuple(dict(evidencia.category_counts)) == E1_SUCCESS_CATEGORIES
            and type(estado.validated_catalog) is dict
        )
    elif stage is E1Stage.CAPTURE_HASH:
        dados = _ler_arquivo_seguro(adapter.config.destination)
        valido = (
            type(evidencia) is CaptureHashEvidence
            and _SHA256.fullmatch(evidencia.photograph_sha256) is not None
            and type(evidencia.photograph_size) is int
            and evidencia.photograph_size > 0
            and hashlib.sha256(dados).hexdigest() == evidencia.photograph_sha256
            and len(dados) == evidencia.photograph_size
        )
    else:
        valido = type(evidencia) is E1StepEvidence and evidencia.stage is stage
        if stage is E1Stage.CONTAINER_CREATE:
            valido = valido and type(estado.container_name) is str and bool(estado.container_name)
        elif stage is E1Stage.PORT_DISCOVERY:
            valido = valido and type(estado.port) is int and 1 <= estado.port <= 65535
        elif stage is E1Stage.DB_CONNECT:
            valido = valido and estado.connection is not None
        elif stage is E1Stage.POSTGRES_VERSION:
            valido = valido and type(estado.postgres_version) is str
        elif stage is E1Stage.SERVER_VERSION_NUM:
            valido = valido and estado.server_version_num is not None
        elif stage is E1Stage.POSTGRES_VERSION_VALIDATE:
            valido = valido and (
                estado.postgres_version == POSTGRES_VERSION
                and type(estado.server_version_num) is int
                and estado.server_version_num == POSTGRES_VERSION_NUM
            )
        elif stage is E1Stage.M0001_CHECKSUM:
            valido = valido and estado.m0001_checksum == adapter.config.expected_m0001_checksum
        elif stage is E1Stage.M0001_READ:
            valido = valido and type(estado.m0001_sql) is str and bool(estado.m0001_sql)
        elif stage is E1Stage.M0001_COMMIT:
            valido = valido and estado.m0001_committed is True
        elif stage is E1Stage.CAPTURE_PREPARE:
            valido = valido and type(estado.metadata) is dict
    if not valido:
        raise E1ContractError("Pos-condicao obrigatoria da etapa E1 nao comprovada.")


def _construir_outcome_e1(adapter, cleanup):
    destino = adapter.config.destination
    if not destino.is_file():
        raise E1ContractError("Fotografia final obrigatoria ausente.")
    dados = _ler_arquivo_seguro(destino)
    catalogo = validar_bytes_catalogo_bruto(dados)
    checksum = hashlib.sha256(dados).hexdigest()
    if checksum != adapter.state.photograph_sha256:
        raise E1ContractError("Hash final da fotografia divergente.")
    metadados = catalogo["metadados"]
    contagens = tuple(
        (categoria, len(catalogo["inventario_public"][categoria]))
        for categoria in E1_SUCCESS_CATEGORIES
    )
    cleanup_receipt = tuple(
        (campo, getattr(cleanup, campo).value)
        for campo in E1CleanupTelemetry.__dataclass_fields__
    )
    return E1FlowOutcome(
        photograph_relative_path="tests/fixtures/h2c4a2_pg15_18_catalogo_bruto.json",
        photograph_sha256=checksum, photograph_size=len(dados),
        capture_id=metadados["capture_id"], captured_at_utc=metadados["captured_at_utc"],
        postgres_version=metadados["postgres_version"],
        server_version_num=metadados["server_version_num"],
        image_digest=metadados["container_image_digest"],
        category_counts=contagens, cleanup=cleanup_receipt,
    )


def _remover_fotografia_criada_em_falha(adapter):
    estado = adapter.state
    destino = adapter.config.destination
    if not estado.capture_created_this_run:
        return
    if (
        estado.written_path != destino
        or destino.is_symlink()
        or not destino.is_file()
        or type(estado.capture_written_sha256) is not str
        or type(estado.capture_written_capture_id) is not str
    ):
        raise E1ContractError("Fotografia da tentativa falha nao identificada com seguranca.")
    dados = _ler_arquivo_seguro(destino)
    if hashlib.sha256(dados).hexdigest() != estado.capture_written_sha256:
        raise E1ContractError("Fotografia da tentativa falha mudou antes do rollback.")
    destino.unlink()
    if destino.exists():
        raise E1ContractError("Fotografia da tentativa falha permaneceu no destino.")
    estado.capture_created_this_run = False


def _executar_fluxo_e1(config, adapter):
    """Runner compartilhado: retorna somente outcome interno, nunca receipt oficial."""
    if type(config) not in (E1Config, E1TestConfig) or not isinstance(adapter, E1Adapter):
        raise TypeError("Execucao interna E1 exige configuracao e adaptador nominais.")
    cleanup = E1CleanupTelemetry()
    erro_principal = None
    erro_cleanup = None
    for stage in E1_EXECUTION_STAGES:
        metodo = getattr(adapter, _E1_REAL_STAGE_METHODS[stage])
        try:
            evidencia = metodo()
            _validar_pos_condicao_e1(stage, adapter, evidencia)
        except Exception as erro:
            erro_principal = _sanitizar_erro_e1(stage, erro)
            break

    cleanup.cleanup_started = E1CheckState.TRUE
    campos_cleanup = {
        E1Stage.CLEANUP_CONNECTIONS: "cleanup_connection_closed",
        E1Stage.CLEANUP_ENV: "cleanup_env_cleared",
        E1Stage.CLEANUP_CONTAINER: "cleanup_container_requested",
        E1Stage.CLEANUP_VERIFY_CONTAINER: "cleanup_container_absent",
        E1Stage.CLEANUP_VERIFY_PORT: "cleanup_port_released",
        E1Stage.CLEANUP_VERIFY_VOLUME: "cleanup_volume_absent",
    }
    for stage in E1_CLEANUP_STAGES:
        metodo = getattr(adapter, _E1_REAL_STAGE_METHODS[stage])
        try:
            evidencia = metodo()
            if type(evidencia) is not E1CleanupEvidence or evidencia.stage is not stage:
                raise E1ContractError("Evidencia nominal de cleanup ausente.")
            setattr(cleanup, campos_cleanup[stage], evidencia.state)
            if evidencia.state is not E1CheckState.TRUE:
                seguro = _sanitizar_erro_e1(stage, E1CleanupCheckFailed())
                if erro_principal is None:
                    erro_principal = seguro
                elif erro_cleanup is None:
                    erro_cleanup = seguro
        except Exception as erro:
            setattr(cleanup, campos_cleanup[stage], E1CheckState.FALSE)
            seguro = _sanitizar_erro_e1(stage, erro)
            if erro_principal is None:
                erro_principal = seguro
            elif erro_cleanup is None:
                erro_cleanup = seguro

    if erro_principal is None:
        try:
            return _construir_outcome_e1(adapter, cleanup)
        except Exception as erro:
            erro_principal = _sanitizar_erro_e1(E1Stage.CAPTURE_VALIDATE, erro)
    try:
        _remover_fotografia_criada_em_falha(adapter)
    except Exception as erro:
        seguro = _sanitizar_erro_e1(E1Stage.CLEANUP_PHOTOGRAPH, erro)
        if erro_cleanup is None:
            erro_cleanup = seguro
    raise H2C4A2E1Failure(E1FailureTelemetry(
        code="H2C4A2_E1_FAILURE", primary_error=erro_principal,
        cleanup=cleanup, cleanup_error=erro_cleanup,
    )) from None


def _validar_pre_receipt_operacional(outcome, config, adapter):
    if (
        type(outcome) is not E1FlowOutcome
        or type(config) is not E1Config
        or type(adapter) is not E1RealAdapter
        or adapter.config is not config
        or config.repository_root != REPOSITORY_ROOT
        or config.destination != RAW_CATALOG_PATH
    ):
        raise E1ContractError("Receipt operacional exige fluxo real fechado.")
    destino = config.destination
    if destino.is_symlink() or not destino.is_file():
        raise E1ContractError("Fotografia canonica regular obrigatoria ausente.")
    dados = _ler_arquivo_seguro(destino)
    if not dados:
        raise E1ContractError("Fotografia canonica vazia.")
    catalogo = validar_bytes_catalogo_bruto(dados)
    checksum = hashlib.sha256(dados).hexdigest()
    metadados = catalogo["metadados"]
    categorias = tuple(
        (categoria, len(catalogo["inventario_public"][categoria]))
        for categoria in E1_SUCCESS_CATEGORIES
    )
    cleanup = dict(outcome.cleanup)
    estado = adapter.state
    if (
        len(outcome.category_counts) != len(E1_SUCCESS_CATEGORIES)
        or len(dict(outcome.category_counts)) != len(E1_SUCCESS_CATEGORIES)
        or tuple(dict(outcome.category_counts)) != E1_SUCCESS_CATEGORIES
        or categorias != outcome.category_counts
        or len(outcome.cleanup) != len(E1CleanupTelemetry.__dataclass_fields__)
        or len(cleanup) != len(E1CleanupTelemetry.__dataclass_fields__)
        or tuple(cleanup) != tuple(E1CleanupTelemetry.__dataclass_fields__)
        or any(valor != E1CheckState.TRUE.value for valor in cleanup.values())
        or checksum != outcome.photograph_sha256
        or len(dados) != outcome.photograph_size
        or metadados["capture_id"] != outcome.capture_id
        or metadados["captured_at_utc"] != outcome.captured_at_utc
        or metadados["postgres_version"] != outcome.postgres_version
        or metadados["server_version_num"] != outcome.server_version_num
        or metadados["container_image_digest"] != outcome.image_digest
        or estado.written_path != destino
        or estado.capture_created_this_run is not True
        or estado.capture_written_sha256 != checksum
        or estado.capture_written_capture_id != outcome.capture_id
        or estado.photograph_sha256 != checksum
        or estado.m0001_applied is not True
        or estado.m0001_committed is not True
        or estado.connection is not None
        or estado.cleanup_env_cleared is not True
        or estado.cleanup_container_requested is not True
        or estado.cleanup_container_absent is not True
        or estado.cleanup_port_released is not True
        or estado.cleanup_volume_absent is not True
    ):
        raise E1ContractError("Proveniencia operacional do receipt nao comprovada.")
    return {
        "photograph_relative_path": outcome.photograph_relative_path,
        "photograph_sha256": checksum,
        "photograph_size": len(dados),
        "capture_id": outcome.capture_id,
        "captured_at_utc": outcome.captured_at_utc,
        "postgres_version": outcome.postgres_version,
        "server_version_num": outcome.server_version_num,
        "image_digest": outcome.image_digest,
        "category_counts": categorias,
        "cleanup": outcome.cleanup,
    }


def executar_e1_controlada():
    """Unica entrada autorizavel da E1 real; nao aceita callbacks ou adaptadores."""
    config = E1Config(REPOSITORY_ROOT, RAW_CATALOG_PATH)
    adapter = E1RealAdapter(config)
    outcome = _executar_fluxo_e1(config, adapter)
    try:
        campos = _validar_pre_receipt_operacional(outcome, config, adapter)
        return E1SuccessReceipt(
            capability=_E1_OPERATIONAL_RECEIPT_CAPABILITY,
            code="H2C4A2_E1_SUCCESS",
            **campos,
        )
    except Exception as erro:
        cleanup_error = None
        try:
            _remover_fotografia_criada_em_falha(adapter)
        except Exception as erro_cleanup:
            cleanup_error = _sanitizar_erro_e1(E1Stage.CLEANUP_PHOTOGRAPH, erro_cleanup)
        raise H2C4A2E1Failure(E1FailureTelemetry(
            code="H2C4A2_E1_FAILURE",
            primary_error=_sanitizar_erro_e1(E1Stage.CAPTURE_VALIDATE, erro),
            cleanup=E1CleanupTelemetry(cleanup_started=E1CheckState.TRUE),
            cleanup_error=cleanup_error,
        )) from None




class _FakeConnectionE1:
    def __init__(self): self.closed = 0
    def close(self): self.closed = 1


class FakeE1Adapter(E1Adapter):
    """Fake nominal: executa o mesmo contrato sem Docker ou PostgreSQL."""
    SENSITIVE = "postgresql://USUARIO:SENHA@HOST:5432/BANCO"

    def __init__(self, config, *, fail_method=None, postgres_version=POSTGRES_VERSION,
                 server_version_num=POSTGRES_VERSION_NUM, omit_photo=False,
                 invalid_photo=None, cleanup_states=None):
        if type(config) is not E1TestConfig:
            raise TypeError("FakeE1Adapter exige E1TestConfig.")
        super().__init__(config)
        self.calls, self.fail_method = [], fail_method
        self.fake_postgres_version, self.fake_server_version_num = postgres_version, server_version_num
        self.omit_photo, self.invalid_photo = omit_photo, invalid_photo
        self.cleanup_states = {} if cleanup_states is None else dict(cleanup_states)

    def _start(self, method):
        self.calls.append(method)
        if self.fail_method == method: raise NotImplementedError(self.SENSITIVE)

    def _step(self, method, stage):
        self._start(method); return _evidencia_etapa(stage)

    def _cleanup(self, method, stage):
        self._start(method)
        return E1CleanupEvidence(stage, self.cleanup_states.get(method, E1CheckState.TRUE))

    def precheck(self):
        if self.config.destination.exists():
            raise E1ContractError("Destino de teste preexistente.")
        return self._step("precheck", E1Stage.PRECHECK)
    def validar_imagem_docker(self):
        self.state.initial_volumes = frozenset()
        return self._step("validar_imagem_docker", E1Stage.DOCKER_IMAGE)
    def criar_contexto_temporario(self):
        self._start("criar_contexto_temporario")
        self.state.container_name = "container-tecnico-sintetico"
        self.state.temporary_user, self.state.temporary_password = "USUARIO", "SENHA"
        self.state.temporary_database, self.state.dsn = "BANCO", self.SENSITIVE
        return _evidencia_etapa(E1Stage.TEMP_CREDENTIALS)
    def criar_container(self): return self._step("criar_container", E1Stage.CONTAINER_CREATE)
    def validar_isolamento(self): return self._step("validar_isolamento", E1Stage.CONTAINER_ISOLATION)
    def aguardar_health(self): return self._step("aguardar_health", E1Stage.CONTAINER_HEALTH)
    def descobrir_porta(self):
        self._start("descobrir_porta"); self.state.port = 15432
        return _evidencia_etapa(E1Stage.PORT_DISCOVERY)
    def conectar_postgresql(self):
        self._start("conectar_postgresql"); self.state.connection = _FakeConnectionE1()
        return _evidencia_etapa(E1Stage.DB_CONNECT)
    def consultar_server_version(self):
        self._start("consultar_server_version"); self.state.postgres_version = self.fake_postgres_version
        return _evidencia_etapa(E1Stage.POSTGRES_VERSION)
    def consultar_server_version_num(self):
        self._start("consultar_server_version_num"); self.state.server_version_num = self.fake_server_version_num
        return _evidencia_etapa(E1Stage.SERVER_VERSION_NUM)
    def validar_postgresql_15_18(self):
        self._start("validar_postgresql_15_18")
        return _validar_versao_e1(self.state.postgres_version, self.state.server_version_num)
    def confirmar_banco_novo(self): return self._step("confirmar_banco_novo", E1Stage.NEW_DATABASE_CHECK)
    def validar_checksum_m0001(self):
        self._start("validar_checksum_m0001"); self.state.m0001_checksum = self.config.expected_m0001_checksum
        return _evidencia_etapa(E1Stage.M0001_CHECKSUM)
    def ler_m0001(self):
        self._start("ler_m0001")
        self.state.m0001_sql = (self.config.repository_root / "migrations_control" / "sql" / "M0001_criar_ledger.sql").read_text(encoding="utf-8")
        return _evidencia_etapa(E1Stage.M0001_READ)
    def aplicar_m0001(self):
        self._start("aplicar_m0001"); self.state.m0001_applied = True
        return M0001AppliedEvidence(self.state.m0001_checksum)
    def commit_m0001(self):
        self._start("commit_m0001"); self.state.m0001_committed = True
        return _evidencia_etapa(E1Stage.M0001_COMMIT)
    def preparar_captura(self):
        self._start("preparar_captura")
        self.state.metadata = {
            "formato":"h2c4a2-pg-catalog-raw","formato_versao":1,
            "postgres_version":POSTGRES_VERSION,"server_version_num":POSTGRES_VERSION_NUM,
            "container_image":POSTGRES_IMAGE,"container_image_digest":POSTGRES_IMAGE_DIGEST,
            "captured_at_utc":"2026-08-10T12:00:00Z","capture_id":"capture-"+"1"*32,
            "m0001_checksum":CHECKSUM_M0001,"manifesto_versao":1,
        }
        return _evidencia_etapa(E1Stage.CAPTURE_PREPARE)
    def coletar_catalogo(self):
        self._start("coletar_catalogo")
        inventory = {key:[] for key in E1_SUCCESS_CATEGORIES}
        coverage = [{"categoria":key,"catalogo":INVENTORY_SPECS[key]["catalogo"],
                     "consulta":INVENTORY_SPECS[key]["consulta"],"quantidade":0,"vazio":True}
                    for key in E1_SUCCESS_CATEGORIES]
        self.state.catalog = {"metadados":dict(self.state.metadata),"pg_constraint":[],
            "pg_index":[],"operator_classes":[],"collations":[],"sequencias":[],
            "inventario_public":inventory,"cobertura_inventario":coverage}
        if self.invalid_photo == "missing_category": inventory.pop(E1_SUCCESS_CATEGORIES[-1])
        self.state.category_counts = {key:len(value) for key,value in inventory.items()}
        return CatalogCollectedEvidence(tuple(self.state.category_counts.items()))
    def serializar_catalogo(self):
        self._start("serializar_catalogo"); self.state.serialized = serializar_catalogo_bruto(self.state.catalog)
        if self.invalid_photo == "invalid_json": self.state.serialized = b"{"
        if self.invalid_photo == "zero_size": self.state.serialized = b""
        return SerializedCaptureEvidence(len(self.state.serialized))
    def gravar_fotografia(self):
        self._start("gravar_fotografia")
        if not self.omit_photo:
            self.config.destination.write_bytes(self.state.serialized)
            self.state.written_path = self.config.destination
            self.state.capture_created_this_run = True
            self.state.capture_written_sha256 = hashlib.sha256(self.state.serialized).hexdigest()
            self.state.capture_written_capture_id = self.state.metadata["capture_id"]
        size = self.config.destination.stat().st_size if self.config.destination.exists() else 0
        return WrittenCaptureEvidence(size)
    def validar_fotografia(self):
        self._start("validar_fotografia")
        self.state.validated_catalog = validar_bytes_catalogo_bruto(self.config.destination.read_bytes())
        metadata = self.state.validated_catalog["metadados"]
        counts = tuple((key,len(self.state.validated_catalog["inventario_public"][key])) for key in E1_SUCCESS_CATEGORIES)
        return ValidatedCaptureEvidence(metadata["capture_id"],metadata["captured_at_utc"],counts)
    def calcular_hash(self):
        self._start("calcular_hash"); data = self.config.destination.read_bytes()
        checksum = hashlib.sha256(data).hexdigest()
        if self.invalid_photo == "hash_mismatch": checksum = "0"*64 if checksum != "0"*64 else "1"*64
        self.state.photograph_sha256 = checksum
        return CaptureHashEvidence(checksum,len(data))
    def fechar_conexoes(self):
        self._start("fechar_conexoes")
        if self.state.connection is not None: self.state.connection.close()
        self.state.connection = None
        return E1CleanupEvidence(E1Stage.CLEANUP_CONNECTIONS,self.cleanup_states.get("fechar_conexoes",E1CheckState.TRUE))
    def limpar_ambiente(self):
        self._start("limpar_ambiente")
        self.state.dsn = self.state.temporary_user = self.state.temporary_password = self.state.temporary_database = None
        self.state.cleanup_env_cleared = True
        return E1CleanupEvidence(E1Stage.CLEANUP_ENV,self.cleanup_states.get("limpar_ambiente",E1CheckState.TRUE))
    def remover_container(self):
        self.state.cleanup_container_requested=True
        return self._cleanup("remover_container",E1Stage.CLEANUP_CONTAINER)
    def confirmar_container_ausente(self):
        self.state.cleanup_container_absent=True
        return self._cleanup("confirmar_container_ausente",E1Stage.CLEANUP_VERIFY_CONTAINER)
    def confirmar_porta_liberada(self):
        self.state.cleanup_port_released=True
        return self._cleanup("confirmar_porta_liberada",E1Stage.CLEANUP_VERIFY_PORT)
    def confirmar_volume_ausente(self):
        self.state.cleanup_volume_absent=True
        return self._cleanup("confirmar_volume_ausente",E1Stage.CLEANUP_VERIFY_VOLUME)


class E1ConcreteOrchestrationOfflineTests(unittest.TestCase):
    SENSITIVE = ("USUARIO","SENHA","HOST","5432","BANCO","postgresql://","Authorization","TOKEN",r"C:\Users\Pessoa","/home/pessoa")

    @contextmanager
    def fake(self, **options):
        with tempfile.TemporaryDirectory() as directory:
            config=E1TestConfig(REPOSITORY_ROOT,Path(directory)/RAW_CATALOG_PATH.name)
            yield config,FakeE1Adapter(config,**options)
    def failure(self,config,adapter):
        with self.assertRaises(H2C4A2E1Failure) as caught: _executar_fluxo_e1(config,adapter)
        return caught.exception
    def assertSafe(self,value):
        rendered=value if isinstance(value,str) else repr(value)
        for fragment in self.SENSITIVE: self.assertNotIn(fragment,rendered)

    def test_real_adapter_and_single_public_entry_exist(self):
        self.assertTrue(issubclass(E1RealAdapter,E1Adapter))
        self.assertEqual((),tuple(inspect.signature(executar_e1_controlada).parameters))
        self.assertNotIn("executar_orquestracao_e1",globals())
        source=inspect.getsource(executar_e1_controlada)
        self.assertIn("E1RealAdapter(config)",source); self.assertNotIn("operacoes",source)
    def test_public_entry_requires_guard_and_preserves_real_photo(self):
        photograph_before = RAW_CATALOG_PATH.read_bytes()
        with mock.patch.object(E1RealAdapter,"_docker",side_effect=AssertionError("Docker nao autorizado")) as docker:
            with self.assertRaises(H2C4A2E1Failure) as caught: executar_e1_controlada()
        self.assertEqual("PRECHECK",caught.exception.telemetria["primary_error"]["stage"])
        docker.assert_not_called()
        self.assertEqual(photograph_before, RAW_CATALOG_PATH.read_bytes())
    def test_real_photo_no_inherit_matches_expected_schema(self):
        catalog = validar_bytes_catalogo_bruto(RAW_CATALOG_PATH.read_bytes())
        expected = {
            (table.nome, constraint.nome): constraint.no_inherit
            for table in EXPECTED_LEDGER_SCHEMA.tabelas
            for constraint in table.constraints
        }
        captured = {
            (constraint["tabela"], constraint["conname"]): constraint["connoinherit"]
            for constraint in catalog["pg_constraint"]
        }
        self.assertEqual(expected, captured)
    def test_concrete_stage_binding_is_closed_and_immutable(self):
        self.assertEqual(28,len(_E1_REAL_STAGE_METHODS))
        expected={E1Stage.PORT_DISCOVERY:"descobrir_porta",E1Stage.DB_CONNECT:"conectar_postgresql",
                  E1Stage.M0001_APPLY:"aplicar_m0001",E1Stage.CATALOG_COLLECT:"coletar_catalogo",
                  E1Stage.CAPTURE_WRITE:"gravar_fotografia"}
        for stage,method in expected.items(): self.assertEqual(method,_E1_REAL_STAGE_METHODS[stage])
        with self.assertRaises(TypeError): _E1_REAL_STAGE_METHODS[E1Stage.DB_CONNECT]="descobrir_porta"
    def test_real_adapter_contains_all_concrete_operations(self):
        source=inspect.getsource(E1RealAdapter)
        for token in ("subprocess.run",'"docker"',"psycopg2.connect","SHOW server_version","cursor.execute",
                      "connection.commit","coletar_catalogo_bruto","serializar_catalogo_bruto",
                      "gravar_catalogo_bruto_se_habilitado","validar_bytes_catalogo_bruto","socket.socket"):
            self.assertIn(token,source)
    def test_adapter_missing_method_cannot_be_instantiated(self):
        class Incomplete(E1Adapter): pass
        with self.fake() as (config,_):
            with self.assertRaises(TypeError): Incomplete(config)
    def test_empty_operation_cannot_report_success(self):
        class Empty(FakeE1Adapter):
            def precheck(self): self.calls.append("precheck"); return None
        with self.fake() as (config,_):
            error=self.failure(config,Empty(config)); self.assertEqual("PRECHECK",error.telemetria["primary_error"]["stage"])
            self.assertFalse(config.destination.exists())
    def test_full_offline_success_has_exact_order_and_internal_outcome(self):
        with self.fake() as (config,adapter):
            outcome=_executar_fluxo_e1(config,adapter)
            self.assertEqual([_E1_REAL_STAGE_METHODS[s] for s in E1_EXECUTION_STAGES+E1_CLEANUP_STAGES],adapter.calls)
            self.assertIs(type(outcome),E1FlowOutcome); self.assertNotIsInstance(outcome,E1SuccessReceipt)
            self.assertNotIn("H2C4A2_E1_SUCCESS",repr(outcome)); self.assertTrue(config.destination.is_file())
            self.assertFalse(hasattr(outcome,"results")); self.assertEqual(19,len(outcome.category_counts))
            data=config.destination.read_bytes()
            self.assertEqual(hashlib.sha256(data).hexdigest(),outcome.photograph_sha256)
            self.assertEqual(len(data),outcome.photograph_size)
    def test_swapped_operations_are_detected(self):
        swaps=(("conectar_postgresql","descobrir_porta",E1Stage.DB_CONNECT),("aplicar_m0001","coletar_catalogo",E1Stage.M0001_APPLY),
               ("coletar_catalogo","aplicar_m0001",E1Stage.CATALOG_COLLECT),("gravar_fotografia","calcular_hash",E1Stage.CAPTURE_WRITE))
        for target,wrong,stage in swaps:
            with self.subTest(target=target),self.fake() as (config,adapter):
                setattr(adapter,target,getattr(adapter,wrong)); error=self.failure(config,adapter)
                self.assertEqual(stage.value,error.telemetria["primary_error"]["stage"])
    def test_not_implemented_reports_nominal_stage(self):
        with self.fake(fail_method="aplicar_m0001") as (config,adapter):
            error=self.failure(config,adapter); self.assertEqual("M0001_APPLY",error.telemetria["primary_error"]["stage"])
            self.assertEqual("NotImplementedError",error.telemetria["primary_error"]["error_type"]); self.assertSafe(error)
    def test_incompatible_versions_are_rejected_before_m0001(self):
        cases=(("15.17",150017),("15.19",150019),("16.0",160000),("16.3",160003),("15.18",150019),("15.18",True),("15.18","150018"))
        for version,number in cases:
            with self.subTest(version=version,number=number),self.fake(postgres_version=version,server_version_num=number) as (config,adapter):
                error=self.failure(config,adapter); self.assertEqual("POSTGRES_VERSION_VALIDATE",error.telemetria["primary_error"]["stage"])
                self.assertEqual("VERSION_MISMATCH",error.telemetria["primary_error"]["category"])
                self.assertNotIn("validar_checksum_m0001",adapter.calls); self.assertNotIn("preparar_captura",adapter.calls)
                self.assertIn("fechar_conexoes",adapter.calls)
    def test_exact_version_is_accepted(self):
        self.assertEqual(E1StepEvidence(E1Stage.POSTGRES_VERSION_VALIDATE),_validar_versao_e1("15.18",150018))
    def test_photograph_is_mandatory(self):
        with self.fake(omit_photo=True) as (config,adapter):
            error=self.failure(config,adapter); self.assertEqual("CAPTURE_WRITE",error.telemetria["primary_error"]["stage"])
            self.assertFalse(config.destination.exists())
    def test_invalid_photographs_never_return_receipt(self):
        expected={"missing_category":"CATALOG_COLLECT","invalid_json":"CAPTURE_VALIDATE","zero_size":"CATALOG_SERIALIZE","hash_mismatch":"CAPTURE_HASH"}
        for mode,stage in expected.items():
            with self.subTest(mode=mode),self.fake(invalid_photo=mode) as (config,adapter):
                self.assertEqual(stage,self.failure(config,adapter).telemetria["primary_error"]["stage"])
    def test_m0001_failures_stop_capture_and_run_cleanup(self):
        mapping={"validar_checksum_m0001":E1Stage.M0001_CHECKSUM,"ler_m0001":E1Stage.M0001_READ,
                 "aplicar_m0001":E1Stage.M0001_APPLY,"commit_m0001":E1Stage.M0001_COMMIT}
        for method,stage in mapping.items():
            with self.subTest(method=method),self.fake(fail_method=method) as (config,adapter):
                error=self.failure(config,adapter); self.assertEqual(stage.value,error.telemetria["primary_error"]["stage"])
                self.assertNotIn("preparar_captura",adapter.calls); self.assertIn("fechar_conexoes",adapter.calls)
    def test_capture_failures_are_nominal(self):
        methods=("preparar_captura","coletar_catalogo","serializar_catalogo","gravar_fotografia","validar_fotografia","calcular_hash")
        for method in methods:
            with self.subTest(method=method),self.fake(fail_method=method) as (config,adapter):
                error=self.failure(config,adapter); stage=next(k for k,v in _E1_REAL_STAGE_METHODS.items() if v==method)
                self.assertEqual(stage.value,error.telemetria["primary_error"]["stage"]); self.assertIn("fechar_conexoes",adapter.calls)
    def test_cleanup_failure_after_valid_photo_blocks_success(self):
        with self.fake(fail_method="confirmar_porta_liberada") as (config,adapter):
            error=self.failure(config,adapter); self.assertFalse(config.destination.exists())
            self.assertEqual("CLEANUP_VERIFY_PORT",error.telemetria["primary_error"]["stage"])
    def test_cleanup_none_or_unknown_never_becomes_true(self):
        class NoneCleanup(FakeE1Adapter):
            def fechar_conexoes(self): self.calls.append("fechar_conexoes"); return None
        with self.fake() as (config,_):
            error=self.failure(config,NoneCleanup(config)); self.assertEqual("CLEANUP_CONNECTIONS",error.telemetria["primary_error"]["stage"])
            self.assertEqual("FALSE",error.telemetria["cleanup"]["cleanup_connection_closed"])
        with self.fake(cleanup_states={"confirmar_volume_ausente":E1CheckState.UNKNOWN}) as (config,adapter):
            error=self.failure(config,adapter); self.assertEqual("CLEANUP_VERIFY_VOLUME",error.telemetria["primary_error"]["stage"])
            self.assertEqual("UNKNOWN",error.telemetria["cleanup"]["cleanup_volume_absent"])
    def test_state_and_receipt_repr_are_safe(self):
        state=E1RuntimeState(temporary_user="USUARIO",temporary_password="SENHA",temporary_database="BANCO",dsn=FakeE1Adapter.SENSITIVE,port=5432)
        self.assertSafe(state); self.assertSafe(str(state))
        with self.fake() as (config,adapter):
            outcome=_executar_fluxo_e1(config,adapter); self.assertSafe(outcome); self.assertSafe(str(outcome))
    def test_receipt_rejects_bad_types_hash_and_categories(self):
        with self.fake() as (config,adapter):
            outcome=_executar_fluxo_e1(config,adapter)
            base=dict(outcome.__dict__); base.update(capability=_E1_OPERATIONAL_RECEIPT_CAPABILITY,code="H2C4A2_E1_SUCCESS"); cases=[]
            for field,value in (("photograph_size",True),("photograph_size",0),("photograph_sha256","A"*64),("server_version_num",True),("server_version_num",150017)):
                changed=dict(base); changed[field]=value; cases.append(changed)
            changed=dict(base); changed["category_counts"]=outcome.category_counts[:-1]; cases.append(changed)
            changed=dict(base); changed["category_counts"]+=(("extra",0),); cases.append(changed)
            changed=dict(base); changed["category_counts"]=((outcome.category_counts[0][0],True),)+outcome.category_counts[1:]; cases.append(changed)
            for kwargs in cases:
                with self.assertRaises(ValueError): E1SuccessReceipt(**kwargs)
    def test_failure_traceback_and_technical_fields_are_safe(self):
        import traceback
        with self.fake(fail_method="conectar_postgresql") as (config,adapter):
            error=self.failure(config,adapter); rendered="".join(traceback.format_exception(type(error),error,error.__traceback__))
            self.assertSafe(rendered); self.assertIsNone(error.__cause__); self.assertIsNone(error.__context__); self.assertTrue(error.__suppress_context__)
        class DbError(Exception): pgcode="42P01"
        self.assertEqual("42P01",_sanitizar_erro_e1(E1Stage.DB_CONNECT,DbError(FakeE1Adapter.SENSITIVE)).sqlstate)
        class BadDbError(Exception): pgcode="abcde"
        self.assertIsNone(_sanitizar_erro_e1(E1Stage.DB_CONNECT,BadDbError(FakeE1Adapter.SENSITIVE)).sqlstate)
        file_error=OSError(2,"Authorization TOKEN",r"C:\Users\Pessoa\arquivo"); file_error.winerror=3
        safe=_sanitizar_erro_e1(E1Stage.M0001_READ,file_error); self.assertEqual((2,3),(safe.errno,safe.winerror))
        self.assertSafe(safe)
        class BoolCodes(Exception): errno=True; winerror=False
        bool_codes=_sanitizar_erro_e1(E1Stage.M0001_READ,BoolCodes())
        self.assertEqual((None,None),(bool_codes.errno,bool_codes.winerror))
        malformed=type("Bad.Name\n",(Exception,),{})()
        self.assertEqual("Exception",_sanitizar_erro_e1(E1Stage.PRECHECK,malformed).error_type)
    def test_primary_error_not_masked_by_cleanup_error(self):
        with self.fake(fail_method="consultar_server_version") as (config,adapter):
            def fail_cleanup(): adapter.calls.append("confirmar_porta_liberada"); raise OSError(5,FakeE1Adapter.SENSITIVE)
            adapter.confirmar_porta_liberada=fail_cleanup; error=self.failure(config,adapter)
            self.assertEqual("POSTGRES_VERSION",error.telemetria["primary_error"]["stage"])
            self.assertEqual("CLEANUP_VERIFY_PORT",error.telemetria["cleanup_error"]["stage"]); self.assertSafe(error)
    def test_all_post_health_failures_stop_next_and_cleanup(self):
        stages=E1_EXECUTION_STAGES[E1_EXECUTION_STAGES.index(E1Stage.CONTAINER_HEALTH)+1:]
        for stage in stages:
            method=_E1_REAL_STAGE_METHODS[stage]
            with self.subTest(stage=stage.value),self.fake(fail_method=method) as (config,adapter):
                error=self.failure(config,adapter); self.assertEqual(stage.value,error.telemetria["primary_error"]["stage"])
                index=E1_EXECUTION_STAGES.index(stage)
                if index+1<len(E1_EXECUTION_STAGES): self.assertNotIn(_E1_REAL_STAGE_METHODS[E1_EXECUTION_STAGES[index+1]],adapter.calls)
                self.assertEqual([_E1_REAL_STAGE_METHODS[s] for s in E1_CLEANUP_STAGES],adapter.calls[-len(E1_CLEANUP_STAGES):])
    def test_config_is_closed_and_real_photo_is_regular(self):
        with self.fake() as (config,_):
            self.assertEqual("E1TestConfig(test_only=True)",repr(config))
            with self.assertRaises(ValueError): E1Config(REPOSITORY_ROOT,config.destination)
            with self.assertRaises(ValueError): E1Config(REPOSITORY_ROOT,RAW_CATALOG_PATH,expected_server_version_num=True)
        self.assertEqual("E1Config(approved=True)",repr(E1Config(REPOSITORY_ROOT,RAW_CATALOG_PATH)))
        self.assertTrue(RAW_CATALOG_PATH.is_file())
        self.assertFalse(RAW_CATALOG_PATH.is_symlink())


class E1OperationalPerimeterC3OfflineTests(unittest.TestCase):
    IMAGE_ID = "sha256:" + "a" * 64
    SECRET_USER = "USUARIO_SUPER_SECRETO"
    SECRET_PASSWORD = "SENHA_SUPER_SECRETA"
    SECRET_DATABASE = "BANCO_SUPER_SECRETO"

    @contextmanager
    def config_and_fake(self, **options):
        with tempfile.TemporaryDirectory() as directory:
            config = E1TestConfig(REPOSITORY_ROOT, Path(directory) / RAW_CATALOG_PATH.name)
            yield config, FakeE1Adapter(config, **options)

    @staticmethod
    def docker_result(args=(), returncode=0, stdout="", stderr=""):
        return subprocess.CompletedProcess(args, returncode, stdout, stderr)

    @staticmethod
    def isolation_payload(image_id=None, mounts=None, config_env=None):
        return json.dumps([{
            "Image": image_id or E1OperationalPerimeterC3OfflineTests.IMAGE_ID,
            "Config": {
                "Image": POSTGRES_IMMUTABLE_REFERENCE,
                "Env": [] if config_env is None else config_env,
            },
            "HostConfig": {
                "AutoRemove": True,
                "Tmpfs": {"/var/lib/postgresql/data": "rw,noexec,nosuid,size=512m"},
            },
            "NetworkSettings": {"Ports": {"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "15432"}]}},
            "Mounts": [] if mounts is None else mounts,
        }])

    @staticmethod
    def synthetic_catalog(metadata):
        inventory = {key: [] for key in E1_SUCCESS_CATEGORIES}
        coverage = [{
            "categoria": key,
            "catalogo": INVENTORY_SPECS[key]["catalogo"],
            "consulta": INVENTORY_SPECS[key]["consulta"],
            "quantidade": 0,
            "vazio": True,
        } for key in E1_SUCCESS_CATEGORIES]
        return {
            "metadados": dict(metadata), "pg_constraint": [], "pg_index": [],
            "operator_classes": [], "collations": [], "sequencias": [],
            "inventario_public": inventory, "cobertura_inventario": coverage,
        }

    def test_c3_01_complete_fake_returns_only_internal_outcome(self):
        with self.config_and_fake() as (config, adapter):
            outcome = _executar_fluxo_e1(config, adapter)
            self.assertIs(type(outcome), E1FlowOutcome)
            self.assertNotIsInstance(outcome, E1SuccessReceipt)
            self.assertNotIn("H2C4A2_E1_SUCCESS", repr(outcome))
            self.assertEqual(28, len(adapter.calls))

    def test_c3_02_fake_and_generic_runner_cannot_emit_operational_receipt(self):
        with self.config_and_fake() as (config, adapter):
            outcome = _executar_fluxo_e1(config, adapter)
            with self.assertRaises(E1ContractError):
                _validar_pre_receipt_operacional(outcome, config, adapter)
            with self.assertRaises(ValueError):
                E1SuccessReceipt(object(), "H2C4A2_E1_SUCCESS", **outcome.__dict__)

    def test_c3_03_only_public_entry_contains_operational_emission(self):
        public_source = inspect.getsource(executar_e1_controlada)
        flow_source = inspect.getsource(_executar_fluxo_e1)
        self.assertIn("E1RealAdapter(config)", public_source)
        self.assertIn("E1SuccessReceipt", public_source)
        self.assertIn("_validar_pre_receipt_operacional", public_source)
        self.assertNotIn("_emitir_receipt_operacional", globals())
        self.assertNotIn("E1SuccessReceipt", flow_source)
        self.assertEqual((), tuple(inspect.signature(executar_e1_controlada).parameters))

    def test_c3_04_operational_config_is_exact_and_test_config_is_separate(self):
        self.assertIs(type(E1Config(REPOSITORY_ROOT, RAW_CATALOG_PATH)), E1Config)
        with tempfile.TemporaryDirectory() as directory:
            external = Path(directory) / RAW_CATALOG_PATH.name
            with self.assertRaises(ValueError): E1Config(REPOSITORY_ROOT, external)
            test_config = E1TestConfig(REPOSITORY_ROOT, external)
            self.assertIs(type(test_config), E1TestConfig)
            self.assertNotEqual(test_config.destination, RAW_CATALOG_PATH)

    def test_c3_05_docker_run_uses_immutable_digest_not_tag(self):
        with self.config_and_fake() as (config, _):
            adapter = E1RealAdapter(config)
            adapter.state.container_name = "container-sintetico"
            adapter.state.temporary_user = self.SECRET_USER
            adapter.state.temporary_password = self.SECRET_PASSWORD
            adapter.state.temporary_database = self.SECRET_DATABASE
            with mock.patch.object(adapter, "_docker", return_value=self.docker_result()) as docker:
                adapter.criar_container()
            argv = docker.call_args.args
            self.assertEqual(POSTGRES_IMMUTABLE_REFERENCE, argv[-1])
            self.assertNotIn(POSTGRES_IMAGE, argv)

    def test_c3_06_image_validation_records_id_and_platform(self):
        with self.config_and_fake() as (config, _):
            adapter = E1RealAdapter(config)
            responses = iter((
                self.docker_result(stdout=json.dumps([{"Endpoints": {"docker": {"Host": DOCKER_LOCAL_ENDPOINT}}}])),
                self.docker_result(stdout=json.dumps([{"Id": self.IMAGE_ID, "RepoDigests": [POSTGRES_IMMUTABLE_REFERENCE], "Os": "linux", "Architecture": "amd64"}])),
                self.docker_result(stdout=""),
            ))
            with mock.patch.object(adapter, "_docker", side_effect=lambda *a, **k: next(responses)) as docker:
                adapter.validar_imagem_docker()
            self.assertEqual(self.IMAGE_ID, adapter.state.expected_image_id)
            self.assertIn(POSTGRES_IMMUTABLE_REFERENCE, docker.call_args_list[1].args)

    def test_c3_07_tag_race_cannot_change_run_and_wrong_container_id_is_rejected(self):
        with self.config_and_fake() as (config, _):
            adapter = E1RealAdapter(config)
            adapter.state.expected_image_id = self.IMAGE_ID
            adapter.state.container_name = "container-sintetico"
            wrong = "sha256:" + "b" * 64
            with mock.patch.object(adapter, "_docker", return_value=self.docker_result(stdout=self.isolation_payload(wrong))):
                with self.assertRaises(E1ContractError): adapter.validar_isolamento()
            self.assertEqual(POSTGRES_IMMUTABLE_REFERENCE, POSTGRES_IMMUTABLE_REFERENCE)

    def test_c3_08_all_docker_commands_force_desktop_linux_and_shell_false(self):
        captured = []
        def fake_run(argv, **kwargs):
            captured.append((argv, kwargs)); return self.docker_result(argv)
        with mock.patch("tests.test_migrations_control_h2c4a2_postgresql.subprocess.run", side_effect=fake_run):
            E1RealAdapter._docker("volume", "ls")
        argv, kwargs = captured[0]
        self.assertEqual(("docker", "--context", DOCKER_CONTEXT), argv[:3])
        self.assertIs(kwargs["shell"], False)

    def test_c3_09_docker_host_override_is_rejected_by_operational_precheck(self):
        config = E1Config(REPOSITORY_ROOT, RAW_CATALOG_PATH)
        env = {"H2C4A2_E1_AUTHORIZED": "1", "H2C4A2_CAPTURE_RAW_CATALOG": "1", "DOCKER_HOST": "tcp://remote:2375"}
        with mock.patch.dict(os.environ, env, clear=False):
            with self.assertRaises(E1ContractError): E1RealAdapter(config).precheck()

    def test_c3_10_docker_context_override_is_rejected_by_operational_precheck(self):
        config = E1Config(REPOSITORY_ROOT, RAW_CATALOG_PATH)
        env = {"H2C4A2_E1_AUTHORIZED": "1", "H2C4A2_CAPTURE_RAW_CATALOG": "1", "DOCKER_CONTEXT": "remoto"}
        with mock.patch.dict(os.environ, env, clear=False):
            with self.assertRaises(E1ContractError): E1RealAdapter(config).precheck()

    def test_c3_11_secrets_are_absent_from_argv_and_present_only_in_run_child_env(self):
        with self.config_and_fake() as (config, _):
            adapter = E1RealAdapter(config)
            adapter.state.container_name = "container-sintetico"
            adapter.state.temporary_user = self.SECRET_USER
            adapter.state.temporary_password = self.SECRET_PASSWORD
            adapter.state.temporary_database = self.SECRET_DATABASE
            captured = []
            with mock.patch("tests.test_migrations_control_h2c4a2_postgresql.subprocess.run", side_effect=lambda argv, **kw: captured.append((argv, kw)) or self.docker_result(argv)):
                adapter.criar_container()
                adapter._docker("volume", "ls")
            run_argv, run_kwargs = captured[0]
            rendered = " ".join(run_argv)
            for secret in (self.SECRET_USER, self.SECRET_PASSWORD, self.SECRET_DATABASE):
                self.assertNotIn(secret, rendered)
            self.assertEqual({"POSTGRES_USER", "POSTGRES_PASSWORD", "POSTGRES_DB"}, {run_argv[index + 1] for index, value in enumerate(run_argv[:-1]) if value == "--env"})
            self.assertEqual(self.SECRET_PASSWORD, run_kwargs["env"]["POSTGRES_PASSWORD"])
            other_env = captured[1][1]["env"]
            self.assertNotIn("POSTGRES_PASSWORD", other_env)
            self.assertNotIn("DATABASE_URL", other_env)

    def test_c3_12_subprocess_failure_and_telemetry_never_render_secrets(self):
        error = subprocess.CalledProcessError(1, ("docker", "run"), output=self.SECRET_PASSWORD, stderr=self.SECRET_USER)
        safe = _sanitizar_erro_e1(E1Stage.CONTAINER_CREATE, error)
        rendered = repr(safe)
        self.assertNotIn(self.SECRET_PASSWORD, rendered)
        self.assertNotIn(self.SECRET_USER, rendered)

    def test_c3_13_inspect_config_env_is_not_retained_or_rendered(self):
        with self.config_and_fake() as (config, _):
            adapter = E1RealAdapter(config)
            adapter.state.expected_image_id = self.IMAGE_ID
            adapter.state.container_name = "container-sintetico"
            payload = self.isolation_payload(config_env=["POSTGRES_PASSWORD=" + self.SECRET_PASSWORD])
            with mock.patch.object(adapter, "_docker", return_value=self.docker_result(stdout=payload)):
                evidence = adapter.validar_isolamento()
            self.assertEqual(E1Stage.CONTAINER_ISOLATION, evidence.stage)
            self.assertNotIn(self.SECRET_PASSWORD, repr(adapter.state))
            self.assertFalse(hasattr(adapter.state, "container_env"))

    def test_c3_14_any_bind_volume_or_unexpected_mount_is_rejected(self):
        for mount_type in ("bind", "volume", "tmpfs"):
            with self.subTest(mount_type=mount_type), self.config_and_fake() as (config, _):
                adapter = E1RealAdapter(config)
                adapter.state.expected_image_id = self.IMAGE_ID
                adapter.state.container_name = "container-sintetico"
                payload = self.isolation_payload(mounts=[{"Type": mount_type, "Destination": "/inesperado"}])
                with mock.patch.object(adapter, "_docker", return_value=self.docker_result(stdout=payload)):
                    with self.assertRaises(E1ContractError): adapter.validar_isolamento()

    def test_c3_15_container_absence_requires_success_and_well_formed_empty_output(self):
        cases = ((0, "", E1CheckState.TRUE), (0, "a" * 64, E1CheckState.FALSE))
        for code, stdout, expected in cases:
            with self.subTest(stdout=stdout), self.config_and_fake() as (config, _):
                adapter = E1RealAdapter(config); adapter.state.container_name = "nome-exato"
                with mock.patch.object(adapter, "_docker", return_value=self.docker_result(returncode=code, stdout=stdout)) as docker:
                    self.assertEqual(expected, adapter.confirmar_container_ausente().state)
                self.assertIn("name=^/nome-exato$", docker.call_args.args)
        for code, stdout in ((1, ""), (0, "saida malformada")):
            with self.subTest(code=code, malformed=stdout), self.config_and_fake() as (config, _):
                adapter = E1RealAdapter(config); adapter.state.container_name = "nome-exato"
                with mock.patch.object(adapter, "_docker", return_value=self.docker_result(returncode=code, stdout=stdout)):
                    with self.assertRaises(E1ContractError): adapter.confirmar_container_ausente()

    def test_c3_16_failure_after_validate_removes_current_test_photograph(self):
        with self.config_and_fake(fail_method="validar_fotografia") as (config, adapter):
            with self.assertRaises(H2C4A2E1Failure): _executar_fluxo_e1(config, adapter)
            self.assertFalse(config.destination.exists())

    def test_c3_17_failure_after_hash_removes_current_test_photograph(self):
        with self.config_and_fake(fail_method="calcular_hash") as (config, adapter):
            with self.assertRaises(H2C4A2E1Failure): _executar_fluxo_e1(config, adapter)
            self.assertFalse(config.destination.exists())

    def test_c3_18_cleanup_failures_remove_current_test_photograph(self):
        for method in ("remover_container", "confirmar_porta_liberada", "confirmar_volume_ausente"):
            with self.subTest(method=method), self.config_and_fake(fail_method=method) as (config, adapter):
                with self.assertRaises(H2C4A2E1Failure): _executar_fluxo_e1(config, adapter)
                self.assertFalse(config.destination.exists())

    def test_c3_19_preexisting_file_is_rejected_preserved_and_never_deleted(self):
        with self.config_and_fake() as (config, adapter):
            original = b"EVIDENCIA_PREEXISTENTE"
            config.destination.write_bytes(original)
            with self.assertRaises(H2C4A2E1Failure): _executar_fluxo_e1(config, adapter)
            self.assertEqual(original, config.destination.read_bytes())

    def test_c3_20_invalid_typed_evidence_is_rejected_at_construction(self):
        invalid = (
            lambda: M0001AppliedEvidence(None),
            lambda: WrittenCaptureEvidence(True),
            lambda: SerializedCaptureEvidence(0),
            lambda: CaptureHashEvidence("abc", 1),
            lambda: CatalogCollectedEvidence(tuple((key, 0) for key in E1_SUCCESS_CATEGORIES[:-1])),
            lambda: E1CleanupEvidence(E1Stage.PRECHECK, E1CheckState.TRUE),
        )
        for factory in invalid:
            with self.subTest(factory=factory), self.assertRaises(ValueError): factory()

    def test_c3_21_runtime_outcome_receipt_and_failure_are_secret_free(self):
        state = E1RuntimeState(temporary_user=self.SECRET_USER, temporary_password=self.SECRET_PASSWORD, temporary_database=self.SECRET_DATABASE)
        self.assertNotIn(self.SECRET_PASSWORD, repr(state))
        with self.config_and_fake(fail_method="conectar_postgresql") as (config, adapter):
            with self.assertRaises(H2C4A2E1Failure) as caught: _executar_fluxo_e1(config, adapter)
            self.assertNotIn("postgresql://", str(caught.exception))

    def test_c3_22_real_adapter_full_flow_with_only_external_boundaries_mocked(self):
        class Cursor:
            def __init__(self, owner): self.owner, self.command = owner, None
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def execute(self, command): self.command = command; self.owner.executed.append(command)
            def fetchone(self):
                if self.command == "SHOW server_version": return (POSTGRES_VERSION,)
                if "server_version_num" in self.command: return (POSTGRES_VERSION_NUM,)
                return (1,)
        class Connection:
            def __init__(self): self.closed, self.executed, self.committed = 0, [], False
            def cursor(self): return Cursor(self)
            def commit(self): self.committed = True
            def close(self): self.closed = 1
        class LocalSocket:
            def __enter__(self): return self
            def __exit__(self, *args): return False
            def settimeout(self, value): self.timeout = value
            def connect_ex(self, address): return 1

        with tempfile.TemporaryDirectory() as directory:
            config = E1TestConfig(REPOSITORY_ROOT, Path(directory) / RAW_CATALOG_PATH.name)
            connection = Connection(); calls = []
            def docker_run(argv, **kwargs):
                calls.append((argv, kwargs)); args = argv[3:]
                if args[:2] == ("context", "inspect"):
                    stdout = json.dumps([{"Endpoints": {"docker": {"Host": DOCKER_LOCAL_ENDPOINT}}}])
                elif args[:2] == ("image", "inspect"):
                    stdout = json.dumps([{"Id": self.IMAGE_ID, "RepoDigests": [POSTGRES_IMMUTABLE_REFERENCE], "Os": "linux", "Architecture": "amd64"}])
                elif args[:2] == ("volume", "ls"): stdout = ""
                elif args and args[0] == "run": stdout = "container-id\n"
                elif args[:2] == ("inspect", "--format"): stdout = "healthy\n"
                elif args and args[0] == "inspect": stdout = self.isolation_payload()
                elif args and args[0] == "port": stdout = "127.0.0.1:15432\n"
                elif args and args[0] == "ps": stdout = ""
                else: stdout = ""
                return self.docker_result(argv, 0, stdout)
            def writer(catalog, destination, ambiente):
                destination.write_bytes(serializar_catalogo_bruto(catalog)); return True
            classification = type("Classification", (), {"classificacao": DatabaseClassification.BANCO_NOVO, "pode_prosseguir": True})()
            with (
                mock.patch("tests.test_migrations_control_h2c4a2_postgresql.subprocess.run", side_effect=docker_run),
                mock.patch("tests.test_migrations_control_h2c4a2_postgresql.psycopg2.connect", return_value=connection),
                mock.patch("tests.test_migrations_control_h2c4a2_postgresql.coletar_snapshot", return_value=object()),
                mock.patch("tests.test_migrations_control_h2c4a2_postgresql.classificar_preflight", return_value=classification),
                mock.patch("tests.test_migrations_control_h2c4a2_postgresql.coletar_catalogo_bruto", side_effect=lambda conn, meta: self.synthetic_catalog(meta)),
                mock.patch("tests.test_migrations_control_h2c4a2_postgresql.gravar_catalogo_bruto_se_habilitado", side_effect=writer),
                mock.patch("tests.test_migrations_control_h2c4a2_postgresql.socket.socket", return_value=LocalSocket()),
            ):
                adapter = E1RealAdapter(config)
                outcome = _executar_fluxo_e1(config, adapter)
            self.assertIs(type(outcome), E1FlowOutcome)
            self.assertNotIsInstance(outcome, E1SuccessReceipt)
            self.assertTrue(connection.committed)
            self.assertTrue(any(isinstance(command, str) and "CREATE" in command.upper() for command in connection.executed))
            run_call = next((argv, kwargs) for argv, kwargs in calls if argv[3] == "run")
            self.assertEqual(POSTGRES_IMMUTABLE_REFERENCE, run_call[0][-1])
            self.assertNotIn(self.SECRET_PASSWORD, " ".join(run_call[0]))
            self.assertTrue(config.destination.is_file())


class E1OperationalBlockersFastTests(unittest.TestCase):
    @staticmethod
    def outcome(*, categories=None, cleanup=None):
        if categories is None:
            categories = tuple((key, 0) for key in E1_SUCCESS_CATEGORIES)
        if cleanup is None:
            cleanup = tuple(
                (key, E1CheckState.TRUE.value)
                for key in E1CleanupTelemetry.__dataclass_fields__
            )
        return E1FlowOutcome(
            photograph_relative_path="tests/fixtures/h2c4a2_pg15_18_catalogo_bruto.json",
            photograph_sha256="0" * 64,
            photograph_size=1,
            capture_id="capture-" + "1" * 32,
            captured_at_utc="2026-08-10T12:00:00Z",
            postgres_version=POSTGRES_VERSION,
            server_version_num=POSTGRES_VERSION_NUM,
            image_digest=POSTGRES_IMAGE_DIGEST,
            category_counts=categories,
            cleanup=cleanup,
        )

    def test_fast_a_manual_outcome_and_unexecuted_adapter_do_not_generate_receipt(self):
        config = E1Config(REPOSITORY_ROOT, RAW_CATALOG_PATH)
        adapter = E1RealAdapter(config)
        self.assertNotIn("_emitir_receipt_operacional", globals())
        with self.assertRaises(E1ContractError):
            _validar_pre_receipt_operacional(self.outcome(), config, adapter)

    def test_fast_b_missing_photograph_never_validates_for_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / RAW_CATALOG_PATH.name
            with mock.patch(
                "tests.test_migrations_control_h2c4a2_postgresql.RAW_CATALOG_PATH",
                destination,
            ):
                config = E1Config(REPOSITORY_ROOT, destination)
                self.assertFalse(destination.exists())
                with self.assertRaises(E1ContractError):
                    _validar_pre_receipt_operacional(
                        self.outcome(), config, E1RealAdapter(config),
                    )
                self.assertFalse(destination.exists())

    def test_fast_c_mismatched_adapter_and_config_do_not_validate(self):
        with tempfile.TemporaryDirectory() as directory:
            test_config = E1TestConfig(REPOSITORY_ROOT, Path(directory) / RAW_CATALOG_PATH.name)
            operational = E1Config(REPOSITORY_ROOT, RAW_CATALOG_PATH)
            with self.assertRaises(E1ContractError):
                _validar_pre_receipt_operacional(
                    self.outcome(), operational, E1RealAdapter(test_config),
                )

    def test_fast_d_fake_rejects_operational_config(self):
        with self.assertRaises(TypeError):
            FakeE1Adapter(E1Config(REPOSITORY_ROOT, RAW_CATALOG_PATH))

    def test_fast_e_complete_fake_produces_only_flow_outcome(self):
        with tempfile.TemporaryDirectory() as directory:
            config = E1TestConfig(REPOSITORY_ROOT, Path(directory) / RAW_CATALOG_PATH.name)
            result = _executar_fluxo_e1(config, FakeE1Adapter(config))
            self.assertIs(type(result), E1FlowOutcome)
            self.assertNotIsInstance(result, E1SuccessReceipt)

    def test_fast_f_twenty_categories_with_duplicate_are_rejected(self):
        duplicated = tuple((key, 0) for key in E1_SUCCESS_CATEGORIES) + (
            (E1_SUCCESS_CATEGORIES[-1], 0),
        )
        with self.assertRaises(ValueError):
            self.outcome(categories=duplicated)

    def test_fast_g_nineteen_unique_categories_are_accepted(self):
        result = self.outcome()
        self.assertEqual(19, len(result.category_counts))
        self.assertEqual(19, len(dict(result.category_counts)))

    def test_fast_h_duplicate_cleanup_is_rejected(self):
        cleanup = tuple(
            (key, E1CheckState.TRUE.value)
            for key in E1CleanupTelemetry.__dataclass_fields__
        )
        with self.assertRaises(ValueError):
            self.outcome(cleanup=cleanup + (cleanup[-1],))

    def test_fast_i_public_path_emits_receipt_only_after_valid_file_hash_and_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            destination = Path(directory) / RAW_CATALOG_PATH.name

            def completed_flow(config, adapter):
                metadata = {
                    "formato": "h2c4a2-pg-catalog-raw",
                    "formato_versao": 1,
                    "postgres_version": POSTGRES_VERSION,
                    "server_version_num": POSTGRES_VERSION_NUM,
                    "container_image": POSTGRES_IMAGE,
                    "container_image_digest": POSTGRES_IMAGE_DIGEST,
                    "captured_at_utc": "2026-08-10T12:00:00Z",
                    "capture_id": "capture-" + "1" * 32,
                    "m0001_checksum": CHECKSUM_M0001,
                    "manifesto_versao": 1,
                }
                catalog = E1OperationalPerimeterC3OfflineTests.synthetic_catalog(metadata)
                data = serializar_catalogo_bruto(catalog)
                config.destination.write_bytes(data)
                checksum = hashlib.sha256(data).hexdigest()
                adapter.state.written_path = config.destination
                adapter.state.capture_created_this_run = True
                adapter.state.capture_written_sha256 = checksum
                adapter.state.capture_written_capture_id = metadata["capture_id"]
                adapter.state.photograph_sha256 = checksum
                adapter.state.m0001_applied = True
                adapter.state.m0001_committed = True
                adapter.state.connection = None
                adapter.state.cleanup_env_cleared = True
                adapter.state.cleanup_container_requested = True
                adapter.state.cleanup_container_absent = True
                adapter.state.cleanup_port_released = True
                adapter.state.cleanup_volume_absent = True
                cleanup = tuple(
                    (key, E1CheckState.TRUE.value)
                    for key in E1CleanupTelemetry.__dataclass_fields__
                )
                return E1FlowOutcome(
                    photograph_relative_path="tests/fixtures/h2c4a2_pg15_18_catalogo_bruto.json",
                    photograph_sha256=checksum,
                    photograph_size=len(data),
                    capture_id=metadata["capture_id"],
                    captured_at_utc=metadata["captured_at_utc"],
                    postgres_version=POSTGRES_VERSION,
                    server_version_num=POSTGRES_VERSION_NUM,
                    image_digest=POSTGRES_IMAGE_DIGEST,
                    category_counts=tuple((key, 0) for key in E1_SUCCESS_CATEGORIES),
                    cleanup=cleanup,
                )

            with (
                mock.patch("tests.test_migrations_control_h2c4a2_postgresql.RAW_CATALOG_PATH", destination),
                mock.patch("tests.test_migrations_control_h2c4a2_postgresql._executar_fluxo_e1", side_effect=completed_flow),
            ):
                receipt = executar_e1_controlada()
            self.assertIs(type(receipt), E1SuccessReceipt)
            self.assertTrue(destination.is_file())
            self.assertEqual(hashlib.sha256(destination.read_bytes()).hexdigest(), receipt.photograph_sha256)


class TPGStorageFastOfflineTests(unittest.TestCase):
    IMAGE_ID = "sha256:" + "a" * 64
    CONTAINER_NAME = "h2c4a2-tpg-" + "1" * 12
    CREDENTIALS = {
        "POSTGRES_USER": "temporary_user",
        "POSTGRES_PASSWORD": "temporary_password",
        "POSTGRES_DB": "temporary_database",
    }

    @classmethod
    def inspect_payload(cls, *, tmpfs=None, mounts=None, binds=None, host_mounts=None):
        return json.dumps([{
            "HostConfig": {
                "AutoRemove": True,
                "Tmpfs": {
                    TPG_PGDATA_PATH: "rw,noexec,nosuid,size=536870912",
                } if tmpfs is None else tmpfs,
                "Binds": binds,
                "Mounts": host_mounts,
            },
            "NetworkSettings": {
                "Ports": {"5432/tcp": [{"HostIp": "127.0.0.1", "HostPort": "49152"}]},
            },
            "Mounts": [] if mounts is None else mounts,
            "Image": cls.IMAGE_ID,
            "Config": {
                "Image": POSTGRES_IMMUTABLE_REFERENCE,
                "Env": [f"PGDATA={TPG_PGDATA_PATH}"],
            },
        }])

    def test_storage_container_real_argv_uses_only_approved_tmpfs(self):
        captured = []

        def fake_run(argv, **kwargs):
            captured.append((argv, kwargs))
            return subprocess.CompletedProcess(argv, 0, stdout="container-id\n", stderr="")

        with mock.patch(
            "tests.test_migrations_control_h2c4a2_postgresql.subprocess.run",
            side_effect=fake_run,
        ):
            criar_container_tpg_controlado(self.CONTAINER_NAME, self.CREDENTIALS)
        self.assertEqual(1, len(captured))
        argv, kwargs = captured[0]
        self.assertEqual(("docker", "--context", DOCKER_CONTEXT), argv[:3])
        self.assertEqual(POSTGRES_IMMUTABLE_REFERENCE, argv[-1])
        self.assertEqual(TPG_TMPFS_SPEC, argv[argv.index("--tmpfs") + 1])
        self.assertEqual("127.0.0.1::5432", argv[argv.index("--publish") + 1])
        self.assertNotIn("--volume", argv)
        self.assertNotIn("-v", argv)
        self.assertNotIn("--mount", argv)
        for secret in self.CREDENTIALS.values():
            self.assertNotIn(secret, argv)
        self.assertEqual(self.CREDENTIALS, {
            key: kwargs["env"][key] for key in self.CREDENTIALS
        })
        self.assertIs(kwargs["shell"], False)

    def test_storage_inspect_accepts_only_expected_tmpfs_without_mounts(self):
        def validate(payload):
            with mock.patch(
                "tests.test_migrations_control_h2c4a2_postgresql.subprocess.run",
                return_value=subprocess.CompletedProcess(
                    ("docker", "inspect"), 0, stdout=payload, stderr="",
                ),
            ) as docker:
                result = validar_container_tpg_controlado(
                    self.CONTAINER_NAME, self.IMAGE_ID,
                )
            self.assertEqual(
                ("docker", "--context", DOCKER_CONTEXT, "inspect", self.CONTAINER_NAME),
                docker.call_args.args[0],
            )
            return result

        self.assertTrue(validate(self.inspect_payload()))
        cases = {
            "volume": self.inspect_payload(mounts=[{
                "Type": "volume", "Destination": TPG_PGDATA_PATH,
            }]),
            "bind": self.inspect_payload(mounts=[{
                "Type": "bind", "Destination": TPG_PGDATA_PATH,
            }], binds=["C:/temp:/var/lib/postgresql/data"]),
            "wrong_tmpfs": self.inspect_payload(tmpfs={
                "/tmp/postgresql": "rw,noexec,nosuid,size=536870912",
            }),
            "additional_mount": self.inspect_payload(mounts=[{
                "Type": "tmpfs", "Destination": "/tmp/unexpected",
            }]),
        }
        for case, payload in cases.items():
            with self.subTest(case=case), self.assertRaises(TPGStorageContractError):
                validate(payload)

    def test_version_runner_normalizes_suffix_and_keeps_exact_contract(self):
        accepted = (
            "15.18 (Debian 15.18-1.pgdg13+1)",
            "15.18",
        )
        for server_version in accepted:
            with self.subTest(server_version=server_version):
                self.assertEqual(
                    "15.18",
                    validar_versao_postgresql_tpg(server_version, 150018),
                )
        rejected = (
            ("15.17 (Debian package)", 150017),
            ("15.19 (Debian package)", 150019),
            ("16.0 (Debian package)", 160000),
            ("15.18 (Debian package)", 150017),
        )
        for server_version, server_version_num in rejected:
            with self.subTest(
                server_version=server_version,
                server_version_num=server_version_num,
            ), self.assertRaises(TPGStorageContractError):
                validar_versao_postgresql_tpg(server_version, server_version_num)


class PostgreSqlEfemeroTests(unittest.TestCase):
    """Cenários que criam e removem bancos apenas no servidor descartável."""

    @classmethod
    def setUpClass(cls):
        dsn = os.environ.get("H2C4A2_ADMIN_DSN")
        if not dsn:
            raise unittest.SkipTest(
                "H2C4A2_ADMIN_DSN ausente; integração PostgreSQL não executada."
            )
        cls._admin_dsn = dsn
        cls._dsn_params = parse_dsn(dsn)
        cls._admin = psycopg2.connect(dsn)
        cls._admin.autocommit = True
        cls._manifesto = carregar_manifesto()

    @classmethod
    def tearDownClass(cls):
        admin = getattr(cls, "_admin", None)
        if admin is not None and not admin.closed:
            admin.close()
        cls._admin_dsn = None
        cls._dsn_params = None

    @classmethod
    def _dsn_banco(cls, nome):
        parametros = dict(cls._dsn_params)
        parametros["dbname"] = nome
        return make_dsn(**parametros)

    @classmethod
    def _criar_banco(cls):
        nome = f"h2c4a2_{uuid4().hex}"
        with cls._admin.cursor() as cursor:
            cursor.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(nome)))
        return nome

    @classmethod
    def _remover_banco(cls, nome):
        with cls._admin.cursor() as cursor:
            cursor.execute(
                "SELECT pg_catalog.pg_terminate_backend(pid) "
                "FROM pg_catalog.pg_stat_activity "
                "WHERE datname = %s AND pid <> pg_catalog.pg_backend_pid()",
                (nome,),
            )
            cursor.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(nome)))

    @contextmanager
    def banco(self):
        nome = self._criar_banco()
        conexoes = []
        try:
            def conectar():
                conexao = psycopg2.connect(self._dsn_banco(nome))
                conexoes.append(conexao)
                return conexao

            yield nome, conectar
        finally:
            for conexao in reversed(conexoes):
                if not conexao.closed:
                    conexao.close()
            self._remover_banco(nome)

    @staticmethod
    def runner(conexao, **opcoes):
        return MigrationRunner(
            conexao,
            event_logger=lambda *args, **kwargs: None,
            **opcoes,
        )

    @staticmethod
    def executar(conexao, comando, parametros=()):
        with conexao.cursor() as cursor:
            cursor.execute(comando, parametros)
            if cursor.description:
                return cursor.fetchall()
        return []

    def test_tpg_001_versao_e_capacidade(self):
        with self._admin.cursor() as cursor:
            cursor.execute("SHOW server_version_num")
            versao = int(cursor.fetchone()[0])
            cursor.execute(
                "SELECT a.attname FROM pg_catalog.pg_attribute AS a "
                "WHERE a.attrelid = 'pg_catalog.pg_index'::pg_catalog.regclass "
                "AND a.attname = ANY(%s) AND a.attnum > 0 AND NOT a.attisdropped",
                ([
                    "indimmediate", "indisclustered", "indisreplident",
                    "indnullsnotdistinct", "indcheckxmin",
                ],),
            )
            capacidades = {linha[0] for linha in cursor.fetchall()}
        self.assertGreaterEqual(versao, 150000)
        self.assertEqual(5, len(capacidades))

    def test_tpg_002_banco_novo(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            snapshot = coletar_snapshot(conexao)
            resultado = classificar_preflight(snapshot, self._manifesto)
            self.assertEqual(DatabaseClassification.BANCO_NOVO, resultado.classificacao)
            self.assertTrue(resultado.pode_prosseguir)
            self.assertFalse(snapshot.objetos_encontrados)

    def test_tpg_003_advisory_lock(self):
        self.assertEqual(LOCK_KEY, derivar_chave_lock())
        with self.banco() as (_, conectar):
            primeira, segunda = conectar(), conectar()
            lock1 = AdvisoryLock(primeira, timeout_segundos=1)
            lock2 = AdvisoryLock(segunda, timeout_segundos=0.1, intervalo_segundos=0.02)
            lock1.adquirir()
            with self.assertRaises(LockTimeoutError):
                lock2.adquirir()
            lock1.liberar()
            lock2.adquirir()
            lock2.liberar()

    def test_tpg_004_aplicacao_m0001(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.assertEqual(TRANSACTION_STATUS_IDLE, conexao.get_transaction_status())
            autocommit = conexao.autocommit
            resultado = self.runner(conexao).executar()
            self.assertTrue(resultado.sucesso)
            self.assertEqual(("M0001",), resultado.aplicadas)
            self.assertEqual(autocommit, conexao.autocommit)
            self.assertEqual(TRANSACTION_STATUS_IDLE, conexao.get_transaction_status())

    def test_tpg_005_objetos_criados(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.runner(conexao).executar()
            snapshot = coletar_snapshot(conexao)
            self.assertEqual(LEDGER_OBJECTS, snapshot.objetos_encontrados)

    def test_tpg_006_estrutura_integral(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.runner(conexao).executar()
            assinatura = coletar_assinatura_ledger(conexao)
            self.assertEqual(EXPECTED_LEDGER_SCHEMA, assinatura)
            self.assertEqual((True, "Assinatura física integral do ledger confirmada."),
                             validar_assinatura_ledger(assinatura))
            self.assertFalse(any(c.tipo == "f" for t in assinatura.tabelas for c in t.constraints))

    def test_tpg_007_sequencias_identity(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.runner(conexao).executar()
            atuais = coletar_assinatura_ledger(conexao).sequencias
            self.assertEqual(EXPECTED_LEDGER_SCHEMA.sequencias, atuais)
            self.assertTrue(all(item.identity == "d" and item.tipo_dependencia == "i" for item in atuais))

    def test_tpg_008_indices_reais(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.runner(conexao).executar()
            assinatura = coletar_assinatura_ledger(conexao)
            indices = [indice for tabela in assinatura.tabelas for indice in tabela.indices]
            self.assertEqual(6, len(indices))
            self.assertTrue(all(indice.check_xmin is False for indice in indices))
            self.assertTrue(all(indice.valid and indice.ready and indice.live for indice in indices))
            self.assertTrue(all(not indice.colunas_include for indice in indices))
            self.assertEqual(EXPECTED_LEDGER_SCHEMA, assinatura)

    def test_tpg_009_autorregistro(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.runner(conexao).executar()
            aplicadas, execucoes = coletar_conteudo_ledger(conexao)
            self.assertEqual(1, len(aplicadas))
            self.assertEqual(("M0001", CHECKSUM_M0001),
                             (aplicadas[0].migration_id, aplicadas[0].checksum_sha256))
            self.assertEqual(1, len(execucoes))
            self.assertEqual(("M0001", 0, "APLICADA"),
                             (execucoes[0].migration_id, execucoes[0].tentativa,
                              execucoes[0].situacao))
            self.assertIsNone(execucoes[0].erro_codigo)

    def test_tpg_010_segunda_execucao(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.runner(conexao).executar()
            segunda = self.runner(conexao).executar()
            self.assertEqual("BANCO_CONTROLADO", segunda.classificacao_preflight)
            self.assertEqual((), segunda.aplicadas)
            self.assertEqual(("M0001",), segunda.ignoradas)
            aplicadas, execucoes = coletar_conteudo_ledger(conexao)
            self.assertEqual((1, 1), (len(aplicadas), len(execucoes)))
            self.assertEqual(0, execucoes[0].tentativa)

    def test_tpg_011_search_path_inesperado(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            conexao.autocommit = True
            self.executar(conexao, "CREATE SCHEMA alternativo")
            self.executar(conexao, "CREATE TABLE alternativo.schema_migrations (marcador integer)")
            self.executar(conexao, "CREATE TABLE alternativo.schema_migration_execucoes (marcador integer)")
            self.executar(conexao, "SET search_path TO alternativo, public")
            self.runner(conexao).executar()
            self.assertEqual([(1,)], self.executar(
                conexao, "SELECT pg_catalog.count(*) FROM public.schema_migrations"
            ))
            self.assertEqual([(0,)], self.executar(
                conexao, "SELECT pg_catalog.count(*) FROM alternativo.schema_migrations"
            ))

    def test_tpg_012_funcao_isolada_em_public(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.executar(conexao, "CREATE FUNCTION public.teste() RETURNS integer LANGUAGE sql AS 'SELECT 1'")
            conexao.commit()
            resultado = classificar_preflight(coletar_snapshot(conexao), self._manifesto)
            self.assertEqual(DatabaseClassification.BANCO_DESCONHECIDO, resultado.classificacao)
            self.assertFalse(resultado.pode_prosseguir)

    def test_tpg_013_enum_isolado(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.executar(conexao, "CREATE TYPE public.estado_teste AS ENUM ('A')")
            conexao.commit()
            resultado = classificar_preflight(coletar_snapshot(conexao), self._manifesto)
            self.assertEqual(DatabaseClassification.BANCO_DESCONHECIDO, resultado.classificacao)

    def test_tpg_014_tabela_funcional_sem_ledger(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.executar(conexao, "CREATE TABLE public.funcional_teste (id integer)")
            conexao.commit()
            with self.assertRaises(UnknownDatabaseError):
                self.runner(conexao).executar()
            self.assertEqual([], self.executar(
                conexao,
                "SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n "
                "ON n.oid=c.relnamespace WHERE n.nspname='public' "
                "AND c.relname='schema_migrations'",
            ))

    def test_tpg_015_ledger_parcial(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.executar(conexao, "CREATE TABLE public.schema_migrations (id bigint)")
            conexao.commit()
            resultado = classificar_preflight(coletar_snapshot(conexao), self._manifesto)
            self.assertEqual(DatabaseClassification.BANCO_DESCONHECIDO, resultado.classificacao)
            self.assertFalse(resultado.pode_prosseguir)

    def test_tpg_016_ledger_fisicamente_incompativel(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.runner(conexao).executar()
            self.executar(
                conexao,
                "ALTER TABLE public.schema_migrations ALTER COLUMN modulo DROP NOT NULL",
            )
            conexao.commit()
            resultado = classificar_preflight(coletar_snapshot(conexao), self._manifesto)
            self.assertEqual(DatabaseClassification.BANCO_DESCONHECIDO, resultado.classificacao)
            self.assertFalse(resultado.pode_prosseguir)

    def test_tpg_017_schema_public_ausente(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.executar(conexao, "DROP SCHEMA public")
            conexao.commit()
            resultado = classificar_preflight(coletar_snapshot(conexao), self._manifesto)
            self.assertEqual(DatabaseClassification.BANCO_DESCONHECIDO, resultado.classificacao)
            conexao.rollback()
            with self.assertRaises(UnknownDatabaseError):
                self.runner(conexao).executar()
            self.assertFalse(coletar_snapshot(conexao).public_existe)

    def test_tpg_018_transacao_ativa_do_chamador(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.executar(conexao, "CREATE TABLE public.trabalho_chamador (id integer)")
            self.assertEqual(TRANSACTION_STATUS_INTRANS, conexao.get_transaction_status())
            with self.assertRaises(ConnectionNotIdleError):
                self.runner(conexao).executar()
            self.assertEqual(TRANSACTION_STATUS_INTRANS, conexao.get_transaction_status())
            self.assertEqual([("trabalho_chamador",)], self.executar(
                conexao, "SELECT to_regclass('public.trabalho_chamador')::text"
            ))
            conexao.rollback()

    def test_tpg_019_restauracao_autocommit(self):
        with self.banco() as (_, conectar):
            primeira = conectar()
            primeira.autocommit = True
            self.runner(primeira).executar()
            self.assertTrue(primeira.autocommit)
        with self.banco() as (_, conectar):
            segunda = conectar()
            self.assertFalse(segunda.autocommit)
            self.runner(segunda).executar()
            self.assertFalse(segunda.autocommit)
            self.assertEqual(TRANSACTION_STATUS_IDLE, segunda.get_transaction_status())

    def test_tpg_020_rollback_apos_ddl(self):
        with self.banco() as (_, conectar):
            conexao = conectar()

            def falhar_depois_do_ddl(conexao_real):
                coletar_assinatura_ledger(conexao_real)
                raise RuntimeError("falha controlada do teste")

            with self.assertRaises(MigrationExecutionError):
                self.runner(conexao, schema_factory=falhar_depois_do_ddl).executar()
            snapshot = coletar_snapshot(conexao)
            self.assertFalse(snapshot.objetos_encontrados)
            lock = AdvisoryLock(conexao, timeout_segundos=0.2)
            lock.adquirir()
            lock.liberar()

    def test_tpg_021_concorrencia_e_timeout(self):
        with self.banco() as (_, conectar):
            primeira, segunda = conectar(), conectar()
            lock = AdvisoryLock(primeira, timeout_segundos=1)
            lock.adquirir()
            with self.assertRaises(LockTimeoutError):
                self.runner(segunda, timeout_lock_segundos=0.1).executar()
            self.assertFalse(coletar_snapshot(segunda).objetos_encontrados)
            segunda.rollback()
            lock.liberar()
            self.assertTrue(self.runner(segunda).executar().sucesso)

    def test_tpg_022_dois_runners(self):
        with self.banco() as (_, conectar):
            conexoes = (conectar(), conectar())
            barreira = threading.Barrier(2)
            resultados, erros = [], []

            def executar_runner(conexao):
                try:
                    barreira.wait(timeout=2)
                    resultados.append(self.runner(
                        conexao, timeout_lock_segundos=5
                    ).executar())
                except BaseException as erro:  # evidência preservada pela asserção
                    erros.append(erro)

            threads = [threading.Thread(target=executar_runner, args=(c,)) for c in conexoes]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)
            self.assertFalse(any(thread.is_alive() for thread in threads))
            self.assertEqual([], erros)
            self.assertEqual(2, len(resultados))
            self.assertEqual(1, sum(bool(item.aplicadas) for item in resultados))
            aplicadas, execucoes = coletar_conteudo_ledger(conexoes[0])
            self.assertEqual((1, 1), (len(aplicadas), len(execucoes)))

    def test_tpg_023_persistencia(self):
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.runner(conexao).executar()
            linhas = self.executar(
                conexao,
                "SELECT c.relkind, c.relpersistence FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='public' AND c.relkind IN ('r','S','i')",
            )
            self.assertEqual(10, len(linhas))
            self.assertTrue(all(persistencia == "p" for _, persistencia in linhas))

    def test_tpg_024_ausencia_h001_h011(self):
        self.assertEqual({"M0000", "M0001"}, {
            item.identificador for item in self._manifesto.operacoes
        })
        with self.banco() as (_, conectar):
            conexao = conectar()
            self.runner(conexao).executar()
            nomes = [linha[0] for linha in self.executar(
                conexao,
                "SELECT c.relname FROM pg_catalog.pg_class c "
                "JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace "
                "WHERE n.nspname='public' AND c.relkind IN ('r','p')",
            )]
            self.assertEqual(
                {"schema_migrations", "schema_migration_execucoes"}, set(nomes)
            )
            self.assertFalse(any(nome.startswith("fc_") for nome in nomes))


if __name__ == "__main__":
    unittest.main()
