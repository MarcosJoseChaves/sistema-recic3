"""Modelos imutáveis usados pelo manifesto, preflight e runner."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class OperationType(str, Enum):
    EXECUTOR = "EXECUTOR"
    NOVA_DDL = "NOVA_DDL"
    HISTORICA_DDL = "HISTORICA_DDL"


class DatabaseClassification(str, Enum):
    BANCO_NOVO = "BANCO_NOVO"
    BANCO_CONTROLADO = "BANCO_CONTROLADO"
    BANCO_DESCONHECIDO = "BANCO_DESCONHECIDO"


class ExecutionState(str, Enum):
    INICIADA = "INICIADA"
    APLICADA = "APLICADA"
    FALHOU = "FALHOU"


@dataclass(frozen=True)
class ManifestOperation:
    identificador: str
    ordem_global: int
    modulo: str
    tipo: OperationType
    descricao: str
    caminho: str | None
    checksum: str | None
    dependencias: tuple[str, ...]
    transacional: bool
    imutavel: bool
    possui_ddl: bool
    dados_estruturais: bool
    testes_exigidos: tuple[str, ...]
    habilitada: bool
    arquivo_resolvido: Path | None = field(default=None, repr=False)


@dataclass(frozen=True)
class MigrationManifest:
    versao_formato: int
    sistema: str
    descricao: str
    normalizacao_checksum: str
    algoritmo_checksum: str
    operacoes: tuple[ManifestOperation, ...]
    caminho: Path = field(repr=False)

    def por_id(self) -> dict[str, ManifestOperation]:
        return {operacao.identificador: operacao for operacao in self.operacoes}


@dataclass(frozen=True)
class AppliedMigration:
    migration_id: str
    ordem: int
    modulo: str
    checksum_sha256: str
    versao: int
    manifesto_versao: int
    aplicada_em: Any = None
    duracao_ms: int | None = None
    versao_aplicativo: int = 1


@dataclass(frozen=True)
class MigrationExecution:
    migration_id: str
    tentativa: int
    situacao: ExecutionState | str
    iniciada_em: Any
    concluida_em: Any
    duracao_ms: int | None
    checksum_sha256: str
    erro_codigo: str | None
    erro_sanitizado: str | None
    request_id: Any
    host_identificador: str | None
    processo_id: int | None
    versao_aplicativo: int


@dataclass(frozen=True)
class PreflightSnapshot:
    public_existe: bool
    objetos_encontrados: frozenset[str]
    objetos_ignorados: frozenset[str] = frozenset()
    assinatura_ledger: Any = None
    migrations_aplicadas: tuple[AppliedMigration, ...] = ()
    execucoes: tuple[MigrationExecution, ...] = ()
    erro_ledger: bool = False


@dataclass(frozen=True)
class PreflightResult:
    classificacao: DatabaseClassification
    objetos_encontrados: tuple[str, ...]
    objetos_ignorados: tuple[str, ...]
    motivo: str
    codigo_saida: int
    pode_prosseguir: bool


@dataclass(frozen=True)
class PlanItem:
    identificador: str
    ordem: int
    tipo: str
    estado: str
    caminho: str | None


@dataclass(frozen=True)
class RunnerResult:
    sucesso: bool
    classificacao_preflight: str
    aplicadas: tuple[str, ...]
    ignoradas: tuple[str, ...]
    codigo_saida: int
    mensagem: str
    request_id: str
