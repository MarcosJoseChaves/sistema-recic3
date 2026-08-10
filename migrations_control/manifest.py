"""Leitura estrita do manifesto de migrations, sem executar código."""

from __future__ import annotations

import json
import re
from pathlib import Path, PurePosixPath
from typing import Any

from .checksum import calcular_sha256_arquivo
from .errors import (
    ChecksumMismatchError,
    CircularDependencyError,
    DuplicateMigrationError,
    DuplicateOrderError,
    DuplicateJsonKeyError,
    ExtraMigrationFileError,
    InvalidManifestJsonError,
    ManifestError,
    MissingDependencyError,
    MissingMigrationFileError,
    UnknownOperationTypeError,
    UnsafeMigrationPathError,
    UnsupportedManifestVersionError,
)
from .models import ManifestOperation, MigrationManifest, OperationType


ROOT_FIELDS = frozenset({
    "versao_formato",
    "sistema",
    "descricao",
    "normalizacao_checksum",
    "algoritmo_checksum",
    "operacoes",
})
OPERATION_FIELDS = frozenset({
    "identificador",
    "ordem_global",
    "modulo",
    "tipo",
    "descricao",
    "caminho",
    "checksum",
    "dependencias",
    "transacional",
    "imutavel",
    "possui_ddl",
    "dados_estruturais",
    "testes_exigidos",
    "habilitada",
})
IDENTIFIER_PATTERN = re.compile(r"^M\d{4}$")
CHECKSUM_PATTERN = re.compile(r"^[0-9a-f]{64}$")


def _objeto_sem_chaves_duplicadas(pares: list[tuple[str, Any]]) -> dict[str, Any]:
    resultado: dict[str, Any] = {}
    for chave, valor in pares:
        if chave in resultado:
            raise DuplicateJsonKeyError()
        resultado[chave] = valor
    return resultado


def _objeto_exato(valor: Any, campos: frozenset[str], contexto: str) -> dict[str, Any]:
    if not isinstance(valor, dict) or set(valor) != campos:
        raise ManifestError(f"Campos inválidos em {contexto}.")
    return valor


def _texto(valor: Any, campo: str) -> str:
    if not isinstance(valor, str) or not valor.strip():
        raise ManifestError(f"Campo textual inválido: {campo}.")
    return valor


def _booleano(valor: Any, campo: str) -> bool:
    if not isinstance(valor, bool):
        raise ManifestError(f"Campo booleano inválido: {campo}.")
    return valor


def _lista_textos(valor: Any, campo: str, *, permite_vazia: bool) -> tuple[str, ...]:
    if not isinstance(valor, list) or (not permite_vazia and not valor):
        raise ManifestError(f"Lista inválida: {campo}.")
    if any(not isinstance(item, str) or not item.strip() for item in valor):
        raise ManifestError(f"Item inválido em {campo}.")
    return tuple(valor)


def _resolver_sql(base: Path, caminho: Any) -> tuple[str, Path]:
    if not isinstance(caminho, str) or not caminho:
        raise MissingMigrationFileError()
    posix = PurePosixPath(caminho)
    if (
        posix.is_absolute()
        or ".." in posix.parts
        or "\\" in caminho
        or not posix.parts
        or posix.parts[0] != "sql"
        or posix.suffix.lower() != ".sql"
    ):
        raise UnsafeMigrationPathError()
    raiz_sql = (base / "sql").resolve()
    arquivo = (base / Path(*posix.parts)).resolve()
    try:
        arquivo.relative_to(raiz_sql)
    except ValueError as erro:
        raise UnsafeMigrationPathError() from erro
    if not arquivo.is_file():
        raise MissingMigrationFileError()
    return caminho, arquivo


def _ler_operacao(dados: Any, base: Path) -> ManifestOperation:
    op = _objeto_exato(dados, OPERATION_FIELDS, "operação")
    identificador = _texto(op["identificador"], "identificador")
    if not IDENTIFIER_PATTERN.fullmatch(identificador):
        raise ManifestError("Identificador de migration inválido.")
    ordem = op["ordem_global"]
    if type(ordem) is not int or ordem < 0:
        raise ManifestError("Ordem global inválida.")
    try:
        tipo = OperationType(op["tipo"])
    except (TypeError, ValueError) as erro:
        raise UnknownOperationTypeError() from erro

    caminho = op["caminho"]
    checksum = op["checksum"]
    arquivo: Path | None = None
    possui_ddl = _booleano(op["possui_ddl"], "possui_ddl")
    if caminho is None:
        if checksum is not None:
            raise ManifestError("Operação sem arquivo não pode ter checksum.")
    else:
        caminho, arquivo = _resolver_sql(base, caminho)
        if not isinstance(checksum, str) or not CHECKSUM_PATTERN.fullmatch(checksum):
            raise ManifestError("Checksum inválido.")
        if calcular_sha256_arquivo(arquivo) != checksum:
            raise ChecksumMismatchError()

    operacao = ManifestOperation(
        identificador=identificador,
        ordem_global=ordem,
        modulo=_texto(op["modulo"], "modulo"),
        tipo=tipo,
        descricao=_texto(op["descricao"], "descricao"),
        caminho=caminho,
        checksum=checksum,
        dependencias=_lista_textos(op["dependencias"], "dependencias", permite_vazia=True),
        transacional=_booleano(op["transacional"], "transacional"),
        imutavel=_booleano(op["imutavel"], "imutavel"),
        possui_ddl=possui_ddl,
        dados_estruturais=_booleano(op["dados_estruturais"], "dados_estruturais"),
        testes_exigidos=_lista_textos(
            op["testes_exigidos"], "testes_exigidos", permite_vazia=False
        ),
        habilitada=_booleano(op["habilitada"], "habilitada"),
        arquivo_resolvido=arquivo,
    )
    if operacao.tipo is OperationType.EXECUTOR and (arquivo is not None or possui_ddl):
        raise ManifestError("Operação do executor não pode conter DDL.")
    if operacao.tipo is OperationType.NOVA_DDL and (arquivo is None or not possui_ddl):
        raise ManifestError("Migration física deve declarar seu arquivo e DDL.")
    return operacao


def _validar_dependencias(operacoes: tuple[ManifestOperation, ...]) -> None:
    por_id = {op.identificador: op for op in operacoes}
    for op in operacoes:
        for dependencia in op.dependencias:
            if dependencia not in por_id:
                raise MissingDependencyError()

    visitando: set[str] = set()
    visitados: set[str] = set()

    def visitar(identificador: str) -> None:
        if identificador in visitando:
            raise CircularDependencyError()
        if identificador in visitados:
            return
        visitando.add(identificador)
        for dependencia in por_id[identificador].dependencias:
            visitar(dependencia)
        visitando.remove(identificador)
        visitados.add(identificador)

    for identificador in por_id:
        visitar(identificador)
    for op in operacoes:
        if any(por_id[dep].ordem_global >= op.ordem_global for dep in op.dependencias):
            raise ManifestError("A ordem não respeita as dependências.")
        if op.habilitada and any(not por_id[dep].habilitada for dep in op.dependencias):
            raise ManifestError("Operação habilitada depende de operação desabilitada.")


def _validar_contrato_inicial(operacoes: tuple[ManifestOperation, ...]) -> None:
    por_id = {op.identificador: op for op in operacoes}
    if set(por_id) != {"M0000", "M0001"}:
        raise ManifestError("Esta versão aceita somente M0000 e M0001.")
    m0000, m0001 = por_id["M0000"], por_id["M0001"]
    if not (
        m0000.ordem_global == 0
        and m0000.tipo is OperationType.EXECUTOR
        and m0000.caminho is None
        and not m0000.transacional
        and not m0000.possui_ddl
        and m0000.habilitada
    ):
        raise ManifestError("Definição inválida da M0000.")
    if not (
        m0001.ordem_global == 1
        and m0001.tipo is OperationType.NOVA_DDL
        and m0001.dependencias == ("M0000",)
        and m0001.transacional
        and m0001.imutavel
        and m0001.possui_ddl
        and m0001.habilitada
        and m0001.caminho == "sql/M0001_criar_ledger.sql"
    ):
        raise ManifestError("Definição inválida da M0001.")


def carregar_manifesto(caminho: Path | str | None = None) -> MigrationManifest:
    """Carrega e valida integralmente o manifesto JSON e seus SQLs."""
    arquivo_manifesto = (
        Path(caminho)
        if caminho is not None
        else Path(__file__).with_name("manifesto.json")
    ).resolve()
    try:
        dados = json.loads(
            arquivo_manifesto.read_text(encoding="utf-8"),
            object_pairs_hook=_objeto_sem_chaves_duplicadas,
        )
    except (OSError, UnicodeError, json.JSONDecodeError) as erro:
        raise InvalidManifestJsonError() from erro
    raiz = _objeto_exato(dados, ROOT_FIELDS, "raiz")
    if type(raiz["versao_formato"]) is not int or raiz["versao_formato"] != 1:
        raise UnsupportedManifestVersionError()
    if raiz["normalizacao_checksum"] != "UTF-8/LF" or raiz["algoritmo_checksum"] != "SHA-256":
        raise ManifestError("Política de checksum inválida.")
    if not isinstance(raiz["operacoes"], list) or not raiz["operacoes"]:
        raise ManifestError("Lista de operações inválida.")
    operacoes = tuple(_ler_operacao(item, arquivo_manifesto.parent) for item in raiz["operacoes"])
    ids = [op.identificador for op in operacoes]
    ordens = [op.ordem_global for op in operacoes]
    if len(ids) != len(set(ids)):
        raise DuplicateMigrationError()
    if len(ordens) != len(set(ordens)):
        raise DuplicateOrderError()
    _validar_dependencias(operacoes)
    _validar_contrato_inicial(operacoes)

    referenciados = {op.arquivo_resolvido for op in operacoes if op.arquivo_resolvido}
    pasta_sql = arquivo_manifesto.parent / "sql"
    encontrados = {item.resolve() for item in pasta_sql.rglob("*.sql")} if pasta_sql.exists() else set()
    if encontrados - referenciados:
        raise ExtraMigrationFileError()
    return MigrationManifest(
        versao_formato=1,
        sistema=_texto(raiz["sistema"], "sistema"),
        descricao=_texto(raiz["descricao"], "descricao"),
        normalizacao_checksum=raiz["normalizacao_checksum"],
        algoritmo_checksum=raiz["algoritmo_checksum"],
        operacoes=tuple(sorted(operacoes, key=lambda op: op.ordem_global)),
        caminho=arquivo_manifesto,
    )
