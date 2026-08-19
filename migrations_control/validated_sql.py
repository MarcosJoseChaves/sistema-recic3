"""Carregamento controlado do mesmo SQL validado por checksum."""

from __future__ import annotations

import hashlib
import os
import re
import stat
from dataclasses import dataclass
from pathlib import Path

from .checksum import normalizar_utf8_lf
from .errors import (
    ChecksumMismatchError,
    MissingMigrationFileError,
    UnsafeMigrationPathError,
)


_CHECKSUM_PATTERN = re.compile(r"^[0-9a-f]{64}$")
FileIdentity = tuple[int, int, int, int]


def _identidade_arquivo(resultado_stat) -> FileIdentity:
    return (
        int(resultado_stat.st_dev),
        int(resultado_stat.st_ino),
        int(resultado_stat.st_size),
        int(resultado_stat.st_mtime_ns),
    )


def _conferir_caminho(
    raiz_autorizada: Path,
    caminho_autorizado: Path,
) -> tuple[Path, Path]:
    if not isinstance(raiz_autorizada, Path) or not isinstance(caminho_autorizado, Path):
        raise UnsafeMigrationPathError()
    if not raiz_autorizada.is_absolute() or not caminho_autorizado.is_absolute():
        raise UnsafeMigrationPathError()
    try:
        raiz_resolvida = raiz_autorizada.resolve(strict=True)
        caminho_resolvido = caminho_autorizado.resolve(strict=True)
    except OSError as erro:
        raise MissingMigrationFileError() from erro
    if raiz_resolvida != raiz_autorizada or not raiz_resolvida.is_dir():
        raise UnsafeMigrationPathError()
    if caminho_resolvido != caminho_autorizado:
        raise UnsafeMigrationPathError()
    try:
        caminho_resolvido.relative_to(raiz_resolvida)
    except ValueError as erro:
        raise UnsafeMigrationPathError() from erro
    return raiz_resolvida, caminho_resolvido


@dataclass(frozen=True, slots=True, init=False)
class ValidatedSql:
    """Artefato imutável criado somente por :func:`carregar_sql_validado`.

    O bloqueio do construtor é um contrato da API suportada, não uma fronteira
    contra introspecção deliberada de baixo nível em Python. Por isso o runner
    continua revalidando independentemente todos os invariantes.
    """

    operacao_id: str
    raiz_autorizada: Path
    caminho_autorizado: Path
    caminho_resolvido: Path
    checksum_esperado: str
    checksum_calculado: str
    bytes_normalizados: bytes
    texto_sql: str
    identidade_arquivo: FileIdentity

    def __init__(self, *args, **kwargs) -> None:
        raise TypeError(
            "ValidatedSql não possui construtor público; use carregar_sql_validado()."
        )

    def __post_init__(self) -> None:
        if type(self.operacao_id) is not str or not self.operacao_id.strip():
            raise ChecksumMismatchError()
        if any(not isinstance(item, Path) for item in (
            self.raiz_autorizada, self.caminho_autorizado, self.caminho_resolvido,
        )):
            raise UnsafeMigrationPathError()
        if not self.raiz_autorizada.is_absolute():
            raise UnsafeMigrationPathError()
        if (
            not self.caminho_autorizado.is_absolute()
            or self.caminho_resolvido != self.caminho_autorizado
        ):
            raise UnsafeMigrationPathError()
        try:
            self.caminho_resolvido.relative_to(self.raiz_autorizada)
        except ValueError as erro:
            raise UnsafeMigrationPathError() from erro
        if (
            type(self.checksum_esperado) is not str
            or type(self.checksum_calculado) is not str
            or not _CHECKSUM_PATTERN.fullmatch(self.checksum_esperado)
            or not _CHECKSUM_PATTERN.fullmatch(self.checksum_calculado)
        ):
            raise ChecksumMismatchError()
        if type(self.bytes_normalizados) is not bytes or type(self.texto_sql) is not str:
            raise ChecksumMismatchError()
        if not self.bytes_normalizados or not self.texto_sql.strip():
            raise ChecksumMismatchError()
        try:
            texto_decodificado = self.bytes_normalizados.decode("utf-8", errors="strict")
        except UnicodeDecodeError as erro:
            raise ChecksumMismatchError() from erro
        if (
            normalizar_utf8_lf(self.bytes_normalizados) != self.bytes_normalizados
            or texto_decodificado != self.texto_sql
            or self.texto_sql.encode("utf-8") != self.bytes_normalizados
        ):
            raise ChecksumMismatchError()
        calculado = hashlib.sha256(self.bytes_normalizados).hexdigest()
        if calculado != self.checksum_calculado or calculado != self.checksum_esperado:
            raise ChecksumMismatchError()
        if (
            type(self.identidade_arquivo) is not tuple
            or len(self.identidade_arquivo) != 4
            or any(type(item) is not int or item < 0 for item in self.identidade_arquivo)
        ):
            raise UnsafeMigrationPathError()


def carregar_sql_validado(
    *,
    operacao_id: str,
    raiz_autorizada: Path,
    caminho_autorizado: Path,
    checksum_esperado: str,
) -> ValidatedSql:
    """Lê uma vez e cria o artefato somente após conferir caminho e conteúdo."""
    raiz_resolvida, caminho_resolvido = _conferir_caminho(
        raiz_autorizada, caminho_autorizado
    )
    try:
        stat_caminho_antes = os.lstat(caminho_resolvido)
        if not stat.S_ISREG(stat_caminho_antes.st_mode):
            raise UnsafeMigrationPathError()
        with caminho_resolvido.open("rb") as arquivo:
            stat_aberto_antes = os.fstat(arquivo.fileno())
            conteudo = arquivo.read()
            stat_aberto_depois = os.fstat(arquivo.fileno())
        raiz_depois, caminho_depois = _conferir_caminho(
            raiz_resolvida, caminho_resolvido
        )
        stat_caminho_depois = os.lstat(caminho_depois)
    except UnsafeMigrationPathError:
        raise
    except OSError as erro:
        raise MissingMigrationFileError() from erro

    identidades = {
        _identidade_arquivo(stat_caminho_antes),
        _identidade_arquivo(stat_aberto_antes),
        _identidade_arquivo(stat_aberto_depois),
        _identidade_arquivo(stat_caminho_depois),
    }
    if len(identidades) != 1 or raiz_depois != raiz_resolvida:
        raise UnsafeMigrationPathError()
    identidade = identidades.pop()
    normalizado = normalizar_utf8_lf(conteudo)
    checksum_calculado = hashlib.sha256(normalizado).hexdigest()
    if checksum_calculado != checksum_esperado:
        raise ChecksumMismatchError()
    artefato = object.__new__(ValidatedSql)
    valores_derivados = {
        "operacao_id": operacao_id,
        "raiz_autorizada": raiz_resolvida,
        "caminho_autorizado": caminho_resolvido,
        "caminho_resolvido": caminho_depois,
        "checksum_esperado": checksum_esperado,
        "checksum_calculado": checksum_calculado,
        "bytes_normalizados": normalizado,
        "texto_sql": normalizado.decode("utf-8", errors="strict"),
        "identidade_arquivo": identidade,
    }
    for campo, valor in valores_derivados.items():
        object.__setattr__(artefato, campo, valor)
    artefato.__post_init__()
    return artefato


def validar_artefato_sql(
    artefato,
    *,
    operacao_id: str,
    raiz_autorizada: Path,
    caminho_autorizado: Path,
    checksum_esperado: str,
) -> ValidatedSql:
    """Revalida em memória e reconfirma o caminho antes de entregar ao cursor."""
    if type(artefato) is not ValidatedSql:
        raise ChecksumMismatchError()
    try:
        artefato.__post_init__()
    except (ChecksumMismatchError, UnsafeMigrationPathError):
        raise
    except Exception as erro:
        raise ChecksumMismatchError() from erro
    raiz_resolvida, caminho_resolvido = _conferir_caminho(
        raiz_autorizada, caminho_autorizado
    )
    if (
        artefato.operacao_id != operacao_id
        or artefato.raiz_autorizada != raiz_resolvida
        or artefato.caminho_autorizado != caminho_resolvido
        or artefato.caminho_resolvido != caminho_resolvido
        or artefato.checksum_esperado != checksum_esperado
        or artefato.checksum_calculado != checksum_esperado
    ):
        raise ChecksumMismatchError()
    try:
        atual = os.lstat(caminho_resolvido)
    except OSError as erro:
        raise MissingMigrationFileError() from erro
    if (
        not stat.S_ISREG(atual.st_mode)
        or _identidade_arquivo(atual) != artefato.identidade_arquivo
    ):
        raise UnsafeMigrationPathError()
    return artefato
