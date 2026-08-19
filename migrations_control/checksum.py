"""Normalização reproduzível e checksum dos arquivos de migration."""

from __future__ import annotations

import hashlib
from pathlib import Path

from .errors import NonUtf8FileError


def normalizar_utf8_lf(conteudo: bytes) -> bytes:
    """Decodifica UTF-8 estrito e normaliza apenas CRLF/CR para LF."""
    try:
        texto = conteudo.decode("utf-8", errors="strict")
    except UnicodeDecodeError as erro:
        raise NonUtf8FileError() from erro
    return texto.replace("\r\n", "\n").replace("\r", "\n").encode("utf-8")


def calcular_sha256_normalizado(conteudo: bytes) -> str:
    """Calcula SHA-256 minúsculo sobre o conteúdo UTF-8/LF normalizado."""
    return hashlib.sha256(normalizar_utf8_lf(conteudo)).hexdigest()


def calcular_sha256_arquivo(caminho: Path) -> str:
    """Lê sem alterar o arquivo e calcula seu checksum normalizado."""
    return calcular_sha256_normalizado(caminho.read_bytes())
