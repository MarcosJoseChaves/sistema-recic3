"""Operações parametrizadas do ledger de migrations."""

from __future__ import annotations

import hashlib
import os
import socket
from datetime import datetime, timezone
from uuid import UUID

from .errors import ImpossibleLedgerStateError
from .models import ManifestOperation


VERSAO_APLICATIVO = 1


def identificador_host_seguro() -> str | None:
    """Retorna somente um hash curto do host, nunca seu nome original."""
    try:
        nome = socket.gethostname()
    except OSError:
        return None
    return hashlib.sha256(nome.encode("utf-8")).hexdigest()[:16] if nome else None


def registrar_m0001_aplicada(
    cursor,
    operacao: ManifestOperation,
    *,
    request_id: UUID,
    iniciada_em: datetime,
    concluida_em: datetime,
    duracao_ms: int,
    manifesto_versao: int,
) -> None:
    """Autorregistra a M0001 e sua execução na transação corrente."""
    cursor.execute(
        "INSERT INTO public.schema_migrations "
        "(migration_id, modulo, versao, ordem, checksum_sha256, aplicada_em, "
        "duracao_ms, versao_aplicativo, manifesto_versao) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            operacao.identificador, operacao.modulo, 1, operacao.ordem_global,
            operacao.checksum, concluida_em, duracao_ms, VERSAO_APLICATIVO,
            manifesto_versao,
        ),
    )
    cursor.execute(
        "INSERT INTO public.schema_migration_execucoes "
        "(migration_id, tentativa, situacao, iniciada_em, concluida_em, duracao_ms, "
        "checksum_sha256, erro_codigo, erro_sanitizado, request_id, "
        "host_identificador, processo_id, versao_aplicativo) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            operacao.identificador, 0, "APLICADA", iniciada_em, concluida_em,
            duracao_ms, operacao.checksum, None, None, str(request_id),
            identificador_host_seguro(), os.getpid(), VERSAO_APLICATIVO,
        ),
    )


def iniciar_tentativa(
    cursor,
    operacao: ManifestOperation,
    *,
    tentativa: int,
    request_id: UUID,
    iniciada_em: datetime,
) -> None:
    """Acrescenta uma tentativa futura no estado INICIADA."""
    if tentativa < 0:
        raise ImpossibleLedgerStateError()
    cursor.execute(
        "INSERT INTO public.schema_migration_execucoes "
        "(migration_id, tentativa, situacao, iniciada_em, checksum_sha256, "
        "request_id, host_identificador, processo_id, versao_aplicativo) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
        (
            operacao.identificador, tentativa, "INICIADA", iniciada_em,
            operacao.checksum, str(request_id), identificador_host_seguro(),
            os.getpid(), VERSAO_APLICATIVO,
        ),
    )


def concluir_tentativa(
    cursor,
    operacao: ManifestOperation,
    *,
    tentativa: int,
    situacao: str,
    concluida_em: datetime,
    duracao_ms: int,
    erro_codigo: str | None = None,
    erro_sanitizado: str | None = None,
) -> None:
    """Finaliza uma tentativa existente sem apagar seu registro histórico."""
    if situacao not in {"APLICADA", "FALHOU"} or tentativa < 0 or duracao_ms < 0:
        raise ImpossibleLedgerStateError()
    if situacao == "APLICADA" and (erro_codigo is not None or erro_sanitizado is not None):
        raise ImpossibleLedgerStateError()
    if situacao == "FALHOU" and (not erro_codigo or not erro_sanitizado):
        raise ImpossibleLedgerStateError()
    cursor.execute(
        "UPDATE public.schema_migration_execucoes SET situacao = %s, concluida_em = %s, "
        "duracao_ms = %s, erro_codigo = %s, erro_sanitizado = %s "
        "WHERE migration_id = %s AND tentativa = %s AND situacao = %s "
        "RETURNING id",
        (
            situacao, concluida_em, duracao_ms, erro_codigo, erro_sanitizado,
            operacao.identificador, tentativa, "INICIADA",
        ),
    )
    if cursor.fetchone() is None:
        raise ImpossibleLedgerStateError()


def agora_utc() -> datetime:
    """Fornece data/hora consciente de fuso para auditoria."""
    return datetime.now(timezone.utc)
