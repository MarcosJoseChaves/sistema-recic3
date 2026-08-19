"""Entrada explícita e controlada para instalar ou reconhecer o ledger inicial."""

from __future__ import annotations

from .errors import DatabaseConnectionError
from .runner import MigrationRunner


def executar_bootstrap_controlado(
    conexao,
    *,
    caminho_manifesto=None,
    timeout_lock_segundos: float = 30.0,
    event_logger=None,
):
    """Executa somente o bootstrap M0001 usando uma conexão fornecida pelo chamador."""
    if conexao is None:
        raise DatabaseConnectionError()
    runner = MigrationRunner(
        conexao,
        caminho_manifesto=caminho_manifesto,
        timeout_lock_segundos=timeout_lock_segundos,
        event_logger=event_logger,
    )
    return runner.executar()
