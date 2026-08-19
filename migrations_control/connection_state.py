"""Contrato seguro da conexão dedicada entregue ao runner."""

from __future__ import annotations

from .errors import (
    ConnectionClosedError,
    ConnectionNotIdleError,
    ConnectionRestoreError,
    ConnectionStateError,
)


TRANSACTION_STATUS_IDLE = 0
TRANSACTION_STATUS_ACTIVE = 1
TRANSACTION_STATUS_INTRANS = 2
TRANSACTION_STATUS_INERROR = 3
TRANSACTION_STATUS_UNKNOWN = 4


class ConnectionState:
    """Controla autocommit sem confirmar ou reverter trabalho anterior."""

    def __init__(self, conexao) -> None:
        self.conexao = conexao
        self.autocommit_original: bool | None = None

    def status(self) -> int:
        try:
            return self.conexao.get_transaction_status()
        except Exception as erro:
            raise ConnectionStateError() from erro

    def validar_entrada(self) -> None:
        try:
            if self.conexao.closed:
                raise ConnectionClosedError()
            autocommit = self.conexao.autocommit
        except ConnectionClosedError:
            raise
        except Exception as erro:
            raise ConnectionStateError() from erro
        if type(autocommit) is not bool:
            raise ConnectionStateError()
        if self.status() != TRANSACTION_STATUS_IDLE:
            raise ConnectionNotIdleError()
        self.autocommit_original = autocommit

    def definir_autocommit(self, valor: bool) -> None:
        if self.status() != TRANSACTION_STATUS_IDLE:
            raise ConnectionNotIdleError()
        try:
            self.conexao.autocommit = valor
        except Exception as erro:
            raise ConnectionStateError() from erro
        if self.conexao.autocommit is not valor or self.status() != TRANSACTION_STATUS_IDLE:
            raise ConnectionStateError()

    def preparar_operacoes_sem_transacao(self) -> None:
        self.definir_autocommit(True)

    def iniciar_migration(self) -> None:
        self.definir_autocommit(False)

    def confirmar_migration(self) -> None:
        try:
            self.conexao.commit()
        except Exception as erro:
            raise ConnectionStateError() from erro
        if self.status() != TRANSACTION_STATUS_IDLE:
            raise ConnectionStateError()

    def reverter_migration(self) -> None:
        try:
            self.conexao.rollback()
        except Exception as erro:
            raise ConnectionStateError() from erro
        if self.status() != TRANSACTION_STATUS_IDLE:
            raise ConnectionStateError()

    def restaurar(self) -> None:
        if self.autocommit_original is None:
            return
        if self.status() != TRANSACTION_STATUS_IDLE:
            raise ConnectionRestoreError()
        try:
            self.conexao.autocommit = self.autocommit_original
        except Exception as erro:
            raise ConnectionRestoreError() from erro
        if self.conexao.autocommit is not self.autocommit_original:
            raise ConnectionRestoreError()
        if self.status() != TRANSACTION_STATUS_IDLE:
            raise ConnectionRestoreError()
