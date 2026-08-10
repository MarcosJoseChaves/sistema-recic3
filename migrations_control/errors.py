"""Erros públicos e sanitizados do executor de migrations."""

from __future__ import annotations


class MigrationControlError(Exception):
    """Erro conhecido que pode ser apresentado sem detalhes sensíveis."""

    codigo = "MIGRATION_CONTROL_ERROR"
    mensagem_publica = "A operação de migration não pôde ser concluída."

    def __init__(self, mensagem: str | None = None) -> None:
        super().__init__(mensagem or self.mensagem_publica)


class ManifestError(MigrationControlError):
    codigo = "MANIFESTO_INVALIDO"
    mensagem_publica = "O manifesto de migrations é inválido."


class InvalidManifestJsonError(ManifestError):
    codigo = "MANIFESTO_JSON_INVALIDO"


class DuplicateJsonKeyError(ManifestError):
    codigo = "MANIFESTO_CHAVE_DUPLICADA"


class UnsupportedManifestVersionError(ManifestError):
    codigo = "MANIFESTO_VERSAO_INCOMPATIVEL"


class DuplicateMigrationError(ManifestError):
    codigo = "MIGRATION_DUPLICADA"


class DuplicateOrderError(ManifestError):
    codigo = "ORDEM_DUPLICADA"


class MissingDependencyError(ManifestError):
    codigo = "DEPENDENCIA_AUSENTE"


class CircularDependencyError(ManifestError):
    codigo = "DEPENDENCIA_CIRCULAR"


class MissingMigrationFileError(ManifestError):
    codigo = "ARQUIVO_AUSENTE"


class ExtraMigrationFileError(ManifestError):
    codigo = "ARQUIVO_EXTRA"


class ChecksumMismatchError(ManifestError):
    codigo = "CHECKSUM_DIVERGENTE"


class NonUtf8FileError(ManifestError):
    codigo = "ARQUIVO_NAO_UTF8"


class UnsafeMigrationPathError(ManifestError):
    codigo = "CAMINHO_INSEGURO"


class UnknownOperationTypeError(ManifestError):
    codigo = "TIPO_OPERACAO_DESCONHECIDO"


class PreflightError(MigrationControlError):
    codigo = "PREFLIGHT_FALHOU"
    mensagem_publica = "O banco não passou pela verificação de segurança."


class UnknownDatabaseError(PreflightError):
    codigo = "BANCO_DESCONHECIDO"


class InvalidLedgerError(PreflightError):
    codigo = "LEDGER_INVALIDO"


class ImpossibleLedgerStateError(InvalidLedgerError):
    codigo = "ESTADO_LEDGER_IMPOSSIVEL"


class LockError(MigrationControlError):
    codigo = "LOCK_FALHOU"
    mensagem_publica = "Não foi possível obter o bloqueio exclusivo."


class LockTimeoutError(LockError):
    codigo = "LOCK_TIMEOUT"


class LockReleaseError(LockError):
    codigo = "LOCK_LIBERACAO_FALHOU"


class AppliedMigrationHashMismatchError(MigrationControlError):
    codigo = "MIGRATION_APLICADA_HASH_DIVERGENTE"
    mensagem_publica = "Uma migration aplicada possui checksum incompatível."


class MigrationExecutionError(MigrationControlError):
    codigo = "MIGRATION_FALHOU"


class DatabaseConnectionError(MigrationControlError):
    codigo = "CONEXAO_FALHOU"
    mensagem_publica = "Não foi possível usar a conexão fornecida."


class ConnectionClosedError(DatabaseConnectionError):
    codigo = "CONEXAO_FECHADA"


class ConnectionNotIdleError(DatabaseConnectionError):
    codigo = "CONEXAO_NAO_OCIOSA"
    mensagem_publica = "A conexão fornecida possui uma transação em andamento."


class ConnectionStateError(DatabaseConnectionError):
    codigo = "ESTADO_CONEXAO_INVALIDO"


class ConnectionRestoreError(DatabaseConnectionError):
    codigo = "RESTAURACAO_CONEXAO_FALHOU"


def sanitizar_erro(erro: BaseException) -> dict[str, str]:
    """Retorna somente código e mensagem previamente aprovados."""
    if isinstance(erro, MigrationControlError):
        return {"codigo": erro.codigo, "mensagem": erro.mensagem_publica}
    return {
        "codigo": MigrationExecutionError.codigo,
        "mensagem": MigrationExecutionError.mensagem_publica,
    }
