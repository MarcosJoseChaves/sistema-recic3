"""Advisory lock determinístico usado durante uma execução controlada."""

from __future__ import annotations

import hashlib
import time

from .errors import LockError, LockReleaseError, LockTimeoutError


LOCK_SOURCE = "sistema-recic3:baseline:public:v1"
DEFAULT_TIMEOUT_SECONDS = 30.0


def derivar_chave_lock(origem: str = LOCK_SOURCE) -> int:
    """Converte os oito primeiros bytes do SHA-256 em inteiro assinado estável."""
    return int.from_bytes(hashlib.sha256(origem.encode("utf-8")).digest()[:8], "big", signed=True)


class AdvisoryLock:
    """Mantém um advisory lock na mesma conexão e o libera explicitamente."""

    def __init__(
        self, conexao, *, timeout_segundos: float = DEFAULT_TIMEOUT_SECONDS,
        intervalo_segundos: float = 0.1, clock=time.monotonic, sleeper=time.sleep,
    ) -> None:
        if timeout_segundos < 0 or intervalo_segundos <= 0:
            raise ValueError("Timeout ou intervalo inválido.")
        self.conexao = conexao
        self.timeout_segundos = timeout_segundos
        self.intervalo_segundos = intervalo_segundos
        self.clock = clock
        self.sleeper = sleeper
        self.chave = derivar_chave_lock()
        self.adquirido = False

    def adquirir(self) -> None:
        limite = self.clock() + self.timeout_segundos
        while True:
            cursor = None
            erro_consulta: BaseException | None = None
            erro_fechamento: BaseException | None = None
            linha = None
            try:
                cursor = self.conexao.cursor()
                cursor.execute("SELECT pg_catalog.pg_try_advisory_lock(%s)", (self.chave,))
                linha = cursor.fetchone()
            except Exception as erro:
                erro_consulta = erro
            finally:
                if cursor is not None:
                    try:
                        cursor.close()
                    except Exception as erro:
                        erro_fechamento = erro
            if linha and linha[0] is True:
                self.adquirido = True
            if erro_consulta is not None:
                if erro_fechamento is not None:
                    erro_consulta.add_note("Falha secundária ao fechar cursor do lock.")
                raise LockError() from erro_consulta
            if erro_fechamento is not None:
                raise LockError() from erro_fechamento
            if self.adquirido:
                return
            if self.clock() >= limite:
                raise LockTimeoutError()
            self.sleeper(min(self.intervalo_segundos, max(0.0, limite - self.clock())))

    def liberar(self) -> None:
        if not self.adquirido:
            return
        cursor = None
        erro_consulta: BaseException | None = None
        erro_fechamento: BaseException | None = None
        linha = None
        try:
            cursor = self.conexao.cursor()
            cursor.execute("SELECT pg_catalog.pg_advisory_unlock(%s)", (self.chave,))
            linha = cursor.fetchone()
        except Exception as erro:
            erro_consulta = erro
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception as erro:
                    erro_fechamento = erro
        if linha and linha[0] is True:
            self.adquirido = False
        if erro_consulta is not None:
            if erro_fechamento is not None:
                erro_consulta.add_note("Falha secundária ao fechar cursor do unlock.")
            raise LockReleaseError() from erro_consulta
        if erro_fechamento is not None:
            raise LockReleaseError() from erro_fechamento
        if self.adquirido:
            raise LockReleaseError()

    def __enter__(self):
        self.adquirir()
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> bool:
        try:
            self.liberar()
        except LockError:
            if exc_value is None:
                raise
        return False
