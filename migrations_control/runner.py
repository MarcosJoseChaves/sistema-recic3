"""Orquestração segura e transacional da bootstrap M0001."""

from __future__ import annotations

import time
from collections.abc import Callable
from uuid import uuid4

from .connection_state import ConnectionState, TRANSACTION_STATUS_IDLE
from .errors import (
    AppliedMigrationHashMismatchError,
    DatabaseConnectionError,
    InvalidLedgerError,
    LockError,
    MigrationControlError,
    MigrationExecutionError,
    UnknownDatabaseError,
    sanitizar_erro,
)
from .historical_sql import adaptar_wrapper_historico
from .ledger import (
    agora_utc,
    registrar_m0001_aplicada,
    registrar_migration_aplicada,
    registrar_migration_falhou,
)
from .locking import AdvisoryLock
from .manifest import carregar_manifesto
from .models import (
    DatabaseClassification,
    OperationType,
    PlanItem,
    PreflightSnapshot,
    RunnerResult,
)
from .preflight import (
    classificar_preflight,
    coletar_conteudo_ledger,
    coletar_snapshot,
    validar_conteudo_ledger,
)
from .schema_validation import coletar_assinatura_ledger, validar_assinatura_ledger
from .validated_sql import carregar_sql_validado, validar_artefato_sql


EventLogger = Callable[..., None]


def _registrar_evento_padrao(evento: str, **campos) -> None:
    """Usa o log estruturado existente apenas quando o runner é executado."""
    try:
        from logging_operacional import registrar_evento
    except ImportError:
        import logging

        logging.getLogger("sistema_recic3.migrations").info(evento)
        return
    registrar_evento(evento, **campos)


def _erro_controlado(erro: BaseException) -> MigrationControlError:
    if isinstance(erro, MigrationControlError):
        return erro
    controlado = MigrationExecutionError()
    controlado.__cause__ = erro
    return controlado


class MigrationRunner:
    """Runner com conexão dedicada, explícita e inicialmente ociosa."""

    def __init__(
        self,
        conexao=None,
        *,
        caminho_manifesto=None,
        timeout_lock_segundos: float = 30.0,
        event_logger: EventLogger | None = None,
        lock_factory=AdvisoryLock,
        snapshot_factory=coletar_snapshot,
        schema_factory=coletar_assinatura_ledger,
        content_factory=coletar_conteudo_ledger,
        sql_loader=carregar_sql_validado,
        clock=time.monotonic,
    ) -> None:
        self.conexao = conexao
        self.manifesto = carregar_manifesto(caminho_manifesto)
        self.timeout_lock_segundos = timeout_lock_segundos
        self.event_logger = event_logger or _registrar_evento_padrao
        self.lock_factory = lock_factory
        self.snapshot_factory = snapshot_factory
        self.schema_factory = schema_factory
        self.content_factory = content_factory
        self.sql_loader = sql_loader
        self.clock = clock

    def mostrar_plano(self, snapshot=None) -> tuple[PlanItem, ...]:
        """Mostra executor, migrations desabilitadas, aplicadas e pendentes."""
        aplicadas = {
            item.migration_id: item for item in (snapshot.migrations_aplicadas if snapshot else ())
        }
        itens: list[PlanItem] = []
        for op in self.manifesto.operacoes:
            if not op.habilitada:
                estado = "DESABILITADA"
            elif op.identificador == "M0000":
                estado = "EXECUTOR"
            elif op.identificador in aplicadas:
                if aplicadas[op.identificador].checksum_sha256 != op.checksum:
                    raise AppliedMigrationHashMismatchError()
                estado = "APLICADA"
            else:
                estado = "PENDENTE"
            itens.append(PlanItem(op.identificador, op.ordem_global, op.tipo.value, estado, op.caminho))
        return tuple(itens)

    def _safe_log(self, evento: str, **campos) -> None:
        """Logging best effort: uma falha do logger nunca altera a operação."""
        try:
            self.event_logger(evento, **campos)
        except Exception:
            return

    def _log_secundario(self, evento: str, erro: BaseException, request_id: str) -> None:
        seguro = sanitizar_erro(_erro_controlado(erro))
        self._safe_log(
            evento, nivel="ERROR", request_id=request_id,
            erro_codigo=seguro["codigo"], resultado="falha_secundaria",
        )

    def executar(self) -> RunnerResult:
        """Executa somente M0001 sem assumir ou limpar estado anterior do chamador."""
        if self.conexao is None:
            raise DatabaseConnectionError()
        request_id = uuid4()
        request_id_texto = str(request_id)
        estado = ConnectionState(self.conexao)
        estado.validar_entrada()
        lock = self.lock_factory(self.conexao, timeout_segundos=self.timeout_lock_segundos)
        erro_principal: MigrationControlError | None = None
        erro_limpeza: MigrationControlError | None = None
        resultado: RunnerResult | None = None
        migration_iniciada = False
        self._safe_log("migration_runner_iniciado", request_id=request_id_texto)
        try:
            estado.preparar_operacoes_sem_transacao()
            lock.adquirir()
            self._safe_log("migration_lock_adquirido", request_id=request_id_texto)
            snapshot = self.snapshot_factory(self.conexao)
            for aplicada in snapshot.migrations_aplicadas:
                esperada = self.manifesto.por_id().get(aplicada.migration_id)
                if esperada is not None and esperada.checksum != aplicada.checksum_sha256:
                    raise AppliedMigrationHashMismatchError()
            preflight = classificar_preflight(snapshot, self.manifesto)
            self._safe_log(
                "migration_preflight_concluido", request_id=request_id_texto,
                classificacao=preflight.classificacao.value,
                resultado="permitido" if preflight.pode_prosseguir else "bloqueado",
            )
            if not preflight.pode_prosseguir:
                raise UnknownDatabaseError()
            plano = self.mostrar_plano(snapshot)
            pendentes = [item for item in plano if item.estado == "PENDENTE"]
            if preflight.classificacao is DatabaseClassification.BANCO_CONTROLADO:
                resultado = RunnerResult(
                    True, preflight.classificacao.value, (), ("M0001",), 0,
                    "Nenhuma migration pendente.", request_id_texto,
                )
            else:
                if "M0001" not in {item.identificador for item in pendentes}:
                    raise UnknownDatabaseError()
                estado.iniciar_migration()
                migration_iniciada = True
                duracao_ms = self._aplicar_m0001(request_id)
                estado.confirmar_migration()
                migration_iniciada = False
                estado.preparar_operacoes_sem_transacao()
                self._safe_log(
                    "migration_aplicada", request_id=request_id_texto,
                    migration="M0001", ordem=1, estado="APLICADA",
                    duracao_ms=duracao_ms, resultado="sucesso",
                )
                resultado = RunnerResult(
                    True, preflight.classificacao.value, ("M0001",), (), 0,
                    "M0001 aplicada com sucesso.", request_id_texto,
                )
        except Exception as erro:
            erro_principal = _erro_controlado(erro)
            if migration_iniciada:
                try:
                    if estado.status() != TRANSACTION_STATUS_IDLE:
                        estado.reverter_migration()
                except Exception as erro_rollback:
                    self._log_secundario(
                        "migration_rollback_falhou", erro_rollback, request_id_texto
                    )
        finally:
            if lock.adquirido:
                try:
                    if estado.status() != TRANSACTION_STATUS_IDLE:
                        raise LockError()
                    estado.preparar_operacoes_sem_transacao()
                    lock.liberar()
                    self._safe_log("migration_lock_liberado", request_id=request_id_texto)
                except Exception as erro_unlock:
                    convertido = _erro_controlado(erro_unlock)
                    self._log_secundario(
                        "migration_lock_liberacao_falhou", convertido, request_id_texto
                    )
                    if erro_principal is None:
                        erro_limpeza = convertido
            try:
                estado.restaurar()
            except Exception as erro_restauracao:
                convertido = _erro_controlado(erro_restauracao)
                self._log_secundario(
                    "migration_conexao_restauracao_falhou", convertido, request_id_texto
                )
                if erro_principal is None and erro_limpeza is None:
                    erro_limpeza = convertido

        erro_final = erro_principal or erro_limpeza
        if erro_final is not None:
            seguro = sanitizar_erro(erro_final)
            self._safe_log(
                "migration_runner_falhou", nivel="ERROR", request_id=request_id_texto,
                erro_codigo=seguro["codigo"], resultado="falha",
            )
            raise erro_final
        if resultado is None:
            raise MigrationExecutionError()
        return resultado

    def _aplicar_m0001(self, request_id) -> int:
        operacao = self.manifesto.por_id()["M0001"]
        iniciada_em = agora_utc()
        inicio = self.clock()
        raiz_sql = (self.manifesto.caminho.parent / "sql").resolve(strict=True)
        artefato = self.sql_loader(
            operacao_id=operacao.identificador,
            raiz_autorizada=raiz_sql,
            caminho_autorizado=operacao.arquivo_resolvido,
            checksum_esperado=operacao.checksum,
        )
        artefato = validar_artefato_sql(
            artefato,
            operacao_id=operacao.identificador,
            raiz_autorizada=raiz_sql,
            caminho_autorizado=operacao.arquivo_resolvido,
            checksum_esperado=operacao.checksum,
        )
        cursor = None
        erro_principal: Exception | None = None
        try:
            cursor = self.conexao.cursor()
            cursor.execute(artefato.texto_sql)
        except Exception as erro:
            erro_principal = erro
            raise MigrationExecutionError() from erro
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception as erro_close:
                    if erro_principal is None:
                        raise MigrationExecutionError() from erro_close
                    self._log_secundario(
                        "migration_cursor_fechamento_falhou", erro_close, str(request_id)
                    )

        assinatura = self.schema_factory(self.conexao)
        estrutura_valida, _ = validar_assinatura_ledger(assinatura)
        if not estrutura_valida:
            raise InvalidLedgerError()

        concluida_em = agora_utc()
        duracao_ms = max(0, round((self.clock() - inicio) * 1000))
        cursor = None
        erro_principal = None
        try:
            cursor = self.conexao.cursor()
            registrar_m0001_aplicada(
                cursor, operacao, request_id=request_id, iniciada_em=iniciada_em,
                concluida_em=concluida_em, duracao_ms=duracao_ms,
                manifesto_versao=self.manifesto.versao_formato,
            )
        except Exception as erro:
            erro_principal = erro
            raise MigrationExecutionError() from erro
        finally:
            if cursor is not None:
                try:
                    cursor.close()
                except Exception as erro_close:
                    if erro_principal is None:
                        raise MigrationExecutionError() from erro_close
                    self._log_secundario(
                        "migration_cursor_fechamento_falhou", erro_close, str(request_id)
                    )

        aplicadas, execucoes = self.content_factory(self.conexao)
        snapshot = PreflightSnapshot(
            True, frozenset({"schema_migrations", "schema_migration_execucoes"}),
            assinatura_ledger=assinatura,
            migrations_aplicadas=aplicadas,
            execucoes=execucoes,
        )
        conteudo_valido, _ = validar_conteudo_ledger(snapshot, self.manifesto)
        if not conteudo_valido:
            raise InvalidLedgerError()
        if not any(
            item.migration_id == "M0001"
            and item.tentativa == 0
            and item.situacao == "APLICADA"
            and item.request_id == request_id
            and item.duracao_ms == duracao_ms
            for item in execucoes
        ):
            raise InvalidLedgerError()
        return duracao_ms

    def _raiz_autorizada(self, operacao):
        if operacao.caminho.startswith("sql/"):
            return (self.manifesto.caminho.parent / "sql").resolve(strict=True)
        return (
            self.manifesto.caminho.parent.parent
            / "modulos" / "fiscalizacao_contratos" / "migrations"
        ).resolve(strict=True)

    def _carregar_texto_operacao(self, operacao) -> str:
        raiz = self._raiz_autorizada(operacao)
        artefato = self.sql_loader(
            operacao_id=operacao.identificador,
            raiz_autorizada=raiz,
            caminho_autorizado=operacao.arquivo_resolvido,
            checksum_esperado=operacao.checksum,
        )
        artefato = validar_artefato_sql(
            artefato,
            operacao_id=operacao.identificador,
            raiz_autorizada=raiz,
            caminho_autorizado=operacao.arquivo_resolvido,
            checksum_esperado=operacao.checksum,
        )
        if operacao.tipo is OperationType.HISTORICA_DDL:
            return adaptar_wrapper_historico(artefato.texto_sql)
        return artefato.texto_sql

    def executar_cadeia_controlada(self) -> RunnerResult:
        """Aplica explicitamente M0002–H011; nunca executa o bootstrap M0001."""
        if self.conexao is None:
            raise DatabaseConnectionError()
        request_id = uuid4()
        request_id_texto = str(request_id)
        estado = ConnectionState(self.conexao)
        estado.validar_entrada()
        lock = self.lock_factory(self.conexao, timeout_segundos=self.timeout_lock_segundos)
        aplicadas: list[str] = []
        erro_principal: MigrationControlError | None = None
        erro_limpeza: MigrationControlError | None = None
        resultado: RunnerResult | None = None
        migration_iniciada = False
        operacao_atual = None
        iniciada_em_atual = None
        inicio_atual = None
        try:
            estado.preparar_operacoes_sem_transacao()
            lock.adquirir()
            snapshot = self.snapshot_factory(self.conexao)
            preflight = classificar_preflight(snapshot, self.manifesto)
            ids_aplicados = {item.migration_id for item in snapshot.migrations_aplicadas}
            if (
                not preflight.pode_prosseguir
                or preflight.classificacao is not DatabaseClassification.BANCO_CONTROLADO
                or ids_aplicados != {"M0001"}
            ):
                raise UnknownDatabaseError()
            pendentes = [
                operacao for operacao in self.manifesto.operacoes
                if operacao.identificador not in {"M0000", "M0001"}
                and operacao.habilitada
                and operacao.identificador not in ids_aplicados
            ]
            anterior_aplicada = "M0001"
            for operacao in pendentes:
                operacao_atual = operacao
                if operacao.dependencias != (anterior_aplicada,):
                    raise InvalidLedgerError()
                iniciada_em = agora_utc()
                inicio = self.clock()
                iniciada_em_atual = iniciada_em
                inicio_atual = inicio
                estado.iniciar_migration()
                migration_iniciada = True
                cursor = None
                try:
                    texto_sql = self._carregar_texto_operacao(operacao)
                    cursor = self.conexao.cursor()
                    cursor.execute(texto_sql)
                    concluida_em = agora_utc()
                    duracao_ms = max(0, round((self.clock() - inicio) * 1000))
                    registrar_migration_aplicada(
                        cursor, operacao, tentativa=1, request_id=request_id,
                        iniciada_em=iniciada_em, concluida_em=concluida_em,
                        duracao_ms=duracao_ms,
                        manifesto_versao=self.manifesto.versao_formato,
                    )
                finally:
                    if cursor is not None:
                        cursor.close()
                estado.confirmar_migration()
                migration_iniciada = False
                estado.preparar_operacoes_sem_transacao()
                aplicadas.append(operacao.identificador)
                anterior_aplicada = operacao.identificador
                operacao_atual = None
                iniciada_em_atual = None
                inicio_atual = None
            resultado = RunnerResult(
                True, preflight.classificacao.value, tuple(aplicadas), (), 0,
                "Cadeia controlada aplicada com sucesso.", request_id_texto,
            )
        except Exception as erro:
            erro_principal = _erro_controlado(erro)
            if migration_iniciada:
                try:
                    if estado.status() != TRANSACTION_STATUS_IDLE:
                        estado.reverter_migration()
                except Exception as erro_rollback:
                    self._log_secundario(
                        "migration_rollback_falhou", erro_rollback, request_id_texto
                    )
            if operacao_atual is not None and iniciada_em_atual is not None and inicio_atual is not None:
                cursor = None
                try:
                    estado.preparar_operacoes_sem_transacao()
                    estado.iniciar_migration()
                    cursor = self.conexao.cursor()
                    concluida_em = agora_utc()
                    duracao_ms = max(0, round((self.clock() - inicio_atual) * 1000))
                    erro_seguro = sanitizar_erro(erro_principal)
                    registrar_migration_falhou(
                        cursor, operacao_atual, tentativa=1, request_id=request_id,
                        iniciada_em=iniciada_em_atual, concluida_em=concluida_em,
                        duracao_ms=duracao_ms, erro_codigo=erro_seguro["codigo"],
                        erro_sanitizado=erro_seguro["mensagem"],
                    )
                    cursor.close()
                    cursor = None
                    estado.confirmar_migration()
                    estado.preparar_operacoes_sem_transacao()
                except Exception as erro_registro:
                    if cursor is not None:
                        try:
                            cursor.close()
                        except Exception:
                            pass
                    try:
                        if estado.status() != TRANSACTION_STATUS_IDLE:
                            estado.reverter_migration()
                    except Exception:
                        pass
                    self._log_secundario(
                        "migration_falha_registro_falhou", erro_registro, request_id_texto
                    )
        finally:
            if lock.adquirido:
                try:
                    if estado.status() != TRANSACTION_STATUS_IDLE:
                        raise LockError()
                    estado.preparar_operacoes_sem_transacao()
                    lock.liberar()
                except Exception as erro_unlock:
                    convertido = _erro_controlado(erro_unlock)
                    self._log_secundario(
                        "migration_lock_liberacao_falhou", convertido, request_id_texto
                    )
                    if erro_principal is None:
                        erro_limpeza = convertido
            try:
                estado.restaurar()
            except Exception as erro_restauracao:
                convertido = _erro_controlado(erro_restauracao)
                self._log_secundario(
                    "migration_conexao_restauracao_falhou", convertido, request_id_texto
                )
                if erro_principal is None and erro_limpeza is None:
                    erro_limpeza = convertido

        erro_final = erro_principal or erro_limpeza
        if erro_final is not None:
            raise erro_final
        if resultado is None:
            raise MigrationExecutionError()
        return resultado


def executar_cadeia_controlada(conexao, **kwargs) -> RunnerResult:
    """Entrada explícita para a cadeia posterior; exige conexão fornecida."""
    return MigrationRunner(conexao, **kwargs).executar_cadeia_controlada()
