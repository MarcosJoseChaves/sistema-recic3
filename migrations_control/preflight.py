"""Inventário conservador e classificação do schema public."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from .errors import DatabaseConnectionError, InvalidLedgerError
from .models import (
    AppliedMigration,
    DatabaseClassification,
    ExecutionState,
    MigrationExecution,
    MigrationManifest,
    OperationType,
    PreflightResult,
    PreflightSnapshot,
)
from .schema_validation import (
    EXPECTED_LEDGER_SEQUENCES,
    coletar_assinatura_ledger,
    validar_assinatura_ledger,
)


LEDGER_TABLES = frozenset({"schema_migrations", "schema_migration_execucoes"})
LEDGER_SEQUENCE_OBJECTS = frozenset(
    f"relation:S:{nome}" for nome in EXPECTED_LEDGER_SEQUENCES
)
LEDGER_OBJECTS = LEDGER_TABLES | LEDGER_SEQUENCE_OBJECTS


def _resultado(
    snapshot: PreflightSnapshot,
    classificacao: DatabaseClassification,
    motivo: str,
    codigo: int,
    pode_prosseguir: bool,
) -> PreflightResult:
    return PreflightResult(
        classificacao,
        tuple(sorted(snapshot.objetos_encontrados)),
        tuple(sorted(snapshot.objetos_ignorados)),
        motivo,
        codigo,
        pode_prosseguir,
    )


def validar_conteudo_ledger(
    snapshot: PreflightSnapshot,
    manifesto: MigrationManifest,
) -> tuple[bool, str]:
    """Valida sucessos e tentativas sem aceitar estados não comprovados."""
    por_id = manifesto.por_id()
    aplicadas_por_id: dict[str, AppliedMigration] = {}
    for aplicada in snapshot.migrations_aplicadas:
        esperada = por_id.get(aplicada.migration_id)
        if aplicada.migration_id in aplicadas_por_id:
            return False, "Migration repetida em schema_migrations."
        if esperada is None or not esperada.habilitada or esperada.tipo is OperationType.EXECUTOR:
            return False, "Migration desconhecida, desabilitada ou não persistível no ledger."
        if (
            aplicada.ordem != esperada.ordem_global
            or aplicada.modulo != esperada.modulo
            or aplicada.checksum_sha256 != esperada.checksum
            or type(aplicada.versao) is not int
            or aplicada.versao <= 0
            or aplicada.manifesto_versao != manifesto.versao_formato
            or not isinstance(aplicada.aplicada_em, datetime)
            or type(aplicada.versao_aplicativo) is not int
            or aplicada.versao_aplicativo <= 0
            or (
                aplicada.duracao_ms is not None
                and (type(aplicada.duracao_ms) is not int or aplicada.duracao_ms < 0)
            )
        ):
            return False, "Registro de migration aplicada incompatível."
        aplicadas_por_id[aplicada.migration_id] = aplicada

    for aplicada_id in aplicadas_por_id:
        for dependencia in por_id[aplicada_id].dependencias:
            operacao_dependente = por_id[dependencia]
            if operacao_dependente.tipo is not OperationType.EXECUTOR and dependencia not in aplicadas_por_id:
                return False, "Dependência aplicada não satisfeita."

    tentativas_vistas: set[tuple[str, int]] = set()
    sucessos: dict[str, list[MigrationExecution]] = {}
    for execucao in snapshot.execucoes:
        esperada = por_id.get(execucao.migration_id)
        chave = (execucao.migration_id, execucao.tentativa)
        if chave in tentativas_vistas or type(execucao.tentativa) is not int or execucao.tentativa < 0:
            return False, "Tentativa duplicada ou inválida."
        tentativas_vistas.add(chave)
        if esperada is None or not esperada.habilitada or esperada.tipo is OperationType.EXECUTOR:
            return False, "Execução referencia migration desconhecida ou desabilitada."
        if (
            execucao.checksum_sha256 != esperada.checksum
            or not isinstance(execucao.iniciada_em, datetime)
            or not isinstance(execucao.request_id, UUID)
            or type(execucao.versao_aplicativo) is not int
            or execucao.versao_aplicativo <= 0
            or (
                execucao.processo_id is not None
                and (type(execucao.processo_id) is not int or execucao.processo_id <= 0)
            )
        ):
            return False, "Metadados da execução são incompatíveis."
        try:
            estado = ExecutionState(execucao.situacao)
        except ValueError:
            return False, "Estado de execução desconhecido."
        if estado is ExecutionState.INICIADA:
            if any(valor is not None for valor in (
                execucao.concluida_em, execucao.duracao_ms,
                execucao.erro_codigo, execucao.erro_sanitizado,
            )):
                return False, "Execução INICIADA possui campos de conclusão."
        else:
            if (
                not isinstance(execucao.concluida_em, datetime)
                or type(execucao.duracao_ms) is not int
                or execucao.duracao_ms < 0
                or execucao.concluida_em < execucao.iniciada_em
            ):
                return False, "Execução concluída sem término ou duração válida."
            if estado in {ExecutionState.APLICADA, ExecutionState.ADOTADA}:
                if execucao.erro_codigo is not None or execucao.erro_sanitizado is not None:
                    return False, "Execução terminal de sucesso contém erro."
                if estado is ExecutionState.ADOTADA and (
                    execucao.migration_id == "M0001"
                    or execucao.tentativa != 0
                    or execucao.duracao_ms != 0
                ):
                    return False, "Execução ADOTADA incompatível."
                if estado is ExecutionState.APLICADA and (
                    (execucao.migration_id == "M0001" and execucao.tentativa != 0)
                    or (execucao.migration_id != "M0001" and execucao.tentativa <= 0)
                ):
                    return False, "Tentativa APLICADA incompatível."
                sucessos.setdefault(execucao.migration_id, []).append(execucao)
            elif not execucao.erro_codigo or not execucao.erro_sanitizado:
                return False, "Execução FALHOU sem erro sanitizado."

    if any(len(itens) != 1 for itens in sucessos.values()):
        return False, "Existe mais de um sucesso terminal para a mesma migration."
    if set(sucessos) != set(aplicadas_por_id):
        return False, "Sucessos e registros aplicados não correspondem."
    for migration_id, aplicada in aplicadas_por_id.items():
        sucesso = sucessos[migration_id][0]
        if (
            sucesso.concluida_em != aplicada.aplicada_em
            or sucesso.duracao_ms != aplicada.duracao_ms
        ):
            return False, "Fotografia de sucesso diverge do registro aplicado."
    persistiveis = tuple(
        op.identificador for op in manifesto.operacoes
        if op.habilitada and op.tipo is not OperationType.EXECUTOR
    )
    ids_aplicados = tuple(
        op.identificador for op in manifesto.operacoes
        if op.identificador in aplicadas_por_id
    )
    if ids_aplicados != persistiveis[:len(ids_aplicados)]:
        return False, "O ledger não representa prefixo dependency-closed da cadeia."
    if not ids_aplicados or ids_aplicados[0] != "M0001":
        return False, "O ledger não comprova a M0001 inicial."
    execucoes_m0001 = [item for item in snapshot.execucoes if item.migration_id == "M0001"]
    if not (
        len(execucoes_m0001) == 1
        and execucoes_m0001[0].tentativa == 0
        and ExecutionState(execucoes_m0001[0].situacao) is ExecutionState.APLICADA
        and len(sucessos.get("M0001", ())) == 1
    ):
        return False, "O autorregistro inicial da M0001 é incompatível."
    return True, "Conteúdo integral do ledger confirmado."


def classificar_preflight(
    snapshot: PreflightSnapshot,
    manifesto: MigrationManifest,
) -> PreflightResult:
    """Classifica o banco sem permitir estado presumido."""
    if not snapshot.public_existe:
        return _resultado(
            snapshot, DatabaseClassification.BANCO_DESCONHECIDO,
            "O schema public não existe.", 20, False,
        )
    encontrados = snapshot.objetos_encontrados - snapshot.objetos_ignorados
    presentes = encontrados & LEDGER_TABLES
    if snapshot.erro_ledger or (presentes and presentes != LEDGER_TABLES):
        return _resultado(
            snapshot, DatabaseClassification.BANCO_DESCONHECIDO,
            "Ledger parcial ou ilegível.", 21, False,
        )
    if not presentes:
        if encontrados:
            return _resultado(
                snapshot, DatabaseClassification.BANCO_DESCONHECIDO,
                "Existem objetos no public sem ledger válido.", 22, False,
            )
        return _resultado(
            snapshot, DatabaseClassification.BANCO_NOVO,
            "Schema public existente, vazio e sem ledger.", 0, True,
        )
    sequencias_presentes = encontrados & LEDGER_SEQUENCE_OBJECTS
    if sequencias_presentes != LEDGER_SEQUENCE_OBJECTS:
        return _resultado(
            snapshot, DatabaseClassification.BANCO_DESCONHECIDO,
            "As sequências IDENTITY do ledger estão ausentes ou incompletas.", 26, False,
        )
    total_persistivel = sum(
        op.habilitada and op.tipo is not OperationType.EXECUTOR
        for op in manifesto.operacoes
    )
    if (
        encontrados - LEDGER_OBJECTS
        and len(snapshot.migrations_aplicadas) != total_persistivel
    ):
        return _resultado(
            snapshot, DatabaseClassification.BANCO_DESCONHECIDO,
            "Existem objetos não reconhecidos além do ledger inicial.", 23, False,
        )
    estrutura_valida, motivo = validar_assinatura_ledger(snapshot.assinatura_ledger)
    if not estrutura_valida:
        return _resultado(
            snapshot, DatabaseClassification.BANCO_DESCONHECIDO, motivo, 24, False
        )
    conteudo_valido, motivo = validar_conteudo_ledger(snapshot, manifesto)
    if not conteudo_valido:
        return _resultado(
            snapshot, DatabaseClassification.BANCO_DESCONHECIDO, motivo, 25, False
        )
    return _resultado(
        snapshot, DatabaseClassification.BANCO_CONTROLADO,
        "Estrutura e conteúdo do ledger são integralmente compatíveis.", 0, True,
    )


def _select(conexao, sql: str, parametros: tuple = ()):
    cursor = None
    erro_principal: BaseException | None = None
    try:
        cursor = conexao.cursor()
        cursor.execute(sql, parametros)
        return cursor.fetchall()
    except Exception as erro:
        erro_principal = erro
        raise DatabaseConnectionError() from erro
    finally:
        if cursor is not None:
            try:
                cursor.close()
            except Exception as erro:
                if erro_principal is None:
                    raise DatabaseConnectionError() from erro
                erro_principal.add_note("Falha secundária ao fechar cursor do preflight.")


def _coletar_objetos(conexao) -> frozenset[str]:
    objetos: set[str] = set()
    relacoes = _select(
        conexao,
        "SELECT c.relkind, c.relname FROM pg_catalog.pg_class AS c "
        "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relkind IN ('r', 'p', 'v', 'm', 'S', 'f')",
        ("public",),
    )
    for relkind, nome in relacoes:
        objetos.add(nome if relkind in {"r", "p"} else f"relation:{relkind}:{nome}")
    for tipo, nome, argumentos in _select(
        conexao,
        "SELECT p.prokind, p.proname, pg_catalog.pg_get_function_identity_arguments(p.oid) "
        "FROM pg_catalog.pg_proc AS p "
        "JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace "
        "WHERE n.nspname = %s",
        ("public",),
    ):
        objetos.add(f"routine:{tipo}:{nome}({argumentos})")
    for tipo, nome in _select(
        conexao,
        "SELECT t.typtype, t.typname FROM pg_catalog.pg_type AS t "
        "JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace "
        "LEFT JOIN pg_catalog.pg_class AS c ON c.oid = t.typrelid "
        "WHERE n.nspname = %s AND t.typisdefined "
        "AND NOT (t.typelem <> 0 AND t.typcategory = 'A') "
        "AND (t.typtype IN ('e', 'd', 'r', 'm') "
        "OR (t.typtype = 'c' AND c.relkind = 'c') "
        "OR (t.typtype = 'b' AND t.typrelid = 0))",
        ("public",),
    ):
        objetos.add(f"type:{tipo}:{nome}")
    for nome, tabela in _select(
        conexao,
        "SELECT t.tgname, c.relname FROM pg_catalog.pg_trigger AS t "
        "JOIN pg_catalog.pg_class AS c ON c.oid = t.tgrelid "
        "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND NOT t.tgisinternal",
        ("public",),
    ):
        objetos.add(f"trigger:{tabela}:{nome}")
    for nome, tabela in _select(
        conexao,
        "SELECT p.polname, c.relname FROM pg_catalog.pg_policy AS p "
        "JOIN pg_catalog.pg_class AS c ON c.oid = p.polrelid "
        "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s",
        ("public",),
    ):
        objetos.add(f"policy:{tabela}:{nome}")
    for nome, tabela in _select(
        conexao,
        "SELECT r.rulename, c.relname FROM pg_catalog.pg_rewrite AS r "
        "JOIN pg_catalog.pg_class AS c ON c.oid = r.ev_class "
        "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND r.rulename <> %s",
        ("public", "_RETURN"),
    ):
        objetos.add(f"rule:{tabela}:{nome}")
    for extensao, classe, objeto in _select(
        conexao,
        "SELECT e.extname, d.classid::pg_catalog.regclass::text, d.objid "
        "FROM pg_catalog.pg_depend AS d "
        "JOIN pg_catalog.pg_extension AS e ON e.oid = d.refobjid "
        "WHERE d.deptype = %s AND e.extnamespace = "
        "(SELECT n.oid FROM pg_catalog.pg_namespace AS n WHERE n.nspname = %s)",
        ("e", "public"),
    ):
        objetos.add(f"extension_object:{extensao}:{classe}:{objeto}")
    return frozenset(objetos)


def coletar_conteudo_ledger(conexao) -> tuple[tuple[AppliedMigration, ...], tuple[MigrationExecution, ...]]:
    """Lê as duas tabelas qualificadas do ledger em uma transação existente."""
    aplicadas = tuple(
        AppliedMigration(*linha)
        for linha in _select(
            conexao,
            "SELECT migration_id, ordem, modulo, checksum_sha256, versao, "
            "manifesto_versao, aplicada_em, duracao_ms, versao_aplicativo "
            "FROM public.schema_migrations ORDER BY ordem",
        )
    )
    linhas_execucoes = _select(
            conexao,
            "SELECT migration_id, tentativa, situacao, iniciada_em, concluida_em, "
            "duracao_ms, checksum_sha256, erro_codigo, erro_sanitizado, request_id, "
            "host_identificador, processo_id, versao_aplicativo "
            "FROM public.schema_migration_execucoes ORDER BY migration_id, tentativa",
        )
    execucoes = []
    for linha in linhas_execucoes:
        try:
            request_id = linha[9] if type(linha[9]) is UUID else UUID(linha[9])
        except (IndexError, TypeError, ValueError, AttributeError):
            raise InvalidLedgerError() from None
        execucoes.append(MigrationExecution(*linha[:9], request_id, *linha[10:]))
    return aplicadas, tuple(execucoes)


def coletar_snapshot(conexao) -> PreflightSnapshot:
    """Coleta inventário, assinatura e conteúdo usando somente SELECTs."""
    existe = _select(
        conexao,
        "SELECT EXISTS(SELECT 1 FROM pg_catalog.pg_namespace AS n "
        "WHERE n.nspname = %s)",
        ("public",),
    )
    public_existe = bool(existe and existe[0][0])
    if not public_existe:
        return PreflightSnapshot(False, frozenset())
    objetos = _coletar_objetos(conexao)
    presentes = objetos & LEDGER_TABLES
    if presentes != LEDGER_TABLES:
        return PreflightSnapshot(True, objetos)
    assinatura = coletar_assinatura_ledger(conexao)
    estrutura_valida, _ = validar_assinatura_ledger(assinatura)
    if not estrutura_valida:
        return PreflightSnapshot(True, objetos, assinatura_ledger=assinatura)
    aplicadas, execucoes = coletar_conteudo_ledger(conexao)
    return PreflightSnapshot(
        True, objetos, assinatura_ledger=assinatura,
        migrations_aplicadas=aplicadas, execucoes=execucoes,
    )
