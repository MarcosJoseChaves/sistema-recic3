"""Assinatura física normativa e validação integral do ledger."""

from __future__ import annotations

import re
from dataclasses import dataclass
from types import MappingProxyType

from .errors import DatabaseConnectionError


@dataclass(frozen=True, order=True)
class ColumnSignature:
    posicao: int
    nome: str
    tipo: str
    tamanho: int | None
    precisao: int | None
    escala: int | None
    nullable: bool
    default: tuple[str, ...] | None
    identity: str
    generation: str


@dataclass(frozen=True, order=True)
class ConstraintSignature:
    nome: str
    tipo: str
    conkey: tuple[int, ...] | None
    colunas: tuple[str, ...]
    definicao: tuple[str, ...]
    deferrable: bool
    initially_deferred: bool
    validated: bool
    local: bool
    inheritance_count: int
    no_inherit: bool

    def __post_init__(self) -> None:
        if (
            type(self.nome) is not str
            or not self.nome
            or type(self.tipo) is not str
            or not self.tipo
            or (
                self.conkey is not None
                and (
                    type(self.conkey) is not tuple
                    or any(type(item) is not int or item <= 0 for item in self.conkey)
                    or len(set(self.conkey)) != len(self.conkey)
                )
            )
            or type(self.colunas) is not tuple
            or any(type(item) is not str or not item for item in self.colunas)
            or len(set(self.colunas)) != len(self.colunas)
            or (self.conkey is None and self.colunas)
            or (self.conkey is not None and len(self.conkey) != len(self.colunas))
            or type(self.definicao) is not tuple
            or any(type(item) is not str for item in self.definicao)
        ):
            raise ValueError("Assinatura de constraint inválida.")
        if any(type(valor) is not bool for valor in (
            self.deferrable, self.initially_deferred, self.validated,
            self.local, self.no_inherit,
        )):
            raise ValueError("Propriedade booleana de constraint inválida.")
        if type(self.inheritance_count) is not int or self.inheritance_count < 0:
            raise ValueError("Estado de herança de constraint inválido.")


@dataclass(frozen=True, order=True, slots=True)
class QualifiedName:
    schema: str
    nome: str

    def __post_init__(self) -> None:
        if (
            type(self.schema) is not str
            or type(self.nome) is not str
            or not self.schema
            or not self.nome
        ):
            raise ValueError("Nome físico qualificado inválido.")


@dataclass(frozen=True, order=True)
class IndexSignature:
    schema: str
    tabela_schema: str
    nome: str
    tabela: str
    metodo: str
    unique: bool
    primary: bool
    exclusion: bool
    immediate: bool
    check_xmin: bool
    valid: bool
    ready: bool
    live: bool
    clustered: bool
    replica_identity: bool
    nulls_not_distinct: bool
    numero_colunas_chave: int
    numero_atributos: int
    colunas_chave: tuple[str, ...]
    colunas_include: tuple[str, ...]
    expressoes: tuple[str, ...] | None
    predicado: tuple[str, ...] | None
    vinculado_constraint: bool
    collations: tuple[QualifiedName | None, ...]
    operator_classes: tuple[QualifiedName, ...]
    direcoes: tuple[str, ...]
    nulls: tuple[str, ...]
    indoptions: tuple[int, ...]
    definicao: tuple[str, ...]

    def __post_init__(self) -> None:
        booleanos = (
            self.unique, self.primary, self.exclusion, self.immediate,
            self.check_xmin,
            self.valid, self.ready, self.live, self.clustered,
            self.replica_identity, self.nulls_not_distinct,
            self.vinculado_constraint,
        )
        if any(type(valor) is not bool for valor in booleanos):
            raise ValueError("Propriedade booleana de índice inválida.")
        if (
            type(self.numero_colunas_chave) is not int
            or type(self.numero_atributos) is not int
            or self.numero_colunas_chave < 0
            or self.numero_atributos < self.numero_colunas_chave
        ):
            raise ValueError("Quantidade de atributos do índice inválida.")
        quantidade = self.numero_colunas_chave
        if (
            len(self.colunas_chave) != quantidade
            or len(self.colunas_include) != self.numero_atributos - quantidade
            or len(self.collations) != quantidade
            or len(self.operator_classes) != quantidade
            or len(self.direcoes) != quantidade
            or len(self.nulls) != quantidade
            or len(self.indoptions) != quantidade
        ):
            raise ValueError("Assinatura posicional do índice inválida.")
        if any(type(item) is not QualifiedName for item in self.operator_classes):
            raise ValueError("Operator class sem identidade estruturada.")
        if any(item is not None and type(item) is not QualifiedName for item in self.collations):
            raise ValueError("Collation sem identidade estruturada.")
        for direcao, nulos, opcao in zip(self.direcoes, self.nulls, self.indoptions):
            if direcao not in {"ASC", "DESC"} or nulos not in {"FIRST", "LAST"}:
                raise ValueError("Direção ou tratamento de NULL inválido.")
            if type(opcao) is not int or opcao not in {0, 1, 2, 3}:
                raise ValueError("indoption inválido.")
            esperado = (1 if direcao == "DESC" else 0) | (2 if nulos == "FIRST" else 0)
            if opcao != esperado:
                raise ValueError("indoption diverge de direção e NULLS.")


@dataclass(frozen=True, order=True)
class SequenceSignature:
    schema: str
    nome: str
    relkind: str
    tabela_schema: str | None
    tabela: str | None
    coluna: str | None
    numero_coluna: int | None
    tipo_coluna: str | None
    identity: str | None
    tipo_dependencia: str | None
    tipo_sequencia: str
    inicio: int
    incremento: int
    minimo: int
    maximo: int
    cache: int
    cycle: bool


@dataclass(frozen=True)
class TableSignature:
    schema: str
    nome: str
    relkind: str
    colunas: tuple[ColumnSignature, ...]
    constraints: tuple[ConstraintSignature, ...]
    indices: tuple[IndexSignature, ...]


@dataclass(frozen=True)
class LedgerSchemaSnapshot:
    tabelas: tuple[TableSignature, ...]
    sequencias: tuple[SequenceSignature, ...] = ()


_IDENTIFICADOR = re.compile(r"[A-Za-z_\x80-\uffff][A-Za-z0-9_$\x80-\uffff]*")
_NUMERO = re.compile(r"(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?")
_DOLLAR_TAG = re.compile(r"\$(?:[A-Za-z_][A-Za-z0-9_]*)?\$")
_OPERADORES = frozenset({
    "::", ">=", "<=", "<>", "!=", "||", "&&", "->", "->>", "#>", "#>>",
    "@>", "<@", "?&", "?|", "<<", ">>", "~*", "!~", "!~*", "^@", ":=",
})
_PONTUACAO = frozenset("(),.[];")
_CARACTERES_OPERADOR = frozenset("+-*/%<>=~!@#^&|?:")


def tokenizar_sql(valor: str) -> tuple[str, ...]:
    """Tokeniza conservadoramente sem unir palavras nem alterar literais."""
    tokens: list[str] = []
    indice = 0
    while indice < len(valor):
        caractere = valor[indice]
        if caractere.isspace():
            indice += 1
            continue
        if valor.startswith("--", indice) or valor.startswith("/*", indice):
            raise ValueError("Comentários SQL não são aceitos na assinatura.")
        if caractere == "'":
            inicio = indice
            indice += 1
            while indice < len(valor):
                if valor[indice] == "'":
                    if indice + 1 < len(valor) and valor[indice + 1] == "'":
                        indice += 2
                        continue
                    indice += 1
                    break
                indice += 1
            else:
                raise ValueError("Literal SQL não terminado.")
            tokens.append(valor[inicio:indice])
            continue
        if caractere == '"':
            inicio = indice
            indice += 1
            while indice < len(valor):
                if valor[indice] == '"':
                    if indice + 1 < len(valor) and valor[indice + 1] == '"':
                        indice += 2
                        continue
                    indice += 1
                    break
                indice += 1
            else:
                raise ValueError("Identificador SQL não terminado.")
            tokens.append(valor[inicio:indice])
            continue
        if caractere == "$":
            tag = _DOLLAR_TAG.match(valor, indice)
            if tag:
                delimitador = tag.group(0)
                fim = valor.find(delimitador, tag.end())
                if fim < 0:
                    raise ValueError("Literal dollar-quoted não terminado.")
                fim += len(delimitador)
                tokens.append(valor[indice:fim])
                indice = fim
                continue
        identificador = _IDENTIFICADOR.match(valor, indice)
        if identificador:
            tokens.append(identificador.group(0).lower())
            indice = identificador.end()
            continue
        numero = _NUMERO.match(valor, indice)
        if numero:
            tokens.append(numero.group(0).lower())
            indice = numero.end()
            continue
        operador = next(
            (op for op in sorted(_OPERADORES, key=len, reverse=True) if valor.startswith(op, indice)),
            None,
        )
        if operador:
            tokens.append(operador)
            indice += len(operador)
            continue
        if caractere in _PONTUACAO or caractere in _CARACTERES_OPERADOR:
            if caractere in _PONTUACAO:
                tokens.append(caractere)
                indice += 1
                continue
            fim = indice + 1
            while fim < len(valor) and valor[fim] in _CARACTERES_OPERADOR:
                fim += 1
            tokens.append(valor[indice:fim])
            indice = fim
            continue
        raise ValueError("Construção SQL não suportada na assinatura.")
    return tuple(tokens)


def canonicalizar_sql(valor: str | None) -> tuple[str, ...] | None:
    """Compara tokens sem perder limites, operadores ou conteúdo delimitado."""
    if valor is None:
        return None
    return tokenizar_sql(valor)


_FUNCOES_CHECK_QUALIFICADAS_EQUIVALENTES = frozenset({
    ("pg_catalog", "btrim"),
})


def _normalizar_funcoes_check(tokens: tuple[str, ...]) -> tuple[str, ...]:
    """Normaliza somente pg_catalog.btrim comprovado no deparser PG 15.18.

    A exceção é fechada, exige uma chamada de função e é usada apenas na
    expressão de CHECK. Argumentos, ordem, casts e todos os demais tokens
    permanecem na comparação final.
    """
    resultado: list[str] = []
    indice = 0
    while indice < len(tokens):
        if (
            indice + 3 < len(tokens)
            and (tokens[indice], tokens[indice + 2])
            in _FUNCOES_CHECK_QUALIFICADAS_EQUIVALENTES
            and tokens[indice + 1] == "."
            and tokens[indice + 3] == "("
        ):
            resultado.append(tokens[indice + 2])
            indice += 3
            continue
        resultado.append(tokens[indice])
        indice += 1
    return tuple(resultado)


def _remover_parenteses_externos(tokens: tuple[str, ...]) -> tuple[str, ...]:
    """Remove apenas pares externos que envolvem a expressão inteira."""
    resultado = tokens
    while resultado and resultado[0] == "(":
        profundidade = 0
        fecha_externo = None
        for indice, token in enumerate(resultado):
            if token == "(":
                profundidade += 1
            elif token == ")":
                profundidade -= 1
                if profundidade < 0:
                    raise ValueError("Parênteses desbalanceados na expressão SQL.")
                if profundidade == 0:
                    fecha_externo = indice
                    break
        if fecha_externo is None:
            raise ValueError("Parênteses desbalanceados na expressão SQL.")
        if fecha_externo != len(resultado) - 1:
            break
        resultado = resultado[1:-1]
    profundidade = 0
    for token in resultado:
        if token == "(":
            profundidade += 1
        elif token == ")":
            profundidade -= 1
            if profundidade < 0:
                raise ValueError("Parênteses desbalanceados na expressão SQL.")
    if profundidade:
        raise ValueError("Parênteses desbalanceados na expressão SQL.")
    return resultado


def canonicalizar_constraintdef(valor: str | None, tipo: str) -> tuple[str, ...]:
    """Normaliza somente o invólucro e parênteses externos redundantes de CHECK."""
    tokens = canonicalizar_sql(valor)
    if not tokens:
        return ()
    if tipo != "c":
        return tokens
    if len(tokens) < 3 or tokens[0] != "check" or tokens[1] != "(":
        raise ValueError("Definição CHECK inválida.")
    conteudo = _remover_parenteses_externos(tokens[1:])
    conteudo = _remover_parenteses_externos(conteudo)
    conteudo = _normalizar_funcoes_check(conteudo)
    if not conteudo:
        raise ValueError("Expressão CHECK ausente.")
    return ("check", "(", *conteudo, ")")


def normalizar_conkey_constraint(attnums, nomes) -> tuple[int, ...] | None:
    """Preserva conkey nulo/vazio e valida sua resolução sem ambiguidades."""
    if attnums is None:
        if nomes is None or nomes in ((), []):
            return None
        raise ValueError("Constraint sem conkey possui nomes de colunas.")
    if type(attnums) not in (tuple, list) or type(nomes) not in (tuple, list):
        raise ValueError("conkey ou nomes de colunas possuem tipo inválido.")
    if len(attnums) != len(nomes):
        raise ValueError("conkey e nomes de colunas possuem tamanhos diferentes.")
    attnums_tupla = tuple(attnums)
    nomes_tupla = tuple(nomes)
    if any(type(item) is not int or type(item) is bool or item <= 0 for item in attnums_tupla):
        raise ValueError("conkey possui attnum inválido.")
    if len(set(attnums_tupla)) != len(attnums_tupla):
        raise ValueError("conkey possui attnum duplicado.")
    if any(type(item) is not str or not item for item in nomes_tupla):
        raise ValueError("conkey referencia coluna ausente ou removida.")
    if len(set(nomes_tupla)) != len(nomes_tupla):
        raise ValueError("conkey resolve para coluna duplicada.")
    return attnums_tupla


def normalizar_colunas_constraint(attnums, nomes) -> tuple[str, ...]:
    """Converte conkey em nomes sem perder ordem nem aceitar catálogo ambíguo."""
    normalizar_conkey_constraint(attnums, nomes)
    if attnums is None:
        return ()
    nomes_tupla = tuple(nomes)
    return nomes_tupla


def _ler_nome_indexdef(
    tokens: tuple[str, ...], posicao: int, schema: str, nome: str,
) -> tuple[int, tuple[str, ...]] | None:
    if posicao < len(tokens) and tokens[posicao] == nome:
        return posicao + 1, (nome,)
    if (
        posicao + 2 < len(tokens)
        and tokens[posicao] == schema
        and tokens[posicao + 1] == "."
        and tokens[posicao + 2] == nome
    ):
        return posicao + 3, (nome,)
    return None


def canonicalizar_indexdef(
    valor: str | None,
    *,
    indice_schema: str,
    indice_nome: str,
    tabela_schema: str,
    tabela_nome: str,
) -> tuple[str, ...]:
    """Aceita omissão textual apenas de public já comprovado estruturalmente."""
    tokens = canonicalizar_sql(valor)
    if not tokens:
        return ()
    if indice_schema != "public" or tabela_schema != "public":
        return tokens
    try:
        posicao_index = tokens.index("index") + 1
    except ValueError:
        return tokens
    indice = _ler_nome_indexdef(tokens, posicao_index, indice_schema, indice_nome)
    if indice is None:
        return tokens
    depois_indice, indice_normalizado = indice
    if depois_indice >= len(tokens) or tokens[depois_indice] != "on":
        return tokens
    posicao_tabela = depois_indice + 1
    tabela = _ler_nome_indexdef(tokens, posicao_tabela, tabela_schema, tabela_nome)
    if tabela is None:
        return tokens
    depois_tabela, tabela_normalizada = tabela
    return (
        *tokens[:posicao_index], *indice_normalizado,
        "on", *tabela_normalizada, *tokens[depois_tabela:],
    )


def _col(
    posicao: int,
    nome: str,
    tipo: str,
    *,
    nullable: bool = False,
    default: str | None = None,
    identity: str = "",
    tamanho: int | None = None,
) -> ColumnSignature:
    return ColumnSignature(
        posicao, nome, tipo, tamanho, None, None, nullable,
        canonicalizar_sql(default), identity, "",
    )


def _constraint(
    nome: str,
    tipo: str,
    conkey: tuple[int, ...],
    colunas: tuple[str, ...],
    definicao: str,
) -> ConstraintSignature:
    return ConstraintSignature(
        nome, tipo, conkey, colunas, canonicalizar_constraintdef(definicao, tipo),
        False, False, True, True, 0, tipo in {"p", "u"},
    )


def _index(
    nome: str,
    tabela: str,
    colunas: tuple[str, ...],
    *,
    unique: bool,
    vinculado: bool,
    operator_classes: tuple[QualifiedName, ...],
    collations: tuple[QualifiedName | None, ...],
    definicao: str,
    primary: bool = False,
) -> IndexSignature:
    quantidade = len(colunas)
    return IndexSignature(
        "public", "public", nome, tabela, "btree", unique, primary, False,
        True, False, True, True, True, False, False, False,
        quantidade, quantidade, colunas, (), None, None,
        vinculado, collations, operator_classes, ("ASC",) * quantidade,
        ("LAST",) * quantidade, (0,) * quantidade,
        canonicalizar_indexdef(
            definicao, indice_schema="public", indice_nome=nome,
            tabela_schema="public", tabela_nome=tabela,
        ),
    )


SCHEMA_MIGRATIONS = TableSignature(
    "public",
    "schema_migrations",
    "r",
    (
        _col(1, "id", "bigint", identity="d"),
        _col(2, "migration_id", "text"),
        _col(3, "modulo", "text"),
        _col(4, "versao", "integer", default="1"),
        _col(5, "ordem", "integer"),
        _col(6, "checksum_sha256", "character varying(64)", tamanho=64),
        _col(7, "aplicada_em", "timestamp with time zone"),
        _col(8, "duracao_ms", "bigint", nullable=True),
        _col(9, "versao_aplicativo", "integer"),
        _col(10, "manifesto_versao", "integer", default="1"),
    ),
    tuple(sorted((
        _constraint("pk_schema_migrations", "p", (1,), ("id",), "PRIMARY KEY (id)"),
        _constraint(
            "uq_schema_migrations__migration_id", "u", (2,), ("migration_id",),
            "UNIQUE (migration_id)",
        ),
        _constraint(
            "ck_schema_migrations__migration_id_preenchido", "c", (2,), ("migration_id",),
            "CHECK ((pg_catalog.btrim(migration_id) <> ''::text))",
        ),
        _constraint(
            "ck_schema_migrations__modulo_preenchido", "c", (3,), ("modulo",),
            "CHECK ((pg_catalog.btrim(modulo) <> ''::text))",
        ),
        _constraint(
            "ck_schema_migrations__versao_positivo", "c", (4,), ("versao",),
            "CHECK ((versao > 0))",
        ),
        _constraint(
            "ck_schema_migrations__ordem_positivo", "c", (5,), ("ordem",),
            "CHECK ((ordem > 0))",
        ),
        _constraint(
            "ck_schema_migrations__manifesto_versao_positivo", "c", (10,), ("manifesto_versao",),
            "CHECK ((manifesto_versao > 0))",
        ),
    ))),
    tuple(sorted((
        _index(
            "pk_schema_migrations", "schema_migrations", ("id",),
            unique=True, vinculado=True, primary=True,
            operator_classes=(QualifiedName("pg_catalog", "int8_ops"),), collations=(None,),
            definicao="CREATE UNIQUE INDEX pk_schema_migrations ON public.schema_migrations USING btree (id)",
        ),
        _index(
            "uq_schema_migrations__migration_id", "schema_migrations",
            ("migration_id",), unique=True, vinculado=True,
            operator_classes=(QualifiedName("pg_catalog", "text_ops"),),
            collations=(QualifiedName("pg_catalog", "default"),),
            definicao="CREATE UNIQUE INDEX uq_schema_migrations__migration_id ON public.schema_migrations USING btree (migration_id)",
        ),
    ))),
)


SCHEMA_MIGRATION_EXECUCOES = TableSignature(
    "public",
    "schema_migration_execucoes",
    "r",
    (
        _col(1, "id", "bigint", identity="d"),
        _col(2, "migration_id", "text"),
        _col(3, "tentativa", "integer"),
        _col(4, "situacao", "text"),
        _col(5, "iniciada_em", "timestamp with time zone", default="CURRENT_TIMESTAMP"),
        _col(6, "concluida_em", "timestamp with time zone", nullable=True),
        _col(7, "duracao_ms", "bigint", nullable=True),
        _col(8, "checksum_sha256", "character varying(64)", tamanho=64),
        _col(9, "erro_codigo", "text", nullable=True),
        _col(10, "erro_sanitizado", "text", nullable=True),
        _col(11, "request_id", "uuid"),
        _col(12, "host_identificador", "text", nullable=True),
        _col(13, "processo_id", "integer", nullable=True),
        _col(14, "versao_aplicativo", "integer"),
    ),
    tuple(sorted((
        _constraint("pk_schema_migration_exec", "p", (1,), ("id",), "PRIMARY KEY (id)"),
        _constraint(
            "uq_schema_migration_exec__migration_id_tentativa", "u",
            (2, 3), ("migration_id", "tentativa"), "UNIQUE (migration_id, tentativa)",
        ),
        _constraint(
            "ck_schema_migration_exec__migration_id_preenchido", "c", (2,), ("migration_id",),
            "CHECK ((pg_catalog.btrim(migration_id) <> ''::text))",
        ),
        _constraint(
            "ck_schema_migration_exec__tentativa_positivo", "c", (3,), ("tentativa",),
            "CHECK ((tentativa >= 0))",
        ),
        _constraint(
            "ck_schema_migration_exec__situacao_preenchido", "c", (4,), ("situacao",),
            "CHECK ((pg_catalog.btrim(situacao) <> ''::text))",
        ),
    ))),
    tuple(sorted((
        _index(
            "pk_schema_migration_exec", "schema_migration_execucoes",
            ("id",), unique=True, vinculado=True, primary=True,
            operator_classes=(QualifiedName("pg_catalog", "int8_ops"),), collations=(None,),
            definicao="CREATE UNIQUE INDEX pk_schema_migration_exec ON public.schema_migration_execucoes USING btree (id)",
        ),
        _index(
            "uq_schema_migration_exec__migration_id_tentativa",
            "schema_migration_execucoes", ("migration_id", "tentativa"),
            unique=True, vinculado=True,
            operator_classes=(
                QualifiedName("pg_catalog", "text_ops"),
                QualifiedName("pg_catalog", "int4_ops"),
            ),
            collations=(QualifiedName("pg_catalog", "default"), None),
            definicao="CREATE UNIQUE INDEX uq_schema_migration_exec__migration_id_tentativa ON public.schema_migration_execucoes USING btree (migration_id, tentativa)",
        ),
        _index(
            "ix_schema_migration_exec__migration_id_iniciada_em",
            "schema_migration_execucoes", ("migration_id", "iniciada_em"),
            unique=False, vinculado=False,
            operator_classes=(
                QualifiedName("pg_catalog", "text_ops"),
                QualifiedName("pg_catalog", "timestamptz_ops"),
            ),
            collations=(QualifiedName("pg_catalog", "default"), None),
            definicao="CREATE INDEX ix_schema_migration_exec__migration_id_iniciada_em ON public.schema_migration_execucoes USING btree (migration_id, iniciada_em)",
        ),
        _index(
            "ix_schema_migration_exec__request_id", "schema_migration_execucoes",
            ("request_id",), unique=False, vinculado=False,
            operator_classes=(QualifiedName("pg_catalog", "uuid_ops"),), collations=(None,),
            definicao="CREATE INDEX ix_schema_migration_exec__request_id ON public.schema_migration_execucoes USING btree (request_id)",
        ),
    ))),
)


SCHEMA_MIGRATIONS_ID_SEQ = SequenceSignature(
    "public", "schema_migrations_id_seq", "S", "public", "schema_migrations",
    "id", 1, "bigint", "d", "i", "bigint", 1, 1, 1,
    9223372036854775807, 1, False,
)
SCHEMA_MIGRATION_EXECUCOES_ID_SEQ = SequenceSignature(
    "public", "schema_migration_execucoes_id_seq", "S", "public",
    "schema_migration_execucoes", "id", 1, "bigint", "d", "i", "bigint",
    1, 1, 1, 9223372036854775807, 1, False,
)


EXPECTED_LEDGER_TABLES = MappingProxyType({
    SCHEMA_MIGRATIONS.nome: SCHEMA_MIGRATIONS,
    SCHEMA_MIGRATION_EXECUCOES.nome: SCHEMA_MIGRATION_EXECUCOES,
})
EXPECTED_LEDGER_SEQUENCES = MappingProxyType({
    SCHEMA_MIGRATIONS_ID_SEQ.nome: SCHEMA_MIGRATIONS_ID_SEQ,
    SCHEMA_MIGRATION_EXECUCOES_ID_SEQ.nome: SCHEMA_MIGRATION_EXECUCOES_ID_SEQ,
})
EXPECTED_LEDGER_SCHEMA = LedgerSchemaSnapshot(
    tuple(sorted(EXPECTED_LEDGER_TABLES.values(), key=lambda tabela: tabela.nome)),
    tuple(sorted(EXPECTED_LEDGER_SEQUENCES.values(), key=lambda sequencia: sequencia.nome)),
)


def validar_assinatura_ledger(snapshot: LedgerSchemaSnapshot | None) -> tuple[bool, str]:
    """Compara toda a assinatura, inclusive ausências e nomes físicos."""
    if snapshot is None:
        return False, "Assinatura física do ledger ausente."
    if snapshot != EXPECTED_LEDGER_SCHEMA:
        return False, "A assinatura física do ledger diverge do catálogo aprovado."
    nomes = [tabela.nome for tabela in snapshot.tabelas]
    nomes.extend(sequencia.nome for sequencia in snapshot.sequencias)
    nomes.extend(
        item.nome
        for tabela in snapshot.tabelas
        for item in (*tabela.constraints, *tabela.indices)
    )
    if any(len(nome.encode("utf-8")) > 63 for nome in nomes):
        return False, "Nome físico do ledger excede o limite do PostgreSQL."
    return True, "Assinatura física integral do ledger confirmada."


def _cursor(conexao):
    try:
        return conexao.cursor()
    except Exception as erro:
        raise DatabaseConnectionError() from erro


def coletar_assinatura_ledger(conexao) -> LedgerSchemaSnapshot:
    """Coleta a assinatura via pg_catalog, sem usar search_path ou DDL."""
    tabelas: list[TableSignature] = []
    sequencias: list[SequenceSignature] = []
    cursor_capacidade = None
    try:
        cursor_capacidade = _cursor(conexao)
        cursor_capacidade.execute(
            "SELECT pg_catalog.count(*) = %s "
            "FROM pg_catalog.pg_attribute AS a "
            "WHERE a.attrelid = 'pg_catalog.pg_index'::pg_catalog.regclass "
            "AND a.attname = ANY(%s) AND a.attnum > 0 AND NOT a.attisdropped",
            (
                5,
                [
                    "indimmediate", "indisclustered", "indisreplident",
                    "indnullsnotdistinct", "indcheckxmin",
                ],
            ),
        )
        capacidade = cursor_capacidade.fetchone()
        if not capacidade or capacidade[0] is not True:
            raise DatabaseConnectionError()
    except DatabaseConnectionError:
        raise
    except Exception as erro:
        raise DatabaseConnectionError() from erro
    finally:
        if cursor_capacidade is not None:
            try:
                cursor_capacidade.close()
            except Exception as erro:
                raise DatabaseConnectionError() from erro
    for nome_tabela in sorted(EXPECTED_LEDGER_TABLES):
        cursor = None
        erro_principal: BaseException | None = None
        try:
            cursor = _cursor(conexao)
            cursor.execute(
                "SELECT n.nspname, c.relname, c.relkind "
                "FROM pg_catalog.pg_class AS c "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s",
                ("public", nome_tabela),
            )
            tabela_linha = cursor.fetchone()
            if tabela_linha is None:
                continue
            cursor.execute(
                "SELECT a.attnum, a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), "
                "CASE WHEN a.atttypid = 1043 THEN a.atttypmod - 4 END, NULL::integer, "
                "NULL::integer, NOT a.attnotnull, pg_catalog.pg_get_expr(d.adbin, d.adrelid), "
                "a.attidentity, a.attgenerated "
                "FROM pg_catalog.pg_attribute AS a "
                "JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "LEFT JOIN pg_catalog.pg_attrdef AS d "
                "ON d.adrelid = a.attrelid AND d.adnum = a.attnum "
                "WHERE n.nspname = %s AND c.relname = %s "
                "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum",
                ("public", nome_tabela),
            )
            colunas = tuple(
                ColumnSignature(
                    linha[0], linha[1], linha[2], linha[3], linha[4], linha[5],
                    linha[6], canonicalizar_sql(linha[7]), linha[8] or "", linha[9] or "",
                )
                for linha in cursor.fetchall()
            )
            cursor.execute(
                "SELECT con.conname, con.contype, con.conkey, COALESCE(("
                "SELECT pg_catalog.array_agg(a.attname ORDER BY chave.ordem) "
                "FROM pg_catalog.unnest(con.conkey) WITH ORDINALITY AS chave(attnum, ordem) "
                "LEFT JOIN pg_catalog.pg_attribute AS a "
                "ON a.attrelid = con.conrelid AND a.attnum = chave.attnum "
                "AND a.attnum > 0 AND NOT a.attisdropped), ARRAY[]::name[]), "
                "pg_catalog.pg_get_constraintdef(con.oid, true), "
                "con.condeferrable, con.condeferred, con.convalidated, "
                "con.conislocal, con.coninhcount, con.connoinherit "
                "FROM pg_catalog.pg_constraint AS con "
                "JOIN pg_catalog.pg_class AS c ON c.oid = con.conrelid "
                "JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
                "WHERE n.nspname = %s AND c.relname = %s ORDER BY con.conname",
                ("public", nome_tabela),
            )
            constraints = tuple(sorted(
                ConstraintSignature(
                    linha[0], linha[1],
                    normalizar_conkey_constraint(linha[2], linha[3]),
                    normalizar_colunas_constraint(linha[2], linha[3]),
                    canonicalizar_constraintdef(linha[4], linha[1]),
                    linha[5], linha[6], linha[7], linha[8], linha[9], linha[10],
                )
                for linha in cursor.fetchall()
            ))
            cursor.execute(
                "SELECT ni.nspname, nt.nspname, indice.relname, tabela.relname, am.amname, "
                "ix.indisunique, ix.indisprimary, ix.indisexclusion, ix.indimmediate, "
                "ix.indcheckxmin, ix.indisvalid, ix.indisready, ix.indislive, ix.indisclustered, "
                "ix.indisreplident, ix.indnullsnotdistinct, ix.indnkeyatts, ix.indnatts, "
                "COALESCE((SELECT pg_catalog.array_agg(a.attname ORDER BY k.ordem) "
                "FROM pg_catalog.unnest(ix.indkey::smallint[]) WITH ORDINALITY AS k(attnum, ordem) "
                "JOIN pg_catalog.pg_attribute AS a ON a.attrelid = ix.indrelid AND a.attnum = k.attnum "
                "WHERE k.ordem <= ix.indnkeyatts), ARRAY[]::name[]), "
                "COALESCE((SELECT pg_catalog.array_agg(a.attname ORDER BY k.ordem) "
                "FROM pg_catalog.unnest(ix.indkey::smallint[]) WITH ORDINALITY AS k(attnum, ordem) "
                "JOIN pg_catalog.pg_attribute AS a ON a.attrelid = ix.indrelid AND a.attnum = k.attnum "
                "WHERE k.ordem > ix.indnkeyatts), ARRAY[]::name[]), "
                "pg_catalog.pg_get_expr(ix.indexprs, ix.indrelid), "
                "pg_catalog.pg_get_expr(ix.indpred, ix.indrelid), con.oid IS NOT NULL, "
                "COALESCE((SELECT pg_catalog.array_agg(nc.nspname ORDER BY k.ordem) "
                "FROM pg_catalog.unnest(ix.indcollation::oid[]) WITH ORDINALITY AS k(oid, ordem) "
                "LEFT JOIN pg_catalog.pg_collation AS c ON c.oid = k.oid "
                "LEFT JOIN pg_catalog.pg_namespace AS nc ON nc.oid = c.collnamespace "
                "WHERE k.ordem <= ix.indnkeyatts), ARRAY[]::text[]), "
                "COALESCE((SELECT pg_catalog.array_agg(c.collname ORDER BY k.ordem) "
                "FROM pg_catalog.unnest(ix.indcollation::oid[]) WITH ORDINALITY AS k(oid, ordem) "
                "LEFT JOIN pg_catalog.pg_collation AS c ON c.oid = k.oid "
                "WHERE k.ordem <= ix.indnkeyatts), ARRAY[]::text[]), "
                "COALESCE((SELECT pg_catalog.array_agg(no.nspname ORDER BY k.ordem) "
                "FROM pg_catalog.unnest(ix.indclass::oid[]) WITH ORDINALITY AS k(oid, ordem) "
                "JOIN pg_catalog.pg_opclass AS o ON o.oid = k.oid "
                "JOIN pg_catalog.pg_namespace AS no ON no.oid = o.opcnamespace "
                "WHERE k.ordem <= ix.indnkeyatts), ARRAY[]::text[]), "
                "COALESCE((SELECT pg_catalog.array_agg(o.opcname ORDER BY k.ordem) "
                "FROM pg_catalog.unnest(ix.indclass::oid[]) WITH ORDINALITY AS k(oid, ordem) "
                "JOIN pg_catalog.pg_opclass AS o ON o.oid = k.oid "
                "WHERE k.ordem <= ix.indnkeyatts), ARRAY[]::text[]), "
                "COALESCE((SELECT pg_catalog.array_agg(CASE WHEN (k.opcao & 1) = 1 THEN 'DESC' ELSE 'ASC' END ORDER BY k.ordem) "
                "FROM pg_catalog.unnest(ix.indoption::smallint[]) WITH ORDINALITY AS k(opcao, ordem) "
                "WHERE k.ordem <= ix.indnkeyatts), ARRAY[]::text[]), "
                "COALESCE((SELECT pg_catalog.array_agg(CASE WHEN (k.opcao & 2) = 2 THEN 'FIRST' ELSE 'LAST' END ORDER BY k.ordem) "
                "FROM pg_catalog.unnest(ix.indoption::smallint[]) WITH ORDINALITY AS k(opcao, ordem) "
                "WHERE k.ordem <= ix.indnkeyatts), ARRAY[]::text[]), "
                "COALESCE((SELECT pg_catalog.array_agg(k.opcao ORDER BY k.ordem) "
                "FROM pg_catalog.unnest(ix.indoption::smallint[]) WITH ORDINALITY AS k(opcao, ordem) "
                "WHERE k.ordem <= ix.indnkeyatts), ARRAY[]::smallint[]), "
                "pg_catalog.pg_get_indexdef(ix.indexrelid, 0, true) "
                "FROM pg_catalog.pg_index AS ix "
                "JOIN pg_catalog.pg_class AS indice ON indice.oid = ix.indexrelid "
                "JOIN pg_catalog.pg_class AS tabela ON tabela.oid = ix.indrelid "
                "JOIN pg_catalog.pg_namespace AS ni ON ni.oid = indice.relnamespace "
                "JOIN pg_catalog.pg_namespace AS nt ON nt.oid = tabela.relnamespace "
                "JOIN pg_catalog.pg_am AS am ON am.oid = indice.relam "
                "LEFT JOIN pg_catalog.pg_constraint AS con ON con.conindid = ix.indexrelid "
                "WHERE nt.nspname = %s AND tabela.relname = %s ORDER BY indice.relname",
                ("public", nome_tabela),
            )
            indices_coletados: list[IndexSignature] = []
            for linha in cursor.fetchall():
                schemas_collation, nomes_collation = tuple(linha[23]), tuple(linha[24])
                schemas_opclass, nomes_opclass = tuple(linha[25]), tuple(linha[26])
                if (
                    len(schemas_collation) != len(nomes_collation)
                    or len(schemas_opclass) != len(nomes_opclass)
                ):
                    raise ValueError("Identidade física de índice incompleta.")
                collations = tuple(
                    None if schema is None and nome is None else QualifiedName(schema, nome)
                    for schema, nome in zip(schemas_collation, nomes_collation)
                )
                operator_classes = tuple(
                    QualifiedName(schema, nome)
                    for schema, nome in zip(schemas_opclass, nomes_opclass)
                )
                indices_coletados.append(IndexSignature(
                    linha[0], linha[1], linha[2], linha[3], linha[4], linha[5],
                    linha[6], linha[7], linha[8], linha[9], linha[10], linha[11],
                    linha[12], linha[13], linha[14], linha[15], linha[16], linha[17],
                    tuple(linha[18]), tuple(linha[19]),
                    canonicalizar_sql(linha[20]), canonicalizar_sql(linha[21]),
                    linha[22], collations, operator_classes, tuple(linha[27]),
                    tuple(linha[28]), tuple(linha[29]),
                    canonicalizar_indexdef(
                        linha[30],
                        indice_schema=linha[0], indice_nome=linha[2],
                        tabela_schema=linha[1], tabela_nome=linha[3],
                    ),
                ))
            indices = tuple(sorted(indices_coletados))
            tabelas.append(TableSignature(
                tabela_linha[0], tabela_linha[1], tabela_linha[2],
                colunas, constraints, indices,
            ))
        except DatabaseConnectionError:
            raise
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
                    erro_principal.add_note(
                        "Falha secundária ao fechar cursor da assinatura física."
                    )
    for nome_sequencia in sorted(EXPECTED_LEDGER_SEQUENCES):
        cursor = None
        erro_principal = None
        try:
            cursor = _cursor(conexao)
            cursor.execute(
                "SELECT ns.nspname, s.relname, s.relkind, nt.nspname, t.relname, "
                "a.attname, a.attnum, pg_catalog.format_type(a.atttypid, a.atttypmod), "
                "a.attidentity, d.deptype, pg_catalog.format_type(ps.seqtypid, NULL), "
                "ps.seqstart, ps.seqincrement, ps.seqmin, ps.seqmax, ps.seqcache, ps.seqcycle "
                "FROM pg_catalog.pg_class AS s "
                "JOIN pg_catalog.pg_namespace AS ns ON ns.oid = s.relnamespace "
                "JOIN pg_catalog.pg_sequence AS ps ON ps.seqrelid = s.oid "
                "LEFT JOIN pg_catalog.pg_depend AS d ON d.classid = "
                "'pg_catalog.pg_class'::pg_catalog.regclass AND d.objid = s.oid "
                "AND d.objsubid = 0 AND d.refclassid = "
                "'pg_catalog.pg_class'::pg_catalog.regclass AND d.refobjsubid > 0 "
                "LEFT JOIN pg_catalog.pg_class AS t ON t.oid = d.refobjid "
                "LEFT JOIN pg_catalog.pg_namespace AS nt ON nt.oid = t.relnamespace "
                "LEFT JOIN pg_catalog.pg_attribute AS a ON a.attrelid = d.refobjid "
                "AND a.attnum = d.refobjsubid "
                "WHERE ns.nspname = %s AND s.relname = %s AND s.relkind = %s",
                ("public", nome_sequencia, "S"),
            )
            linhas = cursor.fetchall()
            if len(linhas) > 1:
                raise ValueError("Sequência possui mais de um proprietário de coluna.")
            if linhas:
                sequencias.append(SequenceSignature(*linhas[0]))
        except DatabaseConnectionError:
            raise
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
                    erro_principal.add_note(
                        "Falha secundária ao fechar cursor da sequência do ledger."
                    )
    return LedgerSchemaSnapshot(
        tuple(sorted(tabelas, key=lambda tabela: tabela.nome)),
        tuple(sorted(sequencias, key=lambda sequencia: sequencia.nome)),
    )
