"""Consultas catalogais normalizadas usadas na reconciliação de schemas."""

from __future__ import annotations


ROUTINE_FIELDS = (
    "schema",
    "name",
    "identity_arguments",
    "kind",
    "return_type",
    "language",
    "security_definer",
    "volatility",
    "strict",
    "parallel",
    "definition",
)

ROUTINES_CATALOG_SQL = """
SELECT
    n.nspname,
    p.proname,
    pg_catalog.pg_get_function_identity_arguments(p.oid),
    p.prokind,
    pg_catalog.format_type(p.prorettype, NULL),
    l.lanname,
    p.prosecdef,
    p.provolatile,
    p.proisstrict,
    p.proparallel,
    CASE
        WHEN l.lanname IN ('internal', 'c') THEN NULL
        ELSE pg_catalog.pg_get_functiondef(p.oid)
    END
FROM pg_catalog.pg_proc AS p
JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
JOIN pg_catalog.pg_language AS l ON l.oid = p.prolang
WHERE n.nspname = 'public'
ORDER BY
    n.nspname,
    p.proname,
    pg_catalog.pg_get_function_identity_arguments(p.oid)
""".strip()


def coletar_rotinas_catalogais(conexao):
    """Coleta metadados estruturais de rotinas sem OIDs, owners ou ACLs."""
    with conexao.cursor() as cursor:
        cursor.execute(ROUTINES_CATALOG_SQL)
        return [dict(zip(ROUTINE_FIELDS, linha)) for linha in cursor.fetchall()]
