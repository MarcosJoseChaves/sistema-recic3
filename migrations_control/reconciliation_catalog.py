"""Consultas catalogais normalizadas usadas na reconciliação de schemas."""

from __future__ import annotations

import json

from .reconciliation_spec import CatalogObject, deep_freeze


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


RECONCILIATION_CATALOG_SQL = """
/* H2D24C_CATALOG */
WITH objects AS (
 SELECT 'table|'||c.relname AS logical_key, 'table' AS category,
        jsonb_build_object('relkind',c.relkind,'persistence',c.relpersistence) AS attributes
 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace
 WHERE n.nspname=%s AND c.relkind IN ('r','p')
 UNION ALL
 SELECT 'column|'||t.relname||'|'||a.attname, 'column',
        jsonb_build_object('type',pg_catalog.format_type(a.atttypid,a.atttypmod),
          'not_null',a.attnotnull,'identity',a.attidentity,'generated',a.attgenerated,
          'default',CASE WHEN d.oid IS NULL THEN NULL ELSE pg_catalog.pg_get_expr(d.adbin,d.adrelid,true) END)
 FROM pg_catalog.pg_attribute a JOIN pg_catalog.pg_class t ON t.oid=a.attrelid
 JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace
 LEFT JOIN pg_catalog.pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
 WHERE n.nspname=%s AND t.relkind IN ('r','p') AND a.attnum>0 AND NOT a.attisdropped
 UNION ALL
 SELECT 'sequence|'||s.relname, 'sequence',
        jsonb_build_object('type',pg_catalog.format_type(q.seqtypid,NULL),'start',q.seqstart,
          'increment',q.seqincrement,'min',q.seqmin,'max',q.seqmax,'cache',q.seqcache,
          'cycle',q.seqcycle,'owned_schema',onsp.nspname,'owned_table',ot.relname,'owned_column',oa.attname)
 FROM pg_catalog.pg_class s JOIN pg_catalog.pg_namespace n ON n.oid=s.relnamespace
 JOIN pg_catalog.pg_sequence q ON q.seqrelid=s.oid
 LEFT JOIN pg_catalog.pg_depend dep ON dep.classid='pg_catalog.pg_class'::regclass
   AND dep.objid=s.oid AND dep.objsubid=0 AND dep.refclassid='pg_catalog.pg_class'::regclass
   AND dep.deptype IN ('a','i')
 LEFT JOIN pg_catalog.pg_class ot ON ot.oid=dep.refobjid
 LEFT JOIN pg_catalog.pg_namespace onsp ON onsp.oid=ot.relnamespace
 LEFT JOIN pg_catalog.pg_attribute oa ON oa.attrelid=dep.refobjid AND oa.attnum=dep.refobjsubid
 WHERE n.nspname=%s AND s.relkind='S'
 UNION ALL
 SELECT 'constraint|'||t.relname||'|'||c.conname, 'constraint',
        jsonb_build_object('type',c.contype,'definition',pg_catalog.pg_get_constraintdef(c.oid,true),
          'validated',c.convalidated,'deferrable',c.condeferrable,'deferred',c.condeferred)
 FROM pg_catalog.pg_constraint c JOIN pg_catalog.pg_class t ON t.oid=c.conrelid
 JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace WHERE n.nspname=%s
 UNION ALL
 SELECT 'index|'||t.relname||'|'||x.relname, 'index',
        jsonb_build_object('unique',i.indisunique,'primary',i.indisprimary,'valid',i.indisvalid,
          'definition',pg_catalog.pg_get_indexdef(i.indexrelid,0,true))
 FROM pg_catalog.pg_index i JOIN pg_catalog.pg_class t ON t.oid=i.indrelid
 JOIN pg_catalog.pg_namespace n ON n.oid=t.relnamespace
 JOIN pg_catalog.pg_class x ON x.oid=i.indexrelid WHERE n.nspname=%s
)
SELECT logical_key, category, attributes FROM objects ORDER BY logical_key
""".strip()


def coletar_catalogo_reconciliacao(conexao):
    """Coleta o catálogo `public` usando a conexão recebida e apenas WITH/SELECT."""
    with conexao.cursor() as cursor:
        cursor.execute(RECONCILIATION_CATALOG_SQL, ("public",) * 5)
        rows = cursor.fetchall()
    objects = []
    seen = set()
    for logical_key, category, attributes in rows:
        if logical_key in seen:
            raise ValueError(f"chave catalogal duplicada: {logical_key}")
        seen.add(logical_key)
        if isinstance(attributes, str):
            attributes = json.loads(attributes)
        if not isinstance(attributes, dict):
            raise ValueError("atributos catalogais inválidos")
        forbidden = {"oid", "objid", "relfilenode"}
        if forbidden & set(attributes):
            raise ValueError("atributo catalogal volátil")
        objects.append(CatalogObject(
            migration_id="", category=category, logical_key=logical_key,
            attributes=deep_freeze(attributes),
        ))
    return tuple(objects)
