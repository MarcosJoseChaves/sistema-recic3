import re
import unittest
from unittest.mock import MagicMock

from migrations_control.reconciliation_catalog import (
    ROUTINE_FIELDS,
    ROUTINES_CATALOG_SQL,
    coletar_rotinas_catalogais,
)


class ReconciliationCatalogH2D2Tests(unittest.TestCase):
    def test_order_by_usa_expressoes_disponiveis_e_deterministicas(self):
        sql = " ".join(ROUTINES_CATALOG_SQL.split())
        order_by = sql.split(" ORDER BY ", 1)[1]

        self.assertNotRegex(order_by, r"(^|,\s*)args(\s*,|$)")
        self.assertEqual(
            order_by,
            "n.nspname, p.proname, "
            "pg_catalog.pg_get_function_identity_arguments(p.oid)",
        )

    def test_coleta_preserva_campos_sem_identidade_operacional(self):
        linha = (
            "public", "calcular", "integer", "f", "integer", "sql",
            False, "v", True, "s", "CREATE FUNCTION calcular(integer)",
        )
        conexao = MagicMock()
        cursor = conexao.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [linha]

        resultado = coletar_rotinas_catalogais(conexao)

        cursor.execute.assert_called_once_with(ROUTINES_CATALOG_SQL)
        self.assertEqual(resultado, [dict(zip(ROUTINE_FIELDS, linha))])
        self.assertNotIn("oid", ROUTINE_FIELDS)
        self.assertNotIn("owner", ROUTINE_FIELDS)
        self.assertNotIn("acl", ROUTINE_FIELDS)

    def test_select_mantem_campos_necessarios_sem_expor_oid(self):
        select = ROUTINES_CATALOG_SQL.split("FROM pg_catalog.pg_proc", 1)[0]
        normalizado = " ".join(select.split()).lower()

        for campo in (
            "p.proname",
            "pg_catalog.pg_get_function_identity_arguments(p.oid)",
            "p.prokind",
            "pg_catalog.format_type(p.prorettype, null)",
            "l.lanname",
            "p.prosecdef",
            "p.provolatile",
            "p.proisstrict",
            "p.proparallel",
            "pg_catalog.pg_get_functiondef(p.oid)",
        ):
            self.assertIn(campo, normalizado)
        self.assertIsNone(re.search(r"select\s+p\.oid(?:\s|,)", normalizado))


if __name__ == "__main__":
    unittest.main()
