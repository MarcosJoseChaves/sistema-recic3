import hashlib
import json
import re
import unittest
from pathlib import Path

from migrations_control.manifest import carregar_manifesto


ROOT = Path(__file__).resolve().parents[1]
RECONCILIATION = ROOT / "migrations_control" / "reconciliation"
R0001 = RECONCILIATION / "R0001_legacy_to_baseline.sql"
R0002 = RECONCILIATION / "R0002_legacy_to_baseline.sql"
PRECHECKS = RECONCILIATION / "R0002_prechecks.sql"
PLAN = ROOT / "PLANO_RECONCILIACAO_DEV_H2D3.md"
REFERENCE = Path(r"C:\sistema-recic3_backups\reconciliacao_h2d2_recon2_20260811_112754.json")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def object_key(item):
    if item["kind"] == "table":
        return f"table|{item['name']}"
    if item["kind"] == "sequence":
        return f"sequence|{item['name']}"
    return f"{item['kind']}|{item['table']}|{item['name']}"


class TestReconciliationPlanH2D5(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = R0002.read_text(encoding="utf-8")
        cls.prechecks = PRECHECKS.read_text(encoding="utf-8")
        cls.plan = PLAN.read_text(encoding="utf-8")
        cls.reference = json.loads(REFERENCE.read_text(encoding="utf-8"))

    def test_reference_and_r0001_are_the_approved_sources(self):
        self.assertEqual(
            sha256(REFERENCE),
            "24641c2e3fbd95d3ce9e48a02347c71682c4dc52449eb5838c48e29f28d39da6",
        )
        self.assertEqual(
            sha256(R0001),
            "57275057fd1b836ea959eb5de4eba148a3bd59d82ca0c88f2f9a7e3713f0ec67",
        )

    def test_all_53_absent_m0003_objects_are_explicitly_assigned(self):
        expected = {
            object_key(item)
            for item in self.reference["absent_objects"]
            if item.get("migration") == "M0003"
        }
        assignments = re.findall(
            r"(?m)^-- H2D5-ABSENT (?:MATERIALIZE|PRESERVE_EQUIVALENT): (.+)$",
            self.sql,
        )
        self.assertEqual(len(expected), 53)
        self.assertEqual(len(assignments), 53)
        self.assertEqual(set(assignments), expected)

    def test_absences_are_grouped_and_legacy_pk_is_preserved(self):
        absent = [
            item for item in self.reference["absent_objects"]
            if item.get("migration") == "M0003"
        ]
        recovery = [item for item in absent if item.get("table") == "usuario_recuperacoes_senha"]
        users = [item for item in absent if item.get("table") == "usuarios"]
        self.assertEqual(len(recovery), 26)
        self.assertEqual(len(users), 27)
        preserved = set(re.findall(
            r"(?m)^-- H2D5-ABSENT PRESERVE_EQUIVALENT: (.+)$", self.sql
        ))
        self.assertEqual(preserved, {
            "constraint|usuarios|pk_usuarios",
            "index|usuarios|pk_usuarios",
        })
        self.assertNotRegex(self.sql, r"ADD\s+CONSTRAINT\s+pk_usuarios")

    def test_four_class_b_divergences_have_named_prechecks(self):
        markers = dict(re.findall(
            r"(?m)^-- H2D5-CLASS-B: (column\|usuarios\|\w+) PRECHECK=(P\d+)$",
            self.sql,
        ))
        self.assertEqual(markers, {
            "column|usuarios|username": "P101",
            "column|usuarios|password_hash": "P102",
            "column|usuarios|nome_completo": "P103",
            "column|usuarios|email": "P104",
        })
        for precheck in markers.values():
            self.assertRegex(self.prechecks, rf"(?m)^-- {precheck}:")

    def test_two_class_c_differences_do_not_generate_alter(self):
        markers = set(re.findall(r"(?m)^-- H2D5-CLASS-C: (.+?) PRESERVE_", self.sql))
        self.assertEqual(markers, {
            "column|usuarios|id",
            "sequence|usuarios_id_seq",
        })
        self.assertNotRegex(self.sql, r"(?is)ALTER\s+TABLE\s+usuarios\s+ALTER\s+COLUMN\s+id\b")
        self.assertNotRegex(self.sql, r"(?is)ALTER\s+SEQUENCE\s+usuarios_id_seq\b")

    def test_existing_users_table_is_not_rebuilt(self):
        self.assertNotRegex(self.sql, r"(?is)CREATE\s+TABLE\s+usuarios\s*\(")
        self.assertNotRegex(self.sql, r"(?is)ALTER\s+TABLE\s+usuarios\s+RENAME")
        self.assertNotRegex(self.sql, r"(?is)DROP\s+(?:TABLE|COLUMN)")
        self.assertIn("GENERATED ALWAYS AS (lower(btrim(username))) STORED", self.sql)
        self.assertIn("ALTER COLUMN username_normalizado DROP EXPRESSION", self.sql)

    def test_r0002_has_no_destructive_or_dml_statement(self):
        no_comments = re.sub(r"(?m)^\s*--.*$", "", self.sql)
        forbidden_statements = (
            r"(?mi)^\s*DROP\s+(?:TABLE|COLUMN)\b",
            r"(?mi)^\s*TRUNCATE\b",
            r"(?mi)^\s*DELETE\s+FROM\b",
            r"(?mi)^\s*UPDATE\s+\S+\s+SET\b",
            r"(?mi)^\s*INSERT\s+INTO\b",
        )
        for pattern in forbidden_statements:
            self.assertNotRegex(no_comments, pattern)

    def test_r0002_protects_m0002_fc_and_ledger(self):
        lowered = self.sql.lower()
        for token in ("auth_modulos", "auth_acoes", "naturezas_financeiras", "schema_migrations"):
            self.assertNotIn(token, lowered)
        self.assertNotRegex(lowered, r"\bfc_[a-z0-9_]*\b")
        self.assertNotIn("ledger", lowered)

    def test_prechecks_are_read_only_and_complete(self):
        ids = re.findall(r"(?m)^-- (P\d+):", self.prechecks)
        self.assertEqual(ids, [f"P{number}" for number in range(100, 108)])
        no_comments = re.sub(r"(?m)^\s*--.*$", "", self.prechecks)
        statements = [part.strip() for part in no_comments.split(";") if part.strip()]
        self.assertEqual(len(statements), 8)
        for statement in statements:
            self.assertRegex(statement, r"(?is)^(SELECT|WITH)\b")
            self.assertNotRegex(
                statement,
                r"(?is)\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|CALL|DO|COPY)\b",
            )

    def test_future_order_requires_r0001_before_r0002(self):
        section = self.plan.split("## R0002 / M0003", 1)[1]
        self.assertRegex(section, r"R0001/M0002 64/64\s*\n?→ executar P100–P107 → executar R0002")

    def test_normal_manifest_checksums_remain_valid(self):
        manifest = carregar_manifesto()
        self.assertEqual(len(manifest.operacoes), 25)
        self.assertEqual(sum(item.checksum is not None for item in manifest.operacoes), 24)


if __name__ == "__main__":
    unittest.main()
