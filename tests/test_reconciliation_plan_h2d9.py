import hashlib
import json
import re
import unittest
from pathlib import Path

from migrations_control.manifest import carregar_manifesto


ROOT = Path(__file__).resolve().parents[1]
RECON = ROOT / "migrations_control" / "reconciliation"
R0004 = RECON / "R0004_M0005_M0008_legacy_to_baseline.sql"
PRECHECKS = RECON / "R0004_M0005_M0008_prechecks.sql"
PLAN = ROOT / "PLANO_RECONCILIACAO_DEV_H2D3.md"
REFERENCE = Path(r"C:\sistema-recic3_backups\reconciliacao_h2d2_recon2_20260811_112754.json")
MIGRATIONS = {
    "M0005": ROOT / "migrations_control/sql/M0005_auditoria.sql",
    "M0006": ROOT / "migrations_control/sql/M0006_organizacoes.sql",
    "M0007": ROOT / "migrations_control/sql/M0007_escopos.sql",
    "M0008": ROOT / "migrations_control/sql/M0008_documentos.sql",
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class TestReconciliationPlanH2D9(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = R0004.read_text(encoding="utf-8")
        cls.prechecks = PRECHECKS.read_text(encoding="utf-8")
        cls.plan = PLAN.read_text(encoding="utf-8")
        cls.reference = json.loads(REFERENCE.read_text(encoding="utf-8"))

    def test_source_counts_and_all_307_are_absent(self):
        self.assertEqual(sha256(REFERENCE), "24641c2e3fbd95d3ce9e48a02347c71682c4dc52449eb5838c48e29f28d39da6")
        expected = {"M0005": 40, "M0006": 169, "M0007": 54, "M0008": 44}
        summaries = {item["migration"]: item for item in self.reference["migrations"]}
        for migration, count in expected.items():
            item = summaries[migration]
            self.assertEqual(
                (item["status"], item["expected"], item["equivalent"], item["absent"], item["divergent"]),
                ("AUSENTE", count, 0, count, 0),
            )
        absent = [item for item in self.reference["absent_objects"] if item.get("migration") in expected]
        self.assertEqual(len(absent), 307)

    def test_inventory_closes_by_migration_kind_and_table(self):
        kinds = {
            "M0005": {"table": 1, "column": 22, "sequence": 1, "constraint": 9, "index": 7},
            "M0006": {"table": 6, "column": 72, "sequence": 6, "constraint": 58, "index": 27},
            "M0007": {"table": 3, "column": 23, "sequence": 3, "constraint": 14, "index": 11},
            "M0008": {"table": 1, "column": 19, "sequence": 1, "constraint": 16, "index": 7},
        }
        tables = {
            "M0005": {"auditoria_tecnica": 40},
            "M0006": {"associacao_aliases": 26, "associacao_eventos": 21, "associacoes": 34, "auditoria_tecnica": 2, "uvr_aliases": 26, "uvr_eventos": 21, "uvrs": 39},
            "M0007": {"auth_escopos_associacao": 19, "auth_escopos_globais": 16, "auth_escopos_uvr": 19},
            "M0008": {"documentos_privados": 44},
        }
        for migration in kinds:
            items = [x for x in self.reference["absent_objects"] if x.get("migration") == migration]
            self.assertEqual({kind: sum(x["kind"] == kind for x in items) for kind in kinds[migration]}, kinds[migration])
            self.assertEqual({table: sum(x["table"] == table for x in items) for table in tables[migration]}, tables[migration])

    def test_sections_are_exact_normative_migrations_in_order(self):
        markers = [f"-- SAFE_SUBSET_BEGIN: {migration}" for migration in MIGRATIONS]
        positions = [self.sql.index(marker) for marker in markers]
        self.assertEqual(positions, sorted(positions))
        for index, (migration, path) in enumerate(MIGRATIONS.items()):
            start = positions[index] + len(markers[index])
            end = positions[index + 1] if index + 1 < len(positions) else len(self.sql)
            self.assertEqual(self.sql[start:end].strip(), path.read_text(encoding="utf-8").strip())

    def test_special_audit_rules(self):
        m5 = self.sql.split("-- SAFE_SUBSET_BEGIN: M0005", 1)[1].split("-- SAFE_SUBSET_BEGIN: M0006", 1)[0]
        m6 = self.sql.split("-- SAFE_SUBSET_BEGIN: M0006", 1)[1].split("-- SAFE_SUBSET_BEGIN: M0007", 1)[0]
        for name in ("fk_auditoria_tecnica__assoc_id", "fk_auditoria_tecnica__uvr_id"):
            self.assertNotIn(name, m5)
            self.assertIn(name, m6)
        self.assertIn("associacao_id", m5)
        self.assertIn("uvr_id", m5)
        self.assertNotIn("componente_sistema", self.sql.lower())
        self.assertNotIn("ck_auditoria_tecnica__regra_273", self.sql.lower())

    def test_no_dml_destructive_fc_ledger_or_m0009_plus(self):
        no_comments = re.sub(r"(?m)^\s*--.*$", "", self.sql)
        for pattern in (
            r"(?mi)^\s*DROP\s+(?:TABLE|COLUMN)\b", r"(?mi)^\s*TRUNCATE\b",
            r"(?mi)^\s*DELETE\s+FROM\b", r"(?mi)^\s*UPDATE\s+\S+\s+SET\b",
            r"(?mi)^\s*INSERT\s+INTO\b",
        ):
            self.assertNotRegex(no_comments, pattern)
        self.assertNotRegex(self.sql.lower(), r"\bfc_[a-z0-9_]*\b")
        self.assertNotIn("ledger", self.sql.lower())
        self.assertNotRegex(self.sql, r"\b(?:M0009|M001[0-3])\b")

    def test_dependencies_do_not_modify_m0002_to_m0004(self):
        targets = {x["table"] for x in self.reference["absent_objects"] if x.get("migration") in MIGRATIONS}
        altered = set(re.findall(r"(?i)ALTER\s+TABLE\s+([a-z_][a-z0-9_]*)", self.sql))
        references = set(re.findall(r"(?i)REFERENCES\s+([a-z_][a-z0-9_]*)", self.sql))
        self.assertLessEqual(altered, targets)
        self.assertLessEqual(references, targets | {"usuarios", "auth_usuario_perfis"})

    def test_prechecks_are_read_only_and_traceable(self):
        ids = re.findall(r"(?m)^-- (P\d+):", self.prechecks)
        self.assertEqual(ids, [
            "P300", "P301", "P302", "P303", "P304",
            "P320", "P321", "P322", "P323", "P324",
            "P340", "P341", "P342", "P343", "P344",
            "P360", "P361", "P362", "P363", "P364",
        ])
        statements = [x.strip() for x in re.sub(r"(?m)^\s*--.*$", "", self.prechecks).split(";") if x.strip()]
        self.assertEqual(len(statements), 20)
        for statement in statements:
            self.assertRegex(statement, r"(?is)^(SELECT|WITH)\b")
            self.assertNotRegex(statement, r"(?is)\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|CALL|DO|COPY)\b")

    def test_r0001_r0002_r0003_are_byte_identical(self):
        expected = {
            "R0001_legacy_to_baseline.sql": "57275057fd1b836ea959eb5de4eba148a3bd59d82ca0c88f2f9a7e3713f0ec67",
            "R0002_legacy_to_baseline.sql": "eea4c46e78abf631bc4bd62d9779f9dd2a34ea1b7fdc70d974e13021bfa7e8d9",
            "R0003_legacy_to_baseline.sql": "ea94e951c2a14148379a3a27cca53d5100d99f27c30b5ffdde14b4775c7de3a8",
        }
        for name, digest in expected.items():
            self.assertEqual(sha256(RECON / name), digest)

    def test_manifest_has_24_valid_normal_checksums(self):
        manifest = carregar_manifesto()
        self.assertEqual(sum(item.checksum is not None for item in manifest.operacoes), 24)
        self.assertNotIn("R0004", manifest.por_id())

    def test_future_plan_is_one_atomic_group(self):
        section = self.plan.split("## R0004 agrupada / M0005-M0008", 1)[1]
        self.assertRegex(section, r"(?s)M0005.*M0006.*M0007.*M0008")
        self.assertIn("307/307", section)
        self.assertIn("ROLLBACK", section)
        self.assertIn("COMMIT", section)
        self.assertNotRegex(self.sql, r"(?mi)^\s*(?:BEGIN|COMMIT)\s*;")


if __name__ == "__main__":
    unittest.main()
