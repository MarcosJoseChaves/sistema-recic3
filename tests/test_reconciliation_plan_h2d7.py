import hashlib
import json
import re
import unittest
from pathlib import Path

from migrations_control.manifest import carregar_manifesto


ROOT = Path(__file__).resolve().parents[1]
RECON = ROOT / "migrations_control" / "reconciliation"
R0001 = RECON / "R0001_legacy_to_baseline.sql"
R0002 = RECON / "R0002_legacy_to_baseline.sql"
R0002_PRECHECKS = RECON / "R0002_prechecks.sql"
R0003 = RECON / "R0003_legacy_to_baseline.sql"
R0003_PRECHECKS = RECON / "R0003_prechecks.sql"
M0004 = ROOT / "migrations_control" / "sql" / "M0004_autorizacao.sql"
PLAN = ROOT / "PLANO_RECONCILIACAO_DEV_H2D3.md"
REFERENCE = Path(r"C:\sistema-recic3_backups\reconciliacao_h2d2_recon2_20260811_112754.json")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class TestReconciliationPlanH2D7(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = R0003.read_text(encoding="utf-8")
        cls.prechecks = R0003_PRECHECKS.read_text(encoding="utf-8")
        cls.plan = PLAN.read_text(encoding="utf-8")
        cls.reference = json.loads(REFERENCE.read_text(encoding="utf-8"))

    def test_m0004_is_unequivocally_99_of_99_absent(self):
        summary = next(item for item in self.reference["migrations"] if item["migration"] == "M0004")
        absent = [item for item in self.reference["absent_objects"] if item.get("migration") == "M0004"]
        self.assertEqual(summary["status"], "AUSENTE")
        self.assertEqual(
            (summary["expected"], summary["equivalent"], summary["absent"], summary["divergent"]),
            (99, 0, 99, 0),
        )
        self.assertEqual(len(absent), 99)

    def test_99_objects_are_grouped_by_kind_and_table(self):
        absent = [item for item in self.reference["absent_objects"] if item.get("migration") == "M0004"]
        by_kind = {kind: sum(item["kind"] == kind for item in absent) for kind in {item["kind"] for item in absent}}
        by_table = {table: sum(item["table"] == table for item in absent) for table in {item["table"] for item in absent}}
        self.assertEqual(by_kind, {"table": 4, "column": 41, "constraint": 33, "index": 17, "sequence": 4})
        self.assertEqual(by_table, {
            "auth_permissoes": 24,
            "auth_perfis": 28,
            "auth_perfil_permissoes": 17,
            "auth_usuario_perfis": 30,
        })

    def test_r0003_is_exactly_the_normative_m0004_subset(self):
        marker = "-- SAFE_SUBSET_BEGIN: M0004\n"
        payload = self.sql.split(marker, 1)[1].lstrip()
        self.assertEqual(payload, M0004.read_text(encoding="utf-8").lstrip())

    def test_dependencies_are_only_m0002_m0003_or_internal_m0004(self):
        references = set(re.findall(r"(?i)REFERENCES\s+([a-z_][a-z0-9_]*)", self.sql))
        self.assertEqual(references, {
            "usuarios", "auth_modulos", "auth_acoes",
            "auth_perfis", "auth_permissoes",
        })
        self.assertIn("('usuarios', 'integer')", self.prechecks)
        self.assertIn("('auth_modulos', 'bigint')", self.prechecks)
        self.assertIn("('auth_acoes', 'bigint')", self.prechecks)

    def test_auth_usuario_perfis_estado_uses_consolidated_domain(self):
        self.assertRegex(
            self.sql,
            r"estado TEXT NOT NULL DEFAULT 'ATIVA'",
        )
        self.assertIn("estado IN ('ATIVA', 'REVOGADA', 'EXPIRADA')", self.sql)
        self.assertRegex(
            self.sql,
            r"(?s)CREATE UNIQUE INDEX uq_auth_usr_perfis__usr_id_perfil_id.+?WHERE estado = 'ATIVA' AND fim_em IS NULL",
        )
        self.assertNotIn("ATIVO', 'REVOGADO", self.sql)

    def test_r0003_has_no_dml_or_destructive_statement(self):
        no_comments = re.sub(r"(?m)^\s*--.*$", "", self.sql)
        for pattern in (
            r"(?mi)^\s*DROP\b", r"(?mi)^\s*TRUNCATE\b",
            r"(?mi)^\s*DELETE\s+FROM\b", r"(?mi)^\s*UPDATE\s+\S+\s+SET\b",
            r"(?mi)^\s*INSERT\s+INTO\b",
        ):
            self.assertNotRegex(no_comments, pattern)

    def test_r0003_protects_previous_reconciliation_fc_extras_and_ledger(self):
        lowered = self.sql.lower()
        self.assertNotRegex(lowered, r"\bfc_[a-z0-9_]*\b")
        self.assertNotIn("ledger", lowered)
        self.assertNotRegex(lowered, r"alter\s+table\s+usuarios\b")
        self.assertNotIn("usuario_recuperacoes_senha", lowered)
        self.assertNotIn("naturezas_financeiras", lowered)

    def test_prechecks_p200_p207_are_read_only(self):
        self.assertEqual(re.findall(r"(?m)^-- (P\d+):", self.prechecks), [f"P{number}" for number in range(200, 208)])
        no_comments = re.sub(r"(?m)^\s*--.*$", "", self.prechecks)
        statements = [part.strip() for part in no_comments.split(";") if part.strip()]
        self.assertEqual(len(statements), 8)
        for statement in statements:
            self.assertRegex(statement, r"(?is)^(SELECT|WITH)\b")
            self.assertNotRegex(statement, r"(?is)\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|CALL|DO|COPY)\b")

    def test_previous_reconciliation_artifacts_remain_byte_identical(self):
        self.assertEqual(sha256(R0001), "57275057fd1b836ea959eb5de4eba148a3bd59d82ca0c88f2f9a7e3713f0ec67")
        self.assertEqual(sha256(R0002), "eea4c46e78abf631bc4bd62d9779f9dd2a34ea1b7fdc70d974e13021bfa7e8d9")
        self.assertEqual(sha256(R0002_PRECHECKS), "e9f7867f6115ed24bd1ba89fe70c32a4c049a71c7aa4c695f9f558462c1713ef")

    def test_normal_manifest_and_24_checksums_remain_valid(self):
        manifest = carregar_manifesto()
        persistent = [item for item in manifest.operacoes if item.identificador != "M0000"]
        self.assertEqual(len(persistent), 24)
        self.assertTrue(all(item.checksum for item in persistent))
        self.assertNotIn("R0003", manifest.por_id())

    def test_future_order_requires_r0001_r0002_r0003(self):
        section = self.plan.split("## R0003 / M0004", 1)[1]
        self.assertRegex(section, r"R0001 → M0002 64/64 → R0002 → M0003 60/60 funcional\s*\n?→ P200–P207 → R0003")


if __name__ == "__main__":
    unittest.main()
