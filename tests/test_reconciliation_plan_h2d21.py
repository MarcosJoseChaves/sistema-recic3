import ast
import hashlib
import json
import re
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RECON = ROOT / "migrations_control" / "reconciliation"
SQL = RECON / "R0007_M0013_legacy_to_baseline.sql"
PRECHECKS = RECON / "R0007_M0013_prechecks.sql"
TOLERANCES = RECON / "R0007_functional_tolerances.json"
PLAN = ROOT / "PLANO_RECONCILIACAO_DEV_H2D3.md"
M0013 = ROOT / "migrations_control" / "sql" / "M0013_solicitacoes.sql"
REFERENCE = Path(r"C:\sistema-recic3_backups\reconciliacao_h2d2_recon2_20260811_112754.json")


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class H2D21OfflineTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = SQL.read_text(encoding="utf-8")
        cls.prechecks = PRECHECKS.read_text(encoding="utf-8")
        cls.plan = PLAN.read_text(encoding="utf-8")
        cls.reference = json.loads(REFERENCE.read_text(encoding="utf-8"))
        cls.tolerances = json.loads(TOLERANCES.read_text(encoding="utf-8"))["tolerances"]

    def test_reference_is_exactly_272_1_269_2(self):
        self.assertEqual(sha256(REFERENCE), "24641c2e3fbd95d3ce9e48a02347c71682c4dc52449eb5838c48e29f28d39da6")
        item = next(x for x in self.reference["migrations"] if x["migration"] == "M0013")
        self.assertEqual({k:item[k] for k in ("expected","equivalent","absent","divergent")}, {"expected":272,"equivalent":1,"absent":269,"divergent":2})

    def test_equivalent_and_divergences_are_exact(self):
        divergences = {x["key"] for x in self.reference["divergences"] if x.get("migration") == "M0013"}
        self.assertEqual(divergences, {"column|solicitacoes_alteracao|id","sequence|solicitacoes_alteracao_id_seq"})
        absent = {f'{x["kind"]}|{x["table"]}|{x["name"]}' for x in self.reference["absent_objects"] if x.get("migration") == "M0013"}
        self.assertNotIn("table|solicitacoes_alteracao|solicitacoes_alteracao", absent)

    def test_all_269_absent_objects_are_assigned_once(self):
        expected = {f'{x["kind"]}|{x["table"]}|{x["name"]}' for x in self.reference["absent_objects"] if x.get("migration") == "M0013"}
        assignments = re.findall(r"(?m)^-- H2D21-ABSENT (?:MATERIALIZE|PRESERVE_EQUIVALENT): (.+)$", self.sql)
        self.assertEqual(len(assignments), 269)
        self.assertEqual(set(assignments), expected)

    def test_only_id_and_sequence_are_class_c(self):
        class_c = {(x["key"],x["path"]) for x in self.tolerances if x["class"] == "C"}
        self.assertEqual(class_c, {
            ("column|solicitacoes_alteracao|id","public.solicitacoes_alteracao.id"),
            ("sequence|solicitacoes_alteracao_id_seq","public.solicitacoes_alteracao_id_seq"),
        })
        self.assertNotRegex(self.sql, r"(?is)ALTER\s+(?:TABLE|SEQUENCE).*solicitacoes_alteracao(?:_id_seq)?.*(?:TYPE|AS BIGINT)")

    def test_legacy_pk_and_backing_index_are_preserved(self):
        preserved = set(re.findall(r"(?m)^-- H2D21-ABSENT PRESERVE_EQUIVALENT: (.+)$", self.sql))
        self.assertEqual(preserved, {"constraint|solicitacoes_alteracao|pk_solicitacoes_alteracao","index|solicitacoes_alteracao|pk_solicitacoes_alteracao"})
        self.assertNotIn("ADD CONSTRAINT pk_solicitacoes_alteracao", self.sql)
        aliases = [x for x in self.tolerances if x["class"] == "LEGACY_PK_NAME_EQUIVALENCE"]
        self.assertEqual(len(aliases), 2)

    def test_eleven_missing_tables_are_created(self):
        expected = {"solicitacao_aplicacoes","solicitacao_aprovacoes","solicitacao_associacoes","solicitacao_associados","solicitacao_catalogo_itens","solicitacao_documentos","solicitacao_eventos","solicitacao_mensagens","solicitacao_patrimonios","solicitacao_transacoes","solicitacao_uvrs"}
        created = set(re.findall(r"(?im)^CREATE TABLE\s+(\w+)", self.sql))
        self.assertEqual(created, expected)
        self.assertNotRegex(self.sql, r"(?im)^CREATE TABLE\s+(?:public\.)?solicitacoes_alteracao\b")

    def test_child_solicitacao_ids_remain_bigint(self):
        self.assertEqual(len(re.findall(r"(?m)^\s+solicitacao_id BIGINT NOT NULL", self.sql)), 11)

    def test_eight_legacy_columns_are_never_targets(self):
        for field in ("tabela_alvo","id_registro","tipo_solicitacao","dados_novos","usuario_solicitante","data_solicitacao","status","observacoes_admin"):
            self.assertNotRegex(self.sql, rf"(?im)^\s*(?:ALTER COLUMN|SET)\s+{field}\s*=")
            self.assertNotRegex(self.sql, rf"(?is)ALTER\s+TABLE.*(?:DROP|RENAME)\s+(?:COLUMN\s+)?{field}\b")

    def test_backfill_sentinels_and_state_mapping_are_exact(self):
        for text in ("WHEN 'APROVADO' THEN 'APLICADA'","WHEN 'REJEITADO' THEN 'REJEITADA'","modulo = 'LEGADO'","risco = 'LEGADO_NAO_CLASSIFICADO'","versao_esperada = 0"):
            self.assertIn(text, self.sql)
        self.assertNotIn("WHEN 'PENDENTE'", self.sql)

    def test_photographs_do_not_invent_history(self):
        self.assertIn("fotografia_proposta = dados_novos", self.sql)
        for field in ("fotografia_original","fotografia_aprovada","fotografia_aplicada"):
            self.assertIn(f"{field} = NULL", self.sql)

    def test_utc_is_only_a_documented_technical_convention(self):
        self.assertIn("data_solicitacao AT TIME ZONE 'UTC'", self.sql)
        self.assertIn("UTC é convenção técnica", self.sql)
        self.assertIn("data_solicitacao original permanece intocada", self.sql)

    def test_user_resolution_has_three_strict_levels(self):
        self.assertIn("usuario.username = solicitacao.usuario_solicitante", self.sql)
        self.assertIn("usuario.username_normalizado = lower(btrim(solicitacao.usuario_solicitante))", self.sql)
        self.assertIn("exact.candidate_count = 0", self.sql)
        self.assertIn("canonical.candidate_count = 0", self.sql)
        self.assertIn("candidate_count = 1", self.sql)

    def test_historical_actor_is_unambiguously_blocked(self):
        for text in ("'BLOQUEADO'","TRUE","'migracao'","NULL, FALSE","Ator histórico legado:"):
            self.assertIn(text, self.sql)
        self.assertIn("AND ativo = TRUE", (ROOT / "app.py").read_text(encoding="utf-8"))
        self.assertNotRegex(self.sql, r"(?is)INSERT\s+INTO\s+public\.auth_usuario_perfis")

    def test_no_arbitrary_or_fuzzy_selector(self):
        combined = self.sql + self.prechecks
        for pattern in (r"(?i)\bmin\s*\(\s*(?:usuario|candidate|candidato)\.id",r"(?i)\bmax\s*\(\s*(?:usuario|candidate|candidato)\.id",r"(?i)\bLIMIT\s+1\b",r"(?i)DISTINCT\s+ON",r"(?i)row_number",r"(?i)first_value",r"(?i)unaccent|levenshtein|similarity|soundex"):
            self.assertNotRegex(combined, pattern)

    def test_no_drop_truncate_rebuild_or_extension(self):
        self.assertNotRegex(self.sql, r"(?i)\b(?:DROP|TRUNCATE|CREATE\s+EXTENSION)\b")
        self.assertNotRegex(self.sql, r"(?i)CREATE\s+TABLE\s+.*_new\b")

    def test_prechecks_are_read_only_and_complete(self):
        self.assertEqual(set(re.findall(r"(?m)^-- (P7\d\d):", self.prechecks)), {f"P{i}" for i in range(700,714)})
        self.assertNotRegex(self.prechecks, r"(?i)\b(?:INSERT|UPDATE|DELETE|MERGE|ALTER|CREATE|DROP|TRUNCATE)\b")

    def test_normative_m0013_and_expected_are_unchanged(self):
        self.assertEqual(sha256(M0013), "1d2173c9d5dd2cd7a62aa42ef655a7c50e3e08002053d8aa9d9a73630905e9da")
        self.assertIn("272/272", self.plan)

    def test_plan_documents_technical_not_historical_values(self):
        for text in ("H2D.21","LEGADO_NAO_CLASSIFICADO","versão histórica do objeto não registrada","convenção técnica UTC","não é data histórica"):
            self.assertIn(text, self.plan)


if __name__ == "__main__":
    unittest.main()
