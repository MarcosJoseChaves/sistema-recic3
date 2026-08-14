import hashlib
import json
import re
import unittest
from pathlib import Path

from migrations_control.manifest import carregar_manifesto


ROOT = Path(__file__).resolve().parents[1]
RECON = ROOT / "migrations_control" / "reconciliation"
R0006 = RECON / "R0006_M0011_M0012_legacy_to_baseline.sql"
PRECHECKS = RECON / "R0006_M0011_M0012_prechecks.sql"
TOLERANCES = RECON / "R0006_functional_tolerances.json"
PLAN = ROOT / "PLANO_RECONCILIACAO_DEV_H2D3.md"
REFERENCE = Path(r"C:\sistema-recic3_backups\reconciliacao_h2d2_recon2_20260811_112754.json")
M0011 = ROOT / "migrations_control" / "sql" / "M0011_financeiro.sql"
M0012 = ROOT / "migrations_control" / "sql" / "M0012_patrimonio.sql"

NINE_BACKFILLS = {
    "identificador_publico", "associacao_id", "natureza_id", "conta_financeira_id",
    "competencia_data", "valor_total", "fotografia", "criado_por_usuario_id",
    "atualizado_por_usuario_id",
}
EXTRA_FKS = {
    "auditoria_rateios_transacoes_id_transacao_fkey",
    "documentos_id_transacao_origem_fkey",
    "fluxo_caixa_transacoes_link_id_transacao_financeira_fkey",
    "itens_transacao_id_transacao_fkey",
}
TECH_NULLABLE = {"instituicao", "agencia", "conta", "abertura_data", "encerramento_data"}
H2D15_FIXTURE = [
    {"associacao": "ACAN", "uvr": "UVR 02", "quantidade": 135},
    {"associacao": "ASCAMAR", "uvr": "UVR 01", "quantidade": 621},
]


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def deterministic_association_plan(rows, associations=(), aliases=()):
    """Modelo sintético da regra SQL; não consulta banco nem escolhe candidatos."""
    pairs = {}
    uvr_tokens = {}
    for row in rows:
        raw = row["associacao"].strip() if row.get("associacao") is not None else ""
        token = raw.lower()
        uvr = row["uvr"].strip().lower() if row.get("uvr") is not None else ""
        pairs.setdefault(token, {"spellings": set(), "uvrs": set(), "count": 0})
        pairs[token]["spellings"].add(raw)
        pairs[token]["uvrs"].add(uvr)
        pairs[token]["count"] += row.get("quantidade", 1)
        uvr_tokens.setdefault(uvr, set()).add(token)

    result = {}
    for token, identity in pairs.items():
        if not token or len(identity["spellings"]) != 1 or len(identity["uvrs"]) != 1:
            result[token] = "FAIL"
            continue
        uvr = next(iter(identity["uvrs"]))
        if not uvr or len(uvr_tokens[uvr]) != 1:
            result[token] = "FAIL"
            continue
        candidates = set()
        divergent = False
        for association in associations:
            association_id = association["id"]
            code = (association.get("codigo") or "").strip().lower()
            by_code = code == token
            by_name = association.get("nome_normalizado") == token
            by_alias = any(
                alias["associacao_id"] == association_id
                and alias["alias_normalizado"] == token
                for alias in aliases
            )
            if by_code or by_name or by_alias:
                candidates.add(association_id)
            if (by_name or by_alias) and code and code != token:
                divergent = True
        if divergent or len(candidates) > 1:
            result[token] = "FAIL"
        elif len(candidates) == 1:
            result[token] = "REUSE"
        else:
            result[token] = "CREATE"
    return result


class TestReconciliationPlanH2D13(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = R0006.read_text(encoding="utf-8")
        cls.prechecks = PRECHECKS.read_text(encoding="utf-8")
        cls.plan = PLAN.read_text(encoding="utf-8")
        cls.reference = json.loads(REFERENCE.read_text(encoding="utf-8"))
        cls.tolerances = json.loads(TOLERANCES.read_text(encoding="utf-8"))["tolerances"]

    def test_approved_reference_counts_close_184_and_193(self):
        self.assertEqual(sha256(REFERENCE), "24641c2e3fbd95d3ce9e48a02347c71682c4dc52449eb5838c48e29f28d39da6")
        summaries = {item["migration"]: item for item in self.reference["migrations"]}
        self.assertEqual(
            {k: summaries["M0011"][k] for k in ("expected", "equivalent", "absent", "divergent")},
            {"expected": 184, "equivalent": 2, "absent": 179, "divergent": 3},
        )
        self.assertEqual(
            {k: summaries["M0012"][k] for k in ("expected", "equivalent", "absent", "divergent")},
            {"expected": 193, "equivalent": 0, "absent": 193, "divergent": 0},
        )
        self.assertEqual(184 + 193, 377)

    def test_three_m0011_divergences_are_exact(self):
        found = {item["key"] for item in self.reference["divergences"] if item["migration"] == "M0011"}
        self.assertEqual(found, {
            "column|transacoes_financeiras|id",
            "column|transacoes_financeiras|numero_documento",
            "sequence|transacoes_financeiras_id_seq",
        })
        self.assertIn("ALTER COLUMN numero_documento TYPE TEXT", self.sql)

    def test_only_id_and_sequence_are_class_c(self):
        class_c = {(item["key"], item["path"]) for item in self.tolerances if item["class"] == "C"}
        self.assertEqual(class_c, {
            ("column|transacoes_financeiras|id", "public.transacoes_financeiras.id"),
            ("sequence|transacoes_financeiras_id_seq", "public.transacoes_financeiras_id_seq"),
        })
        self.assertFalse(any(item["class"] == "C" and "contas_financeiras" in item["path"] for item in self.tolerances))
        self.assertNotRegex(self.sql, r"(?is)ALTER\s+TABLE\s+public\.transacoes_financeiras\s+ALTER\s+COLUMN\s+id\b")
        self.assertNotRegex(self.sql, r"(?is)ALTER\s+SEQUENCE\s+public\.transacoes_financeiras_id_seq")

    def test_complete_accounts_schema_is_materialized(self):
        block = self.sql.split("CREATE TABLE contas_financeiras (", 1)[1].split(");", 1)[0]
        fields = re.findall(r"(?m)^\s{4}([a-z_]+)\s+", block)
        self.assertEqual(fields, [
            "id", "associacao_id", "codigo", "nome", "tipo", "instituicao", "agencia",
            "conta", "estado", "abertura_data", "encerramento_data", "observacoes",
            "criado_em", "atualizado_em", "criado_por_usuario_id",
            "atualizado_por_usuario_id", "versao_registro",
        ])
        for field in TECH_NULLABLE:
            self.assertNotRegex(block, rf"(?m)^\s+{field}\s+[^,]+\bNOT NULL\b")

    def test_technical_account_has_no_banking_or_historical_fiction(self):
        section = self.sql.split("-- M0011: uma conta tecnica", 1)[1].split("-- M0011: backfill", 1)[0]
        self.assertRegex(section, r"(?s)'MIGRACAO_LEGADO'.*NULL, NULL, NULL,.*'INATIVA'.*NULL, NULL")
        for placeholder in ("'N/A'", "'0000'"):
            self.assertNotIn(placeholder, section)
        self.assertIn("Registro técnico criado exclusivamente para vinculação", section)
        self.assertNotIn("CURRENT_DATE", section)

    def test_conditional_constraints_confine_the_exception(self):
        for field in ("instituicao", "agencia", "conta"):
            self.assertIn(f"tipo = 'MIGRACAO_LEGADO' AND {field} IS NULL", self.sql)
            self.assertIn(f"tipo <> 'MIGRACAO_LEGADO' AND {field} IS NOT NULL AND btrim({field}) <> ''", self.sql)
        self.assertIn("tipo = 'MIGRACAO_LEGADO' AND abertura_data IS NULL AND encerramento_data IS NULL", self.sql)
        self.assertIn("tipo <> 'MIGRACAO_LEGADO' AND abertura_data IS NOT NULL AND encerramento_data IS NOT NULL", self.sql)
        self.assertIn("conta normal conserva banco preenchido", self.prechecks)

    def test_technical_tolerances_are_all_path_specific(self):
        tech = [item for item in self.tolerances if item["class"] == "TECHACCOUNT_CONDITIONAL"]
        self.assertEqual(len(tech), 9)
        self.assertEqual(
            {item["key"] for item in tech if item["key"].startswith("column|")},
            {f"column|contas_financeiras|{field}" for field in TECH_NULLABLE},
        )
        self.assertTrue(all(item["path"].startswith("public.contas_financeiras.") for item in tech))
        self.assertFalse(any("*" in item["path"] or item["path"] == "public.contas_financeiras" for item in tech))
        for item in tech:
            self.assertTrue(item["compensating_test"])
            self.assertTrue(item["scope"])

    def test_uuid_is_native_unique_and_without_extension(self):
        self.assertIn("pg_catalog.gen_random_uuid()", self.sql)
        self.assertIn("n.nspname = 'pg_catalog'", self.prechecks)
        self.assertNotRegex(self.sql, r"(?i)CREATE\s+EXTENSION")
        self.assertNotIn("uuid_generate", self.sql)
        self.assertIn("uq_transacoes_financeiras__identificador_publico", self.sql)

    def test_association_matching_is_strictly_one_to_one(self):
        for text in ("associacoes", "associacao_aliases", "lower(btrim(tf.associacao))"):
            self.assertIn(text, self.sql)
            self.assertIn(text, self.prechecks)
        self.assertIn("lower(btrim(a.codigo)) = li.token_normalizado", self.prechecks)
        self.assertIn("UNION", self.prechecks)
        self.assertIn("count(*) AS candidate_count", self.prechecks)
        self.assertIn("candidate_count > 1", self.prechecks)
        self.assertIn("candidate_count = 0", self.prechecks)
        self.assertNotRegex(self.sql + self.prechecks, r"(?i)fuzzy|similarity|levenshtein")

    def test_h2d15_fixture_has_deterministic_create_plan_and_full_coverage(self):
        plan = deterministic_association_plan(H2D15_FIXTURE)
        self.assertEqual(plan, {"acan": "CREATE", "ascamar": "CREATE"})
        self.assertEqual(sum(row["quantidade"] for row in H2D15_FIXTURE), 756)

    def test_existing_candidate_is_reused_and_duplicate_paths_are_deduplicated(self):
        association = {"id": 7, "codigo": "ACAN", "nome_normalizado": "acan"}
        aliases = [{"associacao_id": 7, "alias_normalizado": "acan"}]
        self.assertEqual(
            deterministic_association_plan(H2D15_FIXTURE[:1], [association], aliases),
            {"acan": "REUSE"},
        )
        self.assertRegex(self.sql, r"(?s)canonical_candidates AS \(.*?UNION.*?UNION")

    def test_ambiguous_missing_and_conflicting_fixture_cases_fail(self):
        ambiguous_uvr = [
            {"associacao": "ACAN", "uvr": "UVR 01"},
            {"associacao": "ACAN", "uvr": "UVR 02"},
        ]
        two_ids = [
            {"id": 1, "codigo": "ACAN", "nome_normalizado": "acan"},
            {"id": 2, "codigo": "acan", "nome_normalizado": "outra"},
        ]
        divergent = [{"id": 1, "codigo": "OUTRA", "nome_normalizado": "acan"}]
        missing = [{"associacao": "", "uvr": "UVR 02"}]
        self.assertEqual(deterministic_association_plan(ambiguous_uvr)["acan"], "FAIL")
        self.assertEqual(deterministic_association_plan(H2D15_FIXTURE[:1], two_ids)["acan"], "FAIL")
        self.assertEqual(deterministic_association_plan(H2D15_FIXTURE[:1], divergent)["acan"], "FAIL")
        self.assertEqual(deterministic_association_plan(missing)[""], "FAIL")

    def test_same_uvr_for_two_tokens_requires_human_decision(self):
        rows = [
            {"associacao": "ACAN", "uvr": "UVR 02"},
            {"associacao": "OUTRA", "uvr": "UVR 02"},
        ]
        self.assertEqual(deterministic_association_plan(rows), {"acan": "FAIL", "outra": "FAIL"})
        self.assertIn("tokens_por_uvr", self.sql)
        self.assertIn("tokens_por_uvr", self.prechecks)

    def test_historical_code_and_name_are_derived_without_alias_creation(self):
        materialization = self.sql.split("INSERT INTO public.associacoes", 1)[1].split(";", 1)[0]
        self.assertIn("lp.sigla_historica, lp.sigla_historica, lp.token_normalizado", materialization)
        self.assertNotIn("'ACAN'", materialization)
        self.assertNotIn("'ASCAMAR'", materialization)
        self.assertNotRegex(self.sql, r"(?is)INSERT\s+INTO\s+public\.associacao_aliases")

    def test_no_arbitrary_association_selector_remains(self):
        combined = self.sql + "\n" + self.prechecks
        for pattern in (
            r"(?i)min\s*\(\s*(?:candidato\.)?associacao_id",
            r"(?i)max\s*\(\s*(?:candidato\.)?associacao_id",
            r"(?i)\bLIMIT\s+1\b",
            r"(?i)\bDISTINCT\s+ON\b",
            r"(?i)\brow_number\s*\(",
            r"(?i)\bfirst_value\s*\(",
        ):
            self.assertNotRegex(combined, pattern)
        self.assertIn("WHERE n.candidate_count = 1", self.sql)

    def test_r0006_defensively_rechecks_final_cardinality(self):
        section = self.sql.split("DO $r0006_associacoes$", 1)[1].split("$r0006_associacoes$;", 1)[0]
        self.assertIn("COALESCE(cc.candidate_count, 0) <> 1", section)
        self.assertIn("RAISE EXCEPTION 'R0006/ASSOCIACAO", section)
        self.assertIn("INTO STRICT v_usuario_id", section)

    def test_only_receita_and_despesa_seeds_are_supported(self):
        self.assertIn("ARRAY['RECEITA', 'DESPESA']", self.sql)
        self.assertIn("INSERT INTO public.naturezas_financeiras (codigo, nome, nome_normalizado)", self.sql)
        self.assertIn("v_total = 0", self.sql)
        self.assertIn("v_total <> 1 OR v_seguro <> 1", self.sql)
        self.assertIn("NOT IN ('RECEITA','DESPESA')", self.prechecks)

    def test_backfill_is_exactly_the_nine_authorized_columns(self):
        update = self.sql.split("UPDATE public.transacoes_financeiras AS tf", 1)[1].split(";", 1)[0]
        set_clause = update.split(" SET ", 1)[1].split("  FROM ", 1)[0]
        assigned = set(re.findall(r"(?m)^\s*([a-z_]+)\s*=", set_clause))
        self.assertEqual(assigned, NINE_BACKFILLS)
        self.assertIn("competencia_data = tf.data_documento", set_clause)
        self.assertIn("valor_total = tf.valor_total_documento", set_clause)
        self.assertNotIn("numero_documento =", set_clause)

    def test_snapshot_is_versioned_and_whitelisted(self):
        update = self.sql.split("UPDATE public.transacoes_financeiras AS tf", 1)[1].split(";", 1)[0]
        for token in ("'versao', 1", "'origem', 'MIGRACAO_LEGADO'", "'legado', jsonb_build_object"):
            self.assertIn(token, update)
        whitelist = set(re.findall(r"(?m)^\s{15}'([a-z_]+)', tf\.", update))
        self.assertEqual(whitelist, {
            "id", "numero_documento", "data_documento", "tipo_transacao",
            "valor_total_documento", "associacao",
        })

    def test_technical_user_is_reused_and_never_inserted(self):
        self.assertIn("migracao_dados_legados", self.sql)
        self.assertIn("estado = 'BLOQUEADO'", self.sql)
        self.assertNotRegex(self.sql, r"(?is)INSERT\s+INTO\s+public\.usuarios")
        self.assertNotRegex(self.sql, r"(?:criado|atualizado)_por_usuario_id\s*=\s*\d+")

    def test_dml_scope_is_exact(self):
        no_comments = re.sub(r"(?m)^\s*--.*$", "", self.sql)
        inserts = re.findall(r"(?mi)^\s*INSERT\s+INTO\s+([^\s(]+)", no_comments)
        updates = re.findall(r"(?mi)^\s*UPDATE\s+([^\s]+)", no_comments)
        self.assertEqual(inserts, ["public.naturezas_financeiras", "public.associacoes", "public.contas_financeiras"])
        self.assertEqual(updates, ["public.transacoes_financeiras"])
        self.assertNotRegex(no_comments, r"(?mi)^\s*(?:DELETE\s+FROM|TRUNCATE|DROP\s+(?:TABLE|COLUMN))\b")
        self.assertNotRegex(no_comments, r"(?i)\bON\s+CONFLICT\b")

    def test_four_extra_fks_are_prechecked_and_never_modified(self):
        for name in EXTRA_FKS:
            self.assertIn(name, self.prechecks)
            self.assertNotIn(name, self.sql)
        self.assertIn("EXTRA_LEGADO PRESERVADO POR DECISAO NORMATIVA", self.sql)

    def test_m0012_is_exact_normative_sql_and_ordered_last(self):
        normative = M0012.read_text(encoding="utf-8").split("\n", 1)[1].strip()
        self.assertIn(normative, self.sql)
        self.assertLess(self.sql.index("-- M0011"), self.sql.index("-- M0012\n"))
        self.assertNotRegex(self.sql, r"(?m)^-- M0013")
        counts = {table: 0 for table in (
            "patrimonio_bloqueios", "patrimonio_documentos", "patrimonio_eventos",
            "patrimonio_identificadores", "patrimonio_vinculos", "patrimonios",
        )}
        for item in self.reference["absent_objects"]:
            if item.get("migration") == "M0012":
                counts[item["table"]] += 1
        self.assertEqual(counts, {
            "patrimonio_bloqueios": 28, "patrimonio_documentos": 28,
            "patrimonio_eventos": 25, "patrimonio_identificadores": 29,
            "patrimonio_vinculos": 34, "patrimonios": 49,
        })

    def test_prechecks_are_read_only_and_in_authorized_ranges(self):
        ids = re.findall(r"(?m)^-- (P\d+):", self.prechecks)
        self.assertEqual(ids, [*(f"P{i}" for i in range(500, 521)), *(f"P{i}" for i in range(550, 554))])
        no_comments = re.sub(r"(?m)^\s*--.*$", "", self.prechecks)
        statements = [part.strip() for part in no_comments.split(";") if part.strip()]
        self.assertEqual(len(statements), len(ids))
        for statement in statements:
            self.assertRegex(statement, r"(?is)^(SELECT|WITH)\b")
            without_literals = re.sub(r"'(?:''|[^'])*'", "''", statement)
            self.assertNotRegex(without_literals, r"(?is)\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|CALL|DO|COPY)\b")

    def test_scope_excludes_m0013_fc_and_ledger(self):
        executable = re.sub(r"(?m)^\s*--.*$", "", self.sql).lower()
        self.assertNotRegex(executable, r"\bm0013\b")
        self.assertNotRegex(executable, r"\bfc_[a-z0-9_]*\b")
        self.assertNotIn("ledger", executable)
        self.assertNotRegex(executable, r"(?m)^\s*(?:begin|commit|rollback)\s*;")

    def test_r0001_through_r0005_are_unchanged(self):
        expected = {
            "R0001_legacy_to_baseline.sql":"57275057fd1b836ea959eb5de4eba148a3bd59d82ca0c88f2f9a7e3713f0ec67",
            "R0001_prechecks.sql":"b923c7734c785903ab13d95290e309cce66e0d623c363b930f40fb9caf442d49",
            "R0002_legacy_to_baseline.sql":"eea4c46e78abf631bc4bd62d9779f9dd2a34ea1b7fdc70d974e13021bfa7e8d9",
            "R0002_prechecks.sql":"e9f7867f6115ed24bd1ba89fe70c32a4c049a71c7aa4c695f9f558462c1713ef",
            "R0003_legacy_to_baseline.sql":"ea94e951c2a14148379a3a27cca53d5100d99f27c30b5ffdde14b4775c7de3a8",
            "R0003_prechecks.sql":"248493aec4453518700bafaff55b3b7e356bf6d51e211750196b131d0093a033",
            "R0004_M0005_M0008_legacy_to_baseline.sql":"e65cad2de38eccada78a45d5974fd97012d227e9ead9a2f22a3491f287079a05",
            "R0004_M0005_M0008_prechecks.sql":"b6bf0518ec986984d162167e289c083252e2fbf5d2aee76093bcc9b360edc1b7",
            "R0005_M0009_M0010_legacy_to_baseline.sql":"242adf28ce2cf5c450d20f6c47fbcc894d1e04fd48643081012a13f5dff75e34",
            "R0005_M0009_M0010_prechecks.sql":"13e54c3c7b20fa3b31feabc424e5c96c7f5d478617f8c432e7910101ffb459bc",
            "R0005_functional_tolerances.json":"b34dbd7f6ae987f5bec27f60c891eb9ef731beabc6a895dfb0bc4435595eeed2",
        }
        self.assertEqual({name: sha256(RECON / name) for name in expected}, expected)

    def test_normative_m0011_and_manifest_checksums_are_unchanged(self):
        self.assertEqual(sha256(M0011), "ef148a3278b33aa020f4839afb560d7ca21c5e5287005c7179e45624220bf5b8")
        manifest = carregar_manifesto()
        self.assertEqual(sum(item.checksum is not None for item in manifest.operacoes), 24)
        self.assertNotIn("R0006", manifest.por_id())

    def test_plan_documents_atomic_future_target(self):
        section = self.plan.split("## R0006 agrupada / M0011-M0012", 1)[1]
        for token in (
            "MIGRACAO_LEGADO", "377/377", "184/184", "193/193",
            "pg_catalog.gen_random_uuid()", "RECEITA", "DESPESA",
            "uma única transação", "ROLLBACK integral",
        ):
            self.assertIn(token, section)


if __name__ == "__main__":
    unittest.main()
