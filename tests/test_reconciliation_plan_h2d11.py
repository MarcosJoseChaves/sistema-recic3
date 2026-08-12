import hashlib
import json
import re
import unittest
from pathlib import Path

from migrations_control.manifest import carregar_manifesto


ROOT = Path(__file__).resolve().parents[1]
RECON = ROOT / "migrations_control" / "reconciliation"
R0005 = RECON / "R0005_M0009_M0010_legacy_to_baseline.sql"
PRECHECKS = RECON / "R0005_M0009_M0010_prechecks.sql"
TOLERANCES = RECON / "R0005_functional_tolerances.json"
PLAN = ROOT / "PLANO_RECONCILIACAO_DEV_H2D3.md"
REFERENCE = Path(r"C:\sistema-recic3_backups\reconciliacao_h2d2_recon2_20260811_112754.json")
M0009 = ROOT / "migrations_control" / "sql" / "M0009_associados.sql"
M0010 = ROOT / "migrations_control" / "sql" / "M0010_catalogo.sql"

CLASS_B = {
    "numero": "P401",
    "nome": "P401",
    "cpf": "P402",
    "data_nascimento": "P403",
    "telefone": "P401",
    "cep": "P403",
    "logradouro": "P401",
    "endereco_numero": "P401",
    "bairro": "P401",
    "cidade": "P401",
    "uf": "P404",
    "data_admissao": "P403",
}

NEW_ASSOCIADOS_COLUMNS = {
    "nome_normalizado",
    "documento_alternativo",
    "justificativa_sem_cpf",
    "email",
    "estado",
    "condicao_regularizacao",
    "data_desligamento",
    "criado_em",
    "atualizado_em",
    "criado_por_usuario_id",
    "atualizado_por_usuario_id",
    "versao_registro",
}

EXTRA_FKS = {
    "auditoria_associados_id_associado_fkey",
    "auditoria_rateios_id_associado_fkey",
    "auditoria_rateios_transacoes_id_associado_fkey",
    "epi_entregas_id_associado_fkey",
    "epi_entregas_id_responsavel_fkey",
}


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def object_key(item):
    if item["kind"] in {"table", "sequence"}:
        return f"{item['kind']}|{item['name']}"
    return f"{item['kind']}|{item['table']}|{item['name']}"


def tolerates_integer_bigint(tolerances, key):
    return any(item["key"] == key and item["class"] == "C" for item in tolerances)


def classify_unaccent_state(state):
    if (
        state["installed"] == 1
        and state["schema_public"]
        and state["function_exists"]
        and state["resolved"]
        and state["returns_text"]
        and state["executable"]
        and state["extension_owned"]
        and not state["collisions"]
    ):
        return "E1_INSTALADA_FUNCIONAL"
    if (
        state["installed"] == 0
        and state["available"] == 1
        and state["schema_public"]
        and state["database_create"]
        and state["schema_create"]
        and (state["trusted"] or state["superuser"])
        and not state["collisions"]
    ):
        return "E2_AUSENTE_INSTALAVEL"
    return "E3_BLOQUEIO"


class TestReconciliationPlanH2D11(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.sql = R0005.read_text(encoding="utf-8")
        cls.prechecks = PRECHECKS.read_text(encoding="utf-8")
        cls.plan = PLAN.read_text(encoding="utf-8")
        cls.reference = json.loads(REFERENCE.read_text(encoding="utf-8"))
        cls.tolerances = json.loads(TOLERANCES.read_text(encoding="utf-8"))["tolerances"]

    def test_approved_reference_and_migration_totals(self):
        self.assertEqual(sha256(REFERENCE), "24641c2e3fbd95d3ce9e48a02347c71682c4dc52449eb5838c48e29f28d39da6")
        summaries = {item["migration"]: item for item in self.reference["migrations"]}
        self.assertEqual(
            {key: summaries["M0009"][key] for key in ("expected", "equivalent", "absent", "divergent")},
            {"expected": 196, "equivalent": 1, "absent": 181, "divergent": 14},
        )
        self.assertEqual(
            {key: summaries["M0010"][key] for key in ("expected", "equivalent", "absent", "divergent")},
            {"expected": 221, "equivalent": 0, "absent": 221, "divergent": 0},
        )

    def test_all_181_m0009_absences_are_assigned_once(self):
        expected = {
            object_key(item)
            for item in self.reference["absent_objects"]
            if item.get("migration") == "M0009"
        }
        assignments = re.findall(
            r"(?m)^-- H2D11-ABSENT (?:MATERIALIZE|PRESERVE_EQUIVALENT): (.+)$",
            self.sql,
        )
        self.assertEqual(len(expected), 181)
        self.assertEqual(len(assignments), 181)
        self.assertEqual(set(assignments), expected)

    def test_twelve_class_b_divergences_have_prechecks(self):
        markers = dict(re.findall(
            r"(?m)^-- H2D11-CLASS-B: column\|associados\|(\w+) PRECHECK=(P\d+)$",
            self.sql,
        ))
        self.assertEqual(markers, CLASS_B)
        for precheck in set(markers.values()):
            self.assertRegex(self.prechecks, rf"(?m)^-- {precheck}:")

    def test_only_two_path_specific_class_c_tolerances_exist(self):
        self.assertEqual(
            {(item["key"], item["path"], item["baseline_type"], item["legacy_type"]) for item in self.tolerances},
            {
                ("column|associados|id", "public.associados.id", "bigint", "integer"),
                ("sequence|associados_id_seq", "public.associados_id_seq", "bigint", "integer"),
            },
        )
        self.assertTrue(tolerates_integer_bigint(self.tolerances, "column|associados|id"))
        self.assertTrue(tolerates_integer_bigint(self.tolerances, "sequence|associados_id_seq"))
        self.assertFalse(tolerates_integer_bigint(self.tolerances, "column|qualquer_outra|id"))
        self.assertNotRegex(self.sql, r"(?is)ALTER\s+TABLE\s+(?:public\.)?associados\s+ALTER\s+COLUMN\s+id\b")
        self.assertNotRegex(self.sql, r"(?is)ALTER\s+SEQUENCE\s+(?:public\.)?associados_id_seq\b")

    def test_twelve_new_associados_columns_and_only_three_backfills(self):
        absent_columns = {
            item["name"]
            for item in self.reference["absent_objects"]
            if item.get("migration") == "M0009"
            and item.get("table") == "associados"
            and item.get("kind") == "column"
        }
        self.assertEqual(absent_columns, NEW_ASSOCIADOS_COLUMNS)
        update = re.search(r"(?is)UPDATE\s+public\.associados\s+SET\s+(.+?)\s*;", self.sql).group(1)
        assigned = set(re.findall(r"(?m)^\s*([a-z_]+)\s*=", update))
        self.assertEqual(assigned, {"nome_normalizado", "criado_por_usuario_id", "atualizado_por_usuario_id"})

    def test_exact_name_normalization_is_used_and_prechecked(self):
        expression = "lower(unaccent(btrim(nome)))"
        self.assertIn(expression, self.sql)
        self.assertIn(expression, self.prechecks)
        self.assertIn("to_regprocedure('public.unaccent(text)')", self.prechecks)

    def test_p405_is_read_only_catalog_classification_without_invocation(self):
        p405 = self.prechecks.split("-- P405:", 1)[1].split("-- P406:", 1)[0]
        self.assertRegex(p405, r"(?is)^.*WITH\s+contexto\s+AS")
        self.assertIn("pg_catalog.pg_extension", p405)
        self.assertIn("pg_catalog.pg_available_extensions", p405)
        self.assertIn("pg_catalog.pg_available_extension_versions", p405)
        self.assertIn("pg_catalog.pg_depend", p405)
        self.assertIn("has_database_privilege", p405)
        self.assertIn("has_schema_privilege", p405)
        self.assertNotIn("lower(unaccent(btrim(nome)))", p405)
        p405_without_literals = re.sub(r"'(?:''|[^'])*'", "''", p405)
        self.assertNotRegex(p405_without_literals, r"(?i)\b(CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|CALL|DO)\b")

    def test_p405_distinguishes_e1_e2_and_e3(self):
        base = {
            "installed": 0,
            "available": 1,
            "schema_public": True,
            "function_exists": False,
            "resolved": False,
            "returns_text": False,
            "executable": False,
            "extension_owned": False,
            "database_create": True,
            "schema_create": True,
            "trusted": True,
            "superuser": False,
            "collisions": False,
        }
        e1 = dict(base, installed=1, function_exists=True, resolved=True,
                  returns_text=True, executable=True, extension_owned=True)
        self.assertEqual(classify_unaccent_state(e1), "E1_INSTALADA_FUNCIONAL")
        self.assertEqual(classify_unaccent_state(base), "E2_AUSENTE_INSTALAVEL")
        self.assertEqual(classify_unaccent_state(dict(base, collisions=True)), "E3_BLOQUEIO")
        self.assertEqual(classify_unaccent_state(dict(base, database_create=False)), "E3_BLOQUEIO")
        for label in ("E1_INSTALADA_FUNCIONAL", "E2_AUSENTE_INSTALAVEL", "E3_BLOQUEIO"):
            self.assertIn(label, self.prechecks)
        self.assertRegex(
            self.prechecks,
            r"(?s)WHEN extensoes_instaladas = 0.*funcao_resolvida_oid IS NULL.*THEN 'E2_AUSENTE_INSTALAVEL'",
        )

    def test_extension_installation_has_single_executor_responsibility(self):
        command = "CREATE EXTENSION unaccent WITH SCHEMA public;"
        self.assertNotIn("CREATE EXTENSION", self.sql)
        self.assertNotIn("CREATE EXTENSION", self.prechecks)
        self.assertEqual(self.plan.count(command), 1)
        self.assertNotRegex(self.sql + self.prechecks + self.plan, r"(?i)CREATE\s+EXTENSION\s+IF\s+NOT\s+EXISTS")
        self.assertIn("futuro executor H2D.12 é a única camada responsável", self.plan)

    def test_e1_e2_e3_future_transaction_order_and_rollback(self):
        section = self.plan.split("### Dependência `unaccent`", 1)[1].split("## Ledger", 1)[0]
        self.assertRegex(section, r"(?s)P400-P405.*transação read-only.*encerra.*E3 para")
        self.assertRegex(section, r"(?s)transação principal.*E2.*instala.*P406-P416.*P450-P454.*R0005")
        self.assertRegex(section, r"ROLLBACK\s+integral, inclusive da extensão no E2")
        self.assertIn("E1 abre a transação principal sem instalar nada", section)

    def test_extension_objects_are_authorized_infrastructure_outside_417(self):
        section = self.plan.split("### Dependência `unaccent`", 1)[1].split("## Ledger", 1)[0]
        self.assertRegex(section, r"não\s+contam nos 417")
        self.assertRegex(section, r"INFRAESTRUTURA AUTORIZADA DA\s+RECONCILIAÇÃO — EXTENSÃO unaccent")

    def test_technical_user_u1_u2_u3_u4_and_authentication_lock(self):
        self.assertIn("'migracao_dados_legados'", self.sql)
        self.assertIn("'Migração de dados legados'", self.sql)
        self.assertRegex(self.sql, r"(?is)IF\s+v_candidatos\s*=\s*0\s+THEN\s+INSERT INTO public\.usuarios")
        self.assertRegex(self.sql, r"(?is)ELSIF\s+v_candidatos\s*=\s*1\s+THEN")
        self.assertIn("R0005/U3", self.sql)
        self.assertIn("R0005/U4", self.sql)
        self.assertIn("'BLOQUEADO'", self.sql)
        self.assertRegex(self.sql, r"(?is)'migracao'\s*,\s*NULL\s*,\s*FALSE")
        app = (ROOT / "app.py").read_text(encoding="utf-8")
        self.assertIn("FROM usuarios WHERE username = %s AND ativo = TRUE", app)
        self.assertIn("NOT EXISTS", self.sql)
        self.assertIn("public.auth_usuario_perfis", self.sql)

    def test_technical_user_id_is_returned_not_hardcoded(self):
        self.assertIn("RETURNING id INTO v_usuario_id", self.sql)
        self.assertIn("criado_por_usuario_id = v_usuario_id", self.sql)
        self.assertIn("atualizado_por_usuario_id = v_usuario_id", self.sql)
        self.assertNotRegex(self.sql, r"(?i)(?:criado|atualizado)_por_usuario_id\s*=\s*\d+")

    def test_dml_is_strictly_scoped(self):
        no_comments = re.sub(r"(?m)^\s*--.*$", "", self.sql)
        inserts = re.findall(r"(?mi)^\s*INSERT\s+INTO\s+([^\s(]+)", no_comments)
        updates = re.findall(r"(?mi)^\s*UPDATE\s+([^\s]+)", no_comments)
        self.assertEqual(inserts, ["public.usuarios"])
        self.assertEqual(updates, ["public.associados"])
        self.assertNotRegex(no_comments, r"(?mi)^\s*(?:DELETE\s+FROM|TRUNCATE|DROP\s+(?:TABLE|COLUMN))\b")
        self.assertNotRegex(no_comments, r"(?i)\bON\s+CONFLICT\b")

    def test_extra_legacy_foreign_keys_are_only_read_by_prechecks(self):
        for name in EXTRA_FKS:
            self.assertNotIn(name, self.sql)
            self.assertIn(name, self.prechecks)
        self.assertIn("EXTRA_LEGADO PRESERVADO POR DECISAO NORMATIVA", self.sql)

    def test_m0010_is_exact_normative_sql_and_ordered_after_m0009(self):
        normative = M0010.read_text(encoding="utf-8").strip()
        self.assertIn(normative, self.sql)
        self.assertLess(self.sql.index("-- M0009"), self.sql.index("-- M0010\n"))
        absent = [item for item in self.reference["absent_objects"] if item.get("migration") == "M0010"]
        by_kind = {kind: sum(item["kind"] == kind for item in absent) for kind in {item["kind"] for item in absent}}
        self.assertEqual(by_kind, {"table": 7, "column": 90, "sequence": 7, "constraint": 82, "index": 35})

    def test_r0005_scope_and_future_atomicity(self):
        lowered = self.sql.lower()
        self.assertNotRegex(lowered, r"\bm001[1-3]\b")
        self.assertNotRegex(lowered, r"\bfc_[a-z0-9_]*\b")
        self.assertNotIn("ledger", lowered)
        self.assertNotRegex(lowered, r"(?m)^\s*(?:begin|commit|rollback)\s*;")
        self.assertIn("uma unica transacao", lowered)
        self.assertIn("rollback integral", lowered)

    def test_prechecks_are_read_only_and_cover_p400_p450_ranges(self):
        ids = re.findall(r"(?m)^-- (P\d+):", self.prechecks)
        self.assertEqual(ids, [
            "P400", "P401", "P402", "P403", "P404", "P405", "P406", "P407",
            "P408", "P409", "P410", "P411", "P412", "P413", "P414", "P415",
            "P416", "P450", "P451", "P452", "P453", "P454",
        ])
        no_comments = re.sub(r"(?m)^\s*--.*$", "", self.prechecks)
        statements = [part.strip() for part in no_comments.split(";") if part.strip()]
        self.assertEqual(len(statements), len(ids))
        for statement in statements:
            self.assertRegex(statement, r"(?is)^(SELECT|WITH)\b")
            statement_without_literals = re.sub(r"'(?:''|[^'])*'", "''", statement)
            self.assertNotRegex(statement_without_literals, r"(?is)\b(INSERT|UPDATE|DELETE|MERGE|CREATE|ALTER|DROP|TRUNCATE|CALL|DO|COPY)\b")

    def test_r0001_through_r0004_are_unchanged(self):
        expected = {
            "R0001_legacy_to_baseline.sql": "57275057fd1b836ea959eb5de4eba148a3bd59d82ca0c88f2f9a7e3713f0ec67",
            "R0001_prechecks.sql": "b923c7734c785903ab13d95290e309cce66e0d623c363b930f40fb9caf442d49",
            "R0002_legacy_to_baseline.sql": "eea4c46e78abf631bc4bd62d9779f9dd2a34ea1b7fdc70d974e13021bfa7e8d9",
            "R0002_prechecks.sql": "e9f7867f6115ed24bd1ba89fe70c32a4c049a71c7aa4c695f9f558462c1713ef",
            "R0003_legacy_to_baseline.sql": "ea94e951c2a14148379a3a27cca53d5100d99f27c30b5ffdde14b4775c7de3a8",
            "R0003_prechecks.sql": "248493aec4453518700bafaff55b3b7e356bf6d51e211750196b131d0093a033",
            "R0004_M0005_M0008_legacy_to_baseline.sql": "e65cad2de38eccada78a45d5974fd97012d227e9ead9a2f22a3491f287079a05",
            "R0004_M0005_M0008_prechecks.sql": "b6bf0518ec986984d162167e289c083252e2fbf5d2aee76093bcc9b360edc1b7",
        }
        self.assertEqual({name: sha256(RECON / name) for name in expected}, expected)

    def test_24_normal_manifest_checksums_remain_valid(self):
        manifest = carregar_manifesto()
        self.assertEqual(sum(item.checksum is not None for item in manifest.operacoes), 24)
        self.assertNotIn("R0005", manifest.por_id())

    def test_plan_documents_417_target_and_preservation_exceptions(self):
        section = self.plan.split("## R0005 agrupada / M0009-M0010", 1)[1]
        for token in (
            "417/417", "U1", "U2", "U3", "U4", "nome_normalizado",
            "criado_por_usuario_id", "atualizado_por_usuario_id",
            "EXTRA_LEGADO PRESERVADO POR DECISÃO NORMATIVA",
        ):
            self.assertIn(token, section)


if __name__ == "__main__":
    unittest.main()
