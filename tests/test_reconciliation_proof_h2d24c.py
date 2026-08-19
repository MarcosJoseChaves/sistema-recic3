import ast
import hashlib
import inspect
import json
import re
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path
from types import MappingProxyType

from migrations_control import reconciliation_catalog as catalog_module
from migrations_control import reconciliation_proof as proof_module
from migrations_control import reconciliation_runtime_rules as rules_module
from migrations_control import reconciliation_spec as spec_module
from migrations_control.reconciliation_proof import (
    POST,
    PRE,
    CatalogProof,
    comparar_catalogo,
    provar_catalogo_normativo_completo,
    provar_legado_reconciliado_para_adocao,
)
from migrations_control.reconciliation_runtime_rules import (
    ALIASES,
    RUNTIME_RULES,
    TOLERANCES,
    AliasRule,
    RuntimeRules,
)
from migrations_control.reconciliation_spec import (
    CatalogObject,
    carregar_catalog_spec_v1,
    deep_freeze,
    deep_thaw,
    sha256_canonico,
)


ROOT = Path(__file__).resolve().parents[1]
NEW_FILES = (
    ROOT / "migrations_control" / "reconciliation_spec.py",
    ROOT / "migrations_control" / "reconciliation_runtime_rules.py",
    ROOT / "migrations_control" / "reconciliation_proof.py",
    ROOT / "tests" / "test_reconciliation_proof_h2d24c.py",
)
EXPECTED_NEW_EVIDENCE = "c739b435f3d494c6cce12c86fb1e461151e10cfd8d9074ab4f48b4a34595baa6"
CLASS_C_KEYS = (
    "column|usuarios|id",
    "sequence|usuarios_id_seq",
    "column|associados|id",
    "sequence|associados_id_seq",
    "column|transacoes_financeiras|id",
    "sequence|transacoes_financeiras_id_seq",
    "column|solicitacoes_alteracao|id",
    "sequence|solicitacoes_alteracao_id_seq",
)


class FakeCursor:
    def __init__(self, connection):
        self.connection = connection

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, params=None):
        self.connection.queries.append((query, params))

    def fetchall(self):
        return list(self.connection.rows)


class FakeConnection:
    def __init__(self, rows):
        self.rows = rows
        self.queries = []
        self.cursor_calls = 0

    def cursor(self):
        self.cursor_calls += 1
        return FakeCursor(self)


def _actual(obj, *, key=None, attrs=None):
    return CatalogObject(
        migration_id="",
        category=(key or obj.logical_key).split("|", 1)[0],
        logical_key=key or obj.logical_key,
        attributes=deep_freeze(deep_thaw(obj.attributes) if attrs is None else attrs),
    )


def synthetic_objects(mode, *, aliases=True, tolerances=True, extras=True):
    spec = carregar_catalog_spec_v1()
    objects = {
        obj.logical_key: _actual(obj)
        for migration in spec.migrations
        if not (mode == PRE and migration.migration_id == "M0001")
        for obj in migration.objects
    }
    if aliases:
        for rule in ALIASES:
            expected = spec.by_key[rule.expected_logical_key]
            attrs = deep_thaw(expected.attributes)
            if expected.category == "index":
                old = expected.logical_key.rsplit("|", 1)[1]
                new = rule.accepted_actual_logical_key.rsplit("|", 1)[1]
                attrs["definition"] = attrs["definition"].replace(
                    f"INDEX {old} ON ", f"INDEX {new} ON ", 1
                )
            objects.pop(expected.logical_key, None)
            objects[rule.accepted_actual_logical_key] = _actual(
                expected, key=rule.accepted_actual_logical_key, attrs=attrs
            )
    if tolerances:
        for rule in TOLERANCES:
            if rule.extra_legacy:
                objects[rule.logical_key] = CatalogObject(
                    migration_id="", category=rule.category,
                    logical_key=rule.logical_key, attributes=rule.accepted_attributes,
                )
                continue
            expected = spec.by_key[rule.logical_key]
            key = rule.accepted_actual_logical_key if hasattr(rule, "accepted_actual_logical_key") else rule.logical_key
            selected = objects.get(key) or objects.get(rule.logical_key)
            if selected is None:
                continue
            attrs = deep_thaw(expected.attributes)
            attrs.update(deep_thaw(rule.accepted_changes))
            objects[selected.logical_key] = CatalogObject(
                migration_id="", category=selected.category,
                logical_key=selected.logical_key, attributes=deep_freeze(attrs),
            )
    if extras:
        objects["table|extra_legado_h2d24c"] = CatalogObject(
            migration_id="", category="table", logical_key="table|extra_legado_h2d24c",
            attributes=deep_freeze({"relkind": "r", "persistence": "p"}),
        )
    return tuple(objects[key] for key in sorted(objects))


def rows_for(mode, **kwargs):
    return tuple(
        (obj.logical_key, obj.category, deep_thaw(obj.attributes))
        for obj in synthetic_objects(mode, **kwargs)
    )


def proof_for(mode, **kwargs):
    connection = FakeConnection(rows_for(mode, **kwargs))
    api = provar_legado_reconciliado_para_adocao if mode == PRE else provar_catalogo_normativo_completo
    return api(connection), connection


class LoaderAndModelsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spec = carregar_catalog_spec_v1()

    def test_01_strict_loader_counts(self):
        self.assertEqual((len(self.spec.migrations), len(self.spec.objects)), (24, 2355))

    def test_02_strict_loader_chain(self):
        self.assertEqual(tuple(self.spec.by_migration), spec_module.MIGRATION_IDS)

    def test_03_m0000_absent(self):
        self.assertNotIn("M0000", self.spec.by_migration)

    def test_04_categories_exact(self):
        self.assertEqual(self.spec.categories, spec_module.VALID_CATEGORIES)

    def test_05_spec_sha(self):
        self.assertEqual(self.spec.spec_sha256, spec_module.SPEC_SHA256)

    def test_06_manifest_sha(self):
        self.assertEqual(self.spec.manifest_sha256, spec_module.MANIFEST_SHA256)

    def test_07_normative_fingerprint(self):
        self.assertEqual(self.spec.normative_fingerprint, spec_module.SPEC_NORMATIVE_FINGERPRINT)

    def test_08_object_keys_unique(self):
        self.assertEqual(len(self.spec.by_key), 2355)

    def test_09_migration_models_frozen(self):
        with self.assertRaises(FrozenInstanceError):
            self.spec.migrations[0].migration_id = "X"

    def test_10_object_models_frozen(self):
        with self.assertRaises(FrozenInstanceError):
            self.spec.objects[0].category = "X"

    def test_11_nested_attributes_frozen(self):
        with self.assertRaises(TypeError):
            self.spec.objects[0].attributes["x"] = 1

    def test_12_migrations_tuple(self):
        self.assertIsInstance(self.spec.migrations, tuple)

    def test_13_adjustment_frozen(self):
        self.assertIsInstance(self.spec.ownership_adjustments[0], MappingProxyType)

    def test_14_adjustment_normative_h006(self):
        key = "index|fc_aditivos|uq_fc_aditivos_id_contrato_id"
        self.assertEqual(self.spec.by_key[key].migration_id, "H006")

    def test_15_no_volatile_fields(self):
        for obj in self.spec.objects:
            self.assertTrue({"oid", "objid", "relfilenode"}.isdisjoint(obj.attributes))


class RuntimeRulesTests(unittest.TestCase):
    def test_16_alias_count(self):
        self.assertEqual(len(ALIASES), 8)

    def test_17_new_alias_count(self):
        self.assertEqual(sum(a.evidence_sha256 == EXPECTED_NEW_EVIDENCE for a in ALIASES), 6)

    def test_18_r0007_alias_count(self):
        self.assertEqual(sum(a.provenance == "R0007" for a in ALIASES), 2)

    def test_19_alias_actuals_unique(self):
        self.assertEqual(len({a.accepted_actual_logical_key for a in ALIASES}), 8)

    def test_20_alias_expecteds_unique(self):
        self.assertEqual(len({a.expected_logical_key for a in ALIASES}), 8)

    def test_21_zero_wildcards(self):
        self.assertFalse(any("*" in a.expected_logical_key or "*" in a.accepted_actual_logical_key for a in ALIASES))

    def test_22_zero_regex(self):
        self.assertFalse(any(re.search(r"[\[\]()?+]", a.accepted_actual_logical_key) for a in ALIASES))

    def test_23_zero_fallback(self):
        text = Path(rules_module.__file__).read_text(encoding="utf-8").casefold()
        self.assertNotIn("endswith", text)
        self.assertNotIn("casefold", text)

    def test_24_tolerance_count(self):
        self.assertEqual(len(TOLERANCES), 17)

    def test_25_class_c_paths(self):
        self.assertEqual(tuple(t.logical_key for t in TOLERANCES if t.tolerance_class == "C"), CLASS_C_KEYS)

    def test_26_techaccount_count(self):
        self.assertEqual(sum(t.tolerance_class == "TECHACCOUNT_CONDITIONAL" for t in TOLERANCES), 9)

    def test_27_no_global_integer_bigint(self):
        self.assertTrue(all("|" in t.logical_key for t in TOLERANCES))

    def test_28_r0001_r0003_r0004_have_no_rules(self):
        self.assertTrue({"M0001", "M0004", "M0005", "M0006", "M0007", "M0008"}.isdisjoint(
            {a.migration_id for a in ALIASES} | {t.migration_id for t in TOLERANCES}
        ))

    def test_103_r0005_inventory_matches_runtime(self):
        payload = json.loads((ROOT / "migrations_control/reconciliation/R0005_functional_tolerances.json").read_text(encoding="utf-8"))
        self.assertEqual(
            {item["key"] for item in payload["tolerances"]},
            {rule.logical_key for rule in TOLERANCES if rule.provenance == "R0005"},
        )

    def test_104_r0006_inventory_matches_runtime(self):
        payload = json.loads((ROOT / "migrations_control/reconciliation/R0006_functional_tolerances.json").read_text(encoding="utf-8"))
        self.assertEqual(
            {item["key"] for item in payload["tolerances"]},
            {rule.logical_key for rule in TOLERANCES if rule.provenance == "R0006"},
        )

    def test_105_r0007_inventory_splits_two_tolerances_two_aliases(self):
        payload = json.loads((ROOT / "migrations_control/reconciliation/R0007_functional_tolerances.json").read_text(encoding="utf-8"))
        class_c = {item["key"] for item in payload["tolerances"] if item["class"] == "C"}
        aliases = {
            item["key"]: item["key"].split("|", 1)[0] + "|" +
            item["reconciled_path"].replace("public.", "", 1).replace(".", "|", 1)
            for item in payload["tolerances"]
            if item["class"] == "LEGACY_PK_NAME_EQUIVALENCE"
        }
        self.assertEqual(class_c, {rule.logical_key for rule in TOLERANCES if rule.provenance == "R0007"})
        self.assertEqual(aliases, {rule.expected_logical_key: rule.accepted_actual_logical_key for rule in ALIASES if rule.provenance == "R0007"})


class CollectorTests(unittest.TestCase):
    def test_29_collector_uses_received_connection(self):
        conn = FakeConnection(rows_for(PRE))
        catalog_module.coletar_catalogo_reconciliacao(conn)
        self.assertEqual(conn.cursor_calls, 1)

    def test_30_collector_executes_one_query(self):
        conn = FakeConnection(rows_for(PRE))
        catalog_module.coletar_catalogo_reconciliacao(conn)
        self.assertEqual(len(conn.queries), 1)

    def test_31_collector_query_is_with_select(self):
        query = catalog_module.RECONCILIATION_CATALOG_SQL
        clean = re.sub(r"/\*.*?\*/", "", query, flags=re.S).lstrip()
        self.assertTrue(clean.upper().startswith("WITH "))
        self.assertIn("SELECT ", clean.upper())

    def test_32_collector_has_no_write_sql(self):
        query = catalog_module.RECONCILIATION_CATALOG_SQL.upper()
        for token in ("INSERT ", "UPDATE ", "DELETE ", "MERGE ", "CREATE ", "ALTER ", "DROP ", "TRUNCATE ", "COMMIT", "ROLLBACK", " SET "):
            self.assertNotIn(token, query)

    def test_33_collector_schema_is_parameterized(self):
        conn = FakeConnection(rows_for(PRE))
        catalog_module.coletar_catalogo_reconciliacao(conn)
        self.assertEqual(conn.queries[0][1], ("public",) * 5)

    def test_34_collector_five_categories(self):
        conn = FakeConnection(rows_for(POST, aliases=False, tolerances=False, extras=False))
        objects = catalog_module.coletar_catalogo_reconciliacao(conn)
        self.assertEqual({o.category for o in objects}, spec_module.VALID_CATEGORIES)

    def test_35_collector_rejects_duplicate_key(self):
        row = rows_for(PRE)[0]
        with self.assertRaises(ValueError):
            catalog_module.coletar_catalogo_reconciliacao(FakeConnection((row, row)))

    def test_36_collector_rejects_volatile_attribute(self):
        with self.assertRaises(ValueError):
            catalog_module.coletar_catalogo_reconciliacao(FakeConnection((("table|x", "table", {"oid": 1}),)))

    def test_37_collector_output_is_immutable(self):
        obj = catalog_module.coletar_catalogo_reconciliacao(FakeConnection(rows_for(PRE)))[0]
        with self.assertRaises(TypeError):
            obj.attributes["x"] = 1

    def test_38_collector_does_not_expose_oid(self):
        objects = catalog_module.coletar_catalogo_reconciliacao(FakeConnection(rows_for(PRE)))
        self.assertTrue(all("oid" not in o.attributes for o in objects))


class ProofHappyPathTests(unittest.TestCase):
    def test_39_pre_full_path_23_of_23(self):
        proof, _ = proof_for(PRE)
        self.assertTrue(proof.global_result)
        self.assertEqual((proof.candidate_functional, proof.candidate_total), (23, 23))

    def test_40_pre_m0001_absent(self):
        proof, _ = proof_for(PRE)
        self.assertEqual(proof.m0001_state, "AUSENTE")

    def test_41_post_full_path_24_of_24(self):
        proof, _ = proof_for(POST)
        self.assertTrue(proof.global_result)
        self.assertEqual((proof.candidate_functional, proof.candidate_total), (24, 24))

    def test_42_post_m0001_complete(self):
        proof, _ = proof_for(POST)
        self.assertEqual(proof.m0001_state, "PRESENTE_COMPLETO")

    def test_43_pre_and_post_auto_collect(self):
        pre, pre_conn = proof_for(PRE)
        post, post_conn = proof_for(POST)
        self.assertTrue(pre.global_result and post.global_result)
        self.assertEqual((len(pre_conn.queries), len(post_conn.queries)), (1, 1))

    def test_44_all_eight_aliases_used(self):
        proof, _ = proof_for(POST)
        used = {key for result in proof.migration_results for key in result.aliases_used}
        self.assertEqual(used, {a.accepted_actual_logical_key for a in ALIASES})

    def test_45_extras_reported_not_blocking(self):
        proof, _ = proof_for(PRE)
        self.assertTrue(proof.global_result)
        self.assertEqual([o.logical_key for o in proof.extras_legacy], ["table|extra_legado_h2d24c"])

    def test_46_extra_tolerance_not_reported_as_extra(self):
        proof, _ = proof_for(PRE)
        self.assertNotIn("constraint|contas_financeiras|ck_contas_financeiras__datas_modalidade", {o.logical_key for o in proof.extras_legacy})

    def test_47_post_projection_fingerprint_is_normative(self):
        proof, _ = proof_for(POST)
        self.assertEqual(proof.functional_projection_fingerprint, spec_module.SPEC_NORMATIVE_FINGERPRINT)

    def test_48_pre_projection_fingerprint_is_normative_subset(self):
        spec = carregar_catalog_spec_v1()
        expected = [obj for m in spec.migrations if m.migration_id != "M0001" for obj in m.objects]
        proof, _ = proof_for(PRE)
        self.assertEqual(proof.functional_projection_fingerprint, proof_module._projection_fingerprint(expected))

    def test_49_snapshot_fingerprint_deterministic(self):
        a, _ = proof_for(PRE)
        b, _ = proof_for(PRE)
        self.assertEqual(a.snapshot_fingerprint, b.snapshot_fingerprint)

    def test_50_proof_frozen(self):
        proof, _ = proof_for(PRE)
        with self.assertRaises(FrozenInstanceError):
            proof.mode = POST

    def test_51_migration_results_tuple(self):
        proof, _ = proof_for(PRE)
        self.assertIsInstance(proof.migration_results, tuple)


class ProofNegativeTests(unittest.TestCase):
    def _compare(self, objects, mode=PRE, rules=RUNTIME_RULES):
        return comparar_catalogo(objects, carregar_catalog_spec_v1(), mode, rules)

    def test_52_pre_one_of_46_m0001_fails(self):
        spec = carregar_catalog_spec_v1()
        objects = list(synthetic_objects(PRE)) + [_actual(spec.by_migration["M0001"].objects[0])]
        proof = self._compare(objects)
        self.assertFalse(proof.global_result)
        self.assertEqual(proof.m0001_state, "PARCIAL")

    def test_53_pre_45_of_46_m0001_fails(self):
        spec = carregar_catalog_spec_v1()
        objects = list(synthetic_objects(PRE)) + [_actual(o) for o in spec.by_migration["M0001"].objects[:45]]
        self.assertFalse(self._compare(objects).global_result)

    def test_54_pre_46_of_46_m0001_fails(self):
        spec = carregar_catalog_spec_v1()
        objects = list(synthetic_objects(PRE)) + [_actual(o) for o in spec.by_migration["M0001"].objects]
        proof = self._compare(objects)
        self.assertFalse(proof.global_result)
        self.assertEqual(proof.m0001_state, "PRESENTE_COMPLETO")

    def test_55_post_m0001_absent_fails(self):
        proof = self._compare(synthetic_objects(PRE), POST)
        self.assertFalse(proof.global_result)
        self.assertEqual(proof.m0001_state, "AUSENTE")

    def test_56_post_m0001_partial_fails(self):
        spec = carregar_catalog_spec_v1()
        objects = list(synthetic_objects(PRE)) + [_actual(spec.by_migration["M0001"].objects[0])]
        self.assertFalse(self._compare(objects, POST).global_result)

    def test_57_similar_unapproved_alias_fails(self):
        objects = list(synthetic_objects(PRE))
        target = "constraint|usuarios|usuarios_pkey"
        obj = next(o for o in objects if o.logical_key == target)
        objects.remove(obj)
        objects.append(_actual(obj, key=target + "_x"))
        self.assertFalse(self._compare(objects).global_result)

    def test_58_alias_case_difference_fails(self):
        objects = list(synthetic_objects(PRE))
        target = "index|usuarios|usuarios_pkey"
        obj = next(o for o in objects if o.logical_key == target)
        objects.remove(obj)
        objects.append(_actual(obj, key="index|usuarios|USUARIOS_PKEY"))
        self.assertFalse(self._compare(objects).global_result)

    def test_59_same_suffix_other_table_fails(self):
        objects = list(synthetic_objects(PRE))
        target = "constraint|associados|associados_pkey"
        obj = next(o for o in objects if o.logical_key == target)
        objects.remove(obj)
        objects.append(_actual(obj, key="constraint|outra|associados_pkey"))
        self.assertFalse(self._compare(objects).global_result)

    def test_60_exact_plus_alias_is_ambiguous(self):
        spec = carregar_catalog_spec_v1()
        objects = list(synthetic_objects(PRE))
        objects.append(_actual(spec.by_key["constraint|usuarios|pk_usuarios"]))
        self.assertFalse(self._compare(objects).global_result)

    def test_61_alias_mapping_two_candidates_rejected(self):
        aliases = list(ALIASES)
        aliases[1] = AliasRule(
            aliases[1].migration_id, aliases[1].category, aliases[1].expected_logical_key,
            aliases[0].accepted_actual_logical_key, aliases[1].provenance, aliases[1].evidence_sha256,
        )
        with self.assertRaises(ValueError):
            self._compare(synthetic_objects(PRE), rules=RuntimeRules(tuple(aliases), TOLERANCES))

    def test_62_missing_expected_not_replaced_by_extra(self):
        objects = [o for o in synthetic_objects(PRE, aliases=False) if o.logical_key != "table|auth_acoes"]
        self.assertFalse(self._compare(objects).global_result)

    def test_63_class_c_same_value_other_path_fails(self):
        objects = list(synthetic_objects(PRE))
        key = "column|associado_associacao_vinculos|id"
        obj = next(o for o in objects if o.logical_key == key)
        attrs = deep_thaw(obj.attributes); attrs["type"] = "integer"
        objects[objects.index(obj)] = _actual(obj, attrs=attrs)
        self.assertFalse(self._compare(objects).global_result)

    def test_64_class_c_second_difference_fails(self):
        objects = list(synthetic_objects(PRE))
        key = "column|associados|id"
        obj = next(o for o in objects if o.logical_key == key)
        attrs = deep_thaw(obj.attributes); attrs["not_null"] = False
        objects[objects.index(obj)] = _actual(obj, attrs=attrs)
        self.assertFalse(self._compare(objects).global_result)

    def test_65_class_c_different_value_fails(self):
        objects = list(synthetic_objects(PRE))
        key = "sequence|associados_id_seq"
        obj = next(o for o in objects if o.logical_key == key)
        attrs = deep_thaw(obj.attributes); attrs["max"] = 999
        objects[objects.index(obj)] = _actual(obj, attrs=attrs)
        self.assertFalse(self._compare(objects).global_result)

    def test_66_techaccount_nullability_other_path_fails(self):
        objects = list(synthetic_objects(PRE))
        key = "column|contas_financeiras|nome"
        obj = next(o for o in objects if o.logical_key == key)
        attrs = deep_thaw(obj.attributes); attrs["not_null"] = False
        objects[objects.index(obj)] = _actual(obj, attrs=attrs)
        self.assertFalse(self._compare(objects).global_result)

    def test_67_techaccount_constraint_relaxation_other_path_fails(self):
        objects = list(synthetic_objects(PRE))
        key = "constraint|contas_financeiras|ck_contas_financeiras__nome_preenchido"
        obj = next(o for o in objects if o.logical_key == key)
        attrs = deep_thaw(obj.attributes); attrs["definition"] = "CHECK (true)"
        objects[objects.index(obj)] = _actual(obj, attrs=attrs)
        self.assertFalse(self._compare(objects).global_result)

    def test_68_unapproved_extra_never_satisfies_missing(self):
        objects = [o for o in synthetic_objects(PRE) if o.logical_key != "column|auth_acoes|nome"]
        objects.append(CatalogObject("", "column", "column|extra_legado|nome", deep_freeze({"type": "text"})))
        self.assertFalse(self._compare(objects).global_result)


class DefinitionCanonicalizationTests(unittest.TestCase):
    def canonical(self, value):
        return proof_module._canonicalize_definition_array_text_casts(value)

    def test_check_array_cast_and_element_casts_are_equivalent(self):
        expected = "CHECK (status = ANY (ARRAY['A','B']::text[]))"
        observed = "CHECK (status = ANY (ARRAY['A'::text,'B'::text]))"
        self.assertEqual(self.canonical(expected), self.canonical(observed))

    def test_partial_index_array_cast_and_element_casts_are_equivalent(self):
        expected = (
            "CREATE UNIQUE INDEX uq_x ON exemplo USING btree (grupo) "
            "WHERE ativo = true AND (status::text = ANY "
            "(ARRAY['A'::character varying, 'B'::character varying]::text[]))"
        )
        observed = (
            "CREATE UNIQUE INDEX uq_x ON exemplo USING btree (grupo) "
            "WHERE ativo = true AND (status::text = ANY "
            "(ARRAY['A'::character varying::text, 'B'::character varying::text]))"
        )
        self.assertEqual(self.canonical(expected), self.canonical(observed))

    def test_different_values_remain_different(self):
        expected = "CHECK (status = ANY (ARRAY['A','B']::text[]))"
        observed = "CHECK (status = ANY (ARRAY['A'::text,'C'::text]))"
        self.assertNotEqual(self.canonical(expected), self.canonical(observed))

    def test_different_order_remains_different(self):
        expected = "CHECK (status = ANY (ARRAY['A','B']::text[]))"
        observed = "CHECK (status = ANY (ARRAY['B'::text,'A'::text]))"
        self.assertNotEqual(self.canonical(expected), self.canonical(observed))

    def test_different_quantity_remains_different(self):
        expected = "CHECK (status = ANY (ARRAY['A','B']::text[]))"
        observed = "CHECK (status = ANY (ARRAY['A'::text]))"
        self.assertNotEqual(self.canonical(expected), self.canonical(observed))

    def test_integer_cast_remains_different(self):
        expected = "CHECK (status = ANY (ARRAY['A','B']::text[]))"
        observed = "CHECK (status = ANY (ARRAY['A'::integer,'B'::integer]))"
        self.assertNotEqual(self.canonical(expected), self.canonical(observed))

    def test_column_operator_and_predicate_changes_remain_different(self):
        expected = "CHECK (status = ANY (ARRAY['A','B']::text[]) AND ativo = true)"
        variants = (
            "CHECK (estado = ANY (ARRAY['A'::text,'B'::text]) AND ativo = true)",
            "CHECK (status <> ALL (ARRAY['A'::text,'B'::text]) AND ativo = true)",
            "CHECK (status = ANY (ARRAY['A'::text,'B'::text]) AND ativo = false)",
            "CHECK (status = ANY (ARRAY['A'::text,'B'::text]) OR ativo = true)",
        )
        for observed in variants:
            with self.subTest(observed=observed):
                self.assertNotEqual(self.canonical(expected), self.canonical(observed))

    def test_null_and_structural_expression_are_not_canonicalized(self):
        expected = "CHECK (status = ANY (ARRAY['A','B']::text[]))"
        variants = (
            "CHECK (status = ANY (ARRAY['A'::text,NULL::text]))",
            "CHECK (status = ANY (ARRAY[lower('A')::text,'B'::text]))",
        )
        for observed in variants:
            with self.subTest(observed=observed):
                self.assertNotEqual(self.canonical(expected), self.canonical(observed))


class SecurityContractTests(unittest.TestCase):
    def test_69_pre_signature_only_connection(self):
        self.assertEqual(tuple(inspect.signature(provar_legado_reconciliado_para_adocao).parameters), ("conexao",))

    def test_70_post_signature_only_connection(self):
        self.assertEqual(tuple(inspect.signature(provar_catalogo_normativo_completo).parameters), ("conexao",))

    def test_71_comparator_is_pure_no_io_names(self):
        names = set(comparar_catalogo.__code__.co_names)
        self.assertTrue({"open", "Path", "subprocess", "connect"}.isdisjoint(names))

    def test_72_runtime_has_no_external_checkpoint_path(self):
        text = "\n".join(path.read_text(encoding="utf-8") for path in NEW_FILES[:3])
        self.assertNotIn("visualizations", text)
        self.assertNotIn("sistema-recic3_backups", text)

    def test_73_checkpoint_sha_only_in_rules_provenance(self):
        occurrences = {
            path.name: path.read_text(encoding="utf-8").count(EXPECTED_NEW_EVIDENCE)
            for path in NEW_FILES[:3]
        }
        self.assertEqual(occurrences, {"reconciliation_spec.py": 0, "reconciliation_runtime_rules.py": 1, "reconciliation_proof.py": 0})

    def test_74_no_database_url_or_subprocess_or_docker(self):
        text = "\n".join(path.read_text(encoding="utf-8") for path in NEW_FILES[:3]).casefold()
        self.assertNotIn("database_url", text)
        self.assertNotIn("subprocess", text)
        self.assertNotIn("docker", text)

    def test_75_no_adoption_implementation(self):
        text = "\n".join(path.read_text(encoding="utf-8") for path in NEW_FILES[:3])
        for token in ("ExecutionState.ADOTADA", "registrar_migration_adotada", "adotar_banco_legado_reconciliado"):
            self.assertNotIn(token, text)

    def test_76_ast_parses_all_new_files(self):
        for path in NEW_FILES:
            ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    def test_77_public_api_does_not_accept_proof_flags(self):
        forbidden = {"proof", "force", "expected", "snapshot", "catalog", "report", "trusted", "assume", "equivalent"}
        for api in (provar_legado_reconciliado_para_adocao, provar_catalogo_normativo_completo):
            self.assertTrue(forbidden.isdisjoint(inspect.signature(api).parameters))


def _install_alias_case(number, rule):
    def test(self):
        self.assertEqual(rule.category, rule.expected_logical_key.split("|", 1)[0])
        self.assertNotEqual(rule.expected_logical_key, rule.accepted_actual_logical_key)
        proof, _ = proof_for(PRE if rule.migration_id != "M0001" else POST)
        used = {key for result in proof.migration_results for key in result.aliases_used}
        self.assertIn(rule.accepted_actual_logical_key, used)
    test.__name__ = f"test_{number:02d}_alias_{rule.migration_id}_{rule.category}"
    setattr(RuntimeRulesTests, test.__name__, test)


for _number, _rule in enumerate(ALIASES, 78):
    _install_alias_case(_number, _rule)


def _install_tolerance_case(number, rule):
    def test(self):
        spec = carregar_catalog_spec_v1()
        if rule.extra_legacy:
            self.assertEqual(rule.accepted_attributes, rules_module.TOLERANCES_BY_KEY[rule.logical_key].accepted_attributes)
            self.assertNotIn(rule.logical_key, spec.by_key)
            return
        expected = spec.by_key[rule.logical_key]
        attrs = deep_thaw(expected.attributes)
        attrs.update(deep_thaw(rule.accepted_changes))
        actual = _actual(expected, attrs=attrs)
        self.assertTrue(proof_module._tolerance_accepts(actual, expected, rule))
    test.__name__ = f"test_{number:03d}_tolerance_{rule.migration_id}_{rule.category}_{number}"
    setattr(RuntimeRulesTests, test.__name__, test)


for _number, _rule in enumerate(TOLERANCES, 86):
    _install_tolerance_case(_number, _rule)


if __name__ == "__main__":
    unittest.main()
