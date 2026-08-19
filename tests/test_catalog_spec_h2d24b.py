import hashlib
import json
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = ROOT / "migrations_control" / "reconciliation" / "catalog_spec_v1.json"
MANIFEST_PATH = ROOT / "migrations_control" / "manifesto.json"
TARGET_KEY = "index|fc_aditivos|uq_fc_aditivos_id_contrato_id"

EXPECTED_COUNTS = {
    "M0001": 46,
    "M0002": 64,
    "M0003": 60,
    "M0004": 99,
    "M0005": 40,
    "M0006": 169,
    "M0007": 54,
    "M0008": 44,
    "M0009": 196,
    "M0010": 221,
    "M0011": 184,
    "M0012": 193,
    "M0013": 272,
    "H001": 29,
    "H002": 24,
    "H003": 58,
    "H004": 36,
    "H005": 39,
    "H006": 67,
    "H007": 70,
    "H008": 99,
    "H009": 20,
    "H010": 161,
    "H011": 110,
}


class CatalogSpecH2D24BTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw = SPEC_PATH.read_bytes()
        cls.text = cls.raw.decode("utf-8")
        cls.spec = json.loads(cls.text)
        cls.manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
        cls.by_id = {item["migration_id"]: item for item in cls.spec["migrations"]}

    def test_01_json_is_valid_utf8_and_format_v1(self):
        self.assertEqual(self.spec["versao_formato"], 1)
        self.assertFalse(self.raw.startswith(b"\xef\xbb\xbf"))

    def test_02_root_contract_is_minimal_and_complete(self):
        self.assertEqual(
            set(self.spec),
            {
                "versao_formato",
                "cadeia",
                "categorias",
                "proveniencia",
                "ownership_adjustments",
                "migrations",
            },
        )

    def test_03_exact_persistable_migration_chain(self):
        expected = [f"M{i:04d}" for i in range(1, 14)] + [f"H{i:03d}" for i in range(1, 12)]
        actual = [item["migration_id"] for item in self.spec["migrations"]]
        self.assertEqual(actual, expected)
        self.assertEqual(len(actual), 24)
        self.assertNotIn("M0000", actual)
        self.assertEqual([item["ordem_global"] for item in self.spec["migrations"]], list(range(1, 25)))

    def test_04_checksums_equal_manifest(self):
        manifest_checksums = {
            item["identificador"]: item["checksum"]
            for item in self.manifest["operacoes"]
            if item["identificador"] != "M0000"
        }
        spec_checksums = {
            item["migration_id"]: item["checksum_sha256"]
            for item in self.spec["migrations"]
        }
        self.assertEqual(spec_checksums, manifest_checksums)

    def test_05_each_count_is_derived_from_object_list(self):
        actual = {migration_id: len(item["objetos"]) for migration_id, item in self.by_id.items()}
        self.assertEqual(actual, EXPECTED_COUNTS)

    def test_06_normative_totals_are_exact(self):
        m_total = sum(len(item["objetos"]) for key, item in self.by_id.items() if key.startswith("M"))
        h_total = sum(len(item["objetos"]) for key, item in self.by_id.items() if key.startswith("H"))
        self.assertEqual(m_total, 1642)
        self.assertEqual(h_total, 713)
        self.assertEqual(m_total + h_total, 2355)
        self.assertEqual(self.spec["cadeia"]["total_objetos"], 2355)
        self.assertEqual(self.spec["cadeia"]["migrations_persistiveis"], 24)

    def test_07_exactly_one_ownership_adjustment(self):
        self.assertEqual(len(self.spec["ownership_adjustments"]), 1)
        adjustment = self.spec["ownership_adjustments"][0]
        self.assertEqual(adjustment["key"], TARGET_KEY)
        self.assertEqual(adjustment["physical_owner"], "H005")
        self.assertEqual(adjustment["normative_owner"], "H006")

    def test_08_adjustment_is_not_tolerance_or_alias(self):
        adjustment = self.spec["ownership_adjustments"][0]
        forbidden = {"alias", "aliases", "tolerance", "tolerances"}
        self.assertTrue(forbidden.isdisjoint(adjustment))
        self.assertTrue(forbidden.isdisjoint(self.spec))

    def test_09_adjusted_object_is_listed_once_under_h006(self):
        occurrences = [
            (migration["migration_id"], obj)
            for migration in self.spec["migrations"]
            for obj in migration["objetos"]
            if obj["chave"] == TARGET_KEY
        ]
        self.assertEqual(len(occurrences), 1)
        self.assertEqual(occurrences[0][0], "H006")
        self.assertFalse(any(obj["chave"] == TARGET_KEY for obj in self.by_id["H005"]["objetos"]))

    def test_10_adjusted_object_attributes_are_preserved(self):
        target = next(obj for obj in self.by_id["H006"]["objetos"] if obj["chave"] == TARGET_KEY)
        self.assertEqual(target["categoria"], "index")
        self.assertEqual(
            target["atributos"],
            {
                "definition": "CREATE UNIQUE INDEX uq_fc_aditivos_id_contrato_id ON fc_aditivos USING btree (id, contrato_id)",
                "primary": False,
                "unique": True,
                "valid": True,
            },
        )

    def test_11_only_frozen_categories_are_used(self):
        allowed = {"table", "column", "sequence", "constraint", "index"}
        self.assertEqual(set(self.spec["categorias"]), allowed)
        actual = {
            obj["categoria"]
            for migration in self.spec["migrations"]
            for obj in migration["objetos"]
        }
        self.assertEqual(actual, allowed)

    def test_12_logical_identity_excludes_physical_identifiers(self):
        forbidden = {"oid", "objid", "relfilenode"}
        for migration in self.spec["migrations"]:
            for obj in migration["objetos"]:
                key_parts = {part.casefold() for part in obj["chave"].split("|")}
                self.assertTrue(forbidden.isdisjoint(key_parts), obj["chave"])
                self.assertTrue(forbidden.isdisjoint(obj["atributos"]), obj["chave"])

    def test_13_normative_fingerprint_matches_checkpoint_provenance(self):
        enumeration = [
            {
                "normative_migration_id": migration["migration_id"],
                "category": obj["categoria"],
                "logical_key": obj["chave"],
                "normalized_attributes": obj["atributos"],
            }
            for migration in self.spec["migrations"]
            for obj in migration["objetos"]
        ]
        enumeration.sort(key=lambda item: (item["normative_migration_id"], item["category"], item["logical_key"]))
        canonical = json.dumps(
            enumeration,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"
        fingerprint = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        self.assertEqual(fingerprint, "8ff247ce84be215cb00e0e13ff1be84405f90b00ce408111d60135b1d39a1f24")
        self.assertEqual(fingerprint, self.spec["proveniencia"]["normative_enumeration_fingerprint"])

    def test_14_physical_and_normative_provenance_is_complete(self):
        provenance = self.spec["proveniencia"]
        self.assertEqual(provenance["postgresql"], "17.10")
        self.assertEqual(provenance["server_version_num"], "170010")
        self.assertEqual(
            provenance["image_digest"],
            "postgres@sha256:7958605b474b3d264a969cb3a123d6aa00ad1e1fe9da8a69984dabb704d93317",
        )
        self.assertEqual(provenance["generator_sha256"], "03cb2b8ef40bc6cf0ea945cef79792841f87fa34ceeac3d23de19b2111d0bc09")
        self.assertEqual(provenance["historical_harness_sha256"], "b1a8072a35180409931b27e6128a69a8c2f8f9db66b4ca489e774eca20c139d1")
        self.assertEqual(provenance["h2d2_snapshot_sha256"], "24641c2e3fbd95d3ce9e48a02347c71682c4dc52449eb5838c48e29f28d39da6")
        self.assertEqual(provenance["physical_checkpoint_sha256"], "b52850ff2b5d142e95441903a86f5595413a975f5abd72449ab5f76643ebb9e5")
        self.assertEqual(provenance["physical_enumeration_fingerprint"], "4cf6b4dbbfc777e81ad6b24fa4a6c72d9399ee95d4292ddc4fca1bc6ff7b6ea5")
        self.assertEqual(provenance["normative_checkpoint_sha256"], "4fd93b407a0f4fc65653b13c9f1f59e60abd752c293c709a56fb43aa83ad8bf3")


if __name__ == "__main__":
    unittest.main()
