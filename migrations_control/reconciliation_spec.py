"""Loader estrito e modelos imutáveis da especificação catalogal H2D.24B."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping


ROOT = Path(__file__).resolve().parents[1]
SPEC_PATH = ROOT / "migrations_control" / "reconciliation" / "catalog_spec_v1.json"
MANIFEST_PATH = ROOT / "migrations_control" / "manifesto.json"
SPEC_SHA256 = "b055bc82880e6f0ff0d0b2826a54e63a3c85e889d179119ead1997e9852ac017"
SPEC_NORMATIVE_FINGERPRINT = "8ff247ce84be215cb00e0e13ff1be84405f90b00ce408111d60135b1d39a1f24"
MANIFEST_SHA256 = "c2490bb90b09856857db14eee72b2c13ba183e7b3ffa941cc43333ce260df294"
VALID_CATEGORIES = frozenset({"table", "column", "sequence", "constraint", "index"})
MIGRATION_IDS = tuple([f"M{i:04d}" for i in range(1, 14)] + [f"H{i:03d}" for i in range(1, 12)])
FORBIDDEN_IDENTITY_FIELDS = frozenset({"oid", "objid", "relfilenode"})


def deep_freeze(value: Any) -> Any:
    """Converte recursivamente JSON mutável em estruturas imutáveis."""
    if isinstance(value, Mapping):
        return MappingProxyType({str(key): deep_freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(deep_freeze(item) for item in value)
    if isinstance(value, tuple):
        return tuple(deep_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(deep_freeze(item) for item in value)
    return value


def deep_thaw(value: Any) -> Any:
    """Produz representação JSON canônica a partir de modelos congelados."""
    if isinstance(value, Mapping):
        return {key: deep_thaw(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [deep_thaw(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted(deep_thaw(item) for item in value)
    return value


def canonical_bytes(value: Any) -> bytes:
    return (json.dumps(deep_thaw(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sha256_canonico(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


@dataclass(frozen=True, slots=True)
class CatalogObject:
    migration_id: str
    category: str
    logical_key: str
    attributes: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class MigrationSpec:
    migration_id: str
    checksum_sha256: str
    global_order: int
    objects: tuple[CatalogObject, ...]


@dataclass(frozen=True, slots=True)
class CatalogSpec:
    version: int
    spec_sha256: str
    normative_fingerprint: str
    manifest_sha256: str
    categories: frozenset[str]
    migrations: tuple[MigrationSpec, ...]
    ownership_adjustments: tuple[Mapping[str, Any], ...]
    provenance: Mapping[str, Any]

    @property
    def objects(self) -> tuple[CatalogObject, ...]:
        return tuple(obj for migration in self.migrations for obj in migration.objects)

    @property
    def by_migration(self) -> Mapping[str, MigrationSpec]:
        return MappingProxyType({migration.migration_id: migration for migration in self.migrations})

    @property
    def by_key(self) -> Mapping[str, CatalogObject]:
        return MappingProxyType({obj.logical_key: obj for obj in self.objects})


def _sha_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _normative_fingerprint(payload: Mapping[str, Any]) -> str:
    enumeration = [
        {
            "normative_migration_id": migration["migration_id"],
            "category": obj["categoria"],
            "logical_key": obj["chave"],
            "normalized_attributes": obj["atributos"],
        }
        for migration in payload["migrations"]
        for obj in migration["objetos"]
    ]
    enumeration.sort(key=lambda item: (
        item["normative_migration_id"], item["category"], item["logical_key"]
    ))
    return sha256_canonico(enumeration)


def carregar_catalog_spec_v1() -> CatalogSpec:
    """Carrega somente a spec interna congelada e falha diante de qualquer desvio."""
    raw = SPEC_PATH.read_bytes()
    if hashlib.sha256(raw).hexdigest() != SPEC_SHA256:
        raise ValueError("catalog_spec_v1 SHA-256 divergente")
    if _sha_file(MANIFEST_PATH) != MANIFEST_SHA256:
        raise ValueError("manifesto SHA-256 divergente")
    payload = json.loads(raw.decode("utf-8", errors="strict"))
    required_root = {
        "versao_formato", "cadeia", "categorias", "proveniencia",
        "ownership_adjustments", "migrations",
    }
    if set(payload) != required_root or payload["versao_formato"] != 1:
        raise ValueError("contrato raiz/versão da spec inválido")
    if payload["categorias"] != ["column", "constraint", "index", "sequence", "table"]:
        raise ValueError("categorias da spec inválidas")
    migrations = payload["migrations"]
    ids = tuple(item.get("migration_id") for item in migrations)
    if ids != MIGRATION_IDS or len(set(ids)) != 24 or "M0000" in ids:
        raise ValueError("cadeia normativa inválida")
    if tuple(item.get("ordem_global") for item in migrations) != tuple(range(1, 25)):
        raise ValueError("ordem normativa inválida")
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    manifest_checksums = {
        item["identificador"]: item["checksum"]
        for item in manifest["operacoes"] if item["identificador"] != "M0000"
    }
    spec_checksums = {item["migration_id"]: item["checksum_sha256"] for item in migrations}
    if spec_checksums != manifest_checksums:
        raise ValueError("checksums da spec divergem do manifesto")
    keys: set[str] = set()
    model_migrations: list[MigrationSpec] = []
    for migration in migrations:
        objects: list[CatalogObject] = []
        for item in migration["objetos"]:
            if set(item) != {"categoria", "chave", "atributos"}:
                raise ValueError("objeto normativo fora do contrato")
            category, key, attrs = item["categoria"], item["chave"], item["atributos"]
            if category not in VALID_CATEGORIES or key in keys or not isinstance(attrs, dict):
                raise ValueError("categoria/chave/atributos normativos inválidos")
            if key.split("|", 1)[0] != category:
                raise ValueError("categoria não corresponde à chave")
            if FORBIDDEN_IDENTITY_FIELDS & ({part.casefold() for part in key.split("|")} | set(attrs)):
                raise ValueError("identificador físico volátil na spec")
            keys.add(key)
            objects.append(CatalogObject(
                migration_id=migration["migration_id"], category=category,
                logical_key=key, attributes=deep_freeze(attrs),
            ))
        model_migrations.append(MigrationSpec(
            migration_id=migration["migration_id"],
            checksum_sha256=migration["checksum_sha256"],
            global_order=migration["ordem_global"], objects=tuple(objects),
        ))
    if len(keys) != 2355 or payload["cadeia"] != {
        "manifesto_versao": 1, "migrations_persistiveis": 24, "total_objetos": 2355,
    }:
        raise ValueError("totais normativos inválidos")
    fingerprint = _normative_fingerprint(payload)
    if fingerprint != SPEC_NORMATIVE_FINGERPRINT:
        raise ValueError("fingerprint normativo divergente")
    if payload["proveniencia"].get("normative_enumeration_fingerprint") != fingerprint:
        raise ValueError("proveniência do fingerprint divergente")
    adjustments = payload["ownership_adjustments"]
    if len(adjustments) != 1:
        raise ValueError("ownership adjustment inválido")
    adjustment = adjustments[0]
    target = "index|fc_aditivos|uq_fc_aditivos_id_contrato_id"
    if (adjustment.get("key"), adjustment.get("physical_owner"), adjustment.get("normative_owner")) != (target, "H005", "H006"):
        raise ValueError("ownership adjustment não aprovado")
    owners = [obj.migration_id for migration in model_migrations for obj in migration.objects if obj.logical_key == target]
    if owners != ["H006"]:
        raise ValueError("ownership normativo H006 não preservado")
    return CatalogSpec(
        version=1, spec_sha256=SPEC_SHA256,
        normative_fingerprint=fingerprint, manifest_sha256=MANIFEST_SHA256,
        categories=VALID_CATEGORIES, migrations=tuple(model_migrations),
        ownership_adjustments=tuple(deep_freeze(item) for item in adjustments),
        provenance=deep_freeze(payload["proveniencia"]),
    )
