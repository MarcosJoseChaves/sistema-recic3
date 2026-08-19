"""Prova runtime catalogal H2D.24C, sem adoção e sem I/O externo."""

from __future__ import annotations

from dataclasses import dataclass
import re
from types import MappingProxyType
from typing import Any, Iterable, Mapping

from .reconciliation_catalog import coletar_catalogo_reconciliacao
from .reconciliation_runtime_rules import RUNTIME_RULES, RuntimeRules
from .reconciliation_spec import (
    CatalogObject,
    CatalogSpec,
    carregar_catalog_spec_v1,
    deep_freeze,
    deep_thaw,
    sha256_canonico,
)


PRE = "PRE"
POST = "POST"


@dataclass(frozen=True, slots=True)
class MigrationCatalogResult:
    migration_id: str
    checksum_sha256: str
    expected_count: int
    functional_count: int
    missing: tuple[str, ...]
    divergent: tuple[str, ...]
    tolerated: tuple[str, ...]
    aliases_used: tuple[str, ...]
    result: str


@dataclass(frozen=True, slots=True)
class CatalogProof:
    spec_version: int
    spec_sha256: str
    spec_normative_fingerprint: str
    manifest_sha256: str
    mode: str
    snapshot_fingerprint: str
    functional_projection_fingerprint: str
    migration_results: tuple[MigrationCatalogResult, ...]
    extras_legacy: tuple[CatalogObject, ...]
    m0001_state: str
    candidate_total: int
    candidate_functional: int
    global_result: bool


def _object_map(objects: Iterable[CatalogObject]) -> Mapping[str, CatalogObject]:
    result: dict[str, CatalogObject] = {}
    for obj in objects:
        if obj.logical_key in result:
            raise ValueError(f"actual key duplicada: {obj.logical_key}")
        if obj.category != obj.logical_key.split("|", 1)[0]:
            raise ValueError(f"categoria actual inválida: {obj.logical_key}")
        result[obj.logical_key] = obj
    return MappingProxyType(result)


def _snapshot_fingerprint(actual: Mapping[str, CatalogObject]) -> str:
    rows = [
        {
            "category": obj.category,
            "logical_key": obj.logical_key,
            "normalized_attributes": deep_thaw(obj.attributes),
        }
        for obj in actual.values()
    ]
    rows.sort(key=lambda item: (item["category"], item["logical_key"]))
    return sha256_canonico(rows)


def _projection_fingerprint(objects: Iterable[CatalogObject]) -> str:
    rows = [
        {
            "normative_migration_id": obj.migration_id,
            "category": obj.category,
            "logical_key": obj.logical_key,
            "normalized_attributes": deep_thaw(obj.attributes),
        }
        for obj in objects
    ]
    rows.sort(key=lambda item: (
        item["normative_migration_id"], item["category"], item["logical_key"]
    ))
    return sha256_canonico(rows)


def _alias_attributes(actual: CatalogObject, expected: CatalogObject) -> Mapping[str, Any]:
    """Normaliza somente o token nominal explicitamente coberto pelo alias."""
    attrs = deep_thaw(actual.attributes)
    if actual.category == "index" and isinstance(attrs.get("definition"), str):
        actual_name = actual.logical_key.rsplit("|", 1)[1]
        expected_name = expected.logical_key.rsplit("|", 1)[1]
        marker = f"INDEX {actual_name} ON "
        if marker not in attrs["definition"]:
            return deep_freeze(attrs)
        attrs["definition"] = attrs["definition"].replace(
            marker, f"INDEX {expected_name} ON ", 1
        )
    return deep_freeze(attrs)


_TEXT_ARRAY_ELEMENT = re.compile(
    r"\A(?P<value>\s*'(?:''|[^'])*'"
    r"(?:\s*::\s*(?:character\s+varying|varchar)(?:\s*\(\s*\d+\s*\))?)?\s*)"
    r"::\s*text\s*\Z",
    re.IGNORECASE,
)


def _split_array_elements(body: str) -> list[str] | None:
    elements: list[str] = []
    start = 0
    depth = 0
    quoted = False
    index = 0
    while index < len(body):
        char = body[index]
        if quoted:
            if char == "'" and index + 1 < len(body) and body[index + 1] == "'":
                index += 2
                continue
            if char == "'":
                quoted = False
        elif char == "'":
            quoted = True
        elif char in "([":
            depth += 1
        elif char in ")]":
            depth -= 1
            if depth < 0:
                return None
        elif char == "," and depth == 0:
            elements.append(body[start:index])
            start = index + 1
        index += 1
    if quoted or depth != 0:
        return None
    elements.append(body[start:])
    return elements if elements and all(item.strip() for item in elements) else None


def _array_close(definition: str, open_index: int) -> int | None:
    depth = 1
    quoted = False
    index = open_index + 1
    while index < len(definition):
        char = definition[index]
        if quoted:
            if char == "'" and index + 1 < len(definition) and definition[index + 1] == "'":
                index += 2
                continue
            if char == "'":
                quoted = False
        elif char == "'":
            quoted = True
        elif char == "[":
            depth += 1
        elif char == "]":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return None


def _canonicalize_definition_array_text_casts(definition: str) -> str:
    """Move casts `text` literais para o ARRAY, sem alterar outra estrutura SQL."""
    output: list[str] = []
    cursor = 0
    quoted = False
    index = 0
    while index < len(definition):
        char = definition[index]
        if quoted:
            if char == "'" and index + 1 < len(definition) and definition[index + 1] == "'":
                index += 2
                continue
            if char == "'":
                quoted = False
            index += 1
            continue
        if char == "'":
            quoted = True
            index += 1
            continue
        match = re.match(r"ARRAY\s*\[", definition[index:], re.IGNORECASE)
        if not match:
            index += 1
            continue
        if index and (definition[index - 1].isalnum() or definition[index - 1] == "_"):
            index += 1
            continue
        open_index = index + match.end() - 1
        close_index = _array_close(definition, open_index)
        if close_index is None:
            index += 1
            continue
        after = definition[close_index + 1:]
        if re.match(r"\s*::\s*text\s*\[\s*\]", after, re.IGNORECASE):
            index = close_index + 1
            continue
        elements = _split_array_elements(definition[open_index + 1:close_index])
        matches = [_TEXT_ARRAY_ELEMENT.fullmatch(item) for item in elements or ()]
        if not matches or not all(matches):
            index = close_index + 1
            continue
        canonical_body = ",".join(item.group("value") for item in matches)
        output.append(definition[cursor:open_index + 1])
        output.append(canonical_body)
        output.append("]::text[]")
        cursor = close_index + 1
        index = close_index + 1
    output.append(definition[cursor:])
    return "".join(output)


def _comparison_attributes(attributes: Mapping[str, Any]) -> dict[str, Any]:
    comparable = deep_thaw(attributes)
    definition = comparable.get("definition")
    if isinstance(definition, str):
        comparable["definition"] = _canonicalize_definition_array_text_casts(definition)
    return comparable


def _tolerance_accepts(actual: CatalogObject, expected: CatalogObject, rule) -> bool:
    if (rule.migration_id, rule.category, rule.logical_key) != (
        expected.migration_id, expected.category, expected.logical_key,
    ) or rule.extra_legacy:
        return False
    accepted = deep_thaw(expected.attributes)
    accepted.update(deep_thaw(rule.accepted_changes))
    return _comparison_attributes(actual.attributes) == _comparison_attributes(deep_freeze(accepted))


def comparar_catalogo(
    actual: Iterable[CatalogObject],
    spec: CatalogSpec,
    mode: str,
    rules: RuntimeRules,
) -> CatalogProof:
    """Compara catálogo em memória; não executa I/O nem aceita atalhos de aprovação."""
    if mode not in {PRE, POST}:
        raise ValueError("modo de prova inválido")
    actual_by_key = _object_map(actual)
    aliases = {rule.expected_logical_key: rule for rule in rules.aliases}
    if len(aliases) != len(rules.aliases):
        raise ValueError("aliases duplicados")
    actual_aliases = [rule.accepted_actual_logical_key for rule in rules.aliases]
    if len(set(actual_aliases)) != len(actual_aliases):
        raise ValueError("alias actual ambíguo")
    tolerances = {rule.logical_key: rule for rule in rules.tolerances}
    if len(tolerances) != len(rules.tolerances):
        raise ValueError("tolerâncias duplicadas")

    m1 = spec.by_migration["M0001"]
    m1_keys = {obj.logical_key for obj in m1.objects}
    m1_present = m1_keys & set(actual_by_key)
    if not m1_present:
        m0001_state = "AUSENTE"
    elif len(m1_present) == len(m1_keys):
        m0001_state = "PRESENTE_COMPLETO"
    else:
        m0001_state = "PARCIAL"

    consumed: set[str] = set(m1_present)
    projection: list[CatalogObject] = []
    results: list[MigrationCatalogResult] = []
    candidate_passes = 0
    for migration in spec.migrations:
        if mode == PRE and migration.migration_id == "M0001":
            ok = m0001_state == "AUSENTE"
            results.append(MigrationCatalogResult(
                migration_id="M0001", checksum_sha256=migration.checksum_sha256,
                expected_count=len(migration.objects), functional_count=0,
                missing=tuple(obj.logical_key for obj in migration.objects if obj.logical_key not in actual_by_key),
                divergent=tuple(sorted(m1_present)), tolerated=(), aliases_used=(),
                result="AUSENTE" if ok else f"FAIL_{m0001_state}",
            ))
            continue
        missing: list[str] = []
        divergent: list[str] = []
        tolerated: list[str] = []
        aliases_used: list[str] = []
        functional = 0
        for expected in migration.objects:
            exact = actual_by_key.get(expected.logical_key)
            alias_rule = aliases.get(expected.logical_key)
            alias_obj = actual_by_key.get(alias_rule.accepted_actual_logical_key) if alias_rule else None
            if exact is not None and alias_obj is not None:
                divergent.append(expected.logical_key)
                consumed.update((exact.logical_key, alias_obj.logical_key))
                continue
            selected = exact or alias_obj
            if selected is None:
                missing.append(expected.logical_key)
                continue
            consumed.add(selected.logical_key)
            if selected.category != expected.category:
                divergent.append(expected.logical_key)
                continue
            selected_attrs = _alias_attributes(selected, expected) if alias_obj is not None else selected.attributes
            if _comparison_attributes(selected_attrs) == _comparison_attributes(expected.attributes):
                functional += 1
                projection.append(expected)
                if alias_obj is not None:
                    aliases_used.append(alias_obj.logical_key)
                continue
            tolerance = tolerances.get(expected.logical_key)
            if tolerance and _tolerance_accepts(selected, expected, tolerance):
                functional += 1
                tolerated.append(expected.logical_key)
                projection.append(expected)
                if alias_obj is not None:
                    aliases_used.append(alias_obj.logical_key)
                continue
            divergent.append(expected.logical_key)
        for rule in rules.tolerances:
            if not rule.extra_legacy or rule.migration_id != migration.migration_id:
                continue
            extra = actual_by_key.get(rule.logical_key)
            if extra is not None and deep_thaw(extra.attributes) == deep_thaw(rule.accepted_attributes):
                consumed.add(extra.logical_key)
                tolerated.append(extra.logical_key)
        ok = functional == len(migration.objects) and not missing and not divergent
        if ok:
            candidate_passes += 1
        results.append(MigrationCatalogResult(
            migration_id=migration.migration_id,
            checksum_sha256=migration.checksum_sha256,
            expected_count=len(migration.objects), functional_count=functional,
            missing=tuple(sorted(missing)), divergent=tuple(sorted(divergent)),
            tolerated=tuple(sorted(tolerated)), aliases_used=tuple(sorted(aliases_used)),
            result="PASS" if ok else "FAIL",
        ))

    candidate_total = 23 if mode == PRE else 24
    m1_ok = m0001_state == ("AUSENTE" if mode == PRE else "PRESENTE_COMPLETO")
    global_result = m1_ok and candidate_passes == candidate_total
    extras = tuple(
        actual_by_key[key] for key in sorted(set(actual_by_key) - consumed)
    )
    return CatalogProof(
        spec_version=spec.version, spec_sha256=spec.spec_sha256,
        spec_normative_fingerprint=spec.normative_fingerprint,
        manifest_sha256=spec.manifest_sha256, mode=mode,
        snapshot_fingerprint=_snapshot_fingerprint(actual_by_key),
        functional_projection_fingerprint=_projection_fingerprint(projection),
        migration_results=tuple(results), extras_legacy=extras,
        m0001_state=m0001_state, candidate_total=candidate_total,
        candidate_functional=candidate_passes, global_result=global_result,
    )


def provar_legado_reconciliado_para_adocao(conexao):
    spec = carregar_catalog_spec_v1()
    actual = coletar_catalogo_reconciliacao(conexao)
    return comparar_catalogo(actual, spec, PRE, RUNTIME_RULES)


def provar_catalogo_normativo_completo(conexao):
    spec = carregar_catalog_spec_v1()
    actual = coletar_catalogo_reconciliacao(conexao)
    return comparar_catalogo(actual, spec, POST, RUNTIME_RULES)
