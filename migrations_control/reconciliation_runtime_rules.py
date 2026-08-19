"""Regras runtime H2D.24C estritamente path-specific e imutáveis."""

from __future__ import annotations

from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Mapping

from .reconciliation_spec import deep_freeze


H2D24C_EVIDENCE_SHA256 = "c739b435f3d494c6cce12c86fb1e461151e10cfd8d9074ab4f48b4a34595baa6"
R0007_EVIDENCE_SHA256 = "a868b6be6b95e7e2b228fc32a5971d8589c46ed35814c6f4fc8c483427cc88d9"


@dataclass(frozen=True, slots=True)
class AliasRule:
    migration_id: str
    category: str
    expected_logical_key: str
    accepted_actual_logical_key: str
    provenance: str
    evidence_sha256: str


@dataclass(frozen=True, slots=True)
class ToleranceRule:
    migration_id: str
    category: str
    logical_key: str
    tolerance_class: str
    accepted_changes: Mapping[str, Any]
    provenance: str
    extra_legacy: bool = False
    accepted_attributes: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class RuntimeRules:
    aliases: tuple[AliasRule, ...]
    tolerances: tuple[ToleranceRule, ...]


ALIASES = (
    AliasRule("M0003", "constraint", "constraint|usuarios|pk_usuarios", "constraint|usuarios|usuarios_pkey", "R0002", H2D24C_EVIDENCE_SHA256),
    AliasRule("M0003", "index", "index|usuarios|pk_usuarios", "index|usuarios|usuarios_pkey", "R0002", H2D24C_EVIDENCE_SHA256),
    AliasRule("M0009", "constraint", "constraint|associados|pk_associados", "constraint|associados|associados_pkey", "R0005", H2D24C_EVIDENCE_SHA256),
    AliasRule("M0009", "index", "index|associados|pk_associados", "index|associados|associados_pkey", "R0005", H2D24C_EVIDENCE_SHA256),
    AliasRule("M0011", "constraint", "constraint|transacoes_financeiras|pk_transacoes_financeiras", "constraint|transacoes_financeiras|transacoes_financeiras_pkey", "R0006", H2D24C_EVIDENCE_SHA256),
    AliasRule("M0011", "index", "index|transacoes_financeiras|pk_transacoes_financeiras", "index|transacoes_financeiras|transacoes_financeiras_pkey", "R0006", H2D24C_EVIDENCE_SHA256),
    AliasRule("M0013", "constraint", "constraint|solicitacoes_alteracao|pk_solicitacoes_alteracao", "constraint|solicitacoes_alteracao|solicitacoes_alteracao_pkey", "R0007", R0007_EVIDENCE_SHA256),
    AliasRule("M0013", "index", "index|solicitacoes_alteracao|pk_solicitacoes_alteracao", "index|solicitacoes_alteracao|solicitacoes_alteracao_pkey", "R0007", R0007_EVIDENCE_SHA256),
)


def _changes(**values: Any) -> Mapping[str, Any]:
    return deep_freeze(values)


def _attrs(**values: Any) -> Mapping[str, Any]:
    return deep_freeze(values)


TOLERANCES = (
    ToleranceRule("M0003", "column", "column|usuarios|id", "C", _changes(default="nextval('usuarios_id_seq'::regclass)", identity=""), "R0002"),
    ToleranceRule("M0003", "sequence", "sequence|usuarios_id_seq", "C", _changes(), "R0002"),
    ToleranceRule("M0009", "column", "column|associados|id", "C", _changes(type="integer", default="nextval('associados_id_seq'::regclass)", identity=""), "R0005"),
    ToleranceRule("M0009", "sequence", "sequence|associados_id_seq", "C", _changes(type="integer", max=2147483647), "R0005"),
    ToleranceRule("M0011", "column", "column|transacoes_financeiras|id", "C", _changes(type="integer", default="nextval('transacoes_financeiras_id_seq'::regclass)", identity=""), "R0006"),
    ToleranceRule("M0011", "sequence", "sequence|transacoes_financeiras_id_seq", "C", _changes(type="integer", max=2147483647), "R0006"),
    ToleranceRule("M0013", "column", "column|solicitacoes_alteracao|id", "C", _changes(type="integer", default="nextval('solicitacoes_alteracao_id_seq'::regclass)", identity=""), "R0007"),
    ToleranceRule("M0013", "sequence", "sequence|solicitacoes_alteracao_id_seq", "C", _changes(type="integer", max=2147483647), "R0007"),
    ToleranceRule("M0011", "column", "column|contas_financeiras|instituicao", "TECHACCOUNT_CONDITIONAL", _changes(not_null=False), "R0006"),
    ToleranceRule("M0011", "column", "column|contas_financeiras|agencia", "TECHACCOUNT_CONDITIONAL", _changes(not_null=False), "R0006"),
    ToleranceRule("M0011", "column", "column|contas_financeiras|conta", "TECHACCOUNT_CONDITIONAL", _changes(not_null=False), "R0006"),
    ToleranceRule("M0011", "column", "column|contas_financeiras|abertura_data", "TECHACCOUNT_CONDITIONAL", _changes(not_null=False), "R0006"),
    ToleranceRule("M0011", "column", "column|contas_financeiras|encerramento_data", "TECHACCOUNT_CONDITIONAL", _changes(not_null=False), "R0006"),
    ToleranceRule("M0011", "constraint", "constraint|contas_financeiras|ck_contas_financeiras__instituicao_preenchido", "TECHACCOUNT_CONDITIONAL", _changes(definition="CHECK (tipo = 'MIGRACAO_LEGADO'::text AND instituicao IS NULL OR tipo <> 'MIGRACAO_LEGADO'::text AND instituicao IS NOT NULL AND btrim(instituicao) <> ''::text)"), "R0006"),
    ToleranceRule("M0011", "constraint", "constraint|contas_financeiras|ck_contas_financeiras__agencia_preenchido", "TECHACCOUNT_CONDITIONAL", _changes(definition="CHECK (tipo = 'MIGRACAO_LEGADO'::text AND agencia IS NULL OR tipo <> 'MIGRACAO_LEGADO'::text AND agencia IS NOT NULL AND btrim(agencia) <> ''::text)"), "R0006"),
    ToleranceRule("M0011", "constraint", "constraint|contas_financeiras|ck_contas_financeiras__conta_preenchido", "TECHACCOUNT_CONDITIONAL", _changes(definition="CHECK (tipo = 'MIGRACAO_LEGADO'::text AND conta IS NULL OR tipo <> 'MIGRACAO_LEGADO'::text AND conta IS NOT NULL AND btrim(conta) <> ''::text)"), "R0006"),
    ToleranceRule(
        "M0011", "constraint", "constraint|contas_financeiras|ck_contas_financeiras__datas_modalidade",
        "TECHACCOUNT_CONDITIONAL", _changes(), "R0006", extra_legacy=True,
        accepted_attributes=_attrs(
            type="c", validated=True, deferrable=False, deferred=False,
            definition="CHECK (tipo = 'MIGRACAO_LEGADO'::text AND abertura_data IS NULL AND encerramento_data IS NULL OR tipo <> 'MIGRACAO_LEGADO'::text AND abertura_data IS NOT NULL AND encerramento_data IS NOT NULL)",
        ),
    ),
)


ALIASES_BY_EXPECTED = MappingProxyType({rule.expected_logical_key: rule for rule in ALIASES})
TOLERANCES_BY_KEY = MappingProxyType({rule.logical_key: rule for rule in TOLERANCES})
RUNTIME_RULES = RuntimeRules(ALIASES, TOLERANCES)


def validate_runtime_rules() -> None:
    if len(ALIASES) != 8 or len({rule.expected_logical_key for rule in ALIASES}) != 8:
        raise ValueError("inventário de aliases inválido")
    if len({rule.accepted_actual_logical_key for rule in ALIASES}) != 8:
        raise ValueError("actual alias reutilizado")
    if sum(rule.evidence_sha256 == H2D24C_EVIDENCE_SHA256 for rule in ALIASES) != 6:
        raise ValueError("proveniência H2D.24C inválida")
    if len(TOLERANCES) != 17 or len(TOLERANCES_BY_KEY) != 17:
        raise ValueError("inventário de tolerâncias inválido")
    if any(token in rule.expected_logical_key for rule in ALIASES for token in ("*", "(?", "[")):
        raise ValueError("alias não exato")


validate_runtime_rules()
