import hashlib
import re
import unittest
from pathlib import Path

from migrations_control.manifest import carregar_manifesto


ROOT = Path(__file__).resolve().parents[1]
RECON = ROOT / "migrations_control" / "reconciliation"
R0001 = RECON / "R0001_legacy_to_baseline.sql"
PRECHECKS = RECON / "R0001_prechecks.sql"
M0002 = ROOT / "migrations_control" / "sql" / "M0002_tipos.sql"
PLAN = ROOT / "PLANO_RECONCILIACAO_DEV_H2D3.md"


class ReconciliationPlanH2D3Tests(unittest.TestCase):
    def test_r0001_reproduz_somente_m0002_ausente(self):
        marker = "-- SAFE_SUBSET_BEGIN: M0002\n"
        payload = R0001.read_text(encoding="utf-8").split(marker, 1)[1].lstrip()
        self.assertEqual(payload, M0002.read_text(encoding="utf-8").lstrip())

    def test_r0001_nao_contem_operacao_destrutiva_ou_fc(self):
        sql = R0001.read_text(encoding="utf-8")
        self.assertIsNone(re.search(r"\b(?:DROP|TRUNCATE|DELETE)\b", sql, re.I))
        self.assertNotIn("fc_", sql.lower())

    def test_extras_legados_nao_sao_alvos(self):
        sql = R0001.read_text(encoding="utf-8").lower()
        for tabela in (
            "auditoria_associados", "cadastros", "contas_correntes",
            "denuncias", "epis", "fluxo_caixa", "ouvidoria_manifestacoes",
            "patrimonio", "produtos", "tipos_documentos",
        ):
            self.assertNotIn(tabela, sql)

    def test_prechecks_sao_somente_leitura_e_cobrem_divergencias(self):
        sql = PRECHECKS.read_text(encoding="utf-8")
        sem_comentarios = re.sub(r"--[^\n]*", "", sql)
        self.assertIsNone(
            re.search(
                r"\b(?:INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|TRUNCATE)\b",
                sem_comentarios,
                re.I,
            )
        )
        for tabela, coluna in (
            ("usuarios", "nome_completo"),
            ("associados", "uf"),
            ("transacoes_financeiras", "numero_documento"),
            ("solicitacoes_alteracao", "id"),
        ):
            self.assertIn(tabela, sql)
            self.assertIn(coluna, sql)

    def test_p05_pareia_fk_composta_pelo_mesmo_indice_posicional(self):
        sql = PRECHECKS.read_text(encoding="utf-8")
        p05 = sql.split("-- P05:", 1)[1].split("-- P06:", 1)[0]

        self.assertNotRegex(
            p05,
            r"unnest\s*\([^)]*conkey\s*,\s*[^)]*confkey",
        )
        self.assertIn(
            "pg_catalog.generate_subscripts(constraint_row.conkey, 1)", p05
        )
        self.assertIn("constraint_row.conkey[keys.position]", p05)
        self.assertIn("constraint_row.confkey[keys.position]", p05)
        self.assertIn(
            "ORDER BY parent.relname, constraint_row.conname, keys.position",
            p05,
        )
        self.assertIn("constraint_row.contype = 'f'", p05)

    def test_prechecks_p00_a_p07_permanecem_presentes(self):
        sql = PRECHECKS.read_text(encoding="utf-8")
        self.assertEqual(
            [f"P{numero:02d}" for numero in range(8)],
            re.findall(r"(?m)^-- (P\d{2}):", sql),
        )

    def test_r0001_permanece_byte_a_byte_inalterada(self):
        self.assertEqual(
            "57275057fd1b836ea959eb5de4eba148a3bd59d82ca0c88f2f9a7e3713f0ec67",
            hashlib.sha256(R0001.read_bytes()).hexdigest(),
        )

    def test_ordem_e_serial_identity_toleraveis_nao_geram_alter(self):
        sql = R0001.read_text(encoding="utf-8").lower()
        self.assertNotIn("usuarios", sql)
        self.assertNotIn("ordinal_position", sql)
        plano = PLAN.read_text(encoding="utf-8")
        self.assertIn("serial versus identity, mesmo tipo", plano)
        self.assertIn("Ordem física diferente nunca motiva", plano)

    def test_manifesto_e_24_checksums_normais_continuam_validos(self):
        manifesto = carregar_manifesto()
        persistiveis = [
            item for item in manifesto.operacoes if item.identificador != "M0000"
        ]
        self.assertEqual(24, len(persistiveis))
        self.assertTrue(all(item.checksum for item in persistiveis))
        self.assertNotIn("R0001", manifesto.por_id())

    def test_plano_nao_improvisa_adocao_no_ledger_atual(self):
        plano = PLAN.read_text(encoding="utf-8")
        self.assertRegex(plano, r"não\s+distingue adoção de execução")
        self.assertIn("Não será criada API de adoção nesta etapa", plano)
        self.assertIn("bootstrap atual recusa", plano)


if __name__ == "__main__":
    unittest.main()
