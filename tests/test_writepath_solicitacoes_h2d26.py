import ast
import re
import unittest
from pathlib import Path
from types import SimpleNamespace


ROOT = Path(__file__).resolve().parents[1]
APP = ROOT / "app.py"
M0013 = ROOT / "migrations_control/sql/M0013_solicitacoes.sql"


class Cursor:
    def __init__(self, row=(1,)):
        self.row = row
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))

    def fetchone(self):
        return self.row


def carregar_funcao(usuario=None):
    tree = ast.parse(APP.read_text(encoding="utf-8"), filename=str(APP))
    node = next(
        item for item in tree.body
        if isinstance(item, ast.FunctionDef)
        and item.name == "_inserir_solicitacao_escopada"
    )
    namespace = {
        "current_user": usuario or SimpleNamespace(
            id=7, username="operador", uvr_acesso="UVR-1"
        )
    }
    exec(compile(ast.Module(body=[node], type_ignores=[]), str(APP), "exec"), namespace)
    return namespace[node.name]


class H2D26WritePathTests(unittest.TestCase):
    def executar(self, *, usuario=None, dados='{"nome":"novo"}', tipo="EDICAO"):
        cursor = Cursor()
        resultado = carregar_funcao(usuario)(
            cursor, "associados", 31, tipo, dados
        )
        return resultado, cursor

    def test_not_null_normativos_sem_default_sao_fornecidos(self):
        resultado, cursor = self.executar()
        self.assertTrue(resultado)
        sql, _ = cursor.calls[0]
        for coluna in (
            "identificador_publico", "tipo", "modulo", "objeto_tipo_logico",
            "objeto_identificador_logico", "solicitante_usuario_id", "estado",
            "risco", "versao_esperada", "fotografia_proposta", "request_id",
        ):
            self.assertIn(coluna, sql)

    def test_campos_legados_e_dados_novos_sao_preservados(self):
        dados = '{"campo":"valor"}'
        _, cursor = self.executar(dados=dados)
        sql, params = cursor.calls[0]
        for coluna in (
            "tabela_alvo", "id_registro", "tipo_solicitacao",
            "dados_novos", "usuario_solicitante",
        ):
            self.assertIn(coluna, sql)
        self.assertEqual(params.count(dados), 2)

    def test_mapping_legado_normativo_e_exato(self):
        _, cursor = self.executar(tipo=" EXCLUSAO ")
        sql, params = cursor.calls[0]
        self.assertEqual(params[1], "EXCLUSAO")
        self.assertEqual(params[4], "EXCLUSAO")
        self.assertEqual(params[5], "associados")
        for literal in ("'LEGADO'", "'ENVIADA'", "'LEGADO_NAO_CLASSIFICADO'", " 0,"):
            self.assertIn(literal, sql)

    def test_identificadores_usam_geracao_uuid_do_postgresql(self):
        _, cursor = self.executar()
        sql, _ = cursor.calls[0]
        self.assertEqual(sql.count("pg_catalog.gen_random_uuid()"), 2)
        m0013 = M0013.read_text(encoding="utf-8")
        self.assertIn("UNIQUE (identificador_publico)", m0013)

    def test_duas_solicitacoes_nao_reutilizam_uuid_parametrizado(self):
        _, primeiro = self.executar()
        _, segundo = self.executar()
        for cursor in (primeiro, segundo):
            sql, params = cursor.calls[0]
            self.assertEqual(sql.count("pg_catalog.gen_random_uuid()"), 2)
            self.assertFalse(any(re.fullmatch(r"[0-9a-f-]{36}", str(x)) for x in params))

    def test_usuario_e_resolvido_pelo_id_autenticado(self):
        _, cursor = self.executar(
            usuario=SimpleNamespace(id=42, username="real", uvr_acesso="UVR-1")
        )
        _, params = cursor.calls[0]
        self.assertEqual(params[3], "real")
        self.assertEqual(params[6], 42)

    def test_informacao_obrigatoria_ausente_falha_controladamente(self):
        casos = (
            SimpleNamespace(id=None, username="real", uvr_acesso="UVR-1"),
            SimpleNamespace(id=1, username="", uvr_acesso="UVR-1"),
            SimpleNamespace(id=1, username="real", uvr_acesso=""),
        )
        for usuario in casos:
            cursor = Cursor()
            self.assertFalse(carregar_funcao(usuario)(cursor, "associados", 31, "EDICAO", "{}"))
            self.assertEqual(cursor.calls, [])

    def test_tipo_vazio_e_alvo_inexistente_falham_controladamente(self):
        funcao = carregar_funcao()
        for tabela, tipo in (("associados", "  "), ("nao_permitida", "EDICAO")):
            cursor = Cursor()
            self.assertFalse(funcao(cursor, tabela, 31, tipo, "{}"))
            self.assertEqual(cursor.calls, [])


if __name__ == "__main__":
    unittest.main()
