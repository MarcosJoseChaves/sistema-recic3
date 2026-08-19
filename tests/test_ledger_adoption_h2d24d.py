"""Provas offline da adoção explícita H2D.24D."""

from __future__ import annotations

import contextlib
import io
import sys
import types
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest import mock
from uuid import UUID


if "psycopg2" not in sys.modules:
    psycopg2 = types.ModuleType("psycopg2")
    extensions = types.ModuleType("psycopg2.extensions")
    sql_module = types.ModuleType("psycopg2.sql")
    extensions.TRANSACTION_STATUS_IDLE = 0
    extensions.TRANSACTION_STATUS_INTRANS = 2
    extensions.TRANSACTION_STATUS_INERROR = 3
    extensions.make_dsn = lambda **kwargs: ""
    extensions.parse_dsn = lambda value: {}
    psycopg2.connect = lambda *args, **kwargs: (_ for _ in ()).throw(
        AssertionError("Conexão PostgreSQL real proibida no teste H2D.24D")
    )
    psycopg2.sql = sql_module
    sql_module.SQL = lambda value: value
    sql_module.Identifier = lambda value: value
    sys.modules["psycopg2"] = psycopg2
    sys.modules["psycopg2.extensions"] = extensions
    sys.modules["psycopg2.sql"] = sql_module

from migrations_control.cli import main as cli_main
from migrations_control.errors import (
    ImpossibleLedgerStateError,
    InvalidLedgerError,
    UnknownDatabaseError,
)
from migrations_control.ledger import registrar_migration_adotada
from migrations_control.manifest import _validar_contrato_inicial, carregar_manifesto
from migrations_control.models import AppliedMigration, MigrationExecution, PreflightSnapshot
from migrations_control.preflight import LEDGER_OBJECTS, validar_conteudo_ledger
from migrations_control.runner import MigrationRunner
from migrations_control.schema_validation import EXPECTED_LEDGER_SCHEMA
from tests.test_migrations_control_h2c4a1 import (
    FakeConnection,
    snapshot_novo,
)


def prova(*, pre: bool, ok: bool = True):
    total = 23 if pre else 24
    return SimpleNamespace(
        global_result=ok,
        candidate_total=total,
        candidate_functional=total if ok else total - 1,
        m0001_state="AUSENTE" if pre else "PRESENTE_COMPLETO",
    )


def conteudo_persistido(conexao):
    aplicadas = []
    execucoes = []
    for sql, params, _ in conexao.executions:
        if sql.startswith("INSERT INTO public.schema_migrations "):
            aplicadas.append(AppliedMigration(
                params[0], params[3], params[1], params[4], params[2], params[8],
                params[5], params[6], params[7],
            ))
        elif sql.startswith("INSERT INTO public.schema_migration_execucoes "):
            execucoes.append(MigrationExecution(
                params[0], params[1], params[2], params[3], params[4], params[5],
                params[6], params[7], params[8], UUID(params[9]),
                params[10], params[11], params[12],
            ))
    return tuple(aplicadas), tuple(execucoes)


def criar_runner_adocao(conexao, *, snapshot=None):
    relogio = iter((0.0, 0.005)).__next__
    return MigrationRunner(
        conexao,
        snapshot_factory=lambda _: snapshot or snapshot_novo(("usuarios",)),
        schema_factory=lambda _: EXPECTED_LEDGER_SCHEMA,
        content_factory=lambda _: conteudo_persistido(conexao),
        clock=relogio,
        event_logger=lambda *args, **kwargs: None,
    )


class LedgerAdoptionTests(unittest.TestCase):
    def executar_sucesso(self):
        conexao = FakeConnection()
        runner = criar_runner_adocao(conexao)
        with (
            mock.patch(
                "migrations_control.runner.provar_legado_reconciliado_para_adocao",
                side_effect=lambda recebida: prova(pre=True),
            ) as pre,
            mock.patch(
                "migrations_control.runner.provar_catalogo_normativo_completo",
                side_effect=lambda recebida: prova(pre=False),
            ) as post,
        ):
            resultado = runner.adotar_legado_reconciliado()
        return conexao, runner, resultado, pre, post

    def test_adocao_atomica_estados_e_zero_sql_historico(self):
        conexao, runner, resultado, pre, post = self.executar_sucesso()
        aplicadas, execucoes = conteudo_persistido(conexao)
        self.assertTrue(resultado.sucesso)
        self.assertEqual((1, 0), (conexao.commits, conexao.rollbacks))
        self.assertEqual(24, len(aplicadas))
        self.assertEqual(24, len(execucoes))
        self.assertEqual(("M0001", "APLICADA", 0), (
            execucoes[0].migration_id, execucoes[0].situacao, execucoes[0].tentativa,
        ))
        self.assertTrue(all(
            item.situacao == "ADOTADA" and item.tentativa == 0
            and item.duracao_ms == 0 and item.erro_codigo is None
            and item.erro_sanitizado is None
            for item in execucoes[1:]
        ))
        self.assertEqual(23, len(resultado.ignoradas))
        ddl = [sql for sql, _, _ in conexao.executions if "CREATE TABLE" in sql]
        self.assertEqual(1, len(ddl))
        self.assertIn("schema_migrations", ddl[0])
        self.assertNotIn("fc_", ddl[0])
        pre.assert_called_once_with(conexao)
        post.assert_called_once_with(conexao)
        snapshot = PreflightSnapshot(
            True, LEDGER_OBJECTS | frozenset({"usuarios"}),
            assinatura_ledger=EXPECTED_LEDGER_SCHEMA,
            migrations_aplicadas=aplicadas, execucoes=execucoes,
        )
        self.assertFalse(any(
            item.estado == "PENDENTE" for item in runner.mostrar_plano(snapshot)
        ))

    def test_post_proof_falha_reverte_tudo(self):
        conexao = FakeConnection()
        runner = criar_runner_adocao(conexao)
        with (
            mock.patch(
                "migrations_control.runner.provar_legado_reconciliado_para_adocao",
                return_value=prova(pre=True),
            ),
            mock.patch(
                "migrations_control.runner.provar_catalogo_normativo_completo",
                return_value=prova(pre=False, ok=False),
            ),
            self.assertRaises(UnknownDatabaseError),
        ):
            runner.adotar_legado_reconciliado()
        self.assertEqual((0, 1), (conexao.commits, conexao.rollbacks))

    def test_pre_proof_falha_antes_de_m0001_e_reverte(self):
        conexao = FakeConnection()
        runner = criar_runner_adocao(conexao)
        def prova_pre_falha(_):
            conexao.transaction_status = 2
            return prova(pre=True, ok=False)
        with (
            mock.patch(
                "migrations_control.runner.provar_legado_reconciliado_para_adocao",
                side_effect=prova_pre_falha,
            ),
            mock.patch(
                "migrations_control.runner.provar_catalogo_normativo_completo"
            ) as post,
            self.assertRaises(UnknownDatabaseError),
        ):
            runner.adotar_legado_reconciliado()
        self.assertEqual((0, 1), (conexao.commits, conexao.rollbacks))
        self.assertFalse(any("CREATE TABLE" in sql for sql, _, _ in conexao.executions))
        post.assert_not_called()

    def test_fluxo_normal_bloqueia_populado_sem_ledger(self):
        conexao = FakeConnection()
        with self.assertRaises(UnknownDatabaseError):
            criar_runner_adocao(conexao).executar()
        self.assertFalse(any("CREATE TABLE" in sql for sql, _, _ in conexao.executions))

    def test_runner_normal_idempotente_apos_adocao(self):
        conexao_adocao, _, _, _, _ = self.executar_sucesso()
        aplicadas, execucoes = conteudo_persistido(conexao_adocao)
        snapshot = PreflightSnapshot(
            True, LEDGER_OBJECTS | frozenset({"usuarios"}),
            assinatura_ledger=EXPECTED_LEDGER_SCHEMA,
            migrations_aplicadas=aplicadas, execucoes=execucoes,
        )
        conexao = FakeConnection()
        runner = MigrationRunner(
            conexao, snapshot_factory=lambda _: snapshot,
            event_logger=lambda *args, **kwargs: None,
        )
        resultado = runner.executar()
        self.assertEqual((), resultado.aplicadas)
        self.assertEqual(24, len(resultado.ignoradas))
        self.assertFalse(any("CREATE TABLE" in sql for sql, _, _ in conexao.executions))

    def test_m0001_jamais_adotada(self):
        manifesto = carregar_manifesto()
        aplicadas, execucoes = conteudo_persistido(self.executar_sucesso()[0])
        ruim = replace(execucoes[0], situacao="ADOTADA", duracao_ms=0)
        snapshot = PreflightSnapshot(
            True, LEDGER_OBJECTS, assinatura_ledger=EXPECTED_LEDGER_SCHEMA,
            migrations_aplicadas=aplicadas, execucoes=(ruim, *execucoes[1:]),
        )
        valido, _ = validar_conteudo_ledger(snapshot, manifesto)
        self.assertFalse(valido)
        with self.assertRaises(ImpossibleLedgerStateError):
            registrar_migration_adotada(
                FakeConnection().cursor(), manifesto.por_id()["M0001"],
                request_id=execucoes[0].request_id,
                adotada_em=execucoes[0].iniciada_em,
                manifesto_versao=manifesto.versao_formato,
            )

    def test_prefixo_parcial_com_gap_e_rejeitado(self):
        aplicadas, execucoes = conteudo_persistido(self.executar_sucesso()[0])
        snapshot = PreflightSnapshot(
            True, LEDGER_OBJECTS, assinatura_ledger=EXPECTED_LEDGER_SCHEMA,
            migrations_aplicadas=(aplicadas[0], aplicadas[2]),
            execucoes=(execucoes[0], execucoes[2]),
        )
        self.assertFalse(validar_conteudo_ledger(snapshot, carregar_manifesto())[0])

    def test_prefixo_dependency_closed_e_valido(self):
        aplicadas, execucoes = conteudo_persistido(self.executar_sucesso()[0])
        snapshot = PreflightSnapshot(
            True, LEDGER_OBJECTS, assinatura_ledger=EXPECTED_LEDGER_SCHEMA,
            migrations_aplicadas=aplicadas[:2], execucoes=execucoes[:2],
        )
        self.assertTrue(validar_conteudo_ledger(snapshot, carregar_manifesto())[0])

    def test_qualquer_objeto_parcial_do_ledger_bloqueia_adocao(self):
        conexao = FakeConnection()
        runner = criar_runner_adocao(
            conexao, snapshot=snapshot_novo(("usuarios", "relation:S:schema_migrations_id_seq"))
        )
        with (
            mock.patch(
                "migrations_control.runner.provar_legado_reconciliado_para_adocao"
            ) as pre,
            self.assertRaises(UnknownDatabaseError),
        ):
            runner.adotar_legado_reconciliado()
        pre.assert_not_called()
        self.assertEqual((0, 0), (conexao.commits, conexao.rollbacks))

    def test_cli_adocao_e_explicita_e_exige_conexao(self):
        saida = io.StringIO()
        with contextlib.redirect_stdout(saida):
            codigo = cli_main(["adotar-legado-reconciliado"])
        self.assertEqual(2, codigo)
        with mock.patch(
            "migrations_control.cli.MigrationRunner"
        ) as factory, contextlib.redirect_stdout(io.StringIO()):
            factory.return_value.adotar_legado_reconciliado.return_value = SimpleNamespace(
                sucesso=True, classificacao_preflight="BANCO_CONTROLADO",
                aplicadas=("M0001",), ignoradas=("M0002",), codigo_saida=0,
            )
            self.assertEqual(0, cli_main(
                ["adotar-legado-reconciliado"], conexao=object()
            ))
        factory.return_value.adotar_legado_reconciliado.assert_called_once_with()


class FutureManifestContinuityTests(unittest.TestCase):
    def fixture_futura(self):
        atuais = list(carregar_manifesto().operacoes)
        m_ops = [op for op in atuais if op.identificador.startswith("M")]
        h_ops = [op for op in atuais if op.identificador.startswith("H")]
        m14 = replace(
            m_ops[-1], identificador="M0014", dependencias=("M0013",),
        )
        cadeia = [*m_ops, m14, *h_ops]
        h12 = replace(h_ops[-1], identificador="H012", dependencias=("H011",))
        cadeia.append(h12)
        resultado = []
        anterior = None
        for ordem, op in enumerate(cadeia):
            deps = () if ordem == 0 else (anterior,)
            resultado.append(replace(op, ordem_global=ordem, dependencias=deps))
            anterior = op.identificador
        return tuple(resultado)

    def test_m0014_h012_sinteticos_aceitos_sem_hardcode(self):
        _validar_contrato_inicial(self.fixture_futura())

    def test_gap_sintetico_rejeitado(self):
        fixture = tuple(
            op for op in self.fixture_futura() if op.identificador != "M0013"
        )
        with self.assertRaises(Exception):
            _validar_contrato_inicial(fixture)


if __name__ == "__main__":
    unittest.main()
