"""Persistência ESG com DB-API simulado; SQL só é inspecionado como texto."""

from collections import deque
from dataclasses import dataclass, replace
from datetime import date
from decimal import Decimal
import os
from pathlib import Path
import unittest
from unittest.mock import Mock, patch
from uuid import UUID

from modulos.esg.contratos import (
    DefinicaoIndicador, DimensaoESG, Evidencia, FonteDocumental, Perimetro,
    Periodo, RegistroPeriodo, SituacaoIndicador, SituacaoRegistro, UnidadeMedida,
)
from modulos.esg.repositorio import (
    ConfirmacaoIncertaESGError, ConflitoESGError, IntegridadeESGError, NaoEncontradoESGError,
    PersistenciaESGError, RepositorioESG,
)
from modulos.esg.validacoes import ContratoESGInvalido


class ErroDBSimulado(Exception):
    def __init__(self, codigo=None):
        super().__init__("Detalhe interno de teste que não deve ser exposto")
        self.pgcode = codigo


@dataclass
class Passo:
    sql: str
    resultado: object = None
    erro: Exception | None = None


class CursorSimulado:
    """Respostas roteirizadas; não interpreta SQL nem simula o PostgreSQL."""

    def __init__(self, conexao):
        self.conexao = conexao
        self.resultado = None
        self.fechado = False

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.fechado = True

    def execute(self, sql, parametros):
        sql = " ".join(sql.split())
        self.conexao.execucoes.append((sql, parametros))
        if not self.conexao.passos:
            raise AssertionError("Comando não previsto no roteiro simulado")
        passo = self.conexao.passos.popleft()
        if passo.sql not in sql:
            raise AssertionError(f"Esperado {passo.sql}; recebido {sql}")
        if passo.erro:
            raise passo.erro
        self.resultado = passo.resultado

    def fetchone(self):
        return self.resultado

    def fetchall(self):
        return self.resultado


class ConexaoSimulada:
    def __init__(self, passos=()):
        self.passos = deque(passos)
        self.execucoes = []
        self.autocommit = False
        self.closed = 0
        self.status = 0
        self.commits = self.rollbacks = self.fechamentos = 0
        self.erro_commit = self.erro_rollback = self.erro_close = None
        self.sessoes = []
        self.cursor_simulado = CursorSimulado(self)

    def get_transaction_status(self):
        return self.status

    def cursor(self):
        return self.cursor_simulado

    def set_session(self, **opcoes):
        self.sessoes.append(opcoes)

    def commit(self):
        self.commits += 1
        if self.passos:
            raise AssertionError("Transação confirmou antes de concluir o roteiro")
        if self.erro_commit:
            raise self.erro_commit

    def rollback(self):
        self.rollbacks += 1
        if self.erro_rollback:
            raise self.erro_rollback

    def close(self):
        self.fechamentos += 1
        if self.erro_close:
            raise self.erro_close
        self.closed = 1


class TestRepositorioESG(unittest.TestCase):
    def setUp(self):
        # Nenhum driver é importado. Todas as fábricas abaixo retornam somente
        # ConexaoSimulada. Barreiras adicionais tornam qualquer I/O inesperado fatal.
        self.enterContext(patch.dict(os.environ, {}, clear=True))
        self.bloqueios = [self.enterContext(patch(alvo, side_effect=AssertionError("I/O proibido no teste"))) for alvo in (
            "builtins.open", "io.open", "socket.socket.connect", "socket.socket.connect_ex",
            "socket.create_connection", "socket.getaddrinfo", "socket.socket.bind",
        )]
        self.fonte = FonteDocumental("fonte-teste", "Documento fictício, somente teste.", date(2026, 10, 27))
        self.perimetro = Perimetro("recorte-teste", "Abrangência fictícia de teste.")
        self.definicao = DefinicaoIndicador(
            "indicador-teste", "Indicador de teste", DimensaoESG.AMBIENTAL,
            "Descrição fictícia.", UnidadeMedida("t", "Toneladas"),
            "Sem metodologia institucional validada.", self.perimetro, self.fonte,
        )
        self.registro = RegistroPeriodo(UUID(int=1), self.definicao.codigo, Periodo(2025), self.fonte, date(2025, 12, 31), self.perimetro)
        self.evidencia = Evidencia(UUID(int=2), self.registro.identificador, "Documento de teste", self.fonte, "Observação fictícia.")

    def tearDown(self):
        for bloqueio in self.bloqueios:
            bloqueio.assert_not_called()

    def fonte_linha(self, fonte=None):
        fonte = fonte or self.fonte
        return (fonte.identificador, fonte.referencia_documental, fonte.data_documento)

    def definicao_linha(self, definicao=None):
        d = definicao or self.definicao
        aprovacao = self.fonte_linha(d.fonte_aprovacao) if d.fonte_aprovacao else (None,) * 3
        return (d.codigo, d.nome, d.dimensao.value, d.descricao, d.metodologia, d.situacao.value,
                d.unidade.simbolo, d.unidade.descricao, d.perimetro.codigo, d.perimetro.descricao,
                *self.fonte_linha(d.fonte_metodologica), *aprovacao)

    def registro_linha(self, registro=None):
        r = registro or self.registro
        aprovacao = self.fonte_linha(r.fonte_aprovacao) if r.fonte_aprovacao else (None,) * 3
        return (str(r.identificador), r.indicador_codigo, r.periodo.ano, r.periodo.mes, r.valor,
                r.data_referencia, r.situacao.value, r.perimetro.codigo, r.perimetro.descricao,
                *self.fonte_linha(r.fonte), *aprovacao)

    def evidencia_linha(self):
        e = self.evidencia
        return (str(e.identificador), str(e.registro_id), e.tipo, e.observacoes, *self.fonte_linha())

    def passos_fonte(self):
        return [Passo("INSERT INTO esg_fontes_documentais"), Passo("FROM esg_fontes_documentais WHERE", self.fonte_linha())]

    def passos_definicao(self):
        return [*self.passos_fonte(), Passo("INSERT INTO esg_perimetros"),
                Passo("SELECT codigo, descricao", (self.perimetro.codigo, self.perimetro.descricao)),
                Passo("INSERT INTO esg_unidades"), Passo("SELECT simbolo, descricao", ("t", "Toneladas")),
                Passo("INSERT INTO esg_definicoes")]

    def passos_registro(self, com_evidencia=False):
        passos = [Passo("FROM esg_definicoes d", self.definicao_linha()), *self.passos_fonte(), Passo("INSERT INTO esg_registros")]
        if com_evidencia:
            passos += [*self.passos_fonte(), Passo("INSERT INTO esg_evidencias")]
        return passos

    def repositorio(self, passos=()):
        conexao = ConexaoSimulada(passos)
        fabrica = Mock(return_value=conexao)
        return RepositorioESG(fabrica), conexao, fabrica

    def conferir_sucesso(self, conexao):
        self.assertEqual(conexao.commits, 1)
        self.assertEqual(conexao.rollbacks, 0)
        self.assertEqual(conexao.fechamentos, 1)
        self.assertTrue(conexao.cursor_simulado.fechado)
        self.assertFalse(conexao.passos)

    def conferir_rollback(self, conexao):
        self.assertEqual(conexao.rollbacks, 1)
        self.assertEqual(conexao.fechamentos, 1)

    def test_construtor_nao_abre_conexao(self):
        _, conexao, fabrica = self.repositorio()
        fabrica.assert_not_called()
        self.assertEqual(conexao.execucoes, [])
        with self.assertRaises(ContratoESGInvalido):
            RepositorioESG(conexao)

    def test_salvar_definicao_proposta_com_fontes_e_autoria(self):
        repo, conexao, fabrica = self.repositorio(self.passos_definicao())
        self.assertIs(repo.salvar_definicao(self.definicao, 7), self.definicao)
        fabrica.assert_called_once_with()
        sql, params = conexao.execucoes[-1]
        self.assertIn("'proposto'", sql)
        self.assertNotIn("fonte_aprovacao_id", sql)
        self.assertEqual(params, (self.definicao.codigo, self.definicao.nome, "ambiental", self.definicao.descricao,
                                 "t", self.definicao.metodologia, self.perimetro.codigo, self.fonte.identificador, 7))
        self.conferir_sucesso(conexao)

    def test_cadastros_recusam_estados_e_aprovacao_forjados_antes_da_conexao(self):
        repo, _, fabrica = self.repositorio()
        definicoes = [replace(self.definicao, situacao=s, fonte_aprovacao=self.fonte if s is SituacaoIndicador.APROVADO else None)
                      for s in (SituacaoIndicador.APROVADO, SituacaoIndicador.EM_ANALISE, SituacaoIndicador.REJEITADO)]
        definicoes.append(replace(self.definicao, fonte_aprovacao=self.fonte))
        for definicao in definicoes:
            with self.subTest(situacao=definicao.situacao), self.assertRaises(ContratoESGInvalido):
                repo.salvar_definicao(definicao, 7)
        registros = [replace(self.registro, valor=Decimal("0"), situacao=s,
                             fonte_aprovacao=self.fonte if s is SituacaoRegistro.APROVADO else None)
                     for s in (SituacaoRegistro.APROVADO, SituacaoRegistro.EM_ANALISE, SituacaoRegistro.REJEITADO)]
        registros.append(replace(self.registro, fonte_aprovacao=self.fonte))
        for registro in registros:
            with self.subTest(situacao=registro.situacao), self.assertRaises(ContratoESGInvalido):
                repo.salvar_registro(registro, 7)
        fabrica.assert_not_called()

    def test_entrada_nao_tipado_e_autor_invalido_nao_abrem_conexao(self):
        repo, _, fabrica = self.repositorio()
        for chamada in (lambda: repo.salvar_definicao({"situacao": "aprovado"}, 7),
                        lambda: repo.salvar_registro({"situacao": "aprovado"}, 7),
                        lambda: repo.salvar_fonte({}, 7), lambda: repo.associar_evidencia({}, 7),
                        lambda: repo.obter_registro(self.definicao.codigo, "2025", self.perimetro),
                        lambda: repo.listar_registros(self.definicao.codigo, self.perimetro.codigo),
                        lambda: repo.salvar_registro(self.registro, 7, iter(()))):
            with self.assertRaises(ContratoESGInvalido):
                chamada()
        for usuario_id in (None, True, 0, -1, "7", 7.0):
            with self.subTest(usuario=usuario_id), self.assertRaises(ContratoESGInvalido):
                repo.salvar_definicao(self.definicao, usuario_id)
        fabrica.assert_not_called()

    def test_registro_preserva_none_zero_decimal_data_uuid_e_competencia(self):
        for valor in (None, Decimal("0"), Decimal("11343.0700123456789"), Decimal("-1.23")):
            for periodo in (Periodo(2025), Periodo(2025, 12)):
                with self.subTest(valor=valor, periodo=periodo):
                    registro = replace(self.registro, valor=valor, periodo=periodo)
                    repo, conexao, _ = self.repositorio(self.passos_registro())
                    self.assertIs(repo.salvar_registro(registro, 7), registro)
                    sql, params = conexao.execucoes[-1]
                    self.assertIn("'rascunho'", sql)
                    self.assertNotIn("fonte_aprovacao_id", sql)
                    self.assertEqual(params, (str(registro.identificador), registro.indicador_codigo, 2025, periodo.mes,
                                             valor, self.fonte.identificador, date(2025, 12, 31), self.perimetro.codigo, 7))
                    self.assertIs(params[4], valor)
                    self.assertTrue(all(type(p) is not float for p in params))
                    self.conferir_sucesso(conexao)

    def test_registro_e_evidencia_atomicos(self):
        repo, conexao, _ = self.repositorio(self.passos_registro(True))
        repo.salvar_registro(self.registro, 7, [self.evidencia])
        self.assertEqual(conexao.execucoes[-1][1], (str(self.evidencia.identificador), str(self.registro.identificador),
                         self.evidencia.tipo, self.fonte.identificador, self.evidencia.observacoes, 7))
        self.conferir_sucesso(conexao)

    def test_falha_em_cada_etapa_de_cadastro_reverte_tudo(self):
        for operacao, passos in (("definicao", self.passos_definicao()), ("registro", self.passos_registro(True))):
            for indice in range(len(passos)):
                roteiro = [replace(p) for p in passos]
                roteiro[indice].erro = ErroDBSimulado()
                repo, conexao, _ = self.repositorio(roteiro)
                with self.subTest(operacao=operacao, passo=indice), self.assertRaises(PersistenciaESGError) as contexto:
                    if operacao == "definicao":
                        repo.salvar_definicao(self.definicao, 7)
                    else:
                        repo.salvar_registro(self.registro, 7, [self.evidencia])
                self.assertNotIn("Detalhe interno", str(contexto.exception))
                self.assertEqual(conexao.commits, 0)
                self.assertEqual(len(conexao.execucoes), indice + 1)
                self.conferir_rollback(conexao)

    def test_conflitos_e_integridade_do_driver_sem_vazar_detalhes(self):
        for codigo, esperado in (("23505", ConflitoESGError), ("23503", IntegridadeESGError),
                                 ("23502", IntegridadeESGError), ("23514", IntegridadeESGError),
                                 ("22003", IntegridadeESGError)):
            passos = self.passos_registro()
            passos[-1].erro = ErroDBSimulado(codigo)
            repo, conexao, _ = self.repositorio(passos)
            with self.subTest(codigo=codigo), self.assertRaises(esperado) as contexto:
                repo.salvar_registro(self.registro, 7)
            self.assertNotIn("Detalhe interno", str(contexto.exception))
            self.assertEqual(conexao.commits, 0)
            self.conferir_rollback(conexao)

    def test_identidades_de_fonte_perimetro_e_unidade_nao_sao_sobrescritas(self):
        for indice, resultado in ((1, ("fonte-teste", "Documento divergente", None)),
                                  (3, (self.perimetro.codigo, "Outra abrangência")),
                                  (5, ("t", "Outra descrição"))):
            passos = self.passos_definicao()
            passos[indice].resultado = resultado
            repo, conexao, _ = self.repositorio(passos)
            with self.subTest(indice=indice), self.assertRaises(ConflitoESGError):
                repo.salvar_definicao(self.definicao, 7)
            self.assertFalse(any("INSERT INTO esg_definicoes" in sql for sql, _ in conexao.execucoes))
            self.conferir_rollback(conexao)

    def test_salvar_fonte_idempotente_somente_quando_identica(self):
        repo, conexao, _ = self.repositorio(self.passos_fonte())
        self.assertEqual(repo.salvar_fonte(self.fonte, 7), self.fonte)
        self.assertIn("DO NOTHING", conexao.execucoes[0][0])
        self.conferir_sucesso(conexao)

    def test_definicao_ausente_ou_perimetro_distinto_rejeitam_registro(self):
        for linha, registro, erro in ((None, self.registro, NaoEncontradoESGError),
                                      (self.definicao_linha(), replace(self.registro, perimetro=Perimetro("outro", "Outro recorte")), ContratoESGInvalido),
                                      (self.definicao_linha(), replace(self.registro, perimetro=replace(self.perimetro, descricao="Outro recorte")), ContratoESGInvalido)):
            repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", linha)])
            with self.subTest(perimetro=registro.perimetro), self.assertRaises(erro):
                repo.salvar_registro(registro, 7)
            self.assertEqual(len(conexao.execucoes), 1)
            self.conferir_rollback(conexao)

    def test_evidencias_orfas_e_duplicadas_nao_gravam_registro(self):
        for evidencias in ((replace(self.evidencia, registro_id=UUID(int=999)),), (self.evidencia, self.evidencia)):
            repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", self.definicao_linha())])
            with self.assertRaises(ContratoESGInvalido):
                repo.salvar_registro(self.registro, 7, evidencias)
            self.assertEqual(len(conexao.execucoes), 1)
            self.conferir_rollback(conexao)

    def test_associar_evidencia_somente_em_rascunho_bloqueado(self):
        passos = [Passo("FROM esg_registros r", self.registro_linha()),
                  Passo("FROM esg_definicoes d", self.definicao_linha()), *self.passos_fonte(), Passo("INSERT INTO esg_evidencias")]
        repo, conexao, _ = self.repositorio(passos)
        self.assertEqual(repo.associar_evidencia(self.evidencia, 7), self.evidencia)
        self.assertIn("FOR SHARE OF r", conexao.execucoes[0][0])
        self.assertIn("FOR SHARE OF d", conexao.execucoes[1][0])
        self.conferir_sucesso(conexao)

    def test_associacao_recusa_registro_ausente_e_outros_estados(self):
        for situacao in (None, SituacaoRegistro.EM_ANALISE, SituacaoRegistro.REJEITADO, SituacaoRegistro.APROVADO):
            registro = replace(self.registro, situacao=situacao, valor=Decimal("0"),
                               fonte_aprovacao=self.fonte if situacao is SituacaoRegistro.APROVADO else None) if situacao else None
            repo, conexao, _ = self.repositorio([Passo("FROM esg_registros r", self.registro_linha(registro) if registro else None)])
            with self.subTest(situacao=situacao), self.assertRaises(NaoEncontradoESGError if situacao is None else IntegridadeESGError):
                repo.associar_evidencia(self.evidencia, 7)
            self.assertEqual(len(conexao.execucoes), 1)
            self.conferir_rollback(conexao)

    def test_consulta_definicao_reconstitui_contrato(self):
        for situacao in SituacaoIndicador:
            definicao = replace(self.definicao, situacao=situacao, fonte_aprovacao=self.fonte if situacao is SituacaoIndicador.APROVADO else None)
            repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", self.definicao_linha(definicao))])
            with self.subTest(situacao=situacao):
                self.assertEqual(repo.obter_definicao(definicao.codigo), definicao)
                self.assertEqual(conexao.sessoes, [{"readonly": True, "isolation_level": "REPEATABLE READ"}])
                self.conferir_sucesso(conexao)

    def test_consulta_por_codigo_periodo_preserva_valor_e_fonte(self):
        for valor in (None, Decimal("0"), Decimal("1.234567890123456789")):
            for periodo in (Periodo(2025), Periodo(2025, 12)):
                registro = replace(self.registro, valor=valor, periodo=periodo)
                repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", self.definicao_linha()),
                                                    Passo("FROM esg_registros r", self.registro_linha(registro))])
                with self.subTest(valor=valor, periodo=periodo):
                    recebido = repo.obter_registro(registro.indicador_codigo, periodo, self.perimetro)
                    self.assertEqual(recebido, registro)
                    self.assertIs(recebido.valor, valor)
                    self.assertIs(type(recebido.identificador), UUID)
                    self.assertIn("IS NOT DISTINCT FROM %s", conexao.execucoes[-1][0])
                    self.assertEqual(conexao.execucoes[-1][1], (registro.indicador_codigo, 2025, periodo.mes, self.perimetro.codigo))
                    self.conferir_sucesso(conexao)

    def test_consultas_sem_dados(self):
        for consulta in (lambda r: r.obter_definicao(self.definicao.codigo),
                         lambda r: r.obter_registro(self.definicao.codigo, Periodo(2025), self.perimetro),
                         lambda r: r.listar_registros(self.definicao.codigo, self.perimetro)):
            repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", None)])
            self.assertIsNone(consulta(repo))
            self.conferir_sucesso(conexao)
        repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", self.definicao_linha()), Passo("FROM esg_registros r", None)])
        self.assertIsNone(repo.obter_registro(self.definicao.codigo, Periodo(2025), self.perimetro))
        self.conferir_sucesso(conexao)
        repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", self.definicao_linha()),
                                            Passo("FROM esg_registros r", []), Passo("FROM esg_evidencias e", [])])
        self.assertEqual(repo.listar_registros(self.definicao.codigo, self.perimetro).registros, ())
        self.conferir_sucesso(conexao)

    def test_listagem_reconstitui_fontes_evidencias_estados_e_ordem(self):
        for situacao in SituacaoRegistro:
            definicao = replace(self.definicao, situacao=SituacaoIndicador.APROVADO, fonte_aprovacao=self.fonte)
            registro = replace(self.registro, valor=Decimal("1.23456789"), situacao=situacao,
                               fonte_aprovacao=self.fonte if situacao is SituacaoRegistro.APROVADO else None)
            anterior = replace(self.registro, identificador=UUID(int=3), periodo=Periodo(2024), data_referencia=date(2024, 12, 31))
            repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", self.definicao_linha(definicao)),
                                                Passo("FROM esg_registros r", [self.registro_linha(registro), self.registro_linha(anterior)]),
                                                Passo("FROM esg_evidencias e", [self.evidencia_linha()])])
            with self.subTest(situacao=situacao):
                serie = repo.listar_registros(definicao.codigo, self.perimetro)
                self.assertEqual(serie.registros, (anterior, registro))
                self.assertEqual(serie.evidencias, (self.evidencia,))
                self.assertEqual(serie.registros[-1].fonte, self.fonte)
                self.assertIs(serie.registros[-1].situacao, situacao)
                for _, params in conexao.execucoes[1:]:
                    self.assertEqual(params, (definicao.codigo, self.perimetro.codigo))
                self.conferir_sucesso(conexao)

    def test_consultas_rejeitam_perimetro_distinto(self):
        for perimetro in (Perimetro("outro", "Outra abrangência"), replace(self.perimetro, descricao="Outra abrangência")):
            for listar in (False, True):
                repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", self.definicao_linha())])
                with self.subTest(perimetro=perimetro, listar=listar), self.assertRaises(IntegridadeESGError):
                    if listar:
                        repo.listar_registros(self.definicao.codigo, perimetro)
                    else:
                        repo.obter_registro(self.definicao.codigo, Periodo(2025), perimetro)
                self.assertEqual(len(conexao.execucoes), 1)
                self.conferir_rollback(conexao)

    def test_leitura_corrompida_reverte_sem_entregar_contrato_invalido(self):
        for indice, valor in ((4, 1.25), (4, Decimal("NaN")), (0, "uuid-invalido"), (7, "outro"), (6, "estado-invalido")):
            linha = list(self.registro_linha())
            linha[indice] = valor
            repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", self.definicao_linha()), Passo("FROM esg_registros r", tuple(linha))])
            with self.subTest(indice=indice, valor=valor), self.assertRaises((PersistenciaESGError, ContratoESGInvalido)):
                repo.obter_registro(self.definicao.codigo, Periodo(2025), self.perimetro)
            self.conferir_rollback(conexao)

    def test_falha_de_consulta_commit_rollback_e_close(self):
        repo, conexao, _ = self.repositorio([Passo("FROM esg_definicoes d", erro=ErroDBSimulado())])
        with self.assertRaises(PersistenciaESGError):
            repo.obter_definicao(self.definicao.codigo)
        self.conferir_rollback(conexao)
        for falha in ("commit", "rollback", "close"):
            passos = self.passos_fonte()
            if falha == "rollback":
                passos[0].erro = ErroDBSimulado()
            repo, conexao, _ = self.repositorio(passos)
            setattr(conexao, "erro_" + falha, ErroDBSimulado())
            with self.subTest(falha=falha), self.assertRaises(PersistenciaESGError) as contexto:
                repo.salvar_fonte(self.fonte, 7)
            self.assertNotIn("Detalhe interno", str(contexto.exception))
            if falha == "commit":
                self.assertIsInstance(contexto.exception, ConfirmacaoIncertaESGError)
            self.assertEqual(conexao.fechamentos, 1)
            self.assertEqual(conexao.rollbacks, 0 if falha == "close" else 1)

    def test_falha_na_fabrica_nao_tenta_reconectar(self):
        fabrica = Mock(side_effect=ErroDBSimulado())
        with self.assertRaises(PersistenciaESGError):
            RepositorioESG(fabrica).obter_definicao(self.definicao.codigo)
        fabrica.assert_called_once_with()

    def test_conexao_invalida_ou_em_uso_nao_executa_sql_nem_finaliza_transacao_alheia(self):
        for atributo, valor in (("closed", 1), ("autocommit", True), ("autocommit", None), ("status", 2), ("status", 3)):
            repo, conexao, _ = self.repositorio()
            setattr(conexao, atributo, valor)
            with self.subTest(atributo=atributo, valor=valor), self.assertRaises(PersistenciaESGError):
                repo.obter_definicao(self.definicao.codigo)
            self.assertEqual(conexao.execucoes, [])
            self.assertEqual((conexao.commits, conexao.rollbacks, conexao.fechamentos), (0, 0, 0))

    def test_texto_externo_e_parametrizado_sem_interpolacao(self):
        fonte = replace(self.fonte, referencia_documental="Documento '); DROP TABLE usuarios; --")
        repo, conexao, _ = self.repositorio([Passo("INSERT INTO esg_fontes_documentais"), Passo("FROM esg_fontes_documentais WHERE", self.fonte_linha(fonte))])
        repo.salvar_fonte(fonte, 7)
        self.assertNotIn(fonte.referencia_documental, conexao.execucoes[0][0])
        self.assertEqual(conexao.execucoes[0][1][1], fonte.referencia_documental)
        self.conferir_sucesso(conexao)


class TestEsquemaESGSomenteTexto(unittest.TestCase):
    def test_ddl_aditivo_com_integridade_sem_carga_ou_alteracoes(self):
        # Única leitura de arquivo nesta classe: SQL versionado, nunca executado.
        arquivo = Path(__file__).resolve().parents[1] / "modulos/esg/migrations/001_criar_esg_indicadores.sql"
        sql = arquivo.read_text(encoding="utf-8")
        self.assertIn("BEGIN;", sql)
        self.assertTrue(sql.rstrip().endswith("COMMIT;"))
        for tabela in ("fontes_documentais", "perimetros", "unidades", "definicoes", "registros", "evidencias"):
            self.assertIn("CREATE TABLE IF NOT EXISTS esg_" + tabela, sql)
        for restricao in ("REFERENCES usuarios(id)", "FOREIGN KEY (indicador_codigo, perimetro_codigo)",
                          "REFERENCES esg_definicoes(codigo, perimetro_codigo)", "WHERE mes IS NULL",
                          "WHERE mes IS NOT NULL", "EXTRACT(YEAR FROM data_referencia) = ano",
                          "valor NUMERIC,", "identificador UUID PRIMARY KEY", "CHECK (revisao = 1)"):
            self.assertIn(restricao, sql)
        self.assertNotRegex(sql, r"(?i)\b(INSERT|UPDATE|DELETE|DROP|ALTER|TRUNCATE)\b")
        self.assertNotIn("ON DELETE CASCADE", sql)


if __name__ == "__main__":
    unittest.main()
