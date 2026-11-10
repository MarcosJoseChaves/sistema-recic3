"""Persistência ESG DB-API com conexão dedicada injetada, sem importar app/driver.

Nenhuma conexão é criada na importação ou no construtor. O chamador fornece uma
fábrica de conexões PostgreSQL dedicadas, abertas, ociosas e sem autocommit.
"""

from contextlib import contextmanager
from uuid import UUID

from .contratos import (
    DefinicaoIndicador, DimensaoESG, Evidencia, FonteDocumental, Perimetro,
    Periodo, RegistroPeriodo, SituacaoIndicador, SituacaoRegistro, UnidadeMedida,
)
from .services import organizar_registros
from .validacoes import ContratoESGInvalido, exigir_tipo, validar_codigo


class PersistenciaESGError(Exception):
    """Falha segura de persistência, sem mensagem SQL ou credenciais."""


class ConflitoESGError(PersistenciaESGError):
    """Identidade ou período já utilizado; nenhuma sobrescrita é permitida."""


class IntegridadeESGError(PersistenciaESGError):
    """Vínculo ou dado armazenado incompatível com os contratos."""


class NaoEncontradoESGError(PersistenciaESGError):
    """Entidade exigida para o cadastro não encontrada."""


class ConfirmacaoIncertaESGError(PersistenciaESGError):
    """Commit sem confirmação confiável; consultar antes de repetir a operação."""


DEFINICAO_SELECT = """
SELECT d.codigo, d.nome, d.dimensao, d.descricao, d.metodologia, d.situacao,
       u.simbolo, u.descricao, p.codigo, p.descricao,
       fm.identificador, fm.referencia_documental, fm.data_documento,
       fa.identificador, fa.referencia_documental, fa.data_documento
FROM esg_definicoes d
JOIN esg_unidades u ON u.simbolo = d.unidade_simbolo
JOIN esg_perimetros p ON p.codigo = d.perimetro_codigo
JOIN esg_fontes_documentais fm ON fm.identificador = d.fonte_metodologica_id
LEFT JOIN esg_fontes_documentais fa ON fa.identificador = d.fonte_aprovacao_id
"""

REGISTRO_SELECT = """
SELECT r.identificador, r.indicador_codigo, r.ano, r.mes, r.valor,
       r.data_referencia, r.situacao, p.codigo, p.descricao,
       f.identificador, f.referencia_documental, f.data_documento,
       fa.identificador, fa.referencia_documental, fa.data_documento
FROM esg_registros r
JOIN esg_perimetros p ON p.codigo = r.perimetro_codigo
JOIN esg_fontes_documentais f ON f.identificador = r.fonte_id
LEFT JOIN esg_fontes_documentais fa ON fa.identificador = r.fonte_aprovacao_id
"""

EVIDENCIA_SELECT = """
SELECT e.identificador, e.registro_id, e.tipo, e.observacoes,
       f.identificador, f.referencia_documental, f.data_documento
FROM esg_evidencias e
JOIN esg_fontes_documentais f ON f.identificador = e.fonte_id
JOIN esg_registros r ON r.identificador = e.registro_id
"""


def _uuid(valor):
    if type(valor) is UUID:
        return valor
    exigir_tipo(valor, str, "uuid armazenado")
    return UUID(valor)


def _fonte(linha, inicio):
    if linha[inicio] is None:
        return None
    return FonteDocumental(*linha[inicio:inicio + 3])


def _definicao(linha):
    return DefinicaoIndicador(
        codigo=linha[0], nome=linha[1], dimensao=DimensaoESG(linha[2]),
        descricao=linha[3], metodologia=linha[4], situacao=SituacaoIndicador(linha[5]),
        unidade=UnidadeMedida(*linha[6:8]), perimetro=Perimetro(*linha[8:10]),
        fonte_metodologica=_fonte(linha, 10), fonte_aprovacao=_fonte(linha, 13),
    )


def _registro(linha):
    return RegistroPeriodo(
        identificador=_uuid(linha[0]), indicador_codigo=linha[1],
        periodo=Periodo(linha[2], linha[3]), valor=linha[4],
        data_referencia=linha[5], situacao=SituacaoRegistro(linha[6]),
        perimetro=Perimetro(*linha[7:9]), fonte=_fonte(linha, 9),
        fonte_aprovacao=_fonte(linha, 12),
    )


def _evidencia(linha):
    return Evidencia(_uuid(linha[0]), _uuid(linha[1]), linha[2], _fonte(linha, 4), linha[3])


def _autor(usuario_id):
    exigir_tipo(usuario_id, int, "usuario_id")
    if usuario_id <= 0:
        raise ContratoESGInvalido("usuario_id: informe autor autenticado válido.")


class RepositorioESG:
    def __init__(self, conectar_banco):
        if not callable(conectar_banco):
            raise ContratoESGInvalido("conectar_banco: informe fábrica de conexão dedicada.")
        self._conectar_banco = conectar_banco

    @contextmanager
    def _transacao(self, leitura=False):
        conexao = None
        dedicada = False
        commit_iniciado = False
        try:
            conexao = self._conectar_banco()
            if conexao.closed or conexao.autocommit is not False or conexao.get_transaction_status() != 0:
                raise PersistenciaESGError("Conexão deve estar aberta, ociosa e sem autocommit.")
            dedicada = True
            if leitura:
                conexao.set_session(readonly=True, isolation_level="REPEATABLE READ")
            with conexao.cursor() as cursor:
                yield cursor
            commit_iniciado = True
            conexao.commit()
        except Exception as erro:
            if dedicada:
                try:
                    conexao.rollback()
                except Exception:
                    raise PersistenciaESGError("Falha ao reverter a transação ESG; conexão descartada.") from None
            if commit_iniciado:
                raise ConfirmacaoIncertaESGError("Confirmação da operação ESG incerta; consultar antes de repetir o cadastro.") from None
            if isinstance(erro, (PersistenciaESGError, ContratoESGInvalido)):
                raise
            codigo = getattr(erro, "pgcode", None)
            if codigo == "23505":
                raise ConflitoESGError("Identificador ou período já cadastrado; revisão necessária.") from None
            if codigo in {"23503", "23502", "23514", "22003"}:
                raise IntegridadeESGError("Dados ou vínculos violam a integridade ESG.") from None
            raise PersistenciaESGError("Não foi possível concluir a operação ESG.") from None
        finally:
            if dedicada:
                try:
                    conexao.close()
                except Exception:
                    raise PersistenciaESGError("Falha ao encerrar a conexão ESG; não repetir cadastro automaticamente.") from None

    @staticmethod
    def _garantir_fonte(cursor, fonte, usuario_id):
        cursor.execute("""INSERT INTO esg_fontes_documentais
            (identificador, referencia_documental, data_documento, criado_por_usuario_id)
            VALUES (%s, %s, %s, %s) ON CONFLICT (identificador) DO NOTHING""",
            (fonte.identificador, fonte.referencia_documental, fonte.data_documento, usuario_id))
        cursor.execute("""SELECT identificador, referencia_documental, data_documento
            FROM esg_fontes_documentais WHERE identificador = %s FOR SHARE""", (fonte.identificador,))
        linha = cursor.fetchone()
        if linha is None or FonteDocumental(*linha) != fonte:
            raise ConflitoESGError("Identificador documental já associado a outra referência.")

    @staticmethod
    def _garantir_perimetro_unidade(cursor, definicao, usuario_id):
        perimetro, unidade = definicao.perimetro, definicao.unidade
        cursor.execute("""INSERT INTO esg_perimetros (codigo, descricao, criado_por_usuario_id)
            VALUES (%s, %s, %s) ON CONFLICT (codigo) DO NOTHING""",
            (perimetro.codigo, perimetro.descricao, usuario_id))
        cursor.execute("SELECT codigo, descricao FROM esg_perimetros WHERE codigo = %s FOR SHARE", (perimetro.codigo,))
        linha = cursor.fetchone()
        if linha is None or Perimetro(*linha) != perimetro:
            raise ConflitoESGError("Código de perímetro já associado a outra abrangência.")
        cursor.execute("""INSERT INTO esg_unidades (simbolo, descricao, criado_por_usuario_id)
            VALUES (%s, %s, %s) ON CONFLICT (simbolo) DO NOTHING""",
            (unidade.simbolo, unidade.descricao, usuario_id))
        cursor.execute("SELECT simbolo, descricao FROM esg_unidades WHERE simbolo = %s FOR SHARE", (unidade.simbolo,))
        linha = cursor.fetchone()
        if linha is None or UnidadeMedida(*linha) != unidade:
            raise ConflitoESGError("Símbolo de unidade já associado a outra descrição.")

    @staticmethod
    def _ler_definicao(cursor, codigo, bloquear=False):
        cursor.execute(DEFINICAO_SELECT + " WHERE d.codigo = %s" + (" FOR SHARE OF d" if bloquear else ""), (codigo,))
        linha = cursor.fetchone()
        return _definicao(linha) if linha is not None else None

    @staticmethod
    def _exigir_perimetro(definicao, perimetro):
        if definicao.perimetro != perimetro:
            raise IntegridadeESGError("Perímetro incompatível com a definição; consulta não pode misturar abrangências.")

    def salvar_fonte(self, fonte, usuario_id):
        exigir_tipo(fonte, FonteDocumental, "fonte")
        _autor(usuario_id)
        with self._transacao() as cursor:
            self._garantir_fonte(cursor, fonte, usuario_id)
        return fonte

    def salvar_definicao(self, definicao, usuario_id):
        exigir_tipo(definicao, DefinicaoIndicador, "definicao")
        _autor(usuario_id)
        if definicao.situacao is not SituacaoIndicador.PROPOSTO or definicao.fonte_aprovacao is not None:
            raise ContratoESGInvalido("Cadastro aceita somente definição proposta, sem aprovação declarada.")
        with self._transacao() as cursor:
            self._garantir_fonte(cursor, definicao.fonte_metodologica, usuario_id)
            self._garantir_perimetro_unidade(cursor, definicao, usuario_id)
            cursor.execute("""INSERT INTO esg_definicoes
                (codigo, nome, dimensao, descricao, unidade_simbolo, metodologia,
                 perimetro_codigo, fonte_metodologica_id, situacao, criado_por_usuario_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'proposto', %s)""",
                (definicao.codigo, definicao.nome, definicao.dimensao.value, definicao.descricao,
                 definicao.unidade.simbolo, definicao.metodologia, definicao.perimetro.codigo,
                 definicao.fonte_metodologica.identificador, usuario_id))
        return definicao

    @staticmethod
    def _inserir_evidencia(cursor, evidencia, usuario_id):
        RepositorioESG._garantir_fonte(cursor, evidencia.fonte, usuario_id)
        cursor.execute("""INSERT INTO esg_evidencias
            (identificador, registro_id, tipo, fonte_id, observacoes, criado_por_usuario_id)
            VALUES (%s::uuid, %s::uuid, %s, %s, %s, %s)""",
            (str(evidencia.identificador), str(evidencia.registro_id), evidencia.tipo,
             evidencia.fonte.identificador, evidencia.observacoes, usuario_id))

    def salvar_registro(self, registro, usuario_id, evidencias=()):
        exigir_tipo(registro, RegistroPeriodo, "registro")
        _autor(usuario_id)
        if registro.situacao is not SituacaoRegistro.RASCUNHO or registro.fonte_aprovacao is not None:
            raise ContratoESGInvalido("Cadastro aceita somente registro rascunho, sem aprovação declarada.")
        if type(evidencias) not in (tuple, list):
            raise ContratoESGInvalido("evidencias: informe lista ou tupla em memória.")
        with self._transacao() as cursor:
            definicao = self._ler_definicao(cursor, registro.indicador_codigo, bloquear=True)
            if definicao is None:
                raise NaoEncontradoESGError("Definição não encontrada.")
            organizar_registros(definicao, (registro,), evidencias)
            self._garantir_fonte(cursor, registro.fonte, usuario_id)
            cursor.execute("""INSERT INTO esg_registros
                (identificador, indicador_codigo, ano, mes, valor, fonte_id,
                 data_referencia, perimetro_codigo, situacao, criado_por_usuario_id)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, %s, %s, 'rascunho', %s)""",
                (str(registro.identificador), registro.indicador_codigo, registro.periodo.ano,
                 registro.periodo.mes, registro.valor, registro.fonte.identificador,
                 registro.data_referencia, registro.perimetro.codigo, usuario_id))
            for evidencia in evidencias:
                self._inserir_evidencia(cursor, evidencia, usuario_id)
        return registro

    def associar_evidencia(self, evidencia, usuario_id):
        exigir_tipo(evidencia, Evidencia, "evidencia")
        _autor(usuario_id)
        with self._transacao() as cursor:
            cursor.execute(REGISTRO_SELECT + " WHERE r.identificador = %s::uuid FOR SHARE OF r", (str(evidencia.registro_id),))
            linha = cursor.fetchone()
            if linha is None:
                raise NaoEncontradoESGError("Registro da evidência não encontrado.")
            registro = _registro(linha)
            if registro.situacao is not SituacaoRegistro.RASCUNHO or registro.fonte_aprovacao is not None:
                raise IntegridadeESGError("Evidência só pode ser associada a rascunho; demais estados exigem fluxo auditado.")
            definicao = self._ler_definicao(cursor, registro.indicador_codigo, bloquear=True)
            if definicao is None:
                raise IntegridadeESGError("Definição do registro ausente.")
            organizar_registros(definicao, (registro,), (evidencia,))
            self._inserir_evidencia(cursor, evidencia, usuario_id)
        return evidencia

    def obter_definicao(self, codigo):
        validar_codigo(codigo, "codigo")
        with self._transacao(leitura=True) as cursor:
            return self._ler_definicao(cursor, codigo)

    def listar_definicoes(self):
        """Lista contratos e seus estados, sem agregar perímetros ou valores."""
        with self._transacao(leitura=True) as cursor:
            cursor.execute(DEFINICAO_SELECT + " ORDER BY d.dimensao, d.nome, d.codigo", ())
            return tuple(_definicao(linha) for linha in cursor.fetchall())

    def obter_registro(self, codigo, periodo, perimetro):
        validar_codigo(codigo, "codigo")
        exigir_tipo(periodo, Periodo, "periodo")
        exigir_tipo(perimetro, Perimetro, "perimetro")
        with self._transacao(leitura=True) as cursor:
            definicao = self._ler_definicao(cursor, codigo)
            if definicao is None:
                return None
            self._exigir_perimetro(definicao, perimetro)
            cursor.execute(REGISTRO_SELECT + """ WHERE r.indicador_codigo = %s
                AND r.ano = %s AND r.mes IS NOT DISTINCT FROM %s
                AND r.perimetro_codigo = %s""", (codigo, periodo.ano, periodo.mes, perimetro.codigo))
            linha = cursor.fetchone()
            if linha is None:
                return None
            registro = _registro(linha)
            organizar_registros(definicao, (registro,))
            return registro

    def listar_registros(self, codigo, perimetro):
        validar_codigo(codigo, "codigo")
        exigir_tipo(perimetro, Perimetro, "perimetro")
        with self._transacao(leitura=True) as cursor:
            definicao = self._ler_definicao(cursor, codigo)
            if definicao is None:
                return None
            self._exigir_perimetro(definicao, perimetro)
            cursor.execute(REGISTRO_SELECT + """ WHERE r.indicador_codigo = %s AND r.perimetro_codigo = %s
                ORDER BY r.ano, r.mes NULLS FIRST""", (codigo, perimetro.codigo))
            registros = tuple(_registro(linha) for linha in cursor.fetchall())
            cursor.execute(EVIDENCIA_SELECT + """ WHERE r.indicador_codigo = %s AND r.perimetro_codigo = %s
                ORDER BY e.identificador""", (codigo, perimetro.codigo))
            evidencias = tuple(_evidencia(linha) for linha in cursor.fetchall())
            return organizar_registros(definicao, registros, evidencias)
