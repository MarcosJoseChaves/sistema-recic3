"""ESG-03A: repositório sintético em memória; nenhuma ativação na aplicação real."""

from datetime import date
from decimal import Decimal
from types import MappingProxyType
from uuid import uuid4

from .contratos import (
    DefinicaoIndicador, DimensaoESG, Evidencia, FonteDocumental, Perimetro,
    RegistroPeriodo, Periodo, SituacaoIndicador, SituacaoRegistro, UnidadeMedida,
)
from .repositorio import ConflitoESGError, IntegridadeESGError, NaoEncontradoESGError
from .services import organizar_registros
from .validacoes import ContratoESGInvalido, exigir_tipo, validar_codigo


AVISO_DEMONSTRACAO = "DADOS FICTICIOS — NAO OFICIAIS"


class RepositorioDemonstrativoESG:
    """Instância vazia e independente. Objetos imutáveis; sem I/O ou aprovação.

    Uso sequencial no test_client; não simula concorrência, SQL ou durabilidade.
    Toda validação precede a alteração dos mapas, inclusive fontes/evidências.
    """

    def __init__(self):
        self._definicoes = {}
        self._registros = {}
        self._evidencias = {}
        self._fontes = {}
        self._perimetros = {}
        self._unidades = {}
        self._autorias = {}

    @property
    def autorias(self):
        """Snapshot somente leitura da autoria sintética fornecida pelo servidor."""
        return MappingProxyType(dict(self._autorias))

    @staticmethod
    def _autor(usuario_id):
        exigir_tipo(usuario_id, int, "usuario_id")
        if usuario_id <= 0:
            raise ContratoESGInvalido("usuario_id: informe autor autenticado válido.")

    @staticmethod
    def _identidades(atuais, objetos, atributo):
        novas = dict(atuais)
        for objeto in objetos:
            chave = getattr(objeto, atributo)
            if chave in novas and novas[chave] != objeto:
                raise ConflitoESGError("Identidade demonstrativa já utilizada com dados diferentes.")
            novas[chave] = objeto
        return novas

    def listar_definicoes(self):
        return tuple(sorted(self._definicoes.values(), key=lambda d: (d.dimensao.value, d.nome, d.codigo)))

    def obter_definicao(self, codigo):
        validar_codigo(codigo, "codigo")
        return self._definicoes.get(codigo)

    def salvar_definicao(self, definicao, usuario_id):
        exigir_tipo(definicao, DefinicaoIndicador, "definicao")
        self._autor(usuario_id)
        if definicao.situacao is not SituacaoIndicador.PROPOSTO or definicao.fonte_aprovacao is not None:
            raise ContratoESGInvalido("Demonstração aceita somente proposta sem aprovação.")
        if definicao.codigo in self._definicoes:
            raise ConflitoESGError("Código demonstrativo já cadastrado.")
        fontes = self._identidades(self._fontes, (definicao.fonte_metodologica,), "identificador")
        perimetros = self._identidades(self._perimetros, (definicao.perimetro,), "codigo")
        unidades = self._identidades(self._unidades, (definicao.unidade,), "simbolo")
        self._fontes, self._perimetros, self._unidades = fontes, perimetros, unidades
        self._definicoes[definicao.codigo] = definicao
        self._autorias[("definicao", definicao.codigo)] = usuario_id
        return definicao

    def listar_registros(self, codigo, perimetro):
        validar_codigo(codigo, "codigo")
        exigir_tipo(perimetro, Perimetro, "perimetro")
        definicao = self.obter_definicao(codigo)
        if definicao is None:
            return None
        if perimetro != definicao.perimetro:
            raise IntegridadeESGError("Perímetro demonstrativo incompatível com a definição.")
        registros = tuple(r for r in self._registros.values() if r.indicador_codigo == codigo)
        ids = {r.identificador for r in registros}
        evidencias = tuple(e for e in self._evidencias.values() if e.registro_id in ids)
        return organizar_registros(definicao, registros, evidencias)

    def salvar_registro(self, registro, usuario_id, evidencias=()):
        exigir_tipo(registro, RegistroPeriodo, "registro")
        self._autor(usuario_id)
        if registro.situacao is not SituacaoRegistro.RASCUNHO or registro.fonte_aprovacao is not None:
            raise ContratoESGInvalido("Demonstração aceita somente rascunho sem aprovação.")
        if type(evidencias) not in (list, tuple):
            raise ContratoESGInvalido("evidencias: informe lista ou tupla em memória.")
        definicao = self.obter_definicao(registro.indicador_codigo)
        if definicao is None:
            raise NaoEncontradoESGError("Definição demonstrativa não encontrada.")
        if registro.identificador in self._registros or any(
            r.indicador_codigo == registro.indicador_codigo and r.periodo == registro.periodo
            for r in self._registros.values()
        ):
            raise ConflitoESGError("Identificador ou período demonstrativo já cadastrado.")
        organizar_registros(definicao, (registro,), evidencias)
        if any(e.identificador in self._evidencias for e in evidencias):
            raise ConflitoESGError("Identificador de evidência demonstrativa já cadastrado.")
        fontes = self._identidades(self._fontes, (registro.fonte,) + tuple(e.fonte for e in evidencias), "identificador")
        self._fontes = fontes
        self._registros[registro.identificador] = registro
        self._evidencias.update((e.identificador, e) for e in evidencias)
        self._autorias[("registro", registro.identificador)] = usuario_id
        self._autorias.update((("evidencia", e.identificador), usuario_id) for e in evidencias)
        return registro


def criar_repositorio_com_exemplos():
    """Carga explícita e descartável, exclusivamente sintética; não é catálogo oficial."""
    repo = RepositorioDemonstrativoESG()
    fonte = FonteDocumental("demo-fonte", AVISO_DEMONSTRACAO + ": documento sintético sem emissor institucional.")
    definicao = DefinicaoIndicador(
        "demo-material", AVISO_DEMONSTRACAO + ": material de exemplo", DimensaoESG.AMBIENTAL,
        "Exemplo sintético para demonstrar campos; sem correspondência com dados municipais.",
        UnidadeMedida("ud", "Unidades demonstrativas fictícias"),
        "Metodologia fictícia para navegação; não homologada.",
        Perimetro("demo-recorte", "Abrangência inteiramente fictícia"), fonte,
    )
    repo.salvar_definicao(definicao, 1)
    for periodo, valor, referencia in (
        (Periodo(2030), Decimal("0"), date(2030, 12, 31)),
        (Periodo(2030, 1), None, date(2030, 1, 31)),
        (Periodo(2030, 2), Decimal("12.3400"), date(2030, 2, 28)),
    ):
        registro = RegistroPeriodo(uuid4(), definicao.codigo, periodo, fonte, referencia, definicao.perimetro, valor)
        evidencia = Evidencia(uuid4(), registro.identificador, "Referência sintética", fonte, AVISO_DEMONSTRACAO)
        repo.salvar_registro(registro, 1, evidencias=(evidencia,))
    return repo
