"""Organização em memória dos contratos ESG, sem agregação ou persistência."""

from dataclasses import dataclass

from .contratos import (
    DefinicaoIndicador,
    Evidencia,
    FonteDocumental,
    RegistroPeriodo,
    SituacaoIndicador,
    SituacaoRegistro,
)
from .validacoes import ContratoESGInvalido, exigir_tipo


@dataclass(frozen=True)
class SerieIndicador:
    """Uma definição e seus registros/evidências com vínculos consistentes."""

    definicao: DefinicaoIndicador
    registros: tuple[RegistroPeriodo, ...] = ()
    evidencias: tuple[Evidencia, ...] = ()

    def __post_init__(self):
        exigir_tipo(self.definicao, DefinicaoIndicador, "definicao")
        exigir_tipo(self.registros, tuple, "registros")
        exigir_tipo(self.evidencias, tuple, "evidencias")
        registros_por_id = {}
        periodos = set()
        fontes = {}

        def rastrear(fonte):
            if fonte is None:
                return
            exigir_tipo(fonte, FonteDocumental, "fonte")
            anterior = fontes.setdefault(fonte.identificador, fonte)
            if anterior != fonte:
                raise ContratoESGInvalido("fonte: o mesmo identificador não pode designar referências distintas.")

        rastrear(self.definicao.fonte_metodologica)
        rastrear(self.definicao.fonte_aprovacao)
        for registro in self.registros:
            exigir_tipo(registro, RegistroPeriodo, "registro")
            if registro.indicador_codigo != self.definicao.codigo:
                raise ContratoESGInvalido("indicador_codigo: registro vinculado a outra definição.")
            if registro.perimetro != self.definicao.perimetro:
                raise ContratoESGInvalido("perimetro: registros de abrangências distintas exigem séries separadas.")
            if registro.identificador in registros_por_id or registro.periodo in periodos:
                raise ContratoESGInvalido("registro: identificador ou período duplicado na série.")
            if registro.situacao is SituacaoRegistro.APROVADO and self.definicao.situacao is not SituacaoIndicador.APROVADO:
                raise ContratoESGInvalido("situacao: registro aprovado exige definição aprovada.")
            registros_por_id[registro.identificador] = registro
            periodos.add(registro.periodo)
            rastrear(registro.fonte)
            rastrear(registro.fonte_aprovacao)

        evidencias_ids = set()
        for evidencia in self.evidencias:
            exigir_tipo(evidencia, Evidencia, "evidencia")
            if evidencia.registro_id not in registros_por_id:
                raise ContratoESGInvalido("evidencia.registro_id: registro não pertence à série.")
            if evidencia.identificador in evidencias_ids:
                raise ContratoESGInvalido("evidencia.identificador: duplicado na série.")
            evidencias_ids.add(evidencia.identificador)
            rastrear(evidencia.fonte)


def organizar_registros(definicao, registros=(), evidencias=()):
    """Valida vínculos e ordena por período; preserva valores, fontes e estados.

    Aceita somente listas/tuplas em memória. Não consome iteradores que possam
    consultar banco ou rede. Ano completo e meses continuam registros separados.
    """
    for campo, itens in (("registros", registros), ("evidencias", evidencias)):
        if type(itens) not in (list, tuple):
            raise ContratoESGInvalido(f"{campo}: informe lista ou tupla em memória.")
    serie = SerieIndicador(definicao, tuple(registros), tuple(evidencias))
    return SerieIndicador(
        serie.definicao,
        tuple(sorted(serie.registros, key=lambda r: (r.periodo.ano, r.periodo.mes or 0))),
        tuple(sorted(serie.evidencias, key=lambda e: str(e.identificador))),
    )
