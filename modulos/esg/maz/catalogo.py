"""Catálogo proposto do recorte autorizado ESG-MAZ-02; sem medições históricas."""
from dataclasses import dataclass
from ..contratos import DimensaoESG, SituacaoIndicador
from ..validacoes import ContratoESGInvalido, exigir_tipo, validar_codigo, validar_texto

VERSAO = "maz02-recorte-prompt-v1"
ORIGEM = "Recorte autorizado ESG-MAZ-02; conferência acadêmica v46/v47 pendente."
CODIGOS = ("K01", "K02", "K03", "K04a", "K04b", "K04c", "K05", "K06",
           "K07", "K08", "K09a", "K09b", "K09c", "K09d", "K10", "K11")

@dataclass(frozen=True)
class DefinicaoMAZ:
    codigo: str
    nome: str
    dimensao: DimensaoESG
    definicao: str
    unidade: str | None
    metodologia: str
    versao_metodologica: str
    fontes_verificacao: tuple[str, ...]
    qualidade_minima: tuple[str, ...]
    limitacoes: tuple[str, ...]
    impedimentos: tuple[str, ...]
    vinculos_conceituais: tuple[str, ...] = ()
    numerador: str | None = None
    denominador: str | None = None
    perimetro: str | None = None
    periodicidade_proposta: str | None = None
    situacao: SituacaoIndicador = SituacaoIndicador.PROPOSTO

    def __post_init__(self):
        validar_codigo(self.codigo, "codigo")
        validar_codigo(self.versao_metodologica, "versao")
        exigir_tipo(self.dimensao, DimensaoESG, "dimensao")
        exigir_tipo(self.situacao, SituacaoIndicador, "situacao")
        if self.situacao is not SituacaoIndicador.PROPOSTO:
            raise ContratoESGInvalido("Catálogo deste recorte aceita apenas proposta.")
        for campo in ("nome", "definicao", "metodologia"):
            validar_texto(getattr(self, campo), campo)
        for campo in ("unidade", "numerador", "denominador", "perimetro", "periodicidade_proposta"):
            if getattr(self, campo) is not None:
                validar_texto(getattr(self, campo), campo)
        for campo in ("fontes_verificacao", "qualidade_minima", "limitacoes", "impedimentos", "vinculos_conceituais"):
            itens = getattr(self, campo)
            exigir_tipo(itens, tuple, campo)
            if campo != "vinculos_conceituais" and not itens:
                raise ContratoESGInvalido(f"{campo}: declaração obrigatória.")
            for item in itens:
                validar_texto(item, campo)

@dataclass(frozen=True)
class CatalogoMAZ:
    versao: str
    origem_especificacao: str
    definicoes: tuple[DefinicaoMAZ, ...]

    def __post_init__(self):
        validar_codigo(self.versao, "versao")
        validar_texto(self.origem_especificacao, "origem")
        exigir_tipo(self.definicoes, tuple, "definicoes")
        for d in self.definicoes:
            exigir_tipo(d, DefinicaoMAZ, "definicao")
            if d.versao_metodologica != self.versao:
                raise ContratoESGInvalido("Versões incompatíveis.")
        if tuple(d.codigo for d in self.definicoes) != CODIGOS:
            raise ContratoESGInvalido("Códigos ausentes, duplicados ou em ordem divergente.")

def criar_catalogo_maz():
    """Construção explícita de metadados, sem seeds, fontes fictícias ou medições."""
    E, S, G = DimensaoESG.AMBIENTAL, DimensaoESG.SOCIAL, DimensaoESG.GOVERNANCA
    linhas = (
        ("K01", "TRCS", E, "%", "Rejeito efetivamente pesado e expedido da UVR em relação à entrada comparável; requer reconciliação de estoques.", "Pesagens de entrada, saída e reconciliação física", "A1", "Massa de rejeitos expedidos", "Massa de entrada comparável"),
        ("K02", "Destinações e redução de aterramento", E, None, "Massa aterrada por origem e massas recuperadas ou tratadas; fronteiras compatíveis, sem dupla contagem.", "Pesagens por origem, destinação e aceite", "K01", None, None),
        ("K03", "Orgânicos e IDO", E, None, "Distinguir orgânicos, estruturantes, rejeitos, perdas, estoques, composto produzido e aproveitamento comprovado. Fórmula de IDO pendente.", "Balanço por fluxo e comprovantes de aproveitamento", "K02", None, None),
        ("K04a", "Triagem", E, None, "Massa triada por categoria reconciliável com recebimentos, estoques e rejeitos.", "Pesagens por categoria e estoques", "K01", None, None),
        ("K04b", "Expedição e aceite", E, None, "Distinguir expedição, faturamento, aceite, recusa, devolução e transformação industrial comprovada.", "Registros de expedição e aceite do receptor", "K04a", None, None),
        ("K04c", "Margem operacional por kg aceito", G, None, "Receitas e custos atribuíveis, glosas não deduzidas e competência comparável; regra de glosas e unidade monetária pendentes.", "Receitas, custos atribuíveis, glosas e kg aceitos", "K04b", "Margem operacional atribuível", "kg aceitos"),
        ("K05", "Completude", G, "%", "Registros obrigatórios válidos em relação aos registros exigíveis.", "Inventário de exigíveis e validações", "G2", "Registros obrigatórios válidos", "Registros exigíveis"),
        ("K06", "Tempestividade", G, "%", "Registros lançados e validados dentro do prazo em relação aos exigíveis.", "Datas de lançamento, validação e prazo documentado", "K05", "Registros lançados e validados no prazo", "Registros exigíveis"),
        ("K07", "Divergência física", G, None, "Divergências de reconciliação física; tolerância e método devem ser documentados.", "Reconciliação e tolerância documentadas", "K01", None, None),
        ("K08", "Custos e financiamento", G, None, "Custos por pagador, remuneração municipal preexistente, receitas comerciais e incentivos de logística reversa sem duplicar transferências.", "Registros financeiros por pagador e natureza", "G4", None, None),
        *tuple((c, "Inclusão social — subcódigo a confirmar", S, None, "Grupo K09: renda líquida distribuída, horas, distribuição, EPI/SST, participação e inclusão; correspondência individual não confirmada.", "Registros agregados de renda, trabalho, SST e participação; acesso protegido", "S1", None, None) for c in ("K09a", "K09b", "K09c", "K09d")),
        ("K10", "CDR", E, None, "Alternativa residual condicionada à hierarquia, caracterização técnica, viabilidade econômica, conformidade e aceite do receptor.", "Caracterização, viabilidade, conformidade e aceite", "K02", None, None),
        ("K11", "Gravimetria", E, None, "Composição por fluxo, método amostral, período e incerteza.", "Plano amostral, massas e incerteza", "K03", None, None),
    )
    return CatalogoMAZ(VERSAO, ORIGEM, tuple(
        DefinicaoMAZ(c, nome, dimensao, regra, unidade, regra, VERSAO,
            (fontes,), ("Período, unidade, perímetro e fontes comparáveis e verificáveis.",),
            ("Conferência v46/v47 pendente; classificação ASG proposta, não institucional.",),
            ("Sem fronteiras/fontes comparáveis ou regra específica confirmada, não calcular.",),
            (vinculo,), num, den)
        for c, nome, dimensao, unidade, regra, fontes, vinculo, num, den in linhas))
