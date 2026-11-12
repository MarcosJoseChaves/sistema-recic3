"""ESG-04B: carga inventada exclusiva do pitch FGV; RF-1A: NO-GO OPERACIONAL.

Carga alternativa às fixtures existentes; nunca combinar seus repositórios.
Nenhum dado corresponde a resultados municipais. Importar não cria a carga.
"""

from datetime import date
from decimal import Decimal
from uuid import uuid4

from .contratos import (
    DefinicaoIndicador, DimensaoESG, Evidencia, FonteDocumental, Perimetro,
    Periodo, RegistroPeriodo, UnidadeMedida,
)
from .demonstracao import AVISO_DEMONSTRACAO, RepositorioDemonstrativoESG


def criar_repositorio_pitch_fgv():
    """Cria uma instância nova, somente em memória, sem aprovação ou agregação.

    Os códigos exec-* atendem à ordenação visual existente. Sua reutilização
    não autoriza mesclar esta carga às anteriores. Autor 1 é inteiramente fictício.
    Outubro de 2026 é competência inventada, não resultado municipal observado.
    """
    repo = RepositorioDemonstrativoESG()
    aviso = AVISO_DEMONSTRACAO + ": exemplo fictício, sem correspondência com resultados municipais."
    exemplos = (
        ("recuperacao", "Recuperação de materiais", DimensaoESG.AMBIENTAL,
         "t", "Toneladas no exemplo fictício", Decimal("12"),
         "Massa declarada como encaminhada à recuperação no recorte fictício."),
        ("destinacao", "Destinação de rejeitos", DimensaoESG.AMBIENTAL,
         "t", "Toneladas no exemplo fictício", Decimal("0"),
         "Massa declarada como destinada no recorte fictício; zero é valor explicitamente registrado."),
        ("compostagem", "Compostagem de orgânicos", DimensaoESG.AMBIENTAL,
         "t", "Toneladas no exemplo fictício", Decimal("3"),
         "Massa de orgânicos recebida para compostagem; não representa massa de composto produzido."),
        ("associacoes", "Participação das associações", DimensaoESG.SOCIAL,
         "associações", "Associações distintas no exemplo fictício", Decimal("2"),
         "Quantidade declarada de associações distintas com participação registrada no recorte fictício."),
        ("inclusao", "Inclusão produtiva", DimensaoESG.SOCIAL,
         "pessoas", "Pessoas distintas no exemplo fictício", None,
         "Pessoas distintas com participação produtiva documentada; registro com valor indisponível."),
        ("capacitacoes", "Capacitações", DimensaoESG.SOCIAL,
         "eventos", "Eventos de capacitação no exemplo fictício", None,
         "Quantidade de eventos realizados; sem registro, medição ou período presumido neste exemplo."),
        ("rastreabilidade", "Rastreabilidade documental", DimensaoESG.GOVERNANCA,
         "documentos", "Documentos distintos no exemplo fictício", Decimal("4"),
         "Quantidade declarada de quatro documentos sintéticos vinculados ao registro; "
         "não representa percentual de conformidade ou aprovação institucional."),
    )
    for tema, nome, dimensao, simbolo, unidade, valor, metodologia in exemplos:
        fonte = FonteDocumental(
            "pitch-fonte-" + tema,
            aviso + " Referência sintética de " + nome.lower() + "; sem emissor institucional.",
            date(2026, 10, 31),
        )
        definicao = DefinicaoIndicador(
            codigo="exec-" + tema,
            nome=nome + " — exemplo fictício",
            dimensao=dimensao,
            descricao=aviso + " RF-1A: NO-GO OPERACIONAL.",
            unidade=UnidadeMedida(simbolo, unidade),
            metodologia=aviso + " " + metodologia + " Sem cálculo, percentual ou agregação.",
            perimetro=Perimetro(
                "pitch-recorte-" + tema,
                aviso + " Universo exclusivo de " + nome.lower()
                + "; exclui os demais recortes. Sem soma ou comparação entre perímetros.",
            ),
            fonte_metodologica=fonte,
        )
        repo.salvar_definicao(definicao, usuario_id=1)
        if tema == "capacitacoes":
            continue
        registro = RegistroPeriodo(
            identificador=uuid4(),
            indicador_codigo=definicao.codigo,
            periodo=Periodo(2026, 10),
            fonte=fonte,
            data_referencia=date(2026, 10, 31),
            perimetro=definicao.perimetro,
            valor=valor,
        )
        if tema == "rastreabilidade":
            # Quatro documentos distintos; o valor declarado não é calculado.
            evidencias = tuple(
                Evidencia(
                    uuid4(), registro.identificador, "Documento sintético — exemplo fictício",
                    FonteDocumental(
                        "pitch-documento-governanca-" + str(numero),
                        aviso + " Documento sintético distinto " + str(numero)
                        + "; sem emissor ou aprovação institucional.",
                        date(2026, 10, 31),
                    ),
                    aviso + " Vínculo documental demonstrativo; não comprova validação institucional.",
                )
                for numero in (1, 2, 3, 4)
            )
        else:
            evidencias = (
                Evidencia(
                    uuid4(), registro.identificador, "Referência sintética — exemplo fictício",
                    fonte, aviso + " Evidência exclusivamente demonstrativa.",
                ),
            )
        repo.salvar_registro(registro, usuario_id=1, evidencias=evidencias)
    return repo
