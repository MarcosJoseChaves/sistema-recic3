"""Consultas metodológicas em memória; nenhuma conexão ou persistência."""
import unicodedata
from flask import render_template, request, abort
from .maz.catalogo import criar_catalogo_maz

# Organização consultiva dos temas conceituais de maz/METODOLOGIA.md.
# Não define métodos de cálculo, prioridades ou pontuações.
TEMAS = (
    ("Resíduos e capacidade do aterro", "Pressões ambientais e territoriais", "Custos e continuidade da destinação", ("K01", "K02", "K11")),
    ("Inclusão socioprodutiva e segurança dos catadores", "Renda, trabalho, SST e participação", "Proteção e continuidade institucional", ("K09a", "K09b", "K09c", "K09d")),
    ("Compostagem e regularização", "Tratamento dos orgânicos e aproveitamento comprovado", "Viabilidade e condições documentais", ("K03", "K11")),
    ("Governança dos dados", "Transparência e qualidade da informação", "Rastreabilidade e riscos de decisão", ("K05", "K06", "K07")),
    ("Viabilidade econômica e recuperação complementar", "Alternativas conforme a hierarquia de resíduos", "Margem, financiamento e aceite", ("K04b", "K04c", "K08", "K10")),
)

def pagina_inicial():
    return render_template("esg/nativo/inicio.html")

def catalogo():
    q=request.args.get("q", "").strip()
    dimensao=request.args.get("dimensao", "")
    motor=request.args.get("motor", "")
    if dimensao not in ("", "ambiental", "social", "governanca") or motor not in ("", "implementado", "pendente"):
        abort(400)
    def normalizar(value):
        return "".join(c for c in unicodedata.normalize("NFD", value.casefold()) if not unicodedata.combining(c))
    catalogo=criar_catalogo_maz()
    definicoes=tuple(d for d in catalogo.definicoes if (not q or normalizar(q) in normalizar(d.codigo+" "+d.nome+" "+d.definicao)) and (not dimensao or d.dimensao.value==dimensao) and (not motor or (d.codigo=="K01")== (motor=="implementado")))
    return render_template("esg/nativo/catalogo.html", definicoes=definicoes, q=q, dimensao=dimensao, motor=motor, versao=catalogo.versao)

def materialidade():
    return render_template("esg/nativo/materialidade.html", temas=TEMAS)
