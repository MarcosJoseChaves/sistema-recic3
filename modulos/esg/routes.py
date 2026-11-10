"""Cadastro e consulta administrativa ESG com persistência explicitamente injetada."""

from functools import wraps

from flask import abort, flash, redirect, render_template, request, url_for
from flask_login import current_user

from modulos.fiscalizacao_contratos.permissions import admin_required
from .contratos import DimensaoESG, SituacaoRegistro
from .formularios import definicao_formulario, filtros_formulario, registro_formulario
from .repositorio import (
    ConfirmacaoIncertaESGError, ConflitoESGError, NaoEncontradoESGError, PersistenciaESGError,
)
from .validacoes import ContratoESGInvalido, validar_codigo


ROTULOS = {
    "proposto": "Proposto — sem aprovação institucional",
    "rascunho": "Rascunho — sem aprovação institucional",
    "em_analise": "Em análise", "aprovado": "Aprovado", "rejeitado": "Rejeitado",
}


def registrar_rotas(blueprint, repositorio=None):
    """Não constrói repositório nem infere conexão a partir de app/configuração."""

    @blueprint.context_processor
    def contexto_esg():
        return {"rotulos_esg": ROTULOS, "dimensoes_esg": DimensaoESG,
                "situacoes_registro": SituacaoRegistro,
                "persistencia_configurada": repositorio is not None}

    def erro(mensagem, status):
        return render_template("esg/erro.html", mensagem=mensagem), status

    def persistencia_exigida(view):
        @wraps(view)
        def protegida(*args, **kwargs):
            if repositorio is None:
                return erro("Persistência ESG não configurada. Cadastro e consulta estão indisponíveis; nenhum dado foi consultado ou gravado.", 503)
            try:
                return view(*args, **kwargs)
            except ConfirmacaoIncertaESGError:
                return erro("A confirmação do cadastro é incerta. Consulte os dados antes de reenviar; não repita a operação automaticamente.", 503)
            except NaoEncontradoESGError:
                return erro("Indicador ou registro não encontrado.", 404)
            except ConflitoESGError:
                return erro("Identificador, fonte ou período já cadastrado com dados incompatíveis. Nenhum dado existente foi sobrescrito.", 409)
            except PersistenciaESGError:
                return erro("Não foi possível concluir a operação de persistência ESG. Consulte os cadastros antes de reenviar.", 503)
            except ContratoESGInvalido:
                return erro("Os dados consultados são incompatíveis com os contratos ESG. A operação foi interrompida.", 503)
        return protegida

    def obter_definicao(codigo):
        try:
            validar_codigo(codigo, "codigo")
        except ContratoESGInvalido:
            abort(404)
        definicao = repositorio.obter_definicao(codigo)
        if definicao is None:
            abort(404)
        return definicao

    def autor():
        usuario_id = getattr(current_user, "id", None)
        if type(usuario_id) is not int or usuario_id <= 0:
            abort(403)
        return usuario_id

    @blueprint.route("", methods=["GET"], strict_slashes=False)
    @admin_required
    def painel():
        estado = "nao_configurado"
        status = 200
        if repositorio is not None:
            try:
                indicadores = repositorio.listar_definicoes()
            except (PersistenciaESGError, ContratoESGInvalido):
                estado = "erro"
                status = 503
            else:
                estado = "com_dados" if indicadores else "vazio"
        return render_template("esg/painel.html", estado_painel=estado), status

    @blueprint.get("/indicadores")
    @admin_required
    @persistencia_exigida
    def indicadores_lista():
        return render_template("esg/indicadores/lista.html", indicadores=repositorio.listar_definicoes())

    @blueprint.route("/indicadores/novo", methods=["GET", "POST"])
    @admin_required
    @persistencia_exigida
    def indicadores_novo():
        if request.method == "POST":
            usuario_id = autor()
            try:
                definicao = definicao_formulario(request.form)
            except ContratoESGInvalido as falha:
                return render_template("esg/indicadores/form.html", dados=request.form, erro=str(falha)), 400
            repositorio.salvar_definicao(definicao, usuario_id)
            flash("Indicador cadastrado como proposto, sem aprovação institucional.", "success")
            return redirect(url_for("esg.indicadores_detalhe", codigo=definicao.codigo), code=303)
        return render_template("esg/indicadores/form.html", dados={})

    @blueprint.get("/indicadores/<codigo>/detalhe")
    @admin_required
    @persistencia_exigida
    def indicadores_detalhe(codigo):
        definicao = obter_definicao(codigo)
        try:
            periodo, situacao = filtros_formulario(request.args)
        except ContratoESGInvalido as falha:
            return erro(str(falha), 400)
        serie = repositorio.listar_registros(codigo, definicao.perimetro)
        if serie is None:
            abort(404)
        if serie.definicao != definicao:
            raise ContratoESGInvalido("Definição mudou durante a consulta.")
        registros = tuple(r for r in serie.registros
                          if (periodo is None or r.periodo == periodo)
                          and (situacao is None or r.situacao is situacao))
        return render_template("esg/indicadores/detalhe.html", definicao=definicao,
                               registros=registros, evidencias=serie.evidencias, filtros=request.args)

    @blueprint.route("/indicadores/<codigo>/registros/novo", methods=["GET", "POST"])
    @admin_required
    @persistencia_exigida
    def registros_novo(codigo):
        definicao = obter_definicao(codigo)
        if request.method == "POST":
            usuario_id = autor()
            try:
                registro, evidencias = registro_formulario(request.form, definicao)
            except ContratoESGInvalido as falha:
                return render_template("esg/registros/form.html", definicao=definicao, dados=request.form, erro=str(falha)), 400
            repositorio.salvar_registro(registro, usuario_id, evidencias=evidencias)
            flash("Registro salvo como rascunho, sem aprovação institucional.", "success")
            return redirect(url_for("esg.indicadores_detalhe", codigo=codigo, competencia=registro.periodo.competencia), code=303)
        return render_template("esg/registros/form.html", definicao=definicao, dados={})
