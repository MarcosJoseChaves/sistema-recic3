"""Composição nativa 08B.1: opt-in e indisponível, sem persistência."""

def configurar_maz_nativo(app):
    """Registra somente a superfície protegida; não aceita adaptadores/fábricas.

    Não habilita persistência por configuração. A ativação futura exige revisão
    explícita deste componente e dos gates de schema e autorização.
    """
    habilitado = app.config.get("ESG_MAZ_NATIVE_ENABLED", False)
    if type(habilitado) is not bool:
        raise RuntimeError("ESG_MAZ_NATIVE_ENABLED deve ser booleano validado.")
    if not habilitado:
        return False

    from flask import jsonify
    from modulos.fiscalizacao_contratos.permissions import admin_required
    from .maz_web import criar_blueprint_maz

    blueprint = criar_blueprint_maz(trcs=None, obrigacoes=None, metodologia=None)

    @blueprint.before_request
    @admin_required
    def indisponivel_nativo():
        # Barreira incondicional anterior às views, inclusive POST e assets.
        # Não consulta configuração de adaptadores, fixtures ou MAZ_DEMO_*.
        return jsonify(error="ESG-MAZ nativo indisponível nesta unidade; persistência não habilitada."), 503

    app.register_blueprint(blueprint, url_prefix="/esg/maz")
    return True
