"""Interface MAZ explícita; não descobre conexão nem altera aplicação principal."""
from functools import wraps
from decimal import Decimal,InvalidOperation
from uuid import UUID,uuid4
from dataclasses import replace
from datetime import date

def criar_blueprint_maz(trcs=None,obrigacoes=None,metodologia='demo05a-v1'):
    from flask import Blueprint,render_template,request,redirect,url_for,abort,current_app
    from flask_login import current_user
    from modulos.fiscalizacao_contratos.permissions import admin_required
    from .maz.persistencia import PersistenciaMAZError,ConflitoMAZ,ConfirmacaoIncertaMAZ,EvidenciaMAZ
    from .maz.obrigacoes import Obrigacao,DocumentoObrigacao
    from .maz.modelos import ComponenteMassa,UnidadeMassa,OrigemValor
    from .maz.catalogo import criar_catalogo_maz
    from .contratos import FonteDocumental,Periodo,Perimetro
    from .validacoes import ContratoESGInvalido
    bp=Blueprint('maz',__name__,template_folder='templates',static_folder='static',static_url_path='assets')
    labels={
        'em_acompanhamento':'Em acompanhamento','em_analise':'Em análise','pendente':'Pendente',
        'desconhecida':'Desconhecida','analise_tecnica':'Análise técnica','nao_avaliada':'Não avaliada',
        'evidencia':'Evidência','decisao_documental':'Manifestação / decisão documental',
        'apresentado':'Apresentado','analisado':'Analisado','suficiente':'Suficiente','insuficiente':'Insuficiente',
        'preenchido':'Preenchido','indisponivel':'Explicitamente indisponível','nao_informado':'Não informado',
        'instrumento_ref':'Instrumento de origem','texto':'Texto documental','competencia':'Competência',
        'prazo':'Prazo','responsavel_proposto':'Responsável proposto','responsavel_designado':'Responsável designado',
        'titulo':'Título','fonte':'Fonte documental','situacao':'Situação','documentos':'Documentos',
        'pendencias':'Pendências','riscos':'Riscos','encaminhamentos':'Encaminhamentos',
        'ambiental':'Ambiental','social':'Social','governanca':'Governança'}
    bp.add_app_template_filter(lambda v:labels.get(v,v),'maz_label')
    def percent(v):
        if v is None:return 'Indisponível'
        from decimal import localcontext
        with localcontext() as ctx:
            ctx.prec=max(60,len(v.as_tuple().digits)+4)
            return format(v,'.2f').replace('.',',')+'%'
    bp.add_app_template_filter(percent,'maz_percent')
    def page(view,**kw):return render_template('esg/maz/app.html',view=view,**kw)
    def scope():
        value=request.args.get('escopo','demonstracao')
        if value not in ('demonstracao','todos'):raise ValueError('Escopo inválido')
        return value
    def series():
        # A lista explícita da demonstração exclui ensaios sem apagar registros.
        rows=tuple(trcs.obter(i) for i in trcs.listar())
        anchors=set(current_app.config.get('MAZ_DEMO_TRCS_REFERENCIAS',()))
        allowed={m.serie_id for m in rows if str(m.revisao_id) in anchors or m.entrada_original.perimetro.codigo in current_app.config.get('MAZ_DEMO_PERIMETROS',())}
        groups={}
        for m in rows:
            if scope()=='todos' or m.serie_id in allowed:groups.setdefault(m.serie_id,[]).append(m)
        result=[]
        for sid,history in groups.items():
            current=trcs.obter(trcs.corrente(sid))
            result.append(dict(atual=current,historico=tuple(sorted(history,key=lambda m:m.numero,reverse=True))))
        return tuple(sorted(result,key=lambda g:(g['atual'].entrada_original.periodo.competencia,g['atual'].entrada_original.perimetro.descricao,str(g['atual'].serie_id))))
    def cases():
        allowed=current_app.config.get('MAZ_DEMO_OBRIGACOES',('O14-demo',))
        return tuple(obrigacoes.obter(o['identificador']) for o in obrigacoes.listar() if scope()=='todos' or o['identificador'] in allowed)
    def period(f):
        year=f.get('competencia_ano','').strip();month=f.get('competencia_mes','').strip()
        if not year:
            if month:raise ValueError('Mês exige ano')
            return None
        return Periodo(int(year),int(month) if month else None)
    def new_source(f,key,prefix):
        return FonteDocumental(prefix+uuid4().hex,f[key].strip())
    def guarded(fn):
        @admin_required
        @wraps(fn)
        def run(*a,**kw):
            if trcs is None or obrigacoes is None:return page('error',message='Persistência MAZ não configurada. Nenhuma conexão automática foi realizada.'),503
            try:return fn(*a,**kw)
            except ConflitoMAZ:return page('error',message='Conflito de edição. Reabra a ficha atual; nenhuma revisão foi sobrescrita.'),409
            except ConfirmacaoIncertaMAZ:return page('error',message='Confirmação incerta. Consulte a ficha antes de reenviar. Não repetimos a gravação automaticamente.'),503
            except (ContratoESGInvalido,ValueError,TypeError,KeyError,InvalidOperation):return page('error',message='Dados inválidos. Confira os campos e o escopo informado.'),400
            except PersistenciaMAZError:return page('error',message='Operação indisponível. Nenhum detalhe privado é exibido; consulte o estado antes de reenviar.'),503
        return run
    def actor():
        value=getattr(current_user,'id',None)
        if type(value)is not int or value<=0:abort(403)
        return value
    @bp.get('/')
    @guarded
    def executive():
        groups=series();selected=request.args.get('serie');measurement=None
        if selected:
            sid=UUID(selected)
            if not any(g['atual'].serie_id==sid for g in groups):raise ValueError('Série fora do filtro')
            measurement=trcs.obter(trcs.corrente(sid))
        else:
            anchor=current_app.config.get('MAZ_PAINEL_TRCS_REVISAO_REFERENCIA')
            match=next((g for g in groups if any(str(m.revisao_id)==anchor for m in g['historico'])),None)
            if match:measurement=match['atual']
        records=cases();completeness=next((obrigacoes.g2_registro(r) for r in records if r.caso.identificador=='O14-demo'),None)
        return page('executive',medicao=measurement,series=groups,registros=records,g2=completeness,catalogo=criar_catalogo_maz(),escopo=scope())
    @bp.get('/catalogo')
    @admin_required
    def catalog():
        import unicodedata
        normalize=lambda x:''.join(c for c in unicodedata.normalize('NFD',x.casefold()) if not unicodedata.combining(c))
        q=request.args.get('q','');dimension=request.args.get('dimensao','');availability=request.args.get('disponibilidade','')
        defs=tuple(d for d in criar_catalogo_maz().definicoes if (not q or normalize(q) in normalize(d.codigo+' '+d.nome+' '+d.definicao)) and (not dimension or d.dimensao.value==dimension) and (not availability or (d.codigo=='K01')==(availability=='motor')))
        return page('catalog',definicoes=defs,q=q,dimensao=dimension,disponibilidade=availability)
    @bp.get('/materialidade')
    @admin_required
    def materiality():return page('materiality')
    @bp.get('/trcs')
    @guarded
    def measurements():
        q=request.args.get('q','').casefold();competence=request.args.get('competencia','')
        groups=tuple(g for g in series() if (not q or q in g['atual'].entrada_original.perimetro.descricao.casefold()) and (not competence or g['atual'].entrada_original.periodo.competencia==competence))
        return page('measurements',series=groups,escopo=scope(),q=request.args.get('q',''),competencia=competence)
    @bp.route('/trcs/nova',methods=['GET','POST'])
    @bp.route('/trcs/<uuid:rid>/revisar',methods=['GET','POST'])
    @guarded
    def measure_form(rid=None):
        old=trcs.obter(rid) if rid else None
        if old and trcs.corrente(old.serie_id)!=rid:raise ConflitoMAZ('Revisão histórica não pode gerar sucessora.')
        if request.method=='GET':return page('measure_form',old=old)
        f=request.form
        if old and UUID(f['esperada'])!=rid:raise ConflitoMAZ('Rota e revisão esperada divergem.')
        per=old.entrada_original.perimetro if old else Perimetro('web'+uuid4().hex,f.get('perimetro','Recorte sintético'))
        period=old.entrada_original.periodo if old else Periodo(int(f['ano']),int(f['mes']) if f.get('mes') else None)
        source=new_source(f,'fonte','webf') if f.get('fonte','').strip() else (old.entrada_original.fonte if old else new_source(f,'fonte','webf'))
        source_version=1 if not old or f.get('fonte','').strip() else dict(old.versoes_fontes)['entrada']
        rejects_source=old.rejeitos_original.fonte if old and not f.get('fonte','').strip() else source
        rejects_version=dict(old.versoes_fontes)['rejeitos'] if old and not f.get('fonte','').strip() else 1
        evidence_source=new_source(f,'fonte_evidencia','webev') if f.get('fonte_evidencia','').strip() else source
        evidence_version=1 if evidence_source!=source else source_version
        def comp(name):return ComponenteMassa(Decimal(f[name]) if f.get(name) else None,UnidadeMassa(f.get('unidade_'+name,f.get('unidade','kg'))),period,per,rejects_source if name=='rejeitos' else source,OrigemValor.DECLARADO)
        new=trcs.registrar(comp('entrada'),comp('rejeitos'),old.metodologia if old else metodologia,ator=actor(),request_id=uuid4(),motivo=f['motivo'],serie_id=old.serie_id if old else None,revisao_esperada=rid if old else None,versao_entrada=source_version,versao_rejeitos=rejects_version,evidencias=tuple(old.evidencias if old else ()) + ((EvidenciaMAZ(evidence_source,evidence_version,f.get('evidencia') or 'Referência sintética declarada'),) if not old or f.get('evidencia') or f.get('fonte_evidencia') else ()))
        return redirect(url_for('maz.measure_detail',rid=new),code=303)
    @bp.get('/trcs/<uuid:rid>')
    @guarded
    def measure_detail(rid):
        m=trcs.obter(rid)
        return page('measure_detail',medicao=m,historico=tuple(trcs.obter(i) for i in trcs.listar(serie_id=m.serie_id)),corrente=trcs.corrente(m.serie_id))
    @bp.get('/obrigacoes')
    @guarded
    def obligations():
        state=request.args.get('situacao','');q=request.args.get('q','').casefold()
        records=tuple(r for r in cases() if (not state or r.caso.situacao==state) and (not q or q in (r.caso.identificador+' '+r.caso.titulo+' '+(r.caso.instrumento_ref or '')).casefold()))
        return page('obligations',registros=records,escopo=scope(),q=request.args.get('q',''),situacao=state)
    @bp.route('/obrigacoes/nova',methods=['GET','POST'])
    @bp.route('/obrigacoes/<identificador>/revisar',methods=['GET','POST'])
    @guarded
    def obligation_form(identificador=None):
        old=obrigacoes.obter(identificador) if identificador else None
        if request.method=='GET':return page('obligation_form',old=old)
        f=request.form;source=new_source(f,'fonte','webob') if f.get('fonte','').strip() else (old.caso.fonte if old else new_source(f,'fonte','webob'))
        source_version=old.caso.versao_fonte if old and source==old.caso.fonte else 1
        docs=old.caso.documentos if old else ()
        if f.get('documento'):
            doc_source=new_source(f,'fonte_documento','webdoc') if f.get('fonte_documento','').strip() else new_source({'fonte_documento':f['documento']},'fonte_documento','webdoc')
            docs=docs+(DocumentoObrigacao(doc_source,1,f['documento'],f.get('tipo_documento','evidencia'),f.get('estado_documento','apresentado'),f.get('avaliacao','nao_avaliada')),)
        formal=f.get('responsavel_designado') or None
        designation=(old.caso.designacao_fonte if old and formal==old.caso.responsavel_designado and f.get('fonte_designacao')==old.caso.designacao_fonte.referencia_documental else new_source(f,'fonte_designacao','des')) if formal else None
        c=Obrigacao(old.caso.identificador if old else f['identificador'],f['titulo'],source,versao_fonte=source_version,competencia=period(f) if 'competencia_ano' in f or 'competencia_mes' in f else (old.caso.competencia if old else None),instrumento_ref=f.get('instrumento') or None,texto=f.get('texto') or None,prazo=date.fromisoformat(f['prazo']) if f.get('prazo') else None,responsavel_proposto=f.get('responsavel_proposto') or None,responsavel_designado=formal,designacao_fonte=designation,designacao_versao=old.caso.designacao_versao if old and designation==old.caso.designacao_fonte else 1,situacao=f.get('situacao','desconhecida'),documentos=docs,pendencias=tuple(x.strip() for x in f.get('pendencias','').splitlines() if x.strip()),riscos=tuple(x.strip() for x in f.get('riscos','').splitlines() if x.strip()),encaminhamentos=tuple(x.strip() for x in f.get('encaminhamentos','').splitlines() if x.strip()))
        obrigacoes.salvar(c,ator=actor(),request_id=uuid4(),justificativa=f['justificativa'],revisao_esperada=UUID(f['esperada']) if old else None)
        return redirect(url_for('maz.obligation_detail',identificador=c.identificador),code=303)
    @bp.get('/obrigacoes/<identificador>')
    @bp.get('/obrigacoes/<identificador>/revisoes/<uuid:revision>')
    @guarded
    def obligation_detail(identificador,revision=None):
        record=obrigacoes.obter(identificador,revision);current=obrigacoes.obter(identificador) if revision else record
        return page('obligation_detail',registro=record,historico=obrigacoes.historico(identificador),g2=obrigacoes.g2_registro(record),is_current=record.revisao_id==current.revisao_id,corrente=current.revisao_id)
    return bp

def criar_demo_maz(trcs,obrigacoes,ator,password_hash,secret_key):
    """Aplicação separada: autenticação sintética explícita, nunca bypass de app.py."""
    from pathlib import Path
    from flask import Flask,render_template,request,redirect,url_for
    from flask_login import LoginManager,UserMixin,login_user,logout_user
    from werkzeug.security import check_password_hash
    from seguranca_csrf import configurar_csrf
    from . import criar_blueprint_esg
    root=Path(__file__).resolve().parents[2]
    app=Flask('maz_demo_local',root_path=str(root),template_folder=str(root/'templates'))
    app.config.update(MAZ_DEMO_ISOLADA=True,MAZ_DEMO_TRCS_REFERENCIAS=('599170cb-3f1f-4798-9c06-996d7ac78d47',),MAZ_DEMO_PERIMETROS=('demo05a-recorte',),MAZ_DEMO_OBRIGACOES=('O14-demo',),MAZ_PAINEL_TRCS_REVISAO_REFERENCIA='599170cb-3f1f-4798-9c06-996d7ac78d47',SECRET_KEY=secret_key,SESSION_COOKIE_HTTPONLY=True,SESSION_COOKIE_SAMESITE='Lax',MAX_CONTENT_LENGTH=65536)
    configurar_csrf(app);lm=LoginManager(app);lm.login_view='demo_login'
    class Synthetic(UserMixin):
        def __init__(self,value,role):self.id=value;self.role=role
    @lm.user_loader
    def load(value):
        if value==str(ator):return Synthetic(ator,'admin')
        if value=='readonly':return Synthetic(-1,'user')
        return None
    @app.route('/login',methods=['GET','POST'])
    def demo_login():
        error=None
        if request.method=='POST':
            if check_password_hash(password_hash,request.form.get('password','')):
                login_user(Synthetic(ator,'admin'));return redirect(url_for('maz.executive'),code=303)
            error='Código demonstrativo inválido.'
        return render_template('esg/maz/app.html',view='login',error=error)
    @app.post('/logout')
    def demo_logout():logout_user();return redirect(url_for('demo_login'),code=303)
    app.add_url_rule('/sair',endpoint='logout',view_func=demo_logout,methods=['POST'])
    @app.get('/')
    def index():return redirect(url_for('maz.executive'))
    from flask_wtf.csrf import CSRFError
    @app.errorhandler(CSRFError)
    def csrf_error(_):return render_template('esg/maz/app.html',view='error',message='Solicitação recusada por CSRF.'),400
    app.register_blueprint(criar_blueprint_esg(),url_prefix='/esg')
    app.register_blueprint(criar_blueprint_maz(trcs,obrigacoes),url_prefix='/esg/maz')
    @app.errorhandler(403)
    def denied(_):return render_template('esg/maz/app.html',view='error',message='Acesso não autorizado.'),403
    return app
