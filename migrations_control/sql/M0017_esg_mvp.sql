SET LOCAL search_path = public, pg_catalog;
DO $guard$
BEGIN
 IF EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND left(c.relname,4)='esg_')
 OR EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public' AND left(p.proname,4)='esg_')
 THEN RAISE EXCEPTION 'M0017 exige ESG vazio; objetos existentes requerem reconciliacao revisada'; END IF;
END;
$guard$;

-- CANDIDATA M0017; externa, NAO EXECUTADA. PostgreSQL17 proposto.
-- Perfil suportado: core M0016 equivalente e NENHUM objeto esg_* preexistente.
-- CREATE sem IF NOT EXISTS aborta colisao; nao adota proposta V1 automaticamente.
-- BEGIN abaixo ocorre somente em blocos PL/pgSQL, nunca transacao interna.

CREATE TABLE esg_unidades (
simbolo TEXT PRIMARY KEY CHECK (simbolo = 'kg'), descricao TEXT NOT NULL CHECK (btrim(descricao) <> ''),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_perimetros (
codigo VARCHAR(64) PRIMARY KEY CHECK (codigo ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'), descricao TEXT NOT NULL CHECK (btrim(descricao) <> ''),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_fontes_documentais (
identificador VARCHAR(64) PRIMARY KEY CHECK (identificador ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_fonte_versoes (
fonte_id VARCHAR(64) NOT NULL REFERENCES esg_fontes_documentais(identificador),
versao INTEGER NOT NULL CHECK (versao > 0),
anterior INTEGER,
referencia TEXT NOT NULL CHECK (btrim(referencia) <> ''),
data_documento DATE,
documento_privado_id BIGINT REFERENCES documentos_privados(id),
sha256 VARCHAR(64) CHECK (sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'),
motivo TEXT NOT NULL CHECK (btrim(motivo) <> ''),
PRIMARY KEY (fonte_id, versao),
FOREIGN KEY (fonte_id, anterior) REFERENCES esg_fonte_versoes(fonte_id,versao),
CHECK ((versao = 1 AND anterior IS NULL) OR (versao > 1 AND anterior IS NOT NULL AND anterior = versao - 1)),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_definicoes (
codigo VARCHAR(8) PRIMARY KEY CHECK (codigo IN ('K01','K02','K03','K04a','K04b','K04c','K05','K06','K07','K08','K09a','K09b','K09c','K09d','K10','K11')),
nome TEXT NOT NULL CHECK (btrim(nome) <> ''),
dimensao TEXT NOT NULL CHECK (dimensao IN ('ambiental','social','governanca')),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_aliases (
alias TEXT PRIMARY KEY CHECK (alias = 'A1'),
indicador_codigo VARCHAR(8) NOT NULL REFERENCES esg_definicoes(codigo) CHECK (indicador_codigo = 'K01'),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_metodologias (
indicador_codigo VARCHAR(8) NOT NULL REFERENCES esg_definicoes(codigo),
versao VARCHAR(64) NOT NULL CHECK (versao ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'),
especificacao JSONB NOT NULL CHECK (jsonb_typeof(especificacao) = 'object'),
CHECK (indicador_codigo <> 'K01' OR (
 especificacao ?& ARRAY['formula','contrato_numerico'] AND
 (especificacao->>'formula') IS NOT DISTINCT FROM 'TRCS_BRUTA_100_REJEITOS_ENTRADA' AND
 (especificacao->>'contrato_numerico') IS NOT DISTINCT FROM 'R2-kg-1e9-6')) ,
hash_especificacao VARCHAR(64) NOT NULL CHECK (hash_especificacao ~ '^[0-9a-f]{64}$'),
fonte_id VARCHAR(64) NOT NULL,
fonte_versao INTEGER NOT NULL,
situacao TEXT NOT NULL DEFAULT 'proposto' CHECK (situacao = 'proposto'),
PRIMARY KEY (indicador_codigo,versao),
FOREIGN KEY (fonte_id,fonte_versao) REFERENCES esg_fonte_versoes(fonte_id,versao),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_series (
id UUID PRIMARY KEY,
indicador_codigo VARCHAR(8) NOT NULL REFERENCES esg_definicoes(codigo) CHECK (indicador_codigo = 'K01'),
ano SMALLINT NOT NULL CHECK (ano BETWEEN 1 AND 9999),
mes SMALLINT CHECK (mes BETWEEN 1 AND 12),
perimetro_codigo VARCHAR(64) NOT NULL REFERENCES esg_perimetros(codigo),
UNIQUE (id,indicador_codigo),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE UNIQUE INDEX uq_esg_series_anual ON esg_series(indicador_codigo,ano,perimetro_codigo) WHERE mes IS NULL;
CREATE UNIQUE INDEX uq_esg_series_mensal ON esg_series(indicador_codigo,ano,mes,perimetro_codigo) WHERE mes IS NOT NULL;

CREATE TABLE esg_revisoes (
id UUID PRIMARY KEY,
serie_id UUID NOT NULL,
indicador_codigo VARCHAR(8) NOT NULL CHECK (indicador_codigo = 'K01'),
metodologia_versao VARCHAR(64) NOT NULL,
numero INTEGER NOT NULL CHECK (numero > 0),
anterior UUID,
numero_anterior INTEGER,
motivo TEXT NOT NULL CHECK (btrim(motivo) <> ''),
situacao TEXT NOT NULL DEFAULT 'rascunho' CHECK (situacao = 'rascunho'),
UNIQUE (serie_id,numero),
UNIQUE (id,serie_id),
UNIQUE (id,serie_id,indicador_codigo,metodologia_versao),
UNIQUE (id,serie_id,numero),
FOREIGN KEY (serie_id,indicador_codigo) REFERENCES esg_series(id,indicador_codigo),
FOREIGN KEY (indicador_codigo,metodologia_versao) REFERENCES esg_metodologias(indicador_codigo,versao),
FOREIGN KEY (anterior,serie_id,numero_anterior) REFERENCES esg_revisoes(id,serie_id,numero),
CHECK ((numero=1 AND anterior IS NULL AND numero_anterior IS NULL) OR
       (numero>1 AND anterior IS NOT NULL AND numero_anterior IS NOT NULL AND numero_anterior=numero-1)),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_registros (
revisao_id UUID NOT NULL,
serie_id UUID NOT NULL,
indicador_codigo VARCHAR(8) NOT NULL CHECK (indicador_codigo = 'K01'),
metodologia_versao VARCHAR(64) NOT NULL,
papel TEXT NOT NULL CHECK (papel IN ('rejeitos','entrada')),
valor NUMERIC CHECK (valor IS NULL OR (valor::text NOT IN ('NaN','Infinity','-Infinity') AND valor BETWEEN 0 AND 1000000000 AND valor=trunc(valor,6))),
valor_original NUMERIC,
valor_original_texto TEXT CHECK (valor_original_texto IS NULL OR length(valor_original_texto)<=256),
unidade_original TEXT CHECK (unidade_original IN ('kg','t')),
regra_conversao TEXT CHECK (regra_conversao IN ('kg-identidade-v1','t-kg-x1000-v1')),
CHECK ((valor IS NULL AND valor_original IS NULL AND valor_original_texto IS NULL AND unidade_original IS NULL AND regra_conversao IS NULL)
 OR (valor IS NOT NULL AND valor_original IS NOT NULL AND valor_original_texto IS NOT NULL AND unidade_original IS NOT NULL AND regra_conversao IS NOT NULL
 AND valor_original::text NOT IN ('NaN','Infinity','-Infinity') AND valor_original>=0
 AND valor_original_texto::numeric=valor_original
 AND ((unidade_original='kg' AND regra_conversao='kg-identidade-v1' AND valor=valor_original)
 OR (unidade_original='t' AND regra_conversao='t-kg-x1000-v1' AND valor=valor_original*1000)))),
CHECK (qualidade<>'validado' OR valor IS NOT NULL),
unidade TEXT NOT NULL REFERENCES esg_unidades(simbolo) CHECK (unidade='kg'),
fonte_id VARCHAR(64) NOT NULL,
fonte_versao INTEGER NOT NULL,
origem TEXT NOT NULL CHECK (origem IN ('declarado','estimado','calculado')),
qualidade TEXT NOT NULL CHECK (qualidade IN ('provisorio','validado','inconclusivo')),
disponibilidade TEXT NOT NULL CHECK (disponibilidade IN ('informado','indisponivel','nao_observado')),
validacao_fonte_id VARCHAR(64),
validacao_fonte_versao INTEGER,
PRIMARY KEY (revisao_id,papel),
FOREIGN KEY (revisao_id,serie_id,indicador_codigo,metodologia_versao)
 REFERENCES esg_revisoes(id,serie_id,indicador_codigo,metodologia_versao),
FOREIGN KEY (fonte_id,fonte_versao) REFERENCES esg_fonte_versoes(fonte_id,versao),
FOREIGN KEY (validacao_fonte_id,validacao_fonte_versao) REFERENCES esg_fonte_versoes(fonte_id,versao) MATCH FULL,
CHECK ((disponibilidade='informado' AND valor IS NOT NULL) OR (disponibilidade<>'informado' AND valor IS NULL)),
CHECK (qualidade<>'validado' OR validacao_fonte_id IS NOT NULL),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_resultados_trcs (
revisao_id UUID PRIMARY KEY REFERENCES esg_revisoes(id),
percentual NUMERIC CHECK (percentual IS NULL OR (percentual::text NOT IN ('NaN','Infinity','-Infinity') AND percentual>=0)),
razao_numerador NUMERIC CHECK (razao_numerador IS NULL OR (razao_numerador BETWEEN 0 AND 100000000000000000 AND razao_numerador=trunc(razao_numerador))),
razao_denominador NUMERIC CHECK (razao_denominador IS NULL OR (razao_denominador BETWEEN 1 AND 1000000000000000 AND razao_denominador=trunc(razao_denominador))),
precisao SMALLINT NOT NULL CHECK (precisao BETWEEN 28 AND 200),
arredondamento TEXT NOT NULL DEFAULT 'ROUND_HALF_EVEN' CHECK (arredondamento='ROUND_HALF_EVEN'),
disponibilidade TEXT NOT NULL CHECK (disponibilidade IN ('informado','indisponivel','nao_calculavel')),
origem TEXT NOT NULL DEFAULT 'calculado' CHECK (origem='calculado'),
qualidade TEXT NOT NULL DEFAULT 'provisorio' CHECK (qualidade='provisorio'),
aprovacao TEXT NOT NULL DEFAULT 'nao_autorizada' CHECK (aprovacao='nao_autorizada'),
publicavel BOOLEAN NOT NULL DEFAULT FALSE CHECK (publicavel=FALSE),
CHECK ((disponibilidade='informado' AND percentual IS NOT NULL AND razao_numerador IS NOT NULL AND razao_denominador IS NOT NULL)
 OR (disponibilidade<>'informado' AND percentual IS NULL AND razao_numerador IS NULL AND razao_denominador IS NULL)),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_evidencias (
id UUID PRIMARY KEY,
revisao_id UUID NOT NULL REFERENCES esg_revisoes(id),
fonte_id VARCHAR(64) NOT NULL,
fonte_versao INTEGER NOT NULL,
tipo TEXT NOT NULL CHECK (btrim(tipo) <> ''),
FOREIGN KEY (fonte_id,fonte_versao) REFERENCES esg_fonte_versoes(fonte_id,versao),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_avaliacoes (
id UUID PRIMARY KEY,
revisao_id UUID NOT NULL REFERENCES esg_resultados_trcs(revisao_id),
validade_metodologica TEXT NOT NULL CHECK (validade_metodologica IN ('pendente','inconclusiva','referencia_apresentada')),
reconciliacao_fonte_id VARCHAR(64),
reconciliacao_fonte_versao INTEGER,
reconciliacao_ano SMALLINT,
reconciliacao_mes SMALLINT CHECK (reconciliacao_mes BETWEEN 1 AND 12),
reconciliacao_perimetro VARCHAR(64) REFERENCES esg_perimetros(codigo),
CHECK ((reconciliacao_fonte_id IS NULL AND reconciliacao_ano IS NULL AND reconciliacao_mes IS NULL AND reconciliacao_perimetro IS NULL)
 OR (reconciliacao_fonte_id IS NOT NULL AND reconciliacao_ano IS NOT NULL AND reconciliacao_perimetro IS NOT NULL)),
pendencias JSONB NOT NULL CHECK (jsonb_typeof(pendencias)='array' AND jsonb_array_length(pendencias)>0),
aprovacao TEXT NOT NULL DEFAULT 'nao_autorizada' CHECK (aprovacao='nao_autorizada'),
publicavel BOOLEAN NOT NULL DEFAULT FALSE CHECK (publicavel=FALSE),
FOREIGN KEY (reconciliacao_fonte_id,reconciliacao_fonte_versao) REFERENCES esg_fonte_versoes(fonte_id,versao) MATCH FULL,
CHECK ((validade_metodologica='referencia_apresentada' AND reconciliacao_fonte_id IS NOT NULL)
 OR (validade_metodologica<>'referencia_apresentada' AND reconciliacao_fonte_id IS NULL)),
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_decisoes_tecnicas (
id UUID PRIMARY KEY,
revisao_id UUID NOT NULL REFERENCES esg_revisoes(id),
avaliacao_id UUID,
tipo TEXT NOT NULL CHECK (tipo IN ('anotacao','solicitacao_revisao','devolucao')),
justificativa TEXT NOT NULL CHECK (btrim(justificativa)<>''),
fonte_id VARCHAR(64),
fonte_versao INTEGER,
FOREIGN KEY (fonte_id,fonte_versao) REFERENCES esg_fonte_versoes(fonte_id,versao) MATCH FULL,
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE esg_correntes (
serie_id UUID PRIMARY KEY REFERENCES esg_series(id),
revisao_id UUID NOT NULL,
numero INTEGER NOT NULL,
FOREIGN KEY (revisao_id,serie_id,numero) REFERENCES esg_revisoes(id,serie_id,numero),
FOREIGN KEY (revisao_id) REFERENCES esg_resultados_trcs(revisao_id) DEFERRABLE INITIALLY DEFERRED,
criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
request_id UUID NOT NULL,
auditoria_id BIGINT NOT NULL REFERENCES auditoria_tecnica(id) DEFERRABLE INITIALLY DEFERRED
);

-- Projecao decimal exata com half-even por aritmetica inteira; sem round() do PostgreSQL.
CREATE FUNCTION esg_percentual_half_even(n NUMERIC,d NUMERIC,p INTEGER) RETURNS NUMERIC
LANGUAGE SQL IMMUTABLE STRICT SET search_path=pg_catalog,public,pg_temp AS $fn$
WITH e0 AS (SELECT length(trunc(n)::text)-length(trunc(d)::text) AS e),
expo AS (SELECT e-CASE WHEN n<d*power(10::numeric,e) THEN 1 ELSE 0 END AS e FROM e0),
s AS (SELECT p-1-e AS casas FROM expo),
q AS (SELECT casas,div(n*power(10::numeric,casas),d) AS inteiro,
 mod(n*power(10::numeric,casas),d) AS resto FROM s),
r AS (SELECT casas,inteiro+CASE WHEN resto*2>d OR (resto*2=d AND mod(inteiro,2)=1) THEN 1 ELSE 0 END AS valor FROM q)
SELECT CASE WHEN n=0 THEN 0::numeric ELSE (valor::text||'e-'||casas::text)::numeric END FROM r
$fn$;

CREATE FUNCTION esg_recusar_mutacao() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $fn$
BEGIN
 RAISE EXCEPTION 'ESG: fato imutavel; criar revisao' USING ERRCODE='23514';
END;
$fn$;

CREATE FUNCTION esg_conferir_auditoria() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $fn$
BEGIN
 PERFORM 1 FROM public.auditoria_tecnica WHERE id=NEW.auditoria_id
 AND usuario_id=NEW.criado_por_usuario_id AND request_id=NEW.request_id AND modulo='ESG';
 IF NOT FOUND THEN RAISE EXCEPTION 'ESG: autoria/request/auditoria incoerentes' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END;
$fn$;

CREATE FUNCTION esg_conferir_resultado() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $fn$
DECLARE n public.esg_registros%ROWTYPE; d public.esg_registros%ROWTYPE;
BEGIN
 SELECT * INTO STRICT n FROM public.esg_registros WHERE revisao_id=NEW.revisao_id AND papel='rejeitos';
 SELECT * INTO STRICT d FROM public.esg_registros WHERE revisao_id=NEW.revisao_id AND papel='entrada';
 IF n.fonte_id=d.fonte_id AND n.fonte_versao<>d.fonte_versao THEN RAISE EXCEPTION 'ESG: fontes conflitantes'; END IF;
 IF n.valor IS NULL OR d.valor IS NULL THEN
   IF NEW.disponibilidade<>'indisponivel' THEN RAISE EXCEPTION 'ESG: ausencia nao e zero'; END IF;
 ELSIF d.valor=0 THEN
   IF NEW.disponibilidade<>'nao_calculavel' THEN RAISE EXCEPTION 'ESG: denominador zero'; END IF;
 ELSE
   IF NEW.disponibilidade<>'informado'
      OR NEW.razao_numerador<>n.valor*100000000
      OR NEW.razao_denominador<>d.valor*1000000
      OR NEW.percentual<>public.esg_percentual_half_even(NEW.razao_numerador,NEW.razao_denominador,NEW.precisao)
   THEN RAISE EXCEPTION 'ESG: derivacao TRCS incoerente' USING ERRCODE='23514'; END IF;
 END IF;
 RETURN NEW;
END;
$fn$;
CREATE CONSTRAINT TRIGGER ct_esg_resultado AFTER INSERT ON esg_resultados_trcs
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION esg_conferir_resultado();

CREATE FUNCTION esg_conferir_avaliacao() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $fn$
DECLARE n public.esg_registros%ROWTYPE; d public.esg_registros%ROWTYPE; s public.esg_series%ROWTYPE; disp TEXT; pend TEXT[];
BEGIN
 SELECT * INTO STRICT n FROM public.esg_registros WHERE revisao_id=NEW.revisao_id AND papel='rejeitos';
 SELECT * INTO STRICT d FROM public.esg_registros WHERE revisao_id=NEW.revisao_id AND papel='entrada';
 SELECT a.* INTO STRICT s FROM public.esg_series a JOIN public.esg_revisoes r ON r.serie_id=a.id WHERE r.id=NEW.revisao_id;
 SELECT disponibilidade INTO STRICT disp FROM public.esg_resultados_trcs WHERE revisao_id=NEW.revisao_id;
 pend:=ARRAY['Metodologia proposta; aprovação institucional não verificada.','Pesagem e expedição efetivas devem ser verificadas documentalmente.'];
 IF NEW.reconciliacao_fonte_id IS NULL THEN
   pend:=array_append(pend,'Reconciliação física e de estoques não apresentada.');
 ELSE
   IF NEW.reconciliacao_ano<>s.ano OR NEW.reconciliacao_mes IS DISTINCT FROM s.mes OR NEW.reconciliacao_perimetro<>s.perimetro_codigo
   THEN RAISE EXCEPTION 'ESG: reconciliacao temporal/territorial incompativel'; END IF;
   IF (NEW.reconciliacao_fonte_id=n.fonte_id AND NEW.reconciliacao_fonte_versao<>n.fonte_versao)
    OR (NEW.reconciliacao_fonte_id=d.fonte_id AND NEW.reconciliacao_fonte_versao<>d.fonte_versao)
   THEN RAISE EXCEPTION 'ESG: fontes conflitantes'; END IF;
   pend:=array_append(pend,'Referência de reconciliação apresentada; conteúdo ainda não verificado.');
 END IF;
 IF disp<>'informado' THEN pend:=array_append(pend,'Componentes ausentes ou denominador zero impedem o cálculo.'); END IF;
 IF n.origem='estimado' OR d.origem='estimado' THEN pend:=array_append(pend,'Componente estimado exige conferência de qualidade.'); END IF;
 IF n.valor>d.valor THEN pend:=array_append(pend,'Rejeitos superiores à entrada: examinar fronteiras e estoques; sem ajuste presumido.'); END IF;
 IF NEW.pendencias<>to_jsonb(pend) THEN RAISE EXCEPTION 'ESG: pendencias divergem do nucleo R1'; END IF;
 RETURN NEW;
END;
$fn$;
CREATE CONSTRAINT TRIGGER ct_esg_avaliacao AFTER INSERT ON esg_avaliacoes
 DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION esg_conferir_avaliacao();

CREATE TRIGGER au_esg_unidades BEFORE INSERT OR UPDATE ON esg_unidades FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_unidades BEFORE UPDATE OR DELETE ON esg_unidades FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_perimetros BEFORE INSERT OR UPDATE ON esg_perimetros FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_perimetros BEFORE UPDATE OR DELETE ON esg_perimetros FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_fontes_documentais BEFORE INSERT OR UPDATE ON esg_fontes_documentais FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_fontes_documentais BEFORE UPDATE OR DELETE ON esg_fontes_documentais FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_fonte_versoes BEFORE INSERT OR UPDATE ON esg_fonte_versoes FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_fonte_versoes BEFORE UPDATE OR DELETE ON esg_fonte_versoes FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_definicoes BEFORE INSERT OR UPDATE ON esg_definicoes FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_definicoes BEFORE UPDATE OR DELETE ON esg_definicoes FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_aliases BEFORE INSERT OR UPDATE ON esg_aliases FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_aliases BEFORE UPDATE OR DELETE ON esg_aliases FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_metodologias BEFORE INSERT OR UPDATE ON esg_metodologias FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_metodologias BEFORE UPDATE OR DELETE ON esg_metodologias FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_series BEFORE INSERT OR UPDATE ON esg_series FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_series BEFORE UPDATE OR DELETE ON esg_series FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_revisoes BEFORE INSERT OR UPDATE ON esg_revisoes FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_revisoes BEFORE UPDATE OR DELETE ON esg_revisoes FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_registros BEFORE INSERT OR UPDATE ON esg_registros FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_registros BEFORE UPDATE OR DELETE ON esg_registros FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_resultados_trcs BEFORE INSERT OR UPDATE ON esg_resultados_trcs FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_resultados_trcs BEFORE UPDATE OR DELETE ON esg_resultados_trcs FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_evidencias BEFORE INSERT OR UPDATE ON esg_evidencias FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_evidencias BEFORE UPDATE OR DELETE ON esg_evidencias FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_avaliacoes BEFORE INSERT OR UPDATE ON esg_avaliacoes FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_avaliacoes BEFORE UPDATE OR DELETE ON esg_avaliacoes FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_decisoes_tecnicas BEFORE INSERT OR UPDATE ON esg_decisoes_tecnicas FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

CREATE TRIGGER im_esg_decisoes_tecnicas BEFORE UPDATE OR DELETE ON esg_decisoes_tecnicas FOR EACH ROW EXECUTE FUNCTION esg_recusar_mutacao();

CREATE TRIGGER au_esg_correntes BEFORE INSERT OR UPDATE ON esg_correntes FOR EACH ROW EXECUTE FUNCTION esg_conferir_auditoria();

ALTER TABLE esg_avaliacoes ADD CONSTRAINT uq_esg_avaliacao_revisao UNIQUE(id,revisao_id);
ALTER TABLE esg_decisoes_tecnicas ADD CONSTRAINT fk_esg_decisao_avaliacao
 FOREIGN KEY(avaliacao_id,revisao_id) REFERENCES esg_avaliacoes(id,revisao_id);
-- Sem INSERT/seed e sem acesso a servicos externos. Aplicacao futura escreve auditoria na mesma transacao.

CREATE FUNCTION esg_conferir_corrente() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $fn$
BEGIN
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'ESG: corrente nao pode ser apagada'; END IF;
 IF TG_OP='INSERT' THEN
   IF NEW.numero<>1 THEN RAISE EXCEPTION 'ESG: corrente inicial deve ser revisao1'; END IF;
 ELSE
   IF NEW.serie_id<>OLD.serie_id OR NEW.numero<>OLD.numero+1 THEN RAISE EXCEPTION 'ESG: conflito de revisao corrente'; END IF;
   PERFORM 1 FROM public.esg_revisoes WHERE id=NEW.revisao_id AND anterior=OLD.revisao_id AND serie_id=OLD.serie_id;
   IF NOT FOUND THEN RAISE EXCEPTION 'ESG: corrente nao aponta para sucessora'; END IF;
 END IF;
 RETURN NEW;
END;
$fn$;
CREATE TRIGGER ct_esg_corrente BEFORE INSERT OR UPDATE OR DELETE ON esg_correntes FOR EACH ROW EXECUTE FUNCTION esg_conferir_corrente();

CREATE FUNCTION esg_conferir_documento() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $fn$
DECLARE h TEXT;
BEGIN
 IF NEW.documento_privado_id IS NOT NULL AND NEW.sha256 IS NOT NULL THEN
   SELECT sha256 INTO STRICT h FROM public.documentos_privados WHERE id=NEW.documento_privado_id;
   IF h<>NEW.sha256 THEN RAISE EXCEPTION 'ESG: hash nao corresponde ao documento privado'; END IF;
 END IF;
 RETURN NEW;
END;
$fn$;
CREATE TRIGGER ct_esg_documento BEFORE INSERT ON esg_fonte_versoes FOR EACH ROW EXECUTE FUNCTION esg_conferir_documento();
