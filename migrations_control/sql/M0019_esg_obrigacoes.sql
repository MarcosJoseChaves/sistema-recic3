-- M0019 candidate: documentary follow-up only; no legal/operational authorization.
CREATE TABLE public.esg_obrigacoes (
 id VARCHAR(64) PRIMARY KEY CHECK (id ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'),
 criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 criado_por_usuario_id INTEGER NOT NULL REFERENCES public.usuarios(id),
 request_id UUID NOT NULL, auditoria_id BIGINT NOT NULL REFERENCES public.auditoria_tecnica(id)
);
CREATE TABLE public.esg_obrigacao_revisoes (
 id UUID PRIMARY KEY, obrigacao_id VARCHAR(64) NOT NULL REFERENCES public.esg_obrigacoes(id),
 numero INTEGER NOT NULL CHECK(numero>0), anterior UUID,
 titulo TEXT NOT NULL CHECK(btrim(titulo)<>''), instrumento_ref TEXT,
 fonte_id VARCHAR(64) NOT NULL, fonte_versao INTEGER NOT NULL,
 texto TEXT, ano SMALLINT CHECK(ano BETWEEN 1 AND 9999), mes SMALLINT CHECK(mes BETWEEN 1 AND 12),
 prazo DATE, prazo_conhecido BOOLEAN NOT NULL DEFAULT FALSE,
 responsavel_proposto TEXT, responsavel_designado TEXT,
 designacao_fonte_id VARCHAR(64), designacao_fonte_versao INTEGER,
 situacao TEXT NOT NULL CHECK(situacao IN ('desconhecida','em_acompanhamento','pendente','em_analise')),
 pendencias TEXT[] NOT NULL DEFAULT '{}', riscos TEXT[] NOT NULL DEFAULT '{}', encaminhamentos TEXT[] NOT NULL DEFAULT '{}',
 justificativa TEXT NOT NULL CHECK(btrim(justificativa)<>''),
 cumprimento_juridico TEXT NOT NULL DEFAULT 'nao_determinado' CHECK(cumprimento_juridico='nao_determinado'),
 autorizacao_operacional BOOLEAN NOT NULL DEFAULT FALSE CHECK(autorizacao_operacional=FALSE),
 criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 criado_por_usuario_id INTEGER NOT NULL REFERENCES public.usuarios(id), request_id UUID NOT NULL,
 auditoria_id BIGINT NOT NULL REFERENCES public.auditoria_tecnica(id),
 UNIQUE(obrigacao_id,numero), UNIQUE(id,obrigacao_id,numero),
 FOREIGN KEY(fonte_id,fonte_versao) REFERENCES public.esg_fonte_versoes(fonte_id,versao),
 FOREIGN KEY(designacao_fonte_id,designacao_fonte_versao) REFERENCES public.esg_fonte_versoes(fonte_id,versao) MATCH FULL,
 CHECK ((numero=1 AND anterior IS NULL) OR (numero>1 AND anterior IS NOT NULL)),
 CHECK (mes IS NULL OR ano IS NOT NULL), CHECK(prazo_conhecido=(prazo IS NOT NULL)),
 CHECK ((responsavel_designado IS NULL AND designacao_fonte_id IS NULL) OR (responsavel_designado IS NOT NULL AND designacao_fonte_id IS NOT NULL))
);
-- A predecessor must be the preceding revision of the same obligation.
CREATE FUNCTION public.esg_obrigacao_predecessor() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $f$
BEGIN
 IF NEW.numero>1 AND NOT EXISTS(SELECT 1 FROM public.esg_obrigacao_revisoes WHERE id=NEW.anterior AND obrigacao_id=NEW.obrigacao_id AND numero=NEW.numero-1)
 THEN RAISE EXCEPTION 'Predecessor incompatível' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END;$f$;
CREATE TRIGGER predecessor_obrigacao BEFORE INSERT ON public.esg_obrigacao_revisoes FOR EACH ROW EXECUTE FUNCTION public.esg_obrigacao_predecessor();
CREATE TABLE public.esg_obrigacao_documentos (
 id UUID PRIMARY KEY, revisao_id UUID NOT NULL REFERENCES public.esg_obrigacao_revisoes(id),
 fonte_id VARCHAR(64) NOT NULL, fonte_versao INTEGER NOT NULL,
 documento_privado_id BIGINT REFERENCES public.documentos_privados(id),
 sha256 VARCHAR(64) CHECK(sha256 IS NULL OR sha256 ~ '^[0-9a-f]{64}$'),
 tipo TEXT NOT NULL CHECK(tipo IN ('evidencia','analise_tecnica','decisao_documental')),
 descricao TEXT NOT NULL CHECK(btrim(descricao)<>''),
 estado TEXT NOT NULL CHECK(estado IN ('apresentado','analisado')),
 avaliacao TEXT NOT NULL CHECK(avaliacao IN ('nao_avaliada','suficiente','insuficiente')),
 eficacia_juridica BOOLEAN NOT NULL DEFAULT FALSE CHECK(eficacia_juridica=FALSE),
 criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 criado_por_usuario_id INTEGER NOT NULL REFERENCES public.usuarios(id), request_id UUID NOT NULL,
 auditoria_id BIGINT NOT NULL REFERENCES public.auditoria_tecnica(id),
 FOREIGN KEY(fonte_id,fonte_versao) REFERENCES public.esg_fonte_versoes(fonte_id,versao),
 CHECK(avaliacao='nao_avaliada' OR (estado='analisado' AND tipo='analise_tecnica'))
);
CREATE TRIGGER documento_obrigacao BEFORE INSERT ON public.esg_obrigacao_documentos FOR EACH ROW EXECUTE FUNCTION public.esg_conferir_documento();
CREATE TABLE public.esg_obrigacao_correntes (
 obrigacao_id VARCHAR(64) PRIMARY KEY REFERENCES public.esg_obrigacoes(id),
 revisao_id UUID NOT NULL, numero INTEGER NOT NULL,
 criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
 criado_por_usuario_id INTEGER NOT NULL REFERENCES public.usuarios(id), request_id UUID NOT NULL,
 auditoria_id BIGINT NOT NULL REFERENCES public.auditoria_tecnica(id),
 FOREIGN KEY(revisao_id,obrigacao_id,numero) REFERENCES public.esg_obrigacao_revisoes(id,obrigacao_id,numero)
);
CREATE FUNCTION public.esg_obrigacao_corrente() RETURNS TRIGGER
LANGUAGE plpgsql SET search_path=pg_catalog,public,pg_temp AS $f$
BEGIN
 IF TG_OP='DELETE' OR (TG_OP='INSERT' AND NEW.numero<>1) THEN RAISE EXCEPTION 'Corrente inválida' USING ERRCODE='23514'; END IF;
 IF TG_OP='UPDATE' AND (NEW.obrigacao_id<>OLD.obrigacao_id OR NEW.numero<>OLD.numero+1 OR NOT EXISTS(SELECT 1 FROM public.esg_obrigacao_revisoes WHERE id=NEW.revisao_id AND anterior=OLD.revisao_id)) THEN RAISE EXCEPTION 'Salto inválido' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END;$f$;
CREATE TRIGGER corrente_obrigacao BEFORE INSERT OR UPDATE OR DELETE ON public.esg_obrigacao_correntes FOR EACH ROW EXECUTE FUNCTION public.esg_obrigacao_corrente();
CREATE TRIGGER audit_esg_obrigacoes BEFORE INSERT OR UPDATE ON public.esg_obrigacoes FOR EACH ROW EXECUTE FUNCTION public.esg_conferir_auditoria();
CREATE TRIGGER im_esg_obrigacoes BEFORE UPDATE OR DELETE ON public.esg_obrigacoes FOR EACH ROW EXECUTE FUNCTION public.esg_recusar_mutacao();
CREATE TRIGGER audit_esg_obrigacao_revisoes BEFORE INSERT OR UPDATE ON public.esg_obrigacao_revisoes FOR EACH ROW EXECUTE FUNCTION public.esg_conferir_auditoria();
CREATE TRIGGER im_esg_obrigacao_revisoes BEFORE UPDATE OR DELETE ON public.esg_obrigacao_revisoes FOR EACH ROW EXECUTE FUNCTION public.esg_recusar_mutacao();
CREATE TRIGGER audit_esg_obrigacao_documentos BEFORE INSERT OR UPDATE ON public.esg_obrigacao_documentos FOR EACH ROW EXECUTE FUNCTION public.esg_conferir_auditoria();
CREATE TRIGGER im_esg_obrigacao_documentos BEFORE UPDATE OR DELETE ON public.esg_obrigacao_documentos FOR EACH ROW EXECUTE FUNCTION public.esg_recusar_mutacao();
CREATE TRIGGER audit_esg_obrigacao_correntes BEFORE INSERT OR UPDATE ON public.esg_obrigacao_correntes FOR EACH ROW EXECUTE FUNCTION public.esg_conferir_auditoria();
