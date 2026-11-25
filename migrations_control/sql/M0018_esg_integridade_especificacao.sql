-- M0018 candidate v1. Exact byte integrity, not semantic canonicalization.
-- Historical bytes explicitly recovered from the reviewed 03B4 fixture writer.
ALTER TABLE public.esg_metodologias
 ADD COLUMN especificacao_serializacao TEXT NOT NULL DEFAULT 'MAZ-SPEC-UTF8-JSON-v1',
 ADD COLUMN especificacao_bytes BYTEA DEFAULT convert_to('{"contrato_numerico":"R2-kg-1e9-6","formula":"TRCS_BRUTA_100_REJEITOS_ENTRADA"}','UTF8');
ALTER TABLE public.esg_metodologias ALTER COLUMN especificacao_bytes DROP DEFAULT;
CREATE FUNCTION public.esg_validar_especificacao_bytes(payload BYTEA, doc JSONB, digest TEXT, contrato TEXT)
RETURNS BOOLEAN LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,public,pg_temp AS $fn$
DECLARE txt TEXT;
BEGIN
 IF payload IS NULL OR doc IS NULL OR digest IS NULL OR contrato IS DISTINCT FROM 'MAZ-SPEC-UTF8-JSON-v1' THEN RETURN FALSE; END IF;
 txt:=convert_from(payload,'UTF8');
 IF NOT (txt IS JSON OBJECT WITH UNIQUE KEYS) THEN RETURN FALSE; END IF;
 RETURN txt::jsonb=doc AND encode(sha256(payload),'hex')=digest;
END;
$fn$;
ALTER TABLE public.esg_metodologias
 ALTER COLUMN especificacao_bytes SET NOT NULL,
 ADD CONSTRAINT ck_esg_metodologia_integridade_bytes CHECK
 (public.esg_validar_especificacao_bytes(especificacao_bytes,especificacao,hash_especificacao,especificacao_serializacao));
-- Existing records are checked immediately. Any unmatched historical bytes/hash
-- abort installation. No historical JSON, digest or ledger is rewritten.
