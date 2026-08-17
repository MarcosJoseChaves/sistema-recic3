-- R0007 / M0013: P700-P713, exclusivamente leitura.

-- P700: tabela legado e os oito extras exatos.
WITH expected(name) AS (VALUES
 ('tabela_alvo'),('id_registro'),('tipo_solicitacao'),('dados_novos'),
 ('usuario_solicitante'),('data_solicitacao'),('status'),('observacoes_admin')
), actual AS (
 SELECT column_name AS name FROM information_schema.columns
 WHERE table_schema='public' AND table_name='solicitacoes_alteracao' AND column_name<>'id'
)
SELECT 'P700' AS precheck,
       (SELECT count(*) FROM expected) AS expected_count,
       (SELECT count(*) FROM actual) AS actual_count,
       (SELECT count(*) FROM expected e LEFT JOIN actual a USING(name) WHERE a.name IS NULL) AS missing,
       (SELECT count(*) FROM actual a LEFT JOIN expected e USING(name) WHERE e.name IS NULL) AS unexpected;

-- P701: id INTEGER, PK legado única e faixa int4 segura.
SELECT 'P701' AS precheck,
       pg_catalog.format_type(attribute.atttypid,attribute.atttypmod) AS id_type,
       attribute.attnotnull AS id_not_null,
       count(DISTINCT constraint_row.oid) AS pk_count,
       min(constraint_row.conname) AS pk_name,
       (SELECT count(*) FROM public.solicitacoes_alteracao WHERE id IS NULL) AS null_ids,
       (SELECT min(id) FROM public.solicitacoes_alteracao) AS min_id,
       (SELECT max(id) FROM public.solicitacoes_alteracao) AS max_id
FROM pg_catalog.pg_attribute attribute
LEFT JOIN pg_catalog.pg_constraint constraint_row
  ON constraint_row.conrelid=attribute.attrelid AND constraint_row.contype='p'
WHERE attribute.attrelid='public.solicitacoes_alteracao'::regclass
  AND attribute.attname='id'
GROUP BY attribute.atttypid,attribute.atttypmod,attribute.attnotnull;

-- P702: default/sequence INTEGER serial-owned e capacidade residual.
SELECT 'P702' AS precheck,
       pg_catalog.pg_get_expr(def.adbin,def.adrelid) AS id_default,
       sequence_view.data_type AS sequence_type,
       sequence_view.max_value,
       sequence_view.last_value,
       dependency.deptype,
       dependency.refobjid::regclass::text AS owned_table,
       attribute.attname AS owned_column
FROM pg_catalog.pg_attribute attribute
JOIN pg_catalog.pg_attrdef def ON def.adrelid=attribute.attrelid AND def.adnum=attribute.attnum
JOIN pg_catalog.pg_class sequence_class ON sequence_class.oid=to_regclass(pg_get_serial_sequence('public.solicitacoes_alteracao','id'))
JOIN pg_catalog.pg_depend dependency ON dependency.classid='pg_catalog.pg_class'::regclass AND dependency.objid=sequence_class.oid AND dependency.deptype='a'
JOIN pg_catalog.pg_attribute owned_attribute ON owned_attribute.attrelid=dependency.refobjid AND owned_attribute.attnum=dependency.refobjsubid
JOIN pg_catalog.pg_sequences sequence_view ON sequence_view.schemaname='public' AND sequence_view.sequencename=sequence_class.relname
WHERE attribute.attrelid='public.solicitacoes_alteracao'::regclass AND attribute.attname='id';

-- P703: nenhuma FK legado aponta para solicitacoes_alteracao.
SELECT 'P703' AS precheck,count(*) AS legacy_fk_count
FROM pg_catalog.pg_constraint
WHERE contype='f' AND confrelid='public.solicitacoes_alteracao'::regclass;

-- P704: as onze tabelas filhas devem estar ausentes.
WITH expected(name) AS (VALUES
 ('solicitacao_aplicacoes'),('solicitacao_aprovacoes'),('solicitacao_associacoes'),
 ('solicitacao_associados'),('solicitacao_catalogo_itens'),('solicitacao_documentos'),
 ('solicitacao_eventos'),('solicitacao_mensagens'),('solicitacao_patrimonios'),
 ('solicitacao_transacoes'),('solicitacao_uvrs')
)
SELECT 'P704' AS precheck,count(*) AS expected_count,
       count(*) FILTER(WHERE to_regclass('public.'||name) IS NOT NULL) AS collisions
FROM expected;

-- P705: somente estados terminais autorizados.
SELECT 'P705' AS precheck,count(*) AS total,
       count(*) FILTER(WHERE status IS NULL OR btrim(status)='') AS null_or_empty,
       count(*) FILTER(WHERE upper(btrim(status)) NOT IN ('APROVADO','REJEITADO')) AS unsupported,
       count(*) FILTER(WHERE upper(btrim(status))='APROVADO') AS approved,
       count(*) FILTER(WHERE upper(btrim(status))='REJEITADO') AS rejected
FROM public.solicitacoes_alteracao;

-- P706: fontes obrigatórias de backfill preenchidas.
SELECT 'P706' AS precheck,count(*) AS total,
       count(*) FILTER(WHERE tipo_solicitacao IS NULL OR btrim(tipo_solicitacao)='') AS invalid_type,
       count(*) FILTER(WHERE upper(btrim(tipo_solicitacao)) NOT IN ('EDICAO','EXCLUSAO')) AS unsupported_type,
       count(*) FILTER(WHERE tabela_alvo IS NULL OR btrim(tabela_alvo)='') AS invalid_target,
       count(*) FILTER(WHERE id_registro IS NULL) AS null_logical_id,
       count(*) FILTER(WHERE data_solicitacao IS NULL) AS null_created_at,
       count(*) FILTER(WHERE usuario_solicitante IS NULL OR btrim(usuario_solicitante)='') AS invalid_user
FROM public.solicitacoes_alteracao;

-- P707: cardinalidade exata/canônica, sem candidatos múltiplos.
WITH tokens AS (
 SELECT usuario_solicitante token,lower(btrim(usuario_solicitante)) normalized,count(*) requests
 FROM public.solicitacoes_alteracao GROUP BY usuario_solicitante,lower(btrim(usuario_solicitante))
), counts AS (
 SELECT token.*,
        (SELECT count(*) FROM public.usuarios u WHERE u.username=token.token) exact_count,
        (SELECT count(*) FROM public.usuarios u WHERE u.username_normalizado=token.normalized) canonical_count
 FROM tokens token
)
SELECT 'P707' AS precheck,
       count(*) FILTER(WHERE exact_count=1) AS exact_tokens,
       coalesce(sum(requests) FILTER(WHERE exact_count=1),0) AS exact_requests,
       count(*) FILTER(WHERE exact_count=0 AND canonical_count=1) AS canonical_tokens,
       coalesce(sum(requests) FILTER(WHERE exact_count=0 AND canonical_count=1),0) AS canonical_requests,
       count(*) FILTER(WHERE exact_count=0 AND canonical_count=0) AS actor_tokens,
       coalesce(sum(requests) FILTER(WHERE exact_count=0 AND canonical_count=0),0) AS actor_requests,
       count(*) FILTER(WHERE exact_count>1 OR (exact_count=0 AND canonical_count>1)) AS ambiguous_tokens
FROM counts;

-- P708: tokens de ator são canônicos válidos, não colidem entre si nem com contas existentes.
WITH tokens AS (
 SELECT DISTINCT usuario_solicitante token,lower(btrim(usuario_solicitante)) normalized
 FROM public.solicitacoes_alteracao
), unresolved AS (
 SELECT token.* FROM tokens token
 WHERE NOT EXISTS (SELECT 1 FROM public.usuarios u WHERE u.username=token.token)
   AND NOT EXISTS (SELECT 1 FROM public.usuarios u WHERE u.username_normalizado=token.normalized)
), collisions AS (
 SELECT normalized,count(*) quantity FROM unresolved GROUP BY normalized HAVING count(*)>1
)
SELECT 'P708' AS precheck,
       (SELECT count(*) FROM unresolved) AS actor_tokens,
       (SELECT count(*) FROM unresolved WHERE normalized IS NULL OR normalized='' OR normalized ~ '[[:space:]]') AS invalid_normalized,
       (SELECT count(*) FROM collisions) AS canonical_collisions;

-- P709: schema de usuario suporta ator bloqueado e app exige ativo=TRUE.
WITH expected(name) AS (VALUES
 ('username'),('username_normalizado'),('password_hash'),('nome_completo'),('email'),
 ('email_normalizado'),('estado'),('exige_troca_senha'),('role'),('uvr_acesso'),('ativo')
)
SELECT 'P709' AS precheck,count(*) AS expected_count,
       count(*) FILTER(WHERE column_row.column_name IS NULL) AS missing
FROM expected
LEFT JOIN information_schema.columns column_row
  ON column_row.table_schema='public' AND column_row.table_name='usuarios' AND column_row.column_name=expected.name;

-- P710: pais normativos de todas as FKs existem.
WITH expected(name) AS (VALUES
 ('usuarios'),('associacoes'),('associados'),('catalogo_itens'),('documentos_privados'),
 ('patrimonios'),('transacoes_financeiras'),('uvrs')
)
SELECT 'P710' AS precheck,count(*) AS expected_count,
       count(*) FILTER(WHERE to_regclass('public.'||name) IS NULL) AS missing
FROM expected;

-- P711: as 23 colunas baseline ainda não colidem.
WITH expected(name) AS (VALUES
 ('identificador_publico'),('tipo'),('modulo'),('objeto_tipo_logico'),('objeto_identificador_logico'),
 ('solicitante_usuario_id'),('estado'),('risco'),('versao_esperada'),('fotografia_original'),
 ('fotografia_proposta'),('fotografia_aprovada'),('fotografia_aplicada'),('formato_versao'),
 ('justificativa'),('criada_em'),('enviada_em'),('concluida_em'),('atualizado_em'),
 ('atualizado_por_usuario_id'),('versao_registro'),('request_id'),('associacao_contexto_id')
)
SELECT 'P711' AS precheck,count(*) AS expected_count,
       count(*) FILTER(WHERE column_row.column_name IS NOT NULL) AS collisions
FROM expected
LEFT JOIN information_schema.columns column_row
  ON column_row.table_schema='public' AND column_row.table_name='solicitacoes_alteracao' AND column_row.column_name=expected.name;

-- P712: UUID técnico disponível em pg_catalog sem extensão criada por R0007.
SELECT 'P712' AS precheck,
       to_regprocedure('pg_catalog.gen_random_uuid()') IS NOT NULL AS gen_random_uuid_available;

-- P713: nomes de constraints/índices materiais de solicitacoes_alteracao não colidem; PK legado é exceção explícita.
WITH expected(name) AS (VALUES
 ('uq_solicitacoes_alteracao__identificador_publico'),
 ('ck_solicitacoes_alteracao__tipo_preenchido'),('ck_solicitacoes_alteracao__modulo_preenchido'),
 ('ck_solicitacoes_alteracao__objeto_tipo_logico_preenchido'),('ck_solicitacoes_alteracao__estado_preenchido'),
 ('ck_solicitacoes_alteracao__risco_preenchido'),('ck_solicitacoes_alteracao__formato_versao_positivo'),
 ('ck_solicitacoes_alteracao__versao_registro_positivo'),('ck_solicitacoes_alteracao__estado'),
 ('fk_solicitacoes_alteracao__assoc_contexto_id'),('fk_solicitacoes_alteracao__atualizado_por_usr_id'),
 ('fk_solicitacoes_alteracao__solicitante_usr_id'),('ix_solicitacoes_alteracao__assoc_contexto_id'),
 ('ix_solicitacoes_alteracao__atualizado_por_usr_id'),('ix_solicitacoes_alteracao__solicitante_usr_id'),
 ('ix_solicitacoes_alteracao__estado_risco_criada_em'),
 ('ix_solicitacoes_alteracao__objeto_tipo_logico_objeto_d7a1161f')
)
SELECT 'P713' AS precheck,count(*) AS expected_count,
       count(*) FILTER(WHERE to_regclass('public.'||name) IS NOT NULL OR EXISTS(SELECT 1 FROM pg_catalog.pg_constraint c WHERE c.conname=name)) AS collisions
FROM expected;
