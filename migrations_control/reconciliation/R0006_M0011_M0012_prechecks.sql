-- R0006 prechecks read-only. P500-P549: M0011; P550-P579: M0012.

-- P500: deve existir exatamente um usuario tecnico seguro produzido por R0005.
SELECT count(*) AS candidatos,
       count(*) FILTER (WHERE username = 'migracao_dados_legados'
                          AND username_normalizado = 'migracao_dados_legados'
                          AND estado = 'BLOQUEADO' AND ativo = FALSE) AS seguros
FROM public.usuarios
WHERE username = 'migracao_dados_legados'
   OR username_normalizado = 'migracao_dados_legados';

-- P501: IDs existentes devem permanecer dentro de int4.
SELECT count(*) AS fora_int4, min(id) AS menor_id, max(id) AS maior_id
FROM public.transacoes_financeiras
WHERE id < -2147483648 OR id > 2147483647;

-- P502: sequence serial deve estar owned by id, gerar automaticamente e ter capacidade residual.
WITH seq AS (
    SELECT s.seqtypid::pg_catalog.regtype::text AS tipo,
           s.seqmax AS maximo,
           pg_catalog.pg_get_serial_sequence('public.transacoes_financeiras', 'id') AS nome
    FROM pg_catalog.pg_sequence AS s
    JOIN pg_catalog.pg_class AS c ON c.oid = s.seqrelid
    WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
      AND c.relname = 'transacoes_financeiras_id_seq'
), dados AS (
    SELECT COALESCE(max(id), 0) AS maior_id FROM public.transacoes_financeiras
)
SELECT seq.*, dados.maior_id, seq.maximo - dados.maior_id AS capacidade_residual
FROM seq CROSS JOIN dados;

-- P503: quatro FKs EXTRA_LEGADO devem permanecer int4 e apontar para a chave legada.
WITH esperadas(tabela, coluna, constraint_nome) AS (
    VALUES
      ('auditoria_rateios_transacoes','id_transacao','auditoria_rateios_transacoes_id_transacao_fkey'),
      ('documentos','id_transacao_origem','documentos_id_transacao_origem_fkey'),
      ('fluxo_caixa_transacoes_link','id_transacao_financeira','fluxo_caixa_transacoes_link_id_transacao_financeira_fkey'),
      ('itens_transacao','id_transacao','itens_transacao_id_transacao_fkey')
)
SELECT e.*, a.atttypid::pg_catalog.regtype::text AS tipo,
       c.confrelid = 'public.transacoes_financeiras'::pg_catalog.regclass AS alvo_correto
FROM esperadas AS e
LEFT JOIN pg_catalog.pg_class AS t
  ON t.relnamespace = 'public'::pg_catalog.regnamespace AND t.relname = e.tabela
LEFT JOIN pg_catalog.pg_attribute AS a
  ON a.attrelid = t.oid AND a.attname = e.coluna AND NOT a.attisdropped
LEFT JOIN pg_catalog.pg_constraint AS c
  ON c.conrelid = t.oid AND c.conname = e.constraint_nome AND c.contype = 'f'
ORDER BY e.tabela;

-- P504: numero_documento deve ser varchar(100) ou ja text, sem dado a converter.
SELECT data_type, character_maximum_length,
       data_type = 'text' OR (data_type = 'character varying' AND character_maximum_length = 100) AS seguro
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'transacoes_financeiras'
  AND column_name = 'numero_documento';

-- P505: UUID deve ser provido pela funcao nativa pg_catalog.gen_random_uuid(), sem extensao.
WITH f AS (
    SELECT p.oid, p.prorettype::pg_catalog.regtype::text AS retorno,
           pg_catalog.pg_function_is_visible(p.oid) AS visivel
    FROM pg_catalog.pg_proc AS p
    JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
    WHERE n.nspname = 'pg_catalog' AND p.proname = 'gen_random_uuid'
      AND p.pronargs = 0
)
SELECT count(*) AS funcoes, min(retorno) AS retorno,
       count(*) FILTER (WHERE NOT EXISTS (
           SELECT 1 FROM pg_catalog.pg_depend AS d
           WHERE d.objid = f.oid AND d.deptype = 'e'
       )) AS nativas
FROM f;

-- P506: plano deterministico REUSE/CREATE por identidade legada token<->UVR.
WITH legacy_pairs AS (
    SELECT lower(btrim(tf.associacao)) AS token_normalizado,
           btrim(tf.associacao) AS sigla_historica,
           lower(btrim(tf.uvr)) AS uvr_legada,
           count(*) AS quantidade_transacoes
      FROM public.transacoes_financeiras AS tf
     GROUP BY 1, 2, 3
), legacy_identity AS (
    SELECT token_normalizado,
           count(*) AS pares_legacy,
           count(DISTINCT sigla_historica) AS siglas_historicas,
           count(DISTINCT uvr_legada) AS uvrs_legacy,
           sum(quantidade_transacoes) AS quantidade_transacoes
      FROM legacy_pairs
     GROUP BY token_normalizado
), uvr_identity AS (
    SELECT uvr_legada, count(DISTINCT token_normalizado) AS tokens_por_uvr
      FROM legacy_pairs
     GROUP BY uvr_legada
), canonical_candidates AS (
    SELECT li.token_normalizado, a.id AS associacao_id
      FROM legacy_identity AS li
      JOIN public.associacoes AS a
        ON lower(btrim(a.codigo)) = li.token_normalizado
    UNION
    SELECT li.token_normalizado, a.id AS associacao_id
      FROM legacy_identity AS li
      JOIN public.associacoes AS a
        ON a.nome_normalizado = li.token_normalizado
    UNION
    SELECT li.token_normalizado, aa.associacao_id
      FROM legacy_identity AS li
      JOIN public.associacao_aliases AS aa
        ON aa.alias_normalizado = li.token_normalizado
), candidate_counts AS (
    SELECT token_normalizado, count(*) AS candidate_count
      FROM canonical_candidates
     GROUP BY token_normalizado
), divergent_codes AS (
    SELECT DISTINCT li.token_normalizado, a.id AS associacao_id
      FROM legacy_identity AS li
      JOIN public.associacoes AS a
        ON a.nome_normalizado = li.token_normalizado
        OR EXISTS (
            SELECT 1 FROM public.associacao_aliases AS aa
             WHERE aa.associacao_id = a.id
               AND aa.alias_normalizado = li.token_normalizado
        )
     WHERE a.codigo IS NOT NULL
       AND btrim(a.codigo) <> ''
       AND lower(btrim(a.codigo)) <> li.token_normalizado
), plano AS (
    SELECT li.token_normalizado,
           li.quantidade_transacoes,
           li.siglas_historicas,
           li.uvrs_legacy,
           COALESCE(ui.tokens_por_uvr, 0) AS tokens_por_uvr,
           COALESCE(cc.candidate_count, 0) AS candidate_count,
           count(dc.associacao_id) AS codigos_divergentes
      FROM legacy_identity AS li
      LEFT JOIN legacy_pairs AS lp
        ON lp.token_normalizado = li.token_normalizado
       AND li.pares_legacy = 1
      LEFT JOIN uvr_identity AS ui ON ui.uvr_legada = lp.uvr_legada
      LEFT JOIN candidate_counts AS cc ON cc.token_normalizado = li.token_normalizado
      LEFT JOIN divergent_codes AS dc ON dc.token_normalizado = li.token_normalizado
     GROUP BY li.token_normalizado, li.quantidade_transacoes, li.siglas_historicas,
              li.uvrs_legacy, ui.tokens_por_uvr, cc.candidate_count
)
SELECT count(*) FILTER (
           WHERE token_normalizado IS NULL OR token_normalizado = ''
              OR siglas_historicas <> 1 OR uvrs_legacy <> 1 OR tokens_por_uvr <> 1
              OR candidate_count > 1 OR codigos_divergentes > 0
       ) AS sem_plano,
       count(*) FILTER (WHERE candidate_count > 1) AS ambiguas,
       count(*) FILTER (
           WHERE candidate_count = 0 AND token_normalizado <> ''
             AND siglas_historicas = 1 AND uvrs_legacy = 1 AND tokens_por_uvr = 1
       ) AS criar,
       count(*) FILTER (WHERE candidate_count = 1 AND codigos_divergentes = 0) AS reutilizar,
       COALESCE(sum(quantidade_transacoes) FILTER (
           WHERE candidate_count IN (0, 1) AND token_normalizado <> ''
             AND siglas_historicas = 1 AND uvrs_legacy = 1 AND tokens_por_uvr = 1
             AND codigos_divergentes = 0
       ), 0) AS transacoes_com_plano
FROM plano;

-- P507: tipo_transacao deve fechar somente em RECEITA/DESPESA.
SELECT upper(public.unaccent(btrim(tipo_transacao))) AS valor_normalizado, count(*) AS linhas
FROM public.transacoes_financeiras
GROUP BY 1
HAVING upper(public.unaccent(btrim(tipo_transacao))) NOT IN ('RECEITA','DESPESA')
    OR upper(public.unaccent(btrim(tipo_transacao))) IS NULL;

-- P508: seeds existentes devem ser ausentes ou exatamente um registro canonico seguro.
WITH codigos(codigo) AS (VALUES ('RECEITA'), ('DESPESA'))
SELECT c.codigo,
       count(n.id) AS candidatos,
       count(n.id) FILTER (WHERE n.codigo = c.codigo AND n.nome = c.codigo
                             AND n.nome_normalizado = lower(c.codigo)
                             AND n.estado = 'ATIVO') AS seguros
FROM codigos AS c
LEFT JOIN public.naturezas_financeiras AS n
  ON n.codigo = c.codigo OR n.nome_normalizado = lower(c.codigo)
GROUP BY c.codigo ORDER BY c.codigo;

-- P509: fotografia completa dos campos obrigatorios/defaults de naturezas_financeiras.
SELECT column_name, data_type, is_nullable, column_default
FROM information_schema.columns
WHERE table_schema = 'public' AND table_name = 'naturezas_financeiras'
ORDER BY ordinal_position;

-- P510: fotografia completa dos 17 campos reais de contas_financeiras antes da criacao.
WITH esperados(ordem,nome,tipo,nullable,classe) AS (
    VALUES
      (1,'id','bigint','NO','A'),(2,'associacao_id','bigint','NO','E'),
      (3,'codigo','text','NO','A'),(4,'nome','text','NO','A'),
      (5,'tipo','text','NO','A'),(6,'instituicao','text','YES','B'),
      (7,'agencia','text','YES','B'),(8,'conta','text','YES','B'),
      (9,'estado','text','NO','C'),(10,'abertura_data','date','YES','C'),
      (11,'encerramento_data','date','YES','C'),(12,'observacoes','text','NO','D'),
      (13,'criado_em','timestamp with time zone','NO','D'),
      (14,'atualizado_em','timestamp with time zone','NO','D'),
      (15,'criado_por_usuario_id','integer','NO','D'),
      (16,'atualizado_por_usuario_id','integer','NO','D'),
      (17,'versao_registro','integer','NO','F')
)
SELECT * FROM esperados ORDER BY ordem;

-- P511: contrato materializado da conta tecnica; campos bancarios/datas sao NULL, sem placeholders.
SELECT 'MIGRACAO_LEGADO'::text AS tipo,
       NULL::text AS instituicao, NULL::text AS agencia, NULL::text AS conta,
       NULL::date AS abertura_data, NULL::date AS encerramento_data,
       'INATIVA'::text AS estado,
       ARRAY['N/A','0','0000','MIGRACAO','']::text[] AS placeholders_proibidos;

-- P512: a identidade logica usa a UNIQUE normativa associacao_id+codigo.
SELECT ARRAY['associacao_id','codigo']::text[] AS chave_logica,
       'MIGRACAO_LEGADO'::text AS codigo_tecnico,
       1::integer AS maximo_por_associacao;

-- P513: contrato negativo: conta normal conserva banco preenchido e ambas as datas obrigatorias.
SELECT '(tipo <> MIGRACAO_LEGADO) => banco NOT NULL/preenchido e datas NOT NULL'::text
       AS regra_compensatoria_conta_normal;

-- P514: data_documento deve existir em todas as linhas para a convencao de competencia.
SELECT count(*) AS datas_nulas FROM public.transacoes_financeiras WHERE data_documento IS NULL;

-- P515: valor_total_documento deve caber exatamente no dominio NUMERIC(18,2) nao negativo.
SELECT count(*) AS valores_invalidos
FROM public.transacoes_financeiras
WHERE valor_total_documento IS NULL OR valor_total_documento < 0
   OR valor_total_documento > 9999999999999999.99
   OR valor_total_documento <> round(valor_total_documento, 2);

-- P516: os seis campos da whitelist existem nominalmente no legado.
WITH whitelist(nome) AS (VALUES ('id'),('numero_documento'),('data_documento'),
                                ('tipo_transacao'),('valor_total_documento'),('associacao'))
SELECT w.nome, c.column_name IS NOT NULL AS existe
FROM whitelist AS w
LEFT JOIN information_schema.columns AS c
  ON c.table_schema='public' AND c.table_name='transacoes_financeiras' AND c.column_name=w.nome
ORDER BY w.nome;

-- P517: nenhuma das nove colunas de backfill pode existir em estado parcial.
WITH novas(nome) AS (VALUES ('identificador_publico'),('associacao_id'),('natureza_id'),
    ('conta_financeira_id'),('competencia_data'),('valor_total'),('fotografia'),
    ('criado_por_usuario_id'),('atualizado_por_usuario_id'))
SELECT count(*) AS colunas_parciais
FROM information_schema.columns
WHERE table_schema='public' AND table_name='transacoes_financeiras'
  AND column_name IN (SELECT nome FROM novas);

-- P518: pais M0011 devem estar presentes apos R0001-R0005.
WITH pais(nome) AS (VALUES ('associacoes'),('associados'),('usuarios'),('uvrs'),
                           ('catalogo_itens'),('naturezas_financeiras'),('associacao_aliases'))
SELECT p.nome, pg_catalog.to_regclass('public.' || p.nome) IS NOT NULL AS presente
FROM pais AS p ORDER BY p.nome;

-- P519: nomes das quatro tabelas novas M0011 e suas sequences nao podem colidir.
SELECT relkind, relname
FROM pg_catalog.pg_class
WHERE relnamespace='public'::pg_catalog.regnamespace
  AND relname = ANY (ARRAY['contas_financeiras','transacao_itens','transacao_rateios_uvr',
      'transacao_eventos','contas_financeiras_id_seq','transacao_itens_id_seq',
      'transacao_rateios_uvr_id_seq','transacao_eventos_id_seq']);

-- P520: resumo atomico das nove fontes de backfill.
SELECT count(*) AS linhas,
       count(*) FILTER (WHERE data_documento IS NULL) AS sem_competencia,
       count(*) FILTER (WHERE valor_total_documento IS NULL OR valor_total_documento < 0) AS sem_valor,
       count(*) FILTER (WHERE associacao IS NULL OR btrim(associacao)='') AS sem_associacao,
       count(*) FILTER (WHERE tipo_transacao IS NULL OR btrim(tipo_transacao)='') AS sem_natureza
FROM public.transacoes_financeiras;

-- P550: as seis tabelas M0012 devem estar integralmente ausentes.
SELECT relkind, relname
FROM pg_catalog.pg_class
WHERE relnamespace='public'::pg_catalog.regnamespace
  AND relname = ANY (ARRAY['patrimonios','patrimonio_identificadores','patrimonio_vinculos',
      'patrimonio_eventos','patrimonio_documentos','patrimonio_bloqueios']);

-- P551: as seis sequences M0012 nao podem colidir.
SELECT relname
FROM pg_catalog.pg_class
WHERE relnamespace='public'::pg_catalog.regnamespace AND relkind='S'
  AND relname = ANY (ARRAY['patrimonios_id_seq','patrimonio_identificadores_id_seq',
      'patrimonio_vinculos_id_seq','patrimonio_eventos_id_seq',
      'patrimonio_documentos_id_seq','patrimonio_bloqueios_id_seq']);

-- P552: dependencias externas M0012 devem existir antes da transacao R0006.
WITH pais(nome) AS (VALUES ('associacoes'),('usuarios'),('uvrs'),('documentos_privados'))
SELECT p.nome, pg_catalog.to_regclass('public.' || p.nome) IS NOT NULL AS presente
FROM pais AS p ORDER BY p.nome;

-- P553: fotografia catalogal confirma M0012 totalmente ausente, alvo futuro 193 objetos.
WITH alvos(nome) AS (VALUES ('patrimonios'),('patrimonio_identificadores'),('patrimonio_vinculos'),
    ('patrimonio_eventos'),('patrimonio_documentos'),('patrimonio_bloqueios'))
SELECT 193 AS objetos_esperados,
       count(*) AS tabelas_presentes
FROM pg_catalog.pg_class
WHERE relnamespace='public'::pg_catalog.regnamespace
  AND relkind IN ('r','p') AND relname IN (SELECT nome FROM alvos);
