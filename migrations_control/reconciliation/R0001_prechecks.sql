-- R0001 prechecks: consultas somente leitura para o próximo clone efêmero.
-- Cada resultado deve ser anexado à prova antes de materializar novos ALTERs.

-- P00: os três alvos do subconjunto M0002 devem continuar ausentes.
SELECT
    to_regclass('public.auth_modulos') AS auth_modulos,
    to_regclass('public.auth_acoes') AS auth_acoes,
    to_regclass('public.naturezas_financeiras') AS naturezas_financeiras;

-- P01: fotografia estrutural das 21 colunas divergentes.
SELECT
    table_name,
    column_name,
    ordinal_position,
    data_type,
    character_maximum_length,
    is_nullable,
    column_default,
    is_identity,
    identity_generation
FROM information_schema.columns
WHERE table_schema = 'public'
  AND (table_name, column_name) IN (
      ('usuarios', 'id'),
      ('usuarios', 'username'),
      ('usuarios', 'password_hash'),
      ('usuarios', 'nome_completo'),
      ('usuarios', 'email'),
      ('associados', 'id'),
      ('associados', 'numero'),
      ('associados', 'nome'),
      ('associados', 'cpf'),
      ('associados', 'data_nascimento'),
      ('associados', 'telefone'),
      ('associados', 'cep'),
      ('associados', 'logradouro'),
      ('associados', 'endereco_numero'),
      ('associados', 'bairro'),
      ('associados', 'cidade'),
      ('associados', 'uf'),
      ('associados', 'data_admissao'),
      ('transacoes_financeiras', 'id'),
      ('transacoes_financeiras', 'numero_documento'),
      ('solicitacoes_alteracao', 'id')
  )
ORDER BY table_name, ordinal_position;

-- P02: nullability e comprimentos de usuarios, sem expor valores.
SELECT
    count(*) FILTER (WHERE nome_completo IS NULL) AS nome_completo_nulos,
    max(char_length(username)) AS username_maximo,
    max(char_length(password_hash)) AS password_hash_maximo,
    max(char_length(nome_completo)) AS nome_completo_maximo,
    max(char_length(email)) AS email_maximo
FROM public.usuarios;

-- P03: nullability, comprimentos e compatibilidade de UF de associados.
SELECT
    count(*) FILTER (WHERE cpf IS NULL) AS cpf_nulos,
    count(*) FILTER (WHERE data_nascimento IS NULL) AS data_nascimento_nulos,
    count(*) FILTER (WHERE telefone IS NULL) AS telefone_nulos,
    count(*) FILTER (WHERE cep IS NULL) AS cep_nulos,
    count(*) FILTER (WHERE data_admissao IS NULL) AS data_admissao_nulos,
    count(*) FILTER (
        WHERE uf IS NOT NULL AND char_length(btrim(uf)) <> 2
    ) AS uf_incompativel_char_2,
    max(char_length(numero)) AS numero_maximo,
    max(char_length(nome)) AS nome_maximo,
    max(char_length(telefone)) AS telefone_maximo,
    max(char_length(logradouro)) AS logradouro_maximo,
    max(char_length(endereco_numero)) AS endereco_numero_maximo,
    max(char_length(bairro)) AS bairro_maximo,
    max(char_length(cidade)) AS cidade_maximo
FROM public.associados;

-- P04: faixas dos identificadores candidatos a INTEGER -> BIGINT.
SELECT 'associados' AS tabela, min(id) AS minimo, max(id) AS maximo
FROM public.associados
UNION ALL
SELECT 'transacoes_financeiras', min(id), max(id)
FROM public.transacoes_financeiras
UNION ALL
SELECT 'solicitacoes_alteracao', min(id), max(id)
FROM public.solicitacoes_alteracao
ORDER BY tabela;

-- P05: tipos de todas as colunas que referenciam os IDs candidatos.
SELECT
    parent.relname AS tabela_referenciada,
    parent_col.attname AS coluna_referenciada,
    child.relname AS tabela_origem,
    child_col.attname AS coluna_origem,
    pg_catalog.format_type(parent_col.atttypid, parent_col.atttypmod) AS tipo_referenciado,
    pg_catalog.format_type(child_col.atttypid, child_col.atttypmod) AS tipo_origem,
    constraint_row.conname
FROM pg_catalog.pg_constraint AS constraint_row
JOIN pg_catalog.pg_class AS child ON child.oid = constraint_row.conrelid
JOIN pg_catalog.pg_class AS parent ON parent.oid = constraint_row.confrelid
JOIN LATERAL pg_catalog.generate_subscripts(constraint_row.conkey, 1)
    AS keys(position) ON true
JOIN pg_catalog.pg_attribute AS child_col
    ON child_col.attrelid = child.oid
   AND child_col.attnum = constraint_row.conkey[keys.position]
JOIN pg_catalog.pg_attribute AS parent_col
    ON parent_col.attrelid = parent.oid
   AND parent_col.attnum = constraint_row.confkey[keys.position]
WHERE constraint_row.contype = 'f'
  AND parent.relnamespace = 'public'::pg_catalog.regnamespace
  AND parent.relname IN (
      'associados', 'transacoes_financeiras', 'solicitacoes_alteracao'
  )
ORDER BY parent.relname, constraint_row.conname, keys.position;

-- P06: ownership e geração automática das quatro sequences divergentes.
SELECT
    sequence_row.relname AS sequence_name,
    table_row.relname AS owned_table,
    column_row.attname AS owned_column,
    dependency.deptype,
    pg_catalog.format_type(sequence_catalog.seqtypid, NULL) AS sequence_type,
    pg_catalog.pg_get_expr(default_row.adbin, default_row.adrelid, true) AS column_default
FROM pg_catalog.pg_class AS sequence_row
JOIN pg_catalog.pg_namespace AS sequence_schema
    ON sequence_schema.oid = sequence_row.relnamespace
JOIN pg_catalog.pg_sequence AS sequence_catalog
    ON sequence_catalog.seqrelid = sequence_row.oid
LEFT JOIN pg_catalog.pg_depend AS dependency
    ON dependency.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
   AND dependency.objid = sequence_row.oid
   AND dependency.objsubid = 0
   AND dependency.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
   AND dependency.refobjsubid > 0
LEFT JOIN pg_catalog.pg_class AS table_row ON table_row.oid = dependency.refobjid
LEFT JOIN pg_catalog.pg_attribute AS column_row
    ON column_row.attrelid = dependency.refobjid
   AND column_row.attnum = dependency.refobjsubid
LEFT JOIN pg_catalog.pg_attrdef AS default_row
    ON default_row.adrelid = column_row.attrelid
   AND default_row.adnum = column_row.attnum
WHERE sequence_schema.nspname = 'public'
  AND sequence_row.relname IN (
      'usuarios_id_seq',
      'associados_id_seq',
      'transacoes_financeiras_id_seq',
      'solicitacoes_alteracao_id_seq'
  )
ORDER BY sequence_row.relname;

-- P07: tamanho máximo do documento financeiro antes de VARCHAR(100) -> TEXT.
SELECT max(char_length(numero_documento)) AS numero_documento_maximo
FROM public.transacoes_financeiras;
