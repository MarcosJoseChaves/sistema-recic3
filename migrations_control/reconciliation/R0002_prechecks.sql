-- R0002 prechecks: somente leitura para reconciliar exclusivamente M0003.
-- Execução futura: restore legado -> R0001 -> P100-P107 -> R0002.

-- P100: dependência M0002 presente e alvo novo de M0003 ainda ausente.
SELECT
    to_regclass('public.usuarios') IS NOT NULL AS usuarios_presente,
    to_regclass('public.usuario_recuperacoes_senha') IS NULL AS recuperacoes_ausente,
    to_regclass('public.auth_modulos') IS NOT NULL AS auth_modulos_presente,
    to_regclass('public.auth_acoes') IS NOT NULL AS auth_acoes_presente,
    to_regclass('public.naturezas_financeiras') IS NOT NULL AS naturezas_presente;

-- P101: usuarios.username pode ampliar para TEXT e gerar chave normalizada válida e única.
WITH normalizados AS (
    SELECT lower(btrim(username)) AS valor, count(*) AS quantidade
    FROM public.usuarios
    GROUP BY lower(btrim(username))
)
SELECT
    count(*) FILTER (WHERE username IS NULL) AS username_nulos,
    count(*) FILTER (WHERE btrim(username) = '') AS username_vazios,
    count(*) FILTER (
        WHERE lower(btrim(username)) = ''
           OR lower(btrim(username)) ~ '[[:space:]]'
    ) AS username_normalizado_invalidos,
    (SELECT count(*) FROM normalizados WHERE quantidade > 1) AS username_normalizado_duplicados,
    max(char_length(username)) AS username_maximo
FROM public.usuarios;

-- P102: usuarios.password_hash pode ampliar de VARCHAR(255) para TEXT sem perda.
SELECT
    count(*) FILTER (WHERE password_hash IS NULL) AS password_hash_nulos,
    max(char_length(password_hash)) AS password_hash_maximo
FROM public.usuarios;

-- P103: usuarios.nome_completo pode ampliar para TEXT e receber NOT NULL/CHECK.
SELECT
    count(*) FILTER (WHERE nome_completo IS NULL) AS nome_completo_nulos,
    count(*) FILTER (WHERE nome_completo IS NOT NULL AND btrim(nome_completo) = '') AS nome_completo_vazios,
    max(char_length(nome_completo)) AS nome_completo_maximo
FROM public.usuarios;

-- P104: usuarios.email pode ampliar para TEXT e gerar email_normalizado sem colisões.
WITH normalizados AS (
    SELECT NULLIF(lower(btrim(email)), '') AS valor, count(*) AS quantidade
    FROM public.usuarios
    WHERE email IS NOT NULL
    GROUP BY NULLIF(lower(btrim(email)), '')
)
SELECT
    max(char_length(email)) AS email_maximo,
    (SELECT count(*) FROM normalizados WHERE valor IS NOT NULL AND quantidade > 1) AS email_normalizado_duplicados
FROM public.usuarios;

-- P105: as dez colunas novas de usuarios ainda devem estar ausentes.
SELECT count(*) AS colunas_novas_ja_presentes
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'usuarios'
  AND column_name IN (
      'username_normalizado', 'email_normalizado', 'estado',
      'exige_troca_senha', 'criado_em', 'atualizado_em',
      'versao_registro', 'criado_por_usuario_id', 'inativado_em',
      'inativado_por_usuario_id'
  );

-- P106: PK serial legada e sequence de usuarios devem permanecer funcionalmente válidas.
SELECT
    primary_key.conname AS pk_legada,
    pg_catalog.pg_get_constraintdef(primary_key.oid, true) AS pk_definicao,
    pg_catalog.pg_get_expr(default_row.adbin, default_row.adrelid, true) AS id_default,
    sequence_row.relname AS sequence_name,
    dependency.deptype AS sequence_dependency
FROM pg_catalog.pg_class AS usuarios
JOIN pg_catalog.pg_namespace AS schema_row
    ON schema_row.oid = usuarios.relnamespace
JOIN pg_catalog.pg_constraint AS primary_key
    ON primary_key.conrelid = usuarios.oid
   AND primary_key.contype = 'p'
JOIN pg_catalog.pg_attribute AS id_column
    ON id_column.attrelid = usuarios.oid
   AND id_column.attname = 'id'
LEFT JOIN pg_catalog.pg_attrdef AS default_row
    ON default_row.adrelid = usuarios.oid
   AND default_row.adnum = id_column.attnum
LEFT JOIN pg_catalog.pg_depend AS dependency
    ON dependency.refclassid = 'pg_catalog.pg_class'::pg_catalog.regclass
   AND dependency.refobjid = usuarios.oid
   AND dependency.refobjsubid = id_column.attnum
   AND dependency.classid = 'pg_catalog.pg_class'::pg_catalog.regclass
LEFT JOIN pg_catalog.pg_class AS sequence_row
    ON sequence_row.oid = dependency.objid
   AND sequence_row.relkind = 'S'
WHERE schema_row.nspname = 'public'
  AND usuarios.relname = 'usuarios';

-- P107: nomes de constraints/índices novos de usuarios não podem colidir.
SELECT
    (SELECT count(*)
       FROM pg_catalog.pg_constraint AS constraint_row
      WHERE constraint_row.connamespace = 'public'::pg_catalog.regnamespace
        AND constraint_row.conname IN (
            'ck_usuarios__estado', 'ck_usuarios__estado_preenchido',
            'ck_usuarios__nome_completo_preenchido', 'ck_usuarios__regra_263',
            'ck_usuarios__username_normalizado_preenchido',
            'ck_usuarios__username_preenchido',
            'ck_usuarios__versao_registro_positivo',
            'fk_usuarios__criado_por_usr_id',
            'fk_usuarios__inativado_por_usr_id',
            'uq_usuarios__username_normalizado'
        )) AS constraints_colidentes,
    (SELECT count(*)
       FROM pg_catalog.pg_class AS index_row
      WHERE index_row.relnamespace = 'public'::pg_catalog.regnamespace
        AND index_row.relkind = 'i'
        AND index_row.relname IN (
            'ix_usuarios__criado_por_usr_id',
            'ix_usuarios__estado_username_normalizado',
            'ix_usuarios__inativado_por_usr_id',
            'uq_usuarios__email_normalizado',
            'uq_usuarios__username_normalizado'
        )) AS indices_colidentes;
