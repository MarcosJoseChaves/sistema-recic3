-- R0003 prechecks: somente leitura para os 99 objetos ausentes de M0004.
-- Execução futura: R0001 -> R0002 -> P200-P207 -> R0003.

-- P200: as quatro tabelas M0004 devem continuar integralmente ausentes.
SELECT
    to_regclass('public.auth_permissoes') IS NULL AS auth_permissoes_ausente,
    to_regclass('public.auth_perfis') IS NULL AS auth_perfis_ausente,
    to_regclass('public.auth_perfil_permissoes') IS NULL AS auth_perfil_permissoes_ausente,
    to_regclass('public.auth_usuario_perfis') IS NULL AS auth_usuario_perfis_ausente;

-- P201: as quatro sequences identity de M0004 não podem colidir.
SELECT count(*) AS sequences_colidentes
FROM pg_catalog.pg_class AS sequence_row
WHERE sequence_row.relnamespace = 'public'::pg_catalog.regnamespace
  AND sequence_row.relkind = 'S'
  AND sequence_row.relname IN (
      'auth_permissoes_id_seq',
      'auth_perfis_id_seq',
      'auth_perfil_permissoes_id_seq',
      'auth_usuario_perfis_id_seq'
  );

-- P202: nenhum nome de constraint ou índice esperado por M0004 pode colidir.
SELECT
    (SELECT count(*)
       FROM pg_catalog.pg_constraint AS constraint_row
      WHERE constraint_row.connamespace = 'public'::pg_catalog.regnamespace
        AND constraint_row.conname = ANY (ARRAY[
            'fk_auth_perfil_permissoes__criado_por_usr_id',
            'fk_auth_perfil_permissoes__perfil_id',
            'fk_auth_perfil_permissoes__permissao_id',
            'pk_auth_perfil_permissoes',
            'uq_auth_perfil_permissoes__perfil_id_permissao_id',
            'ck_auth_perfis__codigo_preenchido',
            'ck_auth_perfis__escopo_tipo_preenchido',
            'ck_auth_perfis__estado',
            'ck_auth_perfis__estado_preenchido',
            'ck_auth_perfis__nome_preenchido',
            'ck_auth_perfis__versao_registro_positivo',
            'fk_auth_perfis__atualizado_por_usr_id',
            'fk_auth_perfis__criado_por_usr_id',
            'pk_auth_perfis',
            'uq_auth_perfis__codigo',
            'ck_auth_permissoes__codigo_preenchido',
            'ck_auth_permissoes__estado',
            'ck_auth_permissoes__estado_preenchido',
            'ck_auth_permissoes__versao_registro_positivo',
            'fk_auth_permissoes__acao_id',
            'fk_auth_permissoes__modulo_id',
            'pk_auth_permissoes',
            'uq_auth_permissoes__codigo',
            'uq_auth_permissoes__modulo_id_acao_id',
            'ck_auth_usr_perfis__estado',
            'ck_auth_usr_perfis__estado_preenchido',
            'ck_auth_usr_perfis__periodo',
            'ck_auth_usr_perfis__versao_registro_positivo',
            'fk_auth_usr_perfis__concedido_por_usr_id',
            'fk_auth_usr_perfis__perfil_id',
            'fk_auth_usr_perfis__revogado_por_usr_id',
            'fk_auth_usr_perfis__usr_id',
            'pk_auth_usr_perfis'
        ]::text[])) AS constraints_colidentes,
    (SELECT count(*)
       FROM pg_catalog.pg_class AS index_row
      WHERE index_row.relnamespace = 'public'::pg_catalog.regnamespace
        AND index_row.relkind = 'i'
        AND index_row.relname = ANY (ARRAY[
            'ix_auth_perfil_permissoes__criado_por_usr_id',
            'ix_auth_perfil_permissoes__permissao_id',
            'pk_auth_perfil_permissoes',
            'uq_auth_perfil_permissoes__perfil_id_permissao_id',
            'ix_auth_perfis__atualizado_por_usr_id',
            'ix_auth_perfis__criado_por_usr_id',
            'pk_auth_perfis',
            'uq_auth_perfis__codigo',
            'ix_auth_permissoes__acao_id',
            'pk_auth_permissoes',
            'uq_auth_permissoes__codigo',
            'uq_auth_permissoes__modulo_id_acao_id',
            'ix_auth_usr_perfis__concedido_por_usr_id',
            'ix_auth_usr_perfis__perfil_id',
            'ix_auth_usr_perfis__revogado_por_usr_id',
            'pk_auth_usr_perfis',
            'uq_auth_usr_perfis__usr_id_perfil_id'
        ]::text[])) AS indices_colidentes;

-- P203: as três tabelas pais externas devem existir e possuir PK funcional em id.
WITH pais(tabela) AS (
    VALUES ('usuarios'), ('auth_modulos'), ('auth_acoes')
)
SELECT
    pais.tabela,
    parent.oid IS NOT NULL AS tabela_presente,
    EXISTS (
        SELECT 1
        FROM pg_catalog.pg_constraint AS pk
        WHERE pk.conrelid = parent.oid
          AND pk.contype = 'p'
          AND pk.conkey = ARRAY[id_column.attnum]::smallint[]
    ) AS pk_id_presente
FROM pais
LEFT JOIN pg_catalog.pg_class AS parent
    ON parent.relnamespace = 'public'::pg_catalog.regnamespace
   AND parent.relname = pais.tabela
   AND parent.relkind IN ('r', 'p')
LEFT JOIN pg_catalog.pg_attribute AS id_column
    ON id_column.attrelid = parent.oid
   AND id_column.attname = 'id'
   AND id_column.attnum > 0
   AND NOT id_column.attisdropped
ORDER BY pais.tabela;

-- P204: tipos das PKs pais devem ser compatíveis com todas as FKs de M0004.
WITH esperados(tabela, tipo_esperado) AS (
    VALUES
        ('usuarios', 'integer'),
        ('auth_modulos', 'bigint'),
        ('auth_acoes', 'bigint')
)
SELECT
    esperados.tabela,
    esperados.tipo_esperado,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) AS tipo_atual,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) = esperados.tipo_esperado
        AS tipo_compativel
FROM esperados
LEFT JOIN pg_catalog.pg_class AS parent
    ON parent.relnamespace = 'public'::pg_catalog.regnamespace
   AND parent.relname = esperados.tabela
   AND parent.relkind IN ('r', 'p')
LEFT JOIN pg_catalog.pg_attribute AS id_column
    ON id_column.attrelid = parent.oid
   AND id_column.attname = 'id'
   AND id_column.attnum > 0
   AND NOT id_column.attisdropped
ORDER BY esperados.tabela;

-- P205: fotografia agrupada comprova os 99 objetos ainda ausentes nos alvos M0004.
WITH alvos(tabela) AS (
    VALUES
        ('auth_permissoes'),
        ('auth_perfis'),
        ('auth_perfil_permissoes'),
        ('auth_usuario_perfis')
), estado AS (
    SELECT
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind IN ('r', 'p') AND c.relname IN (SELECT tabela FROM alvos)) AS tabelas_presentes,
        (SELECT count(*) FROM information_schema.columns AS c
          WHERE c.table_schema = 'public' AND c.table_name IN (SELECT tabela FROM alvos)) AS colunas_presentes,
        (SELECT count(*) FROM pg_catalog.pg_constraint AS x
          JOIN pg_catalog.pg_class AS c ON c.oid = x.conrelid
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relname IN (SELECT tabela FROM alvos)) AS constraints_presentes,
        (SELECT count(*) FROM pg_catalog.pg_index AS x
          JOIN pg_catalog.pg_class AS c ON c.oid = x.indrelid
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relname IN (SELECT tabela FROM alvos)) AS indices_presentes,
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind = 'S'
            AND c.relname IN (
                'auth_permissoes_id_seq', 'auth_perfis_id_seq',
                'auth_perfil_permissoes_id_seq', 'auth_usuario_perfis_id_seq'
            )) AS sequences_presentes
)
SELECT
    99 AS objetos_esperados,
    tabelas_presentes + colunas_presentes + constraints_presentes
        + indices_presentes + sequences_presentes AS objetos_presentes,
    99 - (tabelas_presentes + colunas_presentes + constraints_presentes
        + indices_presentes + sequences_presentes) AS objetos_ausentes
FROM estado;

-- P206: não deve haver tabela alternativa/partial com prefixo de domínio M0004.
SELECT count(*) AS tabelas_alternativas
FROM pg_catalog.pg_class AS table_row
WHERE table_row.relnamespace = 'public'::pg_catalog.regnamespace
  AND table_row.relkind IN ('r', 'p')
  AND (
      table_row.relname LIKE 'auth_permiss%'
      OR table_row.relname LIKE 'auth_perfi%'
      OR table_row.relname LIKE 'auth_usuario_perfi%'
  );

-- P207: guardas pós-R0002 exigidas antes de qualquer objeto M0004.
SELECT
    to_regclass('public.auth_modulos') IS NOT NULL AS auth_modulos_presente,
    to_regclass('public.auth_acoes') IS NOT NULL AS auth_acoes_presente,
    to_regclass('public.naturezas_financeiras') IS NOT NULL AS naturezas_presente,
    to_regclass('public.usuarios') IS NOT NULL AS usuarios_presente,
    to_regclass('public.usuario_recuperacoes_senha') IS NOT NULL AS recuperacoes_presente,
    EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'usuarios'
          AND column_name = 'username_normalizado' AND data_type = 'text'
          AND is_nullable = 'NO'
    ) AS usuarios_reconciliado;
