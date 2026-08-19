-- R0004 prechecks agrupados: somente SELECT/read-only para M0005-M0008.
-- Executar todos os P300+ apos R0001, R0002 e R0003; qualquer falha bloqueia R0004.

-- P300: M0005 (40 objetos) - todas as tabelas alvo devem continuar ausentes.
SELECT
    to_regclass('public.auditoria_tecnica') IS NULL AS auditoria_tecnica_ausente;

-- P301: M0005 (40 objetos) - nenhuma sequence esperada pode colidir.
SELECT count(*) AS sequences_colidentes
FROM pg_catalog.pg_class AS sequence_row
WHERE sequence_row.relnamespace = 'public'::pg_catalog.regnamespace
  AND sequence_row.relkind = 'S'
  AND sequence_row.relname = ANY (ARRAY[
        'auditoria_tecnica_id_seq'
  ]::text[]);

-- P302: M0005 (40 objetos) - constraints e indices esperados nao podem colidir.
SELECT
    (SELECT count(*)
       FROM pg_catalog.pg_constraint AS constraint_row
      WHERE constraint_row.connamespace = 'public'::pg_catalog.regnamespace
        AND constraint_row.conname = ANY (ARRAY[
            'ck_auditoria_tecnica__acao_preenchido',
            'ck_auditoria_tecnica__modulo_preenchido',
            'ck_auditoria_tecnica__origem_preenchido',
            'ck_auditoria_tecnica__regra_274',
            'ck_auditoria_tecnica__resultado_preenchido',
            'ck_auditoria_tecnica__tipo_ator_preenchido',
            'ck_auditoria_tecnica__versao_formato_positivo',
            'fk_auditoria_tecnica__usr_id',
            'pk_auditoria_tecnica'
        ]::text[])) AS constraints_colidentes,
    (SELECT count(*)
       FROM pg_catalog.pg_class AS index_row
      WHERE index_row.relnamespace = 'public'::pg_catalog.regnamespace
        AND index_row.relkind = 'i'
        AND index_row.relname = ANY (ARRAY[
            'ix_auditoria_tecnica__assoc_id',
            'ix_auditoria_tecnica__objeto_tipo_objeto_identificad_b1794c8b',
            'ix_auditoria_tecnica__request_id',
            'ix_auditoria_tecnica__tipo_ator_usr_id_ocorrido_em',
            'ix_auditoria_tecnica__usr_id',
            'ix_auditoria_tecnica__uvr_id',
            'pk_auditoria_tecnica'
        ]::text[])) AS indices_colidentes;

-- P303: M0005 (40 objetos) - pais externos devem existir, ter PK em id e tipo compativel.
WITH esperados(tabela, tipo_esperado) AS (
    VALUES
        ('usuarios', 'integer')
)
SELECT
    esperados.tabela,
    parent.oid IS NOT NULL AS tabela_presente,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint AS pk
        WHERE pk.conrelid = parent.oid
          AND pk.contype = 'p'
          AND pk.conkey = ARRAY[id_column.attnum]::smallint[]
    ) AS pk_id_presente,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) AS tipo_atual,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) = esperados.tipo_esperado AS tipo_compativel
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

-- P304: M0005 (40 objetos) - fotografia catalogal deve confirmar ausencia integral, nao parcial.
WITH alvos(tabela) AS (
    VALUES
        ('auditoria_tecnica')
), estado AS (
    SELECT
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind IN ('r', 'p') AND c.relname IN (SELECT tabela FROM alvos)) AS tabelas_presentes,
        (SELECT count(*) FROM information_schema.columns AS c
          WHERE c.table_schema = 'public' AND c.table_name IN (SELECT tabela FROM alvos)) AS colunas_presentes,
        (SELECT count(*) FROM pg_catalog.pg_constraint AS c
          JOIN pg_catalog.pg_class AS t ON t.oid = c.conrelid
          WHERE t.relnamespace = 'public'::pg_catalog.regnamespace
            AND t.relname IN (SELECT tabela FROM alvos)) AS constraints_presentes,
        (SELECT count(*) FROM pg_catalog.pg_index AS i
          JOIN pg_catalog.pg_class AS t ON t.oid = i.indrelid
          WHERE t.relnamespace = 'public'::pg_catalog.regnamespace
            AND t.relname IN (SELECT tabela FROM alvos)) AS indices_presentes,
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind = 'S'
            AND c.relname = ANY (ARRAY[
                'auditoria_tecnica_id_seq'
            ]::text[])) AS sequences_presentes
)
SELECT
    40 AS objetos_esperados,
    tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes AS objetos_presentes,
    40 - (tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes) AS objetos_ausentes,
    (tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes) = 0 AS bloco_integralmente_ausente
FROM estado;

-- P320: M0006 (169 objetos) - todas as tabelas alvo devem continuar ausentes.
SELECT
    to_regclass('public.associacao_aliases') IS NULL AS associacao_aliases_ausente,
    to_regclass('public.associacao_eventos') IS NULL AS associacao_eventos_ausente,
    to_regclass('public.associacoes') IS NULL AS associacoes_ausente,
    to_regclass('public.auditoria_tecnica') IS NULL AS auditoria_tecnica_ausente,
    to_regclass('public.uvr_aliases') IS NULL AS uvr_aliases_ausente,
    to_regclass('public.uvr_eventos') IS NULL AS uvr_eventos_ausente,
    to_regclass('public.uvrs') IS NULL AS uvrs_ausente;

-- P321: M0006 (169 objetos) - nenhuma sequence esperada pode colidir.
SELECT count(*) AS sequences_colidentes
FROM pg_catalog.pg_class AS sequence_row
WHERE sequence_row.relnamespace = 'public'::pg_catalog.regnamespace
  AND sequence_row.relkind = 'S'
  AND sequence_row.relname = ANY (ARRAY[
        'associacao_aliases_id_seq',
        'associacao_eventos_id_seq',
        'associacoes_id_seq',
        'uvr_aliases_id_seq',
        'uvr_eventos_id_seq',
        'uvrs_id_seq'
  ]::text[]);

-- P322: M0006 (169 objetos) - constraints e indices esperados nao podem colidir.
SELECT
    (SELECT count(*)
       FROM pg_catalog.pg_constraint AS constraint_row
      WHERE constraint_row.connamespace = 'public'::pg_catalog.regnamespace
        AND constraint_row.conname = ANY (ARRAY[
            'ck_assoc_aliases__alias_normalizado_preenchido',
            'ck_assoc_aliases__alias_preenchido',
            'ck_assoc_aliases__estado',
            'ck_assoc_aliases__estado_preenchido',
            'ck_assoc_aliases__origem_preenchido',
            'ck_assoc_aliases__periodo',
            'ck_assoc_eventos__tipo_evento_preenchido',
            'ck_assoc_eventos__versao_formato_positivo',
            'ck_associacoes__codigo_preenchido',
            'ck_associacoes__estado',
            'ck_associacoes__estado_preenchido',
            'ck_associacoes__nome_normalizado_preenchido',
            'ck_associacoes__nome_preenchido',
            'ck_associacoes__periodo',
            'ck_associacoes__versao_registro_positivo',
            'ck_uvr_aliases__alias_normalizado_preenchido',
            'ck_uvr_aliases__alias_preenchido',
            'ck_uvr_aliases__estado',
            'ck_uvr_aliases__estado_preenchido',
            'ck_uvr_aliases__origem_preenchido',
            'ck_uvr_aliases__periodo',
            'ck_uvr_eventos__tipo_evento_preenchido',
            'ck_uvr_eventos__versao_formato_positivo',
            'ck_uvrs__codigo_preenchido',
            'ck_uvrs__estado',
            'ck_uvrs__estado_preenchido',
            'ck_uvrs__nome_normalizado_preenchido',
            'ck_uvrs__nome_preenchido',
            'ck_uvrs__periodo',
            'ck_uvrs__versao_registro_positivo',
            'fk_assoc_aliases__assoc_id',
            'fk_assoc_aliases__criado_por_usr_id',
            'fk_assoc_eventos__assoc_id',
            'fk_assoc_eventos__criado_por_usr_id',
            'fk_associacoes__atualizado_por_usr_id',
            'fk_associacoes__criado_por_usr_id',
            'fk_auditoria_tecnica__assoc_id',
            'fk_auditoria_tecnica__uvr_id',
            'fk_uvr_aliases__criado_por_usr_id',
            'fk_uvr_aliases__uvr_id',
            'fk_uvr_eventos__criado_por_usr_id',
            'fk_uvr_eventos__uvr_id',
            'fk_uvrs__assoc_id',
            'fk_uvrs__atualizado_por_usr_id',
            'fk_uvrs__criado_por_usr_id',
            'fk_uvrs__responsavel_usr_id',
            'pk_assoc_aliases',
            'pk_assoc_eventos',
            'pk_associacoes',
            'pk_uvr_aliases',
            'pk_uvr_eventos',
            'pk_uvrs',
            'uq_assoc_aliases__alias_normalizado',
            'uq_associacoes__codigo',
            'uq_associacoes__nome_normalizado',
            'uq_uvr_aliases__alias_normalizado',
            'uq_uvrs__assoc_id_nome_normalizado',
            'uq_uvrs__codigo'
        ]::text[])) AS constraints_colidentes,
    (SELECT count(*)
       FROM pg_catalog.pg_class AS index_row
      WHERE index_row.relnamespace = 'public'::pg_catalog.regnamespace
        AND index_row.relkind = 'i'
        AND index_row.relname = ANY (ARRAY[
            'ix_assoc_aliases__assoc_id',
            'ix_assoc_aliases__criado_por_usr_id',
            'ix_assoc_eventos__assoc_id',
            'ix_assoc_eventos__criado_por_usr_id',
            'ix_associacoes__atualizado_por_usr_id',
            'ix_associacoes__criado_por_usr_id',
            'ix_associacoes__estado_nome_normalizado',
            'ix_uvr_aliases__criado_por_usr_id',
            'ix_uvr_aliases__uvr_id',
            'ix_uvr_eventos__criado_por_usr_id',
            'ix_uvr_eventos__uvr_id',
            'ix_uvrs__assoc_id_estado_nome_normalizado',
            'ix_uvrs__atualizado_por_usr_id',
            'ix_uvrs__criado_por_usr_id',
            'ix_uvrs__responsavel_usr_id',
            'pk_assoc_aliases',
            'pk_assoc_eventos',
            'pk_associacoes',
            'pk_uvr_aliases',
            'pk_uvr_eventos',
            'pk_uvrs',
            'uq_assoc_aliases__alias_normalizado',
            'uq_associacoes__codigo',
            'uq_associacoes__nome_normalizado',
            'uq_uvr_aliases__alias_normalizado',
            'uq_uvrs__assoc_id_nome_normalizado',
            'uq_uvrs__codigo'
        ]::text[])) AS indices_colidentes;

-- P323: M0006 (169 objetos) - pais externos devem existir, ter PK em id e tipo compativel.
WITH esperados(tabela, tipo_esperado) AS (
    VALUES
        ('usuarios', 'integer')
)
SELECT
    esperados.tabela,
    parent.oid IS NOT NULL AS tabela_presente,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint AS pk
        WHERE pk.conrelid = parent.oid
          AND pk.contype = 'p'
          AND pk.conkey = ARRAY[id_column.attnum]::smallint[]
    ) AS pk_id_presente,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) AS tipo_atual,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) = esperados.tipo_esperado AS tipo_compativel
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

-- P324: M0006 (169 objetos) - fotografia catalogal deve confirmar ausencia integral, nao parcial.
WITH alvos(tabela) AS (
    VALUES
        ('associacao_aliases'),
        ('associacao_eventos'),
        ('associacoes'),
        ('auditoria_tecnica'),
        ('uvr_aliases'),
        ('uvr_eventos'),
        ('uvrs')
), estado AS (
    SELECT
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind IN ('r', 'p') AND c.relname IN (SELECT tabela FROM alvos)) AS tabelas_presentes,
        (SELECT count(*) FROM information_schema.columns AS c
          WHERE c.table_schema = 'public' AND c.table_name IN (SELECT tabela FROM alvos)) AS colunas_presentes,
        (SELECT count(*) FROM pg_catalog.pg_constraint AS c
          JOIN pg_catalog.pg_class AS t ON t.oid = c.conrelid
          WHERE t.relnamespace = 'public'::pg_catalog.regnamespace
            AND t.relname IN (SELECT tabela FROM alvos)) AS constraints_presentes,
        (SELECT count(*) FROM pg_catalog.pg_index AS i
          JOIN pg_catalog.pg_class AS t ON t.oid = i.indrelid
          WHERE t.relnamespace = 'public'::pg_catalog.regnamespace
            AND t.relname IN (SELECT tabela FROM alvos)) AS indices_presentes,
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind = 'S'
            AND c.relname = ANY (ARRAY[
                'associacao_aliases_id_seq',
                'associacao_eventos_id_seq',
                'associacoes_id_seq',
                'uvr_aliases_id_seq',
                'uvr_eventos_id_seq',
                'uvrs_id_seq'
            ]::text[])) AS sequences_presentes
)
SELECT
    169 AS objetos_esperados,
    tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes AS objetos_presentes,
    169 - (tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes) AS objetos_ausentes,
    (tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes) = 0 AS bloco_integralmente_ausente
FROM estado;

-- P340: M0007 (54 objetos) - todas as tabelas alvo devem continuar ausentes.
SELECT
    to_regclass('public.auth_escopos_associacao') IS NULL AS auth_escopos_associacao_ausente,
    to_regclass('public.auth_escopos_globais') IS NULL AS auth_escopos_globais_ausente,
    to_regclass('public.auth_escopos_uvr') IS NULL AS auth_escopos_uvr_ausente;

-- P341: M0007 (54 objetos) - nenhuma sequence esperada pode colidir.
SELECT count(*) AS sequences_colidentes
FROM pg_catalog.pg_class AS sequence_row
WHERE sequence_row.relnamespace = 'public'::pg_catalog.regnamespace
  AND sequence_row.relkind = 'S'
  AND sequence_row.relname = ANY (ARRAY[
        'auth_escopos_associacao_id_seq',
        'auth_escopos_globais_id_seq',
        'auth_escopos_uvr_id_seq'
  ]::text[]);

-- P342: M0007 (54 objetos) - constraints e indices esperados nao podem colidir.
SELECT
    (SELECT count(*)
       FROM pg_catalog.pg_constraint AS constraint_row
      WHERE constraint_row.connamespace = 'public'::pg_catalog.regnamespace
        AND constraint_row.conname = ANY (ARRAY[
            'ck_auth_escopos_assoc__periodo',
            'ck_auth_escopos_globais__periodo',
            'ck_auth_escopos_uvr__periodo',
            'fk_auth_escopos_assoc__assoc_id',
            'fk_auth_escopos_assoc__criado_por_usr_id',
            'fk_auth_escopos_assoc__usr_perfil_id',
            'fk_auth_escopos_globais__criado_por_usr_id',
            'fk_auth_escopos_globais__usr_perfil_id',
            'fk_auth_escopos_uvr__criado_por_usr_id',
            'fk_auth_escopos_uvr__usr_perfil_id',
            'fk_auth_escopos_uvr__uvr_id',
            'pk_auth_escopos_associacao',
            'pk_auth_escopos_globais',
            'pk_auth_escopos_uvr'
        ]::text[])) AS constraints_colidentes,
    (SELECT count(*)
       FROM pg_catalog.pg_class AS index_row
      WHERE index_row.relnamespace = 'public'::pg_catalog.regnamespace
        AND index_row.relkind = 'i'
        AND index_row.relname = ANY (ARRAY[
            'ix_auth_escopos_assoc__assoc_id',
            'ix_auth_escopos_assoc__criado_por_usr_id',
            'ix_auth_escopos_globais__criado_por_usr_id',
            'ix_auth_escopos_uvr__criado_por_usr_id',
            'ix_auth_escopos_uvr__uvr_id',
            'pk_auth_escopos_associacao',
            'pk_auth_escopos_globais',
            'pk_auth_escopos_uvr',
            'uq_auth_escopos_assoc__usr_perfil_id_assoc_id',
            'uq_auth_escopos_globais__usr_perfil_id',
            'uq_auth_escopos_uvr__usr_perfil_id_uvr_id'
        ]::text[])) AS indices_colidentes;

-- P343: M0007 (54 objetos) - pais externos devem existir, ter PK em id e tipo compativel.
WITH esperados(tabela, tipo_esperado) AS (
    VALUES
        ('usuarios', 'integer'),
        ('auth_usuario_perfis', 'bigint')
)
SELECT
    esperados.tabela,
    parent.oid IS NOT NULL AS tabela_presente,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint AS pk
        WHERE pk.conrelid = parent.oid
          AND pk.contype = 'p'
          AND pk.conkey = ARRAY[id_column.attnum]::smallint[]
    ) AS pk_id_presente,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) AS tipo_atual,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) = esperados.tipo_esperado AS tipo_compativel
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

-- P344: M0007 (54 objetos) - fotografia catalogal deve confirmar ausencia integral, nao parcial.
WITH alvos(tabela) AS (
    VALUES
        ('auth_escopos_associacao'),
        ('auth_escopos_globais'),
        ('auth_escopos_uvr')
), estado AS (
    SELECT
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind IN ('r', 'p') AND c.relname IN (SELECT tabela FROM alvos)) AS tabelas_presentes,
        (SELECT count(*) FROM information_schema.columns AS c
          WHERE c.table_schema = 'public' AND c.table_name IN (SELECT tabela FROM alvos)) AS colunas_presentes,
        (SELECT count(*) FROM pg_catalog.pg_constraint AS c
          JOIN pg_catalog.pg_class AS t ON t.oid = c.conrelid
          WHERE t.relnamespace = 'public'::pg_catalog.regnamespace
            AND t.relname IN (SELECT tabela FROM alvos)) AS constraints_presentes,
        (SELECT count(*) FROM pg_catalog.pg_index AS i
          JOIN pg_catalog.pg_class AS t ON t.oid = i.indrelid
          WHERE t.relnamespace = 'public'::pg_catalog.regnamespace
            AND t.relname IN (SELECT tabela FROM alvos)) AS indices_presentes,
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind = 'S'
            AND c.relname = ANY (ARRAY[
                'auth_escopos_associacao_id_seq',
                'auth_escopos_globais_id_seq',
                'auth_escopos_uvr_id_seq'
            ]::text[])) AS sequences_presentes
)
SELECT
    54 AS objetos_esperados,
    tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes AS objetos_presentes,
    54 - (tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes) AS objetos_ausentes,
    (tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes) = 0 AS bloco_integralmente_ausente
FROM estado;

-- P360: M0008 (44 objetos) - todas as tabelas alvo devem continuar ausentes.
SELECT
    to_regclass('public.documentos_privados') IS NULL AS documentos_privados_ausente;

-- P361: M0008 (44 objetos) - nenhuma sequence esperada pode colidir.
SELECT count(*) AS sequences_colidentes
FROM pg_catalog.pg_class AS sequence_row
WHERE sequence_row.relnamespace = 'public'::pg_catalog.regnamespace
  AND sequence_row.relkind = 'S'
  AND sequence_row.relname = ANY (ARRAY[
        'documentos_privados_id_seq'
  ]::text[]);

-- P362: M0008 (44 objetos) - constraints e indices esperados nao podem colidir.
SELECT
    (SELECT count(*)
       FROM pg_catalog.pg_constraint AS constraint_row
      WHERE constraint_row.connamespace = 'public'::pg_catalog.regnamespace
        AND constraint_row.conname = ANY (ARRAY[
            'ck_documentos_privados__chave_provedor_preenchido',
            'ck_documentos_privados__estado',
            'ck_documentos_privados__estado_preenchido',
            'ck_documentos_privados__extensao_preenchido',
            'ck_documentos_privados__mime_type_preenchido',
            'ck_documentos_privados__nome_original_preenchido',
            'ck_documentos_privados__nome_seguro_preenchido',
            'ck_documentos_privados__privacidade_preenchido',
            'ck_documentos_privados__provedor_preenchido',
            'ck_documentos_privados__regra_268',
            'ck_documentos_privados__versao_positivo',
            'fk_documentos_privados__criado_por_usr_id',
            'fk_documentos_privados__substituido_por_id',
            'pk_documentos_privados',
            'uq_documentos_privados__chave_provedor',
            'uq_documentos_privados__identificador_publico'
        ]::text[])) AS constraints_colidentes,
    (SELECT count(*)
       FROM pg_catalog.pg_class AS index_row
      WHERE index_row.relnamespace = 'public'::pg_catalog.regnamespace
        AND index_row.relkind = 'i'
        AND index_row.relname = ANY (ARRAY[
            'ix_documentos_privados__criado_por_usr_id',
            'ix_documentos_privados__estado_retencao_ate',
            'ix_documentos_privados__sha256',
            'ix_documentos_privados__substituido_por_id',
            'pk_documentos_privados',
            'uq_documentos_privados__chave_provedor',
            'uq_documentos_privados__identificador_publico'
        ]::text[])) AS indices_colidentes;

-- P363: M0008 (44 objetos) - pais externos devem existir, ter PK em id e tipo compativel.
WITH esperados(tabela, tipo_esperado) AS (
    VALUES
        ('usuarios', 'integer')
)
SELECT
    esperados.tabela,
    parent.oid IS NOT NULL AS tabela_presente,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint AS pk
        WHERE pk.conrelid = parent.oid
          AND pk.contype = 'p'
          AND pk.conkey = ARRAY[id_column.attnum]::smallint[]
    ) AS pk_id_presente,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) AS tipo_atual,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) = esperados.tipo_esperado AS tipo_compativel
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

-- P364: M0008 (44 objetos) - fotografia catalogal deve confirmar ausencia integral, nao parcial.
WITH alvos(tabela) AS (
    VALUES
        ('documentos_privados')
), estado AS (
    SELECT
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind IN ('r', 'p') AND c.relname IN (SELECT tabela FROM alvos)) AS tabelas_presentes,
        (SELECT count(*) FROM information_schema.columns AS c
          WHERE c.table_schema = 'public' AND c.table_name IN (SELECT tabela FROM alvos)) AS colunas_presentes,
        (SELECT count(*) FROM pg_catalog.pg_constraint AS c
          JOIN pg_catalog.pg_class AS t ON t.oid = c.conrelid
          WHERE t.relnamespace = 'public'::pg_catalog.regnamespace
            AND t.relname IN (SELECT tabela FROM alvos)) AS constraints_presentes,
        (SELECT count(*) FROM pg_catalog.pg_index AS i
          JOIN pg_catalog.pg_class AS t ON t.oid = i.indrelid
          WHERE t.relnamespace = 'public'::pg_catalog.regnamespace
            AND t.relname IN (SELECT tabela FROM alvos)) AS indices_presentes,
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind = 'S'
            AND c.relname = ANY (ARRAY[
                'documentos_privados_id_seq'
            ]::text[])) AS sequences_presentes
)
SELECT
    44 AS objetos_esperados,
    tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes AS objetos_presentes,
    44 - (tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes) AS objetos_ausentes,
    (tabelas_presentes + colunas_presentes + constraints_presentes + indices_presentes + sequences_presentes) = 0 AS bloco_integralmente_ausente
FROM estado;
