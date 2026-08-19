-- R0005 prechecks: somente SELECT/read-only para M0009-M0010 e backfill legado.
-- Executar apos R0001-R0004; qualquer resultado fora do esperado bloqueia R0005.

-- P400: dependencias reconciliadas M0002-M0008 e alvos M0009/M0010.
SELECT
    to_regclass('public.usuarios') IS NOT NULL AS usuarios_presente,
    to_regclass('public.auth_usuario_perfis') IS NOT NULL AS auth_usuario_perfis_presente,
    to_regclass('public.associacoes') IS NOT NULL AS associacoes_presente,
    to_regclass('public.uvrs') IS NOT NULL AS uvrs_presente,
    to_regclass('public.documentos_privados') IS NOT NULL AS documentos_privados_presente,
    to_regclass('public.associados') IS NOT NULL AS associados_presente,
    to_regclass('public.associado_associacao_vinculos') IS NULL AS primeiro_alvo_m0009_ausente,
    to_regclass('public.unidades_medida') IS NULL AS primeiro_alvo_m0010_ausente;

-- P401: tipos ampliados, textos obrigatorios e unicidade de numero (B: numero/nome/telefone/logradouro/endereco_numero/bairro/cidade).
WITH numeros AS (
    SELECT numero, count(*) AS quantidade
    FROM public.associados
    GROUP BY numero
)
SELECT
    count(*) FILTER (WHERE numero IS NULL OR btrim(numero) = '') AS numero_invalidos,
    count(*) FILTER (WHERE nome IS NULL OR btrim(nome) = '') AS nome_invalidos,
    count(*) FILTER (WHERE telefone IS NULL) AS telefone_nulos_atuais,
    max(char_length(numero)) AS numero_maximo,
    max(char_length(nome)) AS nome_maximo,
    max(char_length(telefone)) AS telefone_maximo,
    max(char_length(logradouro)) AS logradouro_maximo,
    max(char_length(endereco_numero)) AS endereco_numero_maximo,
    max(char_length(bairro)) AS bairro_maximo,
    max(char_length(cidade)) AS cidade_maximo,
    (SELECT count(*) FROM numeros WHERE quantidade > 1) AS numeros_duplicados
FROM public.associados;

-- P402: CPF atual deve continuar valido para UNIQUE parcial e ck_associados__regra_265 (B: cpf).
SELECT
    count(*) FILTER (WHERE cpf IS NULL) AS cpf_nulos_atuais,
    count(*) FILTER (WHERE cpf IS NOT NULL AND cpf !~ '^[0-9]{11}$') AS cpf_formato_invalido,
    count(*) - count(DISTINCT cpf) FILTER (WHERE cpf IS NOT NULL) AS cpf_duplicados
FROM public.associados;

-- P403: relaxamentos de nullability nao convertem dados (B: data_nascimento/cep/data_admissao).
SELECT
    count(*) FILTER (WHERE data_nascimento IS NULL) AS data_nascimento_nulos_atuais,
    count(*) FILTER (WHERE cep IS NULL) AS cep_nulos_atuais,
    count(*) FILTER (WHERE data_admissao IS NULL) AS data_admissao_nulos_atuais,
    count(*) FILTER (WHERE cep IS NOT NULL AND char_length(cep) > 8) AS cep_longos,
    count(*) FILTER (WHERE data_nascimento IS NOT NULL AND data_admissao IS NOT NULL AND data_admissao < data_nascimento) AS datas_incoerentes
FROM public.associados;

-- P404: conversao VARCHAR(2) para CHAR(2) sem truncamento (B: uf).
SELECT
    count(*) FILTER (WHERE uf IS NOT NULL AND char_length(uf) > 2) AS uf_longas,
    count(*) FILTER (WHERE uf IS NOT NULL AND char_length(btrim(uf)) <> 2) AS uf_tamanho_invalido,
    count(*) FILTER (WHERE uf IS NOT NULL AND btrim(uf) !~ '^[A-Za-z]{2}$') AS uf_formato_invalido
FROM public.associados;

-- P405: classifica unaccent em E1/E2/E3 por catalogo e privilegios; nao instala nem invoca a funcao.
WITH contexto AS (
    SELECT
        current_user AS usuario_atual,
        current_database() AS banco_atual,
        to_regnamespace('public') AS public_oid,
        to_regprocedure('public.unaccent(text)') AS funcao_public_text_oid,
        to_regprocedure('unaccent(text)') AS funcao_resolvida_oid
), extensao AS (
    SELECT extension_row.oid, extension_row.extnamespace
    FROM pg_catalog.pg_extension AS extension_row
    WHERE extension_row.extname = 'unaccent'
), disponibilidade AS (
    SELECT
        available.default_version,
        version_row.trusted,
        version_row.superuser
    FROM pg_catalog.pg_available_extensions AS available
    LEFT JOIN pg_catalog.pg_available_extension_versions AS version_row
      ON version_row.name = available.name
     AND version_row.version = available.default_version
    WHERE available.name = 'unaccent'
), funcao AS (
    SELECT
        procedure_row.oid,
        procedure_row.pronamespace,
        procedure_row.prorettype = 'text'::pg_catalog.regtype AS retorna_text,
        pg_catalog.has_function_privilege(
            contexto.usuario_atual,
            procedure_row.oid,
            'EXECUTE'
        ) AS pode_executar,
        EXISTS (
            SELECT 1
            FROM pg_catalog.pg_depend AS dependency
            JOIN extensao ON extensao.oid = dependency.refobjid
            WHERE dependency.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
              AND dependency.objid = procedure_row.oid
              AND dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
              AND dependency.deptype = 'e'
        ) AS pertence_extensao
    FROM contexto
    JOIN pg_catalog.pg_proc AS procedure_row
      ON procedure_row.oid = contexto.funcao_public_text_oid
), colisoes AS (
    SELECT
        (SELECT count(*)
           FROM pg_catalog.pg_proc AS procedure_row
          WHERE procedure_row.proname = 'unaccent'
            AND procedure_row.pronamespace = contexto.public_oid
            AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_depend AS dependency
                JOIN extensao ON extensao.oid = dependency.refobjid
                WHERE dependency.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                  AND dependency.objid = procedure_row.oid
                  AND dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                  AND dependency.deptype = 'e'
            )) AS funcoes_estranhas,
        (SELECT count(*)
           FROM pg_catalog.pg_ts_dict AS dictionary_row
          WHERE dictionary_row.dictname = 'unaccent'
            AND dictionary_row.dictnamespace = contexto.public_oid
            AND NOT EXISTS (
                SELECT 1
                FROM pg_catalog.pg_depend AS dependency
                JOIN extensao ON extensao.oid = dependency.refobjid
                WHERE dependency.classid = 'pg_catalog.pg_ts_dict'::pg_catalog.regclass
                  AND dependency.objid = dictionary_row.oid
                  AND dependency.refclassid = 'pg_catalog.pg_extension'::pg_catalog.regclass
                  AND dependency.deptype = 'e'
            )) AS dicionarios_estranhos
    FROM contexto
), estado AS (
    SELECT
        contexto.*,
        (SELECT count(*) FROM extensao) AS extensoes_instaladas,
        (SELECT extnamespace FROM extensao LIMIT 1) AS extensao_schema_oid,
        (SELECT count(*) FROM disponibilidade) AS versoes_disponiveis,
        COALESCE((SELECT trusted FROM disponibilidade LIMIT 1), FALSE) AS versao_trusted,
        COALESCE((SELECT superuser FROM disponibilidade LIMIT 1), TRUE) AS versao_exige_superuser,
        COALESCE((SELECT retorna_text FROM funcao LIMIT 1), FALSE) AS funcao_retorna_text,
        COALESCE((SELECT pode_executar FROM funcao LIMIT 1), FALSE) AS funcao_executavel,
        COALESCE((SELECT pertence_extensao FROM funcao LIMIT 1), FALSE) AS funcao_da_extensao,
        colisoes.funcoes_estranhas,
        colisoes.dicionarios_estranhos,
        pg_catalog.has_database_privilege(
            contexto.usuario_atual,
            contexto.banco_atual,
            'CREATE'
        ) AS pode_criar_no_banco,
        CASE WHEN contexto.public_oid IS NULL THEN FALSE ELSE
            pg_catalog.has_schema_privilege(
                contexto.usuario_atual,
                contexto.public_oid,
                'CREATE'
            )
        END AS pode_criar_no_schema,
        COALESCE((
            SELECT role_row.rolsuper
            FROM pg_catalog.pg_roles AS role_row
            WHERE role_row.rolname = contexto.usuario_atual
        ), FALSE) AS usuario_superuser
    FROM contexto
    CROSS JOIN colisoes
)
SELECT
    extensoes_instaladas,
    versoes_disponiveis,
    public_oid IS NOT NULL AS schema_public_existe,
    funcao_public_text_oid IS NOT NULL AS funcao_public_text_existe,
    funcao_resolvida_oid = funcao_public_text_oid AS resolucao_search_path_correta,
    funcao_retorna_text,
    funcao_executavel,
    funcao_da_extensao,
    funcoes_estranhas,
    dicionarios_estranhos,
    pode_criar_no_banco,
    pode_criar_no_schema,
    versao_trusted,
    versao_exige_superuser,
    usuario_superuser,
    CASE
        WHEN extensoes_instaladas = 1
         AND extensao_schema_oid = public_oid
         AND funcao_public_text_oid IS NOT NULL
         AND funcao_resolvida_oid = funcao_public_text_oid
         AND funcao_retorna_text
         AND funcao_executavel
         AND funcao_da_extensao
         AND funcoes_estranhas = 0
         AND dicionarios_estranhos = 0
            THEN 'E1_INSTALADA_FUNCIONAL'
        WHEN extensoes_instaladas = 0
         AND versoes_disponiveis = 1
         AND public_oid IS NOT NULL
         AND funcao_resolvida_oid IS NULL
         AND pode_criar_no_banco
         AND pode_criar_no_schema
         AND (versao_trusted OR usuario_superuser)
         AND funcoes_estranhas = 0
         AND dicionarios_estranhos = 0
            THEN 'E2_AUSENTE_INSTALAVEL'
        ELSE 'E3_BLOQUEIO'
    END AS classificacao_unaccent
FROM estado;

-- P406: apos E1/E2, valida a regra normativa e a ausencia das 12 colunas novas de associados.
WITH normalizacao AS (
    SELECT
        count(*) FILTER (WHERE nome IS NULL OR btrim(nome) = '') AS nomes_sem_origem,
        count(*) FILTER (
            WHERE nome IS NOT NULL
              AND btrim(nome) <> ''
              AND lower(unaccent(btrim(nome))) = ''
        ) AS normalizacoes_vazias,
        count(*) FILTER (
            WHERE nome IS NOT NULL
              AND lower(unaccent(btrim(nome))) IS NULL
        ) AS normalizacoes_nulas
    FROM public.associados
), colunas AS (
    SELECT count(*) AS colunas_novas_ja_presentes
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'associados'
      AND column_name IN (
          'nome_normalizado', 'documento_alternativo', 'justificativa_sem_cpf',
          'email', 'estado', 'condicao_regularizacao', 'data_desligamento',
          'criado_em', 'atualizado_em', 'criado_por_usuario_id',
          'atualizado_por_usuario_id', 'versao_registro'
      )
)
SELECT normalizacao.*, colunas.colunas_novas_ja_presentes
FROM normalizacao
CROSS JOIN colunas;

-- P407: schema real de usuarios deve suportar identidade tecnica bloqueada e legado preservado.
WITH esperadas(nome, tipo, nula) AS (
    VALUES
        ('id', 'integer', 'NO'),
        ('username', 'text', 'NO'),
        ('username_normalizado', 'text', 'NO'),
        ('password_hash', 'text', 'NO'),
        ('nome_completo', 'text', 'NO'),
        ('email', 'text', 'YES'),
        ('email_normalizado', 'text', 'YES'),
        ('estado', 'text', 'NO'),
        ('exige_troca_senha', 'boolean', 'NO'),
        ('role', 'character varying', 'NO'),
        ('uvr_acesso', 'character varying', 'YES'),
        ('ativo', 'boolean', 'YES')
)
SELECT
    count(*) AS colunas_confirmadas,
    count(*) FILTER (WHERE coluna.column_name IS NULL) AS colunas_ausentes,
    count(*) FILTER (WHERE coluna.column_name IS NOT NULL AND (coluna.data_type <> esperadas.tipo OR coluna.is_nullable <> esperadas.nula)) AS colunas_incompativeis
FROM esperadas
LEFT JOIN information_schema.columns AS coluna
  ON coluna.table_schema = 'public'
 AND coluna.table_name = 'usuarios'
 AND coluna.column_name = esperadas.nome;

-- P408: U1 reutiliza, U2 cria um, U3 colisao insegura e U4 multiplos; somente U1/U2 passam.
WITH candidatos AS (
    SELECT usuario.*,
           NOT EXISTS (
               SELECT 1 FROM public.auth_usuario_perfis AS vinculo
               WHERE vinculo.usuario_id = usuario.id
           ) AS sem_perfil
    FROM public.usuarios AS usuario
    WHERE usuario.username = 'migracao_dados_legados'
       OR usuario.username_normalizado = 'migracao_dados_legados'
), classificados AS (
    SELECT *,
           username = 'migracao_dados_legados'
       AND username_normalizado = 'migracao_dados_legados'
       AND nome_completo = 'Migração de dados legados'
       AND password_hash = 'pbkdf2:sha256:1000000$0caPhVgPLfRuxvRAGu8srw$80dab8fa229f014098e1b004ed61a1065b6f324a2c2dc30ec2d01c5f9351097c'
       AND email IS NULL
       AND email_normalizado IS NULL
       AND estado = 'BLOQUEADO'
       AND exige_troca_senha = TRUE
       AND ativo = FALSE
       AND role = 'migracao'
       AND uvr_acesso IS NULL
       AND sem_perfil AS seguro
    FROM candidatos
)
SELECT
    count(*) AS candidatos,
    count(*) FILTER (WHERE seguro) AS candidatos_seguros,
    CASE
        WHEN count(*) = 0 THEN 'U2_CRIAR'
        WHEN count(*) = 1 AND count(*) FILTER (WHERE seguro) = 1 THEN 'U1_REUTILIZAR'
        WHEN count(*) > 1 THEN 'U4_FAIL'
        ELSE 'U3_FAIL'
    END AS classificacao
FROM classificados;

-- P409: identidade canonica nao pode colidir por username normalizado nem receber perfil/permissao.
SELECT
    count(*) FILTER (WHERE username = 'migracao_dados_legados') AS username_literal,
    count(*) FILTER (WHERE username_normalizado = 'migracao_dados_legados') AS username_normalizado,
    count(*) FILTER (
        WHERE id IN (
            SELECT usuario_id FROM public.auth_usuario_perfis
        )
          AND (username = 'migracao_dados_legados' OR username_normalizado = 'migracao_dados_legados')
    ) AS candidatos_com_perfil
FROM public.usuarios;

-- P410: as cinco FKs EXTRA_LEGADO devem existir, permanecer validas e INTEGER -> INTEGER.
WITH esperadas(tabela, coluna, constraint_name) AS (
    VALUES
        ('auditoria_associados', 'id_associado', 'auditoria_associados_id_associado_fkey'),
        ('auditoria_rateios', 'id_associado', 'auditoria_rateios_id_associado_fkey'),
        ('auditoria_rateios_transacoes', 'id_associado', 'auditoria_rateios_transacoes_id_associado_fkey'),
        ('epi_entregas', 'id_associado', 'epi_entregas_id_associado_fkey'),
        ('epi_entregas', 'id_responsavel', 'epi_entregas_id_responsavel_fkey')
)
SELECT
    esperadas.tabela,
    esperadas.coluna,
    esperadas.constraint_name,
    constraint_row.oid IS NOT NULL AS fk_presente,
    constraint_row.convalidated AS fk_validada,
    pg_catalog.format_type(source_column.atttypid, source_column.atttypmod) AS tipo_origem,
    pg_catalog.format_type(target_column.atttypid, target_column.atttypmod) AS tipo_associados_id
FROM esperadas
LEFT JOIN pg_catalog.pg_class AS source_table
  ON source_table.relnamespace = 'public'::pg_catalog.regnamespace
 AND source_table.relname = esperadas.tabela
LEFT JOIN pg_catalog.pg_attribute AS source_column
  ON source_column.attrelid = source_table.oid
 AND source_column.attname = esperadas.coluna
LEFT JOIN pg_catalog.pg_constraint AS constraint_row
  ON constraint_row.conrelid = source_table.oid
 AND constraint_row.conname = esperadas.constraint_name
 AND constraint_row.contype = 'f'
LEFT JOIN pg_catalog.pg_class AS target_table
  ON target_table.relnamespace = 'public'::pg_catalog.regnamespace
 AND target_table.relname = 'associados'
LEFT JOIN pg_catalog.pg_attribute AS target_column
  ON target_column.attrelid = target_table.oid
 AND target_column.attname = 'id'
ORDER BY esperadas.tabela, esperadas.coluna;

-- P411: cinco tabelas novas M0009 devem estar ausentes.
SELECT count(*) AS tabelas_m0009_presentes
FROM pg_catalog.pg_class
WHERE relnamespace = 'public'::pg_catalog.regnamespace
  AND relkind IN ('r', 'p')
  AND relname = ANY (ARRAY[
      'associado_associacao_vinculos', 'associado_conta_documentos',
      'associado_contas_bancarias', 'associado_eventos', 'associado_uvr_vinculos'
  ]::text[]);

-- P412: cinco sequences novas M0009 nao podem colidir; sequence legada e INTEGER sao preservados.
SELECT
    count(*) FILTER (WHERE sequence_row.relname <> 'associados_id_seq') AS sequences_novas_colidentes,
    count(*) FILTER (WHERE sequence_row.relname = 'associados_id_seq') AS sequence_legada_presente
FROM pg_catalog.pg_class AS sequence_row
WHERE sequence_row.relnamespace = 'public'::pg_catalog.regnamespace
  AND sequence_row.relkind = 'S'
  AND sequence_row.relname = ANY (ARRAY[
      'associados_id_seq', 'associado_associacao_vinculos_id_seq',
      'associado_conta_documentos_id_seq', 'associado_contas_bancarias_id_seq',
      'associado_eventos_id_seq', 'associado_uvr_vinculos_id_seq'
  ]::text[]);

-- P413: PK/serial legadas de associados permanecem funcionais e IDs cabem em INTEGER.
SELECT
    min(id) AS id_minimo,
    max(id) AS id_maximo,
    count(*) FILTER (WHERE id < -2147483648::bigint OR id > 2147483647::bigint) AS ids_fora_integer,
    pg_catalog.format_type(id_column.atttypid, id_column.atttypmod) AS tipo_id,
    pg_catalog.pg_get_expr(default_row.adbin, default_row.adrelid, true) AS id_default,
    primary_key.conname AS pk_legada
FROM public.associados
CROSS JOIN pg_catalog.pg_class AS associados_table
JOIN pg_catalog.pg_attribute AS id_column
  ON id_column.attrelid = associados_table.oid AND id_column.attname = 'id'
LEFT JOIN pg_catalog.pg_attrdef AS default_row
  ON default_row.adrelid = associados_table.oid AND default_row.adnum = id_column.attnum
LEFT JOIN pg_catalog.pg_constraint AS primary_key
  ON primary_key.conrelid = associados_table.oid AND primary_key.contype = 'p'
WHERE associados_table.relnamespace = 'public'::pg_catalog.regnamespace
  AND associados_table.relname = 'associados'
GROUP BY id_column.atttypid, id_column.atttypmod, default_row.adbin, default_row.adrelid, primary_key.conname;

-- P414: pais externos usados por M0009 devem existir com PK funcional.
WITH pais(tabela) AS (
    VALUES ('usuarios'), ('associacoes'), ('uvrs'), ('documentos_privados')
)
SELECT
    pais.tabela,
    parent.oid IS NOT NULL AS tabela_presente,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint AS pk
        WHERE pk.conrelid = parent.oid AND pk.contype = 'p'
    ) AS pk_presente
FROM pais
LEFT JOIN pg_catalog.pg_class AS parent
  ON parent.relnamespace = 'public'::pg_catalog.regnamespace
 AND parent.relname = pais.tabela
 AND parent.relkind IN ('r', 'p')
ORDER BY pais.tabela;

-- P415: nenhuma linha de associados pode perder integridade durante o backfill autorizado.
SELECT
    count(*) AS associados_antes,
    count(*) FILTER (WHERE nome IS NULL OR btrim(nome) = '') AS sem_nome_para_backfill,
    count(*) FILTER (WHERE id IS NULL) AS sem_pk,
    count(DISTINCT id) AS ids_distintos
FROM public.associados;

-- P416: nomes normativos de constraints/indices M0009 nao podem colidir.
SELECT
    (SELECT count(*)
       FROM pg_catalog.pg_constraint AS constraint_row
      WHERE constraint_row.connamespace = 'public'::pg_catalog.regnamespace
        AND constraint_row.conname ~ '^(ck|fk|pk|uq)_associad') AS constraints_colidentes,
    (SELECT count(*)
       FROM pg_catalog.pg_class AS index_row
      WHERE index_row.relnamespace = 'public'::pg_catalog.regnamespace
        AND index_row.relkind = 'i'
        AND index_row.relname ~ '^(ix|pk|uq)_associad') AS indices_colidentes;

-- P450: as sete tabelas M0010 devem estar integralmente ausentes.
SELECT count(*) AS tabelas_m0010_presentes
FROM pg_catalog.pg_class
WHERE relnamespace = 'public'::pg_catalog.regnamespace
  AND relkind IN ('r', 'p')
  AND relname = ANY (ARRAY[
      'unidades_medida', 'catalogo_grupos', 'catalogo_subgrupos',
      'catalogo_itens', 'catalogo_aliases', 'catalogo_substituicoes',
      'catalogo_eventos'
  ]::text[]);

-- P451: as sete sequences M0010 nao podem colidir.
SELECT count(*) AS sequences_m0010_colidentes
FROM pg_catalog.pg_class
WHERE relnamespace = 'public'::pg_catalog.regnamespace
  AND relkind = 'S'
  AND relname = ANY (ARRAY[
      'unidades_medida_id_seq', 'catalogo_grupos_id_seq',
      'catalogo_subgrupos_id_seq', 'catalogo_itens_id_seq',
      'catalogo_aliases_id_seq', 'catalogo_substituicoes_id_seq',
      'catalogo_eventos_id_seq'
  ]::text[]);

-- P452: dependencias externas de M0010 devem estar satisfeitas apos M0009.
WITH pais(tabela) AS (
    VALUES ('usuarios'), ('associados')
)
SELECT
    pais.tabela,
    parent.oid IS NOT NULL AS tabela_presente,
    EXISTS (
        SELECT 1 FROM pg_catalog.pg_constraint AS pk
        WHERE pk.conrelid = parent.oid AND pk.contype = 'p'
    ) AS pk_presente
FROM pais
LEFT JOIN pg_catalog.pg_class AS parent
  ON parent.relnamespace = 'public'::pg_catalog.regnamespace
 AND parent.relname = pais.tabela
 AND parent.relkind IN ('r', 'p')
ORDER BY pais.tabela;

-- P453: fotografia catalogal M0010 deve confirmar 221 objetos ausentes pelo metodo aprovado.
WITH alvos(tabela) AS (
    VALUES
        ('unidades_medida'), ('catalogo_grupos'), ('catalogo_subgrupos'),
        ('catalogo_itens'), ('catalogo_aliases'), ('catalogo_substituicoes'),
        ('catalogo_eventos')
), estado AS (
    SELECT
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind IN ('r', 'p') AND c.relname IN (SELECT tabela FROM alvos)) AS tabelas,
        (SELECT count(*) FROM information_schema.columns AS c
          WHERE c.table_schema = 'public' AND c.table_name IN (SELECT tabela FROM alvos)) AS colunas,
        (SELECT count(*) FROM pg_catalog.pg_constraint AS c
          JOIN pg_catalog.pg_class AS t ON t.oid = c.conrelid
          WHERE t.relnamespace = 'public'::pg_catalog.regnamespace
            AND t.relname IN (SELECT tabela FROM alvos)) AS constraints,
        (SELECT count(*) FROM pg_catalog.pg_index AS i
          JOIN pg_catalog.pg_class AS t ON t.oid = i.indrelid
          WHERE t.relnamespace = 'public'::pg_catalog.regnamespace
            AND t.relname IN (SELECT tabela FROM alvos)) AS indices,
        (SELECT count(*) FROM pg_catalog.pg_class AS c
          WHERE c.relnamespace = 'public'::pg_catalog.regnamespace
            AND c.relkind = 'S'
            AND c.relname = ANY (ARRAY[
                'unidades_medida_id_seq', 'catalogo_grupos_id_seq',
                'catalogo_subgrupos_id_seq', 'catalogo_itens_id_seq',
                'catalogo_aliases_id_seq', 'catalogo_substituicoes_id_seq',
                'catalogo_eventos_id_seq'
            ]::text[])) AS sequences
)
SELECT
    221 AS objetos_esperados,
    tabelas + colunas + constraints + indices + sequences AS objetos_presentes,
    221 - (tabelas + colunas + constraints + indices + sequences) AS objetos_ausentes
FROM estado;

-- P454: nomes normativos de constraints/indices M0010 nao podem colidir.
SELECT
    (SELECT count(*)
       FROM pg_catalog.pg_constraint AS constraint_row
      WHERE constraint_row.connamespace = 'public'::pg_catalog.regnamespace
        AND constraint_row.conname ~ '^(ck|fk|pk|uq)_(catalogo|unidades_medida)') AS constraints_colidentes,
    (SELECT count(*)
       FROM pg_catalog.pg_class AS index_row
      WHERE index_row.relnamespace = 'public'::pg_catalog.regnamespace
        AND index_row.relkind = 'i'
        AND index_row.relname ~ '^(ix|pk|uq)_(catalogo|unidades_medida)') AS indices_colidentes;
