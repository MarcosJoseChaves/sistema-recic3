-- R0002: reconcilia exclusivamente M0003 após R0001 validada.
-- Requer P100-P107 = PASS. Não executa DML e preserva PK/serial legadas.

-- H2D5-CLASS-B: column|usuarios|username PRECHECK=P101
-- H2D5-CLASS-B: column|usuarios|password_hash PRECHECK=P102
-- H2D5-CLASS-B: column|usuarios|nome_completo PRECHECK=P103
-- H2D5-CLASS-B: column|usuarios|email PRECHECK=P104
-- H2D5-CLASS-C: column|usuarios|id PRESERVE_SERIAL_IDENTITY_EQUIVALENCE
-- H2D5-CLASS-C: sequence|usuarios_id_seq PRESERVE_OWNERSHIP_EQUIVALENCE

-- 53 ausências de M0003 atribuídas sem comandos cegos.
-- H2D5-ABSENT MATERIALIZE: table|usuario_recuperacoes_senha
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|id
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|usuario_id
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|token_hash
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|criado_em
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|expira_em
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|usado_em
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|invalidado_em
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|motivo_invalidacao
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|request_id
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|ip
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|agente_resumido
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|criado_por_tipo_ator
-- H2D5-ABSENT MATERIALIZE: column|usuario_recuperacoes_senha|invalidado_por_usuario_id
-- H2D5-ABSENT MATERIALIZE: column|usuarios|username_normalizado
-- H2D5-ABSENT MATERIALIZE: column|usuarios|email_normalizado
-- H2D5-ABSENT MATERIALIZE: column|usuarios|estado
-- H2D5-ABSENT MATERIALIZE: column|usuarios|exige_troca_senha
-- H2D5-ABSENT MATERIALIZE: column|usuarios|criado_em
-- H2D5-ABSENT MATERIALIZE: column|usuarios|atualizado_em
-- H2D5-ABSENT MATERIALIZE: column|usuarios|versao_registro
-- H2D5-ABSENT MATERIALIZE: column|usuarios|criado_por_usuario_id
-- H2D5-ABSENT MATERIALIZE: column|usuarios|inativado_em
-- H2D5-ABSENT MATERIALIZE: column|usuarios|inativado_por_usuario_id
-- H2D5-ABSENT MATERIALIZE: constraint|usuario_recuperacoes_senha|ck_usr_recuperacoes_senha__criado_por_tipo_ator_preenchido
-- H2D5-ABSENT MATERIALIZE: constraint|usuario_recuperacoes_senha|ck_usr_recuperacoes_senha__regra_264
-- H2D5-ABSENT MATERIALIZE: constraint|usuario_recuperacoes_senha|fk_usr_recuperacoes_senha__invalidado_por_usr_id
-- H2D5-ABSENT MATERIALIZE: constraint|usuario_recuperacoes_senha|fk_usr_recuperacoes_senha__usr_id
-- H2D5-ABSENT MATERIALIZE: constraint|usuario_recuperacoes_senha|pk_usr_recuperacoes_senha
-- H2D5-ABSENT MATERIALIZE: constraint|usuario_recuperacoes_senha|uq_usr_recuperacoes_senha__token_hash
-- H2D5-ABSENT MATERIALIZE: constraint|usuarios|ck_usuarios__estado
-- H2D5-ABSENT MATERIALIZE: constraint|usuarios|ck_usuarios__estado_preenchido
-- H2D5-ABSENT MATERIALIZE: constraint|usuarios|ck_usuarios__nome_completo_preenchido
-- H2D5-ABSENT MATERIALIZE: constraint|usuarios|ck_usuarios__regra_263
-- H2D5-ABSENT MATERIALIZE: constraint|usuarios|ck_usuarios__username_normalizado_preenchido
-- H2D5-ABSENT MATERIALIZE: constraint|usuarios|ck_usuarios__username_preenchido
-- H2D5-ABSENT MATERIALIZE: constraint|usuarios|ck_usuarios__versao_registro_positivo
-- H2D5-ABSENT MATERIALIZE: constraint|usuarios|fk_usuarios__criado_por_usr_id
-- H2D5-ABSENT MATERIALIZE: constraint|usuarios|fk_usuarios__inativado_por_usr_id
-- H2D5-ABSENT PRESERVE_EQUIVALENT: constraint|usuarios|pk_usuarios
-- H2D5-ABSENT MATERIALIZE: constraint|usuarios|uq_usuarios__username_normalizado
-- H2D5-ABSENT MATERIALIZE: index|usuario_recuperacoes_senha|ix_usr_recuperacoes_senha__invalidado_por_usr_id
-- H2D5-ABSENT MATERIALIZE: index|usuario_recuperacoes_senha|ix_usr_recuperacoes_senha__usr_id
-- H2D5-ABSENT MATERIALIZE: index|usuario_recuperacoes_senha|ix_usr_recuperacoes_senha__usr_id_expira_em
-- H2D5-ABSENT MATERIALIZE: index|usuario_recuperacoes_senha|pk_usr_recuperacoes_senha
-- H2D5-ABSENT MATERIALIZE: index|usuario_recuperacoes_senha|uq_usr_recuperacoes_senha__token_hash
-- H2D5-ABSENT MATERIALIZE: index|usuarios|ix_usuarios__criado_por_usr_id
-- H2D5-ABSENT MATERIALIZE: index|usuarios|ix_usuarios__estado_username_normalizado
-- H2D5-ABSENT MATERIALIZE: index|usuarios|ix_usuarios__inativado_por_usr_id
-- H2D5-ABSENT PRESERVE_EQUIVALENT: index|usuarios|pk_usuarios
-- H2D5-ABSENT MATERIALIZE: index|usuarios|uq_usuarios__email_normalizado
-- H2D5-ABSENT MATERIALIZE: index|usuarios|uq_usuarios__username_normalizado
-- H2D5-ABSENT MATERIALIZE: sequence|usuario_recuperacoes_senha_id_seq

-- Quatro divergências classe B: ampliações sem truncamento e nullability protegida.
ALTER TABLE usuarios
    ALTER COLUMN username TYPE TEXT USING username::TEXT,
    ALTER COLUMN password_hash TYPE TEXT USING password_hash::TEXT,
    ALTER COLUMN nome_completo TYPE TEXT USING nome_completo::TEXT,
    ALTER COLUMN nome_completo SET NOT NULL,
    ALTER COLUMN email TYPE TEXT USING email::TEXT;

-- Colunas normalizadas são preenchidas por DDL gerada e terminam regulares, sem default.
ALTER TABLE usuarios
    ADD COLUMN username_normalizado TEXT
        GENERATED ALWAYS AS (lower(btrim(username))) STORED,
    ADD COLUMN email_normalizado TEXT
        GENERATED ALWAYS AS (NULLIF(lower(btrim(email)), '')) STORED;

ALTER TABLE usuarios
    ALTER COLUMN username_normalizado DROP EXPRESSION,
    ALTER COLUMN username_normalizado SET NOT NULL,
    ALTER COLUMN email_normalizado DROP EXPRESSION;

ALTER TABLE usuarios
    ADD COLUMN estado TEXT NOT NULL DEFAULT 'PENDENTE',
    ADD COLUMN exige_troca_senha BOOLEAN NOT NULL DEFAULT TRUE,
    ADD COLUMN criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN versao_registro INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN criado_por_usuario_id INTEGER,
    ADD COLUMN inativado_em TIMESTAMPTZ,
    ADD COLUMN inativado_por_usuario_id INTEGER;

ALTER TABLE usuarios
    ADD CONSTRAINT uq_usuarios__username_normalizado UNIQUE (username_normalizado),
    ADD CONSTRAINT ck_usuarios__username_preenchido CHECK (btrim(username) <> ''),
    ADD CONSTRAINT ck_usuarios__username_normalizado_preenchido CHECK (btrim(username_normalizado) <> ''),
    ADD CONSTRAINT ck_usuarios__nome_completo_preenchido CHECK (btrim(nome_completo) <> ''),
    ADD CONSTRAINT ck_usuarios__estado_preenchido CHECK (btrim(estado) <> ''),
    ADD CONSTRAINT ck_usuarios__versao_registro_positivo CHECK (versao_registro > 0),
    ADD CONSTRAINT ck_usuarios__estado CHECK (estado IN ('ATIVO', 'INATIVO', 'BLOQUEADO', 'PENDENTE')),
    ADD CONSTRAINT ck_usuarios__regra_263 CHECK (
        username_normalizado = lower(btrim(username_normalizado))
        AND username_normalizado !~ '[[:space:]]'
        AND username_normalizado <> ''
    ),
    ADD CONSTRAINT fk_usuarios__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
        REFERENCES usuarios (id) ON DELETE RESTRICT NOT DEFERRABLE,
    ADD CONSTRAINT fk_usuarios__inativado_por_usr_id FOREIGN KEY (inativado_por_usuario_id)
        REFERENCES usuarios (id) ON DELETE RESTRICT NOT DEFERRABLE;

CREATE UNIQUE INDEX uq_usuarios__email_normalizado
    ON usuarios (email_normalizado)
    WHERE email_normalizado IS NOT NULL;

CREATE INDEX ix_usuarios__criado_por_usr_id
    ON usuarios (criado_por_usuario_id);

CREATE INDEX ix_usuarios__inativado_por_usr_id
    ON usuarios (inativado_por_usuario_id);

CREATE INDEX ix_usuarios__estado_username_normalizado
    ON usuarios (estado, username_normalizado);

CREATE TABLE usuario_recuperacoes_senha (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    usuario_id INTEGER NOT NULL,
    token_hash VARCHAR(64) NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expira_em TIMESTAMPTZ NOT NULL,
    usado_em TIMESTAMPTZ,
    invalidado_em TIMESTAMPTZ,
    motivo_invalidacao TEXT,
    request_id UUID NOT NULL,
    ip INET,
    agente_resumido TEXT,
    criado_por_tipo_ator TEXT NOT NULL,
    invalidado_por_usuario_id INTEGER
);

ALTER TABLE usuario_recuperacoes_senha
    ADD CONSTRAINT pk_usr_recuperacoes_senha PRIMARY KEY (id),
    ADD CONSTRAINT uq_usr_recuperacoes_senha__token_hash UNIQUE (token_hash),
    ADD CONSTRAINT ck_usr_recuperacoes_senha__criado_por_tipo_ator_preenchido
        CHECK (btrim(criado_por_tipo_ator) <> ''),
    ADD CONSTRAINT ck_usr_recuperacoes_senha__regra_264 CHECK (
        expira_em > criado_em
        AND (usado_em IS NULL OR usado_em >= criado_em)
        AND (invalidado_em IS NULL OR invalidado_em >= criado_em)
    ),
    ADD CONSTRAINT fk_usr_recuperacoes_senha__invalidado_por_usr_id
        FOREIGN KEY (invalidado_por_usuario_id)
        REFERENCES usuarios (id) ON DELETE RESTRICT NOT DEFERRABLE,
    ADD CONSTRAINT fk_usr_recuperacoes_senha__usr_id FOREIGN KEY (usuario_id)
        REFERENCES usuarios (id) ON DELETE RESTRICT NOT DEFERRABLE;

CREATE INDEX ix_usr_recuperacoes_senha__invalidado_por_usr_id
    ON usuario_recuperacoes_senha (invalidado_por_usuario_id);

CREATE INDEX ix_usr_recuperacoes_senha__usr_id
    ON usuario_recuperacoes_senha (usuario_id);

CREATE INDEX ix_usr_recuperacoes_senha__usr_id_expira_em
    ON usuario_recuperacoes_senha (usuario_id, expira_em);
