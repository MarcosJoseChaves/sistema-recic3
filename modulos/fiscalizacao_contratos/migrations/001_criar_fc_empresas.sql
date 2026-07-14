BEGIN;

CREATE TABLE IF NOT EXISTS fc_empresas (
    id BIGSERIAL PRIMARY KEY,
    cnpj VARCHAR(14) NOT NULL,
    razao_social VARCHAR(255) NOT NULL,
    nome_fantasia VARCHAR(255),
    cep VARCHAR(8) NOT NULL,
    logradouro VARCHAR(255),
    numero VARCHAR(30),
    bairro VARCHAR(120),
    cidade VARCHAR(120),
    uf CHAR(2),
    telefone VARCHAR(30),
    email VARCHAR(254),
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT uq_fc_empresas_cnpj UNIQUE (cnpj),
    CONSTRAINT ck_fc_empresas_cnpj_numerico CHECK (cnpj ~ '^[0-9]{14}$'),
    CONSTRAINT ck_fc_empresas_cep_numerico CHECK (cep ~ '^[0-9]{8}$'),
    CONSTRAINT ck_fc_empresas_uf CHECK (uf IS NULL OR uf ~ '^[A-Z]{2}$')
);

CREATE INDEX IF NOT EXISTS idx_fc_empresas_ativo_razao_social
    ON fc_empresas (ativo, razao_social);

COMMIT;
