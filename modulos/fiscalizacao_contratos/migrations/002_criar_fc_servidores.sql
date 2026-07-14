BEGIN;

CREATE TABLE IF NOT EXISTS fc_servidores (
    id BIGSERIAL PRIMARY KEY,
    nome VARCHAR(255) NOT NULL,
    matricula VARCHAR(50) NOT NULL,
    cargo VARCHAR(150),
    setor VARCHAR(150),
    email VARCHAR(254),
    telefone VARCHAR(30),
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT uq_fc_servidores_matricula UNIQUE (matricula),
    CONSTRAINT ck_fc_servidores_nome_preenchido CHECK (BTRIM(nome) <> ''),
    CONSTRAINT ck_fc_servidores_matricula_preenchida CHECK (BTRIM(matricula) <> '')
);

CREATE INDEX IF NOT EXISTS idx_fc_servidores_ativo_nome
    ON fc_servidores (ativo, nome);

COMMIT;
