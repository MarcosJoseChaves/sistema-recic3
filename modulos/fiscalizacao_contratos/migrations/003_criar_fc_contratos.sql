BEGIN;

CREATE TABLE IF NOT EXISTS fc_contratos (
    id BIGSERIAL PRIMARY KEY,
    numero_contrato VARCHAR(100) NOT NULL,
    processo_administrativo VARCHAR(100),
    objeto TEXT NOT NULL,
    empresa_id BIGINT NOT NULL REFERENCES fc_empresas(id),
    valor_original NUMERIC(15, 2) NOT NULL,
    data_assinatura DATE,
    vigencia_inicio DATE,
    vigencia_fim DATE,
    situacao VARCHAR(30) NOT NULL DEFAULT 'Em elaboração',
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT uq_fc_contratos_numero UNIQUE (numero_contrato),
    CONSTRAINT ck_fc_contratos_numero_preenchido CHECK (BTRIM(numero_contrato) <> ''),
    CONSTRAINT ck_fc_contratos_objeto_preenchido CHECK (BTRIM(objeto) <> ''),
    CONSTRAINT ck_fc_contratos_valor_nao_negativo CHECK (valor_original >= 0),
    CONSTRAINT ck_fc_contratos_vigencia CHECK (
        vigencia_inicio IS NULL OR vigencia_fim IS NULL OR vigencia_fim >= vigencia_inicio
    ),
    CONSTRAINT ck_fc_contratos_situacao CHECK (
        situacao IN ('Em elaboração', 'Vigente', 'Suspenso', 'Encerrado', 'Cancelado')
    )
);

CREATE TABLE IF NOT EXISTS fc_contrato_responsaveis (
    id BIGSERIAL PRIMARY KEY,
    contrato_id BIGINT NOT NULL REFERENCES fc_contratos(id),
    servidor_id BIGINT NOT NULL REFERENCES fc_servidores(id),
    tipo_responsabilidade VARCHAR(30) NOT NULL,
    titular BOOLEAN NOT NULL DEFAULT FALSE,
    data_inicio DATE NOT NULL DEFAULT CURRENT_DATE,
    data_fim DATE,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_contrato_responsaveis_tipo CHECK (
        tipo_responsabilidade IN ('Gestor', 'Fiscal titular', 'Fiscal substituto')
    ),
    CONSTRAINT ck_fc_contrato_responsaveis_titular CHECK (
        (tipo_responsabilidade IN ('Gestor', 'Fiscal titular') AND titular = TRUE)
        OR (tipo_responsabilidade = 'Fiscal substituto' AND titular = FALSE)
    ),
    CONSTRAINT ck_fc_contrato_responsaveis_periodo CHECK (
        data_fim IS NULL OR data_fim >= data_inicio
    )
);

CREATE INDEX IF NOT EXISTS idx_fc_contratos_empresa
    ON fc_contratos (empresa_id);

CREATE INDEX IF NOT EXISTS idx_fc_contratos_situacao_vigencia
    ON fc_contratos (ativo, situacao, vigencia_fim);

CREATE INDEX IF NOT EXISTS idx_fc_contrato_responsaveis_contrato
    ON fc_contrato_responsaveis (contrato_id, ativo);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_contrato_responsavel_tipo_ativo
    ON fc_contrato_responsaveis (contrato_id, servidor_id, tipo_responsabilidade)
    WHERE ativo = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_contrato_titular_tipo_ativo
    ON fc_contrato_responsaveis (contrato_id, tipo_responsabilidade)
    WHERE ativo = TRUE AND tipo_responsabilidade IN ('Gestor', 'Fiscal titular');

COMMIT;
