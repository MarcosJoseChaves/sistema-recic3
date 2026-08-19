BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_aditivos_id_contrato_id
    ON fc_aditivos (id, contrato_id);

CREATE TABLE IF NOT EXISTS fc_planilhas_orcamentarias (
    id BIGSERIAL PRIMARY KEY,
    contrato_id BIGINT NOT NULL REFERENCES fc_contratos(id),
    aditivo_id BIGINT,
    nome VARCHAR(200) NOT NULL,
    versao INTEGER NOT NULL,
    tipo_planilha VARCHAR(30) NOT NULL,
    data_referencia DATE NOT NULL,
    descricao_referencia TEXT,
    status VARCHAR(20) NOT NULL DEFAULT 'Em elaboração',
    vigente BOOLEAN NOT NULL DEFAULT FALSE,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT uq_fc_planilhas_contrato_versao UNIQUE (contrato_id, versao),
    CONSTRAINT fk_fc_planilhas_aditivo_contrato FOREIGN KEY (aditivo_id, contrato_id)
        REFERENCES fc_aditivos(id, contrato_id),
    CONSTRAINT ck_fc_planilhas_nome_preenchido CHECK (BTRIM(nome) <> ''),
    CONSTRAINT ck_fc_planilhas_versao_positiva CHECK (versao > 0),
    CONSTRAINT ck_fc_planilhas_tipo CHECK (
        tipo_planilha IN ('Original', 'Aditivada', 'Reajustada', 'Repactuada', 'Revisada', 'Outra')
    ),
    CONSTRAINT ck_fc_planilhas_status CHECK (status IN ('Em elaboração', 'Consolidada')),
    CONSTRAINT ck_fc_planilhas_vigente_consolidada CHECK (
        vigente = FALSE OR (status = 'Consolidada' AND ativo = TRUE)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_planilhas_original_contrato
    ON fc_planilhas_orcamentarias (contrato_id)
    WHERE tipo_planilha = 'Original';

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_planilhas_vigente_ativa_contrato
    ON fc_planilhas_orcamentarias (contrato_id)
    WHERE vigente = TRUE AND ativo = TRUE;

CREATE INDEX IF NOT EXISTS idx_fc_planilhas_contrato_ativo
    ON fc_planilhas_orcamentarias (contrato_id, ativo, versao);

CREATE INDEX IF NOT EXISTS idx_fc_planilhas_aditivo
    ON fc_planilhas_orcamentarias (aditivo_id);

CREATE INDEX IF NOT EXISTS idx_fc_planilhas_nome
    ON fc_planilhas_orcamentarias (nome);

CREATE TABLE IF NOT EXISTS fc_planilha_itens (
    id BIGSERIAL PRIMARY KEY,
    planilha_id BIGINT NOT NULL REFERENCES fc_planilhas_orcamentarias(id),
    ordem INTEGER NOT NULL,
    grupo VARCHAR(150),
    codigo_item VARCHAR(100),
    descricao TEXT NOT NULL,
    unidade VARCHAR(50) NOT NULL,
    quantidade NUMERIC(24, 8) NOT NULL,
    valor_unitario NUMERIC(24, 8) NOT NULL,
    fator_multiplicador NUMERIC(24, 8) NOT NULL DEFAULT 1,
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_planilha_itens_ordem_positiva CHECK (ordem > 0),
    CONSTRAINT ck_fc_planilha_itens_descricao_preenchida CHECK (BTRIM(descricao) <> ''),
    CONSTRAINT ck_fc_planilha_itens_unidade_preenchida CHECK (BTRIM(unidade) <> ''),
    CONSTRAINT ck_fc_planilha_itens_quantidade_nao_negativa CHECK (quantidade >= 0),
    CONSTRAINT ck_fc_planilha_itens_valor_nao_negativo CHECK (valor_unitario >= 0),
    CONSTRAINT ck_fc_planilha_itens_fator_positivo CHECK (fator_multiplicador > 0)
);

CREATE INDEX IF NOT EXISTS idx_fc_planilha_itens_planilha_ativo_ordem
    ON fc_planilha_itens (planilha_id, ativo, ordem, id);

CREATE INDEX IF NOT EXISTS idx_fc_planilha_itens_grupo
    ON fc_planilha_itens (planilha_id, grupo);

COMMIT;
