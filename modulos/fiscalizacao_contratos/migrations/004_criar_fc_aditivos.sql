BEGIN;

CREATE TABLE IF NOT EXISTS fc_aditivos (
    id BIGSERIAL PRIMARY KEY,
    contrato_id BIGINT NOT NULL REFERENCES fc_contratos(id),
    numero_termo VARCHAR(100) NOT NULL,
    tipo_aditivo VARCHAR(50) NOT NULL,
    data_assinatura DATE NOT NULL,
    data_inicio_efeitos DATE,
    dias_acrescidos INTEGER,
    nova_vigencia_fim DATE,
    valor_acrescimo NUMERIC(15, 2) NOT NULL DEFAULT 0,
    valor_supressao NUMERIC(15, 2) NOT NULL DEFAULT 0,
    percentual_alteracao NUMERIC(9, 4),
    descricao_alteracao TEXT,
    justificativa TEXT,
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT uq_fc_aditivos_contrato_numero UNIQUE (contrato_id, numero_termo),
    CONSTRAINT ck_fc_aditivos_tipo CHECK (
        tipo_aditivo IN (
            'Prazo',
            'Acréscimo de valor',
            'Supressão de valor',
            'Prazo e valor',
            'Reajuste',
            'Repactuação',
            'Revisão',
            'Alteração de objeto',
            'Alteração quantitativa',
            'Alteração de cronograma',
            'Garantia',
            'Outro'
        )
    ),
    CONSTRAINT ck_fc_aditivos_dias_nao_negativos CHECK (
        dias_acrescidos IS NULL OR dias_acrescidos >= 0
    ),
    CONSTRAINT ck_fc_aditivos_valor_acrescimo_nao_negativo CHECK (
        valor_acrescimo >= 0
    ),
    CONSTRAINT ck_fc_aditivos_valor_supressao_nao_negativo CHECK (
        valor_supressao >= 0
    ),
    CONSTRAINT ck_fc_aditivos_percentual_nao_negativo CHECK (
        percentual_alteracao IS NULL OR percentual_alteracao >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_fc_aditivos_contrato_ativo
    ON fc_aditivos (contrato_id, ativo);

CREATE INDEX IF NOT EXISTS idx_fc_aditivos_tipo_ativo
    ON fc_aditivos (tipo_aditivo, ativo);

CREATE INDEX IF NOT EXISTS idx_fc_aditivos_data_assinatura
    ON fc_aditivos (data_assinatura);

COMMIT;
