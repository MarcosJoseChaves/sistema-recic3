BEGIN;

CREATE TABLE IF NOT EXISTS fc_fiscalizacao_eventos (
    id BIGSERIAL PRIMARY KEY,
    fiscalizacao_id BIGINT NOT NULL REFERENCES fc_fiscalizacoes(id),
    tipo_evento VARCHAR(20) NOT NULL,
    status_anterior VARCHAR(20) NOT NULL,
    status_novo VARCHAR(20) NOT NULL,
    justificativa TEXT,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    CONSTRAINT ck_fc_fiscalizacao_eventos_tipo CHECK (
        tipo_evento IN ('Finalização', 'Cancelamento', 'Reabertura')
    ),
    CONSTRAINT ck_fc_fiscalizacao_eventos_status_anterior CHECK (
        status_anterior IN ('Em elaboração', 'Finalizada', 'Cancelada')
    ),
    CONSTRAINT ck_fc_fiscalizacao_eventos_status_novo CHECK (
        status_novo IN ('Em elaboração', 'Finalizada', 'Cancelada')
    ),
    CONSTRAINT ck_fc_fiscalizacao_eventos_justificativa CHECK (
        tipo_evento = 'Finalização'
        OR BTRIM(COALESCE(justificativa, '')) <> ''
    ),
    CONSTRAINT ck_fc_fiscalizacao_eventos_transicao CHECK (
        (tipo_evento = 'Finalização'
            AND status_anterior = 'Em elaboração' AND status_novo = 'Finalizada')
        OR (tipo_evento = 'Cancelamento'
            AND status_anterior = 'Em elaboração' AND status_novo = 'Cancelada')
        OR (tipo_evento = 'Reabertura'
            AND status_anterior = 'Finalizada' AND status_novo = 'Em elaboração')
    )
);

CREATE INDEX IF NOT EXISTS idx_fc_fiscalizacao_eventos_fiscalizacao_data
    ON fc_fiscalizacao_eventos (fiscalizacao_id, criado_em DESC, id DESC);

COMMIT;
