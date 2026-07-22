BEGIN;

CREATE TABLE IF NOT EXISTS fc_atestes (
    id BIGSERIAL PRIMARY KEY,
    medicao_id BIGINT NOT NULL REFERENCES fc_medicoes(id),
    numero_ateste INTEGER NOT NULL,
    servidor_atestador_id BIGINT NOT NULL REFERENCES fc_servidores(id),
    data_ateste DATE,
    status VARCHAR(40) NOT NULL DEFAULT 'Em elaboração',
    parecer TEXT,
    observacoes TEXT,
    valor_atestado NUMERIC(18,2) NOT NULL,
    protocolo_encaminhamento VARCHAR(200),
    encaminhado_em TIMESTAMPTZ,
    servidor_encaminhador_id BIGINT REFERENCES fc_servidores(id),
    encaminhado_por_usuario_id INTEGER REFERENCES usuarios(id),
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_atestes_numero_positivo CHECK (numero_ateste > 0),
    CONSTRAINT ck_fc_atestes_status CHECK (status IN (
        'Em elaboração', 'Devolvido para correção', 'Atestado',
        'Encaminhado para pagamento', 'Cancelado'
    )),
    CONSTRAINT ck_fc_atestes_valor_nao_negativo CHECK (valor_atestado >= 0),
    CONSTRAINT ck_fc_atestes_data_ateste CHECK (
        status NOT IN ('Atestado', 'Encaminhado para pagamento') OR data_ateste IS NOT NULL
    ),
    CONSTRAINT ck_fc_atestes_encaminhamento CHECK (
        (status = 'Encaminhado para pagamento' AND protocolo_encaminhamento IS NOT NULL
         AND BTRIM(protocolo_encaminhamento) <> '' AND encaminhado_em IS NOT NULL
         AND servidor_encaminhador_id IS NOT NULL AND encaminhado_por_usuario_id IS NOT NULL)
        OR
        (status <> 'Encaminhado para pagamento' AND protocolo_encaminhamento IS NULL
         AND encaminhado_em IS NULL AND servidor_encaminhador_id IS NULL
         AND encaminhado_por_usuario_id IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_atestes_medicao_numero
    ON fc_atestes (medicao_id, numero_ateste);
CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_atestes_ativo_medicao
    ON fc_atestes (medicao_id) WHERE ativo = TRUE AND status <> 'Cancelado';
CREATE INDEX IF NOT EXISTS idx_fc_atestes_medicao ON fc_atestes (medicao_id, ativo);
CREATE INDEX IF NOT EXISTS idx_fc_atestes_status_data ON fc_atestes (status, data_ateste);
CREATE INDEX IF NOT EXISTS idx_fc_atestes_servidor ON fc_atestes (servidor_atestador_id, ativo);
CREATE INDEX IF NOT EXISTS idx_fc_atestes_encaminhamento ON fc_atestes (encaminhado_em, status);

CREATE TABLE IF NOT EXISTS fc_ateste_notas_fiscais (
    id BIGSERIAL PRIMARY KEY,
    ateste_id BIGINT NOT NULL REFERENCES fc_atestes(id),
    numero_nota VARCHAR(100) NOT NULL,
    serie VARCHAR(50),
    data_emissao DATE NOT NULL,
    valor_nota NUMERIC(18,2) NOT NULL,
    chave_acesso VARCHAR(100),
    documento_id BIGINT REFERENCES fc_documentos(id),
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_notas_numero CHECK (BTRIM(numero_nota) <> ''),
    CONSTRAINT ck_fc_notas_valor_positivo CHECK (valor_nota > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_ateste_nota_ativa
    ON fc_ateste_notas_fiscais (ateste_id, LOWER(BTRIM(numero_nota)), LOWER(BTRIM(COALESCE(serie, ''))))
    WHERE ativo = TRUE;
CREATE INDEX IF NOT EXISTS idx_fc_ateste_notas_ateste ON fc_ateste_notas_fiscais (ateste_id, ativo);
CREATE INDEX IF NOT EXISTS idx_fc_ateste_notas_numero_serie ON fc_ateste_notas_fiscais (numero_nota, serie);
CREATE INDEX IF NOT EXISTS idx_fc_ateste_notas_chave ON fc_ateste_notas_fiscais (chave_acesso);

CREATE TABLE IF NOT EXISTS fc_ateste_documentos (
    id BIGSERIAL PRIMARY KEY,
    ateste_id BIGINT NOT NULL REFERENCES fc_atestes(id),
    documento_id BIGINT NOT NULL REFERENCES fc_documentos(id),
    categoria VARCHAR(40) NOT NULL,
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_ateste_documentos_categoria CHECK (categoria IN (
        'Nota fiscal', 'Relatório de execução', 'Certidão', 'Comprovante',
        'Declaração', 'Ordem de serviço', 'Memória de cálculo', 'Outro'
    ))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_ateste_documento_ativo
    ON fc_ateste_documentos (ateste_id, documento_id) WHERE ativo = TRUE;
CREATE INDEX IF NOT EXISTS idx_fc_ateste_documentos_ateste
    ON fc_ateste_documentos (ateste_id, ativo, categoria);

CREATE TABLE IF NOT EXISTS fc_ateste_eventos (
    id BIGSERIAL PRIMARY KEY,
    ateste_id BIGINT NOT NULL REFERENCES fc_atestes(id),
    tipo_evento VARCHAR(50) NOT NULL,
    status_anterior VARCHAR(40),
    status_novo VARCHAR(40) NOT NULL,
    justificativa TEXT,
    valor_atestado NUMERIC(18,2) NOT NULL,
    total_notas NUMERIC(18,2) NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    CONSTRAINT ck_fc_ateste_eventos_tipo CHECK (tipo_evento IN (
        'Criação', 'Ateste', 'Devolução para correção', 'Retorno para elaboração',
        'Encaminhamento para pagamento', 'Cancelamento'
    )),
    CONSTRAINT ck_fc_ateste_eventos_status_anterior CHECK (
        status_anterior IS NULL OR status_anterior IN (
            'Em elaboração', 'Devolvido para correção', 'Atestado',
            'Encaminhado para pagamento', 'Cancelado'
        )
    ),
    CONSTRAINT ck_fc_ateste_eventos_status_novo CHECK (status_novo IN (
        'Em elaboração', 'Devolvido para correção', 'Atestado',
        'Encaminhado para pagamento', 'Cancelado'
    )),
    CONSTRAINT ck_fc_ateste_eventos_justificativa CHECK (
        tipo_evento NOT IN ('Devolução para correção', 'Retorno para elaboração', 'Cancelamento')
        OR (justificativa IS NOT NULL AND BTRIM(justificativa) <> '')
    ),
    CONSTRAINT ck_fc_ateste_eventos_valores CHECK (valor_atestado >= 0 AND total_notas >= 0)
);

CREATE INDEX IF NOT EXISTS idx_fc_ateste_eventos_ateste_data
    ON fc_ateste_eventos (ateste_id, criado_em DESC, id DESC);

COMMIT;
