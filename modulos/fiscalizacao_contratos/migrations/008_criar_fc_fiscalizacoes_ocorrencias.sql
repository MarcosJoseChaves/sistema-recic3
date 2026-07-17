BEGIN;

CREATE TABLE IF NOT EXISTS fc_fiscalizacoes (
    id BIGSERIAL PRIMARY KEY,
    contrato_id BIGINT NOT NULL REFERENCES fc_contratos(id),
    servidor_responsavel_id BIGINT NOT NULL REFERENCES fc_servidores(id),
    data_fiscalizacao DATE NOT NULL,
    hora_inicio TIME,
    hora_fim TIME,
    tipo_fiscalizacao VARCHAR(40) NOT NULL,
    local_fiscalizacao TEXT,
    objeto_verificado TEXT NOT NULL,
    resultado VARCHAR(40) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'Em elaboração',
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_fiscalizacoes_tipo CHECK (
        tipo_fiscalizacao IN ('Rotina', 'Extraordinária', 'Retorno',
            'Recebimento de serviço', 'Conferência documental', 'Remota', 'Outra')
    ),
    CONSTRAINT ck_fc_fiscalizacoes_resultado CHECK (
        resultado IN ('Conforme', 'Conforme com ressalvas', 'Não conforme',
            'Pendente de análise')
    ),
    CONSTRAINT ck_fc_fiscalizacoes_status CHECK (
        status IN ('Em elaboração', 'Finalizada', 'Cancelada')
    ),
    CONSTRAINT ck_fc_fiscalizacoes_objeto CHECK (BTRIM(objeto_verificado) <> ''),
    CONSTRAINT ck_fc_fiscalizacoes_horarios CHECK (
        hora_fim IS NULL OR hora_inicio IS NULL OR hora_fim >= hora_inicio
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_fiscalizacoes_id_contrato
    ON fc_fiscalizacoes (id, contrato_id);

CREATE INDEX IF NOT EXISTS idx_fc_fiscalizacoes_contrato_data
    ON fc_fiscalizacoes (contrato_id, data_fiscalizacao DESC);

CREATE INDEX IF NOT EXISTS idx_fc_fiscalizacoes_servidor
    ON fc_fiscalizacoes (servidor_responsavel_id, data_fiscalizacao DESC);

CREATE INDEX IF NOT EXISTS idx_fc_fiscalizacoes_status
    ON fc_fiscalizacoes (status, ativo, data_fiscalizacao DESC);

CREATE TABLE IF NOT EXISTS fc_ocorrencias (
    id BIGSERIAL PRIMARY KEY,
    contrato_id BIGINT NOT NULL REFERENCES fc_contratos(id),
    fiscalizacao_id BIGINT,
    ativo_contratual_id BIGINT REFERENCES fc_ativos_contratuais(id),
    servidor_responsavel_id BIGINT NOT NULL REFERENCES fc_servidores(id),
    titulo VARCHAR(200) NOT NULL,
    categoria VARCHAR(50) NOT NULL,
    gravidade VARCHAR(20) NOT NULL,
    descricao TEXT NOT NULL,
    data_identificacao DATE NOT NULL,
    prazo_correcao DATE,
    status VARCHAR(30) NOT NULL DEFAULT 'Aberta',
    exige_notificacao BOOLEAN NOT NULL DEFAULT FALSE,
    numero_notificacao VARCHAR(100),
    data_regularizacao DATE,
    conclusao TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT fk_fc_ocorrencias_fiscalizacao_contrato FOREIGN KEY
        (fiscalizacao_id, contrato_id)
        REFERENCES fc_fiscalizacoes (id, contrato_id),
    CONSTRAINT ck_fc_ocorrencias_titulo CHECK (BTRIM(titulo) <> ''),
    CONSTRAINT ck_fc_ocorrencias_descricao CHECK (BTRIM(descricao) <> ''),
    CONSTRAINT ck_fc_ocorrencias_categoria CHECK (
        categoria IN ('Execução do serviço', 'Qualidade', 'Prazo', 'Mão de obra',
            'Segurança do trabalho', 'Veículo ou equipamento', 'Documentação',
            'Ambiental', 'Trabalhista', 'Descumprimento contratual', 'Outro')
    ),
    CONSTRAINT ck_fc_ocorrencias_gravidade CHECK (
        gravidade IN ('Leve', 'Média', 'Grave', 'Crítica')
    ),
    CONSTRAINT ck_fc_ocorrencias_status CHECK (
        status IN ('Aberta', 'Em acompanhamento', 'Regularizada',
            'Não regularizada', 'Cancelada')
    ),
    CONSTRAINT ck_fc_ocorrencias_prazo CHECK (
        prazo_correcao IS NULL OR prazo_correcao >= data_identificacao
    ),
    CONSTRAINT ck_fc_ocorrencias_notificacao CHECK (
        exige_notificacao = FALSE OR BTRIM(COALESCE(numero_notificacao, '')) <> ''
    ),
    CONSTRAINT ck_fc_ocorrencias_data_regularizacao CHECK (
        data_regularizacao IS NULL OR status IN ('Regularizada', 'Não regularizada')
    ),
    CONSTRAINT ck_fc_ocorrencias_regularizada_com_data CHECK (
        status <> 'Regularizada' OR data_regularizacao IS NOT NULL
    )
);

CREATE INDEX IF NOT EXISTS idx_fc_ocorrencias_contrato_status
    ON fc_ocorrencias (contrato_id, status, ativo);

CREATE INDEX IF NOT EXISTS idx_fc_ocorrencias_fiscalizacao
    ON fc_ocorrencias (fiscalizacao_id, ativo);

CREATE INDEX IF NOT EXISTS idx_fc_ocorrencias_ativo
    ON fc_ocorrencias (ativo_contratual_id, ativo);

CREATE INDEX IF NOT EXISTS idx_fc_ocorrencias_prazo_status
    ON fc_ocorrencias (prazo_correcao, status, ativo);

CREATE INDEX IF NOT EXISTS idx_fc_ocorrencias_gravidade
    ON fc_ocorrencias (gravidade, status, ativo);

CREATE TABLE IF NOT EXISTS fc_ocorrencia_acompanhamentos (
    id BIGSERIAL PRIMARY KEY,
    ocorrencia_id BIGINT NOT NULL REFERENCES fc_ocorrencias(id),
    data_acompanhamento DATE NOT NULL,
    status_anterior VARCHAR(30) NOT NULL,
    status_novo VARCHAR(30) NOT NULL,
    descricao TEXT NOT NULL,
    providencia_contratada TEXT,
    observacoes TEXT,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    CONSTRAINT ck_fc_acomp_status_anterior CHECK (
        status_anterior IN ('Aberta', 'Em acompanhamento', 'Regularizada',
            'Não regularizada', 'Cancelada')
    ),
    CONSTRAINT ck_fc_acomp_status_novo CHECK (
        status_novo IN ('Aberta', 'Em acompanhamento', 'Regularizada',
            'Não regularizada', 'Cancelada')
    ),
    CONSTRAINT ck_fc_acomp_descricao CHECK (BTRIM(descricao) <> '')
);

CREATE INDEX IF NOT EXISTS idx_fc_acompanhamentos_ocorrencia_data
    ON fc_ocorrencia_acompanhamentos
       (ocorrencia_id, data_acompanhamento, id);

COMMIT;
