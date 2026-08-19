BEGIN;

CREATE TABLE IF NOT EXISTS fc_medicoes (
    id BIGSERIAL PRIMARY KEY,
    contrato_id BIGINT NOT NULL REFERENCES fc_contratos(id),
    numero_medicao INTEGER NOT NULL,
    competencia DATE NOT NULL,
    periodo_inicio DATE NOT NULL,
    periodo_fim DATE NOT NULL,
    versao INTEGER NOT NULL DEFAULT 1,
    medicao_origem_id BIGINT REFERENCES fc_medicoes(id),
    atual BOOLEAN NOT NULL DEFAULT TRUE,
    servidor_fiscal_id BIGINT NOT NULL REFERENCES fc_servidores(id),
    data_apresentacao DATE,
    status VARCHAR(40) NOT NULL DEFAULT 'Em elaboração',
    valor_bruto NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_acrescimos NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_descontos NUMERIC(18, 2) NOT NULL DEFAULT 0,
    total_glosas NUMERIC(18, 2) NOT NULL DEFAULT 0,
    valor_liquido NUMERIC(18, 2) NOT NULL DEFAULT 0,
    observacoes TEXT,
    aprovado_em TIMESTAMPTZ,
    servidor_aprovador_id BIGINT REFERENCES fc_servidores(id),
    aprovado_por_usuario_id INTEGER REFERENCES usuarios(id),
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT uq_fc_medicoes_contrato_numero_versao UNIQUE
        (contrato_id, numero_medicao, versao),
    CONSTRAINT ck_fc_medicoes_numero_positivo CHECK (numero_medicao > 0),
    CONSTRAINT ck_fc_medicoes_competencia_primeiro_dia CHECK
        (EXTRACT(DAY FROM competencia) = 1),
    CONSTRAINT ck_fc_medicoes_periodo CHECK (periodo_fim >= periodo_inicio),
    CONSTRAINT ck_fc_medicoes_versao_positiva CHECK (versao > 0),
    CONSTRAINT ck_fc_medicoes_status CHECK (status IN (
        'Em elaboração', 'Em análise', 'Devolvida para correção',
        'Aprovada', 'Cancelada'
    )),
    CONSTRAINT ck_fc_medicoes_valores_nao_negativos CHECK (
        valor_bruto >= 0 AND total_acrescimos >= 0 AND total_descontos >= 0
        AND total_glosas >= 0 AND valor_liquido >= 0
    ),
    CONSTRAINT ck_fc_medicoes_aprovacao CHECK (
        (status = 'Aprovada' AND aprovado_em IS NOT NULL
            AND servidor_aprovador_id IS NOT NULL
            AND aprovado_por_usuario_id IS NOT NULL)
        OR (status <> 'Aprovada' AND aprovado_em IS NULL
            AND servidor_aprovador_id IS NULL
            AND aprovado_por_usuario_id IS NULL)
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_medicoes_atual_ativa_competencia
    ON fc_medicoes (contrato_id, competencia)
    WHERE atual = TRUE AND ativo = TRUE;

CREATE INDEX IF NOT EXISTS idx_fc_medicoes_contrato_competencia_status
    ON fc_medicoes (contrato_id, competencia DESC, status);

CREATE INDEX IF NOT EXISTS idx_fc_medicoes_versoes
    ON fc_medicoes (contrato_id, numero_medicao, versao DESC);

CREATE INDEX IF NOT EXISTS idx_fc_medicoes_servidor_fiscal
    ON fc_medicoes (servidor_fiscal_id, status);

CREATE INDEX IF NOT EXISTS idx_fc_medicoes_atuais
    ON fc_medicoes (atual, ativo, competencia DESC);

CREATE TABLE IF NOT EXISTS fc_medicao_itens (
    id BIGSERIAL PRIMARY KEY,
    medicao_id BIGINT NOT NULL REFERENCES fc_medicoes(id),
    planilha_item_id BIGINT REFERENCES fc_planilha_itens(id),
    ordem INTEGER NOT NULL,
    codigo_item VARCHAR(100),
    descricao TEXT NOT NULL,
    unidade VARCHAR(50) NOT NULL,
    quantidade_prevista NUMERIC(24, 8),
    quantidade_medida NUMERIC(24, 8) NOT NULL,
    preco_unitario NUMERIC(24, 8) NOT NULL,
    valor_medido NUMERIC(18, 2) NOT NULL,
    justificativa_excedente TEXT,
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_medicao_itens_ordem CHECK (ordem > 0),
    CONSTRAINT ck_fc_medicao_itens_descricao CHECK (BTRIM(descricao) <> ''),
    CONSTRAINT ck_fc_medicao_itens_unidade CHECK (BTRIM(unidade) <> ''),
    CONSTRAINT ck_fc_medicao_itens_quantidades CHECK (
        (quantidade_prevista IS NULL OR quantidade_prevista >= 0)
        AND quantidade_medida >= 0
    ),
    CONSTRAINT ck_fc_medicao_itens_preco CHECK (preco_unitario >= 0),
    CONSTRAINT ck_fc_medicao_itens_valor CHECK (valor_medido >= 0),
    CONSTRAINT ck_fc_medicao_itens_excedente CHECK (
        quantidade_prevista IS NULL OR quantidade_medida <= quantidade_prevista
        OR BTRIM(COALESCE(justificativa_excedente, '')) <> ''
    )
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_medicao_item_planilha_ativo
    ON fc_medicao_itens (medicao_id, planilha_item_id)
    WHERE ativo = TRUE AND planilha_item_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_fc_medicao_itens_medicao_ordem
    ON fc_medicao_itens (medicao_id, ativo, ordem, id);

CREATE INDEX IF NOT EXISTS idx_fc_medicao_itens_planilha
    ON fc_medicao_itens (planilha_item_id, medicao_id);

CREATE TABLE IF NOT EXISTS fc_medicao_ajustes (
    id BIGSERIAL PRIMARY KEY,
    medicao_id BIGINT NOT NULL REFERENCES fc_medicoes(id),
    tipo_ajuste VARCHAR(20) NOT NULL,
    descricao TEXT NOT NULL,
    valor NUMERIC(18, 2) NOT NULL,
    fiscalizacao_id BIGINT REFERENCES fc_fiscalizacoes(id),
    ocorrencia_id BIGINT REFERENCES fc_ocorrencias(id),
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_medicao_ajustes_tipo CHECK
        (tipo_ajuste IN ('Acréscimo', 'Desconto', 'Glosa')),
    CONSTRAINT ck_fc_medicao_ajustes_descricao CHECK (BTRIM(descricao) <> ''),
    CONSTRAINT ck_fc_medicao_ajustes_valor CHECK (valor > 0)
);

CREATE INDEX IF NOT EXISTS idx_fc_medicao_ajustes_medicao_tipo
    ON fc_medicao_ajustes (medicao_id, ativo, tipo_ajuste);

CREATE INDEX IF NOT EXISTS idx_fc_medicao_ajustes_ocorrencia
    ON fc_medicao_ajustes (ocorrencia_id);

CREATE INDEX IF NOT EXISTS idx_fc_medicao_ajustes_fiscalizacao
    ON fc_medicao_ajustes (fiscalizacao_id);

CREATE TABLE IF NOT EXISTS fc_medicao_documentos (
    id BIGSERIAL PRIMARY KEY,
    medicao_id BIGINT NOT NULL REFERENCES fc_medicoes(id),
    documento_id BIGINT NOT NULL REFERENCES fc_documentos(id),
    categoria VARCHAR(40) NOT NULL,
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_medicao_documentos_categoria CHECK (categoria IN (
        'Memória de cálculo', 'Relatório de medição', 'Evidência da execução',
        'Nota fiscal', 'Planilha', 'Ordem de serviço', 'Outro'
    ))
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_medicao_documento_ativo
    ON fc_medicao_documentos (medicao_id, documento_id)
    WHERE ativo = TRUE;

CREATE INDEX IF NOT EXISTS idx_fc_medicao_documentos_medicao
    ON fc_medicao_documentos (medicao_id, ativo, categoria);

CREATE TABLE IF NOT EXISTS fc_medicao_eventos (
    id BIGSERIAL PRIMARY KEY,
    medicao_id BIGINT NOT NULL REFERENCES fc_medicoes(id),
    tipo_evento VARCHAR(40) NOT NULL,
    status_anterior VARCHAR(40),
    status_novo VARCHAR(40) NOT NULL,
    justificativa TEXT,
    valor_bruto NUMERIC(18, 2) NOT NULL,
    total_acrescimos NUMERIC(18, 2) NOT NULL,
    total_descontos NUMERIC(18, 2) NOT NULL,
    total_glosas NUMERIC(18, 2) NOT NULL,
    valor_liquido NUMERIC(18, 2) NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    CONSTRAINT ck_fc_medicao_eventos_tipo CHECK (tipo_evento IN (
        'Criação', 'Envio para análise', 'Devolução para correção',
        'Aprovação', 'Cancelamento', 'Revisão criada',
        'Substituição por revisão'
    )),
    CONSTRAINT ck_fc_medicao_eventos_status_anterior CHECK (
        status_anterior IS NULL OR status_anterior IN (
            'Em elaboração', 'Em análise', 'Devolvida para correção',
            'Aprovada', 'Cancelada'
        )
    ),
    CONSTRAINT ck_fc_medicao_eventos_status_novo CHECK (status_novo IN (
        'Em elaboração', 'Em análise', 'Devolvida para correção',
        'Aprovada', 'Cancelada'
    )),
    CONSTRAINT ck_fc_medicao_eventos_justificativa CHECK (
        tipo_evento NOT IN ('Devolução para correção', 'Cancelamento', 'Revisão criada')
        OR BTRIM(COALESCE(justificativa, '')) <> ''
    ),
    CONSTRAINT ck_fc_medicao_eventos_valores CHECK (
        valor_bruto >= 0 AND total_acrescimos >= 0 AND total_descontos >= 0
        AND total_glosas >= 0 AND valor_liquido >= 0
    )
);

CREATE INDEX IF NOT EXISTS idx_fc_medicao_eventos_medicao_data
    ON fc_medicao_eventos (medicao_id, criado_em DESC, id DESC);

COMMIT;
