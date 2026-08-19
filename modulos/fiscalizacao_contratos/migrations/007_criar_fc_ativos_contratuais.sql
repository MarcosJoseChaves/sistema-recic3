BEGIN;

CREATE TABLE IF NOT EXISTS fc_ativos_contratuais (
    id BIGSERIAL PRIMARY KEY,
    codigo_interno VARCHAR(100) NOT NULL,
    tipo_ativo VARCHAR(40) NOT NULL,
    descricao TEXT NOT NULL,
    marca VARCHAR(100),
    modelo VARCHAR(100),
    ano_fabricacao INTEGER,
    placa VARCHAR(20),
    renavam VARCHAR(30),
    chassi VARCHAR(50),
    numero_serie VARCHAR(100),
    numero_patrimonio VARCHAR(100),
    origem_ativo VARCHAR(20) NOT NULL,
    empresa_proprietaria_id BIGINT REFERENCES fc_empresas(id),
    capacidade NUMERIC(24, 8),
    unidade_capacidade VARCHAR(50),
    situacao VARCHAR(30) NOT NULL,
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_ativos_codigo_preenchido CHECK (BTRIM(codigo_interno) <> ''),
    CONSTRAINT ck_fc_ativos_descricao_preenchida CHECK (BTRIM(descricao) <> ''),
    CONSTRAINT ck_fc_ativos_tipo CHECK (
        tipo_ativo IN ('Veículo', 'Máquina', 'Equipamento', 'Implemento',
                       'Contentor ou recipiente', 'Imóvel ou instalação',
                       'Tecnologia ou sistema', 'Ferramenta', 'Outro')
    ),
    CONSTRAINT ck_fc_ativos_origem CHECK (
        origem_ativo IN ('Município', 'Contratada', 'Locado', 'Terceiro')
    ),
    CONSTRAINT ck_fc_ativos_situacao CHECK (
        situacao IN ('Disponível', 'Em operação', 'Em manutenção',
                     'Indisponível', 'Baixado')
    ),
    CONSTRAINT ck_fc_ativos_ano CHECK (ano_fabricacao IS NULL OR ano_fabricacao >= 1900),
    CONSTRAINT ck_fc_ativos_capacidade CHECK (capacidade IS NULL OR capacidade >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_ativos_codigo_normalizado
    ON fc_ativos_contratuais (UPPER(BTRIM(codigo_interno)));

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_ativos_placa_normalizada
    ON fc_ativos_contratuais (UPPER(REGEXP_REPLACE(placa, '[^A-Za-z0-9]', '', 'g')))
    WHERE placa IS NOT NULL AND BTRIM(placa) <> '';

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_ativos_chassi_normalizado
    ON fc_ativos_contratuais (UPPER(REGEXP_REPLACE(chassi, '[^A-Za-z0-9]', '', 'g')))
    WHERE chassi IS NOT NULL AND BTRIM(chassi) <> '';

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_ativos_patrimonio_normalizado
    ON fc_ativos_contratuais (UPPER(BTRIM(numero_patrimonio)))
    WHERE numero_patrimonio IS NOT NULL AND BTRIM(numero_patrimonio) <> '';

CREATE INDEX IF NOT EXISTS idx_fc_ativos_tipo_situacao_ativo
    ON fc_ativos_contratuais (tipo_ativo, situacao, ativo);

CREATE INDEX IF NOT EXISTS idx_fc_ativos_empresa
    ON fc_ativos_contratuais (empresa_proprietaria_id);

CREATE TABLE IF NOT EXISTS fc_ativo_vinculos (
    id BIGSERIAL PRIMARY KEY,
    ativo_id BIGINT NOT NULL REFERENCES fc_ativos_contratuais(id),
    contrato_id BIGINT NOT NULL REFERENCES fc_contratos(id),
    natureza_vinculo VARCHAR(40) NOT NULL,
    data_inicio DATE NOT NULL,
    data_fim DATE,
    principal BOOLEAN NOT NULL DEFAULT FALSE,
    observacoes TEXT,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT ck_fc_ativo_vinculos_natureza CHECK (
        natureza_vinculo IN ('Exigido pelo contrato', 'Operacional', 'Reserva',
                             'Substituto', 'Cedido pelo Município', 'Outro')
    ),
    CONSTRAINT ck_fc_ativo_vinculos_datas CHECK (
        data_fim IS NULL OR data_fim >= data_inicio
    ),
    CONSTRAINT ck_fc_ativo_vinculos_estado_datas CHECK (
        (ativo = TRUE AND data_fim IS NULL)
        OR (ativo = FALSE AND data_fim IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_fc_ativo_vinculos_contrato
    ON fc_ativo_vinculos (contrato_id, ativo, data_inicio);

CREATE INDEX IF NOT EXISTS idx_fc_ativo_vinculos_ativo
    ON fc_ativo_vinculos (ativo_id, ativo, data_inicio);

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_ativo_vinculo_ativo
    ON fc_ativo_vinculos (ativo_id, contrato_id)
    WHERE ativo = TRUE;

COMMIT;
