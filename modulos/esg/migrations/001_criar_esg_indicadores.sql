-- ESG-02B1: proposta aditiva, sem carga de dados e sem execução automática.
-- Revisão 1 apenas. Alterações/versionamento exigem outra migration revisada.
BEGIN;

CREATE TABLE IF NOT EXISTS esg_fontes_documentais (
    identificador VARCHAR(64) PRIMARY KEY,
    referencia_documental TEXT NOT NULL CHECK (BTRIM(referencia_documental) <> ''),
    data_documento DATE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    CHECK (identificador ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$')
);

CREATE TABLE IF NOT EXISTS esg_perimetros (
    codigo VARCHAR(64) PRIMARY KEY,
    descricao TEXT NOT NULL CHECK (BTRIM(descricao) <> ''),
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    CHECK (codigo ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$')
);

CREATE TABLE IF NOT EXISTS esg_unidades (
    simbolo VARCHAR(32) PRIMARY KEY,
    descricao TEXT NOT NULL CHECK (BTRIM(descricao) <> ''),
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    CHECK (BTRIM(simbolo) <> '' AND simbolo !~ '[[:space:]]')
);

CREATE TABLE IF NOT EXISTS esg_definicoes (
    codigo VARCHAR(64) PRIMARY KEY,
    nome TEXT NOT NULL CHECK (BTRIM(nome) <> ''),
    dimensao VARCHAR(16) NOT NULL CHECK (dimensao IN ('ambiental', 'social', 'governanca')),
    descricao TEXT NOT NULL CHECK (BTRIM(descricao) <> ''),
    unidade_simbolo VARCHAR(32) NOT NULL REFERENCES esg_unidades(simbolo),
    metodologia TEXT NOT NULL CHECK (BTRIM(metodologia) <> ''),
    perimetro_codigo VARCHAR(64) NOT NULL REFERENCES esg_perimetros(codigo),
    fonte_metodologica_id VARCHAR(64) NOT NULL REFERENCES esg_fontes_documentais(identificador),
    situacao VARCHAR(16) NOT NULL DEFAULT 'proposto'
        CHECK (situacao IN ('proposto', 'em_analise', 'aprovado', 'rejeitado')),
    fonte_aprovacao_id VARCHAR(64) REFERENCES esg_fontes_documentais(identificador),
    aprovado_em TIMESTAMPTZ,
    aprovado_por_usuario_id INTEGER REFERENCES usuarios(id),
    revisao INTEGER NOT NULL DEFAULT 1 CHECK (revisao = 1),
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    CONSTRAINT uq_esg_definicoes_perimetro UNIQUE (codigo, perimetro_codigo),
    CHECK (codigo ~ '^[A-Za-z][A-Za-z0-9_.-]{0,63}$'),
    CONSTRAINT ck_esg_definicoes_aprovacao CHECK (
        (situacao = 'aprovado' AND fonte_aprovacao_id IS NOT NULL
            AND aprovado_em IS NOT NULL AND aprovado_por_usuario_id IS NOT NULL)
        OR (situacao <> 'aprovado' AND fonte_aprovacao_id IS NULL
            AND aprovado_em IS NULL AND aprovado_por_usuario_id IS NULL)
    )
);

CREATE TABLE IF NOT EXISTS esg_registros (
    identificador UUID PRIMARY KEY,
    indicador_codigo VARCHAR(64) NOT NULL,
    perimetro_codigo VARCHAR(64) NOT NULL,
    ano SMALLINT NOT NULL CHECK (ano BETWEEN 1 AND 9999),
    mes SMALLINT CHECK (mes BETWEEN 1 AND 12),
    valor NUMERIC,
    fonte_id VARCHAR(64) NOT NULL REFERENCES esg_fontes_documentais(identificador),
    data_referencia DATE NOT NULL,
    situacao VARCHAR(16) NOT NULL DEFAULT 'rascunho'
        CHECK (situacao IN ('rascunho', 'em_analise', 'aprovado', 'rejeitado')),
    fonte_aprovacao_id VARCHAR(64) REFERENCES esg_fontes_documentais(identificador),
    aprovado_em TIMESTAMPTZ,
    aprovado_por_usuario_id INTEGER REFERENCES usuarios(id),
    revisao INTEGER NOT NULL DEFAULT 1 CHECK (revisao = 1),
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    CONSTRAINT fk_esg_registros_definicao_perimetro
        FOREIGN KEY (indicador_codigo, perimetro_codigo)
        REFERENCES esg_definicoes(codigo, perimetro_codigo),
    CONSTRAINT ck_esg_registros_data CHECK (
        EXTRACT(YEAR FROM data_referencia) = ano
        AND (mes IS NULL OR EXTRACT(MONTH FROM data_referencia) = mes)
    ),
    CONSTRAINT ck_esg_registros_finito CHECK (
        valor IS NULL OR valor::TEXT NOT IN ('NaN', 'Infinity', '-Infinity')
    ),
    CONSTRAINT ck_esg_registros_aprovacao CHECK (
        (situacao = 'aprovado' AND valor IS NOT NULL AND fonte_aprovacao_id IS NOT NULL
            AND aprovado_em IS NOT NULL AND aprovado_por_usuario_id IS NOT NULL)
        OR (situacao <> 'aprovado' AND fonte_aprovacao_id IS NULL
            AND aprovado_em IS NULL AND aprovado_por_usuario_id IS NULL)
    )
);

-- NULL distingue o ano completo; dois índices impedem duplicidade anual/mensal.
CREATE UNIQUE INDEX IF NOT EXISTS uq_esg_registros_anual
    ON esg_registros (indicador_codigo, ano) WHERE mes IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS uq_esg_registros_mensal
    ON esg_registros (indicador_codigo, ano, mes) WHERE mes IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_esg_registros_perimetro_situacao
    ON esg_registros (perimetro_codigo, situacao, ano, mes);

CREATE TABLE IF NOT EXISTS esg_evidencias (
    identificador UUID PRIMARY KEY,
    registro_id UUID NOT NULL REFERENCES esg_registros(identificador),
    tipo TEXT NOT NULL CHECK (BTRIM(tipo) <> ''),
    fonte_id VARCHAR(64) NOT NULL REFERENCES esg_fontes_documentais(identificador),
    observacoes TEXT CHECK (observacoes IS NULL OR BTRIM(observacoes) <> ''),
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id)
);
CREATE INDEX IF NOT EXISTS idx_esg_evidencias_registro ON esg_evidencias (registro_id);

COMMIT;
