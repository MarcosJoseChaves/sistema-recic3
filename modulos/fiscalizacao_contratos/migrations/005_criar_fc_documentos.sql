BEGIN;

CREATE UNIQUE INDEX IF NOT EXISTS uq_fc_aditivos_id_contrato_id
    ON fc_aditivos (id, contrato_id);

CREATE TABLE IF NOT EXISTS fc_documentos (
    id BIGSERIAL PRIMARY KEY,
    contrato_id BIGINT NOT NULL REFERENCES fc_contratos(id),
    aditivo_id BIGINT,
    categoria VARCHAR(50) NOT NULL,
    titulo VARCHAR(200) NOT NULL,
    descricao TEXT,
    nome_original VARCHAR(255) NOT NULL,
    armazenamento_provedor VARCHAR(30) NOT NULL DEFAULT 'cloudinary',
    armazenamento_chave VARCHAR(500) NOT NULL,
    armazenamento_versao BIGINT,
    mime_type VARCHAR(150) NOT NULL,
    extensao VARCHAR(10) NOT NULL,
    tamanho_bytes BIGINT NOT NULL,
    sha256 VARCHAR(64) NOT NULL,
    ativo BOOLEAN NOT NULL DEFAULT TRUE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL REFERENCES usuarios(id),
    atualizado_por_usuario_id INTEGER REFERENCES usuarios(id),
    CONSTRAINT fk_fc_documentos_aditivo_contrato FOREIGN KEY (aditivo_id, contrato_id)
        REFERENCES fc_aditivos(id, contrato_id),
    CONSTRAINT uq_fc_documentos_armazenamento_chave UNIQUE (armazenamento_chave),
    CONSTRAINT ck_fc_documentos_categoria CHECK (
        categoria IN (
            'Contrato',
            'Edital',
            'Termo de Referência',
            'Estudo Técnico Preliminar',
            'Proposta',
            'Planilha Orçamentária',
            'Ordem de Serviço',
            'Aditivo',
            'Apostilamento',
            'Garantia',
            'ART ou RRT',
            'Notificação',
            'Relatório',
            'Parecer',
            'Comprovante',
            'Outro'
        )
    ),
    CONSTRAINT ck_fc_documentos_titulo_preenchido CHECK (BTRIM(titulo) <> ''),
    CONSTRAINT ck_fc_documentos_provedor CHECK (
        armazenamento_provedor = 'cloudinary'
    ),
    CONSTRAINT ck_fc_documentos_tamanho_nao_negativo CHECK (tamanho_bytes >= 0),
    CONSTRAINT ck_fc_documentos_sha256 CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    CONSTRAINT ck_fc_documentos_extensao CHECK (
        extensao IN (
            'pdf', 'doc', 'docx', 'xls', 'xlsx', 'csv',
            'jpg', 'jpeg', 'png', 'odt', 'ods'
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_fc_documentos_contrato_ativo
    ON fc_documentos (contrato_id, ativo);

CREATE INDEX IF NOT EXISTS idx_fc_documentos_aditivo_ativo
    ON fc_documentos (aditivo_id, ativo);

CREATE INDEX IF NOT EXISTS idx_fc_documentos_categoria_ativo
    ON fc_documentos (categoria, ativo);

CREATE INDEX IF NOT EXISTS idx_fc_documentos_titulo
    ON fc_documentos (titulo);

COMMIT;
