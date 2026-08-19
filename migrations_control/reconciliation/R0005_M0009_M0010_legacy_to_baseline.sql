-- R0005 agrupada: reconcilia M0009-M0010 com backfill legado autorizado.
-- Requer R0001-R0004 e todos os prechecks P400-P453 aprovados.
-- O harness futuro deve abrir uma unica transacao e manter rollback integral em qualquer falha.
-- EXTRA_LEGADO PRESERVADO POR DECISAO NORMATIVA.

-- H2D11-CLASS-C: column|associados|id PATH=public.associados.id
-- H2D11-CLASS-C: sequence|associados_id_seq PATH=public.associados_id_seq
-- H2D11-CLASS-B: column|associados|numero PRECHECK=P401
-- H2D11-CLASS-B: column|associados|nome PRECHECK=P401
-- H2D11-CLASS-B: column|associados|cpf PRECHECK=P402
-- H2D11-CLASS-B: column|associados|data_nascimento PRECHECK=P403
-- H2D11-CLASS-B: column|associados|telefone PRECHECK=P401
-- H2D11-CLASS-B: column|associados|cep PRECHECK=P403
-- H2D11-CLASS-B: column|associados|logradouro PRECHECK=P401
-- H2D11-CLASS-B: column|associados|endereco_numero PRECHECK=P401
-- H2D11-CLASS-B: column|associados|bairro PRECHECK=P401
-- H2D11-CLASS-B: column|associados|cidade PRECHECK=P401
-- H2D11-CLASS-B: column|associados|uf PRECHECK=P404
-- H2D11-CLASS-B: column|associados|data_admissao PRECHECK=P403

-- H2D11-ABSENT MATERIALIZE: table|associado_associacao_vinculos
-- H2D11-ABSENT MATERIALIZE: table|associado_conta_documentos
-- H2D11-ABSENT MATERIALIZE: table|associado_contas_bancarias
-- H2D11-ABSENT MATERIALIZE: table|associado_eventos
-- H2D11-ABSENT MATERIALIZE: table|associado_uvr_vinculos
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|id
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|associado_id
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|associacao_id
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|tipo_vinculo
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|principal
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|inicio_data
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|fim_data
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|estado
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|motivo
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|solicitado_por_usuario_id
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|aprovado_por_usuario_id
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|criado_em
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|atualizado_em
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|versao_registro
-- H2D11-ABSENT MATERIALIZE: column|associado_associacao_vinculos|request_id
-- H2D11-ABSENT MATERIALIZE: column|associado_conta_documentos|id
-- H2D11-ABSENT MATERIALIZE: column|associado_conta_documentos|conta_bancaria_id
-- H2D11-ABSENT MATERIALIZE: column|associado_conta_documentos|documento_id
-- H2D11-ABSENT MATERIALIZE: column|associado_conta_documentos|categoria
-- H2D11-ABSENT MATERIALIZE: column|associado_conta_documentos|inicio_em
-- H2D11-ABSENT MATERIALIZE: column|associado_conta_documentos|fim_em
-- H2D11-ABSENT MATERIALIZE: column|associado_conta_documentos|criado_em
-- H2D11-ABSENT MATERIALIZE: column|associado_conta_documentos|criado_por_usuario_id
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|id
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|associado_id
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|finalidade
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|principal
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|instituicao_codigo
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|instituicao_nome
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|agencia
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|agencia_digito
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|conta
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|conta_digito
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|tipo_conta
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|chave_pix
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|tipo_chave_pix
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|titular_nome
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|titular_documento
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|estado
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|inicio_data
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|fim_data
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|criado_em
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|atualizado_em
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|criado_por_usuario_id
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|atualizado_por_usuario_id
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|versao_registro
-- H2D11-ABSENT MATERIALIZE: column|associado_contas_bancarias|inativado_em
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|id
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|associado_id
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|tipo_evento
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|estado_anterior
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|estado_novo
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|motivo
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|fotografia
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|versao_formato
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|criado_em
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|criado_por_usuario_id
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|request_id
-- H2D11-ABSENT MATERIALIZE: column|associado_eventos|origem
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|id
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|associado_id
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|uvr_id
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|tipo_vinculo
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|principal
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|inicio_data
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|fim_data
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|estado
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|motivo
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|solicitado_por_usuario_id
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|aprovado_por_usuario_id
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|criado_em
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|atualizado_em
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|versao_registro
-- H2D11-ABSENT MATERIALIZE: column|associado_uvr_vinculos|request_id
-- H2D11-ABSENT MATERIALIZE: column|associados|nome_normalizado
-- H2D11-ABSENT MATERIALIZE: column|associados|documento_alternativo
-- H2D11-ABSENT MATERIALIZE: column|associados|justificativa_sem_cpf
-- H2D11-ABSENT MATERIALIZE: column|associados|email
-- H2D11-ABSENT MATERIALIZE: column|associados|estado
-- H2D11-ABSENT MATERIALIZE: column|associados|condicao_regularizacao
-- H2D11-ABSENT MATERIALIZE: column|associados|data_desligamento
-- H2D11-ABSENT MATERIALIZE: column|associados|criado_em
-- H2D11-ABSENT MATERIALIZE: column|associados|atualizado_em
-- H2D11-ABSENT MATERIALIZE: column|associados|criado_por_usuario_id
-- H2D11-ABSENT MATERIALIZE: column|associados|atualizado_por_usuario_id
-- H2D11-ABSENT MATERIALIZE: column|associados|versao_registro
-- H2D11-ABSENT MATERIALIZE: constraint|associado_associacao_vinculos|ck_associado_assoc_vinculos__estado
-- H2D11-ABSENT MATERIALIZE: constraint|associado_associacao_vinculos|ck_associado_assoc_vinculos__estado_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_associacao_vinculos|ck_associado_assoc_vinculos__periodo
-- H2D11-ABSENT MATERIALIZE: constraint|associado_associacao_vinculos|ck_associado_assoc_vinculos__tipo_vinculo_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_associacao_vinculos|ck_associado_assoc_vinculos__versao_registro_positivo
-- H2D11-ABSENT MATERIALIZE: constraint|associado_associacao_vinculos|fk_associado_assoc_vinculos__aprovado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_associacao_vinculos|fk_associado_assoc_vinculos__assoc_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_associacao_vinculos|fk_associado_assoc_vinculos__associado_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_associacao_vinculos|fk_associado_assoc_vinculos__solicitado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_associacao_vinculos|pk_associado_assoc_vinculos
-- H2D11-ABSENT MATERIALIZE: constraint|associado_conta_documentos|ck_associado_conta_documentos__categoria_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_conta_documentos|ck_associado_conta_documentos__periodo
-- H2D11-ABSENT MATERIALIZE: constraint|associado_conta_documentos|fk_associado_conta_documentos__conta_bancaria_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_conta_documentos|fk_associado_conta_documentos__criado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_conta_documentos|fk_associado_conta_documentos__documento_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_conta_documentos|pk_associado_conta_documentos
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__agencia_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__conta_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__estado
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__estado_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__finalidade_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__instituicao_codigo_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__instituicao_nome_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__periodo
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__tipo_conta_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__titular_nome_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|ck_associado_contas_bancarias__versao_registro_positivo
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|fk_associado_contas_bancarias__associado_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|fk_associado_contas_bancarias__atualizado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|fk_associado_contas_bancarias__criado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_contas_bancarias|pk_associado_contas_bancarias
-- H2D11-ABSENT MATERIALIZE: constraint|associado_eventos|ck_associado_eventos__origem_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_eventos|ck_associado_eventos__tipo_evento_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_eventos|ck_associado_eventos__versao_formato_positivo
-- H2D11-ABSENT MATERIALIZE: constraint|associado_eventos|fk_associado_eventos__associado_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_eventos|fk_associado_eventos__criado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_eventos|pk_associado_eventos
-- H2D11-ABSENT MATERIALIZE: constraint|associado_uvr_vinculos|ck_associado_uvr_vinculos__estado
-- H2D11-ABSENT MATERIALIZE: constraint|associado_uvr_vinculos|ck_associado_uvr_vinculos__estado_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_uvr_vinculos|ck_associado_uvr_vinculos__periodo
-- H2D11-ABSENT MATERIALIZE: constraint|associado_uvr_vinculos|ck_associado_uvr_vinculos__tipo_vinculo_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associado_uvr_vinculos|ck_associado_uvr_vinculos__versao_registro_positivo
-- H2D11-ABSENT MATERIALIZE: constraint|associado_uvr_vinculos|fk_associado_uvr_vinculos__aprovado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_uvr_vinculos|fk_associado_uvr_vinculos__associado_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_uvr_vinculos|fk_associado_uvr_vinculos__solicitado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_uvr_vinculos|fk_associado_uvr_vinculos__uvr_id
-- H2D11-ABSENT MATERIALIZE: constraint|associado_uvr_vinculos|pk_associado_uvr_vinculos
-- H2D11-ABSENT MATERIALIZE: constraint|associados|ck_associados__estado
-- H2D11-ABSENT MATERIALIZE: constraint|associados|ck_associados__estado_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associados|ck_associados__nome_normalizado_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associados|ck_associados__nome_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associados|ck_associados__numero_preenchido
-- H2D11-ABSENT MATERIALIZE: constraint|associados|ck_associados__regra_265
-- H2D11-ABSENT MATERIALIZE: constraint|associados|ck_associados__versao_registro_positivo
-- H2D11-ABSENT MATERIALIZE: constraint|associados|fk_associados__atualizado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: constraint|associados|fk_associados__criado_por_usr_id
-- H2D11-ABSENT PRESERVE_EQUIVALENT: constraint|associados|pk_associados
-- H2D11-ABSENT MATERIALIZE: constraint|associados|uq_associados__numero
-- H2D11-ABSENT MATERIALIZE: index|associado_associacao_vinculos|ix_associado_assoc_vinculos__aprovado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: index|associado_associacao_vinculos|ix_associado_assoc_vinculos__assoc_id
-- H2D11-ABSENT MATERIALIZE: index|associado_associacao_vinculos|ix_associado_assoc_vinculos__solicitado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: index|associado_associacao_vinculos|pk_associado_assoc_vinculos
-- H2D11-ABSENT MATERIALIZE: index|associado_associacao_vinculos|uq_associado_assoc_vinculos__associado_id
-- H2D11-ABSENT MATERIALIZE: index|associado_conta_documentos|ix_associado_conta_documentos__conta_bancaria_id
-- H2D11-ABSENT MATERIALIZE: index|associado_conta_documentos|ix_associado_conta_documentos__criado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: index|associado_conta_documentos|ix_associado_conta_documentos__documento_id
-- H2D11-ABSENT MATERIALIZE: index|associado_conta_documentos|pk_associado_conta_documentos
-- H2D11-ABSENT MATERIALIZE: index|associado_contas_bancarias|ix_associado_contas_bancarias__atualizado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: index|associado_contas_bancarias|ix_associado_contas_bancarias__criado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: index|associado_contas_bancarias|pk_associado_contas_bancarias
-- H2D11-ABSENT MATERIALIZE: index|associado_contas_bancarias|uq_associado_contas_bancarias__associado_id_finalidade
-- H2D11-ABSENT MATERIALIZE: index|associado_eventos|ix_associado_eventos__associado_id
-- H2D11-ABSENT MATERIALIZE: index|associado_eventos|ix_associado_eventos__criado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: index|associado_eventos|pk_associado_eventos
-- H2D11-ABSENT MATERIALIZE: index|associado_uvr_vinculos|ix_associado_uvr_vinculos__aprovado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: index|associado_uvr_vinculos|ix_associado_uvr_vinculos__solicitado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: index|associado_uvr_vinculos|ix_associado_uvr_vinculos__uvr_id
-- H2D11-ABSENT MATERIALIZE: index|associado_uvr_vinculos|pk_associado_uvr_vinculos
-- H2D11-ABSENT MATERIALIZE: index|associado_uvr_vinculos|uq_associado_uvr_vinculos__associado_id
-- H2D11-ABSENT MATERIALIZE: index|associados|ix_associados__atualizado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: index|associados|ix_associados__criado_por_usr_id
-- H2D11-ABSENT MATERIALIZE: index|associados|ix_associados__estado_nome_normalizado
-- H2D11-ABSENT PRESERVE_EQUIVALENT: index|associados|pk_associados
-- H2D11-ABSENT MATERIALIZE: index|associados|uq_associados__cpf
-- H2D11-ABSENT MATERIALIZE: index|associados|uq_associados__numero
-- H2D11-ABSENT MATERIALIZE: sequence|associado_associacao_vinculos_id_seq
-- H2D11-ABSENT MATERIALIZE: sequence|associado_conta_documentos_id_seq
-- H2D11-ABSENT MATERIALIZE: sequence|associado_contas_bancarias_id_seq
-- H2D11-ABSENT MATERIALIZE: sequence|associado_eventos_id_seq
-- H2D11-ABSENT MATERIALIZE: sequence|associado_uvr_vinculos_id_seq

-- M0009
-- A PK serial legada e seu indice sao equivalentes funcionais e nao sao recriados.
LOCK TABLE public.usuarios, public.associados IN SHARE ROW EXCLUSIVE MODE;

ALTER TABLE public.associados
    ALTER COLUMN numero TYPE TEXT USING numero::TEXT,
    ALTER COLUMN nome TYPE TEXT USING nome::TEXT,
    ALTER COLUMN cpf DROP NOT NULL,
    ALTER COLUMN data_nascimento DROP NOT NULL,
    ALTER COLUMN telefone TYPE TEXT USING telefone::TEXT,
    ALTER COLUMN telefone DROP NOT NULL,
    ALTER COLUMN cep DROP NOT NULL,
    ALTER COLUMN logradouro TYPE TEXT USING logradouro::TEXT,
    ALTER COLUMN endereco_numero TYPE TEXT USING endereco_numero::TEXT,
    ALTER COLUMN bairro TYPE TEXT USING bairro::TEXT,
    ALTER COLUMN cidade TYPE TEXT USING cidade::TEXT,
    ALTER COLUMN uf TYPE CHAR(2) USING uf::CHAR(2),
    ALTER COLUMN data_admissao DROP NOT NULL;

ALTER TABLE public.associados
    ADD COLUMN nome_normalizado TEXT,
    ADD COLUMN documento_alternativo TEXT,
    ADD COLUMN justificativa_sem_cpf TEXT,
    ADD COLUMN email TEXT,
    ADD COLUMN estado TEXT NOT NULL DEFAULT 'RASCUNHO',
    ADD COLUMN condicao_regularizacao TEXT,
    ADD COLUMN data_desligamento DATE,
    ADD COLUMN criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN criado_por_usuario_id INTEGER,
    ADD COLUMN atualizado_por_usuario_id INTEGER,
    ADD COLUMN versao_registro INTEGER NOT NULL DEFAULT 1;

-- backfill legado autorizado
DO $r0005_backfill$
DECLARE
    v_usuario_id INTEGER;
    v_candidatos INTEGER;
BEGIN
    SELECT count(*), min(id)
      INTO v_candidatos, v_usuario_id
      FROM public.usuarios
     WHERE username = 'migracao_dados_legados'
        OR username_normalizado = 'migracao_dados_legados';

    IF v_candidatos = 0 THEN
        INSERT INTO public.usuarios (
            username,
            username_normalizado,
            password_hash,
            nome_completo,
            email,
            email_normalizado,
            estado,
            exige_troca_senha,
            criado_por_usuario_id,
            inativado_em,
            inativado_por_usuario_id,
            role,
            uvr_acesso,
            ativo
        )
        VALUES (
            'migracao_dados_legados',
            'migracao_dados_legados',
            'pbkdf2:sha256:1000000$0caPhVgPLfRuxvRAGu8srw$80dab8fa229f014098e1b004ed61a1065b6f324a2c2dc30ec2d01c5f9351097c',
            'Migração de dados legados',
            NULL,
            NULL,
            'BLOQUEADO',
            TRUE,
            NULL,
            CURRENT_TIMESTAMP,
            NULL,
            'migracao',
            NULL,
            FALSE
        )
        RETURNING id INTO v_usuario_id;
    ELSIF v_candidatos = 1 THEN
        v_usuario_id := NULL;
        SELECT id
          INTO v_usuario_id
          FROM public.usuarios AS usuario
         WHERE (usuario.username = 'migracao_dados_legados'
             OR usuario.username_normalizado = 'migracao_dados_legados')
           AND usuario.username = 'migracao_dados_legados'
           AND usuario.username_normalizado = 'migracao_dados_legados'
           AND usuario.nome_completo = 'Migração de dados legados'
           AND usuario.password_hash = 'pbkdf2:sha256:1000000$0caPhVgPLfRuxvRAGu8srw$80dab8fa229f014098e1b004ed61a1065b6f324a2c2dc30ec2d01c5f9351097c'
           AND usuario.email IS NULL
           AND usuario.email_normalizado IS NULL
           AND usuario.estado = 'BLOQUEADO'
           AND usuario.exige_troca_senha = TRUE
           AND usuario.ativo = FALSE
           AND usuario.role = 'migracao'
           AND usuario.uvr_acesso IS NULL
           AND NOT EXISTS (
               SELECT 1
                 FROM public.auth_usuario_perfis AS vinculo
                WHERE vinculo.usuario_id = usuario.id
           );

        IF v_usuario_id IS NULL THEN
            RAISE EXCEPTION 'R0005/U3: colisao insegura com a identidade tecnica canonica';
        END IF;
    ELSE
        RAISE EXCEPTION 'R0005/U4: multiplos candidatos para a identidade tecnica canonica';
    END IF;

    UPDATE public.associados
       SET nome_normalizado = lower(unaccent(btrim(nome))),
           criado_por_usuario_id = v_usuario_id,
           atualizado_por_usuario_id = v_usuario_id;
END
$r0005_backfill$;

ALTER TABLE public.associados
    ALTER COLUMN nome_normalizado SET NOT NULL,
    ALTER COLUMN criado_por_usuario_id SET NOT NULL,
    ALTER COLUMN atualizado_por_usuario_id SET NOT NULL;

CREATE TABLE associado_associacao_vinculos (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    associado_id BIGINT NOT NULL,
    associacao_id BIGINT NOT NULL,
    tipo_vinculo TEXT NOT NULL,
    principal BOOLEAN NOT NULL DEFAULT FALSE,
    inicio_data DATE NOT NULL,
    fim_data DATE,
    estado TEXT NOT NULL DEFAULT 'PENDENTE',
    motivo TEXT,
    solicitado_por_usuario_id INTEGER NOT NULL,
    aprovado_por_usuario_id INTEGER NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    versao_registro INTEGER NOT NULL DEFAULT 1,
    request_id UUID NOT NULL
);

CREATE TABLE associado_uvr_vinculos (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    associado_id BIGINT NOT NULL,
    uvr_id BIGINT NOT NULL,
    tipo_vinculo TEXT NOT NULL,
    principal BOOLEAN NOT NULL DEFAULT FALSE,
    inicio_data DATE NOT NULL,
    fim_data DATE,
    estado TEXT NOT NULL DEFAULT 'PENDENTE',
    motivo TEXT,
    solicitado_por_usuario_id INTEGER NOT NULL,
    aprovado_por_usuario_id INTEGER NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    versao_registro INTEGER NOT NULL DEFAULT 1,
    request_id UUID NOT NULL
);

CREATE TABLE associado_contas_bancarias (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    associado_id BIGINT NOT NULL,
    finalidade TEXT NOT NULL,
    principal BOOLEAN NOT NULL DEFAULT FALSE,
    instituicao_codigo TEXT NOT NULL,
    instituicao_nome TEXT NOT NULL,
    agencia TEXT NOT NULL,
    agencia_digito TEXT,
    conta TEXT NOT NULL,
    conta_digito TEXT,
    tipo_conta TEXT NOT NULL,
    chave_pix TEXT,
    tipo_chave_pix TEXT,
    titular_nome TEXT NOT NULL,
    titular_documento TEXT,
    estado TEXT NOT NULL DEFAULT 'PENDENTE',
    inicio_data DATE NOT NULL,
    fim_data DATE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL,
    atualizado_por_usuario_id INTEGER NOT NULL,
    versao_registro INTEGER NOT NULL DEFAULT 1,
    inativado_em TIMESTAMPTZ
);

CREATE TABLE associado_eventos (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    associado_id BIGINT NOT NULL,
    tipo_evento TEXT NOT NULL,
    estado_anterior TEXT,
    estado_novo TEXT,
    motivo TEXT,
    fotografia JSONB NOT NULL,
    versao_formato INTEGER NOT NULL DEFAULT 1,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL,
    request_id UUID NOT NULL,
    origem TEXT NOT NULL
);

CREATE TABLE associado_conta_documentos (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    conta_bancaria_id BIGINT NOT NULL,
    documento_id BIGINT NOT NULL,
    categoria TEXT NOT NULL,
    inicio_em TIMESTAMPTZ NOT NULL,
    fim_em TIMESTAMPTZ,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL
);

ALTER TABLE associado_associacao_vinculos
    ADD CONSTRAINT pk_associado_assoc_vinculos PRIMARY KEY (id);

ALTER TABLE associado_uvr_vinculos
    ADD CONSTRAINT pk_associado_uvr_vinculos PRIMARY KEY (id);

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT pk_associado_contas_bancarias PRIMARY KEY (id);

ALTER TABLE associado_eventos
    ADD CONSTRAINT pk_associado_eventos PRIMARY KEY (id);

ALTER TABLE associado_conta_documentos
    ADD CONSTRAINT pk_associado_conta_documentos PRIMARY KEY (id);

ALTER TABLE associados
    ADD CONSTRAINT uq_associados__numero UNIQUE (numero);

CREATE UNIQUE INDEX uq_associados__cpf
    ON associados (cpf)
    WHERE cpf IS NOT NULL;

CREATE UNIQUE INDEX uq_associado_assoc_vinculos__associado_id
    ON associado_associacao_vinculos (associado_id)
    WHERE principal = TRUE AND estado = 'ATIVO' AND fim_data IS NULL;

CREATE UNIQUE INDEX uq_associado_uvr_vinculos__associado_id
    ON associado_uvr_vinculos (associado_id)
    WHERE principal = TRUE AND estado = 'ATIVO' AND fim_data IS NULL;

CREATE UNIQUE INDEX uq_associado_contas_bancarias__associado_id_finalidade
    ON associado_contas_bancarias (associado_id, finalidade)
    WHERE principal = TRUE AND estado = 'ATIVA' AND fim_data IS NULL;

ALTER TABLE associados
    ADD CONSTRAINT ck_associados__numero_preenchido CHECK (btrim(numero) <> '');

ALTER TABLE associados
    ADD CONSTRAINT ck_associados__nome_preenchido CHECK (btrim(nome) <> '');

ALTER TABLE associados
    ADD CONSTRAINT ck_associados__nome_normalizado_preenchido CHECK (btrim(nome_normalizado) <> '');

ALTER TABLE associados
    ADD CONSTRAINT ck_associados__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE associados
    ADD CONSTRAINT ck_associados__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE associados
    ADD CONSTRAINT ck_associados__estado CHECK (estado IN ('RASCUNHO', 'ATIVO', 'INATIVO', 'DESLIGADO'));

ALTER TABLE associado_associacao_vinculos
    ADD CONSTRAINT ck_associado_assoc_vinculos__tipo_vinculo_preenchido CHECK (btrim(tipo_vinculo) <> '');

ALTER TABLE associado_associacao_vinculos
    ADD CONSTRAINT ck_associado_assoc_vinculos__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE associado_associacao_vinculos
    ADD CONSTRAINT ck_associado_assoc_vinculos__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE associado_associacao_vinculos
    ADD CONSTRAINT ck_associado_assoc_vinculos__estado CHECK (estado IN ('PENDENTE', 'ATIVO', 'ENCERRADO', 'REJEITADO'));

ALTER TABLE associado_associacao_vinculos
    ADD CONSTRAINT ck_associado_assoc_vinculos__periodo CHECK (fim_data IS NULL OR fim_data >= inicio_data);

ALTER TABLE associado_uvr_vinculos
    ADD CONSTRAINT ck_associado_uvr_vinculos__tipo_vinculo_preenchido CHECK (btrim(tipo_vinculo) <> '');

ALTER TABLE associado_uvr_vinculos
    ADD CONSTRAINT ck_associado_uvr_vinculos__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE associado_uvr_vinculos
    ADD CONSTRAINT ck_associado_uvr_vinculos__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE associado_uvr_vinculos
    ADD CONSTRAINT ck_associado_uvr_vinculos__estado CHECK (estado IN ('PENDENTE', 'ATIVO', 'ENCERRADO', 'REJEITADO'));

ALTER TABLE associado_uvr_vinculos
    ADD CONSTRAINT ck_associado_uvr_vinculos__periodo CHECK (fim_data IS NULL OR fim_data >= inicio_data);

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__finalidade_preenchido CHECK (btrim(finalidade) <> '');

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__instituicao_codigo_preenchido CHECK (btrim(instituicao_codigo) <> '');

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__instituicao_nome_preenchido CHECK (btrim(instituicao_nome) <> '');

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__agencia_preenchido CHECK (btrim(agencia) <> '');

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__conta_preenchido CHECK (btrim(conta) <> '');

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__tipo_conta_preenchido CHECK (btrim(tipo_conta) <> '');

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__titular_nome_preenchido CHECK (btrim(titular_nome) <> '');

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__estado CHECK (estado IN ('PENDENTE', 'ATIVA', 'INATIVA'));

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT ck_associado_contas_bancarias__periodo CHECK (fim_data IS NULL OR fim_data >= inicio_data);

ALTER TABLE associado_eventos
    ADD CONSTRAINT ck_associado_eventos__tipo_evento_preenchido CHECK (btrim(tipo_evento) <> '');

ALTER TABLE associado_eventos
    ADD CONSTRAINT ck_associado_eventos__versao_formato_positivo CHECK (versao_formato > 0);

ALTER TABLE associado_eventos
    ADD CONSTRAINT ck_associado_eventos__origem_preenchido CHECK (btrim(origem) <> '');

ALTER TABLE associado_conta_documentos
    ADD CONSTRAINT ck_associado_conta_documentos__categoria_preenchido CHECK (btrim(categoria) <> '');

ALTER TABLE associado_conta_documentos
    ADD CONSTRAINT ck_associado_conta_documentos__periodo CHECK (fim_em IS NULL OR fim_em >= inicio_em);

ALTER TABLE associados
    ADD CONSTRAINT ck_associados__regra_265 CHECK ((cpf IS NOT NULL AND cpf ~ '^[0-9]{11}$' AND justificativa_sem_cpf IS NULL) OR (cpf IS NULL AND btrim(coalesce(justificativa_sem_cpf,'')) <> ''));

ALTER TABLE associado_associacao_vinculos
    ADD CONSTRAINT fk_associado_assoc_vinculos__aprovado_por_usr_id FOREIGN KEY (aprovado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_associacao_vinculos
    ADD CONSTRAINT fk_associado_assoc_vinculos__assoc_id FOREIGN KEY (associacao_id)
    REFERENCES associacoes (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_associacao_vinculos
    ADD CONSTRAINT fk_associado_assoc_vinculos__associado_id FOREIGN KEY (associado_id)
    REFERENCES associados (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_associacao_vinculos
    ADD CONSTRAINT fk_associado_assoc_vinculos__solicitado_por_usr_id FOREIGN KEY (solicitado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_conta_documentos
    ADD CONSTRAINT fk_associado_conta_documentos__conta_bancaria_id FOREIGN KEY (conta_bancaria_id)
    REFERENCES associado_contas_bancarias (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_conta_documentos
    ADD CONSTRAINT fk_associado_conta_documentos__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_conta_documentos
    ADD CONSTRAINT fk_associado_conta_documentos__documento_id FOREIGN KEY (documento_id)
    REFERENCES documentos_privados (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT fk_associado_contas_bancarias__associado_id FOREIGN KEY (associado_id)
    REFERENCES associados (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT fk_associado_contas_bancarias__atualizado_por_usr_id FOREIGN KEY (atualizado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_contas_bancarias
    ADD CONSTRAINT fk_associado_contas_bancarias__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_eventos
    ADD CONSTRAINT fk_associado_eventos__associado_id FOREIGN KEY (associado_id)
    REFERENCES associados (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_eventos
    ADD CONSTRAINT fk_associado_eventos__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_uvr_vinculos
    ADD CONSTRAINT fk_associado_uvr_vinculos__aprovado_por_usr_id FOREIGN KEY (aprovado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_uvr_vinculos
    ADD CONSTRAINT fk_associado_uvr_vinculos__associado_id FOREIGN KEY (associado_id)
    REFERENCES associados (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_uvr_vinculos
    ADD CONSTRAINT fk_associado_uvr_vinculos__solicitado_por_usr_id FOREIGN KEY (solicitado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associado_uvr_vinculos
    ADD CONSTRAINT fk_associado_uvr_vinculos__uvr_id FOREIGN KEY (uvr_id)
    REFERENCES uvrs (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associados
    ADD CONSTRAINT fk_associados__atualizado_por_usr_id FOREIGN KEY (atualizado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE associados
    ADD CONSTRAINT fk_associados__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

CREATE INDEX ix_associado_assoc_vinculos__aprovado_por_usr_id
    ON associado_associacao_vinculos (aprovado_por_usuario_id);

CREATE INDEX ix_associado_assoc_vinculos__assoc_id
    ON associado_associacao_vinculos (associacao_id);

CREATE INDEX ix_associado_assoc_vinculos__solicitado_por_usr_id
    ON associado_associacao_vinculos (solicitado_por_usuario_id);

CREATE INDEX ix_associado_conta_documentos__conta_bancaria_id
    ON associado_conta_documentos (conta_bancaria_id);

CREATE INDEX ix_associado_conta_documentos__criado_por_usr_id
    ON associado_conta_documentos (criado_por_usuario_id);

CREATE INDEX ix_associado_conta_documentos__documento_id
    ON associado_conta_documentos (documento_id);

CREATE INDEX ix_associado_contas_bancarias__atualizado_por_usr_id
    ON associado_contas_bancarias (atualizado_por_usuario_id);

CREATE INDEX ix_associado_contas_bancarias__criado_por_usr_id
    ON associado_contas_bancarias (criado_por_usuario_id);

CREATE INDEX ix_associado_eventos__associado_id
    ON associado_eventos (associado_id);

CREATE INDEX ix_associado_eventos__criado_por_usr_id
    ON associado_eventos (criado_por_usuario_id);

CREATE INDEX ix_associado_uvr_vinculos__aprovado_por_usr_id
    ON associado_uvr_vinculos (aprovado_por_usuario_id);

CREATE INDEX ix_associado_uvr_vinculos__solicitado_por_usr_id
    ON associado_uvr_vinculos (solicitado_por_usuario_id);

CREATE INDEX ix_associado_uvr_vinculos__uvr_id
    ON associado_uvr_vinculos (uvr_id);

CREATE INDEX ix_associados__atualizado_por_usr_id
    ON associados (atualizado_por_usuario_id);

CREATE INDEX ix_associados__criado_por_usr_id
    ON associados (criado_por_usuario_id);

CREATE INDEX ix_associados__estado_nome_normalizado
    ON associados (estado, nome_normalizado);

-- M0010
-- M0010: catalogo conforme baseline H2C.3E

CREATE TABLE unidades_medida (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    codigo TEXT NOT NULL,
    nome TEXT NOT NULL,
    nome_normalizado TEXT NOT NULL,
    simbolo TEXT,
    casas_decimais INTEGER NOT NULL,
    estado TEXT NOT NULL DEFAULT 'ATIVO',
    protegido BOOLEAN NOT NULL DEFAULT FALSE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    versao_registro INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE catalogo_grupos (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    codigo TEXT NOT NULL,
    nome TEXT NOT NULL,
    nome_normalizado TEXT NOT NULL,
    descricao TEXT,
    estado TEXT NOT NULL DEFAULT 'ATIVO',
    ordem INTEGER NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL,
    atualizado_por_usuario_id INTEGER NOT NULL,
    versao_registro INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE catalogo_subgrupos (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    grupo_id BIGINT NOT NULL,
    codigo TEXT NOT NULL,
    nome TEXT NOT NULL,
    nome_normalizado TEXT NOT NULL,
    descricao TEXT,
    estado TEXT NOT NULL DEFAULT 'ATIVO',
    ordem INTEGER NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL,
    atualizado_por_usuario_id INTEGER NOT NULL,
    versao_registro INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE catalogo_itens (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    subgrupo_id BIGINT NOT NULL,
    unidade_padrao_id BIGINT NOT NULL,
    codigo TEXT NOT NULL,
    nome TEXT NOT NULL,
    nome_normalizado TEXT NOT NULL,
    descricao TEXT,
    tipo TEXT NOT NULL,
    estado TEXT NOT NULL DEFAULT 'ATIVO',
    inicio_data DATE NOT NULL,
    fim_data DATE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    atualizado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL,
    atualizado_por_usuario_id INTEGER NOT NULL,
    versao_registro INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE catalogo_aliases (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    entidade_tipo TEXT NOT NULL,
    grupo_id BIGINT,
    subgrupo_id BIGINT,
    produto_servico_id BIGINT,
    alias TEXT NOT NULL,
    alias_normalizado TEXT NOT NULL,
    origem TEXT NOT NULL,
    estado TEXT NOT NULL DEFAULT 'ATIVO',
    inicio_data DATE NOT NULL,
    fim_data DATE,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE catalogo_substituicoes (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    item_origem_id BIGINT NOT NULL,
    item_destino_id BIGINT NOT NULL,
    motivo TEXT,
    inicio_data DATE NOT NULL,
    fim_data DATE,
    estado TEXT NOT NULL DEFAULT 'ATIVA',
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL,
    request_id UUID NOT NULL,
    versao_registro INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE catalogo_eventos (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    entidade_tipo TEXT NOT NULL,
    grupo_id BIGINT,
    subgrupo_id BIGINT,
    produto_servico_id BIGINT,
    tipo_evento TEXT NOT NULL,
    estado_anterior TEXT,
    estado_novo TEXT,
    motivo TEXT,
    fotografia JSONB NOT NULL,
    versao_formato INTEGER NOT NULL DEFAULT 1,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL,
    request_id UUID NOT NULL,
    origem TEXT NOT NULL
);

ALTER TABLE unidades_medida
    ADD CONSTRAINT pk_unidades_medida PRIMARY KEY (id);

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT pk_catalogo_grupos PRIMARY KEY (id);

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT pk_catalogo_subgrupos PRIMARY KEY (id);

ALTER TABLE catalogo_itens
    ADD CONSTRAINT pk_catalogo_itens PRIMARY KEY (id);

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT pk_catalogo_aliases PRIMARY KEY (id);

ALTER TABLE catalogo_substituicoes
    ADD CONSTRAINT pk_catalogo_substituicoes PRIMARY KEY (id);

ALTER TABLE catalogo_eventos
    ADD CONSTRAINT pk_catalogo_eventos PRIMARY KEY (id);

ALTER TABLE unidades_medida
    ADD CONSTRAINT uq_unidades_medida__codigo UNIQUE (codigo);

ALTER TABLE unidades_medida
    ADD CONSTRAINT uq_unidades_medida__nome_normalizado UNIQUE (nome_normalizado);

ALTER TABLE unidades_medida
    ADD CONSTRAINT uq_unidades_medida__simbolo UNIQUE (simbolo);

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT uq_catalogo_grupos__codigo UNIQUE (codigo);

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT uq_catalogo_grupos__nome_normalizado UNIQUE (nome_normalizado);

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT uq_catalogo_subgrupos__codigo UNIQUE (codigo);

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT uq_catalogo_subgrupos__grupo_id_nome_normalizado UNIQUE (grupo_id, nome_normalizado);

ALTER TABLE catalogo_itens
    ADD CONSTRAINT uq_catalogo_itens__codigo UNIQUE (codigo);

ALTER TABLE catalogo_itens
    ADD CONSTRAINT uq_catalogo_itens__subgrupo_id_nome_normalizado UNIQUE (subgrupo_id, nome_normalizado);

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT uq_catalogo_aliases__alias_normalizado UNIQUE (alias_normalizado);

CREATE UNIQUE INDEX uq_catalogo_substituicoes__item_origem_id
    ON catalogo_substituicoes (item_origem_id)
    WHERE estado = 'ATIVA' AND fim_data IS NULL;

ALTER TABLE unidades_medida
    ADD CONSTRAINT ck_unidades_medida__codigo_preenchido CHECK (btrim(codigo) <> '');

ALTER TABLE unidades_medida
    ADD CONSTRAINT ck_unidades_medida__nome_preenchido CHECK (btrim(nome) <> '');

ALTER TABLE unidades_medida
    ADD CONSTRAINT ck_unidades_medida__nome_normalizado_preenchido CHECK (btrim(nome_normalizado) <> '');

ALTER TABLE unidades_medida
    ADD CONSTRAINT ck_unidades_medida__casas_decimais_positivo CHECK (casas_decimais BETWEEN 0 AND 8);

ALTER TABLE unidades_medida
    ADD CONSTRAINT ck_unidades_medida__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE unidades_medida
    ADD CONSTRAINT ck_unidades_medida__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE unidades_medida
    ADD CONSTRAINT ck_unidades_medida__estado CHECK (estado IN ('ATIVO', 'INATIVO'));

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT ck_catalogo_grupos__codigo_preenchido CHECK (btrim(codigo) <> '');

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT ck_catalogo_grupos__nome_preenchido CHECK (btrim(nome) <> '');

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT ck_catalogo_grupos__nome_normalizado_preenchido CHECK (btrim(nome_normalizado) <> '');

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT ck_catalogo_grupos__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT ck_catalogo_grupos__ordem_positivo CHECK (ordem > 0);

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT ck_catalogo_grupos__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT ck_catalogo_grupos__estado CHECK (estado IN ('ATIVO', 'INATIVO'));

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT ck_catalogo_subgrupos__codigo_preenchido CHECK (btrim(codigo) <> '');

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT ck_catalogo_subgrupos__nome_preenchido CHECK (btrim(nome) <> '');

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT ck_catalogo_subgrupos__nome_normalizado_preenchido CHECK (btrim(nome_normalizado) <> '');

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT ck_catalogo_subgrupos__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT ck_catalogo_subgrupos__ordem_positivo CHECK (ordem > 0);

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT ck_catalogo_subgrupos__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT ck_catalogo_subgrupos__estado CHECK (estado IN ('ATIVO', 'INATIVO'));

ALTER TABLE catalogo_itens
    ADD CONSTRAINT ck_catalogo_itens__codigo_preenchido CHECK (btrim(codigo) <> '');

ALTER TABLE catalogo_itens
    ADD CONSTRAINT ck_catalogo_itens__nome_preenchido CHECK (btrim(nome) <> '');

ALTER TABLE catalogo_itens
    ADD CONSTRAINT ck_catalogo_itens__nome_normalizado_preenchido CHECK (btrim(nome_normalizado) <> '');

ALTER TABLE catalogo_itens
    ADD CONSTRAINT ck_catalogo_itens__tipo_preenchido CHECK (btrim(tipo) <> '');

ALTER TABLE catalogo_itens
    ADD CONSTRAINT ck_catalogo_itens__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE catalogo_itens
    ADD CONSTRAINT ck_catalogo_itens__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE catalogo_itens
    ADD CONSTRAINT ck_catalogo_itens__estado CHECK (estado IN ('ATIVO', 'INATIVO'));

ALTER TABLE catalogo_itens
    ADD CONSTRAINT ck_catalogo_itens__periodo CHECK (fim_data IS NULL OR fim_data >= inicio_data);

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT ck_catalogo_aliases__entidade_tipo_preenchido CHECK (btrim(entidade_tipo) <> '');

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT ck_catalogo_aliases__alias_preenchido CHECK (btrim(alias) <> '');

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT ck_catalogo_aliases__alias_normalizado_preenchido CHECK (btrim(alias_normalizado) <> '');

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT ck_catalogo_aliases__origem_preenchido CHECK (btrim(origem) <> '');

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT ck_catalogo_aliases__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT ck_catalogo_aliases__estado CHECK (estado IN ('ATIVO', 'INATIVO'));

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT ck_catalogo_aliases__periodo CHECK (fim_data IS NULL OR fim_data >= inicio_data);

ALTER TABLE catalogo_substituicoes
    ADD CONSTRAINT ck_catalogo_substituicoes__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE catalogo_substituicoes
    ADD CONSTRAINT ck_catalogo_substituicoes__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE catalogo_substituicoes
    ADD CONSTRAINT ck_catalogo_substituicoes__estado CHECK (estado IN ('ATIVA', 'ENCERRADA', 'INATIVA'));

ALTER TABLE catalogo_substituicoes
    ADD CONSTRAINT ck_catalogo_substituicoes__periodo CHECK (fim_data IS NULL OR fim_data >= inicio_data);

ALTER TABLE catalogo_eventos
    ADD CONSTRAINT ck_catalogo_eventos__entidade_tipo_preenchido CHECK (btrim(entidade_tipo) <> '');

ALTER TABLE catalogo_eventos
    ADD CONSTRAINT ck_catalogo_eventos__tipo_evento_preenchido CHECK (btrim(tipo_evento) <> '');

ALTER TABLE catalogo_eventos
    ADD CONSTRAINT ck_catalogo_eventos__versao_formato_positivo CHECK (versao_formato > 0);

ALTER TABLE catalogo_eventos
    ADD CONSTRAINT ck_catalogo_eventos__origem_preenchido CHECK (btrim(origem) <> '');

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT ck_catalogo_aliases__regra_266 CHECK (num_nonnulls(grupo_id,subgrupo_id,produto_servico_id)=1 AND ((entidade_tipo='GRUPO' AND grupo_id IS NOT NULL) OR (entidade_tipo='SUBGRUPO' AND subgrupo_id IS NOT NULL) OR (entidade_tipo='PRODUTO_SERVICO' AND produto_servico_id IS NOT NULL)));

ALTER TABLE catalogo_eventos
    ADD CONSTRAINT ck_catalogo_eventos__regra_267 CHECK (num_nonnulls(grupo_id,subgrupo_id,produto_servico_id)=1 AND ((entidade_tipo='GRUPO' AND grupo_id IS NOT NULL) OR (entidade_tipo='SUBGRUPO' AND subgrupo_id IS NOT NULL) OR (entidade_tipo='PRODUTO_SERVICO' AND produto_servico_id IS NOT NULL)));

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT fk_catalogo_aliases__grupo_id FOREIGN KEY (grupo_id)
    REFERENCES catalogo_grupos (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT fk_catalogo_aliases__produto_servico_id FOREIGN KEY (produto_servico_id)
    REFERENCES catalogo_itens (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_aliases
    ADD CONSTRAINT fk_catalogo_aliases__subgrupo_id FOREIGN KEY (subgrupo_id)
    REFERENCES catalogo_subgrupos (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_eventos
    ADD CONSTRAINT fk_catalogo_eventos__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_eventos
    ADD CONSTRAINT fk_catalogo_eventos__grupo_id FOREIGN KEY (grupo_id)
    REFERENCES catalogo_grupos (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_eventos
    ADD CONSTRAINT fk_catalogo_eventos__produto_servico_id FOREIGN KEY (produto_servico_id)
    REFERENCES catalogo_itens (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_eventos
    ADD CONSTRAINT fk_catalogo_eventos__subgrupo_id FOREIGN KEY (subgrupo_id)
    REFERENCES catalogo_subgrupos (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT fk_catalogo_grupos__atualizado_por_usr_id FOREIGN KEY (atualizado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_grupos
    ADD CONSTRAINT fk_catalogo_grupos__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_itens
    ADD CONSTRAINT fk_catalogo_itens__atualizado_por_usr_id FOREIGN KEY (atualizado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_itens
    ADD CONSTRAINT fk_catalogo_itens__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_itens
    ADD CONSTRAINT fk_catalogo_itens__subgrupo_id FOREIGN KEY (subgrupo_id)
    REFERENCES catalogo_subgrupos (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_itens
    ADD CONSTRAINT fk_catalogo_itens__unidade_padrao_id FOREIGN KEY (unidade_padrao_id)
    REFERENCES unidades_medida (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT fk_catalogo_subgrupos__atualizado_por_usr_id FOREIGN KEY (atualizado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT fk_catalogo_subgrupos__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_subgrupos
    ADD CONSTRAINT fk_catalogo_subgrupos__grupo_id FOREIGN KEY (grupo_id)
    REFERENCES catalogo_grupos (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_substituicoes
    ADD CONSTRAINT fk_catalogo_substituicoes__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_substituicoes
    ADD CONSTRAINT fk_catalogo_substituicoes__item_destino_id FOREIGN KEY (item_destino_id)
    REFERENCES catalogo_itens (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE catalogo_substituicoes
    ADD CONSTRAINT fk_catalogo_substituicoes__item_origem_id FOREIGN KEY (item_origem_id)
    REFERENCES catalogo_itens (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

CREATE INDEX ix_catalogo_aliases__grupo_id
    ON catalogo_aliases (grupo_id);

CREATE INDEX ix_catalogo_aliases__produto_servico_id
    ON catalogo_aliases (produto_servico_id);

CREATE INDEX ix_catalogo_aliases__subgrupo_id
    ON catalogo_aliases (subgrupo_id);

CREATE INDEX ix_catalogo_eventos__criado_por_usr_id
    ON catalogo_eventos (criado_por_usuario_id);

CREATE INDEX ix_catalogo_eventos__grupo_id
    ON catalogo_eventos (grupo_id);

CREATE INDEX ix_catalogo_eventos__produto_servico_id
    ON catalogo_eventos (produto_servico_id);

CREATE INDEX ix_catalogo_eventos__subgrupo_id
    ON catalogo_eventos (subgrupo_id);

CREATE INDEX ix_catalogo_grupos__atualizado_por_usr_id
    ON catalogo_grupos (atualizado_por_usuario_id);

CREATE INDEX ix_catalogo_grupos__criado_por_usr_id
    ON catalogo_grupos (criado_por_usuario_id);

CREATE INDEX ix_catalogo_itens__atualizado_por_usr_id
    ON catalogo_itens (atualizado_por_usuario_id);

CREATE INDEX ix_catalogo_itens__criado_por_usr_id
    ON catalogo_itens (criado_por_usuario_id);

CREATE INDEX ix_catalogo_itens__unidade_padrao_id
    ON catalogo_itens (unidade_padrao_id);

CREATE INDEX ix_catalogo_subgrupos__atualizado_por_usr_id
    ON catalogo_subgrupos (atualizado_por_usuario_id);

CREATE INDEX ix_catalogo_subgrupos__criado_por_usr_id
    ON catalogo_subgrupos (criado_por_usuario_id);

CREATE INDEX ix_catalogo_substituicoes__criado_por_usr_id
    ON catalogo_substituicoes (criado_por_usuario_id);

CREATE INDEX ix_catalogo_substituicoes__item_destino_id
    ON catalogo_substituicoes (item_destino_id);

CREATE INDEX ix_catalogo_itens__subgrupo_id_estado_nome_normalizado
    ON catalogo_itens (subgrupo_id, estado, nome_normalizado);
