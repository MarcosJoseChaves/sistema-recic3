-- R0007: reconcilia exclusivamente M0013 após R0001-R0006 validadas.
-- Requer P700-P713 = PASS. Não controla transação e não executa M0013 normativa.
-- H2D21-CLASS-C: column|solicitacoes_alteracao|id public.solicitacoes_alteracao.id
-- H2D21-CLASS-C: sequence|solicitacoes_alteracao_id_seq public.solicitacoes_alteracao_id_seq
-- H2D21-PK-NAME: constraint|solicitacoes_alteracao|pk_solicitacoes_alteracao -> solicitacoes_alteracao_pkey
-- H2D21-PK-NAME: index|solicitacoes_alteracao|pk_solicitacoes_alteracao -> solicitacoes_alteracao_pkey
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_aplicacoes|solicitacao_aplicacoes
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_aprovacoes|solicitacao_aprovacoes
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_associacoes|solicitacao_associacoes
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_associados|solicitacao_associados
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_catalogo_itens|solicitacao_catalogo_itens
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_documentos|solicitacao_documentos
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_eventos|solicitacao_eventos
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_mensagens|solicitacao_mensagens
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_patrimonios|solicitacao_patrimonios
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_transacoes|solicitacao_transacoes
-- H2D21-ABSENT MATERIALIZE: table|solicitacao_uvrs|solicitacao_uvrs
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|tentativa
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|aplicador_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|iniciada_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|concluida_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|versao_observada
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|versao_resultante
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|resultado
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|erro_codigo
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|erro_sanitizado
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|idempotencia_chave
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|request_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|fotografia_resultado
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aplicacoes|versao_formato
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|etapa
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|decisao
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|aprovador_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|justificativa
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|risco_observado
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|criado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|request_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|estado
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|revogada_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|revogada_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_aprovacoes|motivo_revogacao
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_associacoes|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_associacoes|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_associacoes|associacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_associacoes|criado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_associacoes|criado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_associados|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_associados|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_associados|associado_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_associados|criado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_associados|criado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_catalogo_itens|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_catalogo_itens|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_catalogo_itens|catalogo_item_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_catalogo_itens|criado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_catalogo_itens|criado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_documentos|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_documentos|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_documentos|documento_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_documentos|categoria
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_documentos|visibilidade
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_documentos|estado
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_documentos|criado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_documentos|criado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_documentos|request_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|tipo_evento
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|estado_anterior
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|estado_novo
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|justificativa
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|resultado
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|metadados
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|criado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|criado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|request_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|origem
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_eventos|versao_formato
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|mensagem
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|visibilidade
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|estado
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|criado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|criado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|editado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|editado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|versao_registro
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|request_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_mensagens|removido_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_patrimonios|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_patrimonios|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_patrimonios|patrimonio_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_patrimonios|criado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_patrimonios|criado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_transacoes|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_transacoes|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_transacoes|transacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_transacoes|criado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_transacoes|criado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_uvrs|id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_uvrs|solicitacao_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_uvrs|uvr_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_uvrs|criado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacao_uvrs|criado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|identificador_publico
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|tipo
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|modulo
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|objeto_tipo_logico
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|objeto_identificador_logico
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|solicitante_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|estado
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|risco
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|versao_esperada
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|fotografia_original
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|fotografia_proposta
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|fotografia_aprovada
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|fotografia_aplicada
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|formato_versao
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|justificativa
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|criada_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|enviada_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|concluida_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|atualizado_em
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|atualizado_por_usuario_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|versao_registro
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|request_id
-- H2D21-ABSENT MATERIALIZE: column|solicitacoes_alteracao|associacao_contexto_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aplicacoes|ck_sol_alt_aplicacoes__resultado_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aplicacoes|ck_sol_alt_aplicacoes__tentativa_positivo
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aplicacoes|ck_sol_alt_aplicacoes__versao_formato_positivo
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aplicacoes|fk_sol_alt_aplicacoes__aplicador_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aplicacoes|fk_sol_alt_aplicacoes__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aplicacoes|pk_sol_alt_aplicacoes
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aplicacoes|uq_sol_alt_aplicacoes__idempotencia_chave
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aprovacoes|ck_sol_alt_aprovacoes__decisao_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aprovacoes|ck_sol_alt_aprovacoes__estado
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aprovacoes|ck_sol_alt_aprovacoes__estado_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aprovacoes|ck_sol_alt_aprovacoes__etapa_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aprovacoes|fk_sol_alt_aprovacoes__aprovador_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aprovacoes|fk_sol_alt_aprovacoes__revogada_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aprovacoes|fk_sol_alt_aprovacoes__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_aprovacoes|pk_sol_alt_aprovacoes
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_associacoes|fk_sol_alt_associacoes__assoc_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_associacoes|fk_sol_alt_associacoes__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_associacoes|fk_sol_alt_associacoes__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_associacoes|pk_sol_alt_associacoes
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_associacoes|uq_sol_alt_associacoes__sol_alt_id_assoc_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_associados|fk_sol_alt_associados__associado_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_associados|fk_sol_alt_associados__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_associados|fk_sol_alt_associados__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_associados|pk_sol_alt_associados
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_associados|uq_sol_alt_associados__sol_alt_id_associado_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_catalogo_itens|fk_sol_alt_catalogo_itens__catalogo_item_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_catalogo_itens|fk_sol_alt_catalogo_itens__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_catalogo_itens|fk_sol_alt_catalogo_itens__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_catalogo_itens|pk_sol_alt_catalogo_itens
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_catalogo_itens|uq_sol_alt_catalogo_itens__sol_alt_id_catalogo_item_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_documentos|ck_sol_alt_documentos__categoria_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_documentos|ck_sol_alt_documentos__estado
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_documentos|ck_sol_alt_documentos__estado_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_documentos|ck_sol_alt_documentos__visibilidade_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_documentos|fk_sol_alt_documentos__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_documentos|fk_sol_alt_documentos__documento_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_documentos|fk_sol_alt_documentos__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_documentos|pk_sol_alt_documentos
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_eventos|ck_sol_alt_eventos__origem_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_eventos|ck_sol_alt_eventos__resultado_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_eventos|ck_sol_alt_eventos__tipo_evento_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_eventos|ck_sol_alt_eventos__versao_formato_positivo
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_eventos|fk_sol_alt_eventos__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_eventos|fk_sol_alt_eventos__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_eventos|pk_sol_alt_eventos
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_mensagens|ck_sol_alt_mensagens__estado
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_mensagens|ck_sol_alt_mensagens__estado_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_mensagens|ck_sol_alt_mensagens__mensagem_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_mensagens|ck_sol_alt_mensagens__versao_registro_positivo
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_mensagens|ck_sol_alt_mensagens__visibilidade_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_mensagens|fk_sol_alt_mensagens__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_mensagens|fk_sol_alt_mensagens__editado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_mensagens|fk_sol_alt_mensagens__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_mensagens|pk_sol_alt_mensagens
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_patrimonios|fk_sol_alt_patrimonios__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_patrimonios|fk_sol_alt_patrimonios__patr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_patrimonios|fk_sol_alt_patrimonios__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_patrimonios|pk_sol_alt_patrimonios
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_patrimonios|uq_sol_alt_patrimonios__sol_alt_id_patr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_transacoes|fk_sol_alt_transacoes__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_transacoes|fk_sol_alt_transacoes__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_transacoes|fk_sol_alt_transacoes__transacao_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_transacoes|pk_sol_alt_transacoes
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_transacoes|uq_sol_alt_transacoes__sol_alt_id_transacao_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_uvrs|fk_sol_alt_uvrs__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_uvrs|fk_sol_alt_uvrs__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_uvrs|fk_sol_alt_uvrs__uvr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_uvrs|pk_sol_alt_uvrs
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacao_uvrs|uq_sol_alt_uvrs__sol_alt_id_uvr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|ck_solicitacoes_alteracao__estado
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|ck_solicitacoes_alteracao__estado_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|ck_solicitacoes_alteracao__formato_versao_positivo
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|ck_solicitacoes_alteracao__modulo_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|ck_solicitacoes_alteracao__objeto_tipo_logico_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|ck_solicitacoes_alteracao__risco_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|ck_solicitacoes_alteracao__tipo_preenchido
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|ck_solicitacoes_alteracao__versao_registro_positivo
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|fk_solicitacoes_alteracao__assoc_contexto_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|fk_solicitacoes_alteracao__atualizado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|fk_solicitacoes_alteracao__solicitante_usr_id
-- H2D21-ABSENT PRESERVE_EQUIVALENT: constraint|solicitacoes_alteracao|pk_solicitacoes_alteracao
-- H2D21-ABSENT MATERIALIZE: constraint|solicitacoes_alteracao|uq_solicitacoes_alteracao__identificador_publico
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_aplicacoes|ix_sol_alt_aplicacoes__aplicador_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_aplicacoes|ix_sol_alt_aplicacoes__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_aplicacoes|pk_sol_alt_aplicacoes
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_aplicacoes|uq_sol_alt_aplicacoes__idempotencia_chave
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_aprovacoes|ix_sol_alt_aprovacoes__aprovador_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_aprovacoes|ix_sol_alt_aprovacoes__revogada_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_aprovacoes|ix_sol_alt_aprovacoes__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_aprovacoes|pk_sol_alt_aprovacoes
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_associacoes|ix_sol_alt_associacoes__assoc_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_associacoes|ix_sol_alt_associacoes__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_associacoes|pk_sol_alt_associacoes
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_associacoes|uq_sol_alt_associacoes__sol_alt_id_assoc_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_associados|ix_sol_alt_associados__associado_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_associados|ix_sol_alt_associados__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_associados|pk_sol_alt_associados
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_associados|uq_sol_alt_associados__sol_alt_id_associado_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_catalogo_itens|ix_sol_alt_catalogo_itens__catalogo_item_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_catalogo_itens|ix_sol_alt_catalogo_itens__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_catalogo_itens|pk_sol_alt_catalogo_itens
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_catalogo_itens|uq_sol_alt_catalogo_itens__sol_alt_id_catalogo_item_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_documentos|ix_sol_alt_documentos__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_documentos|ix_sol_alt_documentos__documento_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_documentos|pk_sol_alt_documentos
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_documentos|uq_sol_alt_documentos__sol_alt_id_documento_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_eventos|ix_sol_alt_eventos__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_eventos|ix_sol_alt_eventos__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_eventos|pk_sol_alt_eventos
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_mensagens|ix_sol_alt_mensagens__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_mensagens|ix_sol_alt_mensagens__editado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_mensagens|ix_sol_alt_mensagens__sol_alt_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_mensagens|pk_sol_alt_mensagens
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_patrimonios|ix_sol_alt_patrimonios__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_patrimonios|ix_sol_alt_patrimonios__patr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_patrimonios|pk_sol_alt_patrimonios
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_patrimonios|uq_sol_alt_patrimonios__sol_alt_id_patr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_transacoes|ix_sol_alt_transacoes__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_transacoes|ix_sol_alt_transacoes__transacao_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_transacoes|pk_sol_alt_transacoes
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_transacoes|uq_sol_alt_transacoes__sol_alt_id_transacao_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_uvrs|ix_sol_alt_uvrs__criado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_uvrs|ix_sol_alt_uvrs__uvr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_uvrs|pk_sol_alt_uvrs
-- H2D21-ABSENT MATERIALIZE: index|solicitacao_uvrs|uq_sol_alt_uvrs__sol_alt_id_uvr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacoes_alteracao|ix_solicitacoes_alteracao__assoc_contexto_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacoes_alteracao|ix_solicitacoes_alteracao__atualizado_por_usr_id
-- H2D21-ABSENT MATERIALIZE: index|solicitacoes_alteracao|ix_solicitacoes_alteracao__estado_risco_criada_em
-- H2D21-ABSENT MATERIALIZE: index|solicitacoes_alteracao|ix_solicitacoes_alteracao__objeto_tipo_logico_objeto_d7a1161f
-- H2D21-ABSENT MATERIALIZE: index|solicitacoes_alteracao|ix_solicitacoes_alteracao__solicitante_usr_id
-- H2D21-ABSENT PRESERVE_EQUIVALENT: index|solicitacoes_alteracao|pk_solicitacoes_alteracao
-- H2D21-ABSENT MATERIALIZE: index|solicitacoes_alteracao|uq_solicitacoes_alteracao__identificador_publico
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_aplicacoes|solicitacao_aplicacoes_id_seq
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_aprovacoes|solicitacao_aprovacoes_id_seq
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_associacoes|solicitacao_associacoes_id_seq
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_associados|solicitacao_associados_id_seq
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_catalogo_itens|solicitacao_catalogo_itens_id_seq
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_documentos|solicitacao_documentos_id_seq
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_eventos|solicitacao_eventos_id_seq
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_mensagens|solicitacao_mensagens_id_seq
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_patrimonios|solicitacao_patrimonios_id_seq
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_transacoes|solicitacao_transacoes_id_seq
-- H2D21-ABSENT MATERIALIZE: sequence|solicitacao_uvrs|solicitacao_uvrs_id_seq

-- Oito colunas EXTRA_LEGADO são somente fonte: nenhuma aparece no lado esquerdo de UPDATE/ALTER.
ALTER TABLE public.solicitacoes_alteracao
    ADD COLUMN identificador_publico UUID,
    ADD COLUMN tipo TEXT,
    ADD COLUMN modulo TEXT,
    ADD COLUMN objeto_tipo_logico TEXT,
    ADD COLUMN objeto_identificador_logico TEXT,
    ADD COLUMN solicitante_usuario_id INTEGER,
    ADD COLUMN estado TEXT,
    ADD COLUMN risco TEXT,
    ADD COLUMN versao_esperada INTEGER,
    ADD COLUMN fotografia_original JSONB,
    ADD COLUMN fotografia_proposta JSONB,
    ADD COLUMN fotografia_aprovada JSONB,
    ADD COLUMN fotografia_aplicada JSONB,
    ADD COLUMN formato_versao INTEGER,
    ADD COLUMN justificativa TEXT,
    ADD COLUMN criada_em TIMESTAMPTZ,
    ADD COLUMN enviada_em TIMESTAMPTZ,
    ADD COLUMN concluida_em TIMESTAMPTZ,
    ADD COLUMN atualizado_em TIMESTAMPTZ,
    ADD COLUMN atualizado_por_usuario_id INTEGER,
    ADD COLUMN versao_registro INTEGER,
    ADD COLUMN request_id UUID,
    ADD COLUMN associacao_contexto_id BIGINT;

-- Nível 3: cria um principal técnico bloqueado apenas para identidades sem candidato exato/canônico.
-- O hash é o marcador inutilizável já aprovado em R0005; ativo=FALSE bloqueia o login legado.
WITH legacy_tokens AS (
    SELECT DISTINCT
           solicitacao.usuario_solicitante AS token_legado,
           lower(btrim(solicitacao.usuario_solicitante)) AS token_normalizado
      FROM public.solicitacoes_alteracao AS solicitacao
), exact_counts AS (
    SELECT token.token_legado,
           count(usuario.id) AS candidate_count
      FROM legacy_tokens AS token
      LEFT JOIN public.usuarios AS usuario
        ON usuario.username = token.token_legado
     GROUP BY token.token_legado
), canonical_counts AS (
    SELECT token.token_legado,
           count(usuario.id) AS candidate_count
      FROM legacy_tokens AS token
      LEFT JOIN public.usuarios AS usuario
        ON usuario.username_normalizado = token.token_normalizado
     GROUP BY token.token_legado
), unresolved AS (
    SELECT token.token_legado, token.token_normalizado
      FROM legacy_tokens AS token
      JOIN exact_counts AS exact USING (token_legado)
      JOIN canonical_counts AS canonical USING (token_legado)
     WHERE exact.candidate_count = 0
       AND canonical.candidate_count = 0
)
INSERT INTO public.usuarios (
    username, username_normalizado, password_hash, nome_completo,
    email, email_normalizado, estado, exige_troca_senha,
    criado_por_usuario_id, inativado_em, inativado_por_usuario_id,
    role, uvr_acesso, ativo
)
SELECT unresolved.token_legado,
       unresolved.token_normalizado,
       'pbkdf2:sha256:1000000$0caPhVgPLfRuxvRAGu8srw$80dab8fa229f014098e1b004ed61a1065b6f324a2c2dc30ec2d01c5f9351097c',
       'Ator histórico legado: ' || unresolved.token_legado,
       NULL, NULL, 'BLOQUEADO', TRUE,
       NULL, CURRENT_TIMESTAMP, NULL,
       'migracao', NULL, FALSE
  FROM unresolved;

-- Níveis 1 e 2: o canônico só participa quando não existe candidato exato.
WITH exact_candidates AS (
    SELECT solicitacao.id AS solicitacao_id, usuario.id AS usuario_id
      FROM public.solicitacoes_alteracao AS solicitacao
      JOIN public.usuarios AS usuario
        ON usuario.username = solicitacao.usuario_solicitante
), canonical_candidates AS (
    SELECT solicitacao.id AS solicitacao_id, usuario.id AS usuario_id
      FROM public.solicitacoes_alteracao AS solicitacao
      JOIN public.usuarios AS usuario
        ON usuario.username_normalizado = lower(btrim(solicitacao.usuario_solicitante))
     WHERE NOT EXISTS (
         SELECT 1
           FROM exact_candidates AS exact
          WHERE exact.solicitacao_id = solicitacao.id
     )
), candidates AS (
    SELECT * FROM exact_candidates
    UNION ALL
    SELECT * FROM canonical_candidates
), candidate_counts AS (
    SELECT solicitacao_id, count(*) AS candidate_count
      FROM candidates
     GROUP BY solicitacao_id
)
UPDATE public.solicitacoes_alteracao AS solicitacao
   SET solicitante_usuario_id = candidate.usuario_id
  FROM candidates AS candidate
  JOIN candidate_counts AS cardinality
    ON cardinality.solicitacao_id = candidate.solicitacao_id
   AND cardinality.candidate_count = 1
 WHERE solicitacao.id = candidate.solicitacao_id;

DO $r0007_user_guard$
BEGIN
    IF EXISTS (
        SELECT 1
          FROM public.solicitacoes_alteracao
         WHERE solicitante_usuario_id IS NULL
    ) THEN
        RAISE EXCEPTION 'R0007: solicitante sem resolução determinística 1:1';
    END IF;
END
$r0007_user_guard$;

-- Backfill autorizado. UTC é convenção técnica; data_solicitacao original permanece intocada.
-- atualizado_em é o único timestamp técnico da transação, não uma data histórica.
UPDATE public.solicitacoes_alteracao
   SET identificador_publico = pg_catalog.gen_random_uuid(),
       tipo = btrim(tipo_solicitacao),
       modulo = 'LEGADO',
       objeto_tipo_logico = btrim(tabela_alvo),
       objeto_identificador_logico = id_registro::text,
       estado = CASE upper(btrim(status))
                    WHEN 'APROVADO' THEN 'APLICADA'
                    WHEN 'REJEITADO' THEN 'REJEITADA'
                END,
       risco = 'LEGADO_NAO_CLASSIFICADO',
       versao_esperada = 0,
       fotografia_original = NULL,
       fotografia_proposta = dados_novos,
       fotografia_aprovada = NULL,
       fotografia_aplicada = NULL,
       formato_versao = 1,
       justificativa = NULL,
       criada_em = data_solicitacao AT TIME ZONE 'UTC',
       enviada_em = NULL,
       concluida_em = NULL,
       atualizado_em = CURRENT_TIMESTAMP,
       atualizado_por_usuario_id = NULL,
       versao_registro = 1,
       request_id = pg_catalog.gen_random_uuid(),
       associacao_contexto_id = NULL;

ALTER TABLE public.solicitacoes_alteracao
    ALTER COLUMN identificador_publico SET NOT NULL,
    ALTER COLUMN tipo SET NOT NULL,
    ALTER COLUMN modulo SET NOT NULL,
    ALTER COLUMN objeto_tipo_logico SET NOT NULL,
    ALTER COLUMN solicitante_usuario_id SET NOT NULL,
    ALTER COLUMN estado SET NOT NULL,
    ALTER COLUMN estado SET DEFAULT 'RASCUNHO',
    ALTER COLUMN risco SET NOT NULL,
    ALTER COLUMN versao_esperada SET NOT NULL,
    ALTER COLUMN formato_versao SET NOT NULL,
    ALTER COLUMN formato_versao SET DEFAULT 1,
    ALTER COLUMN criada_em SET NOT NULL,
    ALTER COLUMN criada_em SET DEFAULT CURRENT_TIMESTAMP,
    ALTER COLUMN atualizado_em SET NOT NULL,
    ALTER COLUMN atualizado_em SET DEFAULT CURRENT_TIMESTAMP,
    ALTER COLUMN versao_registro SET NOT NULL,
    ALTER COLUMN versao_registro SET DEFAULT 1,
    ALTER COLUMN request_id SET NOT NULL;

CREATE TABLE solicitacao_eventos (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    tipo_evento TEXT NOT NULL,
    estado_anterior TEXT,
    estado_novo TEXT,
    justificativa TEXT,
    resultado TEXT NOT NULL,
    metadados JSONB,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL,
    request_id UUID NOT NULL,
    origem TEXT NOT NULL,
    versao_formato INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE solicitacao_mensagens (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    mensagem TEXT NOT NULL,
    visibilidade TEXT NOT NULL,
    estado TEXT NOT NULL DEFAULT 'ATIVA',
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL,
    editado_em TIMESTAMPTZ,
    editado_por_usuario_id INTEGER,
    versao_registro INTEGER NOT NULL DEFAULT 1,
    request_id UUID NOT NULL,
    removido_em TIMESTAMPTZ
);

CREATE TABLE solicitacao_aprovacoes (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    etapa TEXT NOT NULL,
    decisao TEXT NOT NULL,
    aprovador_usuario_id INTEGER NOT NULL,
    justificativa TEXT,
    risco_observado TEXT,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    request_id UUID NOT NULL,
    estado TEXT NOT NULL DEFAULT 'ATIVA',
    revogada_em TIMESTAMPTZ,
    revogada_por_usuario_id INTEGER,
    motivo_revogacao TEXT
);

CREATE TABLE solicitacao_aplicacoes (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    tentativa INTEGER NOT NULL,
    aplicador_usuario_id INTEGER NOT NULL,
    iniciada_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    concluida_em TIMESTAMPTZ,
    versao_observada INTEGER NOT NULL,
    versao_resultante INTEGER,
    resultado TEXT NOT NULL,
    erro_codigo TEXT,
    erro_sanitizado TEXT,
    idempotencia_chave UUID NOT NULL,
    request_id UUID NOT NULL,
    fotografia_resultado JSONB,
    versao_formato INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE solicitacao_documentos (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    documento_id BIGINT NOT NULL,
    categoria TEXT NOT NULL,
    visibilidade TEXT NOT NULL,
    estado TEXT NOT NULL DEFAULT 'ATIVA',
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL,
    request_id UUID NOT NULL
);

CREATE TABLE solicitacao_associados (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    associado_id BIGINT NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL
);

CREATE TABLE solicitacao_associacoes (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    associacao_id BIGINT NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL
);

CREATE TABLE solicitacao_uvrs (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    uvr_id BIGINT NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL
);

CREATE TABLE solicitacao_catalogo_itens (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    catalogo_item_id BIGINT NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL
);

CREATE TABLE solicitacao_transacoes (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    transacao_id BIGINT NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL
);

CREATE TABLE solicitacao_patrimonios (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY NOT NULL,
    solicitacao_id BIGINT NOT NULL,
    patrimonio_id BIGINT NOT NULL,
    criado_em TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
    criado_por_usuario_id INTEGER NOT NULL
);

ALTER TABLE solicitacao_eventos
    ADD CONSTRAINT pk_sol_alt_eventos PRIMARY KEY (id);

ALTER TABLE solicitacao_mensagens
    ADD CONSTRAINT pk_sol_alt_mensagens PRIMARY KEY (id);

ALTER TABLE solicitacao_aprovacoes
    ADD CONSTRAINT pk_sol_alt_aprovacoes PRIMARY KEY (id);

ALTER TABLE solicitacao_aplicacoes
    ADD CONSTRAINT pk_sol_alt_aplicacoes PRIMARY KEY (id);

ALTER TABLE solicitacao_documentos
    ADD CONSTRAINT pk_sol_alt_documentos PRIMARY KEY (id);

ALTER TABLE solicitacao_associados
    ADD CONSTRAINT pk_sol_alt_associados PRIMARY KEY (id);

ALTER TABLE solicitacao_associacoes
    ADD CONSTRAINT pk_sol_alt_associacoes PRIMARY KEY (id);

ALTER TABLE solicitacao_uvrs
    ADD CONSTRAINT pk_sol_alt_uvrs PRIMARY KEY (id);

ALTER TABLE solicitacao_catalogo_itens
    ADD CONSTRAINT pk_sol_alt_catalogo_itens PRIMARY KEY (id);

ALTER TABLE solicitacao_transacoes
    ADD CONSTRAINT pk_sol_alt_transacoes PRIMARY KEY (id);

ALTER TABLE solicitacao_patrimonios
    ADD CONSTRAINT pk_sol_alt_patrimonios PRIMARY KEY (id);

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT uq_solicitacoes_alteracao__identificador_publico UNIQUE (identificador_publico);

ALTER TABLE solicitacao_aplicacoes
    ADD CONSTRAINT uq_sol_alt_aplicacoes__idempotencia_chave UNIQUE (idempotencia_chave);

CREATE UNIQUE INDEX uq_sol_alt_documentos__sol_alt_id_documento_id
    ON solicitacao_documentos (solicitacao_id, documento_id)
    WHERE estado = 'ATIVA';

ALTER TABLE solicitacao_associados
    ADD CONSTRAINT uq_sol_alt_associados__sol_alt_id_associado_id UNIQUE (solicitacao_id, associado_id);

ALTER TABLE solicitacao_associacoes
    ADD CONSTRAINT uq_sol_alt_associacoes__sol_alt_id_assoc_id UNIQUE (solicitacao_id, associacao_id);

ALTER TABLE solicitacao_uvrs
    ADD CONSTRAINT uq_sol_alt_uvrs__sol_alt_id_uvr_id UNIQUE (solicitacao_id, uvr_id);

ALTER TABLE solicitacao_catalogo_itens
    ADD CONSTRAINT uq_sol_alt_catalogo_itens__sol_alt_id_catalogo_item_id UNIQUE (solicitacao_id, catalogo_item_id);

ALTER TABLE solicitacao_transacoes
    ADD CONSTRAINT uq_sol_alt_transacoes__sol_alt_id_transacao_id UNIQUE (solicitacao_id, transacao_id);

ALTER TABLE solicitacao_patrimonios
    ADD CONSTRAINT uq_sol_alt_patrimonios__sol_alt_id_patr_id UNIQUE (solicitacao_id, patrimonio_id);

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT ck_solicitacoes_alteracao__tipo_preenchido CHECK (btrim(tipo) <> '');

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT ck_solicitacoes_alteracao__modulo_preenchido CHECK (btrim(modulo) <> '');

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT ck_solicitacoes_alteracao__objeto_tipo_logico_preenchido CHECK (btrim(objeto_tipo_logico) <> '');

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT ck_solicitacoes_alteracao__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT ck_solicitacoes_alteracao__risco_preenchido CHECK (btrim(risco) <> '');

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT ck_solicitacoes_alteracao__formato_versao_positivo CHECK (formato_versao > 0);

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT ck_solicitacoes_alteracao__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT ck_solicitacoes_alteracao__estado CHECK (estado IN ('RASCUNHO', 'ENVIADA', 'EM_ANALISE', 'APROVADA', 'REJEITADA', 'APLICADA', 'FALHA', 'CANCELADA'));

ALTER TABLE solicitacao_eventos
    ADD CONSTRAINT ck_sol_alt_eventos__tipo_evento_preenchido CHECK (btrim(tipo_evento) <> '');

ALTER TABLE solicitacao_eventos
    ADD CONSTRAINT ck_sol_alt_eventos__resultado_preenchido CHECK (btrim(resultado) <> '');

ALTER TABLE solicitacao_eventos
    ADD CONSTRAINT ck_sol_alt_eventos__origem_preenchido CHECK (btrim(origem) <> '');

ALTER TABLE solicitacao_eventos
    ADD CONSTRAINT ck_sol_alt_eventos__versao_formato_positivo CHECK (versao_formato > 0);

ALTER TABLE solicitacao_mensagens
    ADD CONSTRAINT ck_sol_alt_mensagens__mensagem_preenchido CHECK (btrim(mensagem) <> '');

ALTER TABLE solicitacao_mensagens
    ADD CONSTRAINT ck_sol_alt_mensagens__visibilidade_preenchido CHECK (btrim(visibilidade) <> '');

ALTER TABLE solicitacao_mensagens
    ADD CONSTRAINT ck_sol_alt_mensagens__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE solicitacao_mensagens
    ADD CONSTRAINT ck_sol_alt_mensagens__versao_registro_positivo CHECK (versao_registro > 0);

ALTER TABLE solicitacao_mensagens
    ADD CONSTRAINT ck_sol_alt_mensagens__estado CHECK (estado IN ('ATIVA', 'REVOGADA', 'REMOVIDA'));

ALTER TABLE solicitacao_aprovacoes
    ADD CONSTRAINT ck_sol_alt_aprovacoes__etapa_preenchido CHECK (btrim(etapa) <> '');

ALTER TABLE solicitacao_aprovacoes
    ADD CONSTRAINT ck_sol_alt_aprovacoes__decisao_preenchido CHECK (btrim(decisao) <> '');

ALTER TABLE solicitacao_aprovacoes
    ADD CONSTRAINT ck_sol_alt_aprovacoes__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE solicitacao_aprovacoes
    ADD CONSTRAINT ck_sol_alt_aprovacoes__estado CHECK (estado IN ('ATIVA', 'REVOGADA', 'REMOVIDA'));

ALTER TABLE solicitacao_aplicacoes
    ADD CONSTRAINT ck_sol_alt_aplicacoes__tentativa_positivo CHECK (tentativa >= 0);

ALTER TABLE solicitacao_aplicacoes
    ADD CONSTRAINT ck_sol_alt_aplicacoes__resultado_preenchido CHECK (btrim(resultado) <> '');

ALTER TABLE solicitacao_aplicacoes
    ADD CONSTRAINT ck_sol_alt_aplicacoes__versao_formato_positivo CHECK (versao_formato > 0);

ALTER TABLE solicitacao_documentos
    ADD CONSTRAINT ck_sol_alt_documentos__categoria_preenchido CHECK (btrim(categoria) <> '');

ALTER TABLE solicitacao_documentos
    ADD CONSTRAINT ck_sol_alt_documentos__visibilidade_preenchido CHECK (btrim(visibilidade) <> '');

ALTER TABLE solicitacao_documentos
    ADD CONSTRAINT ck_sol_alt_documentos__estado_preenchido CHECK (btrim(estado) <> '');

ALTER TABLE solicitacao_documentos
    ADD CONSTRAINT ck_sol_alt_documentos__estado CHECK (estado IN ('ATIVA', 'REVOGADA', 'REMOVIDA'));

ALTER TABLE solicitacao_aplicacoes
    ADD CONSTRAINT fk_sol_alt_aplicacoes__aplicador_usr_id FOREIGN KEY (aplicador_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_aplicacoes
    ADD CONSTRAINT fk_sol_alt_aplicacoes__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_aprovacoes
    ADD CONSTRAINT fk_sol_alt_aprovacoes__aprovador_usr_id FOREIGN KEY (aprovador_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_aprovacoes
    ADD CONSTRAINT fk_sol_alt_aprovacoes__revogada_por_usr_id FOREIGN KEY (revogada_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_aprovacoes
    ADD CONSTRAINT fk_sol_alt_aprovacoes__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_associacoes
    ADD CONSTRAINT fk_sol_alt_associacoes__assoc_id FOREIGN KEY (associacao_id)
    REFERENCES associacoes (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_associacoes
    ADD CONSTRAINT fk_sol_alt_associacoes__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_associacoes
    ADD CONSTRAINT fk_sol_alt_associacoes__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_associados
    ADD CONSTRAINT fk_sol_alt_associados__associado_id FOREIGN KEY (associado_id)
    REFERENCES associados (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_associados
    ADD CONSTRAINT fk_sol_alt_associados__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_associados
    ADD CONSTRAINT fk_sol_alt_associados__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_catalogo_itens
    ADD CONSTRAINT fk_sol_alt_catalogo_itens__catalogo_item_id FOREIGN KEY (catalogo_item_id)
    REFERENCES catalogo_itens (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_catalogo_itens
    ADD CONSTRAINT fk_sol_alt_catalogo_itens__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_catalogo_itens
    ADD CONSTRAINT fk_sol_alt_catalogo_itens__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_documentos
    ADD CONSTRAINT fk_sol_alt_documentos__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_documentos
    ADD CONSTRAINT fk_sol_alt_documentos__documento_id FOREIGN KEY (documento_id)
    REFERENCES documentos_privados (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_documentos
    ADD CONSTRAINT fk_sol_alt_documentos__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_eventos
    ADD CONSTRAINT fk_sol_alt_eventos__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_eventos
    ADD CONSTRAINT fk_sol_alt_eventos__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_mensagens
    ADD CONSTRAINT fk_sol_alt_mensagens__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_mensagens
    ADD CONSTRAINT fk_sol_alt_mensagens__editado_por_usr_id FOREIGN KEY (editado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_mensagens
    ADD CONSTRAINT fk_sol_alt_mensagens__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_patrimonios
    ADD CONSTRAINT fk_sol_alt_patrimonios__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_patrimonios
    ADD CONSTRAINT fk_sol_alt_patrimonios__patr_id FOREIGN KEY (patrimonio_id)
    REFERENCES patrimonios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_patrimonios
    ADD CONSTRAINT fk_sol_alt_patrimonios__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_transacoes
    ADD CONSTRAINT fk_sol_alt_transacoes__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_transacoes
    ADD CONSTRAINT fk_sol_alt_transacoes__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_transacoes
    ADD CONSTRAINT fk_sol_alt_transacoes__transacao_id FOREIGN KEY (transacao_id)
    REFERENCES transacoes_financeiras (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_uvrs
    ADD CONSTRAINT fk_sol_alt_uvrs__criado_por_usr_id FOREIGN KEY (criado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_uvrs
    ADD CONSTRAINT fk_sol_alt_uvrs__sol_alt_id FOREIGN KEY (solicitacao_id)
    REFERENCES solicitacoes_alteracao (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacao_uvrs
    ADD CONSTRAINT fk_sol_alt_uvrs__uvr_id FOREIGN KEY (uvr_id)
    REFERENCES uvrs (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT fk_solicitacoes_alteracao__assoc_contexto_id FOREIGN KEY (associacao_contexto_id)
    REFERENCES associacoes (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT fk_solicitacoes_alteracao__atualizado_por_usr_id FOREIGN KEY (atualizado_por_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

ALTER TABLE solicitacoes_alteracao
    ADD CONSTRAINT fk_solicitacoes_alteracao__solicitante_usr_id FOREIGN KEY (solicitante_usuario_id)
    REFERENCES usuarios (id) ON DELETE RESTRICT ON UPDATE NO ACTION NOT DEFERRABLE;

CREATE INDEX ix_sol_alt_aplicacoes__aplicador_usr_id
    ON solicitacao_aplicacoes (aplicador_usuario_id);

CREATE INDEX ix_sol_alt_aplicacoes__sol_alt_id
    ON solicitacao_aplicacoes (solicitacao_id);

CREATE INDEX ix_sol_alt_aprovacoes__aprovador_usr_id
    ON solicitacao_aprovacoes (aprovador_usuario_id);

CREATE INDEX ix_sol_alt_aprovacoes__revogada_por_usr_id
    ON solicitacao_aprovacoes (revogada_por_usuario_id);

CREATE INDEX ix_sol_alt_aprovacoes__sol_alt_id
    ON solicitacao_aprovacoes (solicitacao_id);

CREATE INDEX ix_sol_alt_associacoes__assoc_id
    ON solicitacao_associacoes (associacao_id);

CREATE INDEX ix_sol_alt_associacoes__criado_por_usr_id
    ON solicitacao_associacoes (criado_por_usuario_id);

CREATE INDEX ix_sol_alt_associados__associado_id
    ON solicitacao_associados (associado_id);

CREATE INDEX ix_sol_alt_associados__criado_por_usr_id
    ON solicitacao_associados (criado_por_usuario_id);

CREATE INDEX ix_sol_alt_catalogo_itens__catalogo_item_id
    ON solicitacao_catalogo_itens (catalogo_item_id);

CREATE INDEX ix_sol_alt_catalogo_itens__criado_por_usr_id
    ON solicitacao_catalogo_itens (criado_por_usuario_id);

CREATE INDEX ix_sol_alt_documentos__criado_por_usr_id
    ON solicitacao_documentos (criado_por_usuario_id);

CREATE INDEX ix_sol_alt_documentos__documento_id
    ON solicitacao_documentos (documento_id);

CREATE INDEX ix_sol_alt_eventos__criado_por_usr_id
    ON solicitacao_eventos (criado_por_usuario_id);

CREATE INDEX ix_sol_alt_eventos__sol_alt_id
    ON solicitacao_eventos (solicitacao_id);

CREATE INDEX ix_sol_alt_mensagens__criado_por_usr_id
    ON solicitacao_mensagens (criado_por_usuario_id);

CREATE INDEX ix_sol_alt_mensagens__editado_por_usr_id
    ON solicitacao_mensagens (editado_por_usuario_id);

CREATE INDEX ix_sol_alt_mensagens__sol_alt_id
    ON solicitacao_mensagens (solicitacao_id);

CREATE INDEX ix_sol_alt_patrimonios__criado_por_usr_id
    ON solicitacao_patrimonios (criado_por_usuario_id);

CREATE INDEX ix_sol_alt_patrimonios__patr_id
    ON solicitacao_patrimonios (patrimonio_id);

CREATE INDEX ix_sol_alt_transacoes__criado_por_usr_id
    ON solicitacao_transacoes (criado_por_usuario_id);

CREATE INDEX ix_sol_alt_transacoes__transacao_id
    ON solicitacao_transacoes (transacao_id);

CREATE INDEX ix_sol_alt_uvrs__criado_por_usr_id
    ON solicitacao_uvrs (criado_por_usuario_id);

CREATE INDEX ix_sol_alt_uvrs__uvr_id
    ON solicitacao_uvrs (uvr_id);

CREATE INDEX ix_solicitacoes_alteracao__assoc_contexto_id
    ON solicitacoes_alteracao (associacao_contexto_id);

CREATE INDEX ix_solicitacoes_alteracao__atualizado_por_usr_id
    ON solicitacoes_alteracao (atualizado_por_usuario_id);

CREATE INDEX ix_solicitacoes_alteracao__solicitante_usr_id
    ON solicitacoes_alteracao (solicitante_usuario_id);

CREATE INDEX ix_solicitacoes_alteracao__estado_risco_criada_em
    ON solicitacoes_alteracao (estado, risco, criada_em);

CREATE INDEX ix_solicitacoes_alteracao__objeto_tipo_logico_objeto_d7a1161f
    ON solicitacoes_alteracao (objeto_tipo_logico, objeto_identificador_logico);
