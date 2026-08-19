# Matriz de cobertura — Etapa 2I

Esta matriz relaciona os 72 cenários obrigatórios da Etapa 2I aos testes
automatizados em `test_fiscalizacao_contratos_medicoes.py`. Um teste pode cobrir
mais de um comportamento relacionado.

| # | Cenário | Teste automatizado |
|---:|---|---|
| 1 | Administrador acessa a lista | `test_admin_acessa_lista_formulario_detalhe_e_cartao` |
| 2 | Visitante vai ao login | `test_visitante_e_usuario_comum_bloqueados_em_todas_as_rotas` |
| 3 | Usuário comum recebe 403 | `test_visitante_e_usuario_comum_bloqueados_em_todas_as_rotas` |
| 4 | Todas as rotas exigem administrador | `test_eventos_sao_somente_leitura_e_todas_as_rotas_sao_administrativas` |
| 5 | Administrador cria medição | `test_cria_medicao_com_estado_versao_totais_e_evento_iniciais` |
| 6 | Contrato inexistente rejeitado | `test_rejeita_contrato_e_fiscal_inexistentes_ou_inativos` |
| 7 | Contrato inativo rejeitado | `test_rejeita_contrato_e_fiscal_inexistentes_ou_inativos` |
| 8 | Fiscal inexistente rejeitado | `test_rejeita_contrato_e_fiscal_inexistentes_ou_inativos` |
| 9 | Fiscal inativo rejeitado | `test_rejeita_contrato_e_fiscal_inexistentes_ou_inativos` |
| 10 | Número inválido rejeitado | `test_validacoes_numero_competencia_e_periodo` |
| 11 | Competência inválida rejeitada | `test_validacoes_numero_competencia_e_periodo` |
| 12 | Período inválido rejeitado | `test_validacoes_numero_competencia_e_periodo` |
| 13 | Versão inicial igual a 1 | `test_cria_medicao_com_estado_versao_totais_e_evento_iniciais` |
| 14 | Status inicial Em elaboração | `test_cria_medicao_com_estado_versao_totais_e_evento_iniciais` |
| 15 | Evento de criação registrado | `test_cria_medicao_com_estado_versao_totais_e_evento_iniciais`, `test_criacao_trata_concorrencia_e_falha_do_evento_com_rollback` |
| 16 | Item manual criado | `test_item_manual_calcula_com_decimal_sem_confiar_em_total_do_formulario` |
| 17 | Item da planilha do contrato criado | `test_item_de_planilha_copia_fotografia_e_rejeita_outro_contrato` |
| 18 | Item da planilha de outro contrato rejeitado | `test_item_de_planilha_copia_fotografia_e_rejeita_outro_contrato` |
| 19 | Quantidade negativa rejeitada | `test_item_duplicado_excesso_e_valores_negativos_sao_rejeitados` |
| 20 | Preço negativo rejeitado | `test_item_duplicado_excesso_e_valores_negativos_sao_rejeitados` |
| 21 | Cálculo do item usa Decimal | `test_calculos_decimal_arredondamento_e_sem_float`, `test_arredondamento_monetario_explicito_e_entradas_vazias` |
| 22 | Excesso exige justificativa | `test_item_duplicado_excesso_e_valores_negativos_sao_rejeitados` |
| 23 | Item da planilha duplicado rejeitado | `test_item_duplicado_excesso_e_valores_negativos_sao_rejeitados` |
| 24 | Inativação preserva item | `test_edicao_e_inativacao_de_item_recalculam_sem_apagar` |
| 25 | Acréscimo calculado | `test_acrescimo_desconto_e_glosa_calculam_totais` |
| 26 | Desconto calculado | `test_acrescimo_desconto_e_glosa_calculam_totais` |
| 27 | Glosa calculada | `test_acrescimo_desconto_e_glosa_calculam_totais` |
| 28 | Ajuste zero rejeitado | `test_ajuste_zero_referencias_de_outro_contrato_e_liquido_negativo` |
| 29 | Ocorrência de outro contrato rejeitada | `test_ajuste_zero_referencias_de_outro_contrato_e_liquido_negativo` |
| 30 | Fiscalização de outro contrato rejeitada | `test_ajuste_zero_referencias_de_outro_contrato_e_liquido_negativo` |
| 31 | Valor líquido negativo rejeitado e revertido | `test_ajuste_zero_referencias_de_outro_contrato_e_liquido_negativo`, `test_inativacao_que_geraria_liquido_negativo_faz_rollback` |
| 32 | Totais enviados pelo formulário ignorados | `test_item_manual_calcula_com_decimal_sem_confiar_em_total_do_formulario` |
| 33 | Documento do contrato vinculado | `test_documento_mesmo_contrato_duplicado_outro_contrato_e_inativacao` |
| 34 | Documento de outro contrato rejeitado | `test_documento_mesmo_contrato_duplicado_outro_contrato_e_inativacao` |
| 35 | Vínculo duplicado rejeitado | `test_documento_mesmo_contrato_duplicado_outro_contrato_e_inativacao` |
| 36 | Documento físico preservado | `test_documento_mesmo_contrato_duplicado_outro_contrato_e_inativacao` |
| 37 | Envio para análise funciona | `test_envio_exige_item_e_bloqueia_edicao` |
| 38 | Envio sem item rejeitado | `test_envio_exige_item_e_bloqueia_edicao` |
| 39 | Medição em análise bloqueada | `test_envio_exige_item_e_bloqueia_edicao` |
| 40 | Devolução exige justificativa | `test_devolucao_exige_justificativa_e_libera_correcao` |
| 41 | Devolvida permite editar | `test_devolucao_exige_justificativa_e_libera_correcao` |
| 42 | Aprovação funciona | `test_aprovacao_exige_aprovador_ativo_e_torna_imutavel` |
| 43 | Aprovação exige aprovador ativo | `test_aprovacao_exige_aprovador_ativo_e_torna_imutavel` |
| 44 | Aprovada fica imutável | `test_aprovacao_exige_aprovador_ativo_e_torna_imutavel`, `test_acesso_direto_respeita_bloqueios_de_status` |
| 45 | Cancelamento exige justificativa | `test_cancelamento_exige_justificativa_e_preserva_registros` |
| 46 | Cancelamento preserva registros | `test_cancelamento_exige_justificativa_e_preserva_registros` |
| 47 | Revisão de aprovada funciona | `test_revisao_exige_justificativa_e_copia_com_novos_ids` |
| 48 | Revisão exige justificativa | `test_revisao_exige_justificativa_e_copia_com_novos_ids` |
| 49 | Revisão cria versão seguinte | `test_revisao_exige_justificativa_e_copia_com_novos_ids` |
| 50 | Versão anterior deixa de ser atual | `test_revisao_exige_justificativa_e_copia_com_novos_ids` |
| 51 | Versão anterior permanece aprovada | `test_revisao_exige_justificativa_e_copia_com_novos_ids` |
| 52 | Itens copiados com novos IDs | `test_revisao_exige_justificativa_e_copia_com_novos_ids` |
| 53 | Ajustes copiados com novos IDs | `test_revisao_exige_justificativa_e_copia_com_novos_ids` |
| 54 | Documentos vinculados à revisão | `test_revisao_exige_justificativa_e_copia_com_novos_ids` |
| 55 | Eventos registrados nas duas versões | `test_revisao_exige_justificativa_e_copia_com_novos_ids` |
| 56 | Falha na revisão executa rollback | `test_falha_de_revisao_faz_rollback_simulado`, `test_revisao_concorrente_e_conflito_de_indice_fazem_rollback` |
| 57 | Não ficam duas versões atuais | `test_uma_atual_por_contrato_e_competencia`, `test_revisao_concorrente_e_conflito_de_indice_fazem_rollback` |
| 58 | Revisões sucessivas preservam histórico | `test_varias_revisoes_preservam_historico_e_uma_unica_atual` |
| 59 | Versão histórica não pode ser editada | `test_acesso_direto_respeita_bloqueios_de_status` |
| 60 | Pesquisa funciona | `test_pesquisa_filtros_eventos_versoes_e_indicadores` |
| 61 | Filtros funcionam | `test_pesquisa_filtros_eventos_versoes_e_indicadores` |
| 62 | Medições aparecem no contrato | `test_integracao_com_contrato_e_painel_esta_presente` |
| 63 | Indicadores aparecem no painel | `test_pesquisa_filtros_eventos_versoes_e_indicadores`, `test_integracao_com_contrato_e_painel_esta_presente` |
| 64 | Eventos em ordem correta e somente leitura | `test_pesquisa_filtros_eventos_versoes_e_indicadores`, `test_eventos_sao_somente_leitura_e_todas_as_rotas_sao_administrativas` |
| 65 | Nenhuma operação usa DELETE | `test_servico_nao_usa_delete_sql_dinamico_credencial_ou_app` |
| 66 | Consultas parametrizadas e sem f-string | `test_servico_nao_usa_delete_sql_dinamico_credencial_ou_app` |
| 67 | 230 testes anteriores preservados | execução integral por descoberta `test_*.py` |
| 68 | PostgreSQL e Cloudinary reais bloqueados | `test_app_py_patrimonio_cloudinary_e_arquivos_reais_nao_sao_tocados` e barreira global `test_00_bloqueio_servicos_reais.py` |
| 69 | Testes não alteram arquivos reais | `test_app_py_patrimonio_cloudinary_e_arquivos_reais_nao_sao_tocados` |
| 70 | Sintaxe aprovada | `py_compile` e `compileall` executados na revisão |
| 71 | `git diff --check` aprovado | comando executado na revisão |
| 72 | Nenhuma credencial no diff | auditoria automatizada do diff na revisão |

Coberturas adicionais da revisão final:

- cancelamento deixa de ser atual e libera a competência:
  `test_cancelamento_libera_competencia_sem_reutilizar_numero`;
- rollback por falha na atualização ou no evento de cancelamento:
  `test_cancelamento_real_e_atomico_com_rollback_de_update_e_evento`;
- concorrência na criação e rollback do evento inicial:
  `test_criacao_trata_concorrencia_e_falha_do_evento_com_rollback`;
- concorrência e conflito de índice na revisão:
  `test_revisao_concorrente_e_conflito_de_indice_fazem_rollback`;
- fotografia do item de planilha preservada na edição:
  `test_edicao_preserva_fotografia_do_item_de_planilha_sem_nova_consulta`;
- arredondamento explícito de `3 × 0,335 = 1,01`:
  `test_arredondamento_monetario_explicito_e_entradas_vazias`.
