


# Matriz de Acesso das Rotas

Data: **22/07/2026**
Branch: `codex/modulo-fiscalizacao-contratos`
Commit-base: `9444664980ad344b7d3313d14ada83a3d8a6f0b6`

Diagnóstico documental: nenhuma permissão, rota ou dado foi alterado. A aplicação foi carregada em `testing`, com PostgreSQL e Cloudinary reais bloqueados.

## 1. Resumo executivo

Foram encontradas **177 regras de rota**: 34 públicas, 38 com `login_required` e 105 com `admin_required`. A Basic só existe na homologação e não substitui o login. O CSRF cobre as 105 rotas mutáveis, mas não autoriza o usuário.

Existem **22 bloqueadores para homologação**: 11 rotas públicas mutáveis e 11 GETs públicos que consultam dados internos. Foram marcados **15 possíveis IDORs**, isto é, rotas por ID sem comprovação completa do escopo de UVR/objeto.

Não existe cadastro público de contas em `usuarios`. `/cadastrar` cria cliente/fornecedor e exige login. Contas internas devem continuar administrativas ou por convite controlado.

## 2. Contagens

| Medida | Quantidade |
|---|---:|
| Rotas | 177 |
| Aceitam GET | 121 |
| Aceitam POST | 105 |
| Aceitam PUT | 0 |
| Aceitam PATCH | 0 |
| Aceitam DELETE | 1 |
| Mutáveis | 105 |
| JSON/AJAX | 44 |
| Downloads/documentos | 7 |

| Proteção atual | Quantidade |
|---|---:|
| Pública | 34 |
| `login_required` | 38 |
| `admin_required` | 105 |

| Acesso proposto | Quantidade |
|---|---:|
| A — pública essencial | 4 |
| B — autenticado | 16 |
| C — administrador | 111 |
| D — regra específica | 44 |
| E — desativada online | 2 |

D significa preservar as regras existentes de UVR/associação e validar o objeto, sem inventar perfil novo.

## 3. Públicas essenciais

- `GET /health`: health check sem banco/Cloudinary e única exceção da Basic.
- `GET/POST /login`: entrada interna; POST com CSRF.
- As duas rotas estáticas: recursos visuais.

## 4. Públicas que consultam dados internos — 11

- `GET /get_associados_ativos` — associados/dados pessoais; risco de enumeração ou exposição de valores/identificadores. Proposta: **D — regra específica**.
- `GET /get_cadastros_ativos` — clientes/fornecedores/CNPJ/contato; risco de enumeração ou exposição de valores/identificadores. Proposta: **D — regra específica**.
- `GET /get_clientes_fornecedores_com_pendencias` — interface/sessão; risco de enumeração ou exposição de valores/identificadores. Proposta: **D — regra específica**.
- `GET /get_contas_correntes` — contas/dados bancários; risco de enumeração ou exposição de valores/identificadores. Proposta: **D — regra específica**.
- `GET /get_notas_em_aberto` — interface/sessão; risco de enumeração ou exposição de valores/identificadores. Proposta: **D — regra específica**.
- `GET /get_produtos_servicos` — catálogo; risco de enumeração ou exposição de valores/identificadores. Proposta: **B — autenticado**.
- `GET /get_relatorio_catalog_options` — relatórios/entidades/valores; risco de enumeração ou exposição de valores/identificadores. Proposta: **B — autenticado**.
- `GET /get_relatorio_entidades_para_filtro` — relatórios/entidades/valores; risco de enumeração ou exposição de valores/identificadores. Proposta: **D — regra específica**.
- `GET /get_relatorio_tipos_atividade_transacao` — transações/valores; risco de enumeração ou exposição de valores/identificadores. Proposta: **B — autenticado**.
- `GET /get_relatorio_uvrs` — relatórios/entidades/valores; risco de enumeração ou exposição de valores/identificadores. Proposta: **D — regra específica**.
- `GET /get_resumo_fluxo_caixa` — fluxo de caixa/valores; risco de enumeração ou exposição de valores/identificadores. Proposta: **D — regra específica**.

`/buscar_cep/...` e `/buscar_cnpj/...` não usam o banco local, mas devem exigir login ou rate limit antes da produção.

## 5. Mutáveis sem proteção adequada — 11

> Esta seção preserva a fotografia do diagnóstico anterior à H2A.3B.1. O
> estado implementado está registrado na seção 18, sem apagar o histórico.

| Rota | Operação | Atual | Proposta | Prioridade |
|---|---|---|---|---|
| `POST /baixar_csv_extrato` | gera/abre arquivo; contas/extrato/valores | Pública + CSRF | D — regra específica | Bloqueador |
| `POST /baixar_csv_relatorio` | gera/abre arquivo; relatórios/entidades/valores | Pública + CSRF | D — regra específica | Bloqueador |
| `POST /baixar_pdf_extrato` | gera/abre arquivo; contas/extrato/valores | Pública + CSRF | D — regra específica | Bloqueador |
| `POST /baixar_pdf_relatorio_financeiro` | gera/abre arquivo; relatórios/entidades/valores | Pública + CSRF | D — regra específica | Bloqueador |
| `POST /cadastrar_conta_corrente` | GET exibe formulário; POST cria/vincula/envia; contas/dados bancários | Pública + CSRF | D — regra específica | Bloqueador |
| `POST /cadastrar_produto_servico` | GET exibe formulário; POST cria/vincula/envia; catálogo | Pública + CSRF | C — administrador | Bloqueador |
| `POST /gerar_extrato_bancario` | exibe página ou mantém dados; contas/extrato/valores | Pública + CSRF | D — regra específica | Bloqueador |
| `POST /gerar_relatorio` | exibe página ou mantém dados; relatórios/entidades/valores | Pública + CSRF | D — regra específica | Bloqueador |
| `POST /registrar_denuncia` | GET exibe formulário; POST cria/vincula/envia; denúncias | Pública + CSRF | E — desativada online | Bloqueador |
| `POST /registrar_fluxo_caixa` | GET exibe formulário; POST cria/vincula/envia; fluxo de caixa/valores | Pública + CSRF | D — regra específica | Bloqueador |
| `POST /registrar_transacao_financeira` | GET exibe formulário; POST cria/vincula/envia; transações/valores | Pública + CSRF | D — regra específica | Bloqueador |

O login também muda sessão, mas — público por necessidade e usa CSRF. Nenhuma rota GET com escrita foi encontrada.

## 6. Cadastro público

- Nenhuma rota insere em `usuarios`, escolhe `role`, ativa conta ou cria admin.
- Não há recuperação de senha.
- `/cadastrar` e `/cadastrar_associado` não são contas e exigem login.
- Proposta: usuário somente por admin ou convite; cadastro público de conta continua desabilitado.

## 7. Login, logout e redirects

- Login usa hash e SQL parametrizado; não usa `next` e sempre volta ao painel.
- Não foi encontrado open redirect.
- Logout — POST com login e CSRF; troca de senha afeta o próprio usuário.
- Mensagens distintas para usuário inexistente e senha errada permitem enumeração e devem ser unificadas antes da produção.

## 8. Downloads e documentos — 7

| Rota | Atual | Análise |
|---|---|---|
| `POST /baixar_csv_extrato` | Pública; CSRF; Basic só na homologação | Exportação financeira pública; bloqueador. |
| `POST /baixar_csv_relatorio` | Pública; CSRF; Basic só na homologação | Exportação financeira pública; bloqueador. |
| `POST /baixar_pdf_extrato` | Pública; CSRF; Basic só na homologação | Exportação financeira pública; bloqueador. |
| `POST /baixar_pdf_relatorio_financeiro` | Pública; CSRF; Basic só na homologação | Exportação financeira pública; bloqueador. |
| `GET /fiscalizacao-contratos/documentos/<int:documento_id>/arquivo` | `admin_required`; Basic na homologação | Admin; URL privada de cinco minutos, não persistida e sem public_id exposto. |
| `GET /imprimir_ficha_associado/<int:id>` | `login_required`; Basic na homologação | Possível IDOR por ID sem escopo comprovado. |
| `GET /imprimir_ficha_cadastro/<int:id>` | `login_required`; Basic na homologação | Possível IDOR por ID sem escopo comprovado. |

Nenhum arquivo foi baixado. Documentos do módulo continuam restritos a admin.

## 9. JSON/AJAX

Foram identificados **44 endpoints JSON/AJAX**. Onze GETs públicos consultam banco e 11 POSTs públicos fazem escrita, relatório ou exportação. CSRF não substitui login. Respostas com `str(e)` podem revelar detalhes técnicos e devem ser generalizadas.

## 10. Possíveis IDORs — 15

- `POST /editar_associado` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `POST /editar_cadastro` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `POST /editar_conta_corrente` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `POST /editar_patrimonio` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `POST /editar_transacao` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `POST /excluir_associado/<int:id>` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `POST /excluir_cadastro/<int:id>` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `POST /excluir_patrimonio/<int:id>` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `POST /excluir_transacao/<int:id>` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `GET /get_conta_corrente_detalhe/<int:id>` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `GET /get_movimentacao_detalhes/<int:id>` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `GET /get_patrimonio_detalhes/<int:id>` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `GET /get_transacao_detalhes/<int:id>` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `GET /imprimir_ficha_associado/<int:id>` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.
- `GET /imprimir_ficha_cadastro/<int:id>` — login existe, mas o escopo de UVR/objeto não está comprovado nesta rota.

`get_associado` e `get_cadastro` já verificam UVR. O módulo — admin global, logo não foi marcado como IDOR de usuário comum.

## 11. Fiscalização de Contratos

- 105 rotas funcionais no Blueprint, todas com `admin_required`, mais uma estática.
- POSTs protegidos por CSRF; visitante vai ao login; usuário comum recebe 403.
- Basic não concede acesso interno e não há download público nem rota funcional fora do Blueprint.

## 12. Administração e migrations

- Não há rota web de migration, manutenção, teste, limpeza ou criação de admin.
- `criar_tabelas_se_nao_existir()` e `migrar_dados_antigos_produtos()` não são chamados no import nem por rota.
- `executar_migracao_produtos.py` — manual, não — rota, não inicia com o app e exige confirmação.
- Migrations SQL são arquivos, não endpoints.

## 13. Bloqueadores e correções

### Homologação — 22 bloqueadores

1. Proteger as 11 rotas públicas mutáveis.
2. Proteger e aplicar escopo às 11 consultas públicas ao banco.
3. Desativar a denúncia pública online até decisão expressa.
4. Manter Basic, login e autorização como camadas independentes.

### Antes da produção

- Corrigir os 15 possíveis IDORs.
- Unificar erro de login e ocultar exceções SQL.
- Proteger/rate-limit CEP e CNPJ.
- Centralizar verificações administrativas.
- Revisar exclusões físicas legadas e SQL dinâmico em tarefa própria.

## 14. Política proposta

- Público: health, login e estáticos.
- Autenticado: páginas internas comuns.
- Admin: gestão global e todo o módulo de contratos.
- Regra específica: UVR, associação e objeto permitido.
- Homologação: Basic + login + autorização; nenhuma manutenção web.

## 15. Matriz completa

Cada linha informa caminho, métodos, endpoint, origem, função, operação, dados, saída, proteção, risco e recomendação.

### 15.1 Inventario - parte 1 de 4

| Caminho | Metodos | Endpoint | Origem | Funcao | Operacao | Dados/servicos | Saida | Protecao atual | Risco | Recomendacao |
|---|---|---|---|---|---|---|---|---|---|---|
| `/` | `GET` | `index` | `app.py:417` | `index` | exibe pagina/mantem dados | interface/sessao | HTML/JSON/redirect | login_required + Basic homologacao | medio/baixo | B - autenticado |
| `/alterar_senha` | `GET,POST` | `alterar_senha` | `app.py:373` | `alterar_senha` | exibe pagina/mantem dados | usuarios/senha | HTML/JSON/redirect | login_required + CSRF + Basic homologacao | medio/baixo | B - autenticado |
| `/api/produtos_crud` | `DELETE,GET,POST` | `api_produtos_crud` | `app.py:3950` | `api_produtos_crud` | exibe pagina/mantem dados | catalogo | JSON | login_required + CSRF + Basic homologacao | medio/baixo | C - administrador |
| `/api/subgrupos` | `GET,POST` | `api_subgrupos` | `app.py:3887` | `api_subgrupos` | exibe pagina/mantem dados | catalogo | JSON | login_required + CSRF + Basic homologacao | medio/baixo | C - administrador |
| `/baixar_csv_extrato` | `POST` | `baixar_csv_extrato` | `app.py:2646` | `baixar_csv_extrato` | gera/abre arquivo | extrato/valores | arquivo/redirect privado | publica + CSRF + Basic homologacao | critico: publico mutavel | D - regra especifica |
| `/baixar_csv_relatorio` | `POST` | `baixar_csv_relatorio` | `app.py:2187` | `baixar_csv_relatorio` | gera/abre arquivo | relatorios/valores | arquivo/redirect privado | publica + CSRF + Basic homologacao | critico: publico mutavel | D - regra especifica |
| `/baixar_pdf_extrato` | `POST` | `baixar_pdf_extrato` | `app.py:2437` | `baixar_pdf_extrato` | gera/abre arquivo | extrato/valores | arquivo/redirect privado | publica + CSRF + Basic homologacao | critico: publico mutavel | D - regra especifica |
| `/baixar_pdf_relatorio_financeiro` | `POST` | `baixar_pdf_relatorio_financeiro` | `app.py:2285` | `baixar_pdf_relatorio_financeiro` | gera/abre arquivo | relatorios/valores | arquivo/redirect privado | publica + CSRF + Basic homologacao | critico: publico mutavel | D - regra especifica |
| `/buscar_associados` | `GET` | `buscar_associados` | `app.py:618` | `buscar_associados` | consulta/lista | associados/dados pessoais | JSON | login_required + Basic homologacao | medio/baixo | D - regra especifica |
| `/buscar_cadastros` | `GET` | `buscar_cadastros` | `app.py:3175` | `buscar_cadastros` | consulta/lista | clientes/fornecedores | JSON | login_required + Basic homologacao | medio/baixo | D - regra especifica |
| `/buscar_cep/<string:cep_numeros>` | `GET` | `buscar_cep` | `app.py:425` | `buscar_cep` | consulta/lista | API CEP | JSON | publica + Basic homologacao | medio: abuso de API | B - autenticado |
| `/buscar_cnpj/<string:cnpj>` | `GET` | `buscar_cnpj` | `app.py:2695` | `buscar_cnpj` | consulta/lista | API CNPJ | JSON | publica + Basic homologacao | medio: abuso de API | B - autenticado |
| `/buscar_contas_correntes_gestao` | `GET` | `buscar_contas_correntes_gestao` | `app.py:3447` | `buscar_contas_correntes_gestao` | consulta/lista | contas bancarias | JSON | login_required + Basic homologacao | medio/baixo | D - regra especifica |
| `/buscar_patrimonio` | `GET` | `buscar_patrimonio` | `app.py:4268` | `buscar_patrimonio` | consulta/lista | patrimonio | JSON | login_required + Basic homologacao | medio/baixo | D - regra especifica |
| `/buscar_transacoes_gestao` | `GET` | `buscar_transacoes_gestao` | `app.py:3734` | `buscar_transacoes_gestao` | consulta/lista | interface/sessao | JSON | login_required + Basic homologacao | medio/baixo | D - regra especifica |
| `/cadastrar` | `POST` | `cadastrar` | `app.py:474` | `cadastrar` | formulario e criacao/vinculo | interface/sessao | HTML/JSON/redirect | login_required + CSRF + Basic homologacao | medio/baixo | D - regra especifica |
| `/cadastrar_associado` | `POST` | `cadastrar_associado` | `app.py:534` | `cadastrar_associado` | formulario e criacao/vinculo | associados/dados pessoais | HTML/JSON/redirect | login_required + CSRF + Basic homologacao | medio/baixo | D - regra especifica |
| `/cadastrar_conta_corrente` | `POST` | `cadastrar_conta_corrente` | `app.py:897` | `cadastrar_conta_corrente` | formulario e criacao/vinculo | contas bancarias | HTML/JSON/redirect | publica + CSRF + Basic homologacao | critico: publico mutavel | D - regra especifica |
| `/cadastrar_patrimonio` | `POST` | `cadastrar_patrimonio` | `app.py:4206` | `cadastrar_patrimonio` | formulario e criacao/vinculo | patrimonio | HTML/JSON/redirect | login_required + CSRF + Basic homologacao | medio/baixo | D - regra especifica |
| `/cadastrar_produto_servico` | `POST` | `cadastrar_produto_servico` | `app.py:842` | `cadastrar_produto_servico` | formulario e criacao/vinculo | catalogo | HTML/JSON/redirect | publica + CSRF + Basic homologacao | critico: publico mutavel | C - administrador |
| `/editar_associado` | `POST` | `editar_associado` | `app.py:753` | `editar_associado` | formulario e edicao | associados/dados pessoais | HTML/JSON/redirect | login_required + CSRF + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/editar_cadastro` | `POST` | `editar_cadastro` | `app.py:3323` | `editar_cadastro` | formulario e edicao | clientes/fornecedores | HTML/JSON/redirect | login_required + CSRF + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/editar_conta_corrente` | `POST` | `editar_conta_corrente` | `app.py:2801` | `editar_conta_corrente` | formulario e edicao | contas bancarias | HTML/JSON/redirect | login_required + CSRF + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/editar_patrimonio` | `POST` | `editar_patrimonio` | `app.py:4340` | `editar_patrimonio` | formulario e edicao | patrimonio | HTML/JSON/redirect | login_required + CSRF + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/editar_transacao` | `POST` | `editar_transacao` | `app.py:1342` | `editar_transacao` | formulario e edicao | transacoes/valores | HTML/JSON/redirect | login_required + CSRF + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/excluir_associado/<int:id>` | `POST` | `excluir_associado` | `app.py:3408` | `excluir_associado` | exclui/solicita exclusao | associados/dados pessoais | JSON | login_required + CSRF + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/excluir_cadastro/<int:id>` | `POST` | `excluir_cadastro` | `app.py:3373` | `excluir_cadastro` | exclui/solicita exclusao | clientes/fornecedores | JSON | login_required + CSRF + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/excluir_conta_corrente/<int:id>` | `POST` | `excluir_conta_corrente` | `app.py:3518` | `excluir_conta_corrente` | exclui/solicita exclusao | contas bancarias | JSON | login_required + admin manual + CSRF + Basic homologacao | medio/baixo | C - administrador |
| `/excluir_movimentacao/<int:id>` | `POST` | `excluir_movimentacao` | `app.py:4151` | `excluir_movimentacao` | exclui/solicita exclusao | interface/sessao | JSON | login_required + admin manual + CSRF + Basic homologacao | medio/baixo | D - regra especifica |
| `/excluir_patrimonio/<int:id>` | `POST` | `excluir_patrimonio` | `app.py:4434` | `excluir_patrimonio` | exclui/solicita exclusao | patrimonio | JSON | login_required + CSRF + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/excluir_transacao/<int:id>` | `POST` | `excluir_transacao` | `app.py:4052` | `excluir_transacao` | exclui/solicita exclusao | transacoes/valores | JSON | login_required + CSRF + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/fiscalizacao-contratos` | `GET` | `fiscalizacao_contratos.painel` | `modulos/fiscalizacao_contratos/routes/__init__.py:25` | `painel` | exibe pagina/mantem dados | painel/estaticos do modulo | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/aditivos` | `GET` | `fiscalizacao_contratos.aditivos_lista` | `modulos/fiscalizacao_contratos/routes/aditivos.py:42` | `aditivos_lista` | consulta/lista | aditivos | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/aditivos/<int:aditivo_id>` | `GET` | `fiscalizacao_contratos.aditivos_detalhe` | `modulos/fiscalizacao_contratos/routes/aditivos.py:100` | `aditivos_detalhe` | consulta/lista | aditivos | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/aditivos/<int:aditivo_id>/editar` | `GET,POST` | `fiscalizacao_contratos.aditivos_editar` | `modulos/fiscalizacao_contratos/routes/aditivos.py:124` | `aditivos_editar` | formulario e edicao | aditivos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/aditivos/<int:aditivo_id>/inativar` | `POST` | `fiscalizacao_contratos.aditivos_inativar` | `modulos/fiscalizacao_contratos/routes/aditivos.py:168` | `aditivos_inativar` | altera estado | aditivos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/aditivos/<int:aditivo_id>/reativar` | `POST` | `fiscalizacao_contratos.aditivos_reativar` | `modulos/fiscalizacao_contratos/routes/aditivos.py:184` | `aditivos_reativar` | altera estado | aditivos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/aditivos/novo` | `GET,POST` | `fiscalizacao_contratos.aditivos_novo` | `modulos/fiscalizacao_contratos/routes/aditivos.py:71` | `aditivos_novo` | formulario e criacao/vinculo | aditivos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes` | `GET` | `fiscalizacao_contratos.atestes_lista` | `modulos/fiscalizacao_contratos/routes/atestes.py:50` | `atestes_lista` | consulta/lista | atestes | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>` | `GET` | `fiscalizacao_contratos.atestes_detalhe` | `modulos/fiscalizacao_contratos/routes/atestes.py:86` | `atestes_detalhe` | consulta/lista | atestes | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/atestar` | `POST` | `fiscalizacao_contratos.atestes_atestar` | `modulos/fiscalizacao_contratos/routes/atestes.py:197` | `atestes_atestar` | altera estado | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/cancelar` | `GET,POST` | `fiscalizacao_contratos.atestes_cancelar` | `modulos/fiscalizacao_contratos/routes/atestes.py:232` | `atestes_cancelar` | altera estado | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/devolver` | `GET,POST` | `fiscalizacao_contratos.atestes_devolver` | `modulos/fiscalizacao_contratos/routes/atestes.py:220` | `atestes_devolver` | altera estado | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/documentos/<int:vinculo_id>/inativar` | `POST` | `fiscalizacao_contratos.atestes_documento_inativar` | `modulos/fiscalizacao_contratos/routes/atestes.py:185` | `atestes_documento_inativar` | altera estado | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/documentos/vincular` | `GET,POST` | `fiscalizacao_contratos.atestes_documento_vincular` | `modulos/fiscalizacao_contratos/routes/atestes.py:161` | `atestes_documento_vincular` | formulario e criacao/vinculo | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |

### 15.2 Inventario - parte 2 de 4

| Caminho | Metodos | Endpoint | Origem | Funcao | Operacao | Dados/servicos | Saida | Protecao atual | Risco | Recomendacao |
|---|---|---|---|---|---|---|---|---|---|---|
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/editar` | `GET,POST` | `fiscalizacao_contratos.atestes_editar` | `modulos/fiscalizacao_contratos/routes/atestes.py:95` | `atestes_editar` | formulario e edicao | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/encaminhar` | `GET,POST` | `fiscalizacao_contratos.atestes_encaminhar` | `modulos/fiscalizacao_contratos/routes/atestes.py:228` | `atestes_encaminhar` | altera estado | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/eventos` | `GET` | `fiscalizacao_contratos.atestes_eventos` | `modulos/fiscalizacao_contratos/routes/atestes.py:236` | `atestes_eventos` | consulta/lista | atestes | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/notas/<int:nota_id>/editar` | `GET,POST` | `fiscalizacao_contratos.atestes_nota_editar` | `modulos/fiscalizacao_contratos/routes/atestes.py:152` | `atestes_nota_editar` | formulario e edicao | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/notas/<int:nota_id>/inativar` | `POST` | `fiscalizacao_contratos.atestes_nota_inativar` | `modulos/fiscalizacao_contratos/routes/atestes.py:156` | `atestes_nota_inativar` | altera estado | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/notas/nova` | `GET,POST` | `fiscalizacao_contratos.atestes_nota_nova` | `modulos/fiscalizacao_contratos/routes/atestes.py:148` | `atestes_nota_nova` | formulario e criacao/vinculo | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/<int:ateste_id>/retornar` | `GET,POST` | `fiscalizacao_contratos.atestes_retornar` | `modulos/fiscalizacao_contratos/routes/atestes.py:224` | `atestes_retornar` | altera estado | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/atestes/novo` | `GET,POST` | `fiscalizacao_contratos.atestes_novo` | `modulos/fiscalizacao_contratos/routes/atestes.py:73` | `atestes_novo` | formulario e criacao/vinculo | atestes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ativos` | `GET` | `fiscalizacao_contratos.ativos_lista` | `modulos/fiscalizacao_contratos/routes/ativos.py:40` | `ativos_lista` | consulta/lista | ativos | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ativos/<int:ativo_id>` | `GET` | `fiscalizacao_contratos.ativos_detalhe` | `modulos/fiscalizacao_contratos/routes/ativos.py:99` | `ativos_detalhe` | consulta/lista | ativos | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ativos/<int:ativo_id>/editar` | `GET,POST` | `fiscalizacao_contratos.ativos_editar` | `modulos/fiscalizacao_contratos/routes/ativos.py:119` | `ativos_editar` | formulario e edicao | ativos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ativos/<int:ativo_id>/inativar` | `POST` | `fiscalizacao_contratos.ativos_inativar` | `modulos/fiscalizacao_contratos/routes/ativos.py:153` | `ativos_inativar` | altera estado | ativos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ativos/<int:ativo_id>/reativar` | `POST` | `fiscalizacao_contratos.ativos_reativar` | `modulos/fiscalizacao_contratos/routes/ativos.py:157` | `ativos_reativar` | altera estado | ativos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ativos/<int:ativo_id>/vincular` | `GET,POST` | `fiscalizacao_contratos.ativos_vincular` | `modulos/fiscalizacao_contratos/routes/ativos.py:161` | `ativos_vincular` | formulario e criacao/vinculo | ativos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ativos/novo` | `GET,POST` | `fiscalizacao_contratos.ativos_novo` | `modulos/fiscalizacao_contratos/routes/ativos.py:78` | `ativos_novo` | formulario e criacao/vinculo | ativos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ativos/vinculos` | `GET` | `fiscalizacao_contratos.ativos_vinculos_lista` | `modulos/fiscalizacao_contratos/routes/ativos.py:190` | `ativos_vinculos_lista` | consulta/lista | ativos | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ativos/vinculos/<int:vinculo_id>/encerrar` | `POST` | `fiscalizacao_contratos.ativos_vinculo_encerrar` | `modulos/fiscalizacao_contratos/routes/ativos.py:203` | `ativos_vinculo_encerrar` | altera estado | ativos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/contratos` | `GET` | `fiscalizacao_contratos.contratos_lista` | `modulos/fiscalizacao_contratos/routes/contratos.py:54` | `contratos_lista` | consulta/lista | contratos | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/contratos/<int:contrato_id>` | `GET` | `fiscalizacao_contratos.contratos_detalhe` | `modulos/fiscalizacao_contratos/routes/contratos.py:136` | `contratos_detalhe` | consulta/lista | contratos | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/contratos/<int:contrato_id>/editar` | `GET,POST` | `fiscalizacao_contratos.contratos_editar` | `modulos/fiscalizacao_contratos/routes/contratos.py:228` | `contratos_editar` | formulario e edicao | contratos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/contratos/<int:contrato_id>/inativar` | `POST` | `fiscalizacao_contratos.contratos_inativar` | `modulos/fiscalizacao_contratos/routes/contratos.py:299` | `contratos_inativar` | altera estado | contratos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/contratos/<int:contrato_id>/reativar` | `POST` | `fiscalizacao_contratos.contratos_reativar` | `modulos/fiscalizacao_contratos/routes/contratos.py:315` | `contratos_reativar` | altera estado | contratos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/contratos/novo` | `GET,POST` | `fiscalizacao_contratos.contratos_novo` | `modulos/fiscalizacao_contratos/routes/contratos.py:97` | `contratos_novo` | formulario e criacao/vinculo | contratos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/documentos` | `GET` | `fiscalizacao_contratos.documentos_lista` | `modulos/fiscalizacao_contratos/routes/documentos.py:46` | `documentos_lista` | consulta/lista | documentos | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/documentos/<int:documento_id>` | `GET` | `fiscalizacao_contratos.documentos_detalhe` | `modulos/fiscalizacao_contratos/routes/documentos.py:124` | `documentos_detalhe` | consulta/lista | documentos | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/documentos/<int:documento_id>/arquivo` | `GET` | `fiscalizacao_contratos.documentos_arquivo` | `modulos/fiscalizacao_contratos/routes/documentos.py:140` | `documentos_arquivo` | gera/abre arquivo | documentos | arquivo/redirect privado | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/documentos/<int:documento_id>/inativar` | `POST` | `fiscalizacao_contratos.documentos_inativar` | `modulos/fiscalizacao_contratos/routes/documentos.py:160` | `documentos_inativar` | altera estado | documentos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/documentos/<int:documento_id>/reativar` | `POST` | `fiscalizacao_contratos.documentos_reativar` | `modulos/fiscalizacao_contratos/routes/documentos.py:176` | `documentos_reativar` | altera estado | documentos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/documentos/novo` | `GET,POST` | `fiscalizacao_contratos.documentos_novo` | `modulos/fiscalizacao_contratos/routes/documentos.py:83` | `documentos_novo` | formulario e criacao/vinculo | documentos | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/empresas` | `GET` | `fiscalizacao_contratos.empresas_lista` | `modulos/fiscalizacao_contratos/routes/empresas.py:27` | `empresas_lista` | consulta/lista | empresas | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/empresas/<int:empresa_id>` | `GET` | `fiscalizacao_contratos.empresas_detalhe` | `modulos/fiscalizacao_contratos/routes/empresas.py:84` | `empresas_detalhe` | consulta/lista | empresas | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/empresas/<int:empresa_id>/editar` | `GET,POST` | `fiscalizacao_contratos.empresas_editar` | `modulos/fiscalizacao_contratos/routes/empresas.py:102` | `empresas_editar` | formulario e edicao | empresas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/empresas/<int:empresa_id>/inativar` | `POST` | `fiscalizacao_contratos.empresas_inativar` | `modulos/fiscalizacao_contratos/routes/empresas.py:156` | `empresas_inativar` | altera estado | empresas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/empresas/<int:empresa_id>/reativar` | `POST` | `fiscalizacao_contratos.empresas_reativar` | `modulos/fiscalizacao_contratos/routes/empresas.py:170` | `empresas_reativar` | altera estado | empresas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/empresas/consultar-cep/<string:cep>` | `GET` | `fiscalizacao_contratos.empresas_consultar_cep` | `modulos/fiscalizacao_contratos/routes/empresas.py:194` | `empresas_consultar_cep` | exibe pagina/mantem dados | empresas | JSON | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/empresas/consultar-cnpj/<string:cnpj>` | `GET` | `fiscalizacao_contratos.empresas_consultar_cnpj` | `modulos/fiscalizacao_contratos/routes/empresas.py:184` | `empresas_consultar_cnpj` | exibe pagina/mantem dados | empresas | JSON | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/empresas/nova` | `GET,POST` | `fiscalizacao_contratos.empresas_nova` | `modulos/fiscalizacao_contratos/routes/empresas.py:43` | `empresas_nova` | formulario e criacao/vinculo | empresas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/fiscalizacoes` | `GET` | `fiscalizacao_contratos.fiscalizacoes_lista` | `modulos/fiscalizacao_contratos/routes/fiscalizacoes.py:45` | `fiscalizacoes_lista` | consulta/lista | fiscalizacoes | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/fiscalizacoes/<int:fiscalizacao_id>` | `GET` | `fiscalizacao_contratos.fiscalizacoes_detalhe` | `modulos/fiscalizacao_contratos/routes/fiscalizacoes.py:80` | `fiscalizacoes_detalhe` | consulta/lista | fiscalizacoes | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/fiscalizacoes/<int:fiscalizacao_id>/cancelar` | `GET,POST` | `fiscalizacao_contratos.fiscalizacoes_cancelar` | `modulos/fiscalizacao_contratos/routes/fiscalizacoes.py:119` | `fiscalizacoes_cancelar` | altera estado | fiscalizacoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/fiscalizacoes/<int:fiscalizacao_id>/editar` | `GET,POST` | `fiscalizacao_contratos.fiscalizacoes_editar` | `modulos/fiscalizacao_contratos/routes/fiscalizacoes.py:92` | `fiscalizacoes_editar` | formulario e edicao | fiscalizacoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/fiscalizacoes/<int:fiscalizacao_id>/finalizar` | `POST` | `fiscalizacao_contratos.fiscalizacoes_finalizar` | `modulos/fiscalizacao_contratos/routes/fiscalizacoes.py:115` | `fiscalizacoes_finalizar` | altera estado | fiscalizacoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/fiscalizacoes/<int:fiscalizacao_id>/reabrir` | `GET,POST` | `fiscalizacao_contratos.fiscalizacoes_reabrir` | `modulos/fiscalizacao_contratos/routes/fiscalizacoes.py:137` | `fiscalizacoes_reabrir` | altera estado | fiscalizacoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/fiscalizacoes/nova` | `GET,POST` | `fiscalizacao_contratos.fiscalizacoes_nova` | `modulos/fiscalizacao_contratos/routes/fiscalizacoes.py:65` | `fiscalizacoes_nova` | formulario e criacao/vinculo | fiscalizacoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes` | `GET` | `fiscalizacao_contratos.medicoes_lista` | `modulos/fiscalizacao_contratos/routes/medicoes.py:111` | `medicoes_lista` | consulta/lista | medicoes | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |

### 15.3 Inventario - parte 3 de 4

| Caminho | Metodos | Endpoint | Origem | Funcao | Operacao | Dados/servicos | Saida | Protecao atual | Risco | Recomendacao |
|---|---|---|---|---|---|---|---|---|---|---|
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>` | `GET` | `fiscalizacao_contratos.medicoes_detalhe` | `modulos/fiscalizacao_contratos/routes/medicoes.py:174` | `medicoes_detalhe` | consulta/lista | medicoes | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/ajustes/<int:ajuste_id>/editar` | `GET,POST` | `fiscalizacao_contratos.medicoes_ajuste_editar` | `modulos/fiscalizacao_contratos/routes/medicoes.py:361` | `medicoes_ajuste_editar` | formulario e edicao | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/ajustes/<int:ajuste_id>/inativar` | `POST` | `fiscalizacao_contratos.medicoes_ajuste_inativar` | `modulos/fiscalizacao_contratos/routes/medicoes.py:366` | `medicoes_ajuste_inativar` | altera estado | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/ajustes/novo` | `GET,POST` | `fiscalizacao_contratos.medicoes_ajuste_novo` | `modulos/fiscalizacao_contratos/routes/medicoes.py:356` | `medicoes_ajuste_novo` | formulario e criacao/vinculo | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/aprovar` | `GET,POST` | `fiscalizacao_contratos.medicoes_aprovar` | `modulos/fiscalizacao_contratos/routes/medicoes.py:579` | `medicoes_aprovar` | altera estado | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/cancelar` | `GET,POST` | `fiscalizacao_contratos.medicoes_cancelar` | `modulos/fiscalizacao_contratos/routes/medicoes.py:584` | `medicoes_cancelar` | altera estado | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/devolver` | `GET,POST` | `fiscalizacao_contratos.medicoes_devolver` | `modulos/fiscalizacao_contratos/routes/medicoes.py:574` | `medicoes_devolver` | altera estado | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/documentos/<int:vinculo_id>/inativar` | `POST` | `fiscalizacao_contratos.medicoes_documento_inativar` | `modulos/fiscalizacao_contratos/routes/medicoes.py:491` | `medicoes_documento_inativar` | altera estado | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/documentos/enviar` | `GET,POST` | `fiscalizacao_contratos.medicoes_documento_enviar` | `modulos/fiscalizacao_contratos/routes/medicoes.py:427` | `medicoes_documento_enviar` | formulario e criacao/vinculo | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/documentos/vincular` | `GET,POST` | `fiscalizacao_contratos.medicoes_documento_vincular` | `modulos/fiscalizacao_contratos/routes/medicoes.py:375` | `medicoes_documento_vincular` | formulario e criacao/vinculo | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/editar` | `GET,POST` | `fiscalizacao_contratos.medicoes_editar` | `modulos/fiscalizacao_contratos/routes/medicoes.py:198` | `medicoes_editar` | formulario e edicao | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/enviar` | `POST` | `fiscalizacao_contratos.medicoes_enviar` | `modulos/fiscalizacao_contratos/routes/medicoes.py:512` | `medicoes_enviar` | formulario e criacao/vinculo | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/eventos` | `GET` | `fiscalizacao_contratos.medicoes_eventos` | `modulos/fiscalizacao_contratos/routes/medicoes.py:594` | `medicoes_eventos` | consulta/lista | medicoes | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/itens/<int:item_id>/editar` | `GET,POST` | `fiscalizacao_contratos.medicoes_item_editar` | `modulos/fiscalizacao_contratos/routes/medicoes.py:288` | `medicoes_item_editar` | formulario e edicao | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/itens/<int:item_id>/inativar` | `POST` | `fiscalizacao_contratos.medicoes_item_inativar` | `modulos/fiscalizacao_contratos/routes/medicoes.py:293` | `medicoes_item_inativar` | altera estado | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/itens/novo` | `GET,POST` | `fiscalizacao_contratos.medicoes_item_novo` | `modulos/fiscalizacao_contratos/routes/medicoes.py:283` | `medicoes_item_novo` | formulario e criacao/vinculo | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/revisao` | `GET,POST` | `fiscalizacao_contratos.medicoes_revisao` | `modulos/fiscalizacao_contratos/routes/medicoes.py:589` | `medicoes_revisao` | formulario e criacao/vinculo | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/<int:medicao_id>/versoes` | `GET` | `fiscalizacao_contratos.medicoes_versoes` | `modulos/fiscalizacao_contratos/routes/medicoes.py:599` | `medicoes_versoes` | consulta/lista | medicoes | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/medicoes/nova` | `GET,POST` | `fiscalizacao_contratos.medicoes_nova` | `modulos/fiscalizacao_contratos/routes/medicoes.py:146` | `medicoes_nova` | formulario e criacao/vinculo | medicoes | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ocorrencias` | `GET` | `fiscalizacao_contratos.ocorrencias_lista` | `modulos/fiscalizacao_contratos/routes/ocorrencias.py:34` | `ocorrencias_lista` | consulta/lista | ocorrencias | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ocorrencias/<int:ocorrencia_id>` | `GET` | `fiscalizacao_contratos.ocorrencias_detalhe` | `modulos/fiscalizacao_contratos/routes/ocorrencias.py:61` | `ocorrencias_detalhe` | consulta/lista | ocorrencias | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ocorrencias/<int:ocorrencia_id>/acompanhamentos/novo` | `GET,POST` | `fiscalizacao_contratos.ocorrencias_acompanhamento_novo` | `modulos/fiscalizacao_contratos/routes/ocorrencias.py:96` | `ocorrencias_acompanhamento_novo` | formulario e criacao/vinculo | ocorrencias | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ocorrencias/<int:ocorrencia_id>/editar` | `GET,POST` | `fiscalizacao_contratos.ocorrencias_editar` | `modulos/fiscalizacao_contratos/routes/ocorrencias.py:68` | `ocorrencias_editar` | formulario e edicao | ocorrencias | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ocorrencias/<int:ocorrencia_id>/inativar` | `POST` | `fiscalizacao_contratos.ocorrencias_inativar` | `modulos/fiscalizacao_contratos/routes/ocorrencias.py:89` | `ocorrencias_inativar` | altera estado | ocorrencias | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ocorrencias/<int:ocorrencia_id>/reativar` | `POST` | `fiscalizacao_contratos.ocorrencias_reativar` | `modulos/fiscalizacao_contratos/routes/ocorrencias.py:92` | `ocorrencias_reativar` | altera estado | ocorrencias | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/ocorrencias/nova` | `GET,POST` | `fiscalizacao_contratos.ocorrencias_nova` | `modulos/fiscalizacao_contratos/routes/ocorrencias.py:48` | `ocorrencias_nova` | formulario e criacao/vinculo | ocorrencias | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas` | `GET` | `fiscalizacao_contratos.planilhas_lista` | `modulos/fiscalizacao_contratos/routes/planilhas.py:50` | `planilhas_lista` | consulta/lista | planilhas | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>` | `GET` | `fiscalizacao_contratos.planilhas_detalhe` | `modulos/fiscalizacao_contratos/routes/planilhas.py:117` | `planilhas_detalhe` | consulta/lista | planilhas | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>/consolidar` | `POST` | `fiscalizacao_contratos.planilhas_consolidar` | `modulos/fiscalizacao_contratos/routes/planilhas.py:267` | `planilhas_consolidar` | altera estado | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>/definir-vigente` | `POST` | `fiscalizacao_contratos.planilhas_definir_vigente` | `modulos/fiscalizacao_contratos/routes/planilhas.py:272` | `planilhas_definir_vigente` | altera estado | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>/editar` | `GET,POST` | `fiscalizacao_contratos.planilhas_editar` | `modulos/fiscalizacao_contratos/routes/planilhas.py:140` | `planilhas_editar` | formulario e edicao | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>/inativar` | `POST` | `fiscalizacao_contratos.planilhas_inativar` | `modulos/fiscalizacao_contratos/routes/planilhas.py:277` | `planilhas_inativar` | altera estado | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>/itens/<int:item_id>/editar` | `GET,POST` | `fiscalizacao_contratos.planilhas_item_editar` | `modulos/fiscalizacao_contratos/routes/planilhas.py:213` | `planilhas_item_editar` | formulario e edicao | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>/itens/<int:item_id>/inativar` | `POST` | `fiscalizacao_contratos.planilhas_item_inativar` | `modulos/fiscalizacao_contratos/routes/planilhas.py:257` | `planilhas_item_inativar` | altera estado | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>/itens/<int:item_id>/reativar` | `POST` | `fiscalizacao_contratos.planilhas_item_reativar` | `modulos/fiscalizacao_contratos/routes/planilhas.py:262` | `planilhas_item_reativar` | altera estado | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>/itens/novo` | `GET,POST` | `fiscalizacao_contratos.planilhas_item_novo` | `modulos/fiscalizacao_contratos/routes/planilhas.py:173` | `planilhas_item_novo` | formulario e criacao/vinculo | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>/nova-versao` | `GET,POST` | `fiscalizacao_contratos.planilhas_nova_versao` | `modulos/fiscalizacao_contratos/routes/planilhas.py:287` | `planilhas_nova_versao` | formulario e criacao/vinculo | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/<int:planilha_id>/reativar` | `POST` | `fiscalizacao_contratos.planilhas_reativar` | `modulos/fiscalizacao_contratos/routes/planilhas.py:282` | `planilhas_reativar` | altera estado | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/contratos/<int:contrato_id>/comparar` | `GET` | `fiscalizacao_contratos.planilhas_comparar` | `modulos/fiscalizacao_contratos/routes/planilhas.py:131` | `planilhas_comparar` | consulta/lista | planilhas | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/planilhas/nova` | `GET,POST` | `fiscalizacao_contratos.planilhas_nova` | `modulos/fiscalizacao_contratos/routes/planilhas.py:88` | `planilhas_nova` | formulario e criacao/vinculo | planilhas | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/servidores` | `GET` | `fiscalizacao_contratos.servidores_lista` | `modulos/fiscalizacao_contratos/routes/servidores.py:22` | `servidores_lista` | consulta/lista | servidores | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/servidores/<int:servidor_id>` | `GET` | `fiscalizacao_contratos.servidores_detalhe` | `modulos/fiscalizacao_contratos/routes/servidores.py:86` | `servidores_detalhe` | consulta/lista | servidores | HTML/JSON/redirect | admin_required + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/servidores/<int:servidor_id>/editar` | `GET,POST` | `fiscalizacao_contratos.servidores_editar` | `modulos/fiscalizacao_contratos/routes/servidores.py:104` | `servidores_editar` | formulario e edicao | servidores | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/servidores/<int:servidor_id>/inativar` | `POST` | `fiscalizacao_contratos.servidores_inativar` | `modulos/fiscalizacao_contratos/routes/servidores.py:160` | `servidores_inativar` | altera estado | servidores | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/servidores/<int:servidor_id>/reativar` | `POST` | `fiscalizacao_contratos.servidores_reativar` | `modulos/fiscalizacao_contratos/routes/servidores.py:176` | `servidores_reativar` | altera estado | servidores | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |

### 15.4 Inventario - parte 4 de 4

| Caminho | Metodos | Endpoint | Origem | Funcao | Operacao | Dados/servicos | Saida | Protecao atual | Risco | Recomendacao |
|---|---|---|---|---|---|---|---|---|---|---|
| `/fiscalizacao-contratos/servidores/novo` | `GET,POST` | `fiscalizacao_contratos.servidores_novo` | `modulos/fiscalizacao_contratos/routes/servidores.py:43` | `servidores_novo` | formulario e criacao/vinculo | servidores | HTML/JSON/redirect | admin_required + CSRF + Basic homologacao | baixo: admin global | C - administrador |
| `/fiscalizacao-contratos/static/<path:filename>` | `GET` | `fiscalizacao_contratos.static` | `Flask automatica` | `send_static_file` | entrega estatico | painel/estaticos do modulo | arquivo estatico | publica + Basic homologacao | baixo: admin global | A - publica essencial |
| `/gerar_extrato_bancario` | `POST` | `gerar_extrato_bancario_json` | `app.py:2633` | `gerar_extrato_bancario_json` | exibe pagina/mantem dados | extrato/valores | JSON | publica + CSRF + Basic homologacao | critico: publico mutavel | D - regra especifica |
| `/gerar_relatorio` | `POST` | `gerar_relatorio` | `app.py:2176` | `gerar_relatorio` | exibe pagina/mantem dados | relatorios/valores | JSON | publica + CSRF + Basic homologacao | critico: publico mutavel | D - regra especifica |
| `/get_associado/<int:id>` | `GET` | `get_associado` | `app.py:702` | `get_associado` | consulta/lista | associados/dados pessoais | JSON | login_required + Basic homologacao | medio/baixo | D - regra especifica |
| `/get_associados_ativos` | `GET` | `get_associados_ativos` | `app.py:1100` | `get_associados_ativos` | consulta/lista | associados/dados pessoais | JSON | publica + Basic homologacao | alto: dado interno publico | D - regra especifica |
| `/get_cadastro/<int:id>` | `GET` | `get_cadastro` | `app.py:3224` | `get_cadastro` | consulta/lista | clientes/fornecedores | JSON | login_required + Basic homologacao | medio/baixo | D - regra especifica |
| `/get_cadastros_ativos` | `GET` | `get_cadastros_ativos` | `app.py:981` | `get_cadastros_ativos` | consulta/lista | clientes/fornecedores | JSON | publica + Basic homologacao | alto: dado interno publico | D - regra especifica |
| `/get_clientes_fornecedores_com_pendencias` | `GET` | `get_clientes_fornecedores_com_pendencias` | `app.py:1455` | `get_clientes_fornecedores_com_pendencias` | consulta/lista | interface/sessao | JSON | publica + Basic homologacao | alto: dado interno publico | D - regra especifica |
| `/get_conta_corrente_detalhe/<int:id>` | `GET` | `get_conta_corrente_detalhe` | `app.py:3491` | `get_conta_corrente_detalhe` | consulta/lista | contas bancarias | JSON | login_required + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/get_contas_correntes` | `GET` | `get_contas_correntes_fluxo_caixa` | `app.py:1067` | `get_contas_correntes_fluxo_caixa` | consulta/lista | contas bancarias | JSON | publica + Basic homologacao | alto: dado interno publico | D - regra especifica |
| `/get_detalhes_solicitacao/<int:id>` | `GET` | `get_detalhes_solicitacao` | `app.py:3540` | `get_detalhes_solicitacao` | consulta/lista | solicitacoes | JSON | login_required + Basic homologacao | medio/baixo | D - regra especifica |
| `/get_distinct_grupos` | `GET` | `get_distinct_grupos` | `app.py:1122` | `get_distinct_grupos` | consulta/lista | interface/sessao | JSON | login_required + Basic homologacao | medio/baixo | B - autenticado |
| `/get_distinct_subgrupos` | `GET` | `get_distinct_subgrupos` | `app.py:1152` | `get_distinct_subgrupos` | consulta/lista | catalogo | JSON | login_required + Basic homologacao | medio/baixo | B - autenticado |
| `/get_items_for_filters` | `GET` | `get_items_for_filters` | `app.py:1172` | `get_items_for_filters` | consulta/lista | interface/sessao | JSON | login_required + Basic homologacao | medio/baixo | B - autenticado |
| `/get_movimentacao_detalhes/<int:id>` | `GET` | `get_movimentacao_detalhes` | `app.py:4096` | `get_movimentacao_detalhes` | consulta/lista | interface/sessao | JSON | login_required + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/get_notas_em_aberto` | `GET` | `get_notas_em_aberto` | `app.py:1527` | `get_notas_em_aberto` | consulta/lista | interface/sessao | JSON | publica + Basic homologacao | alto: dado interno publico | D - regra especifica |
| `/get_patrimonio_detalhes/<int:id>` | `GET` | `get_patrimonio_detalhes` | `app.py:4317` | `get_patrimonio_detalhes` | consulta/lista | patrimonio | JSON | login_required + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/get_produtos_servicos` | `GET` | `get_produtos_servicos` | `app.py:962` | `get_produtos_servicos` | consulta/lista | catalogo | JSON | publica + Basic homologacao | alto: dado interno publico | B - autenticado |
| `/get_relatorio_catalog_options` | `GET` | `get_relatorio_catalog_options` | `app.py:1840` | `get_relatorio_catalog_options` | consulta/lista | relatorios/valores | JSON | publica + Basic homologacao | alto: dado interno publico | B - autenticado |
| `/get_relatorio_entidades_para_filtro` | `GET` | `get_relatorio_entidades_para_filtro` | `app.py:1898` | `get_relatorio_entidades_para_filtro` | consulta/lista | relatorios/valores | JSON | publica + Basic homologacao | alto: dado interno publico | D - regra especifica |
| `/get_relatorio_tipos_atividade_transacao` | `GET` | `get_relatorio_tipos_atividade_transacao` | `app.py:1818` | `get_relatorio_tipos_atividade_transacao` | consulta/lista | transacoes/valores | JSON | publica + Basic homologacao | alto: dado interno publico | B - autenticado |
| `/get_relatorio_uvrs` | `GET` | `get_relatorio_uvrs` | `app.py:1795` | `get_relatorio_uvrs` | consulta/lista | relatorios/valores | JSON | publica + Basic homologacao | alto: dado interno publico | D - regra especifica |
| `/get_resumo_fluxo_caixa` | `GET` | `get_resumo_fluxo_caixa` | `app.py:1017` | `get_resumo_fluxo_caixa` | consulta/lista | fluxo/valores | JSON | publica + Basic homologacao | alto: dado interno publico | D - regra especifica |
| `/get_solicitacoes_pendentes` | `GET` | `get_solicitacoes_pendentes` | `app.py:2757` | `get_solicitacoes_pendentes` | consulta/lista | interface/sessao | JSON | login_required + admin manual + Basic homologacao | medio/baixo | C - administrador |
| `/get_transacao_detalhes/<int:id>` | `GET` | `get_transacao_detalhes` | `app.py:3815` | `get_transacao_detalhes` | consulta/lista | transacoes/valores | JSON | login_required + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/health` | `GET` | `health` | `app.py:72` | `health` | health sem servicos | interface/sessao | JSON | publica; fora da Basic | medio/baixo | A - publica essencial |
| `/imprimir_ficha_associado/<int:id>` | `GET` | `imprimir_ficha_associado` | `app.py:2989` | `imprimir_ficha_associado` | gera/abre arquivo | associados/dados pessoais | arquivo/redirect privado | login_required + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/imprimir_ficha_cadastro/<int:id>` | `GET` | `imprimir_ficha_cadastro` | `app.py:3254` | `imprimir_ficha_cadastro` | gera/abre arquivo | clientes/fornecedores | arquivo/redirect privado | login_required + Basic homologacao | alto: possivel IDOR | D - regra especifica |
| `/login` | `GET,POST` | `login` | `app.py:337` | `login` | autentica/abre sessao | usuarios/sessao | HTML/JSON/redirect | publica + CSRF + Basic homologacao | medio: enumeracao de conta | A - publica essencial |
| `/logout` | `POST` | `logout` | `app.py:366` | `logout` | encerra sessao | sessao | HTML/JSON/redirect | login_required + CSRF + Basic homologacao | medio/baixo | B - autenticado |
| `/registrar_denuncia` | `POST` | `registrar_denuncia` | `app.py:1743` | `registrar_denuncia` | formulario e criacao/vinculo | denuncias | HTML/JSON/redirect | publica + CSRF + Basic homologacao | critico: publico mutavel | E - desativada online |
| `/registrar_fluxo_caixa` | `POST` | `registrar_fluxo_caixa` | `app.py:1601` | `registrar_fluxo_caixa` | formulario e criacao/vinculo | fluxo/valores | JSON | publica + CSRF + Basic homologacao | critico: publico mutavel | D - regra especifica |
| `/registrar_transacao_financeira` | `POST` | `registrar_transacao_financeira` | `app.py:1204` | `registrar_transacao_financeira` | formulario e criacao/vinculo | transacoes/valores | HTML/JSON/redirect | publica + CSRF + Basic homologacao | critico: publico mutavel | D - regra especifica |
| `/responder_solicitacao` | `POST` | `responder_solicitacao` | `app.py:2866` | `responder_solicitacao` | exibe pagina/mantem dados | solicitacoes | JSON | login_required + admin manual + CSRF + Basic homologacao | medio/baixo | C - administrador |
| `/static/<path:filename>` | `GET` | `static` | `Flask automatica` | `<lambda>` | entrega estatico | interface/sessao | arquivo estatico | publica + Basic homologacao | medio/baixo | A - publica essencial |
| `/sucesso` | `GET` | `sucesso` | `app.py:4476` | `sucesso` | exibe pagina/mantem dados | interface/sessao | HTML/JSON/redirect | publica + Basic homologacao | medio/baixo | B - autenticado |
| `/sucesso_associado` | `GET` | `sucesso_associado` | `app.py:4478` | `sucesso_associado` | exibe pagina/mantem dados | associados/dados pessoais | HTML/JSON/redirect | publica + Basic homologacao | medio/baixo | B - autenticado |
| `/sucesso_conta_corrente` | `GET` | `sucesso_conta_corrente` | `app.py:4484` | `sucesso_conta_corrente` | exibe pagina/mantem dados | contas bancarias | HTML/JSON/redirect | publica + Basic homologacao | medio/baixo | B - autenticado |
| `/sucesso_denuncia` | `GET` | `sucesso_denuncia` | `app.py:4486` | `sucesso_denuncia` | exibe pagina/mantem dados | denuncias | HTML/JSON/redirect | publica + Basic homologacao | medio/baixo | E - desativada online |
| `/sucesso_produto_servico` | `GET` | `sucesso_produto_servico` | `app.py:4482` | `sucesso_produto_servico` | exibe pagina/mantem dados | catalogo | HTML/JSON/redirect | publica + Basic homologacao | medio/baixo | B - autenticado |
| `/sucesso_transacao` | `GET` | `sucesso_transacao` | `app.py:4480` | `sucesso_transacao` | exibe pagina/mantem dados | transacoes/valores | HTML/JSON/redirect | publica + Basic homologacao | medio/baixo | B - autenticado |

## 16. Roteiro da futura H2A.3B

1. Criar testes de caracterização para os 22 bloqueadores e os 15 possíveis IDORs.
2. Centralizar os decorators necessários sem alterar as regras funcionais.
3. Aplicar os níveis B e C e validar UVR, associação e objeto nas rotas D.
4. Desativar as rotas E nos ambientes online por configuração segura.
5. Padronizar erros JSON para não expor SQL, caminhos ou exceções.
6. Testar cada nível, repetir a suíte e só então rever a liberação da homologação.

## 17. Método de auditoria e limites

O inventário combinou `app.url_map` com busca estática de decorators,
`current_user`, `role`, `uvr_acesso`, sessão, métodos HTTP, SQL, commits,
rollbacks, uploads, destruição no Cloudinary, arquivos, redirects, `next` e
`jsonify`. A importação ocorreu em `testing`, com serviços reais bloqueados.

Não houve acesso a banco, Cloudinary ou API externa; também não houve upload,
download, migration ou deploy. A matriz apenas propõe níveis de acesso: nenhuma
permissão foi concedida, removida ou alterada.

## 18. H2A.3B.1 — proteção implementada nas 11 rotas mutáveis

Em **22/07/2026**, o primeiro bloco de correção eliminou o acesso público às 11
rotas mutáveis apontadas no diagnóstico. A tabela abaixo registra a transição;
a seção 5 continua sendo a fotografia histórica anterior à correção. As
referências de teste desta tabela pertencem a
`tests/test_rotas_mutaveis_h2a3b1.py`.

| Rota | Proteção anterior | Proteção implementada e nível final | Teste | Pendência específica |
|---|---|---|---|---|
| `POST /baixar_csv_extrato` | Pública + CSRF | Login interno, resposta JSON segura e validação da conta pela UVR do `current_user` — D | `test_03`, `test_07`, `test_10` | Nenhuma no modelo atual de UVR |
| `POST /baixar_csv_relatorio` | Pública + CSRF | Login interno, UVR derivada da sessão e validação da entidade — D | `test_03`, `test_08`, `test_09` | Nenhuma no modelo atual de UVR |
| `POST /baixar_pdf_extrato` | Pública + CSRF | Login interno, resposta JSON segura e validação da conta pela UVR do `current_user` — D | `test_03`, `test_07`, `test_10` | Nenhuma no modelo atual de UVR |
| `POST /baixar_pdf_relatorio_financeiro` | Pública + CSRF | Login interno, UVR derivada da sessão e validação da entidade — D | `test_03`, `test_08`, `test_09` | Nenhuma no modelo atual de UVR |
| `POST /cadastrar_conta_corrente` | Pública + CSRF | Login interno; UVR recebida é conferida e substituída pela UVR do `current_user` — D | `test_03`, `test_06`, `test_11` | Associação continua atributo funcional; o modelo de usuário autoriza por UVR |
| `POST /cadastrar_produto_servico` | Pública + CSRF | `admin_required` — C | `test_03`, `test_06`, `test_13` | Nenhuma |
| `POST /gerar_extrato_bancario` | Pública + CSRF | Login interno, resposta JSON segura e validação da conta pela UVR do `current_user` — D | `test_03`, `test_07`, `test_10` | Nenhuma no modelo atual de UVR |
| `POST /gerar_relatorio` | Pública + CSRF | Login interno, UVR derivada da sessão e validação da entidade — D | `test_03`, `test_08`, `test_09` | Nenhuma no modelo atual de UVR |
| `POST /registrar_denuncia` | Pública + CSRF | HTTP 404 em homologação/produção; em desenvolvimento/teste exige login e UVR da sessão — E | `test_03`, `test_06`, `test_11`, `test_14` | A outra rota proposta para desativação online permanece fora deste bloco |
| `POST /registrar_fluxo_caixa` | Pública + CSRF | Login interno; UVR, conta, transações e cadastro são conferidos no servidor — D | `test_03`, `test_06`, `test_12` | Associação continua atributo funcional; o modelo de usuário autoriza por UVR |
| `POST /registrar_transacao_financeira` | Pública + CSRF | Login interno; UVR e entidade de origem são conferidas no servidor — D | `test_03`, `test_06`, `test_11` | Associação continua atributo funcional; o modelo de usuário autoriza por UVR |

Resultado deste bloco: **10 rotas receberam `login_required`**, sendo nove com
regra específica D e uma desativada online; **uma recebeu `admin_required`**;
**uma foi desativada em homologação e produção**. As sete APIs JSON deste grupo
retornam erro JSON seguro quando não há sessão, em vez de HTML inesperado. CSRF
e a barreira Basic continuam camadas independentes e não concedem login ou
autorização.

O login passou a usar uma mensagem única para usuário inexistente, inativo ou
senha errada. Quando a consulta não encontra uma conta ativa, ainda é executada
uma verificação contra um hash fictício criado uma única vez por processo. Isso
reduz a diferença temporal sem prometer tempo constante e sem registrar senha,
hash, formulário, token CSRF ou cabeçalho de autorização.

O inventário continua com **177 rotas** e as **105 rotas funcionais do Blueprint
de Fiscalização de Contratos permanecem administrativas**. Após este bloco, a
fotografia de proteção passa a ser 23 públicas, 48 com login interno e 106
administrativas. Permanecem os 11 GETs públicos que consultam banco, os 15
possíveis IDORs, a revisão geral de JSON/AJAX e downloads fora deste conjunto,
além dos
demais riscos de homologação já documentados.

A revisão técnica final confirmou **105 rotas mutáveis** e nenhuma rota mutável
pública inadequada. O escopo de UVR passou a constar também no SQL final de
extratos, relatórios, transações e fluxo de caixa. Os testes adicionais cobrem
objeto inexistente, falha fechada do banco, administrador global conforme a
política, usuário inativado e lista mista com dois IDs autorizados e um alheio.
Resultado: **28 testes específicos e 435 testes totais aprovados**.
