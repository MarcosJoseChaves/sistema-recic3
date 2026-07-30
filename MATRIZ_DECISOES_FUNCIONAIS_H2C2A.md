# Matriz de decisões funcionais — H2C.2A

## 1. Finalidade

Este documento transforma as decisões aprovadas pelo gestor em um plano de
evolução pequeno, verificável e compreensível para os usuários.

A análise foi feita somente com documentos e código versionado, em modo de
leitura. Não houve acesso ao banco, execução de SQL, migration, alteração
funcional, instalação, teste de integração ou deploy.

Referência estrutural: schema auditado pelo SHA-256
`e2a9237b123aae8cab94e94055c9e31061b00f341536678b604f43f684c228cc`.

## 2. Princípios aprovados

1. nenhum dado, tabela ou coluna será removido nesta fase;
2. uma estrutura existente não se torna automaticamente um item de menu;
3. funcionalidade incompleta permanece oculta de usuários comuns;
4. histórico é preservado por inativação, versionamento ou evento;
5. telas são organizadas por tarefas do usuário, não por nomes técnicos;
6. cada incremento terá escopo único, testes, homologação e caminho de
   reversão;
7. mudanças de vínculo textual para identificador serão graduais e manterão
   compatibilidade durante a transição;
8. migrations históricas não serão reescritas para esconder a evolução.

## 3. Matriz principal

| Tema | Situação atual | Problema identificado | Decisão validada | Comportamento esperado para o usuário | Alteração futura necessária | Impacto no banco | Impacto no código | Risco | Prioridade | Dependências | Critério de aceite | Etapa sugerida |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| Preservação geral | 64 tabelas e estruturas históricas | uso de parte delas ainda não foi determinado | não excluir estrutura ou dados | nada desaparece durante a evolução | política formal de retenção e inativação | somente mudanças aditivas | consultas devem continuar compatíveis | alto se houver remoção prematura | 1 | decisões por módulo | comparação antes/depois sem perda | regra transversal |
| Patrimônio | CRUD legado com `DELETE`; 38 colunas | exclusão física e vínculo lógico sem FK | substituir uso normal por inativação e permitir reativação | usuário inativa, consulta histórico e reativa se autorizado | especificar estado, filtros, alertas e permissões | coluna/índices aditivos ou tabela de evento, se aprovados | trocar exclusão por transição de estado | alto | 1 | regra sobre transações vinculadas | nenhuma exclusão física; inativo fora de novas movimentações | H2C.2B |
| Grupos | cadastro físico existe, sem ligação por ID | hierarquia não é relacional | estruturar grupo → subgrupo → produto | primeiro escolhe grupo | definir entidade oficial e estados | FKs/colunas aditivas futuras | serviços e seletores encadeados | alto | 1 | tratamento dos registros antigos | grupo selecionável e validado | H2C.2C |
| Subgrupos | ligados ao grupo por texto `atividade_pai` | renomear texto pode gerar inconsistência | vincular por identificador no futuro | lista mostra apenas subgrupos do grupo | transição texto → FK, sem apagar texto inicialmente | nova FK e backfill controlado | API e validação no servidor | alto | 1 | grupos oficiais | não permite vínculo a grupo inexistente | H2C.2C |
| Produtos | têm texto de grupo/subgrupo e FK apenas de subgrupo | duplicidade entre texto e ID | seleção estruturada, evitando digitação livre | escolhe produto filtrado pelo subgrupo | definir fonte oficial e compatibilidade | possível vínculo direto ao grupo; preservar campos antigos | formulário e consultas encadeadas | alto | 1 | grupos e subgrupos | produto sempre pertence a caminho válido ou “Não classificado” | H2C.2C |
| Importador antigo | exige `id_grupo` inexistente | é incompatível com schema atual | não executar nem adaptar silenciosamente | usuário não vê nem aciona o importador | reescrever como ferramenta separada, com pré-validação | somente após desenho do catálogo | comando administrativo isolado | crítico | 1 | H2C.2C e arquivo de entrada aprovado | falha fechada e relatório antes de gravar | etapa futura própria |
| Solicitações versão A | ativa no banco e no `app.py` | fluxo concentrado na tela principal | versão A é oficial | usuário solicita; admin analisa com mensagem clara | consolidar estados, histórico e apresentação | manter JSONB, usuário NN e `observacoes_admin` | serviço separado e telas dedicadas no futuro | médio | 1 | permissões e patrimônio | criação, aprovação e rejeição preservam autoria | H2C.2F |
| Script versão B | DDL incompatível com banco e código | pode criar estrutura errada em banco vazio | marcar como legado incompatível e não executar | nenhum usuário tem acesso | isolar/depreciar documentalmente | nenhum nesta fase | impedir uso em procedimentos oficiais | alto | 1 | aprovação formal da versão A | documentação e automação não o referenciam | H2C.2F |
| UVRs | texto em `usuarios.uvr_acesso` e tabelas legadas | grafias livres e ausência de integridade | criar cadastro próprio e vínculo futuro por ID | seleciona UVR em lista | especificar cadastro, alias, estado e migração textual | novas tabelas/colunas/FKs; texto preservado na transição | camada de resolução texto/ID e escopo | crítico | 1 | inventário dos valores e permissões | nenhuma perda de escopo; valores não mapeados tratados | H2C.2D |
| Usuários | login e troca de senha; sem tela de gestão | perfis e escopos são pouco administráveis | preservar e-mail/tokens; planejar recuperação | vê apenas tarefas e dados permitidos | gestão de usuário, perfis, módulos e UVRs | estruturas aditivas de permissão, se necessárias | decorators, menus e serviços de autorização | crítico | 1 | cadastro de UVR | matriz de acesso aprovada e testada | H2C.2E |
| Recuperação de senha | colunas existem, fluxo não existe | tokens sem funcionalidade completa | manter oculto até implementação segura | usuário recebe fluxo simples e temporário futuramente | especificar expiração, envio, uso único e auditoria | campos atuais preservados; possível evento | novas rotas e serviço de e-mail | alto | 2 | e-mail confirmado e segurança | token não aparece em tela/log e expira | etapa posterior à H2C.2E |
| Colunas históricas | campos extras sem uso atual | finalidade nem sempre está no Git | classificar; não remover | usuário não vê campo sem finalidade aprovada | decisão por conjunto e documentação | nenhuma remoção agora | evitar dependência nova sem regra | médio | 1 | responsáveis funcionais | 100% das colunas extras classificadas | H2C.2B–H2C.2G.1 |
| 27 tabelas adicionais | existem só no banco atual | módulos e responsáveis desconhecidos | analisar individualmente e manter ocultas | nenhum menu incompleto aparece | decisão humana por conjunto | nenhuma remoção; baseline aguarda escopo | nenhum código novo nesta fase | alto | 3 | responsáveis de auditoria, EPI, ouvidoria e documentos | cada tabela recebe decisão formal | H2C.2G.1 |
| Menus e navegação | tela principal grande, organizada por blocos técnicos | muitas tarefas competem na mesma página | organizar por tarefa em sete áreas | encontra a ação principal rapidamente | desenho de informação e visibilidade por permissão | nenhum, salvo preferências futuras | templates, navegação e autorização | médio | 2 | H2C.2D/E/F | teste com teclado, celular e perfis | H2C.2G.2 |
| Permissões | escopo UVR e admin existem; Fiscalização é admin global | faltam perfis por módulo e gestão visual | definir perfis, módulos e UVRs permitidos | não vê botão que não pode usar; URL também bloqueia | matriz de autorização interna | possível relação usuário-perfil-módulo-UVR | decorators e consultas por objeto | crítico | 1 | H2C.2D e H2C.2E | menu e acesso direto produzem a mesma decisão | H2C.2E |
| Relatórios essenciais | financeiro/extrato ficam no bloco administrativo | rotas já suportam parte do escopo por UVR, mas menu não | exibir somente relatórios úteis ao perfil | usuário acessa relatório do próprio escopo | definir catálogo mínimo e donos | nenhum inicialmente | navegação, filtros e autorização | médio | 2 | permissões e UVR | dados globais só para perfil autorizado | H2C.2G.2 |
| Experiência do usuário | formulários e ações existem, mas padrões variam | textos técnicos e ações destrutivas causam dúvida | padronizar nomes, estados, botões, filtros e erros | uma ação principal e orientação para corrigir erros | guia de interface e componentes reutilizáveis | nenhum | templates, JS e respostas | médio | 2 | mapa de menus | testes de uso, foco, celular e mensagens | H2C.2G.2 |
| Fiscalização de Contratos | 11 módulos completos no painel, todos admin | usuário comum não possui perfil específico | manter administrativo até nova matriz de perfis | admin continua usando; comum não vê | futura permissão granular, se aprovada | possível relação de permissão | substituir regra global somente depois de especificação | médio | 2 | H2C.2E | nenhuma regressão nas 11 áreas | posterior à H2C.2E |
| Funcionalidades incompletas | EPI, ouvidoria, documentos legados e recuperação não têm fluxo atual | estrutura física poderia ser confundida com produto pronto | manter ocultas | usuário não vê atalhos vazios | implementar somente após especificação e testes | depende de cada módulo | novos Blueprints/serviços, nunca dentro do monólito sem plano | alto | 3 | H2C.2G.1 | sem rota/menu antes do aceite | etapas próprias |

## 4. Patrimônio: decisões detalhadas

### Comportamento futuro

- “Excluir” será substituído por “Inativar” no fluxo normal.
- A inativação exigirá confirmação e explicará o efeito.
- Patrimônio inativo continuará aparecendo quando o filtro correspondente for
  selecionado.
- Novas transações, vínculos ou movimentações não aceitarão patrimônio inativo.
- Usuário autorizado poderá reativar.
- A tela alertará quando houver referência em transação ou outro vínculo.
- Exclusão física não fará parte da operação cotidiana.

### Decisões ainda necessárias

- nome e valores exatos do campo de situação;
- quem pode inativar e reativar;
- se a solicitação de usuário comum continuará passando pela versão A;
- quais referências bloqueiam a inativação e quais apenas geram alerta;
- se será necessária tabela de eventos de patrimônio.

### Reversão segura

Uma implantação futura deverá manter a rota anterior indisponível na interface,
mas não remover coluna ou registro. A reversão de código reexibirá o fluxo
anterior sem precisar desfazer dados; qualquer migration deverá ser aditiva e
compatível.

## 5. Catálogo: grupo → subgrupo → produto

### Modelo desejado

1. grupo ativo;
2. subgrupo ativo pertencente ao grupo;
3. produto ativo pertencente ao subgrupo;
4. opção técnica “Não classificado” somente para migração de registros antigos;
5. seleção encadeada com validação também no servidor.

Os textos atuais serão preservados durante a transição. Um processo de
conciliação deverá produzir três listas antes de qualquer atualização:

- correspondências seguras;
- valores ambíguos;
- valores sem correspondência.

Nenhum valor ambíguo será vinculado automaticamente.

## 6. UVR e permissões

O cadastro futuro de UVR precisa, no mínimo, de identificador, nome oficial,
código estável, estado ativo/inativo e aliases textuais históricos. A migração
de vínculo deverá:

1. levantar grafias atuais sem alterar registros;
2. normalizar somente para comparação;
3. pedir decisão humana para ambiguidades;
4. preencher o novo ID gradualmente;
5. manter o texto antigo durante a homologação;
6. comparar autorização antiga e nova;
7. só tornar o ID obrigatório quando não houver divergência.

Permissões deverão responder separadamente:

- quais módulos o usuário vê;
- quais ações pode executar;
- quais UVRs pode consultar;
- se pode operar globalmente;
- se pode aprovar solicitações;
- se pode administrar usuários e configurações.

## 7. Colunas históricas

| Tabela/conjunto | Classificação atual | Justificativa | Decisão futura |
|---|---|---|---|
| `patrimonio.data_cadastro` | vigente | auditoria temporal automática | manter e exibir apenas quando útil |
| afastamento/suspensão/exclusão/readmissão de `associados` | histórica/reservada | estrutura existe, sem fluxo versionado atual | validar com responsável por associados |
| `associados.funcao` | reservada para evolução | pode apoiar EPI e atividade, mas não há uso atual | definir fonte oficial da função |
| patrimônio/manutenção/combustível em `transacoes_financeiras` | reservada para evolução | sugere integração financeira com frota | decidir junto à H2C.2B |
| `usuarios.email` | reservada para evolução | decisão aprovada para novos usuários/recuperação | preservar; definir validação |
| `usuarios.reset_token` | reservada para evolução e sensível | suporte futuro a recuperação | nunca exibir; definir ciclo de vida |
| `usuarios.reset_token_expira` | reservada para evolução e sensível | expiração futura | nunca exibir; definir timezone e invalidação |
| defaults `now()` legados | vigente no banco, decisão técnica pendente | tornam datas automáticas | validar na baseline por tabela |

Nenhuma coluna é classificada como removível nesta etapa.

## 8. Análise individual das 27 tabelas adicionais

“Sem referência” significa que não foi localizado SQL dirigido à tabela no
código versionado atual. Não significa que a tabela esteja vazia ou obsoleta.

| Tabela | Finalidade aparente | Referência no código | Relacionamentos | Risco de manter/remover | Classificação funcional | Recomendação | Prioridade | Decisão humana adicional |
|---|---|---|---|---|---|---|---|---|
| `auditoria_associados` | auditoria de associado por período | sem referência | associado e documento, com cascata | manutenção baixa; remoção pode perder histórico | histórica | preservar oculta | 3 | confirmar processo de auditoria |
| `auditoria_passo1_observacoes` | observações da primeira etapa | sem referência | nenhuma FK | tabela isolada; remoção pode perder justificativas | histórica | preservar oculta | 3 | identificar dono funcional |
| `auditoria_passo2_observacoes` | observações gerais por período | sem referência | nenhuma FK | idem | histórica | preservar oculta | 3 | identificar dono funcional |
| `auditoria_rateios` | auditoria de rateios por associado | sem referência | associado, com cascata | remoção pode apagar trilha financeira | histórica | preservar oculta | 3 | confirmar retenção |
| `auditoria_rateios_transacoes` | auditoria de rateios e transações | sem referência | associado e transação, com cascata | alto impacto histórico | histórica | preservar oculta | 3 | confirmar retenção legal |
| `auditoria_relatorios` | metadados de relatórios gerados | sem referência | nenhuma FK | pode conter referência a arquivo externo | histórica | preservar; não expor caminho | 3 | confirmar armazenamento e retenção |
| `cadastro_pessoa_fisica` | provável importação simples de pessoa/CPF | sem referência | sem PK/FK | CPF exige privacidade; estrutura frágil | situação ainda não conclusiva | manter isolada e oculta | 3 | identificar origem, base legal e duplicidade |
| `documentos` | documentos legados vinculáveis a transação | sem referência SQL | tipo de documento e transação | caminho de arquivo e histórico sensíveis | reservada para módulo futuro | preservar; não confundir com `fc_documentos` | 3 | decidir migração ou coexistência |
| `entrega_documentos_itens` | itens de lote/pacote documental | sem referência | lote, pacote, tipo e documento | remoção quebra cadeia documental | auxiliar | preservar oculta | 3 | confirmar se houve operação externa |
| `entrega_documentos_lotes` | controle de lotes por UVR/período | sem referência | pai de itens | remoção quebra itens | auxiliar | preservar oculta | 3 | confirmar ciclo e estados |
| `entrega_documentos_pacotes` | pacotes e etapas de entrega | sem referência | pai de itens | remoção quebra itens | auxiliar | preservar oculta | 3 | confirmar finalidade do “passo” |
| `epi_entrega_itens` | itens de uma entrega de EPI | sem referência | entrega e item | histórico de segurança do trabalho | histórica/auxiliar | preservar oculta | 3 | confirmar retenção obrigatória |
| `epi_entregas` | entrega de EPI a associado | sem referência | associado e responsável | dados trabalhistas e históricos | reservada para módulo futuro | preservar oculta | 3 | confirmar responsáveis e regras |
| `epi_estoque` | saldo por item e UVR | sem referência | item de EPI | saldos podem divergir sem movimentos | auxiliar | preservar sem expor | 3 | decidir fonte oficial do estoque |
| `epi_itens` | catálogo operacional de EPI | sem referência | pai de estoque/entregas/movimentos | há catálogos EPI concorrentes | reservada para módulo futuro | tratar como candidata a catálogo oficial, sem decidir agora | 3 | escolher entre três catálogos |
| `epi_movimentos` | histórico de entradas e saídas | sem referência | item de EPI | remoção perde rastreabilidade | histórica | preservar oculta | 3 | confirmar regra de saldo |
| `epi_solicitacoes` | solicitações de mudança em EPI | sem referência | item de EPI | pode duplicar fluxo da versão A | possível candidata à consolidação futura | preservar e comparar fluxos | 3 | decidir se migra para solicitações oficiais |
| `epis` | catálogo simples de EPI | sem referência | nenhuma FK | possível duplicidade com `epi_itens` | possível candidata à descontinuação futura | manter oculta; não apagar | 3 | decidir catálogo oficial |
| `epis_catalogo` | catálogo detalhado por grupo/função | sem referência | nenhuma FK | possível fonte de referência sem vínculo | reservada para módulo futuro | preservar e avaliar importação controlada | 3 | decidir catálogo oficial |
| `ouvidoria_grupos` | grupos de classificação | sem referência | pai de subgrupos | hierarquia pode ser necessária | reservada para módulo futuro | preservar oculta | 3 | confirmar projeto de ouvidoria |
| `ouvidoria_manifestacao_fotos` | fotos/anexos de manifestação | sem referência | manifestação, com cascata | arquivo e privacidade; exclusão em cascata | histórica/auxiliar | preservar e auditar armazenamento antes de uso | 3 | definir retenção e acesso |
| `ouvidoria_manifestacoes` | manifestações, contato e localização | sem referência | sem FKs para a classificação | muitos dados pessoais e potencial inconsistência | reservada para módulo futuro | manter oculta e fazer avaliação de privacidade | 3 | confirmar base legal, estados e responsáveis |
| `ouvidoria_subgrupos` | segundo nível da classificação | sem referência | grupo com `RESTRICT` | necessário à hierarquia | auxiliar | preservar oculta | 3 | confirmar nomenclatura |
| `ouvidoria_subtipos` | quarto nível da classificação | sem referência | tipo com `RESTRICT` | necessário à hierarquia | auxiliar | preservar oculta | 3 | confirmar necessidade de quatro níveis |
| `ouvidoria_tipos` | terceiro nível da classificação | sem referência | subgrupo com `RESTRICT` | necessário à hierarquia | auxiliar | preservar oculta | 3 | confirmar necessidade de quatro níveis |
| `produtos` | catálogo alternativo simples | sem referência SQL | nenhuma FK | duplicidade com `produtos_servicos` | possível candidata à descontinuação futura | manter oculta e comparar conceitos | 3 | escolher catálogo oficial |
| `tipos_documentos` | catálogo de tipos e exigências | sem referência | pai de documentos/itens | necessário se módulo documental voltar | auxiliar | preservar oculta | 3 | decidir integração com documentos `fc_` |

Resultado: **27 de 27 tabelas analisadas individualmente**. Todas permanecem
preservadas e sem novo menu.

## 9. Mapa de funcionalidades visíveis

| Área/tarefa | Situação atual | Visibilidade recomendada agora | Evolução necessária |
|---|---|---|---|
| Login, logout e alterar senha | funcionando | manter para usuários autorizados | recuperação de senha fica oculta |
| Início | três grandes blocos no mesmo painel | manter até substituição testada | reorganizar nas sete áreas aprovadas |
| Clientes/fornecedores | criar, pesquisar, editar e solicitar/excluir | manter conforme escopo UVR | trocar linguagem destrutiva por estados onde aplicável |
| Associados | criar, pesquisar, editar e solicitar/excluir | manter conforme escopo UVR | decidir campos históricos e inativação |
| Patrimônio/frota | cadastro, pesquisa, edição e exclusão/solicitação | manter consulta/cadastro; ação destrutiva precisa melhoria prioritária | inativação, reativação e alertas |
| Financeiro | transações e contas | manter conforme permissões atuais | revisar ações destrutivas separadamente |
| Fluxo de caixa | administração | manter restrito à administração | separar tarefa e melhorar navegação |
| Catálogo de produtos | administração; grupo ainda textual | manter restrito à administração | reconstruir seleção encadeada |
| Solicitações/aprovações | administração no painel principal | manter restrito; versão A oficial | criar tela dedicada e histórico claro |
| Relatórios e extratos | menu administrativo, embora haja escopo UVR no servidor | manter conservadoramente restrito | decidir relatórios por perfil |
| Fiscalização de Contratos | 11 cards; `admin_required` | manter administrativo | permissão granular somente após H2C.2E |
| Denúncias legadas | rotas desativadas online | manter ocultas | decidir aposentadoria ou novo módulo |
| Gestão de usuários | não há tela | manter oculta | implementar após especificação |
| Cadastro de UVRs | não existe | manter oculto | implementar após especificação |
| Recuperação de senha | não existe | manter oculta | implementar com segurança |
| Auditoria adicional | apenas tabelas | manter oculta | decisão funcional |
| Documentos/entregas legados | apenas tabelas | manter ocultos | decisão funcional |
| EPI | apenas tabelas | manter oculto | módulo futuro completo |
| Ouvidoria | apenas tabelas | manter oculta | módulo futuro e avaliação de privacidade |

### Estrutura preliminar futura do menu

- **Início:** atalhos e pendências compatíveis com o perfil;
- **Fiscalização:** contratos, execução, documentos, medições e atestes;
- **Patrimônio:** bens, frota, vínculos e histórico;
- **Solicitações:** minhas solicitações; aprovações apenas para autorizados;
- **Cadastros:** pessoas, entidades, catálogo e UVRs conforme permissão;
- **Relatórios:** somente relatórios liberados ao perfil e escopo;
- **Administração:** usuários, permissões e configurações.

O menu não substitui autorização no servidor. Acesso direto por URL deverá
produzir a mesma decisão de permissão.

## 10. Backlog funcional priorizado

### Prioridade 1 — funcionamento essencial

| Ordem | Item | Entrega mínima |
|---:|---|---|
| 1 | patrimônio | especificar inativação, reativação, vínculos e histórico |
| 2 | catálogo estruturado | definir grupo → subgrupo → produto e “Não classificado” |
| 3 | UVR | cadastro, aliases e plano seguro de migração textual |
| 4 | usuários e permissões | perfis, módulos, ações e escopos |
| 5 | solicitações versão A | fluxo oficial, estados, autoria e mensagens |
| 6 | triagem das 27 tabelas | decisão humana por módulo antes da baseline |

### Prioridade 2 — usabilidade

| Ordem | Item | Entrega mínima |
|---:|---|---|
| 1 | arquitetura do menu | sete áreas orientadas por tarefa |
| 2 | pesquisas e filtros | nome, código, CPF/CNPJ e estado |
| 3 | mensagens | erro acionável sem termos técnicos |
| 4 | botões e estados | posição e vocabulário consistentes |
| 5 | relatórios essenciais | catálogo por perfil e escopo |
| 6 | acessibilidade | teclado, foco, contraste e celular |

### Prioridade 3 — módulos adicionais

1. EPI;
2. documentos e entregas legados;
3. ouvidoria;
4. auditoria operacional;
5. catálogos alternativos;
6. recuperação de senha;
7. demais estruturas históricas aprovadas.

## 11. Incrementos pequenos e reversíveis

Cada incremento futuro deverá seguir este padrão:

1. especificação funcional aprovada;
2. inventário dos dados afetados sem alterá-los;
3. migration exclusivamente aditiva;
4. código compatível com o modelo antigo e o novo durante a transição;
5. testes unitários, integração e autorização;
6. homologação com dados fictícios;
7. validação manual pelo responsável;
8. ativação da nova interface por escopo controlado;
9. reversão por desligamento da funcionalidade, sem apagar estruturas;
10. encerramento documentado antes do incremento seguinte.

Não misturar patrimônio, catálogo, UVR e usuários na mesma migration ou
implantação.

## 12. Sequência recomendada

1. **H2C.2B — especificação funcional detalhada do patrimônio.**
2. **H2C.2C — especificação do catálogo grupo/subgrupo/produto.**
3. **H2C.2D — especificação do cadastro e vínculo de UVRs.**
4. **H2C.2E — especificação de usuários e permissões.**
5. **H2C.2F — consolidação do fluxo de solicitações.**
6. **H2C.2G.1 — decisão de escopo dos módulos adicionais.**
7. **H2C.2G.2 — planejamento da interface, menus e relatórios.**
8. **H2C.2H — plano técnico de migrations por incremento.**
9. **H2C.2I — plano de homologação e reversão por incremento.**

A subdivisão de H2C.2G evita misturar a decisão sobre módulos desconhecidos
com a reforma visual do sistema. H2C.2H só deverá consolidar a baseline depois
das decisões funcionais anteriores.

## 13. Critério de encerramento da H2C.2A

- decisões do gestor registradas;
- 27 tabelas analisadas;
- mapa de visibilidade concluído;
- backlog priorizado;
- critérios de aceite definidos;
- sequência futura definida;
- nenhum código, banco, migration ou dado alterado.

## 14. Resultado da H2C.2B — patrimônio

A especificação detalhada confirmou cinco rotas e uma área única de patrimônio
no painel principal. As 38 colunas foram classificadas e os fluxos atuais de
cadastro, consulta, edição e exclusão foram documentados.

Decisões detalhadas:

- reutilizar `status_bem` como fonte futura da situação, após caracterização dos
  valores existentes;
- preservar os valores Ativo, Em manutenção, Inativo e Baixado;
- separar mudança de situação da edição comum;
- substituir exclusão física cotidiana por inativação e reativação;
- manter movimentações antigas e impedir uso novo de patrimônio indisponível;
- criar histórico aditivo em etapa futura;
- corrigir primeiro a autorização de UVR no cadastro;
- não vincular automaticamente patrimônio legado a ativo contratual;
- preservar todas as 38 colunas.

Os detalhes, critérios de aceite e incrementos H2C.3B.1–H2C.3B.9 estão em
`ESPECIFICACAO_FUNCIONAL_PATRIMONIO_H2C2B.md`.

## 15. Resultado da H2C.2C — catálogo

A especificação confirmou que o catálogo atual ainda não possui o vínculo
físico completo Grupo → Subgrupo → Produto. `grupos_atividade` não alimenta as
listas atuais, `subgrupos` identifica o grupo por texto e
`produtos_servicos` combina textos legados com `id_subgrupo` opcional.

Decisões preliminares consolidadas:

- preservar `produtos_servicos` como catálogo operacional durante a transição;
- usar futuramente `grupos_atividade` como entidade oficial de grupo;
- criar vínculos de forma aditiva, inicialmente opcionais;
- representar registros antigos pendentes como “Não classificado”, sem criar
  grupo técnico selecionável;
- impedir novas classificações inválidas e não fundir nomes automaticamente;
- substituir exclusão cotidiana por inativação e reativação;
- tratar reclassificação como operação auditada, separada da edição comum;
- não integrar automaticamente patrimônio, EPI, Fiscalização ou a tabela
  adicional `produtos`;
- classificar o importador antigo como incompatível e não autorizado.

As decisões sobre Receita/Despesa, nomes repetidos, unidade, código, perfil
administrador e destino da tabela `produtos` ainda dependem de validação humana.
O detalhamento está em `ESPECIFICACAO_FUNCIONAL_CATALOGO_H2C2C.md`.

## 16. Resultado aprovado da H2C.2D — UVRs e associações

A especificação confirmou que UVR é hoje um texto usado como escopo do usuário,
propriedade de registros, filtro e informação de relatório. Associação é outro
texto, armazenado separadamente, mas sem cadastro central e sem participação
direta na autorização.

Foram comparados três modelos e foi aprovado o modelo com associação e UVR como
entidades distintas, identificadores próprios, aliases, vínculos auditáveis e
migração gradual. Uma associação poderá possuir várias UVRs; usuário comum
poderá possuir várias UVRs com uma principal; e poderão existir administrador
global e administrador limitado a associação.

Também foram aprovados: falha fechada para texto desconhecido, preservação
histórica, transferência auditada, conta corrente pertencente à associação,
separação entre responsabilidade institucional e unidade de uso do patrimônio,
Fiscalização de Contratos global, baseline sem dados reais e bloqueio da
inativação enquanto houver usuários ou operações ativas.

Permanecem pendentes somente desenho SQL, nomes e tipos finais, constraints,
migration dos textos, implementação de permissões, auditoria e interfaces.

A especificação e as perguntas de decisão estão em
`ESPECIFICACAO_FUNCIONAL_UVR_H2C2D.md`.
