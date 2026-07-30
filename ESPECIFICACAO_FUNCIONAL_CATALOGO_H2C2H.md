# Etapa H2C.2H — Consolidação funcional do catálogo

**Situação: APROVADA em 30/07/2026.**

As 35 decisões funcionais foram aprovadas integralmente pelo usuário. As
alternativas registradas na seção 17 permanecem apenas como memória da análise;
as respostas oficiais são as da seção 1.1. Essa aprovação não representa
implementação: tabelas, colunas, constraints, códigos reais, aliases,
permissões, interfaces, relatórios e migrations continuam tecnicamente
pendentes.

## 1. Finalidade, limites e grau de certeza

Este documento consolida as decisões finais sobre grupos, subgrupos e
produtos/serviços. Ele complementa, sem substituir, a especificação histórica
`ESPECIFICACAO_FUNCIONAL_CATALOGO_H2C2C.md`.

Esta etapa é exclusivamente documental. Foram consultados documentos e código
versionado. Não houve acesso a PostgreSQL, dump externo, `.env`, API, serviço
real ou conteúdo dos CSVs; nenhum importador, SQL ou migration foi executado.

As expressões usadas neste documento significam:

- **confirmado:** comprovado no schema documentado ou no código versionado;
- **aprovado anteriormente:** decisão funcional já validada na H2C.2C–H2C.2G;
- **decisão aprovada:** regra funcional validada em 30/07/2026;
- **pendência técnica:** detalhe de implementação que esta etapa não resolveu.

### 1.1 Decisões funcionais aprovadas

| Nº | Decisão aprovada |
|---:|---|
| 1 | Receita e Despesa são naturezas financeiras separadas da árvore do catálogo. |
| 2 | Todo Produto/Serviço precisa de Grupo e Subgrupo antes de ativação e uso novo. |
| 3 | A interface usará “Produto/Serviço”. |
| 4 | `PRODUTO` e `SERVICO` são tipos estruturais distintos e obrigatórios. |
| 5 | Grupo terá código obrigatório antes da ativação. |
| 6 | Subgrupo terá código obrigatório antes da ativação. |
| 7 | Produto/Serviço terá código obrigatório antes da ativação e uso. |
| 8 | Códigos serão globais dentro do tipo, estáveis e nunca reutilizados. |
| 9 | Nome normalizado de Grupo será globalmente único. |
| 10 | Nome normalizado de Subgrupo será único dentro do Grupo. |
| 11 | Nome normalizado de Produto/Serviço será único dentro do Subgrupo. |
| 12 | Nomes iguais serão permitidos em Subgrupos distintos quando houver diferença funcional legítima. |
| 13 | Comparação ignorará caixa, acentos e espaços excedentes, preservando a grafia oficial. |
| 14 | Aliases e sinônimos controlados serão permitidos com vínculo inequívoco. |
| 15 | Descrição será obrigatória antes da ativação. |
| 16 | Todo Produto/Serviço ativo terá unidade de medida. |
| 17 | Existirá catálogo central de unidades. |
| 18 | O item terá unidade padrão e a transação preservará a unidade efetiva. |
| 19 | Estados: `RASCUNHO`, `ATIVO`, `INATIVO` e `SUBSTITUIDO`; pendência de classificação é condição transitória. |
| 20 | Grupo com filhos ativos não poderá ser inativado. |
| 21 | Inativação em cascata será proibida. |
| 22 | Reclassificação estrutural exigirá justificativa, aprovação e histórico antes/depois. |
| 23 | Transação concluída preservará a classificação original. |
| 24 | Item não classificado não poderá ser usado em nova transação. |
| 25 | Gestor do Catálogo ou equivalente aprovará Grupos e Subgrupos oficiais. |
| 26 | Gestor do Catálogo ou equivalente aprovará Produtos e Serviços. |
| 27 | Associações e UVRs poderão sugerir, mas não ativar registros oficiais. |
| 28 | A tabela `produtos` será preservada como legado e ficará fora da baseline inicial. |
| 29 | CSVs antigos serão apenas apoio documental, nunca fonte oficial ou carga automática. |
| 30 | Importação futura exigirá prévia, validação, conflitos, confirmação, transação, idempotência e auditoria. |
| 31 | Exclusão física será limitada a rascunho nunca ativado, aprovado, usado ou vinculado. |
| 32 | Os relatórios mínimos cobrirão qualidade, hierarquia, utilização, histórico, solicitações e impacto. |
| 33 | A baseline conterá estruturas vazias e códigos técnicos estáveis, sem catálogo real. |
| 34 | `RECEITA` e `DESPESA` poderão integrar a baseline como códigos estruturais da natureza financeira. |
| 35 | Reclassificações de alto impacto exigirão duas aprovações distintas. |

### 1.2 Implementação técnica pendente

Continuam pendentes nomes definitivos de tabelas e colunas, formatos de códigos,
tipos, constraints, índices, catálogo real de unidades e itens, migrations,
mapeamento e saneamento dos dados atuais, aliases reais, integração com
transações, permissões, interfaces, relatórios, importadores e testes.

## 2. Comportamento atual reconstruído

O catálogo atual é híbrido e ainda não forma uma hierarquia relacional completa:

1. a interface separa Receita e Despesa e usa listas fixas de grupos no
   `app.py` e em `templates/cadastro.html`;
2. `grupos_atividade` não alimenta essas listas e não possui relação física com
   `subgrupos`;
3. `subgrupos` liga-se ao grupo pelo texto `atividade_pai`;
4. `produtos_servicos` mantém grupo e subgrupo em texto e pode, opcionalmente,
   guardar `id_subgrupo`;
5. o cadastro administrativo permite criar, editar e excluir fisicamente
   subgrupos e produtos; não há inativação ou histórico;
6. a validação atual não comprova que o subgrupo escolhido pertence ao grupo
   enviado;
7. Receita/Despesa é inferida pelo nome do grupo em uma das rotas, o que não é
   uma regra financeira segura;
8. `itens_transacao` guarda descrição, unidade, quantidade e valores, mas não o
   identificador do produto;
9. relatórios relacionam item e catálogo por igualdade textual entre descrição
   e nome, portanto renomear ou excluir pode romper a classificação histórica;
10. as consultas de catálogo exigem login e as operações administrativas usam
    proteção administrativa; o modelo futuro de permissão ainda não foi
    implementado.

### 2.1 Rotas e finalidades relevantes

| Rota/função | Operação atual | Autorização atual | Risco principal |
|---|---|---|---|
| `cadastrar_produto_servico` | inclui produto por textos | administrador | não vincula o subgrupo por ID |
| `get_produtos_servicos` | lista o catálogo | usuário autenticado | não há estado ativo/inativo |
| `get_distinct_grupos` | devolve mapa fixo | usuário autenticado | não consulta o cadastro de grupos |
| `get_distinct_subgrupos` | filtra por `atividade_pai` | usuário autenticado | relação textual |
| `get_items_for_filters` | filtra itens por textos | usuário autenticado | depende de classificação textual |
| `api_subgrupos` | lista, inclui, edita e exclui | administrador | exclusão física e atualização parcial |
| `api_produtos_crud` | lista, inclui, edita e exclui | administrador | inferência de natureza e exclusão física |
| `migrar_dados_antigos_produtos` | liga textos a subgrupos | execução manual | saneamento automático insuficiente |

Os filtros e relatórios financeiros consultam grupo, subgrupo e item textuais.
As linhas financeiras não armazenam uma fotografia estruturada completa do
catálogo. Não foi localizado uso operacional da tabela adicional `produtos`.

## 3. Estruturas atuais comprovadas

### 3.1 `grupos_atividade`

| Coluna | Estrutura atual |
|---|---|
| `id` | `INTEGER`, obrigatório, chave primária, sequência |
| `nome` | `VARCHAR(100)`, obrigatório, único |

Não há chave estrangeira, estado, código funcional, descrição ou auditoria. O
uso localizado está em scripts legados, não no fluxo principal da interface.

### 3.2 `subgrupos`

| Coluna | Estrutura atual |
|---|---|
| `id` | `SERIAL`, chave primária |
| `nome` | `VARCHAR(255)`, obrigatório |
| `atividade_pai` | `VARCHAR(255)`, obrigatório e textual |

Há unicidade em `(nome, atividade_pai)`. Não existe `id_grupo`, estado,
auditoria ou histórico. A API atual admite exclusão física quando não encontra
produto ligado por `id_subgrupo`; vínculos apenas textuais podem escapar dessa
verificação.

### 3.3 `produtos_servicos`

| Coluna | Estrutura atual |
|---|---|
| `id` | `INTEGER`, chave primária, sequência |
| `tipo` | `VARCHAR(20)`, obrigatório |
| `tipo_atividade` | `VARCHAR(255)`, obrigatório |
| `grupo` | `VARCHAR(255)`, opcional |
| `subgrupo` | `VARCHAR(255)`, opcional |
| `item` | `VARCHAR(255)`, obrigatório e único globalmente |
| `data_hora_cadastro` | `TIMESTAMP`, obrigatório, padrão instalado `now()` |
| `id_subgrupo` | `INTEGER`, opcional, FK para `subgrupos(id)` sem cascata |

Não há `id_grupo`, unidade padrão, código estável, descrição, estado, autoria ou
histórico. A tabela participa de formulários, filtros e relatórios; transações a
referenciam apenas indiretamente pelo texto da descrição. A API permite exclusão
física se a busca textual não encontrar uso.

### 3.4 `produtos`

| Coluna | Estrutura atual |
|---|---|
| `id` | `INTEGER`, chave primária, sequência |
| `nome` | `VARCHAR(255)`, obrigatório |
| `grupo` | `VARCHAR(100)`, opcional |
| `subgrupo` | `VARCHAR(100)`, opcional |
| `unidade` | `VARCHAR(20)`, opcional |
| `valor_padrao` | `NUMERIC(10,2)`, opcional |
| `tipo` | `VARCHAR(50)`, opcional |
| `uvr` | `VARCHAR(20)`, opcional |

Não há FKs nem uso SQL localizado no código e testes versionados. Isso comprova
apenas ausência de uso identificado, não que a tabela possa ser removida.
Conforme decisão anterior, não haverá integração automática.

## 4. Conceitos que não devem ser misturados

- **Natureza financeira:** efeito da movimentação; Receita aumenta recursos e
  Despesa representa aplicação/saída. Não é localização na árvore do catálogo.
- **Grupo e subgrupo:** classificação administrativa do item.
- **Produto/serviço:** objeto selecionável em uma operação.
- **Unidade:** forma de quantificar o item; pode ter padrão cadastral e valor
  efetivamente usado na transação.
- **Não classificado:** condição transitória de saneamento, não um grupo.
- **Inativo:** registro preservado e indisponível para novos usos.
- **Substituído:** registro histórico sucedido por outro.
- **Reclassificado:** item cuja posição mudou por processo formal.
- **Texto histórico:** fotografia legível que permanece na transação mesmo que
  o cadastro seja alterado.

## 5. Receita e Despesa: alternativas

| Alternativa | Vantagem | Risco e impacto histórico |
|---|---|---|
| A — grupos principais | interface simples | mistura efeito financeiro com classificação e perpetua listas fixas |
| B — natureza separada | separa conceitos corretamente | exige adaptar filtros e preservar textos antigos |
| C — natureza fixa em cada item | automatiza lançamentos | impede uso legítimo em outra natureza e transfere regra financeira ao cadastro |
| D — natureza da operação + regras permitidas no item | flexível e controlável | exige validação adicional e fotografia histórica |

**Decisão aprovada:** natureza financeira separada, com validações explícitas. A
natureza pertence à operação; o item pode declarar naturezas permitidas, mas
seu nome ou grupo nunca decide sozinho Receita/Despesa.

## 6. Hierarquia funcional desejada

A hierarquia funcional aprovada é:

```text
Natureza da operação (fora da árvore)
Grupo → Subgrupo → Produto/Serviço
```

- todo subgrupo ativo pertence a exatamente um grupo;
- todo item ativo pertence a exatamente um subgrupo;
- grupos e subgrupos podem existir sem filhos para preparação administrativa;
- item sem classificação só existe em saneamento/importação controlada e não
  participa de novo lançamento;
- produto e serviço usam a mesma árvore e são diferenciados pelo tipo;
- não se recomenda relação muitos-para-muitos sem caso funcional comprovado;
- a mesma designação em contextos diferentes deve ser diferenciada por código,
  grupo, subgrupo, tipo e unidade.

## 7. Produto, serviço, nomes e códigos

**Decisão aprovada de interface:** “Produto/Serviço”, com os tipos obrigatórios
`PRODUTO` e `SERVICO`.

Cada entidade deve ter:

- ID interno sem significado funcional;
- código funcional estável, obrigatório antes da ativação, único em seu escopo
  e nunca reutilizado;
- nome oficial obrigatório e alterável com histórico;
- nome curto opcional;
- descrição e observação opcionais;
- aliases separados, cada um apontando inequivocamente para uma entidade.

Não se definem códigos reais nem tamanhos SQL nesta etapa.

## 8. Unicidade e normalização

**Decisão aprovada:**

- grupo único pelo nome normalizado em todo o catálogo;
- subgrupo único pelo nome normalizado dentro do grupo;
- produto/serviço único pelo nome normalizado dentro do subgrupo;
- nomes iguais podem existir em subgrupos diferentes se o contexto for
  realmente distinto;
- produto e serviço com mesmo nome exigem códigos e contexto distintos;
- aliases não são itens independentes.

A chave de comparação deve ignorar maiúsculas/minúsculas, acentos, espaços
externos e espaços consecutivos. Deve preservar a grafia oficial, pontuação,
hífens, barras e palavras internas. Singular/plural e abreviações não devem ser
convertidos automaticamente; servem para alerta ou alias revisado. A
normalização detecta conflito, mas não reescreve silenciosamente dados antigos.

## 9. Unidades de medida

Hoje a unidade é texto da linha financeira e a tabela adicional `produtos`
possui outro texto de unidade, sem integração comprovada.

**Decisão aprovada:** catálogo central de unidades com ID, código, nome,
símbolo, precisão admitida e estado. O item guarda unidade padrão; a transação
guarda ID/código e fotografia da unidade efetiva. Serviços podem usar uma
unidade própria como “serviço” ou “hora”, mas não devem ficar semanticamente
ambíguos. Conversões automáticas só devem existir após regras específicas e
aprovadas.

## 10. Estados, inativação, reativação e substituição

Estados cadastrais recomendados: `RASCUNHO`, `ATIVO`, `INATIVO` e
`SUBSTITUIDO`. “Em análise” pertence à solicitação, e “pendente de
classificação” é uma condição de saneamento, evitando estados redundantes.

- item inativo permanece em transações e relatórios históricos;
- novo lançamento só usa item ativo e autorizado;
- não há inativação em cascata;
- grupo com filhos ativos tem inativação bloqueada até plano individual;
- inativação e reativação exigem justificativa, permissão e evento;
- reativação verifica conflito de código, nome, pai, unidade e substituto;
- código nunca é reutilizado;
- item substituído aponta para sucessor sem apagar seu histórico.

## 11. Reclassificação e fotografia histórica

Mover subgrupo, mover item, mudar tipo ou alterar naturezas permitidas é
reclassificação formal. Ela deve ter justificativa, aprovador, vigência,
fotografia anterior/posterior, impacto e controle de concorrência.

Uma transação futura deve preservar ao menos: ID, código, nome, tipo, grupo,
subgrupo, natureza da operação, unidade, descrição relevante e data/versão da
classificação. Alterar o catálogo só afeta novos lançamentos. Correção do passado
exige fluxo próprio e nunca ocorre silenciosamente.

## 12. Não classificados, aliases e duplicidades antigas

“Não classificado” permanece condição transitória, não cadastro artificial.
Recomenda-se permitir essa condição somente em importação/saneamento, exibi-la
em relatórios e bloquear novos lançamentos até revisão humana.

Aliases podem registrar nomes antigos, abreviações, grafias alternativas e
erros conhecidos. Correspondência automática só ocorre quando houver um único
destino; conflito exige decisão humana. Duplicidades antigas não serão fundidas
nem excluídas automaticamente.

## 13. Tabela `produtos`, CSVs e importadores

**Decisão aprovada para `produtos`:** preservá-la como legado no banco
atual e mantê-la fora da baseline nova até auditoria de dependências e dados.
Integração seletiva só poderá ocorrer depois de revisão humana.

Foram localizados `padrao_itens.csv`, `padrao_itens2.csv` e scripts legados que
criam/atualizam grupos, subgrupos ou produtos. O conteúdo dos CSVs não foi lido.
Há scripts incompatíveis com o schema instalado e scripts com configuração
histórica sensível. Nenhum deles é fonte oficial ou está autorizado.

Um importador futuro deverá ter pré-visualização, validação, relatório de
conflitos, confirmação humana, transação, idempotência, auditoria e rejeição de
ambiguidade, sem credenciais incorporadas.

## 14. Atores, permissões e segregação

| Ator | Responsabilidade recomendada |
|---|---|
| Gestor do Catálogo | qualidade, aprovação e administração central |
| Administrador Global | somente com permissão específica |
| Associação/UVR | consultar e sugerir dentro do escopo |
| Operador | consultar e usar itens ativos autorizados |
| Consulta | leitura; exportação exige permissão própria |

Criar, editar, inativar, reativar, reclassificar, aprovar, importar, exportar,
ver histórico e administrar aliases são permissões separadas. Mudanças críticas
usam solicitação e segregação entre solicitante e aprovador. Dupla aprovação é
candidata para fusão, importação em massa, alteração de código/natureza e
reclassificação de alto impacto, não para simples correção de grafia.

## 15. Exclusão, relatórios e retenção

Exclusão física é recomendada apenas para rascunho nunca ativado, usado,
aprovado ou vinculado. Registro ativado é inativado ou substituído. Códigos,
nomes antigos, aliases, eventos e fotografias históricas são preservados.

Relatórios mínimos recomendados:

- hierarquia completa e estados;
- não classificados, sem código e sem unidade;
- possíveis duplicidades e aliases conflitantes;
- itens utilizados/nunca utilizados;
- reclassificações e substituições;
- solicitações pendentes;
- impacto de inativação;
- legado ainda não mapeado.

Exportar exige permissão própria e escopo.

## 16. Baseline e migração futura

A baseline poderá conter estruturas vazias para naturezas/regras permitidas,
grupos, subgrupos, itens, unidades, aliases, estados, eventos,
reclassificações e substituições. Poderá conter somente códigos técnicos
estáveis aprovados, como tipos e estados. Não conterá grupos, subgrupos,
produtos, serviços, aliases, transações, associações, UVRs ou dados reais.

Receita/Despesa só integrarão a baseline como códigos estruturais se a decisão
funcional confirmar que não duplicam a natureza já existente na transação.

Migração futura, sem SQL nesta etapa:

1. criar estruturas aditivas;
2. preservar tabelas e textos;
3. cadastrar catálogo oficial revisado;
4. mapear grupos, subgrupos e itens com revisão humana;
5. registrar aliases, não classificados e conflitos;
6. adicionar IDs opcionais e fotografias às transações;
7. operar temporariamente com texto e ID;
8. comparar relatórios;
9. usar IDs em novos lançamentos;
10. retirar o fallback textual somente após equivalência comprovada.

Concorrência deve impedir duplicidade e sobrescrita silenciosa, usando versão,
restrições e bloqueio transacional na implementação futura.

## 17. Registro das alternativas consideradas

As alternativas abaixo preservam o raciocínio da análise. As respostas
oficialmente aprovadas são as 35 decisões da seção 1.1.

1. **Receita e Despesa ficarão fora da árvore?** Alternativas: (A) grupos; (B)
   natureza separada; (C) natureza fixa no item; (D) natureza da operação com
   regras no item. **Recomendação:** D. **Impacto:** filtros, validação e
   fotografia financeira.
2. **Todo item terá grupo e subgrupo?** Alternativas: (A) sempre; (B) opcional;
   (C) opcional só no saneamento. **Recomendação:** C. **Impacto:** qualidade e
   tratamento do legado.
3. **Nome da interface?** Alternativas: Produto; Produto/Serviço; Item; Item do
   Catálogo; Produto ou Serviço. **Recomendação:** Produto/Serviço. **Impacto:**
   clareza e textos da interface.
4. **Produto e serviço serão tipos distintos?** Alternativas: não; sim com dois
   tipos; sim com `OUTRO`. **Recomendação:** `PRODUTO`/`SERVICO`, adicionando
   `OUTRO` só se comprovado. **Impacto:** unidade, pesquisa e relatórios.
5. **Grupo terá código obrigatório?** Alternativas: não; opcional; obrigatório
   antes de ativar. **Recomendação:** última. **Impacto:** identificação estável.
6. **Subgrupo terá código obrigatório?** Alternativas: não; opcional;
   obrigatório antes de ativar. **Recomendação:** obrigatório antes de ativar.
   **Impacto:** integração e histórico.
7. **Item terá código obrigatório?** Alternativas: não; opcional; obrigatório
   antes de ativar. **Recomendação:** obrigatório antes de ativar. **Impacto:**
   evita identidade pelo nome.
8. **Códigos serão globais e nunca reutilizados?** Alternativas: globais;
   hierárquicos; reutilizáveis após inativação. **Recomendação:** únicos por tipo
   de entidade e nunca reutilizados. **Impacto:** auditoria e migração.
9. **Nome de grupo será globalmente único?** Alternativas: sim; por natureza;
   repetido. **Recomendação:** sim, normalizado. **Impacto:** árvore sem
   ambiguidade.
10. **Subgrupo será único dentro do grupo?** Alternativas: global; dentro do
    grupo; livre. **Recomendação:** dentro do grupo. **Impacto:** permite
    contextos legítimos.
11. **Item será único dentro do subgrupo?** Alternativas: global; dentro do
    subgrupo; livre. **Recomendação:** dentro do subgrupo, após IDs nas
    transações. **Impacto:** requer migração gradual da unicidade atual.
12. **Nomes iguais em subgrupos diferentes?** Alternativas: proibir; permitir;
    permitir com justificativa. **Recomendação:** permitir com contexto e código
    distintos. **Impacto:** pesquisa deve mostrar a hierarquia.
13. **Comparação ignorará acentos, caixa e espaços?** Alternativas: literal;
    normalizada; alerta sem bloqueio. **Recomendação:** normalizada para
    conflito, preservando exibição. **Impacto:** reduz duplicidades.
14. **Aliases e sinônimos serão permitidos?** Alternativas: não; aliases livres;
    aliases controlados. **Recomendação:** controlados e inequívocos. **Impacto:**
    melhora busca e saneamento.
15. **Descrição do item será obrigatória?** Alternativas: obrigatória; opcional;
    obrigatória por tipo. **Recomendação:** opcional inicialmente. **Impacto:**
    menor barreira, porém exige regra contra nomes genéricos.
16. **Unidade será obrigatória?** Alternativas: no item; só na transação; padrão
    + efetiva; dispensável para serviços. **Recomendação:** padrão + efetiva, com
    unidade adequada para serviço. **Impacto:** consistência quantitativa.
17. **Existirá catálogo central de unidades?** Alternativas: texto livre; lista
    fixa; cadastro central. **Recomendação:** cadastro central. **Impacto:**
    validação e relatórios.
18. **Item terá unidade padrão e transação guardará a efetiva?** Alternativas:
    só padrão; só efetiva; ambas. **Recomendação:** ambas. **Impacto:** preserva
    contexto histórico.
19. **Quais estados cadastrais?** Alternativas: ativo/inativo; seis estados
    mistos; rascunho/ativo/inativo/substituído. **Recomendação:** quatro estados,
    deixando análise no fluxo. **Impacto:** evita redundância.
20. **Grupo com filhos ativos poderá ser inativado?** Alternativas: sim; cascata;
    bloquear; plano formal. **Recomendação:** bloquear até plano individual.
    **Impacto:** impede interrupção acidental.
21. **Cascata automática será proibida?** Alternativas: permitir; proibir;
    permitir só em lote aprovado. **Recomendação:** proibir. **Impacto:** mais
    trabalho controlado, menor risco.
22. **Reclassificação exigirá justificativa e aprovação?** Alternativas: edição
    direta; justificativa; fluxo formal. **Recomendação:** fluxo formal.
    **Impacto:** auditoria e vigência.
23. **Transações antigas preservarão a classificação original?** Alternativas:
    sempre atual; fotografia; híbrido. **Recomendação:** ID + fotografia.
    **Impacto:** relatórios históricos estáveis.
24. **Não classificado pode ser usado em nova transação?** Alternativas: sempre;
    temporariamente; só saneamento/importação; nunca existir. **Recomendação:**
    só saneamento/importação. **Impacto:** bloqueia classificação incerta.
25. **Quem aprova grupos e subgrupos?** Alternativas: admin global; gestor do
    catálogo; dupla aprovação. **Recomendação:** gestor, com segregação nos
    casos críticos. **Impacto:** governança central.
26. **Quem aprova produtos e serviços?** Alternativas: admin global; gestor do
    catálogo; dupla aprovação. **Recomendação:** gestor do catálogo.
    **Impacto:** qualidade e prazo de atendimento.
27. **UVRs somente sugerem?** Alternativas: administram localmente; sugerem;
    criam rascunho oficial. **Recomendação:** sugerem pelo fluxo. **Impacto:**
    evita catálogos divergentes.
28. **`produtos` fica fora da baseline até auditoria?** Alternativas: integrar;
    preservar na baseline; manter legado fora; arquivar. **Recomendação:** manter
    legado fora até auditoria. **Impacto:** evita fusão incorreta.
29. **CSVs antigos serão só apoio documental?** Alternativas: fonte oficial;
    carga automática; apoio; descartar. **Recomendação:** apoio revisado.
    **Impacto:** nenhum dado entra sem validação.
30. **Importação futura terá prévia e confirmação?** Alternativas: direta;
    prévia opcional; prévia obrigatória. **Recomendação:** obrigatória, com
    transação e idempotência. **Impacto:** segurança operacional.
31. **Exclusão física só para rascunho sem uso?** Alternativas: nunca; rascunho
    sem vínculo; qualquer inativo. **Recomendação:** rascunho sem vínculo.
    **Impacto:** retenção e integridade.
32. **Quais relatórios serão obrigatórios?** Alternativas: só hierarquia;
    hierarquia + qualidade; conjunto completo da seção 15. **Recomendação:**
    conjunto completo por fases. **Impacto:** esforço e governança.
33. **Baseline terá apenas estruturas/códigos técnicos?** Alternativas: incluir
    catálogo real; somente estruturas; estruturas + códigos estáveis.
    **Recomendação:** última, sem dados reais. **Impacto:** ambiente reproduzível
    e neutro.
34. **Receita/Despesa serão códigos estruturais?** Alternativas: grupos; códigos
    do item; códigos de natureza; reutilizar natureza existente. **Recomendação:**
    reutilizar ou consolidar a natureza existente, sem duplicar. **Impacto:**
    desenho da baseline.
35. **Alto impacto terá duas aprovações?** Alternativas: nunca; sempre; por
    matriz de risco. **Recomendação:** por risco para fusão, importação, código,
    natureza e reclassificação ampla. **Impacto:** mais controle e prazo.

## 18. Modelo funcional aprovado

A regra aprovada separa a natureza financeira da árvore,
usar Grupo → Subgrupo → Produto/Serviço, códigos estáveis, nomes normalizados,
unidades centrais, estados simples, sem cascata, reclassificação formal e
fotografia histórica. Itens não classificados ficam bloqueados para novos
lançamentos; UVRs sugerem e a gestão oficial é central. `produtos` e CSVs ficam
como legado/apoio até auditoria. A baseline contém estruturas e códigos técnicos,
não catálogo real.

**Vantagens:** reduz ambiguidade, preserva o passado, melhora busca, permissão e
auditoria. **Riscos:** saneamento humano, coexistência temporária de texto e ID,
mudanças em filtros e relatórios. **Complexidade:** média/alta e necessariamente
incremental; não deve ser resolvida por uma carga automática.

## 19. Resultado e próximo ponto de controle

A H2C.2H encerra as 35 decisões funcionais em 30/07/2026. Nenhuma regra foi
implementada nesta etapa. O próximo passo recomendado é **H2C.2I —
Delimitação Funcional das Tabelas Adicionais do Banco**. O desenho técnico de
tabelas, constraints, migrations, interfaces e testes permanece futuro.
