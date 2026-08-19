# Etapa H2C.2C — Especificação funcional do catálogo

## 1. Resumo executivo em linguagem simples

O sistema já possui cadastros e consultas relacionados a grupos, subgrupos e
produtos/serviços, mas eles ainda não compõem um catálogo único e totalmente
ligado. Parte da relação é feita por nomes escritos em campos de texto, parte
usa o identificador do subgrupo e os grupos disponíveis na interface são listas
fixas no código.

**Confirmado pelo schema:** há as tabelas `grupos_atividade`, `subgrupos`,
`produtos_servicos` e uma tabela adicional chamada `produtos`. O vínculo físico
confirmado é apenas de `produtos_servicos.id_subgrupo` para `subgrupos.id`.
Não existe hoje `id_grupo` em `subgrupos` nem em `produtos_servicos`.

**Decisão funcional já validada:** os dados existentes devem ser preservados,
a implantação deve ser incremental e a exclusão cotidiana deverá ser
substituída por inativação e reativação.

**Recomendação:** organizar o catálogo como Grupo → Subgrupo → Produto/Serviço,
manter temporariamente os textos antigos, classificar os registros com revisão
humana e só depois tornar os novos vínculos obrigatórios. Nenhuma alteração
funcional ou de banco é feita nesta etapa.

### 1.1 Decisões validadas após a revisão

Em **29/07/2026**, foram validadas as seguintes diretrizes:

- `grupos_atividade` será a referência inicial do cadastro oficial de grupos;
- a interface mostrará somente o nome amigável “Grupos”;
- subgrupos serão vinculados aos grupos e produtos aos subgrupos;
- os campos textuais antigos serão preservados durante a transição;
- registros antigos sem vínculo aparecerão como “Não classificado”, condição
  transitória que não será um grupo artificial;
- nomes semelhantes não serão fundidos automaticamente;
- grupo, subgrupo e produto serão inativados, não apagados;
- mudanças de classificação terão histórico;
- usuários operacionais poderão solicitar inclusões e correções;
- o administrador do catálogo responderá pela qualidade e organização;
- importadores antigos permanecerão bloqueados até eventual reescrita;
- a tabela adicional `produtos` não será integrada automaticamente a
  `produtos_servicos`;
- patrimônio, EPI e Fiscalização exigirão análise própria antes de qualquer
  vínculo.

**Ponto de controle obrigatório para a implementação:** antes de reaproveitar
os registros existentes de `grupos_atividade`, deverá ser confirmado que eles
representam categorias adequadas aos produtos. Se houver registros com outra
finalidade, eles deverão ser separados ou adaptados com revisão humana, sem
reaproveitamento automático.

## 2. Situação atual

### 2.1 Método e grau de certeza

A análise utilizou somente schema exportado e versionado, código, templates,
scripts, testes e documentos. As conclusões usam estas etiquetas:

- **Confirmada pelo schema:** comprovada pela estrutura PostgreSQL exportada.
- **Confirmada pelo código:** comprovada por rotas, SQL, template, JavaScript,
  script ou teste existente.
- **Inferida:** consequência provável, ainda dependente de validação.
- **Não confirmada:** não há evidência suficiente nos artefatos analisados.
- **Decisão funcional já validada:** diretriz aprovada em etapa anterior.
- **Decisão humana ainda necessária:** escolha que não deve ser automatizada.

Nenhum banco foi acessado e nenhum script ou importador foi executado.

### 2.2 Estruturas existentes

**Confirmado pelo schema:**

- `grupos_atividade` guarda apenas identificador e nome;
- `subgrupos` guarda identificador, nome e `atividade_pai` em texto;
- `produtos_servicos` guarda classificação textual e possui um vínculo opcional
  com `subgrupos`;
- `produtos` é uma estrutura adicional sem chave estrangeira para o catálogo;
- `epi_itens`, `epis` e `epis_catalogo` são catálogos separados;
- não existe vínculo físico completo Grupo → Subgrupo → Produto.

### 2.3 Funcionamento atual no código

**Confirmado pelo código:**

- a administração de subgrupos e produtos está concentrada no `app.py` e em
  `templates/cadastro.html`;
- `/api/subgrupos` e `/api/produtos_crud` exigem administrador;
- usuários autenticados consultam produtos, grupos fixos, subgrupos e opções de
  relatório nos formulários autorizados;
- os grupos de Receita e Despesa são listas fixas repetidas no backend e no
  template, e não são lidos de `grupos_atividade`;
- o cadastro administrativo de produto grava `tipo_atividade` e `grupo` como
  textos, além de `id_subgrupo` quando informado;
- as transações guardam a descrição do item, unidade, quantidade e valores, mas
  não guardam `produto_id`;
- relatórios relacionam itens e produtos pela igualdade do texto da descrição;
- a tela atual permite editar e excluir, mas não possui inativação, reativação,
  detalhes, paginação ou administração de grupos;
- existe uma rota legada de cadastro de produto/serviço sem formulário atual
  correspondente identificado.

### 2.4 Cobertura atual de testes

**Confirmado pelo código:** os testes atuais cobrem autenticação, CSRF, acesso
administrativo, respostas JSON e proteção de erros. Não foi encontrada
cobertura funcional completa para hierarquia, normalização, duplicidades,
movimentação, inativação, reativação e preservação histórica do catálogo.

## 3. Problemas encontrados

1. **Confirmado pelo schema:** falta o vínculo físico entre subgrupo e grupo.
2. **Confirmado pelo schema:** o produto pode possuir texto de subgrupo e
   `id_subgrupo` divergentes ou incompletos.
3. **Confirmado pelo código:** grupos são duplicados em listas fixas.
4. **Confirmado pelo código:** a validação atual não confirma que o subgrupo
   pertence ao grupo selecionado.
5. **Confirmado pelo código:** a inferência automática de Receita/Despesa usa
   palavras que não correspondem de forma segura à lista atual; um produto de
   receita pode ser classificado como despesa.
6. **Confirmado pelo código:** renomear produto não atualiza itens históricos,
   que são relacionados pelo nome.
7. **Confirmado pelo código:** excluir produto após renomeá-lo pode deixar de
   detectar o uso histórico pelo nome antigo.
8. **Confirmado pelo código:** há exclusão física de subgrupos e produtos.
9. **Confirmado pelo código:** a exclusão de subgrupo verifica somente
   `id_subgrupo`; produtos ligados apenas por texto podem não ser percebidos.
10. **Confirmado pelo código:** `item` é único em todo o catálogo, o que evita
    ambiguidade hoje, mas também impede nomes legítimos iguais em classificações
    diferentes.
11. **Confirmado pelo código:** uma opção específica do filtro de relatório
    referencia alias SQL não declarado e pode falhar.
12. **Confirmado pelo código:** o importador antigo espera colunas inexistentes.
13. **Inferida:** mudanças de nome podem alterar a classificação apresentada em
    relatórios históricos.
14. **Não confirmada:** a quantidade e a qualidade dos registros sem
    classificação, duplicados ou divergentes só poderão ser medidas numa etapa
    futura autorizada.

## 4. Objetivos do catálogo

- apresentar uma única hierarquia compreensível;
- reduzir digitação repetida e divergências de nomes;
- impedir vínculos incompatíveis;
- preservar todos os registros e relatórios antigos;
- permitir consulta rápida e seleção encadeada;
- usar inativação e reativação em vez de exclusão cotidiana;
- registrar autoria, datas e mudanças relevantes;
- distinguir administração do catálogo de uso operacional;
- permitir transição sem interromper transações atuais;
- preparar integrações sem fundir automaticamente catálogos distintos.

## 5. Conceitos de grupo, subgrupo e produto

**Grupo:** categoria principal e ampla. Deve indicar também o domínio funcional
de Receita ou Despesa, se essa dimensão for mantida no grupo.

**Subgrupo:** divisão interna obrigatoriamente pertencente a um grupo.

**Produto/Serviço:** item específico pertencente a um subgrupo e disponível para
seleção nos fluxos autorizados.

**Classificação:** conjunto formado pelo caminho Grupo → Subgrupo →
Produto/Serviço.

**Situação:** Ativo ou Inativo. Um registro inativo continua visível no
histórico, mas não aparece em novas seleções comuns.

**Não classificado:** estado transitório de registro antigo que ainda não teve
seu vínculo validado.

Os exemplos de grupos fornecidos no pedido são ilustrativos e não devem ser
gravados automaticamente.

## 6. Estrutura hierárquica recomendada

Fluxo recomendado:

1. selecionar o tipo de atividade, quando aplicável;
2. selecionar um grupo ativo;
3. exibir apenas subgrupos ativos daquele grupo;
4. selecionar um subgrupo;
5. exibir apenas produtos/serviços ativos daquele subgrupo;
6. permitir cadastro ou solicitação conforme a permissão.

**Decisão funcional já validada:** `grupos_atividade` será a referência inicial
da entidade oficial de grupo;
`subgrupos` deve receber futuramente um vínculo por identificador; e
`produtos_servicos` deve continuar como catálogo operacional durante a
transição, por já ser usado por transações e relatórios.

**Decisão humana ainda necessária:** definir se Receita/Despesa pertence ao
grupo, ao produto ou a uma dimensão separada. A recomendação preliminar é
associá-la ao grupo para impedir combinações incoerentes, preservando o campo
antigo durante a transição.

**Decisão humana ainda necessária:** decidir o destino da tabela adicional
`produtos`. Ela deve permanecer preservada e oculta até que sua origem e
responsável funcional sejam confirmados.

## 7. Fluxo de cadastro de grupo

1. administrador abre “Novo grupo”;
2. informa nome e tipo de atividade, se aprovado;
3. sistema remove espaços externos e reduz espaços internos repetidos;
4. sistema procura nome normalizado equivalente;
5. se houver igual, bloqueia com mensagem amigável;
6. se houver nome parecido, alerta e permite revisão;
7. salva como ativo, com data e autoria;
8. retorna aos detalhes ou à listagem filtrada.

Regras:

- nome obrigatório, entre 2 e 100 caracteres;
- ordenação alfabética pelo nome amigável;
- comparação futura sem diferença entre maiúsculas e minúsculas;
- o nome exibido preserva capitalização aprovada;
- usuário não vê identificador nem termos de banco;
- somente administrador do catálogo ou geral cadastra.

**Confirmado pelo schema:** o `UNIQUE` atual atua sobre o valor armazenado.
Sem consultar o banco, não é possível afirmar o efeito exato da collation sobre
maiúsculas/minúsculas. Espaço final e normalização também não são tratados pelo
schema atual de forma funcionalmente suficiente.

## 8. Fluxo de cadastro de subgrupo

1. selecionar grupo ativo;
2. informar nome;
3. normalizar o texto;
4. verificar duplicidade dentro do mesmo grupo;
5. alertar sobre nomes semelhantes;
6. salvar ativo, com vínculo, data e autoria.

Regras:

- grupo e nome obrigatórios;
- nome recomendado entre 2 e 255 caracteres;
- mesmo nome pode existir em grupos diferentes;
- mesmo nome normalizado não pode repetir dentro do mesmo grupo;
- seleção mostra “Grupo — Subgrupo” quando houver risco de ambiguidade;
- somente grupo ativo aceita novo subgrupo;
- novos registros não podem usar apenas `atividade_pai` textual.

## 9. Fluxo de cadastro de produto

1. selecionar grupo;
2. selecionar subgrupo filtrado pelo grupo;
3. informar o nome do produto/serviço;
4. informar unidade, código e descrição somente se esses campos forem aprovados;
5. pesquisar duplicidades exatas e semelhantes no subgrupo;
6. confirmar os dados;
7. salvar ativo, com auditoria.

Regras:

- grupo, subgrupo e nome obrigatórios para novos registros;
- vínculo com grupo é obtido pelo subgrupo, evitando duas fontes conflitantes;
- não aceitar identificador de subgrupo que pertença a outro grupo;
- não calcular valores financeiros no catálogo;
- não expor identificadores na interface;
- produto inativo não aparece em nova transação;
- produto histórico continua consultável.

**Decisão humana ainda necessária:** confirmar se o nome pode repetir em
subgrupos diferentes. A recomendação é permitir no futuro, mas somente depois
que transações guardarem `produto_id` e uma fotografia textual; hoje a
unicidade global evita ambiguidade no vínculo por descrição.

## 10. Fluxo de consulta

A ação principal de cada listagem deve ser “Visualizar”.

- grupos: nome, tipo, situação e quantidades de subgrupos/produtos;
- subgrupos: caminho completo, situação e quantidade de produtos;
- produtos: caminho completo, nome, código/unidade quando existirem e situação;
- detalhes: auditoria, vínculos, alertas e histórico disponível;
- registros antigos: indicação visível “Não classificado”, sem termos técnicos.

Usuários autorizados a operar transações veem apenas opções ativas na seleção,
mas podem consultar a fotografia histórica dos itens já usados.

## 11. Fluxo de edição

Edição comum:

- corrige nome, descrição, unidade ou dados de apresentação;
- exige validação de duplicidade;
- registra usuário e data;
- não muda silenciosamente o vínculo hierárquico.

Reclassificação:

- mover subgrupo para outro grupo ou produto para outro subgrupo é ação própria;
- mostra registros afetados;
- exige confirmação e motivo;
- registra classificação anterior e nova;
- ocorre numa transação única;
- não reescreve fotografias históricas já consolidadas.

**Recomendação:** a simples edição do registro não é suficiente para mudanças
de vínculo relevantes. Deve existir histórico de classificação antes de
liberar esse recurso.

## 12. Fluxo de inativação

Produto:

- solicitar confirmação e motivo;
- gravar situação, data, usuário e motivo;
- retirar de novas seleções;
- manter em consultas e vínculos antigos.

Subgrupo:

- listar produtos ativos afetados;
- não inativar filhos automaticamente;
- recomendar bloqueio enquanto houver produtos ativos, até reclassificação ou
  inativação individual aprovada.

Grupo:

- listar subgrupos e produtos ativos afetados;
- não inativar descendentes automaticamente;
- recomendar bloqueio enquanto houver descendentes ativos.

Nenhum desses fluxos usa exclusão física cotidiana.

## 13. Fluxo de reativação

- exigir confirmação;
- validar duplicidade criada durante o período inativo;
- produto só reativa se seu subgrupo e grupo estiverem ativos;
- subgrupo só reativa se seu grupo estiver ativo;
- grupo pode reativar sem reativar filhos automaticamente;
- registrar data, usuário e motivo opcional;
- manter todos os períodos anteriores no histórico.

## 14. Tratamento de registros vinculados

- registro usado historicamente nunca desaparece;
- inativação não altera transações, documentos ou relatórios consolidados;
- exclusão física deve ser reservada, se existir, a procedimento técnico
  excepcional, não à interface comum;
- mudança de nome não deve apagar o texto histórico;
- mudança de classificação não deve reclassificar automaticamente transações
  antigas;
- relatórios futuros devem usar `produto_id` e fotografia da classificação no
  momento do lançamento;
- chaves estrangeiras futuras devem bloquear exclusão, nunca usar cascata para
  apagar histórico.

**Confirmado pelo código:** hoje transações usam a descrição como ligação.
Portanto, a migração para identificadores precisa ocorrer antes de relaxar a
unicidade global ou permitir renomeações com segurança histórica.

## 15. Tratamento de registros sem classificação

Alternativas avaliadas:

- grupo técnico “Não classificado”: simples, mas mistura pendência com grupo
  real e polui relatórios;
- subgrupo técnico “Não classificado”: cria classificação artificial;
- vínculo temporariamente nulo: preserva o dado sem inventar categoria;
- fila administrativa: organiza a correção humana.

**Recomendação:** permitir temporariamente vínculo nulo apenas para registros
legados, exibir estado derivado “Não classificado” e criar uma fila
administrativa de classificação. “Não classificado” não deve ser um grupo
selecionável para novos cadastros.

Novos produtos exigem classificação válida. Registros antigos continuam
pesquisáveis, utilizáveis apenas conforme regra de compatibilidade e nunca são
fundidos automaticamente.

## 16. Pesquisa e filtros

Grupos:

- pesquisa por nome;
- filtro por tipo e situação;
- ordenação alfabética e paginação.

Subgrupos:

- pesquisa por nome;
- filtros por grupo, tipo e situação;
- caminho completo na resposta.

Produtos:

- pesquisa por nome e, se aprovado, código;
- filtros por grupo, subgrupo, tipo, situação e classificação;
- opção “Não classificados”;
- paginação e ordenação;
- filtros mantidos ao voltar dos detalhes.

Quando não houver resultado: “Nenhum registro encontrado com os filtros
informados.”

## 17. Campos e regras de preenchimento

### 17.1 `grupos_atividade` — 2 campos atuais

| Campo técnico | Nome amigável | Tipo atual | Obrigatório atual | Padrão | Uso e regra recomendada |
|---|---|---:|---:|---|---|
| `id` | Identificador | INTEGER | Sim | sequência | Interno, automático, somente leitura e nunca exibido ao usuário comum. |
| `nome` | Grupo | VARCHAR(100) | Sim | nenhum | Obrigatório; normalizar espaços; comparar sem diferença de caixa; editável com auditoria futura. |

**Confirmado pelo schema:** `nome` possui unicidade.

**Não confirmada:** a semântica funcional para caixa, acentos e espaços.

### 17.2 `subgrupos` — 3 campos atuais

| Campo técnico | Nome amigável | Tipo atual | Obrigatório atual | Padrão | Uso e regra recomendada |
|---|---|---:|---:|---|---|
| `id` | Identificador | INTEGER | Sim | sequência | Interno e automático. |
| `nome` | Subgrupo | VARCHAR(255) | Sim | nenhum | Obrigatório; duplicidade futura avaliada dentro do grupo. |
| `atividade_pai` | Grupo atual | VARCHAR(255) | Sim | nenhum | Texto legado; manter durante transição e substituir como fonte oficial por vínculo com grupo. |

**Confirmado pelo schema:** a unicidade atual é do par `nome` +
`atividade_pai`, ainda baseado em texto.

### 17.3 `produtos_servicos` — 8 campos atuais

| Campo técnico | Nome amigável | Tipo atual | Obrigatório atual | Padrão | Uso e regra recomendada |
|---|---|---:|---:|---|---|
| `id` | Identificador | INTEGER | Sim | sequência | Interno e automático. |
| `tipo` | Tipo de transação | VARCHAR(20) | Sim | nenhum | Receita/Despesa no fluxo atual; regra futura depende da decisão sobre o grupo. |
| `tipo_atividade` | Tipo de atividade | VARCHAR(255) | Sim | nenhum | Texto legado usado em filtros; preservar até consolidar a hierarquia. |
| `grupo` | Grupo | VARCHAR(255) | Não | nulo | Texto legado; não deve competir com o vínculo oficial futuro. |
| `subgrupo` | Subgrupo | VARCHAR(255) | Não | nulo | Fotografia textual atual; preservar durante transição. |
| `item` | Produto/Serviço | VARCHAR(255) | Sim | nenhum | Nome atual, globalmente único; normalizar e manter restrição até eliminar vínculo por texto. |
| `data_hora_cadastro` | Cadastrado em | TIMESTAMP | Sim | `now()` | Automático, somente leitura; avaliar TIMESTAMPTZ na baseline futura. |
| `id_subgrupo` | Subgrupo vinculado | INTEGER | Não | nulo | FK opcional para `subgrupos.id`; deverá ser a base do vínculo, após saneamento. |

Não existem hoje, nessa tabela, código, descrição própria, unidade, situação,
data de inativação, motivo ou auditoria por usuário.

### 17.4 `produtos` — 8 campos adicionais

| Campo técnico | Nome amigável | Tipo atual | Obrigatório atual | Padrão | Uso e regra recomendada |
|---|---|---:|---:|---|---|
| `id` | Identificador | INTEGER | Sim | sequência | Interno; origem funcional ainda não confirmada. |
| `nome` | Produto | VARCHAR(255) | Sim | nenhum | Nome da estrutura adicional; não fundir automaticamente. |
| `grupo` | Grupo | VARCHAR(100) | Não | nulo | Texto sem FK; preservar. |
| `subgrupo` | Subgrupo | VARCHAR(100) | Não | nulo | Texto sem FK; preservar. |
| `unidade` | Unidade | VARCHAR(20) | Não | nulo | Possível unidade padrão; uso atual não confirmado. |
| `valor_padrao` | Valor padrão | NUMERIC(10,2) | Não | nulo | Possível referência; não usar sem regra funcional aprovada. |
| `tipo` | Tipo | VARCHAR(50) | Não | nulo | Sem domínio confirmado. |
| `uvr` | UVR | VARCHAR(20) | Não | nulo | Texto legado; não criar integração automática. |

**Confirmado pelo código:** não foi localizada operação ativa dessa tabela no
fluxo atual analisado.

**Decisão humana ainda necessária:** preservar oculta, arquivar ou incorporar
após identificar origem, responsável e dados.

### 17.5 Campos futuros recomendados

De forma aditiva e somente em etapas futuras:

- Grupo: tipo/domínio, ativo, criado/atualizado/inativado em, usuários e motivo.
- Subgrupo: `grupo_id`, ativo e os mesmos campos de auditoria.
- Produto: vínculo oficial com subgrupo, ativo e auditoria; código, descrição e
  unidade dependem de decisão humana.
- Histórico: entidade de eventos ou classificações para mudanças relevantes.

## 18. Duplicidades

Normalização recomendada:

- remover espaços externos;
- transformar espaços internos repetidos em um;
- comparar sem diferença entre maiúsculas e minúsculas;
- preservar a grafia escolhida para exibição;
- decidir posteriormente se acentos diferentes são equivalentes.

Bloqueios:

- grupo igual normalizado;
- subgrupo igual normalizado no mesmo grupo;
- produto igual normalizado no mesmo subgrupo, após transição segura.

Alertas, sem bloqueio automático:

- nomes parecidos;
- mesmo produto em subgrupos diferentes;
- códigos semelhantes;
- descrições aproximadas.

Não usar unidade ou descrição isoladamente como prova de duplicidade. Não fundir
registros sem validação humana.

## 19. Importador antigo

### 19.1 Inventário

| Artefato | Finalidade identificada | Situação |
|---|---|---|
| `importar_csv_nuvem.py` | Importar grupo, subgrupo e produto de CSV para banco configurado | **Legado incompatível e não autorizado:** espera `id_grupo` em tabelas que não possuem a coluna. |
| `atualizar_padrao_v2.py` | Atualizar classificação por nome usando CSV | **Legado arriscado:** usa configuração histórica e correspondência textual. |
| `migracao_inteligente.py` | Criar/migrar subgrupos e vínculos | **Ferramenta histórica:** não representa migration-base oficial. |
| `executar_migracao_produtos.py` | Acionar migração textual para `id_subgrupo` | **Manual:** possui confirmação explícita, mas precisa de caracterização antes de uso. |
| `migrar_dados.py` | Copiar tabela de produtos entre bancos | **Migração histórica:** não é importador funcional do catálogo. |
| `padrao_itens.csv` e `padrao_itens2.csv` | Fontes candidatas de classificação | **Não canônicas:** os conteúdos diferem e nenhuma foi aprovada como fonte oficial. |

### 19.2 Risco e proteção futura

- não executar nenhum desses artefatos no ambiente atual;
- registrar aviso claro na documentação operacional;
- mantê-los fora de menus e inicialização;
- futuramente renomear ou arquivar com aprovação;
- reescrever o importador com prévia, validação, transação e relatório;
- criar teste que garanta ausência de importação automática;
- exigir ambiente, arquivo e confirmação explícitos.

Nenhum importador foi executado nesta etapa.

## 20. Migração dos dados existentes

Plano futuro, sem SQL:

1. criar campos e estruturas novas como opcionais;
2. inventariar registros, textos distintos e vínculos atuais;
3. identificar divergências, órfãos e possíveis duplicidades;
4. apresentar uma fila de classificação assistida;
5. validar cada correspondência relevante com responsável humano;
6. preencher vínculos em lotes pequenos e auditáveis;
7. comparar contagens e relatórios antes/depois;
8. adaptar leituras para preferir o vínculo e manter texto como fotografia;
9. adaptar transações para guardar identificador e fotografia;
10. somente depois avaliar obrigatoriedade e novas unicidades;
11. manter mecanismo de reversão para a leitura antiga;
12. nunca fundir automaticamente nomes apenas semelhantes.

Registros antigos nulos continuam visíveis como “Não classificado”. Nenhuma data
ou autoria histórica deve ser inventada.

## 21. Permissões

| Perfil funcional | Consultar | Usar em formulário | Solicitar inclusão/correção | Administrar | Reclassificar/inativar |
|---|---:|---:|---:|---:|---:|
| Consulta | Sim, ativos | Conforme módulo autorizado | Não | Não | Não |
| Operacional | Sim | Sim | Recomendado | Não | Não |
| Administrador do catálogo | Sim, todos | Sim | Sim | Sim | Sim |
| Administrador geral | Sim, todos | Sim | Sim | Sim | Sim, inclusive exceções auditadas |

**Recomendação:** usuário operacional solicita novo produto ou correção; não
cadastra diretamente na primeira versão. Isso reduz duplicidade e mantém um
responsável pela qualidade.

**Confirmado pelo código:** atualmente a administração é reservada a
administradores, enquanto consultas necessárias aos formulários exigem login.

## 22. Mensagens ao usuário

- “Grupo cadastrado com sucesso.”
- “Subgrupo cadastrado com sucesso.”
- “Produto cadastrado com sucesso.”
- “Alterações salvas com sucesso.”
- “Registro inativado. Ele permanece disponível no histórico.”
- “Registro reativado com sucesso.”
- “Já existe um grupo com esse nome.”
- “Já existe esse subgrupo no grupo selecionado.”
- “Já existe um produto com nome semelhante neste subgrupo. Confira antes de
  continuar.”
- “Selecione um grupo.”
- “Selecione um subgrupo.”
- “Este registro está inativo e não pode ser usado em um novo lançamento.”
- “Registro não encontrado ou indisponível.”
- “Não foi possível concluir a operação agora. Tente novamente.”

Nenhuma mensagem deve mostrar SQL, tabela, coluna, identificador interno ou
traceback.

## 23. Relatórios e exportações

| Opção | Classificação | Justificativa |
|---|---|---|
| Catálogo completo | Essencial | Base para conferência e implantação. |
| Grupos ativos e inativos | Essencial | Governança e reativação. |
| Subgrupos por grupo | Essencial | Validação da hierarquia. |
| Produtos por grupo e subgrupo | Essencial | Uso operacional e auditoria. |
| Produtos não classificados | Essencial | Fila de saneamento. |
| Produtos inativos | Essencial | Preservação histórica e consulta. |
| Possíveis duplicidades | Útil | Apoia revisão humana sem fusão automática. |
| Histórico de classificação | Futura | Depende da estrutura de eventos. |
| Exportação CSV curada | Útil | Revisão assistida e homologação. |
| Exportação técnica bruta | Dispensável ao usuário comum | Expõe detalhes sem ganho operacional. |

Relatórios históricos devem usar a fotografia do lançamento, não apenas a
classificação atual.

## 24. Integrações com outros módulos

| Área | Evidência | Dependência/risco | Comportamento esperado |
|---|---|---|---|
| Transações financeiras | **Confirmada pelo código** | Vínculo atual pelo texto da descrição | Adicionar futuramente `produto_id` e preservar fotografia textual. |
| Relatórios financeiros | **Confirmada pelo código** | Junção textual pode perder classificação após renomear | Migrar gradualmente para identificador + fotografia. |
| Clientes/fornecedores | **Confirmada pelo código** | Usa as mesmas listas fixas de atividade | Compartilhar fonte oficial de grupos somente após validar semântica. |
| Patrimônio | **Não confirmada** | Conceitos podem parecer semelhantes, mas possuem regras distintas | Não vincular automaticamente; decidir após H2C.2B. |
| EPI | **Confirmada pelo schema; uso atual não confirmado** | Existem três catálogos próprios | Preservar separado até decisão do responsável funcional. |
| Documentos/entregas/estoque | **Inferida** | Tabelas legadas sugerem relação, sem fluxo atual comprovado | Manter oculto e mapear antes de integrar. |
| Fiscalização de Contratos | **Confirmada pelo código como catálogo separado** | Planilhas e ativos possuem entidades próprias | Não substituir nem fundir automaticamente. |
| Tabela `produtos` | **Confirmada pelo schema; uso não confirmado** | Pode ser catálogo histórico alternativo | Preservar, identificar origem e decidir humanamente. |

## 25. Impactos futuros no banco

Possíveis mudanças aditivas:

- vínculo de `subgrupos` com `grupos_atividade`;
- vínculo confiável de `produtos_servicos` com `subgrupos`;
- situação ativa/inativa e auditoria;
- índices para nomes normalizados, hierarquia, situação e pesquisa;
- unicidade de subgrupo dentro do grupo;
- histórico de reclassificação e situação;
- identificador do produto e fotografia em itens de transação.

Sequência segura:

1. criar estrutura sem obrigatoriedade;
2. caracterizar e classificar dados;
3. validar inconsistências;
4. mudar leituras e escritas;
5. somente depois adicionar obrigatoriedade ou restrições.

Não remover os textos legados nem alterar a tabela adicional `produtos` antes
de decisão específica.

## 26. Impactos futuros no código

- retirar listas fixas duplicadas e usar serviço único de catálogo;
- separar rotas e serviços do grande `app.py` de forma incremental;
- criar validações centralizadas de normalização e hierarquia;
- criar telas próprias de grupo, subgrupo e produto;
- trocar exclusão por inativação e reativação;
- criar seleção encadeada acessível;
- preservar contratos JSON esperados durante a transição;
- corrigir a inferência de Receita/Despesa;
- guardar produto por identificador nos novos itens;
- ajustar relatórios sem alterar resultados históricos;
- adicionar testes de caracterização antes de mudanças.

## 27. Compatibilidade

- rotas antigas devem continuar respondendo até a migração dos consumidores;
- itens de transação antigos continuam com sua descrição;
- filtros e relatórios antigos devem produzir resultados equivalentes;
- registros sem vínculo não desaparecem;
- a unicidade global do produto não deve ser removida antes do novo vínculo nas
  transações;
- EPI, patrimônio, Fiscalização e tabela `produtos` permanecem independentes;
- APIs devem manter campos necessários aos JavaScripts existentes durante a
  transição;
- nenhuma alteração deve importar CSV ou classificar dados na inicialização.

## 28. Estratégia de homologação

Usar ambiente isolado e dados fictícios para testar:

- nomes com caixa, acentos e espaços diferentes;
- nomes iguais em grupos diferentes;
- produto sem classificação legado;
- grupo, subgrupo e produto ativos/inativos;
- reativação com conflito;
- bloqueio de inativação com filhos ativos;
- reclassificação com histórico;
- transações antigas e novas;
- relatórios antes e depois;
- paginação, filtros e retorno de tela;
- perfis de consulta, operacional e administrador;
- importador legado bloqueado;
- falhas de banco apresentadas sem detalhes técnicos.

Contagens e amostras de relatórios devem ser comparadas antes e depois. Banco de
homologação e credenciais devem permanecer separados.

## 29. Estratégia de reversão

- cada incremento deve possuir commit e ativação isolados;
- mudanças de banco futuras devem ser aditivas;
- leituras novas devem poder voltar temporariamente aos textos legados;
- não remover coluna, tabela ou dado durante a transição;
- não desfazer classificação apagando histórico;
- desativar a nova interface sem apagar registros, se necessário;
- conservar relatório de correspondências e erros;
- restaurar código pelo commit anterior, sem migration destrutiva;
- validar novamente contagens e relatórios após qualquer reversão.

## 30. Critérios de aceite

- hierarquia e situação atual documentadas;
- todos os 21 campos das quatro tabelas centrais/relacionadas analisados;
- vínculos existentes e ausentes registrados;
- fluxos de grupo, subgrupo e produto descritos;
- registros não classificados preservados;
- inativação e reativação especificadas;
- reclassificação separada de edição comum;
- duplicidades exatas e semelhantes tratadas;
- importadores mapeados e classificados como não autorizados;
- permissões e mensagens propostas;
- pesquisas, relatórios e integrações classificados;
- migração gradual, homologação e reversão definidas;
- questões humanas explicitadas;
- nenhum código, banco, SQL, migration, importação ou deploy executado.

## 31. Incrementos futuros

| Incremento | Objetivo e áreas | Dependências e riscos | Testes e aceite | Reversão |
|---|---|---|---|---|
| H2C.3C.1 | Caracterizar rotas, SQL, respostas e dados esperados do catálogo atual | Sem banco real; risco de cobertura incompleta | Testes preservam contratos atuais | Remover somente testes novos |
| H2C.3C.2 | Bloquear uso acidental dos importadores legados | Inventário aprovado; risco operacional | Inicialização não importa dados; scripts falham fechados | Restaurar documentação/entrada anterior |
| H2C.3C.3 | Preparar estrutura aditiva de grupo, subgrupo, produto e auditoria | Decisões humanas e migration-base | Estruturas opcionais e dados antigos intactos | Ignorar campos novos; sem apagar |
| H2C.3C.4 | Criar fila e classificação transitória | Estrutura pronta; risco de correspondência errada | Não classificados visíveis, sem fusão automática | Reverter vínculos preservando relatório |
| H2C.3C.5 | Implementar backend de grupos | H2C.3C.3 | Normalização, duplicidade, permissões e situação | Desativar novas rotas |
| H2C.3C.6 | Implementar backend de subgrupos | Grupos ativos | Hierarquia e duplicidade por grupo | Voltar à consulta antiga |
| H2C.3C.7 | Implementar backend de produtos | Subgrupos e decisão de unicidade | Vínculo validado, auditoria e compatibilidade | Manter escrita antiga controlada |
| H2C.3C.8 | Criar seleção encadeada e telas simples | APIs estáveis | Teclado, celular, filtros e nenhum ID exposto | Reativar template anterior |
| H2C.3C.9 | Adicionar pesquisa, filtros e mensagens | Listagens prontas | Paginação, retorno e erros amigáveis | Desativar filtros novos |
| H2C.3C.10 | Inativação, reativação, reclassificação e histórico | Auditoria pronta | Sem DELETE, filhos protegidos e histórico íntegro | Desativar ações, manter eventos |
| H2C.3C.11 | Relatórios e exportações | Identificadores/fotografias confiáveis | Totais equivalentes e não classificados visíveis | Voltar aos relatórios antigos |
| H2C.3C.12 | Homologação final e liberação controlada | Todos os incrementos | Aceite funcional, segurança e regressão | Desativar recurso novo sem apagar dados |

Cada incremento exige autorização própria e não autoriza automaticamente o
seguinte.

## 32. Questões que dependem de decisão humana

1. Receita/Despesa deve pertencer ao grupo, ao produto ou a outra classificação?
2. Quais são os grupos oficiais e quem pode aprová-los?
3. O termo exibido deve ser “Produto”, “Produto/Serviço” ou variar pelo tipo?
4. Usuários operacionais cadastram diretamente ou enviam solicitação?
5. Código do produto será necessário e, se sim, quem o define?
6. Unidade padrão pertence ao catálogo ou somente ao lançamento?
7. Descrição complementar será necessária?
8. Nomes iguais de produto serão permitidos em subgrupos diferentes após a
   migração para identificadores?
9. A comparação de duplicidade deve ignorar acentos?
10. Grupo com filhos ativos deve ser bloqueado para inativação, como recomendado,
    ou poderá ficar indisponível por efeito derivado?
11. Quem poderá reclassificar e quais motivos serão obrigatórios?
12. A classificação histórica deve registrar somente mudanças ou também uma
    fotografia em cada transação?
13. Qual é a origem e o responsável pela tabela `produtos`?
14. Os catálogos de EPI devem permanecer totalmente separados?
15. Algum vínculo com patrimônio é realmente necessário?
16. Qual dos dois CSVs históricos, se algum, pode servir apenas como material de
    apoio para revisão humana?
17. Por quanto tempo os campos textuais legados devem permanecer visíveis?
18. Produtos antigos não classificados poderão ser escolhidos em novos
    lançamentos durante a transição?
19. Qual perfil assumirá a função de administrador do catálogo?
20. Quais relatórios precisam estar prontos antes da primeira homologação?

Até essas decisões serem aprovadas, a especificação recomenda preservar dados,
manter estruturas incompletas ocultas e não automatizar classificações.
