# H2C.4A.1 — Infraestrutura inicial de migrations

## Situação

A infraestrutura inicial foi implementada para validação técnica, sem iniciar
PostgreSQL, sem ler `.env`, sem abrir conexão real e sem aplicar qualquer
migration. A integração com o Flask também não foi realizada.

## Componentes

- **Manifesto JSON:** contém exclusivamente M0000 e M0001. Campos desconhecidos,
  tipos incorretos, duplicidades, dependências inválidas, ciclos e caminhos fora
  de `migrations_control/sql/` são rejeitados.
- **Checksum:** lê bytes, decodifica UTF-8 estrito, normaliza somente CRLF e CR
  para LF e calcula SHA-256 hexadecimal minúsculo. Espaços, Unicode e newline
  final não são modificados.
- **Preflight:** usa apenas consultas `SELECT` parametrizadas a `pg_catalog` e
  classifica o banco como `BANCO_NOVO`, `BANCO_CONTROLADO` ou
  `BANCO_DESCONHECIDO`. Relações, rotinas, tipos, triggers, policies, regras e
  objetos de extensão no `public` entram no inventário conservador. O schema
  `public` ausente e qualquer estado não comprovado bloqueiam antes de DDL.
- **Advisory lock:** a chave `bigint` assinada deriva dos oito primeiros bytes do
  SHA-256 de `sistema-recic3:baseline:public:v1`. O timeout padrão é 30 segundos,
  a conexão é mantida e a liberação é explícita.
- **Ledger:** a M0001 define somente `schema_migrations` e
  `schema_migration_execucoes`, com as colunas, constraints e índices do catálogo
  físico aprovado.
- **Assinatura física em validação:** implementa a conferência de schema,
  relkind, ordem e definição de colunas, PKs, UNIQUEs, CHECKs, índices e
  sequências `IDENTITY` do ledger. Os índices incluem direção, NULLS, operator
  class, collation, INCLUDE, propriedades internas e `pg_get_indexdef`.
  Defaults e expressões usam tokens
  conservadores baseados nas saídas futuras de `pg_get_expr`,
  `pg_get_constraintdef` e `pg_get_indexdef`.
- **Runner:** exige conexão dedicada, aberta e `IDLE`; preserva o autocommit
  original; usa autocommit no lock/preflight e uma transação exclusiva para
  M0001. Estrutura e histórico são validados antes do commit. Trabalho anterior
  do chamador nunca é confirmado nem revertido.
- **Logs:** os eventos contêm apenas identificadores, estados, duração e códigos
  sanitizados. SQL, credenciais, URL de conexão e conteúdo do `.env` não são
  registrados.
- **CLI offline:** `validar-manifesto`, `verificar-checksums` e `mostrar-plano`
  funcionam sem banco. `preflight` e `aplicar` permanecem bloqueados até uma
  subetapa com entrega explícita e segura de conexão.

## M0000 e M0001

M0000 é uma operação interna do executor, ordem zero, sem arquivo SQL e sem
DDL. Ela representa preflight e advisory lock e não entra no ledger como
migration aplicada.

M0001 é a primeira migration física, ordem um. O runner executa seu DDL,
inspeciona integralmente a estrutura ainda dentro da transação, registra M0001
nas duas tabelas e valida o histórico antes do `commit`. Se qualquer passo
falhar, o `rollback` remove toda a tentativa, inclusive as tabelas que acabariam
de ser criadas. Como o ledger ainda não existia antes dessa transação, uma
falha da própria M0001 fica somente no log seguro.

Todos os objetos da M0001 e todos os comandos do ledger usam explicitamente o
schema `public`; as funções de catálogo e advisory lock usam `pg_catalog`. O
`search_path` não decide onde o ledger será criado ou consultado.

O SQL não usa `IF NOT EXISTS`, `DROP`, comandos de alteração de dados ou criação
de tabelas funcionais. H001 a H011 não aparecem como operações executáveis.

## Testes locais

Após a revisão H2C.4A.1R, que emitiu parecer C, foram corrigidos três
bloqueadores: inventário incompleto, validação superficial do ledger e contrato
transacional insuficiente. Também foram corrigidos unlock, cursores, JSON com
chaves duplicadas, `bool` aceito como inteiro e operações desabilitadas.

Existem agora **210 testes unitários offline** para manifesto, checksum,
inventário, assinatura física, histórico, conexão, lock, runner, CLI e
segurança. O teste de duração exercita o runner real, e o teste de importação
usa um novo interpretador Python local com rede, ambiente e escrita bloqueados.
Os doubles reproduzem `closed`, `autocommit`, `get_transaction_status`, commit,
rollback e falhas. Isso continua sendo teste unitário, não integração real.

O checksum normativo da M0001 mudou porque `public` e `pg_catalog` passaram a
ser explícitos:

- anterior: `9ced3c54400c145846e68e2b2cf13af3f1c4983be61b39318c9b91e391508d81`;
- atual: `1966113e8d20f4f3aaa2ebc0b6b1f312470ac99835ea97026305c732ab5e0f39`.

## Segunda correção técnica H2C.4A.1C2

A revisão H2C.4A.1R2 manteve o parecer C e encontrou quatro bloqueadores e dois
achados altos: sequências automáticas do ledger rejeitadas, tentativa zero da
M0001 insuficientemente comprovada, assinatura incompleta dos índices, logger
capaz de mascarar erro, janela entre checksum e execução e normalização textual
capaz de unir tokens distintos.

A H2C.4A.1C2 corrigiu os seis pontos:

- as sequências `public.schema_migrations_id_seq` e
  `public.schema_migration_execucoes_id_seq` fazem parte da assinatura esperada;
  schema, relkind, proprietário, coluna, `attidentity`, dependência, tipo,
  incremento, limites, início, cache e cycle são conferidos;
- o estado inicial controlado exige uma única execução da M0001, `APLICADA` e
  precisamente na tentativa zero; uma falha da própria bootstrap não pode ficar
  persistida porque a transação inteira é revertida;
- índices passaram a comparar propriedades completas, inclusive ASC/DESC,
  NULLS FIRST/LAST, operator classes, collations, atributos INCLUDE,
  `indoption`, estados físicos e definição de `pg_get_indexdef`;
- expressões são tokenizadas sem juntar palavras, preservando strings,
  identificadores delimitados, dollar quotes, casts e operadores; construções
  não suportadas são rejeitadas;
- imediatamente antes do DDL, o SQL é lido uma única vez, normalizado, conferido
  por checksum e preservado em um artefato imutável; o cursor recebe exatamente
  o texto desse artefato, sem releitura posterior;
- logging passou a ser best effort e nunca substitui erros de migration,
  rollback, unlock ou restauração.

Os testes reproduzem nominalmente todos os achados da revisão, além de corrigir
o cenário de falha da M0001, usar exceção específica e conferir escape por
symlink. O SQL M0001 não precisou mudar; portanto, o checksum normativo permanece
`1966113e8d20f4f3aaa2ebc0b6b1f312470ac99835ea97026305c732ab5e0f39`.

Nenhum PostgreSQL ou SQL foi executado. A implementação aguarda uma terceira
revisão técnica, e a H2C.4A.2 continua dependente de parecer posterior e
autorização humana expressa.

## Terceira correção técnica H2C.4A.1C3

A revisão H2C.4A.1R3 emitiu parecer C e identificou construção manual
incoerente de `ValidatedSql`, confinamento final incompleto, quatro propriedades
ausentes na assinatura de índices, nomes físicos concatenados, lacunas de testes
e afirmações documentais prematuras.

A H2C.4A.1C3 passou a vincular cada artefato ao identificador da operação, raiz
SQL, caminho canônico e checksums esperado e calculado. Bytes e texto são
conferidos nos dois sentidos, a identidade do arquivo aberto é comparada antes e
depois da leitura, e o runner revalida o artefato e o caminho sem reler o
conteúdo antes de entregar `texto_sql` ao cursor. Artefatos falsos, subclasses,
operações divergentes e caminhos fora da raiz são rejeitados.

Os índices passaram a incluir `indimmediate`, `indisclustered`,
`indisreplident` e `indnullsnotdistinct`. A presença desses campos no catálogo
é uma capacidade obrigatória; versão incompatível é bloqueada, sem presumir
valor `false`. Operator classes e collations usam pares estruturados de schema e
nome. Para os seis índices produzidos pela M0001, os valores derivados são
`indimmediate = true`, `indisclustered = false`, `indisreplident = false` e
`indnullsnotdistinct = false`.

Foram acrescentados testes nominais para os invariantes do artefato, caminho e
identidade do arquivo, campos de índice, nomes qualificados, quatro combinações
de direção/NULLS, INCLUDE e propriedades individuais das sequências. Existem
agora **249 testes unitários offline** aprovados. Isso ainda não comprova o
formato retornado por um PostgreSQL real.

Nenhum PostgreSQL, SQL ou migration foi executado. A implementação aguarda uma
quarta revisão técnica independente, e a H2C.4A.2 continua bloqueada até parecer
posterior e autorização humana expressa.

Comandos offline previstos:

```text
python -m migrations_control.cli validar-manifesto
python -m migrations_control.cli verificar-checksums
python -m migrations_control.cli mostrar-plano
python -m unittest tests.test_migrations_control_h2c4a1
```

## Limitações e próximos passos

- B2 foi parcialmente tratado: o executor inicial existe, mas ainda precisa ser
  validado contra PostgreSQL efêmero.
- B1 permanece ativo: `role` e `uvr_acesso` não foram alterados.
- B3 permanece ativo: o bootstrap não foi implementado.
- B4 permanece ativo: H001 a H011 não foram executadas nem validadas.
- Não há integração automática com a inicialização do sistema.
- A H2C.4A.1C3 está pronta para quarta revisão técnica. A H2C.4A.2 permanece
  bloqueada até parecer posterior e autorização humana expressa.

## Quarta correção técnica H2C.4A.1C4

A revisão independente H2C.4A.1R4 emitiu parecer **B — recomendada após
ajustes menores no código ou nos testes**. Ela confirmou as proteções do runner
e os 249 testes anteriores, mas identificou dois pontos restantes: o construtor
público de `ValidatedSql` ainda aceitava campos físicos fornecidos livremente e
a assinatura dos índices ainda não incluía `pg_index.indcheckxmin`.

`ValidatedSql` deixou de possuir construtor público utilizável. A API suportada
é `carregar_sql_validado()`, que resolve e confina o caminho, abre o arquivo,
obtém `st_dev`, `st_ino`, `st_size` e `st_mtime_ns` do descritor, lê os bytes,
normaliza UTF-8/LF, calcula o checksum, deriva o texto dos mesmos bytes e repete
as verificações de caminho e identidade antes de criar internamente o objeto
frozen com slots. O chamador não fornece identidade, bytes validados, texto
independente nem checksum calculado. Uma chamada direta `ValidatedSql(...)`
falha explicitamente.

Esse contrato de API não é apresentado como barreira contra introspecção
deliberada de baixo nível em Python. O runner permanece como defesa independente:
exige o tipo exato, repete os invariantes e reconfirma operação, raiz, caminho,
checksum e identidade antes de criar o cursor de DDL. Um objeto artificial
incompleto criado por bypass é rejeitado antes de `cursor.execute()`.

A assinatura física passou a consultar e comparar também `indcheckxmin`. Os
seis índices declarados pela M0001 esperam individualmente
`indcheckxmin = false`, coerente com índices novos criados diretamente pela DDL,
sem estado de atualização que imponha a proteção de horizonte. O valor físico
será confirmado em PostgreSQL efêmero; a M0001 não foi alterada para forçá-lo.
A consulta de capacidade agora exige simultaneamente `indimmediate`,
`indisclustered`, `indisreplident`, `indnullsnotdistinct` e `indcheckxmin`, sem
fallback permissivo.

Foram acrescentados testes nominais para construtor direto, fábrica válida,
arquivo inexistente, bypass de baixo nível, caminhos finais externos, arquivo
homônimo, troca simulada de symlink, `indcheckxmin` e nomes qualificados com
pontos em posições distintas. A suíte da infraestrutura possui agora **262
testes unitários offline** aprovados, preservando os 249 anteriores.

A futura H2C.4A.2 deverá usar **PostgreSQL 15 ou superior**. Essa exigência
decorre, entre outros pontos, da presença de `indnullsnotdistinct`; a capacidade
real do catálogo será verificada antes dos testes, e versão ou catálogo
incompatível bloqueará a execução sem fallback. Essa exigência vale apenas para
o ambiente efêmero da H2C.4A.2 até decisão posterior sobre o ambiente definitivo.

Permanece um risco TOCTOU informativo entre a última verificação do arquivo e o
uso do artefato. Ele não troca o SQL executado, pois o runner entrega ao cursor o
texto já validado e preservado em memória. A validação real das garantias do
sistema de arquivos continua reservada ao ambiente descartável.

Nenhum PostgreSQL foi iniciado, nenhum SQL ou migration foi executado e nenhum
banco, `.env` ou serviço externo foi acessado. A implementação aguarda a
H2C.4A.1R5. A H2C.4A.2 continua bloqueada até parecer posterior e autorização
humana expressa.

## Primeira correção pós-integração H2C.4A.2C1

Em **04/08/2026**, foi preservado o parecer histórico **C — H2C.4A.2 não
validada em PostgreSQL efêmero**. A execução anterior em PostgreSQL 15.18
comprovou rollback integral da M0001 e revelou três diferenças de representação
entre o catálogo real e o modelo offline: os `CHECKs` expõem suas colunas por
`conkey`, o deparser varia somente os parênteses externos redundantes dos
`CHECKs`, e `pg_get_indexdef(..., true)` pode omitir `public.` no texto dos seis
índices.

A H2C.4A.2C1 corrigiu somente a leitura e a comparação do catálogo. Os oito
`CHECKs` passaram a declarar suas colunas reais, na ordem de `conkey`; a coleta
preserva os `attnums`, rejeita coluna ausente, removida, inválida ou duplicada e
também compara `convalidated`, `conislocal`, `coninhcount` e `connoinherit`. A
expressão continua tokenizada de forma conservadora e remove apenas o invólucro
`CHECK (...)` e pares externos balanceados que cobrem a expressão inteira.
Operadores, literais, casts, funções, ordem e parênteses internos permanecem
significativos.

Para os índices, a comparação estrutural integral continua sendo a autoridade.
A normalização textual aceita somente a presença ou ausência de `public.` no
nome do índice e da tabela quando os schemas estruturais de ambos já são
exatamente `public`. Outro schema, homônimo, método, chave, predicado, direção,
NULLS, operator class, collation, INCLUDE ou propriedade física diferente
continua rejeitado. A decisão não depende de `search_path`. A revisão posterior
H2C.4A.2C1R demonstrou, porém, que o canonicalizador geral ainda herdava uma
remoção global de `pg_catalog.`, ponto que não estava corretamente descrito
nesta conclusão histórica da C1.

Foram aprovados **277 testes unitários offline**, incluindo uma fixture
sintética derivada do modelo normativo e das divergências observadas no
PostgreSQL 15.18, variantes equivalentes e divergências semânticas. Essa
fixture não constitui fotografia bruta ou integral do catálogo real. O teste
de integração foi importado sem DSN e pulou com segurança. A validação
estrutural permaneceu estrita. M0001, manifesto e relatório histórico não foram
alterados.

Nenhum PostgreSQL, Docker, SQL ou migration foi executado nesta correção. A
H2C.4A.2C1 aguarda revisão técnica offline independente; uma nova execução em
PostgreSQL não está autorizada por esta etapa.

## Segunda correção pós-integração H2C.4A.2C2

Em **04/08/2026**, a revisão H2C.4A.2C1R emitiu parecer **C — não recomendada
nova execução PostgreSQL**. Foram identificados dois achados altos. Primeiro, o
canonicalizador geral eliminava qualquer sequência de tokens `pg_catalog .`,
fazendo, por exemplo, `pg_catalog.lower(a)` parecer igual a `lower(a)`. Segundo,
a fixture de regressão da C1 era derivada de `EXPECTED_LEDGER_SCHEMA` e não
preservava integralmente as linhas brutas de `pg_constraint`, `pg_index`,
operator classes, collations, sequências e objetos.

A C2 removeu a regra global. Qualificadores de schema agora permanecem
significativos em defaults, expressões e predicados de índice, definições de
índice e SQL canonicalizado em geral. A única equivalência fechada é
`pg_catalog.btrim(...)` versus `btrim(...)`, aplicada exclusivamente ao
conteúdo de constraints `CHECK`. Ela exige uma chamada de função e preserva
argumentos, quantidade, ordem, casts, operadores, literais e todos os demais
tokens. `pg_catalog.lower`, outras funções e `btrim` de outros schemas continuam
distintos. A correção anterior de parênteses externos balanceados foi
preservada sem alterar precedência ou parênteses internos.

O modelo físico passou também a preservar separadamente `conkey IS NULL` e
`conkey` vazio. Os oito `CHECKs` normativos continuam exigindo seus respectivos
`attnums`; portanto, ambos os estados incorretos são rejeitados nesses casos.

A fixture usada nos testes foi reclassificada como **sintética, baseada nas
divergências observadas**. A evidência real disponível continua sendo o
relatório histórico da primeira execução e seus resultados parciais. Uma
fotografia bruta integral ainda não existe e não foi reconstruída a partir do
modelo esperado.

Foi preparado, sem ativação automática, um coletor direto de `pg_catalog` e um
serializador JSON UTF-8 determinístico para uma futura coleta controlada. O
contrato inclui identificação sanitizada, constraints, índices, operator
classes, collations, sequências e inventário do schema `public`; preserva OIDs
como evidência não normativa, `None`, arrays vazios, booleanos, inteiros,
strings e ordem semanticamente relevante. O coletor e o serializador não
preenchem dados a partir de `EXPECTED_LEDGER_SCHEMA` e não canonicalizam as
expressões brutas.

O futuro arquivo terá o nome exato
`tests/fixtures/h2c4a2_pg15_18_catalogo_bruto.json`. Sua gravação permanece
desabilitada por padrão e exige simultaneamente DSN administrativa efêmera,
`H2C4A2_CAPTURE_RAW_CATALOG=1`, PostgreSQL 15.18, checksum correto da M0001,
manifesto versão 1, destino confinado e ausência do arquivo ou autorização
explícita de substituição. Esse arquivo **não foi criado**, e nenhuma variável
de captura foi definida nesta etapa.

A suíte final aprovou **290 testes unitários offline**, sem falhas ou erros. As
24 reproduções nominais da C2 também foram aprovadas. O módulo de integração,
importado sem DSN, realizou skip seguro antes de abrir conexão. Sintaxe,
manifesto, checksums e plano offline foram verificados.

Nenhum Docker, PostgreSQL, SQL ou migration foi executado. A captura real e uma
nova execução completa da H2C.4A.2 continuam bloqueadas. A correção aguarda a
revisão offline independente **H2C.4A.2C2R**; somente essa revisão poderá
recomendar a etapa limitada de coleta H2C.4A.2E1.

## Endurecimento da captura H2C.4A.2C3

A revisão independente H2C.4A.2C2R emitiu parecer **C — não recomendada a
coleta controlada de evidência**. A canonicalização foi aprovada, mas a revisão
reproduziu gravação parcial, corrupção da evidência anterior em falha de
sobrescrita, aceitação de `Authorization` e hostname pessoal, inventário do
`public` limitado a `pg_class`, destino implícito e ausência de suporte a
tuplas.

A H2C.4A.2C3 endureceu exclusivamente o mecanismo offline do teste de
integração. O destino passou a ser absoluto e obrigatório. A saída é gerada e
validada em memória, escrita integralmente em temporário exclusivo no mesmo
diretório, sincronizada, relida e validada antes de `os.replace`. Falhas antes
da substituição removem o temporário e preservam o destino. Substituir evidência
existente exige autorização separada, SHA-256 esperado e validação integral do
arquivo anterior.

Metadados e estrutura passaram a usar listas fechadas. Chaves e valores são
inspecionados recursivamente contra credenciais, Authorization/Bearer/Basic,
hosts, usuários, portas e caminhos pessoais. A imagem permitida é exatamente
`postgres:15`, com digest SHA-256 separado, PostgreSQL 15.18, data UTC e ID
técnico. Tuplas são convertidas recursivamente em listas JSON sem confundir
`None`, booleanos ou inteiros.

O inventário bruto do `public` foi separado em categorias obrigatórias para
relações, rotinas, tipos, enums, domínios, triggers, políticas, regras,
extensões, collations, conversões, operadores, operator classes/families e text
search. Cada categoria registra consulta explícita a `pg_catalog`, quantidade e
resultado vazio. Objetos automáticos permanecem visíveis quando sua origem pode
ser comprovada.

A fotografia real continua ausente e a coleta H2C.4A.2E1 não foi autorizada.
A C3 registrou **983 testes**, mas a revisão C3R demonstrou que esse número
somava execuções repetidas, testes focados e reproduções já contidas na suíte;
portanto, ele não representa testes distintos e foi invalidado. Nenhum Docker,
PostgreSQL, SQL ou migration foi executado. A implementação aguardava
**H2C.4A.2C3R — revisão técnica offline independente da captura segura**.

## Correção do ambiente e endurecimento final H2C.4A.2C4

A revisão H2C.4A.2C3R emitiu parecer **C — não recomendada a coleta
controlada de evidência**. O ambiente virtual apontava para uma instalação
Python 3.12.10 removida e nenhum teste da revisão pôde iniciar. O instalador
oficial assinado preservado pelo Windows foi usado em modo de reparo; depois, o
venv `C:\dev\recic4\.venv` foi recriado corretamente fora do repositório e
recebeu somente as versões fixadas em `requirements.txt`. O interpretador final
é Python 3.12.10, com psycopg2 2.9.10 e `pip check` aprovado.

A escrita completa agora aceita exclusivamente retorno cujo tipo exato é
`int`, maior que zero e não superior aos bytes restantes. `None`, booleanos,
zero, negativos, floats, strings e valores excessivos falham de forma
conservadora. Doubles interceptam o próprio `write()` e comprovam escritas de um
ou dois bytes, metade, fragmentos variáveis e bloco completo. Falhas posteriores
a fragmentos removem o temporário. Erros de fechamento e limpeza são anexados
como informação secundária e não substituem erros de write, fsync, validação
ou replace.

O filtro passou a usar NFKC somente em uma cópia destinada à detecção de
segredos. O dado bruto serializado não é modificado. Credenciais, DSNs e
Authorization/Bearer/Basic, inclusive variantes Unicode compatíveis, continuam
rejeitados. SQL legítimo com `password = 'x'`, coluna `token`, `host_id`,
`username_type`, `password_policy` ou `login_attempt` é aceito.

O inventário passou a ser descrito como **cobertura explícita do catálogo
relevante à H2C.4A.2**. A matriz do contrato distingue as 19 categorias
cobertas de extended statistics, atributos/defaults gerais, ACLs, comentários,
security labels, opções de foreign tables e objetos globais fora do escopo. Não
há promessa de dump universal.

A análise AST final encontrou **326 métodos** no módulo offline e **24** no
módulo PostgreSQL, totalizando **350 métodos declarados**, sem geração
dinâmica. A suíte offline executou uma vez 326 testes, sem falhas ou erros. O
módulo PostgreSQL, sem DSN, registrou um skip de classe e abriu zero conexões.
O conjunto focado executou 44 testes; as reproduções executaram 46 testes com
um skip seguro. Esses comandos são evidências separadas e não são somados.

A fotografia real continua ausente e a H2C.4A.2E1 não foi autorizada. M0001,
manifesto e relatório histórico permaneceram inalterados. Nenhum Docker,
PostgreSQL, SQL ou migration foi executado. A correção aguarda
**H2C.4A.2C4R — revisão técnica offline final antes da coleta bruta**.

## Telemetria segura da orquestração H2C.4A.2E1C1

Em **10/08/2026**, a primeira coleta H2C.4A.2E1 recebeu parecer **C — não
concluída com segurança**. O container chegou a `healthy`, mas a falha posterior
foi reduzida pelo wrapper temporário a `controlled E1 failure`. Assim, o estágio
real da falha permanece desconhecido. O container foi removido e nenhuma
fotografia foi produzida. O relatório histórico não foi alterado e não foi
fabricado diagnóstico retroativo.

A E1C1 adicionou ao módulo de integração uma orquestração reutilizável com
operações injetáveis e estágios fechados: prechecks; imagem; criação, isolamento
e health do container; descoberta da porta; credenciais temporárias; conexão;
duas consultas de versão e validação 15.18; banco novo; checksum, leitura,
aplicação e commit da M0001; preparação, coleta, serialização, escrita,
validação e hash da captura; fechamento de conexões; limpeza do ambiente;
remoção e verificações finais do container, porta e volume. O estágio é definido
imediatamente antes de cada callback que pode falhar.

`H2C4A2E1Failure` apresenta somente `H2C4A2_E1_FAILURE`, estágio, categoria,
classe de exceção validada, SQLSTATE de cinco caracteres quando válido,
`errno`/`winerror` inteiros e estados de cleanup. O sanitizador aceita apenas o
contrato fechado. Ele não consulta `str(exc)`, `repr(exc)` ou `exc.args` e não
inclui host, porta, usuário, banco, senha, DSN ou caminho. A exceção original
não é exibida pelo fluxo normal. Erro principal e primeiro erro secundário de
cleanup são mantidos separadamente.

Foram adicionados **17 métodos de teste offline**, com subtests que injetam
falha em todos os 22 estágios de execução e nos 6 estágios de cleanup. Eles
cobrem DSNs e parâmetros de conexão sintéticos, caminhos Windows/macOS/Linux,
SQLSTATE, `errno`, `winerror`, versão, M0001, captura, filesystem, cleanup
completo/parcial/com erro e reprodução do antigo `controlled E1 failure`.
A contagem AST passou a **326 métodos offline + 41 no módulo PostgreSQL**, dos
quais 17 são offline e 24 pertencem à classe de integração. Não há geração
dinâmica. A suíte principal aprovou 326 testes; o módulo PostgreSQL sem DSN
aprovou os 17 testes offline e registrou um skip de classe para os 24 TPGs.

Nenhum Docker, PostgreSQL, SQL, M0001 ou H001–H011 foi executado nesta etapa.
Nenhuma variável de captura foi definida e nenhuma fotografia foi criada.
M0001, manifesto e relatório histórico foram preservados. B2 continua
parcialmente tratado; B1, B3 e B4 permanecem ativos. Uma segunda E1 não está
autorizada. A próxima etapa é somente **H2C.4A.2E1C1R — revisão técnica
offline independente da telemetria segura**.

## Vinculação concreta e contrato de sucesso H2C.4A.2E1C2

A E1C1R recebeu parecer **C — não recomendada nova tentativa da E1**. A
sanitização de falhas foi aprovada, mas três riscos impediam a coleta: não havia
adaptador operacional concreto, callbacks vazios podiam retornar sucesso sem
fotografia e `E1OrchestrationResult.results` podia renderizar valores sensíveis.

A C2 eliminou o dicionário de callbacks da API real e criou a entrada única
`executar_e1_controlada()`. Ela não recebe parâmetros e instancia internamente
`E1RealAdapter` com `E1Config` fechada. O adaptador contém métodos nominais para
imagem Docker, credenciais efêmeras, container isolado, health, porta loopback,
conexão psycopg2, consultas e validação 15.18, banco novo, checksum/leitura/
aplicação/commit da M0001, coleta, serialização, gravação, validação, hash e seis
operações de cleanup. Um mapa interno imutável liga cada estágio ao seu método;
o chamador não pode substituí-lo.

O estado sensível usa `E1RuntimeState(repr=False)` com representação fixa
redigida. Retornos arbitrários foram removidos. O receipt de sucesso contém
somente código, caminho relativo fixo, SHA-256, tamanho, capture ID, horário,
versões, digest, 19 contagens e cleanup. Ele só é construído depois de reler e
validar o arquivo final, recalcular seu hash e confirmar todo o cleanup.

Pós-condições e evidências tipadas impedem `None` de representar sucesso.
Foram criados **23 métodos C2 offline** — seis a mais que os 17 métodos C1 que
foram substituídos — usando `FakeE1Adapter` com a mesma ABC nominal. Eles cobrem
ordem exata, troca de operações, adaptador incompleto, `NotImplementedError`,
15.17/15.19/16.x, fotografia ausente ou inválida, hash divergente, 19 categorias,
receipt, representações seguras, falhas pós-health e cleanup incompleto.

A contagem AST é **326 métodos em C4A1 + 47 no módulo PostgreSQL**, sendo 23 C2
offline e 24 TPGs. Não há geração dinâmica. A suíte de 326 e os 23 testes C2
passaram; sem DSN, os 23 rodaram e a classe dos 24 TPGs teve um skip seguro.

Nenhum Docker, PostgreSQL, SQL, M0001 ou H001–H011 foi executado. A fotografia
real continua ausente. B2 permanece parcial; B1, B3 e B4 continuam ativos. A
segunda E1 não está autorizada. A próxima etapa exclusiva é **H2C.4A.2E1C2R —
revisão técnica offline independente da vinculação concreta da E1**.

## Fechamento do perímetro operacional H2C.4A.2E1C3

A E1C2R recebeu parecer **C — não recomendada nova tentativa da E1**. Os três
bloqueadores eram: receipt operacional alcançável pelo runner genérico com
`FakeE1Adapter`, credenciais no argv de `docker run` e uso efetivo da tag mutável
sem contexto local nem confirmação do image ID. A fotografia canônica também
podia sobreviver a uma falha de cleanup sem marca inequívoca de reprovação.

A C3 introduziu `E1FlowOutcome`, que representa somente conclusão interna do
fluxo e não possui código de sucesso. `_executar_fluxo_e1()` aceita os adapters
nominais para testes, mas jamais constrói `E1SuccessReceipt`. Somente
`executar_e1_controlada()`, com `E1Config` canônica e `E1RealAdapter` de tipo
exato, chama a fábrica operacional protegida por capability interna. A
configuração de teste passou a ser `E1TestConfig`, separada e proibida no destino
real.

Todos os comandos Docker recebem `--context desktop-linux` e ambiente filho
reduzido. O precheck operacional rejeita `DOCKER_HOST` e `DOCKER_CONTEXT`
herdados. A imagem é inspecionada e executada pela referência imutável
`postgres@sha256:74e110c41804365e3915fcc09d5e7a1eff50161aaa94d5da0e58e0cd75ae509c`;
linux/amd64, RepoDigest e image ID são verificados, inclusive depois da criação.
O `docker run` contém apenas `--env POSTGRES_USER`, `--env POSTGRES_PASSWORD` e
`--env POSTGRES_DB`; os valores existem somente no `env=` daquela chamada.

O isolamento rejeita qualquer mount inesperado. A ausência do container usa
consulta exata por nome e somente considera TRUE quando o comando termina com
sucesso e stdout vazio; erro ou saída malformada falham. A tentativa marca a
fotografia efetivamente criada, guarda hash e capture ID internos e remove o
arquivo canônico em qualquer falha posterior, sem tocar em arquivo preexistente.
As evidências críticas possuem invariantes próprias para hash, tamanhos,
categorias, estágios e tipos.

Foram adicionados 22 testes C3 offline, incluindo um fluxo integral pelos métodos
reais de `E1RealAdapter` com apenas subprocesso, conexão, preflight, coleta,
gravação e socket substituídos nas fronteiras. Nenhum Docker, PostgreSQL, SQL,
M0001 ou H001–H011 foi executado e nenhuma fotografia real foi criada. B2 segue
parcial; B1, B3 e B4 permanecem ativos. Uma segunda E1 continua não autorizada;
a próxima etapa exclusiva é **H2C.4A.2E1C3R — revisão técnica offline
independente final do perímetro operacional da segunda E1**.

## Correção mínima dos bloqueadores H2C.4A.2E1C4-FAST

A C3R reproduziu receipt sem fotografia por meio da fábrica separada, inclusive
com outcome manual e adapter não executado. Também encontrou config/adapter
desvinculados, Fake capaz de usar `E1Config` e duplicatas silenciosamente
eliminadas por `dict()`.

A C4-FAST eliminou `_emitir_receipt_operacional`. O único construtor normal do
`E1SuccessReceipt` está dentro de `executar_e1_controlada()`. Antes da construção,
um validador sem poder de emissão exige o mesmo objeto config/adapter, relê o
arquivo canônico, rejeita symlink/ausência/vazio, valida JSON, hash, tamanho,
metadados, categorias, cleanup e o estado de conclusão da M0001/captura/cleanup.
Falha nessa validação remove a fotografia da própria tentativa quando sua
identidade ainda é comprovada e retorna telemetria sanitizada.

`FakeE1Adapter` passou a exigir `E1TestConfig`. Categorias e cleanup agora exigem
comprimento e unicidade exatos tanto no outcome quanto no receipt. Foram
adicionados nove testes FAST; as suítes offline completas permaneceram verdes.
Nenhum Docker, PostgreSQL, SQL, M0001 ou H001–H011 foi executado. Não será criada
C4R; a segunda E1 somente poderá ocorrer após autorização humana expressa. B2
permanece parcial e B1, B3 e B4 continuam ativos.
