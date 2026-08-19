# Contrato da futura fotografia bruta H2C.4A.2

Este diretório ainda **não contém uma fotografia real** do PostgreSQL. O teste
atual usa somente fixtures sintéticas, claramente identificadas, para validar o
formato e o mecanismo de serialização.

O arquivo futuro terá exatamente o nome:

`h2c4a2_pg15_18_catalogo_bruto.json`

Ele somente poderá ser criado por uma etapa autorizada de coleta, usando um
PostgreSQL 15.18 efêmero novo. A coleta deverá ler diretamente `pg_catalog`, sem
consultar `EXPECTED_LEDGER_SCHEMA` ou preencher campos com valores normativos.

## Conteúdo previsto

- identificação sanitizada da captura, versão do PostgreSQL, imagem, digest,
  data, checksum da M0001 e versão do manifesto;
- linhas brutas de `pg_constraint`, inclusive `conkey`, propriedades,
  `pg_get_constraintdef`, `pg_get_expr` e colunas resolvidas separadamente;
- linhas brutas de `pg_index`, inclusive arrays, estados físicos,
  `pg_get_indexdef`, persistência e vínculo com constraint;
- operator classes e collations por posição, incluindo OIDs não normativos e
  ausência explícita de collation;
- sequências `IDENTITY`, dependências, propriedade, parâmetros e persistência;
- inventário bruto dos objetos do schema `public`.

Os OIDs serão preservados apenas como evidência daquela captura. Eles não serão
transformados em expectativa normativa.

## Serialização e ordem

O serializador preserva `None`, arrays vazios, booleanos, inteiros, strings,
parênteses e qualificadores. Tuplas são convertidas recursivamente em listas
JSON; essa decisão é compatível com os arrays devolvidos pelo psycopg2 e não
confunde vazio com `None`. Ele não canonicaliza expressões. A ordem interna de
arrays e listas semanticamente relevantes é mantida. Somente as chaves JSON
são ordenadas para gerar bytes UTF-8 determinísticos. `NaN`, infinito e chaves
JSON duplicadas são rejeitados.

Campo obrigatório ausente produz erro. Senhas, DSNs, tokens, URLs com
credenciais, host ou caminho pessoal não são permitidos.

## Controle futuro

A gravação exigirá simultaneamente:

- `H2C4A2_ADMIN_DSN` entregue apenas em memória;
- `H2C4A2_CAPTURE_RAW_CATALOG=1`;
- PostgreSQL exatamente 15.18, salvo autorização humana posterior diferente;
- checksum normativo da M0001;
- destino absoluto, canônico e explicitamente informado, com o nome exato
  dentro deste diretório real e sem symlinks;
- arquivo ausente ou substituição explicitamente autorizada com o SHA-256
  esperado da evidência anterior.

A fotografia é preparada integralmente em memória e validada antes de qualquer
arquivo temporário. O temporário usa nome imprevisível, criação exclusiva,
permissão restrita quando suportada e o mesmo diretório do destino. A escrita
trata resultados parciais, executa `flush` e `fsync`, relê os bytes, confirma o
SHA-256, o UTF-8, o JSON, a estrutura e a ausência de segredos. Somente depois
ocorre `os.replace`, que preserva o destino anterior até a substituição atômica.
Falhas anteriores ao replace removem o temporário e não criam JSON parcial.

No POSIX, o diretório também recebe `fsync`. No Windows não existe garantia
portátil idêntica para sincronizar diretórios; usam-se criação exclusiva,
revalidação do caminho e `os.replace`, sem afirmar durabilidade absoluta além
do que o sistema operacional oferece. Janelas residuais de troca de caminho
entre chamadas do sistema são mitigadas por verificações repetidas, mas só
podem ser eliminadas integralmente por APIs específicas de cada plataforma.

O filtro percorre chaves e valores, bloqueando DSNs, credenciais, cabeçalhos
Authorization, Bearer/Basic, hosts, usuários, portas e caminhos pessoais. Para
detecção, uma cópia recebe normalização Unicode NFKC; o valor bruto original
nunca é modificado. Chaves técnicas e metadados usam regras fechadas. Textos
brutos de catálogo são avaliados contextualmente: SQL legítimo como
`CHECK (password = 'x')`, `token`, `host_id` ou `username_type` permanece
aceito, enquanto uma DSN, um cabeçalho de autenticação ou uma atribuição de
credencial continua rejeitada. Os metadados permitem somente imagem
`postgres:15`, digest SHA-256, PostgreSQL 15.18, horário UTC e identificador
técnico aleatório.

O inventário do `public` possui seções obrigatórias para relações, rotinas,
tipos, enums, domínios, triggers, políticas, regras, extensões, collations,
conversões, operadores, operator classes/families e objetos de text search.
Objetos automáticos são preservados e marcados quando o catálogo permite
comprovar sua origem. A seção de cobertura registra catálogo, consulta,
quantidade e resultado vazio para cada categoria.

## Matriz explícita de cobertura

O contrato adota **cobertura explícita do catálogo relevante à H2C.4A.2**.
Seu objetivo é demonstrar quais objetos a M0001 criou e registrar objetos
relevantes encontrados em `public`; ele não é um dump universal do PostgreSQL.

| Categoria | Catálogo | Coberta? | Finalidade e razão | Impacto na validação da M0001 |
|---|---|---:|---|---|
| Relações | `pg_catalog.pg_class` | Sim | Detectar tabelas, índices, sequências, views e outros `relkind` em `public` | Identifica objetos esperados e extras |
| Rotinas | `pg_catalog.pg_proc` | Sim | Preservar funções, procedures, aggregates e window functions | Detecta rotinas estranhas à M0001 |
| Tipos | `pg_catalog.pg_type` | Sim | Registrar base, composite, domain, enum, range, multirange, pseudo e arrays | Detecta tipos extras e automáticos |
| Enums | `pg_catalog.pg_enum` | Sim | Preservar labels, ordem e OIDs | Evidencia enums não previstos |
| Domains | `pg_catalog.pg_constraint` | Sim | Preservar constraints e definições brutas de domains | Evidencia domains e regras adicionais |
| Triggers | `pg_catalog.pg_trigger` | Sim | Registrar inclusive triggers internos | Detecta efeitos automáticos relevantes |
| Policies | `pg_catalog.pg_policy` | Sim | Registrar RLS, roles e expressões | Detecta políticas não criadas pela M0001 |
| Rules | `pg_catalog.pg_rewrite` | Sim | Preservar regras e `_RETURN` | Distingue regras automáticas e extras |
| Extensões | `pg_catalog.pg_extension` | Sim, limitada | Registra a linha da extensão cujo `extnamespace` é `public`, não todos os seus membros | Sinaliza extensão no namespace sem prometer enumerar cada objeto membro |
| Collations | `pg_catalog.pg_collation` | Sim | Registrar collations pertencentes a `public` | Detecta customização não prevista |
| Conversions | `pg_catalog.pg_conversion` | Sim | Registrar conversões pertencentes a `public` | Detecta objeto adicional |
| Operators | `pg_catalog.pg_operator` | Sim | Registrar operadores pertencentes a `public` | Detecta objeto adicional |
| Operator classes | `pg_catalog.pg_opclass` | Sim | Registrar classes e métodos | Detecta objeto adicional |
| Operator families | `pg_catalog.pg_opfamily` | Sim | Registrar famílias e métodos | Detecta objeto adicional |
| Text search configurations | `pg_catalog.pg_ts_config` | Sim | Registrar configurações | Detecta objeto adicional |
| Text search mappings | `pg_catalog.pg_ts_config_map` | Sim | Registrar mappings das configurações | Completa a evidência das configurações cobertas |
| Text search dictionaries | `pg_catalog.pg_ts_dict` | Sim | Registrar dicionários | Detecta objeto adicional |
| Text search parsers | `pg_catalog.pg_ts_parser` | Sim | Registrar parsers | Detecta objeto adicional |
| Text search templates | `pg_catalog.pg_ts_template` | Sim | Registrar templates | Detecta objeto adicional |
| Extended statistics | `pg_catalog.pg_statistic_ext` | Não | Fora do contrato atual; a existência da relação-base continua visível | Não prova detalhes de estatísticas estendidas |
| Atributos e defaults gerais | `pg_catalog.pg_attribute` / `pg_catalog.pg_attrdef` | Não, salvo ledger | A assinatura detalhada é coletada separadamente apenas para as tabelas da M0001 | Não descreve colunas de toda relação arbitrária |
| ACLs e ownership | catálogos por objeto | Não | Autorizações pertencem a outra trilha de validação | Não comprova privilégios completos |
| Comentários | `pg_catalog.pg_description` | Não | Metadado documental fora da DDL nuclear validada | Sem impacto na assinatura estrutural atual |
| Security labels | `pg_catalog.pg_seclabel` | Não | Metadado de segurança fora do contrato atual | Deve ser auditado separadamente se exigido |
| Opções de foreign tables | `pg_catalog.pg_foreign_table` e associados | Não | `relkind` de foreign table é visível, mas server/options não são detalhados | Detecta a relação, não sua configuração completa |
| Event triggers | `pg_catalog.pg_event_trigger` | Não | Objeto global, não pertencente a `public` da mesma forma | Fora do escopo da M0001 |
| Publications/subscriptions | catálogos globais de replicação | Não | Objetos globais/externos ao namespace | Fora do escopo da M0001 |

Uma futura ampliação deve acrescentar nova categoria, consulta qualificada,
contrato de campos e testes; categorias ausentes não podem ser apresentadas
silenciosamente como cobertas.

Sem essas condições, nenhum arquivo é escrito. A futura coleta não substituirá
o relatório histórico da primeira execução e não significará aprovação dos 24
TPGs. Depois da captura, o JSON deverá passar por revisão independente antes de
qualquer uso posterior; a coleta não será a validação final da H2C.4A.2.

## Histórico da E1 e telemetria E1C1

Em **10/08/2026**, a primeira H2C.4A.2E1 recebeu parecer **C — não concluída
com segurança**. O único container efêmero chegou a `healthy`, mas uma falha
posterior foi apresentada apenas como `controlled E1 failure`. O estágio exato
ficou desconhecido, o cleanup foi concluído e nenhuma fotografia foi produzida.
Esse fato histórico não foi reconstruído retroativamente.

A H2C.4A.2E1C1 acrescentou um contrato de telemetria somente em memória. Os
estágios são valores fechados e sem host, porta, usuário, banco, senha, DSN,
identificador de container ou caminho pessoal. A falha externa permite apenas
código interno fixo, estágio, categoria fechada, classe de exceção validada,
SQLSTATE válido, `errno`/`winerror` numéricos e estados fechados do cleanup.
Mensagens e argumentos da exceção original nunca entram na apresentação.

O cleanup distingue fechamento de conexões, limpeza do ambiente, pedido de
remoção do container e verificações de ausência do container/volume e liberação
da porta. Resultado não verificável permanece `UNKNOWN`; resultado negativo ou
desconhecido impede sucesso. Uma falha secundária de cleanup não substitui o
erro principal.

A E1C1 foi exercitada apenas com doubles offline. Ela não executou Docker,
PostgreSQL, SQL ou M0001, não habilitou captura e não criou este JSON. Uma
segunda E1 continua **não autorizada**. A próxima etapa é exclusivamente
**H2C.4A.2E1C1R — revisão técnica offline independente da telemetria segura**.

## Vinculação concreta H2C.4A.2E1C2

A revisão E1C1R emitiu **C — não recomendada nova tentativa da E1**. Embora a
exceção sanitizada estivesse segura, o executor recebia callbacks arbitrários,
callbacks vazios podiam declarar sucesso sem fotografia e o resultado positivo
preservava retornos capazes de revelar dados sensíveis.

A E1C2 removeu essa API operacional genérica. A única entrada futura é
`executar_e1_controlada()`, sem argumentos, callbacks ou adaptador fornecido
pelo chamador. Ela instancia `E1RealAdapter`, cujos 28 métodos nominais ligam
explicitamente os 22 estágios de execução e os 6 de cleanup a Docker,
PostgreSQL, versão, M0001, coletor, serializador, gravador e verificações finais.
Importar o módulo continua sem efeitos externos.

O sucesso agora exige evidências tipadas, fotografia regular no destino
autorizado, JSON válido, hash SHA-256 do arquivo final, metadados 15.18, as 19
categorias exatas e cleanup integralmente confirmado. O receipt positivo possui
somente campos fechados e sanitizados; estado interno e configuração usam
representação redigida. Retorno `None`, operação ausente, troca de métodos,
fotografia ausente/inválida ou cleanup `FALSE`/`UNKNOWN` impedem sucesso.

Toda a C2 foi testada com `FakeE1Adapter` nominal e arquivos exclusivamente
temporários. Nenhum Docker, PostgreSQL, SQL ou M0001 foi executado e a fotografia
real continua ausente. Uma segunda E1 permanece **não autorizada**. A próxima
etapa é somente **H2C.4A.2E1C2R — revisão técnica offline independente da
vinculação concreta da E1**.

## Fechamento do perímetro operacional H2C.4A.2E1C3

A revisão E1C2R emitiu **C — não recomendada nova tentativa da E1**. Ela
identificou três bloqueadores: o runner genérico podia emitir receipt usando o
Fake, as credenciais temporárias apareciam no argv do Docker e a tag mutável não
fixava nem comprovava a imagem executada. Também registrou que uma fotografia
canônica podia permanecer depois de falha posterior.

A E1C3 separou o resultado test-only `E1FlowOutcome` do
`E1SuccessReceipt`. O runner compartilhado nunca emite receipt; a conversão
operacional exige `E1Config` canônica, `E1RealAdapter` exato e a entrada pública
sem argumentos `executar_e1_controlada()`. Testes usam `E1TestConfig` confinada
a diretório temporário e não conseguem produzir evidência operacional.

O Docker futuro usa `postgres@sha256:74e110c41804365e3915fcc09d5e7a1eff50161aaa94d5da0e58e0cd75ae509c`,
contexto explícito `desktop-linux`, endpoint npipe local, plataforma linux/amd64
e confirmação do image ID do container. Usuário, senha e banco são passados
somente no ambiente filho controlado do `docker run`; o argv contém apenas os
nomes das variáveis. Mounts inesperados são rejeitados e ausência do container
exige comando Docker bem-sucedido com saída vazia e rigorosamente analisada.

Uma fotografia criada pela tentativa é identificada por caminho, SHA-256 e
capture ID interno. Se qualquer etapa posterior ou cleanup falhar, o arquivo
canônico dessa tentativa é removido e sua ausência é confirmada. Arquivo
preexistente é recusado e preservado. Evidências tipadas agora rejeitam valores
inconsistentes na própria construção.

Toda a C3 foi exercitada offline com arquivos temporários e doubles apenas nas
fronteiras externas. A fotografia real continua ausente. Uma segunda E1 segue
**não autorizada**. A próxima etapa exclusiva é **H2C.4A.2E1C3R — revisão
técnica offline independente final do perímetro operacional da segunda E1**.

## Correção mínima H2C.4A.2E1C4-FAST

A C3R recebeu parecer C porque ainda era possível chamar uma fábrica separada
com outcome sintético, adapter não executado e fotografia inexistente. Também
foram confirmadas combinação entre config/adapter distintos, duplicatas nas
categorias e uso do Fake com configuração operacional.

A C4-FAST removeu a fábrica emissora. Somente `executar_e1_controlada()` constrói
o receipt, depois de reler a fotografia canônica regular, validar JSON, hash,
metadados, 19 categorias únicas, cleanup e estado do mesmo `E1RealAdapter` ligado
à mesma `E1Config`. `FakeE1Adapter` agora aceita exclusivamente `E1TestConfig`;
outcome e receipt rejeitam duplicatas de categorias e cleanup.

Nove testes FAST cobrem os bloqueadores e o caminho positivo sintético em
tempdir. Nenhum Docker, PostgreSQL, SQL ou fotografia real foi executado/criado.
Não há nova C4R: o próximo passo é somente uma decisão humana expressa sobre uma
segunda e única tentativa da E1. Até essa decisão, a execução continua não
autorizada.
