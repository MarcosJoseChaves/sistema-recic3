# Plano de reconciliação do banco de desenvolvimento — H2D.3

## Estado e decisão

A RECON2 comprovou 2.355 objetos esperados: 718 equivalentes, 1.612 ausentes,
25 divergentes e 649 extras legados. H001–H011 são equivalentes e ficam
preservadas. A estratégia é **C — migration de reconciliação**, aditiva e sem
remoção automática de extras.

## Divergências classificadas

Classe B exige precheck; classe C é tolerável e não gera alteração.

| Migration | Objeto | Diferença relevante | Classe | Ação candidata |
|---|---|---|---:|---|
| M0003 | `usuarios.id` | serial versus identity, mesmo tipo | C | preservar geração existente |
| M0003 | `usuarios.username` | varchar(50) versus text | B | validar e ampliar para text |
| M0003 | `usuarios.password_hash` | varchar(255) versus text; ordem | B | ignorar ordem; validar e ampliar |
| M0003 | `usuarios.nome_completo` | varchar(100) nullable versus text NOT NULL; ordem | B | exigir zero nulos antes do ALTER |
| M0003 | `usuarios.email` | varchar(255) versus text; ordem | B | ignorar ordem; validar e ampliar |
| M0003 | `usuarios_id_seq` | serial-owned versus identity-owned | C | preservar ownership/default funcionais |
| M0009 | `associados.id` | integer serial versus bigint identity | B | validar faixa e FKs; planejar bigint |
| M0009 | `associados.numero` | varchar(20) versus text | B | validar e ampliar |
| M0009 | `associados.nome` | varchar(255) versus text; ordem | B | ignorar ordem; validar e ampliar |
| M0009 | `associados.cpf` | legado NOT NULL; ordem | B | decidir relaxamento após precheck |
| M0009 | `associados.data_nascimento` | legado NOT NULL | B | decidir relaxamento após precheck |
| M0009 | `associados.telefone` | varchar(20) NOT NULL versus text nullable; ordem | B | validar tipo/nullability |
| M0009 | `associados.cep` | legado NOT NULL | B | decidir relaxamento após precheck |
| M0009 | `associados.logradouro` | varchar(255) versus text | B | validar e ampliar |
| M0009 | `associados.endereco_numero` | varchar(20) versus text | B | validar e ampliar |
| M0009 | `associados.bairro` | varchar(100) versus text | B | validar e ampliar |
| M0009 | `associados.cidade` | varchar(100) versus text | B | validar e ampliar |
| M0009 | `associados.uf` | varchar(2) versus char(2) | B | validar conteúdo antes do cast |
| M0009 | `associados.data_admissao` | legado NOT NULL; ordem | B | decidir relaxamento após precheck |
| M0009 | `associados_id_seq` | integer serial versus bigint identity | B | acompanhar alteração do ID/FKs |
| M0011 | `transacoes_financeiras.id` | integer serial versus bigint identity | B | validar faixa e FKs; planejar bigint |
| M0011 | `transacoes_financeiras.numero_documento` | varchar(100) versus text; ordem | B | ignorar ordem; validar e ampliar |
| M0011 | `transacoes_financeiras_id_seq` | integer serial versus bigint identity | B | acompanhar alteração do ID/FKs |
| M0013 | `solicitacoes_alteracao.id` | integer serial versus bigint identity | B | validar faixa e FKs; planejar bigint |
| M0013 | `solicitacoes_alteracao_id_seq` | integer serial versus bigint identity | B | acompanhar alteração do ID/FKs |

Resumo: 23 classe B, 2 classe C, nenhuma classe A ou D entre as divergências.
Ordem física diferente nunca motiva reconstrução de tabela.

## Ausências e ação por migration

| Migration | Tabelas ausentes | Divergências | Ação | Precheck? | Resultado esperado |
|---|---:|---:|---|---:|---|
| M0001 | 2 | 0 | ledger somente após equivalência global | Sim | bootstrap especial comprovado |
| M0002 | 3 | 0 | criar objetos ausentes via R0001 | Sim | M0002 equivalente |
| M0003 | 1 | 6 | reconciliar `usuarios`; depois criar dependente | Sim | M0003 equivalente |
| M0004 | 4 | 0 | criar após M0002/M0003 | Sim | M0004 equivalente |
| M0005 | 1 | 0 | criar após usuários | Sim | M0005 equivalente |
| M0006 | 6 | 0 | criar após usuários/auditoria | Sim | M0006 equivalente |
| M0007 | 3 | 0 | criar após autorização/organizações | Sim | M0007 equivalente |
| M0008 | 1 | 0 | criar após usuários | Sim | M0008 equivalente |
| M0009 | 5 | 14 | reconciliar `associados`; depois criar dependentes | Sim | M0009 equivalente |
| M0010 | 7 | 0 | criar após organizações/associados | Sim | M0010 equivalente |
| M0011 | 4 | 3 | reconciliar transações; depois criar dependentes | Sim | M0011 equivalente |
| M0012 | 6 | 0 | criar após organizações/documentos | Sim | M0012 equivalente |
| M0013 | 11 | 2 | reconciliar solicitações; depois criar dependentes | Sim | M0013 equivalente |
| H001–H011 | 0 | 0 | nenhuma ação | Não | preservadas e candidatas à adoção |

R0001 materializa somente M0002, cuja ausência integral foi comprovada. Os
demais blocos não são copiados parcialmente antes de resolver tipos dos pais e
compatibilidade das FKs. Os 37 objetos extras de nível tabela permanecem fora
de qualquer alvo de remoção.

## Prechecks e próxima prova em clone

`R0001_prechecks.sql` confirma colisões, tipos, nullability, comprimentos, UF,
faixas dos IDs, tipos das FKs e ownership/default das sequences. O próximo clone
deve: restaurar o backup; executar todos os prechecks; aplicar R0001 em uma única
transação; comparar o catálogo; comprovar M0002 equivalente e ausência de
alterações em dados, extras e `fc_*`; então fazer rollback ou descartar o clone.

## R0002 / M0003

R0002 preserva a tabela `usuarios`, sua PK e a geração serial já funcionais.
As quatro divergências classe B são ampliações `VARCHAR` → `TEXT` em
`username`, `password_hash`, `nome_completo` e `email`; apenas
`nome_completo` também recebe `NOT NULL`. P101–P104 verificam nulls,
preenchimento, normalização e colisões antes desses ALTERs. As duas diferenças
classe C (`usuarios.id` serial/identity e ownership de `usuarios_id_seq`) são
toleradas e não geram ALTER.

| Objeto B | Legado | Baseline | Tipo/risco | ALTER condicionado | Precheck | Pós-condição |
|---|---|---|---|---|---|---|
| `usuarios.username` | `VARCHAR(50) NOT NULL` | `TEXT NOT NULL` | ampliação; chave normalizada vazia, inválida ou duplicada | `TYPE TEXT` | P101 | texto preservado e normalização válida/única |
| `usuarios.password_hash` | `VARCHAR(255) NOT NULL` | `TEXT NOT NULL` | ampliação de dado sensível; não admitir null | `TYPE TEXT` | P102 | texto integral e não nulo |
| `usuarios.nome_completo` | `VARCHAR(100)` nullable | `TEXT NOT NULL` | ampliação e endurecimento; null/vazio bloquearia | `TYPE TEXT`, `SET NOT NULL` | P103 | texto não nulo e preenchido |
| `usuarios.email` | `VARCHAR(255)` nullable | `TEXT` nullable | ampliação; normalização pode colidir no índice parcial | `TYPE TEXT` | P104 | texto preservado e emails normalizados sem duplicidade |

As 53 ausências ficam atribuídas em quatro grupos: 26 objetos da tabela nova
`usuario_recuperacoes_senha`; 10 colunas de `usuarios`; 11 constraints e 6
índices de `usuarios`. Desses 53, 51 são materializados e os dois nomes
`pk_usuarios` (constraint/índice) são satisfeitos pela PK legada funcional,
sem renomear ou recriar o extra. As colunas normalizadas usam expressão gerada
temporária protegida pelos prechecks e terminam como colunas regulares.

O teste futuro exige: restaurar legado → executar e validar R0001/M0002 64/64
→ executar P100–P107 → executar R0002 em transação aberta → validar M0003
funcionalmente equivalente, dados, extras e `fc_*` → somente então COMMIT.
Nenhuma diferença de ordem física, objeto M0004+, ledger ou `fc_*` integra R0002.

## R0003 / M0004

M0004 está integralmente `AUSENTE`: 99 objetos, agrupados em 4 tabelas, 41
colunas, 4 sequences identity, 33 constraints e 17 índices. R0003 reproduz
somente esse subconjunto normativo, distribuído entre `auth_permissoes` (24),
`auth_perfis` (28), `auth_perfil_permissoes` (17) e
`auth_usuario_perfis` (30).

As dependências externas são apenas `auth_modulos.id` e `auth_acoes.id`
(M0002, `BIGINT`) e `usuarios.id` (M0003, `INTEGER` com PK funcional). P200–P207
confirmam ausência total, colisões de tabelas/sequences/constraints/índices,
nomes alternativos, existência das PKs pais e compatibilidade dos tipos. Não há
dependência de M0005+.

`auth_usuario_perfis.estado` mantém `DEFAULT 'ATIVA'`, CHECK com
`ATIVA/REVOGADA/EXPIRADA` e unique parcial para `estado = 'ATIVA' AND fim_em IS
NULL`. O teste futuro exige R0001 → M0002 64/64 → R0002 → M0003 60/60 funcional
→ P200–P207 → R0003 transacional → M0004 99/99 antes do COMMIT, preservando
M0002/M0003, dados, extras e `fc_*`.

## R0004 agrupada / M0005-M0008

O FAST-TRACK agrupa operacionalmente M0005-M0008, preservando atribuição e ordem
normativa: M0005 (40 objetos em `auditoria_tecnica`), M0006 (169 objetos em
`associacoes`, `uvrs`, aliases/eventos e duas FKs de `auditoria_tecnica`), M0007
(54 objetos nas três tabelas `auth_escopos_*`) e M0008 (44 objetos em
`documentos_privados`), totalizando 307 objetos integralmente ausentes.

As dependências formam a cadeia M0005 -> M0006 -> M0007 -> M0008 e alcançam
externamente somente objetos já reconciliados até M0004. M0005 cria
`auditoria_tecnica.associacao_id` e `.uvr_id`; as FKs FK-N-025/FK-N-027 ficam em
M0006, depois da criação de `associacoes` e `uvrs`. `componente_sistema` e o
CHECK residual `ck_auditoria_tecnica__regra_273` permanecem ausentes.

P300-P304, P320-P324, P340-P344 e P360-P364 verificam, somente por leitura,
ausência integral, colisões, pais, PKs e tipos. A futura execução deve rodar
todos os prechecks e aplicar as quatro seções numa única transação aberta.
Somente após validar individualmente 40/40, 169/169, 54/54, 44/44 e o total
307/307, além da preservação de M0002-M0004, dados, `fc_*` e extras, é permitido
COMMIT; qualquer falha exige ROLLBACK integral, sem commit intermediário.

## R0005 agrupada / M0009-M0010

R0005 agrupa M0009 e M0010, nesta ordem, para uma futura transação única. A
fonte catalogal aprovada registra M0009 como 196 objetos: 1 equivalente, 181
ausentes e 14 divergentes. Duas diferenças são formalizadas como Classe C
funcional tolerada, exclusivamente no contexto legado para baseline:

- `public.associados.id`: `INTEGER` serial legado versus `BIGINT` identity;
- `public.associados_id_seq`: diferença de capacidade e ownership correlata.

Os IDs observados estão entre 2 e 154, a PK/default/sequence permanecem
funcionais e a aplicação não exige faixa superior a INTEGER. Não existe regra
global `INTEGER = BIGINT`: somente os dois paths declarados em
`R0005_functional_tolerances.json` são tolerados. A capacidade INTEGER é uma
limitação futura conhecida e aceita para esta reconciliação.

EXTRA_LEGADO PRESERVADO POR DECISÃO NORMATIVA. Permanecem sem DROP, ALTER TYPE,
rename ou rebuild as cinco FKs de `auditoria_associados.id_associado`,
`auditoria_rateios.id_associado`,
`auditoria_rateios_transacoes.id_associado`, `epi_entregas.id_associado` e
`epi_entregas.id_responsavel`.

As 12 divergências Classe B de M0009 são:

| Coluna | Legado | Baseline | ALTER | Risco protegido | Precheck | Pós-condição |
|---|---|---|---|---|---|---|
| `numero` | `VARCHAR(20) NOT NULL` | `TEXT NOT NULL` | `TYPE TEXT` | vazio/duplicidade antes de CHECK/UQ | P401 | texto preservado, preenchido e único |
| `nome` | `VARCHAR(255) NOT NULL` | `TEXT NOT NULL` | `TYPE TEXT` | vazio e origem inválida da normalização | P401/P406 | texto preservado e normalizável |
| `cpf` | `VARCHAR(11) NOT NULL` | `VARCHAR(11) NULL` | `DROP NOT NULL` | formato e duplicidade | P402 | nullable, valores existentes válidos |
| `data_nascimento` | `DATE NOT NULL` | `DATE NULL` | `DROP NOT NULL` | coerência das datas existentes | P403 | nullable sem conversão |
| `telefone` | `VARCHAR(20) NOT NULL` | `TEXT NULL` | `TYPE TEXT`, `DROP NOT NULL` | perda/truncamento | P401 | texto nullable preservado |
| `cep` | `VARCHAR(8) NOT NULL` | `VARCHAR(8) NULL` | `DROP NOT NULL` | comprimento | P403 | nullable sem conversão |
| `logradouro` | `VARCHAR(255)` | `TEXT` | `TYPE TEXT` | perda/truncamento | P401 | texto preservado |
| `endereco_numero` | `VARCHAR(20)` | `TEXT` | `TYPE TEXT` | perda/truncamento | P401 | texto preservado |
| `bairro` | `VARCHAR(100)` | `TEXT` | `TYPE TEXT` | perda/truncamento | P401 | texto preservado |
| `cidade` | `VARCHAR(100)` | `TEXT` | `TYPE TEXT` | perda/truncamento | P401 | texto preservado |
| `uf` | `VARCHAR(2)` | `CHAR(2)` | `TYPE CHAR(2)` | truncamento/formato | P404 | valores com dois caracteres |
| `data_admissao` | `DATE NOT NULL` | `DATE NULL` | `DROP NOT NULL` | coerência das datas existentes | P403 | nullable sem conversão |

As 12 colunas ausentes de `associados` ficam classificadas assim:

| Coluna | Classificação | Tratamento |
|---|---|---|
| `nome_normalizado` | C, E | adicionada nullable, backfill determinístico, depois NOT NULL |
| `documento_alternativo` | A | nullable, sem backfill |
| `justificativa_sem_cpf` | A | nullable; CPF legado válido mantém a regra 265 |
| `email` | A | nullable, sem backfill |
| `estado` | B | NOT NULL com default seguro `RASCUNHO` |
| `condicao_regularizacao` | A | nullable, sem backfill |
| `data_desligamento` | A | nullable, sem backfill |
| `criado_em` | B, F | NOT NULL com `CURRENT_TIMESTAMP` |
| `atualizado_em` | B, F | NOT NULL com `CURRENT_TIMESTAMP` |
| `criado_por_usuario_id` | C, D, F | backfill com o usuário técnico, depois NOT NULL/FK |
| `atualizado_por_usuario_id` | C, D, F | backfill com o mesmo usuário, depois NOT NULL/FK |
| `versao_registro` | B | NOT NULL com default seguro `1` |

Somente `nome_normalizado`, `criado_por_usuario_id` e
`atualizado_por_usuario_id` recebem UPDATE. A expressão normativa exata é
`lower(unaccent(btrim(nome)))`; não é permitido substituí-la por `translate`,
regex, função própria, Python ou remoção manual de acentos.

### Dependência `unaccent`

`unaccent` é infraestrutura explícita e controlada da reconciliação. P405 é
somente leitura e classifica o catálogo, resolução, ownership e privilégios:

- E1 — extensão instalada em `public`, `public.unaccent(text)` pertencente à
  extensão, executável e resolvida sem colisão pelo `search_path`: reutilizar;
- E2 — extensão ausente, mas versão disponível, schema `public`, privilégios
  de banco/schema e requisito trusted/superuser atendidos, sem objeto homônimo
  estranho: instalar exatamente uma vez;
- E3 — indisponibilidade, privilégio insuficiente, schema incompatível,
  colisão, estado parcial ou função instalada inválida: bloquear.

O futuro executor H2D.12 é a única camada responsável pela instalação E2 e
deve executar exatamente `CREATE EXTENSION unaccent WITH SCHEMA public;`, sem
`IF NOT EXISTS`. R0005 não contém nem duplica `CREATE EXTENSION`; ela assume que
a função já está operacional quando o backfill é alcançado.

O fluxo futuro executa P400-P405 em transação read-only e a encerra. E3 para.
E1 abre a transação principal sem instalar nada. E2 abre a mesma transação
principal da R0005, instala a extensão e valida imediatamente ownership,
resolução e a expressão normativa. Depois executa P406-P416 e P450-P454, R0005
e toda a pós-validação 417/417. Qualquer falha após `BEGIN` causa ROLLBACK
integral, inclusive da extensão no E2; é proibido commit intermediário.

Objetos pertencentes exclusivamente à extensão não integram M0009/M0010, não
contam nos 417 e são registrados como `INFRAESTRUTURA AUTORIZADA DA
RECONCILIAÇÃO — EXTENSÃO unaccent`, não como EXTRA_LEGADO ou divergência.

A identidade técnica canônica é `migracao_dados_legados`, nome
`Migração de dados legados`, sem e-mail, UVR ou perfil, `estado=BLOQUEADO`,
`ativo=FALSE`, `role=migracao` e hash válido criado a partir de segredo aleatório
descartado. O fluxo atual de login seleciona somente `ativo=TRUE`, portanto essa
conta não autentica. U1 reutiliza exatamente um candidato que satisfaça todos
esses critérios; U2 insere exatamente um quando não há candidato; U3 rejeita
colisão insegura; U4 rejeita múltiplos candidatos. O ID nunca é fixado: vem do
candidato U1 ou de `INSERT ... RETURNING` no U2. Lock de `usuarios` e a UNIQUE
normalizada impedem criação concorrente duplicada, sem `ON CONFLICT` tolerante.

O DML autorizado limita-se a no máximo um INSERT condicional em `usuarios` e
um UPDATE de `associados` nas três colunas novas. No futuro teste, o row count de
`associados` não muda e todas as colunas preexistentes permanecem idênticas.
Em U1, `usuarios` mantém o row count; em U2, cresce exatamente um e somente
`usuarios_id_seq` pode avançar. Usuários preexistentes, demais tabelas, `fc_*` e
extras preservam row count e fingerprint.

P400-P405 cobrem dependências, as 12 B iniciais e classificação E1/E2/E3 sem
invocar a função ausente. P406-P416 cobrem normalização, inventário de colunas,
U1/U2/U3/U4, ausência de perfil, as cinco FKs legadas, PK/sequence e integridade
dos associados e colisões. P450-P454 cobrem ausência, dependências e colisões de M0010. M0010 possui
221 objetos ausentes — 7 tabelas, 90 colunas, 7 sequences, 82 constraints e 35
índices — sem DML próprio, destrutivos ou dependência M0011+.

O alvo H2D.12 é: restore original → R0001 → R0002 → R0003 → R0004 → validar
M0002-M0008 → executar P400-P405 read-only → classificar E1/E2/E3 → encerrar a
transação read-only → abrir a transação principal → em E2 instalar/validar
`unaccent` → executar P406-P416 e P450-P454 → executar R0005 → validar M0009
196/196 funcional, com as duas tolerâncias declaradas, e
M0010 221/221 → validar total 417/417, cinco FKs, dados e fingerprints → COMMIT.
Qualquer falha exige ROLLBACK integral. O harness deve preservar as colunas
antigas de `associados`, admitir somente as três novas preenchidas, admitir
delta 0/+1 em `usuarios` conforme U1/U2 e manter todos os usuários originais.

## Ledger e adoção

O ledger atual só representa `INICIADA`, `APLICADA` e `FALHOU`; portanto não
distingue adoção de execução. Não será criada API de adoção nesta etapa.
A mudança mínima futura precisa:

1. adicionar origem/modo `EXECUTADA` ou `ADOTADA` ao registro aplicado;
2. aceitar `ADOTADA` como evento distinto, sem executar o DDL adotado;
3. exigir conexão IDLE, manifesto/checksums válidos, predecessor e prova
   catalogal completa com hash;
4. rejeitar qualquer ausência/divergência e preservar o executor normal;
5. criar um bootstrap especial de reconciliação, separado do fluxo normal,
   que instale somente M0001 após prova global — o bootstrap atual recusa
   corretamente um schema populado sem ledger;
6. adotar M0002–M0013 e H001–H011 em ordem, sem inserções manuais.

H001–H011 ficam candidatas à adoção somente após equivalência global das M.

## R0006 agrupada / M0011-M0012

R0006 materializa offline M0011 e M0012 para futura validação atômica. M0011
parte da fotografia 184 objetos: 2 equivalentes, 179 ausentes e 3 divergentes.
`public.transacoes_financeiras.id` e
`public.transacoes_financeiras_id_seq` permanecem INTEGER/SERIAL como Classe C
path-specific; a ampliação `numero_documento` VARCHAR(100) para TEXT é física e
não altera valores. As quatro FKs EXTRA_LEGADO continuam intocadas.

As nove colunas novas obrigatórias de `transacoes_financeiras` recebem somente
as fontes autorizadas: `pg_catalog.gen_random_uuid()` nativo do PostgreSQL 17;
associação canônica por `lower(btrim(associacao))` com cardinalidade estrita
igual a 1; natureza RECEITA ou DESPESA; conta técnica da associação; competência
igual a `data_documento` como convenção de migração; valor igual a
`valor_total_documento`; fotografia JSONB versão 1, origem MIGRACAO_LEGADO e
whitelist dos seis campos legados; e o mesmo usuário `migracao_dados_legados`
nos dois campos de auditoria.

`contas_financeiras` possui 17 campos. `instituicao`, `agencia` e `conta` são
dados bancários; `abertura_data` e `encerramento_data` são fatos de negócio;
`criado_em`, `atualizado_em`, `criado_por_usuario_id` e
`atualizado_por_usuario_id` são auditoria técnica; `associacao_id` é FK;
`id`, `codigo`, `nome`, `tipo` e `versao_registro` são estruturais; `estado` é
controle operacional e `observacoes` registra proveniência técnica.

A modalidade MIGRACAO_LEGADO representa exclusivamente o vínculo de transações
históricas sem conta financeira identificável. Existe no máximo uma por
associação necessária, com `codigo=tipo=MIGRACAO_LEGADO`, estado `INATIVA`,
campos bancários e datas de negócio obrigatoriamente NULL, e observação técnica
fixa. Não representa banco, agência, conta, saldo ou datas históricas. As cinco
colunas ficam fisicamente nullable somente porque NOT NULL não é condicional;
CHECKs compensatórios exigem banco preenchido e ambas as datas para qualquer
conta normal, enquanto exigem NULL na modalidade técnica. As nove diferenças
físicas correspondentes são declaradas individualmente em
`R0006_functional_tolerances.json`; não há wildcard nem tolerância da tabela.

Os seeds estruturais usam todos os campos obrigatórios reais de
`naturezas_financeiras`: `codigo`, `nome` e `nome_normalizado`; `descricao`
permanece NULL e `estado`, `protegido`, timestamps e versão usam os defaults
normativos. N1 reutiliza uma correspondência canônica, N2 cria quando ausente,
e N3/N4 bloqueiam colisão ou ambiguidade. Nenhuma terceira natureza é criada.

O DML planejado limita-se a INSERT condicional dos dois seeds, materialização
condicional das associações canônicas ausentes, INSERT condicional de contas
técnicas necessárias e um UPDATE das nove colunas novas. Não há DML em
usuários, aliases, fc_*, EXTRA_LEGADO ou M0012. P500-P520 cobrem todas as
fontes, schemas, colisões, UUID nativo, int4/sequence, FKs, plano determinístico
de associação, seeds, conta técnica, datas, valor, fotografia e estado parcial.
P550-P553 cobrem apenas colisões e dependências úteis de M0012.

### Evidência H2D.15 e decisão P506

O diagnóstico real H2D.15 foi registrado no relatório
`h2d15_p506_associacao_diag_v3_real_20260814_103818.json`, SHA-256
`4349de8b518a13b3e58b1b664ef258a96b12e6e29783696c9c0990a8f7bcb83e`.
No legado bruto, as 756 transações possuem associação preenchida: ACAN aparece
em 135 e aponta exclusivamente para UVR 02; ASCAMAR aparece em 621 e aponta
exclusivamente para UVR 01. As cardinalidades token→UVR são 0/2/0 para
0/1/>1 candidato, cobrindo 756/756 (100%). A evidência foi classificada como
A — FONTE LEGADA INEQUÍVOCA.

P506 passa a validar um plano REUSE/CREATE antes da escrita. A identidade
`LEGACY_ASSOCIATION_IDENTITY` deriva token normalizado, sigla histórica, UVR
legada e quantidade de transações diretamente de `transacoes_financeiras`. A
busca canônica preserva nome e alias e acrescenta `associacoes.codigo`; o mesmo
`associacao_id` encontrado por mais de uma chave é deduplicado. Ausência de
candidato com identidade 1:1 autoriza CREATE; um único ID autoriza REUSE; zero
sem identidade materializável ou mais de um ID bloqueiam. Código divergente,
grafia histórica indeterminável e token→UVR ou UVR→token não 1:1 também
bloqueiam.

Na criação, `associacoes.codigo` e `associacoes.nome` preservam a própria sigla
histórica, sem expansão inferida; `nome_normalizado` recebe o token. ACAN e
ASCAMAR não são regras hardcoded e não geram aliases no snapshot aprovado. A
associação nasce em `EM_IMPLANTACAO` e `inicio_data=CURRENT_DATE` registra
somente o início operacional da materialização canônica; não representa nem
infere a fundação histórica da instituição. Os campos de auditoria reutilizam o
único usuário técnico `migracao_dados_legados` já criado por R0005.

A R0006 removeu `min(associacao_id)` e usa somente o conjunto deduplicado cuja
cardinalidade foi comprovada igual a 1, repetindo defensivamente essa prova na
DML. As metas permanecem M0011 184/184, M0012 193/193 e total 377/377, sem
alterar contagens ou tolerâncias.

À época da revisão H2D.16, a atividade era somente materialização offline: a
R0006 ainda não havia sido validada em PostgreSQL e 377/377 permanecia como
meta, situação posteriormente encerrada pela H2D.17-RETRY2 descrita abaixo.

M0012 é agrupada integralmente, sem M0013 e sem DML: 193 objetos distribuídos
em `patrimonio_bloqueios` 28, `patrimonio_documentos` 28,
`patrimonio_eventos` 25, `patrimonio_identificadores` 29,
`patrimonio_vinculos` 34 e `patrimonios` 49. À época do planejamento, o alvo
futuro era M0011 184/184, M0012 193/193 e total 377/377.

O protocolo aprovado exigia uma única transação depois de R0001-R0005 e dos
prechecks, mantendo-a aberta durante a comparação de M0011/M0012 e a regressão
M0002-M0010; qualquer falha exigia ROLLBACK integral. O COMMIT no clone somente
poderia ocorrer depois de 377/377 e da preservação de dados, extras e `fc_*`.

### Encerramento H2D.17-RETRY2 — R0006 validada

A H2D.17-RETRY2 validou a R0006 em clone efêmero local PostgreSQL 17.10
(`server_version_num=170010`). A evidência normativa aprovada é o relatório
`h2d17_r0006_377_real_20260814_121234.json`, SHA-256
`2ad055a51b2760ae434e891b947c27d14efc80c98e328b6adbfda519de718dc4`.

A cadeia precedente encerrou R0005 em 417/417 PASS: M0009 196/196 funcional e
M0010 221/221. Os prechecks P500-P520 e P550-P553 passaram integralmente. P506
registrou `sem_plano=0`, `ambíguas=0`, `CREATE=2`, `REUSE=0` e cobertura
planejada 756/756.

O teste negativo da conta normal com `instituicao=NULL` passou com erro
PostgreSQL estruturado SQLSTATE 23514, tabela `contas_financeiras` e constraint
`ck_contas_financeiras__instituicao_preenchido`. A materialização efetiva teve
2 CREATE e 0 REUSE. A cardinalidade final 0/1/>1 foi 0/2/0 tokens; 756/756
transações foram resolvidas, sem não resolvidas, aliases criados ou códigos
divergentes.

O comparador funcional confirmou M0011 184/184 e M0012 193/193, total R0006
377/377. O COMMIT atômico ocorreu somente no clone após todas as validações, e
a confirmação read-only pós-COMMIT passou. A aprovação comprova também a Classe
C path-specific: `public.transacoes_financeiras.id` permaneceu INTEGER/SERIAL e
`public.transacoes_financeiras_id_seq` permaneceu no caminho legado autorizado.
As quatro FKs EXTRA_LEGADO — `auditoria_rateios_transacoes.id_transacao`,
`documentos.id_transacao_origem`,
`fluxo_caixa_transacoes_link.id_transacao_financeira` e
`itens_transacao.id_transacao` — permaneceram 4/4 preservadas. Nenhuma
equivalência global INTEGER/BIGINT ou int4/int8 foi introduzida.

A execução permaneceu isolada: Neon e DEV não foram acessados. O cleanup passou,
com container removido, volumes persistentes 0→0, zero bind mounts e zero portas
persistentes. Esta aprovação vale exclusivamente para a validação efêmera da
R0006; não declara execução em DEV/Neon, adoção de ledger, validação de M0013,
homologação global do projeto ou deploy.

## Segurança, retorno e autorizações pendentes

## H2D.21 — R0007 / M0013 materializada offline

A H2D.21 confirma o universo M0013 em 272 objetos: 1 tabela equivalente,
269 objetos ausentes e 2 divergências. R0007 trata individualmente os 269:
267 são materializados e a PK/índice backing já funcionais são preservados com
equivalência nominal path-specific. As 11 tabelas filhas permanecem normativas,
inclusive com `solicitacao_id BIGINT`.

`public.solicitacoes_alteracao.id` e
`public.solicitacoes_alteracao_id_seq` são as únicas tolerâncias Classe C:
INTEGER/SERIAL, PK, default e ownership são preservados. Não existe equivalência
global int4/int8. A constraint/índice `solicitacoes_alteracao_pkey` permanece a
única PK, sem rename, segunda PK ou rebuild.

Os oito campos EXTRA_LEGADO permanecem integralmente preservados. O backfill usa
`tipo_solicitacao` e `tabela_alvo` literalmente após `btrim`, mapeia apenas
APROVADO→APLICADA e REJEITADO→REJEITADA e bloqueia qualquer domínio diferente.
`LEGADO` é sentinela técnica, não módulo histórico conhecido;
`LEGADO_NAO_CLASSIFICADO` não atribui risco real; `versao_esperada=0` significa
“versão histórica do objeto não registrada”. `fotografia_proposta` preserva
`dados_novos`; nenhuma fotografia histórica ausente é inventada.

`criada_em` usa a convenção técnica UTC sobre o `TIMESTAMP` legado, sem afirmar
o timezone histórico e sem alterar `data_solicitacao`. `atualizado_em` recebe um
timestamp técnico único da transação e não é data histórica da solicitação.
UUIDs são identificadores técnicos novos.

O solicitante é resolvido em três níveis estritos: igualdade exata, identidade
canônica M0003/R0002 `lower(btrim)` somente na ausência do exato, e ator histórico
bloqueado somente quando ambos são zero. O ator não representa pessoa civil,
fica `BLOQUEADO`, `ativo=FALSE`, sem e-mail, perfil, permissão, escopo, UVR ou
associação. Cada solicitação deve terminar com cardinalidade real 1:1.

P700–P713 cobrem estrutura legado, PK/sequence, faixa int4, FKs, colisões,
domínios, fontes NOT NULL, resolução de usuários, ator bloqueado, pais e UUID.
A meta funcional da futura H2D.22 é M0013 272/272, preservação dos 358 registros
do backup aprovado e COMMIT somente após todas as provas. Nesta etapa R0007 está
somente materializada/validada offline; não foi executada em DEV ou Neon.

O backup validado da H2D.1 é o ponto de retorno. Nenhum SQL deste plano deve ser
executado no desenvolvimento antes de passar no clone. Exigem nova autorização:
execução de prechecks, aplicação de R0001 no clone, materialização dos ALTERs
classe B, instalação do ledger especial e qualquer adoção.
