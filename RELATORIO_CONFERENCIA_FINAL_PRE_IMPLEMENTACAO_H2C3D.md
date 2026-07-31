# Etapa H2C.3D — Relatório da conferência final pré-implementação

## 1. Situação, método e limites

**Situação: APROVADA DOCUMENTALMENTE em 31/07/2026.** A conferência foi estática,
comparando H2C.2, H2C.3A–H2C.3C, código, testes, configurações e migrations
001–011. Não houve acesso a banco, dump, `.env`, serviço externo ou dado real;
nenhum SQL, migration, manifesto, executor, bootstrap ou teste PostgreSQL foi
criado ou executado.

## 2. Conferência aprovada e parecer final

**C — NÃO APROVADA PARA IMPLEMENTAÇÃO.** O modelo funcional continua sólido,
mas o congelamento físico ainda não é executável sem decisões técnicas
inventadas. O impedimento é documental e corrigível, não uma rejeição do sistema.

Bloqueios encontrados nesta conferência:

1. o catálogo compacto não individualiza tipo, NULL/default, FK, constraint e
   índice de todas as 753 colunas do núcleo;
2. as matrizes de constraints, índices e DELETE são por família, não por objeto,
   portanto suas contagens e nomes finais não são auditáveis;
3. o hash documentado da migration 001 não corresponde à regra UTF-8/LF;
4. `solicitacao_organizacoes` não consegue oferecer FK real simultânea para
   associação e UVR;
5. `catalogo_eventos.entidade_id` é referência lógica em histórico funcional,
   contrariando a preferência por FKs reais;
6. `patrimonio_fotografias` duplica o vínculo documental patrimonial sem
   finalidade independente suficiente.

Primeira etapa recomendada: uma correção documental H2C.3E, ainda sem SQL, para
expandir o catálogo físico, aplicar as fusões/separações e fechar as matrizes.

### 2.1 Implementação não autorizada

A aprovação desta conferência valida o diagnóstico e os ajustes documentais.
Não autoriza SQL, migration, manifesto real, executor, bootstrap, código, banco,
teste PostgreSQL ou deploy. As correções serão especificadas na H2C.3E.

## 3. Recontagem independente

| Medida | H2C.3C | Conferência H2C.3D | Resultado |
|---|---:|---:|---|
| tabelas totais | 82 | 82 | confirmado após uma fusão e uma separação líquida |
| tabelas novas | 58 | 58 | confirmado após ajuste de composição |
| tabelas `fc_*` | 23 | 23 | confirmado |
| tabelas técnicas de migrations | 2 | 2 | confirmado |
| colunas do núcleo | 753 | 752 | correção líquida de −1 |
| colunas `fc_*` | 351 | 351 | confirmado |
| colunas totais | 1.104 | 1.103 | exige correção documental |
| PKs | não consolidado | 82 | uma por tabela |
| FKs | não consolidado | 239 inferidas | 155 núcleo + 84 FC; núcleo exige expansão nominal |
| UNIQUEs | não consolidado | não auditável | FC: 25 estruturas distintas; núcleo sem matriz nominal |
| CHECKs | não consolidado | não auditável | FC: 103; núcleo sem matriz nominal |
| índices | não consolidado | não auditável | FC: 69 nomes distintos; núcleo sem matriz nominal |
| regras transacionais | famílias | 9 famílias mínimas | falta registro nominal por operação |
| RESTRICT | famílias | não auditável | 84 FKs FC usam NO ACTION padrão; núcleo sem linha por FK |
| CASCADE | zero recomendado | 0 confirmado nas FC | núcleo não fecha exceções de rascunho |
| SET NULL | excepcional | 0 nas FC | núcleo não fecha ocorrências |

Os números de PK e FK são inferíveis. UNIQUE, CHECK, índices e DELETE não podem
ser declarados completos com segurança até a matriz nominal existir. Isso é
**estrutura não inventariada e exige correção**, não simples diferença de escopo.

## 4. Matriz individual das 58 tabelas novas

Legenda: `C` confirmada; `CA` confirmada com ajuste; `F` fundir; `S` separar.
Todas usam BIGINT IDENTITY, salvo indicação. “Hist/estado” indica finalidade
própria; a recomendação não elimina regra funcional.

| # | Tabela | Domínio/finalidade | Colunas | Hist/estado | Incorporação/fragmentação | Classe e recomendação |
|---:|---|---|---:|---|---|---|
| 1 | schema_migrations | sucessos imutáveis | 10 | sim/não | não; baixa | C; manter |
| 2 | schema_migration_execucoes | tentativas/falhas | 14 | sim/sim | não; baixa | CA; fechar constraints |
| 3 | usuario_recuperacoes_senha | tokens com hash | 13 | sim/sim | não; baixa | C |
| 4 | auth_modulos | catálogo de módulos | 9 | não/sim | poderia ser dados em permissão, mas perde gestão | C |
| 5 | auth_acoes | catálogo de ações | 9 | não/sim | idem | C |
| 6 | auth_permissoes | módulo × ação | 9 | não/sim | relação própria | C |
| 7 | auth_perfis | agrupador protegido | 12 | sim/sim | finalidade própria | C |
| 8 | auth_perfil_permissoes | composição do perfil | 6 | sim/não | vínculo necessário | C |
| 9 | auth_usuario_perfis | concessão/revogação | 14 | sim/sim | fato temporal | C |
| 10 | auth_escopos_globais | alcance global | 7 | sim/não | decisão aprovada por tabela distinta | C |
| 11 | auth_escopos_associacao | alcance por associação | 8 | sim/não | FK específica | C |
| 12 | auth_escopos_uvr | alcance por UVR | 8 | sim/não | FK específica | C |
| 13 | fc_contrato_escopos | alcance por contrato | 9 | sim/não | FK específica FC | C |
| 14 | associacoes | organização principal | 14 | sim/sim | entidade própria | C |
| 15 | associacao_aliases | nomes alternativos | 10 | sim/sim | necessário à transição | C |
| 16 | associacao_eventos | histórico organizacional | 11 | sim/não | FK real própria | C |
| 17 | uvrs | unidade operacional | 16 | sim/sim | entidade própria | C |
| 18 | uvr_aliases | nomes alternativos | 10 | sim/sim | necessário à transição | C |
| 19 | uvr_eventos | histórico da UVR | 11 | sim/não | FK real própria | C |
| 20 | associados | cadastro funcional | 25 | sim/sim | entidade própria | C |
| 21 | associado_associacao_vinculos | vínculo temporal/principal | 15 | sim/sim | fonte de verdade | C |
| 22 | associado_uvr_vinculos | vínculo temporal/principal | 15 | sim/sim | fonte de verdade | C |
| 23 | associado_contas_bancarias | contas pessoais por finalidade | 24 | sim/sim | sensível e própria | C |
| 24 | associado_eventos | histórico cadastral | 12 | sim/não | fato próprio | C |
| 25 | associado_conta_documentos | comprovantes bancários | 8 | sim/não | vínculo documental específico | C |
| 26 | documentos_privados | metadados centrais | 19 | sim/sim | núcleo comum | C |
| 27 | naturezas_financeiras | RECEITA/DESPESA | 10 | não/sim | catálogo estrutural | C |
| 28 | unidades_medida | unidades controladas | 11 | não/sim | reutilizada | C |
| 29 | catalogo_grupos | primeiro nível | 12 | sim/sim | entidade própria | C |
| 30 | catalogo_subgrupos | segundo nível | 13 | sim/sim | FK grupo | C |
| 31 | catalogo_itens | produto/serviço | 16 | sim/sim | entidade própria | C |
| 32 | catalogo_aliases | resolução alternativa | 12 | sim/sim | vínculo tipado | CA; fechar namespace |
| 33 | catalogo_substituicoes | sucessão temporal | 11 | sim/sim | fato entre itens | C |
| 34 | catalogo_eventos | histórico do catálogo | 15 final | sim/não | uma tabela aceitável com 3 FKs opcionais exclusivas | CA; substituir ID lógico |
| 35 | contas_financeiras | conta institucional | 17 | sim/sim | distinta de banco pessoal | C |
| 36 | transacoes_financeiras | fato financeiro | 24 | sim/sim | entidade própria | C |
| 37 | transacao_itens | composição/fotografia | 17 | sim/sim | detalhe histórico | C |
| 38 | transacao_rateios_uvr | distribuição por UVR | 14 | sim/sim | regra entre linhas | C |
| 39 | transacao_eventos | cancelamento/estorno | 14 | sim/não | fato histórico | C |
| 40 | patrimonios | identidade/estado do bem | 26 | sim/sim | entidade própria | C |
| 41 | patrimonio_identificadores | placa/Renavam/série | 12 | sim/sim | múltiplos identificadores | C |
| 42 | patrimonio_vinculos | custódia/transferência | 18 | sim/sim | temporal e compartilhável | C |
| 43 | patrimonio_eventos | transições/baixas | 14 | sim/não | histórico próprio | C |
| 44 | patrimonio_documentos | documento, foto e finalidade | 12 final | sim/sim | absorve fotografias | CA; ampliar |
| 45 | patrimonio_fotografias | somente especialização documental | 11 | sim/sim | duplicada | F; fundir em patrimonio_documentos |
| 46 | patrimonio_bloqueios | impedimentos temporais | 13 | sim/sim | regra própria | C |
| 47 | solicitacoes_alteracao | cabeçalho/fluxo | 24 | sim/sim | entidade própria | C |
| 48 | solicitacao_eventos | transições | 13 | sim/não | histórico próprio | C |
| 49 | solicitacao_mensagens | comunicação | 12 | sim/sim | conteúdo próprio | C |
| 50 | solicitacao_aprovacoes | decisões/segregação | 13 | sim/sim | fato próprio | C |
| 51 | solicitacao_aplicacoes | tentativas/idempotência | 15 | sim/sim | histórico técnico funcional | C |
| 52 | solicitacao_documentos | anexos | 9 | sim/sim | vínculo específico | C |
| 53 | solicitacao_associados | integridade do associado | 5 | sim/não | FK específica | C |
| 54 | solicitacao_organizacoes | associação ou UVR ambígua | 5 | sim/não | não oferece uma FK única | S; substituir por duas tabelas |
| 55 | solicitacao_catalogo_itens | integridade do catálogo | 5 | sim/não | FK específica | C |
| 56 | solicitacao_transacoes | integridade financeira | 5 | sim/não | FK específica | C |
| 57 | solicitacao_patrimonios | integridade patrimonial | 5 | sim/não | FK específica | C |
| 58 | auditoria_tecnica | trilha transversal | 22 | sim/não | não é evento de domínio | C |

Após a fusão de `patrimonio_fotografias` e a substituição de
`solicitacao_organizacoes` por `solicitacao_associacoes` e `solicitacao_uvrs`, o
total continua 58 tabelas novas. Nenhuma foi mantida apenas para preservar número.

## 5. Revisão das colunas e ajustes obrigatórios

| Tabela/coluna | Proposta | Problema | Ajuste e impacto | Migration/teste futuro |
|---|---|---|---|---|
| patrimonio_documentos + patrimonio_fotografias | 9 + 11 colunas | dois vínculos ao mesmo arquivo | fundir em 12 colunas com categoria/finalidade/ordem/principal/período; −8 | patrimônio; vínculo e ordem |
| solicitacao_organizacoes.objeto_id | BIGINT | destino associação ou UVR não definido | duas tabelas de 5 colunas; +5 | solicitações; FKs reais |
| catalogo_eventos.entidade_id | lógico | histórico funcional sem FK | grupo_id, subgrupo_id, item_id e CHECK de exatamente uma; +2 | catálogo; integridade |
| associado_*_vinculos.solicitado_por/aprovado_por | não explicitado | nomes não seguem `_usuario_id` | renomear e tipar INTEGER FK | associados; autoria |
| solicitacoes_alteracao.associação_contexto_id | grafia acentuada | nome físico inválido ao padrão | `associacao_contexto_id` | solicitações; convenção |
| listas com `descrição`, `instituição`, `agência`, `observação` | grafia narrativa ambígua | podem ser lidas como nomes físicos | congelar `descricao`, `instituicao`, `agencia`, `observacoes` | todas; inspeção de catálogo |
| catalogo_aliases.entidade_tipo | TEXT + FKs opcionais | integridade parcialmente genérica | CHECK tipo/FK e namespace transacional nominal | catálogo; colisão concorrente |
| snapshots JSONB | limites conceituais | constraints físicas não individualizadas | definir tamanho/versão/allowlist por coluna | solicitações/financeiro; privacidade |
| todas as 753 do núcleo | catálogo compacto | NULL/default/índice/justificativa não individualizados | expandir uma linha por coluna antes de DDL | todas; revisão automatizada |

Contagem final proposta: **752 colunas de núcleo + 351 FC = 1.103**. Campos
derivados (`valor_total`, `medidor_atual`) só podem ser armazenados quando forem
fotografia histórica ou cache com regra explícita; a matriz expandida deverá
classificá-los nominalmente.

## 6. Nomes e abreviações

Permitidas: `assoc`, `usr`, `patr`, `sol_alt`, `exec`, apenas em nomes de
constraints/índices que excederiam 63 bytes. Proibidas: abreviações diferentes
para o mesmo conceito, siglas não documentadas e truncamento automático.

Vocabulário final: `conta_bancaria` para conta pessoal; `conta_financeira` para
conta institucional; `rateio` para distribuição financeira; `documento` para
metadado/vínculo e `arquivo` somente para conteúdo no provedor; `fotografia`
para arquivo visual ou snapshot explicitamente qualificado; `evento` para fato
imutável; `estado` para ciclo controlado; `situacao` somente nas FC históricas;
`usuario` é conta, `associado` é vínculo funcional, `pessoa` permanece fora;
`organizacao` é termo de domínio, mas FKs concretas usam associação ou UVR.

## 7. Identificadores, FKs e ciclos

`usuarios.id` permanece INTEGER; suas FKs, inclusive autoria FC, são INTEGER.
Novas entidades e relações usam BIGINT; FKs entre elas também. As 23 FC mantêm
BIGSERIAL/BIGINT. Request IDs e IDs públicos opacos são UUID.

Foram inferidas 239 FKs: 155 no núcleo e 84 nas FC. As 84 FC são exatas; as 155
precisam da matriz nominal. FKs opcionais justificadas: e-mail não é FK; escopo,
contexto, substituição, documento e referências de correção podem ser nulos
quando o fato continua inteligível. Não se aceita FK genérica.

Ciclos são resolvíveis: usuário aceita autoria inicial nula; auditoria é criada
antes do bootstrap; autorização básica precede organizações e escopos; documento
precede vínculos; catálogo cria itens antes de substituições; transação origem é
self-FK; patrimônio cria entidade antes de vínculos/eventos; solicitação cria
cabeçalho antes de vínculos; usuários precedem 001–011. Nenhum ciclo
incontornável foi encontrado.

## 8. Ledger, manifesto, preflight e lock

O desenho de duas tabelas é implementável. A migration inicial cria ledger e
execuções, autorregistra aplicação e sucesso na mesma transação. Falha não deixa
estrutura parcial e, antes do ledger, existe apenas em log seguro. Falhas
posteriores são gravadas após rollback, em transação separada.

Manifesto JSON versionado é adequado, mas ainda precisa de JSON Schema físico,
política nominal para arquivos extras por diretório e código de erro por falha.
Hash: UTF-8 sem BOM, finais CRLF/CR convertidos para LF, SHA-256 dos bytes do
conteúdo normalizado, hexadecimal minúsculo.

Preflight final: banco novo aceita `public` sem objeto de aplicação e extensões
em allowlist; qualquer tabela, sequência, view, materialized view, função ou tipo
de usuário bloqueia. Banco controlado exige ledger coerente e trata objetos
desconhecidos conforme severidade fechada no futuro. Banco legado/desconhecido
falha sem DDL. Schemas temporários e internos PostgreSQL são ignorados; schema
de usuário inesperado bloqueia. Códigos de saída propostos: 0 sucesso, 20 destino
não vazio, 21 ledger inválido, 22 manifesto inválido, 23 lock indisponível, 24
checksum divergente, 25 migration falhou, 26 validação final falhou.

Lock: chave determinística derivada de `sistema-recic3:baseline:public:v1`, na
mesma conexão durante preflight definitivo, hashes, ledger, migrations, dados
estruturais e validação. Espera máxima 30 s, saída 23, liberação explícita ou por
desconexão e log sanitizado. Teste com dois executores é obrigatório.

## 9. Autenticação, autorização e bootstrap

Usuário tem original/normalizado, estado por CHECK, versão e troca obrigatória.
Recuperação guarda somente hash. Perfis, permissões, atribuições, validade,
revogação e quatro tipos de escopo permanecem separados. Último Administrador
Global e autoelevação exigem lock e segregação. Pai inativo bloqueia nova
atribuição. Duplicidade/período ativo exigem UNIQUE parcial e consulta sob lock.

A aplicação atual não pode iniciar na baseline: depende de `role` e
`uvr_acesso`. O resolvedor novo e os testes são bloqueadores de ativação.

Bootstrap é implementável: comando interativo, entrada oculta e confirmação,
nenhum segredo em argv/env/log, mesmo advisory lock, apenas sem Admin Global,
autor nulo no primeiro usuário, evento `TECNICO/BOOTSTRAP`, troca obrigatória e
recuperação excepcional auditada. Falta definir operacionalmente canal de
recuperação do Admin Global e política de senha; isso é requisito de segurança,
não decisão de schema.

## 10. Organizações, associados e dados bancários

Associações/UVRs, aliases e eventos têm finalidade. Vínculos são fonte única de
principal; uma associação e uma UVR principais ativas; secundária só dentro da
associação; período coerente; sobreposição impedida por lock e consulta, sem
`btree_gist`. CPF vazio vira NULL; exceção sem CPF exige justificativa formal.

Conta bancária tem finalidade protegida, uma principal ativa por finalidade,
período, comprovante e estado. Não há UNIQUE global para PIX/conta. Agência,
conta, PIX, titular e documento são sensíveis, mascarados e proibidos em logs e
JSONB de auditoria; visualizar/exportar exigem permissões próprias.

## 11. Documentos, catálogo e financeiro

Documento central preserva chave privada, hash, MIME, tamanho, versão,
substituição, retenção e inativação. URL temporária não persiste. Download é
auditado. `fc_documentos` continua independente. Fotografia real é documento;
snapshot JSONB é fotografia estruturada, nunca bytes.

Catálogo mantém naturezas, unidades, grupos, subgrupos, itens, aliases,
substituições e eventos. Códigos não se reutilizam; nome/alias compartilham
namespace; produto/serviço e unidade padrão são explícitos. Legado `produtos`
fica fora. `catalogo_eventos` precisa FKs exclusivas para ser história funcional.

Financeiro mantém associação obrigatória, conta institucional, itens,
rateios e eventos. Sem rateio = geral; uma/várias linhas = UVR(s); modo é
exclusivamente percentual ou valor. Conclusão valida soma, associação da UVR,
versão e `ROUND_HALF_UP` sob lock. Concluída é imutável; cancelamento/estorno cria
fato relacionado. Snapshot é versionado e sanitizado.

## 12. Patrimônio e solicitações

Patrimônio final recomendado usa seis tabelas: patrimônio, identificadores,
vínculos, eventos, documentos/fotografias unificados e bloqueios. A fusão reduz
duplicação sem perder categoria, finalidade, ordem, principal ou período. As 38
colunas legadas continuam com destino documentado; foto Base64 exige migração
separada e nenhum valor é descartado automaticamente.

Solicitações mantêm cabeçalho, eventos, mensagens, aprovações, aplicações,
documentos e seis vínculos de objeto: associado, associação, UVR, item de
catálogo, transação e patrimônio. Referência lógica serve busca, não integridade.
Snapshots são versionados/limitados/allowlist; aplicação é idempotente e falha
sanitizada. Nenhuma tabela é criada para domínio não aprovado.

## 13. Auditoria, dados estruturais e segurança

Auditoria é append-only e distingue `USUARIO`, `TECNICO`, `SISTEMA`; BOOTSTRAP é
técnico. Não armazena senha, token, credencial, URL assinada, CPF completo ou
dado bancário integral. Request, objeto lógico, contexto, resultado e JSONB
sanitizado são suficientes. UPDATE/DELETE cotidianos são proibidos. Retenção e
particionamento continuam pendência operacional após medição.

Dados estruturais indispensáveis: módulos, ações, permissões, perfis protegidos,
tipos de ator/escopo, estados essenciais, `RECEITA`, `DESPESA`, `PRODUTO`,
`SERVICO`. Descrição pode evoluir; código é imutável. Nenhum usuário, associação,
UVR, associado, conta, transação, patrimônio, contrato ou documento real entra.

## 14. Constraints, índices e DELETE

A matriz por família identifica as regras corretas, mas não é suficiente para
DDL. A H2C.3E precisa listar nominalmente cada PK, FK, UQ, CHECK, índice parcial,
regra transacional e teste. Nenhuma regra crítica pode ficar só em narrativa.

Nas FC há 23 PKs, 84 FKs, 25 estruturas UNIQUE distintas, 103 CHECKs e 69
índices distintos. Não há `ON DELETE CASCADE` ou `SET NULL`; o padrão é NO
ACTION. No núcleo, fatos/históricos devem ser RESTRICT, inativáveis usam exclusão
lógica, CASCADE só poderá existir em detalhe técnico de rascunho nominalmente
aprovado e SET NULL apenas com snapshot suficiente. Hoje não há contagem fechada
dessas exceções, portanto implementação está bloqueada.

Índices obrigatórios: PK/UQ implícitos, FKs usadas em busca/remoção de pai,
username/e-mail/CPF normalizados, vínculos ativos, hash/chave documental,
filas/objetos de solicitação e request/objeto/ator da auditoria. Índices de
estado isolado ou baixa seletividade são adiáveis até medição. Nenhum índice
deve duplicar PK/UQ.

## 15. Migrations 001–011 e hashes normalizados

| Migration | Hash UTF-8/LF recalculado | Comparação com H2C.3C | Resultado |
|---|---|---|---|
| 001 | `a8a0b4c410b6243c28946927a20567ced0dc67b435d054db24c903e28f26bebc` | documentado `0e4104...` | DIVERGENTE; corrigir documentação futura |
| 002 | `012af8ddd2e04cae607a93e6d09d6d2eddcd1b6cabe7ea751906c7496da72cd5` | igual | confirmado |
| 003 | `815313c2f03402564127ad494fffdf59baeee26e3cd4262ca7106dbaaeca273d` | igual | confirmado |
| 004 | `88961a6d0cbafde065bbd598425035bef0b2a1b9002126b02cb3d006036e821d` | igual | confirmado |
| 005 | `b57f893eced87931922ddaa5c36f51b6ee4e5601cfc95c3616de0a225994c362` | igual | confirmado |
| 006 | `03c059dfdbeaff80ad8f0282956217c5b92f1b911df7de529640d2ec3f16a7ad` | igual | confirmado |
| 007 | `b0d5f732b2aa9234d7cb88a5be6f467ade2a9e19bb82aac596d3e39ef1dd8d19` | igual | confirmado |
| 008 | `f8f88fe1fe9152cdc243db45243701f953b4367def0b3f1ac62d47526e733531` | igual | confirmado |
| 009 | `2e88c765b5981e8edf97616ee656bcb08982c79b953a7f612de2784e6ceffa46` | igual | confirmado |
| 010 | `ac4c38b2beafa4bf7a8b0f614030898e3ac8cbb7ef2b33bcae10b067fae3d34d` | igual | confirmado |
| 011 | `6521ce402bd41c2520901799fa29ae12c205333c23bc65f48b0ebffc84f65089` | igual | confirmado |

Todos os arquivos continuam imutáveis e transacionais. A divergência 001 decorre
de normalização de final de linha, não de alteração da migration. Ordem,
dependências, tabelas, pré/pós-condições e testes permanecem conforme H2C.3C.

## 16. Ordem final conferida

Executor: classificar destino, adquirir lock, validar manifesto/hashes. Depois:
ledger atômico/autorregistro; catálogos mínimos; usuários; autorização básica;
auditoria mínima; organizações; escopos; documentos; associados; catálogo;
financeiro; patrimônio já simplificado; solicitações já separadas; 001–011;
escopo FC; dados complementares; validação final. A ordem é acíclica, mas só
pode ser congelada após as matrizes expandidas.

## 17. Tratamento dos 24 achados

| Achado | Situação final H2C.3D |
|---|---|
| 01, 03, 06, 17 | BLOQUEIA IMPLEMENTAÇÃO; caminho implementável confirmado, código/executor/teste ausentes |
| 02, 04, 05, 07, 11, 12, 13, 16, 23 | EXIGE AJUSTE ANTES DA IMPLEMENTAÇÃO; solução e teste conhecidos |
| 08, 09, 10, 14, 15, 19 | CONFIRMADO PARA IMPLEMENTAÇÃO após expansão física nominal |
| 18, 20, 21 | RISCO RESIDUAL ACEITO |
| 22, 24 | MELHORIA FUTURA dependente de medição |

Nenhum achado dependente de código foi marcado como implementado.

## 18. Matriz final de testes futuros

| Domínio/regra | Tipo | Banco/concorrência | Dados mínimos/resultado | Bloqueador |
|---|---|---|---|---|
| preflight três classes | integração | PostgreSQL/sim | vazio, controlado, estranho; nenhum DDL indevido | 03 |
| ledger/autorregistro | integração | PostgreSQL | falha atômica e sucesso imutável | 03 |
| manifesto/checksum | unidade+integração | arquivos | ausente/extra/hash/ordem rejeitados | 03/17 |
| advisory lock | concorrência | PostgreSQL/sim | dois executores, apenas um aplica | 03 |
| rollback/falha | integração | PostgreSQL | nada parcial; erro sanitizado | 03 |
| 001–011 | integração | PostgreSQL | 23 tabelas equivalentes e hashes LF | 17 |
| bootstrap | segurança/concorrência | PostgreSQL | um Admin, segredo não vaza, troca exigida | 06 |
| autenticação | unidade+integração | PostgreSQL | normalização/estado/recuperação | 01/02 |
| autorização | matriz/objeto | PostgreSQL | mais restritiva, último admin, escopos | 01 |
| associados/vínculos | integração concorrente | PostgreSQL | principal único e sem sobreposição | 10 |
| documentos | segurança | mock+PostgreSQL | hash/MIME/permissão/URL temporária | 15/23 |
| catálogo | concorrência | PostgreSQL | nome/alias sem colisão e FKs evento | 11/12 |
| financeiro | Decimal/concorrência | PostgreSQL | soma, modo, arredondamento e estorno | 13/19 |
| patrimônio | migração/reversão | PostgreSQL | 38 campos, vínculos, foto unificada | 08/15 |
| solicitações | fluxo/idempotência | PostgreSQL | FKs específicas, conflito e rollback | 16 |
| auditoria | segurança/carga | PostgreSQL | append-only e sem dado proibido | 06/23/24 |
| ausência de dados reais | inspeção | não | somente códigos estruturais | todos |

## 19. Riscos residuais e condições para avançar

Riscos: erro na matriz expandida, nomes acima de 63 bytes, colisão de
normalização, crescimento da auditoria, política operacional de retenção e
complexidade dos testes concorrentes. São controláveis, mas não devem ser
transferidos para o implementador sem especificação.

Condições: corrigir hash 001 documentado; aplicar fusão/separação; fechar 1.103
colunas individualmente; nomear e contar constraints/índices/DELETE; congelar
FKs e ordem; revisar os 58 propósitos; definir JSON Schema/erros do manifesto;
aprovar plano PostgreSQL e realizar nova revisão documental.

## 20. Perguntas para validação

**NENHUMA NOVA DECISÃO HUMANA NECESSÁRIA.** Os ajustes decorrem diretamente das
24 decisões já aprovadas. É necessária aprovação humana do parecer C e autorização
de uma etapa documental corretiva, não uma nova escolha funcional.
