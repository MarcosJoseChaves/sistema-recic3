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

## Segurança, retorno e autorizações pendentes

O backup validado da H2D.1 é o ponto de retorno. Nenhum SQL deste plano deve ser
executado no desenvolvimento antes de passar no clone. Exigem nova autorização:
execução de prechecks, aplicação de R0001 no clone, materialização dos ALTERs
classe B, instalação do ledger especial e qualquer adoção.
