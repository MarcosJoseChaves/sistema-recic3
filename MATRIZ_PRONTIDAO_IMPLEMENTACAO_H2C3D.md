# Matriz de prontidão para implementação — H2C.3D

## 1. Parecer

**APROVADA DOCUMENTALMENTE — C: NÃO APROVADA PARA IMPLEMENTAÇÃO.** Nenhuma implementação foi
realizada. Responsável futuro significa papel técnico, não pessoa identificada.

## 2. Critérios de prontidão

| Critério | Situação/evidência | Bloqueio/ajuste | Responsável futuro | Teste | Decisão humana |
|---|---|---|---|---|---|
| 82 tabelas recontadas | confirmado; composição muda | fundir foto e separar organização | arquitetura | inventário | aprovar parecer |
| finalidade das 58 novas | matriz individual completa | duas alterações | arquitetura/domínio | revisão cruzada | não nova |
| 1.104 colunas | divergente: 1.103 finais | expandir 752 do núcleo | dados | catálogo automatizado | não |
| nomes físicos | parcialmente fechados | corrigir acentos/autorias | dados | limite 63/padrão | não |
| tipos/null/defaults | incompletos por coluna | matriz expandida | dados | lint de especificação | não |
| PKs | 82 inferidas | nomear todas | dados | catálogo PostgreSQL futuro | não |
| FKs | 239 inferidas | nomear 155 do núcleo | dados | tipos/delete/índice | não |
| ciclos | nenhum incontornável | congelar ordem pós-ajuste | arquitetura | instalação vazia | não |
| constraints | FC exatas; núcleo por família | nome/contagem/regra/teste | dados | constraint negativa | não |
| índices | FC 69; núcleo incompleto | matriz nominal/sem redundância | dados/performance | EXPLAIN futuro | não |
| DELETE | FC sem CASCADE/SET NULL; núcleo genérico | linha por FK | dados/domínio | deleção/inativação | não |
| dados estruturais | classes corretas | fechar registros/códigos | domínio | conteúdo determinístico | não |
| manifesto | formato aprovado | JSON Schema e erros | plataforma | parser/arquivos | não |
| ledger | duas tabelas/atomicidade | fechar constraints físicas | plataforma | falha/autorregistro | não |
| preflight | três classes | allowlist/objetos/códigos | plataforma | banco vazio/estranho | não |
| advisory lock | desenho implementável | constante física futura | plataforma | dois processos | não |
| hash 001 | divergente sob LF | corrigir para `a8a0b4...bebc` | plataforma | Windows/Linux | não |
| hashes 002–011 | confirmados | nenhum | plataforma | recomputação | não |
| `role`/`uvr_acesso` | caminho definido | código/testes ausentes | aplicação | matriz comparativa | não |
| bootstrap | caminho definido | comando/testes ausentes | segurança | segredo/corrida | não |
| `fc_*` | 23 tabelas/84 FKs confirmadas | executor/teste ausentes | FC/plataforma | equivalência | não |
| privacidade | regras adequadas | transformar em testes/allowlists | segurança | varredura de segredo | não |
| testabilidade | matriz de 17 áreas | infraestrutura futura | QA/plataforma | PostgreSQL real | não |
| ausência de dados reais | confirmada documentalmente | manter | revisão | inspeção | não |
| autorização executável | não existente | bloqueador | aplicação | integração/objeto | não |
| prontidão final | não atendida | H2C.3E e nova conferência | arquitetura | checklist integral | aprovar parecer |

## 3. Ajustes físicos obrigatórios

| ID | Ajuste | Efeito | Situação |
|---|---|---|---|
| D-01 | fundir `patrimonio_fotografias` em `patrimonio_documentos` | −1 tabela; −8 colunas | APROVADO DOCUMENTALMENTE; corrigir na H2C.3E |
| D-02 | separar `solicitacao_organizacoes` em associação e UVR | +1 tabela; +5 colunas | APROVADO DOCUMENTALMENTE; corrigir na H2C.3E |
| D-03 | trocar ID lógico de `catalogo_eventos` por três FKs exclusivas | +2 colunas | APROVADO DOCUMENTALMENTE; corrigir na H2C.3E |
| D-04 | expandir catálogo das 752 colunas nucleares | permite DDL sem invenção | EXIGE CORREÇÃO NA H2C.3E; BLOQUEIA IMPLEMENTAÇÃO |
| D-05 | produzir matrizes nominais de constraints/índices/DELETE | permite contagem/teste | EXIGE CORREÇÃO NA H2C.3E; BLOQUEIA IMPLEMENTAÇÃO |
| D-06 | corrigir hash normalizado da 001 | manifesto reproduzível | APROVADO DOCUMENTALMENTE; corrigir na H2C.3E |
| D-07 | corrigir nomes físicos ambíguos/acentuados | padrão e automação | EXIGE CORREÇÃO NA H2C.3E |
| D-08 | fechar JSON Schema/códigos de erro do manifesto | executor determinístico | EXIGE CORREÇÃO NA H2C.3E |

## 4. Quatro bloqueadores herdados

| Bloqueador | Caminho implementável | Situação |
|---|---|---|
| autorização sem `role`/`uvr_acesso` | resolvedor novo + comparação transitória | BLOQUEIA código/ativação |
| controle de migrations | preflight + lock + ledger + manifesto | BLOQUEIA executor/testes |
| bootstrap | comando seguro + ator técnico | BLOQUEIA ativação/testes |
| fonte `fc_*` | 001–011 literais + hashes | BLOQUEIA até hash 001 e teste PostgreSQL |

## 5. Tratamento resumido dos 24 achados

| Grupo | Achados | Situação |
|---|---|---|
| bloqueadores | 01, 03, 06, 17 | BLOQUEIA IMPLEMENTAÇÃO |
| ajustes antes de implementar | 02, 04, 05, 07, 11, 12, 13, 16, 23 | EXIGE AJUSTE |
| confirmados após expansão | 08, 09, 10, 14, 15, 19 | CONFIRMADO PARA IMPLEMENTAÇÃO |
| residuais aceitos | 18, 20, 21 | RISCO RESIDUAL ACEITO |
| futuros | 22, 24 | MELHORIA FUTURA |

## 6. Perguntas

**NENHUMA NOVA DECISÃO HUMANA NECESSÁRIA.** O parecer foi aprovado. A próxima
etapa é a correção documental H2C.3E.
