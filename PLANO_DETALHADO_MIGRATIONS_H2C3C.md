# Etapa H2C.3C — Plano detalhado aprovado das migrations

## 1. Situação

**APROVADO DOCUMENTALMENTE em 31/07/2026.** Parecer: **APTA PARA CONGELAMENTO
DOCUMENTAL E CONFERÊNCIA FINAL PRÉ-IMPLEMENTAÇÃO**. Nenhum manifesto, executor, migration,
SQL, tabela ou bootstrap foi criado. Este plano descreve o comportamento futuro
sem conter comandos executáveis.

## 2. Preflight de banco vazio

Antes de qualquer DDL, o executor futuro deverá conectar somente ao destino
explicitamente autorizado e distinguir três situações. **Instalação nova:** sem
ledger válido; qualquer objeto de aplicação em `public` bloqueia, objetos
internos são ignorados e extensões só são aceitas se reconhecidas. **Banco
controlado:** ledger válido; conferir ordem, hashes e pendências, sem repetir o
teste cego de banco vazio. **Banco legado ou desconhecido:** falhar ou exigir
tratamento expresso; nunca adaptar silenciosamente. Na dúvida, falhar antes do
primeiro DDL.

Não haverá DDL antes do resultado. Falha retorna código de saída não zero,
mensagem “destino não está vazio ou não pôde ser comprovado como vazio” e log
sanitizado com request ID, sem conexão, SQL, objeto sensível ou credencial.

## 3. Advisory lock

Chave proposta: hash estável e documentado de `sistema-recic3:baseline:public:v1`,
convertido deterministicamente no par aceito pelo PostgreSQL. Aquisição ocorre
depois do preflight somente-leitura inicial e antes de validar/aplicar o
manifesto. Recomenda-se espera máxima configurável de 30 segundos; expirada,
falha sem DDL. Lock de sessão é liberado explicitamente e também na desconexão.
Teste futuro inicia dois executores e prova que apenas um prossegue. Valor nunca
é aleatório.

## 4. Manifesto conceitual

Formato recomendado: **JSON UTF-8 versionado**, por usar parser padrão seguro,
ordem explícita e nenhum código executável. O arquivo real não foi criado.

Campos raiz: `formato_versao`, `sistema`, `schema_destino`, `hash_algoritmo`,
`migrations`. Cada entrada: `id`, `modulo`, `versao`, `ordem`, `caminho`,
`checksum_sha256`, `dependencias`, `transacional`, `tipo`, `descricao`, `imutavel`,
`dados_estruturais`, `testes_exigidos`.

ID proposto: `<modulo>:<ordem de 4 dígitos>:<slug>`. Versão inteira positiva;
ordem global inteira única. O checksum é SHA-256 hexadecimal minúsculo de 64
caracteres, calculado somente sobre os bytes do conteúdo da migration em UTF-8,
com finais de linha previamente normalizados como LF, sem caminho, data ou
metadado externo. Validação
falha para arquivo ausente, arquivo extra no diretório oficial, ID/ordem/caminho
duplicado, dependência ausente/cíclica/posterior, hash divergente, campo
desconhecido, caminho fora da raiz ou tipo não permitido. JSON Schema futuro
deverá rejeitar propriedades extras.

## 5. Ledger e histórico de execuções

Recomendação: alternativa B, duas tabelas. `schema_migrations` registra apenas
sucessos e é imutável; `schema_migration_execucoes` registra cada tentativa,
inclusive falha sanitizada. Estrutura completa está na especificação física.

Protocolo inicial proposto:

1. preflight sem DDL;
2. advisory lock;
3. carregar e validar todo o manifesto e todos os hashes;
4. aplicar em uma única transação o arquivo futuro que cria as duas tabelas,
   registra a própria migration aplicada e registra sua execução bem-sucedida;
5. confirmar essa transação; em falha, nada do ledger ou autorregistro permanece
   e o erro existe somente no log seguro;
6. iniciar as demais migrations;
7. para cada entrada restante, registrar início, executar em transação própria,
   confirmar e só então registrar sucesso;
8. em erro, rollback integral e registro sanitizado em transação separada;
9. validação final; liberação do lock.

Falha anterior ao ledger existe somente no log seguro. Erro persistido contém
código controlado e resumo sem SQL, parâmetros, credencial ou dado pessoal.

## 6. Ordem exata planejada

| Ordem/ID conceitual | Módulo e objetivo | Estruturas | Dados estruturais | Transação/rollback | Teste/bloqueador |
|---:|---|---|---|---|---|
| 0000 | executor: preflight/lock | nenhuma | não | sem DDL | banco estranho/concorrência |
| 0001 `nucleo:0001:ledger` | controle | 2 tabelas ledger | tipos de situação embutidos | própria/rollback total | B2/H2C3B-03 |
| 0002 `nucleo:0002:tipos` | catálogos técnicos mínimos | naturezas, módulos, ações | códigos indispensáveis | própria | códigos/checksum |
| 0003 `nucleo:0003:usuarios` | autenticação | usuarios, recuperação | nenhum usuário | própria | B3/bootstrap sem usuário fictício |
| 0004 `nucleo:0004:autorizacao` | autorização básica | permissões, perfis, vínculos básicos | perfis protegidos/permissões | própria | B1/último admin |
| 0005 `nucleo:0005:auditoria` | auditoria mínima | auditoria_tecnica | tipos de ator/resultado | própria | BOOTSTRAP/append-only |
| 0006 `nucleo:0006:organizacoes` | associações/UVRs | 6 tabelas | estados/tipos | própria | aliases/períodos |
| 0007 `nucleo:0007:escopos` | escopos organizacionais | 3 tabelas auth | tipos de escopo | própria | FKs/negação segura |
| 0008 `nucleo:0008:documentos` | documento privado | documentos_privados | provedores/estados técnicos | própria | hash/privacidade |
| 0009 `nucleo:0009:associados` | associados/vínculos/bancos | 6 tabelas | estados/finalidades | própria | CPF/períodos/dados sensíveis |
| 0010 `nucleo:0010:catalogo` | catálogo | 6 tabelas restantes | PRODUTO/SERVICO/unidades mínimas | própria | namespace/aliases |
| 0011 `nucleo:0011:financeiro` | contas/transações/rateios | 5 tabelas | estados/eventos | própria | Decimal/soma/lock |
| 0012 `nucleo:0012:patrimonio` | patrimônio | 7 tabelas | estados/condições/eventos | própria | 38 campos/reversão |
| 0013 `nucleo:0013:solicitacoes` | solicitações | 11 tabelas | estados/riscos/eventos | própria | idempotência/segregação |
| 0014–0024 | Fiscalização histórica | arquivos 001–011 literais | somente CHECKs históricos | uma por arquivo | B4/hashes/equivalência |
| 0025 `nucleo:0025:fc_escopos` | escopo contratual | fc_contrato_escopos | nenhum usuário | própria | autorização de objeto |
| 0026 `nucleo:0026:dados_complementares` | completar dados estruturais | somente catálogos aprovados | lista seção 8 | própria | conteúdo exato/idempotência estrita |
| 0027 `nucleo:0027:validacao` | validação declarativa | nenhum novo objeto | não | somente leitura futura | contagens/FKs/checksums |

A ordem antecipa documentos aos vínculos, separa autorização básica de escopos
organizacionais e mantém 001–011 imutáveis. Se uma dependência física exigir
ajuste, o plano volta à validação; não se improvisa durante implementação.

## 7. As migrations históricas `fc_*`

As onze entradas usarão caminhos atuais e hashes documentados na especificação
física. São transacionais e imutáveis. O manifesto registra cada arquivo, não um
snapshot consolidado. Pré-condições: `usuarios` compatível e dependências FC já
criadas em ordem. Pós-condições: tabelas/colunas/constraints/índices conferidos
contra inventário estático. O índice repetido 005/006 é aceito como idempotente.

## 8. Dados estruturais permitidos

| Código/classe | Domínio/finalidade | Etapa | Proteção/teste |
|---|---|---|---|
| `NUCLEO`, `FC`, módulos aprovados | autorização | 0002/0004 | código imutável; descrição editável |
| ações `CONSULTAR`, `CRIAR`, `EDITAR`, `INATIVAR`, `REATIVAR`, `APROVAR`, `EXPORTAR`, `ADMINISTRAR` | permissões | 0002/0004 | conjunto revisado/teste matriz |
| `ADMIN_GLOBAL` e perfis institucionais aprovados | autorização | 0004 | protegidos; sem atribuição a usuário |
| `USUARIO`, `TECNICO`, `SISTEMA`, identificador `BOOTSTRAP` | auditoria | 0005 | CHECK; sem conta/senha |
| estados organizacionais/vínculos | organização | 0006 | códigos imutáveis |
| `RECEITA`, `DESPESA` | financeiro | 0002 | protegidos |
| `PRODUTO`, `SERVICO` | catálogo | 0010 | protegidos |
| estados/riscos/eventos aprovados | domínios | junto à dependência | sem dados reais |
| catálogos exigidos pelas 001–011 | FC | nos próprios CHECKs | arquivos imutáveis |

Não entram usuário, associação, UVR, associado, conta, transação, patrimônio,
contrato, documento, e-mail, CPF, segredo ou qualquer dado real. Conteúdo com
mesmo código e descrição diferente é divergência, não sucesso idempotente.

## 9. Transações, falhas e rollback

Cada migration é transacional por padrão. Exceção só será aceita se o manifesto
declarar `transacional=false`, justificativa, pré-condições, procedimento de
recuperação e testes próprios. Sucesso é registrado após commit. Falha faz rollback e
registra execução em transação separada. Nunca existe continuação automática
após divergência, dependência ausente ou checksum incorreto.

## 10. Plano obrigatório de testes futuros

- parser/Schema do manifesto e arquivos extras/ausentes/duplicados;
- checksums e imutabilidade;
- banco vazio versus objeto estranho, schema vazio e extensão instalada;
- dois executores concorrentes e liberação por desconexão;
- falha antes/depois do ledger, rollback e erro sanitizado;
- bootstrap interativo, segredo fora de argv/env/log, corrida e último admin;
- tipos exatos de todas as FKs; nomes ≤63 bytes;
- constraints, índices e políticas de DELETE;
- normalização e colisão concorrente de nome/alias;
- períodos concorrentes, principais únicos e rateios/ROUND_HALF_UP;
- autorização nova, comparação legada e negação por valor desconhecido;
- instalação PostgreSQL real do zero e segunda execução segura;
- equivalência das 23 `fc_*` com 001–011;
- auditoria sem segredo/dado proibido; restauração e rollback ensaiados.

Testes PostgreSQL reais continuam obrigatórios, mas não foram criados nem
executados nesta etapa.

## 11. Tratamento dos bloqueadores

1. `role`/`uvr_acesso`: contrato duplo documentado; ainda bloqueia até novo
   resolvedor e testes existirem.
2. ledger: duas tabelas e protocolo inicial definidos; ainda bloqueia até
   migration/executor/teste existirem.
3. BOOTSTRAP: ator técnico, autoria excepcional e auditoria definidos; ainda
   bloqueia até comando seguro/testes existirem.
4. `fc_*`: única fonte executável e hashes definidos; ainda bloqueia até
   manifesto/executor/equivalência existirem.

## 12. Contrato de prontidão

Somente após aprovação das 24 perguntas, congelamento do desenho, revisão das
matrizes, conferência dos hashes, aprovação do plano de integração PostgreSQL e
nova autorização expressa poderá ser proposta a criação de arquivos
executáveis. H2C.3C não concede essa autorização.

## 13. Decisões relacionadas

As 24 perguntas, alternativas, recomendações, impactos e riscos aprovados estão na seção 11 de
`ESPECIFICACAO_FISICA_SCHEMA_BASELINE_H2C3C.md`. Este plano depende das respostas
1–5, 7–9, 12 e 14–24. A aprovação congela documentalmente as decisões, mas não
autoriza implementação. A próxima etapa é a H2C.3D.
