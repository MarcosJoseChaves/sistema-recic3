# Validação em PostgreSQL efêmero — H2C.4A.2

## Resultado executivo

Em 04/08/2026, a H2C.4A.2 foi executada com autorização humana expressa em um
PostgreSQL 15 descartável. O resultado é **C — H2C.4A.2 não validada em
PostgreSQL efêmero**.

A M0001 chegou a executar seu DDL dentro da transação, mas a assinatura física
coletada no PostgreSQL 15.18 divergiu da assinatura normativa codificada. O
runner rejeitou o ledger e realizou rollback. Nenhuma alteração foi feita na
implementação, no manifesto ou na M0001.

## Ambiente e isolamento

- Windows com WSL 2.7.11.0 e backend WSL 2.
- Docker Desktop 4.85.0; Engine 29.6.2, contexto local `desktop-linux`.
- Imagem oficial `postgres:15` para Linux/AMD64.
- Digest: `sha256:74e110c41804365e3915fcc09d5e7a1eff50161aaa94d5da0e58e0cd75ae509c`.
- PostgreSQL `15.18`, `server_version_num = 150018`.
- Container com nome aleatório, remoção automática e porta aleatória vinculada
  exclusivamente a `127.0.0.1`.
- Usuário, banco administrativo, senha forte e DSN gerados somente em memória.
- Nenhum bind mount, arquivo do projeto, `.env`, `DATABASE_URL`, Neon,
  Cloudinary ou dado real foi entregue ao container.

A imagem oficial declara um volume de dados. Na primeira execução integral, o
Docker materializou um volume anônimo mesmo sem opção de volume no comando; o
`--rm` o removeu e a conferência final mostrou zero volumes. O diagnóstico
posterior usou armazenamento `tmpfs`, não persistente. Essa ocorrência impede
afirmar ausência de mount durante toda a primeira execução, embora não tenha
restado volume ou dado após a limpeza.

## Verificações anteriores ao PostgreSQL

- Python funcional: `C:\dev\recic4\.venv\Scripts\python.exe` 3.12.10.
- `psycopg2-binary` 2.9.10 importado corretamente.
- 262 testes offline aprovados, sem falhas ou erros.
- Manifesto válido, com somente M0000 e M0001.
- Checksums válidos.
- Plano: M0000 `EXECUTOR`; M0001 `PENDENTE`.
- Checksum recalculado da M0001:
  `1966113e8d20f4f3aaa2ebc0b6b1f312470ac99835ea97026305c732ab5e0f39`.

## Teste de integração

Foi criado `tests/test_migrations_control_h2c4a2_postgresql.py`, com 24
cenários `unittest`, bancos aleatórios e remoção defensiva. O módulo usa somente
`H2C4A2_ADMIN_DSN`, não conecta durante a importação e é ignorado com mensagem
clara quando a variável está ausente. Nomes de bancos são compostos com
`psycopg2.sql.Identifier`.

Primeira execução integral:

- 24 testes executados em 1,907 segundo pela suíte;
- 9 aprovados;
- 1 falha;
- 14 erros;
- 0 ignorados;
- duração externa total: 2,112 segundos.

A segunda execução não foi realizada porque a primeira demonstrou falha da
implementação. Repetir os mesmos cenários não poderia produzir validação e
contrariaria a regra de parar sem corrigir arquivos de produção.

### Resultado por cenário

| Cenário | Resultado | Evidência resumida |
|---|---|---|
| TPG-001 | Aprovado | PostgreSQL 15.18 e os cinco campos exigidos de `pg_index` presentes. |
| TPG-002 | Aprovado | Banco vazio classificado como `BANCO_NOVO`, sem criar ledger. |
| TPG-003 | Aprovado | Chave normativa, exclusão mútua, timeout e aquisição posterior confirmados. |
| TPG-004 | Bloqueado | DDL executado, assinatura rejeitada e rollback realizado. |
| TPG-005 | Bloqueado | Objetos não permaneceram porque a M0001 foi revertida. |
| TPG-006 | Bloqueado | A assinatura integral real divergiu da assinatura esperada. |
| TPG-007 | Bloqueado | Sequências não puderam ser validadas após o rollback. |
| TPG-008 | Bloqueado | Índices coletados antes do rollback divergiram na forma normativa. |
| TPG-009 | Bloqueado | Autorregistro não ocorreu, pois a validação física falhou antes dele. |
| TPG-010 | Bloqueado | Não houve primeira aplicação válida para testar a segunda execução. |
| TPG-011 | Bloqueado | O runner foi interrompido pela divergência física antes da conclusão. |
| TPG-012 | Aprovado | Função isolada em `public` classificada como `BANCO_DESCONHECIDO`. |
| TPG-013 | Aprovado | Enum isolado classificado como `BANCO_DESCONHECIDO`. |
| TPG-014 | Aprovado | Tabela funcional sem ledger bloqueada; M0001 não executada. |
| TPG-015 | Aprovado | Ledger parcial bloqueado sem correção automática. |
| TPG-016 | Bloqueado | Dependia de uma aplicação inicial válida da M0001. |
| TPG-017 | Teste corrigido | Leitura de catálogo abriu transação; foi acrescentado rollback no teste. Não repetido após o bloqueador real. |
| TPG-018 | Aprovado | Transação ativa rejeitada e trabalho do chamador preservado. |
| TPG-019 | Bloqueado | Dependia da aplicação válida para conferir restauração nos dois modos. |
| TPG-020 | Aprovado | Falha após DDL resultou em rollback integral e lock liberado. |
| TPG-021 | Teste corrigido e bloqueado | Rollback após leitura acrescentado; aplicação posterior ainda depende do bloqueador real. |
| TPG-022 | Bloqueado | Os dois runners receberam `LEDGER_INVALIDO`; nenhuma aplicação parcial. |
| TPG-023 | Bloqueado | Persistência não pôde ser validada após rollback. |
| TPG-024 | Bloqueado parcialmente | Manifesto contém somente M0000/M0001, mas a aplicação final não concluiu. |

## Divergência física encontrada

O coletor real confirmou `indcheckxmin = false`, operator classes e collations
estruturadas. A rejeição ocorreu por três diferenças entre o catálogo real e o
modelo normativo:

1. Os `CHECKs` reais informam as colunas referenciadas em `pg_constraint.conkey`,
   enquanto o esperado codificado registra `colunas=()`.
2. `pg_get_constraintdef` no PostgreSQL 15.18 retornou a expressão dos `CHECKs`
   com um nível de parênteses diferente do esperado.
3. `pg_get_indexdef(..., true)` retornou as definições dos seis índices sem o
   prefixo textual `public.` na tabela, enquanto o esperado exige esse prefixo.

Como a comparação é integral, qualquer uma dessas diferenças torna a assinatura
inválida. O comportamento fail-closed funcionou: houve `LEDGER_INVALIDO`,
rollback e nenhum autorregistro.

## Falhas intermediárias

1. A primeira tentativa de orquestração parou antes do container porque o
   PowerShell não oferecia o método estático inicialmente usado para gerar bytes
   aleatórios. A geração foi substituída por uma API criptográfica compatível.
2. A segunda tentativa criou e removeu o container, mas parou antes dos testes
   devido a duas falhas de aspas/formatação do comando externo. Nenhum volume ou
   container permaneceu.
3. Na primeira suíte real, TPG-017 e TPG-021 revelaram que o próprio teste fazia
   uma leitura de catálogo e deixava a conexão em transação. Somente o novo teste
   foi corrigido; a implementação não foi alterada.
4. A divergência de assinatura foi reproduzida em diagnóstico separado com
   rollback e limpeza.

## Limpeza e segurança

- A variável `H2C4A2_ADMIN_DSN` foi removida ao final de cada tentativa.
- Senha e DSN não foram impressas nem gravadas em arquivo.
- Todos os containers efêmeros foram destruídos e `docker inspect` retornou
  objeto inexistente.
- Zero containers ativos, zero containers parados e zero volumes ao final.
- Nenhuma porta permaneceu vinculada.
- A imagem `postgres:15` foi mantida, conforme autorizado.
- Nenhuma H001–H011, tabela `fc_*`, Flask, Neon, banco real ou Cloudinary foi
  acessado.
- Nenhum arquivo de produção, migration histórica, manifesto ou M0001 foi
  alterado.

## Situação dos bloqueadores

B2 permanece em validação e agora possui uma incompatibilidade física comprovada
em PostgreSQL 15.18. B1, B3 e B4 continuam ativos. Uma correção separada da
infraestrutura e nova revisão independente são necessárias antes de repetir a
H2C.4A.2.
