# Status do Desenvolvimento

## Branch atual

`codex/modulo-fiscalizacao-contratos`

A branch `main` permanece no mesmo commit de origem e não recebeu alterações.

## Etapa 1 — Base segura

A Etapa 1 criou somente a base do módulo de Fiscalização de Contratos:

- estrutura separada em `modulos/fiscalizacao_contratos/`;
- Flask Blueprint no endereço `/fiscalizacao-contratos`;
- proteção central com o login existente;
- redirecionamento de visitantes para o login;
- bloqueio de usuários comuns com resposta HTTP 403;
- acesso permitido somente para `role == 'admin'`;
- botão no painel administrativo existente;
- página provisória com o padrão visual atual;
- cartões provisórios para Empresas, Contratos, Aditivos e Fiscalizações.

Nenhum cadastro funcional das etapas futuras foi implementado.

## Arquivos criados

- `modulos/__init__.py`
- `modulos/fiscalizacao_contratos/__init__.py`
- `modulos/fiscalizacao_contratos/blueprint.py`
- `modulos/fiscalizacao_contratos/permissions.py`
- `modulos/fiscalizacao_contratos/routes/__init__.py`
- `modulos/fiscalizacao_contratos/static/css/modulo.css`
- `modulos/fiscalizacao_contratos/templates/fiscalizacao_contratos/painel.html`
- `tests/test_fiscalizacao_contratos_etapa1.py`
- `STATUS_DESENVOLVIMENTO.md`

## Arquivos alterados

- `app.py`: somente importa e registra o novo Blueprint.
- `templates/cadastro.html`: somente adiciona o botão do módulo na área administrativa.
- `.gitignore`: passa a ignorar a pasta local `_referencia_fiscaliza/`.

## Testes

Foram executados cinco testes automatizados:

1. visitante é encaminhado ao login existente;
2. usuário comum recebe acesso negado;
3. administrador acessa o módulo;
4. rotas antigas continuam registradas;
5. o sistema importa e cria a aplicação Flask sem erro.

Resultado final: **5 testes passaram e 0 falharam**.

A verificação de sintaxe também passou. Os testes substituem a conexão PostgreSQL por um objeto falso antes de importar `app.py`.

## Banco de dados

O banco de dados não foi acessado nem alterado nesta etapa. Não foram criadas tabelas, executadas migrações ou modificados dados no Neon. A variável `DATABASE_URL` não foi alterada.

## Referência antiga

A pasta `_referencia_fiscaliza/` permanece somente para consulta. Nenhum arquivo dela foi alterado, movido, renomeado ou excluído.

## Etapa 2A — Empresas contratadas

O cadastro de empresas foi implementado no módulo, ainda sem commit e sem aplicar a migração:

- listagem de empresas ativas e opção para mostrar inativas;
- cadastro, visualização e edição;
- inativação sem exclusão do registro;
- reativação;
- validação e normalização de CNPJ, CEP e UF;
- consulta opcional de CNPJ e CEP, mantendo preenchimento manual em caso de falha;
- acesso restrito a administradores;
- serviço de empresas usando a função `conectar_banco` recebida do sistema principal;
- migração idempotente mantida somente como arquivo para revisão.

Foram executados 20 testes automatizados: 15 relacionados à Etapa 2A e 5 testes de regressão da Etapa 1. Resultado: **20 passaram e 0 falharam**. PostgreSQL foi bloqueado por mock e nenhum SQL real foi executado.

A migração `001_criar_fc_empresas.sql` **não foi executada**. O Neon e qualquer outro banco de dados não foram acessados nesta etapa.

O backup `stash@{0}`, identificado como `Backup parcial Etapa 2A antes da restauração`, continua preservado.

## Próxima etapa recomendada

Revisar o código e a migração da Etapa 2A. A aplicação da migração na branch de desenvolvimento do Neon exige nova autorização expressa. Contratos e as demais áreas do módulo não devem ser iniciados antes dessa revisão.

## Como continuar o trabalho

1. Abrir o projeto `C:\sistema-recic3`.
2. Confirmar a branch com `git branch --show-current`.
3. Confirmar que a branch é `codex/modulo-fiscalizacao-contratos` e não `main`.
4. Ler este arquivo e revisar o `git diff` pendente.
5. Não modificar `_referencia_fiscaliza/`.
6. Solicitar autorização explícita antes de iniciar a Etapa 2.
7. Continuar usando o login, os usuários, os papéis e a conexão PostgreSQL existentes no sistema principal.

A Etapa 1 está registrada no commit `c52ecb8488577aa2d859917a003ef19808a42668`. As alterações da Etapa 2A permanecem sem commit, push, pull request ou merge.
