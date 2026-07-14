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

A Etapa 2A foi concluída. O cadastro de empresas foi implementado no módulo e registrado no commit `02231cdc3dfc1e74a029483c72e46d78bc2cf142`:

- listagem de empresas ativas e opção para mostrar inativas;
- cadastro, visualização e edição;
- inativação sem exclusão do registro;
- reativação;
- validação e normalização de CNPJ, CEP e UF;
- consulta opcional de CNPJ e CEP, mantendo preenchimento manual em caso de falha;
- consulta de CNPJ pela BrasilAPI, com OpenCNPJA como alternativa automática;
- acesso restrito a administradores;
- serviço de empresas usando a função `conectar_banco` recebida do sistema principal;
- migração idempotente revisada e aplicada no ambiente Neon configurado no `.env`.

Foram executados 20 testes automatizados: 15 relacionados à Etapa 2A e 5 testes de regressão da Etapa 1. Resultado: **20 passaram e 0 falharam**. PostgreSQL foi bloqueado por mock e nenhum SQL real foi executado.

Após a correção da consulta externa, foram executados **29 testes automatizados**, incluindo os 20 testes anteriores e 9 testes específicos das APIs simuladas. Resultado: **29 passaram e 0 falharam**.

Os testes manuais da Etapa 2A também foram concluídos com sucesso. Foram validados:

- consulta de CNPJ e preenchimento automático;
- cadastro e visualização dos detalhes da empresa;
- edição;
- inativação e exibição de empresas inativas;
- reativação.

A tabela `fc_empresas` está funcionando corretamente com o cadastro de empresas.

A migração `001_criar_fc_empresas.sql` foi aplicada em **14/07/2026**, usando a conexão Neon atualmente configurada no `.env`, conforme autorização expressa. Nenhuma credencial, URL ou hostname foi registrado neste documento.

Verificações realizadas após a aplicação:

- a tabela `fc_empresas` existe;
- as 17 colunas esperadas foram conferidas;
- chave primária, CNPJ único, validações de CNPJ/CEP/UF e chaves estrangeiras para `usuarios.id` foram conferidas;
- o índice `idx_fc_empresas_ativo_razao_social` existe;
- a tabela foi criada vazia, sem cadastro de empresas;
- o catálogo das demais tabelas permaneceu idêntico antes e depois;
- nenhum dado existente foi apagado ou alterado;
- nenhuma outra migração foi executada.

O backup temporário antigo da Etapa 2A foi revisado e confirmado como contendo somente arquivos parciais que já foram recuperados, concluídos e salvos nos commits atuais.

## Etapa 2B — Servidores e responsáveis

A Etapa 2B foi concluída. O cadastro próprio de servidores e responsáveis foi implementado dentro do módulo e registrado no commit `c0ea8c3c4b0a43164b551efcb9f50eb00f9bfe24`:

- listagem de servidores;
- cadastro, visualização e edição;
- pesquisa por nome, matrícula ou cargo;
- opção para mostrar ou ocultar servidores inativos;
- inativação sem exclusão física;
- reativação;
- validação de nome, matrícula e e-mail;
- matrícula única;
- acesso restrito a administradores;
- cartão funcional no painel do módulo;
- serviço próprio usando a função `conectar_banco` recebida do sistema principal.

Os servidores ficam separados da tabela `usuarios`. A tabela `usuarios` é referenciada somente para registrar quem criou ou atualizou cada cadastro.

A migração idempotente e aditiva `002_criar_fc_servidores.sql` foi aplicada em **14/07/2026**, usando a conexão atualmente configurada no `.env`, conforme autorização expressa. Nenhuma credencial, URL ou hostname foi registrado neste documento.

Verificações realizadas após a aplicação:

- a tabela `fc_servidores` existe e foi criada vazia;
- as 13 colunas esperadas e seus tipos foram conferidos;
- a chave primária em `id` e a matrícula única foram conferidas;
- as chaves estrangeiras de criação e atualização apontam para `usuarios.id`;
- as validações de nome e matrícula preenchidos foram conferidas;
- o índice `idx_fc_servidores_ativo_nome` existe;
- o catálogo de colunas, restrições e índices das demais tabelas permaneceu idêntico;
- nenhum dado existente foi apagado ou alterado;
- nenhuma outra migração foi executada.

Foram executados **50 testes automatizados**: os 29 testes anteriores e 21 testes novos da Etapa 2B. Resultado: **50 passaram e 0 falharam**. Os testes usam objetos simulados e bloqueiam qualquer conexão PostgreSQL real.

Os testes manuais da Etapa 2B também foram concluídos com sucesso. Foram validados:

- cadastro de servidor;
- pesquisa por nome, matrícula ou cargo;
- visualização dos detalhes e edição;
- inativação e exibição de servidores inativos;
- reativação.

A tabela `fc_servidores` está funcionando corretamente com o cadastro de servidores e responsáveis.

## Etapa 2C — Contratos e vínculo de responsáveis

A Etapa 2C foi concluída. O cadastro básico de contratos foi implementado dentro do módulo e registrado no commit `5723933280c8e8bf9b917773db9ef5220dda0ea3`:

- listagem, cadastro, visualização e edição de contratos;
- vínculo obrigatório com empresa ativa, gestor ativo e fiscal titular ativo;
- vínculo opcional com fiscais substitutos;
- confirmação explícita para o mesmo servidor exercer mais de uma função;
- histórico de responsáveis preservado nas substituições;
- pesquisa por número, processo, objeto ou empresa;
- filtros por situação, empresa, cadastro ativo/inativo e vencimento em 60 dias;
- valores aceitos e exibidos no formato brasileiro;
- datas exibidas em `DD/MM/AAAA`;
- inativação e reativação sem exclusão física;
- acesso restrito a administradores;
- cartão de Contratos habilitado no painel do módulo.

A migração idempotente e aditiva `003_criar_fc_contratos.sql` foi aplicada em **14/07/2026**, usando a conexão atualmente configurada no `.env`, conforme autorização expressa. Nenhuma credencial, URL ou hostname foi registrado neste documento.

A primeira tentativa foi recusada pelo PostgreSQL porque a conexão estava em modo somente leitura e não criou nenhuma tabela. Em seguida, a conexão foi explicitamente configurada para leitura e escrita e a migração autorizada foi aplicada com sucesso em uma única transação.

Verificações realizadas após a aplicação:

- as tabelas `fc_contratos` e `fc_contrato_responsaveis` existem;
- `fc_contratos` possui as 16 colunas esperadas;
- `fc_contrato_responsaveis` possui as 12 colunas esperadas;
- chaves primárias e estrangeiras para empresas, servidores e usuários foram conferidas;
- número único, situações permitidas, valor não negativo e período de vigência foram conferidos;
- tipos de responsabilidade, titularidade e período dos responsáveis foram conferidos;
- índices normais e índices únicos parciais foram conferidos;
- as duas tabelas foram criadas vazias, sem cadastro de contratos ou responsáveis;
- o catálogo de colunas, restrições e índices das tabelas anteriores permaneceu idêntico;
- nenhum dado anterior foi apagado ou alterado;
- nenhuma outra migração foi executada.

Foram executados **76 testes automatizados**: os 50 testes anteriores e 26 testes novos da Etapa 2C. Resultado: **76 passaram e 0 falharam**. Os testes usam serviços, banco e APIs simulados.

Os testes manuais da Etapa 2C também foram concluídos com sucesso. Foram validados:

- cadastro de contrato e vínculo com empresa;
- gestor, fiscal titular e fiscais substitutos;
- visualização e edição;
- troca de responsável com preservação do histórico;
- pesquisa e filtros;
- alerta de vencimento;
- inativação e reativação.

As tabelas `fc_contratos` e `fc_contrato_responsaveis` estão funcionando corretamente com o cadastro de contratos e seus responsáveis.

## Próxima etapa recomendada

A próxima etapa recomendada é o cadastro de aditivos contratuais. Ela não deve ser iniciada sem nova autorização expressa.

## Como continuar o trabalho

1. Abrir o projeto `C:\sistema-recic3`.
2. Confirmar a branch com `git branch --show-current`.
3. Confirmar que a branch é `codex/modulo-fiscalizacao-contratos` e não `main`.
4. Ler este arquivo e revisar o `git diff` pendente.
5. Não modificar `_referencia_fiscaliza/`.
6. Solicitar autorização explícita antes de aplicar a migração de servidores.
7. Continuar usando o login, os usuários, os papéis e a conexão PostgreSQL existentes no sistema principal.

A Etapa 1 está registrada no commit `c52ecb8488577aa2d859917a003ef19808a42668`. A Etapa 2A está registrada no commit `02231cdc3dfc1e74a029483c72e46d78bc2cf142`, a aplicação da migração no commit `8d81d3a40e8ffe81a59e9a09d18589ba330f25ae` e a correção da consulta externa no commit `87d55dee19ca8c16e4e14be7888bd603e27c2473`.
