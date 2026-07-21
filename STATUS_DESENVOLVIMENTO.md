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

## Etapa 2D — Aditivos contratuais

A Etapa 2D foi concluída. A implementação foi registrada no commit
`7db580994a2c24d5035c5fc50251b119a8741401`:

- listagem, cadastro, visualização e edição de aditivos;
- pesquisa por contrato, termo, processo, empresa ou tipo;
- filtro por tipo e por cadastro ativo/inativo;
- inativação e reativação sem exclusão do histórico;
- cálculo do valor atualizado usando somente acréscimos e supressões ativos;
- cálculo da vigência atual usando somente alterações de prazo ativas;
- preservação do valor e da vigência originais do contrato;
- resumo e histórico de aditivos na página de detalhes do contrato;
- valores monetários tratados com `Decimal` e exibidos no padrão brasileiro;
- acesso restrito a administradores;
- cartão de Aditivos habilitado no painel do módulo.

A migração idempotente e aditiva `004_criar_fc_aditivos.sql` foi aplicada em
**14/07/2026**, usando a conexão atualmente configurada no `.env`, conforme
autorização expressa. Nenhuma credencial, URL ou hostname foi registrado neste
documento.

Verificações realizadas após a aplicação:

- a tabela `fc_aditivos` existe e possui as 19 colunas esperadas;
- a chave primária, as chaves estrangeiras para `fc_contratos` e `usuarios`, e
  a combinação única de contrato com número do termo foram conferidas;
- as restrições de tipo, dias, valores e percentual não negativos foram
  conferidas;
- os índices de contrato/atividade, tipo/atividade e data de assinatura foram
  conferidos;
- a tabela foi criada vazia, com **0 registros**;
- a estrutura, as restrições, os índices e as quantidades de registros das
  tabelas anteriores permaneceram iguais antes e depois da migração;
- nenhum dado anterior foi apagado ou alterado;
- nenhuma outra migração foi executada e nenhum aditivo foi cadastrado.

Foram executados **100 testes automatizados**: os 76 testes anteriores e 24
testes da Etapa 2D. Resultado: **100 passaram e 0 falharam**. Os testes usam
serviços e banco simulados e bloqueiam conexões PostgreSQL reais.

Os testes manuais da Etapa 2D também foram concluídos com sucesso. Foram
validados:

- cadastro de aditivo de prazo e atualização da vigência atual;
- cadastro de acréscimo e de supressão de valor;
- cálculo correto do valor atualizado;
- edição de aditivo;
- inativação com retirada do efeito dos cálculos;
- reativação com retorno do efeito aos cálculos;
- bloqueio de número de termo duplicado no mesmo contrato;
- listagem geral de aditivos.

A tabela `fc_aditivos` está funcionando corretamente. Os cálculos de valor e
vigência foram validados pelos testes automatizados e manuais.

## Etapa 2E — Documentos e anexos

A Etapa 2E foi concluída. A implementação inicial foi registrada no commit
`7b4cff6f523117b507cfef26b53006013b40f183`:

- armazenamento persistente no Cloudinary usando arquivos `raw` e
  `authenticated`;
- chaves internas com UUID, sem sobrescrita por nomes repetidos;
- URLs privadas temporárias, geradas somente após autorização administrativa;
- documentos vinculados a contratos e, opcionalmente, aos seus aditivos;
- listagem geral, pesquisa, filtros, detalhes, abertura e download protegido;
- documentos exibidos nos detalhes do contrato e do aditivo;
- inativação e reativação sem excluir o arquivo ou o histórico;
- validação de tamanho, extensão e conteúdo, com limite padrão de 20 MB;
- nome original sanitizado e SHA-256 calculado;
- compensação que tenta remover do Cloudinary um arquivo cujo registro falhou
  no banco;
- cartão de Documentos habilitado no painel do módulo.

As variáveis `CLOUDINARY_CLOUD_NAME`, `CLOUDINARY_API_KEY` e
`CLOUDINARY_API_SECRET` foram confirmadas no ambiente local, sem registrar seus
valores. A dependência oficial `cloudinary` foi adicionada ao
`requirements.txt`.

O arquivo `.env` permanece fisicamente no computador, mas foi removido do
rastreamento do Git. O `.gitignore` continua ignorando esse arquivo e o
`.env.example` documenta somente nomes de variáveis e exemplos não sensíveis.

A revisão técnica final confirmou que o conteúdo é inspecionado por assinatura
binária ou estrutura interna, conforme o formato. O ponteiro do arquivo retorna
ao início após a leitura. Se o banco falhar depois do upload, ocorre rollback e
o serviço tenta remover o mesmo `public_id` privado do Cloudinary; eventual
falha nessa limpeza gera somente um aviso técnico seguro e não substitui o erro
principal do cadastro.

A migração idempotente e aditiva `005_criar_fc_documentos.sql` foi aplicada em
**15/07/2026**, usando a conexão atualmente configurada no `.env`, conforme
autorização expressa. Nenhuma credencial, URL ou hostname foi registrado neste
documento. Nenhum arquivo foi enviado ao Cloudinary durante a aplicação da
migração.

Verificações realizadas após a aplicação:

- a tabela `fc_documentos` existe e possui as 19 colunas esperadas;
- chave primária, chaves estrangeiras de contrato e autoria, e vínculo composto
  entre aditivo e contrato foram conferidos;
- `aditivo_id` continua opcional para documentos ligados diretamente ao
  contrato;
- categorias, extensões, SHA-256 em formato hexadecimal minúsculo, tamanho não
  negativo e armazenamento Cloudinary foram conferidos;
- a chave de armazenamento é única;
- os índices de contrato, aditivo, categoria e título existem;
- o índice composto `uq_fc_aditivos_id_contrato_id` foi criado após a
  verificação de ausência de duplicidades;
- a tabela foi criada vazia, com **0 registros**;
- tabelas, colunas, restrições e quantidades de registros anteriores
  permaneceram iguais;
- o único índice acrescentado a uma tabela anterior foi o índice composto
  expressamente autorizado;
- nenhum dado anterior foi apagado ou alterado e nenhuma outra migração foi
  executada.

Foram executados **125 testes automatizados**: os 100 testes anteriores e 25
testes novos da Etapa 2E. Resultado: **125 passaram e 0 falharam**. Banco e
Cloudinary foram substituídos por objetos simulados.

Os testes manuais da Etapa 2E também foram concluídos com sucesso. Foram
validados:

- funcionamento da tabela `fc_documentos` após a migração 005;
- documentos vinculados a contratos e aditivos;
- envio real para o Cloudinary;
- armazenamento privado nos modos `raw` e `authenticated`;
- organização dos documentos por contrato e aditivo;
- registro e exibição do documento no sistema;
- abertura por endereço privado temporário com validade de cinco minutos.

O Cloudinary está configurado e o cadastro de documentos de contratos e
aditivos está funcionando. Nenhuma credencial, assinatura, chave interna
completa ou endereço temporário foi registrada neste documento.

## Etapa 2F — Planilha Orçamentária e Composição de Custos

A Etapa 2F foi concluída e validada manualmente em **17/07/2026**. Foram
adicionadas planilhas orçamentárias versionadas por contrato, com itens de
custo, subtotais por grupo e total geral calculado com `Decimal`, sem uso de
`float` e sem armazenar totais duplicados no banco.

Principais recursos preparados:

- planilha Original e novas versões Aditivada, Reajustada, Repactuada, Revisada
  ou Outra;
- estados Em elaboração e Consolidada;
- bloqueio permanente de edição da versão consolidada;
- itens com quantidade, valor unitário e fator multiplicador;
- inativação e reativação sem exclusão física;
- cópia transacional somente dos itens ativos para uma nova versão;
- escolha transacional de uma única planilha vigente por contrato;
- comparação entre planilha original, vigente, valor original do contrato e
  valor atualizado pelos aditivos;
- pesquisa e filtros, cartão no painel e histórico no detalhe do contrato.

A migração idempotente e aditiva
`006_criar_fc_planilhas_orcamentarias.sql` foi aplicada em **15/07/2026** usando
a conexão configurada localmente no ambiente autorizado. Nenhuma credencial,
URL ou hostname foi registrado neste documento e nenhuma execução automática
foi adicionada.

Verificações realizadas após a aplicação:

- `fc_planilhas_orcamentarias` e `fc_planilha_itens` existem;
- as duas tabelas foram criadas vazias, com **0 registros** em cada uma;
- colunas, tipos, obrigatoriedade, valores padrão e auditoria com `TIMESTAMPTZ`
  foram conferidos;
- chaves primárias e estrangeiras para contratos, aditivos, usuários e
  planilhas foram conferidas;
- o vínculo composto garante que um aditivo pertença ao mesmo contrato;
- versão por contrato, planilha Original e planilha vigente possuem as
  restrições únicas esperadas;
- tipos, estados, nome preenchido, versão positiva e regra de planilha vigente
  consolidada e ativa foram confirmados;
- quantidade e valor unitário usam `NUMERIC(24,8)` não negativo, e o fator
  multiplicador usa `NUMERIC(24,8)` maior que zero;
- todos os índices previstos na migração existem;
- o índice composto de aditivos já existia e permaneceu compatível;
- nenhuma planilha ou item foi cadastrado;
- estruturas e quantidades das tabelas anteriores permaneceram iguais;
- nenhum dado anterior foi apagado ou atualizado e nenhuma outra migração foi
  executada.

Em **17/07/2026**, foi corrigida a abertura do formulário de novo item da
planilha. A rota passou a inicializar explicitamente todos os campos esperados,
o filtro de apresentação decimal passou a tratar valores ausentes ou inválidos
com segurança, e o template recebeu proteção adicional para campos ainda não
preenchidos. Cálculos, consolidação e estrutura do banco não foram alterados.

Foram executados **161 testes automatizados**: os 158 testes anteriores e três
testes específicos da correção. Resultado final: **161 passaram e 0 falharam**.
PostgreSQL e Cloudinary são bloqueados globalmente por mocks que falham
imediatamente se houver uma tentativa de acesso, mesmo quando o computador
possui um `.env` válido. A verificação de sintaxe e o `git diff --check` também
passaram.

Os testes manuais também foram aprovados. Foram validados o formulário e o
cadastro de itens, os cálculos com `Decimal`, a edição, inativação e
reativação, subtotais e total geral, consolidação e bloqueio posterior,
definição da planilha vigente, criação de nova versão por cópia, preservação
das versões anteriores, comparação entre original e vigente e preservação do
valor original do contrato. As tabelas `fc_planilhas_orcamentarias` e
`fc_planilha_itens` estão funcionando após a aplicação e verificação da
migração 006.

## Etapa 2G — Ativos vinculados aos contratos

A implementação da Etapa 2G foi concluída em **17/07/2026** e registrada no
Git. O novo cadastro usa exclusivamente os nomes
`fc_ativos_contratuais` e `fc_ativo_vinculos`, sem modificar o cadastro
`patrimonio`, suas tabelas, rotas ou funções existentes no sistema principal.

Foram preparados:

- cadastro, detalhes, edição, inativação e reativação de ativos contratuais;
- normalização e validação de código, placa, chassi, patrimônio, ano e
  capacidade com `Decimal`;
- empresa proprietária opcional, validada como existente e ativa;
- vínculos transacionais entre ativos e contratos;
- encerramento sem exclusão, com data final e preservação do histórico;
- bloqueio de novos vínculos para ativos inativos ou baixados e contratos
  inativos;
- bloqueio de inativação enquanto houver vínculo ativo;
- pesquisa, filtros, contadores e listagem geral de vínculos;
- cartão no painel e exibição de ativos atuais e anteriores no contrato.

A migração idempotente e aditiva `007_criar_fc_ativos_contratuais.sql` foi
aplicada em **17/07/2026** usando o ambiente configurado localmente. Foram
criadas e verificadas as tabelas `fc_ativos_contratuais` e
`fc_ativo_vinculos`, ambas inicialmente com zero registros. Colunas, tipos,
valores padrão, chaves estrangeiras, restrições e índices foram conferidos.
Nenhuma execução automática foi adicionada e nenhuma credencial foi registrada.

Antes e depois da aplicação, foram comparadas as estruturas e as quantidades
de registros de todas as tabelas anteriores. Nenhuma estrutura ou quantidade
anterior mudou, nenhum dado foi apagado ou atualizado e a tabela `patrimonio`
permaneceu inalterada.

Após a revisão técnica final, foram executados **187 testes automatizados**:
os 161 anteriores e 26 testes da Etapa 2G. Resultado: **187 passaram e 0
falharam**. PostgreSQL e
Cloudinary permaneceram bloqueados por mocks globais. A verificação de sintaxe
e o `git diff --check` também passaram.

A revisão final reforçou a validação direta no serviço, ampliou a cobertura de
normalização e permissões e acrescentou à migração uma restrição de coerência:
vínculo ativo permanece sem data final; vínculo encerrado permanece inativo e
com data final. Essas regras também foram confirmadas na estrutura criada.

Os testes manuais da Etapa 2G foram concluídos e aprovados em **17/07/2026**.
Foram validados o cadastro e a edição do ativo, a normalização e as
duplicidades, os vínculos com contratos, os bloqueios de segurança, o
encerramento e a preservação do histórico, além da criação de vínculo
posterior. As tabelas `fc_ativos_contratuais` e `fc_ativo_vinculos` estão
funcionando. O cadastro patrimonial antigo permaneceu preservado e funcionando
normalmente. Nenhuma operação do novo cadastro utiliza `DELETE`.

## Próxima etapa recomendada

A Etapa 2G está concluída, com migração, testes automatizados e testes manuais
aprovados. A próxima etapa recomendada, somente depois de nova autorização
expressa, é implementar fiscalizações e ocorrências contratuais.

## Como continuar o trabalho

1. Abrir o projeto `C:\sistema-recic3`.
2. Confirmar a branch com `git branch --show-current`.
3. Confirmar que a branch é `codex/modulo-fiscalizacao-contratos` e não `main`.
4. Ler este arquivo e revisar o `git diff` pendente.
5. Não modificar `_referencia_fiscaliza/`.
6. Solicitar autorização explícita antes de iniciar uma etapa posterior.
7. Continuar usando o login, os usuários, os papéis e a conexão PostgreSQL existentes no sistema principal.

A Etapa 1 está registrada no commit `c52ecb8488577aa2d859917a003ef19808a42668`. A Etapa 2A está registrada no commit `02231cdc3dfc1e74a029483c72e46d78bc2cf142`, a aplicação da migração no commit `8d81d3a40e8ffe81a59e9a09d18589ba330f25ae` e a correção da consulta externa no commit `87d55dee19ca8c16e4e14be7888bd603e27c2473`.

## Etapa 2H — Fiscalizações e ocorrências contratuais (concluída)

A implementação da Etapa 2H foi retomada e concluída tecnicamente em
**20/07/2026**. O módulo agora possui cadastro, edição, detalhamento,
finalização e cancelamento de fiscalizações, além de cadastro, acompanhamento,
inativação e reativação de ocorrências contratuais.

As ocorrências podem ser vinculadas ao contrato, a uma fiscalização compatível
e, opcionalmente, a um ativo contratual compatível. Os acompanhamentos preservam
o histórico de mudanças de situação, sem exclusão física. As telas de painel,
contrato e ativo receberam os respectivos atalhos, indicadores e listagens.

A revisão técnica confirmou validações de contratos, servidores e ativos
ativos; prazos e datas coerentes; justificativa para cancelamento; preservação
da regularização anterior; consultas SQL parametrizadas; transações com
`commit` somente ao final e `rollback` em caso de falha; e acesso restrito a
administradores.

Foram executados **217 testes automatizados**: os 187 testes anteriores e 30
testes específicos da Etapa 2H. Resultado: **217 passaram e 0 falharam**. Os
testes usaram serviços e conexões simulados; nenhum PostgreSQL, Cloudinary,
consulta externa ou arquivo real foi acessado.

Na revisão técnica final, a edição da ocorrência passou a bloquear a linha com
`SELECT ... FOR UPDATE` antes de verificar a existência de acompanhamentos.
Isso evita que uma edição e um novo acompanhamento simultâneos utilizem um
estado antigo. Também foi confirmado por teste que a reabertura de uma
ocorrência regularizada preserva no histórico a data anterior e a justificativa.

A migração idempotente e aditiva
`008_criar_fc_fiscalizacoes_ocorrencias.sql` foi aplicada em **20/07/2026** no
ambiente configurado localmente. Foram criadas e verificadas as tabelas
`fc_fiscalizacoes`, `fc_ocorrencias` e `fc_ocorrencia_acompanhamentos`, todas
inicialmente com zero registros.

Foram conferidos colunas, tipos, obrigatoriedade, valores padrão, chaves
primárias e estrangeiras, restrições e índices. A chave estrangeira composta
entre ocorrência, fiscalização e contrato está ativa. As estruturas e as
quantidades de registros das 51 tabelas anteriores foram comparadas antes e
depois e permaneceram inalteradas. Nenhuma fiscalização, ocorrência ou
acompanhamento foi cadastrado e nenhuma credencial foi registrada.

Os testes manuais da Etapa 2H foram concluídos e aprovados. Fiscalizações,
ocorrências e acompanhamentos estão funcionando com a migração 008 aplicada.

## Etapa 2H.1 — Reabertura de fiscalização finalizada (concluída)

A melhoria de reabertura foi implementada em **20/07/2026**. Fiscalizações
ativas e finalizadas podem voltar para `Em elaboração` mediante justificativa
obrigatória. Fiscalizações em elaboração, canceladas ou inativas permanecem
bloqueadas, inclusive por acesso direto à rota.

Foi criada e aplicada a migração aditiva
`009_criar_fc_fiscalizacao_eventos.sql`. A tabela
`fc_fiscalizacao_eventos` foi verificada e está funcionando para armazenar
permanentemente eventos de finalização, cancelamento e reabertura. O histórico
não possui rotas de edição, inativação ou exclusão e não foram criados eventos
retroativos.

Finalização, cancelamento e reabertura bloqueiam a fiscalização com
`SELECT ... FOR UPDATE`, inserem o evento, atualizam o status e a auditoria e
somente então confirmam a transação. Qualquer falha provoca `rollback` completo.
Ocorrências, acompanhamentos, ativos, documentos e demais vínculos não são
alterados pela reabertura.

Foram executados **230 testes automatizados**: os 217 anteriores e 13 novos da
Etapa 2H.1. Resultado: **230 passaram e 0 falharam**. PostgreSQL e Cloudinary
reais permaneceram bloqueados por simulações.

Os testes manuais foram concluídos e aprovados em **21/07/2026**. Foram
validados detalhes de fiscalizações antigas, nova fiscalização, finalização,
reabertura com justificativa, retorno para `Em elaboração`, edição, nova
finalização e o histórico completo. Ocorrências e acompanhamentos permaneceram
preservados. A Etapa 2H e a melhoria 2H.1 estão concluídas.

## Etapa 2I — Medições contratuais (concluída)

A Etapa 2I foi implementada tecnicamente em **21/07/2026**. O módulo agora
possui cadastro de medições por
contrato e competência, itens manuais ou copiados como fotografia da planilha
orçamentária, acréscimos, descontos, glosas e vínculos com documentos já
armazenados pelo sistema.

O fluxo permite elaborar, enviar para análise, devolver para correção, aprovar
e cancelar. Uma medição aprovada permanece imutável; correções posteriores
geram uma nova versão, com cópia dos registros ativos e preservação integral
da versão anterior. Todos os eventos guardam uma fotografia dos totais.

Os cálculos usam `Decimal`, são refeitos no servidor e não confiam em totais
enviados pelo navegador. Inclusão, edição e inativação de itens ou ajustes
recalculam os valores dentro da mesma transação. Falhas provocam `rollback` e
nenhuma operação utiliza `DELETE`.

Foi criada a migração aditiva e idempotente
`010_criar_fc_medicoes.sql`, que prepara `fc_medicoes`, `fc_medicao_itens`,
`fc_medicao_ajustes`, `fc_medicao_documentos` e `fc_medicao_eventos`, com suas
chaves, restrições e índices.

Foram preservados os **230 testes anteriores** e acrescentados **38 testes** da
Etapa 2I, que cobrem os 72 cenários obrigatórios de permissão, validação,
cálculo, fluxo, histórico, integração e segurança. Resultado atual:
**268 testes aprovados e 0 falhas**. PostgreSQL e Cloudinary reais permaneceram
bloqueados; nenhum arquivo real foi alterado pelos testes.

Na revisão técnica final, o cancelamento passou a definir `atual = FALSE`,
mantendo a medição cancelada ativa e consultável, mas liberando a competência
para uma nova medição com numeração própria. Atualização, evento e mudança
de situação permanecem na mesma transação, com rollback integral em falhas.

Também foi reforçada a fotografia dos itens de planilha: depois de copiado para
a medição, o item preserva código, descrição, unidade, quantidade prevista e
preço unitário originais, sem consultar novamente a planilha durante a edição.
Foram adicionados bloqueios nas telas acessadas diretamente por URL, validação
defensiva de ajustes e categorias de documentos, testes de concorrência e uma
matriz relacionando os 72 requisitos obrigatórios aos testes automatizados.

A Etapa 2I foi concluída técnica e funcionalmente.

### Aplicação da migração 010

Em **21/07/2026**, a migração `010_criar_fc_medicoes.sql` foi aplicada no
ambiente configurado e verificada. Foram criadas as cinco tabelas previstas:
`fc_medicoes`, `fc_medicao_itens`, `fc_medicao_ajustes`,
`fc_medicao_documentos` e `fc_medicao_eventos`.

As colunas, tipos, obrigatoriedades, valores padrão, chaves estrangeiras,
restrições e índices foram conferidos. A quantidade inicial verificada foi de
**zero registros em cada uma das cinco tabelas**. Nenhuma medição, item, ajuste,
vínculo de documento ou evento foi criado automaticamente.

As contagens de registros e a estrutura das tabelas anteriores foram comparadas
antes e depois da execução e permaneceram inalteradas. Nenhum dado anterior foi
apagado ou atualizado e nenhuma credencial foi registrada neste documento.

### Envio de documentos pela medição

Em **21/07/2026**, a área de documentos comprobatórios da medição passou a
oferecer duas opções: vincular um documento existente ou enviar um arquivo novo.
O novo arquivo é validado, armazenado de forma privada pelo serviço de documentos
já existente e vinculado automaticamente à medição.

O cadastro do documento e o vínculo com a medição são confirmados na mesma
transação. Se o banco não concluir a operação depois do envio, ocorre rollback e
o arquivo recém-enviado é removido do armazenamento para evitar arquivo órfão.
Também foi explicitada, na tela de detalhes, a composição `valor bruto +
acréscimos - descontos - glosas = valor líquido`, com sinais e formatação
monetária brasileira. O cenário manual de R$ 7.500,00 + R$ 5,00 - R$ 0,00 -
R$ 10,00 = R$ 7.495,00 foi coberto por teste de cálculo e de renderização.

Foram executados **274 testes automatizados**, todos aprovados, sem acesso ao
PostgreSQL ou ao Cloudinary reais.

### Encerramento da Etapa 2I

Os testes manuais foram concluídos e aprovados em **21/07/2026**. Foram
validados cadastro, competência, período, itens manuais e da planilha,
fiscalizações, ocorrências, ajustes, documentos, eventos, indicadores e a
exibição das medições nos contratos.

Os cálculos com `Decimal` e `ROUND_HALF_UP`, o recálculo no servidor, o envio
para análise, a devolução, a aprovação, o cancelamento e o versionamento foram
validados. Versões aprovadas permanecem imutáveis e revisões preservam os itens,
ajustes, documentos e eventos das versões anteriores.

Itens, ajustes e vínculos são inativados sem exclusão física. Documentos
armazenados não são apagados pelas operações normais da medição. A aprovação é
apresentada como uma etapa administrativa distinta do pagamento, e a glosa é
apresentada somente como redução do valor medido, não como multa ou sanção.

A migração 010 e as cinco tabelas de medições estão aplicadas, verificadas e
funcionando. A Etapa 2I está encerrada com **274 testes automatizados aprovados,
0 falhas e 0 erros**, além dos testes manuais aprovados.
