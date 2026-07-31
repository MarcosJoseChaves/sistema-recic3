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

## Etapa 2J — Ateste da execução e encaminhamento para pagamento (implementada para revisão)

A Etapa 2J foi implementada tecnicamente em **21/07/2026**, sem acessar o banco
real, sem acessar o Cloudinary real e sem executar migrações. O módulo agora
permite criar um ateste a partir de uma medição aprovada, ativa e atual, registrar
o servidor atestador, parecer, notas fiscais e documentos complementares.

O fluxo permite atestar a execução, devolver para correção, retornar para
elaboração, encaminhar documentalmente ao setor financeiro e cancelar antes do
encaminhamento. Cada mudança cria um evento permanente com a fotografia do valor
atestado e do total das notas. Notas, vínculos e eventos são preservados; nenhuma
operação normal utiliza exclusão física.

O valor atestado é copiado do valor líquido aprovado da medição e os totais são
calculados no servidor com `Decimal` e `ROUND_HALF_UP`. O encaminhamento exige
nota ativa, arquivo em todas as notas, protocolo, servidor ativo e soma das notas
igual ao valor atestado. “Encaminhado para pagamento” representa somente o envio
ao setor financeiro e não significa liquidação, pagamento ou quitação.

Medições com ateste ativo e não cancelado não podem gerar revisão. A medição, o
contrato e o painel passaram a exibir informações e indicadores de atestes sem
alterar `app.py` ou o patrimônio antigo.

Foi criada a migração aditiva e idempotente `011_criar_fc_atestes.sql`, que
prepara `fc_atestes`, `fc_ateste_notas_fiscais`, `fc_ateste_documentos` e
`fc_ateste_eventos`, com chaves, restrições e índices. **A migração 011 não foi
executada.**

Foram preservados os **274 testes anteriores** e acrescentados **30 testes** da
Etapa 2J, cobrindo os 66 requisitos de permissão, validação, fluxo, documentos,
valores, histórico, transações, integração e segurança. Resultado: **304 testes
aprovados, 0 falhas e 0 erros**. PostgreSQL e Cloudinary reais permaneceram
bloqueados e nenhum arquivo real foi alterado pelos testes.

Na revisão técnica final de **22/07/2026**, a migração foi alinhada ao padrão
das tabelas anteriores, mantendo `atualizado_por_usuario_id` opcional na criação
e preenchido pelo serviço nas inclusões e atualizações. O encaminhamento também
passou a reconfirmar que cada arquivo de nota fiscal continua ativo e pertence
ao contrato. Foram acrescentados testes para concorrência, diferença de um
centavo, nota inativa, documento posteriormente invalidado e repetição do ateste
após devolução.

A Etapa 2J está pronta para revisão técnica e funcional. O próximo passo seguro,
somente mediante nova autorização, é revisar e depois aplicar exclusivamente a
migração 011 no ambiente configurado.

### Aplicação da migração 011

Em **22/07/2026**, a migração `011_criar_fc_atestes.sql` foi aplicada no
ambiente configurado e verificada. Foram criadas as quatro tabelas previstas:
`fc_atestes`, `fc_ateste_notas_fiscais`, `fc_ateste_documentos` e
`fc_ateste_eventos`.

Foram conferidos colunas, tipos, obrigatoriedade, valores padrão, chaves
primárias, chaves estrangeiras, restrições e índices. As quatro tabelas
iniciaram com **0 registros**. Nenhum ateste, nota fiscal, vínculo de documento
ou evento foi criado automaticamente.

As quantidades de registros em `usuarios`, `fc_medicoes`, `fc_servidores` e
`fc_documentos` foram comparadas antes e depois da execução e permaneceram
inalteradas. Nenhum dado anterior foi atualizado ou apagado. Nenhuma credencial
foi registrada neste arquivo.

### Melhoria no lançamento da nota fiscal

Em **22/07/2026**, o formulário da nota fiscal passou a permitir o envio de um
arquivo novo no próprio lançamento, além da seleção de um documento já
cadastrado. O arquivo é validado pelo conteúdo, armazenado de forma privada no
Cloudinary existente, registrado em `fc_documentos` e vinculado automaticamente
à nota fiscal.

Cadastro do documento e gravação da nota utilizam a mesma transação. Se o banco
falhar depois do upload, ocorre rollback e a cópia recém-enviada é removida do
Cloudinary. A suíte passou a ter **310 testes aprovados, 0 falhas e 0 erros**,
sem acesso real ao PostgreSQL ou ao Cloudinary durante os testes.

### Encerramento da Etapa 2J

A Etapa 2J foi concluída em **22/07/2026**. A migração 011 está aplicada e
verificada, com as quatro tabelas de atestes em funcionamento. Os testes manuais
foram aprovados, incluindo o lançamento de nota fiscal com envio direto de
arquivo e a alternativa de selecionar um documento já cadastrado.

Os arquivos enviados permanecem privados e autenticados no Cloudinary. Se o
envio funcionar, mas a gravação no banco falhar, o sistema desfaz a transação e
remove o novo arquivo enviado, evitando arquivo órfão. Documentos antigos já
vinculados são preservados.

A validação final aprovou **310 testes automatizados**, sem acesso ao PostgreSQL
ou Cloudinary reais. O encaminhamento registra o envio para o fluxo de pagamento,
mas não representa a confirmação de que o pagamento foi realizado.

### Simplificação visual do painel inicial

Em **22/07/2026**, o painel inicial do módulo foi simplificado para funcionar
como uma entrada direta para suas funcionalidades. Os indicadores operacionais e
financeiros foram removidos dessa página, evitando também a execução das consultas
que os alimentavam. Os serviços continuam preservados para possível utilização em
um painel gerencial separado.

Foram mantidos os **11 cards de acesso**, em ordem lógica, com os mesmos destinos.
O cabeçalho ficou mais compacto e o layout passou a usar cards uniformes,
responsivos e acessíveis, com foco visível para navegação por teclado. Os ícones
de Aditivos e Atestes foram corrigidos para opções compatíveis com a biblioteca
visual já utilizada pelo sistema.

A validação final aprovou **6 testes específicos do painel** e **311 testes na
suíte completa**, com zero falhas e zero erros. PostgreSQL e Cloudinary reais
permaneceram bloqueados, nenhuma migração foi executada e nenhuma regra de negócio
foi alterada.

### Etapa H2A.1 — Inicialização protegida e isolamento da homologação

Em **22/07/2026**, a Etapa H2A.1 foi implementada e permanece **pendente de
revisão e commit**. A importação de `app.py` deixou de executar automaticamente
a migração legada de produtos. Essa rotina continua disponível por um script
administrativo separado que exige confirmação explícita.

A inicialização agora diferencia `development`, `testing`, `homologation` e
`production`. Não existem mais chave secreta previsível nem conexão local de
banco como alternativas silenciosas. Homologação e produção exigem
`SECRET_KEY` e `DATABASE_URL`; testes usam configuração fictícia explícita.

Foram adicionados `/health` sem dependência externa, `ProxyFix` opcional,
política segura de cookies, cabeçalhos mínimos e uma barreira HTTP Basic
exclusiva da homologação. Novos uploads podem usar prefixo Cloudinary separado
por ambiente, sem modificar as chaves dos documentos antigos.

Foram preservados os **311 testes anteriores** e acrescentados **59 testes** da
Etapa H2A.1. Resultado atual: **370 testes aprovados, 0 falhas e 0 erros**, com
PostgreSQL e Cloudinary reais bloqueados e nenhuma migration executada.

Continuam pendentes: CSRF, fixação das versões do Python e das dependências,
migration-base reproduzível, controle formal das migrations, rotação de
credenciais históricas, criação dos serviços separados de homologação e deploy
no Render.

#### Revisão técnica final da H2A.1

Em **22/07/2026**, a revisão final tornou o `ProxyFix` mais conservador, retirando
a confiança desnecessária em `X-Forwarded-Host` e `X-Forwarded-Port`. Foram
incluídas configurações seguras para os cookies de "lembrar-me" do Flask-Login.

O prefixo Cloudinary passou a rejeitar barras invertidas, além de `.` e `..`.
Configurações parciais do Cloudinary em homologação ou produção agora
interrompem a inicialização com mensagem genérica, sem revelar valores.

Os testes históricos passaram a limpar temporariamente o ambiente durante a
importação do app, evitando dependência do `.env` pessoal. A revisão final
aprovou **370 testes, 0 falhas e 0 erros**. Nenhum banco, Cloudinary ou migration
foi acessado ou executado. O deploy ainda não ocorreu e esta etapa não torna o
sistema pronto para produção.

### Etapa H2A.2 — Proteção CSRF

Em **22/07/2026**, a Etapa H2A.2 foi implementada e revisada tecnicamente. A
aplicação passou a usar a proteção oficial do Flask-WTF em
todos os ambientes, sem exceções e sem criar nova chave secreta.

Foram protegidos 61 formulários POST, inclusive login, logout, uploads e ações
administrativas. O logout passou de GET para POST. Requisições AJAX/JSON internas
enviam `X-CSRFToken`, enquanto filtros GET e serviços externos não recebem o
token. Erros de validação retornam uma página amigável com HTTP 400, sem detalhes
internos.

Os 370 testes anteriores foram preservados por um cliente auxiliar que usa token
real na mesma sessão, sem desligar CSRF. Foram acrescentados 37 testes específicos
para formulários, login, logout, JSON, uploads, permissões, `/health` e a barreira
Basic. A revisão final aprovou **407 testes, 0 falhas e 0 erros**. Nenhum teste
acessou PostgreSQL ou Cloudinary reais; nenhuma migration ou deploy foi executado.

Permanecem pendentes: versões gerais fixadas, migration-base, controle formal de
migrations, rotação de credenciais
históricas, Neon e Cloudinary separados para homologação, deploy no Render,
revisão e autorização individual das rotas públicas, CSP, rate limit, trusted
hosts e monitoramento.

### Etapa H2A.3A — Inventário e matriz de acesso das rotas

Em **22/07/2026**, a Etapa H2A.3A ficou **em análise para revisão**. Foi criado o
documento `MATRIZ_ROTAS_ACESSO.md`, com o inventário das 177 rotas registradas,
suas proteções atuais, dados envolvidos, riscos e níveis de acesso recomendados.

O levantamento encontrou 22 bloqueadores para homologação: 11 rotas públicas
mutáveis e 11 rotas GET públicas que consultam dados internos. Também foram
marcadas 15 rotas por ID para revisão de autorização por objeto. O módulo de
Fiscalização de Contratos mantém 105 rotas funcionais com `admin_required`.

Nenhuma permissão foi alterada, nenhuma rota foi removida e nenhuma publicação
foi realizada. Não houve acesso a PostgreSQL ou Cloudinary reais, execução de
migration, upload, download ou deploy. As correções permanecem reservadas para a
futura Etapa H2A.3B, depois da revisão desta matriz.

### Etapa H2A.3B.1 — proteção das rotas mutáveis públicas

Em **22/07/2026**, a H2A.3B.1 foi implementada e **revisada tecnicamente**. As
11 rotas mutáveis que estavam públicas passaram a exigir a
proteção definida na matriz: uma operação global exige administrador, nove
operações financeiras exigem login e validação de UVR/objeto no servidor, e o
registro de denúncia fica oculto em homologação e produção.

O token CSRF e a barreira Basic continuam independentes do login interno. Um
visitante não alcança banco, geração de relatório ou gravação mesmo quando possui
token CSRF válido ou envia Basic Auth. As APIs JSON devolvem erro JSON seguro sem
sessão. Usuários comuns ficam limitados à UVR de `current_user`; IDs de conta,
transação e entidade são verificados antes da regra de negócio.

As falhas de login agora mostram uma mensagem única para usuário inexistente,
inativo ou senha incorreta. O mecanismo continua usando `check_password_hash` e
faz uma verificação contra um hash fictício criado uma única vez por processo
quando não encontra uma conta ativa, sem guardar ou registrar senha real.

Na revisão final, as consultas e alterações de extrato, transação e fluxo de
caixa passaram a repetir o escopo de UVR no próprio SQL. Listas parcialmente
autorizadas são recusadas por inteiro, com rollback e sem commit. O cursor do
helper de autorização é fechado explicitamente, nomes de arquivos exportados
são higienizados e respostas de erro não exibem SQL ou exceções. Sessões de
usuários posteriormente inativados deixam de ser recarregadas.

As tabelas legadas de contas, transações e fluxo não possuem coluna de autoria
nem estado `ativo` para conta; por isso, não foi inventada migration nesta etapa.
As operações aprovadas registram de forma segura o `usuario_id` no log técnico,
sem dados bancários, formulário ou credenciais.

Os **407 testes anteriores** foram preservados e foram acrescentados **28 testes
específicos**, totalizando **435 testes aprovados, 0 falhas e 0 erros**. Os testes
bloquearam PostgreSQL e Cloudinary reais; nenhuma migration, API externa, upload,
exportação real ou deploy foi executado.

Permanecem pendentes as 11 consultas GET públicas ao banco, relatórios e
downloads fora deste bloco, os 15 possíveis IDORs, a revisão completa de
JSON/AJAX, o segundo endpoint proposto para desativação online, migration-base,
controle de migrations, rotação de credenciais históricas, Neon e Cloudinary
separados, deploy, CSP, rate limit, trusted hosts e monitoramento.

### Etapa H2A.3B.2 — consultas e downloads protegidos

Em **22/07/2026**, a H2A.3B.2 foi implementada, **revisada tecnicamente e
concluída**. As 11 consultas GET antes públicas agora exigem login; as consultas de
entidades, contas e valores também limitam a UVR no servidor. Basic Auth e CSRF
continuam sem conceder autorização interna.

As quatro exportações CSV/PDF financeiras preservam a autorização feita na
H2A.3B.1. CSV passou a neutralizar fórmulas em campos textuais, inclusive após
espaços e controles invisíveis, sem converter números. PDF passou a escapar texto
variável sem duplicar o escape. As fichas de associado e cadastro agora consultam o ID
dentro da UVR autorizada, corrigindo dois IDORs. O documento privado da
Fiscalização exige administrador, vínculo válido e estado ativo antes de gerar
URL temporária HTTPS de cinco minutos. O redirecionamento impede cache público.

Foram preservados os **435 testes anteriores** e adicionados **27 testes**,
totalizando **462 aprovados, zero falhas e zero erros**. Não houve acesso real a
PostgreSQL, Cloudinary ou API, criação de arquivo real, migration ou deploy.

Permanecem pendentes 13 possíveis IDORs fora deste bloco, revisão geral dos 44
endpoints JSON/AJAX, o segundo endpoint proposto para desativação online,
migration-base, controle formal de migrations, rotação das credenciais,
ambientes Neon e Cloudinary separados, versões gerais das dependências, deploy,
CSP, rate limit, trusted hosts e monitoramento.

### Etapa H2A.3B.3 — autorização por objeto

Em **23/07/2026**, a H2A.3B.3 foi implementada e revisada tecnicamente. Foram
corrigidos os 13 casos restantes: cinco edições, quatro
solicitações/exclusões e quatro consultas JSON por ID.

Usuários comuns ficam limitados à UVR de `current_user`; administrador mantém
acesso global. A autorização combina ID e UVR em SQL parametrizado. UVR forjada
é ignorada ou substituída, campos inesperados não entram nas solicitações e o
cadastro relacionado de uma transação é validado novamente no SQL final. Se o
objeto mudar de UVR entre a validação e a gravação, nenhuma solicitação é criada,
ocorre rollback e a resposta permanece 404. Objeto inexistente e objeto de outra
UVR retornam a mesma resposta genérica.

Consultas JSON retornam 401 ao visitante. Falhas de banco não expõem SQL ou
exceção. Nenhum `DELETE` novo, migration, tabela ou perfil foi criado; as
quatro exclusões físicas administrativas existentes não foram ampliadas e
continuam como risco de integridade.

Também foi registrado o `DELETE` preexistente usado exclusivamente para substituir
os itens durante a edição administrativa de uma transação. Ele permanece dentro
da mesma transação, com rollback, mas deve ser avaliado junto das operações
físicas legadas por não preservar os IDs históricos dos itens.

Foram aprovados **25 testes específicos**, com subtestes cobrindo as 13 rotas, e
**487 testes totais**, com zero falhas e zero erros. Não houve banco, Cloudinary,
API externa, migration ou deploy. O inventário continua com 177 rotas (12
públicas, 59 com login e 106 administrativas), e as 105 rotas funcionais da
Fiscalização seguem administrativas.

Os 15 possíveis IDORs levantados na H2A.3A estão corrigidos: dois na H2A.3B.2 e
13 nesta etapa. Não permanece IDOR confirmado nesse inventário. Permanecem
pendentes a revisão geral de JSON/AJAX, o segundo endpoint a desativar online,
as exclusões físicas legadas, migration-base, controle formal de migrations,
rotação de credenciais, Neon e Cloudinary separados, versões gerais, CSP, rate
limit, trusted hosts, monitoramento e deploy.

**Pendência futura:** Conversão das exclusões físicas legadas para inativação ou
exclusão lógica, após decisão funcional e análise de integridade.

### Etapa H2A.3B.4 — revisão geral dos endpoints JSON/AJAX

Em **23/07/2026**, a H2A.3B.4 foi implementada e **aprovada na revisão técnica
final**, ficando pronta para commit.

O inventário confirmou **44 endpoints JSON/AJAX**: 1 público essencial, 8 para
usuário autenticado, 26 com regra de UVR/objeto e 9 administrativos. São 34
leituras e 10 escritas. Os 22 endpoints corrigidos nas H2A.3B.1, H2A.3B.2 e
H2A.3B.3 foram preservados.

As respostas protegidas agora seguem o padrão JSON 401/403/404, sem
redirecionamento HTML ou detalhe interno. Foram reforçados validação de
Content-Type e estrutura, limites de 64 KiB, 200 itens, 5.000 caracteres e
profundidade 2, listas explícitas de campos permitidos, SQL parametrizado,
rollback e cache privado `no-store`. O limite de bytes é aplicado durante a
leitura mesmo sem `Content-Length`, e conteúdo comprimido é recusado. Usuário
comum sem UVR falha de modo fechado.

O JavaScript consumidor passou por revisão de escape de texto, IDs e mensagens.
A lista dinâmica de notas passou a usar elementos DOM e `.text()`, sem
interpolação de dados do servidor em HTML, e respostas 401 orientam novo login.
Não existe CORS permissivo, JSONP, resposta sensível em `localStorage` ou log do
conteúdo completo. PostgreSQL, Cloudinary e APIs externas permaneceram
bloqueados nos testes.

O segundo endpoint incompatível com ambiente online,
`GET /sucesso_denuncia`, foi desativado com 404 em homologação e produção antes
de qualquer efeito. Em desenvolvimento e testes, exige sessão interna ativa.

Foram aprovados **38 testes específicos** e **525 testes totais**, com zero
falhas e zero erros. Sintaxe e `git diff --check` foram aprovados. Nenhuma migration,
banco, Cloudinary, API externa, arquivo real ou deploy foi executado.

Permanecem pendentes as exclusões físicas legadas, migration-base, controle
formal de migrations, rotação de credenciais, Neon separado, Cloudinary
separado, versões gerais do Python e dependências, CSP, rate limit, trusted
hosts, monitoramento e deploy.

## Etapa H2B.1 — concluída

Em **27/07/2026**, foi implementada a base de reprodutibilidade e inicialização
segura do ambiente online:

- Python `3.12.6` fixado;
- dependências diretas e transitivas fixadas e verificadas em instalação
  temporária limpa;
- Gunicorn configurado com porta obrigatória, concorrência e tempos limitados;
- logs de acesso sem query string, cookies ou cabeçalhos;
- hosts confiáveis exatos obrigatórios em homologação e produção;
- confiança no proxy obrigatória e limitada em ambientes online;
- limite global padrão de 64 MB, obrigatório e explícito online;
- resposta 413 segura e distinta para HTML e JSON;
- execução direta de `app.py` restrita ao servidor local e recusada online;
- exemplos de variáveis sem segredos reais;
- instalação limpa com Python 3.12.6, sem `site-packages` globais;
- distribuições para Linux/Python 3.12 verificadas;
- 36 testes específicos e 561 testes totais aprovados, sem falhas ou erros.

Não houve acesso ao PostgreSQL ou Cloudinary real, execução de migration ou
deploy. A revisão final corrigiu a validação de hosts malformados, adicionou
limite global e tratamento 413 e atualizou o Cloudinary para a primeira linha
compatível verificada com Linux/Python 3.12.

Os documentos do módulo mantêm limite individual e inspeção de conteúdo. Alguns
uploads legados de fotos ainda contam apenas com o limite global e ficaram
registrados para uma revisão específica posterior, sem alteração dos formatos
aceitos nesta etapa.

## Etapa H2B.2A — concluída

Em **28/07/2026**, foram implementados, sem deploy, CSP com nonce por resposta,
cabeçalhos adicionais de segurança e limitação de requisições com
Flask-Limiter. Todas as rotas permanecem registradas; login, consultas externas,
relatórios, downloads, uploads e operações mutáveis recebem limites
específicos, enquanto as demais usam o limite geral. Respostas excedidas usam
HTTP 429 amigável e não iniciam a operação de negócio.

A revisão preservou expressamente o uso existente da webcam: a câmera é
permitida somente para a própria aplicação, enquanto microfone, localização e
demais recursos não utilizados permanecem negados.

A política não usa `unsafe-eval`, curingas nem liberação geral de scripts
embutidos. Um script legado extenso permanece no template de cadastro com
nonce. Atributos legados de evento e estilo ainda exigem exceções temporárias e
restritas em `script-src-attr` e `style-src-attr`; sua remoção gradual está
registrada como trabalho futuro.

Desenvolvimento e testes usam armazenamento em memória. Produção exige Redis
compartilhado. A primeira homologação restrita pode usar memória apenas mediante
`RATELIMIT_ALLOW_MEMORY_HOMOLOGATION=true`, com aceitação explícita da limitação
por processo. Nenhuma credencial foi incluída nos arquivos e nenhum PostgreSQL,
Cloudinary, API externa, migration ou deploy foi executado.

Na revisão final, a UVR inserida no script legado passou a usar serialização
JSON segura; páginas HTML com nonce passaram a impedir cache compartilhado; a
URI do armazenamento recebeu validação mais estrita; o uso de `memory://` em
homologação passou a exigir autorização explícita; e os grupos sensíveis
passaram a compartilhar seus contadores. As rotas de denúncia ocultas online
permanecem em 404 mesmo sob repetição. Também foram removidas permissões CSP
para `blob:` e `data:` que não eram necessárias.

Foram aprovados **51 testes específicos da H2B.2A** e **612 testes totais**, com
zero falhas e zero erros. Os 561 testes anteriores foram preservados. Também
passaram, em execução separada, os 245 testes das etapas H2A e H2B.1. A
instalação limpa das dependências fixadas foi validada com Python 3.12.6.
Nenhum PostgreSQL, Cloudinary, Redis ou API externa foi acessado; nenhuma
migration ou deploy foi executado.

## Etapa H2B.2B — concluída

Em **29/07/2026**, foi concluída a base de logs estruturados, identificação de
requisições e monitoramento operacional, sem migration ou deploy:

- logs JSON de uma linha e em UTC obrigatórios online;
- níveis, formato e chaves booleanas validados;
- `request_id` novo por requisição e cabeçalho `X-Request-ID`;
- duração calculada com relógio monotônico;
- eventos operacionais e de segurança com nomes estáveis;
- redação recursiva com proteção contra ciclos, profundidade e tamanho;
- JSON estrito, inclusive para valores não finitos e objetos não serializáveis;
- falha do próprio handler isolada, sem interromper a resposta;
- resposta 500 genérica em HTML e JSON, com código de referência;
- `/health` mínimo e fora do evento comum de requisição;
- stdout sem arquivos locais de log;
- guia `MONITORAMENTO_OPERACIONAL.md`.

O inventário identificou 190 chamadas legadas de log ainda presentes no
sistema principal e no módulo: 20 informativas, 12 avisos, 60 erros, uma de
depuração e 97 chamadas `logger.exception`. Os pontos que expunham nome de
usuário, valores digitados, SQL, parâmetros ou mensagens cruas de exceção foram
corrigidos. As chamadas `logger.exception` restantes usam mensagens genéricas;
o formatador online omite traceback bruto e registra somente o tipo da exceção.
Não existe `print` usado como logging no runtime Flask, nem `FileHandler` ou
`RotatingFileHandler`. Scripts administrativos antigos que já utilizavam
`print` não são executados pela aplicação e ficaram fora do escopo desta etapa.

Foram aprovados **61 testes específicos da H2B.2B** e **673 testes totais**, com
zero falhas e zero erros. Os 612 testes anteriores foram preservados. Também
passaram, em execução separada, os 296 testes das etapas H2A, H2B.1 e H2B.2A.
Nenhum banco, Cloudinary, Redis ou API real foi acessado e nenhuma migration ou
deploy foi executado.

A contagem de sintaxe foi normalizada: existem 84 arquivos Python já rastreados
no commit-base (61 da aplicação e 23 de testes) e dois arquivos novos desta
etapa, totalizando 86 arquivos candidatos ao commit (62 da aplicação e 24 de
testes). A contagem anterior de 103 incluía também 19 arquivos Python ignorados
da pasta `_referencia_fiscaliza`; eles não pertencem ao sistema principal nem
ao commit.

## Etapa H2C.1 — inventário do banco concluído e revisado

Em **29/07/2026**, foi concluído o inventário estático das migrations, DDL,
consultas e testes do repositório:

- 11 migrations numeradas confirmadas;
- 23 tabelas e 351 colunas definidas pelas migrations do módulo;
- 12 tabelas legadas com DDL no `app.py`;
- 37 tabelas de aplicação identificadas ao todo;
- `patrimonio` e `grupos_atividade` usados sem criação completa versionada;
- 41 definições de coluna não determinadas pelo repositório;
- 91 relacionamentos comprovados: 84 FKs no módulo e sete distintos no legado;
- 70 declarações de índice, com 69 nomes distintos;
- 103 restrições `CHECK` confirmadas;
- divergências legadas registradas sem alteração de código ou migration;
- estratégia e ordem da futura baseline documentadas;
- baseline reservada exclusivamente para PostgreSQL vazio;
- necessidade de futura exportação somente do schema, sem dados;
- 673 testes automatizados aprovados, com zero falhas e zero erros;
- sintaxe dos 86 arquivos Python aprovada;
- nenhuma migration criada ou executada;
- nenhum banco, Cloudinary ou deploy acessado.

Foram criados `MAPA_SCHEMA_BANCO.md` e `PLANO_MIGRATION_BASE.md`. A revisão
técnica final corrigiu a contagem de checks de 101 para 103, detalhou as
exclusões físicas e confirmou a classificação **C — é necessário obter o
schema atual do banco**. A futura exportação será somente de estrutura, sem
dados, e permanecerá fora do Git até auditoria. A H2C.2 não deve começar antes
de confirmar o DDL ausente e decidir as divergências documentadas.

## Etapa H2C.1B — comparação com o schema atual

Em **29/07/2026**, a exportação externa somente de estrutura foi validada pelo
hash aprovado e analisada estaticamente, sem executar SQL ou acessar o banco.

- 64 tabelas, 62 sequências, 113 FKs, 73 índices explícitos e 103 CHECKs
  confirmados;
- 23 tabelas `fc_` alinhadas às migrations 001–011;
- estrutura de `patrimonio` e `grupos_atividade` comprovada;
- ausência real das colunas `id_grupo` registrada;
- versão A de `solicitacoes_alteracao` confirmada pelo banco e pelo código;
- UVR confirmada como vínculo textual em `usuarios.uvr_acesso`, sem tabela
  própria;
- 27 tabelas adicionais classificadas, sem uso SQL identificado no código
  versionado;
- drift legado e decisões funcionais documentados;
- relatório criado em `RELATORIO_COMPARACAO_SCHEMA_ATUAL.md`;
- 673 testes automatizados aprovados, com zero falhas e zero erros;
- dump e manifesto permaneceram fora do Git;
- nenhuma migration, banco, API, Cloudinary ou deploy executado;
- nenhuma credencial registrada.

Prontidão para H2C.2: **C — ainda exige decisões funcionais**. Antes da
baseline, é necessário aprovar o escopo das tabelas adicionais, das colunas
históricas e das regras desejadas para catálogo e patrimônio.

## Etapa H2C.2A — matriz de decisões funcionais concluída

Em **29/07/2026**, foi concluída a análise funcional posterior à comparação do
schema:

- matriz principal criada com situação, decisão, impacto, risco, dependências,
  critério de aceite e etapa sugerida;
- 27 de 27 tabelas adicionais analisadas individualmente;
- preservação integral de estruturas, colunas e dados adotada como regra;
- patrimônio direcionado para inativação e reativação, sem exclusão cotidiana;
- catálogo direcionado para a hierarquia grupo, subgrupo e produto;
- versão A de `solicitacoes_alteracao` mantida como referência oficial;
- transição futura de UVR textual para vínculo por identificador planejada sem
  quebra de compatibilidade;
- colunas históricas preservadas até decisão específica;
- funcionalidades incompletas mantidas ocultas;
- mapa das funcionalidades visíveis e proposta futura de menu documentados;
- backlog dividido em três prioridades e sequência H2C.2B–H2C.2I definida.

O resultado está em `MATRIZ_DECISOES_FUNCIONAIS_H2C2A.md`. A próxima etapa
recomendada é **H2C.2B — especificação funcional detalhada do patrimônio**.

Esta etapa foi exclusivamente documental. Nenhum código, rota, template, teste,
SQL, migration, banco, Cloudinary, API, deploy, commit ou push foi executado.

## Etapa H2C.2B — especificação funcional do patrimônio concluída

Em **29/07/2026**, foi concluída a especificação funcional do patrimônio, ainda
sem implementação:

- funcionamento atual de cadastro, consulta, detalhes, edição e exclusão
  documentado;
- cinco rotas e a área única do painel mapeadas;
- 38 de 38 colunas classificadas;
- `data_cadastro` definida como automática e somente para consulta;
- referência lógica de `transacoes_financeiras.id_patrimonio` registrada;
- risco da exclusão física e da ausência de FK documentado;
- risco de UVR no cadastro atual documentado;
- situações Ativo, Em manutenção, Inativo e Baixado preservadas;
- inativação, reativação, histórico, mensagens e permissões especificados;
- relatórios classificados;
- implementação futura dividida em H2C.3B.1–H2C.3B.9.

O documento principal é
`ESPECIFICACAO_FUNCIONAL_PATRIMONIO_H2C2B.md`. O próximo incremento recomendado
é **H2C.3B.1 — testes de caracterização do patrimônio atual**, somente após
revisão e autorização.

Esta etapa foi exclusivamente documental. Nenhum código funcional, teste, SQL,
migration, banco, Cloudinary, API, deploy, commit ou push foi executado.

## Etapa H2C.2C — especificação funcional do catálogo concluída

Em **29/07/2026**, foi concluída a especificação funcional de grupos, subgrupos
e produtos/serviços, ainda sem implementação:

- funcionamento atual, rotas, tela, consultas e relatórios documentados;
- 21 campos de quatro tabelas centrais ou relacionadas analisados;
- vínculo físico incompleto e dependências textuais registrados;
- importadores e CSVs históricos mapeados sem execução;
- importador antigo classificado como incompatível e não autorizado;
- hierarquia Grupo → Subgrupo → Produto/Serviço especificada;
- estado transitório “Não classificado” e fila administrativa recomendados;
- inativação, reativação, reclassificação e preservação histórica definidas;
- permissões, mensagens, filtros, relatórios e integrações propostos;
- implementação futura dividida em H2C.3C.1–H2C.3C.12.

O documento principal é
`ESPECIFICACAO_FUNCIONAL_CATALOGO_H2C2C.md`. O próximo passo documental
recomendado é **H2C.2D — especificação funcional de UVR**, após revisão humana
da H2C.2C.

A revisão humana da H2C.2C foi aprovada em **29/07/2026**. Ficou definido que
`grupos_atividade` será a referência inicial do cadastro oficial, mas seus
registros deverão ser caracterizados antes do reaproveitamento. A interface
usará o nome amigável “Grupos”, e nenhum dado com finalidade diferente será
adaptado automaticamente.

Esta etapa foi exclusivamente documental. Nenhum código funcional, teste, SQL,
migration, banco, Cloudinary, API, importador, deploy, commit ou push foi
executado.

## Etapa H2C.2D — especificação funcional de UVRs aprovada

Em **30/07/2026**, foi elaborada a especificação funcional do cadastro e vínculo
de UVRs e associações:

- uso textual atual em usuários, cadastros e operações inventariado;
- helpers, rotas, filtros, relatórios, formulários e testes analisados;
- diferença entre UVR e associação mantida como decisão funcional;
- modelos de texto controlado, cadastro central e entidades distintas comparados;
- cadastro conceitual, aliases, inativação e migração gradual propostos;
- impactos em usuários, autorização, patrimônio, financeiro e Fiscalização
  documentados;
- vinte perguntas objetivas preparadas para validação do usuário;
- dados reais e usuário administrador mantidos fora da futura baseline.

As vinte decisões funcionais foram aprovadas em **30/07/2026**. Associação e
UVR serão entidades distintas; uma associação poderá possuir várias UVRs;
usuários poderão ter várias UVRs com uma principal; administradores poderão ser
globais ou limitados a associação; e textos desconhecidos não concederão acesso.

Conta corrente pertencerá à associação, patrimônio separará responsabilidade
institucional da unidade de uso e Fiscalização de Contratos permanecerá global.
A baseline nascerá sem associações, UVRs, aliases, usuários ou credenciais
reais.

O documento principal é `ESPECIFICACAO_FUNCIONAL_UVR_H2C2D.md`. A próxima etapa
é **H2C.2E — Especificação Funcional de Perfis e Permissões**. Permanecem
pendentes o desenho SQL, migration, migração dos textos, implementação das
permissões, auditoria, interfaces e outras decisões da migration-base.

Nenhum código funcional, teste, SQL, migration, banco, dump externo, API,
deploy, commit ou push foi executado.

## Etapa H2C.2E — especificação de perfis, permissões e escopos aprovada

Em **30/07/2026**, foi elaborada a especificação funcional:

- modelo atual com `admin`, `user` e UVR textual inventariado;
- regras centrais, locais, legadas e de autorização por objeto registradas;
- perfil, permissão e escopo separados conceitualmente;
- modelos fixo, configurável e híbrido comparados;
- perfis gerais e especializados, módulos, ações, escopos e conflitos definidos;
- administradores, usuários operacionais e Fiscalização analisados;
- ciclo de vida, auditoria, recuperação de senha e baseline documentados;
- 25 decisões funcionais aprovadas pelo usuário.

O modelo híbrido foi aprovado, com perfis institucionais protegidos, permissões
técnicas estáveis, vários perfis por usuário e concessões vinculadas a escopos.
As 105 rotas funcionais de Fiscalização continuam administrativas, pois nenhuma
permissão foi implementada.

O documento principal é
`ESPECIFICACAO_FUNCIONAL_PERFIS_PERMISSOES_H2C2E.md`. Permanecem pendentes
tabelas, nomes técnicos, tipos, constraints, catálogo final de códigos,
migration, transição de `role` e `uvr_acesso`, decorators, helpers, interfaces,
auditoria técnica, encerramento de sessões e testes.

A próxima etapa recomendada é **H2C.2F — Especificação Final do Fluxo de
Solicitações de Alteração**. A baseline continua bloqueada pelas demais decisões
funcionais e técnicas.

Nenhum código, teste completo, SQL, migration, banco, dump externo, API, deploy,
commit ou push foi executado.

## Etapa H2C.2F — fluxo de solicitações aprovado

Em **30/07/2026**, foi elaborada a especificação funcional final do fluxo:

- versão A e suas nove colunas documentadas;
- rotas de criação, lista, detalhe, aprovação, rejeição e aplicação inventariadas;
- cinco objetos atuais e tipos `EDICAO`/`EXCLUSAO` confirmados;
- versão B classificada como legado incompatível e não executável;
- modelos simples, com eventos e configurável comparados;
- atores, segregação, estados, transições e concorrência avaliados;
- impactos em dados pessoais, UVR, permissões, patrimônio, financeiro, catálogo
  e Fiscalização registrados;
- baseline, migração gradual, auditoria, mensagens e anexos planejados;
- 25 decisões funcionais aprovadas pelo usuário.

A versão A será preservada e evoluída aditivamente com eventos, fotografias,
segregação, concorrência e aplicação atômica. A versão B permanece legado
incompatível e não autorizado. Nenhuma decisão está implementada e nenhuma
estrutura foi criada.

O documento principal é
`ESPECIFICACAO_FUNCIONAL_SOLICITACOES_ALTERACAO_H2C2F.md`. Permanecem pendentes
nomes, tipos, constraints, catálogo técnico, migrations, eventos, interfaces,
aplicação transacional, concorrência técnica, anexos, notificações, permissões,
testes e isolamento físico do script legado.

A próxima etapa recomendada é **H2C.2G — Consolidação das Decisões Funcionais
Pendentes de Patrimônio**. A baseline continua bloqueada por decisões
funcionais e técnicas remanescentes.

Nenhum código, teste completo, SQL, migration, banco, dump externo, API, deploy,
commit ou push foi executado.

## Etapa H2C.2G — decisões patrimoniais aprovadas

Em **30/07/2026**, foi concluída a consolidação documental do patrimônio:

- comportamento atual reconstruído no código;
- 38 de 38 colunas preservadas e classificadas;
- identificação, propriedade, responsabilidade, localização, uso, conservação
  e situação separados conceitualmente;
- regras de número patrimonial, placa, Renavam, série e valores ausentes
  aprovadas;
- duplicidades tratadas por saneamento humano, sem fusão automática;
- estados, transições, inativação, baixa e reversão excepcional aprovados;
- atores, segregação, transferências e compartilhamento aprovados;
- bloqueios, alertas, fotos, documentos, retenção, relatórios e baseline
  aprovados funcionalmente;
- 30 decisões funcionais aprovadas pelo usuário em **30/07/2026**.

O documento principal é
`ESPECIFICACAO_FUNCIONAL_PATRIMONIO_H2C2G.md`. Permanecem pendentes nomes e
tipos técnicos, constraints, índices, catálogos, migrations, migração do legado,
saneamento, permissões, fluxos, interfaces, armazenamento, relatórios e testes.
A exclusão física cotidiana permanece risco legado até a implementação.

A próxima etapa recomendada é **H2C.2H — Consolidação das Decisões Funcionais
Pendentes do Catálogo**. A baseline continua bloqueada pelas decisões técnicas e
funcionais remanescentes, e o sistema não está pronto para produção.

Esta etapa alterou somente documentação. Não foram executados suíte completa,
SQL, migration, banco, dump externo, `.env`, API, deploy, commit ou push.

## Etapa H2C.2H — consolidação funcional do catálogo aprovada

Em **30/07/2026**, foi concluída a análise documental das decisões pendentes de
grupos, subgrupos e produtos/serviços:

- comportamento atual, quatro tabelas e dependências financeiras inventariados;
- natureza financeira diferenciada da hierarquia do catálogo;
- alternativas de códigos, nomes, unidades, unicidade e normalização avaliadas;
- estados, inativação, reativação, reclassificação e fotografia histórica
  especificados como propostas;
- tratamento de não classificados, aliases, `produtos`, CSVs e importadores
  delimitado;
- atores, segregação, exclusão física, relatórios e baseline avaliados;
- 35 perguntas objetivas preparadas para decisão humana.

As 35 decisões foram aprovadas integralmente em **30/07/2026**. O modelo separa
natureza financeira da hierarquia Grupo → Subgrupo → Produto/Serviço, estabelece
tipos, códigos, descrição, unidades, estados, inativação sem cascata,
reclassificação formal, fotografia histórica, gestão central e dupla aprovação
para alto impacto.

O documento principal é
`ESPECIFICACAO_FUNCIONAL_CATALOGO_H2C2H.md`. Permanecem pendentes somente
detalhes e implementação técnica. Não houve alteração de código, banco, dump,
SQL, migration, CSV, importador, API ou deploy. A suíte completa de 673 testes
não foi repetida porque a alteração é exclusivamente documental.

A próxima etapa recomendada é **H2C.2I — Delimitação Funcional das Tabelas
Adicionais do Banco**. O sistema não está declarado pronto para produção.

## Etapa H2C.2I — delimitação das tabelas adicionais aprovada

Em **30/07/2026**, foi aberto o inventário documental das 27 tabelas adicionais,
agrupadas em auditoria, pessoa física, documentos/entregas, EPI, Ouvidoria e
`produtos` legado.

Nenhuma possui uso operacional localizado no repositório, mas isso não autoriza
considerá-la órfã ou removê-la. Nenhuma foi aprovada para a baseline; nenhuma
foi autorizada para exclusão. `produtos` continua fora da baseline conforme a
H2C.2H.

As 40 decisões foram aprovadas em **30/07/2026** e as 27 tabelas receberam
classificação individual. A baseline nuclear foi adotada; auditoria funcional,
documentos/entregas e EPI foram delimitados como módulos opcionais; Ouvidoria,
cadastro mestre e legados ficaram fora do núcleo ou adiados. Nenhuma tabela foi
autorizada para remoção.

O documento principal é
`ESPECIFICACAO_FUNCIONAL_TABELAS_ADICIONAIS_H2C2I.md`. A baseline técnica,
migrations opcionais, substitutas, migração, somente leitura, arquivamento,
permissões, interfaces e testes permanecem pendentes. Não houve alteração de
código, banco, dump, SQL, migration, CSV, importador, API ou deploy.

A próxima etapa recomendada é **H2C.2J — Consolidação das Colunas Adicionais e
do Escopo Final da Baseline**. O sistema não está pronto para produção.

## Etapa H2C.2J — concluída e aprovada funcionalmente

Em **31/07/2026**, as 50 decisões funcionais foram aprovadas, o Modelo C foi
adotado e o escopo funcional da baseline nuclear foi consolidado.

Os principais bloqueios funcionais foram encerrados. Nenhuma implementação foi
realizada: nenhuma coluna foi removida e nenhuma tabela, código, banco, dump,
SQL, migration, CSV, importador, API ou deploy foi alterado ou acessado.

A próxima etapa recomendada é **H2C.3A — Consolidação Técnica do Schema da
Baseline Nuclear**, ainda inicialmente documental, para definir tabelas,
colunas, tipos PostgreSQL, chaves, constraints, índices, históricos, catálogos,
dependências, ordem de criação e estratégia técnica de migrations. O sistema
não está pronto para produção.

## Etapa H2C.3A — concluída e aprovada tecnicamente

Em **31/07/2026**, foram aprovadas as 30 decisões técnicas do schema nuclear:
convenções, identificadores, tipos, tabelas, relacionamentos, constraints,
índices, históricos, deletes, dados estruturais, ordem de criação e
compatibilidade das 23 tabelas `fc_*`.

A fase funcional e o modelo técnico conceitual estão aprovados. CPF foi ajustado
para `VARCHAR(11)` ou `TEXT` e checksum textual para `VARCHAR(64)`, ambos com
CHECK; `CHAR` foi descartado nesses casos. Nenhuma migration, SQL, tabela,
bootstrap ou código foi criado; nenhum banco, dump, `.env`, dado real ou serviço
foi acessado.

Próxima etapa: **H2C.3B — Revisão Técnica Independente do Schema e da Estratégia
de Migrations**. A baseline não está pronta para implementação nem produção antes
dessa revisão.

## Etapa H2C.3B — revisão independente concluída e aprovada

Em **31/07/2026**, a proposta H2C.3A foi confrontada com código, documentação e
migrations 001–011. O parecer final **APROVADO COM AJUSTES** foi aprovado.
Foram registradas e aprovadas 20 decisões e 24 achados: 4 bloqueadores, 10 de
prioridade alta, 5 de prioridade média, 3 de prioridade baixa e 2 informativos.

Os bloqueadores concentram-se na compatibilidade do código atual com o novo
modelo de autorização, bootstrap/ledger, ordem de dependências, normalização,
rateios e estratégia única para as 23 tabelas `fc_*`. Nenhum bloqueador foi
implementado; nenhuma migration, SQL, tabela ou código foi criado; nenhum banco,
dump, `.env`, dado real ou serviço foi acessado. Testes PostgreSQL não foram
executados.

Próxima etapa: **H2C.3C**, exclusivamente documental, para incorporar os ajustes
aprovados ao desenho. A baseline continua sem autorização para implementação ou
produção.

## Etapa H2C.3C — especificação física aprovada documentalmente

Em **31/07/2026**, foram aprovados documentalmente o catálogo físico final
proposto e o plano detalhado das migrations. O parecer é **APTA PARA CONGELAMENTO
DOCUMENTAL E CONFERÊNCIA FINAL PRÉ-IMPLEMENTAÇÃO**. A proposta contém 82 tabelas
nucleares, 58 tabelas novas, 1.104 especificações de colunas, matrizes de
constraints, índices e DELETE, além de ledger, execuções, manifesto, preflight,
advisory lock, sequência planejada, dados estruturais e plano de testes.

Os quatro bloqueadores e os 24 achados da H2C.3B possuem tratamento localizado;
nenhum bloqueador foi implementado. As 23 tabelas `fc_*` continuam vinculadas
exclusivamente às migrations históricas 001–011. As 24 decisões físicas foram
aprovadas. O ledger de duas tabelas, autorregistro transacional, manifesto JSON,
checksum UTF-8/LF, preflight em três situações e advisory lock foram consolidados.

A aprovação permanece exclusivamente documental; nenhum SQL, migration, manifesto real,
código, tabela, bootstrap ou teste PostgreSQL foi criado ou executado. Nenhum
banco, dump, `.env`, dado real ou serviço externo foi acessado. Próxima etapa:
**H2C.3D — Conferência Final Pré-Implementação**. A baseline continua sem
autorização para implementação.

## Etapa H2C.3D — conferência final aprovada documentalmente

Em **31/07/2026**, foi concluída a conferência documental pré-implementação. O
parecer final aprovado é **C — NÃO APROVADA PARA IMPLEMENTAÇÃO**. Foram
confirmadas 82 tabelas e 58 novas; após uma fusão, uma
separação e ajuste de FKs, a contagem recomendada é 1.103 colunas.

O hash UTF-8/LF da migration 001 precisa de correção documental; os outros dez
hashes coincidem. As matrizes do núcleo precisam ser expandidas por coluna, FK,
constraint, índice e DELETE. Os quatro bloqueadores continuam não implementados.
Nenhuma nova decisão humana foi necessária.

Nenhuma migration, SQL, manifesto real, código, tabela ou teste PostgreSQL foi
criado ou executado; nenhum banco, dump, `.env`, dado real ou serviço externo
foi acessado. Próxima etapa obrigatória: **H2C.3E — Correção e Completude da
Especificação Física**, inicialmente documental.
