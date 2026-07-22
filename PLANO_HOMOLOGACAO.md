# Plano de Homologação Online

Data do diagnóstico: **22/07/2026**
Repositório analisado: `C:\sistema-recic3`
Branch analisada: `codex/modulo-fiscalizacao-contratos`
Commit analisado: `5ac68bb1cac7c72a145ab3ce69c99deebbcb27fb`

Este documento é apenas um plano. Nenhum serviço foi publicado, nenhum banco ou
Cloudinary real foi acessado e nenhuma migration foi executada nesta etapa.

## 1. Situação atual do projeto

O repositório está limpo e sincronizado com a branch remota. Estão salvos no
histórico:

- implementação da Etapa 2J;
- lançamento de nota fiscal com upload direto ou seleção de documento existente;
- compensação do arquivo recém-enviado quando a gravação no banco falha;
- migração 011 e registro de sua aplicação e verificação;
- testes manuais da Etapa 2J;
- simplificação visual do painel inicial.

A Etapa 2J está completa no código: criação e edição de atestes, notas fiscais,
documentos complementares, ateste, devolução, retorno para elaboração,
encaminhamento documental, cancelamento e eventos históricos. Revisões de medição
são bloqueadas quando existe ateste ativo. Documentos antigos são preservados e
o serviço de atestes não utiliza `DELETE`. As telas deixam claro que encaminhar
para pagamento não significa que o pagamento foi realizado.

A migração 011 prevê as tabelas `fc_atestes`, `fc_ateste_notas_fiscais`,
`fc_ateste_documentos` e `fc_ateste_eventos`.

Resultado técnico desta análise: **311 testes aprovados, 0 falhas e 0 erros**.
Foram verificados também 69 arquivos Python, todos com sintaxe válida.

### Conclusão de prontidão

O projeto **ainda não deve ser publicado na internet**, nem mesmo para
homologação, antes da correção dos bloqueadores descritos na seção 13. O módulo
novo está funcional; os principais riscos estão na preparação do ambiente e no
sistema principal que envolve o módulo.

## 2. Estrutura de implantação identificada

Há sinais de uso anterior do **Render** no histórico e existe um `Procfile` com:

```text
web: gunicorn app:app
```

O `requirements.txt` inclui Flask, Flask-Login, PostgreSQL, Gunicorn, Cloudinary e
outras bibliotecas. Não existem `render.yaml`, `runtime.txt`, Dockerfile ou
configuração de Railway/Fly.io versionados.

Situação encontrada:

- servidor WSGI: Gunicorn;
- aplicação: objeto Flask `app` no arquivo `app.py`;
- templates e arquivos estáticos do módulo: configurados no Blueprint;
- porta: não está explícita no `Procfile`;
- versão do Python: não está fixada;
- versões das dependências: não estão fixadas;
- `DEBUG`: o Gunicorn não ativa o trecho `app.run(debug=True)`, mas a execução
  direta de `app.py` ainda ativa o modo de depuração;
- `ProxyFix`, hosts confiáveis e cabeçalhos de segurança: não configurados;
- health check HTTP: inexistente.

O Render continua compatível com Flask e recomenda Gunicorn, variáveis secretas,
porta fornecida por `PORT` e health check configurável. Antes de publicar, é
recomendado tornar explícito o comando conceitual:

```text
gunicorn --bind 0.0.0.0:$PORT app:app
```

Também é recomendado criar um `render.yaml` somente depois que as decisões deste
plano forem aprovadas. Isso tornará o ambiente reproduzível e desligável.

## 3. Arquitetura proposta

Arquitetura recomendada para a primeira homologação:

1. Uma aplicação web própria de homologação no Render, ligada apenas à branch
   `codex/modulo-fiscalizacao-contratos` ou a uma futura branch exclusiva.
2. Um projeto Neon separado, preferencialmente, ou uma branch Neon criada sem
   dados reais e com conexão própria.
3. Um product environment Cloudinary separado, com credenciais exclusivas.
4. Uma barreira de acesso global antes do login do sistema.
5. Apenas usuários fictícios e autorizados.
6. HTTPS obrigatório, `DEBUG` desligado e cookies seguros.
7. Health check mínimo sem consulta ao banco.
8. Migrations executadas por comando controlado, nunca durante a importação da
   aplicação.

Essa separação permite desligar o serviço, apagar o banco e remover todos os
arquivos de teste sem atingir o ambiente atualmente utilizado.

## 4. Banco PostgreSQL separado

### Estratégia recomendada

Usar um **projeto Neon exclusivo de homologação** é a opção mais segura. Uma
branch Neon exclusiva também oferece conexão isolada, mas um projeto separado
reduz ainda mais o risco operacional de escolher a branch errada.

Requisitos:

- `DATABASE_URL` exclusiva, nunca igual à atual;
- nome visível contendo `homologacao`;
- usuário/role exclusivo e senha exclusiva;
- SSL obrigatório na conexão;
- permissão somente no banco de homologação;
- nenhum dado pessoal, documento ou arquivo real;
- contagens e nomes do ambiente conferidos antes de qualquer migration;
- backup antes de qualquer futura alteração em produção.

### Problema para banco vazio

As migrations do módulo dependem da tabela `usuarios`. O sistema principal possui
SQL de criação de tabelas dentro de `criar_tabelas_se_nao_existir()`, mas não há
uma migration-base formal e reproduzível para o esquema principal. Além disso,
`migrar_dados_antigos_produtos()` é executada automaticamente ao importar
`app.py`, podendo inserir e atualizar dados durante a inicialização do Gunicorn.

Antes da homologação deve ser criada e revisada uma forma segura de:

1. criar o esquema-base do sistema principal, inclusive `usuarios`;
2. registrar a versão desse esquema;
3. aplicar as migrations 001 a 011 em ordem;
4. criar dados fictícios por um comando separado;
5. iniciar o Gunicorn sem executar migração ou alteração de dados.

### Recriação

O procedimento futuro deve permitir: desligar a aplicação, revogar a conexão,
apagar o projeto/branch de homologação, criar outro vazio, aplicar o esquema-base,
aplicar migrations verificadas e inserir apenas o conjunto fictício.

## 5. Cloudinary separado

O serviço atual já usa:

- `resource_type="raw"`;
- `type="authenticated"`;
- identificador UUID;
- `overwrite=False`;
- URL privada assinada com expiração de cinco minutos;
- compensação do upload novo se o banco falhar.

O caminho atual começa em `sistema-recic3/fiscalizacao-contratos/` e não possui
um prefixo configurável por ambiente. Por isso, a recomendação é usar um
**product environment Cloudinary exclusivo de homologação**, com outro cloud
name, chave e segredo.

Se não for possível, será necessário implementar e testar antes do deploy uma
variável como `FC_CLOUDINARY_PREFIX`, obrigatoriamente definida como algo
inequívoco, por exemplo `homologacao/fiscalizacao-contratos`. Nesse cenário, os
arquivos também devem receber uma tag exclusiva de homologação. Sem essa mudança,
reutilizar as credenciais atuais misturaria arquivos de teste e reais.

Ao desligar o ambiente, apagar somente o product environment exclusivo ou os
arquivos identificados simultaneamente pelo prefixo e pela tag de homologação.
Nunca apagar por um prefixo genérico.

## 6. Variáveis de ambiente

Nenhum valor real deve ser copiado. Segredos devem ser cadastrados diretamente
no provedor.

| Variável | Situação | Sensível | Uso em homologação |
|---|---|---:|---|
| `DATABASE_URL` | Obrigatória | Sim | Exclusiva do PostgreSQL de homologação; não reutilizar credencial |
| `SECRET_KEY` | Obrigatória | Sim | Exclusiva, longa e aleatória; o código deve falhar se ausente |
| `CLOUDINARY_CLOUD_NAME` | Obrigatória para documentos | Sim | Product environment exclusivo |
| `CLOUDINARY_API_KEY` | Obrigatória para documentos | Sim | Credencial exclusiva |
| `CLOUDINARY_API_SECRET` | Obrigatória para documentos | Sim | Credencial exclusiva |
| `FC_MAX_UPLOAD_MB` | Opcional | Não | Limite do módulo; padrão atual de 20 MB |
| `PORT` | Fornecida pelo Render | Não | Usada pelo comando do Gunicorn |
| `PYTHON_VERSION` | Recomendada | Não | Fixar uma versão testada |
| `GUNICORN_CMD_ARGS` | Opcional | Não | Workers, timeout e logs, se não forem definidos no comando |

Variáveis existentes no `.env` local relacionadas a SMTP não são lidas pelo
código atual e não devem ser copiadas para a homologação sem uma funcionalidade
que realmente precise delas.

O código também não lê `FLASK_ENV` ou `FLASK_DEBUG`; esses nomes não devem ser
usados para ativar depuração online. As opções de cookie e sessão ainda são
fixadas pelos padrões do Flask, sem variáveis próprias. O limite global do Flask
está fixo em 64 MB no código, enquanto o módulo aplica `FC_MAX_UPLOAD_MB` antes do
Cloudinary. BrasilAPI, OpenCNPJA, Bootstrap e Font Awesome usam endereços definidos
no código/templates e não possuem variável de ambiente atualmente.

Variáveis propostas, mas **ainda não implementadas**:

| Variável proposta | Finalidade |
|---|---|
| `APP_ENV=homologacao` | Identificar o ambiente e ativar configurações seguras |
| `HOMOLOGACAO_GATE_USER` | Usuário da barreira global temporária |
| `HOMOLOGACAO_GATE_PASSWORD` | Senha exclusiva da barreira global |
| `FC_CLOUDINARY_PREFIX` | Separar arquivos caso não haja product environment exclusivo |

Credenciais do administrador interno não devem permanecer como variáveis fixas.
O usuário deve ser criado por comando único, com senha aleatória transmitida fora
do Git e troca obrigatória no primeiro acesso.

O `.env.example` atual já representa todas as variáveis lidas pelo código atual;
por isso não foi alterado nesta etapa.

## 7. Controle de acesso

As **105 rotas do módulo** foram verificadas e todas usam `admin_required`.
Visitantes são direcionados ao login e usuários comuns recebem 403.
Não foi encontrada rota pública para cadastro de usuários; a criação atual é
feita por scripts administrativos e deve ser substituída por um comando seguro
para a homologação.

O sistema principal, porém, possui várias rotas sem `login_required`, inclusive
rotas que consultam relatórios e rotas que gravam dados. Portanto, publicar o
projeto inteiro usando somente o login atual não é seguro.

Solução temporária recomendada antes da homologação:

1. colocar uma barreira global na aplicação ou um serviço de identidade no proxy;
2. proteger todas as rotas, exceto `/health`;
3. usar credenciais exclusivas e HTTPS;
4. manter o login interno e o papel `admin` como segunda barreira;
5. enviar `X-Robots-Tag: noindex, nofollow, noarchive` em todas as respostas;
6. disponibilizar somente contas autorizadas, sem cadastro público.

Para uma solução simples no próprio projeto, pode ser implementada autenticação
HTTP Basic em um `before_request`, habilitada somente quando
`APP_ENV=homologacao`. Uma opção mais robusta é usar Cloudflare Access ou outro
gateway de identidade na frente do Render. A solução não foi implementada nesta
tarefa.

Configurações necessárias antes de expor o serviço:

- `SESSION_COOKIE_SECURE=True`;
- `SESSION_COOKIE_HTTPONLY=True`;
- `SESSION_COOKIE_SAMESITE='Lax'`;
- tempo de sessão controlado;
- proteção CSRF nos formulários que alteram dados;
- `ProxyFix` somente com a quantidade correta de proxies confiáveis;
- hosts confiáveis;
- HSTS, CSP, `X-Content-Type-Options` e proteção contra iframe;
- rate limit no login;
- mensagens genéricas sem traceback ou SQL.

## 8. Health check

Não existe rota de health check. Antes do deploy, propor e testar:

```http
GET /health
```

Resposta mínima:

```json
{"status":"ok"}
```

Essa rota deve responder 200 sem banco, não revelar versão, caminhos, hostname,
variáveis ou credenciais e ser a única exceção à barreira global. Uma verificação
de banco, se desejada futuramente, deve ficar em outro endpoint de prontidão com
acesso controlado.

## 9. Migrations do módulo

Todas as onze migrations são aditivas, usam transação e não contêm `DROP`,
`TRUNCATE`, `DELETE`, `UPDATE`, `INSERT` ou `ALTER TABLE`.

| Nº | Arquivo | Estruturas principais | Dependências anteriores |
|---:|---|---|---|
| 001 | `001_criar_fc_empresas.sql` | `fc_empresas` | `usuarios` |
| 002 | `002_criar_fc_servidores.sql` | `fc_servidores` | `usuarios` |
| 003 | `003_criar_fc_contratos.sql` | `fc_contratos`, `fc_contrato_responsaveis` | 001, 002, `usuarios` |
| 004 | `004_criar_fc_aditivos.sql` | `fc_aditivos` | 003, `usuarios` |
| 005 | `005_criar_fc_documentos.sql` | `fc_documentos` | 003, 004, `usuarios` |
| 006 | `006_criar_fc_planilhas_orcamentarias.sql` | `fc_planilhas_orcamentarias`, `fc_planilha_itens` | 003, 004, `usuarios` |
| 007 | `007_criar_fc_ativos_contratuais.sql` | `fc_ativos_contratuais`, `fc_ativo_vinculos` | 001, 003, `usuarios` |
| 008 | `008_criar_fc_fiscalizacoes_ocorrencias.sql` | `fc_fiscalizacoes`, `fc_ocorrencias`, `fc_ocorrencia_acompanhamentos` | 003, 002, 007, `usuarios` |
| 009 | `009_criar_fc_fiscalizacao_eventos.sql` | `fc_fiscalizacao_eventos` | 008, `usuarios` |
| 010 | `010_criar_fc_medicoes.sql` | cinco tabelas de medição | 003, 002, 005, 006, 008, `usuarios` |
| 011 | `011_criar_fc_atestes.sql` | quatro tabelas de ateste | 010, 002, 005, `usuarios` |

Não existe tabela formal de controle de migrations. `IF NOT EXISTS` reduz o
risco de recriar estruturas, mas não registra data, checksum nem detecta uma
tabela existente com estrutura divergente.

Antes da homologação, recomenda-se criar `schema_migrations` ou adotar uma
ferramenta formal. O controle mínimo deve registrar arquivo, checksum, data e
executor. O aplicador deve:

1. validar que está no ambiente de homologação;
2. obter lock para impedir duas execuções simultâneas;
3. conferir checksum;
4. aplicar somente migrations pendentes, na ordem numérica;
5. registrar sucesso dentro da mesma transação;
6. parar imediatamente em qualquer erro.

## 10. Dados fictícios mínimos

Depois do banco estar preparado, criar por comando separado:

- administrador `admin_homologacao`, sem nome ou senha real;
- servidores `Servidor Fictício Alfa` e `Servidor Fictício Beta`, matrículas
  `HML-001` e `HML-002`;
- `Empresa Fictícia de Homologação Ltda.`, com identificador de teste validado e
  previamente conferido para não corresponder a empresa real;
- contrato `HML-001/2026`;
- planilha `Planilha Fictícia Inicial`, com dois itens sem referência real;
- ativo `ATIVO-HML-001`, sem placa, chassi ou patrimônio real;
- uma fiscalização de rotina fictícia;
- uma ocorrência claramente marcada como simulação;
- uma medição com valores pequenos e fictícios;
- um ateste vinculado à medição;
- PDFs simples com a marca d'água `DOCUMENTO FICTÍCIO — HOMOLOGAÇÃO`.

Não usar CPF, CNPJ, placa, nota fiscal, endereço, telefone, e-mail, nome de pessoa
ou documento reais. O seed deve ser idempotente ou abortar se já tiver sido
aplicado.

## 11. Roteiro futuro de publicação

Executar somente em etapa posterior e com autorização específica:

1. corrigir todos os bloqueadores;
2. rotacionar as credenciais que já apareceram no histórico do Git;
3. fixar Python e dependências;
4. criar configuração segura por ambiente;
5. criar o banco exclusivo e conferir sua identidade sem exibir a URL;
6. aplicar esquema-base e migrations com controle formal;
7. criar dados fictícios;
8. criar Cloudinary exclusivo e testar com um arquivo fictício;
9. criar o serviço Render apontando para a branch autorizada;
10. configurar segredos diretamente no painel do provedor;
11. ativar barreira global, HTTPS, cookies seguros e `noindex`;
12. configurar `/health`;
13. publicar com auto-deploy inicialmente desligado;
14. executar o roteiro de validação;
15. liberar acesso apenas aos homologadores autorizados.

## 12. Roteiro de validação

Após uma futura publicação:

1. confirmar que acesso sem a barreira global é recusado;
2. confirmar que `/health` responde somente `{"status":"ok"}`;
3. confirmar HTTPS e atributos do cookie;
4. confirmar que usuário comum recebe 403 no módulo;
5. executar fluxo completo com dados fictícios;
6. enviar, abrir e compensar um documento fictício;
7. confirmar o prefixo/product environment do Cloudinary;
8. confirmar que o banco utilizado é o exclusivo de homologação;
9. verificar logs sem SQL, traceback, senha, URL assinada ou dados pessoais;
10. testar celular e computador;
11. executar novamente os testes automatizados;
12. registrar aprovação ou desligar o ambiente se houver risco.

## 13. Riscos

### Bloqueadores para homologação

1. **Credenciais no histórico:** o `.env` não é rastreado hoje, mas esteve em
   commits antigos. Todos os segredos que já estiveram nele devem ser considerados
   expostos e rotacionados antes do deploy. Não foi feita reescrita destrutiva.
2. **Rotas públicas no sistema principal:** existem consultas e operações de
   escrita sem `login_required`. É obrigatória uma barreira global temporária.
3. **Alteração automática no startup:** `migrar_dados_antigos_produtos()` roda na
   importação de `app.py`. Deve ser removida do startup e transformada em comando
   explícito antes do Gunicorn.
4. **Banco vazio não reproduzível:** falta uma migration-base do sistema principal
   e as migrations do módulo dependem de `usuarios`.
5. **Sem controle formal de migrations:** não há ledger com versão e checksum.
6. **Segredos não falham de forma segura:** `SECRET_KEY` tem fallback de
   desenvolvimento e a conexão possui fallback local. Em ambiente online, a
   ausência das variáveis deve interromper a inicialização.
7. **Arquivos não estão separados por ambiente:** o prefixo Cloudinary é fixo.
   Usar product environment exclusivo ou implementar prefixo obrigatório.
8. **Proteção web incompleta:** faltam CSRF, cookies seguros e uma política
   explícita para proxy/HTTPS antes da exposição à internet.
9. **Build não reproduzível:** versões das bibliotecas e do Python não estão
   fixadas.

### Corrigir antes da produção oficial

- proteger individualmente todas as rotas do sistema principal;
- evitar respostas que exibem a exceção interna ou SQL ao usuário;
- revisar logs que registram consultas, parâmetros e identificadores;
- remover senhas literais de scripts utilitários e desativar scripts antigos;
- implementar rate limit e auditoria do login;
- revisar exclusões físicas existentes no sistema principal;
- configurar headers de segurança, hosts confiáveis e `ProxyFix` corretamente;
- impedir execução direta com `debug=True` fora do ambiente local;
- avaliar CSP/SRI para bibliotecas carregadas por CDN;
- definir backup, retenção, monitoramento e resposta a incidentes.

### Melhorias recomendadas

- versionar um `render.yaml` depois das correções;
- adicionar `/health` e, futuramente, um readiness check interno;
- automatizar criação e destruição de ambientes temporários;
- adicionar CI no GitHub para testes e auditoria de segredos;
- documentar responsáveis, validade e custo do ambiente;
- adicionar política automática de expiração de dados fictícios.

## 14. Procedimento para desligar o ambiente

1. bloquear novos acessos;
2. desativar auto-deploy;
3. revogar usuário e senha da barreira global;
4. suspender ou apagar o serviço web de homologação;
5. revogar a `DATABASE_URL` e apagar somente o projeto/branch de homologação;
6. revogar as chaves Cloudinary de homologação;
7. apagar somente o product environment exclusivo ou prefixo/tag exclusivos;
8. confirmar que produção e o ambiente atual continuam intactos;
9. registrar a data e o responsável pelo desligamento.

## 15. Itens que podem ficar para a produção oficial

Depois que os bloqueadores de homologação forem resolvidos, podem ficar para uma
fase posterior, desde que documentados:

- domínio definitivo e DNS;
- política completa de backup e recuperação;
- observabilidade avançada e alertas;
- alta disponibilidade e escala;
- autenticação corporativa/SSO;
- CDN ou domínio próprio;
- automação completa de preview environments;
- política formal de retenção de documentos.

## Referências técnicas consultadas

- [Render — Web Services](https://render.com/docs/web-services)
- [Render — Deploy de Flask](https://render.com/docs/deploy-flask)
- [Render — Health Checks](https://render.com/docs/health-checks)
- [Render — Variáveis e segredos](https://render.com/docs/configure-environment-variables)
- [Neon — Fluxo com branches](https://neon.com/docs/get-started-with-neon/workflow-primer)
- [Cloudinary — Controle de acesso a arquivos](https://cloudinary.com/documentation/control_access_to_media)
- [Flask — Segurança](https://flask.palletsprojects.com/en/stable/web-security/)
- [Flask — Aplicação atrás de proxy](https://flask.palletsprojects.com/en/stable/deploying/proxy_fix/)
