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
`migrar_dados_antigos_produtos()` permanece como rotina manual legada. O código
e os testes atuais comprovam que ela não é executada ao importar `app.py` nem
durante a inicialização do Gunicorn.

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

Variáveis implementadas na Etapa H2A.1:

| Variável proposta | Finalidade |
|---|---|
| `APP_ENV` | Identificar `development`, `testing`, `homologation` ou `production` |
| `APP_DEBUG` | Ativar depuração somente no desenvolvimento local |
| `TRUST_PROXY` | Autorizar explicitamente a confiança em um proxy |
| `HOMOLOGATION_GATE_ENABLED` | Ativar a barreira global somente em homologação |
| `HOMOLOGATION_GATE_USER` | Usuário da barreira global temporária |
| `HOMOLOGATION_GATE_PASSWORD` | Senha exclusiva da barreira global |
| `CLOUDINARY_FOLDER_PREFIX` | Separar novos arquivos por ambiente |

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

Foi implementada autenticação HTTP Basic temporária em um `before_request`,
habilitada somente quando `APP_ENV=homologation` e
`HOMOLOGATION_GATE_ENABLED=true`. A rota `/health` é a única exceção. O login
interno e as permissões continuam sendo a segunda camada. Uma opção mais
robusta, ainda futura, é usar Cloudflare Access ou outro gateway de identidade
na frente do Render.

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

A Etapa H2A.1 criou e testou:

```http
GET /health
```

Resposta mínima:

```json
{"status":"ok"}
```

Essa rota responde 200 sem banco, não revela versão, caminhos, hostname,
variáveis ou credenciais e é a única exceção à barreira global. Uma verificação
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

## 16. Implementação da Etapa H2A.1

Em **22/07/2026**, foi preparada a inicialização protegida e o isolamento da
futura homologação, sem deploy e sem acesso a PostgreSQL ou Cloudinary reais.

Foram implementados:

- identificação explícita por `APP_ENV`, com os valores `development`,
  `testing`, `homologation` e `production`; quando ausente, assume
  `development` apenas para compatibilidade local, enquanto valores desconhecidos
  interrompem a inicialização;
- falha clara quando `SECRET_KEY` estiver ausente em qualquer ambiente e quando
  `DATABASE_URL` estiver ausente em homologação ou produção;
- remoção do banco local e da chave previsível usados anteriormente como
  alternativas silenciosas;
- retirada da chamada automática de `migrar_dados_antigos_produtos()` durante a
  importação de `app.py`;
- script administrativo separado, com confirmação textual obrigatória, que não
  deve ser executado automaticamente pelo Render;
- `GET /health`, com resposta JSON mínima e sem consulta a banco ou Cloudinary;
- `ProxyFix` para exatamente um proxy, aplicado somente com
  `TRUST_PROXY=true`, confiando apenas no endereço de origem e protocolo;
- cookies de sessão e de "lembrar-me" com `HttpOnly` e `SameSite=Lax` em todos
  os ambientes e `Secure` em homologação e produção;
- esquema HTTPS preferencial nos ambientes online e `DEBUG` sempre desligado
  neles;
- cabeçalhos `X-Content-Type-Options`, `X-Frame-Options` e `Referrer-Policy`,
  além de HSTS conservador em requisições HTTPS online, sem `preload` e sem
  `includeSubDomains`;
- barreira HTTP Basic temporária somente para homologação, com comparação
  segura, resposta genérica e exceção exclusiva para `/health`;
- prefixo configurável para novos uploads por `CLOUDINARY_FOLDER_PREFIX`, com
  normalização de barras, rejeição de `.`, `..` e barras invertidas, validação
  de configuração parcial e composição centralizada;
- preservação das chaves de documentos antigos e do mesmo `public_id` na
  limpeza compensatória de uploads novos;
- atualização do `.env.example` somente com valores fictícios.

Os testes usam `APP_ENV=testing`, chave fictícia e bloqueios globais para
PostgreSQL e Cloudinary. Foram aprovados os **311 testes anteriores** e **59
testes novos**, totalizando **370 testes, 0 falhas e 0 erros**. Nenhuma migration
foi executada e nenhum dado ou arquivo real foi alterado.

### Riscos que permanecem pendentes

- fixação da versão do Python;
- fixação das versões das dependências;
- migration-base reproduzível do sistema principal;
- controle formal das migrations com versão e checksum;
- rotação das credenciais presentes no histórico;
- criação do banco Neon e do ambiente Cloudinary exclusivos de homologação;
- configuração e deploy no Render;
- revisão individual das rotas públicas do sistema principal;
- CSP, hosts confiáveis, rate limit, monitoramento e resposta a incidentes.

## 17. Implementação da Etapa H2A.2 — proteção CSRF

Em **22/07/2026**, foi implementada proteção CSRF global com
`Flask-WTF==1.2.1`, `CSRFProtect` e `CSRFError`. A proteção permanece ativa em
desenvolvimento, testes, homologação e produção, usando a `SECRET_KEY` já
obrigatória. Não foi criada uma segunda chave.

Foram protegidos os **61 formulários POST** encontrados, incluindo login,
logout, cadastros, alterações de estado e os **cinco formulários multipart**.
Quatro desses formulários atualmente possuem campo de arquivo; o quinto conserva
um `enctype` legado, mas também está protegido. As planilhas orçamentárias atuais
são preenchidas por campos do formulário e não possuem rota de importação de
arquivo. Formulários GET de
pesquisa e filtros não receberam token. As **105 rotas mutáveis** do projeto
foram inventariadas; autenticação, `admin_required`, validações de estado e
transações existentes continuam independentes da validação CSRF.

O logout, que antes alterava a sessão por GET, passou a aceitar somente POST e
o link foi substituído por um pequeno formulário protegido. As demais rotas com
GET e POST auditadas usam o GET apenas para mostrar o formulário de confirmação
e executam a mudança somente no POST.

O layout fornece o token em uma meta tag. Requisições AJAX/JSON do mesmo domínio
enviam `X-CSRFToken` somente nos métodos POST, PUT, PATCH e DELETE. O token não é
enviado a serviços externos, colocado em URLs ou registrado em logs. Uploads
continuam usando o formulário multipart e são recusados antes de banco ou
Cloudinary quando o token não é válido.

Falhas de CSRF retornam HTTP 400 em uma página simples e genérica, sem informar
se o token estava ausente, vencido ou incorreto. O log registra somente que a
validação foi recusada. Não existem rotas isentas de CSRF; `GET /health` não
precisa de token porque não altera dados. Foi mantida a validade padrão
conservadora do Flask-WTF, de uma hora.

Nos testes, CSRF não é desabilitado globalmente. Um cliente auxiliar abre o
login, obtém um token real na mesma sessão e o envia nos POSTs históricos; testes
negativos usam o cliente original para omitir ou adulterar o token. A barreira
Basic da homologação, o login interno e a permissão administrativa continuam
camadas independentes.

A revisão técnica final preservou os **370 testes anteriores** e aprovou **37
testes específicos de CSRF**, totalizando **407 testes, 0 falhas e 0 erros**.
Foram verificados tokens ausentes, inválidos, vazios, expirados, duplicados,
malformados e de outra sessão, além das onze áreas administrativas. PostgreSQL e
Cloudinary reais permaneceram bloqueados. Nenhuma migration ou deploy foi
executado.

Continuam pendentes a fixação geral das versões do Python e dependências, uma
migration-base reproduzível, controle formal das migrations, rotação de
credenciais históricas, Neon e Cloudinary exclusivos de homologação, deploy no
Render, revisão e autorização individual das rotas públicas, CSP, rate limit,
trusted hosts e monitoramento.

## 18. Etapa H2A.3A — inventário e matriz de acesso

Em **22/07/2026**, foi executado o inventário completo das rotas registradas na
aplicação. O resultado detalhado está em [`MATRIZ_ROTAS_ACESSO.md`](MATRIZ_ROTAS_ACESSO.md),
com uma linha para cada rota, sua proteção atual, dados envolvidos, risco e nível
de acesso recomendado.

Foram encontradas **177 regras de rota**: 34 públicas, 38 protegidas por
`login_required` e 105 protegidas por `admin_required`. A proposta reduz a
superfície pública ao estritamente necessário e classifica as rotas em 4 públicas
essenciais, 16 autenticadas, 111 administrativas, 44 dependentes de regra
específica de UVR, associação ou objeto e 2 desativadas nos ambientes online.

O diagnóstico registrou **22 bloqueadores para homologação** ainda não
implementados: 11 rotas públicas mutáveis e 11 consultas públicas ao banco. Foram
também identificadas 15 rotas por ID que precisam de confirmação funcional e
correção de autorização por objeto antes da produção. Não existe cadastro público
de contas em `usuarios` nem rota web de migration ou criação de administrador.

Esta etapa foi exclusivamente documental. Nenhuma permissão foi alterada,
nenhuma rota foi removida e nenhum deploy, migration, banco ou Cloudinary real foi
executado. As correções pertencem à futura Etapa H2A.3B e devem ser aplicadas com
testes de caracterização para preservar os fluxos atuais por UVR e associação.

## 19. Etapa H2A.3B.1 — primeiro bloco de restrição

Em **22/07/2026**, foram protegidas as **11 rotas mutáveis públicas** indicadas
pela matriz. Uma operação global de catálogo passou a exigir administrador;
nove operações financeiras passaram a exigir login e validação da UVR ou do
objeto no servidor; e o registro de denúncia passou a retornar 404 em
homologação e produção, permanecendo disponível somente com login e UVR válida
nos ambientes de desenvolvimento e teste.

Campos de UVR enviados pelo navegador não determinam mais o escopo de acesso,
que vem do `current_user`. Contas, transações e entidades recebidas por ID são
consultadas antes da regra de negócio com SQL parametrizado. As APIs deste bloco
respondem em JSON quando falta sessão interna. Basic Auth e token CSRF continuam
independentes: nenhum dos dois concede login ou autorização.

O login passou a apresentar a mesma mensagem para usuário inexistente, inativo
ou senha errada. A validação continua usando `check_password_hash`; quando não há
conta ativa, usa um hash fictício gerado uma única vez por processo para reduzir
a diferença temporal. Não há promessa de tempo constante.

A revisão final incluiu o escopo de UVR também nas consultas e alterações que
efetivamente produzem extratos, transações e fluxo de caixa. Uma lista com apenas
um ID alheio é recusada integralmente e sofre rollback. Erros de autorização ou
banco geram mensagens genéricas, e nomes de arquivos de exportação são
higienizados. O carregamento da sessão agora exige que o usuário permaneça ativo.
Foram aprovados **435 testes**, sendo 407 anteriores e 28 específicos desta
etapa, sem serviços externos reais.

Permanecem bloqueadores: 11 consultas GET públicas ao banco, relatórios e
downloads fora deste bloco, 15 casos possíveis de IDOR, revisão dos endpoints
JSON/AJAX e o segundo endpoint classificado como E, migration-base, controle de
migrations, rotação de credenciais históricas, Neon e Cloudinary separados,
deploy, CSP, rate limit, trusted hosts e monitoramento. Esta etapa não executou
deploy, migration, PostgreSQL ou Cloudinary real.

## 20. Etapa H2A.3B.2 — segundo bloco de restrição

Em **22/07/2026**, as 11 consultas GET públicas ao banco foram protegidas. Três
catálogos gerais exigem login; as oito consultas de dados financeiros, entidades
ou UVRs aplicam a UVR derivada de `current_user`, com política global explícita
para administrador. Visitante e Basic Auth sem login não chegam ao banco.

Os filtros de relatórios foram incluídos nessa proteção. As exportações CSV de
relatório e extrato neutralizam fórmulas em texto controlável pelo usuário, e os
PDFs escapam conteúdo variável de modo idempotente antes do ReportLab. A
neutralização considera espaços e controles invisíveis, sem converter números.
As quatro exportações
financeiras continuam autorizando UVR/objeto antes de gerar qualquer conteúdo.

Das sete rotas de download ou abertura, as quatro exportações permanecem
protegidas, o documento privado do módulo exige administrador e estado ativo
antes de criar a URL temporária HTTPS de cinco minutos, cujo redirecionamento não
pode ser armazenado em cache público, e as duas fichas PDF passaram a consultar
por ID e UVR no próprio SQL. Com isso, 2 dos 15 possíveis IDORs foram corrigidos; 13
continuam pendentes fora deste bloco.

Foram aprovados **462 testes**, sendo 435 anteriores e 27 novos, sem banco,
Cloudinary, API, arquivo ou gerador externo real. Nenhuma migration ou deploy foi
executado.

Continuam pendentes os 13 possíveis IDORs fora deste bloco, a revisão geral dos
44 endpoints JSON/AJAX, o segundo endpoint classificado para desativação online,
migration-base, controle formal de migrations, rotação das credenciais
históricas, Neon e Cloudinary separados, versões gerais das dependências, deploy,
CSP, rate limit, trusted hosts e monitoramento.

## 21. Etapa H2A.3B.3 — autorização por objeto

Em **23/07/2026**, foi concluída a revisão técnica da correção dos 13 casos de
autorização por objeto restantes. Usuários comuns só
consultam ou solicitam mudanças em objetos pertencentes à UVR carregada de
`current_user`; administrador preserva o acesso global previsto.

A comprovação usa SQL parametrizado com ID e UVR. IDs e UVRs do navegador não
concedem acesso, campos sensíveis ou inesperados não entram nas solicitações e o
cadastro relacionado de uma transação é conferido novamente no SQL final.
Consultas JSON sem sessão
retornam 401; objeto inexistente ou alheio retorna 404 genérico.

Não foi criado `DELETE`, perfil, migration ou tabela. As exclusões físicas
legadas do administrador não foram ampliadas e permanecem como risco de
integridade. O total continua em 177 rotas: 12
públicas, 59 com login e 106 administrativas; as 105 rotas funcionais da
Fiscalização permanecem administrativas. A suíte aprovou **487 testes**, dos
quais 25 específicos percorrem as 13 rotas por subtestes, sem serviços reais.

Com os dois casos da H2A.3B.2, os 15 possíveis IDORs do inventário possuem
correção implementada e não existe IDOR confirmado pendente nesse inventário.
Permanecem pendentes a revisão geral de JSON/AJAX, a
decisão sobre o segundo endpoint proposto para desativação online, migration-base,
controle de migrations, rotação de credenciais, ambientes separados, versões
fixadas, exclusões físicas legadas, deploy, CSP, rate limit, trusted hosts e
monitoramento.

## 22. Etapa H2A.3B.4 — endpoints JSON/AJAX

Em **23/07/2026**, foi concluída a revisão dos **44 endpoints JSON/AJAX**:
1 público essencial, 8 autenticados, 26 sujeitos a UVR/objeto e 9
administrativos. O conjunto possui 34 leituras e 10 escritas. Os 22 endpoints
tratados nas três etapas anteriores foram preservados e os demais passaram pela
mesma auditoria de sessão, permissão, escopo, CSRF, entrada, SQL, resposta,
cache e consumo no navegador.

Visitante recebe JSON 401, permissão insuficiente recebe 403 e objeto alheio ou
inexistente recebe 404 equivalente quando necessário evitar enumeração. Erros de
validação e formato usam 400/415/413; método incorreto usa 405; falha interna não
expõe exceção, SQL ou infraestrutura. Entradas JSON possuem limites
conservadores documentados. A leitura é limitada a 64 KiB no próprio fluxo,
inclusive sem `Content-Length`; a soma das listas é limitada a 200 itens, textos
a 5.000 caracteres e estruturas a dois níveis. Conteúdo comprimido é recusado.
Campos mutáveis são selecionados por listas explícitas e autoria/UVR continuam
vindo da sessão e do servidor.

As respostas JSON protegidas não podem ser armazenadas em cache. Não foi
habilitado CORS permissivo, JSONP, armazenamento sensível no navegador ou envio
de CSRF para domínio externo. Os consumidores AJAX passaram a escapar conteúdo
variável e deixaram de registrar respostas completas no console. A revisão final
substituiu a montagem HTML da lista de notas por elementos DOM e texto seguro,
além de orientar novo login após resposta 401.

O segundo endpoint classificado para desativação online é
`GET /sucesso_denuncia`, página HTML legada ligada ao fluxo de denúncia. Em
homologação e produção ele retorna 404 antes de qualquer efeito, inclusive para
administrador. Em desenvolvimento e testes, permanece apenas para usuário com
sessão interna ativa. O endpoint mutável correspondente,
`POST /registrar_denuncia`, já estava desativado.

A suíte aprovou **525 testes**, sendo 487 anteriores e 38 específicos da etapa,
sem PostgreSQL, Cloudinary, API externa, arquivo real, migration ou deploy.

Riscos corrigidos neste bloco: JSON protegido redirecionando para HTML, erros
CSRF/405 em formato HTML, limite dependente de `Content-Length`, estruturas
aninhadas não limitadas, falta de proteção uniforme em consultas, entrada
malformada e campos forjados, mensagens técnicas, cache de resposta sensível,
inserção insegura de conteúdo retornado no HTML e o segundo endpoint
incompatível com ambiente online.

Continuam pendentes as exclusões físicas legadas, migration-base, controle
formal de migrations, rotação das credenciais históricas, Neon e Cloudinary
separados, fixação geral das versões do Python e dependências, CSP, rate limit,
trusted hosts, monitoramento e deploy.

## Etapa H2B.1 — ambiente reproduzível e inicialização online

Em **27/07/2026**, foi preparada a Etapa H2B.1, ainda sem deploy:

- Python fixado em `3.12.6` pelo arquivo `.python-version`;
- dependências diretas documentadas em `requirements.in`;
- dependências diretas e transitivas fixadas por versão em `requirements.txt`;
- resolução das versões validada e suíte executada em instalação temporária
  limpa;
- Gunicorn iniciado pelo `Procfile` com configuração dedicada;
- `APP_ENV` online e `PORT` válidos obrigatórios para iniciar o Gunicorn;
- limites explícitos de workers, threads, timeouts e tamanho de cabeçalhos;
- reinício periódico de workers e logs sem query string, cookies ou cabeçalhos;
- `TRUSTED_HOSTS` obrigatório em homologação e produção, aceitando somente nomes
  exatos ou endereços IP válidos;
- `TRUST_PROXY=true` obrigatório online, com confiança limitada ao endereço de
  origem e ao protocolo informados pelo proxy;
- `MAX_REQUEST_MB` obrigatório online, aceitando somente valores inteiros entre
  1 e 128 MB; o valor recomendado é 64 MB;
- respostas 413 separadas para páginas HTML e endpoints JSON, sem executar a
  operação de negócio;
- execução direta de `app.py` restrita ao computador local e bloqueada em
  homologação e produção;
- inicialização do Gunicorn sem migration, banco, Cloudinary ou script
  administrativo.

Na revisão final, a normalização de hosts passou a rejeitar quebra de linha,
CRLF, listas vazias e entradas malformadas, sem transformar silenciosamente um
valor inválido em válido. Portas presentes na requisição são aceitas para um
host exato autorizado; portas na configuração são recusadas. IPv6 local foi
validado no formato esperado pelo Flask. `X-Forwarded-Host` não é confiável e
não contorna a validação.

A instalação limpa usou Python `3.12.6`, pip `24.2`, `requirements.txt` e uma
pasta temporária com todos os `site-packages` globais removidos do caminho.
Importação, `/health` e a suíte completa foram validados com PostgreSQL,
Cloudinary e APIs externas bloqueados. A pasta temporária foi removida. Também
foram confirmadas as 24 distribuições para Linux/Python 3.12; essa conferência
exigiu a correção mínima do Cloudinary para `1.42.2`.

Foram aprovados **36 testes específicos** e **561 testes totais**, sem falhas ou
erros. Nenhum banco, Cloudinary, API externa, migration ou deploy foi executado.
O Gunicorn não foi iniciado como processo real porque o computador de revisão é
Windows; sua configuração foi importada e validada sem iniciar serviço, e a
distribuição Linux foi confirmada.

Com esta etapa, deixam de ser pendências a fixação do Python, a fixação das
dependências, a validação de hosts e a configuração básica segura do Gunicorn.
Continuam pendentes, entre outros pontos, migration-base, controle formal de
migrations, rotação de credenciais, ambientes Neon e Cloudinary separados, CSP,
rate limit, monitoramento e o processo controlado de deploy. Os documentos do
módulo continuam com limite individual e inspeção de conteúdo; alguns uploads
legados de fotos permanecem protegidos apenas pelo limite global e precisam de
uma revisão própria antes da homologação, sem mudança apressada dos formatos
atualmente aceitos.

## Referências técnicas consultadas

- [Render — Web Services](https://render.com/docs/web-services)
- [Render — Deploy de Flask](https://render.com/docs/deploy-flask)
- [Render — Health Checks](https://render.com/docs/health-checks)
- [Render — Variáveis e segredos](https://render.com/docs/configure-environment-variables)
- [Neon — Fluxo com branches](https://neon.com/docs/get-started-with-neon/workflow-primer)
- [Cloudinary — Controle de acesso a arquivos](https://cloudinary.com/documentation/control_access_to_media)
- [Flask — Segurança](https://flask.palletsprojects.com/en/stable/web-security/)
- [Flask — Aplicação atrás de proxy](https://flask.palletsprojects.com/en/stable/deploying/proxy_fix/)

## 23. Etapa H2B.2A — CSP, cabeçalhos e rate limit

Em **28/07/2026**, foi preparada a proteção de conteúdo e de volume de
requisições, ainda sem deploy:

- CSP efetiva por padrão, com nonce criptográfico novo por resposta HTML;
- `script-src` restrito à própria aplicação, Bootstrap e Font Awesome, sem
  `unsafe-inline`, `unsafe-eval` ou curingas;
- origens de imagens limitadas à aplicação, `data:` e ao domínio de entrega do
  Cloudinary já utilizado;
- cabeçalhos `nosniff`, bloqueio de frames, política de referência, política de
  permissões (câmera somente para a própria aplicação, demais recursos não
  usados negados), isolamento de origem e HSTS condicionado a HTTPS online;
- Flask-Limiter com cobertura geral e grupos específicos para login, consultas
  de CNPJ/CEP, relatórios, downloads, uploads e operações mutáveis;
- resposta 429 amigável em HTML e JSON, antes da operação de negócio;
- configuração online validada de forma estrita, sem desativação silenciosa.

O inventário encontrou um grande script legado no cadastro que depende de
valores Jinja. Ele recebeu nonce e não foi reescrito nesta etapa para evitar
regressão. A UVR usada nesse script é serializada com `tojson`. Blocos menores
foram movidos para arquivos estáticos. Permanecem exatamente 60 atributos de
evento e 44 atributos de estilo no HTML legado; por isso a política usa,
temporariamente, `script-src-attr 'unsafe-inline'` e
`style-src-attr 'unsafe-inline'`. Essas exceções são restritas aos atributos e
não liberam scripts embutidos em geral.

**Pendência:** Remoção gradual dos atributos de evento e estilo inline, seguida
da retirada de `script-src-attr` e `style-src-attr` com `unsafe-inline`. A CSP
não será classificada como plenamente estrita antes dessa retirada. A extração
do script principal do cadastro também deve ocorrer de forma gradual.

Bootstrap, jQuery, jQuery Mask e Font Awesome usam HTTPS e versões fixadas. SRI
não foi adicionado porque os hashes dos arquivos oficiais não foram comprovados
nesta revisão sem consultar os CDNs. Permanecem como alternativas futuras o SRI
verificado e a hospedagem local dessas bibliotecas.

O armazenamento do limitador é `memory://` em desenvolvimento e testes.
Produção exige Redis compartilhado. A homologação inicial, globalmente
restringida pela barreira já implementada, pode usar memória apenas quando
`RATELIMIT_ALLOW_MEMORY_HOMOLOGATION=true` registrar a decisão explícita; nesse
modo, cada processo mantém seus próprios contadores e os perde ao reiniciar.
Antes de ampliar acesso ou escalar para vários processos, deve-se adotar Redis.

O inventário real possui 177 rotas e 113 endpoints com grupo específico:
91 operações mutáveis, 8 relatórios/PDF/CSV, 8 uploads, 4 consultas externas,
1 login e 1 download privado. A classificação usa, nesta ordem: login;
consultas de CEP/CNPJ; uploads; download privado; prefixos de relatório,
extrato, PDF, CSV e impressão; e métodos POST/PUT/PATCH/DELETE restantes.
OPTIONS e HEAD não entram nos grupos de escrita. As demais rotas usam o teto
geral de 300 por minuto por endpoint. Os contadores específicos são
compartilhados dentro de cada categoria, de modo que alternar entre endpoints
do mesmo grupo não amplia o limite. `/health` e arquivos estáticos são isentos.

As duas rotas de denúncia desativadas são ocultadas com 404 antes do limitador,
da barreira Basic, do CSRF e da regra de negócio em ambientes online; por isso
repetições não mudam a resposta para 429. Fora dos ambientes online, continuam
protegidas pelo limite de operações mutáveis e pelo CSRF aplicável.

A ordem normal é: bloqueio ambiental; rate limit; barreira Basic da
homologação; CSRF; autenticação/autorização interna e por objeto; regra de
negócio. O limitador é executado antes da consulta do login e antes de uploads,
relatórios, APIs e transações. Páginas HTML com nonce recebem `no-store` para
evitar reutilização por cache compartilhado.

Esta preparação não significa que a homologação ou a produção já estejam
liberadas. Continuam pendentes a migration-base, o controle formal de
migrations, a rotação de credenciais históricas, Neon e Cloudinary separados,
monitoramento, remoção das exceções CSP legadas e o deploy controlado.

## 24. Etapa H2B.2B — logs e monitoramento operacional básico

Em **29/07/2026**, foi revisada, ainda sem deploy, a camada de logs
operacionais estruturados:

- `request_id` hexadecimal gerado no servidor por requisição e devolvido em
  `X-Request-ID`;
- duração em milissegundos calculada por relógio monotônico;
- JSON de uma linha, timestamp UTC e campos opcionais omitidos online;
- níveis e formatos validados por lista permitida;
- `DEBUG` e formato textual recusados online;
- stdout para a aplicação e stdout/stderr para o Gunicorn, sem arquivos locais;
- respostas 500 genéricas em HTML e JSON, com código de referência;
- eventos estáveis de startup, autenticação, autorização, CSRF, rate limit,
  serviço externo, upload, URL privada e erro interno;
- redação recursiva com limites de tamanho, profundidade e ciclos, sem coleta
  de corpos de requisição;
- `/health` público, mínimo, independente de serviços e fora do log comum.

O inventário concentrou os logs legados no grande `app.py` e nas rotas do
módulo. Foram encontrados logs com nome de usuário, valores digitados,
SQL/parâmetros e mensagens cruas de exceções; esses pontos foram substituídos
por eventos ou mensagens genéricas. A configuração manual duplicada do servidor
local também foi removida. Permanecem mensagens operacionais legadas com
`logger.exception`; no formato online, o formatador central omite o traceback e
qualquer mensagem crua, conservando somente mensagem genérica e tipo da
exceção. A conversão gradual
dessas mensagens para nomes de eventos específicos continua recomendada.

O Gunicorn registra eventos de servidor e acessos sem query string, cookies ou
cabeçalhos sensíveis. A aplicação registra eventos operacionais e de segurança,
sem IP bruto. Não há arquivo de log, banco de auditoria, serviço de
monitoramento, alerta, métrica ou retenção centralizada nesta etapa. Os sinais e
procedimentos iniciais estão documentados em `MONITORAMENTO_OPERACIONAL.md`.

Esta etapa não declara o sistema pronto para produção. Permanecem pendentes
retenção e alertas externos, eventual ferramenta especializada, atributos
inline, SRI ou hospedagem local, Redis compartilhado, exclusões físicas
legadas, migration-base, controle formal de migrations, rotação de
credenciais, Neon e Cloudinary separados e deploy.

## 25. Etapa H2C.1 — inventário do banco e projeto da migration-base

Em **29/07/2026**, o banco foi inventariado somente pelos arquivos do
repositório. Nenhum PostgreSQL, Neon, Cloudinary ou `DATABASE_URL` foi acessado
e nenhuma migration foi executada.

Foram confirmadas 11 migrations numeradas, que criam 23 tabelas do módulo. O
`app.py` contém DDL legado para outras 12 tabelas, mas a criação completa de
`patrimonio` e `grupos_atividade` não existe no repositório. Também estão sem
definição comprovada as colunas `id_grupo` pressupostas em `subgrupos` e
`produtos_servicos`, e existem duas definições incompatíveis de
`solicitacoes_alteracao`.

O comportamento atual de inicialização foi confirmado: importar `app` e
iniciar Gunicorn **não** executam migration.

A conclusão é que o repositório, sozinho, ainda não permite criar uma baseline
integral e confiável. Antes da H2C.2 será necessária uma exportação futura
somente do schema do banco atual, sem dados, owners, privilégios ou
credenciais. A baseline será exclusiva para PostgreSQL vazio, deverá recusar
banco já utilizado antes de qualquer DDL e não usará `IF NOT EXISTS` para
ocultar drift.

O inventário detalhado está em `MAPA_SCHEMA_BANCO.md`; a estratégia, ordem,
riscos e decisões pendentes estão em `PLANO_MIGRATION_BASE.md`. A H2C.1 foi
revisada tecnicamente e não criou migration.
