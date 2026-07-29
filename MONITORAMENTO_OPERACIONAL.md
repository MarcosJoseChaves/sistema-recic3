# Monitoramento operacional básico

Este documento descreve a observação inicial do `sistema-recic3` em futura
homologação. A solução atual usa apenas os logs da aplicação, os logs do
Gunicorn e a rota `/health`. Nenhum serviço externo de monitoramento foi
integrado.

## Verificação de saúde

`GET /health` responde somente `{"status":"ok"}` e não consulta PostgreSQL,
Redis, Cloudinary ou APIs. O cabeçalho `X-Request-ID` pode estar presente, mas
versão, commit, hostname e configurações não são expostos. Chamadas bem-sucedidas
de saúde não geram o evento comum `request_completed`, evitando ruído.

Uma falha de configuração online interrompe a inicialização. Portanto, um
processo com configuração insegura não deve permanecer ativo parecendo
saudável.

## Formato e correlação

Em homologação e produção, cada evento da aplicação ocupa uma única linha JSON
em UTC. Os campos mais comuns são:

- `timestamp`, `level`, `event`, `message` e `environment`;
- `request_id`, `method`, `endpoint` e `status_code`;
- `duration_ms`, `actor_id`, `error_type` e `security_category`, quando
  aplicáveis.

Campos sem valor são omitidos. O identificador `request_id` é gerado pelo
servidor para cada requisição, retornado em `X-Request-ID` e compartilhado pelos
eventos daquela operação. Para investigar uma falha, procure nos logs pelo
código de referência mostrado ao usuário.

O Gunicorn continua responsável pelos eventos do servidor e pelo acesso HTTP,
enviando `accesslog` e `errorlog` para stdout e stderr. A aplicação registra
eventos operacionais e de segurança. O formato de acesso não contém query
string, cookies, `Authorization` nem corpo da requisição. O acesso do Gunicorn
e o evento `request_completed` são complementares: o primeiro descreve o
servidor HTTP; o segundo fornece correlação e duração internas. Não são cópias
do mesmo evento.

## Eventos adotados

- `application_startup` e `application_configuration_error`;
- `request_completed` e `internal_error`;
- `authentication_failed`, `authentication_succeeded` e
  `basic_auth_failed`;
- `authorization_denied`, `csrf_rejected` e `rate_limit_exceeded`;
- `external_service_error`, `upload_rejected` e
  `signed_url_generation_failed`;
- `credential_updated` e `maintenance_completed`.

O evento `application_log` identifica mensagens legadas ainda não convertidas.
Em ambiente online, uma chamada `logger.exception` nunca inclui mensagem crua
nem traceback: conserva somente mensagem genérica e tipo da exceção. Em
desenvolvimento, traceback exige `APP_DEBUG=true` e `LOG_LEVEL=DEBUG`; a
redação continua ativa.

`application_startup` ocorre uma vez por processo do servidor. Com mais de um
worker do Gunicorn, haverá um evento por worker; ele não representa uma
inicialização única de toda a plataforma.

## Privacidade

A aplicação não registra corpo de formulário, JSON completo, query string,
cookies, cabeçalho `Authorization`, token CSRF, senha, CPF, CNPJ, e-mail,
telefone, dados bancários, documento, nome original de arquivo, `public_id`,
URL assinada ou credenciais de banco, Redis e Cloudinary. O endereço IP bruto
também não é incluído nos logs da aplicação.

Um redator recursivo funciona como segunda proteção para estruturas
explicitamente destinadas ao log. A regra principal continua sendo não coletar
o conteúdo sensível. O redator não altera a estrutura original, detecta
referências circulares e limita profundidade, quantidade de itens e tamanho de
texto. Se o próprio logging falhar, a requisição continua e somente uma
mensagem mínima é tentada em stderr.

## Sinais para alertas futuros

Merecem investigação:

- `application_configuration_error` ou reinicializações repetidas;
- indisponibilidade de `/health`;
- aumento de `internal_error` e respostas 500;
- aumento de `rate_limit_exceeded` ou `authentication_failed`;
- repetição de `basic_auth_failed`;
- falhas de upload, URL privada ou serviço externo.

Não existe envio automático de alerta nesta etapa.

Durante a homologação inicial, a observação é manual: acompanhar `/health`,
eventos de startup, respostas 500, autenticação, rate limit e integrações
externas. Não há ainda ferramenta externa, painel ou política própria de
retenção; a retenção disponível dependerá da futura plataforma.

## Ações iniciais diante de falhas

1. Confirmar se `/health` está disponível.
2. Localizar o `request_id` informado na tela ou no chamado.
3. Verificar o evento, o endpoint, o status e o tipo genérico do erro.
4. Avaliar eventos próximos de startup, limite ou serviço externo.
5. Não solicitar senha, token, arquivo ou URL assinada ao usuário.
6. Corrigir a causa em ambiente controlado e repetir os testes antes de
   publicar.

## Limitações e pendências

Os logs ainda dependem da retenção da futura plataforma. Não há armazenamento
centralizado, painel, métrica, alerta externo ou rastreamento distribuído.
Permanecem pendentes uma política de retenção, alertas, eventual integração
especializada, conversão gradual dos logs legados, Redis compartilhado,
atributos inline, SRI, migration-base, controle formal de migrations, rotação
de credenciais, ambientes Neon e Cloudinary separados e deploy controlado.
