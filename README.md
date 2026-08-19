# sistema-recic3

Sistema de gestão para associações de catadores em Flask e PostgreSQL.

## Ambiente local

O projeto usa Python **3.12.6**, registrado em `.python-version`. Crie um ambiente
virtual, instale `requirements.txt` e copie somente os nomes de configuração de
`.env.example` para o seu `.env` local. Nunca versione o `.env`.

```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python app.py
```

`requirements.in` lista somente as dependências usadas diretamente pelo
projeto. `requirements.txt` é o arquivo efetivamente instalado localmente e no
deploy; ele fixa também as dependências transitivas. Para atualizar o lock, use
sempre um ambiente temporário limpo:

```powershell
python -m pip install -r requirements.in
python -m pip freeze --exclude pip --exclude setuptools > requirements.txt
python -m pip install --ignore-installed -r requirements.txt
```

Revise o resultado e execute a suíte completa antes de salvar uma atualização.
Não edite versões apenas para acompanhar a versão mais recente.

## Inicialização online

O `Procfile` inicia o Gunicorn com `gunicorn.conf.py`. Essa configuração aceita
somente `APP_ENV=homologation` ou `APP_ENV=production`, exige `PORT`, limita
workers, threads e tempos de espera e não executa migrations.

Ambientes online também exigem:

- `TRUST_PROXY=true`, pois a implantação planejada usa proxy reverso;
- `TRUSTED_HOSTS` com os nomes exatos autorizados, separados por vírgula;
- `MAX_REQUEST_MB`, em megabytes inteiros entre 1 e 128;
- `SECRET_KEY` e `DATABASE_URL`;
- as demais credenciais próprias do ambiente, configuradas fora do Git.

Exemplo conceitual, sem credenciais:

```text
APP_ENV=homologation
TRUST_PROXY=true
TRUSTED_HOSTS=servico-homologacao.exemplo.invalid
MAX_REQUEST_MB=64
PORT=10000
```

O limite global padrão local é 64 MB, preservando os uploads existentes e a
sobrecarga de formulários multipart. Documentos do módulo continuam com o limite
individual padrão de 20 MB. Endpoints JSON mantêm o limite independente de
64 KiB. Excesso global retorna uma página HTML amigável ou JSON genérico,
conforme o endpoint, sem iniciar a operação de negócio.

Migrations são executadas somente por procedimento administrativo separado e
explicitamente autorizado. Importar `app.py` ou iniciar o Gunicorn não aplica
migrations. A execução direta de `app.py` serve apenas para desenvolvimento no
host local; homologação e produção exigem Gunicorn.

## Segurança de conteúdo e limitação de requisições

A aplicação gera um nonce criptográfico novo em cada resposta HTML e o inclui
na Política de Segurança de Conteúdo (CSP). Scripts próprios embutidos só podem
executar quando possuem esse nonce. Bootstrap e Font Awesome continuam
permitidos exclusivamente nos domínios já usados pelo sistema; imagens privadas
do Cloudinary permanecem restritas ao domínio de entrega correspondente.

O código legado ainda utiliza alguns atributos de evento e estilo diretamente
no HTML. Por compatibilidade, a CSP mantém exceções específicas em
`script-src-attr` e `style-src-attr`; elas estão documentadas como pendência para
remoção gradual. Não há `unsafe-eval`, curinga de origem ou liberação geral de
scripts embutidos em `script-src`. O modo `CSP_REPORT_ONLY=true` serve apenas
para diagnóstico controlado e é recusado em produção.

A linha de base atual possui 60 atributos de evento e 44 atributos de estilo.
Testes impedem que essa quantidade cresça silenciosamente. A política ainda não
é considerada plenamente estrita enquanto essas exceções existirem. A extração
do script extenso de `cadastro.html` também permanece como trabalho futuro; por
ora, a única informação variável inserida nele usa serialização JSON segura.

As URLs de Bootstrap, jQuery, jQuery Mask e Font Awesome possuem versões
fixadas e usam HTTPS. Ainda não possuem SRI (`integrity`), pois os hashes não
foram validados contra os arquivos oficiais nesta etapa. Adotar SRI verificado
ou hospedar essas bibliotecas localmente permanece como risco pendente.

As respostas também recebem proteção contra interpretação incorreta de
conteúdo, enquadramento, vazamento de referência e acesso a recursos do
dispositivo. A câmera é permitida somente para a própria aplicação porque o
cadastro existente usa webcam; microfone, localização e demais recursos não
utilizados continuam negados. HSTS é enviado somente em homologação ou produção
e apenas quando a requisição chega como HTTPS pelo proxy confiável.

O rate limit usa Flask-Limiter e cobre todas as rotas, com limites mais
restritivos para login, consultas externas, relatórios, downloads, uploads e
operações mutáveis. Excesso retorna HTTP 429 em HTML ou JSON, sem executar a
operação. Os valores padrão estão em `.env.example`.

Em desenvolvimento e testes, `memory://` é suficiente. Em produção,
`RATELIMIT_STORAGE_URI` deve apontar para um Redis compartilhado e
`RATELIMIT_ENABLED` deve permanecer ativo. A primeira homologação restrita pode
usar memória somente quando
`RATELIMIT_ALLOW_MEMORY_HOMOLOGATION=true` estiver explicitamente configurada,
sabendo que os contadores não são compartilhados entre processos e são
reiniciados com o processo. Usuário autenticado e endereço remoto compõem a
chave do limite; login, senha e conteúdo enviado nunca fazem parte dela.

Os limites iniciais são: 300 requisições por minuto no grupo geral, 5 no login,
30 nas consultas de CEP/CNPJ, 10 em relatórios, 10 em uploads, 30 em downloads
privados e 60 em operações mutáveis. `/health` e arquivos estáticos são isentos.
Cada grupo sensível compartilha seu contador entre os endpoints da mesma
categoria, impedindo ampliar o limite apenas alternando a URL. Esses endpoints
também permanecem sob o teto geral amplo por rota; o limite menor é atingido
primeiro. Um bloqueio retorna HTTP 429, `Retry-After` e resposta sem cache antes
da operação de negócio.

## Logs operacionais e código de referência

A aplicação gera um `X-Request-ID` novo para cada requisição e usa o mesmo
identificador nos eventos relacionados. Em homologação e produção, os logs da
aplicação são linhas JSON em UTC enviadas para stdout. A duração usa relógio
monotônico e `/health` e arquivos estáticos não geram o evento comum de acesso.

Erros 500 apresentam somente uma mensagem genérica e um código de referência,
em HTML ou JSON. Senhas, tokens, formulários, JSON completo, query string,
cookies, dados pessoais, SQL, credenciais, arquivos, `public_id` e URLs
assinadas não são registrados. O documento
`MONITORAMENTO_OPERACIONAL.md` descreve os eventos, a investigação por
`request_id` e as limitações atuais.

As variáveis `LOG_LEVEL`, `LOG_FORMAT`, `LOG_REQUESTS` e
`LOG_SECURITY_EVENTS` controlam a camada. Homologação e produção exigem JSON,
recusam `DEBUG` e não permitem desativar eventos de segurança. O ambiente de
testes usa configuração fixa, sem depender do `.env` pessoal. Logs normais
seguem para stdout; erros mínimos da própria configuração ou emissão podem
seguir para stderr.
