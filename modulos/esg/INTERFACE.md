# Cadastro e consulta — ESG-02B2A

Interface administrativa preparada com testes offline. **RF-1A permanece
NO-GO OPERACIONAL**. Nenhum SQL, migration, conexão real ou serviço foi executado.
Os contratos da ESG-02A e os arquivos SQL da ESG-02B1 permanecem inalterados.

## Rotas e telas

| Rota | Métodos | Conteúdo |
| --- | --- | --- |
| `/esg` | GET | início e estado da consulta; lista definições somente quando há repositório explicitamente injetado |
| `/esg/indicadores` | GET | definições, dimensões, unidades, perímetros e situações |
| `/esg/indicadores/novo` | GET/POST | proposta com metodologia, unidade, abrangência e fonte documental |
| `/esg/indicadores/<codigo>/detalhe` | GET | definição/fontes, registros filtrados, situações, valores e evidências |
| `/esg/indicadores/<codigo>/registros/novo` | GET/POST | registro anual/mensal em rascunho, fonte e evidência opcional |

Na consulta, `competencia=AAAA` seleciona o ano completo e `AAAA-MM` seleciona
uma competência mensal. Não se agregam meses ao ano. `situacao` é um filtro de
leitura: rascunho, em análise, aprovado ou rejeitado. Filtros inválidos retornam
400. Evidências são mostradas somente junto ao registro correspondente, com sua
referência documental e observações; não há upload, download ou busca externa.

O endpoint `esg.indicadores_detalhe` foi preservado. Links, filtros e
redirecionamentos usam `url_for` e passam a apontar para o sufixo `/detalhe`.
O caminho `/esg/indicadores/novo` continua exclusivo do cadastro; um indicador
com código `novo` é consultado em `/esg/indicadores/novo/detalhe`. O caminho
antigo de detalhe, sem o sufixo, não é mantido como alias.

O cadastro de registro fixa unidade, código do indicador e perímetro pela
definição consultada no servidor. O formulário não permite trocar esses vínculos.
A evidência opcional recebe UUID próprio, aponta para o UUID gerado para o
registro e utiliza a mesma fonte documental informada no cadastro. Fonte
metodológica e fontes de aprovação existentes são mostradas separadamente.

Não há telas para editar, excluir, aprovar ou criar revisões. Indicador proposto,
registro em rascunho e informação aprovada têm rótulos explícitos distintos. A
situação de uma definição não altera automaticamente a situação de seus registros.
O painel inicial não publica resultados automaticamente e não carrega fixtures.

## Injeção de dependência e falha segura

As fábricas pública e interna aceitam `criar_blueprint_esg(repositorio=None)`.
A dependência é recebida explicitamente e capturada pelas rotas; não é construída
por elas, não há conexão inferida de configuração/ambiente e não há repositório
global ou fallback de dados simulados. `app.py` continua chamando a fábrica sem
repositório. Portanto o início abre, mas as rotas de dados devolvem 503 com aviso
de persistência não configurada. Isso não é apresentado como consulta vazia ou
cadastro bem-sucedido.

Sem repositório, o painel responde 200 com aviso de consulta indisponível e
nenhum carregamento realizado, sem afirmar ausência de indicadores. Com a
dependência injetada, chama `listar_definicoes()` uma vez: uma consulta vazia
responde 200 e informa que não há indicadores; uma consulta com definições
orienta abrir os cadastros administrativos. Falhas de persistência ou de
contrato respondem 503 no painel, sem expor detalhes nem presumir lista vazia.
O painel não consulta registros, não publica valores e não cria dependências.

Uma integração futura, revisada, deverá fornecer a dependência explicitamente
ao registrar o Blueprint no aplicativo. A interface exige os métodos
`listar_definicoes`, `obter_definicao`, `listar_registros`, `salvar_definicao` e
`salvar_registro`. A adição mínima no repositório nesta etapa é
`listar_definicoes()`, usando a consulta tipada existente em transação somente
leitura. A persistência permanece sem ativação e sem nova migration.

Conflitos de persistência retornam 409, ausência de entidade retorna 404 e
indisponibilidade retorna 503. Falhas de leitura não são convertidas em listas
vazias. Erros internos do repositório não são expostos na página. Confirmação
incerta de commit instrui consultar antes de reenviar. Não há repetição automática
de escrita. Cadastro válido usa redirecionamento 303 e mensagem com o estado inicial.

## Segurança e conversão

Todas as rotas de dados e o início mantêm o `admin_required` já utilizado pelo
RECIC3: login Flask-Login e papel `admin`. A configuração CSRF central permanece
ativa, sem isenções; os formulários POST incluem token. Como no sistema existente,
o middleware CSRF pode rejeitar um POST sem token antes de avaliar a permissão.

Autoria vem exclusivamente de `current_user.id`, exigindo inteiro positivo.
Campos extras ou repetidos são recusados. Situação, aprovação, autoria e UUIDs
não são aceitos como dados de formulário. Apenas o filtro GET de situação é
permitido; ele não altera contratos. O servidor cria contratos com os padrões
proposto/rascunho e sem fonte de aprovação.

`formularios.py` normaliza espaços externos e quebras de linha dos textareas,
converte datas e períodos estritamente e usa os validadores dos contratos.
O valor vazio vira `None`; `0` vira `Decimal("0")`. Aceita ponto ou vírgula decimal,
sem separadores de milhar, expoentes, NaN ou infinito. O adaptador web limita o
valor a 256 caracteres; não há arredondamento ou passagem por float. Os limites
de negócio continuam dependentes de metodologia validada. Erros de formulário
retornam 400, preservando os campos permitidos para correção. Jinja mantém escape
automático tanto na consulta documental quanto na reexibição dos formulários.

## Testes e limites da verificação

Novos testes:

- `tests/test_esg_formularios.py`: conversão pura, campos obrigatórios, valores,
  períodos, fontes, evidências e bloqueio de campos reservados.
- `tests/test_esg_interface.py`: miniaplicação Flask apenas em memória, com
  LoginManager/CSRF reais, usuários exclusivamente de fixture e repositório Mock.
  Há bloqueios de rede, conexão PostgreSQL, leitura operacional, `.env`, SQL e
  construção automática de repositório real. Não importa `app.py`.
- `tests/test_esg_repositorio.py`: caso adicional para a listagem tipada de
  definições com conexão/cursor simulados.

O registro anterior da implementação informa **91 testes aprovados**. Essa
alegação histórica não foi revalidada nas auditorias/correções R1 e R2 e não
comprova a versão atual. Na R2 nenhum Python, teste ou servidor foi executado.

Na R2 foram acrescentados casos offline para o código `novo`, resolução do novo
detalhe e links, estados do painel (sem dependência, vazio, com definições e
falha), listagem inválida, erro em `listar_definicoes()` e conflito/confirmação
incerta no cadastro de registro. A regressão ESG-01 foi ajustada para não exigir
mensagem de ausência de cadastros sem consulta. Esses casos foram incluídos na
validação offline R3 descrita a seguir.
As barreiras de `test_esg_interface.py` são instaladas em `setUpClass`, depois
dos imports iniciais; não constituem prova de isolamento de toda a importação.
Na validação offline **R3**, foi informado o seguinte resultado para os cinco
módulos ESG (`test_esg_contratos`, `test_esg_formularios`,
`test_esg_repositorio`, `test_esg_interface` e `test_esg`):

- Python utilizado: **3.12.10**.
- Interpretador: `C:\sistema-recic3\.venv\Scripts\python.exe`.
- Testes executados: **99**; testes aprovados: **99**.
- Falhas: **0**; erros: **0**; ignorados: **0**.

As barreiras externas foram instaladas **antes dos imports dos módulos de
teste pelo executor controlado**, complementando os mocks internos. Nenhuma
barreira externa foi acionada na execução R3. Os testes unitários, isoladamente,
não comprovam essa proteção anterior aos imports. O `Flask test_client` foi
usado em memória, sem servidor, conexões reais ou execução de SQL/migrations.
O arquivo SQL foi apenas inspecionado como texto.

A R4 acrescenta uma regressão para os textos neutros dos cartões E, S e G; a
contagem de 99 acima registra o resultado R3, anterior a esse novo teste.
A repetição controlada final na R4 executou os mesmos cinco módulos: **100
testes executados e aprovados**, sem falhas, erros ou ignorados, com o mesmo
interpretador e as mesmas barreiras externas; nenhuma barreira foi acionada.
**Não houve validação visual nem validação em PostgreSQL real**; ambas
permanecem pendentes e não são substituídas pelos resultados offline.

## Interpretador e histórico de ambiente

A leitura de `STATUS_DESENVOLVIMENTO.md` identifica `C:\dev\recic4\.venv` como
ambiente oficial previamente reparado. Seu `Scripts\python.exe` existe e o
`pyvenv.cfg` declara Python **3.12.10**. Não foram encontrados executáveis nos
caminhos `.venv`, `venv` e `env` desta cópia do repositório. O README principal e
`.python-version` ainda indicam **3.12.6**. A divergência deve ser verificada
antes de executar a validação, assim como o funcionamento da instalação-base.

Essa identificação de `C:\dev\recic4\.venv` corresponde à investigação histórica
da R2, quando nenhum ambiente foi ativado, modificado ou executado. Na R3, o
ambiente confirmado e efetivamente utilizado foi
`C:\sistema-recic3\.venv\Scripts\python.exe`, Python **3.12.10**. Esse é o
interpretador de referência para repetir a validação offline controlada, sem
utilizar automaticamente `central_ia`, `sistema_tac13`, o `pytest` do Anaconda
ou outros projetos. A indicação de 3.12.6 no README principal e em
`.python-version` continua como divergência documental a revisar separadamente.

## Pendências para demonstrar o MVP

1. Revisar interface, nomenclatura e contratos com a equipe responsável.
2. Autorizar e preparar ambiente de demonstração com dados claramente identificados
   como demonstrativos, sem conectá-lo ao banco operacional ou publicá-los como
   resultados oficiais. Nenhum dado demonstrativo está embarcado na aplicação.
3. Validar aparência em navegador e em dispositivos menores, sem reduzir as
   proteções de autenticação, CSRF ou de estados não aprovados.
4. Resolver integração controlada das migrations e validar persistência e
   concorrência em etapa separada autorizada, antes de fornecer um repositório real.
5. Implementar revisão e aprovação auditada conforme PERSISTENCIA.md, e definir
   paginação/filtros no repositório caso o volume de registros exija. A interface
   atual filtra os contratos em memória sem agregar valores.

Não decorre desta implementação qualquer aprovação institucional, execução
operacional ou liberação de RF-1A.
