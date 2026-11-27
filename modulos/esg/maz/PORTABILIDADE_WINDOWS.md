# Plano de reprodução da demonstração em notebook Windows

Este documento prepara uma instalação futura. Nenhuma ferramenta é instalada, nenhum arquivo é transferido e nenhum cluster é criado no notebook na ESG-MAZ-06A. RF-1A permanece NO-GO OPERACIONAL. O aceite do notebook dependerá de execução própria; não se declara portabilidade já homologada.

## Versões e pacote necessário

Usar como referência ensaiada CPython **3.12.10 x64**, PostgreSQL **17.0 x64** e o conjunto exato de dependências inventariado no pacote 06A. A aplicação é Flask, não Streamlit. `.python-version` e o comentário de `requirements.txt` ainda mencionam 3.12.6: não alterá-los silenciosamente nem tratá-los como evidência de teste nessa versão.

O pacote offline futuro deve conter:

1. Fontes congelados do módulo ESG e dependências locais de importação: `modulos/esg/**`, `modulos/fiscalizacao_contratos/permissions.py`, `seguranca_csrf.py` e seus arquivos de pacote. Preservar a estrutura; validar o fechamento de imports no instalador. A fábrica `criar_demo_maz` não requer iniciar `app.py`.
2. Controlador de migrations, SQLs, reconciliações/contratos necessários e uma **cópia candidata** do manifesto já ensaiado no descartável. Nunca substituir o manifesto operacional do RECIC3.
3. Lock de dependências Windows e wheelhouse para CPython 3.12 x64, acompanhados por hashes e licenças. O inventário `dependencias_06a.json` identifica as distribuições realmente instaladas. `gunicorn==26.0.0` consta no ambiente e em `requirements.txt`, mas o processo demonstrativo Windows usa `werkzeug.serving.make_server`; Gunicorn não é o servidor Windows. Sua exclusão do lock específico deve ser explícita e ensaiada.
4. Instaladores oficiais previamente obtidos/autorizados de Python/PostgreSQL, com hashes conferidos. Não baixar durante a apresentação.
5. Inicializador portátil, configuração local sem segredos, carga sintética e verificador de shutdown. Esses componentes precisam ser parametrizados e ensaiados em etapa posterior; o executor do desktop, com identidades e caminhos fixos, não é um instalador portátil.
6. Documentação de congelamento, plano de teste/aceite e manifesto SHA-256 do pacote. Não incluir PGDATA, `.env`, senhas, sessões, chaves, dumps municipais ou dados pessoais.

O lock completo e o inventário de fontes estão no pacote de revisão 06A. A inclusão de bibliotecas já presentes no ambiente não prova que todas sejam necessárias à fábrica; reduzir o conjunto exige ensaio do fechamento de imports, sem mudar a candidata funcional.

## Estrutura local proposta

```text
RECIC3-ESG-demo/
  app/                         # fontes congelados e SQLs
  runtime/                     # executáveis locais aprovados, se empacotados
  wheelhouse/                  # rodas Windows com hashes
  config/demo.json              # host, portas, caminhos relativos, sem senha
  manifests/                   # hashes e manifesto candidato
  fixtures/                    # dados exclusivamente sintéticos
  tools/                       # lifecycle portátil a preparar e homologar
  state/pgdata/                 # criado por initdb no notebook
  state/secrets/                # gerado localmente, ACL restrita
  state/logs/
```

Resolver caminhos a partir da raiz escolhida ou de uma variável como `RECIC3_ESG_HOME`; não exigir nome de usuário ou diretórios Temp do desktop. Não incluir códigos de login, hashes de senha ou chave de sessão nos fixtures públicos.

## Sequência reproduzível a executar futuramente

1. Conferir manifesto do pacote e versões/arquitetura. Escolher diretório novo gravável pela conta local. Verificar disponibilidade exclusiva de `127.0.0.1:65437` e `127.0.0.1:5055` sem conectar ou sinalizar serviços existentes. Abortar se houver conflito; não reaproveitar um PostgreSQL desconhecido.
2. Criar venv local com Python 3.12.10 e instalar somente do wheelhouse homologado. Com os artefatos preparados, a sequência será:

   ```powershell
   py -3.12 -m venv .venv
   & .\.venv\Scripts\python.exe -c "import sys; assert sys.version_info[:3] == (3,12,10)"
   & .\.venv\Scripts\python.exe -m pip install --no-index --find-links .\wheelhouse --require-hashes -r .\requirements-demo-windows.lock
   ```

   O lock com hashes e o wheelhouse são saídas da preparação futura, não arquivos já instalados no notebook nesta etapa. A opção `py -3.12` deve falhar na conferência se selecionar outra versão; não aceitá-la silenciosamente.

3. Gerar localmente senha do proprietário, senha do runtime, código de login sintético e chave de sessão com gerador criptográfico. Guardar somente o necessário em `state/secrets` com ACL restrita à conta; não imprimir valores nem passá-los como argumento de processo. Para `initdb`, usar arquivo local temporário de senha protegido, removido pelo procedimento aprovado após consumo. A senha não deve estar no pacote.
4. Executar `initdb` em **PGDATA novo** do notebook com UTF-8 e SCRAM. Fixar `listen_addresses=127.0.0.1`, porta 65437 e HBA somente para os pares banco/identidade exatos da demonstração, rejeitando outros destinos. Registrar SID, executáveis e PID localmente; os identificadores do desktop não podem ser reutilizados como prova de identidade.
5. Iniciar exclusivamente esse cluster com `pg_ctl -D <PGDATA-local> -w start`, conferir identidade e só então criar banco novo `maz_demo_notebook` a partir de `template0`. Abortar se já existir; retomada exige validação explícita de sua identidade e estado, sem limpeza automática. Não clonar nem copiar o PGDATA do desktop.
6. Usar a cadeia candidata externa já ensaiada: M0001; M0002–M0013 e H001–H011; M0014–M0019, total de 30 entradas. Rodar pelo controlador existente na ordem e com os preflights previstos, sobre banco vazio. Conferir ledger e checksums UTF-8/LF. M0017 depende de M0016; M0018 de M0017; M0019 de M0018. M0018 precisa ser aplicada antes da carga de metodologias. Não promover o manifesto operacional nem afirmar compatibilidade com banco municipal preexistente.
7. Criar ator local exclusivamente sintético e papel runtime sem superuser/createdb/createrole/replication/bypassrls. Conceder apenas operações já necessárias aos adaptadores, sem UPDATE de snapshots, DELETE, TRUNCATE ou DDL. Criar fontes e metodologia pelos contratos existentes, não por reescrita de histórico.
8. Carregar fixtures sintéticos em sequência controlada: série TRCS, 1000/250 kg e sucessora 1000/200 kg, competência 2055-11 e perímetro de homologação; obrigação O14-demo com revisão 2 em 7/13 e revisão 3 em 8/13, 2/3 documentos, responsável proposto e HV-OBR-001 conforme o congelamento. A carga deve guardar os UUIDs gerados e configurar explicitamente `MAZ_DEMO_TRCS_REFERENCIAS`, `MAZ_PAINEL_TRCS_REVISAO_REFERENCIA`, `MAZ_DEMO_PERIMETROS` e `MAZ_DEMO_OBRIGACOES` na instância. Não depender dos UUIDs físicos do desktop nem presumir equivalência jurídica. Fixture deve ser revisado e ensaiado antes de distribuição; não extrair dump com credenciais.
9. Injetar fábrica de conexão fixa para o banco local validado em `AdaptadorMAZ` e `AdaptadorObrigacoes`; chamar `criar_demo_maz` com ator, hash do código sintético e chave de sessão locais. Servir somente em `127.0.0.1:5055`, sem debug/reloader, sem iniciar `app.py` ou serviços operacionais.
10. Executar o aceite abaixo, registrar evidências e encerrar pelo lifecycle local. Confirmar identidade antes de `pg_ctl -D <PGDATA-local> -m fast -w stop`; fechar o servidor HTTP e confirmar ausência de listeners. Não usar kill genérico.

## Aceite no notebook e uso sem internet

- Conferir versões, hashes e todos os imports no ambiente limpo, sem acesso à rede externa ou banco existente.
- Realizar login/logout, autorização, CSRF e ausência de configuração sem fallback.
- Confirmar os dois percentuais TRCS, duas revisões G2, fontes independentes, histórico sem ação de escrita e rejeição de UUID stale/adulterado.
- Executar as suítes pertinentes em banco de ensaios separado; preservar o cenário principal e ledger por comparação antes/depois.
- Rever visualmente desktop e janela estreita, fontes, contraste, menu, filtros, prévia local e favicon.
- Testar startup/shutdown e nova inicialização offline. Falha deve manter logs e dados; não apagar estado automaticamente.
- Separar o resultado Windows da pendência POSIX dos oito testes de pitch. A demonstração Windows não autoriza implantação Linux/produção.

CSS, JavaScript, favicon e cálculos da candidata usam recursos locais. Isso permite planejar operação sem internet depois da preparação; só o ensaio no notebook poderá confirmar o pacote completo offline. O wheelhouse, o inicializador portátil e a carga reproduzível são pendências de preparação, não novas funcionalidades implementadas na 06A.
