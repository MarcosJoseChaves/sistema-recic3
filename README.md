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
