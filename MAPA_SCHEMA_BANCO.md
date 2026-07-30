# Mapa do schema do banco

## Escopo e método

Inventário da Etapa H2C.1, realizado em 29/07/2026 somente pelos arquivos
versionados do `sistema-recic3`. Não foi usada a `DATABASE_URL`, não houve
conexão PostgreSQL, execução de SQL, migration ou criação de banco.

Atualização H2C.1B: a exportação externa e auditada do schema atual foi lida
estaticamente. Ela foi validada pelo SHA-256
`e2a9237b123aae8cab94e94055c9e31061b00f341536678b604f43f684c228cc`.
O SQL não foi executado nem copiado para o Git. O relatório completo está em
`RELATORIO_COMPARACAO_SCHEMA_ATUAL.md`.

Ordem de confiança das evidências:

1. migrations SQL numeradas;
2. DDL de `criar_tabelas_se_nao_existir()` no `app.py`;
3. SQL usado pelas rotas e serviços;
4. scripts administrativos legados;
5. testes e cursores simulados, apenas como evidência complementar.

Legenda: `NN` = `NOT NULL`; `?` = aceita nulo; `PK` = chave primária; `FK` =
chave estrangeira; `UQ` = único; `DEF` = default.

## Resultado geral

| Medida | Resultado |
|---|---:|
| Migrations SQL numeradas | 11 |
| Tabelas criadas pelas migrations numeradas | 23 |
| Colunas definidas pelas migrations numeradas | 351 |
| Tabelas legadas definidas no `app.py` | 12 |
| Tabelas usadas sem criação completa no repositório | 2 |
| Total de tabelas de aplicação identificadas | 37 |
| Declarações de FK nas migrations numeradas | 84 |
| FKs legadas distintas no `app.py` | 7 |
| Relacionamentos comprovados no total | 91 |
| Declarações `CREATE INDEX` nas migrations | 70 |
| Nomes distintos de índices explícitos | 69 |
| Restrições `CHECK` nas migrations | 103 |
| Colunas com definição não determinada | 41 |

O schema atual acrescenta a seguinte fotografia física:

| Medida | Schema atual |
|---|---:|
| Tabelas | 64 |
| Sequências | 62 |
| PKs | 63 |
| UNIQUE constraints | 32 |
| Índices normais / UNIQUE explícitos | 51 / 22 |
| FKs | 113 |
| CHECKs | 103 |
| Funções, triggers, views, tipos e extensões | 0 |

As 41 lacunas são as 37 colunas conhecidas pelo uso de `patrimonio`, duas de
`grupos_atividade` e `id_grupo` pressuposta em `subgrupos` e
`produtos_servicos`. Nomes históricos alternativos não foram contados.

Essa contagem de lacunas descreve apenas o que faltava no Git durante H2C.1.
Na H2C.1B, o dump resolveu a estrutura física de `patrimonio` e
`grupos_atividade` e comprovou que as duas colunas `id_grupo` não existem no
banco atual.

Critérios de contagem:

- coluna: cada definição de coluna dentro das 23 instruções `CREATE TABLE`;
- FK/relacionamento: cada constraint que contém uma cláusula `REFERENCES`;
  uma FK composta conta uma vez;
- legado: a mesma FK repetida num `CREATE` e num `ALTER` conta uma vez;
- índice: cada comando `CREATE INDEX`/`CREATE UNIQUE INDEX`, além da contagem
  separada de nomes distintos;
- check: cada ocorrência de `CHECK (`; checks na mesma linha contam
  separadamente;
- lacuna: coluna mencionada pelo código sem definição PostgreSQL completa.

## Matriz das 37 tabelas

| Tabela | Uso SQL | Finalidade | Origem do DDL | Situação |
|---|---|---|---|---|
| `cadastros` | `app.py` | clientes/fornecedores | `app.py` | completo + `associacao` incremental |
| `associados` | `app.py` | associados | `app.py` | completo + `foto_base64` incremental |
| `transacoes_financeiras` | `app.py` | documentos financeiros | `app.py` | completo + dois campos incrementais |
| `itens_transacao` | `app.py` | itens financeiros | `app.py` | completo |
| `subgrupos` | `app.py` e scripts | catálogo | `app.py`, `migracao_inteligente.py` | DDL conflitante/parcial |
| `produtos_servicos` | `app.py` e scripts | catálogo | `app.py` | completo, mas `id_grupo` é pressuposta |
| `contas_correntes` | `app.py` | contas por UVR | `app.py` | completo + `associacao` incremental |
| `fluxo_caixa` | `app.py` | movimentações | `app.py` | completo + campos incrementais |
| `fluxo_caixa_transacoes_link` | `app.py` | distribuição de pagamentos | `app.py` | completo |
| `denuncias` | `app.py` | denúncias legadas | `app.py` | completo |
| `usuarios` | `app.py` e scripts | login/auditoria | `app.py`, `criar_admin.py` | completo |
| `solicitacoes_alteracao` | `app.py` | aprovação de alterações | `app.py`, `criar_tabela_solicitacoes.py` | schema confirma versão A do `app.py` |
| `grupos_atividade` | scripts legados | catálogo de grupos | schema atual | DDL físico comprovado; relação pendente |
| `patrimonio` | `app.py` | patrimônio legado | schema atual | 38 colunas comprovadas |
| `fc_empresas` | módulo fiscal | contratadas | migration 001 | completo |
| `fc_servidores` | módulo fiscal | responsáveis | migration 002 | completo |
| `fc_contratos` | módulo fiscal | contratos | migration 003 | completo |
| `fc_contrato_responsaveis` | módulo fiscal | histórico de responsáveis | migration 003 | completo |
| `fc_aditivos` | módulo fiscal | aditivos | migration 004 | completo |
| `fc_documentos` | módulo fiscal | documentos privados | migration 005 | completo |
| `fc_planilhas_orcamentarias` | módulo fiscal | versões de planilha | migration 006 | completo |
| `fc_planilha_itens` | módulo fiscal | itens orçamentários | migration 006 | completo |
| `fc_ativos_contratuais` | módulo fiscal | ativos contratuais | migration 007 | completo |
| `fc_ativo_vinculos` | módulo fiscal | histórico de vínculos | migration 007 | completo |
| `fc_fiscalizacoes` | módulo fiscal | fiscalizações | migration 008 | completo |
| `fc_ocorrencias` | módulo fiscal | ocorrências | migration 008 | completo |
| `fc_ocorrencia_acompanhamentos` | módulo fiscal | histórico da ocorrência | migration 008 | completo |
| `fc_fiscalizacao_eventos` | módulo fiscal | eventos da fiscalização | migration 009 | completo |
| `fc_medicoes` | módulo fiscal | versões de medição | migration 010 | completo |
| `fc_medicao_itens` | módulo fiscal | itens medidos | migration 010 | completo |
| `fc_medicao_ajustes` | módulo fiscal | acréscimos/descontos/glosas | migration 010 | completo |
| `fc_medicao_documentos` | módulo fiscal | documentos da medição | migration 010 | completo |
| `fc_medicao_eventos` | módulo fiscal | eventos da medição | migration 010 | completo |
| `fc_atestes` | módulo fiscal | ateste/encaminhamento | migration 011 | completo |
| `fc_ateste_notas_fiscais` | módulo fiscal | notas fiscais | migration 011 | completo |
| `fc_ateste_documentos` | módulo fiscal | documentos do ateste | migration 011 | completo |
| `fc_ateste_eventos` | módulo fiscal | eventos do ateste | migration 011 | completo |

Rastreabilidade do uso do módulo: `empresas_service.py`,
`servidores_service.py`, `contratos_service.py`, `aditivos_service.py`,
`documentos_service.py`, `planilhas_service.py`, `ativos_service.py`,
`fiscalizacoes_service.py`, `ocorrencias_service.py`, `medicoes_service.py` e
`atestes_service.py`, todos em
`modulos/fiscalizacao_contratos/services/`. Os testes `test_fiscalizacao_*`
confirmam expectativas e fluxos com mocks, mas não são tratados como DDL real.

## Migrations numeradas

Todas ficam em `modulos/fiscalizacao_contratos/migrations/`, usam `BEGIN` e
`COMMIT`, não têm rollback reverso e não inserem dados.

| Nº | Arquivo | Tabelas | Índices | CHECKs | FKs | ALTER/INSERT | Dependências |
|---:|---|---:|---:|---:|---:|---:|---|
| 001 | `001_criar_fc_empresas.sql` | 1 | 1 | 3 | 2 | 0/0 | `usuarios` |
| 002 | `002_criar_fc_servidores.sql` | 1 | 1 | 2 | 2 | 0/0 | `usuarios` |
| 003 | `003_criar_fc_contratos.sql` | 2 | 5 | 8 | 7 | 0/0 | 001, 002, `usuarios` |
| 004 | `004_criar_fc_aditivos.sql` | 1 | 3 | 5 | 3 | 0/0 | 003, `usuarios` |
| 005 | `005_criar_fc_documentos.sql` | 1 | 5 | 6 | 4 | 0/0 | 003, 004, `usuarios` |
| 006 | `006_criar_fc_planilhas_orcamentarias.sql` | 2 | 8 | 11 | 7 | 0/0 | 003, 004, `usuarios` |
| 007 | `007_criar_fc_ativos_contratuais.sql` | 2 | 9 | 10 | 7 | 0/0 | 001, 003, `usuarios` |
| 008 | `008_criar_fc_fiscalizacoes_ocorrencias.sql` | 3 | 10 | 17 | 12 | 0/0 | 002, 003, 007, `usuarios` |
| 009 | `009_criar_fc_fiscalizacao_eventos.sql` | 1 | 1 | 5 | 2 | 0/0 | 008, `usuarios` |
| 010 | `010_criar_fc_medicoes.sql` | 5 | 14 | 23 | 22 | 0/0 | 002, 003, 005, 006, 008, `usuarios` |
| 011 | `011_criar_fc_atestes.sql` | 4 | 13 | 13 | 16 | 0/0 | 002, 005, 010, `usuarios` |

Todas usam `IF NOT EXISTS`. Isso permite repetição parcial, mas não é
idempotência confiável: uma tabela existente e divergente é silenciosamente
aceita.

## DDL e executores legados

| Arquivo | Efeito | Execução e risco |
|---|---|---|
| `app.py` | define 12 tabelas e alterações incrementais | funções não chamadas no import/startup atual |
| `executar_migracao_produtos.py` | chama migração de subgrupos do `app.py` | manual, com confirmação literal |
| `criar_admin.py` | cria `usuarios`; insere/atualiza admin | configuração histórica sensível; excluir da baseline |
| `criar_tabela_solicitacoes.py` | cria outra versão de `solicitacoes_alteracao` | diverge do `app.py` e tem fallback local antigo |
| `criar_coluna_foto.py` | adiciona `associados.foto_base64` | manual |
| `migracao_inteligente.py` | cria subgrupos, adiciona vínculo e migra dados | configuração histórica sensível |
| `fix_nomes_colunas.py` | renomeia colunas antigas | manual |
| `fix_fluxo.py` | adiciona `fluxo_caixa.associacao` | manual |
| `force_fix_academia.py` | remove colunas de `fluxo_caixa` | destrutivo; não usar na baseline |
| `atualizar_padrao_v2.py` | insere/atualiza catálogo | carga de dados |
| `importar_csv_nuvem.py` | insere grupos/subgrupos e atualiza produtos | pressupõe DDL ausente |
| `migrar_dados.py` | copia dados e reajusta sequências | não é criação de schema |
| `criar_usuario_uvr*.py` | insere/atualiza usuários | identidades e credenciais históricas |

Não existe executor das migrations 001–011, ledger, ordem formal executável ou
checksum. O `Procfile` inicia somente Gunicorn. Importar `app` e iniciar
Gunicorn não executam migration.

Foi localizada configuração histórica sensível em script legado; o arquivo não
deve integrar a baseline e a credencial deverá ser rotacionada na etapa
correspondente. Valores, usuários, hosts e URLs não são reproduzidos neste
documento.

## Schema legado comprovado

Tipos e regras abaixo vêm do DDL do `app.py`, inclusive os `ALTER` internos.

### `cadastros`

Operações: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.

```text
id SERIAL NN PK; uvr VARCHAR(10) NN; associacao VARCHAR(50)?;
data_hora_cadastro TIMESTAMP NN; razao_social VARCHAR(255) NN;
cnpj VARCHAR(14) NN; cep VARCHAR(8) NN; logradouro VARCHAR(255)?;
numero VARCHAR(20)?; bairro VARCHAR(100)?; cidade VARCHAR(100)?;
uf VARCHAR(2)?; telefone VARCHAR(20)?; tipo_atividade VARCHAR(255) NN;
tipo_cadastro VARCHAR(50) NN.
```

UQ `uq_cadastros_cnpj_tipo_uvr(cnpj, tipo_cadastro, uvr)`.

### `associados`

Operações: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.

```text
id SERIAL NN PK; numero VARCHAR(20) NN; uvr VARCHAR(10) NN;
associacao VARCHAR(50) NN; nome VARCHAR(255) NN; cpf VARCHAR(11) NN UQ;
rg VARCHAR(20) NN; data_nascimento DATE NN; data_admissao DATE NN;
status VARCHAR(20) NN; cep VARCHAR(8) NN; logradouro VARCHAR(255)?;
endereco_numero VARCHAR(20)?; bairro VARCHAR(100)?; cidade VARCHAR(100)?;
uf VARCHAR(2)?; telefone VARCHAR(20) NN; data_hora_cadastro TIMESTAMP NN;
foto_base64 TEXT?.
```

O schema atual acrescenta 12 colunas opcionais não declaradas nesse DDL:
`funcao`, datas/motivos/observações de afastamento, suspensão, exclusão e
readmissão. O código versionado não usa essas colunas; a baseline depende de
decisão funcional.

### `transacoes_financeiras`

Operações: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.

```text
id SERIAL NN PK; uvr VARCHAR(10) NN; associacao VARCHAR(50) NN;
id_cadastro_origem INTEGER? FK cadastros(id);
nome_cadastro_origem VARCHAR(255) NN; numero_documento VARCHAR(100)?;
data_documento DATE NN; tipo_transacao VARCHAR(20) NN;
tipo_atividade VARCHAR(255) NN; valor_total_documento DECIMAL(12,2) NN;
data_hora_registro TIMESTAMP NN; valor_pago_recebido DECIMAL(12,2) DEF 0.00;
status_pagamento VARCHAR(30) DEF 'Aberto'.
```

O schema atual acrescenta 13 colunas opcionais relacionadas a patrimônio,
motorista, combustível, medidor, manutenção e garantia. Não foi localizado uso
delas no código atual e `id_patrimonio` não possui FK. A baseline não deve
excluí-las nem promovê-las automaticamente sem decisão funcional.

### `itens_transacao`

Operações: `SELECT`, `INSERT` e substituição por exclusão/reinserção.

```text
id SERIAL NN PK; id_transacao INTEGER NN FK transacoes_financeiras(id)
ON DELETE CASCADE; descricao VARCHAR(255) NN; unidade VARCHAR(50) NN;
quantidade DECIMAL(10,3) NN; valor_unitario DECIMAL(12,2) NN;
valor_total_item DECIMAL(12,2) NN.
```

### `subgrupos`

Operações: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.

```text
id SERIAL NN PK; nome VARCHAR(255) NN; atividade_pai VARCHAR(255) NN.
```

UQ comprovada `(nome, atividade_pai)`. Um script usa `VARCHAR(150)` e outro
pressupõe UQ `(nome, id_grupo)`. O schema atual confirma `VARCHAR(255)` e
comprova a ausência de `id_grupo`; o script que exige essa coluna não é
compatível com a estrutura instalada.

### `produtos_servicos`

Operações: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.

```text
id SERIAL NN PK; tipo VARCHAR(20) NN; tipo_atividade VARCHAR(255) NN;
grupo VARCHAR(255)?; subgrupo VARCHAR(255)?; item VARCHAR(255) NN UQ;
data_hora_cadastro TIMESTAMP NN;
id_subgrupo INTEGER? FK subgrupos(id).
```

O schema atual comprova que `id_grupo` não existe. A relação física do produto
é somente com `subgrupos(id)`; o banco também possui
`DEFAULT now()` em `data_hora_cadastro`.

### `contas_correntes`

Operações: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.

```text
id SERIAL NN PK; uvr VARCHAR(10) NN; associacao VARCHAR(50) NN;
banco_codigo VARCHAR(10) NN; banco_nome VARCHAR(100) NN;
agencia VARCHAR(10) NN; conta_corrente VARCHAR(20) NN;
descricao_conta VARCHAR(255)?; data_hora_cadastro TIMESTAMP NN.
```

UQ `(uvr, banco_codigo, agencia, conta_corrente)`.

### `fluxo_caixa`

Operações: `SELECT`, `INSERT`, `UPDATE`, `DELETE`.

```text
id SERIAL NN PK; uvr VARCHAR(10) NN; associacao VARCHAR(50) NN;
tipo_movimentacao VARCHAR(20) NN; id_cadastro_cf INTEGER? FK cadastros(id);
nome_cadastro_cf VARCHAR(255)?; id_conta_corrente INTEGER NN FK contas_correntes(id);
numero_documento_bancario VARCHAR(100)?; data_efetiva DATE NN;
valor_efetivo DECIMAL(12,2) NN; saldo_operacao_calculado DECIMAL(12,2) NN;
data_hora_registro_fluxo TIMESTAMP NN; observacoes TEXT?;
categoria VARCHAR(100)?.
```

### `fluxo_caixa_transacoes_link`

Operações: `SELECT`, `INSERT` e cascata pelo movimento.

```text
id_fluxo_caixa INTEGER NN PK/FK fluxo_caixa(id) ON DELETE CASCADE;
id_transacao_financeira INTEGER NN PK/FK transacoes_financeiras(id);
valor_aplicado_nesta_nf DECIMAL(12,2) NN.
```

PK composta pelos dois IDs.

### `denuncias`

Operações: `SELECT`, `INSERT`, `UPDATE`; rotas desativadas online.

```text
id SERIAL NN PK; numero_denuncia VARCHAR(50) NN UQ;
data_registro TIMESTAMP NN; descricao TEXT NN;
status VARCHAR(50) DEF 'Pendente'; uvr VARCHAR(10)?; associacao VARCHAR(50)?.
```

### `usuarios`

Autenticação e autoria; scripts também fazem `INSERT` e `UPDATE`.

```text
id SERIAL NN PK; username VARCHAR(50) NN UQ;
password_hash VARCHAR(255) NN; nome_completo VARCHAR(100)?;
role VARCHAR(20) NN; uvr_acesso VARCHAR(50)?; ativo BOOLEAN DEF TRUE.
```

`SERIAL` comprova `usuarios.id INTEGER`, compatível com as 44 FKs de auditoria
do módulo.

O schema atual acrescenta três colunas opcionais não usadas pelo código
versionado: `email VARCHAR(255)`, `reset_token VARCHAR(255)` e
`reset_token_expira TIMESTAMP`. A inclusão delas na baseline depende de decisão
funcional.

### `solicitacoes_alteracao`

Operações: `SELECT`, `INSERT`, `UPDATE`. Definição do `app.py`:

```text
id SERIAL NN PK; tabela_alvo VARCHAR(50) NN; id_registro INTEGER NN;
tipo_solicitacao VARCHAR(20) NN; dados_novos JSONB?;
usuario_solicitante VARCHAR(50) NN;
data_solicitacao TIMESTAMP DEF CURRENT_TIMESTAMP;
status VARCHAR(20) DEF 'PENDENTE'; observacoes_admin TEXT?.
```

Drift: `criar_tabela_solicitacoes.py` usa `dados_novos TEXT`,
`usuario_solicitante VARCHAR(100)` anulável e `motivo_rejeicao TEXT`.

Classificação após H2C.1B: **schema atual e código confirmam a versão do
`app.py`**. Estão instalados `JSONB`, usuário obrigatório de 50 caracteres e
`observacoes_admin`; `motivo_rejeicao` não existe. O script antigo representa
uma versão B incompatível e não deve integrar a baseline. Falta apenas a
decisão formal de aposentá-lo.

## Tabelas pressupostas, sem DDL completo

### `grupos_atividade`

Usada por `importar_csv_nuvem.py` e `fix_nomes_colunas.py`.

```text
id INTEGER NN PK DEF nextval(grupos_atividade_id_seq);
nome VARCHAR(100) NN UQ.
```

O importador pressupõe corretamente `UNIQUE(nome)`, mas também pressupõe
`subgrupos.id_grupo` e `produtos_servicos.id_grupo`, que não existem. Não há
FK saindo ou chegando em `grupos_atividade`. A constraint UNIQUE conserva um
nome histórico que menciona `nome_grupo`, embora a coluna atual seja `nome`.
O DDL físico está determinado; a ligação funcional com o catálogo ainda
depende de decisão.

### `patrimonio`

Possui `SELECT`, `INSERT`, `UPDATE` e `DELETE`, mas nenhum `CREATE TABLE`
versionado. A H2C.1B comprovou 38 colunas:

```text
id INTEGER NN PK DEF nextval(patrimonio_id_seq);
uvr VARCHAR(50)?; associacao VARCHAR(100)?; tipo_bem VARCHAR(100)?;
categoria VARCHAR(100)?; descricao VARCHAR(255)?;
codigo_patrimonio VARCHAR(50)?; marca VARCHAR(100)?; modelo VARCHAR(100)?;
ano_fabricacao INTEGER?; numero_serie_chassi VARCHAR(100)?;
situacao_propriedade VARCHAR(100)?; entidade_proprietaria VARCHAR(100)?;
orgao_cedente VARCHAR(100)?; numero_termo_comodato VARCHAR(100)?;
data_inicio_comodato DATE?; data_fim_comodato DATE?;
placa VARCHAR(20)?; renavam VARCHAR(50)?; combustivel VARCHAR(50)?;
capacidade_carga VARCHAR(50)?; controle_por VARCHAR(50)?;
medidor_inicial NUMERIC(15,2)?; medidor_atual NUMERIC(15,2)?;
local_instalacao VARCHAR(150)?; setor_uso VARCHAR(100)?;
nome_responsavel VARCHAR(150)?; nome_operador_principal VARCHAR(150)?;
status_bem VARCHAR(50)?; estado_conservacao VARCHAR(50)?;
permite_abastecimento BOOLEAN?; permite_manutencao BOOLEAN?;
alerta_preventiva INTEGER?; observacoes_gerais TEXT?;
foto_bem_base64 TEXT?; eh_bem_publico BOOLEAN?; uso_compartilhado BOOLEAN?;
data_cadastro TIMESTAMP? DEF CURRENT_TIMESTAMP.
```

As 37 colunas antes inferidas pelo código existem; `data_cadastro` é a coluna
adicional e só aparece indiretamente em `SELECT *`. Não há FK, UNIQUE, CHECK ou
índice explícito além do índice implícito da PK. O código usa `uvr` para
autorização e mantém exclusão física. `transacoes_financeiras.id_patrimonio`
existe, mas não possui FK, portanto a exclusão pode deixar referência lógica
sem proteção do banco. A estrutura física deixou de ser lacuna; nulabilidade e
regra de exclusão continuam sendo decisões funcionais.

## Schema do módulo de Fiscalização de Contratos

As definições a seguir vêm diretamente das migrations.

### 001 `fc_empresas` — 17 colunas

```text
id BIGSERIAL NN PK; cnpj VARCHAR(14) NN UQ; razao_social VARCHAR(255) NN;
nome_fantasia VARCHAR(255)?; cep VARCHAR(8) NN; logradouro VARCHAR(255)?;
numero VARCHAR(30)?; bairro VARCHAR(120)?; cidade VARCHAR(120)?; uf CHAR(2)?;
telefone VARCHAR(30)?; email VARCHAR(254)?; ativo BOOLEAN NN DEF TRUE;
criado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks: CNPJ/CEP numéricos e UF. Índice
`idx_fc_empresas_ativo_razao_social`.

### 002 `fc_servidores` — 13 colunas

```text
id BIGSERIAL NN PK; nome VARCHAR(255) NN; matricula VARCHAR(50) NN UQ;
cargo VARCHAR(150)?; setor VARCHAR(150)?; email VARCHAR(254)?;
telefone VARCHAR(30)?; observacoes TEXT?; ativo BOOLEAN NN DEF TRUE;
criado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de nome/matrícula preenchidos. Índice `idx_fc_servidores_ativo_nome`.

### 003 `fc_contratos` — 16 colunas

```text
id BIGSERIAL NN PK; numero_contrato VARCHAR(100) NN UQ;
processo_administrativo VARCHAR(100)?; objeto TEXT NN;
empresa_id BIGINT NN FK fc_empresas(id); valor_original NUMERIC(15,2) NN;
data_assinatura DATE?; vigencia_inicio DATE?; vigencia_fim DATE?;
situacao VARCHAR(30) NN DEF 'Em elaboração'; observacoes TEXT?;
ativo BOOLEAN NN DEF TRUE; criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de preenchimento, valor, vigência e situação. Índices
`idx_fc_contratos_empresa`, `idx_fc_contratos_situacao_vigencia`.

### 003 `fc_contrato_responsaveis` — 12 colunas

```text
id BIGSERIAL NN PK; contrato_id BIGINT NN FK fc_contratos(id);
servidor_id BIGINT NN FK fc_servidores(id);
tipo_responsabilidade VARCHAR(30) NN; titular BOOLEAN NN DEF FALSE;
data_inicio DATE NN DEF CURRENT_DATE; data_fim DATE?; ativo BOOLEAN NN DEF TRUE;
criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de tipo, titularidade e período. Índices
`idx_fc_contrato_responsaveis_contrato`,
`uq_fc_contrato_responsavel_tipo_ativo`,
`uq_fc_contrato_titular_tipo_ativo`.

### 004 `fc_aditivos` — 19 colunas

```text
id BIGSERIAL NN PK; contrato_id BIGINT NN FK fc_contratos(id);
numero_termo VARCHAR(100) NN; tipo_aditivo VARCHAR(50) NN;
data_assinatura DATE NN; data_inicio_efeitos DATE?; dias_acrescidos INTEGER?;
nova_vigencia_fim DATE?; valor_acrescimo NUMERIC(15,2) NN DEF 0;
valor_supressao NUMERIC(15,2) NN DEF 0; percentual_alteracao NUMERIC(9,4)?;
descricao_alteracao TEXT?; justificativa TEXT?; observacoes TEXT?;
ativo BOOLEAN NN DEF TRUE; criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

UQ `(contrato_id, numero_termo)`; checks de tipo e não negatividade. Índices
`idx_fc_aditivos_contrato_ativo`, `idx_fc_aditivos_tipo_ativo`,
`idx_fc_aditivos_data_assinatura`, `uq_fc_aditivos_id_contrato_id`.

### 005 `fc_documentos` — 19 colunas

```text
id BIGSERIAL NN PK; contrato_id BIGINT NN FK fc_contratos(id); aditivo_id BIGINT?;
categoria VARCHAR(50) NN; titulo VARCHAR(200) NN; descricao TEXT?;
nome_original VARCHAR(255) NN; armazenamento_provedor VARCHAR(30) NN DEF 'cloudinary';
armazenamento_chave VARCHAR(500) NN UQ; armazenamento_versao BIGINT?;
mime_type VARCHAR(150) NN; extensao VARCHAR(10) NN; tamanho_bytes BIGINT NN;
sha256 VARCHAR(64) NN; ativo BOOLEAN NN DEF TRUE;
criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

FK composta `(aditivo_id, contrato_id) -> fc_aditivos(id, contrato_id)`.
Checks de categoria, título, provedor, tamanho, hash e extensão. Índices
`idx_fc_documentos_contrato_ativo`, `idx_fc_documentos_aditivo_ativo`,
`idx_fc_documentos_categoria_ativo`, `idx_fc_documentos_titulo`.

### 006 `fc_planilhas_orcamentarias` — 15 colunas

```text
id BIGSERIAL NN PK; contrato_id BIGINT NN FK fc_contratos(id); aditivo_id BIGINT?;
nome VARCHAR(200) NN; versao INTEGER NN; tipo_planilha VARCHAR(30) NN;
data_referencia DATE NN; descricao_referencia TEXT?;
status VARCHAR(20) NN DEF 'Em elaboração'; vigente BOOLEAN NN DEF FALSE;
ativo BOOLEAN NN DEF TRUE; criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

UQ `(contrato_id, versao)`; FK composta para aditivo/contrato; checks de nome,
versão, tipo, status e vigência. Índices `uq_fc_planilhas_original_contrato`,
`uq_fc_planilhas_vigente_ativa_contrato`, `idx_fc_planilhas_contrato_ativo`,
`idx_fc_planilhas_aditivo`, `idx_fc_planilhas_nome`.

### 006 `fc_planilha_itens` — 16 colunas

```text
id BIGSERIAL NN PK; planilha_id BIGINT NN FK fc_planilhas_orcamentarias(id);
ordem INTEGER NN; grupo VARCHAR(150)?; codigo_item VARCHAR(100)?;
descricao TEXT NN; unidade VARCHAR(50) NN; quantidade NUMERIC(24,8) NN;
valor_unitario NUMERIC(24,8) NN; fator_multiplicador NUMERIC(24,8) NN DEF 1;
observacoes TEXT?; ativo BOOLEAN NN DEF TRUE;
criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de ordem, preenchimento e valores. Índices
`idx_fc_planilha_itens_planilha_ativo_ordem`, `idx_fc_planilha_itens_grupo`.

### 007 `fc_ativos_contratuais` — 23 colunas

```text
id BIGSERIAL NN PK; codigo_interno VARCHAR(100) NN; tipo_ativo VARCHAR(40) NN;
descricao TEXT NN; marca VARCHAR(100)?; modelo VARCHAR(100)?;
ano_fabricacao INTEGER?; placa VARCHAR(20)?; renavam VARCHAR(30)?;
chassi VARCHAR(50)?; numero_serie VARCHAR(100)?; numero_patrimonio VARCHAR(100)?;
origem_ativo VARCHAR(20) NN; empresa_proprietaria_id BIGINT? FK fc_empresas(id);
capacidade NUMERIC(24,8)?; unidade_capacidade VARCHAR(50)?;
situacao VARCHAR(30) NN; observacoes TEXT?; ativo BOOLEAN NN DEF TRUE;
criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de campos, listas, ano e capacidade. Índices normalizados
`uq_fc_ativos_codigo_normalizado`, `uq_fc_ativos_placa_normalizada`,
`uq_fc_ativos_chassi_normalizado`, `uq_fc_ativos_patrimonio_normalizado`;
comuns `idx_fc_ativos_tipo_situacao_ativo`, `idx_fc_ativos_empresa`.

### 007 `fc_ativo_vinculos` — 13 colunas

```text
id BIGSERIAL NN PK; ativo_id BIGINT NN FK fc_ativos_contratuais(id);
contrato_id BIGINT NN FK fc_contratos(id); natureza_vinculo VARCHAR(40) NN;
data_inicio DATE NN; data_fim DATE?; principal BOOLEAN NN DEF FALSE;
observacoes TEXT?; ativo BOOLEAN NN DEF TRUE;
criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de natureza, datas e estado. Índices `idx_fc_ativo_vinculos_contrato`,
`idx_fc_ativo_vinculos_ativo`, `uq_fc_ativo_vinculo_ativo`.

### 008 `fc_fiscalizacoes` — 17 colunas

```text
id BIGSERIAL NN PK; contrato_id BIGINT NN FK fc_contratos(id);
servidor_responsavel_id BIGINT NN FK fc_servidores(id);
data_fiscalizacao DATE NN; hora_inicio TIME?; hora_fim TIME?;
tipo_fiscalizacao VARCHAR(40) NN; local_fiscalizacao TEXT?;
objeto_verificado TEXT NN; resultado VARCHAR(40) NN;
status VARCHAR(20) NN DEF 'Em elaboração'; observacoes TEXT?;
ativo BOOLEAN NN DEF TRUE; criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de listas, objeto e horário. Índices
`uq_fc_fiscalizacoes_id_contrato`, `idx_fc_fiscalizacoes_contrato_data`,
`idx_fc_fiscalizacoes_servidor`, `idx_fc_fiscalizacoes_status`.

### 008 `fc_ocorrencias` — 21 colunas

```text
id BIGSERIAL NN PK; contrato_id BIGINT NN FK fc_contratos(id);
fiscalizacao_id BIGINT?; ativo_contratual_id BIGINT? FK fc_ativos_contratuais(id);
servidor_responsavel_id BIGINT NN FK fc_servidores(id);
titulo VARCHAR(200) NN; categoria VARCHAR(50) NN; gravidade VARCHAR(20) NN;
descricao TEXT NN; data_identificacao DATE NN; prazo_correcao DATE?;
status VARCHAR(30) NN DEF 'Aberta'; exige_notificacao BOOLEAN NN DEF FALSE;
numero_notificacao VARCHAR(100)?; data_regularizacao DATE?; conclusao TEXT?;
ativo BOOLEAN NN DEF TRUE; criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

FK composta `(fiscalizacao_id, contrato_id) -> fc_fiscalizacoes(id,
contrato_id)`; checks de campos, listas, prazo, notificação e regularização.
Cinco índices `idx_fc_ocorrencias_*` por contrato/status, fiscalização, ativo
contratual, prazo e gravidade.

### 008 `fc_ocorrencia_acompanhamentos` — 10 colunas

```text
id BIGSERIAL NN PK; ocorrencia_id BIGINT NN FK fc_ocorrencias(id);
data_acompanhamento DATE NN; status_anterior VARCHAR(30) NN;
status_novo VARCHAR(30) NN; descricao TEXT NN; providencia_contratada TEXT?;
observacoes TEXT?; criado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id).
```

Checks de status e descrição. Índice
`idx_fc_acompanhamentos_ocorrencia_data`.

### 009 `fc_fiscalizacao_eventos` — 8 colunas

```text
id BIGSERIAL NN PK; fiscalizacao_id BIGINT NN FK fc_fiscalizacoes(id);
tipo_evento VARCHAR(20) NN; status_anterior VARCHAR(20) NN;
status_novo VARCHAR(20) NN; justificativa TEXT?;
criado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id).
```

Checks de tipo, status, justificativa e transição. Índice
`idx_fc_fiscalizacao_eventos_fiscalizacao_data`.

### 010 `fc_medicoes` — 26 colunas

```text
id BIGSERIAL NN PK; contrato_id BIGINT NN FK fc_contratos(id);
numero_medicao INTEGER NN; competencia DATE NN; periodo_inicio DATE NN;
periodo_fim DATE NN; versao INTEGER NN DEF 1;
medicao_origem_id BIGINT? FK fc_medicoes(id); atual BOOLEAN NN DEF TRUE;
servidor_fiscal_id BIGINT NN FK fc_servidores(id); data_apresentacao DATE?;
status VARCHAR(40) NN DEF 'Em elaboração';
valor_bruto/total_acrescimos/total_descontos/total_glosas/valor_liquido
NUMERIC(18,2) NN DEF 0; observacoes TEXT?; aprovado_em TIMESTAMPTZ?;
servidor_aprovador_id BIGINT? FK fc_servidores(id);
aprovado_por_usuario_id INTEGER? FK usuarios(id); ativo BOOLEAN NN DEF TRUE;
criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

UQ `(contrato_id, numero_medicao, versao)`; checks de número, competência,
período, versão, status, valores e aprovação. Índices
`uq_fc_medicoes_atual_ativa_competencia`,
`idx_fc_medicoes_contrato_competencia_status`, `idx_fc_medicoes_versoes`,
`idx_fc_medicoes_servidor_fiscal`, `idx_fc_medicoes_atuais`.

### 010 `fc_medicao_itens` — 18 colunas

```text
id BIGSERIAL NN PK; medicao_id BIGINT NN FK fc_medicoes(id);
planilha_item_id BIGINT? FK fc_planilha_itens(id); ordem INTEGER NN;
codigo_item VARCHAR(100)?; descricao TEXT NN; unidade VARCHAR(50) NN;
quantidade_prevista NUMERIC(24,8)?; quantidade_medida NUMERIC(24,8) NN;
preco_unitario NUMERIC(24,8) NN; valor_medido NUMERIC(18,2) NN;
justificativa_excedente TEXT?; observacoes TEXT?; ativo BOOLEAN NN DEF TRUE;
criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de ordem, campos, quantidades, preço, valor e excedente. Índices
`uq_fc_medicao_item_planilha_ativo`, `idx_fc_medicao_itens_medicao_ordem`,
`idx_fc_medicao_itens_planilha`.

### 010 `fc_medicao_ajustes` — 13 colunas

```text
id BIGSERIAL NN PK; medicao_id BIGINT NN FK fc_medicoes(id);
tipo_ajuste VARCHAR(20) NN; descricao TEXT NN; valor NUMERIC(18,2) NN;
fiscalizacao_id BIGINT? FK fc_fiscalizacoes(id);
ocorrencia_id BIGINT? FK fc_ocorrencias(id); observacoes TEXT?;
ativo BOOLEAN NN DEF TRUE; criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de tipo, descrição e valor. Índices por medição/tipo, ocorrência e
fiscalização.

### 010 `fc_medicao_documentos` — 10 colunas

```text
id BIGSERIAL NN PK; medicao_id BIGINT NN FK fc_medicoes(id);
documento_id BIGINT NN FK fc_documentos(id); categoria VARCHAR(40) NN;
observacoes TEXT?; ativo BOOLEAN NN DEF TRUE;
criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Check de categoria. Índices `uq_fc_medicao_documento_ativo`,
`idx_fc_medicao_documentos_medicao`.

### 010 `fc_medicao_eventos` — 13 colunas

```text
id BIGSERIAL NN PK; medicao_id BIGINT NN FK fc_medicoes(id);
tipo_evento VARCHAR(40) NN; status_anterior VARCHAR(40)?;
status_novo VARCHAR(40) NN; justificativa TEXT?;
valor_bruto/total_acrescimos/total_descontos/total_glosas/valor_liquido
NUMERIC(18,2) NN; criado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id).
```

Checks de tipo, status, justificativa e valores. Índice
`idx_fc_medicao_eventos_medicao_data`.

### 011 `fc_atestes` — 18 colunas

```text
id BIGSERIAL NN PK; medicao_id BIGINT NN FK fc_medicoes(id);
numero_ateste INTEGER NN; servidor_atestador_id BIGINT NN FK fc_servidores(id);
data_ateste DATE?; status VARCHAR(40) NN DEF 'Em elaboração'; parecer TEXT?;
observacoes TEXT?; valor_atestado NUMERIC(18,2) NN;
protocolo_encaminhamento VARCHAR(200)?; encaminhado_em TIMESTAMPTZ?;
servidor_encaminhador_id BIGINT? FK fc_servidores(id);
encaminhado_por_usuario_id INTEGER? FK usuarios(id); ativo BOOLEAN NN DEF TRUE;
criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de número, status, valor, data e encaminhamento. Seis índices
`uq_fc_atestes_*`/`idx_fc_atestes_*`.

### 011 `fc_ateste_notas_fiscais` — 14 colunas

```text
id BIGSERIAL NN PK; ateste_id BIGINT NN FK fc_atestes(id);
numero_nota VARCHAR(100) NN; serie VARCHAR(50)?; data_emissao DATE NN;
valor_nota NUMERIC(18,2) NN; chave_acesso VARCHAR(100)?;
documento_id BIGINT? FK fc_documentos(id); observacoes TEXT?;
ativo BOOLEAN NN DEF TRUE; criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Checks de número e valor. Índices `uq_fc_ateste_nota_ativa`,
`idx_fc_ateste_notas_ateste`, `idx_fc_ateste_notas_numero_serie`,
`idx_fc_ateste_notas_chave`.

### 011 `fc_ateste_documentos` — 10 colunas

```text
id BIGSERIAL NN PK; ateste_id BIGINT NN FK fc_atestes(id);
documento_id BIGINT NN FK fc_documentos(id); categoria VARCHAR(40) NN;
observacoes TEXT?; ativo BOOLEAN NN DEF TRUE;
criado_em/atualizado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id);
atualizado_por_usuario_id INTEGER? FK usuarios(id).
```

Check de categoria. Índices `uq_fc_ateste_documento_ativo`,
`idx_fc_ateste_documentos_ateste`.

### 011 `fc_ateste_eventos` — 10 colunas

```text
id BIGSERIAL NN PK; ateste_id BIGINT NN FK fc_atestes(id);
tipo_evento VARCHAR(50) NN; status_anterior VARCHAR(40)?;
status_novo VARCHAR(40) NN; justificativa TEXT?;
valor_atestado NUMERIC(18,2) NN; total_notas NUMERIC(18,2) NN;
criado_em TIMESTAMPTZ NN DEF CURRENT_TIMESTAMP;
criado_por_usuario_id INTEGER NN FK usuarios(id).
```

Checks de tipo, status, justificativa e valores. Índice
`idx_fc_ateste_eventos_ateste_data`.

## Relacionamentos

As sete FKs legadas antes comprovadas no Git são:

- transação → cadastro;
- item → transação (`ON DELETE CASCADE`);
- produto → subgrupo;
- fluxo → cadastro;
- fluxo → conta;
- link → fluxo (`ON DELETE CASCADE`);
- link → transação.

Não existem tabelas `uvrs` ou `associacoes`: o vínculo é texto. Solicitações
também não têm FK, pois guardam nome da tabela e ID genericamente.

O schema atual contém 29 FKs legadas. As 22 adicionais pertencem às tabelas de
auditoria, documentos, EPI e ouvidoria que só existem no banco atual. Somadas
às 84 FKs do módulo, explicam as 113 FKs físicas. A lista nominal e as ações de
exclusão estão em `RELATORIO_COMPARACAO_SCHEMA_ATUAL.md`.

Nas migrations há 84 FKs: 44 para `usuarios`, 8 para `fc_contratos`, 7 para
`fc_servidores`, 6 para `fc_medicoes`, 3 para `fc_fiscalizacoes`, 3 para
`fc_documentos`, 3 para `fc_atestes`, 2 para `fc_empresas`, 2 para
`fc_aditivos`, 2 para `fc_ativos_contratuais`, 2 para `fc_ocorrencias`, 1 para
`fc_planilhas_orcamentarias` e 1 para `fc_planilha_itens`.

Nenhuma FK do módulo declara `ON DELETE`/`ON UPDATE`; vale `NO ACTION`. FKs
compostas preservam o contrato do aditivo/fiscalização. Não há ciclo entre
tabelas distintas; há autorreferência opcional em `fc_medicoes`.

## Índices e constraints

Comprovados:

- 70 comandos de índice, 69 nomes distintos;
- `uq_fc_aditivos_id_contrato_id` é repetido em 005 e 006;
- 23 PKs, 7 UQs internas, 103 checks e 84 FKs no módulo;
- 12 PKs, sete unicidades de negócio e sete FKs distintas no DDL legado;
- índices parciais e por expressão em responsáveis, planilhas, ativos,
  medições e atestes.

O índice repetido é `uq_fc_aditivos_id_contrato_id`, na tabela `fc_aditivos`,
colunas `(id, contrato_id)`. As migrations 005 e 006 executam o mesmo `CREATE
UNIQUE INDEX IF NOT EXISTS`. Em sequência, a 005 cria e a 006 ignora a
repetição, sem falhar. A redundância deve ser removida do estado consolidado da
baseline e considerada pelo controle/checksum da H2D, sem editar as migrations
históricas.

Índices compostos exigidos pelas FKs:

- `uq_fc_aditivos_id_contrato_id`;
- `uq_fc_fiscalizacoes_id_contrato`.

Recomendações ainda não existentes — não incluir automaticamente na baseline:
índices nas FKs legadas usadas em joins, em `fc_medicoes.medicao_origem_id` e
em FKs de documentos/auditoria somente se consultas reais justificarem.

## Tipos dos identificadores

- as 12 tabelas do DDL legado usam `SERIAL`, portanto PK `INTEGER`, salvo a
  tabela de link cuja PK é composta por dois `INTEGER`;
- `usuarios.id` é comprovadamente `INTEGER/SERIAL`;
- as 23 tabelas do módulo usam `BIGSERIAL`, portanto PK `BIGINT`;
- todas as FKs entre tabelas `fc_*` são `BIGINT`;
- as 44 FKs do módulo para `usuarios.id` são `INTEGER`;
- não foi encontrada incompatibilidade de tipo nas migrations 001–011;
- `patrimonio.id` e `grupos_atividade.id` são `INTEGER` com sequência;
- `subgrupos.id_grupo` e `produtos_servicos.id_grupo` não existem no schema
  atual.

## Exclusões físicas legadas

Foram encontradas 15 ocorrências de `DELETE FROM`, atingindo nove tabelas:
`itens_transacao`, `associados`, `cadastros`, `contas_correntes`,
`transacoes_financeiras`, `patrimonio`, `subgrupos`, `produtos_servicos` e
`fluxo_caixa`.

| Tabela | Rota/função e condição | Escopo | Relações/cascata comprovadas e risco |
|---|---|---|---|
| `itens_transacao` | `editar_transacao`: `WHERE id_transacao = %s`; `responder_solicitacao` repete na aprovação de edição | edição direta só para admin; usuário comum solicita | FK para transação com cascata; itens são apagados/recriados na mesma transação, mas o histórico individual anterior é perdido |
| `associados` | `excluir_associado` e `responder_solicitacao`: `WHERE id = %s` | admin apaga; usuário limitado por UVR solicita ao admin | sem dependente comprovado; perda irreversível |
| `cadastros` | `excluir_cadastro` e `responder_solicitacao`: `WHERE id = %s` | admin apaga; usuário limitado por UVR solicita | referenciado por transações e fluxo, sem cascata; pode bloquear |
| `contas_correntes` | `excluir_conta_corrente` e aprovação: `WHERE id = %s` | rota e execução final administrativas | referenciada por fluxo, sem cascata; tratamento espera erro de integridade |
| `transacoes_financeiras` | `excluir_transacao` e aprovação: `WHERE id = %s` | admin apaga; usuário limitado por UVR solicita | itens têm cascata; link não tem cascata; pode apagar itens ou ser bloqueada |
| `patrimonio` | `excluir_patrimonio` e aprovação: `WHERE id = %s` | admin apaga; usuário limitado por UVR solicita | 38 colunas comprovadas; sem FKs; `transacoes_financeiras.id_patrimonio` pode ficar órfão |
| `subgrupos` | `api_subgrupos`: `WHERE id = %s` | `admin_json_required` | produto referencia subgrupo sem cascata; código tenta bloquear quando há uso |
| `produtos_servicos` | `api_produtos_crud`: `WHERE id = %s` | `admin_json_required` | sem FK filha comprovada; código faz busca textual em itens antes de apagar |
| `fluxo_caixa` | `excluir_movimentacao`: `WHERE id = %s` | `admin_json_required` | links têm cascata; função estorna valores antes da exclusão |

O módulo `fc_*` não usa `DELETE`. A baseline não deve inventar cascatas.

## Dados de referência

| Fonte | Classe | Baseline |
|---|---|---|
| listas de tipos/status em checks `fc_*` | A estrutural | manter constraints |
| `padrao_itens.csv` e `padrao_itens2.csv` (50 linhas cada e conteúdo divergente) | B catálogo opcional | decidir e carregar separadamente |
| importadores de CSV | B/C carga administrativa | não executar |
| migração de subgrupos derivada de produtos atuais | D dado existente | excluir |
| scripts de admin/usuários UVR | D identidade/credencial | excluir |
| `migrar_dados.py` | D cópia histórica/real | excluir |
| dados operacionais e documentos | D real/histórico | excluir |

Nenhum registro obrigatório foi comprovado. A baseline não deve criar
administrador.

## Recursos PostgreSQL comprovados

`SERIAL`, `BIGSERIAL`, sequências implícitas, `JSONB`, `TIMESTAMP`,
`TIMESTAMPTZ`, índices parciais/por expressão, checks com regex, `EXTRACT`,
`CURRENT_TIMESTAMP`, `CURRENT_DATE` e `SELECT ... FOR UPDATE`.

Não foram encontrados extensões, enums PostgreSQL, views, materialized views,
triggers, funções SQL, arrays, UUID de banco ou colunas geradas.

## Convenções

- legado: plural, `SERIAL`, `TIMESTAMP`, poucas constraints;
- módulo: prefixo `fc_`, `BIGSERIAL`, auditoria por `usuarios.id INTEGER`,
  `TIMESTAMPTZ`, inativação por `ativo` e nomes `ck_`, `uq_`, `fk_`, `idx_`;
- eventos/acompanhamentos históricos não são editáveis;
- nomes de status/tipo devem ser preservados, pois o código depende deles.

## Drift entre migrations, código e testes

| Divergência | Classificação |
|---|---|
| `patrimonio` sem DDL | objeto não documentado |
| `grupos_atividade` sem DDL | objeto não documentado |
| `id_grupo` em duas tabelas | ausente no banco; script legado incompatível |
| duas versões de `solicitacoes_alteracao` | provável evolução manual divergente |
| `subgrupos.nome` com 255 ou 150 | drift legado |
| fixer admite `nome_subgrupo`/`nome_grupo` antigos | histórico desconhecido |
| índice de aditivos repetido em 005/006 | redundância segura |
| `IF NOT EXISTS` pode ocultar schema errado | risco de drift silencioso |
| testes usam mocks e comparação textual | não comprovam schema real |
| documento antigo dizia haver migration no import | superado pelo código/testes |
| scripts antigos têm configurações/credenciais padrão | risco histórico |

Atualização H2C.1B:

- as 23 tabelas `fc_` não apresentam drift estrutural em relação às migrations;
- a versão A de `solicitacoes_alteracao` está instalada e é usada pelo código;
- `subgrupos.nome` está instalado como `VARCHAR(255)`;
- há 27 tabelas adicionais sem uso SQL identificado no Git;
- `associados`, `transacoes_financeiras` e `usuarios` possuem colunas adicionais
  que exigem decisão funcional;
- sete colunas de data/hora legadas possuem `DEFAULT now()` não declarado no
  `CREATE TABLE` atual;
- a ausência de tabela `uvr` é coerente com o modelo textual do sistema.

## Suficiência do repositório

Resposta após H2C.1B: **C — ainda exige decisões funcionais**.

A exportação somente do schema já foi obtida, auditada e comparada. As lacunas
técnicas de `patrimonio`, `grupos_atividade`, `solicitacoes_alteracao` e UVR
foram esclarecidas. A baseline ainda não deve ser criada porque é necessário
decidir o destino das 27 tabelas adicionais, das colunas históricas extras e
das divergências funcionais do catálogo e do patrimônio.

O schema atual é evidência do que existe, não aprovação automática do que deve
ser recriado. O dump e o manifesto permanecem fora do Git.

## Decisões funcionais da H2C.2A

A H2C.2A definiu que nenhuma das 64 tabelas, coluna ou dado será removido
durante esta fase. As classificações funcionais servem para orientar análise,
visibilidade e prioridade; elas não autorizam exclusão nem inclusão automática
na futura baseline.

Foram analisadas individualmente as 27 tabelas adicionais. Elas permanecem
preservadas e ocultas até que cada conjunto possua responsável, regra de negócio
e critério de aceite. Também foram validadas como direções futuras:

- substituir a exclusão cotidiana de patrimônio por inativação e reativação;
- estruturar gradualmente o catálogo como grupo, subgrupo e produto;
- manter a versão A de `solicitacoes_alteracao` como referência oficial;
- evoluir UVR textual para cadastro e vínculo por identificador, preservando
  compatibilidade durante a transição;
- preservar as colunas históricas adicionais até decisão específica;
- manter os 11 módulos de Fiscalização de Contratos administrativos enquanto
  não houver uma nova matriz de perfis aprovada.

O detalhamento e a sequência dos incrementos estão em
`MATRIZ_DECISOES_FUNCIONAIS_H2C2A.md`.

## Especificação funcional do patrimônio — H2C.2B

A análise de leitura confirmou cinco rotas de patrimônio e uma única área em
`templates/cadastro.html`. O cadastro, a consulta, os detalhes, a edição e a
exclusão estão concentrados no `app.py`.

As 38 colunas foram classificadas. A recomendação é preservar todas, reutilizar
`status_bem` após caracterizar os valores existentes e introduzir histórico de
situações de forma aditiva. `data_cadastro` permanece automática, somente para
consulta, e valores nulos antigos não devem receber datas inventadas.

O cadastro atual não aplica no servidor a proteção de UVR já usada na consulta
e na edição. A exclusão física também não verifica a referência lógica em
`transacoes_financeiras.id_patrimonio`. Esses são bloqueadores para a evolução
segura.

A especificação completa está em
`ESPECIFICACAO_FUNCIONAL_PATRIMONIO_H2C2B.md`.

## Especificação funcional do catálogo — H2C.2C

A leitura confirmou quatro estruturas centrais ou relacionadas:
`grupos_atividade` (2 campos), `subgrupos` (3),
`produtos_servicos` (8) e a tabela adicional `produtos` (8). O único vínculo
físico atual do catálogo operacional é `produtos_servicos.id_subgrupo` para
`subgrupos.id`; não há `id_grupo` nas tabelas esperadas pelo importador antigo.

O desenho futuro deverá ser aditivo: grupo oficial, subgrupo ligado ao grupo e
produto ligado ao subgrupo. Os textos antigos serão preservados durante a
transição, e registros pendentes continuarão visíveis como “Não classificado”.
Não haverá fusão automática com `produtos`, patrimônio, EPI ou catálogos do
módulo Fiscalização.

Os detalhes, campos, riscos e incrementos H2C.3C.1–H2C.3C.12 estão em
`ESPECIFICACAO_FUNCIONAL_CATALOGO_H2C2C.md`.

## Especificação funcional de UVRs — H2C.2D

A leitura do código confirmou que não existe entidade central de UVR ou
associação. `usuarios.uvr_acesso` fornece o escopo textual e as colunas `uvr`
dos registros são usadas para autorização, pesquisa e relatórios. As colunas
`associacao` são textuais e não participam diretamente dos helpers de
autorização.

Cadastros, associados, contas correntes, transações, fluxo de caixa, denúncias e
patrimônio armazenam UVR e associação. A interface contém opções e pares fixos,
sem validação institucional no servidor. Nenhuma FK ou cardinalidade entre os
conceitos foi confirmada.

O modelo desejado foi aprovado: associação e UVR serão entidades distintas; uma
associação poderá possuir várias UVRs; usuários terão vínculos explícitos e uma
UVR principal; e os textos atuais serão legado transitório.

A inexistência atual de tabela `uvr` não é erro do dump. As novas estruturas
serão desenhadas em migration futura, sem inventar DDL nesta especificação. Os
detalhes aprovados estão em `ESPECIFICACAO_FUNCIONAL_UVR_H2C2D.md`.
