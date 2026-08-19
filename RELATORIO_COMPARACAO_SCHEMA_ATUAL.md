# Relatório de comparação do schema atual

## 1. Identificação e limites

Etapa H2C.1B, realizada em 29/07/2026 por leitura estática de uma exportação
somente de estrutura. O arquivo foi identificado pelo SHA-256:

`e2a9237b123aae8cab94e94055c9e31061b00f341536678b604f43f684c228cc`

O hash recalculado coincide com o manifesto. O arquivo tem 168.740 bytes,
permanece fora do repositório e não foi alterado, importado ou executado.
Também não houve conexão PostgreSQL, uso da `DATABASE_URL`, execução de
migration, API ou deploy.

A auditoria confirmou ausência de registros, `COPY ... FROM stdin`, `INSERT`,
`UPDATE`, `DELETE`, `CREATE DATABASE`, `DROP`, owners, privilégios, URLs de
conexão e credenciais. O SQL não é reproduzido neste documento.

## 2. Metodologia

Foi usado um parser temporário, removível e não versionado, para:

1. separar os comandos sem executá-los;
2. reconstruir tabelas, colunas, tipos, nulabilidade, defaults e sequências;
3. associar constraints declaradas posteriormente por `ALTER TABLE`;
4. distinguir PK, UNIQUE constraint, índice normal e índice UNIQUE;
5. associar FKs, regras `ON DELETE`/`ON UPDATE` e índices;
6. comparar o resultado com as migrations 001–011;
7. pesquisar referências SQL no código e nos testes;
8. confrontar os resultados com `MAPA_SCHEMA_BANCO.md`.

Nomes e expressões foram comparados de forma conservadora. Espaços foram
normalizados, mas expressões semanticamente diferentes não foram tratadas como
iguais. Quando o PostgreSQL reescreveu a forma textual de uma expressão, isso
foi registrado separadamente.

## 3. Contagens reproduzidas

| Objeto | Contagem | Critério |
|---|---:|---|
| schemas | 1 | `CREATE SCHEMA` |
| tabelas | 64 | `CREATE TABLE` |
| sequências | 62 | `CREATE SEQUENCE` |
| alterações de tabela | 270 | comandos `ALTER TABLE` |
| índices explícitos | 73 | 51 normais + 22 UNIQUE |
| chaves primárias | 63 | constraints PK; uma tabela não tem PK |
| UNIQUE constraints | 32 | constraints, sem contar PK ou índices |
| índices UNIQUE independentes | 22 | `CREATE UNIQUE INDEX` |
| unicidades sem PK | 54 | 32 constraints + 22 índices |
| chaves estrangeiras | 113 | uma FK composta conta uma vez |
| CHECKs | 103 | constraints CHECK efetivas |
| funções | 0 | `CREATE FUNCTION` |
| triggers | 0 | `CREATE TRIGGER` |
| views/materialized views | 0 | `CREATE VIEW` |
| tipos customizados/enums | 0 | `CREATE TYPE` |
| extensões | 0 | `CREATE EXTENSION` |

As contagens preliminares foram confirmadas. A separação acima evita somar PK,
UNIQUE constraints e índices UNIQUE como se fossem a mesma categoria.

## 4. Classificação das 64 tabelas

Classes usadas:

- **B**: tabela do módulo Fiscalização de Contratos;
- **C**: tabela legada utilizada pelo sistema ou por operação administrativa
  legada comprovada;
- **D**: existe no banco, mas não foi localizado uso SQL no código versionado;
- **E**: auxiliar ou histórica, também sem uso SQL atual identificado.

Nenhuma tabela foi classificada como obsoleta. Classes D e E são decisões
pendentes e não autorizam exclusão da futura baseline.

| Tabela | Classe | Evidência | Módulo provável | Recomendação preliminar |
|---|---|---|---|---|
| `cadastros` | C | SQL no `app.py`; DDL legado | cadastro | preservar estado desejado após revisar defaults |
| `associados` | C | SQL no `app.py`; DDL legado | associados | decidir 12 colunas adicionais |
| `transacoes_financeiras` | C | SQL no `app.py`; DDL legado | financeiro | decidir 13 colunas adicionais |
| `itens_transacao` | C | SQL no `app.py`; DDL legado | financeiro | preservar |
| `subgrupos` | C | SQL no `app.py`; DDL legado | catálogo | preservar estrutura atual; não inventar `id_grupo` |
| `produtos_servicos` | C | SQL no `app.py`; DDL legado | catálogo | preservar estrutura atual; não inventar `id_grupo` |
| `contas_correntes` | C | SQL no `app.py`; DDL legado | financeiro | preservar após revisar default |
| `fluxo_caixa` | C | SQL no `app.py`; DDL legado | financeiro | preservar após revisar default |
| `fluxo_caixa_transacoes_link` | C | SQL no `app.py`; DDL legado | financeiro | preservar |
| `denuncias` | C | SQL no `app.py`; rotas online desativadas | legado | decisão funcional antes da baseline |
| `usuarios` | C | login, autorização e auditoria | identidade | preservar; decidir três colunas adicionais |
| `solicitacoes_alteracao` | C | SQL e DDL no `app.py` | aprovação legada | usar versão A como candidata validada |
| `grupos_atividade` | C | scripts administrativos legados | catálogo | estrutura comprovada; relação funcional pendente |
| `patrimonio` | C | SQL no `app.py` e testes | patrimônio | DDL comprovado; revisar exclusão física |
| `fc_empresas` | B | migration 001, serviço e testes | fiscalização | migration é fonte primária |
| `fc_servidores` | B | migration 002, serviço e testes | fiscalização | migration é fonte primária |
| `fc_contratos` | B | migration 003, serviços e testes | fiscalização | migration é fonte primária |
| `fc_contrato_responsaveis` | B | migration 003 e serviço | fiscalização | migration é fonte primária |
| `fc_aditivos` | B | migration 004 e serviços | fiscalização | migration é fonte primária |
| `fc_documentos` | B | migration 005 e serviços | fiscalização | migration é fonte primária |
| `fc_planilhas_orcamentarias` | B | migration 006 e serviços | fiscalização | migration é fonte primária |
| `fc_planilha_itens` | B | migration 006 e serviços | fiscalização | migration é fonte primária |
| `fc_ativos_contratuais` | B | migration 007 e serviços | fiscalização | migration é fonte primária |
| `fc_ativo_vinculos` | B | migration 007 e serviços | fiscalização | migration é fonte primária |
| `fc_fiscalizacoes` | B | migration 008 e serviços | fiscalização | migration é fonte primária |
| `fc_ocorrencias` | B | migration 008 e serviços | fiscalização | migration é fonte primária |
| `fc_ocorrencia_acompanhamentos` | B | migration 008 e serviço | fiscalização | migration é fonte primária |
| `fc_fiscalizacao_eventos` | B | migration 009 e serviço | fiscalização | migration é fonte primária |
| `fc_medicoes` | B | migration 010 e serviços | fiscalização | migration é fonte primária |
| `fc_medicao_itens` | B | migration 010 e serviço | fiscalização | migration é fonte primária |
| `fc_medicao_ajustes` | B | migration 010 e serviço | fiscalização | migration é fonte primária |
| `fc_medicao_documentos` | B | migration 010 e serviço | fiscalização | migration é fonte primária |
| `fc_medicao_eventos` | B | migration 010 e serviço | fiscalização | migration é fonte primária |
| `fc_atestes` | B | migration 011 e serviços | fiscalização | migration é fonte primária |
| `fc_ateste_notas_fiscais` | B | migration 011 e serviço | fiscalização | migration é fonte primária |
| `fc_ateste_documentos` | B | migration 011 e serviço | fiscalização | migration é fonte primária |
| `fc_ateste_eventos` | B | migration 011 e serviço | fiscalização | migration é fonte primária |
| `auditoria_associados` | E | somente banco; FKs para associado/documento | auditoria | decisão funcional; não excluir |
| `auditoria_passo1_observacoes` | E | somente banco | auditoria | decisão funcional; não excluir |
| `auditoria_passo2_observacoes` | E | somente banco | auditoria | decisão funcional; não excluir |
| `auditoria_rateios` | E | somente banco; FK para associado | auditoria | decisão funcional; não excluir |
| `auditoria_rateios_transacoes` | E | somente banco; duas FKs | auditoria | decisão funcional; não excluir |
| `auditoria_relatorios` | E | somente banco | auditoria | decisão funcional; não excluir |
| `cadastro_pessoa_fisica` | D | somente banco, sem PK | importação/cadastro | origem e necessidade devem ser confirmadas |
| `documentos` | D | somente banco; duas FKs | documentos legado | não confundir com `fc_documentos` |
| `entrega_documentos_itens` | E | somente banco; quatro FKs | entrega documental | decisão funcional; não excluir |
| `entrega_documentos_lotes` | E | somente banco | entrega documental | decisão funcional; não excluir |
| `entrega_documentos_pacotes` | E | somente banco | entrega documental | decisão funcional; não excluir |
| `epi_entrega_itens` | E | somente banco; duas FKs | EPI | decisão funcional; não excluir |
| `epi_entregas` | E | somente banco; duas FKs | EPI | decisão funcional; não excluir |
| `epi_estoque` | E | somente banco; FK para item | EPI | decisão funcional; não excluir |
| `epi_itens` | D | somente banco | EPI | decisão funcional; não excluir |
| `epi_movimentos` | E | somente banco; FK para item | EPI | decisão funcional; não excluir |
| `epi_solicitacoes` | E | somente banco; FK para item | EPI | decisão funcional; não excluir |
| `epis` | D | somente banco | EPI alternativo | esclarecer duplicidade conceitual |
| `epis_catalogo` | D | somente banco | catálogo de EPI | esclarecer duplicidade conceitual |
| `ouvidoria_grupos` | D | somente banco | ouvidoria | decisão funcional; não excluir |
| `ouvidoria_manifestacao_fotos` | E | somente banco; FK para manifestação | ouvidoria | decisão funcional; não excluir |
| `ouvidoria_manifestacoes` | D | somente banco | ouvidoria | decisão funcional; não excluir |
| `ouvidoria_subgrupos` | D | somente banco; FK para grupo | ouvidoria | decisão funcional; não excluir |
| `ouvidoria_subtipos` | D | somente banco; FK para tipo | ouvidoria | decisão funcional; não excluir |
| `ouvidoria_tipos` | D | somente banco; FK para subgrupo | ouvidoria | decisão funcional; não excluir |
| `produtos` | D | somente banco | catálogo alternativo | esclarecer relação com `produtos_servicos` |
| `tipos_documentos` | D | somente banco | documentos legado | decisão funcional; não excluir |

Resumo: 23 tabelas B, 14 tabelas C, 12 tabelas D e 15 tabelas E.

## 5. As 27 tabelas adicionais

As 27 tabelas abaixo explicam integralmente a diferença entre as 37 tabelas
antes conhecidas pelo repositório e as 64 presentes no banco.

| Tabela | Estrutura resumida | Relações | Finalidade provável e risco |
|---|---|---|---|
| `auditoria_associados` | 11 colunas; PK; 2 unicidades | associado e documento, ambas com cascata | auditoria mensal; omitir pode perder histórico funcional |
| `auditoria_passo1_observacoes` | 5 colunas; PK; UNIQUE por UVR | nenhuma | observações de auditoria |
| `auditoria_passo2_observacoes` | 6 colunas; PK; UNIQUE por UVR/período | nenhuma | observações de auditoria |
| `auditoria_rateios` | 9 colunas; PK; 2 unicidades | associado com cascata | auditoria de rateio |
| `auditoria_rateios_transacoes` | 10 colunas; PK; 2 unicidades | associado e transação, com cascata | auditoria financeira |
| `auditoria_relatorios` | 9 colunas; PK; UNIQUE de relatório | nenhuma | metadados de relatórios |
| `cadastro_pessoa_fisica` | 3 colunas (`codigo`, nome e CPF); sem PK | nenhuma | provável tabela de importação; maior incerteza |
| `documentos` | 15 colunas; PK | tipo e transação | documentos legados; distinta de `fc_documentos` |
| `entrega_documentos_itens` | 19 colunas; PK | lote, pacote, tipo e documento | itens de entrega documental |
| `entrega_documentos_lotes` | 9 colunas; PK; UNIQUE UVR/período | nenhuma | lotes de entrega |
| `entrega_documentos_pacotes` | 10 colunas; PK; 2 unicidades | nenhuma | pacotes por etapa |
| `epi_entrega_itens` | 6 colunas; PK | entrega e item | itens entregues |
| `epi_entregas` | 9 colunas; PK | associado e responsável | histórico de entregas |
| `epi_estoque` | 7 colunas; PK; UNIQUE item/UVR | item | estoque |
| `epi_itens` | 8 colunas; PK; UNIQUE de nome | nenhuma | catálogo operacional de EPI |
| `epi_movimentos` | 10 colunas; PK | item | movimentos de estoque |
| `epi_solicitacoes` | 13 colunas; PK | item | solicitações de alteração |
| `epis` | 7 colunas; PK; UNIQUE de nome | nenhuma | catálogo alternativo |
| `epis_catalogo` | 8 colunas; PK; UNIQUE composta | nenhuma | catálogo alternativo detalhado |
| `ouvidoria_grupos` | 5 colunas; PK; UNIQUE de nome | nenhuma | classificação de ouvidoria |
| `ouvidoria_manifestacao_fotos` | 5 colunas; PK | manifestação com cascata | anexos de manifestação |
| `ouvidoria_manifestacoes` | 39 colunas; PK; protocolo único | nenhuma FK para a classificação | manifestações e localização |
| `ouvidoria_subgrupos` | 5 colunas; PK; UNIQUE composta | grupo com `RESTRICT` | hierarquia de ouvidoria |
| `ouvidoria_subtipos` | 5 colunas; PK; UNIQUE composta | tipo com `RESTRICT` | hierarquia de ouvidoria |
| `ouvidoria_tipos` | 5 colunas; PK; UNIQUE composta | subgrupo com `RESTRICT` | hierarquia de ouvidoria |
| `produtos` | 8 colunas; PK | nenhuma | catálogo alternativo |
| `tipos_documentos` | 8 colunas; PK | nenhuma | tipos de documento legado |

Não foi localizado uso SQL dessas 27 tabelas em código ou testes versionados.
Isso comprova ausência de uso identificado, não obsolescência. A exclusão de
qualquer uma da baseline exige decisão funcional sobre módulos possivelmente
externos, antigos ou ainda não versionados.

## 6. Patrimônio

O banco contém 38 colunas. As 37 antes inferidas pelo uso existem; a coluna
adicional é `data_cadastro`.

| Coluna | Tipo real | Nulo/default | Uso atual |
|---|---|---|---|
| `id` | `INTEGER` | NN; sequência; PK | sim |
| `uvr` | `VARCHAR(50)` | nulo | sim, autorização por objeto |
| `associacao` | `VARCHAR(100)` | nulo | sim |
| `tipo_bem` | `VARCHAR(100)` | nulo | sim |
| `categoria` | `VARCHAR(100)` | nulo | sim |
| `descricao` | `VARCHAR(255)` | nulo | sim |
| `codigo_patrimonio` | `VARCHAR(50)` | nulo | sim |
| `marca` | `VARCHAR(100)` | nulo | sim |
| `modelo` | `VARCHAR(100)` | nulo | sim |
| `ano_fabricacao` | `INTEGER` | nulo | sim |
| `numero_serie_chassi` | `VARCHAR(100)` | nulo | sim |
| `situacao_propriedade` | `VARCHAR(100)` | nulo | sim |
| `entidade_proprietaria` | `VARCHAR(100)` | nulo | sim |
| `orgao_cedente` | `VARCHAR(100)` | nulo | sim |
| `numero_termo_comodato` | `VARCHAR(100)` | nulo | sim |
| `data_inicio_comodato` | `DATE` | nulo | sim |
| `data_fim_comodato` | `DATE` | nulo | sim |
| `placa` | `VARCHAR(20)` | nulo | sim |
| `renavam` | `VARCHAR(50)` | nulo | sim |
| `combustivel` | `VARCHAR(50)` | nulo | sim |
| `capacidade_carga` | `VARCHAR(50)` | nulo | sim |
| `controle_por` | `VARCHAR(50)` | nulo | sim |
| `medidor_inicial` | `NUMERIC(15,2)` | nulo | sim |
| `medidor_atual` | `NUMERIC(15,2)` | nulo | sim |
| `local_instalacao` | `VARCHAR(150)` | nulo | sim |
| `setor_uso` | `VARCHAR(100)` | nulo | sim |
| `nome_responsavel` | `VARCHAR(150)` | nulo | sim |
| `nome_operador_principal` | `VARCHAR(150)` | nulo | sim |
| `status_bem` | `VARCHAR(50)` | nulo | sim |
| `estado_conservacao` | `VARCHAR(50)` | nulo | sim |
| `permite_abastecimento` | `BOOLEAN` | nulo | sim |
| `permite_manutencao` | `BOOLEAN` | nulo | sim |
| `alerta_preventiva` | `INTEGER` | nulo | sim |
| `observacoes_gerais` | `TEXT` | nulo | sim |
| `foto_bem_base64` | `TEXT` | nulo | sim |
| `eh_bem_publico` | `BOOLEAN` | nulo | sim |
| `uso_compartilhado` | `BOOLEAN` | nulo | sim |
| `data_cadastro` | `TIMESTAMP` | nulo; `CURRENT_TIMESTAMP` | apenas aparece no `SELECT *` |

Não há FK, UNIQUE, CHECK ou índice explícito. A PK cria seu índice implícito.
`transacoes_financeiras.id_patrimonio` existe, mas não possui FK. Portanto, a
exclusão física de patrimônio não é bloqueada por essa coluna e pode deixar
referência lógica sem integridade declarativa. O schema físico está agora
comprovado, mas a nulabilidade ampla e a exclusão física ainda exigem decisão
de negócio antes da baseline.

## 7. Grupos, subgrupos e produtos

`grupos_atividade` possui:

- `id INTEGER NOT NULL`, sequência e PK;
- `nome VARCHAR(100) NOT NULL`;
- UNIQUE em `nome` (o nome histórico da constraint ainda menciona
  `nome_grupo`);
- nenhuma FK e nenhum índice explícito adicional.

`subgrupos` confirma exatamente o DDL atual do `app.py`: `id SERIAL`,
`nome VARCHAR(255) NOT NULL`, `atividade_pai VARCHAR(255) NOT NULL` e UNIQUE
`(nome, atividade_pai)`. A divergência 150/255 fica resolvida em favor de 255
como estado instalado e código atual.

`produtos_servicos` confirma oito colunas, inclusive `id_subgrupo INTEGER`
opcional com FK para `subgrupos(id)`, sem cascata. O banco acrescenta
`DEFAULT now()` a `data_hora_cadastro`; o código sempre fornece esse valor.

Não existem `subgrupos.id_grupo` nem `produtos_servicos.id_grupo`. Logo,
`grupos_atividade` não está fisicamente ligado a essas tabelas. O script antigo
que exige essas colunas não é compatível com o schema atual e não deve orientar
a baseline sem uma migration futura deliberada.

## 8. Solicitações de alteração

| Coluna | Versão A (`app.py`) | Versão B (script antigo) | Schema atual | Uso atual/conclusão |
|---|---|---|---|---|
| `id` | SERIAL PK | SERIAL PK | INTEGER, sequência, PK | confirma ambas |
| `tabela_alvo` | VARCHAR(50) NN | VARCHAR(50) NN | VARCHAR(50) NN | confirma ambas |
| `id_registro` | INTEGER NN | INTEGER NN | INTEGER NN | confirma ambas |
| `tipo_solicitacao` | VARCHAR(20) NN | VARCHAR(20) NN | VARCHAR(20) NN | confirma ambas |
| `dados_novos` | JSONB | TEXT | JSONB | confirma A; código aceita objeto ou texto |
| `usuario_solicitante` | VARCHAR(50) NN | VARCHAR(100) nulo | VARCHAR(50) NN | confirma A |
| `data_solicitacao` | TIMESTAMP/default | TIMESTAMP/default | TIMESTAMP/default | confirma ambas |
| `status` | VARCHAR(20)/default | VARCHAR(20)/default | VARCHAR(20)/default | confirma ambas |
| `observacoes_admin` | TEXT | ausente | TEXT | confirma A; sem uso atual localizado |
| `motivo_rejeicao` | ausente | TEXT | ausente | B não está instalada |

Conclusão: o schema atual e o código atual confirmam integralmente a versão A.
O script da versão B é legado incompatível e deve ficar fora da baseline. Falta
apenas a decisão formal de aposentá-lo; não há lacuna técnica nessa tabela.

## 9. UVR e usuários

Não existe tabela `uvr`, nem tabela cujo nome contenha `uvr`. Também não existe
SQL `FROM/JOIN/INSERT/UPDATE/DELETE` dirigido a uma tabela com esse nome.

UVR é uma denominação funcional armazenada como texto:

- o usuário possui `usuarios.uvr_acesso VARCHAR(50)`;
- `current_user.uvr_acesso` limita consultas e alterações por objeto;
- tabelas legadas guardam `uvr` e, em vários casos, `associacao` como texto;
- não há FK central de UVR ou associação.

Portanto, não há drift causado pela ausência de uma tabela `uvr`: o vínculo
real entre usuário e UVR está em `usuarios.uvr_acesso`. Criar uma tabela apenas
pelo nome funcional seria inventar um modelo novo.

`usuarios` possui dez colunas:

- estrutura usada: `id INTEGER`/sequência/PK, `username VARCHAR(50)` UNIQUE NN,
  `password_hash VARCHAR(255)` NN, `nome_completo VARCHAR(100)`, `role
  VARCHAR(20)` NN, `uvr_acesso VARCHAR(50)` e `ativo BOOLEAN DEFAULT TRUE`;
- colunas adicionais no banco: `email VARCHAR(255)`, `reset_token
  VARCHAR(255)` e `reset_token_expira TIMESTAMP`, todas opcionais;
- não há CHECK ou FK; somente PK e unicidade de `username`.

O login usa os sete primeiros campos estruturais. Não foi localizado uso atual
das três colunas adicionais. Elas devem ser preservadas como decisão pendente,
sem copiar valores ou hashes.

## 10. Migrations 001–011

| Migration | Tabelas comparadas | Resultado |
|---:|---|---|
| 001 | `fc_empresas` | sem drift estrutural |
| 002 | `fc_servidores` | sem drift estrutural |
| 003 | `fc_contratos`, `fc_contrato_responsaveis` | sem drift estrutural |
| 004 | `fc_aditivos` | sem drift estrutural |
| 005 | `fc_documentos` | sem drift estrutural |
| 006 | `fc_planilhas_orcamentarias`, `fc_planilha_itens` | sem drift estrutural |
| 007 | `fc_ativos_contratuais`, `fc_ativo_vinculos` | sem drift estrutural |
| 008 | três tabelas de fiscalização/ocorrência | sem drift estrutural |
| 009 | `fc_fiscalizacao_eventos` | sem drift estrutural |
| 010 | cinco tabelas de medição | sem drift estrutural |
| 011 | quatro tabelas de ateste | sem drift estrutural |

Foram comparados nomes e conjuntos de colunas, tipos equivalentes
`BIGSERIAL`/`BIGINT + sequência`, nulabilidade, defaults, PKs, FKs, UNIQUEs,
CHECKs e índices. As 23 tabelas, 84 FKs, 103 CHECKs e 69 nomes distintos de
índice previstos estão presentes. Não foi encontrado objeto previsto e
ausente, objeto `fc_` extra ou mudança estrutural relevante.

### Índice repetido dos aditivos

`uq_fc_aditivos_id_contrato_id` existe uma única vez no banco, como índice
UNIQUE de `fc_aditivos(id, contrato_id)`. As migrations 005 e 006 declaram o
mesmo comando. Na ordem 001–011, a 005 cria e a 006 tolera a repetição por
`IF NOT EXISTS`. A 006 isolada, antes da criação de `fc_aditivos`, falharia.
A baseline deve declarar esse índice uma única vez depois da tabela.

## 11. CHECKs

Os 103 CHECKs do banco pertencem às tabelas `fc_`. Todos os 103 pares
`tabela + nome da constraint` previstos nas migrations existem; não há CHECK
`fc_` extra ou ausente.

Na comparação estrita após normalizar somente espaços, as 103 expressões têm
texto diferente porque o PostgreSQL adicionou parênteses, casts e reescreveu
alguns `IN` como `ANY (ARRAY[...])`. A revisão estrutural conservadora não
encontrou mudança de coluna, operador, valor permitido ou regra. Ainda assim, a
baseline deve partir das expressões legíveis das migrations, e não copiar
automaticamente a serialização do dump.

As tabelas legadas não possuem CHECKs no schema atual.

## 12. Chaves estrangeiras

As 84 FKs das migrations coincidem estruturalmente com as 84 FKs `fc_` do
banco. As 29 FKs legadas explicam o total 113. Sete já estavam documentadas;
as outras 22 pertencem às tabelas adicionais:

| Origem | Destino | Ação de exclusão |
|---|---|---|
| `auditoria_associados.id_associado` | `associados.id` | CASCADE |
| `auditoria_associados.id_documento` | `documentos.id` | CASCADE |
| `auditoria_rateios.id_associado` | `associados.id` | CASCADE |
| `auditoria_rateios_transacoes.id_associado` | `associados.id` | CASCADE |
| `auditoria_rateios_transacoes.id_transacao` | `transacoes_financeiras.id` | CASCADE |
| `documentos.id_tipo` | `tipos_documentos.id` | NO ACTION |
| `documentos.id_transacao_origem` | `transacoes_financeiras.id` | NO ACTION |
| `entrega_documentos_itens.id_documento_origem` | `documentos.id` | NO ACTION |
| `entrega_documentos_itens.id_lote` | `entrega_documentos_lotes.id` | CASCADE |
| `entrega_documentos_itens.id_tipo_documento` | `tipos_documentos.id` | NO ACTION |
| `entrega_documentos_itens.id_pacote` | `entrega_documentos_pacotes.id` | CASCADE |
| `epi_entrega_itens.id_entrega` | `epi_entregas.id` | CASCADE |
| `epi_entrega_itens.id_item` | `epi_itens.id` | NO ACTION |
| `epi_entregas.id_associado` | `associados.id` | NO ACTION |
| `epi_entregas.id_responsavel` | `associados.id` | NO ACTION |
| `epi_estoque.id_item` | `epi_itens.id` | NO ACTION |
| `epi_movimentos.id_item` | `epi_itens.id` | NO ACTION |
| `epi_solicitacoes.id_epi` | `epi_itens.id` | NO ACTION |
| `ouvidoria_manifestacao_fotos.id_manifestacao` | `ouvidoria_manifestacoes.id` | CASCADE |
| `ouvidoria_subgrupos.id_grupo` | `ouvidoria_grupos.id` | RESTRICT |
| `ouvidoria_subtipos.id_tipo` | `ouvidoria_tipos.id` | RESTRICT |
| `ouvidoria_tipos.id_subgrupo` | `ouvidoria_subgrupos.id` | RESTRICT |

Todas usam `ON UPDATE NO ACTION`. Não há ciclo entre tabelas distintas. Existe
somente a autorreferência opcional de `fc_medicoes.medicao_origem_id`.

## 13. Índices

As migrations possuem 70 declarações, mas 69 nomes: o índice dos aditivos é
repetido. Os 69 índices distintos estão presentes no banco.

O banco tem quatro índices explícitos adicionais, todos UNIQUE e ligados às
tabelas extras:

- `uq_auditoria_associados_uvr_periodo_associado`;
- `uq_auditoria_rateios_chave`;
- `uq_auditoria_rateios_transacoes_chave`;
- `uq_entrega_documentos_pacote_uvr_periodo_passo`.

Assim, 69 + 4 = 73 índices explícitos. Quinze índices `fc_` têm diferença
apenas na forma textual produzida pelo PostgreSQL para predicados parciais ou
expressões; tabela, colunas, unicidade e regra foram preservadas. PKs e UNIQUE
constraints possuem índices implícitos e não foram recontados como `CREATE
INDEX`.

## 14. Sequências e compatibilidade de IDs

As 62 sequências:

- possuem `OWNED BY`;
- aparecem no default `nextval(...)` da respectiva coluna `id`;
- não apresentam sequência órfã;
- atendem 39 IDs `INTEGER` legados e 23 IDs `BIGINT` das tabelas `fc_`.

O mapeamento é regular: `<tabela>_id_seq` pertence a `<tabela>.id` em todas as
tabelas, exceto:

- `fluxo_caixa_transacoes_link`, cuja PK é composta e não usa sequência;
- `cadastro_pessoa_fisica`, que não possui PK nem sequência.

Todas as FKs comparadas têm tipo compatível com suas PKs. Em particular,
`usuarios.id` é `INTEGER/SERIAL`, e as 44 referências de auditoria do módulo
também são `INTEGER`.

## 15. Outros objetos

Não existem funções, triggers, views, materialized views, enums, tipos
customizados ou extensões declaradas no schema `public`. Também não há
`COMMENT ON`. Nenhum desses objetos precisa ser incluído na baseline com base
na evidência atual.

## 16. Drift consolidado

| Objeto | Banco atual | Migration/código/testes | Classe | Ação recomendada |
|---|---|---|---|---|
| 23 tabelas `fc_` | coincide | migrations 001–011 e testes | 1 — sem drift | usar migrations como fonte |
| `patrimonio` | 38 colunas, quase tudo anulável | código usa 37; sem DDL | 3 — compatível, agora explicado | decidir nulabilidade e exclusão física |
| `grupos_atividade` | duas colunas, sem relações | apenas scripts | 6 — decisão funcional | decidir se entra desconectada |
| `subgrupos.nome` | `VARCHAR(255)` | `app.py` usa 255; script usa 150 | 2 — drift documental | adotar 255 |
| colunas `id_grupo` | ausentes | somente importador antigo pressupõe | 7 — script legado incompatível | não criar sem migration futura |
| `solicitacoes_alteracao` | versão A | código usa A; script usa B | 3 — explicável | baseline candidata: A |
| `usuarios` | três colunas opcionais extras | código não as usa | 6 — decisão funcional | decidir preservação |
| `associados` | 12 colunas opcionais extras | não estão no DDL/código atual | 7 — possível legado | avaliação funcional |
| `transacoes_financeiras` | 13 colunas opcionais extras | não estão no DDL/código atual | 7 — possível legado | avaliação funcional |
| defaults de data em sete tabelas | `now()` no banco | código fornece valores | 3 — compatível | decidir estado desejado |
| 27 tabelas adicionais | somente banco | sem uso SQL versionado | 8 — informação funcional insuficiente | não omitir até decisão |
| quatro índices extras | tabelas adicionais | ausentes no Git | 7 — legado | acompanhar decisão das tabelas |
| 22 FKs extras | tabelas adicionais | ausentes no Git | 7 — legado | preservar se tabelas entrarem |
| índice de aditivos repetido | uma instância | migrations 005/006 | 2 — drift documental | baseline declara uma vez |
| ausência de `uvr` | não existe | vínculo textual em `usuarios` | 1 — sem drift | não criar tabela |

Os defaults adicionais são `now()` em `associados.data_hora_cadastro`,
`cadastros.data_hora_cadastro`, `contas_correntes.data_hora_cadastro`,
`denuncias.data_registro`, `fluxo_caixa.data_hora_registro_fluxo`,
`produtos_servicos.data_hora_cadastro` e
`transacoes_financeiras.data_hora_registro`.

## 17. Fonte de verdade recomendada

| Grupo | Fonte primária | Motivo |
|---|---|---|
| tabelas `fc_` | migrations 001–011 | estado desejado versionado e confirmado pelo banco |
| comportamento e campos realmente usados | código e testes | expressam a aplicação atual |
| estrutura física legada sem DDL | schema atual | única evidência técnica completa |
| tabelas/colunas adicionais sem uso | decisão funcional | existência não prova necessidade futura |
| constraints e índices `fc_` | migrations, validadas pelo schema | forma legível e versionada |
| constraints e índices extras | schema atual + decisão funcional | não existem no repositório |

O dump comprova o que existe, mas não decide sozinho o que deve existir.

## 18. Decisões pendentes antes da H2C.2

1. decidir quais das 27 tabelas adicionais pertencem ao sistema que será
   reconstruído em banco vazio;
2. decidir se os conjuntos de auditoria, documentos, EPI e ouvidoria são
   módulos ativos, arquivados ou externos ao repositório;
3. decidir a permanência das 12 colunas extras de `associados`;
4. decidir a permanência das 13 colunas extras de
   `transacoes_financeiras`;
5. decidir a permanência dos três campos opcionais de recuperação em
   `usuarios`;
6. decidir se os defaults `now()` instalados fazem parte do estado desejado;
7. confirmar que `grupos_atividade` deve permanecer sem FK ou planejar uma
   migration futura, sem inventar `id_grupo` na baseline;
8. aprovar formalmente a versão A de `solicitacoes_alteracao` e aposentar o
   script B;
9. decidir se `patrimonio` manterá nulabilidade ampla e exclusão física;
10. decidir se catálogos CSV são carga opcional, fora da baseline;
11. definir versão/checksum e blocos transacionais esperados pela futura H2D.

## 19. Prontidão para H2C.2

Classificação: **C — ainda exige decisões funcionais**.

As lacunas técnicas de `patrimonio`, `grupos_atividade`,
`solicitacoes_alteracao` e UVR foram esclarecidas. Porém, copiar as 64 tabelas
automaticamente poderia perpetuar módulos sem código, colunas históricas e
drift acumulado; copiar somente as 37 poderia omitir estruturas ainda
necessárias. A H2C.2 deve começar somente depois das decisões acima.

## 20. Verificações

- hash e tamanho do dump conferidos;
- parser estático reproduziu todas as contagens;
- 64 tabelas e 27 adicionais representadas no relatório;
- migrations, código e testes comparados sem executar SQL;
- 673 testes automatizados aprovados, com zero falhas e zero erros;
- sintaxe dos arquivos Python rastreados aprovada;
- PostgreSQL, Cloudinary e APIs reais bloqueados durante os testes;
- nenhum dump, manifesto ou ferramenta temporária incluído no Git;
- nenhuma credencial incluída nos documentos.
