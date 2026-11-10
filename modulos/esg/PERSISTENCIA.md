# Persistência proposta — ESG-02B1

Artefatos para revisão humana. **RF-1A permanece NO-GO OPERACIONAL**.
Nenhuma conexão, SQL, migration, carga histórica ou serviço foi executado.
O repositório não está ligado às rotas ou à interface ESG-01.

## Esquema aditivo

`migrations/001_criar_esg_indicadores.sql` segue a numeração local, prefixos de
tabelas/índices e o envelope `BEGIN`/`COMMIT` do módulo de fiscalização. Não
altera tabelas ou migrations anteriores, não contém carga de dados, extensões,
triggers, funções ou comandos de permissão. As referências a `usuarios(id)`
reutilizam o cadastro de autores do RECIC3 e exigem o núcleo já existente.

| Tabela | Conteúdo e integridade proposta |
| --- | --- |
| `esg_fontes_documentais` | identidade textual, referência e data documental; fonte compartilhada sem sobrescrita |
| `esg_perimetros` | código e descrição de abrangência, comparados integralmente |
| `esg_unidades` | símbolo e descrição, sem conversões implícitas |
| `esg_definicoes` | campos do contrato, unidade, perímetro, fontes, situação, revisão e autoria |
| `esg_registros` | UUID, definição/perímetro, ano/mês, `NUMERIC` anulável, fonte, referência temporal, situação, revisão e autoria |
| `esg_evidencias` | UUID, FK para o registro, tipo, fonte e observações |

O período é um objeto de valor, representado por `ano` e `mes` no registro;
não há catálogo independente de períodos. Dois índices parciais impedem duplicar
um ano completo ou uma competência mensal para o mesmo indicador. Ano completo
e meses coexistem sem soma. A FK composta de registro para definição garante o
mesmo código de perímetro. UUIDs e fontes de evidência têm vínculos obrigatórios.
Exclusões em cascata não são previstas.

`NUMERIC` sem escala fixa evita arredondamento deliberado pela aplicação.
`None` é enviado como NULL; zero continua `Decimal("0")`. Valores permanecem
`Decimal` nos parâmetros, nunca float. Datas são enviadas como `date`. UUIDs são
parametrizados como texto com cast PostgreSQL explícito e reconstruídos como
`UUID` na leitura; não há registro global de adaptadores. Enums usam seus valores
textuais na escrita e são reconstituídos em tipos explícitos na leitura.
Limites nativos de armazenamento e compatibilidade do driver ainda precisam de
validação PostgreSQL autorizada; erros numéricos são tratados sem quantização.

Todas as tabelas registram `criado_em` e `criado_por_usuario_id`. Definições e
registros reservam `fonte_aprovacao_id`, `aprovado_em` e
`aprovado_por_usuario_id`, com constraints para consistência desses metadados.
Esses campos não são preenchidos pelos métodos de cadastro. Não há fluxo de
aprovação ou histórico completo de decisões nesta entrega.

## Uso do repositório

`RepositorioESG(conectar_banco)` recebe uma fábrica, não uma conexão global.
O construtor não a invoca e não importa driver ou `app.py`. Cada chamada recebe
uma conexão dedicada, aberta, ociosa e com `autocommit=False`. O adaptador deve
oferecer a API de conexão PostgreSQL usada no RECIC3 (`closed`, `autocommit`,
`get_transaction_status`, `set_session`, `cursor`, `commit`, `rollback`, `close`).
Cursores devem devolver tuplas DB-API e `NUMERIC` como `Decimal`.

| Método | Resultado e regra |
| --- | --- |
| `salvar_definicao(definicao, usuario_id)` | inclui somente contrato proposto, sem fonte de aprovação; devolve o contrato |
| `salvar_registro(registro, usuario_id, evidencias=())` | inclui somente rascunho, fonte e evidências opcionais na mesma transação |
| `salvar_fonte(fonte, usuario_id)` | cadastra/reutiliza somente referência documental idêntica |
| `associar_evidencia(evidencia, usuario_id)` | inclui fonte/evidência apenas para registro ainda rascunho; bloqueia o registro durante a operação |
| `obter_definicao(codigo)` | contrato ou `None` se ausente |
| `obter_registro(codigo, periodo, perimetro)` | contrato ou `None` se definição/período ausente; recorte divergente é erro |
| `listar_registros(codigo, perimetro)` | `SerieIndicador` com definição, registros/estados/fontes/evidências; série vazia se não há registros, `None` se não há definição |

O autor é um inteiro positivo fornecido pelo código autenticado do servidor,
jamais um campo confiável de formulário. O repositório não substitui autenticação
ou autorização. Dicionários de formulários não são aceitos como contratos.
Cadastros rejeitam qualquer situação diferente da inicial e qualquer fonte de
aprovação recebida, antes de abrir conexão. O SQL também usa os estados iniciais
como literais fixos, sem aproveitar parâmetros externos de situação.

Consultas exigem perímetro explícito e reconstituem/validam contratos e séries,
inclusive as condições de aprovação já presentes na ESG-02A. Não agregam nem
convertem valores. Usam uma transação somente leitura com `REPEATABLE READ` para
obter definição, registros e evidências no mesmo snapshot.

## Transações, conflitos e concorrência

Cada operação confirma somente após concluir todo o trabalho. Falhas de SQL,
validação de vínculo ou reconstituição provocam rollback e fechamento da conexão
dedicada. Falha de rollback é reportada, seguida de tentativa de fechamento.
Conexões inválidas ou já em uso são recusadas sem SQL, commit, rollback ou close,
para não finalizar transação pertencente a outro chamador; o proprietário deve
recolhê-las. A fábrica não deve retornar conexões compartilhadas ou de pools que
exijam devolução por outra API.

Definições/rascunhos e evidências usam INSERT sem substituição. Fontes, perímetros
e unidades usam `ON CONFLICT DO NOTHING`, seguido de leitura bloqueada e comparação
integral; divergências provocam conflito e rollback. Não há `DO UPDATE`, UPDATE,
DELETE ou tentativa automática de reconexão. A criação de rascunho bloqueia a
definição com `FOR SHARE`; associação posterior de evidência bloqueia o registro
e a definição, impedindo transição concorrente por UPDATE enquanto o vínculo é
verificado. A unicidade no SQL resolve cadastros concorrentes por identidade ou
período; a exceção não é convertida em sucesso idempotente para o dado principal.

Erros públicos não incluem mensagens do driver. Violações de unicidade geram
`ConflitoESGError`; outras constraints/faixas numéricas conhecidas geram
`IntegridadeESGError`. Falta de entidade obrigatória gera
`NaoEncontradoESGError`; falhas restantes geram `PersistenciaESGError`.
`ContratoESGInvalido` continua indicando incompatibilidade com os contratos.

Falha durante commit pode ocorrer depois da confirmação no servidor:
`ConfirmacaoIncertaESGError` exige consulta antes de eventual repetição. O código
tenta rollback e fecha a conexão, mas não presume desfazer um commit já efetivado.
Erro de fechamento depois de commit também é reportado sem repetição automática.

## Revisões e aprovação: barreiras atuais e projeto futuro

Definições e registros têm `revisao=1` imposta por CHECK. Não há API para alterar,
substituir, excluir ou criar segunda revisão. Associação de evidência inclui
um novo objeto com autoria, sem alterar o registro, e é limitada a rascunhos.
As proteções do repositório não equivalem a permissões contra SQL administrativo;
privilégios de implantação precisam ser projetados antes de uso operacional.

Antes de permitir alterações, uma etapa posterior deverá implementar:

1. Revisões imutáveis de definições e registros, com revisão anterior explícita,
   motivo, autor, horário e decisão. Cada registro deverá fixar a revisão da
   definição/unidade/perímetro utilizada, preservando o significado histórico.
2. Histórico acrescentado por evento, com fontes/evidências preservadas; nova
   revisão deve ter novo identificador e validar a mesma identidade lógica.
3. Restrições para uma revisão corrente por indicador/período e controle de
   concorrência, mantendo revisões anteriores consultáveis sem sobrescrita.
4. Fluxo autorizado de submissão, devolução, rejeição e aprovação, com decisão
   documental, identidade autenticada, justificativa e trilha na mesma transação.
   A aprovação de registro deve exigir definição/metodologia validada e impedir
   alterações concorrentes de fontes/evidências; sua regra entre tabelas também
   deverá ser reforçada no banco antes de habilitar as transições.
5. Nova migration revisada para habilitar revisões/fluxos; nunca remover
   constraints ou atualizar linhas em silêncio para contornar conflitos.

## Execução, testes e ESG-02B2

O manifesto/controlador global hoje permite apenas os caminhos de SQL já
existentes do núcleo e da fiscalização. O SQL ESG não foi registrado nem
habilitado. Integrá-lo requer tarefa autorizada para manifesto, allowlist,
checksums, dependências e preflight. Não se deve executá-lo manualmente para
contornar essa barreira. Nenhuma infraestrutura global foi modificada.

`tests/test_esg_repositorio.py` usa fábrica, conexão e cursor simulados. A classe
de DDL lê somente o arquivo SQL autorizado como texto, sem engine SQL. Os testes
não importam driver; incluem bloqueios de arquivos/rede durante os casos de
repositório. Os roteiros verificam parâmetros, commits, rollback e fechamento,
mas não comprovam execução, sintaxe PostgreSQL, locks ou constraints reais.

ESG-02B2 depende de revisão deste esquema, integração controlada de migrations e
autorização separada para qualquer teste PostgreSQL. Deve validar concorrência,
constraints, precisão, compatibilidade de driver, permissões e auditoria, antes
de ligar o repositório a rotas. Não há aprovação institucional ou liberação
operacional decorrente dos testes offline. A interface ESG-01 permanece vazia.
