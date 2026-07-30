# Etapa H2C.2I — Delimitação funcional das tabelas adicionais

## 1. Situação e limites

**Situação: APROVADA em 30/07/2026.**

As 40 decisões funcionais foram aprovadas integralmente. As alternativas da
seção 17 permanecem como memória da análise; as respostas oficiais estão na
seção 1.1. Nenhum módulo, migration, modo somente leitura, arquivamento,
substituição ou remoção foi implementado.

Este documento delimita 27 tabelas adicionais identificadas no schema
documentado. A análise foi exclusivamente estática: documentos e arquivos
versionados. Não houve acesso ao banco, dump externo, `.env`, dados reais,
conteúdo de CSV, API ou serviço; nenhum script, SQL, importador ou migration foi
executado.

Ausência de uso no repositório não comprova obsolescência. Nenhuma tabela está
autorizada para remoção.

### 1.1 Decisões funcionais aprovadas

| Nº | Decisão aprovada |
|---:|---|
| 1 | A baseline conterá somente o núcleo obrigatório. |
| 2 | Módulos opcionais terão migrations próprias, versionadas e independentes. |
| 3 | Tabelas sem uso operacional comprovado ficarão fora da baseline inicial. |
| 4 | Tabelas fora da baseline serão preservadas no banco atual. |
| 5 | Auditoria funcional será separada da trilha técnica transversal. |
| 6 | Auditoria de rateios será candidata a módulo opcional. |
| 7 | Auditoria de associados será candidata a módulo opcional. |
| 8 | Registros dos passos 1/2 serão preservados; estrutura depende de confirmação funcional. |
| 9 | `cadastro_pessoa_fisica` não se tornará automaticamente cadastro mestre. |
| 10 | `associados` continuará como cadastro funcional próprio. |
| 11 | Duplicidades pessoais serão saneadas antes de eventual normalização. |
| 12 | `documentos` será tratado provisoriamente como protocolo/entrega específico. |
| 13 | Entrega de documentos poderá continuar como módulo opcional. |
| 14 | Pacote, lote e item dependem de confirmação em especificação própria. |
| 15 | `tipos_documentos` será catálogo do módulo documental, sem unificação automática. |
| 16 | Metadados, conteúdo, download e exportação documental terão permissões distintas. |
| 17 | EPI poderá continuar como módulo opcional futuro. |
| 18 | EPI não integrará a baseline nuclear inicial. |
| 19 | `epis`, `epis_catalogo` e `epi_itens` serão consolidados conceitualmente após auditoria. |
| 20 | Estoque EPI será controlado por UVR, consolidado por associação. |
| 21 | Solicitação/entrega EPI exigirão aprovação, salvo emergência justificada. |
| 22 | Entrega EPI preservará item, quantidade, pessoas, data, associação, UVR e confirmação. |
| 23 | Movimento EPI concluído não será apagado; correção será compensatória. |
| 24 | Ouvidoria não pertence ao núcleo inicial. |
| 25 | Ouvidoria futura será interna/associativa, salvo decisão institucional pública. |
| 26 | Manifestação anônima não será admitida no escopo atual. |
| 27 | Fotos de manifestações serão privadas e terão permissão específica. |
| 28 | Ouvidoria ficará fora da baseline até especificação própria. |
| 29 | Classificações da Ouvidoria serão independentes do catálogo financeiro. |
| 30 | `produtos` continuará legado preservado e fora da baseline. |
| 31 | Nenhuma das 27 tabelas será apagada nesta etapa. |
| 32 | Legado poderá ficar somente leitura após comprovar que escrita não é necessária. |
| 33 | Remoção futura exigirá ausência comprovada de dependências. |
| 34 | Tabela com registros exigirá exportação ou arquivamento antes da remoção. |
| 35 | Cada módulo opcional terá testes e homologação próprios. |
| 36 | Módulos fora da baseline terão classificação de escopo explícita. |
| 37 | Estruturas substitutas preservarão referência a IDs/registros antigos. |
| 38 | Dados reais nunca integrarão migrations estruturais. |
| 39 | Baseline para PostgreSQL vazio falhará antes do primeiro DDL se detectar sistema anterior. |
| 40 | A classificação final será registrada individualmente para cada tabela. |

### 1.2 Implementação técnica pendente

Permanecem pendentes desenho de estruturas substitutas, nomes, colunas, tipos,
constraints, índices, migrations opcionais, migração e saneamento de dados,
scripts de arquivamento, modo somente leitura, desativação de rotas, permissões,
interfaces, testes e homologação de cada módulo.

## 2. Metodologia e evidências

Foram pesquisados os nomes exatos, variações relevantes e operações `CREATE`,
`ALTER`, `SELECT`, `INSERT`, `UPDATE`, `DELETE` e `JOIN` em código, templates,
JavaScript, testes, migrations, scripts e documentação.

Graus usados:

- **COMPROVADA:** estrutura ou relação registrada no schema documentado;
- **PARCIAL:** finalidade inferida de nomes/colunas/relações;
- **NÃO LOCALIZADA:** não encontrada no repositório operacional;
- **AMBÍGUA:** mais de uma finalidade ou destino plausível.

Classificação:

- **A:** ativa e necessária;
- **B:** ativa, mas precisa ser redesenhada;
- **C:** módulo incompleto ou desativado;
- **D:** legado preservado;
- **E:** candidata a módulo futuro;
- **F:** órfã ou não comprovada;
- **G:** decisão humana necessária.

## 3. Resultado geral da pesquisa

Nenhuma das 27 tabelas possui SQL operacional, interface, teste ou migration
localizados no repositório. As estruturas e FKs são comprovadas apenas pelos
documentos produzidos a partir do schema. Isso indica módulos externos,
históricos, incompletos ou ainda não versionados, mas não permite escolher entre
essas origens.

Referências genéricas a “documentos”, “EPI”, “ouvidoria” ou “produtos” não foram
tratadas como uso da tabela sem nome SQL inequívoco. Em especial:

- `documentos` é distinta de `fc_documentos`;
- as tabelas de Ouvidoria são distintas do catálogo financeiro;
- `produtos` é distinta de `produtos_servicos`;
- auditoria funcional é distinta de logs e trilha técnica central.

## 4. Matriz individual das 27 tabelas

Todas têm: migration **não localizada**, código/interface/teste **não
localizados** e dependência de inicialização/rotas atuais **não comprovada**.

| # | Tabela | Estrutura e relações comprovadas | Finalidade/evidência | Riscos | Classe observada na análise |
|---:|---|---|---|---|---|
| 1 | `auditoria_associados` | 11 colunas, PK, 2 UQs; FKs para associado e documento, ambas `CASCADE` | auditoria mensal funcional; PARCIAL | histórico, documentos e cascata | C/E; incluir só após especificação |
| 2 | `auditoria_passo1_observacoes` | 5 colunas, PK, UQ por UVR; sem FK | observação do passo 1; PARCIAL | UVR textual/provável, autoria incerta | C/G; decisão adiada |
| 3 | `auditoria_passo2_observacoes` | 6 colunas, PK, UQ por UVR/período; sem FK | observação do passo 2; PARCIAL | período e fluxo não comprovados | C/G; decisão adiada |
| 4 | `auditoria_rateios` | 9 colunas, PK, 2 UQs; FK associado `CASCADE` | auditoria de rateio; PARCIAL | valores financeiros e perda em cascata | C/E; especificar módulo |
| 5 | `auditoria_rateios_transacoes` | 10 colunas, PK, 2 UQs; FKs associado/transação `CASCADE` | conciliação/auditoria financeira; PARCIAL | histórico financeiro apagável por pai | C/E; especificar módulo |
| 6 | `auditoria_relatorios` | 9 colunas, PK, UQ de relatório; sem FK | metadados de relatórios; PARCIAL | conteúdo, autoria e retenção incertos | C/G; decisão adiada |
| 7 | `cadastro_pessoa_fisica` | `codigo`, nome, CPF; sem PK/FK | importação/cadastro auxiliar; AMBÍGUA | CPF, duplicidade e falta de identidade | F/G; decisão adiada |
| 8 | `documentos` | 15 colunas, PK; FKs tipo e transação, sem cascata | documento financeiro/legado; PARCIAL | arquivo, privacidade e sobreposição | C/D/E; substituir após especificação |
| 9 | `entrega_documentos_itens` | 19 colunas, PK; FKs lote/pacote/tipo/documento | item de protocolo/entrega; PARCIAL | dados pessoais, recebimento e cascatas | C/E; especificar módulo |
| 10 | `entrega_documentos_lotes` | 9 colunas, PK, UQ UVR/período; sem FK | lote documental; PARCIAL | escopo UVR e retenção | C/E; especificar módulo |
| 11 | `entrega_documentos_pacotes` | 10 colunas, PK, 2 UQs; sem FK | pacote por etapa; PARCIAL | relação lógica sem FK comprovada | C/E; especificar módulo |
| 12 | `tipos_documentos` | 8 colunas, PK; sem FK de saída | catálogo documental legado; PARCIAL | conflito com categorias dos módulos | C/E; redesenhar antes de incluir |
| 13 | `epi_entrega_itens` | 6 colunas, PK; FKs entrega `CASCADE` e item | itens efetivamente entregues; PARCIAL | histórico trabalhista e cascata | C/E; módulo opcional |
| 14 | `epi_entregas` | 9 colunas, PK; FKs associado e responsável | entrega/recebimento; PARCIAL | dados pessoais, assinatura e retenção | C/E; módulo opcional |
| 15 | `epi_estoque` | 7 colunas, PK, UQ item/UVR; FK item | saldo por UVR; PARCIAL | estoque negativo e escopo | C/E; redesenhar/especificar |
| 16 | `epi_itens` | 8 colunas, PK, UQ nome; sem FK | catálogo operacional de EPI; PARCIAL | duplicidade com dois catálogos | C/E; escolher fonte futura |
| 17 | `epi_movimentos` | 10 colunas, PK; FK item | movimentação de estoque; PARCIAL | saldo e histórico não podem ser reescritos | C/E; módulo opcional |
| 18 | `epi_solicitacoes` | 13 colunas, PK; FK item | solicitação de alteração/EPI; PARCIAL | possível conflito com fluxo aprovado | C/E; redesenhar |
| 19 | `epis` | 7 colunas, PK, UQ nome; sem FK | catálogo alternativo; AMBÍGUA | três fontes concorrentes | D/G; decisão adiada |
| 20 | `epis_catalogo` | 8 colunas, PK, UQ composta; sem FK | catálogo alternativo detalhado; AMBÍGUA | três fontes concorrentes | D/G; decisão adiada |
| 21 | `ouvidoria_grupos` | 5 colunas, PK, UQ nome | classificação própria; PARCIAL | confusão com grupo financeiro | C/E; módulo separado |
| 22 | `ouvidoria_manifestacao_fotos` | 5 colunas, PK; FK manifestação `CASCADE` | fotos de manifestação; PARCIAL | imagem sensível e exclusão em cascata | C/E; especificar privacidade |
| 23 | `ouvidoria_manifestacoes` | 39 colunas, PK, protocolo UQ; sem FK de classificação | manifestação/localização; PARCIAL | anonimato, contato, endereço e denúncia | C/E/G; fora do núcleo |
| 24 | `ouvidoria_subgrupos` | 5 colunas, PK, UQ composta; FK grupo `RESTRICT` | hierarquia de ouvidoria; COMPROVADA | finalidade dos níveis ainda incerta | C/E; módulo separado |
| 25 | `ouvidoria_subtipos` | 5 colunas, PK, UQ composta; FK tipo `RESTRICT` | hierarquia de ouvidoria; COMPROVADA | sobreposição semântica | C/E; módulo separado |
| 26 | `ouvidoria_tipos` | 5 colunas, PK, UQ composta; FK subgrupo `RESTRICT` | hierarquia de ouvidoria; COMPROVADA | ordem Grupo→Subgrupo→Tipo→Subtipo | C/E; módulo separado |
| 27 | `produtos` | 8 colunas, PK; sem FK | catálogo alternativo legado; COMPROVADA | divergência com catálogo oficial | D; fora da baseline, decisão H2C.2H |

Nenhuma tabela recebeu classe A ou B porque não há uso operacional comprovado
no repositório. Isso não equivale a declarar que estão sem uso fora dele.

### 4.1 Classificação funcional aprovada individualmente

| Tabela | Classificação aprovada | Baseline | Banco atual / revisão futura |
|---|---|---|---|
| `auditoria_associados` | auditoria funcional; candidata opcional | fora do núcleo | preservar; especificar auditoria de associados |
| `auditoria_passo1_observacoes` | etapa ambígua preservada | não reproduzir agora | confirmar significado do passo 1 |
| `auditoria_passo2_observacoes` | etapa ambígua preservada | não reproduzir agora | confirmar significado do passo 2 |
| `auditoria_rateios` | auditoria funcional; candidata opcional | fora do núcleo | preservar; especificar rateios |
| `auditoria_rateios_transacoes` | auditoria funcional financeira | fora do núcleo | preservar referências financeiras |
| `auditoria_relatorios` | apoio da auditoria funcional | fora do núcleo | preservar; definir relatórios |
| `cadastro_pessoa_fisica` | ambígua; não é mestre automaticamente | fora da baseline inicial | preservar; sanear pessoas antes de revisar |
| `documentos` | protocolo/entrega específico | módulo opcional | preservar; possível substituta documental |
| `entrega_documentos_itens` | item do módulo opcional | módulo opcional | preservar; confirmar os três níveis |
| `entrega_documentos_lotes` | lote do módulo opcional | módulo opcional | preservar; confirmar necessidade |
| `entrega_documentos_pacotes` | pacote do módulo opcional | módulo opcional | preservar; confirmar necessidade |
| `tipos_documentos` | catálogo documental específico | módulo opcional | preservar; sem unificação automática |
| `epi_entrega_itens` | histórico de módulo EPI | módulo opcional futuro | preservar; redesenhar entregas |
| `epi_entregas` | histórico de entrega EPI | módulo opcional futuro | preservar pessoas e escopos |
| `epi_estoque` | estoque EPI por UVR | módulo opcional futuro | preservar; consolidar por associação |
| `epi_itens` | candidato a catálogo operacional | módulo opcional futuro | auditar junto aos outros catálogos |
| `epi_movimentos` | histórico de estoque | módulo opcional futuro | preservar; correção compensatória |
| `epi_solicitacoes` | fluxo EPI a redesenhar | módulo opcional futuro | integrar ao fluxo aprovado futuramente |
| `epis` | catálogo EPI concorrente | fora até auditoria | preservar; consolidar conceitualmente |
| `epis_catalogo` | catálogo EPI concorrente | fora até auditoria | preservar; consolidar conceitualmente |
| `ouvidoria_grupos` | classificação própria da Ouvidoria | fora da baseline inicial | preservar; especificação própria |
| `ouvidoria_manifestacao_fotos` | fotos privadas de manifestação | fora da baseline inicial | preservar; definir privacidade |
| `ouvidoria_manifestacoes` | Ouvidoria fora do núcleo | fora da baseline inicial | preservar; definir finalidade/público |
| `ouvidoria_subgrupos` | classificação própria da Ouvidoria | fora da baseline inicial | preservar; independente do financeiro |
| `ouvidoria_subtipos` | classificação própria da Ouvidoria | fora da baseline inicial | preservar; independente do financeiro |
| `ouvidoria_tipos` | classificação própria da Ouvidoria | fora da baseline inicial | preservar; independente do financeiro |
| `produtos` | legado aprovado | fora da baseline | preservar; auditoria adicional antes de revisar |

O grau de confiança é alto para `produtos`, médio para relações comprovadas de
auditoria/documentos/EPI/Ouvidoria e baixo para a finalidade de
`cadastro_pessoa_fisica` e dos passos 1/2. Estruturas substitutas deverão manter
mapas para IDs ou registros antigos.

## 5. Relações comprovadas

| Origem | Destino | Exclusão declarada | Interpretação segura |
|---|---|---|---|
| auditorias de associados/rateios | `associados` | `CASCADE` | processo funcional ligado ao associado |
| auditoria de associado | `documentos` | `CASCADE` | documento integra a evidência |
| auditoria de transações | `transacoes_financeiras` | `CASCADE` | dependência financeira histórica |
| `documentos` | `tipos_documentos`, `transacoes_financeiras` | sem cascata | documento legado tipificado e possivelmente financeiro |
| item de entrega | lote, pacote, tipo e documento | mista | níveis de protocolo documental |
| entrega de EPI | `associados` | sem cascata | recebedor e responsável parecem associados |
| item/movimento/estoque/solicitação EPI | `epi_itens` | mista | `epi_itens` parece catálogo operacional |
| foto de manifestação | manifestação | `CASCADE` | anexo privado potencial |
| subgrupo Ouvidoria | grupo Ouvidoria | `RESTRICT` | classificação própria |
| tipo Ouvidoria | subgrupo Ouvidoria | `RESTRICT` | classificação própria |
| subtipo Ouvidoria | tipo Ouvidoria | `RESTRICT` | classificação própria |

Relações com associação, UVR, usuário ou armazenamento externo sugeridas apenas
por nomes não foram elevadas a requisito.

## 6. Auditoria funcional

As seis tabelas parecem representar um processo de auditoria operacional ou
financeira, com períodos, associados, rateios, transações, documentos,
observações em dois passos e relatórios. Não são a trilha técnica de segurança
do sistema.

“Passo 1” e “Passo 2” sugerem fases de conferência, mas seus significados,
atores, estados, aprovação e encerramento não estão comprovados. As cascatas
podem apagar evidências quando um associado, documento ou transação for apagado.

**Direção aprovada:** módulo opcional de auditoria funcional, especificado
separadamente, preservado no banco atual e fora da baseline inicial até decisão.

## 7. Cadastro de pessoa física

`cadastro_pessoa_fisica` tem apenas código, nome e CPF, sem PK ou relações. Pode
ser importação, cadastro auxiliar, protótipo, legado ou tentativa de cadastro
mestre. Não há evidência para escolher.

Há sobreposição provável com `associados` e possivelmente `usuarios`, porém
login, pessoa e vínculo associativo são conceitos diferentes. Normalizar sem
saneamento pode duplicar CPF, divergir nomes e ampliar acesso a dados pessoais.

**Direção aprovada:** preservar, restringir e adiar. Não criar cadastro
mestre antes de decisão funcional, análise de dependências, base legal e
saneamento.

## 8. Documentos e entregas

O conjunto sugere documentos tipificados ligados a transações e um protocolo em
pacote → lote → item. A finalidade concreta, titular, estados de entrega,
recebimento, devolução, arquivo físico/digital e armazenamento não estão
comprovados.

Ele não é automaticamente um repositório universal e não deve ser confundido
com `fc_documentos`, que pertence à Fiscalização e possui regras próprias.
Documentos podem conter dados pessoais e financeiros; download e exportação
exigem permissões distintas.

**Direção aprovada:** especificação própria e possível substituição por
estrutura versionada. Preservar o legado sem incluí-lo automaticamente.

## 9. EPI

A cadeia provável é:

```text
Catálogo → Estoque → Movimento → Solicitação → Entrega → Itens da entrega
```

`epi_itens` é o candidato mais forte a catálogo operacional porque recebe FKs.
`epis` e `epis_catalogo` são catálogos concorrentes sem relação comprovada. Não
foram confirmados CA, validade, tamanho, lote, devolução, perda, assinatura ou
regra de saldo.

O domínio envolve estoque, dados pessoais e possível histórico trabalhista.
Movimentos e entregas não devem ser apagados ou reescritos silenciosamente.

**Direção aprovada:** módulo futuro opcional, com especificação própria e
migration versionada; decidir fonte do catálogo e escopo associação/UVR antes.

## 10. Ouvidoria

A estrutura sugere:

```text
Grupo → Subgrupo → Tipo → Subtipo
Manifestação → Fotografias
```

Não há FK comprovada da manifestação para a classificação. Também não estão
definidos público, anonimato, contato, encaminhamento, responsável, prazo ou
retenção. Uma manifestação pode conter denúncia, endereço, imagem e dados
sensíveis.

As classificações são independentes do catálogo financeiro. Não há evidência de
que o sistema deva operar uma ouvidoria pública ou substituir serviço municipal.

**Direção aprovada:** fora do núcleo e da baseline inicial até
especificação própria de finalidade, responsabilidade, privacidade e acesso.

## 11. `produtos` legado

Aplica-se a decisão aprovada da H2C.2H: a tabela é preservada no banco atual,
fica fora da baseline nova e não será integrada automaticamente a
`produtos_servicos`. Qualquer migração ou remoção depende de auditoria adicional.

## 12. Sobreposições e dados sensíveis

| Sobreposição | Conclusão |
|---|---|
| pessoa física × associados × usuários | conceitos possivelmente distintos; não fundir |
| `documentos` × `fc_documentos` | domínios diferentes até prova contrária |
| `tipos_documentos` × categorias futuras | harmonizar somente após especificação |
| `epis` × `epis_catalogo` × `epi_itens` | três fontes candidatas; decisão necessária |
| auditoria funcional × trilha técnica | processos distintos |
| grupos/subgrupos da Ouvidoria × catálogo | classificações independentes |
| `produtos` × `produtos_servicos` | legado versus catálogo oficial aprovado |

CPF, nomes, contatos, endereços, documentos, fotos, recebedores, responsáveis e
manifestações podem ser pessoais ou sensíveis. Valores de rateios e transações
são financeiros. Acesso, exportação, download, retenção e descarte precisam de
permissões e políticas próprias.

## 13. Exclusão, retenção e banco atual

- nenhuma das 27 tabelas será apagada nesta etapa;
- histórico funcional, financeiro, estoque, entregas e manifestações não deve
  ser apagado silenciosamente;
- rascunho comprovadamente sem vínculo poderá ter regra futura de descarte;
- documentos e fotos exigem política específica;
- tabelas fora da baseline podem permanecer no banco atual, isoladas ou em
  somente leitura;
- remoção futura exige ausência comprovada de dependência, exportação/arquivo,
  prazo aprovado e rollback.

## 14. Permissões e escopo

Cada módulo futuro deve separar consultar, criar, editar, inativar, aprovar,
entregar, receber, exportar, baixar e administrar. O escopo pode ser global,
associação, UVR ou objeto. Administrador Global não recebe automaticamente
operações especializadas. Dados pessoais, documentos, fotos e manifestações
exigem permissões específicas.

## 15. Alternativas de baseline

| Estratégia | Vantagem | Risco/custo |
|---|---|---|
| A — reproduzir as 27 | máxima compatibilidade estrutural | leva legado, ambiguidades e módulos não governados para instalação nova |
| B — apenas uso comprovado | baseline pequena | como nenhum uso foi localizado, pode perder módulos externos reais |
| C — núcleo + módulos opcionais | separa necessário, opcional e legado | exige especificações e migrations por módulo |

**Decisão aprovada:** C. A baseline nuclear não reproduz automaticamente
as 27 tabelas; módulos aprovados ganham migrations/testes próprios; legados
ficam preservados fora da instalação nova. Complexidade inicial maior, mas menor
risco de institucionalizar estruturas desconhecidas.

## 16. Classificação aprovada por domínio

| Domínio | Situação atual | Destino aprovado | Trabalho futuro |
|---|---|---|---|
| Auditoria funcional | incompleto/desativado | módulo opcional | confirmar passos e desenhar o módulo |
| Pessoa física | ambíguo | fora da baseline inicial | sanear e decidir eventual mestre |
| Documentos/entregas | incompleto | módulo opcional | confirmar níveis e desenhar substituta |
| EPI | incompleto, estrutura rica | módulo opcional após redesenho | consolidar catálogos e regras |
| Ouvidoria | finalidade/público incertos | fora do núcleo e baseline | especificação institucional própria |
| `produtos` | legado aprovado | fora da baseline | auditoria adicional |

## 17. Registro das alternativas analisadas

As alternativas abaixo permanecem como memória da análise. As respostas
aprovadas são as 40 decisões da seção 1.1.

1. **Baseline somente com núcleo obrigatório?** Alternativas: sim; todas as 27;
   núcleo ampliado. **Recomendação:** núcleo. **Impacto:** instalação menor e
   módulos dependentes de aprovação.
2. **Módulos opcionais terão migrations próprias?** Alternativas: próprias;
   migration única; sem reprodução. **Recomendação:** próprias. **Impacto:**
   versionamento independente.
3. **Sem uso comprovado fica fora da baseline inicial?** Alternativas: fora;
   dentro; caso a caso. **Recomendação:** fora, salvo decisão expressa.
   **Impacto:** evita legado automático.
4. **Tabelas fora da baseline permanecem no banco atual?** Alternativas:
   preservar; remover; arquivar já. **Recomendação:** preservar. **Impacto:**
   nenhuma perda prematura.
5. **Auditoria funcional separada da técnica?** Alternativas: separar; fundir;
   descontinuar. **Recomendação:** separar. **Impacto:** conceitos e retenções
   corretos.
6. **Auditoria de rateios continua?** Alternativas: sim; não; decidir após
   usuários. **Recomendação:** decisão após validação funcional. **Impacto:**
   baseline do módulo.
7. **Auditoria de associados continua?** Alternativas: sim; não; decidir após
   ouvir os responsáveis. **Recomendação:** validar responsáveis e uso.
   **Impacto:** histórico.
8. **Passos 1 e 2 devem ser preservados?** Alternativas: preservar; redesenhar;
   retirar. **Recomendação:** documentar o fluxo antes. **Impacto:** estados e
   migração.
9. **Pessoa física será cadastro mestre?** Alternativas: mestre; auxiliar;
   legado. **Recomendação:** não decidir sem saneamento. **Impacto:** CPF e
   identidade.
10. **Associados permanecem cadastro separado?** Alternativas: sim; integrar;
    substituir. **Recomendação:** sim durante transição. **Impacto:** compatibilidade.
11. **Duplicidades pessoais serão saneadas antes?** Alternativas: antes; durante;
    automaticamente. **Recomendação:** antes e com revisão humana. **Impacto:**
    privacidade e integridade.
12. **`documentos` é geral ou específico?** Alternativas: geral; financeiro;
    entrega; legado. **Recomendação:** confirmar com responsáveis. **Impacto:**
    desenho documental.
13. **Entrega de documentos continua?** Alternativas: sim; não; módulo futuro.
    **Recomendação:** módulo futuro se validado. **Impacto:** cinco tabelas.
14. **Pacote, lote e item são necessários?** Alternativas: todos; simplificar;
    retirar. **Recomendação:** validar o processo real. **Impacto:** hierarquia.
15. **Tipos documentais serão centrais?** Alternativas: central; por módulo;
    híbrido. **Recomendação:** central com extensões por módulo. **Impacto:**
    governança.
16. **Download privado exige permissão?** Alternativas: específica; login;
    pública. **Recomendação:** específica. **Impacto:** proteção de dados.
17. **EPI continua no sistema?** Alternativas: sim; externo; legado.
    **Recomendação:** decidir com área responsável. **Impacto:** módulo inteiro.
18. **EPI entra no núcleo ou opcional?** Alternativas: núcleo; opcional; fora.
    **Recomendação:** opcional. **Impacto:** migration própria.
19. **Três catálogos EPI serão consolidados?** Alternativas: consolidar;
    preservar; escolher um. **Recomendação:** mapear e escolher com revisão.
    **Impacto:** IDs e duplicidades.
20. **Estoque EPI por associação ou UVR?** Alternativas: associação; UVR; ambos.
    **Recomendação:** definir propriedade e local de estoque separadamente.
    **Impacto:** escopo.
21. **Solicitação/entrega EPI exige aprovação?** Alternativas: ambas; só entrega;
    nenhuma. **Recomendação:** aprovação configurável. **Impacto:** fluxo.
22. **Entrega preserva recebedor, data e responsável?** Alternativas: sim; parte;
    não. **Recomendação:** sim. **Impacto:** auditoria pessoal.
23. **Movimentos EPI podem ser apagados?** Alternativas: não; rascunhos; sempre.
    **Recomendação:** não após efetivação. **Impacto:** saldo e histórico.
24. **Ouvidoria pertence ao sistema?** Alternativas: sim; externo; decidir.
    **Recomendação:** decisão institucional. **Impacto:** seis tabelas.
25. **Ouvidoria será interna, associativa ou pública?** Alternativas: as três.
    **Recomendação:** escolher uma finalidade antes do desenho. **Impacto:**
    acesso e responsabilidade.
26. **Anônimas serão admitidas?** Alternativas: sim; não; por categoria.
    **Recomendação:** decisão jurídica/funcional. **Impacto:** identidade e abuso.
27. **Fotos serão privadas?** Alternativas: privadas; públicas; por categoria.
    **Recomendação:** privadas por padrão. **Impacto:** armazenamento e acesso.
28. **Ouvidoria fora da baseline até especificação?** Alternativas: fora; dentro;
    estrutura vazia. **Recomendação:** fora. **Impacto:** evita canal indefinido.
29. **Classificação da Ouvidoria é independente?** Alternativas: independente;
    catálogo comum; parcial. **Recomendação:** independente. **Impacto:** sem
    mistura financeira.
30. **`produtos` permanece fora da baseline?** Alternativas: manter decisão;
    reabrir. **Recomendação:** manter H2C.2H. **Impacto:** nenhuma integração.
31. **Nenhuma tabela será apagada agora?** Alternativas: preservar todas;
    apagar sem uso. **Recomendação:** preservar. **Impacto:** segurança.
32. **Legado pode ficar somente leitura?** Alternativas: sim; não; por tabela.
    **Recomendação:** por tabela após análise. **Impacto:** reduz alteração.
33. **Remoção futura exige prova de independência?** Alternativas: sim; não.
    **Recomendação:** sim. **Impacto:** evita quebra oculta.
34. **Remoção exige exportação/arquivamento?** Alternativas: sempre; por risco;
    nunca. **Recomendação:** por risco, obrigatória com histórico. **Impacto:**
    retenção.
35. **Módulo opcional terá testes/homologação próprios?** Alternativas: sim;
    apenas suíte geral; nenhum. **Recomendação:** sim. **Impacto:** qualidade.
36. **Módulo não escolhido será documentado fora do escopo?** Alternativas: sim;
    não. **Recomendação:** sim. **Impacto:** evita expectativa incorreta.
37. **Estrutura substituta preserva IDs/referências?** Alternativas: sempre;
    mapa de equivalência; novos IDs sem mapa. **Recomendação:** mapa explícito.
    **Impacto:** histórico.
38. **Dados reais ficam fora de migrations?** Alternativas: sempre; exceções;
    incluir. **Recomendação:** sempre, salvo códigos estruturais. **Impacto:**
    reprodutibilidade.
39. **Baseline falha em banco antigo não vazio?** Alternativas: falhar; adaptar;
    sobrescrever. **Recomendação:** falhar com diagnóstico seguro. **Impacto:**
    proteção.
40. **Classificação final será individual por tabela?** Alternativas: individual;
    só por domínio. **Recomendação:** individual e consolidada por domínio.
    **Impacto:** rastreabilidade.

## 18. Riscos e pendências

Riscos principais: módulos externos invisíveis ao Git, cascatas que apagam
histórico, CPF e manifestações sensíveis, documentos/fotos sem política,
estoque inconsistente, catálogos concorrentes e baseline excessiva.

Pendem identificação dos responsáveis, inspeção controlada de dados em etapa
expressamente autorizada, especificações próprias e desenho técnico. Nenhuma
classificação aprovada autoriza DDL, migration, integração, isolamento,
arquivamento ou remoção.

## 19. Próxima etapa

A próxima etapa recomendada é **H2C.2J — Consolidação das Colunas Adicionais e
do Escopo Final da Baseline**, abrangendo especialmente colunas adicionais de
`usuarios`, `associados` e `transacoes_financeiras`, dados estruturais mínimos e
os bloqueios restantes do desenho técnico.
