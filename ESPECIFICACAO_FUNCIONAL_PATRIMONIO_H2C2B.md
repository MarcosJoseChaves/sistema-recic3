# Especificação funcional do patrimônio — H2C.2B

## 1. Resumo executivo em linguagem simples

O sistema já permite cadastrar, consultar, editar e excluir bens como veículos,
máquinas e equipamentos. A mesma área aparece para administradores e usuários
comuns autenticados.

O principal problema é que a ação atual de exclusão apaga o cadastro. Isso pode
separar uma movimentação antiga do bem ao qual ela se referia. A evolução
recomendada substitui essa ação normal por **Inativar**. O bem continua
consultável, seu histórico permanece e ele deixa de ficar disponível para novas
movimentações. Um usuário autorizado poderá reativá-lo.

Esta especificação não implementa a mudança. Ela descreve o comportamento
desejado, os riscos e uma ordem segura para a futura implementação.

## 2. Como a análise foi feita

Foram revisados os documentos da H2C.1B e H2C.2A, o `app.py`, o template
`templates/cadastro.html`, o JavaScript embutido nele e os testes de autorização
e endpoints JSON. A análise foi somente de leitura.

Cada afirmação usa uma destas classificações:

- **Confirmado:** está presente no código ou no schema já documentado.
- **Inferido:** é uma conclusão provável a partir dos nomes e relacionamentos,
  mas não há fluxo completo no código.
- **Não confirmado:** depende de dados reais ou de decisão do gestor.
- **Decisão validada:** comportamento futuro já aprovado para esta etapa.

Não houve acesso ao banco, execução de SQL, migration, teste de código ou deploy.

## 3. Situação atual

### 3.1 Acesso e telas

**Confirmado:**

- a página inicial exige login;
- o botão **Frota & Equipamentos** aparece no painel de todos os usuários
  autenticados;
- o botão abre a área **Gestão de Patrimônio e Frota** dentro da própria página;
- existem duas abas: **Lista de Bens** e **Novo Cadastro**;
- os detalhes aparecem em uma janela com as abas Geral, Técnico e Responsáveis;
- a edição reutiliza o formulário de cadastro;
- a listagem apresenta descrição, tipo/categoria, placa ou referência, medidor,
  responsável, situação e três botões por linha;
- as ações atuais são visualizar, editar e excluir ou solicitar exclusão.

Não existe uma página independente, um Blueprint ou um serviço próprio para o
patrimônio. Rotas, validações e acesso ao banco permanecem no `app.py`.

### 3.2 Rotas confirmadas

| Operação atual | Método e endereço | Proteção atual | Comportamento |
|---|---|---|---|
| cadastrar | `POST /cadastrar_patrimonio` | usuário autenticado | grava diretamente |
| pesquisar | `GET /buscar_patrimonio` | usuário autenticado; resposta JSON | aplica escopo de UVR na consulta |
| ver detalhes | `GET /get_patrimonio_detalhes/<id>` | usuário autenticado; resposta JSON | aplica escopo de UVR ao objeto |
| editar | `POST /editar_patrimonio` | usuário autenticado | admin altera; usuário comum solicita aprovação |
| excluir | `POST /excluir_patrimonio/<id>` | usuário autenticado; resposta JSON | admin apaga; usuário comum solicita aprovação |

### 3.3 Cadastro atual

**Confirmado:**

- UVR, descrição, categoria e tipo são obrigatórios no navegador;
- categoria e tipo possuem opções predefinidas;
- responsáveis são carregados entre associados ativos da UVR escolhida;
- associação é preenchida no navegador por um mapa fixo de duas UVRs;
- é possível enviar foto ou capturá-la por webcam;
- a foto é armazenada como texto Base64 no próprio cadastro;
- ano vazio é gravado como zero;
- medidor inicial vazio é gravado como zero;
- medidor atual nasce com o mesmo valor do medidor inicial;
- as caixas de abastecimento e manutenção começam marcadas;
- o cadastro grava diretamente tanto para admin quanto para usuário comum.

**Risco confirmado:** o cadastro não aplica no servidor a mesma proteção de UVR
usada pelas outras operações. Portanto, a UVR recebida do formulário é aceita
diretamente.

### 3.4 Consulta atual

**Confirmado:**

- pesquisa por descrição, placa ou código patrimonial;
- filtro por três categorias;
- usuário comum consulta apenas sua UVR;
- administrador pode consultar todas;
- ordenação fixa pela descrição;
- não há filtro de situação;
- não há filtro de localização;
- não há paginação;
- não há opção de ordenação escolhida pelo usuário;
- todos os resultados encontrados são retornados de uma vez.

A descrição é comparada sem diferença entre maiúsculas e minúsculas. Placa e
código usam comparação textual dependente do comportamento do PostgreSQL, sem
normalização comprovada.

### 3.5 Edição atual

**Confirmado:**

- administrador altera diretamente;
- usuário comum envia uma solicitação da versão A para aprovação;
- o objeto é conferido pela UVR antes da edição;
- a aprovação administrativa aplica os campos permitidos;
- o formulário permite trocar a situação diretamente;
- não há motivo obrigatório para mudança de situação;
- `medidor_atual` não é editado pelo formulário;
- `data_cadastro` não é editada;
- a alteração direta de administrador não gera histórico funcional próprio.

### 3.6 Exclusão atual

**Confirmado:**

- administrador executa exclusão física imediatamente após confirmação;
- usuário comum solicita exclusão, e a aprovação administrativa também executa
  exclusão física;
- não há verificação de transações ligadas antes da exclusão;
- a mensagem informa que a ação não pode ser desfeita;
- o banco não possui chave estrangeira entre a referência financeira e o
  patrimônio.

### 3.7 Importação, exportação e relatórios

**Confirmado:** não foram localizados importador, exportador ou relatório
específico do patrimônio no código atual. Os relatórios financeiros existentes
não utilizam a referência de patrimônio.

## 4. Problemas encontrados

| Problema | Evidência | Consequência possível | Gravidade |
|---|---|---|---|
| exclusão física normal | rota e aprovação usam exclusão definitiva | perda do cadastro e quebra do contexto histórico | crítica |
| referência financeira sem integridade | `transacoes_financeiras.id_patrimonio` não possui FK | movimentação pode apontar para bem inexistente | crítica |
| cadastro sem proteção de UVR no servidor | rota aceita a UVR do formulário | usuário comum pode tentar cadastrar fora do próprio escopo | crítica |
| situações alteradas como campo comum | formulário e edição aceitam mudança direta | baixa ou reativação sem motivo ou histórico | alta |
| ausência de auditoria completa | somente solicitações de usuário comum deixam trilha funcional | alterações diretas de admin não têm histórico próprio | alta |
| quase todas as colunas aceitam nulo | schema atual | cadastros antigos e novos podem ser incompletos | alta |
| código patrimonial sem unicidade | não há UNIQUE nem validação de duplicidade | dois bens podem ter a mesma identificação | alta |
| validação predominantemente no navegador | backend acessa campos, mas não consolida regras amigáveis | erro interno ou dado inconsistente | alta |
| fotografia Base64 sem validação forte confirmada | upload usa tipo fornecido pelo navegador | arquivo inválido, tamanho excessivo ou banco inflado | alta |
| lista sem paginação | busca devolve todos os resultados | lentidão conforme o cadastro cresce | média |
| filtros limitados | apenas categoria e texto | dificuldade para localizar inativos, UVR ou local | média |
| mapa fixo de UVR e associação no JavaScript | duas opções escritas no template | manutenção manual e inconsistência | média |
| responsáveis armazenados por nome | campos textuais, sem FK | renomeação pode quebrar identificação histórica | média |
| ano e medidor vazios viram zero | regra atual | zero pode ser confundido com valor informado | média |
| foto misturada ao cadastro | texto Base64 na linha do bem | consultas maiores e retenção pouco clara | média |
| nomes “bem”, “patrimônio” e “frota” misturados | interface atual | dúvida sobre o alcance do módulo | baixa |

## 5. Objetivos do módulo

O módulo futuro deverá:

1. permitir que uma pessoa sem conhecimento técnico encontre e compreenda um
   patrimônio;
2. preservar todos os registros e vínculos existentes;
3. separar alterações comuns de mudanças de situação;
4. impedir uso novo de patrimônio que não esteja disponível;
5. manter consulta a dados e movimentações antigas;
6. respeitar a UVR e as permissões do usuário no servidor;
7. registrar quem alterou a situação, quando e por quê;
8. apresentar mensagens claras e sem detalhes internos;
9. manter a implantação reversível;
10. não confundir patrimônio do sistema principal com ativos contratuais do
    módulo de Fiscalização.

## 6. Tipos de usuário

O sistema atual possui apenas administrador e usuário comum com UVR. Para o
planejamento, recomenda-se uma matriz simples com quatro capacidades:

- **Consulta:** pesquisa e visualiza dados do próprio escopo.
- **Operacional:** cadastra, atualiza informações e solicita mudanças de
  situação no próprio escopo.
- **Administrador do módulo:** administra patrimônios e aprova mudanças de
  situação nas UVRs autorizadas.
- **Administrador geral:** possui visão global e administra permissões.

Esses nomes representam capacidades futuras. Não autorizam criar novos perfis
ou alterar permissões nesta etapa.

## 7. Permissões propostas

Legenda: **Sim**, **Não**, **Solicita** ou **Somente excepcional**.

| Ação | Consulta | Operacional | Admin do módulo | Admin geral |
|---|---:|---:|---:|---:|
| visualizar no próprio escopo | Sim | Sim | Sim | Sim |
| cadastrar | Não | Sim | Sim | Sim |
| editar informações comuns | Não | Sim | Sim | Sim |
| inativar | Não | Solicita | Sim | Sim |
| reativar | Não | Solicita | Sim | Sim |
| ver histórico | Sim | Sim | Sim | Sim |
| ver movimentações permitidas | Sim | Sim | Sim | Sim |
| exportar dados do próprio escopo | Sim | Sim | Sim | Sim |
| administrar todas as UVRs | Não | Não | conforme concessão | Sim |
| exclusão física | Não | Não | Não | Somente excepcional |

Regras comuns:

- esconder o botão não substitui a autorização no servidor;
- acesso direto pelo endereço deve produzir a mesma decisão da interface;
- usuário sem UVR ou permissão deve receber uma resposta segura;
- uma eventual exclusão física não deve existir no fluxo normal nem no menu;
- a política exata de aprovação de inativação e reativação depende de decisão
  humana antes da implementação.

## 8. Fluxo de cadastro recomendado

1. O usuário escolhe **Cadastrar patrimônio**.
2. O sistema fixa ou oferece apenas as UVRs autorizadas.
3. A interface apresenta primeiro os campos essenciais.
4. Campos de veículo, comodato e manutenção aparecem somente quando aplicáveis.
5. O servidor valida obrigatoriedade, datas, números, identificadores e escopo.
6. O sistema avisa sobre possível duplicidade antes de gravar.
7. O novo patrimônio inicia como **Ativo**, salvo regra expressa diferente.
8. O medidor atual recebe o medidor inicial sem uso de valor enviado
   separadamente pelo navegador.
9. O sistema registra autoria e data.
10. A mensagem confirma o cadastro e oferece **Ver patrimônio**.

Cadastros antigos incompletos continuam consultáveis. Ao editá-los, a interface
deve indicar o que falta sem inventar valores e sem exigir correção de todos os
campos de uma só vez, salvo quando a própria alteração depender deles.

## 9. Fluxo de consulta recomendado

A listagem futura terá:

- pesquisa única por descrição, código, placa, número de série ou Renavam;
- filtro por situação;
- filtro por categoria;
- filtro por localização;
- filtro por UVR para quem possuir mais de uma;
- ordenação por descrição, código, data de cadastro ou situação;
- paginação no servidor;
- indicação clara de **Ativo**, **Em manutenção**, **Inativo** ou **Baixado**;
- ação principal **Visualizar**;
- ações secundárias em um menu **Mais opções**;
- opção explícita de mostrar inativos e baixados;
- mensagem simples quando não houver resultado.

A consulta de usuário comum sempre será limitada no servidor, mesmo que o
filtro do navegador seja alterado.

## 10. Fluxo de edição recomendado

1. O usuário abre os detalhes e escolhe **Editar informações**.
2. O servidor confirma acesso ao patrimônio.
3. Dados de identificação e operação são apresentados conforme a categoria.
4. Situação, medidor atual e dados de auditoria não são modificados como campos
   comuns.
5. O servidor valida novamente todos os campos alterados.
6. A gravação registra autoria e data.
7. Usuário operacional segue o fluxo de aprovação enquanto essa política
   permanecer vigente.
8. A mensagem informa **Alterações salvas** ou **Alteração enviada para
   aprovação**.

Mudança de UVR deve ser tratada como transferência ou operação administrativa,
não como simples edição de texto. A regra detalhada dessa transferência ainda
depende de decisão humana.

## 11. Situações do patrimônio

O campo atual `status_bem` já contém quatro valores na interface. Para evitar
uma segunda fonte de verdade, recomenda-se evoluir esse campo, após
caracterização dos dados existentes.

| Valor preservado | Nome para o usuário | Uso recomendado | Disponível para novas movimentações |
|---|---|---|---:|
| `Ativo` | Ativo | bem disponível para uso normal | Sim |
| `Manutencao` | Em manutenção | indisponibilidade temporária e operacional | Não, exceto registro próprio de manutenção |
| `Inativo` | Inativo | suspensão administrativa reversível | Não |
| `Baixado` | Baixado | fim do ciclo de uso, venda, perda ou descarte formal | Não |

Não se recomenda acrescentar **Transferido** agora: a transferência deve ser um
evento de mudança de UVR ou localização, preservando situação e histórico.
**Extraviado** pode ser necessário no futuro, mas só deve ser incluído depois de
confirmar o processo administrativo correspondente.

### Diferença entre inativação e baixa

- **Inativar** é reversível e indica que o patrimônio não pode ser usado agora.
- **Baixar** encerra o ciclo operacional e exige motivo, data e autorização mais
  forte.
- Reativar um bem baixado não deve ser uma ação comum; eventual correção exigirá
  administrador e justificativa.

## 12. Fluxo de inativação

### Comportamento

- não apaga o patrimônio;
- preserva movimentações e consultas antigas;
- retira o patrimônio das seleções de novas movimentações;
- exige confirmação;
- recomenda-se exigir motivo;
- registra situação anterior, nova situação, data, usuário e motivo;
- ocorre junto com o registro histórico, em uma única operação;
- não deve ser bloqueada apenas por existirem movimentações antigas.

### Texto recomendado

> Este patrimônio deixará de estar disponível para novas movimentações, mas
> seu histórico será preservado. Deseja continuar?

Após confirmar:

> Patrimônio inativado. Ele continua disponível para consulta e poderá ser
> reativado por um usuário autorizado.

## 13. Fluxo de reativação

A reativação será permitida quando:

- o registro estiver inativo e preservado;
- o usuário possuir permissão;
- não houver baixa definitiva sem correção formal;
- UVR, identificação mínima e responsável funcional estiverem válidos;
- não houver conflito de código, placa ou outro identificador aprovado como
  único;
- o motivo for informado, caso essa política seja aprovada.

O sistema registrará situação anterior, nova situação, data, usuário e motivo.
Depois da confirmação, o patrimônio volta às seleções de novas movimentações.

Texto recomendado:

> Ao reativar, este patrimônio voltará a ficar disponível para novas
> movimentações. Confirme se os dados e a localização estão atualizados.

## 14. Tratamento de patrimônios com transações e outras referências

### Vínculos confirmados

| Origem | Vínculo | Integridade atual | Uso no código atual |
|---|---|---|---|
| `transacoes_financeiras` | `id_patrimonio` opcional | sem FK | nenhum fluxo versionado localizado |
| `solicitacoes_alteracao` | nome lógico da entidade e ID do registro | sem FK para patrimônio | edição e exclusão de usuário comum |

O schema de `transacoes_financeiras` possui ainda 12 campos opcionais aparentes
de motorista, combustível, medidor, manutenção e garantia. Seus nomes e usos
não estão documentados no código atual; portanto, não devem ser promovidos nem
removidos sem nova decisão.

### Vínculos não confirmados

Não foram localizados vínculos atuais de patrimônio com:

- uma tabela própria de entradas e saídas;
- transferências;
- histórico de localização;
- documentos;
- manutenção estruturada;
- abastecimento estruturado;
- ativos contratuais do módulo Fiscalização.

`fc_ativos_contratuais` é um cadastro separado. Igualdade de placa, chassi ou
número patrimonial não constitui vínculo automático.

### Regra futura

- movimentações antigas permanecem válidas após inativação;
- patrimônio inativo não aparece em novas seleções;
- consulta de movimentações informa data, tipo, responsável e origem, quando
  esses dados existirem;
- eventual FK futura deve ser criada apenas depois de identificar referências
  órfãs;
- a FK recomendada deve impedir exclusão física, sem apagar movimentações em
  cascata;
- nenhum dado antigo será corrigido automaticamente sem relatório e aprovação.

## 15. Pesquisa e filtros

| Recurso | Situação atual | Recomendação |
|---|---|---|
| descrição | pesquisa existente | manter e normalizar |
| código patrimonial | pesquisa existente | manter; definir regra de duplicidade |
| placa | pesquisa existente | normalizar pontuação e caixa |
| número de série/chassi | não pesquisado | incluir |
| Renavam | não pesquisado | incluir para frota |
| situação | não existe | incluir |
| categoria | existe | manter |
| localização | não existe | incluir |
| UVR | implícita para usuário comum | seletor apenas para escopos autorizados |
| responsável | não existe | incluir se houver necessidade comprovada |
| data de cadastro | não existe | incluir em relatório, não necessariamente na lista |
| paginação | não existe | incluir no servidor |

Valores digitados devem ser tratados como dados e nunca formar comandos de
consulta. Uma busca vazia deve respeitar paginação e escopo.

## 16. Campos e regras de preenchimento

### 16.1 Legenda de visibilidade

- **Cadastro/edição:** aparece em ambos.
- **Condicional:** aparece somente quando aplicável.
- **Automático:** o sistema preenche.
- **Somente leitura:** aparece em detalhes, sem edição comum.
- **Interno:** não aparece na interface cotidiana.

### 16.2 Classificação das 38 colunas

| # | Nome técnico | Nome amigável | Finalidade e tipo | Obrigatoriedade atual | Obrigatoriedade recomendada | Validação e padrão | Visibilidade | Edição | Observações |
|---:|---|---|---|---|---|---|---|---|---|
| 1 | `id` | Identificador | número interno | PK automática | automática | sequência | Interno/detalhes | Não | não usar como nome do bem |
| 2 | `uvr` | UVR | local de responsabilidade; texto | banco aceita nulo; formulário exige | obrigatória em novos | somente UVR autorizada | Cadastro/edição | restrita | preservar texto até H2C.2D |
| 3 | `associacao` | Associação | associação ligada à UVR; texto | opcional e automática no navegador | automática quando mapeada | não confiar em mapa fixo do navegador | Cadastro/edição | restrita | legado vazio permanece válido |
| 4 | `tipo_bem` | Tipo de patrimônio | caminhão, prensa etc.; texto | formulário exige | obrigatória em novos | lista coerente com categoria | Cadastro/edição | Sim | opções atuais não cobrem todo mobiliário |
| 5 | `categoria` | Categoria | frota, equipamento ou mobiliário; texto | formulário exige | obrigatória em novos | lista aprovada | Cadastro/edição | Sim | mudança pode alterar campos aplicáveis |
| 6 | `descricao` | Descrição | nome compreensível; texto | formulário exige | obrigatória | remover espaços externos; limite | Cadastro/edição | Sim | principal identificação visual |
| 7 | `codigo_patrimonio` | Número patrimonial | código interno; texto | opcional | obrigatório em novos, se essa for a identificação oficial | normalizar; duplicidade conforme escopo a decidir | Cadastro/edição | Sim | legado vazio não deve ser apagado |
| 8 | `marca` | Marca | fabricante; texto | opcional | opcional | limite e espaços | Cadastro/edição | Sim | — |
| 9 | `modelo` | Modelo | modelo comercial; texto | opcional | opcional | limite e espaços | Cadastro/edição | Sim | — |
| 10 | `ano_fabricacao` | Ano de fabricação | ano; inteiro | opcional; vazio vira 0 | opcional | ano plausível; vazio permanece vazio | Cadastro/edição | Sim | zero não deve significar “não informado” |
| 11 | `numero_serie_chassi` | Série ou chassi | identificação técnica; texto | opcional | condicional | normalização por categoria | Cadastro/edição | Sim | avaliar unicidade |
| 12 | `situacao_propriedade` | Forma de posse | próprio, comodato ou alugado; texto | possui padrão visual | recomendada | lista aprovada | Cadastro/edição | Sim | diferente da situação operacional |
| 13 | `entidade_proprietaria` | Proprietário | entidade dona do bem; texto | possui padrão visual | recomendada | coerente com forma de posse | Cadastro/edição | Sim | lista atual é fixa |
| 14 | `orgao_cedente` | Órgão cedente | cedente do comodato; texto | opcional | obrigatória para comodato, se aplicável | espaços e limite | Condicional | Sim | ocultar fora de comodato |
| 15 | `numero_termo_comodato` | Número do termo | identificação do documento; texto | opcional | condicional | espaços e limite | Condicional | Sim | — |
| 16 | `data_inicio_comodato` | Início da cessão | data | opcional | condicional | data válida | Condicional | Sim | — |
| 17 | `data_fim_comodato` | Fim da cessão | data | opcional | condicional | não anterior ao início | Condicional | Sim | pode gerar alerta futuro |
| 18 | `placa` | Placa | identificação de veículo; texto | opcional | condicional para veículo emplacado | formato e normalização; duplicidade a decidir | Condicional | Sim | pesquisa atual |
| 19 | `renavam` | Renavam | identificação de veículo; texto | opcional | condicional | somente formato aprovado; normalização | Condicional | Sim | considerar privacidade operacional |
| 20 | `combustivel` | Combustível | tipo de energia; texto | opção padrão | condicional | lista; “Não se aplica” fora de frota | Condicional | Sim | — |
| 21 | `capacidade_carga` | Capacidade de carga | valor com unidade em texto | opcional | opcional | não aceitar texto enganoso; futura estruturação | Condicional | Sim | preservar formato legado |
| 22 | `controle_por` | Forma de controle | km, horas ou nenhum; texto | lista visual, sem `required` efetivo | obrigatória em novos | valores permitidos | Cadastro/edição | Sim | define sentido dos medidores |
| 23 | `medidor_inicial` | Medição inicial | decimal | opcional; vazio vira 0 | opcional | não negativo; Decimal | Cadastro/edição | Sim no cadastro | registrar valor realmente informado |
| 24 | `medidor_atual` | Medição atual | decimal | automático no cadastro | automática | nasce da inicial; nunca diminui sem correção auditada | Somente leitura | por movimento/correção | não usar valor livre do formulário comum |
| 25 | `local_instalacao` | Local de uso | localização; texto | opcional | recomendada quando aplicável | lista futura ou texto controlado | Cadastro/edição | Sim | transferência deve gerar histórico |
| 26 | `setor_uso` | Setor | área de uso; texto | opcional | opcional | lista aprovada | Cadastro/edição | Sim | — |
| 27 | `nome_responsavel` | Responsável | nome textual | opcional | recomendada para ativos em uso | escolher somente pessoa ativa e autorizada | Cadastro/edição | Sim | futura ligação por ID, preservando texto |
| 28 | `nome_operador_principal` | Operador principal | nome textual | opcional | opcional | pessoa ativa da UVR | Cadastro/edição | Sim | não substitui histórico de operadores |
| 29 | `status_bem` | Situação | estado operacional; texto | opcional no banco; lista no formulário | obrigatória | quatro valores preservados | Somente leitura na edição comum | ação própria | mudança exige histórico |
| 30 | `estado_conservacao` | Conservação | ótimo a sucata; texto | opção padrão | recomendada | lista aprovada | Cadastro/edição | Sim | “Sucata” não baixa automaticamente |
| 31 | `permite_abastecimento` | Permite abastecimento | sim/não | opcional; marcado por padrão | automática/condicional | falso fora de item abastecível | Cadastro/edição | Sim | não substitui situação ativa |
| 32 | `permite_manutencao` | Permite manutenção | sim/não | opcional; marcado por padrão | automática/condicional | coerente com categoria | Cadastro/edição | Sim | patrimônio inativo pode ter histórico de manutenção |
| 33 | `alerta_preventiva` | Próxima manutenção | limite do medidor; inteiro | opcional; vazio vira 0 | opcional | positivo e coerente com medidor | Cadastro/edição | Sim | nome amigável deve informar a unidade |
| 34 | `observacoes_gerais` | Observações | texto livre | opcional | opcional | limite e tratamento seguro | Cadastro/edição | Sim | não guardar segredo ou dado desnecessário |
| 35 | `foto_bem_base64` | Foto | imagem em texto | opcional | opcional | assinatura, formato e tamanho; ponteiro seguro | Cadastro/edição | substituir | candidata a armazenamento externo futuro |
| 36 | `eh_bem_publico` | Bem público | sim/não | opcional; falso por padrão do código | recomendada | booleano | Cadastro/edição | Sim | pode influenciar baixa e documentos |
| 37 | `uso_compartilhado` | Uso compartilhado | sim/não | opcional; falso por padrão do código | opcional | booleano | Cadastro/edição | Sim | definir significado entre UVRs |
| 38 | `data_cadastro` | Cadastrado em | data/hora automática | default atual; pode ser nula no legado | automática | não aceitar valor do navegador | Somente leitura | Não | mostrar “Não informado” quando antiga e vazia |

Resultado: **38 de 38 colunas classificadas**.

### 16.3 Regras que ainda precisam de confirmação

- se o código patrimonial é único globalmente ou por UVR;
- quais categorias exigem placa, Renavam, série ou código;
- se responsável é obrigatório para todo patrimônio ativo;
- se capacidade continuará como texto ou será separada em valor e unidade;
- quais valores antigos existem em categoria, tipo, situação e conservação.

## 17. Mensagens ao usuário

| Situação | Mensagem recomendada |
|---|---|
| cadastro | “Patrimônio cadastrado. Você já pode consultar seus detalhes.” |
| alteração direta | “Alterações salvas.” |
| solicitação | “Alteração enviada para aprovação.” |
| inativação | “Patrimônio inativado. O histórico foi preservado.” |
| reativação | “Patrimônio reativado e disponível para novas movimentações.” |
| não encontrado | “Patrimônio não encontrado ou indisponível para seu acesso.” |
| obrigatório | “Preencha o campo {nome amigável}.” |
| código duplicado | “Já existe um patrimônio com este número. Revise o código ou consulte o cadastro existente.” |
| inativo em nova movimentação | “Este patrimônio está inativo e não pode ser usado em uma nova movimentação.” |
| manutenção | “Este patrimônio está em manutenção e não está disponível para uso normal.” |
| falha inesperada | “Não foi possível concluir a operação agora. Tente novamente. Se o problema continuar, informe o código de referência ao suporte.” |

As mensagens não devem apresentar consulta SQL, nomes internos, traceback,
credenciais, caminho de arquivo ou texto bruto da exceção.

## 18. Relatórios essenciais

Não há relatório patrimonial confirmado no código atual.

| Relatório | Classificação | Justificativa |
|---|---|---|
| patrimônios ativos | essencial | base de operação e conferência |
| patrimônios inativos e baixados | essencial | transparência e reativação controlada |
| por localização ou UVR | essencial | responsabilidade e inventário físico |
| histórico de movimentações | essencial após mapear vínculos | preserva rastreabilidade |
| por categoria e tipo | útil | planejamento e inventário |
| cadastrados em período | útil | conferência e auditoria |
| sem movimentação | útil, mas futuro | depende de vínculo confiável com movimentações |
| manutenção e abastecimento | futuro | fluxos atuais não estão implementados |
| fotografia consolidada | dispensável como relatório | aumenta volume sem apoiar decisão cotidiana |

Relatórios devem respeitar escopo de UVR e permissão no servidor. Exportação não
deve ampliar acesso nem incluir foto Base64 por padrão.

## 19. Auditoria e histórico

O histórico futuro deverá registrar:

- cadastro;
- edição relevante;
- inativação;
- reativação;
- entrada e saída de manutenção, se implementadas;
- baixa;
- correção de baixa;
- mudança de UVR, localização ou responsável;
- correção excepcional do medidor;
- aprovação ou rejeição de solicitação.

Cada evento deve guardar patrimônio, tipo, situação anterior e nova, motivo,
data e usuário. Eventos históricos não devem ser editados ou apagados pelo
fluxo normal.

As solicitações versão A continuam preservadas, mas não substituem o histórico:
administradores hoje conseguem alterar diretamente sem passar por solicitação.

## 20. Impactos futuros no banco

Sem escolher SQL nesta etapa, a solução recomendada deverá avaliar:

1. **Reutilizar `status_bem`:** evita criar um segundo indicador conflitante.
   Antes disso, caracterizar e normalizar valores antigos.
2. **Auditoria de mudanças:** uma estrutura aditiva de eventos é preferível a
   guardar apenas a última data e o último motivo, pois suporta vários ciclos.
3. **Autoria de atualização:** avaliar data e usuário da última alteração,
   preservando `data_cadastro`.
4. **Integridade financeira:** depois de identificar referências órfãs,
   recomendar vínculo de `transacoes_financeiras.id_patrimonio` com bloqueio de
   exclusão, nunca exclusão em cascata.
5. **Índices:** avaliar busca normalizada por código e placa, além de UVR,
   situação, categoria e data. Criar somente os sustentados por consultas.
6. **Unicidade:** não criar restrição antes de auditar duplicidades e decidir o
   escopo correto.
7. **Responsáveis:** futura ligação por identificador deve preservar os nomes
   antigos durante transição.
8. **Fotografia:** avaliar armazenamento próprio para arquivos, com referência
   no cadastro, sem migração automática nesta fase.

Todas as mudanças deverão ser aditivas, idempotentes quando apropriado,
revisadas antes da execução e testadas primeiro em ambiente descartável.

## 21. Impactos futuros no código

Áreas potencialmente afetadas:

- rotas de patrimônio hoje localizadas no `app.py`;
- aprovação de `solicitacoes_alteracao`;
- autorização por UVR e por objeto;
- bloco de patrimônio em `templates/cadastro.html`;
- JavaScript de consulta, edição, foto e exclusão;
- seleção futura de patrimônio em transações;
- relatórios e exportações;
- testes de caracterização, permissão, transação e interface.

Recomenda-se extrair gradualmente rotas, serviço, validações e templates para um
módulo separado, sem refatorar todo o `app.py` junto. A primeira mudança deve
ser comportamentalmente neutra e protegida por testes.

## 22. Estratégia de compatibilidade

- preservar as 38 colunas e todos os registros;
- preservar os quatro códigos de situação já apresentados pela interface;
- mostrar campos nulos antigos como **Não informado**, sem fabricar zero ou
  data;
- não exigir atualização em massa dos cadastros antigos;
- aplicar regras mais fortes primeiro aos novos cadastros;
- fornecer fila ou relatório de registros antigos que precisam de revisão;
- não criar FK enquanto existirem referências órfãs desconhecidas;
- manter textos de UVR, associação e responsáveis durante futuras transições;
- não unir automaticamente patrimônio legado e ativo contratual;
- conservar endereços antigos enquanto a nova interface é homologada, mas
  remover da interface qualquer ação de exclusão física.

## 23. Estratégia de homologação

Usar dados fictícios que representem:

- patrimônio completo;
- patrimônio antigo incompleto;
- código duplicado;
- patrimônio com e sem transação;
- Ativo, Em manutenção, Inativo e Baixado;
- usuário de consulta, operacional, admin do módulo e admin geral;
- uma e várias UVRs;
- foto válida, ausente, incompatível e acima do limite;
- comodato válido e datas incoerentes.

Validar em celular e computador:

- pesquisa, filtros, paginação e foco do teclado;
- mensagens e confirmação;
- bloqueio de acesso direto;
- inativação e reativação;
- preservação das movimentações;
- ausência de exclusão física;
- relatórios dentro do escopo;
- desempenho com volume representativo.

Banco e armazenamento reais devem ser bloqueados nos testes automatizados.

## 24. Estratégia de reversão

- migrations futuras permanecem aditivas; reverter código não apaga colunas ou
  eventos;
- ativar a nova interface por configuração controlada durante homologação;
- manter leitura compatível com registros antigos;
- se uma versão falhar, voltar a interface e o serviço anteriores sem reativar
  a exclusão física;
- mudanças de situação já registradas não serão desfeitas por exclusão; uma
  correção será outro evento autorizado;
- não usar remoção de estruturas como estratégia de reversão;
- documentar antes da implantação como desabilitar cada incremento.

## 25. Critérios de aceite funcionais

1. usuário enxerga apenas patrimônios de seu escopo;
2. cadastro não aceita UVR fora da autorização;
3. novos registros têm descrição, categoria, tipo, UVR e situação válidos;
4. duplicidade aprovada como inválida recebe mensagem amigável;
5. patrimônio pode ser inativado sem ser apagado;
6. inativo permanece consultável e não aparece em novas movimentações;
7. usuário autorizado consegue reativar;
8. inativação e reativação registram autoria, data e motivo conforme política;
9. movimentações antigas permanecem ligadas e consultáveis;
10. baixa é diferente de inativação;
11. mudança de situação não ocorre como edição comum;
12. listagem possui pesquisa, situação, categoria, UVR/local e paginação;
13. detalhes mostram histórico e movimentações permitidas;
14. relatórios respeitam UVR e permissão;
15. dados antigos incompletos continuam acessíveis;
16. nenhum fluxo normal oferece exclusão física;
17. erros internos não são apresentados ao usuário;
18. testes bloqueiam banco e serviços externos reais;
19. reversão de código não exige apagar dados;
20. patrimônio legado permanece separado de ativos contratuais.

## 26. Incrementos futuros de implementação

| Incremento | Objetivo único | Áreas potenciais | Dependências e riscos | Testes necessários | Critério de aceite | Reversão |
|---|---|---|---|---|---|---|
| H2C.3B.1 | caracterizar comportamento e dados esperados | testes, rotas e schema documentado | mocks podem não representar dados antigos | rotas, campos, valores e permissões atuais | comportamento atual protegido por testes | somente remover testes novos se incorretos |
| H2C.3B.2 | preparar situação e histórico | migration aditiva futura | valores nulos ou desconhecidos | estrutura, compatibilidade e segunda execução | estrutura criada sem alterar registros | código antigo ignora campos novos |
| H2C.3B.3 | implementar inativação e reativação no servidor | serviço, rotas, solicitações | transação parcial e permissão | estados, motivo, rollback, concorrência | sem exclusão; evento e estado juntos | desabilitar novas rotas, preservar eventos |
| H2C.3B.4 | ajustar telas e ações | template e JavaScript | confusão entre editar, inativar e baixar | renderização, teclado, celular e permissões | ação principal é visualizar; sem lixeira | voltar template sem reativar exclusão |
| H2C.3B.5 | bloquear inativos em novos vínculos | transações e seletores futuros | vínculo atual não é usado pelo código | seleção, acesso direto e corrida de estado | somente Ativo é selecionável | desabilitar seletor novo mantendo dados |
| H2C.3B.6 | melhorar pesquisa, filtros e mensagens | consulta e interface | desempenho e escopo | paginação, normalização, UVR e erros | filtros claros e resposta rápida | manter endpoint anterior temporariamente |
| H2C.3B.7 | exibir histórico e auditoria | serviço, detalhes e eventos | privacidade e volume | ordenação, autoria e imutabilidade | eventos consultáveis e não editáveis | ocultar visualização, preservar eventos |
| H2C.3B.8 | criar relatórios essenciais | consultas e exportações | vazamento entre UVRs | escopo, volume, CSV seguro e filtros | relatórios essenciais aprovados | desativar relatório sem alterar cadastros |
| H2C.3B.9 | homologar o módulo completo | ambiente de homologação | dados e permissões incompletos | suíte completa e roteiro manual | aceite funcional e plano de retorno | restaurar versão anterior do código |

Cada incremento terá revisão própria. Nenhum deve misturar catálogo, usuários ou
ativos contratuais sem necessidade comprovada.

## 27. Questões que dependem de decisão humana

1. O número patrimonial será obrigatório para todo novo cadastro?
2. Sua unicidade será global ou por UVR?
3. Placa, Renavam e série também serão únicos?
4. Usuário operacional apenas solicitará ou poderá inativar diretamente?
5. Reativação sempre exigirá motivo?
6. Quem poderá corrigir uma baixa feita por engano?
7. Em manutenção poderá receber despesas de manutenção mesmo indisponível para
   uso normal?
8. Quais referências apenas alertam e quais impedem mudança de situação?
9. Qual é a fonte oficial de UVR e associação até a H2C.2D?
10. Responsável e operador continuarão textuais durante quanto tempo?
11. Como tratar duplicidades e situações vazias já existentes?
12. Há obrigação legal de guardar fotos ou documentos de baixa?
13. Quais relatórios são exigidos formalmente e por quanto tempo?
14. Uso compartilhado significa compartilhamento entre quais unidades?
15. Existe algum caso real que ainda exija exclusão física?
16. Quais nomes e finalidades possuem os 12 campos financeiros adicionais além
    de `id_patrimonio`?
17. O fluxo de transferência entre UVRs fará parte do patrimônio ou de etapa
    posterior?

## 28. Conclusão da especificação

O patrimônio atual é utilizável, mas sua exclusão física, a ausência de
histórico completo e o vínculo financeiro sem integridade tornam arriscada uma
mudança única e grande.

A direção recomendada é preservar o cadastro atual, reaproveitar
`status_bem`, introduzir histórico aditivo e substituir a lixeira por ações
claras de inativação e reativação. A implementação deve começar por testes de
caracterização e terminar com homologação por perfil e UVR.

Esta H2C.2B encerra apenas a especificação. Patrimônio ainda não foi alterado.
