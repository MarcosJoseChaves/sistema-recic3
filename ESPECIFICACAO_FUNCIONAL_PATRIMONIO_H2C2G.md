# Especificação funcional aprovada do patrimônio — H2C.2G

**Situação:** APROVADA

**Data da aprovação funcional:** 30/07/2026

## 1. Finalidade e limites

Este documento complementa, sem substituir, a especificação histórica
`ESPECIFICACAO_FUNCIONAL_PATRIMONIO_H2C2B.md`. Ele reconstrói o comportamento
atual e registra as decisões funcionais aprovadas pelo usuário.

Nesta etapa não foram acessados banco, `.env`, dump externo, APIs ou serviços
reais. Não foram criados SQL, migration, código funcional, deploy, commit ou
push. A aprovação é exclusivamente funcional: não declara implementação.

## 2. Resumo em linguagem simples

O cadastro atual funciona, mas trata vários conceitos diferentes como textos na
mesma linha: quem responde pelo bem, quem é seu proprietário, onde ele está,
quem o utiliza e qual é sua situação. O banco possui 38 colunas e quase todas
aceitam vazio. O sistema também permite hoje que um administrador apague o bem
definitivamente, mesmo havendo uma referência financeira sem proteção formal.

A evolução segura deve preservar os registros e as 38 colunas, acrescentar
histórico e vínculos próprios e substituir a exclusão cotidiana por inativação
ou baixa. As 30 decisões foram aprovadas em 30/07/2026. O desenho técnico,
as migrations e a implementação permanecem pendentes.

## 3. Comportamento patrimonial atual

### 3.1 Operações, acesso e efeitos

| Operação | Arquivo/função e método | Campos e dependências | Perfil e escopo | Efeito e risco |
|---|---|---|---|---|
| cadastrar | `app.py`, `cadastrar_patrimonio`, POST | grava 37 campos; `data_cadastro` é padrão; usa formulário e foto | login; não valida no servidor a UVR enviada | grava diretamente para admin e usuário comum; risco de cadastro fora do escopo |
| pesquisar | `app.py`, `buscar_patrimonio`, GET/JSON | descrição, placa, código, categoria e UVR; tabela `patrimonio` | login; admin vê todas e usuário comum fica em `uvr_acesso` | sem paginação e sem filtro de situação |
| detalhar | `app.py`, `get_patrimonio_detalhes`, GET/JSON | todas as 38 colunas; tabela `patrimonio` | login e autorização do objeto por UVR | devolve todos os campos do objeto autorizado |
| editar | `app.py`, `editar_patrimonio`, POST | 35 campos possíveis e foto opcional; não edita `medidor_atual`; patrimônio e solicitação | login e autorização por UVR | admin atualiza diretamente; usuário comum cria solicitação |
| aprovar edição | `app.py`, `responder_solicitacao`, POST/JSON | lista permitida de 36 campos; `solicitacoes_alteracao` e patrimônio | administrativa, sem novo escopo patrimonial especializado | aplica a fotografia da solicitação; não cria histórico patrimonial próprio |
| excluir | `app.py`, `excluir_patrimonio`, POST/JSON | ID e descrição; patrimônio e solicitação | login e autorização por UVR | admin usa exclusão física; usuário comum solicita exclusão |
| aprovar exclusão | `app.py`, `responder_solicitacao`, POST/JSON | ID, tabela-alvo e tipo; solicitação e patrimônio | administrativa | usa exclusão física sem conferir `transacoes_financeiras.id_patrimonio` |
| interface | `templates/cadastro.html`, HTML/JavaScript | campos do formulário, lista, detalhes e foto; rotas JSON acima | página autenticada; opções variam por papel | edição reutiliza o cadastro; webcam/upload guardam foto Base64 |

Todas as consultas de patrimônio identificadas usam parâmetros para os valores
digitados. Os `UPDATE` dinâmicos montam apenas nomes de campos definidos pelo
código. O problema atual não é injeção nesses trechos, mas regras incompletas,
exclusão física e falta de auditoria especializada.

### 3.2 Cadastro e formulário

- UVR, descrição, categoria e tipo são obrigatórios apenas no navegador.
- As categorias visíveis são Frota, Equipamento e Mobiliário.
- Os tipos visíveis incluem caminhão, carro, moto, trator, empilhadeira, prensa,
  esteira, balança, triturador e outros.
- Associação é preenchida por um mapa fixo no navegador.
- Responsável e operador são nomes textuais escolhidos entre associados ativos
  da UVR; não há chave estrangeira para a pessoa.
- O ano, o medidor inicial e o alerta vazios viram zero.
- O medidor atual nasce igual ao inicial.
- Abastecimento e manutenção começam permitidos no formulário.
- `status_bem` oferece Ativo, Manutenção, Inativo e Baixado, mas pode ser
  alterado como um campo comum, sem motivo ou evento próprio.
- A foto enviada ou capturada é armazenada como Base64 na própria linha.
- Não há validação de unicidade para número patrimonial, placa, Renavam ou
  série/chassi.

### 3.3 Consulta, relatórios e vínculos

- A listagem mostra descrição, tipo/categoria, placa ou código, medidor,
  responsável e situação.
- A busca usa descrição sem diferenciar maiúsculas, mas placa e código não têm
  normalização comprovada.
- Não existem importador, exportador ou relatório específico de patrimônio no
  código atual.
- `transacoes_financeiras.id_patrimonio` é uma referência opcional sem chave
  estrangeira e sem fluxo versionado localizado.
- Outros campos financeiros opcionais mencionam motorista, combustível,
  medidor, manutenção e garantia, mas não há fluxo patrimonial estruturado que
  permita tratá-los como operação aberta ou encerrada.
- `solicitacoes_alteracao` relaciona logicamente patrimônio e ID para edição ou
  exclusão, sem chave estrangeira.
- Não foram comprovadas estruturas próprias de transferência, custódia,
  compartilhamento, manutenção, abastecimento, fotografia adicional, documento
  ou evento patrimonial.
- `fc_ativos_contratuais` pertence à Fiscalização de Contratos e permanece fora
  deste escopo; identificações semelhantes não criam vínculo automático.

## 4. Os conceitos que precisam ser separados

| Conceito | Definição funcional |
|---|---|
| responsável institucional | entidade que responde administrativamente pelo bem |
| proprietário jurídico | pessoa ou entidade titular do bem, quando isso for aplicável |
| associação responsável | associação que exerce a responsabilidade institucional |
| UVR de localização | unidade onde o bem se encontra fisicamente |
| UVR de uso | unidade autorizada a utilizar o bem, que pode diferir da localização |
| usuário responsável | usuário do sistema encarregado por uma ação ou decisão |
| custodiante | pessoa que detém a guarda cotidiana do bem |
| bem compartilhado | bem com uma unidade principal e uso formal por outras UVRs |
| bem em manutenção | bem temporariamente indisponível por manutenção registrada |
| bem baixado | bem retirado formalmente do patrimônio, com motivo e histórico |
| bem inativo | registro indisponível para novas operações, em condição reversível |
| bem transferido | bem cuja localização, uso ou responsabilidade foi alterada por procedimento formal |

As colunas textuais atuais serão preservadas durante a transição, mas não devem
continuar representando sozinhas todos esses conceitos.

## 5. Classificação das 38 colunas preservadas

Legenda:

- **Atual:** obrigação observada no banco e no formulário atual.
- **Busca/U/A:** participação em busca, unicidade ou autorização hoje.
- **Classe:** avaliação funcional nesta etapa; não autoriza remoção.

| # | Coluna e tipo comprovado | Finalidade e tipo funcional | Uso e campo vazio atuais | Busca/U/A | Classe e risco de alteração |
|---:|---|---|---|---|---|
| 1 | `id` — INTEGER | identificação técnica | PK obrigatória e automática | objeto; não é identificação amigável | funcional e utilizada; risco crítico |
| 2 | `uvr` — VARCHAR(50) | localização/escopo | banco aceita nulo; formulário exige | autorização e filtro; não única | funcional, utilizada e candidata a normalização; crítico |
| 3 | `associacao` — VARCHAR(100) | responsabilidade institucional | nula; navegador preenche quando mapeada | não autoriza hoje | funcional e candidata a normalização; crítico |
| 4 | `tipo_bem` — VARCHAR(100) | classificação | nulo no banco; exigido no formulário | exibido/filtro indireto; não único | funcional e candidata a normalização; alto |
| 5 | `categoria` — VARCHAR(100) | classificação | nulo no banco; exigido no formulário | filtro atual; não única | funcional e candidata a normalização; alto |
| 6 | `descricao` — VARCHAR(255) | identificação descritiva | nula no banco; exigida no formulário | busca atual; não única | funcional e utilizada; alto |
| 7 | `codigo_patrimonio` — VARCHAR(50) | identificação patrimonial | opcional e pode ser vazio | busca; sem unicidade | funcional e utilizada; crítico |
| 8 | `marca` — VARCHAR(100) | classificação técnica | opcional/vazia | não participa | funcional e utilizada; médio |
| 9 | `modelo` — VARCHAR(100) | classificação técnica | opcional/vazio | não participa | funcional e utilizada; médio |
| 10 | `ano_fabricacao` — INTEGER | característica/aquisição | opcional; vazio vira zero | não participa | funcional e utilizada; alto, pois zero é ambíguo |
| 11 | `numero_serie_chassi` — VARCHAR(100) | identificação técnica/veículo | opcional/vazio | sem busca e sem unicidade | funcional e utilizada; crítico |
| 12 | `situacao_propriedade` — VARCHAR(100) | propriedade/posse | nula; formulário sugere próprio | não participa | funcional e candidata a normalização; crítico |
| 13 | `entidade_proprietaria` — VARCHAR(100) | proprietário jurídico | nula; lista fixa no formulário | não participa | funcional e candidata a normalização; crítico |
| 14 | `orgao_cedente` — VARCHAR(100) | propriedade/cessão | opcional/vazio | não participa | funcional condicional; alto |
| 15 | `numero_termo_comodato` — VARCHAR(100) | documento de posse | opcional/vazio | não participa | funcional condicional; alto |
| 16 | `data_inicio_comodato` — DATE | período de posse | opcional/NULL | não participa | funcional condicional; alto |
| 17 | `data_fim_comodato` — DATE | período/alerta | opcional/NULL | não participa | funcional condicional; alto |
| 18 | `placa` — VARCHAR(20) | identificação de veículo | opcional/vazia | busca; sem unicidade | funcional e utilizada; crítico |
| 19 | `renavam` — VARCHAR(50) | identificação de veículo | opcional/vazio | sem busca e sem unicidade | funcional e utilizada; crítico |
| 20 | `combustivel` — VARCHAR(50) | veículo/energia | nulo; lista inclui não aplicável | não participa | funcional condicional e candidata a normalização; médio |
| 21 | `capacidade_carga` — VARCHAR(50) | característica técnica | opcional/vazia, com unidade misturada | não participa | ambígua e candidata a normalização; alto |
| 22 | `controle_por` — VARCHAR(50) | uso por km/horas/nenhum | nulo; seleção visual | não participa | funcional e candidata a normalização; alto |
| 23 | `medidor_inicial` — NUMERIC(15,2) | uso/auditoria inicial | opcional; vazio vira zero | não participa | funcional e utilizada; alto |
| 24 | `medidor_atual` — NUMERIC(15,2) | uso/manutenção | nulo; nasce do inicial | detalhe; não editado no formulário comum | funcional e utilizada; crítico |
| 25 | `local_instalacao` — VARCHAR(150) | localização interna | opcional/vazio | não filtra nem autoriza | funcional e candidata a normalização; alto |
| 26 | `setor_uso` — VARCHAR(100) | localização/uso | opcional/vazio | não filtra nem autoriza | funcional e candidata a normalização; alto |
| 27 | `nome_responsavel` — VARCHAR(150) | responsabilidade/custódia | opcional/vazio; nome textual | exibido; não autoriza | funcional e candidata a normalização; crítico |
| 28 | `nome_operador_principal` — VARCHAR(150) | uso/custódia | opcional/vazio; nome textual | detalhe; não autoriza | funcional e candidata a normalização; alto |
| 29 | `status_bem` — VARCHAR(50) | situação administrativa atual | nulo; formulário oferece quatro valores | exibido; sem filtro atual | funcional e utilizada; crítico |
| 30 | `estado_conservacao` — VARCHAR(50) | estado físico | nulo; formulário sugere Bom | detalhe; não autoriza | funcional e candidata a normalização; alto |
| 31 | `permite_abastecimento` — BOOLEAN | elegibilidade operacional | nulo; formulário começa verdadeiro | não participa | funcional, mas sem fluxo atual; alto |
| 32 | `permite_manutencao` — BOOLEAN | elegibilidade operacional | nulo; formulário começa verdadeiro | não participa | funcional, mas sem fluxo atual; alto |
| 33 | `alerta_preventiva` — INTEGER | manutenção por medidor | opcional; vazio vira zero | não participa | funcional, mas sem uso atual comprovado; alto |
| 34 | `observacoes_gerais` — TEXT | informação complementar | opcional/vazia | não participa | funcional e utilizada; médio |
| 35 | `foto_bem_base64` — TEXT | fotografia principal | opcional/vazia | detalhes; não autoriza | funcional, histórica e candidata a normalização; crítico |
| 36 | `eh_bem_publico` — BOOLEAN | propriedade/regras especiais | nulo; código assume falso se desmarcado | não participa | funcional e utilizada; alto |
| 37 | `uso_compartilhado` — BOOLEAN | uso por várias unidades | nulo; código assume falso | não participa | ambígua e candidata a normalização; alto |
| 38 | `data_cadastro` — TIMESTAMP | auditoria de criação | nula no legado; padrão na criação | detalhe indireto | funcional e histórica; crítico |

Resultado: **38 de 38 colunas classificadas**. Nenhuma foi indicada para remoção.
As colunas textuais de UVR, associação e pessoas, a foto Base64 e os indicadores
sem fluxo próprio devem ser preservados por compatibilidade enquanto estruturas
normalizadas forem introduzidas de forma aditiva.

## 6. Identificação e unicidade

### 6.1 Número patrimonial

| Alternativa | Vantagem | Risco |
|---|---|---|
| obrigatório e único globalmente | identificação simples em toda a instituição | pode conflitar com numerações independentes de associações |
| obrigatório e único por associação | respeita séries locais | exige associação estável e cuidado na transferência |
| opcional no provisório e obrigatório antes de ativar | acolhe bem antigo, doado, sem plaqueta ou carga incompleta | requer estado provisório e fila de regularização |
| histórico sem unicidade | máxima compatibilidade | não evita novos duplicados |

**Decisão aprovada:** permitir cadastro em RASCUNHO ou PROVISORIO sem número,
exigir número antes da ativação e aplicar unicidade por associação. O sistema
terá identificador interno global independente. O número não poderá ser
reaproveitado, e correção ou mudança de associação preservará o valor anterior
em histórico.

### 6.2 Placa, Renavam e série/chassi

- **Placa:** condicional a veículo emplacado; remover espaços e pontuação,
  converter para maiúsculas e aceitar padrões antigo e Mercosul. Quando
  preenchida, terá unicidade global. Alteração exige
  justificativa e histórico.
- **Renavam:** condicional quando aplicável; armazenar como texto de dígitos para
  preservar zeros; validar formato definido posteriormente. Quando preenchido,
  recomenda-se unicidade global.
- **Série/chassi:** condicional por classe; normalizar espaços e capitalização.
  Chassi veicular tende a ser globalmente único. Série de equipamento pode se
  repetir entre fabricantes, recomendando comparação condicionada por
  fabricante/categoria. Ausência deve ser NULL, não “SEM SÉRIE”.

## 7. Duplicidades e incompletude

### 7.1 Tratamento de duplicidades

1. Novos conflitos inequívocos de número, placa ou Renavam devem ser bloqueados.
2. Conflitos de série devem gerar bloqueio ou alerta conforme a classe do bem.
3. Registros antigos semelhantes devem entrar em fila de saneamento humano.
4. Não haverá fusão nem exclusão automática.
5. Quando forem registros realmente distintos, uma exceção justificada pode
   manter ambos.
6. Quando forem o mesmo bem, deve-se escolher um registro principal, inativar a
   duplicata e criar vínculo histórico entre eles.
7. Movimentações, fotos, documentos e referências antigas devem permanecer
   rastreáveis; não serão transferidos silenciosamente.
8. Duplicidade entre UVRs ou associações não deixa de ser duplicidade apenas por
   estar em outro escopo.

### 7.2 Ausência, não aplicabilidade e pendência

- **Rascunho:** ainda não submetido ao fluxo oficial.
- **Provisório:** registrado, mas ainda sem dados suficientes para ativação.
- **Ativo incompleto:** legado ativo que precisa de saneamento, sem bloqueio
  automático até decisão.
- **Histórico incompleto:** registro antigo preservado para consulta.
- **Não aplicável:** regra da classe dispensa o campo.
- **Desconhecido:** informação não foi encontrada.
- **Pendente de regularização:** informação exigível, com providência aberta.

Ausência deve ser representada por campo vazio/NULL e por um motivo estruturado,
não por textos como “NÃO TEM” ou “N/A”. Para rascunho, bastam identificação
interna, descrição provisória e autoria. Para ativar, recomenda-se exigir
descrição, categoria/tipo, associação responsável, UVR, situação administrativa
e identificadores aplicáveis. Transferência e baixa exigem cadastro ativo ou
histórico suficientemente identificado, justificativa e responsáveis. Relatório
oficial deve distinguir dado ausente de não aplicável.

## 8. Classes de bens

As três categorias atuais e os tipos do formulário não formam ainda um catálogo
institucional validado. A classificação futura deve avaliar veículo, máquina,
equipamento, mobiliário, eletrônico, ferramenta, imóvel se houver evidência,
bem durável e outros.

| Classe | Regras específicas a avaliar |
|---|---|
| veículo | placa, Renavam, chassi, combustível, medidor, documento e manutenção |
| máquina/equipamento | série, fabricante, capacidade, horímetro e manutenção |
| mobiliário/eletrônico/ferramenta | número patrimonial, série quando houver e custodiante |
| imóvel | somente após confirmar que pertence ao escopo; não presumir regras móveis |
| outros/duráveis | requisitos definidos por subtipo, sem fabricar classificação contábil |

Não há evidência funcional suficiente para implementar depreciação.

## 9. Situação administrativa e transições

### 9.1 Modelo funcional aprovado

`status_bem` deve representar apenas a situação administrativa. Estado de
conservação, localização, disponibilidade, manutenção, empréstimo e
compartilhamento devem ser informações ou vínculos separados.

Estados aprovados: **RASCUNHO**, **PROVISORIO**, **ATIVO**, **INATIVO** e
**BAIXADO**. Manutenção, transferência, compartilhamento, empréstimo e pendência
de regularização são condições ou vínculos separados.

### 9.2 Transições aprovadas

| Transição | Quem inicia/aprova | Requisitos e bloqueios | Reversão |
|---|---|---|---|
| RASCUNHO → PROVISORIO | ator autorizado | dados mínimos e autoria | pode voltar a rascunho |
| PROVISORIO → ATIVO | patrimônio; aprovação conforme risco | número, foto quando aplicável, associação, UVR e responsável | por inativação, não apagando |
| ATIVO → INATIVO | patrimônio ou solicitação local | motivo, ausência de operação conflitante e evento | reativação justificada |
| INATIVO → ATIVO | patrimônio | revisão de identificação, localização e responsabilidade | novo evento |
| ATIVO → transferência em andamento | gestor solicita; patrimônio aprova | destino, motivo, entrega e bloqueio de outra transferência | cancelar/rejeitar com histórico |
| transferência → ATIVO no destino | destino aceita e patrimônio conclui | recebimento, data e estado físico | nova transferência, não sobrescrita |
| ATIVO/INATIVO → BAIXADO | patrimônio e aprovador independente | motivo, vínculos verificados e documentos aplicáveis | somente reversão excepcional |
| BAIXADO → ATIVO | duas aprovações distintas | reversão excepcional comprovada, documento e justificativa | novo evento, preservando baixa |

## 10. Inativação, baixa e reativação

**Inativação** apenas impede novas operações e é reversível. **Baixa** retira
formalmente o bem do patrimônio e não apaga o registro. Motivos possíveis:
descarte, perda, furto, sinistro, doação, venda, transferência definitiva e
outro autorizado. Erro cadastral deve usar correção ou saneamento, não baixa,
sempre que possível.

A baixa deve guardar motivo, data, solicitante, aprovador, situação anterior,
destino, justificativa, verificações, documento/foto quando aplicáveis e evento
de auditoria. O bem continua em relatórios históricos, preserva vínculos, fotos,
documentos e transações e sai do inventário ativo.

Reativação de inativo é uma operação controlada. Reversão de baixa é
excepcional, não silenciosa, e exige justificativa, documento quando aplicável e
aprovação segregada. O identificador anterior deve ser preservado; eventual
novo número será uma mudança auditada.

## 11. Atores, permissões e segregação

O modelo aprovado na H2C.2E separa perfil, permissão e escopo. As atribuições
funcionais abaixo estão aprovadas; a implementação das permissões está pendente:

| Ação | Atores candidatos | Controle adicional |
|---|---|---|
| consultar | Consulta, Operador, Gestor, Patrimônio e administradores com escopo | limitar associação/UVR e objeto |
| cadastrar patrimônio | Responsável por Patrimônio; Administrador de Associação ou Global com permissão patrimonial | no escopo autorizado |
| abrir provisório/solicitar inclusão | Gestor de UVR | exige fluxo e escopo |
| editar dados simples | Gestor/Patrimônio ou solicitação do Operador | histórico conforme relevância |
| corrigir identificação | solicitação formal por ator autorizado | aprovador distinto, justificativa e histórico |
| transferir entre UVRs | Gestor solicita; Patrimônio aprova; destino aceita | sem autoaceite |
| transferir entre associações | Patrimônio e autoridades das duas associações | dupla aprovação e documento |
| inativar/reativar | Gestor de UVR ou Responsável por Patrimônio solicita | aprovador autorizado distinto, motivo e evento |
| baixar | Gestor de UVR ou Responsável por Patrimônio solicita | permissão específica e aprovador diferente |
| reverter baixa | fluxo excepcional | comprovação, duas aprovações e histórico integral |
| anexar/ver documento | permissão própria | escopo, categoria e privacidade |
| visualizar valor | permissão patrimonial financeira | não decorre de consulta comum |
| exportar/relatar | permissões próprias | mesmos filtros e escopos da tela |

Administrador Global não recebe automaticamente poderes especializados de
patrimônio. Mudança de associação, baixa, reversão de baixa, identificação,
valor, exclusão excepcional e consolidação de duplicidade exigem segregação.
Venda, doação, transferência definitiva entre associações, perda, furto,
sinistro, mudança de responsabilidade institucional, reversão de baixa,
consolidação patrimonial de duplicidade e outras destinações excepcionais
classificadas exigem duas aprovações distintas.

## 12. Transferências

### 12.1 Entre UVRs

A transferência deve registrar origem, destino, associação, solicitante,
responsável pela entrega, responsável pelo recebimento, datas, motivo,
documentos, estado físico e eventos. Durante o trânsito, o bem fica indisponível
para outra transferência ou baixa conflitante.

Decisão aprovada: a origem solicita; o responsável patrimonial aprova; a UVR de
destino aceita; e entrega, recebimento e conclusão ficam registrados. Rejeição
ou cancelamento preserva o histórico. A operação muda localização ou uso, não a
associação responsável.

### 12.2 Entre associações

É excepcional e deve distinguir transferência de uso, guarda e
responsabilidade institucional/propriedade. Recomenda-se concordância das duas
associações, aprovadores diferentes, documento, motivo, origem, destino, data e
histórico imutável. O solicitante não pode autoaprovar.

## 13. Bens compartilhados

O booleano `uso_compartilhado` não informa quais UVRs utilizam o bem, por quanto
tempo, quem responde por ele ou onde está. As alternativas são:

- manter apenas a informação simples;
- criar futuramente vínculo formal com UVR principal, beneficiadas, período,
  responsável, custos e localização;
- adiar todo o tratamento para módulo futuro.

Decisão aprovada: preservar o booleano legado e planejar vínculo formal, sem
implementar agenda ou reserva agora. A associação responsável e uma UVR
custodiante ou principal permanecem identificadas.

## 14. Manutenção, veículos, combustível e finanças

O cadastro guarda placa, Renavam, combustível, medidores, permissão de
abastecimento/manutenção e alerta preventivo. `transacoes_financeiras` possui
campos opcionais relacionados, mas não existe fluxo estruturado comprovado de
manutenção aberta, abastecimento, garantia ou despesa pendente.

Assim, estes vínculos não podem ser declarados bloqueadores atuais. A evolução
deve primeiro caracterizar o fluxo. Não serão criadas funções financeiras nem
depreciação nesta etapa.

## 15. Bloqueios, alertas e preservação

| Situação/vínculo | Classificação aprovada | Motivo |
|---|---|---|
| outra transferência em andamento | bloqueio de transferência/baixa | evita destinos concorrentes |
| empréstimo, cessão, compartilhamento, custódia ou responsabilidade ativa | bloqueio de baixa | exige encerramento formal |
| manutenção aberta | bloqueio de baixa; alerta para transferência | situação precisa ser encerrada ou assumida |
| conflito de duplicidade ou divergência de inventário pendente | bloqueio de baixa | exige saneamento |
| operação financeira ou administrativa aberta | bloqueio de baixa | operação deve ser concluída |
| documento obrigatório de baixa ausente | bloqueio de baixa | requisito do próprio ato |
| identificação insuficiente para ato formal | bloqueio do ato | evita baixar ou transferir bem incerto |
| referência financeira concluída | alerta e preservação | histórico concluído não deve ser apagado |
| manutenção/abastecimento concluído | alerta e preservação | não impede ato, mas permanece consultável |
| solicitação pendente sobre o mesmo bem | bloqueio de operação conflitante | evita sobrescrita |
| foto ou documento histórico | preservação | não deve desaparecer com inativação ou baixa |
| ativo contratual semelhante | nenhum vínculo automático | cadastro separado e fora do escopo |

As categorias estão funcionalmente aprovadas, mas os bloqueios ainda dependem
de estruturas e implementação técnica.

## 16. Fotografias e documentos

### 16.1 Fotografias

A foto Base64 atual será preservada como fotografia histórica principal. Uma
estrutura futura pode distinguir foto principal, identificação, conservação,
transferência, baixa e dano, com data, autoria, finalidade, integridade,
visibilidade e substituição auditada. Foto removida da posição principal não
deve ser apagada do histórico.

Fotografia pode conter pessoas, placas e locais; deve haver minimização,
controle de acesso e retenção. Ela é obrigatória antes da ativação para bens
individualmente controlados, admitindo exceção justificada e regularização do
legado. Na baixa, é obrigatória conforme o motivo; se o bem não estiver
disponível, exige-se documento comprobatório adequado.

### 16.2 Documentos

Tipos previstos: nota fiscal, termo de doação, responsabilidade, veículo,
laudo, orçamento, comprovante de manutenção, transferência, boletim de
ocorrência, baixa e autorização. Toda baixa terá termo ou registro formal,
justificativa, responsável, aprovação, data, motivo estruturado e documentos
específicos aplicáveis. Privacidade, visualização e retenção ainda exigem desenho
técnico. Documento de ato concluído não será apagado no fluxo comum.

Nenhuma fotografia ou documento real integrará a baseline.

## 17. Exclusão física excepcional

Decisão aprovada: permitir exclusão física somente de rascunho nunca ativado,
nunca enviado e sem vínculo ou movimentação. A ação será altamente restrita,
com justificativa, confirmação reforçada e auditoria.

Depois de ativação, movimentação ou vínculo histórico, a exclusão física deve
ser proibida. Usa-se correção, inativação, baixa ou saneamento de duplicidade.

## 18. Valores e informações financeiras

A tabela `patrimonio` não possui coluna comprovada de valor de aquisição, valor
atual ou depreciação. O vínculo opcional com transações não autoriza inferir
esses valores. Qualquer valor futuro deverá distinguir aquisição, estimativa e
custo, preservar a fonte e não reescrever histórico.

Visualizar, editar, relatar e exportar valores exigirá permissões específicas e
separadas. Correção de valor também exige solicitação, justificativa e, conforme
risco, aprovação segregada. Não se propõe cálculo de depreciação.

## 19. Histórico, retenção e concorrência

### 19.1 Eventos mínimos

Cadastro, ativação, edição relevante, correção de identificação, mudança de
responsável, mudança de UVR, início/conclusão/cancelamento de transferência,
compartilhamento, manutenção futura, inativação, reativação, baixa, reversão,
documento, fotografia, duplicidade e saneamento.

Cada evento deve registrar bem, ator, data, antes, depois, motivo, associação,
UVR e solicitação relacionada, sem credenciais. Eventos não serão editados nem
apagados no fluxo normal.

### 19.2 Retenção

- registro ativado, baixa e transferências permanecem;
- documentos e fotos seguem política ainda a definir;
- rascunho descartado pode ser eliminado conforme política;
- dados pessoais incidentais devem ser minimizados;
- não se fixa prazo legal sem fonte ou aprovação;
- backups e retenção operacional serão definidos separadamente.

### 19.3 Concorrência

Alteração durante análise, duas transferências, baixa durante manutenção,
identificação durante transferência e mudança de UVR durante inventário exigem
controle de versão/bloqueio. O sistema deve detectar conflito, impedir
sobrescrita silenciosa e aplicação duplicada e devolver o caso para análise
preservando eventos.

## 20. Relatórios mínimos

Relatórios mínimos aprovados:

1. inventário ativo geral, por associação e por UVR;
2. provisórios, incompletos e sem associação ou UVR válida;
3. duplicidades;
4. em manutenção, transferência ou compartilhamento;
5. inativos e baixas por período/motivo;
6. histórico por bem;
7. veículos e documentação;
8. documentos e fotografias pendentes;
9. valores, apenas para quem possuir permissão específica.

Exportação terá permissão própria e nunca ampliará o escopo de consulta.

## 21. Baseline e migração futura

### 21.1 O que pode integrar a baseline

- estruturas vazias de patrimônio, eventos, transferências, documentos e fotos;
- códigos técnicos estáveis de estados, eventos e motivos aprovados;
- nenhum bem, pessoa, associação, UVR, foto, documento, valor ou histórico real.

Dados configuráveis e catálogos ainda não estabilizados devem ser carregados por
procedimento separado.

### 21.2 Sequência conceitual futura

1. preservar as 38 colunas e todos os registros;
2. criar estruturas adicionais sem remover as antigas;
3. caracterizar e mapear `status_bem`;
4. classificar incompletos sem inventar valores;
5. detectar duplicidades sem fusão automática;
6. mapear associação responsável e UVR;
7. criar marco histórico inicial explicitamente identificado como migração;
8. comparar leituras antigas e novas;
9. migrar autorização para perfil, permissão e escopo;
10. encerrar exclusão física cotidiana;
11. manter compatibilidade e reversão durante a transição.

Não há SQL ou migration autorizado neste documento.

## 22. DECISÕES FUNCIONAIS APROVADAS

As decisões abaixo foram aprovadas integralmente pelo usuário em **30/07/2026**:

1. O número patrimonial é obrigatório antes da ativação.
2. O número é único por associação, não pode ser reutilizado e não substitui o
   identificador interno global.
3. RASCUNHO ou PROVISORIO pode existir sem número, mas não pode ser ativado nem
   usado em operação oficial antes da regularização.
4. Placa preenchida é única, normalizada e possui histórico de alterações.
5. Renavam aplicável e preenchido é globalmente único, textual e preserva zeros.
6. Série é única no contexto de fabricante e classificação ou modelo.
7. Identificador ausente usa valor ausente e classificação própria, nunca texto
   artificial.
8. Duplicidades passam por saneamento humano, sem fusão ou exclusão automática.
9. Os estados são RASCUNHO, PROVISORIO, ATIVO, INATIVO e BAIXADO; condições
   operacionais ficam separadas.
10. Inativação é reversível e distinta da baixa formal.
11. Cadastram oficialmente o Responsável por Patrimônio, o Administrador de
    Associação com permissão patrimonial e o Administrador Global com permissão
    patrimonial específica. Gestor de UVR abre provisório ou solicita inclusão.
12. Identificação crítica exige solicitação, aprovador diferente e histórico.
13. Gestor de UVR ou Responsável por Patrimônio solicita inativação/reativação,
    com aprovação por responsável autorizado distinto.
14. Responsável por Patrimônio ou Gestor de UVR solicita baixa; usuário com
    permissão específica aprova.
15. Toda baixa exige aprovador diferente do solicitante.
16. Venda, doação, transferência definitiva entre associações, perda, furto,
    sinistro, mudança de responsabilidade institucional, reversão de baixa,
    consolidação patrimonial de duplicidade e outras destinações excepcionais
    exigem duas aprovações distintas.
17. Bloqueiam baixa: transferência, manutenção, empréstimo, cessão,
    compartilhamento ou custódia ativos; responsabilidade ativa; duplicidade não
    resolvida; operação financeira/administrativa aberta; documento obrigatório
    ausente; divergência de inventário; e solicitação crítica concorrente.
18. Geram alerta e preservação: transações, manutenções e abastecimentos
    concluídos; transferências, associações, UVRs e compartilhamentos anteriores;
    documentos, fotografias, relatórios e inventários concluídos.
19. Transferência entre UVRs exige origem, aprovação patrimonial, aceite do
    destino, entrega, recebimento e conclusão.
20. Transferência entre associações exige aprovação da origem, aceite do
    destino, autorização administrativa, documento e histórico dos números.
21. Bem pode ser compartilhado formalmente entre UVRs, mantendo associação
    responsável e UVR custodiante ou principal.
22. Foto é obrigatória antes da ativação de bem individualmente controlado, com
    exceção justificada e regularização do legado.
23. Foto de baixa é obrigatória conforme o motivo; indisponibilidade física
    exige documento comprobatório adequado.
24. Toda baixa possui termo/registro formal, justificativa, responsável,
    aprovação, data, motivo e documentos aplicáveis.
25. BAIXADO não retorna pelo fluxo comum; a reversão é excepcional, comprovada,
    possui duas aprovações e histórico integral.
26. Exclusão física limita-se a rascunho nunca ativado, nunca enviado e sem
    vínculo ou movimentação.
27. Depois da ativação, exclusão física é proibida; usam-se correção,
    inativação, baixa ou marcação de duplicidade.
28. Visualizar, editar, relatar e exportar valores exige permissões específicas
    e separadas.
29. São relatórios mínimos: inventário ativo, por associação e por UVR;
    provisórios/incompletos; duplicidades; manutenção; transferência;
    compartilhados; inativos; baixas por período/motivo; histórico; veículos e
    documentação; pendências de foto/documento; bens sem associação/UVR válida;
    e valores para usuários autorizados.
30. A baseline pode conter códigos estruturais estáveis de estados, condições,
    eventos, motivos de baixa, transferências, documentos, fotografias,
    bloqueios e alertas, mas nenhum registro real.

### Modelo, estados e transições aprovados

- associação representa responsabilidade institucional;
- o bem possui identificador interno global;
- o número patrimonial é único por associação;
- UVR representa localização, uso ou custódia;
- compartilhamento formal entre UVRs é permitido;
- estado administrativo é separado de condição operacional;
- todo bem ativado possui histórico permanente.

Transições principais:

```text
RASCUNHO → PROVISORIO → ATIVO
ATIVO → INATIVO → ATIVO
ATIVO → BAIXADO
BAIXADO → ATIVO somente por reversão formal excepcional
```

## 23. IMPLEMENTAÇÃO TÉCNICA PENDENTE

A aprovação funcional não implementa:

- tabelas, colunas novas, tipos, constraints ou índices;
- catálogos técnicos definitivos;
- migrations ou migração dos registros atuais;
- detecção e saneamento técnico de duplicidades;
- perfis, permissões, escopos ou segregação;
- fluxos transacionais, bloqueios de concorrência ou auditoria;
- interfaces, formulários, relatórios ou exportações;
- armazenamento e upload de fotografias e documentos;
- alteração de `status_bem` ou encerramento da exclusão física atual;
- testes de implementação ou homologação.

## 24. Critério para prosseguir

As decisões funcionais estão encerradas. A próxima evolução patrimonial depende
de projeto técnico, migrations revisadas, caracterização do legado e autorização
específica. Esta aprovação não autoriza SQL, banco ou implementação.
