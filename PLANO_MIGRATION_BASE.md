# Plano da futura migration-base

## Finalidade

A futura migration-base deverá montar o schema completo do `sistema-recic3` em
um **PostgreSQL vazio**. Ela não servirá para atualizar, reparar ou conferir o
banco atual e nunca deverá ser aplicada sobre banco já utilizado.

Este plano é resultado da Etapa H2C.1. Nenhum SQL foi criado ou executado.

## Decisão atual

A exportação somente do schema foi obtida, auditada e comparada na H2C.1B. Ela
resolveu as lacunas físicas de `patrimonio` e `grupos_atividade`, comprovou a
ausência de `id_grupo`, confirmou a versão A de `solicitacoes_alteracao` e
validou as 23 tabelas `fc_` contra as migrations 001–011.

Ainda não é seguro implementar a baseline porque o banco possui 27 tabelas e
diversas colunas legadas sem uso identificado no código versionado.

Classificação: **C — ainda exige decisões funcionais**.

## Estratégia recomendada para H2C.2

Depois de resolver as decisões funcionais listadas neste plano:

1. criar um arquivo SQL de baseline, versionado e legível;
2. criar um executor Python pequeno e específico;
3. verificar obrigatoriamente que o banco está vazio antes do primeiro DDL;
4. usar blocos transacionais coerentes, interrompendo e revertendo o bloco em
   qualquer erro;
5. manter manifesto de versão e checksum;
6. testar somente em PostgreSQL temporário e descartável da infraestrutura de
   testes, nunca no banco atual;
7. separar totalmente estrutura e dados fictícios.

Um SQL único facilita a revisão do estado final. Blocos transacionais tornam
falhas mais claras do que uma transação gigantesca. O executor não deve conter
URL, senha nem fallback local.

## Proteção contra banco existente

Antes de qualquer `CREATE` ou `INSERT`, o executor futuro deverá:

1. validar ambiente permitido;
2. abrir transação somente de inspeção;
3. consultar o catálogo por tabelas de aplicação, incluindo `usuarios`,
   `cadastros`, `patrimonio` e qualquer `fc_*`;
4. recusar se encontrar qualquer uma;
5. recusar se encontrar a futura tabela de controle de migrations;
6. exigir confirmação explícita de banco novo e descartável;
7. encerrar antes do primeiro DDL quando houver dúvida.

Não usar `DROP`, não apagar/ajustar objetos existentes e não usar `CREATE TABLE
IF NOT EXISTS` para esconder divergências. A baseline deve executar uma vez em
banco vazio e falhar claramente numa segunda execução.

## Ordem de criação proposta

A ordem abaixo cobre o núcleo de 37 tabelas. As 27 tabelas adicionais só podem
ser inseridas depois de decisão funcional e respeitando suas FKs:

1. `usuarios`;
2. independentes legadas: `cadastros`, `associados`, `grupos_atividade` (se
   confirmada), `subgrupos`, `contas_correntes`, `denuncias`,
   `solicitacoes_alteracao` e `patrimonio`;
3. `produtos_servicos`;
4. `transacoes_financeiras` e `itens_transacao`;
5. `fluxo_caixa` e `fluxo_caixa_transacoes_link`;
6. `fc_empresas` e `fc_servidores`;
7. `fc_contratos` e `fc_contrato_responsaveis`;
8. `fc_aditivos` e seu índice único composto;
9. `fc_documentos`, `fc_planilhas_orcamentarias` e `fc_planilha_itens`;
10. `fc_ativos_contratuais` e `fc_ativo_vinculos`;
11. `fc_fiscalizacoes` e seu índice único composto;
12. `fc_ocorrencias`, `fc_ocorrencia_acompanhamentos` e
    `fc_fiscalizacao_eventos`;
13. `fc_medicoes`;
14. filhos de medição: itens, ajustes, documentos e eventos;
15. `fc_atestes`;
16. filhos de ateste: notas, documentos e eventos;
17. demais índices comprovados;
18. dados estruturais mínimos, somente se uma decisão posterior comprovar sua
    necessidade.

Não há ciclo entre tabelas distintas. A autorreferência
`fc_medicoes.medicao_origem_id` pode ficar na criação da tabela ou ser
adicionada depois, mas a escolha deve ser explícita e testada.

O schema atual confirma que `grupos_atividade` não possui relação física com
`subgrupos` ou `produtos_servicos`; as colunas `id_grupo` não existem.
`patrimonio` também não possui FKs. UVR e associação permanecem textos; não há
tabelas centrais correspondentes.

Se aprovadas, as tabelas extras deverão entrar por grupos: documentos e tipos
antes de auditoria/entrega; associados antes de EPI; hierarquia de ouvidoria
antes de manifestações e fotos. Não há autorização para incluí-las apenas
porque existem no banco atual.

## Relação com as migrations 001–011

- a baseline representará o estado estrutural final aprovado;
- 001–011 permanecerão intactas como histórico;
- banco novo receberá baseline e não repetirá 001–011;
- banco existente não receberá baseline;
- H2D definirá ledger, versão, checksum, lock e ordem formal;
- arquivos históricos não serão renumerados, apagados ou modificados.

A H2D deverá distinguir “baseline aplicada” de “incrementais aplicadas”, sem
fingir que um caminho executou o outro.

Separação arquitetural: H2C mapeia e futuramente cria o estado-base de um banco
vazio; H2D registrará versões, validará ordem, impedirá duplicações, armazenará
checksums e controlará baseline versus incrementais. Esta etapa não cria tabela
de controle nem antecipa seu desenho dentro do SQL da baseline.

## Estrutura, índices e restrições

Reproduzir somente objetos comprovados:

- tipos, nulabilidade e defaults;
- PKs, FKs, UQs e checks;
- índices comuns, parciais, compostos e por expressão;
- `SERIAL`/`BIGSERIAL`, salvo decisão explícita e testada de usar identity;
- `ON DELETE` somente onde já comprovado.

Índices de desempenho apenas recomendados devem ficar em migration posterior,
apoiada por consultas e planos reais. Não adicionar cascatas preventivas.

Para o módulo `fc_`, as migrations são a fonte primária: as 23 tabelas, 84 FKs,
103 CHECKs e 69 índices distintos foram confirmados. O índice
`uq_fc_aditivos_id_contrato_id` deverá aparecer uma única vez.

Para o legado sem DDL versionado, o schema atual é evidência física, mas a
decisão de inclusão continua funcional. O relatório da H2C.1B lista as 64
tabelas, 62 sequências, 113 FKs e 73 índices explícitos.

## Dados de referência

A baseline será primordialmente estrutural. Até agora, nenhum registro
obrigatório foi comprovado; tipos e status são constraints.

Devem ficar fora:

- administrador, usuários, senhas e hashes;
- UVR, associação, associado ou empresa real;
- contratos, documentos, planilhas, ativos, fiscalizações, medições e atestes;
- históricos, arquivos e chaves de armazenamento;
- cópia de dados de `migrar_dados.py`;
- catálogo CSV sem decisão funcional.

Dados fictícios de homologação devem ser criados por comando separado, com
credenciais fornecidas fora do Git.

## Testes previstos

1. recusar banco não vazio antes de qualquer alteração;
2. aplicar baseline em PostgreSQL vazio;
3. conferir tabelas, colunas, tipos, defaults e nulabilidade;
4. conferir PKs, FKs, UQs, checks e índices por nome e definição;
5. importar a aplicação sem executar migration;
6. iniciar Gunicorn sem executar migration;
7. executar suíte completa e suíte estrutural isolada;
8. testar rollback com falha induzida em cada bloco;
9. testar segunda execução e exigir falha clara;
10. comparar schema criado com representação canônica aprovada;
11. comprovar ausência de dados reais e credenciais;
12. comprovar ausência de fallback para banco não autorizado.

## Riscos

- schema legado continua incompleto no Git, embora agora documentado;
- drift provocado por scripts manuais;
- `IF NOT EXISTS` ter ocultado diferenças;
- script B de `solicitacoes_alteracao` conflita com versão A instalada;
- script de importação pressupõe `id_grupo`, ausente no banco;
- `patrimonio` possui nulabilidade ampla e exclusão física;
- 27 tabelas existem apenas no banco atual;
- colunas adicionais em `associados`, `transacoes_financeiras` e `usuarios`;
- exclusões físicas e cascatas legadas adicionais nas tabelas somente do banco;
- `TIMESTAMP` legado versus `TIMESTAMPTZ` no módulo;
- credenciais históricas em scripts antigos;
- dois CSVs de catálogo com conteúdo diferente;
- ausência de ledger até H2D.

## Decisões pendentes

1. decidir quais das 27 tabelas adicionais entram na baseline;
2. confirmar se auditoria, documentos, EPI e ouvidoria ainda são módulos ativos;
3. decidir as 12 colunas adicionais de `associados`;
4. decidir as 13 colunas adicionais de `transacoes_financeiras`;
5. decidir os três campos adicionais de `usuarios`;
6. decidir se os sete defaults `now()` instalados serão preservados;
7. decidir se `grupos_atividade` ficará desconectada ou será objeto de migration
   futura; não criar `id_grupo` na baseline sem essa decisão;
8. aprovar formalmente a versão A de `solicitacoes_alteracao`;
9. decidir nulabilidade e exclusão física de `patrimonio`;
10. decidir se o catálogo CSV é opcional ou obrigatório;
11. decidir se `SERIAL`/`BIGSERIAL` serão preservados literalmente;
12. decidir o tamanho dos blocos transacionais;
13. definir versão/checksum esperado pela H2D;
14. avaliar índices recomendados separadamente;
15. remover ou isolar scripts com credenciais históricas em etapa própria.

## Critério para iniciar H2C.2

H2C.2 só deve começar quando:

- o escopo desejado das 64 tabelas estiver aprovado;
- colunas e defaults legados adicionais tiverem decisão;
- o modelo do catálogo e de patrimônio estiver aprovado;
- a definição final tiver revisão técnica;
- houver PostgreSQL vazio e descartável para teste;
- o banco atual estiver fora do alcance do executor;
- o formato de versão esperado pela H2D estiver decidido.

## Diretrizes aprovadas na H2C.2A

A decisão funcional não autoriza criar a migration-base neste momento. Ela
estabelece as seguintes barreiras para o desenho futuro:

- preservar todas as estruturas e todos os dados durante a evolução;
- usar somente migrations aditivas até que exista autorização específica,
  evidência de ausência de uso e plano de reversão;
- não transformar automaticamente as 27 tabelas adicionais em funcionalidades
  ou itens de menu;
- aguardar etapa funcional específica posterior para decidir formalmente o
  escopo dessas 27 tabelas na baseline;
- manter as colunas históricas de `associados`, `transacoes_financeiras` e
  `usuarios` enquanto sua finalidade é esclarecida;
- preservar a versão A de `solicitacoes_alteracao`;
- tratar patrimônio, catálogo e UVR em incrementos próprios antes de consolidar
  o schema desejado;
- não reescrever migrations históricas.

A ordem funcional aprovada é H2C.2B patrimônio, H2C.2C catálogo, H2C.2D UVR,
H2C.2E usuários e permissões, H2C.2F solicitações, H2C.2G.1 escopo dos módulos
adicionais, H2C.2G.2 interface e relatórios, H2C.2H plano final da baseline e
H2C.2I homologação e reversão.

As decisões completas, critérios de aceite e dependências estão em
`MATRIZ_DECISOES_FUNCIONAIS_H2C2A.md`.

## Decisões estruturais preliminares da H2C.2B

A H2C.2B não criou SQL nem autorizou migration. Para o patrimônio, o futuro
desenho da baseline deverá:

- preservar as 38 colunas comprovadas;
- preservar `status_bem` e os códigos atuais até a caracterização dos dados;
- não criar um segundo indicador concorrente de situação;
- considerar estrutura aditiva de eventos para vários ciclos de inativação e
  reativação;
- preservar `data_cadastro` automática e os nulos históricos;
- não impor unicidade antes de auditar códigos, placas e séries existentes;
- não criar FK em `transacoes_financeiras.id_patrimonio` antes de verificar
  referências órfãs;
- quando aprovada, usar integridade que bloqueie exclusão, nunca cascata;
- manter campos textuais de UVR, associação e responsáveis durante transições;
- tratar fotografia e arquivos em decisão própria.

A solução estrutural só será escolhida depois dos testes de caracterização da
H2C.3B.1 e da aprovação das questões humanas listadas na especificação.

## Decisões estruturais preliminares da H2C.2C

A H2C.2C também não criou SQL nem autorizou migration. Para o catálogo, a
baseline futura deverá:

- preservar `grupos_atividade`, `subgrupos`, `produtos_servicos` e `produtos`;
- criar vínculos hierárquicos de modo aditivo e inicialmente opcional;
- manter textos legados enquanto transações e relatórios ainda dependerem deles;
- classificar registros existentes com revisão humana, sem fusão automática;
- permitir estado transitório “Não classificado” sem criar categoria artificial;
- introduzir situação e auditoria antes de substituir exclusões;
- não remover a unicidade global de item antes de transações guardarem
  identificador e fotografia;
- adicionar obrigatoriedade e unicidade novas somente depois de inventário,
  saneamento, validação e mudança das leituras.

O SQL definitivo dependerá dos testes de caracterização H2C.3C.1 e das decisões
humanas registradas em `ESPECIFICACAO_FUNCIONAL_CATALOGO_H2C2C.md`.

## Decisões funcionais aprovadas na H2C.2D

A H2C.2D não criou SQL nem autorizou migration. A futura baseline deverá prever
estruturas vazias, ainda sem nomes ou tipos definitivos, para:

- associações;
- UVRs vinculadas à entidade responsável;
- aliases;
- vínculos de usuários com uma ou mais UVRs;
- indicação de UVR principal;
- escopos de administrador global ou limitado a associação;
- histórico ou auditoria, conforme desenho técnico posterior.

Não entrarão na baseline associações reais, UVRs reais, aliases reais, usuário
administrador ou credenciais. A carga inicial será separada. A migração dos
textos será posterior e controlada, e o fallback textual nunca concederá acesso
global.

Conta corrente pertencerá à associação; UVR poderá ser dimensão gerencial.
Patrimônio separará responsabilidade institucional da unidade de uso.
Fiscalização de Contratos permanecerá global, sem dependência obrigatória de
UVR.

O desenho SQL, nomes, tipos, constraints e auditoria permanecem tecnicamente
pendentes. As decisões funcionais aprovadas estão em
`ESPECIFICACAO_FUNCIONAL_UVR_H2C2D.md`.

## Decisões aprovadas da H2C.2E

A H2C.2E não criou SQL nem autorizou migration. Foi aprovado que a baseline
futura poderá conter estruturas vazias para perfis, permissões, módulos, ações,
relações entre perfis e permissões, vínculos entre usuários, perfis e escopos,
delegações por módulo e auditoria das alterações de acesso.

Poderão existir como dados estruturais estáveis códigos técnicos de módulos e
ações, permissões estruturais e perfis institucionais protegidos. Não entrarão
na baseline usuários reais, e-mails, senhas, associações ou UVRs reais,
atribuições ou vínculos pessoais, tokens ou credenciais.

O primeiro Administrador Global será criado por procedimento seguro separado.
Nomes de tabelas, tipos, constraints, DDL e migration permanecem pendentes. A
transição futura comparará o modelo antigo e o novo antes de retirar a
dependência de `role` e `uvr_acesso`.

## Decisões aprovadas da H2C.2F

A H2C.2F não criou SQL nem autorizou migration. A baseline futura preservará a
versão A de `solicitacoes_alteracao`; a versão B incompatível ficará fora.

A baseline futura poderá conter estruturas vazias para solicitação principal,
eventos, mensagens, quatro fotografias, tipos, estados, categorias de mensagem,
níveis de risco, controle de aplicação e, quando tecnicamente aprovado, anexos.
Poderão entrar como dados estruturais códigos de estados, eventos, categorias
técnicas e ações aprovadas do fluxo.

Não entrarão solicitações, mensagens, anexos, usuários, aprovações,
justificativas, documentos ou dados históricos reais, nem conteúdo da versão B.
Nomes, tipos, constraints, índices, compatibilidade dos registros antigos e DDL
permanecem pendentes.

## Decisões patrimoniais aprovadas na H2C.2G

A H2C.2G não criou SQL nem autorizou migration. A futura baseline deverá
preservar as 38 colunas atuais de `patrimonio` e poderá, depois do desenho
técnico, conter estruturas vazias para:

- patrimônio;
- estados administrativos e condições operacionais;
- eventos e histórico patrimonial;
- transferências entre UVRs e, excepcionalmente, associações;
- custódia, compartilhamentos e responsabilidades;
- fotografias e documentos com metadados;
- bloqueios, alertas, saneamento de duplicidades e reversões excepcionais.

Poderão integrar como códigos estáveis: estados, condições, eventos, motivos de
baixa, tipos de transferência, categorias de documento e fotografia e
categorias de bloqueio e alerta.

Não poderão integrar: bens e números reais, placas, Renavam, séries, valores,
associações, UVRs, usuários, fotografias, documentos, transferências, baixas ou
históricos reais.

A migração futura deverá ser aditiva: preservar colunas e textos, caracterizar
`status_bem`, classificar incompletos, detectar duplicidades sem fusão
automática, mapear associação/UVR, criar marco histórico identificado, comparar
leituras antigas e novas e retirar a exclusão física cotidiana apenas quando a
compatibilidade estiver comprovada.

Permanecem pendentes nomes técnicos, colunas novas, tipos, constraints, índices,
catálogo técnico definitivo, migrations, migração do legado, detecção de
duplicidades e implementação. O detalhamento funcional aprovado está em
`ESPECIFICACAO_FUNCIONAL_PATRIMONIO_H2C2G.md`.

## Decisões funcionais aprovadas da H2C.2H — catálogo

A H2C.2H não criou SQL nem autorizou migration. As 35 decisões funcionais foram
aprovadas em 30/07/2026. A baseline futura poderá conter:

- estruturas vazias para naturezas, grupos, subgrupos, produtos/serviços,
  unidades, aliases, estados, históricos, reclassificações, substituições e
  fotografias históricas nas transações;
- códigos estáveis `RECEITA`, `DESPESA`, `PRODUTO`, `SERVICO`, estados, eventos,
  ações técnicas e regras estruturais aprovadas;
- transição aditiva com textos e identificadores coexistindo;
- fotografia da classificação nas transações e comparação de relatórios.

Não poderão integrar a baseline: catálogo real, dados dos CSVs, aliases reais,
transações, associações, UVRs, usuários ou conteúdo da tabela legado
`produtos`. Essa tabela permanece fora da baseline nova até auditoria adicional.
Permanecem pendentes os nomes e tipos técnicos finais, constraints, índices,
migrations e catálogo real.

## H2C.2I — estratégia de baseline nuclear aprovada

As 27 tabelas adicionais não entrarão automaticamente na baseline. Foi aprovado
separar:

- núcleo obrigatório;
- módulos opcionais, depois de especificação e com migrations próprias;
- legado preservado no banco atual e ausente de instalações novas.

Módulos opcionais terão migrations independentes e não serão necessários para
inicializar o núcleo. Dados reais não entrarão em migrations estruturais. O
banco atual terá plano separado; suas tabelas serão preservadas e `produtos`
continuará fora.

A baseline é exclusiva para PostgreSQL vazio e deverá parar antes do primeiro
DDL ao detectar tabelas anteriores do sistema. `IF NOT EXISTS` não será usado
para esconder divergências e nenhuma tabela será removida durante a criação da
baseline. Nenhum DDL foi definido nesta etapa.

## H2C.2J — escopo funcional final aprovado

O escopo funcional aprovado inclui controle de migrations, autenticação,
perfis/permissões/escopos,
associações/UVRs, associados, financeiro/contas, patrimônio, catálogo,
solicitações, rateios, dados bancários relacionados, Fiscalização global e
auditoria técnica.

Poderão ser estruturais códigos estáveis de módulos, ações, permissões, perfis,
escopos, estados, eventos, riscos, `RECEITA`, `DESPESA`, `PRODUTO`, `SERVICO`,
patrimônio, catálogo e fluxos `fc_*`. Usuários, entidades, documentos,
transações, catálogos operacionais e demais dados reais são proibidos.
`role`, `uvr_acesso` e tokens legados ficam fora da instalação nova.

O Modelo C foi aprovado. A baseline não contém usuários; o primeiro Administrador
Global será criado por bootstrap seguro separado. A execução falhará antes do
primeiro DDL em banco não vazio. Migrations terão identificação, ordem, versão,
checksum e imutabilidade verificável; módulos opcionais terão controle próprio.
A migração do banco atual será projeto separado. Nenhum DDL ou nome técnico
final foi definido nesta etapa.

## H2C.3A — estratégia técnica aprovada

A baseline foi aprovada em blocos: verificação de banco vazio; ledger central;
catálogos técnicos; usuários; autorização; associações/UVRs; associados;
documentos; catálogo; financeiro/rateios; patrimônio; solicitações; auditoria;
Fiscalização; dados estruturais; validações finais.

O ledger proposto registra módulo, identificador, ordem, versão, checksum,
início/conclusão, situação, duração, erro sanitizado, aplicativo e dependências.
Checksum divergente bloqueia execução. Módulos opcionais mantêm sequência própria
sem criar dependência inversa no núcleo.

As migrations 001–011 permanecem imutáveis e deverão ter seu resultado
reproduzido na ordem histórica. Banco com tabela conhecida falha antes do
primeiro DDL. Só códigos estruturais aprovados podem ser carregados; dados reais
continuam proibidos. Compatibilidade com o banco atual será projeto separado.

Foram aprovados várias migrations nucleares ordenadas, ledger central com módulo,
versão e checksum, imutabilidade, núcleo comum de documentos e compatibilidade
das 23 tabelas `fc_*`. Nenhuma migration real foi criada. A revisão técnica
independente H2C.3B é obrigatória antes da implementação.

## H2C.3B — parecer e ajustes aprovados

O parecer documental **APROVADO COM AJUSTES** foi aprovado em 31/07/2026.
Antes de qualquer DDL, o executor deverá verificar banco vazio, adquirir advisory
lock, validar manifesto/checksums, criar o ledger em bootstrap transacional e
registrar falhas sanitizadas após rollback em transação separada.

A ordem revisada separa autorização básica de escopos organizacionais, cria
auditoria mínima após usuários, antecipa documentos aos vínculos que os usam e
aplica literalmente as migrations imutáveis 001–011 sob o ledger central. Um
snapshot `fc_*` poderá existir apenas para comparação, não como segunda fonte
executável. Futuras mudanças do módulo começam em migration posterior comum.

Dados estruturais indispensáveis entram antes das validações finais e divergência
de código/checksum bloqueia execução. Permanecem bloqueadores a compatibilidade
do código de autorização, protocolo do bootstrap/ledger, ordem, normalização,
rateios e definição formal da estratégia `fc_*`. Os quatro bloqueadores impedem
a implementação até seu tratamento. A H2C.3C deverá tratar também os dez achados
de prioridade alta. Nenhuma migration foi criada e testes PostgreSQL não foram
executados.

## H2C.3C — plano físico detalhado aprovado

A H2C.3C foi aprovada documentalmente. O plano adota manifesto JSON versionado,
SHA-256 minúsculo calculado sobre UTF-8 com finais LF normalizados, duas tabelas
de controle (`schema_migrations` para sucessos imutáveis e
`schema_migration_execucoes` para tentativas), preflight de banco vazio e
advisory lock estável. O protocolo resolve conceitualmente a criação do próprio
ledger: validar destino/manifesto e, na mesma transação, criar as duas tabelas e
registrar a própria aplicação e execução; só então executar as demais migrations.
Falha após rollback é registrada sanitizada em transação separada.

A ordem proposta contém operações do executor, 13 blocos nucleares, execução
literal das 001–011, escopo contratual, dados estruturais complementares e
validação final. Dados indispensáveis entram com suas dependências; usuários e
dados reais são proibidos. As 001–011 são a única fonte executável `fc_*`; não
há snapshot concorrente.

O preflight distingue banco novo, controlado e legado/desconhecido. Os quatro
bloqueadores têm tratamento documental definido, mas continuam
bloqueando até implementação e testes. Testes PostgreSQL reais permanecem
obrigatórios. Nenhum manifesto real, migration, SQL, executor ou teste foi
criado; as 24 decisões físicas foram aprovadas. A conferência final H2C.3D é
obrigatória antes de qualquer arquivo executável.

## H2C.3D — conferência final aprovada documentalmente

Ledger, autorregistro atômico, manifesto JSON, preflight em três classes e lock
determinístico foram considerados implementáveis. A ordem permanece acíclica,
mas só pode ser congelada após ajustar patrimônio, vínculos de solicitações e
FKs de eventos do catálogo, além de expandir as matrizes nominais.

O hash normalizado UTF-8/LF da 001 é
`a8a0b4c410b6243c28946927a20567ced0dc67b435d054db24c903e28f26bebc`, diferente
do valor H2C.3C calculado sobre bytes locais; 002–011 coincidem. Nenhuma migration
foi alterada. Parecer aprovado: **C — NÃO APROVADA PARA IMPLEMENTAÇÃO**. As
lacunas de FKs, constraints, índices e DELETE deverão ser relacionadas
individualmente a migration e teste. A H2C.3E é obrigatória antes de qualquer
arquivo executável.

## H2C.3E — responsabilidade física final das migrations

A ordem documental definitiva é: M0000 preflight/lock; M0001 ledger; M0002
tipos estruturais; M0003 usuários/recuperação; M0004 autorização básica; M0005
auditoria; M0006 associações/UVRs; M0007 escopos organizacionais; M0008
documentos privados; M0009 associados/vínculos/bancos; M0010 catálogo; M0011
financeiro; M0012 as seis estruturas patrimoniais; M0013 as doze estruturas de
solicitações; H001–H011 migrations FC literais; M0025 escopo contratual; M0026
dados estruturais aprovados; M0027 validação somente leitura.

O catálogo H2C.3E atribui exatamente uma migration a cada uma das 82 tabelas,
1.103 colunas, 238 FKs, 86 UNIQUEs e 377 CHECKs. As 213 referências antigas de
índice foram reconciliadas na H2C.3E.2 em 231 índices explícitos e 132
implícitos, total físico 363. O plano anterior possui 2.190 verificações
identificadas; sua recontagem executável fica para H2C.3E.3. Nenhum elemento
fica com “migration a definir”.

O manifesto futuro permanece JSON UTF-8 versionado e fechado a propriedades
extras. Ledger e execuções são separados; preflight distingue banco novo,
controlado e desconhecido; o advisory lock deriva de
`sistema-recic3:baseline:public:v1` e usa espera máxima de 30 segundos.

O hash correto da migration 001 é
`a8a0b4c410b6243c28946927a20567ced0dc67b435d054db24c903e28f26bebc`;
002–011 permanecem conforme a matriz. Arquivos históricos continuam imutáveis.
Parecer A apenas documental; os quatro bloqueadores continuam ativos.

## H2C.3E.2 — sequência global fechada das 28 operações

Identificador e ordem global são campos diferentes. Os namespaces `M` e `H`
são independentes; dependências apontam para o identificador completo, nunca
apenas para o número. Não existem identificadores M0014–M0024: as posições
globais 14–24 são ocupadas por H001–H011, sem buraco na execução.

| Ordem | ID | Tipo | Módulo/objetivo | Dependências | Transação | Estruturas e dados | CHECKs/índices | Pré/pós-condição e teste |
|---:|---|---|---|---|---|---|---|---|
| 0 | M0000 | executor sem DDL | preflight e advisory lock | nenhuma | sessão | nenhuma | nenhum | URL presente sem ser exibida; banco classificado e lock obtido; T-M0000 |
| 1 | M0001 | nova com DDL | ledger e execuções | M0000 | sim | `schema_migrations`, `schema_migration_execucoes` | CK/PK/UQ/IX M0001 | banco novo e lock; ledger criado e autorregistrado; T-M0001 |
| 2 | M0002 | nova com DDL | referências técnicas/financeiras | M0001 | sim | módulos, ações e naturezas | CK/PK/UQ/IX M0002 | ledger íntegro; referências vazias prontas; T-M0002 |
| 3 | M0003 | nova com DDL | usuários e recuperação | M0002 | sim | `usuarios`, recuperações | CK/PK/UQ/IX M0003 | sem usuário real; estrutura de identidade pronta; T-M0003 |
| 4 | M0004 | nova com DDL | autorização básica | M0003 | sim | permissões, perfis e vínculos | CK/PK/UQ/IX M0004 | usuário estrutural disponível; RBAC vazio pronto; T-M0004 |
| 5 | M0005 | nova com DDL | auditoria técnica | M0004 | sim | `auditoria_tecnica` | CK/PK/UQ/IX M0005 | ator exclusivo válido; trilha pronta; T-M0005 |
| 6 | M0006 | nova com DDL | associações e UVRs | M0005 | sim | associações, UVRs, aliases/eventos | CK/PK/UQ/IX M0006 | auditoria pronta; cadastros vazios prontos; T-M0006 |
| 7 | M0007 | nova com DDL | escopos organizacionais | M0006 | sim | escopos global/associação/UVR | CK/PK/UQ/IX M0007 | pais e RBAC prontos; escopos vazios prontos; T-M0007 |
| 8 | M0008 | nova com DDL | documentos privados | M0007 | sim | `documentos_privados` | CK/PK/UQ/IX M0008 | autoria pronta; metadados vazios prontos; T-M0008 |
| 9 | M0009 | nova com DDL | associados, vínculos e bancos | M0008 | sim | estruturas de associados | CK/PK/UQ/IX M0009 | pais/documentos prontos; núcleo vazio pronto; T-M0009 |
| 10 | M0010 | nova com DDL | catálogo | M0009 | sim | grupos, subgrupos, itens, aliases/eventos | CK/PK/UQ/IX M0010 | auditoria pronta; catálogo vazio pronto; T-M0010 |
| 11 | M0011 | nova com DDL | financeiro | M0010 | sim | contas, transações, itens/rateios/eventos | CK/PK/UQ/IX M0011 | catálogo/UVR prontos; financeiro vazio pronto; T-M0011 |
| 12 | M0012 | nova com DDL | seis estruturas patrimoniais | M0011 | sim | patrimônio, identificadores, vínculos, eventos, documentos, bloqueios | CK/PK/UQ/IX M0012 | documentos/associações prontos; patrimônio vazio pronto; T-M0012 |
| 13 | M0013 | nova com DDL | doze estruturas de solicitações | M0012 | sim | fluxo completo de solicitações | CK/PK/UQ/IX M0013 | alvos prontos; fluxo vazio pronto; T-M0013 |
| 14 | H001 | histórica com DDL | empresas | M0013 | literal | migration 001 | CK/PK/UQ/IX H001 | pré-condições FC; equivalência literal; TH-001 |
| 15 | H002 | histórica com DDL | servidores | H001 | literal | migration 002 | CK/PK/UQ/IX H002 | equivalência literal; TH-002 |
| 16 | H003 | histórica com DDL | contratos | H002 | literal | migration 003 | CK/PK/UQ/IX H003 | equivalência literal; TH-003 |
| 17 | H004 | histórica com DDL | aditivos | H003 | literal | migration 004 | CK/PK/UQ/IX H004 | equivalência literal; TH-004 |
| 18 | H005 | histórica com DDL | documentos FC | H004 | literal | migration 005 | CK/PK/UQ/IX H005 | equivalência literal; TH-005 |
| 19 | H006 | histórica com DDL | planilhas | H005 | literal | migration 006 | CK/PK/UQ/IX H006 | equivalência literal; TH-006 |
| 20 | H007 | histórica com DDL | ativos contratuais | H006 | literal | migration 007 | CK/PK/UQ/IX H007 | equivalência literal; TH-007 |
| 21 | H008 | histórica com DDL | fiscalizações/ocorrências | H007 | literal | migration 008 | CK/PK/UQ/IX H008 | equivalência literal; TH-008 |
| 22 | H009 | histórica com DDL | eventos de fiscalização | H008 | literal | migration 009 | CK/PK/UQ/IX H009 | equivalência literal; TH-009 |
| 23 | H010 | histórica com DDL | medições | H009 | literal | migration 010 | CK/PK/UQ/IX H010 | equivalência literal; TH-010 |
| 24 | H011 | histórica com DDL | atestes | H010 | literal | migration 011 | CK/PK/UQ/IX H011 | equivalência literal; TH-011 |
| 25 | M0025 | nova com DDL | escopo contratual | H011, M0007 | sim | `fc_contrato_escopos` | CK/PK/UQ/IX M0025 | FC e escopos prontos; vínculo vazio pronto; T-M0025 |
| 26 | M0026 | dados estruturais | referências aprovadas | M0025 | sim | apenas códigos estruturais aprovados | sem novo DDL | schema íntegro; carga idempotente e sem dados reais; T-M0026 |
| 27 | M0027 | validação somente leitura | conferência final | M0026 | não altera | nenhuma | confere todos | ledger completo, objetos/contagens conformes; T-M0027 |

Classificação: 1 operação sem DDL, 14 migrations novas com DDL, 11 migrations
históricas com DDL, 1 carga estrutural e 1 validação somente leitura: **28**.
Cada uma cobre as tabelas, CHECKs e índices que levam seu identificador nos
catálogos; nenhum elemento possui migration indefinida.

### Ledger e falhas

M0000 não cria tabela nem grava tentativa. Ele valida configuração sem revelar
segredo, classifica o banco e obtém o advisory lock; falha anterior ao ledger é
somente saída sanitizada do executor. M0001 é a primeira migration física: cria
`schema_migrations` e `schema_migration_execucoes` e registra a própria
aplicação na mesma transação. Sucesso confirma DDL e autorregistro juntos;
falha executa rollback integral. Após o ledger existir, cada tentativa é
registrada de modo sanitizado em transação própria e a aplicação bem-sucedida
em transação atômica com a migration. Testes cobrem primeira instalação,
rollback, repetição, hash divergente e concorrência.

### Hashes históricos UTF-8/LF confirmados documentalmente

| ID | Arquivo | SHA-256 |
|---|---|---|
| H001 | `001_criar_fc_empresas.sql` | `a8a0b4c410b6243c28946927a20567ced0dc67b435d054db24c903e28f26bebc` |
| H002 | `002_criar_fc_servidores.sql` | `012af8ddd2e04cae607a93e6d09d6d2eddcd1b6cabe7ea751906c7496da72cd5` |
| H003 | `003_criar_fc_contratos.sql` | `815313c2f03402564127ad494fffdf59baeee26e3cd4262ca7106dbaaeca273d` |
| H004 | `004_criar_fc_aditivos.sql` | `88961a6d0cbafde065bbd598425035bef0b2a1b9002126b02cb3d006036e821d` |
| H005 | `005_criar_fc_documentos.sql` | `b57f893eced87931922ddaa5c36f51b6ee4e5601cfc95c3616de0a225994c362` |
| H006 | `006_criar_fc_planilhas_orcamentarias.sql` | `03c059dfdbeaff80ad8f0282956217c5b92f1b911df7de529640d2ec3f16a7ad` |
| H007 | `007_criar_fc_ativos_contratuais.sql` | `b0d5f732b2aa9234d7cb88a5be6f467ade2a9e19bb82aac596d3e39ef1dd8d19` |
| H008 | `008_criar_fc_fiscalizacoes_ocorrencias.sql` | `f8f88fe1fe9152cdc243db45243701f953b4367def0b3f1ac62d47526e733531` |
| H009 | `009_criar_fc_fiscalizacao_eventos.sql` | `2e88c765b5981e8edf97616ee656bcb08982c79b953a7f612de2784e6ceffa46` |
| H010 | `010_criar_fc_medicoes.sql` | `ac4c38b2beafa4bf7a8b0f614030898e3ac8cbb7ef2b33bcae10b067fae3d34d` |
| H011 | `011_criar_fc_atestes.sql` | `6521ce402bd41c2520901799fa29ae12c205333c23bc65f48b0ebffc84f65089` |

Os arquivos permanecem imutáveis; hashes são minúsculos e o manifesto futuro
usará bytes UTF-8 com LF. Parecer restrito à H2C.3E.2: **A — CHECKS, ÍNDICES,
MEDIÇÃO PATRIMONIAL E SEQUÊNCIA COMPLETADOS**. A H2C.3E integral continua
pendente da H2C.3E.3.

## H2C.3E.3 — testes e autorização futura das operações

As 28 operações têm caso `TMIG` individual. M0000 cobre banco
novo/controlado/desconhecido, lock livre/ocupado, timeout de 30 segundos e zero
DDL. M0001 cobre ledger, autorregistro, reaplicação e rollback. M0002–M0013 e
M0025 cobrem dependência, aplicação, reaplicação, falha, rollback, pós-condições
e ausência de dados reais. H001–H011 cobrem hash UTF-8/LF, literalidade, ordem,
imutabilidade, integração com `usuarios` e fonte executável única. M0026 cobre
carga estrutural idempotente sem usuário/dado operacional; M0027 comprova
somente leitura e falha segura.

Manifesto, checksum, dependências, arquivo ausente/extra, ordem duplicada,
concorrência e mensagem sanitizada pertencem ao modelo T-MIGRATION. Toda
operação mutável usa transação e rollback integral; M0000 e M0027 explicitam
ausência de mutação. A autorização futura exige encerrar os quatro bloqueadores
e aprovar a revisão independente H2C.3F. Nenhuma migration, manifesto ou
executor foi criado nesta etapa.

Recontagem normativa: 82 tabelas, 1.103 colunas, 238 FKs, 377 CHECKs, 363
índices, 28 operações e 2.341 identificadores em nove modelos. Números anteriores
são históricos. Parecer integral H2C.3E: **A — ESPECIFICAÇÃO FÍSICA COMPLETA E
APTA PARA REVISÃO FINAL DE AUTORIZAÇÃO**, sem autorização de implementação.

Os ciclos de vida das 82 tabelas fecham, sem sobreposição de categoria, em 19
A, 8 B, 15 C, 7 D, 11 E, 12 F, 7 G, 2 H e 1 I; o catálogo traz cada tabela e
sua migration responsável.

## Autorização humana para H2C.4A — 03/08/2026

**DOCUMENTAÇÃO APROVADA.** H2C.3E e H2C.3F estão encerradas; a H2C.3F.2 emitiu
**A — RECOMENDADA A AUTORIZAÇÃO HUMANA PARA INÍCIO DA IMPLEMENTAÇÃO
CONTROLADA**.

**AUTORIZAÇÃO PARA IMPLEMENTAÇÃO CONTROLADA.** O usuário autorizou
expressamente o início posterior da **H2C.4A — Infraestrutura Controlada de
Migrations**.

O primeiro incremento autorizado limita-se a manifesto real, parser/validador,
normalização UTF-8/LF, SHA-256, preflight, advisory lock, timeout, ledger,
histórico de execuções, M0000, M0001, testes unitários e PostgreSQL efêmero.
Somente `schema_migrations` e `schema_migration_execucoes` poderão ser criadas.

Limites: nenhuma tabela funcional, H001–H011 não executadas neste incremento,
nenhum banco/dado real, deploy, mudança ou merge na `main`, PR automático,
remoção de legado, bootstrap ou alteração de `role`/`uvr_acesso`.

As 28 operações globais permanecem ordenadas; M0014–M0024 não existem como
identificadores e as ordens 14–24 pertencem a H001–H011. M0001 cria e
autorregistra atomicamente o ledger. H001–H011 são imutáveis; H001 usa hash
UTF-8/LF/SHA-256 minúsculo
`a8a0b4c410b6243c28946927a20567ced0dc67b435d054db24c903e28f26bebc`.

**IMPLEMENTAÇÃO AINDA NÃO INICIADA.** B1, B2, B3 e B4 continuam ativos. Nenhum
manifesto, executor, migration, SQL, bootstrap, teste ou PostgreSQL foi criado
ou executado nesta tarefa documental.
