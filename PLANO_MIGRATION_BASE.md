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
- aguardar a H2C.2G.1 para decidir formalmente o escopo dessas 27 tabelas na
  baseline;
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
