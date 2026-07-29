# Plano da futura migration-base

## Finalidade

A futura migration-base deverá montar o schema completo do `sistema-recic3` em
um **PostgreSQL vazio**. Ela não servirá para atualizar, reparar ou conferir o
banco atual e nunca deverá ser aplicada sobre banco já utilizado.

Este plano é resultado da Etapa H2C.1. Nenhum SQL foi criado ou executado.

## Decisão atual

Ainda não é seguro implementar a baseline. O repositório não contém o DDL
completo de `patrimonio` e `grupos_atividade`, pressupõe `id_grupo` em duas
tabelas e possui definições concorrentes de `solicitacoes_alteracao`.

Classificação: **informação insuficiente sem futura exportação somente do
schema do banco atual**.

## Estratégia recomendada para H2C.2

Depois de resolver as lacunas:

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

A ordem final depende da confirmação do schema legado:

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

Os itens 2 e 3 são apenas uma posição provável: a ordem definitiva de
`grupos_atividade`, `subgrupos`, `produtos_servicos` e `patrimonio` não pode ser
fixada antes de conhecer suas FKs reais. UVR e associação permanecem textos no
DDL conhecido; não há tabelas centrais correspondentes comprovadas.

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

- schema legado incompleto no Git;
- drift provocado por scripts manuais;
- `IF NOT EXISTS` ter ocultado diferenças;
- definições concorrentes de `solicitacoes_alteracao`;
- grupos e `id_grupo` sem DDL;
- tipos e constraints desconhecidos de `patrimonio`;
- exclusões físicas e duas cascatas legadas;
- `TIMESTAMP` legado versus `TIMESTAMPTZ` no módulo;
- credenciais históricas em scripts antigos;
- dois CSVs de catálogo com conteúdo diferente;
- ausência de ledger até H2D.

## Decisões pendentes

1. obter e auditar exportação somente do schema;
2. decidir a definição correta de `solicitacoes_alteracao`;
3. confirmar `grupos_atividade`, `id_grupo` e suas FKs/UQs;
4. confirmar todo o DDL de `patrimonio`;
5. decidir se o catálogo CSV é opcional ou obrigatório;
6. decidir se `SERIAL`/`BIGSERIAL` serão preservados literalmente;
7. decidir o tamanho dos blocos transacionais;
8. definir versão/checksum esperado pela H2D;
9. avaliar índices recomendados separadamente;
10. remover ou isolar scripts com credenciais históricas em etapa própria.

## Critério para iniciar H2C.2

H2C.2 só deve começar quando:

- as lacunas estruturais estiverem resolvidas;
- a definição final tiver revisão técnica;
- houver PostgreSQL vazio e descartável para teste;
- o banco atual estiver fora do alcance do executor;
- o formato de versão esperado pela H2D estiver decidido.
