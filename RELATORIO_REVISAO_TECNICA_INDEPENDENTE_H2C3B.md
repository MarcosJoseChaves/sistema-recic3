# Etapa H2C.3B — Relatório de revisão técnica independente

**Situação: APROVADA DOCUMENTALMENTE em 31/07/2026.**

## 1. Objetivo, método e limites

Esta revisão trata a H2C.3A como proposta a verificar, sem reabrir decisões por
preferência. Foram confrontados documentos H2C.2/H2C.3A, mapa do schema, código,
autenticação/autorização, testes, configuração e migrations 001–011. Não houve
acesso a banco, dump, `.env`, `DATABASE_URL`, dados reais ou serviço externo;
nenhum SQL, migration, código ou bootstrap foi criado ou executado.

Classificações usadas: CONFIRMADA, CONFIRMADA COM RESSALVA, AJUSTE NECESSÁRIO,
BLOQUEIO TÉCNICO e DECISÃO ADIÁVEL.

## 2. Parecer independente final

**APROVADO COM AJUSTES.** Os 24 achados, quatro bloqueadores, ajustes obrigatórios
e 20 decisões foram aprovados. O modelo pode avançar somente à especificação
física H2C.3C. Este parecer não autoriza migrations ou implementação.

Bloqueadores antes da implementação:

1. o código atual ainda depende de `usuarios.role` e `usuarios.uvr_acesso`, que
   não existirão na instalação nova;
2. o executor de migrations ainda não resolve formalmente o bootstrap do próprio
   ledger, lock, falha após rollback e manifesto/checksum;
3. bootstrap do primeiro administrador precisa de ator técnico auditável,
   concorrência protegida e segredo fora de argumentos/logs;
4. a estratégia executável das 23 tabelas `fc_*` precisa escolher uma única
   fonte de verdade.

Nenhum bloqueador exige mudar as decisões funcionais. O tratamento aprovado é:
encerrá-los na especificação física ou antes da implementação; definir solução e
teste para todo achado alto; resolver médios na especificação física; manter
baixos e informativos como riscos documentados quando não exigirem mudança.

## 2.1 Decisões e ajustes aprovados

Foram aprovados: IDs híbridos e `BY DEFAULT AS IDENTITY`; códigos
`^[A-Z][A-Z0-9_]*$`; percentuais `NUMERIC(9,6)` na escala 0–100 para alocações;
ausência de dependência obrigatória de `btree_gist`; períodos protegidos por
CHECK, índices, transações, locks e servidor; ator técnico `BOOTSTRAP` e prompt
oculto; escopos de objeto por módulo; alocações para uma ou várias UVRs com soma
validada na conclusão; triggers apenas quando indispensáveis; documentos comuns
com links específicos; `fc_documentos` independente; auditoria com referência
lógica investigativa; advisory lock no executor; 001–011 como única fonte
executável das 23 `fc_*`; dados estruturais por dependência; suíte PostgreSQL
obrigatória; e parecer limitado à especificação física.

A autorização legada será substituída antes do primeiro start da instalação
nova. O ledger será criado em transação própria, com manifesto/checksum, sucesso
pós-commit e falha sanitizada pós-rollback. O bootstrap não será usuário fictício.

## 2.2 Implementação pendente

Executor, lock, manifesto, ledger, bootstrap, autorização, migrations, tabelas,
constraints, índices, SQL, testes PostgreSQL e demais mudanças ainda não foram
implementados. A H2C.3C deverá demonstrar o tratamento de cada bloqueador e
achado alto, permanecendo inicialmente documental.

## 3. Revisão das 30 decisões H2C.3A

| Decisão | Classificação | Conclusão independente |
|---|---|---|
| DT-01 híbrido de IDs | CONFIRMADA | tipos de FK permanecem coerentes |
| DT-02 IDENTITY | CONFIRMADA | `BY DEFAULT` favorece importação controlada |
| DT-03 TIMESTAMPTZ/DATE | CONFIRMADA | separar instante de data civil |
| DT-04 nomes portugueses | CONFIRMADA COM RESSALVA | glossário e limite de 63 bytes |
| DT-05 TEXT para códigos | CONFIRMADA COM RESSALVA | padrão formal de caracteres obrigatório |
| DT-06 estados híbridos | CONFIRMADA COM RESSALVA | critério CHECK versus catálogo deve ser nominal |
| DT-07 NUMERIC(18,2) | CONFIRMADA COM RESSALVA | preço unitário/cálculo podem exigir escala 8 |
| DT-08 NUMERIC(24,8) | CONFIRMADA | coerente com planilhas/medições `fc_*` |
| DT-09 versão + lock | CONFIRMADA | atualização atômica deve conferir versão |
| DT-10 normalização sem unaccent | CONFIRMADA COM RESSALVA | evitar divergência entre aplicação e banco |
| DT-11 username/e-mail normalizados | AJUSTE NECESSÁRIO | queries atuais são literais; código futuro deve normalizar |
| DT-12 token em tabela/hash | CONFIRMADA | token puro nunca persistido |
| DT-13 escopos híbridos | CONFIRMADA | objeto exige tabela do módulo ou referência lógica controlada |
| DT-14 validade de perfil | CONFIRMADA | período e revogação auditados |
| DT-15 principal único parcial | CONFIRMADA | PostgreSQL garante unicidade ativa |
| DT-16 sem sobreposição | CONFIRMADA COM RESSALVA | evitar `btree_gist` obrigatório; lock transacional |
| DT-17 conta bancária principal | CONFIRMADA COM RESSALVA | uma por finalidade; não impor unicidade externa inventada |
| DT-18 toda UVR via alocação | CONFIRMADA | elimina fonte dupla de verdade |
| DT-19 snapshot financeiro JSONB | CONFIRMADA COM RESSALVA | versão, limite e colunas relacionais principais |
| DT-20 snapshots de solicitação | CONFIRMADA COM RESSALVA | validar schema por tipo e tamanho |
| DT-21 documentos núcleo+vínculos | CONFIRMADA | nunca usar vínculo polimórfico sem FK |
| DT-22 auditoria append-only | CONFIRMADA COM RESSALVA | referência de objeto é lógica; retenção futura |
| DT-23 RESTRICT histórico | CONFIRMADA | CASCADE só para rascunho técnico comprovado |
| DT-24 ledger central | AJUSTE NECESSÁRIO | resolver criação própria, lock, rollback e manifesto |
| DT-25 dados estruturais versionados | CONFIRMADA COM RESSALVA | divergência deve falhar, não ser sobrescrita |
| DT-26 preservar `fc_*` | AJUSTE NECESSÁRIO | definir execução literal versus snapshot |
| DT-27 migrations por blocos | CONFIRMADA | granularidade por dependência |
| DT-28 CPF VARCHAR/TEXT | CONFIRMADA | regex, vazio→NULL e unique parcial |
| DT-29 INET/UUID | CONFIRMADA | retenção/minimização ainda necessárias |
| DT-30 revisão independente | CONFIRMADA | cumprida por esta etapa |

Resultado: 17 confirmadas, 10 confirmadas com ressalva e 3 ajustes necessários.
Os bloqueios de sequência e compatibilidade derivam desses ajustes.

## 4. Convenções e glossário

Português, plural, `snake_case`, `id` e `*_id` são consistentes. PostgreSQL
limita identificadores a 63 bytes; nomes de constraints/índices devem usar
abreviações documentadas sem perder unicidade.

| Termo | Significado reservado |
|---|---|
| conta de usuário | autenticação e identidade de acesso |
| conta bancária do associado | destino financeiro pessoal e sensível |
| conta financeira | caixa/banco institucional da associação |
| alocação por UVR | distribuição gerencial de uma transação; “rateio” é nome de negócio |
| documento privado | metadado comum de arquivo armazenado fora do PostgreSQL |
| fotografia | snapshot imutável de dados ou imagem privada, conforme contexto explícito |
| anexo | vínculo de um documento a um domínio |
| evento | fato funcional imutável de uma entidade |
| auditoria técnica | trilha transversal de ação/resultado, não evento de negócio |

Códigos estruturais devem seguir `^[A-Z][A-Z0-9_]*$`; nomes oficiais preservam
acentos. Estados pequenos e fechados usam CHECK; listas evolutivas, FK.

## 5. Identificadores e tipos

O modelo híbrido permanece adequado. Flask-Login serializa o ID como texto e o
converte no carregador, sem exigir que todas as PKs tenham o mesmo tipo. Join
entre autoria INTEGER e entidades BIGINT não causa conversão porque a FK de
autoria continua INTEGER.

`BY DEFAULT AS IDENTITY` permite preservar IDs em importação controlada, mas a
sequência deve ser reposicionada e validada depois da carga. URLs e formulários
devem validar limites de BIGINT sem converter para ponto flutuante.

Tipos confirmados:

- dinheiro liquidado: `NUMERIC(18,2)`;
- preço/taxa de cálculo e quantidade: escala maior, como `NUMERIC(24,8)`;
- percentual proposto: `NUMERIC(9,6)`, entre 0 e 100;
- CPF: `VARCHAR(11)` ou TEXT, regex de 11 dígitos, vazio convertido em NULL;
- checksum: `VARCHAR(64)`, minúsculo, 64 hexadecimais;
- token/hash: texto hexadecimal ou BYTEA conforme biblioteca, nunca token puro;
- TIMESTAMPTZ para instante; DATE para competência/validade civil;
- JSONB com versão de schema e limite definido na aplicação.

## 6. Campos comuns e concorrência

| Tipo | Campos adequados | Campos a evitar |
|---|---|---|
| entidade mutável | criação/atualização, autores, estado, versão | `ativo` duplicando estado |
| evento imutável | criado_em, autor/tipo/dados | atualizado_em e versão |
| vínculo temporal | início/fim, autores, motivo/estado | flag ativa se `fim_em` bastar |
| catálogo | código, nome, normalizado, estado, versão | inativação duplicada |
| associação pura | FKs e criação; período se necessário | auditoria excessiva |
| auditoria | request, instante, ator, ação, resultado | atualização cotidiana |

Atualização otimista deve condicionar a escrita à versão observada e incrementar
na mesma instrução. Fluxos de soma, última administração e mudança de estado
usam lock transacional adicional.

## 7. Ciclos e ordem de FKs

| Ciclo potencial | É real? | Tratamento |
|---|---|---|
| usuário↔atribuição/perfil | não | usuário e perfis precedem atribuição |
| usuário↔auditoria | bootstrap lógico | auditoria aceita `ator_tipo=BOOTSTRAP` e usuário nulo somente nesse caso |
| associação↔UVR | não se associação não guardar UVR principal | principalidade fica no vínculo/UVR |
| associado↔vínculos principais | sim se houver FK direta duplicada | fonte de verdade são vínculos; não duplicar FK principal na linha |
| transação↔estorno | autorreferência | FK nula criada junto; sem ciclo de criação |
| catálogo↔substituição | autorreferência/evento | evento posterior, sem impedir criação |
| patrimônio↔transferência | não | evento/vínculo depende do patrimônio |
| solicitação↔objeto | polimórfico | referência lógica + link específico por domínio |
| documento↔vínculos | não | documento primeiro, links depois |
| `fc_*`↔usuários | não | usuários antes de 001–011 |

O problema concreto é a ordem: comprovantes bancários e fotografias podem
referenciar documentos, portanto `documentos_privados` deve anteceder as tabelas
que criam essas FKs. Autorização deve ser dividida em base antes das organizações
e escopos depois delas.

## 8. Autenticação e bootstrap

Estrutura de usuário e recuperação estão consistentes, mas o aplicativo atual
consulta `role` e `uvr_acesso` e compara username literalmente. A baseline futura
sem essas colunas só poderá iniciar depois que o novo serviço de autorização e a
normalização de login estiverem implementados. Não se recomenda recolocar os
campos legados na instalação nova.

Bootstrap recomendado:

- comando separado, modo interativo com entrada oculta; segredo nunca em argv,
  log ou arquivo;
- advisory lock e transação; falhar se já houver Administrador Global ativo;
- criar usuário exigindo troca de senha e atribuição protegida;
- auditoria com ator técnico `BOOTSTRAP`, request ID e usuário resultante;
- recuperação emergencial como comando distinto, também auditado e protegido.

Autor nulo genérico não é aceitável; a exceção deve ser limitada por CHECK ao
evento técnico de bootstrap.

## 9. Perfis, escopos, associações e UVRs

O modelo híbrido é confirmado. Escopos global, associação e UVR têm FKs reais;
objetos usam link específico do módulo. Tabela `(tipo, id)` não concede acesso
por si só. Autoelevação, último admin, validade e pais ativos exigem transação.

Recomendações mínimas:

- código da associação globalmente único;
- código da UVR globalmente único para integração; nome único normalizado dentro
  da associação;
- aliases e nomes oficiais compartilham o mesmo namespace de resolução. A forma
  mais íntegra é registrar também a chave oficial em uma tabela de chaves de
  busca, evitando colisão entre duas tabelas;
- UVR ativa exige associação ativa, verificada transacionalmente;
- troca de associação responsável encerra vínculo anterior e cria novo evento.

## 10. Associados e dados bancários

CPF parcial único permite várias pessoas sem CPF. `PENDENTE_REGULARIZACAO` não
duplica estado administrativo. Uma associação/UVR principal ativa é garantida
por índice parcial; secundária na mesma associação exige validação com lock.

Evita-se dependência obrigatória de `btree_gist`. Sobreposição é impedida por
lock na entidade pai, consulta do intervalo e escrita na mesma transação. Uma
constraint de exclusão pode ser opção futura se a extensão for autorizada.

Dados bancários:

- conta principal única por associado/finalidade via índice parcial;
- não impor unicidade global de PIX ou conta sem regra institucional comprovada;
- duplicidade interna ativa pode usar fingerprint normalizado por associado;
- conta conjunta/titular diferente permanece possível;
- agência, conta, PIX, titular e documento ficam fora de logs e auditoria JSON;
- comprovante referencia documento privado por tabela de vínculo/FK.

## 11. Catálogo e normalização

Hierarquia, IDs e fotografia histórica são consistentes. Colisão entre nome
oficial e alias não pode ser garantida por dois UNIQUE separados; recomenda-se
uma tabela única de chaves normalizadas por tipo/escopo, apontando para a entidade
do domínio por link específico, ou validação transacional equivalente.

A aplicação gera `nome_normalizado`, mas o banco deve rejeitar vazio e garantir
unicidade. Sem uma função imutável comum, não há como CHECK provar que oficial e
normalizado correspondem. Auditoria/teste de consistência é obrigatório. Item
inativo permanece referenciado e não pode ser escolhido em novo lançamento.

## 12. Financeiro e rateios

Uma transação geral pode permanecer sem alocação enquanto rascunho. Ao concluir,
se houver dimensão UVR, a tabela de alocação é obrigatória inclusive para 100% em
uma UVR. Não se usa `uvr_id` direto no cabeçalho.

| Regra | Mecanismo |
|---|---|
| valor/percentual positivos e modalidade exclusiva | CHECK |
| uma UVR por transação ativa | UNIQUE parcial |
| UVR pertence à associação | FK + validação transacional |
| soma fecha o total/100% | função de serviço com lock na transação |
| residual de arredondamento | regra explícita para última alocação, auditada |
| imutabilidade após conclusão | estado + autorização transacional |
| estorno parcial/total | nova transação ligada, nunca reescrita |

Trigger não é necessário inicialmente. Conta, catálogo e UVR devem estar ativos
na conclusão; referências históricas continuam válidas depois da inativação.

## 13. Patrimônio

O desenho é consistente, mas sete tabelas podem ser reduzidas fisicamente na
especificação final: documentos e fotografias podem compartilhar o núcleo e uma
tabela de vínculos com categoria; transferências/compartilhamentos podem ser
tipos de `patrimonio_vinculos` mais eventos, desde que consultas permaneçam
claras. Não é bloqueador.

Unicidades condicionais são viáveis com índices parciais. Série por
fabricante+classe requer valores normalizados e permite nulos. Transferência,
baixa e reversão são transacionais e append-only. As 38 colunas atuais só entram
no projeto de migração, não na baseline nova.

## 14. Solicitações de alteração

Estados e quatro snapshots JSONB são coerentes. `APROVADA` não é `APLICADA`;
aplicação tem tentativa própria, versão esperada e idempotency key. Reprocessar
falha cria nova tentativa, não edita a anterior.

Referência polimórfica não terá FK genérica. O cabeçalho pode guardar módulo,
tipo e ID lógico para busca, mas cada domínio implementado cria link específico
com FK. Validador por tipo limita campos e tamanho dos JSONB. Visibilidade de
mensagens é código controlado; segregação entre solicitante/aprovador/aplicador é
testada no serviço.

## 15. Documentos privados e auditoria

O núcleo comum de metadados é confirmado: chave opaca única, provedor, hash,
tamanho, MIME verificado, nome seguro, estado, versão e auditoria. Links por
domínio preservam FKs. `fc_documentos` permanece independente inicialmente;
unificação automática criaria duas fontes e risco de acesso.

Download gera URL temporária somente após autorização; URL nunca é persistida ou
auditada. Substituição cria versão/novo documento e vínculo; remoção física segue
retenção e não apaga metadado histórico.

Auditoria central aceita referência lógica `(modulo, tipo_objeto, objeto_id)` sem
FK polimórfica, porque eventos funcionais específicos preservam integridade. O
JSONB deve ser pequeno, versionado e sanitizado: sem senha, token, CPF integral,
dado bancário ou URL assinada. Append-only exige papel de banco futuro sem
UPDATE/DELETE cotidiano, além da aplicação.

## 16. Revisão das 23 tabelas `fc_*`

As 23 PKs são `BIGSERIAL`; autoria é `INTEGER REFERENCES usuarios(id)`. As FKs,
CHECKs, UNIQUEs e índices das migrations são compatíveis com o modelo híbrido.
As FKs não declaram CASCADE, portanto preservam comportamento `NO ACTION`.

Achados:

- 005 e 006 repetem de forma idempotente o índice composto de aditivos;
- índices de autoria não são necessários se usuários nunca forem apagados;
- vários estados são CHECKs fechados, aceitáveis para fluxos já validados;
- `fc_documentos` é específico de contrato e não deve virar metadado central por
  alteração automática;
- o código atual protege o módulo como admin; escopos por contrato/objeto exigem
  implementação futura antes de substituir essa regra;
- não foi encontrada razão para redesenhar as 23 tabelas.

| Tabela | Migration | Revisão independente |
|---|---|---|
| `fc_empresas` | 001 | PK/FKs/UNIQUE/CHECK/índice confirmados |
| `fc_servidores` | 002 | PK/FKs/matrícula/índice confirmados |
| `fc_contratos` | 003 | empresa, valor, vigência e situação confirmados |
| `fc_contrato_responsaveis` | 003 | vínculos temporais e únicos parciais confirmados |
| `fc_aditivos` | 004 | termo/contrato, tipos, valores e índices confirmados |
| `fc_documentos` | 005 | vínculo composto, chave, hash e índices confirmados |
| `fc_planilhas_orcamentarias` | 006 | versão, vigência e vínculo de aditivo confirmados |
| `fc_planilha_itens` | 006 | quantidades/valores `24,8` e índices confirmados |
| `fc_ativos_contratuais` | 007 | identificadores condicionais e estados confirmados |
| `fc_ativo_vinculos` | 007 | contrato, período e vínculo ativo confirmados |
| `fc_fiscalizacoes` | 008 | contrato/servidor, horários e estados confirmados |
| `fc_ocorrencias` | 008 | FK composta, prazos, notificação e estados confirmados |
| `fc_ocorrencia_acompanhamentos` | 008 | evento temporal e estados confirmados |
| `fc_fiscalizacao_eventos` | 009 | transições e justificativa append-only confirmadas |
| `fc_medicoes` | 010 | versão, competência, totais e aprovação confirmados |
| `fc_medicao_itens` | 010 | planilha, quantidades, preço e excedente confirmados |
| `fc_medicao_ajustes` | 010 | fiscalização/ocorrência, tipo e valor confirmados |
| `fc_medicao_documentos` | 010 | documento/categoria e único parcial confirmados |
| `fc_medicao_eventos` | 010 | transições e fotografia dos totais confirmadas |
| `fc_atestes` | 011 | medição, atestador, valor e encaminhamento confirmados |
| `fc_ateste_notas_fiscais` | 011 | documento, nota, valor e duplicidade confirmados |
| `fc_ateste_documentos` | 011 | vínculo/categoria e único parcial confirmados |
| `fc_ateste_eventos` | 011 | transições, justificativa e valores confirmados |

Estratégia independente recomendada: executor central aplica literalmente os
arquivos imutáveis 001–011, na ordem, com checksum e registro de módulo `FC`.
Um snapshot estrutural pode existir apenas como artefato de comparação/teste,
nunca como segunda fonte executável. Futuras mudanças usam migration 012+ comum
a instalações novas e existentes.

## 17. Controle e ordem das migrations

O ledger não pode registrar sua criação antes de existir. Recomenda-se:

1. executor verifica banco vazio e adquire advisory lock antes de qualquer DDL;
2. manifesto versionado contém IDs, módulos, ordem, dependências e checksums;
3. migration bootstrap cria somente o ledger em transação própria;
4. após commit, o executor registra o próprio bootstrap com checksum conhecido;
5. cada migration seguinte usa transação própria e registro `EM_EXECUCAO`;
6. em falha, rollback e registro sanitizado em transação separada;
7. checksum/ordem/dependência divergentes bloqueiam execução;
8. lock é liberado ao final; logs não contêm SQL completo nem segredos.

Ordem revisada:

0. preflight de banco vazio, manifesto e lock;
1. ledger/bootstrap técnico;
2. catálogos técnicos mínimos;
3. usuários e recuperação;
4. autorização básica, sem escopos organizacionais;
5. auditoria técnica mínima, apta ao ator `BOOTSTRAP`;
6. associações, UVRs e aliases;
7. escopos de associação/UVR;
8. documentos privados;
9. associados, vínculos e dados bancários;
10. catálogo operacional vazio;
11. contas, financeiro e alocações;
12. patrimônio;
13. solicitações;
14. migrations 001–011 de Fiscalização;
15. dados estruturais indispensáveis e demais aprovados;
16. validações/checksums finais.

## 18. Dados estruturais, DELETE e índices

Indispensáveis antes da aplicação: módulos, ações, permissões, perfis protegidos,
tipos de escopo, estados dos fluxos e códigos `RECEITA`, `DESPESA`, `PRODUTO`,
`SERVICO`. Descrições podem evoluir; códigos não. Carga divergente falha.

Políticas:

- RESTRICT/NO ACTION: usuários, perfis referenciados, organizações, associados,
  catálogo usado, financeiro, patrimônio, solicitações, documentos e `fc_*`;
- CASCADE: apenas dependência técnica de rascunho nunca ativado, comprovadamente
  sem história própria;
- SET NULL: ator opcional somente quando snapshot mantém compreensão, incluindo
  exceção técnica de bootstrap;
- eventos e auditoria: nenhuma exclusão cotidiana.

Todo FK usado em filtro/join frequente recebe índice; não se cria índice apenas
por existir FK. UNIQUE/PK já fornecem índice. Estado isolado costuma ter baixa
seletividade; combinar com associação, data ou fila. Auditoria usa request ID,
objeto+data e usuário+data; retenção/particionamento são adiáveis.

## 19. Segurança, privacidade e testabilidade

Campos candidatos a proteção adicional futura: CPF/RG, contatos/endereço, dados
bancários, documentos/fotos e snapshots com dados pessoais. Mascaramento e
permissões de ver/exportar são separados. Metadados, índices e JSONB também são
sensíveis; minimização vale para todos.

| Área | Testes necessários |
|---|---|
| executor | unitário de manifesto/checksum; integração PostgreSQL vazio/não vazio, ordem, lock, rollback |
| bootstrap | integração concorrente, segredo ausente de argv/log, último admin |
| usuários | unitário/integração de normalização, UNIQUE, token hash e estado |
| escopos | autorização por global/associação/UVR/objeto e pais inativos |
| associados | CPF nulo/duplicado, principais, intervalos e exclusão de rascunho |
| bancário | principal por finalidade, mascaramento, logs e documentos |
| catálogo | colisão nome/alias, códigos, inatividade e snapshot |
| financeiro | Decimal, rateio 1/N, arredondamento, conclusão, estorno e rollback |
| patrimônio | identificadores, transferência, baixa, reversão e histórico |
| solicitações | transições, quatro snapshots, concorrência, idempotência e segregação |
| documentos | conteúdo, chave, autorização, URL temporária, retenção e compensação |
| auditoria | append-only, sanitização, bootstrap e crescimento |
| `fc_*` | equivalência 001–011, 23 tabelas, constraints/índices e suíte existente |
| baseline | ausência de dados reais e homologação humana final |

## 20. Condições aprovadas para avançar

- incorporar a matriz de achados e as 20 decisões aprovadas;
- fechar compatibilidade do novo `usuarios` com o código de autorização;
- aprovar a ordem revisada e o protocolo do ledger;
- confirmar execução literal 001–011 como fonte `fc_*`;
- definir ator `BOOTSTRAP`, padrão de códigos e percentuais;
- produzir especificação física nominal e plano de testes;
- manter revisão de segurança antes de qualquer migration.

Melhorias adiáveis: particionamento da auditoria, criptografia de campos,
convergência de `fc_documentos`, extensão `btree_gist` e otimização baseada em
métricas reais.

A próxima etapa autorizada é somente **H2C.3C — Especificação Física Final e
Plano Detalhado das Migrations**.
