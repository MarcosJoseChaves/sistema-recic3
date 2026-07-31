# Etapa H2C.2J — Colunas adicionais e escopo funcional da baseline

## 1. Situação, limites e método

**Situação: APROVADA FUNCIONALMENTE em 31/07/2026.**

As decisões deste documento definem o resultado funcional esperado. Elas não
significam que tabelas, colunas, migrations, bootstrap, constraints, permissões,
interfaces ou fluxos já tenham sido implementados.

Esta etapa usa somente código, testes, templates, scripts e documentação
versionados. Não houve acesso ao banco, dump externo, `.env`, dados reais, API
ou serviço; nenhum SQL, importador ou migration foi executado.

Etiquetas de evidência:

- **CÓDIGO OPERACIONAL:** leitura/gravação localizada no sistema atual;
- **TEMPLATE/TESTE:** campo presente em interface ou cobertura automatizada;
- **DOCUMENTAÇÃO DO SCHEMA:** estrutura comprovada em documento anterior;
- **MIGRATION HISTÓRICA:** DDL legado versionado;
- **APENAS NOME/GRUPO:** finalidade ainda não demonstrada;
- **NÃO DETERMINADA:** falta evidência segura.

Os documentos versionados não reproduzem nominalmente todas as 12 colunas
extras de `associados` nem as 13 extras de `transacoes_financeiras`. Como o dump
não pode ser lido, seus nomes não são inventados; os grupos funcionais já
documentados são registrados como evidência parcial.

## 2. `usuarios`: inventário e classificação

| Coluna documentada | Tipo/nulabilidade/default | Uso e evidência | Classe/destino preliminar | Sensibilidade/risco |
|---|---|---|---|---|
| `id` | INTEGER, PK, sequência, NN | login, autoria e FKs; código | núcleo da conta | identificador |
| `username` | VARCHAR(50), NN, UNIQUE | login e administração; código/template/teste | núcleo | dado de acesso |
| `password_hash` | VARCHAR(255), NN | autenticação; código/teste | núcleo | segredo derivado, nunca exportar |
| `nome_completo` | VARCHAR(100), nulo | exibição e identificação; código | identificação | pessoal |
| `role` | VARCHAR(20), NN | autorização textual atual; código/teste | legado transitório | acesso indevido se ambíguo |
| `uvr_acesso` | VARCHAR(50), nulo | escopo textual atual; código/teste | legado transitório | autorização por objeto |
| `ativo` | BOOLEAN, padrão TRUE | bloqueio de conta; código | núcleo da conta | estado de acesso |
| `email` | VARCHAR(255), nulo | sem uso localizado; schema | recuperação futura/opcional | pessoal |
| `reset_token` | VARCHAR(255), nulo | sem uso localizado; schema | recuperação, não dado estrutural | segredo temporário |
| `reset_token_expira` | TIMESTAMP, nulo | sem uso localizado; schema | recuperação | controle temporal |

Não há CHECK ou FK, além da PK e unicidade de `username`. `role` e `uvr_acesso`
devem coexistir apenas durante a migração do banco atual: valores desconhecidos
não concedem acesso, ausência de escopo não significa alcance global, resultados
antigo/novo serão comparados e o fallback só será desligado após homologação.
Retirada física dependerá de etapa posterior.

### 2.1 Conta, pessoa e associado

- usuário representa autenticação e autorização;
- associado representa vínculo funcional com associação/UVR;
- uma conta pode existir sem associado e um associado sem conta;
- conta não vira associado automaticamente;
- eventual pessoa mestre continua adiada;
- dados pessoais não serão copiados sem finalidade.

### 2.2 Primeiro Administrador Global

| Alternativa | Vantagem | Risco |
|---|---|---|
| migration com usuário | simples | senha/identidade em DDL; proibido |
| variável no startup | automatiza | segredo e repetição no processo normal |
| comando administrativo | explícito e auditável | exige operação segura |
| tela temporária | amigável | superfície pública crítica |
| script de bootstrap | separado | precisa idempotência e proteção |

**Decisão funcional aprovada:** comando/bootstrap separado, somente quando não
existir Administrador Global, com senha fornecida de modo seguro, confirmação ou
troca no primeiro acesso, auditoria e proteção do último administrador. A
baseline não cria usuário, e-mail ou senha.

## 3. `associados`: inventário e classificação

### 3.1 Dezenove colunas nominalmente documentadas

| Coluna | Estrutura | Classe funcional | Evidência/destino preliminar |
|---|---|---|---|
| `id` | SERIAL, PK, NN | identidade | núcleo |
| `numero` | VARCHAR(20), NN | identificação funcional | núcleo, revisar unicidade |
| `uvr` | VARCHAR(10), NN | vínculo textual legado | transição para ID/histórico |
| `associacao` | VARCHAR(50), NN | vínculo textual legado | transição para ID/histórico |
| `nome` | VARCHAR(255), NN | identificação pessoal | núcleo protegido |
| `cpf` | VARCHAR(11), NN, UNIQUE | identificação restrita | obrigatório na ativação, com exceção formal |
| `rg` | VARCHAR(20), NN | documentação pessoal | revisar necessidade/minimização |
| `data_nascimento` | DATE, NN | pessoal restrito | permissão específica |
| `data_admissao` | DATE, NN | vínculo funcional | núcleo/histórico |
| `status` | VARCHAR(20), NN | situação textual | substituir por estado controlado |
| `cep` | VARCHAR(8), NN | endereço | contato/pessoal |
| `logradouro` | VARCHAR(255), nulo | endereço | contato/pessoal |
| `endereco_numero` | VARCHAR(20), nulo | endereço | contato/pessoal |
| `bairro` | VARCHAR(100), nulo | endereço | contato/pessoal |
| `cidade` | VARCHAR(100), nulo | endereço | contato/pessoal |
| `uf` | VARCHAR(2), nulo | endereço | contato/pessoal |
| `telefone` | VARCHAR(20), NN | contato | núcleo funcional protegido |
| `data_hora_cadastro` | TIMESTAMP, NN, `now()` instalado | auditoria | núcleo/auditoria |
| `foto_base64` | TEXT, nulo | documento/foto | fora da linha futura; preservar legado |

O código possui cadastro, consulta, edição, relatório e exclusão física dessas
colunas; autenticação por objeto usa `uvr` textual. Dados pessoais aparecem em
formulários e relatórios autorizados.

### 3.2 Doze colunas adicionais

A documentação do schema comprova 12 colunas opcionais extras, não usadas pelo
código, agrupadas em: `funcao` e datas, motivos ou observações de afastamento,
suspensão, exclusão e readmissão. Os nomes/tipos individuais não estão
integralmente reproduzidos no Git.

Classificação preliminar: situação funcional/histórico ou legado ambíguo. Elas
ficam preservadas no banco atual, não entram automaticamente na baseline e
exigem confirmação nominal autorizada antes do desenho técnico.

### 3.3 CPF, situação e exclusão

Alternativas para CPF: obrigatório na ativação; opcional com cadastro
provisório; identificador alternativo para estrangeiro/sem CPF. Recomenda-se
unicidade normalizada quando informado, bloqueio de duplicidade automática,
correção formal e exibição mascarada salvo permissão específica.

Estados administrativos aprovados: `RASCUNHO`, `ATIVO`, `AFASTADO`, `SUSPENSO`,
`DESLIGADO` e `FALECIDO`. `PENDENTE_REGULARIZACAO` é condição separada, não um
estado. O estado do cadastro, vínculo, operação, rateio e acesso não devem ser
confundidos.

Associado com histórico financeiro, documental, patrimonial, EPI, auditoria ou
rateio não é apagado. Rascunho sem vínculo pode admitir descarte futuro.

### 3.4 Dados bancários

Não foram comprovados campos bancários diretamente em `associados`. Contas
financeiras institucionais estão em `contas_correntes`, conceito diferente.
Qualquer dado bancário pessoal futuro deve usar estrutura relacionada, admitir
histórico/múltiplas contas e separar visualizar de exportar, sem logs sensíveis.

### 3.5 Associação e UVR

O modelo futuro usará IDs e histórico. Texto antigo permanece na transição;
ambiguidade não concede escopo. Mudança de associação é crítica e mudança de UVR
é formal. Ainda se deve decidir se existe uma associação principal, uma UVR
principal e múltiplos vínculos ativos.

## 4. `transacoes_financeiras`: inventário e classificação

### 4.1 Treze colunas nominalmente documentadas e usadas

| Coluna | Estrutura | Classe/uso | Destino preliminar |
|---|---|---|---|
| `id` | SERIAL, PK, NN | identidade | núcleo |
| `uvr` | VARCHAR(10), NN | escopo textual/relatórios | legado + futuro vínculo gerencial |
| `associacao` | VARCHAR(50), NN | organização textual | legado + futuro ID obrigatório |
| `id_cadastro_origem` | INTEGER, nulo, FK `cadastros` | contraparte | núcleo/revisar domínio |
| `nome_cadastro_origem` | VARCHAR(255), NN | fotografia textual | histórico |
| `numero_documento` | VARCHAR(100), nulo | documento/comprovante | núcleo condicional |
| `data_documento` | DATE, NN | data financeira | núcleo |
| `tipo_transacao` | VARCHAR(20), NN | natureza textual | substituir por código explícito |
| `tipo_atividade` | VARCHAR(255), NN | classificação textual | legado/fotografia |
| `valor_total_documento` | DECIMAL(12,2), NN | valor informado | núcleo Decimal |
| `data_hora_registro` | TIMESTAMP, NN, `now()` instalado | auditoria | núcleo |
| `valor_pago_recebido` | DECIMAL(12,2), padrão 0 | execução financeira | núcleo/derivado controlado |
| `status_pagamento` | VARCHAR(30), padrão `Aberto` | estado textual | catálogo de estados futuro |

Formulários, itens, fluxo de caixa, relatórios e exportações usam esses campos.
O código permite exclusão física de transação; itens relacionados usam cascata,
enquanto outros vínculos podem bloquear ou manter referências lógicas.

### 4.2 Treze colunas adicionais

O schema documentado comprova 13 colunas opcionais ligadas a patrimônio,
motorista, combustível, medidor, manutenção e garantia. Apenas
`id_patrimonio` está nominalmente reproduzida e não possui FK; não há uso
operacional localizado das 13.

Classificação: provável extensão patrimonial/operacional, fora do núcleo
financeiro até finalidade confirmada. Preservar no banco atual; não promover ou
remover automaticamente. Estrutura futura, se necessária, deve ser própria e
manter referência histórica.

### 4.3 Natureza, catálogo, valores e estados

- `RECEITA`/`DESPESA` são natureza explícita, não grupo nem inferência por sinal;
- textos equivalentes ou divergentes antigos vão para saneamento, sem regra
  automática insegura;
- novos lançamentos usarão Produto/Serviço por ID e fotografia textual;
- itens atuais já preservam descrição, unidade, quantidade, preço e total;
- dinheiro usa Decimal; valor informado difere de cálculo derivado;
- transação concluída não é reescrita; correção usa estorno ou fluxo formal;
- associação deverá ser vínculo obrigatório; UVR pode ser dimensão gerencial e
  rateio entre várias UVRs permanece decisão funcional;
- estados aprovados: `RASCUNHO`, `REGISTRADA`, `PARCIALMENTE_LIQUIDADA`,
  `LIQUIDADA`, `CANCELADA` e `ESTORNADA`.

## 5. Redundâncias e transição

| Par atual/futuro | Fonte futura | Regra de transição |
|---|---|---|
| `role` × perfis/permissões | estruturas próprias | comparar decisões e encerrar fallback |
| `uvr_acesso` × escopos | vínculos de escopo | ambiguidade nunca amplia acesso |
| `associacao`/`uvr` textos × IDs | entidades/vínculos | manter fotografia até homologação |
| `tipo_transacao` texto × natureza | código estável | sanear divergências |
| `tipo_atividade` × catálogo por ID | ID + fotografia | novos usam ID; histórico mantém texto |
| status textual × estado controlado | catálogo/eventos | coexistência temporária |
| valores informados × derivados | fatos/eventos | não sobrescrever fato concluído |

Coluna sem uso no Git não é inútil: permanece no banco atual, só entra na
baseline se tiver finalidade nuclear/transitória comprovada e nunca é removida
sem dependência, exportação/arquivo e etapa própria.

## 6. Dados estruturais e dados proibidos

### 6.1 Dados estruturais funcionalmente permitidos

| Domínio | Conteúdo candidato | Classe preliminar |
|---|---|---|
| autorização | módulos, ações, permissões, perfis protegidos, escopos | indispensável/definição técnica |
| associação/UVR | estados, tipos de vínculo, eventos | estrutural |
| solicitações | estados, eventos, mensagens, riscos e ações | estrutural |
| patrimônio | estados, condições, eventos, baixas, transferências, documentos, fotos, bloqueios | estrutural/técnico pendente |
| catálogo | `RECEITA`, `DESPESA`, `PRODUTO`, `SERVICO`, estados e eventos | estrutural aprovado em princípio |
| Fiscalização | tipos/status já necessários às estruturas versionadas | módulo global |

Não entram: pessoas, usuários, e-mails, senhas/hashes/tokens, associações/UVRs,
associados/CPFs, dados bancários, contas/transações, patrimônio, contratos,
documentos/fotos, mensagens/solicitações, catálogo/aliases/CSVs reais, dados das
27 tabelas adicionais, credenciais ou URLs sensíveis.

## 7. Escopo funcional aprovado da baseline nuclear

| Domínio/conceito | Classe | Baseline | Observação |
|---|---|---|---|
| usuários/autenticação | núcleo obrigatório | sim | sem usuário real |
| perfis/permissões/escopos | substituta obrigatória | sim | códigos estruturais |
| associações/UVRs | substituta obrigatória | sim | estruturas vazias |
| associados | núcleo obrigatório | sim | modelo futuro aprovado |
| cadastros/financeiro/itens | núcleo obrigatório | sim | preservar fatos/fotografias |
| contas/fluxo/links | núcleo obrigatório | sim | sem contas reais |
| rateios | núcleo obrigatório | sim | estrutura própria e histórica |
| patrimônio | núcleo obrigatório | sim | modelo aprovado, estruturas futuras |
| catálogo | núcleo obrigatório | sim | sem catálogo real |
| solicitações | núcleo obrigatório | sim | versão A evoluída |
| Fiscalização (`fc_*`) | módulo global obrigatório | sim | migrations 001–011 como fonte |
| auditoria técnica/ledger | substituta obrigatória | sim | desenho técnico posterior |
| auditoria funcional/documentos/EPI | módulos opcionais | não no núcleo | migrations próprias futuras |
| Ouvidoria/27 adicionais/`produtos` | legado/fora/adiado | não | preservar banco atual |

## 8. Banco vazio, versionamento e migração atual

A baseline destina-se a PostgreSQL vazio. Antes do primeiro DDL, tabela conhecida
do sistema interrompe a execução e orienta o plano de migração; metadados do
PostgreSQL não contam. Não há adaptação silenciosa, `DROP`, `TRUNCATE`, renomeio
ou `IF NOT EXISTS` para esconder divergência.

O mecanismo futuro deve registrar versão, identificador, ordem, checksum,
transação, sucesso/falha e detectar arquivo aplicado que foi alterado. Módulos
opcionais terão controle independente compatível com o núcleo.

A migração do banco atual é outro plano: inventário, mapeamento, preservação,
saneamento de textos/duplicidades, novos IDs, comparação, fases, homologação e
recuperação planejada. A baseline não modifica banco existente.

## 9. Alternativas de fechamento

| Modelo | Vantagem | Risco |
|---|---|---|
| A — todas as colunas atuais | compatibilidade literal | carrega legado e ambiguidades |
| B — somente modelo futuro | baseline limpa | migração difícil e perda de compatibilidade |
| C — futuro + legados transitórios comprovados | equilíbrio e retirada controlada | coexistência e testes mais complexos |

**Modelo aprovado:** Modelo C; núcleo futuro, legado apenas quando necessário à
compatibilidade, critérios de retirada, bootstrap separado, fatos financeiros
imutáveis, dados reais fora das migrations e banco atual em plano separado.

## 10. Registro das cinquenta decisões aprovadas

Em **31/07/2026**, as recomendações dos 50 itens abaixo foram aprovadas como
decisões funcionais, com estes esclarecimentos vinculantes: `role` e
`uvr_acesso` ficam fora da instalação nova; CPF admite exceção formal quando não
aplicável; existe uma associação principal e uma UVR principal ativas; vínculos
secundários de UVR limitam-se à mesma associação; os estados administrativos do
associado são `RASCUNHO`, `ATIVO`, `AFASTADO`, `SUSPENSO`, `DESLIGADO` e
`FALECIDO`, enquanto `PENDENTE_REGULARIZACAO` é condição separada; Produto ou
Serviço por ID admite somente dispensa expressa; e nenhuma coluna sem uso
comprovado entra preventivamente na baseline. As alternativas são preservadas
abaixo apenas como memória da decisão.

1. **Baseline usará modelo futuro com legado mínimo?** Alternativas: sim; todas
   as colunas; nenhum legado. **Recomendação:** sim. **Impacto:** compatibilidade.
2. **`role` existirá em instalação nova?** Alternativas: sim; só migração; campo
   temporário. **Recomendação:** não no modelo novo. **Impacto:** autorização.
3. **`uvr_acesso` existirá em instalação nova?** Alternativas: sim; só migração;
   temporário. **Recomendação:** não. **Impacto:** escopos formais.
4. **Usuário representa conta/autorização?** Alternativas: somente conta; também
   associado/pessoa. **Recomendação:** somente conta. **Impacto:** separação.
5. **Associado separado de usuário?** Alternativas: sim; fundir.
   **Recomendação:** sim. **Impacto:** vínculos independentes.
6. **Pessoa associada sem conta?** Alternativas: permitir; proibir.
   **Recomendação:** permitir. **Impacto:** inclusão funcional.
7. **Conta sem associado?** Alternativas: permitir; proibir. **Recomendação:**
   permitir. **Impacto:** administradores/servidores.
8. **Primeiro admin por bootstrap seguro?** Alternativas: comando; tela;
   migration. **Recomendação:** comando separado. **Impacto:** segurança.
9. **Baseline sem usuário?** Alternativas: vazia; admin padrão.
   **Recomendação:** vazia. **Impacto:** nenhum dado real.
10. **Bootstrap somente sem Administrador Global?** Alternativas: sim; repetível.
    **Recomendação:** sim. **Impacto:** evita elevação.
11. **CPF obrigatório para ativar?** Alternativas: sim; opcional; por categoria.
    **Recomendação:** sim, com exceções definidas. **Impacto:** identidade.
12. **Associado provisório sem CPF?** Alternativas: permitir bloqueado; proibir.
    **Recomendação:** permitir regularização. **Impacto:** operação transitória.
13. **CPF único no histórico?** Alternativas: global; só ativos; não único.
    **Recomendação:** global quando informado. **Impacto:** duplicidades.
14. **CPF duplicado bloqueia automação?** Alternativas: bloquear; alertar; fundir.
    **Recomendação:** bloquear. **Impacto:** revisão humana.
15. **CPF completo exige permissão?** Alternativas: específica; login; público.
    **Recomendação:** específica. **Impacto:** privacidade.
16. **Uma associação principal ativa?** Alternativas: uma; várias; nenhuma.
    **Recomendação:** uma principal. **Impacto:** responsabilidade.
17. **Várias associações ativas?** Alternativas: permitir; proibir; excepcional.
    **Recomendação:** excepcional e formal. **Impacto:** escopo.
18. **Uma UVR principal ativa?** Alternativas: uma; nenhuma; várias principais.
    **Recomendação:** uma. **Impacto:** operação padrão.
19. **Atuar em várias UVRs?** Alternativas: sim; não; por autorização.
    **Recomendação:** sim por vínculo formal. **Impacto:** flexibilidade.
20. **Mudanças preservam histórico?** Alternativas: sempre; só crítica; não.
    **Recomendação:** sempre. **Impacto:** auditoria.
21. **Estados de associado?** Alternativas: conjunto da seção 3.3; simples;
    configurável. **Recomendação:** conjunto controlado sem redundância.
    **Impacto:** fluxos.
22. **Desligado aparece no histórico?** Alternativas: sim; não.
    **Recomendação:** sim. **Impacto:** relatórios.
23. **Exclusão só de rascunho sem vínculo?** Alternativas: sim; nunca; inativos.
    **Recomendação:** sim. **Impacto:** retenção.
24. **Dados bancários em estrutura própria desde a baseline?** Alternativas:
    própria; na linha; adiar. **Recomendação:** própria. **Impacto:** histórico.
25. **Várias contas pessoais?** Alternativas: sim; uma; nenhuma.
    **Recomendação:** sim com principal. **Impacto:** pagamentos.
26. **Alteração bancária com aprovação?** Alternativas: sim; edição direta.
    **Recomendação:** sim. **Impacto:** fraude/auditoria.
27. **Permissão bancária específica?** Alternativas: visualizar/exportar
    separadas; login; pública. **Recomendação:** separadas. **Impacto:** sigilo.
28. **Natureza obrigatória em toda transação?** Alternativas: sim; inferida;
    opcional. **Recomendação:** sim. **Impacto:** consistência.
29. **Associação obrigatória na transação?** Alternativas: sim; opcional.
    **Recomendação:** sim. **Impacto:** responsabilidade.
30. **UVR na transação?** Alternativas: obrigatória; opcional gerencial;
    derivada. **Recomendação:** opcional/gerencial conforme operação.
    **Impacto:** rateio.
31. **Rateio entre UVRs?** Alternativas: permitir; proibir; módulo futuro.
    **Recomendação:** permitir formalmente. **Impacto:** estrutura de rateio.
32. **Editar transação concluída?** Alternativas: não; sim; campos não
    financeiros. **Recomendação:** não reescrever fatos. **Impacto:** histórico.
33. **Correção por estorno/fluxo formal?** Alternativas: sim; edição direta.
    **Recomendação:** sim. **Impacto:** rastreabilidade.
34. **Estados financeiros?** Alternativas: conjunto da seção 4.3; atuais;
    configuráveis. **Recomendação:** catálogo controlado. **Impacto:** fluxo.
35. **Produto/Serviço por ID obrigatório?** Alternativas: sim; texto; ambos.
    **Recomendação:** sim em novos lançamentos. **Impacto:** catálogo.
36. **Textos preservados como fotografia?** Alternativas: sim; não.
    **Recomendação:** sim. **Impacto:** relatórios históricos.
37. **Fallback textual só encerra após homologação?** Alternativas: sim; imediato.
    **Recomendação:** sim. **Impacto:** compatibilidade.
38. **Coluna sem finalidade fica fora da baseline?** Alternativas: sim; copiar.
    **Recomendação:** sim. **Impacto:** baseline limpa.
39. **Coluna omitida permanece no banco atual?** Alternativas: sim; remover.
    **Recomendação:** sim. **Impacto:** preservação.
40. **Dados estruturais em migrations?** Alternativas: sim; carga separada;
    nenhum. **Recomendação:** códigos estáveis aprovados. **Impacto:** inicialização.
41. **Catálogo real em carga separada?** Alternativas: sim; migration.
    **Recomendação:** sim. **Impacto:** sem dados reais no DDL.
42. **Códigos `RECEITA`/`DESPESA`/`PRODUTO`/`SERVICO`?** Alternativas: baseline;
    carga; não. **Recomendação:** estruturais. **Impacto:** regras estáveis.
43. **Perfis institucionais protegidos na baseline?** Alternativas: sim; carga.
    **Recomendação:** códigos/perfis sem usuários. **Impacto:** autorização.
44. **Admin nunca criado por migration?** Alternativas: nunca; admin padrão.
    **Recomendação:** nunca. **Impacto:** segurança.
45. **Falhar antes do DDL em banco não vazio?** Alternativas: sim; adaptar.
    **Recomendação:** sim. **Impacto:** proteção.
46. **Controle de versão/checksum?** Alternativas: ambos; só ordem; nenhum.
    **Recomendação:** ambos. **Impacto:** integridade.
47. **Detectar migration aplicada alterada?** Alternativas: bloquear; avisar;
    ignorar. **Recomendação:** bloquear. **Impacto:** reprodutibilidade.
48. **Módulos opcionais versionados separadamente?** Alternativas: sim; junto.
    **Recomendação:** sim. **Impacto:** independência.
49. **Migração do banco atual separada?** Alternativas: sim; mesma baseline.
    **Recomendação:** sim. **Impacto:** segurança dos dados.
50. **Há coluna extra a preservar sem uso no Git?** Alternativas: indicar
    nominalmente; nenhuma conhecida; decidir após inventário autorizado.
    **Recomendação:** confirmação humana/documental. **Impacto:** bloqueio final.

## 11. Implementação técnica pendente

Não restou bloqueio funcional da H2C.2J. Permanecem para a fase técnica: nomes
finais de tabelas e colunas, tipos PostgreSQL, chaves, constraints, índices,
formato dos códigos, estruturas físicas de histórico, mecanismo de migrations,
bootstrap, compatibilidade, migração de dados, interfaces, testes e homologação.

Módulos opcionais terão versões, checksums, dependências e testes próprios. A
migração do banco atual continua sendo projeto separado. Nenhuma decisão desta
etapa autoriza implementação, remoção de coluna ou execução de DDL.
