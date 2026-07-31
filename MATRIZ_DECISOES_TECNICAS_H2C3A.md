# Matriz de decisões técnicas — H2C.3A

## 1. Situação

**APROVADA TECNICAMENTE em 31/07/2026.** As 30 decisões abaixo estão
**APROVADAS**. A aprovação conceitual não autoriza migration, SQL, código,
bootstrap ou alteração de banco.

## 2. Matriz técnica

| Código | Assunto | Alternativas | Alternativa aprovada | Justificativa/impacto | Risco/dependência | Situação |
|---|---|---|---|---|---|---|
| DT-01 | IDs novos | INTEGER; BIGINT; UUID; híbrido | BIGINT novo, usuário INTEGER, `fc_*` preservado | compatibilidade e crescimento | FKs atuais | APROVADA |
| DT-02 | geração de IDs | SERIAL; IDENTITY; aplicação | IDENTITY nas novas | padrão PostgreSQL moderno | tooling | APROVADA |
| DT-03 | timestamps | TIMESTAMP; TIMESTAMPTZ | TIMESTAMPTZ | instantes consistentes | conversão do legado | APROVADA |
| DT-04 | idioma | português; inglês; misto | português técnico | coerência | nomes finais | APROVADA |
| DT-05 | códigos | TEXT; VARCHAR; ENUM | TEXT + CHECK/FK | evita tamanho arbitrário | catálogo/estado | APROVADA |
| DT-06 | estados | CHECK; catálogo; ENUM | híbrido CHECK/FK | estabilidade versus evolução | versionamento | APROVADA |
| DT-07 | dinheiro | 12,2; 18,2; 24,8 | NUMERIC(18,2) | margem e simplicidade | regras de arredondamento | APROVADA |
| DT-08 | quantidades | mesma escala; 24,8 | NUMERIC(24,8) | cálculos técnicos | performance | APROVADA |
| DT-09 | concorrência | lock; versão; híbrido | versão + lock crítico | detecta conflito | serviços | APROVADA |
| DT-10 | normalização | extensão; aplicação; função DB | aplicação comum + coluna | portabilidade | consistência | APROVADA |
| DT-11 | usuário/e-mail | case-sensitive; insensitive | UNIQUE funcional minúsculo | evita duplicidade visual | índice funcional | APROVADA |
| DT-12 | recuperação | coluna; tabela/token puro; tabela/hash | tabela própria com hash | reduz exposição | fluxo futuro | APROVADA |
| DT-13 | perfis | tabelas genéricas; específicas; híbridas | híbridas com FKs reais | integridade | escopos de objeto | APROVADA |
| DT-14 | validade de perfil | sem período; período | início/fim | delegação auditável | sobreposição | APROVADA |
| DT-15 | principal ativo | flag solta; índice parcial | índice único parcial | garantia no banco | períodos | APROVADA |
| DT-16 | vínculos UVR | permitir sobreposição; impedir | impedir por período | coerência histórica | exclusion/service | APROVADA |
| DT-17 | banco associado | uma linha; várias relacionadas | várias, uma principal | histórico | dados sensíveis | APROVADA |
| DT-18 | rateio UVR | coluna direta; tabela; híbrido | sempre tabela de rateio | modelo único | validação de soma | APROVADA |
| DT-19 | snapshot financeiro | colunas; JSONB; ambos | IDs + colunas essenciais + JSONB versionado | história legível | schema JSON | APROVADA |
| DT-20 | snapshots solicitação | texto; JSONB | JSONB versionado | antes/depois flexível | validação por tipo | APROVADA |
| DT-21 | documentos | central; separados; núcleo+vínculos | núcleo+vínculos | metadado comum, permissão própria | `fc_documentos` | APROVADA |
| DT-22 | auditoria | editável; append-only | append-only | investigação confiável | retenção | APROVADA |
| DT-23 | delete histórico | CASCADE; SET NULL; RESTRICT | RESTRICT | preservação | descarte de rascunho | APROVADA |
| DT-24 | migration ledger | um por módulo; central; externo | central com módulo | visão única | lock/checksum | APROVADA |
| DT-25 | dados estruturais | manual; migration; seed solto | migration determinística | reprodutibilidade | idempotência estrita | APROVADA |
| DT-26 | `fc_*` | redesenhar; reproduzir; omitir | reproduzir resultado 001–011 | preserva módulo | dependência usuários | APROVADA |
| DT-27 | divisão migrations | monolítica; blocos ordenados | blocos nucleares | revisão/recuperação | dependências | APROVADA |
| DT-28 | CPF | VARCHAR; TEXT; numérico | VARCHAR(11) ou TEXT + CHECK | zeros e formato exato sem CHAR | exceção formal | APROVADA |
| DT-29 | IP/request | TEXT; INET/UUID | INET e UUID | tipos nativos | privacidade/retenção | APROVADA |
| DT-30 | revisão | direta; revisão independente | revisão independente | reduz risco sistêmico | agenda técnica | APROVADA |

## 3. Registro das trinta decisões aprovadas

As alternativas e impactos abaixo são preservados como memória da decisão. A
recomendação de cada item foi aprovada em **31/07/2026**. Os ajustes vinculantes
são CPF em `VARCHAR(11)` ou `TEXT` com CHECK de 11 dígitos e checksum textual em
`VARCHAR(64)` com validação hexadecimal; `CHAR` não será usado nesses campos.

1. **As novas tabelas usarão BIGINT?** Alternativas: todas BIGINT; todas
   INTEGER; modelo híbrido. **Recomendação:** BIGINT nas novas e tipos atuais
   preservados. **Impacto:** crescimento e compatibilidade das FKs.
2. **`usuarios.id` permanecerá INTEGER?** Alternativas: manter; converter para
   BIGINT; UUID. **Recomendação:** manter INTEGER. **Impacto:** evita alterar
   código e 23 tabelas `fc_*`.
3. **IDs novos usarão IDENTITY?** Alternativas: IDENTITY; SERIAL; geração pela
   aplicação. **Recomendação:** `GENERATED BY DEFAULT AS IDENTITY`. **Impacto:**
   padrão moderno sem tocar nas migrations antigas.
4. **Instantes usarão TIMESTAMPTZ?** Alternativas: TIMESTAMPTZ; TIMESTAMP; por
   tabela. **Recomendação:** TIMESTAMPTZ para eventos e auditoria. **Impacto:**
   datas consistentes entre ambientes.
5. **Nomes técnicos permanecerão em português?** Alternativas: português;
   inglês; misto. **Recomendação:** português técnico em `snake_case`.
   **Impacto:** coerência e manutenção.
6. **Códigos estruturais serão TEXT ou VARCHAR?** Alternativas: TEXT com regra;
   VARCHAR fixo; ENUM. **Recomendação:** TEXT com CHECK/FK. **Impacto:** evita
   limites arbitrários e facilita evolução controlada.
7. **Estados usarão catálogo ou CHECK?** Alternativas: catálogo; CHECK; ENUM;
   híbrido. **Recomendação:** CHECK para conjuntos fechados do fluxo e FK para
   catálogos evolutivos. **Impacto:** integridade sem rigidez excessiva.
8. **Qual precisão conceitual para dinheiro?** Alternativas: NUMERIC(12,2);
   (18,2); (24,8). **Recomendação:** NUMERIC(18,2), mantendo maior escala somente
   em cálculo técnico. **Impacto:** capacidade e arredondamento previsíveis.
9. **Quantidades usarão escala maior que dinheiro?** Alternativas: sim, 24,8;
   mesma escala; por módulo. **Recomendação:** NUMERIC(24,8). **Impacto:** evita
   perda em medições e composições.
10. **Será usada versão para concorrência otimista?** Alternativas: sempre em
    entidades mutáveis; somente lock; nenhuma. **Recomendação:**
    `versao_registro` em entidades mutáveis e lock nos fluxos críticos.
    **Impacto:** conflitos ficam visíveis.
11. **E-mail será único quando preenchido?** Alternativas: único sem caixa;
    único literal; repetível. **Recomendação:** índice único parcial em valor
    normalizado. **Impacto:** recuperação de acesso inequívoca.
12. **Username será comparado sem caixa?** Alternativas: sem caixa; literal;
    configurável. **Recomendação:** `LOWER(BTRIM(username))`. **Impacto:** evita
    contas visualmente duplicadas.
13. **Tokens ficarão em tabela própria e com hash?** Alternativas: tabela/hash;
    coluna no usuário; token puro. **Recomendação:** tabela própria e hash.
    **Impacto:** limita exposição e permite uso único.
14. **Escopos usarão tabelas específicas ou genéricas?** Alternativas:
    específicas; `(tipo,id)` genérico; híbrido. **Recomendação:** híbrido, com
    FKs específicas para associação/UVR e estrutura do módulo para objetos.
    **Impacto:** integridade real sem multiplicação desnecessária.
15. **Atribuições de perfil terão validade?** Alternativas: início/fim; somente
    revogação; permanentes. **Recomendação:** início e fim opcionais, revogação e
    autoria. **Impacto:** delegação temporária auditável.
16. **Vínculos de UVR impedirão períodos sobrepostos?** Alternativas: impedir;
    permitir; validar apenas na aplicação. **Recomendação:** impedir, usando
    constraint de período ou transação. **Impacto:** histórico coerente.
17. **Haverá somente uma conta bancária principal ativa?** Alternativas: uma por
    finalidade; uma total; várias principais. **Recomendação:** uma por
    associado/finalidade. **Impacto:** pagamento determinístico.
18. **Toda atribuição de UVR financeira usará rateio?** Alternativas: sempre
    tabela; coluna direta para uma; híbrido. **Recomendação:** sempre tabela,
    inclusive para uma UVR. **Impacto:** elimina duas fontes de verdade.
19. **Fotografia financeira usará JSONB?** Alternativas: JSONB versionado;
    somente colunas; texto. **Recomendação:** IDs/colunas essenciais mais JSONB
    versionado. **Impacto:** preserva contexto e permite consulta básica.
20. **Fotografias de solicitações usarão JSONB?** Alternativas: JSONB; tabelas
    por tipo; texto. **Recomendação:** JSONB versionado e validado por tipo.
    **Impacto:** suporta objetos diversos com controle.
21. **Catálogo terá colunas normalizadas próprias?** Alternativas: sim; cálculo
    em consulta; extensão. **Recomendação:** sim. **Impacto:** unicidade e busca
    previsíveis.
22. **A normalização dependerá de `unaccent`?** Alternativas: depender; não
    depender; opcional. **Recomendação:** não depender; usar rotina comum da
    aplicação e coluna persistida. **Impacto:** portabilidade da baseline.
23. **Documentos usarão núcleo comum de metadados?** Alternativas: central;
    separados; núcleo com vínculos. **Recomendação:** núcleo comum com vínculos
    específicos, preservando `fc_documentos`. **Impacto:** segurança uniforme e
    migração posterior controlada.
24. **Auditoria técnica será append-only?** Alternativas: sim; editável por
    admin; apenas logs. **Recomendação:** sim. **Impacto:** trilha confiável e
    maior necessidade de política de retenção.
25. **Relações históricas usarão RESTRICT?** Alternativas: RESTRICT; CASCADE;
    SET NULL. **Recomendação:** RESTRICT, com CASCADE só para rascunho técnico sem
    valor próprio. **Impacto:** evita perda silenciosa.
26. **Migrations terão controle central com módulo?** Alternativas: central;
    tabela por módulo; ferramenta externa. **Recomendação:** central com módulo,
    versão e checksum. **Impacto:** dependências e auditoria em um ponto.
27. **Dados estruturais serão carregados por migration determinística?**
    Alternativas: migration; seed manual; inicialização do app.
    **Recomendação:** migration versionada e idempotência estrita. **Impacto:**
    instalações reproduzíveis sem carga real.
28. **As 23 tabelas `fc_*` serão reproduzidas sem alterar 001–011?**
    Alternativas: reproduzir resultado; redesenhar; executar arquivos isolados.
    **Recomendação:** preservar arquivos e reproduzir exatamente o resultado na
    ordem histórica, sob o novo controle. **Impacto:** mantém o módulo e exige
    testes de equivalência.
29. **O núcleo será dividido em migrations ordenadas?** Alternativas: uma grande;
    blocos; uma por tabela. **Recomendação:** blocos por dependência e domínio.
    **Impacto:** revisão e recuperação melhores sem fragmentação excessiva.
30. **Haverá revisão técnica independente antes da implementação?**
    Alternativas: sim; revisão na implementação; não. **Recomendação:** sim, com
    checklist de integridade, segurança e compatibilidade. **Impacto:** reduz
    risco antes de qualquer DDL.

## 4. Implementação pendente e revisão independente

As 30 respostas foram aprovadas. Ainda são necessários desenho físico final,
revisão independente H2C.3B e autorização específica antes de criar qualquer
migration. A revisão verificará nomes, tipos, precisão, ciclos, constraints,
índices, deletes, normalização, JSONB, documentos, auditoria, `fc_*`, ordem,
segurança e testabilidade.
