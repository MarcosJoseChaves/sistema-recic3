# Matriz de ajustes técnicos — H2C.3B

## 1. Situação

**APROVADA DOCUMENTALMENTE em 31/07/2026.** Os 24 achados, seus tratamentos e as
20 decisões foram aprovados. Nenhum item está implementado e esta aprovação não
autoriza migration.

## 2. Achados consolidados

| Código | Domínio/gravidade | Evidência e decisão relacionada | Ajuste recomendado | Bloqueia? / decisão humana | Responsável/teste/situação |
|---|---|---|---|---|---|
| H2C3B-01 | compatibilidade / BLOQUEADOR | `app.py` ainda lê `role` e `uvr_acesso`; DT-01/13 | implementar nova autorização antes de iniciar app com baseline nova | sim / sim | aplicação; integração/autorização; BLOQUEIA IMPLEMENTAÇÃO |
| H2C3B-02 | login / ALTA | busca atual de username é literal; DT-11 | normalização comum em escrita e leitura | sim / não funcional | aplicação; login/UNIQUE; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-03 | migrations / BLOQUEADOR | ledger não pode registrar a si antes de existir; DT-24 | manifesto + bootstrap transacional + registro pós-commit | sim / sim | executor; integração; BLOQUEIA IMPLEMENTAÇÃO |
| H2C3B-04 | migrations / ALTA | falha dentro da transação desaparece no rollback | registrar falha sanitizada em transação separada | sim / não | executor; rollback; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-05 | concorrência / ALTA | dois executores podem iniciar juntos | advisory lock antes do preflight/DDL | sim / sim | executor; concorrência; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-06 | bootstrap / BLOQUEADOR | primeiro usuário não possui autor; DT-30 | ator técnico `BOOTSTRAP`, CHECK restritivo e request ID | sim / sim | segurança; bootstrap; BLOQUEIA IMPLEMENTAÇÃO |
| H2C3B-07 | bootstrap / ALTA | segredo pode vazar em argv/env/log | prompt oculto e modo interativo; lock | sim / sim | segurança; teste de processo/log; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-08 | ordem/FK / ALTA | comprovantes/fotos podem referenciar documentos criados depois | documentos antes de associados/patrimônio | sim / sim | schema; migration test; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-09 | autorização/FK / MÉDIA | escopos organizacionais dependem de associação/UVR | dividir autorização básica e escopos | sim / não | schema; dependências; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-10 | períodos / MÉDIA | exclusão temporal pode exigir `btree_gist`; DT-16 | lock no pai + consulta; extensão apenas futura | não / sim | serviço; concorrência; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-11 | normalização / ALTA | banco não prova coluna gerada pela aplicação; DT-10/21 | rotina única, UNIQUE e teste/auditoria de consistência | sim / sim | aplicação/schema; colisões; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-12 | aliases / ALTA | UNIQUE separado não impede alias igual a nome oficial | namespace único de chaves ou transação equivalente | sim / sim | catálogo/UVR; colisão; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-13 | financeiro / ALTA | soma de rateios é regra entre linhas; DT-18 | lock da transação e validação na conclusão | sim / sim | financeiro; arredondamento/rollback; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-14 | snapshots / MÉDIA | JSONB pode crescer e conter dados sensíveis; DT-19/20 | versão, tamanho, allowlist e sanitização | não / não | serviços; schema/privacidade; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-15 | documentos / MÉDIA | núcleo central pode virar vínculo polimórfico | tabelas de vínculo por domínio; `fc_documentos` separada | não / sim | documentos; autorização/FK; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-16 | auditoria / ALTA | objeto polimórfico sem FK e bootstrap sem usuário; DT-22 | referência lógica sanitizada + eventos com FK + ator técnico | sim / sim | auditoria; append-only; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-17 | `fc_*` / BLOQUEADOR | executar snapshot e 001–011 criaria duas fontes; DT-26 | aplicar literalmente 001–011 sob executor central | sim / sim | migrations FC; equivalência; BLOQUEIA IMPLEMENTAÇÃO |
| H2C3B-18 | `fc_*` / BAIXA | índice composto de aditivos repetido em 005/006 | manter idempotente; documentar, não editar | não / não | FC; teste de ordem; RISCO RESIDUAL DOCUMENTADO |
| H2C3B-19 | tipos / MÉDIA | dinheiro 18,2 versus preços 24,8; DT-07/08 | 18,2 para totais; escala 8 para unitários/cálculo | não / sim | domínios; Decimal/arredondamento; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-20 | CPF/checksum / INFORMATIVA | VARCHAR + CHECK aprovados; DT-28/24 | vazio→NULL; minúsculo no checksum | não / não | schema; validação; RISCO RESIDUAL DOCUMENTADO |
| H2C3B-21 | DELETE / INFORMATIVA | 001–011 não usam CASCADE; DT-23 | RESTRICT histórico; CASCADE só rascunho técnico | não / não | schema; integridade; RISCO RESIDUAL DOCUMENTADO |
| H2C3B-22 | índices / BAIXA | índice de FK não é automático no PostgreSQL | criar apenas conforme consulta/delete do pai | não / não | performance; EXPLAIN futuro; RISCO RESIDUAL DOCUMENTADO |
| H2C3B-23 | segurança / ALTA | CPF, banco e URLs podem vazar em JSON/log | allowlist, mascaramento e auditoria sem conteúdo | sim / não | segurança; testes de segredo; APROVADO PARA TRATAMENTO NA H2C.3C |
| H2C3B-24 | auditoria / BAIXA | retenção/particionamento dependem de volume | medir e definir política posterior | não / sim futura | operação; carga; RISCO RESIDUAL DOCUMENTADO |

### 2.1 Classificação

- Confirmadas sem mudança: IDs híbridos, timestamps/datas, quantidades,
  recuperação por hash, perfis temporais, principal único, alocação própria,
  RESTRICT histórico, INET/UUID e revisão independente.
- Ajustes obrigatórios: H2C3B-01 a 09, 11 a 13, 16, 17 e 23.
- Fecháveis tecnicamente: 02, 04, 09, 10, 14, 18, 20 a 22.
- Exigem validação humana: protocolo bootstrap/ledger, códigos, períodos,
  aliases, rateios, documentos/auditoria e estratégia `fc_*`.
- Adiáveis: criptografia específica, particionamento, `btree_gist`, convergência
  documental e otimizações sem métricas.

## 3. Registro das vinte decisões aprovadas

As alternativas, recomendações, impactos e riscos são preservados como memória.
A recomendação de cada item foi aprovada em **31/07/2026**.

1. **O modelo híbrido de identificadores permanece aprovado?** Alternativas:
   manter; converter tudo a BIGINT; UUID geral. **Recomendação independente:**
   manter. **Impacto:** compatibilidade com usuários e `fc_*`. **Risco:** baixo.
   **Situação:** APROVADA.
2. **BIGINT IDENTITY usará `BY DEFAULT`?** Alternativas: BY DEFAULT; ALWAYS;
   SERIAL. **Recomendação independente:** BY DEFAULT. **Impacto:** permite
   importação controlada. **Risco:** sequência exige reposicionamento.
   **Situação:** APROVADA.
3. **Códigos TEXT terão padrão formal?** Alternativas: ASCII maiúsculo com `_`;
   texto livre; padrão por domínio. **Recomendação independente:**
   `^[A-Z][A-Z0-9_]*$`, com exceções documentadas. **Impacto:** integração.
   **Risco:** códigos legados precisam mapeamento. **Situação:** APROVADA.
4. **Percentuais usarão precisão única?** Alternativas: `NUMERIC(9,6)`; por
   domínio; mesma de dinheiro. **Recomendação independente:** `NUMERIC(9,6)` como
   padrão, exceção documentada. **Impacto:** rateios consistentes. **Risco:**
   arredondamento residual. **Situação:** APROVADA.
5. **A baseline evitará dependência obrigatória de `btree_gist`?** Alternativas:
   evitar; exigir; tornar opcional. **Recomendação independente:** evitar na
   primeira versão. **Impacto:** instalação mais portátil. **Risco:** regra passa
   ao serviço transacional. **Situação:** APROVADA.
6. **Sobreposições serão impedidas por constraint quando possível e transação nos
   demais casos?** Alternativas: híbrido; somente trigger; somente aplicação.
   **Recomendação independente:** híbrido sem extensão obrigatória. **Impacto:**
   integridade concorrente. **Risco:** lock incorreto. **Situação:** APROVADA.
7. **A auditoria inicial aceitará ator técnico `BOOTSTRAP`?** Alternativas: ator
   técnico; autor nulo genérico; usuário artificial. **Recomendação independente:**
   ator técnico restrito por CHECK. **Impacto:** resolve o primeiro usuário.
   **Risco:** uso indevido fora do bootstrap. **Situação:** APROVADA.
8. **O bootstrap usará prompt oculto, sem senha em argumento?** Alternativas:
   interativo; variável temporária; argumento. **Recomendação independente:**
   interativo, com canal seguro automatizado futuro. **Impacto:** reduz vazamento.
   **Risco:** automação inicial mais difícil. **Situação:** APROVADA.
9. **Escopos de objeto continuarão específicos por módulo?** Alternativas:
   específicos; tabela polimórfica; híbrido. **Recomendação independente:**
   específicos, com referência lógica apenas para auditoria/busca. **Impacto:**
   FKs reais. **Risco:** mais tabelas. **Situação:** APROVADA.
10. **Alocações serão usadas para uma ou várias UVRs?** Alternativas: sempre;
    somente múltiplas; coluna direta para uma. **Recomendação independente:**
    sempre quando houver UVR. **Impacto:** uma fonte de verdade. **Risco:** mais
    uma linha por transação. **Situação:** APROVADA.
11. **A soma das alocações será validada na conclusão?** Alternativas: serviço
    com lock; trigger; validação tardia. **Recomendação independente:** serviço
    transacional com lock. **Impacto:** total consistente. **Risco:** residual de
    arredondamento precisa regra. **Situação:** APROVADA.
12. **Triggers ficarão restritos ao indispensável?** Alternativas: sim; usar para
    estados/somas; não usar nunca. **Recomendação independente:** sim, após provar
    que constraint+transação não bastam. **Impacto:** lógica rastreável. **Risco:**
    serviço precisa testes fortes. **Situação:** APROVADA.
13. **Documentos privados usarão metadados centrais e vínculos específicos?**
    Alternativas: núcleo+links; central polimórfico; separado por módulo.
    **Recomendação independente:** núcleo+links com FKs. **Impacto:** segurança
    comum e integridade. **Risco:** migração futura. **Situação:** APROVADA.
14. **`fc_documentos` permanecerá independente inicialmente?** Alternativas:
    independente; converter; fazer view. **Recomendação independente:** manter
    independente e integrar só por etapa futura. **Impacto:** zero regressão.
    **Risco:** coexistência temporária. **Situação:** APROVADA.
15. **Auditoria central aceitará objeto lógico sem FK genérica?** Alternativas:
    referência lógica; FK polimórfica impossível; tabela por domínio.
    **Recomendação independente:** lógica na auditoria, FKs nos eventos do
    domínio. **Impacto:** trilha transversal. **Risco:** objeto removido deve
    continuar interpretável. **Situação:** APROVADA.
16. **O executor usará lock contra concorrência?** Alternativas: advisory lock;
    lock de tabela; sem lock. **Recomendação independente:** advisory lock antes
    do preflight. **Impacto:** uma execução por banco. **Risco:** chave/timeout
    precisam padrão. **Situação:** APROVADA.
17. **A instalação nova de `fc_*` aplicará literalmente 001–011 sob o executor
    central?** Alternativas: literal; snapshot executável; híbrido.
    **Recomendação independente:** literal; snapshot apenas para comparação.
    **Impacto:** uma fonte de verdade. **Risco:** executor precisa tratar as
    transações existentes nos arquivos. **Situação:** APROVADA.
18. **Dados estruturais indispensáveis serão carregados antes da validação final?**
    Alternativas: sim; depois do primeiro start; manual. **Recomendação
    independente:** sim, por migration controlada e divergência bloqueante.
    **Impacto:** aplicação inicia coerente. **Risco:** separar dado estrutural de
    real. **Situação:** APROVADA.
19. **Será obrigatória suíte PostgreSQL antes da implementação ser aprovada?**
    Alternativas: sim; somente mocks; após deploy. **Recomendação independente:**
    sim, em banco efêmero. **Impacto:** valida constraints, transações e rollback.
    **Risco:** maior tempo de CI. **Situação:** APROVADA.
20. **O parecer H2C.3B autoriza apenas especificação física?** Alternativas:
    somente especificação; migrations imediatas; protótipo no banco.
    **Recomendação independente:** somente especificação física após os ajustes.
    **Impacto:** mantém ponto de controle. **Risco:** nenhum técnico relevante.
    **Situação:** APROVADA.

## 4. Condição aprovada para avançar

Os 24 achados e 20 decisões estão aprovados. A H2C.3C deverá atualizar a
especificação física nominal, demonstrar o tratamento dos quatro bloqueadores e
dez achados altos e definir o plano de testes, sem criar migrations até nova
autorização.
