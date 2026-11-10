# Contratos ESG — ESG-02A

O texto abaixo documenta os contratos da ESG-02A. A camada adicional ESG-02B1,
seu SQL ainda não executado e a estratégia de revisões estão descritos em
[PERSISTENCIA.md](PERSISTENCIA.md). Os contratos e a interface permanecem iguais.

Contratos Python e organização em memória. Não importam o aplicativo, consultam
arquivos, abrem conexões, persistem ou publicam informações. A interface ESG-01
continua vazia. RF-1A permanece **NO-GO OPERACIONAL**.

## Entrada e validação

`contratos.py` define dataclasses imutáveis com validação na construção.
`validacoes.py` fornece `ContratoESGInvalido`, derivada de `ValueError`, com o
campo/vínculo inválido na mensagem. Não há coerção implícita de texto, número,
enumeração ou data. Adaptadores de formulários e de persistência ficam para outra
etapa e deverão construir estes contratos explicitamente.

| Contrato | Campos |
| --- | --- |
| `FonteDocumental` | identificador estável, referência documental, data do documento opcional |
| `Perimetro` | código estável e descrição do recorte de abrangência |
| `UnidadeMedida` | símbolo e descrição declarados, sem catálogo institucional presumido |
| `Periodo` | ano inteiro e mês opcional; mês ausente representa ano completo |
| `DefinicaoIndicador` | código, nome, dimensão, descrição, unidade, metodologia, perímetro, fonte metodológica, situação e fonte de aprovação opcional |
| `RegistroPeriodo` | UUID, código do indicador, período, fonte, data de referência, perímetro, valor opcional, situação e fonte de aprovação opcional |
| `Evidencia` | UUID, UUID do registro, tipo documental, fonte com referência documental e observações opcionais |

Códigos usam até 64 caracteres ASCII, iniciando por letra, seguidos de letras,
números, ponto, hífen ou sublinhado. Textos obrigatórios não podem estar vazios,
ter espaços nas extremidades ou controles inválidos. Símbolos de unidade têm até
32 caracteres, sem espaços. Essas regras são de estrutura, não homologação de
um indicador ou de sua unidade.

Valores devem ser `Decimal` finito ou `None`. Ausência permanece ausência;
`Decimal("0")` é um valor informado. Não são aceitos floats, inteiros, booleanos,
strings, NaN ou infinitos. Precisão e escala são preservadas sem arredondamento.
Sinal e limites numéricos de negócio dependem de metodologia expressamente
validada; não se presume que todo indicador seja não negativo ou que todo
percentual esteja entre zero e cem. Não há conversão de unidades nem cálculos de
metas, pontuações, percentuais ou desempenho.

O período usa ano de 1 a 9999 e mês de 1 a 12 quando informado. A competência é
representada como `AAAA` ou `AAAA-MM`. A data de referência pertence ao período
medido; a data do documento pertence à fonte e pode ser posterior. Datas exigem
`datetime.date`; strings e `datetime.datetime` não são aceitos.

## Situações e vínculos

- Definições: `proposto` (padrão), `em_analise`, `aprovado`, `rejeitado`.
- Registros: `rascunho` (padrão), `em_analise`, `aprovado`, `rejeitado`.
- Uma situação aprovada exige `fonte_aprovacao` explícita. Um registro aprovado
  também exige valor; zero é permitido. A organização da série exige que a
  definição correspondente esteja aprovada.
- Referências e situações são metadados declarados. O contrato não autentica
  aprovadores, verifica documentos nem executa atos de aprovação. Autorização,
  transições, autoria, datas e histórico de decisões precisam de fluxo próprio
  antes de qualquer operação institucional.

`organizar_registros(definicao, registros, evidencias)` aceita listas ou tuplas
em memória, valida os vínculos e retorna `SerieIndicador`, imutável, ordenada por
ano/mês. A construção direta de `SerieIndicador` também valida os vínculos.
Iteradores não são consumidos. Coleções vazias são válidas.

Cada registro deve apontar ao código da definição e ter perímetro exatamente
igual (código e descrição). Não se inferem equivalência, inclusão geográfica ou
compatibilidade entre recortes. Identificadores de registro, períodos e
identificadores de evidência não podem se repetir na série. Evidências órfãs e
fontes com mesmo identificador mas referências/datas divergentes são rejeitadas.
Ano completo e meses são registros distintos, preservados sem somar ou conciliar.
Revisões do mesmo período ainda não são modeladas.

## Pontos de integração com PostgreSQL — etapa futura

| Elemento | Mapeamento previsto, a revisar antes de implementar |
| --- | --- |
| Definições, unidades e perímetros | códigos textuais, descrições, vínculos e situações controladas |
| Fontes | identidade documental estável, referência textual e data opcional; reutilização por vínculo |
| Registros | UUID, vínculo à definição e ao perímetro, ano, mês anulável, `DATE`, `NUMERIC` anulável e fontes |
| Evidências | UUID, chave estrangeira para o UUID do registro, tipo, fonte e observações anuláveis |
| Situações | valores textuais das enumerações; restrições de integridade e decisões auditadas a implementar |

O futuro serviço deve receber a conexão por injeção, como os serviços RECIC3
existentes. Deve preservar `Decimal`/`None`, converter enumerações explicitamente
pelos seus valores, reconstituir os contratos na leitura e validar os vínculos
antes da escrita. Unicidade de período (inclusive anual), identidades documentais,
chaves estrangeiras, limites de `NUMERIC`, versionamento e transações exigem
projeto e testes próprios. Nenhum SQL, driver ou repositório de persistência foi
adicionado nesta etapa.

## Testes e próximas etapas

`tests/test_esg_contratos.py` usa `unittest` e somente biblioteca padrão. Os três
recortes históricos de 2025 fornecidos na tarefa existem exclusivamente como
fixtures: série anual, balanço detalhado e coleta seletiva. Valores e percentuais
são transcritos sem recálculo, com referências documentais e estados não
aprovados; não constituem catálogo inicial, carga ou conteúdo do painel.

Para ESG-02B: confirmar os contratos e as convenções com a equipe responsável;
validar metodologias, unidades, perímetros e documentação; definir aprovação e
auditoria; projetar persistência/versionamento e adaptadores de entrada;
planejar a apresentação exclusiva de informações autorizadas. RF-1A continua
NO-GO OPERACIONAL até decisão explícita fora desta implementação.
