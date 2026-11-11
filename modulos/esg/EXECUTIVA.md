# ESG-03C1 — visão executiva demonstrativa

**DADOS FICTICIOS — NAO OFICIAIS. RF-1A: NO-GO OPERACIONAL.**

## Entrada explícita e isolamento

A rota GET `/esg/demonstracao/executiva` é registrada exclusivamente por
`criar_aplicacao_demonstrativa(repositorio)`, com a mesma guarda administrativa
Flask-Login. Não foi adicionada ao Blueprint operacional nem a `app.py`.
O repositório exigido pela fábrica continua sendo a instância demonstrativa em
memória. Não há seleção por ambiente, carga implícita, singleton ou fallback.

Para o cenário executivo, chamar explicitamente
`criar_repositorio_executivo_sintetico()` e injetar seu retorno na fábrica.
Autenticar o `test_client` com a fixture administrativa demonstrativa existente.
A fábrica não carrega exemplos por conta própria; um repositório vazio mostra
ausência de dados nas três dimensões. Os exemplos anteriores da ESG-03A seguem
inalterados e podem ser usados na mesma visão.

A tela executiva não contém formulário, token, ação de cadastro, aprovação ou
alteração. POST nessa rota, mesmo com CSRF válido, retorna 405 sem consulta ou
escrita. Os links de consulta apontam aos detalhes administrativos demonstrativos
já existentes e preservam o período quando há registro. As rotas administrativas
anteriores continuam disponíveis na aplicação de demonstração; não são novas
permissões concedidas pela tela executiva.

## Contrato de apresentação

`executivo.py` define dataclasses congeladas:

- `CartaoExecutivo`: definição original, registro original opcional e tupla das
  evidências efetivamente vinculadas a esse UUID de registro.
- `VisaoExecutiva`: tupla de cartões, agrupamento por `DimensaoESG` e seleção
  explícita de cartões sem registro ou sem valor.

`montar_visao_executiva(repositorio)` usa apenas `listar_definicoes()` e
`listar_registros(codigo, perimetro)`. Revalida os vínculos pelo serviço existente;
definições divergentes/duplicadas ou série ausente são erro, nunca lista vazia.
A rota apresenta falhas conhecidas como 503 genérico, sem detalhes internos.

Cada cartão mantém identificador e nome da definição, dimensão, unidade,
perímetro, descrição, metodologia, fontes, estados e, quando existente, UUID,
valor Decimal/None, período, data de referência e evidências do registro. Fontes
de aprovação existentes são preservadas como metadados, sem inferir aprovação.
Informações extensas ficam em expansão nativa de contexto, sem JavaScript.

Há um cartão por registro. Definição sem registros ganha cartão sem medição,
período ou situação de registro presumidos. `None` mostra **Dado indisponível**;
`Decimal("0")` mostra zero. Nenhuma precisão é perdida. Não se escolhe o registro
mais recente silenciosamente, nem se agregam anos/meses, unidades, perímetros,
denominadores, percentuais ou resultados. A lista de lacunas não é um cálculo
de desempenho e não produz totais ou índices de governança.

## Exemplos sintéticos

As definições são propostas e os registros são rascunhos, sem fontes de aprovação.
Os anos abaixo também são fictícios: não são resultado, meta ou projeção.
Cada indicador tem perímetro `exec-recorte-<tema>` e fonte própria
`exec-fonte-<tema>`, com referência explicitamente sintética sem emissor
institucional. Unidades não representam métricas municipais homologadas.

| Dimensão | Tema | Valor sintético | Período |
| --- | --- | --- | --- |
| Ambiental | Destinação de resíduos | `0 ud` | 2034 anual |
| Ambiental | Recuperação de materiais | `12.3400 ud` | 2034 anual |
| Ambiental | Compostagem | Dado indisponível (`None`) | 2034-01 |
| Social | Participação das associações | `3 ass-fict` | 2034-01 |
| Social | Inclusão produtiva | Dado indisponível (`None`) | 2034 anual |
| Social | Capacitações | Sem registro ou medição | Indisponível |
| Governança | Rastreabilidade documental | Dado indisponível (`None`) | 2034 anual |

Há evidências sintéticas vinculadas a destinação, recuperação, participação e
rastreabilidade. Ausência de evidência nos demais cartões é identificada, sem
inventar documentos. A seção de governança contextualiza fontes, evidências,
validação e lista indicadores sem dados suficientes, com período e perímetro.
Não há reutilização de valores históricos municipais de 2025 ou combinação das
bases 77,26%, 78,35% e 38,25%.

## Validação e limites

`tests/test_esg_executivo.py` cobre modelo e tela com fixtures, autenticação,
CSRF válido na rejeição de POST, leitores sem interface de escrita, ausência,
zero, Decimal, períodos, perímetros, fontes/evidências, estados não aprovados,
imutabilidade, falhas seguras e isolamento da aplicação normal. As barreiras
internas começam em `setUp`; a execução deve usar o executor auditado que
instala barreiras externas antes de importar qualquer módulo de teste.

Executar exclusivamente a suíte ESG completa sob esse executor, com Python
`C:\sistema-recic3\.venv\Scripts\python.exe` (3.12.10), sem servidor ou acesso
externo. Não importar `app.py` para usar a visão; sua regressão histórica só pode
ser executada com os mocks e barreiras auditados. O arquivo SQL da regressão é
inspecionado como texto, nunca executado.

Uma prévia estática pode ser exportada pelo processo auditado, com avisos
fictício/estático, CSS local e navegação apenas entre arquivos existentes.
Nenhum navegador é aberto automaticamente. Renderização via test_client não
comprova aparência projetada, contraste efetivo, responsividade, foco ou zoom.
Avaliação visual e validação PostgreSQL/concorrência permanecem pendentes em
etapas separadas autorizadas. Nenhuma liberação operacional decorre desta tela.

## Resultado desta etapa

O executor isolado confirmou Python **3.12.10** e executou **128 testes ESG**:
116 anteriores e 12 novos, todos aprovados, com zero falhas, erros ou ignorados.
Nenhuma barreira externa foi acionada. A regressão conferiu a navegação aos
detalhes existentes, ausência de escrita no GET, rejeição de POST e ausência
da rota na aplicação normal.

A prévia estática foi gravada fora do repositório em
`C:\Users\USUARIO\RECIC3_ESG03C1_PREVIA_20261110_6b0e39a2\00-executiva.html`,
com sete detalhes locais e apenas os dois CSS necessários. Os **9 testes finais
de exportação passaram**, sem falhas, erros ou ignorados. O exportador removeu
tokens/ações de servidor e preservou os avisos fictício/estático e links locais.
Uma referência antiga de nome de arquivo em uma verificação do executor foi
corrigida antes dessa exportação final, sem mudança no aplicativo. Nenhum
navegador foi aberto; não houve avaliação visual da tela projetada.
