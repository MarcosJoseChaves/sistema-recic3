# ESG-03A — fluxo demonstrativo offline

**DADOS FICTICIOS — NAO OFICIAIS. RF-1A permanece NO-GO OPERACIONAL.**

Não há alteração em `app.py`, na persistência PostgreSQL, nos contratos ou no
SQL. Importar os módulos demonstrativos não cria aplicação, repositório,
conexão, carga ou servidor. Não há singleton, seleção por variável de ambiente,
fallback, comando de servidor ou integração com a aplicação principal.

## Componentes e uso explícito

- `demonstracao.RepositorioDemonstrativoESG()` cria um repositório vazio, por
  instância, com os cinco métodos consumidos pela interface ESG existente.
- `demonstracao.criar_repositorio_com_exemplos()` cria uma instância separada
  com exemplos inteiramente sintéticos. Só carrega ao ser chamada explicitamente.
- `demonstracao_web.criar_aplicacao_demonstrativa(repositorio)` recebe somente
  esse tipo demonstrativo e constrói uma aplicação `TESTING=True`, destinada
  exclusivamente a `test_client`. Não importa `app.py` nem chama `run()`.
- `autenticar_administrador_demonstrativo(cliente)` prepara explicitamente uma
  sessão assinada de administrador fictício (ID 1), somente nessa aplicação de
  teste. Não há rota de login automático, usuário real ou credencial carregada.

O roteiro reproduzível está em `tests/test_esg_demonstracao.py`. Ele abre painel
e lista; envia uma proposta com token CSRF; consulta o detalhe; envia um rascunho
com fonte/evidência; e verifica a consulta final, os UUIDs, o Decimal e a autoria.
Tokens são obtidos na própria sessão de teste; não há isenção de CSRF.

## Integridade em memória

As dataclasses imutáveis da ESG-02A são preservadas, sem float, quantização ou
conversão de unidade. `None` continua diferente de `Decimal("0")`; UUIDs são
mantidos. O serviço existente valida vínculos e ordena os registros.

Definições aceitam somente `proposto`; registros somente `rascunho`, ambos sem
fonte de aprovação. A autoria recebida deve ser inteiro positivo, nunca bool,
e é registrada em snapshot de leitura. Nas rotas ela vem da sessão autenticada.

Códigos, UUIDs de registros/evidências e períodos por indicador são únicos.
Ano completo e mês coexistem, sem agregação. Perímetros exigem igualdade de
código e descrição. Fontes, unidades e perímetros de mesma identidade só podem
ser reutilizados se idênticos. Conflitos não sobrescrevem dados. Todas as
validações precedem as mudanças nos mapas, inclusive fontes e evidências.

Os dados desaparecem quando a instância/processo termina. Esse armazenamento
tem finalidade sequencial de demonstração, sem durabilidade, locks, transações
PostgreSQL ou garantia de concorrência entre threads/processos.

## Exemplos e identificação

O único indicador de exemplo é `demo-material`, com unidade fictícia `ud`,
perímetro `demo-recorte` e fonte descrita como documento sintético sem emissor
institucional. Não contém números reais municipais ou fontes institucionais.

| Período sintético | Valor | Finalidade |
| --- | --- | --- |
| 2030 anual | `Decimal("0")` | zero informado |
| 2030-01 | `None` | ausência de valor |
| 2030-02 | `Decimal("12.3400")` | precisão e casas decimais preservadas |

Cada exemplo possui fonte e evidência com UUID próprio vinculado ao registro.
Os anos são parte da ficção; não representam resultado, meta ou projeção.
O cadastro via POST usa outra proposta sintética e `Decimal("7.2500")`.

O aviso literal **DADOS FICTICIOS — NAO OFICIAIS** aparece em todas as páginas
HTML da aplicação demonstrativa, inclusive login e erros. Fontes e nomes dos
exemplos também o contêm. O layout normal só mostra o aviso quando recebe o
contexto da fábrica demonstrativa; não ativa dados ou dependências. A regressão
verifica que ele não aparece no modo normal.

## Validação e limites

Executar somente a suíte explícita ESG sob o executor isolado auditado R3/R4,
com Python `C:\sistema-recic3\.venv\Scripts\python.exe` (3.12.10). Incluir os
cinco módulos anteriores e `tests.test_esg_demonstracao`. As barreiras externas
devem existir antes dos imports: rede, PostgreSQL, arquivos sensíveis, escrita,
subprocessos e servidores bloqueados; ambiente sem credenciais e dotenv
substituído. Os mocks internos da nova suíte não comprovam esse isolamento de
importação por si sós. A suíte não deve ser iniciada diretamente sem o executor.

O `test_client` processa requisições em memória: não abre porta TCP nem permite
uma apresentação navegável em outro dispositivo. A renderização dos templates
comprova conteúdo e fluxo, não aparência, acessibilidade ou responsividade em
navegador. A aplicação principal continua sem repositório ESG configurado.

Na validação ESG-03A, o executor auditado foi reutilizado em processo isolado,
com as barreiras instaladas antes de importar os seis módulos de teste. Foram
encontrados e executados **115 testes: 100 anteriores e 15 novos**, todos
aprovados; **0 falhas, 0 erros, 0 ignorados**, código de saída 0. Nenhuma
barreira externa foi acionada. O interpretador confirmou Python **3.12.10**.

O caso end-to-end comprovou os seguintes passos sem servidor:

| Passo | Resultado observado |
| --- | --- |
| GET `/esg` | 200, painel da instância vazia com aviso fictício |
| GET `/esg/indicadores` | 200, lista vazia com aviso fictício |
| POST `/esg/indicadores/novo` com CSRF | 303, proposta salva com autor da sessão |
| GET `/esg/indicadores/demo-nova/detalhe` | 200, proposta sem aprovação |
| POST `/esg/indicadores/demo-nova/registros/novo` com CSRF | 303, rascunho e evidência salvos |
| GET do detalhe filtrado após o POST | 200, Decimal `7.2500`, fonte e evidência vinculada |

As páginas de cadastro, login, erro e a consulta dos três exemplos também foram
renderizadas pelos testes. A regressão confirmou modo normal sem aviso fictício
e sem carga demonstrativa. Esses resultados não representam inspeção visual.

Permanecem pendentes avaliação visual posterior em ambiente explicitamente
autorizado, revisão humana dos textos e, em tarefa separada, validação de
PostgreSQL/concorrência. Nenhuma aprovação institucional ou liberação operacional
decorre da demonstração.
