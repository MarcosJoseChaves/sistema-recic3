# Etapa H2C.2D — Especificação funcional de UVRs e associações

**Situação:** APROVADA

**Data da aprovação funcional:** 30/07/2026

## 1. Contexto e finalidade

Esta etapa descreve como o sistema utiliza atualmente as expressões “UVR” e
“associação” e propõe alternativas para uma evolução segura. Ela é
exclusivamente documental: não cria tabela, migration, cadastro, perfil ou
regra de autorização.

A análise foi feita somente com documentos e código versionado. O banco, o dump
externo, a `DATABASE_URL`, APIs e serviços externos não foram acessados.

As conclusões usam estas marcações:

- **Confirmado pelo código:** comportamento encontrado em rota, helper,
  template, script ou teste.
- **Confirmado pela documentação do schema:** estrutura já extraída e auditada
  em etapa anterior; o dump não foi relido nesta etapa.
- **Inferido:** interpretação provável, ainda sem confirmação funcional.
- **Decisão funcional aprovada:** regra confirmada pelo responsável do sistema.
- **Implementação técnica pendente:** desenho ou alteração ainda não executada.

## 2. Resumo em linguagem simples

Hoje, o sistema não possui uma lista oficial e central de UVRs. Em vez disso,
cada usuário e vários registros guardam o nome da UVR como texto. Para um
usuário comum, o sistema compara esse texto com o texto do registro para decidir
se ele pode consultar ou alterar o objeto.

Essa proteção já impede vários acessos cruzados, mas é frágil porque pequenas
diferenças de grafia podem criar divergências. Além disso, “associação” é outro
texto armazenado nos mesmos registros, sem uma tabela central ou relação
validada no servidor.

**Decisão funcional aprovada:** associação será a entidade institucional e UVR
será a unidade operacional, com cadastros próprios, identificadores estáveis,
aliases para reconhecer textos antigos e vínculos explícitos de usuários. Uma
associação poderá possuir várias UVRs.

## 3. Significado atual de UVR

**Confirmado pelo código:**

- `usuarios.uvr_acesso` é carregado no login e em `current_user`;
- para usuário comum, esse texto define o escopo permitido;
- filtros enviados pelo navegador são substituídos ou validados contra o texto
  da sessão;
- objetos são consultados por identificador e por sua coluna textual `uvr`;
- relatórios, extratos e downloads também recebem o escopo do servidor;
- administrador com `role == 'admin'` pode consultar sem filtro de UVR;
- usuário comum sem `uvr_acesso` falha de forma fechada nas operações
  protegidas;
- não existe SQL dirigido a uma tabela chamada `uvr`;
- não existem `uvr_id` ou `id_uvr` funcionais no sistema atual.

Portanto, UVR representa hoje simultaneamente:

1. escopo de autorização do usuário;
2. propriedade textual de diversos registros;
3. filtro de pesquisa e relatório;
4. opção exibida em formulários;
5. parte de algumas regras de unicidade;
6. informação histórica apresentada em exportações.

Isso não comprova qual é a definição institucional de UVR.

## 4. Significado atual de associação

**Confirmado pelo código e pela documentação do schema:**

- não existe cadastro central de associações;
- `associacao` é armazenada como texto em diversos registros;
- a interface preenche automaticamente a associação a partir de dois pares
  fixos de UVR e associação;
- esse mapeamento está duplicado no JavaScript;
- o servidor não valida que a associação recebida pertence à UVR;
- a associação não participa dos helpers atuais de autorização por objeto;
- `usuarios` não guarda associação;
- relatórios exibem associação, mas a proteção é feita pela UVR.

**Decisão funcional aprovada:** associação representa a entidade institucional
e UVR representa a unidade operacional. Uma associação pode possuir várias
UVRs. Uma UVR pode ser cadastrada durante a implantação, mas não pode operar
sem entidade responsável.

## 5. Inventário das estruturas textuais

### 5.1 Estruturas usadas atualmente pela aplicação

| Tabela/campo | Obrigatoriedade documentada | Origem atual | Finalidade | Comparação/normalização | Autorização | Risco |
|---|---|---|---|---|---|---|
| `usuarios.uvr_acesso` | Opcional | criação/edição por script ou dado existente | escopo do usuário | `strip()` e `casefold()` nos helpers novos; consultas antigas também usam igualdade exata | Sim, fonte principal | vazio nega operações comuns; grafia divergente pode negar ou separar dados |
| `cadastros.uvr` | Obrigatório | formulário; servidor força UVR da sessão para usuário comum | propriedade, filtro e unicidade | partes novas normalizam para comparação; várias consultas usam igualdade exata | Sim | variação textual e mudança de nome |
| `cadastros.associacao` | Opcional | preenchimento da tela ou valor enviado | exibição e contexto institucional | sem normalização ou validação central | Não diretamente | associação incompatível com UVR |
| `associados.uvr` | Obrigatório | formulário; servidor força UVR da sessão para usuário comum | propriedade e filtro | comparação textual mista | Sim | acesso e histórico dependem do texto |
| `associados.associacao` | Obrigatório no schema documentado | preenchimento da tela ou valor enviado | contexto institucional | sem validação central | Não diretamente | vínculo incorreto ou vazio em fluxos legados |
| `transacoes_financeiras.uvr` | Obrigatório | formulário ou sessão | propriedade, filtro, relatório e vínculo operacional | igualdade exata em várias consultas | Sim | alteração textual pode separar transação de cadastro/conta |
| `transacoes_financeiras.associacao` | Obrigatório | formulário | fotografia e relatório | sem validação central | Não diretamente | relatório institucional incoerente |
| `contas_correntes.uvr` | Obrigatório | formulário ou sessão | propriedade, filtro e parte da unicidade | igualdade exata em consultas financeiras | Sim | conta pode ficar invisível por grafia |
| `contas_correntes.associacao` | Obrigatório | formulário | contexto da conta e relatórios | sem validação central | Não diretamente | conta associada ao texto incorreto |
| `fluxo_caixa.uvr` | Obrigatório | formulário ou sessão | propriedade, filtro e conferência com conta/transação | igualdade exata | Sim | movimentação pode divergir de conta ou transação |
| `fluxo_caixa.associacao` | Obrigatório | formulário | fotografia institucional | sem validação central | Não diretamente | histórico inconsistente |
| `denuncias.uvr` | Opcional | formulário; rota desativada online | classificação e relatório | sem cadastro central | Escopo aplicado na gravação autenticada | valor vazio ou desconhecido |
| `denuncias.associacao` | Opcional | formulário | contexto institucional | sem validação central | Não diretamente | associação inconsistente |
| `patrimonio.uvr` | Opcional | formulário ou sessão | propriedade, pesquisa e escopo | consultas recentes usam normalização ou igualdade | Sim | patrimônio sem UVR ou grafia divergente |
| `patrimonio.associacao` | Opcional | preenchimento da tela | contexto institucional | sem validação central | Não diretamente | transferência e propriedade ambíguas |

### 5.2 Estruturas adicionais sem uso funcional identificado

**Confirmado pela documentação do schema:** há estruturas adicionais cujo
resumo demonstra classificação ou unicidade por UVR:

- `auditoria_passo1_observacoes.uvr`;
- `auditoria_passo2_observacoes.uvr`;
- `entrega_documentos_lotes.uvr`;
- `epi_estoque.uvr`;
- conjuntos de auditoria com chaves compostas que incluem UVR.

Essas tabelas não possuem uso SQL identificado no código versionado. Elas
permanecem preservadas e fora do modelo recomendado até a decisão de escopo da
H2C.2G.1. Como seus detalhes completos não estão reproduzidos no código, esta
etapa não inventa vínculos, obrigatoriedade ou migração para elas.

### 5.3 Entradas e saídas textuais

| Meio | Uso confirmado |
|---|---|
| Formulários HTML | seletores de UVR e campos de associação |
| JavaScript | lista fixa, preenchimento associação/UVR e filtro de seletores |
| Query string | filtros `uvr` em consultas, pesquisa e relatórios |
| JSON e formulário POST | UVR em transações, fluxo, denúncias e relatórios |
| Sessão/login | `current_user.uvr_acesso` |
| SQL | filtros por `uvr`, às vezes com `LOWER(TRIM(...))`, às vezes com igualdade exata |
| CSV/PDF | UVR e associação exibidas como informação histórica |
| Testes | valores fictícios comprovam substituição do filtro, negação cruzada e falha fechada |

## 6. Inventário funcional no código

| Arquivo/função ou grupo | Campo | Finalidade atual |
|---|---|---|
| `app.py`: `User`, `load_user`, `login` | `usuarios.uvr_acesso` | carregar o escopo na identidade autenticada |
| `app.py`: `_uvr_normalizada` | texto de UVR | remover espaços externos e comparar sem diferença de caixa |
| `app.py`: `_aplicar_escopo_uvr` | sessão e corpo recebido | substituir UVR do navegador pela UVR da sessão |
| `app.py`: `_escopo_uvr_consulta` | sessão e query string | restringir filtros; admin pode consultar globalmente |
| `app.py`: `_escopo_uvr_objeto`, `_consulta_objeto_por_uvr` | sessão e objeto | consultar ID dentro do escopo permitido |
| `app.py`: `_autorizar_objeto_por_uvr`, `_autorizar_objetos_da_uvr` | sessão e objetos relacionados | evitar acesso direto ou vínculo cruzado |
| `app.py`: `_inserir_solicitacao_escopada` | sessão e registro alvo | impedir solicitação sobre objeto de outra UVR |
| cadastros e associados | `uvr`, `associacao` | cadastro, busca, detalhe, edição, impressão e solicitação |
| contas correntes | `uvr`, `associacao` | cadastro, busca, detalhe, edição, extrato e unicidade |
| transações financeiras | `uvr`, `associacao` | cadastro, edição, detalhe, exclusão/solicitação e relatório |
| fluxo de caixa | `uvr`, `associacao` | gravação, conferência de objetos, resumo e movimentações |
| patrimônio | `uvr`, `associacao` | cadastro, busca, detalhe, edição e exclusão/solicitação |
| denúncias | `uvr`, `associacao` | registro desativado online e opções de relatório |
| relatórios e extratos | `uvr`, `associacao` | filtros, escopo, exportações e nomes seguros de arquivo |
| `templates/cadastro.html` | seletores e mapas fixos | interface, preenchimento automático e bloqueio visual |
| scripts de criação de usuário | `uvr_acesso` | cadastro manual histórico de usuário com uma UVR textual |
| testes H2A.3B | UVR fictícia | provar autorização por sessão, objeto e filtro |

O bloqueio visual do seletor ajuda a interface, mas a segurança depende dos
helpers e consultas do servidor.

## 7. UVR versus associação: conclusão possível

### O que está comprovado

- são armazenadas em colunas separadas;
- possuem tamanhos diferentes em várias tabelas;
- somente UVR participa da autorização atual;
- a interface associa dois pares fixos;
- não existe FK ou catálogo central para nenhuma delas;
- há registros e estruturas que usam apenas UVR, como `usuarios.uvr_acesso`;
- vários registros operacionais guardam os dois textos.

### O que não está comprovado

- se toda associação possui uma única UVR;
- se uma associação pode possuir várias UVRs;
- se uma UVR pode atender várias associações;
- se UVR pode existir sem associação;
- se associação representa obrigatoriamente uma pessoa jurídica;
- se contas e transações pertencem institucionalmente à associação ou
  operacionalmente à UVR.

Conclusão: o código atual não comprova a relação, mas a decisão funcional
aprovada estabelece que UVR e associação são entidades distintas e que a
relação operacional será de uma associação para uma ou mais UVRs.

## 8. Alternativa A — texto controlado

Descrição:

- manter os campos textuais;
- criar lista central de valores permitidos;
- continuar autorizando por comparação de texto.

Vantagens:

- menor alteração inicial;
- compatibilidade imediata;
- interface simples.

Riscos e limitações:

- nomes continuam funcionando como identificadores;
- renomear uma UVR exige cuidado em muitas tabelas;
- múltiplos vínculos por usuário ficam difíceis;
- aliases e histórico ficam frágeis;
- comparações exatas e normalizadas podem continuar divergentes;
- não resolve de forma completa UVR versus associação.

Avaliação: útil apenas como contenção temporária, não como modelo final.

## 9. Alternativa B — cadastro central de UVR

Descrição:

- criar entidade própria de UVR;
- adicionar identificadores opcionais gradualmente;
- preservar os textos antigos;
- vincular usuários a UVRs.

Vantagens:

- código e nome deixam de ser a identidade técnica;
- melhora autorização, normalização, filtros e auditoria;
- permite inativação e aliases;
- transição pode ser aditiva.

Riscos e limitações:

- não resolve sozinho o significado de associação;
- pode consolidar uma relação institucional errada;
- exige compatibilidade temporária texto/ID;
- vários módulos precisam mudar em pequenos incrementos.

Avaliação: melhor que o modelo A, mas incompleto se associação for uma entidade
real distinta.

## 10. Alternativa C — associação e UVR distintas

Descrição:

- cadastro de associação;
- cadastro de UVR;
- relação explícita entre elas;
- usuário ligado a uma ou mais UVRs;
- aliases para textos históricos;
- autorização futura por identificador.

Vantagens:

- separa entidade institucional de unidade operacional;
- suporta uma associação com várias unidades;
- melhora consolidação financeira e relatórios;
- permite administradores globais, por associação ou por UVR;
- é expansível e auditável.

Riscos:

- maior complexidade;
- exige decisão de cardinalidade;
- migração mais longa;
- erro de mapeamento pode ampliar ou bloquear acesso;
- necessita camada temporária de compatibilidade.

Avaliação: é o modelo tecnicamente mais completo, condicionado à validação
funcional dos conceitos.

## 11. Modelo funcional aprovado

Foi aprovada a alternativa C, com estas regras:

- associação representa a entidade institucional;
- UVR representa a unidade operacional;
- uma associação pode possuir uma ou mais UVRs;
- cada UVR operante possui uma associação responsável;
- usuário comum pode ter uma ou mais UVRs, com uma principal;
- pode existir administrador global;
- pode existir administrador limitado a uma associação;
- textos antigos permanecem como fotografia histórica;
- autorização futura usa identificadores, nunca texto do navegador;
- aliases resolvem grafias antigas, mas não concedem permissão sozinhos.

Esse modelo ainda não existe no banco ou no código.

## 12. Cadastro conceitual de associação

Somente se associação for confirmada como entidade distinta:

| Campo conceitual | Obrigatório | Natureza | Justificativa |
|---|---:|---|---|
| Identificador | Sim | Estrutural | vínculo estável, invisível ao usuário comum |
| Código estável | Sim, recomendado | Decisão funcional | integração e identificação sem depender do nome |
| Nome oficial | Sim | Estrutural | identificação institucional |
| Nome de exibição | Opcional | Recomendação | simplificar interface sem substituir o nome oficial |
| Sigla | Opcional | Decisão funcional | somente se houver sigla oficial |
| Situação ativa | Sim | Estrutural | inativação sem exclusão |
| Datas e usuários de auditoria | Sim | Estrutural | rastrear criação e mudança |

Não devem ser duplicados aqui dados jurídicos já mantidos em outro cadastro sem
uma necessidade comprovada.

## 13. Cadastro conceitual de UVR

| Campo conceitual | Obrigatório | Natureza | Justificativa |
|---|---:|---|---|
| Identificador | Sim | Estrutural | chave estável para autorização |
| Código estável | Sim, recomendado | Decisão funcional | integração e busca sem usar o nome |
| Nome oficial | Sim | Estrutural | identificação administrativa |
| Nome de exibição | Opcional | Recomendação | texto curto e amigável |
| Sigla | Opcional | Decisão funcional | somente se possuir significado oficial |
| Situação ativa | Sim | Estrutural | impedir novos usos sem apagar histórico |
| Associação vinculada | Sim para operação | Decisão aprovada | entidade responsável pela UVR |
| Datas e usuários de auditoria | Sim | Estrutural | autoria e rastreabilidade |

O identificador técnico não deve depender do nome exibido.

## 14. Aliases de UVR

Um alias reconhece um texto antigo e aponta para uma UVR oficial.

Campos conceituais:

- identificador;
- UVR de destino;
- texto original controlado;
- forma normalizada;
- origem conhecida;
- situação ativa;
- observação;
- início e fim de validade, quando necessário;
- criação e autoria.

Regras recomendadas:

1. um alias normalizado aponta para somente uma UVR;
2. remover espaços externos;
3. reduzir espaços internos repetidos;
4. comparar sem diferença entre maiúsculas e minúsculas;
5. definir com o usuário se acentos serão ignorados;
6. conflito bloqueia o mapeamento;
7. valor desconhecido entra em relatório de regularização;
8. alias nunca cria permissão sem vínculo de usuário validado;
9. alias histórico não é apagado;
10. nenhum alias artificial “Não classificado” será criado sem decisão.

## 15. Usuários comuns

| Opção | Benefício | Risco/impacto |
|---|---|---|
| Exatamente uma UVR | simples e próximo do modelo atual | não atende substituição, apoio ou atuação regional |
| UVR principal e secundárias | mantém padrão inicial e permite exceções claras | exige regra de seleção e auditoria |
| Várias UVRs sem principal | flexível | telas e criação de registros ficam ambíguas |
| Vínculo temporário | atende substituições | exige validade e revogação automática |
| Usuário sem UVR | permite conta antes da regularização | deve permanecer sem acesso a dados operacionais |

Decisão funcional aprovada:

- permitir uma ou mais UVRs;
- exigir uma UVR principal;
- permitir validade e inativação do vínculo;
- guardar histórico;
- negar acesso operacional quando o vínculo estiver ausente, ambíguo, inativo
  ou expirado.

## 16. Administradores

Modelos possíveis:

- administrador global;
- administrador de uma associação;
- administrador de uma ou mais UVRs;
- administrador de módulo dentro de determinado escopo;
- usuário comum com ações específicas.

Requisitos independentes da escolha:

- acesso global precisa ser explícito;
- o navegador não decide perfil ou escopo;
- Basic Auth da homologação não concede permissão interna;
- mudança de perfil ou vínculo é auditável;
- usuário não altera o próprio escopo;
- regra de menu e acesso direto deve ser a mesma;
- perfil e escopo são conceitos separados.

**Decisões funcionais aprovadas:** pode existir administrador global e pode
existir administrador limitado a uma associação. A criação, mudança de
responsável, inativação e reativação de UVR ficam inicialmente restritas ao
administrador global.

## 17. Usuário sem UVR ou com vínculo irregular

| Situação | Resposta recomendada |
|---|---|
| Sem UVR | permitir somente funções neutras e módulos globais expressamente permitidos; negar dados operacionais; orientar regularização |
| Texto desconhecido | negar operações por escopo e incluir em relatório administrativo |
| Alias ambíguo | negar; exigir decisão humana |
| UVR inativa | negar novas operações e acesso operacional, preservando auditoria |
| Vínculo expirado | negar a partir do fim da validade |
| Vários vínculos sem principal | permitir consulta somente quando o escopo for escolhido de forma segura; bloquear criação ambígua |

Princípio: **em dúvida, negar acesso**. Nunca promover para acesso global como
fallback.

## 18. Migração gradual dos textos

Fases conceituais:

1. aprovar os conceitos e cardinalidades;
2. criar cadastros centrais vazios;
3. cadastrar UVRs e associações oficiais por procedimento separado;
4. cadastrar aliases validados;
5. inventariar textos sem alterá-los;
6. gerar listas de correspondência segura, ambígua e desconhecida;
7. resolver ambiguidades humanamente;
8. adicionar identificadores opcionais;
9. preencher IDs somente com o mapa aprovado;
10. operar temporariamente com texto e ID;
11. comparar a decisão de autorização antiga e nova;
12. mudar autorização para identificador;
13. congelar novas escritas textuais livres;
14. tornar identificadores obrigatórios somente onde não houver pendência;
15. manter texto como fotografia histórica;
16. encerrar o fallback textual funcional.

Nenhuma fusão ou vinculação será feita apenas por similaridade.

## 19. Compatibilidade durante a transição

Recomendação:

- leitura prioriza ID validado;
- fallback textual existe somente para registro ainda não migrado;
- escrita dupla temporária deriva o texto do cadastro oficial, não do navegador;
- divergência entre ID e texto bloqueia a operação e gera pendência;
- autorização nunca usa alias isoladamente;
- relatórios identificam registros ainda não migrados;
- deve existir data e critério para encerrar o fallback;
- consultas antigas permanecem disponíveis apenas enquanto consumidores são
  migrados e testados.

Fallback textual permanente para autorização não é recomendado.

## 20. Impacto na autorização

| Área | Regra atual | Regra futura recomendada | Risco de transição |
|---|---|---|---|
| Login/current_user | carrega uma UVR textual | carregar vínculos ativos e escopo explícito | sessão com modelo antigo |
| Helpers | comparam texto normalizado ou exato | comparar IDs autorizados | divergência texto/ID |
| IDOR | consulta ID + UVR textual | consulta ID + vínculo de escopo | falha aberta durante compatibilidade |
| Filtros | navegador envia UVR; servidor força sessão | servidor oferece apenas IDs permitidos | confiar no filtro recebido |
| Relatórios | filtram texto | filtrar IDs e exibir fotografia | totais separados por grafia |
| Downloads | escopo textual validado antes de gerar | escopo por ID antes do arquivo | vazamento entre unidades |
| JSON/AJAX | query/corpo com texto | identificador validado no servidor | aceitar ID fora do escopo |
| Uploads | proteção depende do objeto pai | validar objeto pai por ID e escopo | arquivo ligado a objeto alheio |

Todas as rotas devem continuar protegidas por autenticação e autorização
interna. CSRF e Basic Auth não substituem essas regras.

## 21. Impacto nos cadastros operacionais

| Área | Impacto recomendado |
|---|---|
| Clientes/fornecedores (`cadastros`) | vínculo opcional durante migração; novos registros usam UVR validada; associação derivada do cadastro oficial |
| Associados | vínculo à UVR operacional; associação obtida pela relação aprovada |
| Contas correntes | decidir se pertencem à UVR ou à associação antes de adicionar ID |
| Transações | guardar escopo estruturado e fotografia textual; validar todos os objetos relacionados |
| Fluxo de caixa | conta, cadastro e transação precisam pertencer a escopos compatíveis |
| Denúncias | rota permanece desativada online; integração só após decisão do módulo |

## 22. Impacto no patrimônio

**Decisão funcional aprovada:** patrimônio separará responsabilidade
institucional da unidade de uso. Transferências entre UVRs usarão procedimento
formal e auditado. Os textos `patrimonio.uvr` e `patrimonio.associacao`
permanecerão durante a transição, e o histórico nunca será sobrescrito
silenciosamente.

**Implementação técnica pendente:** detalhar eventos, bloqueadores de baixa,
campos obrigatórios da transferência e consultas de usuários com múltiplas
UVRs.

## 23. Impacto financeiro

**Decisão funcional aprovada:** conta corrente pertence à associação. UVR pode
ser usada como dimensão gerencial. Módulos financeiros permanecem segregados
por associação e/ou UVR.

- não alterar regras financeiras antes das respostas;
- preservar fotografias textuais;
- nunca recalcular história por mudança cadastral;
- validar conta, cadastro, transação e fluxo no mesmo escopo;
- relatórios consolidados exigem permissão explícita.

**Implementação técnica pendente:** detalhar contas compartilhadas, dimensões
gerenciais, transferências e relatórios consolidados.

## 24. Impacto no módulo Fiscalização de Contratos

**Confirmado pelo código e documentos:** as 105 rotas funcionais permanecem
administrativas e o módulo possui seu próprio conjunto de empresas, contratos,
servidores, documentos, planilhas, ativos, fiscalizações, medições e atestes.

Não há vínculo funcional confirmado com UVR.

**Decisão funcional aprovada:** Fiscalização de Contratos e os cadastros
municipais relacionados permanecem globais. Não haverá dependência obrigatória
de UVR. As 105 rotas continuam administrativas até etapa própria de permissões.

## 25. Segurança e auditoria

Requisitos conceituais:

- criação, alteração, inativação e reativação de UVR geram histórico;
- mudança de vínculo de usuário guarda anterior, novo, período, motivo e autor;
- transferência de objeto guarda origem e destino;
- não existe exclusão física cotidiana;
- alias usado historicamente não é apagado;
- usuário não altera o próprio vínculo;
- acesso global não é implícito;
- tentativa de acesso cruzado continua bloqueada;
- objeto inexistente e objeto alheio permanecem indistinguíveis quando isso
  reduzir exposição;
- logs usam identificadores técnicos mínimos, sem dados sensíveis;
- atualização de múltiplos vínculos ocorre em transação única.

## 26. Inativação e reativação

UVR:

- pode ser inativada;
- deixa de aceitar novos registros;
- usuários ativos precisam ser listados e regularizados;
- registros históricos permanecem ligados;
- aliases permanecem para leitura histórica;
- reativação exige autorização e auditoria.

Associação:

- pode ser inativada se o conceito for aprovado;
- recomendação é bloquear enquanto possuir UVRs ativas;
- não inativa UVRs automaticamente;
- histórico permanece.

**Decisão funcional aprovada:** UVR com usuários ou operações ativas não pode
ser inativada antes da regularização dos bloqueadores. O histórico permanece
integralmente preservado. Associação com UVRs ativas também não será inativada
automaticamente.

## 27. Nomes, códigos e normalização

- identificador interno: automático e invisível ao usuário;
- código estável: não depende do nome e não é reutilizado;
- nome oficial: obrigatório;
- nome de exibição: opcional;
- sigla: apenas quando oficial;
- remover espaços externos;
- reduzir espaços internos repetidos;
- comparar sem diferença de caixa;
- aliases serão comparados ignorando capitalização, acentos e espaços
  excedentes, preservando a grafia oficial;
- unicidade do código é obrigatória;
- unicidade do nome normalizado é recomendada dentro do escopo aprovado;
- alteração de nome não altera identificador;
- nome antigo vira alias ou fotografia histórica.

Nenhum código ou nome real é definido nesta especificação.

## 28. Dados iniciais

Recomendação:

- a migration-base cria somente estruturas vazias;
- UVRs, associações e aliases reais não entram no arquivo da baseline;
- carga inicial é procedimento separado, revisável e específico do ambiente;
- nenhuma lista fixa do template é copiada automaticamente;
- nenhum CSV antigo é fonte automática;
- usuário administrador é criado por procedimento seguro separado;
- dados fictícios de homologação ficam em carga própria e nunca em produção.

## 29. Decisões funcionais aprovadas

1. UVR e associação são entidades distintas.
2. Uma associação pode possuir várias UVRs.
3. Uma UVR pode ser cadastrada em implantação, mas não pode operar sem entidade
   responsável.
4. Usuário comum pode acessar uma ou mais UVRs, com uma UVR principal.
5. Pode existir administrador global.
6. Pode existir administrador limitado a uma associação.
7. Usuário sem UVR válida acessa somente funções neutras e módulos globais
   expressamente permitidos.
8. Criação, mudança de responsável, inativação e reativação de UVR ficam
   inicialmente sob administrador global.
9. UVR inativa mantém integralmente seu histórico.
10. Transferências entre UVRs usam procedimento formal e auditado.
11. Fiscalização de Contratos e cadastros municipais relacionados permanecem
    globais; módulos operacionais e financeiros são segregados por associação
    e/ou UVR.
12. A migration-base nasce sem associações, UVRs, aliases ou usuários reais.
13. Texto desconhecido ou ambíguo não concede acesso e segue para
    regularização.
14. Associação e UVR possuem identificadores e códigos estáveis próprios.
15. A interface usa “Associação” e “UVR — Unidade de Valorização de
    Recicláveis”.
16. Conta corrente pertence à associação; UVR pode ser dimensão gerencial.
17. Patrimônio separa responsabilidade institucional da unidade de uso.
18. Fiscalização de Contratos continua global.
19. Aliases ignoram capitalização, acentos e espaços excedentes na comparação,
    preservando a grafia oficial.
20. UVR com usuários ou operações ativas não pode ser inativada antes da
    regularização dos bloqueadores.

## 30. Riscos

- mapear grafias semelhantes para unidade errada;
- conceder acesso cruzado durante a transição;
- usar associação como sinônimo sem confirmação;
- alterar relatórios históricos ao renomear cadastro;
- manter fallback textual indefinidamente;
- duplicar a verdade em texto e identificador;
- inativar UVR com usuários ou objetos ativos;
- forçar UVR no módulo Fiscalização sem necessidade;
- inserir dados reais na baseline;
- usar listas fixas ou scripts antigos como fonte oficial.

### 30.1 Implementação técnica pendente

As decisões acima ainda exigem:

- desenho SQL e nomes finais das estruturas;
- tipos, constraints, índices e regras transacionais;
- migration aditiva e plano de reversão;
- inventário e migração assistida dos textos;
- implementação de aliases;
- vínculos de usuário, UVR principal e escopos administrativos;
- histórico e auditoria;
- interfaces administrativas;
- testes de autorização, compatibilidade e homologação.

Nenhuma dessas estruturas ou regras foi implementada nesta etapa.

## 31. Pendências e sequência recomendada

Antes de desenhar SQL:

1. caracterizar os valores textuais existentes em ambiente autorizado;
2. desenhar estruturas técnicas sem dados reais;
3. detalhar escopos na H2C.2E;
4. definir eventos e auditoria;
5. planejar migration aditiva e compatibilidade;
6. homologar com dados fictícios antes de qualquer uso real.

Próxima etapa após validação desta especificação:

**H2C.2E — especificação de usuários e permissões**, usando o modelo de escopo
que vier a ser aprovado.

## 32. Critérios de aceite da H2C.2D

- significado atual e limitações documentados;
- campos textuais e usos funcionais inventariados;
- UVR e associação não tratados como sinônimos sem decisão;
- três modelos comparados;
- vinte decisões funcionais registradas como aprovadas;
- usuários, administradores e situações inválidas especificados;
- aliases e migração gradual definidos;
- impactos por módulo analisados;
- segurança, inativação e dados iniciais definidos;
- implementação técnica separada das decisões funcionais;
- nenhum código, SQL, migration, banco, dump, API ou deploy acessado.
