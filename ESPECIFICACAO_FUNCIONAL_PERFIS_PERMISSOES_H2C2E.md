# Etapa H2C.2E — Especificação funcional de perfis, permissões e escopos

**Situação:** APROVADA
**Data da aprovação funcional:** 30/07/2026

## 1. Situação e finalidade

Este documento descreve o funcionamento atual da autorização e apresenta
o modelo funcional aprovado para sua evolução. Ele não define tabelas, não
contém SQL e não altera o sistema.

Premissas funcionais já aprovadas:

- associação é a entidade institucional;
- UVR é a unidade operacional;
- uma associação pode possuir várias UVRs;
- um usuário pode possuir uma ou mais UVRs e o usuário operacional possui uma
  UVR principal;
- podem existir administradores globais e administradores limitados a uma
  associação;
- usuário sem vínculo válido não recebe acesso operacional;
- Fiscalização de Contratos permanece global;
- a autorização futura usará identificadores, não nomes digitados;
- mudanças de perfil e escopo serão auditadas.

## 2. Fontes e limites da análise

Foram analisados integralmente os documentos funcionais H2C.2A a H2C.2D, o mapa
do schema, o relatório de comparação, os planos de migration-base e homologação
e o status de desenvolvimento. No código versionado foram examinados login,
carregamento de sessão, `current_user`, decorators, verificações de papel e UVR,
consultas por objeto, páginas, JSON, relatórios, arquivos e o Blueprint de
Fiscalização.

Não houve leitura de dump externo, acesso a banco, uso de `.env`, execução de
SQL, migration, API ou deploy.

## 3. Modelo atual em linguagem simples

Hoje o sistema responde essencialmente a duas perguntas:

1. o usuário é administrador?
2. se não for, qual texto está em `usuarios.uvr_acesso`?

O campo `usuarios.role` é texto livre, sem catálogo ou restrição de banco
confirmada. O código versionado cria e reconhece dois valores operacionais:
`admin` e `user`. O valor `admin` libera funções globais; qualquer valor diferente
de `admin` é tratado, na maior parte do sistema, como usuário comum sujeito à
UVR textual. Não existe ainda permissão formal por módulo e ação.

O módulo Fiscalização usa uma regra mais simples: todas as suas 105 rotas
funcionais permanecem administrativas. Portanto, o vínculo de um servidor como
gestor ou fiscal de um contrato não concede login nem acesso ao módulo.

## 4. Inventário dos mecanismos atuais

| Arquivo/local | Mecanismo | Condição e escopo | Negativa | Classificação e limitação |
|---|---|---|---|---|
| `app.py`, `User` e `load_user` | sessão interna | carrega ID, usuário, `role` e `uvr_acesso`; exige `usuarios.ativo = TRUE` a cada carga | sessão deixa de ser reconhecida | regra central; modelo ainda textual |
| `app.py`, `login` | autenticação | consulta usuário ativo e valida hash de senha | mensagem genérica | regra central; autenticar não define autorização fina |
| Flask-Login | `login_required` | exige sessão interna | HTML é encaminhado ao login | regra central; não verifica módulo, ação ou objeto |
| `app.py`, `login_json_required` | login para JSON | exige `is_authenticated` | JSON 401 | regra central de formato; não concede permissão |
| `permissions.py`, `admin_required` | administrador HTML | `role == "admin"` após login | 403 | regra central do módulo; globalidade depende de texto |
| `permissions.py`, `admin_json_required` | administrador JSON | sessão e `role == "admin"` | JSON 401 ou 403 | regra central do módulo |
| `app.py`, condições com `current_user.role` | desvios administrativos | administrador ignora ou amplia filtros; usuário comum fica limitado | varia por rota | regras locais/duplicadas; difíceis de auditar em conjunto |
| `app.py`, `_aplicar_escopo_uvr` | escopo de entrada | substitui UVR recebida pela UVR da sessão e rejeita divergência | 403 HTML/JSON | regra central de transição; depende de nome textual |
| `app.py`, `_escopo_uvr_consulta` | filtros e relatórios | admin pode consultar todas; comum usa sua UVR | JSON 403 | regra central; uma única UVR por usuário |
| `app.py`, `_escopo_uvr_objeto` | preparação da consulta | admin sem filtro; comum exige UVR | 404 equivalente | regra central; falha fechada |
| `app.py`, `_consulta_objeto_por_uvr` | consulta por objeto | combina ID e UVR no SQL para usuário comum | objeto ausente | regra central; protege contra acesso por troca de ID |
| `app.py`, `_autorizar_objeto_por_uvr` | autorização por objeto | confirma ID dentro da UVR da sessão | 404 equivalente | regra central; lista fechada de tabelas |
| `app.py`, `_autorizar_objetos_da_uvr` | autorização composta | verifica vários objetos financeiros na mesma UVR | 403 | regra central; ainda textual e específica |
| `app.py`, helpers de relatório/extrato | relatórios/exportações | força UVR e confere entidade/conta | JSON 403 | regra local especializada |
| `app.py`, `_inserir_solicitacao_escopada` | solicitação de alteração | cria pedido somente se objeto ainda pertence à UVR | operação não criada | regra local especializada; tabela escolhida por lista fixa |
| `templates/cadastro.html` | visibilidade da interface | mostra áreas administrativas quando `role == "admin"` | botão oculto | apoio visual; nunca substitui a verificação do servidor |
| `desativada_online` | indisponibilidade ambiental | responde 404 em homologação/produção | 404 | exceção operacional; não é perfil |
| Basic Auth da homologação | barreira global externa | exige credencial ambiental antes da aplicação | desafio Basic | camada independente; não concede login ou permissão interna |
| CSRF | integridade da requisição | exige token válido em operações mutáveis | requisição recusada | proteção independente; não autentica nem autoriza |

Além das regras centrais, há condições manuais de `role` e `uvr_acesso` dentro
de rotas antigas. Elas funcionam como regras locais ou legadas e aumentam o
risco de uma rota nova esquecer parte da autorização.

## 5. Papéis realmente existentes hoje

| Valor | Finalidade aparente | Permissão efetiva | Escopo | Definição e verificação | Situação e risco |
|---|---|---|---|---|---|
| `admin` | administração geral | áreas administrativas, visão global nas regras antigas e todas as rotas de Fiscalização | global implícito | script administrativo, `app.py`, decorator do módulo e template | utilizado; texto livre transforma a palavra em chave de segurança |
| `user` | operação cotidiana | rotas autenticadas e operações permitidas pela lógica local | uma UVR textual em `uvr_acesso` | scripts de criação; é tratado no código como “não admin” | utilizado; permissões não são explícitas e uma única UVR não atende ao modelo futuro |

Outros nomes aparecem em documentos como propostas e cargos, não como valores
atuais confirmados de `usuarios.role`. Como o campo não possui catálogo ou
`CHECK` confirmado, o banco poderia conter grafias diferentes, mas isso não foi
verificado porque esta etapa proíbe acesso ao banco.

## 6. Problemas e riscos atuais

- uma única palavra mistura responsabilidade e nível de alcance;
- `admin` implica globalidade sem uma concessão global separada;
- todo não administrador tende a receber o mesmo conjunto amplo de operações;
- `uvr_acesso` aceita apenas um texto e não representa várias UVRs;
- nomes equivalentes, espaços e grafias podem causar divergência;
- regras manuais distribuídas podem produzir comportamentos diferentes;
- ocultar um botão não protege acesso direto por URL;
- não há catálogo explícito de permissões para revisão e auditoria;
- não há representação formal de administrador de associação;
- cargos de servidor na Fiscalização não equivalem a contas e permissões;
- revogação e expiração de vínculos ainda não são conceitos formais;
- não há proteção formal confirmada para o último administrador global;
- os campos de recuperação de senha existem, mas o fluxo não foi implementado.

As correções anteriores de autenticação e autorização por objeto continuam
válidas; esta especificação não declara que os antigos bloqueadores permanecem
abertos.

## 7. Perfil, permissão e escopo

**Perfil** é um conjunto reutilizável de responsabilidades, como “operador de
UVR”. **Permissão** é uma ação concreta, como “editar patrimônio”. **Escopo** é
onde essa ação vale: globalmente, em uma associação, em determinadas UVRs, em
um objeto atribuído ou apenas no próprio registro.

Exemplo: duas pessoas podem ter o perfil “responsável financeiro”, mas uma atuar
em duas UVRs e outra em toda uma associação. O perfil pode ser igual, enquanto o
escopo é diferente. Por isso um único `role` não representa as combinações
futuras.

## 8. Princípios obrigatórios

1. negar por padrão;
2. aplicar menor privilégio;
3. autorizar sempre no servidor;
4. não aceitar perfil ou escopo enviados pelo navegador como fonte de verdade;
5. nunca transformar ausência de vínculo em acesso global;
6. manter Basic Auth independente da permissão interna;
7. separar autenticação de autorização;
8. exigir perfil/permissão e escopo válidos;
9. tornar acesso global uma concessão explícita;
10. bloquear usuário inativo;
11. não revelar objeto de outro escopo;
12. auditar concessão, alteração e revogação;
13. impedir autoelevação;
14. reservar permissões sensíveis a administrador autorizado.

## 9. Alternativas de modelo

| Modelo | Vantagens | Limitações e riscos | Avaliação |
|---|---|---|---|
| A — perfis fixos no código | simples de entender, testar e implantar | toda mudança exige código; combinações crescem rapidamente; revisão menos flexível | aceitável somente para poucos perfis estáveis |
| B — perfis cadastráveis | grande flexibilidade; administração pela interface | maior complexidade; risco de perfil excessivo ou mal configurado; exige auditoria e validações fortes | poderoso, mas pesado para a primeira evolução |
| C — híbrido | perfis institucionais protegidos, permissões técnicas catalogadas e escopos separados; permite expansão controlada | exige bom desenho, catálogo estável e interface segura | **modelo funcional aprovado** |

## 10. Perfis funcionais aprovados

| Perfil aprovado | Finalidade e escopo esperado | Módulos/ações típicas | Risco e acumulação |
|---|---|---|---|
| Administrador global | governança de todo o sistema | usuários, entidades, perfis, escopos e delegações | atos especializados exigem permissão própria; acumulação desnecessária deve ser evitada |
| Administrador de associação | gestão institucional de uma associação e suas UVRs | usuários vinculados, UVRs, patrimônio institucional e relatórios consolidados | não pode escapar da associação; pode acumular função operacional local |
| Gestor de UVR | coordenação de uma ou mais UVRs | cadastros, associados, patrimônio em uso, documentos e relatórios locais | não implica administração da associação |
| Operador de UVR | execução cotidiana | criar e editar registros dos módulos atribuídos | sem usuários, perfis, transferências institucionais ou exclusão física |
| Usuário de consulta | leitura controlada | consultas e histórico no escopo | exportar, baixar e ver dados pessoais/financeiros exigem permissões separadas |
| Responsável financeiro | operação financeira no escopo | contas, fluxo, transações e relatórios autorizados | alta sensibilidade; segregação de aprovação pode ser necessária |
| Responsável por patrimônio | gestão patrimonial no escopo | cadastrar, movimentar, transferir e inativar conforme autorização | transferência entre escopos exige duas pontas autorizadas |
| Responsável por associados e cadastros | manutenção de associados e cadastros | consultar e alterar dados pessoais no escopo | proteção de dados pessoais; exportação separada |
| Gestor do catálogo | curadoria central do catálogo | grupos, subgrupos, produtos e classificações | alterações afetam vários módulos; exige histórico |
| Administrador da Fiscalização | administração funcional do módulo global | estruturas centrais e delegações internas autorizadas | não equivale a Administrador Global |
| Fiscal de contrato | registrar acompanhamento do objeto atribuído | fiscalização, ocorrência e evidência | não deve ganhar administração global do módulo |
| Gestor de contrato | coordenar contrato atribuído | responsáveis, documentos, medições e fluxos permitidos | separar criação, aprovação e ateste |
| Consulta/Auditoria da Fiscalização | inspeção independente do módulo | leitura de histórico, eventos e relatórios | exportação e download permanecem separados |

Esses nomes representam responsabilidades institucionais, não cargos pessoais.

## 11. Administrador global

Administra usuários, associações, UVRs, perfis, escopos e delegações, pode
corrigir vínculos e reativar contas conforme as regras técnicas futuras. Não
recebe automaticamente autorização para atos especializados de negócio. O
alcance global deve ser explícito e nunca inferido de UVR vazia.

Proteções recomendadas:

- impedir autoelevação e compartilhamento de conta;
- impedir inativação, revogação ou expiração do último administrador global;
- exigir confirmação reforçada e justificativa em ações críticas;
- registrar antes e depois de cada mudança;
- recomendar autenticação multifator em etapa futura.

## 12. Administrador de associação

O escopo possível é uma associação e suas UVRs válidas. Recomenda-se permitir
consultar usuários vinculados, administrar vínculos locais dentro dos limites
delegados e ver relatórios institucionais autorizados.

- **Ações recomendadas:** consultar usuários da própria associação, vincular
  usuários às UVRs autorizadas e visualizar relatórios institucionais que sua
  permissão permita.
- **Ações aprovadas:** criar ou convidar usuários somente na própria associação
  e atribuir somente perfis previamente autorizados dentro desse escopo.
- **Ações que exigirão permissão especializada:** inativar contas, gerir UVRs,
  consultar finanças, alterar patrimônio e consultar auditoria.
- **Ações exclusivas do Administrador Global:** criar outro Administrador
  Global, atribuir alcance fora da associação, administrar o catálogo técnico de
  permissões e proteger a continuidade do último Administrador Global.

## 13. Gestor e operador de UVR

O gestor pode receber uma ou mais UVRs e responsabilidades locais sobre
cadastros, associados, patrimônio em uso, estoque, EPIs, documentos e
relatórios. Não cria, inativa ou reativa contas; poderá solicitar essas
alterações. Financeiro, patrimônio, dados pessoais e consolidação exigem os
perfis e escopos específicos aprovados.

O operador terá UVR principal, uma ou mais UVRs válidas e acesso somente aos
módulos e ações atribuídos. Não administra usuários, não altera o próprio
perfil/escopo, não recebe globalidade, não faz transferência institucional e
não exclui fisicamente. Responsabilidades adicionais serão concedidas pelo
acúmulo de perfis ativos no escopo correspondente, nunca por autoatribuição.

## 14. Usuário de consulta

“Somente leitura” autoriza consultar, não automaticamente exportar, baixar
arquivo privado, ver finanças, ver dados pessoais ou acessar várias UVRs. Cada
uma dessas capacidades deve ser avaliada separadamente. Acesso a histórico
também pode exigir permissão específica quando revelar informações sensíveis.

## 15. Fiscalização de Contratos

O módulo é global e continuará integralmente administrativo até implementação
futura autorizada. A matriz funcional aprovada é:

| Possibilidade | Ações a avaliar |
|---|---|
| Administrador do módulo | empresas, servidores, contratos, responsáveis, aditivos, documentos e configurações do módulo |
| Gestor de contrato | manter contrato atribuído, responsáveis, documentos, planilhas, medições e encaminhamentos permitidos |
| Fiscal titular/substituto | fiscalizações, ocorrências, acompanhamentos e evidências do contrato atribuído |
| Autoridade que aprova | aprovar/devolver/cancelar etapas formalmente atribuídas |
| Consulta | visualizar contratos e histórico autorizados, sem presumir download |
| Auditor | consultar registros e eventos imutáveis, com exportação separada |

Devem ser separadas as ações de cadastrar estrutura, fiscalizar, medir, ajustar,
atestar, cancelar/corrigir ateste, encaminhar e baixar documento privado. Ser
responsável cadastrado em `fc_servidores` não cria automaticamente uma conta nem
uma permissão.

## 16. Matriz funcional de módulos aprovada

| Módulo | Escopo provável | Perfis possíveis | Sensibilidade | Situação atual | Decisão necessária |
|---|---|---|---|---|---|
| Usuários | global/associação/próprio | administradores e próprio usuário | alta | administração baseada em `admin` e scripts | delegação, ativação, senha e último admin |
| Associações | global/associação | global e admin de associação | alta | entidade futura aprovada | quem cria, edita e inativa |
| UVRs | associação/UVR | global, admin de associação, gestor | alta | texto legado | quem administra vínculos e aliases |
| Associados | UVR | gestor, operador, consulta | alta, dados pessoais | login + UVR textual | ações, exportação e acesso pessoal |
| Cadastros | UVR | gestor, operador, consulta | média/alta | login + UVR textual | separar clientes, fornecedores e ações |
| Financeiro | associação/UVR | financeiro, gestor autorizado, auditor | muito alta | login + UVR e regras locais | segregação, aprovação e consolidação |
| Contas correntes | associação com visão UVR | financeiro e administradores | muito alta | texto UVR; desenho institucional aprovado | titularidade e gestão |
| Fluxo de caixa | associação/UVR | financeiro, consulta autorizada | muito alta | login + regras locais | criar, corrigir, cancelar e exportar |
| Transações | associação/UVR | financeiro | muito alta | login + UVR/objeto | aprovação e exclusão física |
| Patrimônio | associação/UVR | patrimônio, gestor, consulta | alta | texto UVR e regras locais | transferência e responsabilidade |
| Produtos e serviços | global ou institucional | admin de catálogo, consulta | média | administração central e legado | governança do catálogo |
| Estoque | UVR | estoque, gestor, consulta | alta | sem modelo formal de permissão | ações e movimentações |
| EPIs | UVR/objeto | operador especializado e consulta | alta | sem modelo formal de permissão | entrega, devolução e histórico |
| Documentos | conforme objeto | perfil do módulo e auditor | alta | regras por rota/objeto | upload, consulta e download separados |
| Auditoria | global/associação/UVR | global, auditor, controle | muito alta | logs técnicos, sem RBAC formal | alcance, retenção e exportação |
| Ouvidoria | global ou associação | equipe autorizada | muito alta | rotas online desativadas | modelo de sigilo e reativação |
| Fiscalização de Contratos | global/objeto atribuído | perfis específicos do módulo | muito alta | 105 rotas administrativas | matriz detalhada e segregação |
| Relatórios | acompanha a origem | consulta/gestor/auditor | alta | login + filtros e UVR | consulta versus exportação |
| Configurações | global | administrador global | crítica | regras administrativas dispersas | catálogo do que é delegável |

## 17. Catálogo conceitual de ações aprovado

Podem ser genéricas: `visualizar`, `criar`, `editar`, `inativar`, `reativar`,
`rejeitar`, `cancelar`, `exportar`, `baixar_documento` e
`visualizar_auditoria`.

Precisam permanecer específicas quando carregam efeito próprio: `transferir
patrimonio`, `aprovar medicao`, `atestar execucao`, `encaminhar pagamento`,
`reabrir fiscalizacao`, `cancelar ateste`, `atribuir perfil`, `atribuir escopo`,
`administrar usuarios` e eventual `excluir_fisicamente`.

Não se recomenda transformar cada botão em uma permissão. A granularidade deve
representar risco e responsabilidade funcional reconhecível.

## 18. Escopos e resolução de conflitos

Escopos possíveis: global, associação, UVR, conjunto explícito de UVRs, próprio
registro, objeto atribuído e somente leitura.

Regras propostas:

- permissão local não se amplia sozinha;
- ausência, inatividade ou expiração de escopo nega;
- associação limita suas UVRs;
- UVR de outra associação exige concessão explícita compatível;
- o servidor resolve o escopo por IDs persistidos;
- o mesmo usuário pode, se aprovado, ter perfis diferentes em UVRs diferentes;
- a condição mais restritiva prevalece, salvo concessão global explícita,
  válida e compatível com a ação;
- não criar permissões negativas complexas nesta primeira evolução.

| Caso | Resultado funcional aprovado |
|---|---|
| dois perfis válidos | união somente das concessões válidas em cada escopo |
| perfil ativo e vínculo inativo/expirado | negar naquele escopo |
| perfil global e escopo local | global só prevalece se a concessão global for explícita |
| leitura em uma UVR e edição em outra | aplicar a ação correspondente a cada vínculo |
| associação ou UVR inativa | negar operações dependentes |
| usuário inativo | negar tudo e rejeitar a sessão |
| permissão removida durante sessão | reavaliar no servidor na próxima requisição |

## 19. Acúmulo de perfis

| Opção | Flexibilidade | Complexidade/risco |
|---|---|---|
| A — um perfil por usuário | baixa e simples | gera perfis combinatórios |
| B — vários perfis globais | média | amplia acesso além do necessário |
| C — vários perfis ligados a escopos | alta e precisa | exige boa interface, auditoria e revogação |
| D — perfil principal e exceções individuais | alta | exceções difíceis de compreender e revisar |

Foi aprovada a opção C, com vários perfis vinculados a escopos e poucos perfis
básicos protegidos. Toda concessão deverá indicar perfil, escopo, situação,
vigência e responsável.

## 20. Ciclo de vida de usuários

Devem ser definidos os responsáveis por criar, ativar, inativar e reativar
usuários; corrigir e-mail; iniciar redefinição de senha; atribuir associação,
UVRs, UVR principal, perfis e escopos; e administrar administradores.

O usuário nunca escolhe o próprio perfil ou escopo, não reativa a própria conta
e não eleva o próprio acesso. A regra técnica deve impedir que o último
administrador global ativo perca conta, perfil ou concessão global. Nenhum
administrador será criado automaticamente na baseline.

Inativação de usuário deve impedir nova operação e invalidar seu carregamento de
sessão. Inativação de perfil, permissão, associação, UVR ou vínculo deve negar a
parte dependente sem apagar histórico. Reativação será explícita e auditada.

## 21. Auditoria necessária

Devem gerar evento: criação de usuário; perfil e escopo antes/depois; troca de
UVR principal; ativação, inativação e reativação; concessão global; revogação;
tentativa de autoelevação; tentativa de atribuir escopo indevido; troca de
administrador de associação; e ações críticas de cada módulo.

Cada evento preserva ator, data, ação, objeto, estado anterior, estado posterior
e justificativa quando exigida. Não registra senha, token, credencial, conteúdo
integral sensível ou URL privada temporária.

## 22. Ordem da autorização por objeto

1. confirmar autenticação;
2. confirmar usuário ativo;
3. confirmar perfil ativo;
4. confirmar permissão para a ação;
5. confirmar escopo ativo e vigente;
6. confirmar associação e UVR ativas, quando aplicável;
7. confirmar que o objeto pertence ou está atribuído ao escopo;
8. aplicar a regra específica do módulo.

A operação somente será executada se todas as condições aplicáveis forem
satisfeitas; depois será auditada quando exigido. A condição mais restritiva
prevalece.

Para Fiscalização, não se exige UVR. Exige-se permissão global/do módulo e,
quando adotado, vínculo com o contrato ou função competente. Até lá,
`admin_required` deve ser preservado.

## 23. Respostas de acesso negado

- HTML sem sessão: encaminhamento controlado ao login; APIs sem sessão: 401;
- usuário autenticado sem permissão: 403 quando isso não revelar existência;
- objeto inexistente ou fora do escopo: 404 equivalente quando necessário;
- resposta sem nome, existência ou dado de outro escopo;
- nenhum redirecionamento aberto;
- logs técnicos sem dados pessoais ou credenciais.

## 24. Recuperação de senha

O schema reserva `email`, `reset_token` e `reset_token_expira`, mas não existe
fluxo funcional completo. É preciso decidir se haverá recuperação futura ou se
os campos continuarão reservados.

Se implementada, qualquer usuário ativo poderá solicitar sem que a tela confirme
se a conta existe; o token terá expiração, uso único, armazenamento protegido e
invalidação após uso. Deve-se decidir se administrador apenas inicia o processo
ou pode definir senha provisória. Esta etapa não implementa e-mail ou token.

## 25. Baseline e transição

A baseline poderá conter estruturas vazias, catálogo técnico estável de módulos,
ações e permissões e perfis institucionais protegidos. Dados reais, usuários,
senhas, e-mails, associações, UVRs, vínculos e concessões pessoais nunca entram
nela.

É necessário separar:

- **estrutura indispensável:** tabelas, chaves e restrições aprovadas;
- **dados estruturais estáveis:** catálogo técnico de módulos e ações, somente
  se aprovado;
- **configuração do ambiente:** segredos e parâmetros fornecidos fora da
  baseline;
- **dados reais:** associações, UVRs, pessoas e operações, sempre fora da
  baseline;
- **administrador inicial:** nunca criado automaticamente pela baseline.

Transição conceitual:

1. inventariar valores atuais de `role`;
2. mapear valores reconhecidos para perfis aprovados;
3. criar estruturas vazias;
4. cadastrar somente os perfis aprovados;
5. vincular usuários aos perfis;
6. vincular separadamente os escopos por identificador;
7. comparar decisões antigas e novas;
8. operar temporariamente em modo de validação;
9. bloquear ambiguidades e divergências;
10. migrar decorators e helpers por módulos pequenos;
11. retirar gradualmente a dependência de `role` e UVR textuais;
12. preservar eventos e histórico.

## 26. Decisões funcionais aprovadas

1. Foram aprovados os perfis gerais Administrador Global, Administrador de
   Associação, Gestor de UVR, Operador de UVR e Usuário de Consulta; e os perfis
   especializados Responsável Financeiro, Responsável por Patrimônio,
   Responsável por Associados e Cadastros, Gestor do Catálogo, Administrador da
   Fiscalização, Gestor de Contrato, Fiscal de Contrato e Consulta/Auditoria da
   Fiscalização.
2. Um usuário pode acumular vários perfis.
3. Um usuário pode ter perfis diferentes em UVRs distintas.
4. O Administrador Global administra estruturas e acessos, mas não recebe
   automaticamente todos os atos especializados de negócio.
5. O Administrador de Associação pode criar ou convidar usuários somente dentro
   da própria associação.
6. O Administrador de Associação atribui somente perfis autorizados no próprio
   escopo.
7. O Gestor de UVR não cria, inativa ou reativa contas; pode solicitar
   alterações e administrar atividades operacionais.
8. Consulta não concede automaticamente exportação ou download.
9. Informações financeiras exigem permissão financeira explícita.
10. Alterações patrimoniais exigem permissão específica.
11. Transferência patrimonial dentro da associação pode ser feita pelo
    Responsável por Patrimônio; entre associações exige autorização superior e
    histórico formal.
12. Dados pessoais de associados exigem permissão específica e necessidade
    funcional.
13. O catálogo será centralizado e administrado pelo Gestor do Catálogo ou por
    Administrador Global com permissão específica.
14. Fiscalização terá Administrador da Fiscalização, Gestor de Contrato, Fiscal
    de Contrato e Consulta/Auditoria. Fiscal substituto é designação contratual,
    não necessariamente perfil.
15. Fiscal de Contrato atua somente nos contratos formalmente atribuídos e não
    administra cadastros centrais ou permissões.
16. Atestar medição exige designação contratual ativa e permissão explícita;
    perfil administrativo isolado não autoriza o ateste.
17. Atestes concluídos não são excluídos fisicamente; correção, cancelamento ou
    anulação exigem permissão, justificativa e histórico.
18. O sistema impedirá a perda ou inativação do último Administrador Global
    ativo.
19. Foi aprovado o modelo híbrido: perfis institucionais protegidos, permissões
    técnicas com códigos estáveis, perfis adicionais futuramente configuráveis e
    proibição de criar permissões técnicas arbitrárias pela interface.
20. A baseline poderá conter catálogos técnicos e perfis estruturais
    predefinidos, mas nenhum usuário, associação, UVR ou vínculo real.
21. A recuperação de senha será implementada em etapa própria, com token seguro,
    expiração, uso único e resposta pública genérica.
22. Alterações críticas exigirão justificativa obrigatória.
23. Consulta, exportação, geração de relatório e download serão permissões
    distintas.
24. Usuário sem UVR válida acessará módulo global somente com permissão global
    explícita para esse módulo.
25. O Administrador Global poderá delegar administração por módulo com ações,
    escopo, situação e validade explícitos, sem tornar o delegado Administrador
    Global.

## 27. Modelo funcional aprovado

Foi aprovado o modelo híbrido: poucos perfis institucionais protegidos,
permissões técnicas por módulo e ação, e perfis atribuídos separadamente a
escopos. Um usuário poderá ter vários perfis e responsabilidades diferentes em
escopos distintos. O administrador global será explícito; administradores de
associação, gestores de UVR, operadores especializados, consulta e perfis da
Fiscalização integrarão o modelo.

Vantagens: menor privilégio, clareza, expansão controlada e auditoria.
Riscos: interface e regras mais complexas, acúmulo indevido e migração
incompleta. Controles: negar por padrão, vínculos ativos/vigentes, proteção do
último administrador, comparação temporária entre decisões antigas e novas e
baseline sem dados reais.

## 28. Pendências e próximos passos

- detalhar tecnicamente a matriz de permissões por módulo;
- definir nomes, identificadores, vigência e regras de auditoria;
- projetar tabelas, tipos, constraints e catálogo final de códigos;
- preparar a migration e a transição de `role` e `uvr_acesso`;
- implementar decorators, helpers e interfaces administrativas;
- definir encerramento de sessões após revogação;
- criar testes de caracterização antes de trocar decorators;
- planejar transição gradual, reversível e sem acesso ampliado por omissão.

## 29. Implementação técnica pendente

As 25 decisões funcionais estão aprovadas, mas ainda não existem tabelas,
decorators, helpers, interfaces, fluxos administrativos, migration ou migração
dos campos legados decorrentes desta especificação. A aprovação funcional não
declara essas estruturas implementadas nem autoriza execução no banco.
