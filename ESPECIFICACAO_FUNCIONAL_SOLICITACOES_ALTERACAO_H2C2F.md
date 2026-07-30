# Etapa H2C.2F — Especificação funcional final do fluxo de solicitações de alteração

**Situação:** APROVADA
**Data da aprovação funcional:** 30/07/2026

## 1. Contexto e limites

O sistema já possui a tabela `solicitacoes_alteracao` e um fluxo funcional
simples. O schema documentado e o código confirmam a versão A, que deve ser
preservada e evoluída de forma aditiva. Um script histórico contém uma versão B
incompatível e não representa a estrutura atual.

Esta especificação não altera código ou banco, não define DDL e não autoriza
migration. As decisões funcionais estão aprovadas; a implementação técnica
continua pendente.

## 2. Resumo simples do funcionamento atual

Quando um usuário comum tenta editar determinados registros, o sistema não
grava diretamente: cria uma solicitação `PENDENTE`. Em alguns casos de
“exclusão”, também cria pedido para o administrador. O administrador vê os
pendentes, consulta a comparação e escolhe aprovar ou rejeitar.

Ao aprovar, o sistema altera ou apaga o objeto e marca a solicitação como
`APROVADO` na mesma transação. Ao rejeitar, apenas marca `REJEITADO`. Não existe
rascunho, devolução, justificativa obrigatória, aprovador persistido, fotografia
anterior, evento histórico ou etapa de aplicação separada.

## 3. Inventário da versão A

| Arquivo/função | Método/proteção | Objeto/efeito | Estado/resposta | Limitação |
|---|---|---|---|---|
| `app.py`, `_inserir_solicitacao_escopada` | helper interno após `login_required` | insere pedido somente se o objeto ainda pertence à UVR textual da sessão | cria `PENDENTE`; retorna existência | usuário e UVR não têm FK; escopo não fica fotografado |
| `editar_associado` | POST; autenticado | admin edita; comum solicita `EDICAO` | página de sucesso | inclui dados pessoais e foto no JSON |
| `editar_cadastro` | POST; autenticado | admin edita; comum solicita `EDICAO` | página de sucesso | não guarda valores anteriores |
| `editar_conta_corrente` | POST; autenticado | admin edita; comum solicita `EDICAO` | página de sucesso | dados bancários sensíveis; sem aprovador especializado |
| `editar_transacao` | POST; autenticado | admin edita; comum solicita `EDICAO` | página de sucesso | guarda itens e valores; usa números serializados; aprovação recria itens |
| `editar_patrimonio` | POST; autenticado | admin edita; comum solicita `EDICAO` | página de sucesso | conjunto amplo de campos; pode incluir imagem |
| exclusões de associados, cadastros, transações e patrimônio | POST; autenticado | admin apaga; comum solicita `EXCLUSAO` | JSON | aprovação executa exclusão física |
| `get_solicitacoes_pendentes` | GET; `admin_json_required` | lista todos os pedidos pendentes | JSON | sem filtro por escopo, tipo ou responsável |
| `get_detalhes_solicitacao` | GET; `admin_json_required` | compara JSON solicitado ao valor vigente | JSON | consulta o presente, não uma fotografia da criação |
| `responder_solicitacao` | POST; `admin_json_required` | bloqueia pedido com `FOR UPDATE`, aprova/aplica ou rejeita | JSON | somente duas ações; não persiste analisador, data ou motivo |
| `templates/cadastro.html` | interface administrativa | lista, abre detalhes, aprova ou rejeita | modal/alerta | confirmação simples; sem justificativa ou mensagens |

### 3.1 Objetos e tipos usados

| Objeto atual | `EDICAO` | `EXCLUSAO` | Solicitante atual | Analisador atual |
|---|---:|---:|---|---|
| `associados` | sim | sim | usuário comum da UVR | administrador global |
| `cadastros` | sim | sim | usuário comum da UVR | administrador global |
| `contas_correntes` | sim | não há criação atual de pedido de exclusão | usuário comum da UVR | administrador global |
| `transacoes_financeiras` | sim | sim | usuário comum da UVR | administrador global |
| `patrimonio` | sim | sim | usuário comum da UVR | administrador global |

O helper contém allowlist desses cinco nomes. O responder também possui
allowlists de objetos e campos. Não é uma autorização para receber tabela ou
coluna arbitrária do navegador.

### 3.2 Campos efetivamente solicitados hoje

- **Associado:** nome, CPF, RG, nascimento, admissão, situação, UVR, associação,
  endereço, telefone e foto.
- **Cadastro:** razão social, CNPJ, tipo, atividade, UVR, associação, endereço e
  telefone.
- **Conta corrente:** UVR, associação, banco, agência, conta e descrição.
- **Transação:** UVR, associação, data, tipo, atividade, documento, origem,
  valor total e itens.
- **Patrimônio:** identificação, propriedade, comodato, veículo/equipamento,
  medidores, localização, responsáveis, situação, conservação, permissões de
  uso, observações e imagem.
- **Exclusão:** identificação amigável e motivo genérico; o objeto é identificado
  por tabela e ID.

## 4. Estrutura documentada da versão A

Não foi consultado o banco. A estrutura abaixo vem do `app.py`, do mapa e do
relatório de comparação já produzidos.

| Coluna | Tipo documentado | Nulabilidade/default/chave | Relação/índice | Uso e classificação |
|---|---|---|---|---|
| `id` | `SERIAL`/INTEGER | não nulo; PK; sequência | índice da PK | necessário e utilizado |
| `tabela_alvo` | `VARCHAR(50)` | não nulo | sem FK confirmada | necessário e utilizado; nome lógico legado |
| `id_registro` | INTEGER | não nulo | sem FK polimórfica | necessário e utilizado; ambíguo sem tipo |
| `tipo_solicitacao` | `VARCHAR(20)` | não nulo | sem `CHECK` confirmado | necessário e utilizado; `EDICAO`/`EXCLUSAO` |
| `dados_novos` | JSONB | opcional | nenhum índice confirmado | necessário e utilizado; candidato a validação por tipo |
| `usuario_solicitante` | `VARCHAR(50)` | não nulo | sem FK confirmada | necessário e utilizado; texto legado |
| `data_solicitacao` | TIMESTAMP | default `CURRENT_TIMESTAMP` | nenhum índice confirmado | necessário e utilizado |
| `status` | `VARCHAR(20)` | opcional; default `PENDENTE` | sem `CHECK`/índice confirmado | necessário e utilizado; candidato a catálogo controlado |
| `observacoes_admin` | TEXT | opcional | nenhum | necessário, mas sem uso atual localizado |

Não estão comprovados na versão A: aprovador, data de análise, motivo de
rejeição, fotografia anterior, resultado aplicado, versão do objeto, escopo,
eventos, mensagens, anexos ou vigência.

## 5. Versão B incompatível

**Classificação obrigatória: LEGADO INCOMPATÍVEL — NÃO AUTORIZADO PARA EXECUÇÃO.**

O arquivo `criar_tabela_solicitacoes.py` tenta criar outra estrutura com
`dados_novos` em TEXT, solicitante maior e anulável e `motivo_rejeicao`, sem
`observacoes_admin`. Ele abre conexão e executa DDL ao ser chamado, possui
fallback histórico de conexão local escrito no código e mensagens de execução.

Riscos:

- `IF NOT EXISTS` pode ocultar a divergência sem corrigi-la;
- execução em ambiente errado;
- uso de credencial histórica insegura;
- falsa impressão de que `motivo_rejeicao` existe;
- código e schema passarem a esperar formatos diferentes.

Plano técnico futuro: bloquear execução acidental, isolar/arquivar, remover
credenciais históricas em etapa própria, preservar apenas a referência útil e
reescrever qualquer conceito aproveitável sobre a versão A. Não importar seu DDL
ou dados. Exclusão do arquivo exige autorização humana futura.

## 6. Finalidade atual e classificação de objetos

| Objeto | Classe | Uso recomendado do fluxo | Observação |
|---|---|---|---|
| Usuário | B/D | correções cadastrais e pedidos de acesso futuros | acesso nunca produz autoelevação |
| Associado | A | correções cadastrais e dados pessoais permitidos | já usado; exige proteção reforçada |
| Cadastro | A | correção cadastral permitida | já usado |
| Associação | B | correção institucional controlada | futura entidade por ID |
| UVR | B | vínculo, principal e transferências controladas | futura entidade por ID |
| Vínculo usuário–UVR | B | inclusão, remoção e principal | depende do modelo H2C.2D/E |
| Perfil/permissão | B | concessão, revogação, escopo e validade | fluxo crítico e sem autoaprovação |
| Conta corrente | A | apenas campos administrativos expressamente permitidos | já usada em edição |
| Transação financeira | A/D | descrição/classificação; valores e fatos concluídos exigem fluxo financeiro | não reescrever histórico silenciosamente |
| Patrimônio | A/D | correção simples; transferência/baixa seguem regras próprias | 17 decisões patrimoniais continuam separadas |
| Produto/serviço | B | sugestão, correção e reclassificação | catálogo central |
| Contrato | C | manter fluxo específico da Fiscalização | eventual correção central bem delimitada |
| Medição | C | fluxo específico | não usar solicitação genérica |
| Ateste | C | fluxo específico e histórico próprio | não usar solicitação genérica |

A = já usado; B = candidato coerente; C = não deve usar o fluxo genérico;
D = ainda exige decisão funcional.

## 7. Alternativas de evolução

| Modelo | Vantagens | Limitações/riscos | Avaliação |
|---|---|---|---|
| A — simples | pouca mudança; fácil compreensão | continua sem trilha completa, concorrência e separação aprovação/aplicação | insuficiente para alterações críticas |
| B — principal + eventos | preserva versão A, antes/depois, mensagens e cada transição | novas estruturas e regras; exige migração cuidadosa | **modelo funcional aprovado** |
| C — configurável por tipo | máxima flexibilidade e múltiplas aprovações | alto custo, regras difíceis e risco de configuração insegura | excessivo para esta fase |

Foi aprovado o modelo B, evoluído de forma aditiva, com tipos
controlados, eventos, fotografias, justificativa, aprovação separada
conceitualmente da aplicação, falha própria e sem exclusão física. Operações
simples podem aprovar e aplicar na mesma transação, mas geram eventos distintos.

## 8. Princípios para campos alteráveis

Somente campos expressamente autorizados para cada tipo podem ser modificados.
Cada tipo terá allowlist no servidor. Tabela, coluna, perfil e escopo enviados
pelo navegador nunca serão fonte de autorização.

São proibidos genericamente: chaves primárias, campos de auditoria, estados
internos, valores derivados, saldos, hashes, tokens, senhas, credenciais e
qualquer campo não catalogado. Chaves estrangeiras e dados críticos exigem
validação e permissão adicional. A aplicação reconstrói a operação a partir do
tipo conhecido, nunca por SQL formado com nomes recebidos.

## 9. Atores e segregação

| Ator | Pode fazer, conforme permissão e escopo |
|---|---|
| Próprio usuário | correção própria permitida; nunca acesso automático |
| Operador autorizado | solicitar correção de objeto do próprio escopo |
| Gestor de UVR | solicitar/acompanhar operações locais; não gerir contas |
| Administrador de Associação | analisar tipos delegados na própria associação |
| Responsável por módulo | analisar conteúdo especializado no seu escopo |
| Administrador Global | governança e casos globais; ato de negócio ainda exige permissão própria |
| Sistema interno | expirar, aplicar ou registrar falha em operação autenticada tecnicamente |

Níveis aprovados:

- risco baixo: analisador autorizado pode aprovar e aplicar;
- risco alto: solicitante e aprovador devem ser pessoas diferentes;
- risco excepcional: dupla aprovação apenas para tipos expressamente definidos.

Solicitante não aprova a própria solicitação crítica. O aprovador nunca ganha
escopo pelo pedido que analisa. Financeiro, patrimônio e acesso exigem suas
permissões especializadas.

## 10. Estados e transições aprovados

| Estado | Significado | Edição/cancelamento | Efeito no objeto | Natureza |
|---|---|---|---|---|
| RASCUNHO | preparação privada | autor edita e descarta; não bloqueia | nenhum | transitório |
| ENVIADA | pronta para triagem | sem edição direta; autor pode cancelar conforme regra | nenhum | transitório |
| EM_ANALISE | assumida por analisador | somente mensagens/devolução | nenhum | transitório |
| DEVOLVIDA | precisa complementação | autor corrige e reenvia/cancela | nenhum | transitório |
| APROVADA | aplicação autorizada | sem edição | ainda nenhum, salvo aplicação na mesma transação | transitório |
| EM_APLICACAO | execução controlada | bloqueada | operação em andamento | transitório |
| APLICADA | efeito concluído | não cancela; correção exige novo pedido | alteração efetiva | terminal |
| REJEITADA | não autorizada | imutável; pode copiar para novo pedido | nenhum | terminal |
| CANCELADA | encerrada antes da aplicação | imutável | nenhum | terminal |
| FALHA_APLICACAO | aplicação não concluída | nova tentativa controlada | nenhum efeito parcial | transitório/controlado |
| EXPIRADA | prazo terminou | reabertura ou novo pedido conforme tipo | nenhum | terminal, salvo regra excepcional |

Fluxo aprovado:

`RASCUNHO → ENVIADA → EM_ANALISE → APROVADA → EM_APLICACAO → APLICADA`

Desvios aprovados:

- `ENVIADA → CANCELADA`;
- `EM_ANALISE → DEVOLVIDA`;
- `DEVOLVIDA → ENVIADA`;
- `DEVOLVIDA → CANCELADA`;
- `DEVOLVIDA → EXPIRADA`;
- `EM_ANALISE → REJEITADA`;
- `EM_APLICACAO → FALHA_APLICACAO`;
- `FALHA_APLICACAO → EM_APLICACAO`;
- `FALHA_APLICACAO → retorno controlado para análise`.

Os estados acima formam o conjunto funcional aprovado.

## 11. Rascunho, devolução, rejeição e cancelamento

- Rascunho é privado, não altera nem reserva o objeto e pode ser descartado pelo
  autor enquanto nunca enviado. Anexos dependerão do tipo autorizado, e prazos
  de expiração serão definidos posteriormente por categoria.
- Devolução exige mensagem, preserva a versão enviada e cria nova versão no
  reenvio. O solicitante pode cancelar.
- Rejeição exige motivo e categoria, preserva histórico, não permite editar e
  pode permitir copiar para novo pedido. Informação interna sensível fica em
  comentário de visibilidade restrita.
- Solicitante pode cancelar antes da aprovação se isso for aprovado. Depois da
  aprovação, cancelamento exige autoridade. Solicitação aplicada nunca é
  “desaplicada”; requer correção formal.

## 12. Aprovação, aplicação e atomicidade

Aprovar significa autorizar. Aplicar significa alterar o objeto. Em operações
simples, ambos podem ocorrer na mesma transação, mas devem gerar registros
conceitualmente distintos.

Aplicação e registro do resultado serão atômicos e idempotentes: falha não deixa
o pedido como aplicado, não altera parcialmente campos e não duplica efeitos.
`FALHA_APLICACAO` mostra mensagem pública genérica; detalhes ficam em log seguro.
Nova tentativa é controlada e pode exigir nova aprovação conforme a causa.

## 13. Fotografia e concorrência

Devem ser preservadas quatro fotografias estruturadas:

1. dados existentes no envio;
2. dados solicitados;
3. dados vigentes antes da aplicação;
4. dados efetivamente aplicados.

Cada fotografia terá ator e data quando aplicável. Senha, token, hash e segredo
nunca entram nesse conteúdo.

Antes de aplicar, o servidor compara versão/fotografia esperada. Divergência
significativa, especialmente nos campos solicitados, bloqueia a aplicação e
devolve para nova análise. O sistema não sobrescreve silenciosamente uma
alteração mais recente.

## 14. Mensagens, anexos e visibilidade

Mensagem terá autor, data, tipo, conteúdo e visibilidade: solicitante,
analisadores, administradores ou auditoria. Pedido de complementação e
justificativa formal são distintos de comentário interno. Não haverá e-mail ou
notificação externa nesta etapa.

Anexos são uma possibilidade futura: tipos, limite, privacidade, retenção,
visualização e dados pessoais precisam de decisão. Anexo não substitui campos
estruturados nem justificativa. Upload não será implementado nesta etapa.

O solicitante vê seu pedido e mensagens destinadas a ele. Analisadores veem
somente solicitações no escopo. Dados anteriores, anexos e histórico obedecem à
sensibilidade. Comentários internos não são expostos automaticamente.

## 15. Dados pessoais, associação, UVR e acesso

Dados pessoais serão minimizados, aprovadores restritos e acessos sensíveis
auditados. Não se duplicam documentos sem necessidade, não se exportam por
permissão de consulta e não se registram dados pessoais em logs.

Mudança de UVR principal, vínculo secundário, remoção de vínculo ou associação
valida destino, entidades ativas, impacto nas permissões e ausência de
autoaprovação. Texto legado não concede acesso. Mudanças de perfil, permissão,
escopo, delegação e validade só produzem efeito quando a aplicação termina e
devem proteger o último Administrador Global.

## 16. Patrimônio, financeiro, catálogo e Fiscalização

### Patrimônio

Correção simples pode usar o fluxo. Transferência, baixa, reativação e mudança
de responsabilidade institucional exigem tipos próprios, permissão patrimonial,
documentos quando aplicáveis e histórico. Esta especificação não decide as 17
questões patrimoniais pendentes.

### Financeiro

Correção de descrição ou classificação pode ser candidata. Valor, conta e data
de operação concluída não devem ser simples edição: usam estorno, correção formal
ou fluxo financeiro específico. A versão atual, que permite editar transação e
recriar itens após aprovação, é risco a ser caracterizado e não deve orientar a
regra futura sem decisão.

### Catálogo

Sugestão de produto, correção, reclassificação e inativação podem usar tipos
próprios. A hierarquia é central e UVRs não a alteram livremente. Reclassificação
preserva histórico.

### Fiscalização de Contratos

Medições, ocorrências, fiscalizações, atestes e seus eventos continuam nos
fluxos específicos. O fluxo genérico não se torna dependência do módulo. Apenas
eventual correção cadastral administrativa, claramente catalogada, poderá ser
avaliada.

## 17. Auditoria, listagens e relatórios

Eventos mínimos: rascunho, envio, início de análise, devolução, reenvio,
aprovação, rejeição, cancelamento, início de aplicação, aplicação, falha,
expiração/reabertura excepcional, comentário, inclusão/remoção de anexo e
visualização sensível. Cada evento guarda ator, data, estado anterior/posterior,
justificativa e escopo, sem segredo.

Listagens: minhas solicitações, pendentes de minha análise, estado, tipo,
associação, UVR, período, objeto, atrasadas, falhas e concluídas.

Relatórios: volume, tempo médio, aprovação/rejeição, pendências, expiradas,
falhas, alterações críticas, associação/UVR e trilha de um pedido. Exportação
exige permissão separada.

## 18. Notificações futuras

Serão avaliadas notificações de envio, devolução, aprovação, rejeição,
cancelamento, falha, proximidade do prazo e nova mensagem. Não são autorizados
nesta etapa e-mail, WhatsApp, webhook, push ou serviço externo.

## 19. Baseline

A baseline poderá conter estruturas vazias e códigos estruturais estáveis de
estados, eventos, categorias de mensagem, riscos e ações. Tipos configuráveis e
regras técnicas de aprovação ainda dependerão de desenho posterior.

Nenhuma solicitação, mensagem, anexo, usuário, dado pessoal ou conteúdo real
entra na baseline.

## 20. Evolução da versão A

1. preservar a tabela atual;
2. mapear todos os campos;
3. criar estruturas adicionais;
4. preservar solicitações existentes;
5. classificar registros antigos;
6. criar eventos iniciais equivalentes, se aprovado;
7. manter leitura compatível;
8. migrar rotas por tipo;
9. comparar comportamentos antigo e novo;
10. encerrar uso de campos legados somente após validação;
11. manter histórico permanentemente.

Não foi definido SQL.

## 21. Riscos

- exclusão física na aprovação atual;
- ausência de fotografia anterior e de concorrência;
- `usuario_solicitante` e escopo textuais;
- JSON amplo com dados pessoais, imagens e valores;
- aprovação e aplicação indistintas;
- apenas três estados e sem constraints confirmadas;
- rejeição sem motivo persistido;
- nenhum aprovador/data/evento persistido;
- administrador global como único analisador atual;
- script B executável e incompatível;
- tentativa de transformar fluxo genérico em motor de qualquer tabela;
- regras configuráveis excessivamente complexas;
- notificações ou anexos ampliarem exposição de dados.

## 22. Decisões funcionais aprovadas

1. O fluxo atenderá alterações cadastrais, vínculos institucionais, acesso,
   patrimônio, catálogo e correções administrativas delimitadas.
2. Dados pessoais simples poderão ter correção direta somente quando autorizada;
   autenticação, identificação, associação, UVR, acesso e banco exigem aprovação.
3. Existirá RASCUNHO editável, sem efeito sobre o objeto.
4. O solicitante poderá cancelar o pedido enviado até a aprovação.
5. Existirá devolução, preservando a versão enviada.
6. Estados transitórios: RASCUNHO, ENVIADA, EM_ANALISE, DEVOLVIDA, APROVADA,
   EM_APLICACAO e FALHA_APLICACAO. Estados finais: APLICADA, REJEITADA,
   CANCELADA e EXPIRADA.
7. Aprovação e aplicação são conceitos separados.
8. Operação crítica exige aprovador diferente do solicitante.
9. Alto risco excepcional poderá exigir duas aprovações distintas.
10. Pendentes poderão expirar por categoria e prazo posterior.
11. Alteração concorrente relevante bloqueia aplicação e devolve para análise.
12. São críticas mudanças de associação, UVR, perfil, permissão, escopo,
    situação, e-mail de autenticação/recuperação e acesso global.
13. Mudança de associação ou UVR com efeito em vínculo, autorização ou
    responsabilidade sempre exige aprovação.
14. Usuário pode pedir perfil ou acesso, sem efeito imediato ou autoaprovação.
15. Financeiro limita-se a correções administrativas sem reescrever fatos
    concluídos.
16. Patrimônio terá tipos distintos para correção, transferência, baixa,
    reativação e mudança de responsabilidade.
17. Catálogo poderá usar o fluxo com decisão centralizada.
18. Fiscalizações, ocorrências, medições, ajustes e atestes permanecem em seus
    fluxos específicos.
19. Anexos poderão existir futuramente somente em tipos autorizados.
20. Comentários internos e mensagens ao solicitante serão separados.
21. Rejeitada não reabre; novo pedido poderá referenciar o anterior.
22. Aplicada não cancela; correção ou reversão exige novo procedimento.
23. Falha será tratada pelo responsável funcional autorizado, sem duplicar
    efeitos nem permitir decisão funcional autônoma da equipe técnica.
24. A baseline poderá conter códigos estáveis de estados, eventos, categorias de
    mensagem, riscos e ações, sem pedido ou dado real.
25. A versão B permanecerá **LEGADO INCOMPATÍVEL — NÃO AUTORIZADO PARA
    EXECUÇÃO**, sem fornecer DDL ou registros para a baseline.

## 23. Implementação técnica pendente

- definir catálogo final de tipos, campos, riscos e ações;
- definir prazos por categoria e tipos que aceitam anexos;
- concluir decisões patrimoniais e financeiras relacionadas;
- desenhar nomes, tipos, constraints, eventos, mensagens e fotografias;
- implementar aplicação transacional e concorrência;
- implementar interfaces, permissões, anexos e notificações futuras;
- planejar isolamento físico definitivo da versão B;
- criar migration somente em etapa autorizada;
- implementar e testar por incrementos reversíveis.
