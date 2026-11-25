# Núcleo MAZ — recorte autorizado ESG-MAZ-02

RF-1A: **NO-GO OPERACIONAL**. Sem integração em app.py, seed, persistência ou publicação.
Os documentos v46/v47 não foram localizados nas buscas locais acessíveis em
Documents, Downloads e RECIC3_ESG05C_ESPECIFICACAO. O prompt autorizado é a fonte
deste recorte. Nenhuma cópia integral acadêmica ou medição histórica foi incluída.

## Arquitetura e catálogo

Componentes aditivos em maz/: modelos de proveniência, catálogo imutável construído
explicitamente e motor puro. Reutilizam Periodo, Perimetro, FonteDocumental,
DimensaoESG e situações institucionais existentes sem modificá-los.
A versão maz02-recorte-prompt-v1 identifica esta proposta local, não aprovação oficial.

O catálogo contém K01, K02, K03, K04a, K04b, K04c, K05, K06, K07, K08,
K09a, K09b, K09c, K09d, K10 e K11, com definições, métodos, requisitos,
limitações e impedimentos. Unidade, periodicidade, perímetro e componentes
permanecem ausentes quando não confirmados. Dimensões ASG são propostas.
K09a–d preservam identidade sem atribuir individualmente temas ainda não confirmados.
Fontes de verificação descrevem tipos de evidência exigidos, não documentos existentes.

## TRCS

calcular_trcs recebe rejeitos e entrada com Decimal/None, fonte, origem, qualidade,
mesma unidade de massa, período e perímetro (inclusive descrição).
Preserva os componentes e a fração racional exata: 100 × rejeitos / entrada.
O Decimal periódico é aproximado com precisão explícita (padrão 50), sem quantização,
sem float e sem alterar contexto global. Nenhum ajuste de estoques é presumido.
Zero é informado; componente ausente é indisponível; denominador zero é não calculável.
Rejeitos acima da entrada podem refletir estoques: mantém aritmética e aponta
impedimento metodológico; não limita silenciosamente a 100%.

avaliar_metodologia é separado: aponta falta de reconciliação, fonte estimada,
anomalias e aprovação pendente. Referência documental de reconciliação não autentica
seu conteúdo. Não existe aprovação automática; saída sempre provisória/rascunho
e avaliação não publicável oficialmente. VALIDADE técnica de entradas não transforma
resultado calculado em aprovado. Demais eixos: origem declarado/estimado/calculado;
qualidade provisório/validado/inconclusivo; disponibilidade informado/indisponível/
não calculável/não observado. Não são uma enumeração institucional única.

## Correspondência MBA

| Código MBA | Relação | Limite |
|---|---|---|
| A1 | K01 diretamente | Não duplicar medições |
| G1 | Estado conhecido de itens do Plano Executivo | Governança/processo; sem equivalência ambiental |
| G2 | Completude do caso demonstrativo; relação conceitual com K05 | Exigíveis distintos; não copiar denominadores |
| G3 | Compromissos acadêmicos C1–C4 | Sem presumir aprovação institucional |
| G4 | Esforço/custo do instrumento; relação conceitual com K08 | Não equivale ao custo municipal total |
| S1 | Renda distribuída e segurança nas UVRs; grupo K09 | Subcódigos individuais pendentes |

K02–K04/K07 exigem reconciliação física; K03/IDO, glosas K04c, tolerância K07,
unidades/períodos não confirmados, métricas sociais K09, CDR K10 e amostragem K11
não têm motores de cálculo nesta etapa. Não há metas nem cargas históricas.

## Obrigação 14 — especificação futura

Acompanhamento da regularização do pátio de compostagem: instrumento de origem,
texto verificável e fonte, competência, situação documental, evidências,
prazo documentado, responsável (proposto versus designado), pendências, riscos,
próxima decisão e histórico imutável de revisão com autoria, motivo e data.
Ausências devem aparecer como desconhecidas, nunca como cumprimento.
Texto jurídico, prazo, responsável formal e situação atual não são presumidos.
Sem persistência, rotas, telas ou declaração jurídica nesta etapa.
Dados pessoais: minimização, agregação social, controle de acesso e retenção a definir.

## Dupla materialidade — representação conceitual

Cada tema terá duas perspectivas independentes: impactos sobre sociedade/ambiente
e efeitos externos sobre capacidade institucional/financeira. Temas preliminares:
resíduos/capacidade do aterro; catadores/renda/SST; qualidade de dados/decisões;
orgânicos/tratamento regular; recuperação complementar/CDR condicionada.
Registrar futuramente fonte, horizonte, justificativa, participantes e revisão.
Não existem pesos, pontuações ou classificações participativas confirmadas.

## Limites da revisão

Pitch e arquivos compartilhados preservados. Oito testes POSIX permanecem pendentes.
Novos componentes não são importados pelo pitch e não alteram suas funcionalidades.
Revisão metodológica v46/v47 e posterior homologação PostgreSQL/fluxos institucionais
permanecem necessárias. Esta etapa prepara revisão da ESG-MAZ-03, sem iniciá-la.
