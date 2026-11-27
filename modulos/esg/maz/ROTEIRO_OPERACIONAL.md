# Integração definitiva ESG-MAZ ao RECIC3

O objetivo é utilização permanente do ESG dentro do RECIC3. O MBA e a apresentação de 08/12/2026 são marcos intermediários. Este roteiro é técnico e versionável; não autoriza nem implementa integração de produção. **RF-1A permanece NO-GO OPERACIONAL.**

## 1. Arquitetura nativa e coexistência com ESG V1

Mapear os contratos, rotas e persistência do ESG V1 e MAZ, definindo uma política explícita de coexistência, migração ou convergência. Registrar o blueprint MAZ no aplicativo principal por configuração institucional, com adaptadores injetados e fábrica de conexão central. Substituir o lifecycle demonstrativo pelo ciclo de vida do RECIC3, evitando arquitetura paralela permanente. Tratar o 503 esperado de V1 sem repositório como configuração pendente, sem simular sucesso ou apagar suas funcionalidades.

Aceite: navegação única; comportamento anterior preservado ou migração documentada; desativação segura do login sintético fora do perfil de demonstração; nenhum fallback para banco descoberto implicitamente.

## 2. Migrations e compatibilidade com bancos preexistentes

Inventariar schema e ledger do ambiente alvo sob autorização específica. Preservar M0017/M0018/M0019 e checksums. Construir plano candidato de promoção ao manifesto efetivo, com dependências, pré-condições, reconciliação, rollback e revisão independente. Ensaiar em cópia sanitizada/autorizada que represente as condições do alvo; o êxito em banco descartável vazio não comprova compatibilidade operacional. Não reescrever SQL imutável ou remendar metodologias históricas silenciosamente.

Aceite: matriz de compatibilidade; preflights aprovados; evidência de rollback/recuperação; ledger íntegro; plano de janela e autorização de implantação. Mudança de migration exige justificativa e autorização próprias.

## 3. Autenticação, autorização e auditoria institucionais

Usar identidades e sessão existentes do RECIC3, perfis e escopos institucionais explícitos. Definir separação entre cadastro, revisão técnica, aprovação e publicação. Manter CSRF, autorização no servidor, segregação e rastreabilidade de autoria/request. Revisar privilégios mínimos do runtime e política de logs, segredos, retenção e acesso a dados pessoais.

Aceite: matriz de permissões, testes positivos/negativos por perfil, auditoria atômica e ausência de promoção automática. Código/ator sintético e credenciais demonstrativas não entram em produção.

## 4. Aproveitamento das pesagens e medições das associações

Mapear os registros reais autorizados de entrada, triagem, expedição, estoque, aceite, recusa e devolução. Definir competência, perímetro, unidades, fontes, qualidade e regras de reconciliação comparáveis. Integrar por contratos rastreáveis, com idempotência, correções por sucessora e tratamento de ausência/zero. Não converter expedição em recuperação comprovada nem somar séries incompatíveis; consolidações devem derivar de massas comparáveis e regra documentada.

Aceite: linhagem até as pesagens, reconciliação aprovada, tratamento de duplicidades e divergências, proteção de dados das associações e ensaio de carga sem alteração de históricos.

## 5. Documentos privados e fechamento das revisões

Integrar com a gestão de documentos privados e suas permissões, sem expor armazenamento/chaves em HTML ou logs. Distinguir referência declarada, arquivo apresentado, análise técnica, suficiência e decisão jurídica. Preservar identidade documental, versão, hash quando disponível, autoria e proveniência. Definir fechamento integral do grafo de uma revisão: fontes, vínculos, documentos e avaliações não podem receber novas relações silenciosas depois do fechamento. A imutabilidade de linhas existente não resolve sozinha esse problema.

Aceite: modelo de estados e autorização para fechar/reabrir; snapshot ou manifesto completo do grafo; testes de mutação/vínculo tardio, concorrência e leitura histórica; recuperação íntegra das versões; nenhum atestado automático de autenticidade ou eficácia jurídica.

## 6. Obrigações, condicionantes e prazos

Vincular instrumento de origem autorizado, texto verificável, competência, prazo e responsáveis formalmente designados. Distinguir proposta de designação. Evoluir acompanhamento de obrigações/condicionantes com encaminhamentos, pendências, riscos, justificativas e evidências. Planejar alertas somente após fonte e regra de prazo homologadas. G2 continua sendo completude documental; cumprimento jurídico depende de análise/decisão institucional própria.

Aceite: validação jurídica/administrativa das fontes e estados, regras de prazo documentadas, rastreabilidade de mudança e testes de histórico/CAS/atomicidade. Não inventar prazo, norma ou responsável para preencher lacunas.

## 7. Demais indicadores e materialidade participativa

Implantar progressivamente os códigos do catálogo conforme disponibilidade de fontes e método confirmado. Para cada motor: definição, unidade, fronteira, periodicidade, numerador/denominador quando pertinentes, incerteza, validações, testes e aprovação da metodologia. Confirmar a correspondência dos subcódigos K09 antes de atribuir resultados individuais. Manter A1 como alias coerente de K01.

Conduzir avaliação participativa dos temas candidatos com partes interessadas e critérios institucionais documentados. Separar impacto socioambiental de riscos/oportunidades financeiros e institucionais. Pontuações, pesos e prioridades só serão registrados quando o processo real existir e for aprovado.

Aceite: metodologia e fontes homologadas por indicador; ausência explícita quando insuficientes; registros do processo participativo, divergências e decisões; sem resultados fabricados.

## 8. Backup, recuperação, desempenho e manutenção

Definir backups consistentes de banco e documentos, criptografia, retenção, segregação e controle de acesso. Ensaiar restauração completa e consistência entre revisões, arquivos, hashes e auditoria. Definir RPO/RTO com a instituição, sem atribuir metas inexistentes. Revisar índices, paginação e consultas de histórico; a listagem demonstrativa por revisão não deve orientar escala operacional sem medição. Planejar monitoramento, manutenção, atualização de dependências e resposta a incidentes.

Aceite: restauração ensaiada em ambiente isolado, procedimentos de operação/encerramento, evidências de desempenho e plano de manutenção com responsáveis reais formalizados.

## 9. Homologação operacional progressiva

Concluir cobertura POSIX no ambiente autorizado, integração nativa, compatibilidade de migrations, autenticação, documentos, dados e recuperação antes do parecer operacional. Executar piloto com escopo/usuários autorizados, critérios de aceite e reversão definidos. Reconciliar indicadores e obrigações com fontes reais autorizadas, preservando logs e versões. Revisar independentemente segurança, método e fluxo institucional.

Aceite final: parecer formal da instituição, procedimentos treinados, manutenção assumida e autorização explícita para promoção operacional. Até esse marco, a aprovação visual/automatizada da demonstração e o futuro checkpoint Git não alteram o NO-GO de RF-1A.
