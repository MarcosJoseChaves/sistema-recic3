# Plano Diretor de Desenvolvimento ESG-MAZ

Baseline aprovada: 27/11/2026. Progresso ponderado aprovado: **39%**. RF-1A permanece **NO-GO OPERACIONAL**.


O RECIC3 é um sistema consolidado, atualmente em funcionamento, utilizando GitHub, Neon com plano pago, Cloudinary e infraestrutura operacional estabelecida.

O ESG-MAZ representa evolução incremental desse sistema.

- Preservar módulos e usuários existentes.
- Reaproveitar infraestrutura, cadastros e serviços.
- Evitar arquitetura paralela permanente.
- Desktop como ambiente principal.
- Notebook como ambiente complementar de desenvolvimento fora da Prefeitura.
- GitHub como referência de sincronização.
- Bancos de ensaio e credenciais independentes.
- Preservar versão acadêmica congelada.
- Implantar funcionalidades individualmente quando homologadas.
- Não exigir conclusão dos 100% para disponibilização de entregas independentes.
- Nenhuma liberação automática baseada em percentual.
- Toda ativação operacional exige autorização específica, regressão pertinente, segurança, reversibilidade e avaliação da versão efetivamente implantada.

Avaliar futuramente o database branching do Neon, mediante autorização específica e análise de isolamento, custos e proteção de dados.


## Etapas e pesos

| Etapa | Peso | Acumulado |
|---|---:|---:|
| 00–02 Metodologia, catálogo e motores | 12% | 12% |
| 03 Persistência e auditoria | 8% | 20% |
| 04 Obrigações e evidências | 5% | 25% |
| 05–07A Interface, homologação e portabilidade | 5% | 30% |
| 08A Diagnóstico arquitetural | 4% | 34% |
| 08B.1 Composição nativa | 2% | 36% |
| 08B.2a Navegação nativa | 1% | 37% |
| 08B.2b Catálogo e materialidade consultiva | 1% | 38% |
| 08B.2c Homologação integrada e visual | 1% | 39% |
| 09.1 Inventário do banco e decisão de schema | 4% | 43% |
| 10.1 Projeto de identidade | 3% | 46% |
| 09.2 Compatibilização das migrations | 7% | 53% |
| 10.2 Autenticação e autorização | 6% | 59% |
| 11A.1 Fontes físicas e medições | 5% | 64% |
| 11B.1 Integração documental | 5% | 69% |
| 11B.2 Fechamento integral das revisões | 6% | 75% |
| 11A.2 TRCS automatizada | 5% | 80% |
| 11C Obrigações, condicionantes e prazos | 5% | 85% |
| 11D Demais indicadores e materialidade | 8% | 93% |
| 12 Homologação operacional | 6% | 99% |
| 13 Monitoramento e manutenção | 1% | 100% |

Regras de acompanhamento:

- 39% aprovados.
- Pesos estimativos de esforço, não prontidão operacional.
- Crédito somente após aceite.
- Subdivisões sem dupla contagem.
- Possibilidade de reordenar unidades independentes.
- Recalibração de pesos somente mediante justificativa e histórico.
- Oito testes POSIX pendentes.
- Homologação no notebook como trilha acadêmica paralela.

## Entregas incrementais

Registrar os seguintes marcos candidatos:

- 39%: navegação, catálogo e materialidade consultiva.
- 59%: infraestrutura ESG e identidade institucional homologadas.
- 75%: evidências, documentos e fechamento de históricos.
- 80%: TRCS integrada às fontes autorizadas.
- 85%: obrigações, licenças, TACs, condicionantes e prazos.
- 93%: gestão ASG ampliada.
- 100%: consolidação operacional.

Avaliar antecipação da etapa 11C quando suas dependências estiverem resolvidas.

Cada entrega deve possuir critérios de aceite próprios e estados distintos: desenvolvimento, homologação técnica, homologação humana, integração desabilitada, piloto e liberação operacional.

O percentual não constitui autorização para piloto ou publicação.


## Histórico e gates

27/11/2026: pesos e acumulado de 36% aprovados pelo solicitante após revisão independente da 08B.1. Etapas até 08B.1 têm crédito aceito; etapas posteriores permanecem candidatas, sem crédito antecipado. A portabilidade integra o marco técnico já aceito, mas a homologação no notebook e os oito testes POSIX continuam pendentes. Cada entrega pode avançar individualmente após aceite e autorização, sem depender dos 100%.

Histórico: 08B.2a foi a unidade candidata após o checkpoint 08B.1; 08B.2a/b/c foram posteriormente aprovadas no escopo consultivo representativo. Nenhuma implementação automática das etapas seguintes. Não há ativação operacional pela publicação deste documento ou pelo percentual. Mudanças futuras nos pesos devem registrar data, motivo, pesos anteriores/novos e efeito no acumulado, sem duplicar crédito.

## Aceite de 08B.2a/b/c — baseline 27/11/2026

39% de desenvolvimento aprovados pelo solicitante, sem alteração dos pesos. 08B.2a (navegação), 08B.2b (catálogo/materialidade) e 08B.2c (homologação consultiva representativa) concluídas nesse escopo.

Evidências separadas: 21 testes isolados e 13 verificações HTTP automatizados registrados; homologação humana de menu, catálogo, pesquisa, filtros e materialidade confirmada pelo usuário. Nenhuma observação visual humana direta pelo Codex é alegada. A revisão independente dos hashes e sintaxe foi informada pelo usuário.

Limitações preservadas: aparência/JavaScript integral do menu herdado limitada por CDN; startup integral do RECIC3 ainda não homologado; implantação operacional não realizada; publicação da branch pendente de comprovação dos gatilhos do provedor. Nenhuma dessas aprovações libera produção ou piloto automaticamente. RF-1A permanece NO-GO OPERACIONAL. Oito testes POSIX e trilha complementar do notebook continuam pendentes.
