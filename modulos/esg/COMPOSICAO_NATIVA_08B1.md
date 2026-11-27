# ESG-MAZ-08B.1 — composição nativa indisponível

ESG_MAZ_NATIVE_ENABLED é lida por configuracao_ambiente.ler_booleano: ausente/vazia → false; true/1/yes/on e false/0/no/off, com trim/case-insensitive; qualquer outro valor interrompe configuração. Composição aceita somente bool real em app.config. Flag é avaliada no startup, nunca em query/cookie/sessão.

False: nenhuma rota MAZ registrada. True: criar_blueprint_maz(None,None,metodologia=None), protegido por Flask-Login/admin_required e barreira incondicional before_request 503 para toda a superfície, incluindo catálogo/materialidade/assets e POST. CSRF central permanece: POST sem token é 400; com sessão admin e token válido é 503, sem alcançar view/persistência. Usuário sem role admin recebe 403; anônimo segue política Flask-Login (401 ou redirect para login institucional).

Não chama criar_demo_maz, não registra login/logout nem cria ator. Não lê MAZ_DEMO_*, não injeta conexão/repositórios e não aceita argumentos de persistência. Configurar objetos de adaptadores não habilita escrita. Nenhuma view de negócio ou template demonstrativo é executado no modo nativo nesta unidade. A aparência/navegação será tratada em 08B.2; não se declara autorização granular ou compatibilidade do banco.

Ponto de composição em app.py após FC e ESG V1, preservando entrypoint e configuração. Blueprint demonstrativo e código persistente não foram alterados. RF-1A permanece NO-GO OPERACIONAL. Sem commit/push até revisão independente.

## Atualização consultiva 08B.2 (histórico 08B.1 preservado)

A barreira integral descrita acima corresponde ao checkpoint 08B.1. Em 08B.2, somente GET de início, catálogo, materialidade e CSS nativo explicitamente permitido são liberados sob a mesma autenticação. Rotas persistentes e escritas permanecem 503. A flag não autoriza persistência. O harness de homologação é externo e representativo, não startup integral do RECIC3.
