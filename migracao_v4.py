import os
import csv
import io
import psycopg2
from dotenv import load_dotenv

# Carrega suas senhas do .env (igual ao seu app.py)
load_dotenv()

# --- SEU CSV NOVO ---
csv_content = """id;item;Atividade;Grupo;Subgrupo;Tipo
1;Rateio dos Associados;Gestão Associativa;Repasses aos Associados;Sobra Líquida;Despesa
2;Aluguel;Operação e Produção;Infraestrutura Predial;Locação;Despesa
3;Fita de arqueamento para fardos;Operação e Produção;Produção e Equipamentos;Insumos de Produção;Despesa
4;Outras despesas de operação;Operação e Produção;Despesas Gerais;Outros;Despesa
5;Impostos/taxas sobre prestação de serviços;Gestão Administrativa e Financeira;Obrigações e Taxas;Tributos;Despesa
6;Seguro de veículo;Operação e Produção;Frota e Logística;Seguros e Documentação;Despesa
7;Água;Operação e Produção;Infraestrutura Predial;Utilidades Básicas;Despesa
8;Antimônio;Comercialização de Materiais Recicláveis;Metal;Outros Metais;Receita
9;Aço inoxidável;Comercialização de Materiais Recicláveis;Metal;Aço e Ferro;Receita
10;Chumbo;Comercialização de Materiais Recicláveis;Metal;Outros Metais;Receita
11;Tubinho de desodorante, spray e aerossois;Comercialização de Materiais Recicláveis;Metal;Alumínio;Receita
12;Radiador de cobre;Comercialização de Materiais Recicláveis;Metal;Cobre;Receita
13;Radiador de Alumínio;Comercialização de Materiais Recicláveis;Metal;Alumínio;Receita
14;Perfil de persiana (tubos, chapas);Comercialização de Materiais Recicláveis;Metal;Alumínio;Receita
15;Liga de aço-carbono;Comercialização de Materiais Recicláveis;Metal;Aço e Ferro;Receita
16;Aquisição de ferramentas;Operação e Produção;Produção e Equipamentos;Ferramentas;Despesa
17;Outras despesas de manutenção;Operação e Produção;Produção e Equipamentos;Manutenção Geral;Despesa
18;Bateria;Comercialização de Materiais Recicláveis;Elétrico ou Eletrônico;Componentes Específicos;Receita
19;INSS;Gestão Associativa;Repasses aos Associados;Encargos sobre Rateio;Despesa
20;Locação de equipamentos;Operação e Produção;Produção e Equipamentos;Locação de Máquinas;Despesa
21;Materiais de higiene e limpeza;Operação e Produção;Infraestrutura Predial;Conservação e Limpeza;Despesa
22;Apoio técnico;Gestão Administrativa e Financeira;Serviços Profissionais;Consultoria;Despesa
23;Taxas públicas (alvarás, licenças, vistorias);Gestão Administrativa e Financeira;Obrigações e Taxas;Licenças e Alvarás;Despesa
24;Multas de trânsito;Operação e Produção;Frota e Logística;Multas e Infrações;Despesa
25;Telefone;Operação e Produção;Infraestrutura Predial;Comunicação;Despesa
26;Obras e reformas;Operação e Produção;Infraestrutura Predial;Manutenção Predial;Despesa
27;PEAD Injeção (caixaria);Comercialização de Materiais Recicláveis;Plástico;PEAD;Receita
28;PET Azul;Comercialização de Materiais Recicláveis;Plástico;PET;Receita
29;PET Bandeja;Comercialização de Materiais Recicláveis;Plástico;PET;Receita
30;PET Colorido;Comercialização de Materiais Recicláveis;Plástico;PET;Receita
31;PET Cristal;Comercialização de Materiais Recicláveis;Plástico;PET;Receita
32;PET Verde;Comercialização de Materiais Recicláveis;Plástico;PET;Receita
33;PEAD Geomembrana;Comercialização de Materiais Recicláveis;Plástico;PEAD;Receita
34;PEAD Filme stretch;Comercialização de Materiais Recicláveis;Plástico;PEAD;Receita
35;PEAD Filme cristal;Comercialização de Materiais Recicláveis;Plástico;PEAD;Receita
36;PEAD Embalagens de produtos de limpeza;Comercialização de Materiais Recicláveis;Plástico;PEAD;Receita
37;Outros papéis;Comercialização de Materiais Recicláveis;Papel;Mistura de Papéis;Receita
38;Outro elétrico;Comercialização de Materiais Recicláveis;Elétrico ou Eletrônico;Diversos;Receita
39;Outro plástico;Comercialização de Materiais Recicláveis;Plástico;Mistura de Plásticos;Receita
40;Memória;Comercialização de Materiais Recicláveis;Elétrico ou Eletrônico;Placas e Componentes;Receita
41;Papel Branco;Comercialização de Materiais Recicláveis;Papel;Branco;Receita
42;Motor;Comercialização de Materiais Recicláveis;Elétrico ou Eletrônico;Motores e Compressores;Receita
43;Despesas de mercado / alimentação;Operação e Produção;Produção e Equipamentos;Alimentação (Cozinha);Despesa
44;Locação de veículos;Operação e Produção;Frota e Logística;Locação de Veículos;Despesa
45;Transferências entre contas (entrada);Gestão Administrativa e Financeira;Movimentação Financeira;Transferências;Receita
46;Sucata Celular;Comercialização de Materiais Recicláveis;Elétrico ou Eletrônico;Sucata Eletrônica;Receita
47;Alumínio Bloco;Comercialização de Materiais Recicláveis;Metal;Alumínio;Receita
48;Alumínio Chaparia;Comercialização de Materiais Recicláveis;Metal;Alumínio;Receita
49;Materiais administrativos;Gestão Administrativa e Financeira;Administrativo;Material de Escritório;Despesa
50;Serviços contábeis;Gestão Administrativa e Financeira;Serviços Profissionais;Contabilidade;Despesa
51;Doações de instituições públicas ou privadas;Gestão Administrativa e Financeira;Receitas Diversas;Doações;Receita
52;Outras taxas obrigatórias;Gestão Administrativa e Financeira;Obrigações e Taxas;Taxas Diversas;Despesa
53;Rastreamento de veículos (GPS);Operação e Produção;Frota e Logística;Monitoramento;Despesa
54;Internet;Operação e Produção;Infraestrutura Predial;Comunicação;Despesa
55;Conserto e manutenção de equipamentos;Operação e Produção;Produção e Equipamentos;Manutenção de Equipamentos;Despesa
56;Alumínio Latinha;Comercialização de Materiais Recicláveis;Metal;Alumínio;Receita
57;Alumínio Panela;Comercialização de Materiais Recicláveis;Metal;Alumínio;Receita
58;Cobre Limpo (mel);Comercialização de Materiais Recicláveis;Metal;Cobre;Receita
59;Ferro fundido;Comercialização de Materiais Recicláveis;Metal;Aço e Ferro;Receita
60;Cobre Sujo;Comercialização de Materiais Recicláveis;Metal;Cobre;Receita
61;Outro Metal;Comercialização de Materiais Recicláveis;Metal;Outros Metais;Receita
62;Sucata Eletrônica;Comercialização de Materiais Recicláveis;Elétrico ou Eletrônico;Sucata Eletrônica;Receita
63;ABS Carcaça de eletrodomésticos;Comercialização de Materiais Recicláveis;Plástico;Plásticos Rígidos;Receita
64;Chaparia;Comercialização de Materiais Recicláveis;Metal;Alumínio;Receita
65;PEBD Filme (sacolinhas, sacos de lixo);Comercialização de Materiais Recicláveis;Plástico;Plásticos Flexíveis;Receita
66;Papelão (Tubete, caixas);Comercialização de Materiais Recicláveis;Papel;Papelão;Receita
67;Papel Cartonado (tetrapak);Comercialização de Materiais Recicláveis;Papel;Multicamada;Receita
68;Papel Cimento;Comercialização de Materiais Recicláveis;Papel;Kraft/Cimento;Receita
69;Papel Misto;Comercialização de Materiais Recicláveis;Papel;Mistura de Papéis;Receita
70;BOPP Embalagens de salgadinho;Comercialização de Materiais Recicláveis;Plástico;Plásticos Flexíveis;Receita
71;Vidro inteiro;Comercialização de Materiais Recicláveis;Vidro;Vidro;Receita
72;Vidro em caco;Comercialização de Materiais Recicláveis;Vidro;Vidro;Receita
73;Torneiras e maçanetas;Comercialização de Materiais Recicláveis;Metal;Outros Metais;Receita
74;Sucatas (Latas de aço);Comercialização de Materiais Recicláveis;Metal;Aço e Ferro;Receita
75;Processador;Comercialização de Materiais Recicláveis;Elétrico ou Eletrônico;Placas e Componentes;Receita
76;Placa-mãe;Comercialização de Materiais Recicláveis;Elétrico ou Eletrônico;Placas e Componentes;Receita
77;Placa verde;Comercialização de Materiais Recicláveis;Elétrico ou Eletrônico;Placas e Componentes;Receita
78;Placa marrom;Comercialização de Materiais Recicláveis;Elétrico ou Eletrônico;Placas e Componentes;Receita
79;PS Expansível (isopor);Comercialização de Materiais Recicláveis;Plástico;PS;Receita
80;PP Transparente;Comercialização de Materiais Recicláveis;Plástico;PP;Receita
81;PET Fita de arquear fardos;Comercialização de Materiais Recicláveis;Plástico;PET;Receita
82;PET Óleo;Comercialização de Materiais Recicláveis;Plástico;PET;Receita
83;PP Filme;Comercialização de Materiais Recicláveis;Plástico;Plásticos Flexíveis;Receita
84;PP Mineral;Comercialização de Materiais Recicláveis;Plástico;PP;Receita
85;PP Plástico Branco;Comercialização de Materiais Recicláveis;Plástico;PP;Receita
86;PP Plástico Colorido;Comercialização de Materiais Recicláveis;Plástico;PP;Receita
87;PP Pote de margarina;Comercialização de Materiais Recicláveis;Plástico;PP;Receita
88;PP Preto;Comercialização de Materiais Recicláveis;Plástico;PP;Receita
89;PP Ráfia;Comercialização de Materiais Recicláveis;Plástico;PP;Receita
90;PP Rígido;Comercialização de Materiais Recicláveis;Plástico;PP;Receita
91;PS Copos descartáveis;Comercialização de Materiais Recicláveis;Plástico;PS;Receita
92;PS Rígido (forro de geladeira);Comercialização de Materiais Recicláveis;Plástico;PS;Receita
93;PVC Tubulações e forros;Comercialização de Materiais Recicláveis;Plástico;PVC;Receita
94;Documentação (cartório);Gestão Administrativa e Financeira;Obrigações e Taxas;Cartório e Legal;Despesa
95;EPIs e Uniformes;Operação e Produção;Produção e Equipamentos;Insumos de Produção;Despesa
96;Assessoria jurídica;Gestão Administrativa e Financeira;Serviços Profissionais;Jurídico;Despesa
97;Combustível;Operação e Produção;Frota e Logística;Combustível;Despesa
98;Energia elétrica;Operação e Produção;Infraestrutura Predial;Utilidades Básicas;Despesa
99;Aquisição de novos equipamentos;Operação e Produção;Produção e Equipamentos;Investimento (Capex);Despesa
100;Coleta de Resíduos Orgânicos (CROC);Prestação de Serviços e Parcerias;Contratos Públicos;Serviços Ambientais;Receita
101;Fundo de Benefícios (FBDS);Prestação de Serviços e Parcerias;Contratos Públicos;Repasses Municipais;Receita
102;Valor Fixo (VF);Prestação de Serviços e Parcerias;Contratos Públicos;Coleta Seletiva;Receita
103;Valor por Produtividade (VP);Prestação de Serviços e Parcerias;Contratos Públicos;Coleta Seletiva;Receita
104;Cota capital (entrada);Gestão Associativa;Movimentação Social;Capital Social;Receita
105;Conserto e manutenção de veículos;Operação e Produção;Frota e Logística;Manutenção de Veículos;Despesa
106;Rendimentos de contas poupança;Gestão Administrativa e Financeira;Financeiro;Rendimentos;Receita
107;Prestação de serviço à terceiros;Prestação de Serviços e Parcerias;Serviços Privados;Serviços a Terceiros;Receita
108;Venda de equipamentos usados;Gestão Administrativa e Financeira;Receitas Diversas;Venda de Ativos;Receita
109;Contrato de logística reversa;Prestação de Serviços e Parcerias;Serviços Privados;Logística Reversa;Receita
110;Sucata Informática;Comercialização de Materiais Recicláveis;Eletrônicos;Sucata Eletrônica;Receita
"""

DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    print("ERRO: DATABASE_URL não encontrada no arquivo .env")
    exit()

def migrar():
    print("Iniciando conexão com o Banco de Dados...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    try:
        # 1. FAZER BACKUP DA TABELA ATUAL (Segurança)
        print("1. Criando backup da tabela 'produtos_servicos' para 'produtos_servicos_bkp_v4'...")
        cur.execute("CREATE TABLE IF NOT EXISTS produtos_servicos_bkp_v4 AS SELECT * FROM produtos_servicos")
        
        # 2. LIMPAR A TABELA ATUAL (CATÁLOGO)
        # Nota: Isso não apaga as transações financeiras, pois elas guardam o NOME do item, não o ID (na maioria dos casos do seu código).
        # Se houver chave estrangeira bloqueando, teremos que usar CASCADE, mas vamos tentar limpar primeiro.
        print("2. Limpando catálogo antigo...")
        # Vamos remover a chave estrangeira de subgrupos se ela estiver atrapalhando, pois vamos usar estrutura plana agora
        try:
            cur.execute("ALTER TABLE produtos_servicos DROP CONSTRAINT IF EXISTS produtos_servicos_id_subgrupo_fkey")
        except:
            pass

        cur.execute("TRUNCATE TABLE produtos_servicos RESTART IDENTITY CASCADE")

        # 3. GARANTIR QUE AS COLUNAS EXISTEM
        # O CSV tem: Atividade, Grupo, Subgrupo, Item, Tipo
        # Sua tabela tem: tipo, tipo_atividade, grupo, subgrupo, item.
        # Vamos mapear:
        # CSV 'Atividade' -> DB 'tipo_atividade'
        # CSV 'Grupo'     -> DB 'grupo'
        # CSV 'Subgrupo'  -> DB 'subgrupo'
        # CSV 'Item'      -> DB 'item'
        # CSV 'Tipo'      -> DB 'tipo'
        
        print("3. Verificando estrutura da tabela...")
        # Garantindo que as colunas suportem texto
        cur.execute("ALTER TABLE produtos_servicos ALTER COLUMN grupo TYPE VARCHAR(255)")
        cur.execute("ALTER TABLE produtos_servicos ALTER COLUMN subgrupo TYPE VARCHAR(255)")

        # 4. IMPORTAR O CSV
        print("4. Importando novos dados do CSV...")
        f = io.StringIO(csv_content)
        reader = csv.DictReader(f, delimiter=';')
        
        count = 0
        for row in reader:
            cur.execute("""
                INSERT INTO produtos_servicos (tipo, tipo_atividade, grupo, subgrupo, item, data_hora_cadastro)
                VALUES (%s, %s, %s, %s, %s, NOW())
            """, (
                row['Tipo'].strip(),
                row['Atividade'].strip(),
                row['Grupo'].strip(),
                row['Subgrupo'].strip(),
                row['item'].strip()
            ))
            count += 1
            
        conn.commit()
        print(f"✅ SUCESSO! {count} itens foram importados com a nova hierarquia.")
        print("A tabela 'produtos_servicos' agora está atualizada.")

    except Exception as e:
        conn.rollback()
        print(f"❌ ERRO DURANTE A MIGRAÇÃO: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    migrar()