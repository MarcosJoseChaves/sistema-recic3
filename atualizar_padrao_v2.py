import psycopg2
import csv
import os

# --- SUAS CONFIGURAÇÕES DE BANCO LOCAL ---
DB_CONFIG = {
    "host": "localhost",
    "port": "5432",
    "database": "recic3",
    "user": "postgres",
    "password": "postgres"
}

def conectar_banco():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Erro ao conectar ao banco: {e}")
        return None

def ajustar_base_pelo_csv_v2():
    # Nome do novo arquivo
    arquivo_csv = 'padrao_itens2.csv'
    
    if not os.path.exists(arquivo_csv):
        print(f"ERRO: O arquivo '{arquivo_csv}' não foi encontrado na pasta.")
        print("Certifique-se de que ele está na mesma pasta deste script.")
        return

    conn = conectar_banco()
    if not conn:
        return

    cur = conn.cursor()
    print(f"--- LENDO '{arquivo_csv}' E ATUALIZANDO O BANCO ---")

    itens_lidos = 0
    itens_atualizados = 0
    subgrupos_criados = 0

    try:
        # Tenta ler com utf-8, se falhar tenta latin-1
        try:
            f = open(arquivo_csv, 'r', encoding='utf-8')
            leitor = csv.DictReader(f, delimiter=';')
            list(leitor) # Teste
            f.seek(0)
            leitor = csv.DictReader(f, delimiter=';')
        except UnicodeDecodeError:
            f = open(arquivo_csv, 'r', encoding='latin-1')
            leitor = csv.DictReader(f, delimiter=';')

        for linha in leitor:
            itens_lidos += 1
            
            # Pega os dados das colunas do CSV
            tipo_transacao = linha.get('Tipo Transação', '').strip()
            grupo = linha.get('Grupo', '').strip()          # Atividade Pai
            subgrupo = linha.get('Subgrupo', '').strip()
            item_nome = linha.get('Item Descrição (Transação)', '').strip()

            if not item_nome:
                continue

            # 1. Garante que o Subgrupo existe na tabela 'subgrupos' ligado ao Grupo correto
            # Se o subgrupo estiver vazio no CSV, usamos "Geral"
            if not subgrupo: subgrupo = "Geral"

            cur.execute("""
                SELECT id FROM subgrupos 
                WHERE nome = %s AND atividade_pai = %s
            """, (subgrupo, grupo))
            
            res_sub = cur.fetchone()
            
            if res_sub:
                id_subgrupo = res_sub[0]
            else:
                # Cria se não existir
                try:
                    cur.execute("""
                        INSERT INTO subgrupos (nome, atividade_pai) 
                        VALUES (%s, %s) 
                        RETURNING id
                    """, (subgrupo, grupo))
                    id_subgrupo = cur.fetchone()[0]
                    subgrupos_criados += 1
                    print(f" [+] Subgrupo criado: {subgrupo} (em {grupo})")
                except Exception as e:
                    print(f"Erro ao criar subgrupo {subgrupo}: {e}")
                    continue

            # 2. Atualiza o produto existente
            # O script procura pelo NOME DO ITEM e atualiza suas classificações
            cur.execute("""
                UPDATE produtos_servicos 
                SET tipo = %s,              -- Atualiza Receita/Despesa
                    tipo_atividade = %s,    -- Atualiza o Grupo
                    grupo = %s,             -- Campo redundante
                    subgrupo = %s,          -- Campo redundante
                    id_subgrupo = %s        -- Vincula ao ID
                WHERE item = %s
            """, (tipo_transacao, grupo, grupo, subgrupo, id_subgrupo, item_nome))
            
            if cur.rowcount > 0:
                itens_atualizados += 1
            
            # Opcional: Se quiser CADASTRAR itens novos que estão no CSV mas não no banco:
            # else:
            #    cur.execute("INSERT INTO produtos_servicos ...")

        conn.commit()
        print("-" * 40)
        print("ATUALIZAÇÃO CONCLUÍDA")
        print(f"Itens lidos do CSV: {itens_lidos}")
        print(f"Subgrupos novos criados: {subgrupos_criados}")
        print(f"Produtos atualizados no banco: {itens_atualizados}")
        print("-" * 40)

    except Exception as e:
        print(f"Erro crítico: {e}")
        conn.rollback()
    finally:
        if 'f' in locals(): f.close()
        cur.close()
        conn.close()

if __name__ == "__main__":
    ajustar_base_pelo_csv_v2()