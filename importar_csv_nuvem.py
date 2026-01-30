import psycopg2
import csv
import os
from dotenv import load_dotenv

# Carrega a senha do banco Neon do arquivo .env
load_dotenv()

def importar_csv_para_nuvem():
    arquivo_csv = 'padrao_itens2.csv'
    
    # 1. Verifica se o CSV existe
    if not os.path.exists(arquivo_csv):
        print(f"❌ ERRO: O arquivo '{arquivo_csv}' não foi encontrado na pasta.")
        print("   Certifique-se de que ele está na mesma pasta deste script.")
        return

    # 2. Conecta na Nuvem
    url = os.getenv("DATABASE_URL")
    if not url:
        print("❌ ERRO: DATABASE_URL não encontrada no .env")
        return

    conn = None
    try:
        print("🔌 Conectando ao NEON...")
        conn = psycopg2.connect(url)
        cur = conn.cursor()
        print(f"--- LENDO '{arquivo_csv}' E ATUALIZANDO O BANCO ---")

        itens_lidos = 0
        itens_atualizados = 0
        grupos_processados = 0
        subgrupos_processados = 0

        # Tenta ler com utf-8, se falhar tenta latin-1 (padrão Excel antigo)
        try:
            f = open(arquivo_csv, 'r', encoding='utf-8')
            leitor = csv.DictReader(f, delimiter=';')
            list(leitor) # Teste de leitura
            f.seek(0)
            leitor = csv.DictReader(f, delimiter=';')
        except UnicodeDecodeError:
            f = open(arquivo_csv, 'r', encoding='latin-1')
            leitor = csv.DictReader(f, delimiter=';')

        for linha in leitor:
            itens_lidos += 1
            
            # Pega os dados das colunas do CSV (ajuste os nomes se necessário)
            # O .strip() remove espaços extras no começo e fim
            tipo_transacao = linha.get('Tipo Transação', '').strip()
            nome_grupo = linha.get('Grupo', '').strip()          # Ex: "Plástico"
            nome_subgrupo = linha.get('Subgrupo', '').strip()    # Ex: "PET"
            nome_item = linha.get('Item Descrição (Transação)', '').strip()

            if not nome_item:
                continue

            # Se o subgrupo estiver vazio no CSV, usamos "Geral"
            if not nome_subgrupo: nome_subgrupo = "Geral"

            # --- PASSO A: Resolver o GRUPO ---
            # Verifica se o grupo existe, se não, cria e pega o ID
            cur.execute("""
                INSERT INTO grupos_atividade (nome) VALUES (%s)
                ON CONFLICT (nome) DO UPDATE SET nome = EXCLUDED.nome
                RETURNING id;
            """, (nome_grupo,))
            id_grupo = cur.fetchone()[0]
            grupos_processados += 1

            # --- PASSO B: Resolver o SUBGRUPO ---
            # Cria o subgrupo vinculado ao ID do Grupo
            cur.execute("""
                INSERT INTO subgrupos (nome, id_grupo) 
                VALUES (%s, %s)
                ON CONFLICT (nome, id_grupo) DO UPDATE SET nome = EXCLUDED.nome
                RETURNING id;
            """, (nome_subgrupo, id_grupo))
            id_subgrupo = cur.fetchone()[0]
            subgrupos_processados += 1

            # --- PASSO C: Atualizar o PRODUTO ---
            # Procura pelo NOME DO ITEM e atualiza suas classificações
            cur.execute("""
                UPDATE produtos_servicos 
                SET tipo = %s,              -- Receita ou Despesa
                    tipo_atividade = %s,    -- Nome do Grupo (legado)
                    grupo = %s,             -- Nome do Grupo (legado)
                    subgrupo = %s,          -- Nome do Subgrupo (legado)
                    id_grupo = %s,          -- Novo vínculo ID
                    id_subgrupo = %s        -- Novo vínculo ID
                WHERE item = %s
            """, (tipo_transacao, nome_grupo, nome_grupo, nome_subgrupo, id_grupo, id_subgrupo, nome_item))
            
            if cur.rowcount > 0:
                itens_atualizados += 1
            else:
                # Opcional: Se quiser INSERIR itens que não existem
                # Descomente as linhas abaixo se quiser cadastrar novos
                """
                cur.execute('''
                    INSERT INTO produtos_servicos (item, tipo, tipo_atividade, grupo, subgrupo, id_grupo, id_subgrupo)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                ''', (nome_item, tipo_transacao, nome_grupo, nome_grupo, nome_subgrupo, id_grupo, id_subgrupo))
                itens_atualizados += 1
                """

        conn.commit()
        print("-" * 40)
        print("✅ ATUALIZAÇÃO CONCLUÍDA NA NUVEM")
        print(f"📊 Linhas lidas do CSV: {itens_lidos}")
        print(f"📦 Produtos atualizados/sincronizados: {itens_atualizados}")
        print("-" * 40)

    except Exception as e:
        print(f"❌ Erro crítico: {e}")
        if conn: conn.rollback()
    finally:
        if 'f' in locals(): f.close()
        if conn: conn.close()

if __name__ == "__main__":
    importar_csv_para_nuvem()