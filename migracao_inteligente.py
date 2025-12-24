import psycopg2

def conectar_banco():
    """Conecta ao banco de dados LOCAL com suas credenciais"""
    
    # --- SUAS CONFIGURAÇÕES LOCAIS ---
    dados_conexao = {
        "host": "localhost",
        "port": "5432",
        "database": "recic3",    # Nome do seu banco
        "user": "postgres",      # Seu usuário
        "password": "postgres"   # Sua senha
    }
    # ---------------------------------

    print(f"Tentando conectar em: {dados_conexao['host']} (Banco: {dados_conexao['database']})...")

    try:
        conn = psycopg2.connect(**dados_conexao)
        return conn
    except Exception as e:
        print(f"ERRO CRÍTICO ao conectar no banco: {e}")
        print("DICA: Verifique se o PostgreSQL está rodando e se a senha está correta.")
        return None

def preparar_banco_se_necessario(conn):
    """Cria a tabela 'subgrupos' se ela ainda não existir no seu computador"""
    cur = conn.cursor()
    try:
        print("--- VERIFICANDO ESTRUTURA DO BANCO ---")
        
        # 1. Cria a tabela subgrupos
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subgrupos (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(150) NOT NULL,
                atividade_pai VARCHAR(255) NOT NULL, -- O Grupo Pai
                CONSTRAINT uq_subgrupo_atividade UNIQUE (nome, atividade_pai)
            );
        """)
        
        # 2. Adiciona a coluna id_subgrupo na tabela de produtos (se não existir)
        cur.execute("""
            ALTER TABLE produtos_servicos 
            ADD COLUMN IF NOT EXISTS id_subgrupo INTEGER REFERENCES subgrupos(id);
        """)
        
        conn.commit()
        print("Tabela 'subgrupos' verificada/criada com sucesso.")
        return True
    except Exception as e:
        conn.rollback()
        print(f"Erro ao preparar o banco: {e}")
        return False

def executar_migracao():
    conn = conectar_banco()
    if not conn:
        return

    # --- PASSO NOVO: CRIA A TABELA ANTES DE TENTAR USAR ---
    if not preparar_banco_se_necessario(conn):
        return
    # ------------------------------------------------------

    cur = conn.cursor()
    
    print("--- INICIANDO MIGRAÇÃO DE DADOS EXISTENTES ---")

    try:
        # 1. Busca todos os produtos atuais para analisar
        cur.execute("""
            SELECT id, item, grupo, subgrupo, tipo_atividade 
            FROM produtos_servicos
        """)
        produtos = cur.fetchall()
        
        print(f"Lendo {len(produtos)} itens cadastrados...")
        
        novos_subgrupos = 0
        itens_atualizados = 0

        for prod in produtos:
            id_prod = prod[0]
            nome_item = prod[1]
            antigo_grupo = prod[2]      # Ex: "Papel"
            antigo_subgrupo = prod[3]   # Ex: "Papelão"
            atividade_pai = prod[4]     # Ex: "Venda de Recicláveis"

            # Validação básica
            if not atividade_pai:
                print(f" > Item '{nome_item}' (ID {id_prod}) sem Atividade. Pulando.")
                continue

            # --- LÓGICA INTELIGENTE ---
            # Define quem será o novo Subgrupo
            # Prioridade 1: O antigo campo 'grupo' (ex: "Papel")
            nome_final_subgrupo = antigo_grupo
            
            # Prioridade 2: Se 'grupo' estiver vazio, tenta o 'subgrupo' antigo
            if not nome_final_subgrupo or not nome_final_subgrupo.strip():
                nome_final_subgrupo = antigo_subgrupo
            
            # Prioridade 3: Se tudo for vazio, chama de "Geral"
            if not nome_final_subgrupo or not nome_final_subgrupo.strip():
                nome_final_subgrupo = "Geral"
                
            nome_final_subgrupo = nome_final_subgrupo.strip()

            # 2. Garante que esse Subgrupo existe na tabela nova
            cur.execute("""
                SELECT id FROM subgrupos 
                WHERE nome = %s AND atividade_pai = %s
            """, (nome_final_subgrupo, atividade_pai))
            
            resultado = cur.fetchone()
            
            if resultado:
                id_subgrupo_novo = resultado[0]
            else:
                # Cria se não existir
                cur.execute("""
                    INSERT INTO subgrupos (nome, atividade_pai) 
                    VALUES (%s, %s) 
                    RETURNING id
                """, (nome_final_subgrupo, atividade_pai))
                id_subgrupo_novo = cur.fetchone()[0]
                novos_subgrupos += 1
                print(f" [+] Novo Subgrupo criado: '{nome_final_subgrupo}' (dentro de {atividade_pai})")

            # 3. Atualiza o produto com o vínculo correto
            cur.execute("""
                UPDATE produtos_servicos 
                SET id_subgrupo = %s,
                    grupo = %s,       -- Atualiza campo texto 'grupo' para ser igual à Atividade
                    subgrupo = %s     -- Atualiza campo texto 'subgrupo' para o nome correto
                WHERE id = %s
            """, (id_subgrupo_novo, atividade_pai, nome_final_subgrupo, id_prod))
            
            itens_atualizados += 1

        conn.commit()
        print("-" * 40)
        print("SUCESSO! BANCO DE DADOS ATUALIZADO.")
        print(f"Subgrupos criados: {novos_subgrupos}")
        print(f"Produtos ajustados: {itens_atualizados}")
        print("-" * 40)

    except Exception as e:
        conn.rollback()
        print(f"ERRO DURANTE A MIGRAÇÃO: {e}")
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    executar_migracao()