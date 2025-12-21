print("--- O SCRIPT ESTÁ RODANDO! (VERSÃO FINAL) ---")

import psycopg2
import os
from dotenv import load_dotenv

# Carrega as variáveis
load_dotenv()

# --- CONFIGURAÇÕES ---
LOCAL_DB = {
    "host": "localhost",
    "database": "recic3",
    "user": "postgres",
    "password": "postgres", # <--- Verifique sua senha
    "port": "5432"
}
CLOUD_URL = os.getenv('DATABASE_URL')

# Lista Completa (Pode descomentar tudo agora)
TABELAS_ORDEM = [
    # "cadastros",                   # Já foi
    # "contas_correntes",            # Já foi
    "produtos_servicos",           # Já foi
    # "associados",                  # Já foi
    # "transacoes_financeiras",      # Já foi
    #"itens_transacao",             # <--- Se ainda não terminou, deixe aqui
    #"fluxo_caixa",                 # <--- Se ainda não terminou, deixe aqui
    #"fluxo_caixa_transacoes_link", # <--- A TABELA PROBLEMÁTICA (Agora vai funcionar)
    #"denuncias"                    # <--- A última
]

def migrar():
    if not CLOUD_URL:
        print("ERRO CRÍTICO: .env não encontrado")
        return

    print("=== INICIANDO MIGRAÇÃO ===")
    
    conn_local = None
    conn_cloud = None

    try:
        conn_local = psycopg2.connect(**LOCAL_DB)
        conn_cloud = psycopg2.connect(CLOUD_URL, sslmode='require')
        cur_local = conn_local.cursor()
        cur_cloud = conn_cloud.cursor()

        for tabela in TABELAS_ORDEM:
            print(f"\n📦 Processando tabela: {tabela}...")
            
            # --- LÓGICA ESPECIAL PARA TABELAS SEM ID ---
            if tabela == "fluxo_caixa_transacoes_link":
                order_by = "id_fluxo_caixa" # Ordena pelo primeiro campo da chave
                conflict_target = "(id_fluxo_caixa, id_transacao_financeira)" # Chave composta
                tem_id_serial = False
            else:
                order_by = "id"
                conflict_target = "(id)"
                tem_id_serial = True
            
            # 1. Lê do local
            try:
                cur_local.execute(f"SELECT * FROM {tabela} ORDER BY {order_by}")
                dados = cur_local.fetchall()
            except Exception as e:
                print(f"   [Pular] Erro ao ler tabela local {tabela}: {str(e).splitlines()[0]}")
                conn_local.rollback()
                continue

            if not dados:
                print("   [Vazio] Nada para copiar.")
                continue

            print(f"   -> Encontrados {len(dados)} registros. Copiando...")

            # 2. Prepara inserção
            colunas = [desc[0] for desc in cur_local.description]
            cols_str = ', '.join(colunas)
            placeholders = ', '.join(['%s'] * len(colunas))
            
            query = f"""
                INSERT INTO {tabela} ({cols_str}) 
                VALUES ({placeholders}) 
                ON CONFLICT {conflict_target} DO NOTHING;
            """
            
            sucessos = 0
            erros = 0
            
            for linha in dados:
                try:
                    cur_cloud.execute(query, linha)
                    conn_cloud.commit()
                    sucessos += 1
                except Exception as e:
                    conn_cloud.rollback()
                    erros += 1
                    # Mostra erro simplificado
                    print(f"   [X] Falha: {str(e).splitlines()[0]}")
            
            print(f"   -> Resumo: {sucessos} salvos, {erros} falhas.")
            
            # 3. Ajusta ID (apenas para tabelas normais)
            if tem_id_serial:
                try:
                    cur_cloud.execute(f"SELECT setval(pg_get_serial_sequence('{tabela}', 'id'), COALESCE(MAX(id), 1)) FROM {tabela};")
                    conn_cloud.commit()
                except: pass

    except Exception as e:
        print(f"❌ Erro Geral: {e}")
    finally:
        if conn_local: conn_local.close()
        if conn_cloud: conn_cloud.close()
        print("\n--- FIM ---")

if __name__ == "__main__":
    migrar()