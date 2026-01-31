import psycopg2
import os
from dotenv import load_dotenv

load_dotenv() # Carrega sua senha do .env

def listar_ultimas_transacoes():
    try:
        print("🔌 Conectando na Nuvem para verificar...")
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()
        
        # Busca as 5 últimas transações
        cur.execute("""
            SELECT id, data_documento, valor_total_documento, tipo_transacao 
            FROM transacoes_financeiras 
            ORDER BY id DESC 
            LIMIT 5;
        """)
        
        registros = cur.fetchall()
        
        if not registros:
            print("⚠️ NENHUM registro encontrado no banco da nuvem.")
        else:
            print(f"✅ Encontrei {len(registros)} registros recentes:")
            for reg in registros:
                print(f"   ID: {reg[0]} | Data: {reg[1]} | Valor: R$ {reg[2]} | Tipo: {reg[3]}")
                
        conn.close()
    except Exception as e:
        print(f"❌ Erro ao conectar: {e}")

if __name__ == "__main__":
    listar_ultimas_transacoes()