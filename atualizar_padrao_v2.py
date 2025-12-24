import psycopg2
import os
from dotenv import load_dotenv

# Carrega as configurações do .env (onde está sua senha do Neon)
load_dotenv()

# --- LISTA PADRÃO DE RECICLAGEM ---
DADOS_PADRAO = {
    "Papel": ["Papelão", "Papel Branco", "Jornal/Revista", "Papel Misto", "Tetra Pak"],
    "Plástico": ["PET Transparente", "PET Verde", "PET Óleo", "PEAD (Leitoso)", "PP (Mole)", "PVC", "Plástico Misto", "Filme Stretch", "ABS", "PS (Isopor)", "Outros Plásticos"],
    "Vidro": ["Garrafa Inteira", "Cacos Limpos", "Vidro Misto", "Vidro Plano"],
    "Metal": ["Alumínio Latinha", "Alumínio Perfil", "Alumínio Panela", "Ferro", "Cobre", "Bronze", "Aço Inox", "Sucata Mista"],
    "Eletrônicos": ["Linha Branca", "Linha Marrom", "Placas de Circuito", "Fios e Cabos", "Baterias"],
    "Perigosos/Outros": ["Óleo de Cozinha", "Pneus", "Rejeito", "Entulho"]
}

def atualizar_estrutura():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("❌ Erro CRÍTICO: DATABASE_URL não encontrada no .env")
        return

    conn = None
    try:
        print("🔌 Conectando ao banco de dados NEON...")
        conn = psycopg2.connect(url)
        cur = conn.cursor()

        # 1. CRIAR TABELA DE GRUPOS (com 'nome')
        print("🔨 Criando/Verificando tabela 'grupos_atividade'...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS grupos_atividade (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(100) UNIQUE NOT NULL
            );
        """)

        # 2. CRIAR TABELA DE SUBGRUPOS (com 'nome')
        print("🔨 Criando/Verificando tabela 'subgrupos'...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subgrupos (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(100) NOT NULL,
                id_grupo INTEGER REFERENCES grupos_atividade(id) ON DELETE CASCADE,
                UNIQUE(nome, id_grupo)
            );
        """)

        # 3. POPULAR DADOS
        print("📥 Inserindo dados padrão de reciclagem...")
        for grupo, lista_subgrupos in DADOS_PADRAO.items():
            # Inserir Grupo
            cur.execute("""
                INSERT INTO grupos_atividade (nome) VALUES (%s) 
                ON CONFLICT (nome) DO UPDATE SET nome = EXCLUDED.nome 
                RETURNING id;
            """, (grupo,))
            id_grupo = cur.fetchone()[0]

            # Inserir Subgrupos
            for sub in lista_subgrupos:
                cur.execute("""
                    INSERT INTO subgrupos (nome, id_grupo)
                    VALUES (%s, %s)
                    ON CONFLICT (nome, id_grupo) DO NOTHING;
                """, (sub, id_grupo))

        conn.commit()
        print("✅ SUCESSO! Dados atualizados na Nuvem com a nova estrutura.")

    except Exception as e:
        print(f"❌ Erro ao atualizar banco: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    atualizar_estrutura()