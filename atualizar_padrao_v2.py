import psycopg2
import os
from dotenv import load_dotenv

# Carrega as configurações do .env (onde está sua senha do Neon)
load_dotenv()

# --- LISTA PADRÃO DE RECICLAGEM ---
# Isso garante que seu banco na nuvem não comece vazio
DADOS_PADRAO = {
    "Papel": ["Papelão", "Papel Branco", "Jornal/Revista", "Papel Misto", "Tetra Pak"],
    "Plástico": ["PET Transparente", "PET Verde", "PET Óleo", "PEAD (Leitoso)", "PP (Mole)", "PVC", "Plástico Misto", "Filme Stretch"],
    "Vidro": ["Garrafa Inteira", "Cacos Limpos", "Vidro Misto", "Vidro Plano"],
    "Metal": ["Alumínio Latinha", "Alumínio Perfil", "Alumínio Panela", "Ferro", "Cobre", "Bronze", "Aço Inox", "Sucata Mista"],
    "Eletrônicos": ["Linha Branca", "Linha Marrom", "Placas de Circuito", "Fios e Cabos", "Baterias"],
    "Perigosos/Outros": ["Óleo de Cozinha", "Pneus", "Rejeito", "Entulho"]
}

def atualizar_estrutura():
    url = os.getenv("DATABASE_URL")
    if not url:
        print("❌ Erro CRÍTICO: DATABASE_URL não encontrada no .env")
        print("   Verifique se a linha do banco NÃO está com # na frente.")
        return

    conn = None
    try:
        print("🔌 Conectando ao banco de dados NEON...")
        conn = psycopg2.connect(url)
        cur = conn.cursor()

        # 1. CRIAR TABELA DE GRUPOS
        print("🔨 Criando/Verificando tabela 'grupos_atividade'...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS grupos_atividade (
                id SERIAL PRIMARY KEY,
                nome_grupo VARCHAR(100) UNIQUE NOT NULL
            );
        """)

        # 2. CRIAR TABELA DE SUBGRUPOS
        print("🔨 Criando/Verificando tabela 'subgrupos'...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subgrupos (
                id SERIAL PRIMARY KEY,
                nome_subgrupo VARCHAR(100) NOT NULL,
                id_grupo INTEGER REFERENCES grupos_atividade(id) ON DELETE CASCADE,
                UNIQUE(nome_subgrupo, id_grupo)
            );
        """)

        # 3. ATUALIZAR TABELA DE PRODUTOS (Adicionar as colunas novas)
        print("🔧 Atualizando tabela 'produtos_servicos' com colunas de relacionamento...")
        cur.execute("""
            ALTER TABLE produtos_servicos
            ADD COLUMN IF NOT EXISTS id_subgrupo INTEGER REFERENCES subgrupos(id),
            ADD COLUMN IF NOT EXISTS id_grupo INTEGER REFERENCES grupos_atividade(id);
        """)

        # 4. POPULAR DADOS (Inserir Papel, Plástico, etc.)
        print("📥 Inserindo dados padrão de reciclagem...")
        for grupo, lista_subgrupos in DADOS_PADRAO.items():
            # Inserir Grupo (Se já existe, pega o ID)
            cur.execute("""
                INSERT INTO grupos_atividade (nome_grupo) VALUES (%s) 
                ON CONFLICT (nome_grupo) DO UPDATE SET nome_grupo = EXCLUDED.nome_grupo 
                RETURNING id;
            """, (grupo,))
            id_grupo = cur.fetchone()[0]

            # Inserir Subgrupos
            for sub in lista_subgrupos:
                cur.execute("""
                    INSERT INTO subgrupos (nome_subgrupo, id_grupo)
                    VALUES (%s, %s)
                    ON CONFLICT (nome_subgrupo, id_grupo) DO NOTHING;
                """, (sub, id_grupo))

        conn.commit()
        print("✅ SUCESSO! Estrutura atualizada e dados carregados na nuvem.")

    except Exception as e:
        print(f"❌ Erro ao atualizar banco: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    atualizar_estrutura()