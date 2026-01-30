import os
import csv
import psycopg2
from werkzeug.security import generate_password_hash
from dotenv import load_dotenv

# 1. Carregar configurações do .env
load_dotenv()
DATABASE_URL = os.getenv('DATABASE_URL')

def reset_emergencia():
    print("--- INICIANDO RESET DE EMERGÊNCIA COMPLETO (Sincronizado com app.py) ---")
    conn = None
    try:
        # Conexão com o banco
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # PASSO 1: APAGAR TUDO (Ordem correta para evitar erros de Foreign Key)
        print("1. Limpando banco de dados...")
        tabelas_para_dropar = [
            "fluxo_caixa_transacoes_link", "fluxo_caixa", "itens_transacao", 
            "transacoes_financeiras", "produtos_servicos", "subgrupos", 
            "associados", "cadastros", "solicitacoes_alteracao", "denuncias", 
            "documentos", "tipos_documentos", "contas_correntes", "usuarios"
        ]
        for tabela in tabelas_para_dropar:
            cur.execute(f"DROP TABLE IF EXISTS {tabela} CASCADE;")
        
        # PASSO 2: RECRIAÇÃO DA ESTRUTURA COMPLETA
        print("2. Recriando estrutura sincronizada com o código atual...")

        # --- USUÁRIOS ---
        cur.execute("""
            CREATE TABLE usuarios (
                id SERIAL PRIMARY KEY,
                username VARCHAR(50) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                nome_completo VARCHAR(100),
                role VARCHAR(20) NOT NULL,
                uvr_acesso VARCHAR(50),
                ativo BOOLEAN DEFAULT TRUE
            )
        """)

        # --- CADASTROS BASE ---
        cur.execute("""
            CREATE TABLE cadastros (
                id SERIAL PRIMARY KEY,
                uvr VARCHAR(10) NOT NULL,
                associacao VARCHAR(50),
                data_hora_cadastro TIMESTAMP NOT NULL DEFAULT NOW(),
                razao_social VARCHAR(255) NOT NULL,
                cnpj VARCHAR(14) NOT NULL,
                cep VARCHAR(8) NOT NULL,
                logradouro VARCHAR(255), 
                numero VARCHAR(20),
                bairro VARCHAR(100),
                cidade VARCHAR(100),
                uf VARCHAR(2), 
                telefone VARCHAR(20),
                tipo_atividade VARCHAR(255) NOT NULL,
                tipo_cadastro VARCHAR(50) NOT NULL,
                CONSTRAINT uq_cadastros_cnpj_tipo_uvr UNIQUE (cnpj, tipo_cadastro, uvr)
            )
        """)

        cur.execute("""
            CREATE TABLE associados (
                id SERIAL PRIMARY KEY,
                numero VARCHAR(20) NOT NULL,
                uvr VARCHAR(10) NOT NULL,
                associacao VARCHAR(50) NOT NULL,
                nome VARCHAR(255) NOT NULL,
                cpf VARCHAR(11) UNIQUE NOT NULL, 
                rg VARCHAR(20) NOT NULL,
                data_nascimento DATE NOT NULL,
                data_admissao DATE NOT NULL,
                status VARCHAR(20) NOT NULL,
                cep VARCHAR(8) NOT NULL,
                logradouro VARCHAR(255), 
                endereco_numero VARCHAR(20),
                bairro VARCHAR(100),
                cidade VARCHAR(100),
                uf VARCHAR(2),
                telefone VARCHAR(20) NOT NULL,
                data_hora_cadastro TIMESTAMP NOT NULL DEFAULT NOW(),
                foto_base64 TEXT
            )
        """)

        # --- FINANCEIRO E TRANSAÇÕES ---
        cur.execute("""
            CREATE TABLE transacoes_financeiras (
                id SERIAL PRIMARY KEY,
                uvr VARCHAR(10) NOT NULL,
                associacao VARCHAR(50) NOT NULL,
                id_cadastro_origem INTEGER REFERENCES cadastros(id),
                nome_cadastro_origem VARCHAR(255) NOT NULL, 
                numero_documento VARCHAR(100),
                data_documento DATE NOT NULL,
                tipo_transacao VARCHAR(20) NOT NULL, 
                tipo_atividade VARCHAR(255) NOT NULL,
                valor_total_documento DECIMAL(12, 2) NOT NULL,
                data_hora_registro TIMESTAMP NOT NULL DEFAULT NOW(),
                valor_pago_recebido DECIMAL(12, 2) DEFAULT 0.00,
                status_pagamento VARCHAR(30) DEFAULT 'Aberto'
            )
        """)

        cur.execute("""
            CREATE TABLE itens_transacao (
                id SERIAL PRIMARY KEY,
                id_transacao INTEGER NOT NULL REFERENCES transacoes_financeiras(id) ON DELETE CASCADE,
                descricao VARCHAR(255) NOT NULL,
                unidade VARCHAR(50) NOT NULL,
                quantidade DECIMAL(10, 3) NOT NULL, 
                valor_unitario DECIMAL(12, 2) NOT NULL,
                valor_total_item DECIMAL(12, 2) NOT NULL
            )
        """)

        # --- TABELA DE SUBGRUPOS (Restaurada para consistência) ---
        cur.execute("""
            CREATE TABLE subgrupos (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(255) NOT NULL,
                atividade_pai VARCHAR(255) NOT NULL,
                UNIQUE(nome, atividade_pai)
            )
        """)

        # --- CATÁLOGO DE PRODUTOS/SERVIÇOS (Incluso Subgrupo como coluna de texto) ---
        cur.execute("""
            CREATE TABLE produtos_servicos (
                id SERIAL PRIMARY KEY,
                tipo VARCHAR(20) NOT NULL,
                tipo_atividade VARCHAR(255) NOT NULL,
                grupo VARCHAR(255),
                subgrupo VARCHAR(255),
                item VARCHAR(255) NOT NULL UNIQUE,
                data_hora_cadastro TIMESTAMP NOT NULL DEFAULT NOW(),
                id_subgrupo INTEGER REFERENCES subgrupos(id)
            )
        """)

        # --- CONTAS E FLUXO DE CAIXA ---
        cur.execute("""
            CREATE TABLE contas_correntes (
                id SERIAL PRIMARY KEY,
                uvr VARCHAR(10) NOT NULL,
                associacao VARCHAR(50) NOT NULL,
                banco_codigo VARCHAR(10) NOT NULL,
                banco_nome VARCHAR(100) NOT NULL,
                agencia VARCHAR(10) NOT NULL,
                conta_corrente VARCHAR(20) NOT NULL,
                descricao_conta VARCHAR(255),
                data_hora_cadastro TIMESTAMP NOT NULL DEFAULT NOW(),
                UNIQUE (uvr, banco_codigo, agencia, conta_corrente)
            )
        """)

        cur.execute("""
            CREATE TABLE fluxo_caixa (
                id SERIAL PRIMARY KEY,
                uvr VARCHAR(10) NOT NULL,
                associacao VARCHAR(50) NOT NULL,
                tipo_movimentacao VARCHAR(20) NOT NULL,
                id_cadastro_cf INTEGER REFERENCES cadastros(id),
                nome_cadastro_cf VARCHAR(255),
                id_conta_corrente INTEGER NOT NULL REFERENCES contas_correntes(id),
                numero_documento_bancario VARCHAR(100),
                data_efetiva DATE NOT NULL,
                valor_efetivo DECIMAL(12, 2) NOT NULL,
                saldo_operacao_calculado DECIMAL(12, 2) NOT NULL,
                data_hora_registro_fluxo TIMESTAMP NOT NULL DEFAULT NOW(),
                observacoes TEXT,
                categoria VARCHAR(100)
            )
        """)

        cur.execute("""
            CREATE TABLE fluxo_caixa_transacoes_link (
                id_fluxo_caixa INTEGER NOT NULL REFERENCES fluxo_caixa(id) ON DELETE CASCADE,
                id_transacao_financeira INTEGER NOT NULL REFERENCES transacoes_financeiras(id),
                valor_aplicado_nesta_nf DECIMAL(12,2) NOT NULL,
                PRIMARY KEY (id_fluxo_caixa, id_transacao_financeira)
            )
        """)

        # --- GESTÃO DE DOCUMENTOS ---
        cur.execute("""
            CREATE TABLE tipos_documentos (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(100) NOT NULL,
                categoria VARCHAR(50) NOT NULL,
                exige_competencia BOOLEAN DEFAULT FALSE,
                exige_validade BOOLEAN DEFAULT FALSE,
                exige_valor BOOLEAN DEFAULT FALSE,
                multiplos_arquivos BOOLEAN DEFAULT FALSE,
                descricao_ajuda TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE documentos (
                id SERIAL PRIMARY KEY,
                uvr VARCHAR(10) NOT NULL,
                id_tipo INTEGER REFERENCES tipos_documentos(id),
                caminho_arquivo VARCHAR(255),
                nome_original VARCHAR(255),
                competencia DATE,
                data_validade DATE,
                valor DECIMAL(12, 2),
                numero_referencia VARCHAR(100),
                observacoes TEXT,
                enviado_por VARCHAR(100),
                data_envio TIMESTAMP DEFAULT NOW(),
                status VARCHAR(20) DEFAULT 'Pendente'
            )
        """)

        # --- OUTRAS ---
        cur.execute("""
            CREATE TABLE denuncias (
                id SERIAL PRIMARY KEY,
                numero_denuncia VARCHAR(50) UNIQUE NOT NULL,
                data_registro TIMESTAMP NOT NULL DEFAULT NOW(),
                descricao TEXT NOT NULL,
                status VARCHAR(50) DEFAULT 'Pendente',
                uvr VARCHAR(10),
                associacao VARCHAR(50)
            )
        """)

        cur.execute("""
            CREATE TABLE solicitacoes_alteracao (
                id SERIAL PRIMARY KEY,
                tabela_alvo VARCHAR(50) NOT NULL,
                id_registro INTEGER NOT NULL,
                tipo_solicitacao VARCHAR(20) NOT NULL,
                dados_novos JSONB,
                usuario_solicitante VARCHAR(50) NOT NULL,
                data_solicitacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status VARCHAR(20) DEFAULT 'PENDENTE',
                observacoes_admin TEXT
            )
        """)

        # PASSO 3: CARREGAR DADOS DO CATÁLOGO
        print("3. Importando dados do catálogo...")
        caminho_csv = 'atividades_grupos_subgrupos_itens.csv'
        if os.path.exists(caminho_csv):
            with open(caminho_csv, mode='r', encoding='utf-8') as f:
                reader = csv.DictReader(f, delimiter=';')
                count = 0
                for row in reader:
                    try:
                        # 1. Primeiro garante que o subgrupo existe na tabela de subgrupos
                        cur.execute("""
                            INSERT INTO subgrupos (nome, atividade_pai) 
                            VALUES (%s, %s) 
                            ON CONFLICT (nome, atividade_pai) DO NOTHING 
                            RETURNING id
                        """, (row['Subgrupo'].strip(), row['Atividade'].strip()))
                        
                        # 2. Insere o produto final
                        cur.execute("""
                            INSERT INTO produtos_servicos (tipo, tipo_atividade, grupo, subgrupo, item)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (item) DO NOTHING
                        """, (
                            row['Tipo'].strip(),
                            row['Atividade'].strip(),
                            row['Grupo'].strip(),
                            row['Subgrupo'].strip(),
                            row['item'].strip()
                        ))
                        count += 1
                    except Exception:
                        continue
            print(f"✅ {count} itens inseridos no catálogo.")
        
        # PASSO 4: CRIAR USUÁRIO ADMIN PADRÃO
        print("4. Criando usuário administrador padrão...")
        admin_pass = generate_password_hash("admin123")
        cur.execute("""
            INSERT INTO usuarios (username, password_hash, nome_completo, role, ativo)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (username) DO NOTHING
        """, ("admin", admin_pass, "Administrador Sistema", "admin", True))

        conn.commit()
        print("\n--- RESET CONCLUÍDO COM SUCESSO! ---")
        print("Login: admin / Senha: admin123")

    except Exception as e:
        if conn: conn.rollback()
        print(f"❌ ERRO DURANTE O RESET: {e}")
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    confirmacao = input("⚠️ ATENÇÃO: Isso apagará TODOS os dados do banco. Continuar? (S/N): ")
    if confirmacao.upper() == 'S':
        reset_emergencia()
    else:
        print("Operação abortada.")