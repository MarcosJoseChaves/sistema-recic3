import base64
import re
import json
import io
import csv
import requests
import os
import psycopg2
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv

from flask import Flask, render_template, request, redirect, url_for, jsonify, Response
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash

# ReportLab Imports (Para PDF)
from reportlab.lib.pagesizes import letter, A4, landscape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image as ReportLabImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
from reportlab.lib import colors
from reportlab.lib.units import inch, cm
from reportlab.lib.utils import ImageReader

# --- CONFIGURAÇÕES GLOBAIS ---
# Estes grupos devem ser os mesmos que aparecem nas opções de "Atividade" do HTML
GRUPOS_FIXOS_SISTEMA = [
    "Despesas de operação",
    "Despesas de manutenção",
    "Elétrico ou Eletrônico",
    "Metal",
    "Não convencionais",
    "Outras Receitas",
    "Papel",
    "Plástico",
    "Rateio dos Associados",
    "Repasses Governamentais",
    "Vidro"    
]

# Carrega as variáveis do arquivo .env
load_dotenv()

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024  # Limite aumentado para 64MB
app.secret_key = os.getenv('SECRET_KEY', 'chave_secreta_padrao_dev')

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# --- CONFIGURAÇÃO DO BANCO DE DADOS ---
DATABASE_URL = os.getenv('DATABASE_URL')

def conectar_banco():
    """Estabelece conexão com o banco de dados."""
    if DATABASE_URL:
        # Removido o sslmode='require' para funcionar localmente
        return psycopg2.connect(DATABASE_URL)
    else:
        return psycopg2.connect(
            host="localhost",
            database="recic3",
            user="postgres",
            password="postgres", 
            port="5432"
        )

# --- VALIDAÇÕES ---
def validar_cnpj(cnpj):
    cnpj = re.sub(r'[^0-9]', '', cnpj)
    if len(cnpj) != 14 or cnpj == cnpj[0] * 14: return False
    soma = sum(int(cnpj[i]) * ([5,4,3,2,9,8,7,6,5,4,3,2][i]) for i in range(12))
    digito1 = (11 - (soma % 11)) % 10 if (soma % 11) > 1 else 0
    if int(cnpj[12]) != digito1: return False
    soma = sum(int(cnpj[i]) * ([6,5,4,3,2,9,8,7,6,5,4,3,2][i]) for i in range(13))
    digito2 = (11 - (soma % 11)) % 10 if (soma % 11) > 1 else 0
    return int(cnpj[13]) == digito2

def validar_cep(cep):
    return len(re.sub(r'[^0-9]', '', cep)) == 8

def validar_cpf(cpf):
    cpf = re.sub(r'[^0-9]', '', cpf)
    if len(cpf) != 11 or cpf == cpf[0] * 11: return False
    soma = sum(int(cpf[i]) * (10 - i) for i in range(9))
    digito1 = (11 - (soma % 11)) % 10 if (soma % 11) > 1 else 0
    if int(cpf[9]) != digito1: return False
    soma = sum(int(cpf[i]) * (11 - i) for i in range(10))
    digito2 = (11 - (soma % 11)) % 10 if (soma % 11) > 1 else 0
    return int(cpf[10]) == digito2

# --- CRIAÇÃO DE TABELAS (ATUALIZADO COM TODAS AS MIGRAÇÕES) ---
def criar_tabelas_se_nao_existir():
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # 1. Tabelas Base
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cadastros (
                id SERIAL PRIMARY KEY, uvr VARCHAR(10) NOT NULL, associacao VARCHAR(50),
                data_hora_cadastro TIMESTAMP NOT NULL, razao_social VARCHAR(255) NOT NULL,
                cnpj VARCHAR(14) NOT NULL, cep VARCHAR(8) NOT NULL, logradouro VARCHAR(255), 
                numero VARCHAR(20), bairro VARCHAR(100), cidade VARCHAR(100), uf VARCHAR(2), 
                telefone VARCHAR(20), tipo_atividade VARCHAR(255) NOT NULL, tipo_cadastro VARCHAR(50) NOT NULL,
                CONSTRAINT uq_cadastros_cnpj_tipo_uvr UNIQUE (cnpj, tipo_cadastro, uvr)
            )
        """)

        # Migração Cadastros
        try:
            cur.execute("ALTER TABLE cadastros ADD COLUMN IF NOT EXISTS associacao VARCHAR(50);")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS associados (
                id SERIAL PRIMARY KEY, numero VARCHAR(20) NOT NULL, uvr VARCHAR(10) NOT NULL,
                associacao VARCHAR(50) NOT NULL, nome VARCHAR(255) NOT NULL, cpf VARCHAR(11) UNIQUE NOT NULL, 
                rg VARCHAR(20) NOT NULL, data_nascimento DATE NOT NULL, data_admissao DATE NOT NULL,
                status VARCHAR(20) NOT NULL, cep VARCHAR(8) NOT NULL, logradouro VARCHAR(255), 
                endereco_numero VARCHAR(20), bairro VARCHAR(100), cidade VARCHAR(100), uf VARCHAR(2),
                telefone VARCHAR(20) NOT NULL, data_hora_cadastro TIMESTAMP NOT NULL
            )
        """)
        
        # Migração Associados (Foto)
        try:
            cur.execute("ALTER TABLE associados ADD COLUMN IF NOT EXISTS foto_base64 TEXT;")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS transacoes_financeiras (
                id SERIAL PRIMARY KEY, uvr VARCHAR(10) NOT NULL, associacao VARCHAR(50) NOT NULL,
                id_cadastro_origem INTEGER REFERENCES cadastros(id), nome_cadastro_origem VARCHAR(255) NOT NULL, 
                numero_documento VARCHAR(100), data_documento DATE NOT NULL, tipo_transacao VARCHAR(20) NOT NULL, 
                tipo_atividade VARCHAR(255) NOT NULL, valor_total_documento DECIMAL(12, 2) NOT NULL,
                data_hora_registro TIMESTAMP NOT NULL, valor_pago_recebido DECIMAL(12, 2) DEFAULT 0.00,
                status_pagamento VARCHAR(30) DEFAULT 'Aberto'
            )
        """)
        
        # Migração Transações
        try:
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS valor_pago_recebido DECIMAL(12, 2) DEFAULT 0.00;")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS status_pagamento VARCHAR(30) DEFAULT 'Aberto';")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS itens_transacao (
                id SERIAL PRIMARY KEY, id_transacao INTEGER NOT NULL REFERENCES transacoes_financeiras(id) ON DELETE CASCADE,
                descricao VARCHAR(255) NOT NULL, unidade VARCHAR(50) NOT NULL, quantidade DECIMAL(10, 3) NOT NULL, 
                valor_unitario DECIMAL(12, 2) NOT NULL, valor_total_item DECIMAL(12, 2) NOT NULL
            )
        """)

        # 2. Tabela de Subgrupos
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subgrupos (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(255) NOT NULL,
                atividade_pai VARCHAR(255) NOT NULL,
                UNIQUE(nome, atividade_pai)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS produtos_servicos (
                id SERIAL PRIMARY KEY, tipo VARCHAR(20) NOT NULL, tipo_atividade VARCHAR(255) NOT NULL, 
                grupo VARCHAR(255), subgrupo VARCHAR(255), item VARCHAR(255) NOT NULL UNIQUE, 
                data_hora_cadastro TIMESTAMP NOT NULL,
                id_subgrupo INTEGER REFERENCES subgrupos(id)
            )
        """)
        
        # Migração Produtos (Vincular ID Subgrupo)
        try:
            cur.execute("ALTER TABLE produtos_servicos ADD COLUMN IF NOT EXISTS id_subgrupo INTEGER REFERENCES subgrupos(id);")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS contas_correntes (
                id SERIAL PRIMARY KEY, uvr VARCHAR(10) NOT NULL, associacao VARCHAR(50) NOT NULL,
                banco_codigo VARCHAR(10) NOT NULL, banco_nome VARCHAR(100) NOT NULL, agencia VARCHAR(10) NOT NULL,
                conta_corrente VARCHAR(20) NOT NULL, descricao_conta VARCHAR(255), data_hora_cadastro TIMESTAMP NOT NULL,
                UNIQUE (uvr, banco_codigo, agencia, conta_corrente) 
            )
        """)

        # --- FIX: MIGRAÇÃO CONTAS CORRENTES (O que faltava) ---
        try:
            cur.execute("ALTER TABLE contas_correntes ADD COLUMN IF NOT EXISTS associacao VARCHAR(50);")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()
        # ------------------------------------------------------

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fluxo_caixa (
                id SERIAL PRIMARY KEY, uvr VARCHAR(10) NOT NULL, associacao VARCHAR(50) NOT NULL,
                tipo_movimentacao VARCHAR(20) NOT NULL, id_cadastro_cf INTEGER REFERENCES cadastros(id), 
                nome_cadastro_cf VARCHAR(255), id_conta_corrente INTEGER NOT NULL REFERENCES contas_correntes(id),
                numero_documento_bancario VARCHAR(100), data_efetiva DATE NOT NULL, valor_efetivo DECIMAL(12, 2) NOT NULL,
                saldo_operacao_calculado DECIMAL(12, 2) NOT NULL, data_hora_registro_fluxo TIMESTAMP NOT NULL, 
                observacoes TEXT, categoria VARCHAR(100)
            )
        """)
        
        # Migração Fluxo de Caixa
        try:
            cur.execute("ALTER TABLE fluxo_caixa ADD COLUMN IF NOT EXISTS observacoes TEXT;")
            cur.execute("ALTER TABLE fluxo_caixa ADD COLUMN IF NOT EXISTS categoria VARCHAR(100);")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()

        cur.execute("""
            CREATE TABLE IF NOT EXISTS fluxo_caixa_transacoes_link (
                id_fluxo_caixa INTEGER NOT NULL REFERENCES fluxo_caixa(id) ON DELETE CASCADE,
                id_transacao_financeira INTEGER NOT NULL REFERENCES transacoes_financeiras(id),
                valor_aplicado_nesta_nf DECIMAL(12,2) NOT NULL, PRIMARY KEY (id_fluxo_caixa, id_transacao_financeira)
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS denuncias (
                id SERIAL PRIMARY KEY, numero_denuncia VARCHAR(50) UNIQUE NOT NULL, data_registro TIMESTAMP NOT NULL,
                descricao TEXT NOT NULL, status VARCHAR(50) DEFAULT 'Pendente', uvr VARCHAR(10), associacao VARCHAR(50)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id SERIAL PRIMARY KEY, username VARCHAR(50) UNIQUE NOT NULL, password_hash VARCHAR(255) NOT NULL,
                nome_completo VARCHAR(100), role VARCHAR(20) NOT NULL, uvr_acesso VARCHAR(50), ativo BOOLEAN DEFAULT TRUE
            )
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS solicitacoes_alteracao (
                id SERIAL PRIMARY KEY, tabela_alvo VARCHAR(50) NOT NULL, id_registro INTEGER NOT NULL,
                tipo_solicitacao VARCHAR(20) NOT NULL, dados_novos JSONB, usuario_solicitante VARCHAR(50) NOT NULL,
                data_solicitacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP, status VARCHAR(20) DEFAULT 'PENDENTE', observacoes_admin TEXT
            )
        """)

        conn.commit()
    except psycopg2.Error as e:
        app.logger.error(f"Erro tabelas: {e}")
        if conn: conn.rollback()
    finally:
        if conn: conn.close()

def migrar_dados_antigos_produtos():
    """Migra subgrupos texto para a tabela 'subgrupos' e vincula IDs."""
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # 1. Busca produtos que têm subgrupo (texto) mas não têm id_subgrupo vinculado
        cur.execute("""
            SELECT DISTINCT subgrupo, tipo_atividade 
            FROM produtos_servicos 
            WHERE subgrupo IS NOT NULL AND subgrupo <> '' 
              AND id_subgrupo IS NULL
        """)
        pendentes = cur.fetchall()
        
        migrados = 0
        for nome_sub, atividade_pai in pendentes:
            # Tenta achar o ID desse subgrupo na tabela nova
            cur.execute("SELECT id FROM subgrupos WHERE nome = %s AND atividade_pai = %s", (nome_sub, atividade_pai))
            res = cur.fetchone()
            
            if res:
                novo_id = res[0]
            else:
                # Se não existe, cria
                cur.execute("INSERT INTO subgrupos (nome, atividade_pai) VALUES (%s, %s) RETURNING id", (nome_sub, atividade_pai))
                novo_id = cur.fetchone()[0]
                migrados += 1
            
            # Atualiza a tabela de produtos com o ID correto
            cur.execute("""
                UPDATE produtos_servicos SET id_subgrupo = %s 
                WHERE subgrupo = %s AND tipo_atividade = %s
            """, (novo_id, nome_sub, atividade_pai))
            
        conn.commit()
        if migrados > 0:
            app.logger.info(f"--- MIGRAÇÃO: {migrados} novos subgrupos foram criados e vinculados. ---")
            
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro na migração de produtos: {e}")
    finally:
        if conn: conn.close()

# CHAMADA DA MIGRAÇÃO (Cole isso logo após a definição da função acima)
migrar_dados_antigos_produtos()

class User(UserMixin):
    def __init__(self, id, username, role, uvr_acesso):
        self.id = id
        self.username = username
        self.role = role
        self.uvr_acesso = uvr_acesso

@login_manager.user_loader
def load_user(user_id):
    conn = conectar_banco()
    cur = conn.cursor()
    cur.execute("SELECT id, username, role, uvr_acesso FROM usuarios WHERE id = %s", (user_id,))
    data = cur.fetchone()
    cur.close()
    conn.close()
    if data:
        return User(id=data[0], username=data[1], role=data[2], uvr_acesso=data[3])
    return None


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        
        conn = conectar_banco()
        cur = conn.cursor()
        # Buscamos o usuário pelo nome
        cur.execute("SELECT id, username, password_hash, role, uvr_acesso FROM usuarios WHERE username = %s AND ativo = TRUE", (username,))
        user_data = cur.fetchone()
        cur.close()
        conn.close()

        if user_data:
            # user_data[2] é o hash da senha
            # check_password_hash verifica se a senha digitada bate com o hash
            if check_password_hash(user_data[2], password):
                user_obj = User(id=user_data[0], username=user_data[1], role=user_data[3], uvr_acesso=user_data[4])
                login_user(user_obj)
                app.logger.info(f"Usuário {username} logado com sucesso.")
                return redirect(url_for('index'))
            else:
                return render_template('login.html', erro="Senha incorreta.")
        else:
            return render_template('login.html', erro="Usuário não encontrado.")
            
    return render_template('login.html')

@app.route('/logout')
@login_required
def logout():
    logout_user()
    return redirect(url_for('login'))

# NOVA ROTA PARA ALTERAR SENHA
@app.route('/alterar_senha', methods=['GET', 'POST'])
@login_required
def alterar_senha():
    if request.method == 'POST':
        senha_atual = request.form.get('senha_atual')
        nova_senha = request.form.get('nova_senha')
        confirmar_senha = request.form.get('confirmar_senha')

        # Validações básicas
        if not senha_atual or not nova_senha or not confirmar_senha:
            return render_template('alterar_senha.html', erro="Todos os campos são obrigatórios.")
        
        if nova_senha != confirmar_senha:
            return render_template('alterar_senha.html', erro="A nova senha e a confirmação não conferem.")
        
        if len(nova_senha) < 6:
            return render_template('alterar_senha.html', erro="A nova senha deve ter pelo menos 6 caracteres.")

        conn = conectar_banco()
        cur = conn.cursor()
        try:
            # Busca a senha atual do banco para verificar
            cur.execute("SELECT password_hash FROM usuarios WHERE id = %s", (current_user.id,))
            resultado = cur.fetchone()
            
            if resultado and check_password_hash(resultado[0], senha_atual):
                # Senha atual correta, gera o hash da nova e salva
                novo_hash = generate_password_hash(nova_senha)
                cur.execute("UPDATE usuarios SET password_hash = %s WHERE id = %s", (novo_hash, current_user.id))
                conn.commit()
                app.logger.info(f"Senha alterada com sucesso para o usuário: {current_user.username}")
                return render_template('alterar_senha.html', sucesso="Senha alterada com sucesso!")
            else:
                return render_template('alterar_senha.html', erro="Senha atual incorreta.")
        except Exception as e:
            conn.rollback()
            app.logger.error(f"Erro ao alterar senha: {e}")
            return render_template('alterar_senha.html', erro="Erro interno ao alterar senha.")
        finally:
            cur.close()
            conn.close()

    return render_template('alterar_senha.html')

@app.route("/", methods=["GET"])
@login_required  # <--- ADICIONE ISSO: Protege a rota
def index():
    """Renderiza a página principal com os formulários."""
    # Passamos o 'current_user' para o HTML saber quem está logado
    return render_template("cadastro.html", usuario=current_user)

# Substitua sua função buscar_cep por esta
@app.route("/buscar_cep/<string:cep_numeros>", methods=["GET"])
def buscar_cep(cep_numeros):
    if not cep_numeros or not cep_numeros.isdigit() or len(cep_numeros) != 8:
        return jsonify({"erro": "CEP inválido. Forneça 8 dígitos numéricos."}), 400

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    try:
        # A URL agora aponta para a BrasilAPI
        response = requests.get(
            f"https://brasilapi.com.br/api/cep/v1/{cep_numeros}",
            headers=headers,
            timeout=10 # Um timeout um pouco maior para ser seguro
        )
        # A BrasilAPI retorna 404 para CEP não encontrado, então raise_for_status cuida disso
        response.raise_for_status()

        data = response.json()

        # Ajuste os nomes das chaves para corresponder à resposta da BrasilAPI
        return jsonify({
            "logradouro": data.get("street", ""),
            "bairro": data.get("neighborhood", ""),
            "cidade": data.get("city", ""),
            "uf": data.get("state", "")
            # A BrasilAPI não retorna o código IBGE neste endpoint
        })

    except requests.exceptions.Timeout:
        app.logger.error(f"Timeout ao buscar CEP {cep_numeros} na BrasilAPI.")
        return jsonify({"erro": "O serviço de CEP demorou muito para responder."}), 504

    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            app.logger.warning(f"CEP {cep_numeros} não encontrado na BrasilAPI.")
            return jsonify({"erro": "CEP não encontrado."}), 404
        app.logger.error(f"Erro HTTP ao buscar CEP {cep_numeros} na BrasilAPI: {e}")
        return jsonify({"erro": "Erro de comunicação ao contatar o serviço de CEP."}), 503

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Erro de rede ao buscar CEP {cep_numeros} na BrasilAPI: {e}")
        return jsonify({"erro": "Erro de comunicação ao contatar o serviço de CEP."}), 503

    except Exception as e:
        app.logger.error(f"Erro inesperado ao processar CEP {cep_numeros}: {e}")
        return jsonify({"erro": "Erro interno ao processar CEP."}), 500

@app.route("/cadastrar", methods=["POST"])
@login_required 
def cadastrar():
    conn = None
    try:
        # 1. Transformamos os dados em um dicionário editável
        dados = request.form.to_dict()
        
        # --- CORREÇÃO DO PROBLEMA DA UVR ---
        # Se o usuário tem uma UVR fixa (ex: uvr01), usamos ela.
        # O campo disabled do HTML não envia dados, por isso dava erro.
        if current_user.uvr_acesso:
            dados["uvr"] = current_user.uvr_acesso
        # -----------------------------------

        required_fields = { "razao_social": "Razão Social", "cnpj": "CNPJ", "cep": "CEP",
                            "tipo_atividade": "Tipo de Atividade", "uvr": "UVR",
                            "data_hora_cadastro": "Data/Hora", "tipo_cadastro": "Tipo de Cadastro"}
        
        for field, msg in required_fields.items():
            if not dados.get(field): return f"{msg} é obrigatório(a).", 400

        cnpj_num = re.sub(r'[^0-9]', '', dados["cnpj"])
        if not validar_cnpj(cnpj_num): return "CNPJ inválido.", 400
        
        cep_num = re.sub(r'[^0-9]', '', dados["cep"])
        if not validar_cep(cep_num): return "CEP inválido.", 400

        try:
            data_hora = datetime.strptime(dados["data_hora_cadastro"], '%d/%m/%Y %H:%M:%S')
        except ValueError:
            return "Formato de Data/Hora do Cadastro inválido. Use DD/MM/AAAA HH:MM:SS", 400

        conn = conectar_banco()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO cadastros (uvr, associacao, data_hora_cadastro, razao_social, cnpj, cep,
                                   logradouro, numero, bairro, cidade, uf, telefone,
                                   tipo_atividade, tipo_cadastro)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            dados["uvr"], dados.get("associacao",""), data_hora, dados["razao_social"],
            cnpj_num, cep_num, dados.get("logradouro", ""), dados.get("numero", ""), 
            dados.get("bairro", ""), dados.get("cidade", ""), dados.get("uf", ""), 
            dados.get("telefone", ""), dados["tipo_atividade"], dados["tipo_cadastro"]
        ))
        conn.commit()
        return redirect(url_for("sucesso"))
    except psycopg2.IntegrityError as e:
        if conn: conn.rollback()
        if 'uq_cadastros_cnpj_tipo_uvr' in str(e): 
            return "Este CNPJ já está cadastrado para o Tipo de Cadastro e UVR selecionados.", 400
        return f"Erro de integridade: {e}", 400
    except Exception as e:
        if conn: conn.rollback()
        return f"Erro ao cadastrar: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/cadastrar_associado", methods=["POST"])
@login_required 
def cadastrar_associado():
    conn = None
    try:
        dados = request.form.to_dict()

        # Segurança de UVR
        if current_user.uvr_acesso and current_user.role != 'admin':
            dados["uvr"] = current_user.uvr_acesso

        # Campos obrigatórios (sem 'numero')
        required_fields = { "nome": "Nome", "cpf": "CPF", "rg": "RG",
                            "data_nascimento": "Data de Nascimento", "data_admissao": "Data de Admissão",
                            "status": "Status", "cep": "CEP", "telefone": "Telefone",
                            "uvr": "UVR", "data_hora_cadastro": "Data/Hora"}
        
        for field, msg in required_fields.items():
            if not dados.get(field): return f"{msg} é obrigatório(a).", 400

        # Validações
        cpf_num = re.sub(r'[^0-9]', '', dados["cpf"])
        if not validar_cpf(cpf_num): return "CPF inválido.", 400
        cep_num = re.sub(r'[^0-9]', '', dados["cep"])
        if not validar_cep(cep_num): return "CEP inválido.", 400
        
        try:
            data_nascimento = datetime.strptime(dados["data_nascimento"], '%Y-%m-%d').date()
            data_admissao = datetime.strptime(dados["data_admissao"], '%Y-%m-%d').date()
            data_hora = datetime.strptime(dados["data_hora_cadastro"], '%d/%m/%Y %H:%M:%S')
        except ValueError as e:
            return f"Formato de data inválido: {e}", 400

        # --- LÓGICA DE FOTO INTELIGENTE (CORREÇÃO) ---
        foto_final = ""
        
        # 1. Verifica se veio da WEBCAM (string base64 já pronta)
        foto_webcam = dados.get("foto_webcam_base64", "")
        if foto_webcam and len(foto_webcam) > 100:
            foto_final = foto_webcam
        
        # 2. Se não tem webcam, verifica se veio ARQUIVO (upload)
        elif 'foto_associado' in request.files:
            arquivo = request.files['foto_associado']
            if arquivo and arquivo.filename:
                # Converte o arquivo enviado para Base64 para salvar no banco igual à webcam
                conteudo_arquivo = arquivo.read()
                encoded_string = base64.b64encode(conteudo_arquivo).decode('utf-8')
                mime_type = arquivo.content_type or "image/jpeg"
                foto_final = f"data:{mime_type};base64,{encoded_string}"
        # -----------------------------------------------

        conn = conectar_banco()
        cur = conn.cursor()
        
        # Gera próximo número
        cur.execute("SELECT MAX(CAST(numero AS INTEGER)) FROM associados")
        res_num = cur.fetchone()
        proximo_numero = (res_num[0] + 1) if res_num and res_num[0] else 1
        numero_gerado_str = str(proximo_numero)
        
        cur.execute("""
            INSERT INTO associados (numero, uvr, associacao, nome, cpf, rg, data_nascimento,
                                    data_admissao, status, cep, logradouro, endereco_numero,
                                    bairro, cidade, uf, telefone, data_hora_cadastro, foto_base64)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            numero_gerado_str, dados["uvr"], dados.get("associacao",""), dados["nome"],
            cpf_num, dados["rg"], data_nascimento, data_admissao, dados["status"],
            cep_num, dados.get("logradouro", ""), dados.get("endereco_numero", ""), 
            dados.get("bairro", ""), dados.get("cidade", ""), dados.get("uf", ""), 
            dados["telefone"], data_hora, foto_final
        ))
        
        conn.commit()
        return redirect(url_for("sucesso_associado"))

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro cadastro associado: {e}")
        return f"Erro: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/buscar_associados", methods=["GET"])
@login_required
def buscar_associados():
    # Coleta os parâmetros da URL
    termo = request.args.get("q", "").lower()
    status_filtro = request.args.get("status", "")
    data_ini = request.args.get("data_inicial", "")
    data_fim = request.args.get("data_final", "")
    
    # Novo: Filtro de UVR vindo da tela (apenas Admin usa isso)
    uvr_filtro_tela = request.args.get("uvr", "")

    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # SQL Base
        sql = "SELECT id, nome, cpf, uvr, status, associacao, data_admissao FROM associados WHERE 1=1"
        params = []
        
        # --- LÓGICA DE SEGURANÇA E FILTRO DE UVR ---
        if current_user.role == 'admin':
            # Se for Admin, ele PODE filtrar por UVR se quiser
            if uvr_filtro_tela and uvr_filtro_tela != "Todas":
                sql += " AND uvr = %s"
                params.append(uvr_filtro_tela)
        elif current_user.uvr_acesso:
            # Se NÃO for admin (e tiver UVR definida), FORÇA a UVR dele
            # Ignora totalmente o que veio da tela (uvr_filtro_tela)
            sql += " AND uvr = %s"
            params.append(current_user.uvr_acesso)
        # -------------------------------------------

        # 1. Filtro de Texto (Nome ou CPF)
        if termo:
            sql += " AND (LOWER(nome) LIKE %s OR cpf LIKE %s)"
            params.append(f"%{termo}%")
            params.append(f"%{termo}%")
            
        # 2. Filtro de Status
        if status_filtro and status_filtro != "Todos":
            sql += " AND status = %s"
            params.append(status_filtro)
            
        # 3. Filtro de Data Inicial
        if data_ini:
            sql += " AND data_admissao >= %s"
            params.append(data_ini)
            
        # 4. Filtro de Data Final
        if data_fim:
            sql += " AND data_admissao <= %s"
            params.append(data_fim)

        # Ordenação
        sql += " ORDER BY nome ASC LIMIT 50"
        
        cur.execute(sql, tuple(params))
        resultados = cur.fetchall()
        
        lista_associados = []
        for row in resultados:
            data_adm_str = ""
            if row[6]: data_adm_str = row[6].strftime('%Y-%m-%d')

            lista_associados.append({
                "id": row[0],
                "nome": row[1],
                "cpf": row[2],
                "uvr": row[3],
                "status": row[4],
                "associacao": row[5],
                "data_admissao": data_adm_str
            })
            
        return jsonify(lista_associados)

    except Exception as e:
        app.logger.error(f"Erro na busca: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()
        
@app.route("/get_associado/<int:id>", methods=["GET"])
@login_required
def get_associado(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # Busca todos os dados do associado pelo ID
        # Nota: Ajuste os nomes das colunas se seu banco estiver diferente
        sql = """
            SELECT id, nome, cpf, rg, data_nascimento, data_admissao, status, 
                   uvr, associacao, logradouro, endereco_numero, bairro, cidade, 
                   uf, cep, telefone, foto_base64
            FROM associados WHERE id = %s
        """
        cur.execute(sql, (id,))
        row = cur.fetchone()
        
        if not row:
            return jsonify({"error": "Associado não encontrado"}), 404

        # Segurança: Se não for admin, verifica se a UVR bate
        if current_user.uvr_acesso and current_user.role != 'admin':
            # row[7] é a coluna UVR
            if row[7] != current_user.uvr_acesso:
                return jsonify({"error": "Acesso não autorizado para esta UVR"}), 403

        # Formatar datas para string (JSON não aceita objeto date direto)
        def format_date(d):
            return d.strftime('%d/%m/%Y') if d else ""

        associado = {
            "id": row[0], "nome": row[1], "cpf": row[2], "rg": row[3],
            "data_nascimento": format_date(row[4]),
            "data_admissao": format_date(row[5]),
            "status": row[6], "uvr": row[7], "associacao": row[8],
            "logradouro": row[9], "numero": row[10], "bairro": row[11],
            "cidade": row[12], "uf": row[13], "cep": row[14],
            "telefone": row[15],
            "foto_base64": row[16] # A foto vem aqui!
        }
        
        return jsonify(associado)

    except Exception as e:
        app.logger.error(f"Erro ao buscar ficha do associado: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()
        
@app.route("/editar_associado", methods=["POST"])
@login_required
def editar_associado():
    conn = None
    try:
        dados = request.form.to_dict()
        id_associado = dados.get("id_associado")
        if not id_associado: return "ID não encontrado.", 400

        # Tratamento básico de dados
        cpf_num = re.sub(r'[^0-9]', '', dados.get("cpf", ""))
        cep_num = re.sub(r'[^0-9]', '', dados.get("cep", ""))
        
        def processar_data(d):
            if not d: return None
            try: return datetime.strptime(d, '%Y-%m-%d').date()
            except: return None

        data_nasc = processar_data(dados.get("data_nascimento"))
        data_adm = processar_data(dados.get("data_admissao"))

        # --- LÓGICA DE FOTO NA EDIÇÃO ---
        foto_final = ""
        
        # 1. Prioridade: Nova foto de Webcam
        foto_webcam = dados.get("foto_webcam_base64", "")
        if foto_webcam and len(foto_webcam) > 100:
            foto_final = foto_webcam
        
        # 2. Prioridade: Novo Arquivo de Upload
        elif 'foto_associado' in request.files:
            arquivo = request.files['foto_associado']
            if arquivo and arquivo.filename:
                conteudo = arquivo.read()
                encoded = base64.b64encode(conteudo).decode('utf-8')
                mime = arquivo.content_type or "image/jpeg"
                foto_final = f"data:{mime};base64,{encoded}"
        
        # 3. Se não enviou nada novo, mantém a existente (que vem num campo hidden)
        if not foto_final:
            foto_final = dados.get("foto_existente_base64", "")
        # --------------------------------

        conn = conectar_banco()
        cur = conn.cursor()

        if current_user.role == 'admin':
            cur.execute("""
                UPDATE associados SET 
                    nome=%s, cpf=%s, rg=%s, data_nascimento=%s, data_admissao=%s,
                    status=%s, uvr=%s, associacao=%s, cep=%s, logradouro=%s,
                    endereco_numero=%s, bairro=%s, cidade=%s, uf=%s, telefone=%s,
                    foto_base64=%s
                WHERE id=%s
            """, (
                dados["nome"], cpf_num, dados["rg"], data_nasc, data_adm,
                dados["status"], dados["uvr"], dados.get("associacao", ""), cep_num,
                dados.get("logradouro", ""), dados.get("endereco_numero", ""),
                dados.get("bairro", ""), dados.get("cidade", ""), dados.get("uf"),
                dados["telefone"], foto_final, int(id_associado)
            ))
            conn.commit()
            msg = "Alterações salvas com sucesso!"
        else:
            # Para usuário comum, salva na solicitação
            import json
            dados_json = dados.copy()
            dados_json['foto_base64'] = foto_final # Garante que a foto vai no JSON
            # Converte datas para string para não quebrar o JSON
            if data_nasc: dados_json['data_nascimento'] = str(data_nasc)
            if data_adm: dados_json['data_admissao'] = str(data_adm)

            cur.execute("""
                INSERT INTO solicitacoes_alteracao 
                (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante)
                VALUES (%s, %s, %s, %s, %s)
            """, ('associados', int(id_associado), 'EDICAO', json.dumps(dados_json), current_user.username))
            conn.commit()
            msg = "Solicitação enviada para aprovação."

        return pagina_sucesso_base("Processado", msg)

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro edição: {e}")
        return f"Erro: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/cadastrar_produto_servico", methods=["POST"])
def cadastrar_produto_servico():
    conn = None
    try:
        dados = request.form
        required_fields = {
            "tipo_produto_servico": "Tipo (Despesa/Receita)",
            "tipo_atividade_produto_servico": "Tipo de Atividade",
            "item_produto_servico": "Item (Descrição Prod./Serv.)"
        }
        for field, message in required_fields.items():
            if not dados.get(field) or dados.get(field).strip() == "":
                return f"{message} é obrigatório(a).", 400
        
        data_hora_str = dados.get("data_hora_cadastro_ps")
        if data_hora_str:
            try:
                data_hora_cadastro = datetime.strptime(data_hora_str, '%d/%m/%Y %H:%M:%S')
            except ValueError:
                 return "Formato de Data/Hora do Cadastro inválido. Use DD/MM/AAAA HH:MM:SS", 400
        else:
            data_hora_cadastro = datetime.now()

        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO produtos_servicos (tipo, tipo_atividade, grupo, subgrupo, item, data_hora_cadastro)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            dados["tipo_produto_servico"],
            dados["tipo_atividade_produto_servico"],
            dados.get("grupo_produto_servico", "").strip(),
            dados.get("subgrupo_produto_servico", "").strip(),
            dados["item_produto_servico"].strip(),
            data_hora_cadastro
        ))
        conn.commit()
        return redirect(url_for("sucesso_produto_servico"))
    except psycopg2.IntegrityError as e:
        if conn: conn.rollback()
        if 'produtos_servicos_item_key' in str(e) or 'violates unique constraint "produtos_servicos_item_key"' in str(e).lower():
             return "Este item (Produto/Serviço) já está cadastrado.", 400
        app.logger.error(f"Erro de integridade em /cadastrar_produto_servico: {e}")
        return f"Erro de integridade no banco de dados: {e}", 400
    except ValueError as e:
        app.logger.error(f"Erro de valor em /cadastrar_produto_servico: {e}")
        return f"Formato de dados inválido: {e}", 400
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro inesperado em /cadastrar_produto_servico: {e}")
        return f"Erro ao cadastrar produto/serviço: {e}", 500
    finally:
        if conn and not conn.closed:
            conn.close()

@app.route("/cadastrar_conta_corrente", methods=["POST"])
def cadastrar_conta_corrente():
    conn = None
    try:
        dados = request.form
        app.logger.info(f"Dados recebidos para conta corrente: {dados}")

        required_fields = {
            "uvr_conta": "UVR", "banco_conta": "Banco",
            "agencia_conta": "Agência", "conta_corrente_conta": "Conta Corrente",
            "data_hora_cadastro_conta": "Data/Hora Cadastro"
        }
        for field, msg in required_fields.items():
            if not dados.get(field):
                app.logger.error(f"Campo obrigatório ausente: {msg}")
                return f"{msg} é obrigatório(a).", 400

        banco_selecionado = dados["banco_conta"]
        try:
            banco_codigo, banco_nome = banco_selecionado.split("|", 1)
        except ValueError:
            app.logger.error(f"Valor inválido para o campo Banco: {banco_selecionado}")
            return "Valor inválido para o campo Banco. Formato esperado: 'codigo|nome'.", 400

        agencia = re.sub(r'[^0-9]', '', dados["agencia_conta"])
        conta_corrente = dados["conta_corrente_conta"] 

        if not agencia: return "Agência inválida. Deve conter apenas números.", 400
        if not conta_corrente: return "Conta corrente inválida.", 400

        try:
            data_hora = datetime.strptime(dados["data_hora_cadastro_conta"], '%d/%m/%Y %H:%M:%S')
        except ValueError:
            return "Formato de Data/Hora do Cadastro inválido. Use DD/MM/AAAA HH:MM:SS", 400

        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO contas_correntes (uvr, associacao, banco_codigo, banco_nome, agencia, conta_corrente, descricao_conta, data_hora_cadastro)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            dados["uvr_conta"], dados.get("associacao_conta",""),
            banco_codigo.strip(), banco_nome.strip(), agencia, conta_corrente,
            dados.get("descricao_apelido_conta", "").strip(), data_hora
        ))
        conn.commit()
        return redirect(url_for("sucesso_conta_corrente"))
    except psycopg2.IntegrityError as e:
        if conn: conn.rollback()
        if 'contas_correntes_uvr_banco_codigo_agencia_conta_corrente_key' in str(e) or \
           'contas_correntes_banco_codigo_agencia_conta_corrente_key' in str(e): 
            app.logger.error(f"Tentativa de cadastrar conta duplicada: {e}")
            return "Esta conta corrente (UVR, Banco, Agência, Conta) já está cadastrada.", 400
        app.logger.error(f"Erro de integridade em /cadastrar_conta_corrente: {e}")
        return f"Erro de integridade no banco de dados: {e}", 400
    except ValueError as e: 
        app.logger.error(f"Erro de valor em /cadastrar_conta_corrente: {e}")
        return f"Formato de dados inválido: {e}", 400
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro inesperado em /cadastrar_conta_corrente: {e}")
        return f"Erro ao cadastrar conta corrente: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/get_produtos_servicos", methods=["GET"])
def get_produtos_servicos():
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("SELECT id, item, tipo, tipo_atividade, grupo, subgrupo FROM produtos_servicos ORDER BY item")
        produtos_servicos = [
            {"id": row[0], "item": row[1], "tipo": row[2], "tipo_atividade": row[3], "grupo": row[4], "subgrupo": row[5]} 
            for row in cur.fetchall()
        ]
        return jsonify(produtos_servicos)
    except Exception as e:
        app.logger.error(f"Erro em /get_produtos_servicos: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn and not conn.closed:
            conn.close()

@app.route("/get_cadastros_ativos", methods=["GET"])
def get_cadastros_ativos():
    conn = None
    try:
        uvr_filter = request.args.get("uvr")
        tipo_cadastro_filter = request.args.get("tipo_cadastro_filtro") 

        conn = conectar_banco()
        cur = conn.cursor()
        
        query = "SELECT id, razao_social, tipo_cadastro FROM cadastros"
        conditions = []
        params = []

        if uvr_filter:
            conditions.append("uvr = %s")
            params.append(uvr_filter)
        
        if tipo_cadastro_filter:
            conditions.append("tipo_cadastro = %s")
            params.append(tipo_cadastro_filter)

        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        query += " ORDER BY razao_social"
        
        cur.execute(query, tuple(params))
        cadastros = [{"id": row[0], "razao_social": row[1], "tipo_cadastro": row[2]} for row in cur.fetchall()]
        return jsonify(cadastros)
    except Exception as e:
        app.logger.error(f"Erro em /get_cadastros_ativos: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/get_resumo_fluxo_caixa")
def get_resumo_fluxo_caixa():
    uvr = request.args.get("uvr")
    data_inicial = request.args.get("data_inicial")
    data_final = request.args.get("data_final")

    if not uvr or not data_inicial or not data_final:
        return jsonify({"erro": "UVR e intervalo de datas são obrigatórios"}), 400

    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        # Adicionada cláusula AND data_documento BETWEEN %s AND %s
        cur.execute("""
            SELECT tipo_transacao, COALESCE(SUM(valor_total_documento - valor_pago_recebido), 0)
            FROM transacoes_financeiras
            WHERE uvr = %s 
              AND status_pagamento <> 'Liquidado'
              AND data_documento >= %s AND data_documento <= %s
            GROUP BY tipo_transacao
        """, (uvr, data_inicial, data_final))
        
        summary = cur.fetchall()
        cur.close()
        conn.close()

        receitas_a_receber = Decimal('0.00')
        despesas_a_pagar = Decimal('0.00')

        for tipo, valor in summary:
            if tipo == 'Receita':
                receitas_a_receber = valor
            elif tipo == 'Despesa':
                despesas_a_pagar = valor
        
        saldo_projetado = receitas_a_receber - despesas_a_pagar

        return jsonify({
            "receitas_a_receber": float(receitas_a_receber),
            "despesas_a_pagar": float(despesas_a_pagar),
            "saldo_projetado": float(saldo_projetado)
        })

    except Exception as e:
        app.logger.error(f"Erro em /get_resumo_fluxo_caixa: {e}")
        if conn: conn.close()
        return jsonify({"error": str(e)}), 500

@app.route("/get_contas_correntes") 
def get_contas_correntes_fluxo_caixa():
    uvr = request.args.get("uvr")
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        query_params = []
        base_query = """
            SELECT id, banco_nome, agencia, conta_corrente, descricao_conta, uvr
            FROM contas_correntes
        """ 
        if uvr:
            base_query += " WHERE uvr = %s"
            query_params.append(uvr)
        
        base_query += " ORDER BY descricao_conta, banco_nome"

        cur.execute(base_query, tuple(query_params))
        contas = [{"id": row[0], 
                   "banco_nome": row[1], 
                   "agencia": row[2], 
                   "conta_corrente": row[3],
                   "display_name": f"{row[4] or row[1]} (Ag: {row[2]} C/C: {row[3]}) - {row[5]}", 
                   "uvr": row[5] 
                  } for row in cur.fetchall()]
        return jsonify(contas)
    except Exception as e:
        app.logger.error(f"Erro em /get_contas_correntes (fluxo de caixa/extrato): {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/get_associados_ativos", methods=["GET"])
def get_associados_ativos():
    conn = None
    try:
        uvr_filter = request.args.get("uvr")
        if not uvr_filter:
            return jsonify({"error": "Parâmetro UVR é obrigatório"}), 400

        conn = conectar_banco()
        cur = conn.cursor()
        
        query = "SELECT id, nome FROM associados WHERE uvr = %s AND status = 'Ativo' ORDER BY nome"
        cur.execute(query, (uvr_filter,))
        
        associados = [{"id": row[0], "nome": row[1]} for row in cur.fetchall()]
        return jsonify(associados)
    except Exception as e:
        app.logger.error(f"Erro em /get_associados_ativos: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/get_distinct_grupos")
@login_required
def get_distinct_grupos():
    """Retorna os Grupos (Atividades) baseados no Tipo (Receita/Despesa)."""
    tipo = request.args.get('tipo')
    
    # Se quiser usar a lista fixa do código (mais rápido e seguro):
    grupos_filtrados = []
    
    # Mapa baseado no seu CSV e configurações
    mapa_grupos = {
        "Receita": [
            "Elétrico ou Eletrônico", "Metal", "Não convencionais", 
            "Outras Receitas", "Papel", "Plástico", 
            "Repasses Governamentais", "Vidro"
        ],
        "Despesa": [
            "Despesas de manutenção", "Despesas de operação", 
            "Rateio dos Associados"    
        ]
    }
    
    if tipo and tipo in mapa_grupos:
        grupos_filtrados = mapa_grupos[tipo]
    else:
        # Se não tiver tipo, retorna tudo junto
        grupos_filtrados = sorted(list(set(mapa_grupos["Receita"] + mapa_grupos["Despesa"])))
        
    return jsonify(grupos_filtrados)

@app.route("/get_distinct_subgrupos")
@login_required
def get_distinct_subgrupos():
    """Retorna os Subgrupos vinculados a um Grupo Pai (tabela 'subgrupos')."""
    grupo = request.args.get('grupo')
    if not grupo:
        return jsonify([])
        
    conn = conectar_banco()
    cur = conn.cursor()
    try:
        # Busca na tabela NOVA de subgrupos
        cur.execute("SELECT nome FROM subgrupos WHERE atividade_pai = %s ORDER BY nome", (grupo,))
        res = [r[0] for r in cur.fetchall()]
        return jsonify(res)
    except Exception as e:
        return jsonify([])
    finally:
        conn.close()

@app.route("/get_items_for_filters")
@login_required
def get_items_for_filters():
    """Retorna itens filtrados por Grupo e Subgrupo."""
    grupo = request.args.get('grupo')
    subgrupo = request.args.get('subgrupo')
    
    sql = "SELECT DISTINCT item FROM produtos_servicos WHERE 1=1"
    params = []
    
    if grupo:
        sql += " AND (grupo = %s OR tipo_atividade = %s)"
        params.append(grupo)
        params.append(grupo)
    
    if subgrupo:
        sql += " AND subgrupo = %s"
        params.append(subgrupo)
        
    sql += " ORDER BY item"
    
    conn = conectar_banco()
    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params))
        res = [r[0] for r in cur.fetchall()]
        return jsonify(res)
    except Exception as e:
        return jsonify([])
    finally:
        conn.close()

@app.route("/registrar_transacao_financeira", methods=["POST"])
def registrar_transacao_financeira():
    conn = None
    try:
        dados = request.form
        app.logger.info(f"Dados para registrar transação: {dados}")
        required_fields = { 
            "uvr_transacao": "UVR", "data_documento_transacao": "Data do Documento", 
            "tipo_transacao": "Tipo (Receita/Despesa)",
            "tipo_atividade_transacao": "Tipo de Atividade", 
            "data_hora_cadastro_transacao": "Data/Hora do Cadastro"
        }
        for field, msg in required_fields.items():
            if not dados.get(field):
                return f"{msg} é obrigatório(a).", 400
        
        tipo_atividade = dados.get("tipo_atividade_transacao")
        id_origem_selecionado = dados.get("fornecedor_prestador_transacao")
        nome_origem_input = dados.get("nome_fornecedor_prestador_transacao", "").strip()

        id_final_origem_fk = None
        nome_final_origem = ""

        if tipo_atividade == "Rateio dos Associados":
            if not id_origem_selecionado: 
                nome_final_origem = "Rateio Geral Associados"
                if nome_origem_input: 
                    nome_final_origem = nome_origem_input
            else: 
                if not nome_origem_input:
                     return "Erro: ID de associado selecionado para rateio sem nome correspondente.", 400
                nome_final_origem = nome_origem_input
        else: 
            if not id_origem_selecionado:
                label_campo = dados.get('labelFornecedorPrestadorCliente', 'Fornecedor / Prestador / Cliente')
                return f"O campo '{label_campo}' é obrigatório.", 400
            if not nome_origem_input:
                label_campo = dados.get('labelFornecedorPrestadorCliente', 'Fornecedor / Prestador / Cliente')
                return f"Nome do '{label_campo}' não encontrado.", 400
            try:
                id_final_origem_fk = int(id_origem_selecionado)
                nome_final_origem = nome_origem_input
            except ValueError:
                return "ID do Fornecedor/Prestador/Cliente inválido.", 400
        
        if not nome_final_origem:
             return "Nome do Fornecedor/Prestador/Cliente/Associado é obrigatório.", 400

        descricoes_list = request.form.getlist("produto_servico_descricao[]")
        unidades_list = request.form.getlist("produto_servico_unidade[]")
        quantidades_str_list = request.form.getlist("produto_servico_quantidade[]")
        valores_unitarios_str_list = request.form.getlist("produto_servico_valor_unitario[]")

        if not (len(descricoes_list) == len(unidades_list) == len(quantidades_str_list) == len(valores_unitarios_str_list)):
            return "Dados de itens inconsistentes.", 400
        if not descricoes_list or not descricoes_list[0].strip(): 
            return "É necessário adicionar pelo menos um produto/serviço com descrição.", 400

        itens_para_db = []
        valor_total_documento_calculado = Decimal('0.00')

        for i in range(len(descricoes_list)):
            descricao_item = descricoes_list[i].strip()
            if not descricao_item: return f"Descrição do item {i+1} não pode ser vazia.", 400
            
            try:
                qtd_str = quantidades_str_list[i].replace(",", ".")
                qtd_decimal = Decimal(qtd_str)
                if qtd_decimal <= Decimal('0'): return f"Quantidade do item {i+1} deve ser maior que zero.", 400
            except InvalidOperation: return f"Quantidade inválida para o item {i+1}.", 400

            try:
                vu_str = valores_unitarios_str_list[i].replace("R$", "").replace(".", "").replace(",", ".").strip()
                vu_decimal = Decimal(vu_str)
                if vu_decimal < Decimal('0'): return f"Valor unitário do item {i+1} não pode ser negativo.", 400
            except InvalidOperation: return f"Valor unitário inválido para o item {i+1}.", 400
            
            total_item_decimal = qtd_decimal * vu_decimal
            itens_para_db.append({
                "descricao": descricao_item, "unidade": unidades_list[i],
                "quantidade": qtd_decimal, "valor_unitario": vu_decimal,
                "valor_total_item": total_item_decimal
            })
            valor_total_documento_calculado += total_item_decimal
        
        try:
            data_documento = datetime.strptime(dados["data_documento_transacao"], '%Y-%m-%d').date()
            data_hora_registro = datetime.strptime(dados["data_hora_cadastro_transacao"], '%d/%m/%Y %H:%M:%S')
        except ValueError as e:
            return f"Formato de data inválido: {e}", 400

        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO transacoes_financeiras
            (uvr, associacao, id_cadastro_origem, nome_cadastro_origem, numero_documento, data_documento,
             tipo_transacao, tipo_atividade, valor_total_documento, data_hora_registro, 
             status_pagamento)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
        """, (
            dados["uvr_transacao"], dados.get("associacao_transacao",""),
            id_final_origem_fk, nome_final_origem,
            dados.get("numero_documento_transacao", ""), data_documento,
            dados["tipo_transacao"], dados["tipo_atividade_transacao"],
            valor_total_documento_calculado, data_hora_registro, 'Aberto' 
        ))
        id_transacao_criada = cur.fetchone()[0]

        for item_data in itens_para_db:
            cur.execute("""
                INSERT INTO itens_transacao
                (id_transacao, descricao, unidade, quantidade, valor_unitario, valor_total_item)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (
                id_transacao_criada, item_data['descricao'], item_data['unidade'],
                item_data['quantidade'], item_data['valor_unitario'], item_data['valor_total_item']
            ))
        conn.commit()
        return redirect(url_for("sucesso_transacao"))
    except psycopg2.Error as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro de DB em /registrar_transacao_financeira: {e} - {getattr(e, 'diag', '')}")
        return f"Erro no banco de dados: {e}", 500
    except ValueError as e: 
        if conn: conn.rollback()
        app.logger.error(f"Erro de valor em /registrar_transacao_financeira: {e}")
        return f"Erro de formato de dados: {e}", 400
    except InvalidOperation as e: 
        if conn: conn.rollback()
        app.logger.error(f"Erro de operação Decimal em /registrar_transacao_financeira: {e}")
        return f"Erro de formato numérico: {e}", 400
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro inesperado em /registrar_transacao_financeira: {e}", exc_info=True)
        return f"Erro ao registrar transação: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/editar_transacao", methods=["POST"])
@login_required
def editar_transacao():
    conn = None
    try:
        dados = request.form
        id_transacao = dados.get("id_transacao")
        if not id_transacao: return "ID da transação não encontrado.", 400

        # 1. Parse dos Itens (Mesma lógica do cadastro)
        descricoes = request.form.getlist("produto_servico_descricao[]")
        unidades = request.form.getlist("produto_servico_unidade[]")
        quantidades = request.form.getlist("produto_servico_quantidade[]")
        valores = request.form.getlist("produto_servico_valor_unitario[]")
        
        itens_processados = []
        valor_total_novo = Decimal('0.00')
        
        for i in range(len(descricoes)):
            # Tratamento de valores numéricos (Vírgula para Ponto)
            qtd = Decimal(quantidades[i].replace(",", "."))
            vu = Decimal(valores[i].replace("R$", "").replace(".", "").replace(",", ".").strip())
            total_item = qtd * vu
            
            itens_processados.append({
                "descricao": descricoes[i], "unidade": unidades[i],
                "quantidade": float(qtd), "valor_unitario": float(vu), # float para salvar no JSON
                "valor_total_item": float(total_item)
            })
            valor_total_novo += total_item

        # 2. Dados do Cabeçalho
        cabecalho = {
            "uvr": dados["uvr_transacao"],
            "associacao": dados.get("associacao_transacao",""),
            "data_documento": dados["data_documento_transacao"],
            "tipo_transacao": dados["tipo_transacao"],
            "tipo_atividade": dados["tipo_atividade_transacao"],
            "numero_documento": dados.get("numero_documento_transacao", ""),
            "id_origem": dados.get("fornecedor_prestador_transacao"),
            "nome_origem": dados.get("nome_fornecedor_prestador_transacao"),
            "valor_total": float(valor_total_novo)
        }

        conn = conectar_banco()
        cur = conn.cursor()

        # 3. TRAVA DE SEGURANÇA (Rigidez Contábil)
        # Verifica se já existe qualquer pagamento/recebimento vinculado
        cur.execute("SELECT valor_pago_recebido, status_pagamento FROM transacoes_financeiras WHERE id = %s", (id_transacao,))
        row_pag = cur.fetchone()
        
        valor_ja_pago = row_pag[0] if row_pag and row_pag[0] else 0
        status_atual = row_pag[1] if row_pag else "Aberto"

        if valor_ja_pago > 0:
             return f"BLOQUEADO: Esta transação está '{status_atual}' com R$ {valor_ja_pago:.2f} quitados. Para editar a nota, você deve primeiro excluir os pagamentos/recebimentos no Fluxo de Caixa.", 400

        # 4. Processamento da Edição
        # ADMIN: Edita direto no banco
        if current_user.role == 'admin':
            # Atualiza Cabeçalho
            # Tratamento especial para Rateio (id_origem pode ser Null/Vazio)
            id_origem_sql = None
            if cabecalho['id_origem'] and cabecalho['id_origem'].isdigit():
                id_origem_sql = int(cabecalho['id_origem'])

            cur.execute("""
                UPDATE transacoes_financeiras SET
                    uvr=%s, associacao=%s, data_documento=%s, tipo_transacao=%s,
                    tipo_atividade=%s, numero_documento=%s, 
                    id_cadastro_origem=%s, nome_cadastro_origem=%s,
                    valor_total_documento=%s
                WHERE id=%s
            """, (
                cabecalho['uvr'], cabecalho['associacao'], cabecalho['data_documento'],
                cabecalho['tipo_transacao'], cabecalho['tipo_atividade'], cabecalho['numero_documento'],
                id_origem_sql, cabecalho['nome_origem'],
                valor_total_novo, id_transacao
            ))

            # Atualiza Itens (Estratégia: Apaga todos antigos e recria os novos)
            cur.execute("DELETE FROM itens_transacao WHERE id_transacao = %s", (id_transacao,))
            for item in itens_processados:
                cur.execute("""
                    INSERT INTO itens_transacao (id_transacao, descricao, unidade, quantidade, valor_unitario, valor_total_item)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (id_transacao, item['descricao'], item['unidade'], item['quantidade'], item['valor_unitario'], item['valor_total_item']))
            
            conn.commit()
            return redirect(url_for("sucesso_transacao")) # Reutiliza página de sucesso

        else:
            # USUÁRIO COMUM: Cria solicitação de alteração
            cabecalho['itens'] = itens_processados
            cabecalho['descricao_visual'] = f"Edição NF {cabecalho['numero_documento']} - {cabecalho['nome_origem']}"
            
            cur.execute("""
                INSERT INTO solicitacoes_alteracao 
                (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante) 
                VALUES (%s, %s, %s, %s, %s)
            """, ('transacoes_financeiras', id_transacao, 'EDICAO', json.dumps(cabecalho), current_user.username))
            
            conn.commit()
            return pagina_sucesso_base("Solicitação Enviada", "A edição da transação foi enviada para aprovação.")

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro edição transacao: {e}")
        return f"Erro: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/get_clientes_fornecedores_com_pendencias", methods=["GET"])
def get_clientes_fornecedores_com_pendencias():
    uvr = request.args.get("uvr")
    tipo_movimentacao = request.args.get("tipo_movimentacao")
    app.logger.info(f"FluxoCaixa: Buscando pendências para UVR: {uvr}, Movimentação: {tipo_movimentacao}")

    if not uvr or not tipo_movimentacao:
        return jsonify({"error": "Parâmetros UVR e Tipo de Movimentação são obrigatórios"}), 400

    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        results = []
        
        if tipo_movimentacao == "Recebimento":
            query_clientes = """
                SELECT DISTINCT c.id::TEXT, c.razao_social, c.tipo_cadastro, FALSE as is_associado_rateio
                FROM cadastros c
                JOIN transacoes_financeiras tf ON c.id = tf.id_cadastro_origem
                WHERE tf.uvr = %s AND tf.tipo_transacao = 'Receita' AND c.tipo_cadastro = 'Cliente' AND tf.status_pagamento <> 'Liquidado'
                ORDER BY c.razao_social
            """
            cur.execute(query_clientes, (uvr,))
            for row in cur.fetchall():
                results.append({"id": row[0], "razao_social": row[1], "tipo_cadastro": row[2], "is_associado_rateio": row[3]})
            app.logger.info(f"FluxoCaixa: {len(results)} clientes encontrados para recebimento.")

        elif tipo_movimentacao == "Pagamento":
            query_fornecedores = """
                SELECT DISTINCT c.id::TEXT, c.razao_social, c.tipo_cadastro, FALSE as is_associado_rateio
                FROM cadastros c
                JOIN transacoes_financeiras tf ON c.id = tf.id_cadastro_origem
                WHERE tf.uvr = %s AND tf.tipo_transacao = 'Despesa' AND c.tipo_cadastro = 'Fornecedor/Prestador' AND tf.status_pagamento <> 'Liquidado'
            """
            cur.execute(query_fornecedores, (uvr,))
            fornecedores_count = 0
            for row in cur.fetchall():
                results.append({"id": row[0], "razao_social": row[1], "tipo_cadastro": row[2], "is_associado_rateio": row[3]})
                fornecedores_count +=1
            app.logger.info(f"FluxoCaixa: {fornecedores_count} fornecedores encontrados para pagamento.")

            query_associados_rateio = """
                SELECT DISTINCT tf.nome_cadastro_origem AS id, tf.nome_cadastro_origem AS razao_social,
                       'Associado (Rateio)' AS tipo_cadastro, TRUE as is_associado_rateio
                FROM transacoes_financeiras tf
                WHERE tf.uvr = %s AND tf.tipo_transacao = 'Despesa' AND tf.tipo_atividade = 'Rateio dos Associados'
                  AND tf.id_cadastro_origem IS NULL AND tf.nome_cadastro_origem IS NOT NULL AND tf.nome_cadastro_origem <> '' 
            """
            cur.execute(query_associados_rateio, (uvr,))
            associados_count = 0
            nomes_rateio_adicionados = set()
            for row in cur.fetchall():
                nome_rateio = row[1]
                if nome_rateio not in nomes_rateio_adicionados:
                    results.append({"id": row[0], "razao_social": nome_rateio, "tipo_cadastro": row[2], "is_associado_rateio": row[3]})
                    nomes_rateio_adicionados.add(nome_rateio)
                    associados_count +=1
            results.sort(key=lambda x: x['razao_social'])
            app.logger.info(f"FluxoCaixa: {associados_count} associados de rateio encontrados para pagamento.")
        else:
            return jsonify({"error": "Tipo de Movimentação inválido"}), 400
        
        app.logger.info(f"FluxoCaixa: Total de {len(results)} entidades retornadas para o dropdown.")
        return jsonify(results)
        
    except Exception as e:
        app.logger.error(f"Erro em /get_clientes_fornecedores_com_pendencias: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/get_notas_em_aberto")
def get_notas_em_aberto():
    uvr = request.args.get("uvr")
    id_cf_str = request.args.get("id_cadastro_cf") 
    tipo_movimentacao = request.args.get("tipo_movimentacao") 
    is_associado_rateio_str = request.args.get("is_associado_rateio", "false")
    is_associado_rateio = is_associado_rateio_str.lower() == "true"
    
    # Novos parâmetros de data
    data_inicial = request.args.get("data_inicial")
    data_final = request.args.get("data_final")

    app.logger.info(f"FluxoCaixa: Buscando notas UVR: {uvr}, ID: {id_cf_str}, Datas: {data_inicial} a {data_final}")

    if not all([uvr, id_cf_str, tipo_movimentacao, data_inicial, data_final]):
        return jsonify({"error": "Parâmetros UVR, ID, Movimentação e Datas são obrigatórios"}), 400

    tipo_transacao_filtro = "Receita" if tipo_movimentacao == "Recebimento" else "Despesa"
    
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # Filtro de data adicionado à query base
        params = [uvr, tipo_transacao_filtro, data_inicial, data_final]
        
        query_select_from = """
            SELECT tf.id, tf.numero_documento, tf.data_documento, tf.valor_total_documento, tf.valor_pago_recebido,
                   (tf.valor_total_documento - tf.valor_pago_recebido) as valor_pendente
            FROM transacoes_financeiras tf
        """
        # Adicionado filtro de data na cláusula WHERE
        query_where_base = """
            WHERE tf.uvr = %s 
            AND tf.tipo_transacao = %s 
            AND tf.data_documento >= %s AND tf.data_documento <= %s
            AND tf.status_pagamento <> 'Liquidado'
        """
        
        specific_condition = ""
        if is_associado_rateio:
            specific_condition = "AND tf.nome_cadastro_origem = %s AND tf.tipo_atividade = 'Rateio dos Associados' AND tf.id_cadastro_origem IS NULL"
            params.append(id_cf_str)
        else:
            try:
                id_cf_int = int(id_cf_str)
                specific_condition = "AND tf.id_cadastro_origem = %s"
                params.append(id_cf_int)
            except ValueError:
                return jsonify({"error": "ID do Cadastro inválido."}), 400
        
        query_order_by = "ORDER BY tf.data_documento, tf.numero_documento"
        final_query = f"{query_select_from} {query_where_base} {specific_condition} {query_order_by}"
        
        cur.execute(final_query, tuple(params))
        
        documentos = []
        for row in cur.fetchall():
            documentos.append({
                "id": row[0], "numero_documento": row[1] or "N/D",
                "data_documento": row[2].isoformat(),
                "valor_total_documento": float(row[3]),
                "valor_pago_recebido": float(row[4]),
                "valor_restante": float(row[5]) 
            })
        return jsonify(documentos)
        
    except Exception as e:
        app.logger.error(f"Erro em /get_notas_em_aberto: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/registrar_fluxo_caixa", methods=["POST"])
def registrar_fluxo_caixa():
    conn = None
    try:
        dados = request.json
        app.logger.info(f"Registrando Fluxo de Caixa com dados JSON: {dados}")
        conn = conectar_banco()
        cur = conn.cursor()

        uvr = dados.get("uvr")
        associacao = dados.get("associacao")
        tipo_mov = dados.get("tipo_movimentacao")
        
        id_cadastro_cf_str_from_js = dados.get("id_cadastro_cf_str") 
        is_associado_rateio_from_js = dados.get("is_associado_rateio", False)
        nome_cf_display_from_js = dados.get("nome_cadastro_cf_display")

        id_cadastro_cf_db = None
        nome_cadastro_cf_db = nome_cf_display_from_js 

        if is_associado_rateio_from_js:
            pass 
        else:
            if id_cadastro_cf_str_from_js:
                try:
                    id_cadastro_cf_db = int(id_cadastro_cf_str_from_js)
                except ValueError:
                    app.logger.error(f"FluxoCaixa: ID '{id_cadastro_cf_str_from_js}' não numérico para não-rateio no registro.")
                    return jsonify({"error": "ID do Cliente/Fornecedor inválido para registro."}), 400
            else: 
                 return jsonify({"error": "ID do Cliente/Fornecedor ausente para não-rateio."}), 400
        
        id_conta = int(dados.get("id_conta_corrente"))
        numero_doc_bancario = dados.get("numero_documento_bancario")
        
        try:
            data_efetiva = datetime.strptime(dados.get("data_efetiva"), '%Y-%m-%d').date()
            valor_efetivo = Decimal(str(dados.get("valor_efetivo")).replace(",", "."))
            data_registro = datetime.strptime(dados.get("data_hora_registro_fluxo"), '%d/%m/%Y %H:%M:%S')
        except (ValueError, TypeError, InvalidOperation) as e:
            app.logger.error(f"Erro de conversão de data/valor no fluxo de caixa: {e}")
            return jsonify({"error": f"Formato de data ou valor inválido: {e}"}), 400
        
        total_nfs_selecionadas_valor = Decimal('0.00')
        notas_ids_selecionadas = dados.get("ids_nfs_selecionadas", [])
        
        if not notas_ids_selecionadas:
             return jsonify({"error": "Nenhuma nota fiscal (transação) foi selecionada para este lançamento."}), 400

        for id_nf_str in notas_ids_selecionadas:
            cur.execute("SELECT (valor_total_documento - valor_pago_recebido) as valor_pendente FROM transacoes_financeiras WHERE id = %s", (int(id_nf_str),))
            nf_pendente_row = cur.fetchone()
            if nf_pendente_row and nf_pendente_row[0] is not None:
                 total_nfs_selecionadas_valor += Decimal(nf_pendente_row[0])
        
        saldo_operacao_calculado = total_nfs_selecionadas_valor - valor_efetivo
        observacoes = dados.get("observacoes")

        cur.execute("""
            INSERT INTO fluxo_caixa
            (uvr, associacao, tipo_movimentacao, id_cadastro_cf, nome_cadastro_cf,
             id_conta_corrente, numero_documento_bancario, data_efetiva, valor_efetivo,
             saldo_operacao_calculado, data_hora_registro_fluxo, observacoes)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING id
        """, (
            uvr, associacao, tipo_mov, id_cadastro_cf_db, nome_cadastro_cf_db, 
            id_conta, numero_doc_bancario, data_efetiva, valor_efetivo,
            saldo_operacao_calculado, data_registro, observacoes
        ))
        id_fluxo = cur.fetchone()[0]

        valor_efetivo_restante_para_aplicar = valor_efetivo
        for id_nf_str in notas_ids_selecionadas:
            id_transacao = int(id_nf_str)
            if valor_efetivo_restante_para_aplicar <= Decimal('0'):
                break 

            cur.execute("SELECT valor_pago_recebido, valor_total_documento, (valor_total_documento - valor_pago_recebido) as valor_pendente FROM transacoes_financeiras WHERE id = %s", (id_transacao,))
            nf_data = cur.fetchone()
            if not nf_data: continue

            atual_pago_na_nf, total_doc_da_nf, pendente_na_nf = Decimal(nf_data[0]), Decimal(nf_data[1]), Decimal(nf_data[2])
            valor_a_aplicar_nesta_nf = min(valor_efetivo_restante_para_aplicar, pendente_na_nf)
            
            if valor_a_aplicar_nesta_nf > Decimal('0'):
                cur.execute("""
                    INSERT INTO fluxo_caixa_transacoes_link
                    (id_fluxo_caixa, id_transacao_financeira, valor_aplicado_nesta_nf)
                    VALUES (%s, %s, %s)
                """, (id_fluxo, id_transacao, valor_a_aplicar_nesta_nf))

                novo_valor_pago_total_na_nf = atual_pago_na_nf + valor_a_aplicar_nesta_nf
                
                if novo_valor_pago_total_na_nf.quantize(Decimal('0.01')) >= total_doc_da_nf.quantize(Decimal('0.01')):
                    status_final_nf = 'Liquidado'
                else:
                    status_final_nf = 'Parcialmente Pago/Recebido'
                
                cur.execute("""
                    UPDATE transacoes_financeiras
                    SET valor_pago_recebido = %s, status_pagamento = %s
                    WHERE id = %s
                """, (novo_valor_pago_total_na_nf, status_final_nf, id_transacao))
                
                valor_efetivo_restante_para_aplicar -= valor_a_aplicar_nesta_nf

        conn.commit()
        return jsonify({"status": "sucesso", "message": "Fluxo de caixa registrado e transações atualizadas."})
    except psycopg2.Error as db_err:
        if conn: conn.rollback()
        app.logger.error(f"Erro de banco de dados em /registrar_fluxo_caixa: {db_err}", exc_info=True)
        return jsonify({"error": f"Erro no banco de dados: {db_err}"}), 500
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro em /registrar_fluxo_caixa: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# Função auxiliar para gerar o próximo número de denúncia
def gerar_proximo_numero_denuncia(ano_atual, cur):
    """Gera o próximo número de denúncia sequencial para o ano."""
    prefixo = f"DEN-{ano_atual}-"
    cur.execute("""
        SELECT numero_denuncia FROM denuncias
        WHERE numero_denuncia LIKE %s
        ORDER BY numero_denuncia DESC
        LIMIT 1
    """, (f"{prefixo}%",))
    
    ultimo_numero = cur.fetchone()
    if ultimo_numero:
        try:
            parte_numerica = int(ultimo_numero[0].split('-')[-1])
            proximo_num = parte_numerica + 1
        except ValueError:
            proximo_num = 1
    else:
        proximo_num = 1
    
    return f"{prefixo}{proximo_num:04d}" # Formata com 4 dígitos, ex: 0001

@app.route("/registrar_denuncia", methods=["POST"])
def registrar_denuncia():
    conn = None
    try:
        dados = request.form
        required_fields = {
            "descricao_denuncia": "Descrição da Denúncia",
            "uvr_denuncia": "UVR",
            "data_registro_denuncia": "Data/Hora do Registro"
        }
        for field, msg in required_fields.items():
            if not dados.get(field):
                return f"{msg} é obrigatório(a).", 400

        try:
            data_registro = datetime.strptime(dados["data_registro_denuncia"], '%d/%m/%Y %H:%M:%S')
        except ValueError:
            return "Formato de Data/Hora do Registro inválido. Use DD/MM/AAAA HH:MM:SS", 400

        conn = conectar_banco()
        cur = conn.cursor()

        # Gerar o número da denúncia
        ano_atual = datetime.now().year
        numero_denuncia_gerado = gerar_proximo_numero_denuncia(ano_atual, cur)
        
        cur.execute("""
            INSERT INTO denuncias (numero_denuncia, data_registro, descricao, status, uvr, associacao)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            numero_denuncia_gerado,
            data_registro,
            dados["descricao_denuncia"],
            "Pendente", # Status inicial
            dados["uvr_denuncia"],
            dados.get("associacao_denuncia", "")
        ))
        conn.commit()
        return redirect(url_for("sucesso_denuncia"))
    except psycopg2.IntegrityError as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro de integridade ao registrar denúncia: {e}")
        return "Erro ao registrar denúncia: Número de denúncia já existe. Tente novamente.", 400
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro inesperado ao registrar denúncia: {e}", exc_info=True)
        return f"Erro ao registrar denúncia: {e}", 500
    finally:
        if conn: conn.close()


# --- ROTAS PARA OS FILTROS DE RELATÓRIO FINANCEIRO ---
@app.route("/get_relatorio_uvrs", methods=["GET"])
def get_relatorio_uvrs():
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT uvr FROM transacoes_financeiras")
        uvrs_transacoes = {row[0] for row in cur.fetchall()}
        cur.execute("SELECT DISTINCT uvr FROM contas_correntes")
        uvrs_contas = {row[0] for row in cur.fetchall()}
        cur.execute("SELECT DISTINCT uvr FROM fluxo_caixa")
        uvrs_fluxo = {row[0] for row in cur.fetchall()}
        cur.execute("SELECT DISTINCT uvr FROM denuncias") # Adicionado para denúncias
        uvrs_denuncias = {row[0] for row in cur.fetchall()}
        
        all_uvrs = sorted(list(uvrs_transacoes.union(uvrs_contas).union(uvrs_fluxo).union(uvrs_denuncias)))
        return jsonify(all_uvrs)
    except Exception as e:
        app.logger.error(f"Erro em /get_relatorio_uvrs: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn and not conn.closed: conn.close()

@app.route("/get_relatorio_tipos_atividade_transacao", methods=["GET"])
def get_relatorio_tipos_atividade_transacao():
    tipo_transacao = request.args.get("tipo_transacao") 
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        query = "SELECT DISTINCT tipo_atividade FROM transacoes_financeiras"
        params = []
        if tipo_transacao:
            query += " WHERE tipo_transacao = %s"
            params.append(tipo_transacao)
        query += " ORDER BY tipo_atividade"
        cur.execute(query, tuple(params))
        tipos_atividade = [row[0] for row in cur.fetchall()]
        return jsonify(tipos_atividade)
    except Exception as e:
        app.logger.error(f"Erro em /get_relatorio_tipos_atividade_transacao: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn and not conn.closed: conn.close()

@app.route("/get_relatorio_catalog_options", methods=["GET"])
def get_relatorio_catalog_options():
    option_type = request.args.get("option_type") 
    tipo_transacao = request.args.get("tipo_transacao") 
    tipo_atividade_catalogo = request.args.get("tipo_atividade_catalogo")
    grupo = request.args.get("grupo")
    subgrupo = request.args.get("subgrupo")
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        query_select = ""
        if option_type == "grupo":
            query_select = "SELECT DISTINCT grupo FROM produtos_servicos WHERE grupo IS NOT NULL AND grupo <> ''"
        elif option_type == "subgrupo":
            query_select = "SELECT DISTINCT subgrupo FROM produtos_servicos WHERE subgrupo IS NOT NULL AND subgrupo <> ''"
        elif option_type == "item":
            query_select = "SELECT DISTINCT item FROM produtos_servicos WHERE item IS NOT NULL AND item <> ''"
        else:
            return jsonify({"error": "Tipo de opção inválido"}), 400

        filters = []
        params = []

        if tipo_transacao: 
            filters.append("tipo = %s")
            params.append(tipo_transacao) 
        if tipo_atividade_catalogo: 
            filters.append("tipo_atividade = %s") 
            params.append(tipo_atividade_catalogo)
        if grupo and option_type != "grupo": 
            filters.append("grupo = %s")
            params.append(grupo)
        if subgrupo and option_type == "item": 
            if subgrupo == "(Nenhum)" or subgrupo == "":
                 filters.append("(ps.subgrupo IS NULL OR ps.subgrupo = '')") # Corrigido para ps.subgrupo se ps for o alias da tabela produtos_servicos
            else:
                filters.append("subgrupo = %s")
                params.append(subgrupo)
        
        if filters:
            query_select += " AND " + " AND ".join(filters)
        
        valid_order_by_columns = ["grupo", "subgrupo", "item"]
        if option_type not in valid_order_by_columns:
            return jsonify({"error": "Tipo de ordenação inválido"}), 400
        query_select += f" ORDER BY {option_type}"
        
        cur.execute(query_select, tuple(params))
        options = [row[0] for row in cur.fetchall()]
        return jsonify(options)
    except Exception as e:
        app.logger.error(f"Erro em /get_relatorio_catalog_options ({option_type}): {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn and not conn.closed: conn.close()

@app.route("/get_relatorio_entidades_para_filtro", methods=["GET"])
def get_relatorio_entidades_para_filtro():
    tipo_entidade = request.args.get("tipo_entidade")
    uvr_param = request.args.get("uvr") 
    tipo_transacao_param = request.args.get("tipo_transacao_rel")

    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        entidades = []
        
        query = ""
        params = []

        if tipo_entidade == "Cliente":
            if tipo_transacao_param and tipo_transacao_param == "Despesa":
                return jsonify([]) # Clientes não são diretamente ligados a despesas neste contexto
            
            base_select = "SELECT DISTINCT c.id, c.razao_social AS nome FROM cadastros c JOIN transacoes_financeiras tf ON tf.id_cadastro_origem = c.id"
            conditions = ["c.tipo_cadastro = 'Cliente'"]
            if tipo_transacao_param == "Receita":
                conditions.append("tf.tipo_transacao = 'Receita'")
            # Se tipo_transacao_param for "" (Todos), não adiciona filtro de tipo de transação específico para cliente.
            if uvr_param:
                conditions.append("tf.uvr = %s")
                params.append(uvr_param)
            
            if conditions:
                 query = f"{base_select} WHERE {' AND '.join(conditions)} ORDER BY nome"
            else: # Caso raro, mas para segurança
                 query = f"{base_select} ORDER BY nome"


        elif tipo_entidade == "Fornecedor/Prestador":
            if tipo_transacao_param and tipo_transacao_param == "Receita":
                return jsonify([]) # Fornecedores não são diretamente ligados a receitas
            
            base_select = "SELECT DISTINCT c.id, c.razao_social AS nome FROM cadastros c JOIN transacoes_financeiras tf ON tf.id_cadastro_origem = c.id"
            conditions = ["c.tipo_cadastro = 'Fornecedor/Prestador'"]
            if tipo_transacao_param == "Despesa":
                conditions.append("tf.tipo_transacao = 'Despesa'")
            if uvr_param:
                conditions.append("tf.uvr = %s")
                params.append(uvr_param)

            if conditions:
                query = f"{base_select} WHERE {' AND '.join(conditions)} ORDER BY nome"
            else:
                query = f"{base_select} ORDER BY nome"


        elif tipo_entidade == "Associado":
            # Associados (para rateio) estão ligados a Despesas do tipo "Rateio dos Associados"
            if tipo_transacao_param and tipo_transacao_param == "Receita":
                return jsonify([]) 
            
            # A query deve buscar associados cujo nome aparece em transações de rateio
            base_select = """
                SELECT DISTINCT a.id, a.nome 
                FROM associados a 
                JOIN transacoes_financeiras tf ON tf.nome_cadastro_origem = a.nome
            """
            conditions = [
                "tf.tipo_atividade = 'Rateio dos Associados'",
                "tf.id_cadastro_origem IS NULL", # Característica de rateio para associado
                "a.status = 'Ativo'",
                "tf.tipo_transacao = 'Despesa'" # Rateio é inerentemente uma despesa
            ]
            # O filtro tipo_transacao_param == "Despesa" é redundante aqui se rateio é sempre despesa,
            # mas mantido para consistência se a regra de negócio mudar.
            # Se tipo_transacao_param for "" (Todos), ainda assim só queremos rateios (que são despesas).
            
            if uvr_param:
                conditions.append("tf.uvr = %s")
                params.append(uvr_param)
            
            if conditions:
                query = f"{base_select} WHERE {' AND '.join(conditions)} ORDER BY a.nome"
            # else: query = f"{base_select} ORDER BY a.nome" # Não faz sentido sem as conditions de rateio

        else: 
            return jsonify([])

        if query:
            app.logger.debug(f"Query para entidades de relatório: {query} com params {params}")
            cur.execute(query, tuple(params))
            entidades = [{"id": row[0], "nome": row[1]} for row in cur.fetchall()]
        
        return jsonify(entidades)
    except Exception as e:
        app.logger.error(f"Erro em /get_relatorio_entidades_para_filtro: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()


# --- ROTAS PARA GERAR E BAIXAR RELATÓRIO FINANCEIRO ---
def fetch_report_data(filters):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        # CTE para calcular o valor pago por item e obter a data do último pagamento da NF.
        cte_query = """
        WITH ItemPagamentosDistribuidos AS (
            SELECT
                it.id as item_id,
                tf.id as transacao_id,
                it.valor_total_item,
                tf.valor_pago_recebido as total_pago_na_nf,
                COALESCE(SUM(it.valor_total_item) OVER (
                    PARTITION BY tf.id 
                    ORDER BY it.valor_total_item ASC, it.id ASC 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
                ), 0) as previous_items_cumulative_value
            FROM
                transacoes_financeiras tf
            JOIN
                itens_transacao it ON tf.id = it.id_transacao
        ),
        UltimoPagamentoData AS (
            SELECT
                fctl.id_transacao_financeira,
                MAX(fc.data_efetiva) as data_ultimo_pagamento_nf
            FROM fluxo_caixa_transacoes_link fctl
            JOIN fluxo_caixa fc ON fctl.id_fluxo_caixa = fc.id
            GROUP BY fctl.id_transacao_financeira
        )
        """
        
        base_query = cte_query + """
            SELECT
                tf.uvr, 
                tf.associacao, 
                tf.nome_cadastro_origem, 
                tf.numero_documento,
                tf.data_documento, 
                COALESCE(upd.data_ultimo_pagamento_nf, tf.data_documento) as data_efetiva_pag_rec, 
                tf.tipo_transacao, 
                tf.tipo_atividade AS tipo_atividade_transacao,
                it.descricao AS item_descricao,
                ps.tipo AS item_tipo_catalogo, 
                ps.tipo_atividade AS item_tipo_atividade_catalogo, 
                ps.grupo AS item_grupo_catalogo, 
                ps.subgrupo AS item_subgrupo_catalogo,
                it.unidade, 
                it.quantidade, 
                it.valor_unitario, 
                it.valor_total_item,
                tf.status_pagamento, 
                LEAST(
                    ipd.valor_total_item, 
                    GREATEST(0, ipd.total_pago_na_nf - ipd.previous_items_cumulative_value)
                ) as valor_pago_neste_item,
                tf.data_hora_registro 
            FROM
                transacoes_financeiras tf
            JOIN
                itens_transacao it ON tf.id = it.id_transacao
            JOIN
                ItemPagamentosDistribuidos ipd ON it.id = ipd.item_id
            LEFT JOIN 
                produtos_servicos ps ON TRIM(it.descricao) = TRIM(ps.item)
            LEFT JOIN
                UltimoPagamentoData upd ON tf.id = upd.id_transacao_financeira
        """
        where_clauses = []
        params = []

        if filters.get("data_inicial"):
            where_clauses.append("(tf.data_documento >= %s OR upd.data_ultimo_pagamento_nf >= %s)")
            params.extend([filters["data_inicial"], filters["data_inicial"]])
        if filters.get("data_final"):
            where_clauses.append("(tf.data_documento <= %s OR upd.data_ultimo_pagamento_nf <= %s)")
            params.extend([filters["data_final"], filters["data_final"]])
        
        if filters.get("uvr"):
            where_clauses.append("tf.uvr = %s")
            params.append(filters["uvr"])

        # Novos filtros de Entidade
        tipo_entidade = filters.get("tipo_entidade")
        id_entidade_str = filters.get("id_entidade") # Este é o ID da tabela cadastros ou associados

        if tipo_entidade and id_entidade_str:
            try:
                id_entidade_int = int(id_entidade_str)
                if tipo_entidade == "Cliente":
                    where_clauses.append("tf.id_cadastro_origem = %s AND tf.tipo_transacao = 'Receita'") # Clientes associados a Receitas
                    params.append(id_entidade_int)
                elif tipo_entidade == "Fornecedor/Prestador":
                    where_clauses.append("tf.id_cadastro_origem = %s AND tf.tipo_transacao = 'Despesa'") # Fornecedores a Despesas
                    params.append(id_entidade_int)
                elif tipo_entidade == "Associado":
                    # Para associado, o filtro é pelo nome em nome_cadastro_origem e tipo_atividade de rateio
                    # Buscamos o nome do associado pelo ID fornecido
                    cur_nome_assoc = conn.cursor()
                    cur_nome_assoc.execute("SELECT nome FROM associados WHERE id = %s", (id_entidade_int,))
                    nome_assoc_row = cur_nome_assoc.fetchone()
                    if nome_assoc_row:
                        nome_associado_para_filtro = nome_assoc_row[0]
                        where_clauses.append("tf.nome_cadastro_origem = %s AND tf.tipo_atividade = 'Rateio dos Associados' AND tf.id_cadastro_origem IS NULL AND tf.tipo_transacao = 'Despesa'")
                        params.append(nome_associado_para_filtro)
                    cur_nome_assoc.close()
                    
            except ValueError:
                app.logger.warning(f"ID da entidade inválido: {id_entidade_str} para tipo {tipo_entidade}")


        if filters.get("tipo_transacao_rel"): 
            # Este filtro já é parcialmente coberto pela lógica de tipo_entidade, mas pode refinar mais
            # Por exemplo, se tipo_entidade for vazio, este filtro se aplica diretamente.
            # Se tipo_entidade for Cliente, e tipo_transacao_rel for Despesa, o resultado será vazio (correto).
            if not (tipo_entidade and id_entidade_str): # Só aplica se não houver filtro de entidade específico que já restrinja o tipo_transacao
                where_clauses.append("tf.tipo_transacao = %s")
                params.append(filters["tipo_transacao_rel"])
            elif tipo_entidade == "Cliente" and filters.get("tipo_transacao_rel") == "Receita":
                pass # Já coberto ou compatível
            elif tipo_entidade == "Fornecedor/Prestador" and filters.get("tipo_transacao_rel") == "Despesa":
                pass # Já coberto ou compatível
            elif tipo_entidade == "Associado" and filters.get("tipo_transacao_rel") == "Despesa":
                pass # Já coberto ou compatível
            # else: # Conflito, mas a lógica de entidade já deve ter retornado vazio. Para segurança:
            #    if (tipo_entidade == "Cliente" and filters.get("tipo_transacao_rel") == "Despesa") or \
            #       (tipo_entidade == "Fornecedor/Prestador" and filters.get("tipo_transacao_rel") == "Receita") or \
            #       (tipo_entidade == "Associado" and filters.get("tipo_transacao_rel") == "Receita"):
            #        where_clauses.append("1=0") # Força resultado vazio devido a conflito


        if filters.get("tipo_atividade_transacao_rel"):
            where_clauses.append("tf.tipo_atividade = %s") 
            params.append(filters["tipo_atividade_transacao_rel"])
        if filters.get("grupo_rel"):
            where_clauses.append("ps.grupo = %s")
            params.append(filters["grupo_rel"])
        if filters.get("subgrupo_rel"):
            if filters["subgrupo_rel"] == "(Nenhum)" or filters["subgrupo_rel"] == "":
                 where_clauses.append("(ps.subgrupo IS NULL OR ps.subgrupo = '')")
            else:
                where_clauses.append("ps.subgrupo = %s")
                params.append(filters["subgrupo_rel"])
        if filters.get("item_rel"):
            where_clauses.append("ps.item = %s")
            params.append(filters["item_rel"])
        if filters.get("status_pagamento_rel"): 
            where_clauses.append("tf.status_pagamento = %s")
            params.append(filters["status_pagamento_rel"])


        if where_clauses:
            base_query += " WHERE " + " AND ".join(where_clauses)
        
        base_query += " ORDER BY data_efetiva_pag_rec, tf.data_documento, tf.id, it.valor_total_item, it.id"
        
        app.logger.debug(f"Query do Relatório Financeiro Atualizada: {base_query}")
        app.logger.debug(f"Parâmetros do Relatório Financeiro: {params}")

        cur.execute(base_query, tuple(params))
        columns = [desc[0] for desc in cur.description]
        report_data = [dict(zip(columns, row)) for row in cur.fetchall()]
        
        for row_dict in report_data:
            for key, value in row_dict.items():
                if isinstance(value, Decimal):
                    row_dict[key] = str(value) 
                elif isinstance(value, date): 
                    row_dict[key] = value.strftime('%Y-%m-%d')
                elif isinstance(value, datetime): 
                     row_dict[key] = value.strftime('%Y-%m-%dT%H:%M:%S')
        return report_data
    except Exception as e:
        app.logger.error(f"Erro ao buscar dados do relatório financeiro: {e}", exc_info=True)
        raise 
    finally:
        if conn: conn.close()

@app.route("/gerar_relatorio", methods=["POST"])
def gerar_relatorio():
    try:
        filters = request.json
        app.logger.info(f"Filtros recebidos para relatório financeiro: {filters}")
        data = fetch_report_data(filters)
        return jsonify(data)
    except Exception as e:
        app.logger.error(f"Erro em /gerar_relatorio: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/baixar_csv_relatorio", methods=["POST"])
def baixar_csv_relatorio():
    try:
        filters = request.json
        app.logger.info(f"Filtros recebidos para CSV do relatório financeiro: {filters}")
        data = fetch_report_data(filters)

        if not data:
            return jsonify({"message": "Nenhum dado encontrado para os filtros fornecidos."}), 404

        output = io.StringIO()
        writer = csv.writer(output, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        header = [
            "UVR", "Associação", "Fornecedor/Cliente/Associado (Transação)", "Nº Documento", 
            "Data Documento", "Data Efetiva Pag./Rec.", 
            "Tipo Transação", "Tipo Atividade (Transação)", "Item Descrição (Transação)", 
            "Tipo Item (Catálogo)", "Tipo Atividade Item (Catálogo)", "Grupo Item (Catálogo)", "Subgrupo Item (Catálogo)",
            "Unidade", "Quantidade", "Valor Unitário (R$)", "Valor Total Item (R$)", 
            "Status Pagamento NF", "Valor Pago/Recebido Item (R$)" 
        ]
        writer.writerow(header)
        
        for row_data_dict in data:
            data_doc_str = ""
            if row_data_dict.get("data_documento"):
                try: data_doc_str = datetime.strptime(row_data_dict["data_documento"], '%Y-%m-%d').strftime('%d/%m/%Y')
                except ValueError: data_doc_str = row_data_dict["data_documento"]
            
            data_efetiva_str = ""
            if row_data_dict.get("data_efetiva_pag_rec"):
                try: data_efetiva_str = datetime.strptime(row_data_dict["data_efetiva_pag_rec"], '%Y-%m-%d').strftime('%d/%m/%Y')
                except ValueError: data_efetiva_str = row_data_dict["data_efetiva_pag_rec"]


            csv_row = [
                row_data_dict.get("uvr", ""), row_data_dict.get("associacao", ""),
                row_data_dict.get("nome_cadastro_origem", ""), row_data_dict.get("numero_documento", ""),
                data_doc_str, 
                data_efetiva_str, 
                row_data_dict.get("tipo_transacao", ""),
                row_data_dict.get("tipo_atividade_transacao", ""), row_data_dict.get("item_descricao", ""),
                row_data_dict.get("item_tipo_catalogo", ""), row_data_dict.get("item_tipo_atividade_catalogo", ""),
                row_data_dict.get("item_grupo_catalogo", ""), row_data_dict.get("item_subgrupo_catalogo", ""),
                row_data_dict.get("unidade", ""), str(row_data_dict.get("quantidade", "")).replace('.', ','),
                str(row_data_dict.get("valor_unitario", "")).replace('.', ','), 
                str(row_data_dict.get("valor_total_item", "")).replace('.', ','),
                row_data_dict.get("status_pagamento", ""), 
                str(row_data_dict.get("valor_pago_neste_item", "")).replace('.', ',') 
            ]
            writer.writerow(csv_row)
        
        output.seek(0)
        return Response(
            output, mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": "attachment;filename=relatorio_financeiro.csv"}
        )
    except Exception as e:
        app.logger.error(f"Erro em /baixar_csv_relatorio: {e}", exc_info=True)
        return jsonify({"error": f"Erro ao gerar CSV: {str(e)}"}), 500

# --- FUNÇÕES AUXILIARES PARA PDF ---
def _format_decimal(value_str):
    if value_str is None or value_str == "": return "0,00"
    try:
        dec_val = Decimal(value_str)
        return '{:,.2f}'.format(dec_val).replace(',', 'v').replace('.', ',').replace('v', '.')
    except InvalidOperation:
        return value_str 

def _format_decimal_quantidade(value_str):
    if value_str is None or value_str == "": return "0,000"
    try:
        dec_val = Decimal(value_str)
        return '{:,.3f}'.format(dec_val).replace(',', 'v').replace('.', ',').replace('v', '.')
    except InvalidOperation:
        return value_str

def _create_pdf_header_footer(canvas, doc, title, subtitle=""):
    canvas.saveState()
    styles = getSampleStyleSheet()
    
    header_text = title
    p_header = Paragraph(header_text, styles['h1'])
    w, h = p_header.wrapOn(canvas, doc.width, doc.topMargin)
    p_header.drawOn(canvas, doc.leftMargin, doc.height + doc.topMargin - h + 0.2*inch)

    if subtitle:
        p_subtitle = Paragraph(subtitle, styles['h3'])
        w_sub, h_sub = p_subtitle.wrapOn(canvas, doc.width, doc.topMargin)
        p_subtitle.drawOn(canvas, doc.leftMargin, doc.height + doc.topMargin - h - h_sub + 0.1*inch)

    page_num_text = f"Página {doc.page}"
    canvas.setFont("Helvetica", 9)
    canvas.drawRightString(doc.width + doc.leftMargin - 0.5*inch, 0.5*inch, page_num_text)
    canvas.restoreState()

# --- ROTAS PARA PDF ---
@app.route("/baixar_pdf_relatorio_financeiro", methods=["POST"])
def baixar_pdf_relatorio_financeiro():
    try:
        filters = request.json
        app.logger.info(f"Filtros recebidos para PDF do relatório financeiro: {filters}")
        data = fetch_report_data(filters)

        if not data:
            return jsonify({"message": "Nenhum dado encontrado para os filtros fornecidos."}), 404

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=landscape(A4), topMargin=1.5*inch, bottomMargin=1*inch, leftMargin=0.3*inch, rightMargin=0.3*inch) 
        
        story = []
        styles = getSampleStyleSheet()
        style_normal = styles['Normal']
        style_normal.fontSize = 7 
        style_body = ParagraphStyle('BodyText', parent=style_normal, alignment=TA_LEFT, leading=9)
        style_right = ParagraphStyle('BodyTextRight', parent=style_body, alignment=TA_RIGHT)
        style_center = ParagraphStyle('BodyTextCenter', parent=style_body, alignment=TA_CENTER)
        style_header_table = ParagraphStyle('TableHeader', parent=style_body, fontName='Helvetica-Bold', alignment=TA_CENTER, fontSize=7.5)

        title_pdf = "Relatório Financeiro Detalhado"
        subtitle_parts = []
        if filters.get("uvr"): subtitle_parts.append(f"UVR: {filters['uvr']}")
        
        if filters.get("tipo_entidade") and filters.get("id_entidade"):
            nome_entidade_display = filters.get("nome_entidade_display") 
            if nome_entidade_display: # Se o nome foi passado pelo JS
                 subtitle_parts.append(f"{filters['tipo_entidade']}: {nome_entidade_display}")
            else: # Senão, busca no banco (requer cursor e conexão aqui, ou simplificar)
                # Para simplificar, podemos apenas mostrar o ID se o nome não vier
                subtitle_parts.append(f"{filters['tipo_entidade']} ID: {filters['id_entidade']}")


        data_inicial_str = filters.get("data_inicial")
        data_final_str = filters.get("data_final")
        
        periodo_str = "Período não especificado"
        if data_inicial_str and data_final_str:
            try:
                di = datetime.strptime(data_inicial_str, '%Y-%m-%d').strftime('%d/%m/%Y')
                df = datetime.strptime(data_final_str, '%Y-%m-%d').strftime('%d/%m/%Y')
                periodo_str = f"Período: {di} a {df}"
            except ValueError:
                periodo_str = f"Período (datas inválidas): {data_inicial_str} a {data_final_str}"
        elif data_inicial_str:
            try:
                di = datetime.strptime(data_inicial_str, '%Y-%m-%d').strftime('%d/%m/%Y')
                periodo_str = f"A partir de: {di}"
            except ValueError:
                 periodo_str = f"A partir de (data inválida): {data_inicial_str}"
        elif data_final_str:
            try:
                df = datetime.strptime(data_final_str, '%Y-%m-%d').strftime('%d/%m/%Y')
                periodo_str = f"Até: {df}"
            except ValueError:
                periodo_str = f"Até (data inválida): {data_final_str}"
        subtitle_parts.append(periodo_str)

        subtitle_pdf = " | ".join(subtitle_parts)
        
        header_data = [
            Paragraph("Data Doc.", style_header_table), Paragraph("Data Efet.", style_header_table), Paragraph("UVR", style_header_table), 
            Paragraph("Cliente/Forn.", style_header_table), Paragraph("Nº Doc.", style_header_table), 
            Paragraph("Tipo", style_header_table), Paragraph("Ativ. Trans.", style_header_table),
            Paragraph("Item Descrição", style_header_table), Paragraph("Grupo", style_header_table), 
            Paragraph("Subgrupo", style_header_table), Paragraph("Qtd.", style_header_table), 
            Paragraph("Vl. Unit.", style_header_table), Paragraph("Vl. Total", style_header_table),
            Paragraph("Status NF", style_header_table), Paragraph("Vl. Pago Item", style_header_table)
        ]
        
        table_data = [header_data]
        col_widths = [
            1.3*cm, 1.3*cm, 0.8*cm, 
            2.5*cm, 1.2*cm, 
            0.8*cm, 2.0*cm, 
            3.0*cm, 1.8*cm, 1.8*cm, 
            1.0*cm, 1.5*cm, 1.5*cm, 
            1.5*cm, 1.5*cm  
        ]
        page_width_useful = landscape(A4)[0] - (doc.leftMargin + doc.rightMargin)
        total_col_width = sum(col_widths)
        if total_col_width > page_width_useful:
            app.logger.warning(f"Largura total das colunas ({total_col_width} cm) excede a largura útil da página ({page_width_useful} cm). Ajustando proporcionalmente.")
            ratio = page_width_useful / total_col_width
            col_widths = [w * ratio for w in col_widths]


        for row in data:
            data_doc_val = row.get("data_documento")
            data_doc_fmt = datetime.strptime(data_doc_val, '%Y-%m-%d').strftime('%d/%m/%y') if data_doc_val else ""
            
            data_efet_val = row.get("data_efetiva_pag_rec")
            data_efet_fmt = datetime.strptime(data_efet_val, '%Y-%m-%d').strftime('%d/%m/%y') if data_efet_val else ""

            table_data.append([
                Paragraph(data_doc_fmt, style_center),
                Paragraph(data_efet_fmt, style_center),
                Paragraph(row.get("uvr", ""), style_center),
                Paragraph(row.get("nome_cadastro_origem", "")[:20], style_body), 
                Paragraph(row.get("numero_documento", "")[:10], style_center),
                Paragraph(row.get("tipo_transacao", "")[:3], style_center), 
                Paragraph(row.get("tipo_atividade_transacao", "")[:15], style_body), 
                Paragraph(row.get("item_descricao", "")[:25], style_body), 
                Paragraph(row.get("item_grupo_catalogo", "")[:12], style_body), 
                Paragraph(row.get("item_subgrupo_catalogo", "")[:12], style_body), 
                Paragraph(_format_decimal_quantidade(row.get("quantidade","")), style_right),
                Paragraph(_format_decimal(row.get("valor_unitario","")), style_right),
                Paragraph(_format_decimal(row.get("valor_total_item","")), style_right),
                Paragraph(row.get("status_pagamento","")[:10], style_center), 
                Paragraph(_format_decimal(row.get("valor_pago_neste_item","")), style_right) 
            ])

        report_table = Table(table_data, colWidths=col_widths)
        report_table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#4F81BD")), 
            ('TEXTCOLOR', (0,0), (-1,0), colors.whitesmoke),
            ('ALIGN', (0,0), (-1,-1), 'CENTER'),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,0), 7.5), 
            ('BOTTOMPADDING', (0,0), (-1,0), 5),
            ('GRID', (0,0), (-1,-1), 0.5, colors.black),
            ('LEFTPADDING', (0,0), (-1,-1), 2),
            ('RIGHTPADDING', (0,0), (-1,-1), 2),
            ('ALIGN', (3,1), (3,-1), 'LEFT'), 
            ('ALIGN', (6,1), (6,-1), 'LEFT'), 
            ('ALIGN', (7,1), (7,-1), 'LEFT'), 
            ('ALIGN', (8,1), (8,-1), 'LEFT'), 
            ('ALIGN', (9,1), (9,-1), 'LEFT'), 
            ('ALIGN', (10,1), (10,-1), 'RIGHT'), 
            ('ALIGN', (11,1), (11,-1), 'RIGHT'), 
            ('ALIGN', (12,1), (12,-1), 'RIGHT'), 
            ('ALIGN', (14,1), (14,-1), 'RIGHT'), 
        ]))
        story.append(report_table)
        
        doc.build(story, onFirstPage=lambda c, d: _create_pdf_header_footer(c, d, title_pdf, subtitle_pdf), 
                         onLaterPages=lambda c, d: _create_pdf_header_footer(c, d, title_pdf, subtitle_pdf))
        
        buffer.seek(0)
        filename = f"relatorio_financeiro_{filters.get('uvr','todos')}_{filters.get('data_inicial','inicio')}_a_{filters.get('data_final','fim')}.pdf"
        return Response(
            buffer, mimetype='application/pdf',
            headers={'Content-Disposition': f'attachment;filename={filename}'}
        )
    except Exception as e:
        app.logger.error(f"Erro em /baixar_pdf_relatorio_financeiro: {e}", exc_info=True)
        return jsonify({"error": f"Erro ao gerar PDF: {str(e)}"}), 500


@app.route("/baixar_pdf_extrato", methods=["POST"])
def baixar_pdf_extrato():
    try:
        filters = request.json
        app.logger.info(f"Filtros recebidos para PDF do extrato: {filters}")
        data = fetch_extrato_data(filters)

        if not data or "movimentacoes" not in data:
            return jsonify({"message": "Nenhum dado encontrado para os filtros fornecidos."}), 404

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=1.5*inch, bottomMargin=1*inch, leftMargin=0.75*inch, rightMargin=0.75*inch)
        story = []
        
        styles = getSampleStyleSheet()
        style_normal = styles['Normal']
        style_normal.fontSize = 9
        style_bold = ParagraphStyle('BoldText', parent=style_normal, fontName='Helvetica-Bold')
        style_body = ParagraphStyle('BodyText', parent=style_normal, leading=12)
        style_right = ParagraphStyle('BodyTextRight', parent=style_body, alignment=TA_RIGHT)
        style_header_table = ParagraphStyle('TableHeader', parent=style_body, fontName='Helvetica-Bold', alignment=TA_CENTER)
        
        conta_info = data.get("conta_info", {})
        title_pdf = "Extrato de Conta Corrente"
        subtitle_pdf = f"{conta_info.get('associacao','')} - {conta_info.get('uvr','')} | Conta: {conta_info.get('display_name','N/A')} | Período: {conta_info.get('periodo','N/A')}"

        story.append(Paragraph(f"<b>Saldo Inicial em {datetime.strptime(filters['data_inicial_extrato'], '%Y-%m-%d').strftime('%d/%m/%Y')}: R$ {_format_decimal(data.get('saldo_inicial','0.00'))}</b>", style_body))
        story.append(Spacer(1, 0.2*inch))

        header_mov_pdf = [
            Paragraph("Data", style_header_table), Paragraph("Histórico", style_header_table),
            Paragraph("Entrada (R$)", style_header_table), Paragraph("Saída (R$)", style_header_table),
            Paragraph("Saldo (R$)", style_header_table)
        ]
        
        col_widths_extrato = [1.5*cm, 9*cm, 2.5*cm, 2.5*cm, 2.5*cm] 
        
        table_data_extrato = [header_mov_pdf]
        for mov in data["movimentacoes"]:
            table_data_extrato.append([
                Paragraph(mov.get("data", ""), style_body),
                Paragraph(mov.get("historico", ""), style_body),
                Paragraph(_format_decimal(mov.get("entrada", "")), style_right),
                Paragraph(_format_decimal(mov.get("saida", "")), style_right),
                Paragraph(_format_decimal(mov.get("saldo_parcial", "")), style_right)
            ])
        
        extrato_table = Table(table_data_extrato, colWidths=col_widths_extrato)
        extrato_table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.lightblue), 
            ('TEXTCOLOR', (0,0), (-1,0), colors.black),
            ('ALIGN', (0,0), (-1,-1), 'LEFT'),
            ('ALIGN', (2,0), (-1,-1), 'RIGHT'), 
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE', (0,0), (-1,0), 9),
            ('BOTTOMPADDING', (0,0), (-1,0), 6),
            ('BACKGROUND', (0,1), (-1,-1), colors.whitesmoke),
            ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
            ('LEFTPADDING', (0,0), (-1,-1), 3),
            ('RIGHTPADDING', (0,0), (-1,-1), 3),
        ]))
        story.append(extrato_table)
        story.append(Spacer(1, 0.2*inch))
        story.append(Paragraph(f"<b>Saldo Final em {datetime.strptime(filters['data_final_extrato'], '%Y-%m-%d').strftime('%d/%m/%Y')}: R$ {_format_decimal(data.get('saldo_final','0.00'))}</b>", style_body))

        doc.build(story, onFirstPage=lambda c, d: _create_pdf_header_footer(c, d, title_pdf, subtitle_pdf), 
                         onLaterPages=lambda c, d: _create_pdf_header_footer(c, d, title_pdf, subtitle_pdf))
        
        buffer.seek(0)
        filename = f"extrato_pdf_{conta_info.get('uvr','UVR')}_{conta_info.get('conta','CONTA').replace('/','-')}_{filters.get('data_inicial_extrato')}_a_{filters.get('data_final_extrato')}.pdf"
        return Response(
            buffer, mimetype='application/pdf',
            headers={'Content-Disposition': f'attachment;filename={filename}'}
        )
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        app.logger.error(f"Erro em /baixar_pdf_extrato: {e}", exc_info=True)
        return jsonify({"error": f"Erro ao gerar PDF do extrato: {str(e)}"}), 500


# --- ROTAS E FUNÇÕES PARA EXTRATO BANCÁRIO (Função fetch_extrato_data ATUALIZADA) ---
def fetch_extrato_data(filters): 
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        id_conta_corrente = filters.get("id_conta_corrente_extrato")
        data_inicial_str = filters.get("data_inicial_extrato")
        data_final_str = filters.get("data_final_extrato")

        if not all([id_conta_corrente, data_inicial_str, data_final_str]):
            raise ValueError("Filtros incompletos para extrato.")

        data_inicial = datetime.strptime(data_inicial_str, '%Y-%m-%d').date()
        data_final = datetime.strptime(data_final_str, '%Y-%m-%d').date()

        saldo_inicial = Decimal('0.00')
        cur.execute("""
            SELECT COALESCE(SUM(CASE tipo_movimentacao WHEN 'Recebimento' THEN valor_efetivo ELSE -valor_efetivo END), 0)
            FROM fluxo_caixa
            WHERE id_conta_corrente = %s AND data_efetiva < %s
        """, (id_conta_corrente, data_inicial))
        saldo_inicial_result = cur.fetchone()
        if saldo_inicial_result:
            saldo_inicial = saldo_inicial_result[0]
        
        app.logger.info(f"Extrato - Saldo inicial calculado para conta {id_conta_corrente} antes de {data_inicial}: {saldo_inicial}")

        # --- QUERY ATUALIZADA COM ID ---
        cur.execute("""
            SELECT 
                fc.id, 
                fc.data_efetiva,
                fc.tipo_movimentacao,
                fc.valor_efetivo,
                fc.nome_cadastro_cf,
                fc.numero_documento_bancario,
                fc.observacoes,
                STRING_AGG(tf.numero_documento, ', ') AS nfs_vinculadas
            FROM fluxo_caixa fc
            LEFT JOIN fluxo_caixa_transacoes_link fctl ON fc.id = fctl.id_fluxo_caixa
            LEFT JOIN transacoes_financeiras tf ON fctl.id_transacao_financeira = tf.id
            WHERE fc.id_conta_corrente = %s AND fc.data_efetiva BETWEEN %s AND %s
            GROUP BY fc.id, fc.data_efetiva, fc.tipo_movimentacao, fc.valor_efetivo, fc.nome_cadastro_cf, fc.numero_documento_bancario, fc.observacoes
            ORDER BY fc.data_efetiva, fc.id
        """, (id_conta_corrente, data_inicial, data_final))
        
        movimentacoes = []
        saldo_acumulado_periodo = saldo_inicial
        for row in cur.fetchall():
            id_mov = row[0] # NOVO CAMPO ID
            data_mov = row[1]
            tipo_mov = row[2]
            valor_mov = Decimal(row[3])
            nome_cf = row[4] or ""
            doc_bancario = row[5] or ""
            obs = row[6] or ""
            nfs = row[7] or ""

            entrada = Decimal('0.00')
            saida = Decimal('0.00')

            if tipo_mov == 'Recebimento':
                entrada = valor_mov
                saldo_acumulado_periodo += valor_mov
            else: 
                saida = valor_mov
                saldo_acumulado_periodo -= valor_mov
            
            historico = f"{tipo_mov} de/para {nome_cf}"
            if doc_bancario: historico += f" (Doc: {doc_bancario})"
            if nfs: historico += f" (NFs: {nfs})"
            if obs: historico += f" - Obs: {obs}"

            movimentacoes.append({
                "id": id_mov, # Importante para o CRUD
                "data": data_mov.strftime('%d/%m/%Y'),
                "historico": historico.strip(),
                "entrada": str(entrada), 
                "saida": str(saida),     
                "saldo_parcial": str(saldo_acumulado_periodo),
                "descricao_simples": f"{tipo_mov} - {nome_cf}"
            })
        
        cur.execute("SELECT uvr, associacao, banco_nome, agencia, conta_corrente, descricao_conta FROM contas_correntes WHERE id = %s", (id_conta_corrente,))
        conta_info_row = cur.fetchone()
        conta_info = {}
        if conta_info_row:
            conta_info = {
                "uvr": conta_info_row[0], "associacao": conta_info_row[1],
                "banco": conta_info_row[2], "agencia": conta_info_row[3],
                "conta": conta_info_row[4], "descricao_conta": conta_info_row[5] or ""
            }
            conta_display = f"{conta_info.get('descricao_conta') or conta_info.get('banco','')} Ag: {conta_info.get('agencia','')} C/C: {conta_info.get('conta','')}"
            conta_info["display_name"] = conta_display
            conta_info["periodo"] = f"{data_inicial.strftime('%d/%m/%Y')} a {data_final.strftime('%d/%m/%Y')}"

        return {
            "conta_info": conta_info,
            "saldo_inicial": str(saldo_inicial), 
            "movimentacoes": movimentacoes,
            "saldo_final": str(saldo_acumulado_periodo) 
        }

    except ValueError as ve: 
        app.logger.error(f"Erro de valor ao buscar dados do extrato: {ve}", exc_info=True)
        raise
    except Exception as e:
        app.logger.error(f"Erro ao buscar dados do extrato: {e}", exc_info=True)
        raise 
    finally:
        if conn: conn.close()

@app.route("/gerar_extrato_bancario", methods=["POST"])
def gerar_extrato_bancario_json():
    try:
        filters = request.json
        app.logger.info(f"Filtros recebidos para extrato bancário: {filters}")
        data = fetch_extrato_data(filters) 
        return jsonify(data)
    except ValueError as ve: 
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        app.logger.error(f"Erro em /gerar_extrato_bancario: {e}", exc_info=True)
        return jsonify({"error": str(e)}), 500

@app.route("/baixar_csv_extrato", methods=["POST"])
def baixar_csv_extrato():
    try:
        filters = request.json
        app.logger.info(f"Filtros recebidos para CSV do extrato: {filters}")
        data = fetch_extrato_data(filters) 

        if not data or "movimentacoes" not in data:
             return jsonify({"message": "Nenhum dado encontrado para os filtros fornecidos."}), 404

        output = io.StringIO()
        writer = csv.writer(output, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
        
        writer.writerow([f"Extrato Bancário - {data.get('conta_info',{}).get('display_name','N/A')}"])
        writer.writerow([f"Período: {data.get('conta_info',{}).get('periodo','N/A')}"])
        writer.writerow([]) 
        writer.writerow(["Saldo Inicial:", str(data.get("saldo_inicial","0.00")).replace('.',',')])
        writer.writerow([]) 
        
        header_mov = ["Data", "Histórico", "Entrada (R$)", "Saída (R$)", "Saldo (R$)"]
        writer.writerow(header_mov)
        
        for mov in data["movimentacoes"]:
            csv_row = [
                mov.get("data", ""),
                mov.get("historico", ""),
                str(mov.get("entrada", "")).replace('.', ','),
                str(mov.get("saida", "")).replace('.', ','),
                str(mov.get("saldo_parcial", "")).replace('.', ',')
            ]
            writer.writerow(csv_row)
        
        writer.writerow([]) 
        writer.writerow(["Saldo Final:", str(data.get("saldo_final","0.00")).replace('.',',')])
        
        output.seek(0)
        filename = f"extrato_{data.get('conta_info',{}).get('uvr','UVR')}_{data.get('conta_info',{}).get('conta','CONTA').replace('/','-')}_{filters.get('data_inicial_extrato')}_a_{filters.get('data_final_extrato')}.csv"
        return Response(
            output,
            mimetype="text/csv; charset=utf-8",
            headers={"Content-Disposition": f"attachment;filename={filename}"}
        )
    except ValueError as ve:
        return jsonify({"error": str(ve)}), 400
    except Exception as e:
        app.logger.error(f"Erro em /baixar_csv_extrato: {e}", exc_info=True)
        return jsonify({"error": f"Erro ao gerar CSV do extrato: {str(e)}"}), 500


@app.route("/buscar_cnpj/<string:cnpj>", methods=["GET"])
def buscar_cnpj(cnpj):
    try:
        cnpj_limpo = re.sub(r'[^0-9]', '', cnpj)
        if len(cnpj_limpo) != 14:
            return jsonify({"erro": "CNPJ deve conter 14 dígitos numéricos"}), 400

        try:
            response_brasilapi = requests.get(f"https://brasilapi.com.br/api/cnpj/v1/{cnpj_limpo}", timeout=5)
            response_brasilapi.raise_for_status()
            dados_brasilapi = response_brasilapi.json()
            
            telefone_principal = ""
            if dados_brasilapi.get("ddd_telefone_1"):
                telefone_principal = f"({dados_brasilapi.get('ddd_telefone_1')}) {dados_brasilapi.get('telefone_1')}"
            elif dados_brasilapi.get("ddd_telefone_2"):
                 telefone_principal = f"({dados_brasilapi.get('ddd_telefone_2')}) {dados_brasilapi.get('telefone_2')}"

            resultado = {
                "razao_social": dados_brasilapi.get("razao_social", ""),
                "cep": dados_brasilapi.get("cep", "").replace(".","").replace("-","")[:8],
                "logradouro": dados_brasilapi.get("logradouro", ""),
                "numero": dados_brasilapi.get("numero", ""),
                "bairro": dados_brasilapi.get("bairro", ""),
                "cidade": dados_brasilapi.get("municipio", ""),
                "uf": dados_brasilapi.get("uf", ""),
                "telefone": telefone_principal,
            }
            app.logger.info(f"CNPJ {cnpj_limpo} encontrado via BrasilAPI.")
            return jsonify(resultado)
        
        except requests.exceptions.RequestException as e_brasilapi:
            app.logger.warning(f"Falha ao buscar CNPJ {cnpj_limpo} na BrasilAPI: {e_brasilapi}. Tentando OpenCNPJA...")
            response_opencnpja = requests.get(f"https://open.cnpja.com/office/{cnpj_limpo}", timeout=5)
            response_opencnpja.raise_for_status()
            dados_opencnpja = response_opencnpja.json()

            resultado = {
                "razao_social": dados_opencnpja.get("company", {}).get("name", ""),
                "cep": dados_opencnpja.get("address", {}).get("zip", "")[:8],
                "logradouro": dados_opencnpja.get("address", {}).get("street", ""),
                "numero": dados_opencnpja.get("address", {}).get("number", ""),
                "bairro": dados_opencnpja.get("address", {}).get("district", ""),
                "cidade": dados_opencnpja.get("address", {}).get("city", ""),
                "uf": dados_opencnpja.get("address", {}).get("state", ""),
                "telefone": f"({dados_opencnpja.get('phones', [{}])[0].get('area','')}) {dados_opencnpja.get('phones', [{}])[0].get('number','')}" if dados_opencnpja.get("phones") else "",
            }
            app.logger.info(f"CNPJ {cnpj_limpo} encontrado via OpenCNPJA.")
            return jsonify(resultado)

    except requests.exceptions.HTTPError as e_http:
        status_code = e_http.response.status_code if e_http.response else 500
        if status_code == 404:
            return jsonify({"erro": "CNPJ não encontrado ou inválido."}), 404
        return jsonify({"erro": f"Erro HTTP ao consultar CNPJ: {status_code}"}), 502
    except requests.exceptions.RequestException as e_req:
        app.logger.error(f"Erro de rede ao consultar CNPJ {cnpj_limpo}: {e_req}")
        return jsonify({"erro": "Erro de rede ao consultar CNPJ. Verifique sua conexão."}), 503
    except Exception as e_geral:
        app.logger.error(f"Erro inesperado na busca por CNPJ {cnpj_limpo}: {e_geral}")
        return jsonify({"erro": "Erro interno ao processar CNPJ."}), 500

@app.route("/get_solicitacoes_pendentes", methods=["GET"])
@login_required
def get_solicitacoes_pendentes():
    if current_user.role != 'admin': return jsonify({"error": "Negado"}), 403
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # Busca TUDO que está pendente (Associados e Cadastros)
        sql = """
            SELECT s.id, s.usuario_solicitante, s.data_solicitacao, s.tabela_alvo, s.tipo_solicitacao, s.dados_novos,
                   CASE 
                       WHEN s.tabela_alvo = 'associados' THEN (SELECT nome FROM associados WHERE id = s.id_registro)
                       WHEN s.tabela_alvo = 'cadastros' THEN (SELECT razao_social FROM cadastros WHERE id = s.id_registro)
                   END as nome_atual
            FROM solicitacoes_alteracao s
            WHERE s.status = 'PENDENTE'
            ORDER BY s.data_solicitacao DESC
        """
        cur.execute(sql)
        res = []
        for r in cur.fetchall():
            # --- CORREÇÃO DE TIPO (STR ou DICT) ---
            raw_data = r[5]
            if isinstance(raw_data, str):
                dados = json.loads(raw_data)
            else:
                dados = raw_data
            # --------------------------------------

            # Se for exclusão, o nome novo é irrelevante, usamos o tipo da ação
            nome_novo_ou_acao = "EXCLUSÃO" if r[4] == 'EXCLUSAO' else dados.get('nome') or dados.get('razao_social')
            
            res.append({
                "id": r[0], "solicitante": r[1], "data": r[2].strftime('%d/%m %H:%M'),
                "tabela": r[3], "tipo": r[4], 
                "nome_atual": r[6] or "(Registro não encontrado/Já excluído)", 
                "nome_novo": nome_novo_ou_acao
            })
        return jsonify(res)
    finally:
        if conn: conn.close()

@app.route("/editar_conta_corrente", methods=["POST"])
@login_required
def editar_conta_corrente():
    conn = None
    try:
        dados = request.form.to_dict()
        id_conta = dados.get("id_conta")
        
        if not id_conta: return "ID da conta não informado", 400
        
        # Tratamento do Banco (vem como "codigo|nome" do select)
        banco_selecionado = dados["banco_conta"]
        if "|" in banco_selecionado:
            banco_cod, banco_nom = banco_selecionado.split("|", 1)
        else:
            return "Formato de banco inválido", 400
        
        conn = conectar_banco()
        cur = conn.cursor()
        
        # --- LÓGICA DE PERMISSÃO ---
        if current_user.role == 'admin':
            # ADMIN: Edita direto no banco (Update Real)
            cur.execute("""
                UPDATE contas_correntes SET 
                    uvr=%s, associacao=%s, banco_codigo=%s, banco_nome=%s, 
                    agencia=%s, conta_corrente=%s, descricao_conta=%s
                WHERE id=%s
            """, (
                dados["uvr_conta"], dados.get("associacao_conta",""), 
                banco_cod.strip(), banco_nom.strip(),
                dados["agencia_conta"], dados["conta_corrente_conta"], 
                dados.get("descricao_apelido_conta", ""), int(id_conta)
            ))
            conn.commit()
            msg = "Conta alterada com sucesso!"
        else:
            # OUTROS: Cria solicitação para aprovação
            dados_novos = {
                "uvr": dados["uvr_conta"],
                "associacao": dados.get("associacao_conta",""),
                "banco_codigo": banco_cod.strip(),
                "banco_nome": banco_nom.strip(),
                "agencia": dados["agencia_conta"],
                "conta_corrente": dados["conta_corrente_conta"],
                "descricao_conta": dados.get("descricao_apelido_conta", "") # <--- A LINHA QUE FALTAVA
            }
            
            cur.execute("""
                INSERT INTO solicitacoes_alteracao 
                (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante, status)
                VALUES (%s, %s, %s, %s, %s, 'PENDENTE')
            """, ('contas_correntes', int(id_conta), 'EDICAO', json.dumps(dados_novos), current_user.username))
            
            conn.commit()
            msg = "Solicitação de edição enviada para aprovação do Administrador."
        # ----------------------------------
        
        return pagina_sucesso_base("Processado", msg)
    except Exception as e:
        if conn: conn.rollback()
        return f"Erro ao editar: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/responder_solicitacao", methods=["POST"])
@login_required
def responder_solicitacao():
    if current_user.role != 'admin': return jsonify({"error": "Negado"}), 403
    data = request.json
    id_sol = data.get('id'); acao = data.get('acao')
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("SELECT id_registro, dados_novos, tabela_alvo, tipo_solicitacao FROM solicitacoes_alteracao WHERE id = %s", (id_sol,))
        solic = cur.fetchone()
        
        if not solic: return jsonify({"error": "Não encontrado"}), 404
        id_reg, d_json, tabela, tipo = solic
        
        if acao == 'aprovar':
            if tipo == 'EXCLUSAO':
                # Executa a exclusão real
                sql_del = f"DELETE FROM {tabela} WHERE id = %s"
                cur.execute(sql_del, (id_reg,))
                msg = "Registro excluído com sucesso!"
            else:
                # Executa a edição
                if isinstance(d_json, str): d = json.loads(d_json)
                else: d = d_json

                if tabela == 'associados':
                    novo_uvr = d.get("uvr")
                    nova_assoc = d.get("associacao", "")
                    if not novo_uvr:
                        cur.execute("SELECT uvr, associacao FROM associados WHERE id = %s", (id_reg,))
                        atual = cur.fetchone()
                        if atual:
                            novo_uvr = atual[0]
                            if not nova_assoc: nova_assoc = atual[1]

                    cur.execute("""UPDATE associados SET nome=%s, cpf=%s, rg=%s, data_nascimento=%s, data_admissao=%s, status=%s, uvr=%s, associacao=%s, cep=%s, logradouro=%s, endereco_numero=%s, bairro=%s, cidade=%s, uf=%s, telefone=%s, foto_base64=%s WHERE id=%s""",
                        (d.get("nome"), re.sub(r'[^0-9]', '', d.get("cpf","")), d.get("rg"), d.get("data_nascimento"), d.get("data_admissao"), d.get("status"), 
                         novo_uvr, nova_assoc,
                         re.sub(r'[^0-9]', '', d.get("cep","")), d.get("logradouro"), d.get("endereco_numero"), d.get("bairro"), d.get("cidade"), d.get("uf"), d.get("telefone"), d.get("foto_base64",""), id_reg))

                elif tabela == 'cadastros':
                    novo_uvr = d.get("uvr")
                    nova_assoc = d.get("associacao", "")
                    if not novo_uvr:
                        cur.execute("SELECT uvr, associacao FROM cadastros WHERE id = %s", (id_reg,))
                        atual = cur.fetchone()
                        if atual:
                            novo_uvr = atual[0]
                            if not nova_assoc: nova_assoc = atual[1]

                    cur.execute("""UPDATE cadastros SET uvr=%s, associacao=%s, razao_social=%s, cnpj=%s, cep=%s, logradouro=%s, numero=%s, bairro=%s, cidade=%s, uf=%s, telefone=%s, tipo_atividade=%s, tipo_cadastro=%s WHERE id=%s""",
                        (novo_uvr, nova_assoc,
                         d.get("razao_social"), re.sub(r'[^0-9]', '', d.get("cnpj","")), re.sub(r'[^0-9]', '', d.get("cep","")), d.get("logradouro"), d.get("numero"), d.get("bairro"), d.get("cidade"), d.get("uf"), d.get("telefone"), d.get("tipo_atividade"), d.get("tipo_cadastro"), id_reg))
                
                elif tabela == 'contas_correntes':
                    cur.execute("""UPDATE contas_correntes SET 
                        uvr=%s, associacao=%s, banco_codigo=%s, banco_nome=%s, 
                        agencia=%s, conta_corrente=%s, descricao_conta=%s 
                        WHERE id=%s""",
                        (d.get("uvr"), d.get("associacao",""), d.get("banco_codigo"), d.get("banco_nome"),
                         d.get("agencia"), d.get("conta_corrente"), d.get("descricao_conta"), id_reg))
                
                # --- NOVO BLOCO: TRANSAÇÕES FINANCEIRAS ---
                elif tabela == 'transacoes_financeiras':
                    # 1. Atualiza Cabeçalho
                    id_origem = d.get("id_origem")
                    if id_origem and str(id_origem).isdigit(): id_origem = int(id_origem)
                    else: id_origem = None

                    cur.execute("""
                        UPDATE transacoes_financeiras SET
                            uvr=%s, associacao=%s, data_documento=%s, tipo_transacao=%s,
                            tipo_atividade=%s, numero_documento=%s, 
                            id_cadastro_origem=%s, nome_cadastro_origem=%s,
                            valor_total_documento=%s
                        WHERE id=%s
                    """, (
                        d["uvr"], d.get("associacao",""), d["data_documento"],
                        d["tipo_transacao"], d["tipo_atividade"], d.get("numero_documento",""),
                        id_origem, d.get("nome_origem"), d["valor_total"], id_reg
                    ))

                    # 2. Atualiza Itens (Apaga antigos e insere os do JSON)
                    cur.execute("DELETE FROM itens_transacao WHERE id_transacao = %s", (id_reg,))
                    for item in d["itens"]:
                        cur.execute("""
                            INSERT INTO itens_transacao (id_transacao, descricao, unidade, quantidade, valor_unitario, valor_total_item)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (id_reg, item['descricao'], item['unidade'], item['quantidade'], item['valor_unitario'], item['valor_total_item']))
                # ------------------------------------------

                msg = "Edição aprovada e aplicada!"

            cur.execute("UPDATE solicitacoes_alteracao SET status='APROVADO' WHERE id=%s", (id_sol,))
        else:
            cur.execute("UPDATE solicitacoes_alteracao SET status='REJEITADO' WHERE id=%s", (id_sol,))
            msg = "Solicitação rejeitada."
            
        conn.commit()
        return jsonify({"status": "sucesso", "message": msg})
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao responder solicitação: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/imprimir_ficha_associado/<int:id>", methods=["GET"])
@login_required
def imprimir_ficha_associado(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        sql = """
            SELECT nome, cpf, rg, data_nascimento, data_admissao, status, 
                   uvr, associacao, logradouro, endereco_numero, bairro, cidade, 
                   uf, cep, telefone, foto_base64, numero
            FROM associados WHERE id = %s
        """
        cur.execute(sql, (id,))
        row = cur.fetchone()
        
        if not row: return "Associado não encontrado", 404

        dados = {
            "nome": row[0], "cpf": row[1], "rg": row[2],
            "nasc": row[3].strftime('%d/%m/%Y') if row[3] else "-",
            "admissao": row[4].strftime('%d/%m/%Y') if row[4] else "-",
            "status": row[5], "uvr": row[6], "assoc": row[7],
            "logradouro": row[8] or "", "num": row[9] or "",
            "bairro": row[10] or "", "cidade": row[11] or "",
            "uf": row[12] or "", "cep": row[13] or "",
            "tel": row[14], "foto": row[15], "matricula": row[16]
        }

        # --- CONFIGURAÇÃO DO PDF ---
        buffer = io.BytesIO()
        # Margens ajustadas
        doc = SimpleDocTemplate(buffer, pagesize=A4, 
                                topMargin=1*cm, bottomMargin=1*cm, 
                                leftMargin=1.5*cm, rightMargin=1.5*cm)
        
        story = []
        styles = getSampleStyleSheet()
        
        # Estilos
        style_titulo = ParagraphStyle('FichaTitulo', parent=styles['Heading1'], alignment=TA_CENTER, fontSize=16, spaceAfter=20)
        style_label = ParagraphStyle('FichaLabel', parent=styles['Normal'], fontSize=8, textColor=colors.gray)
        style_valor = ParagraphStyle('FichaValor', parent=styles['Normal'], fontSize=10, leading=12) # removed spaceAfter to compact rows
        
        # Cabeçalho
        story.append(Paragraph(f"Ficha Cadastral do Associado - {dados['assoc']}", style_titulo))
        story.append(Spacer(1, 0.5*cm))

        # --- PROCESSAMENTO DA FOTO (CORRIGIDO PROPORÇÃO) ---
        img_obj = None
        if dados['foto'] and len(dados['foto']) > 100:
            try:
                img_str = dados['foto'].split(",")[1] if "," in dados['foto'] else dados['foto']
                img_data = base64.b64decode(img_str)
                
                # Cria um objeto de arquivo na memória
                imagem_io = io.BytesIO(img_data)
                
                # Lê as dimensões originais da imagem para calcular a proporção
                utils_img = ImageReader(imagem_io)
                iw, ih = utils_img.getSize() 
                aspect = ih / float(iw) # Calcula a proporção (Altura / Largura)
                
                # Define limites máximos (A coluna do PDF tem 3.5cm de largura)
                max_w = 3.5 * cm
                max_h = 5.0 * cm 
                
                # Cálculo inteligente: Ajusta pela largura máxima primeiro
                display_w = max_w
                display_h = max_w * aspect
                
                # Se a altura resultante for muito grande (foto muito vertical), ajusta pela altura máxima
                if display_h > max_h:
                    display_h = max_h
                    display_w = display_h / aspect
                
                # Reinicia o ponteiro do arquivo para o ReportLab ler do início
                imagem_io.seek(0)
                
                # Cria a imagem com as dimensões calculadas
                img_obj = ReportLabImage(imagem_io, width=display_w, height=display_h)
            except Exception as e: 
                app.logger.error(f"Erro imagem PDF: {e}")

        # --- ORGANIZAÇÃO DOS DADOS EM LISTAS ---
        # Formato: (Label, Valor)
        lista_pessoais = [
            ("Nome Completo", dados['nome']), ("Matrícula", dados['matricula']),
            ("CPF", dados['cpf']),            ("RG", dados['rg']),
            ("Data Nascimento", dados['nasc']),("Telefone", dados['tel'])
        ]
        
        lista_sistema = [
            ("UVR", dados['uvr']),            ("Associação", dados['assoc']),
            ("Data Admissão", dados['admissao']), ("Status", dados['status'])
        ]
        
        lista_endereco = [
            ("Endereço", f"{dados['logradouro']}, {dados['num']}"), ("Bairro", dados['bairro']),
            ("Cidade/UF", f"{dados['cidade']} - {dados['uf']}"),    ("CEP", dados['cep'])
        ]

        # --- FUNÇÃO PARA CRIAR TABELAS DE SEÇÃO (2 COLUNAS) ---
        # Define largura da coluna interna baseada na presença da foto
        # Se tem foto (4cm), sobra ~12cm para texto -> 6cm por coluna
        # Se não tem, sobra ~16cm -> 8cm por coluna
        col_w = 6*cm if img_obj else 8*cm

        def criar_tabela_secao(lista_campos):
            rows = []
            # Itera de 2 em 2 para fazer pares
            for i in range(0, len(lista_campos), 2):
                campo1 = lista_campos[i]
                cell1 = [Paragraph(f"<b>{campo1[0]}</b>", style_label), Paragraph(campo1[1], style_valor)]
                
                cell2 = []
                if i + 1 < len(lista_campos):
                    campo2 = lista_campos[i+1]
                    cell2 = [Paragraph(f"<b>{campo2[0]}</b>", style_label), Paragraph(campo2[1], style_valor)]
                
                rows.append([cell1, cell2])
            
            t = Table(rows, colWidths=[col_w, col_w])
            t.setStyle(TableStyle([
                ('VALIGN', (0,0), (-1,-1), 'TOP'),
                ('LEFTPADDING', (0,0), (-1,-1), 0),
                ('RIGHTPADDING', (0,0), (-1,-1), 5),
                ('BOTTOMPADDING', (0,0), (-1,-1), 6), # Espaço entre linhas de dados
            ]))
            return t

        # --- MONTAGEM DA COLUNA DA ESQUERDA (TEXTOS) ---
        elementos_texto = []
        
        elementos_texto.append(Paragraph("<b>DADOS PESSOAIS</b>", styles['Heading4']))
        elementos_texto.append(criar_tabela_secao(lista_pessoais))
        elementos_texto.append(Spacer(1, 0.3*cm))
        
        elementos_texto.append(Paragraph("<b>DADOS DO SISTEMA</b>", styles['Heading4']))
        elementos_texto.append(criar_tabela_secao(lista_sistema))
        elementos_texto.append(Spacer(1, 0.3*cm))
        
        elementos_texto.append(Paragraph("<b>ENDEREÇO</b>", styles['Heading4']))
        elementos_texto.append(criar_tabela_secao(lista_endereco))

        # --- TABELA PRINCIPAL (TEXTO x FOTO) ---
        if img_obj:
            # Coluna 1: Lista de Elementos de Texto | Coluna 2: Foto
            # Ajuste de largura da coluna da foto para acomodar até 3.5cm + bordas
            data_main = [[elementos_texto, img_obj]]
            widths_main = [12.5*cm, 3.5*cm]
            style_main = [
                ('VALIGN', (0,0), (-1,-1), 'TOP'),
                ('ALIGN', (1,0), (1,0), 'CENTER'), # Centraliza foto
                ('BORDER', (1,0), (1,0), 1, colors.black), # Borda na foto
                ('TOPPADDING', (1,0), (1,0), 5), # Padding na foto
                ('BOTTOMPADDING', (1,0), (1,0), 5),
            ]
        else:
            data_main = [[elementos_texto]]
            widths_main = [16.5*cm]
            style_main = [('VALIGN', (0,0), (-1,-1), 'TOP')]

        tbl_main = Table(data_main, colWidths=widths_main)
        tbl_main.setStyle(TableStyle(style_main))
        
        story.append(tbl_main)
        
        # Assinatura
        story.append(Spacer(1, 2.5*cm))
        story.append(Paragraph("_" * 50, ParagraphStyle('Line', parent=styles['Normal'], alignment=TA_CENTER)))
        story.append(Paragraph("Assinatura do Associado", ParagraphStyle('Text', parent=styles['Normal'], alignment=TA_CENTER, fontSize=8)))

        doc.build(story)
        buffer.seek(0)
        return Response(buffer, mimetype='application/pdf', headers={'Content-Disposition': f'inline;filename=ficha_{id}.pdf'})

    except Exception as e:
        app.logger.error(f"Erro PDF: {e}", exc_info=True)
        return f"Erro: {e}", 500
    finally:
        if conn: conn.close()

# --- GESTÃO DE CLIENTES / FORNECEDORES (LEITURA) ---

@app.route("/buscar_cadastros", methods=["GET"])
@login_required
def buscar_cadastros():
    termo = request.args.get("q", "").lower()
    tipo = request.args.get("tipo", "") # Cliente ou Fornecedor
    uvr_tela = request.args.get("uvr", "")
    
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        sql = "SELECT id, razao_social, cnpj, tipo_cadastro, uvr, cidade, telefone FROM cadastros WHERE 1=1"
        params = []
        
        # Filtro de UVR (Segurança)
        if current_user.role == 'admin':
            if uvr_tela and uvr_tela != "Todas": 
                sql += " AND uvr = %s"
                params.append(uvr_tela)
        elif current_user.uvr_acesso: 
            sql += " AND uvr = %s"
            params.append(current_user.uvr_acesso)
        
        # Filtros de Busca
        if termo: 
            sql += " AND (LOWER(razao_social) LIKE %s OR cnpj LIKE %s)"
            params.extend([f"%{termo}%", f"%{termo}%"])
        
        if tipo and tipo != "Todos": 
            sql += " AND tipo_cadastro = %s"
            params.append(tipo)
            
        sql += " ORDER BY razao_social ASC LIMIT 50"
        
        cur.execute(sql, tuple(params))
        
        res = []
        for r in cur.fetchall():
            res.append({
                "id": r[0], "razao": r[1], "cnpj": r[2], 
                "tipo": r[3], "uvr": r[4], "cidade": r[5], "tel": r[6]
            })
        return jsonify(res)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/get_cadastro/<int:id>", methods=["GET"])
@login_required
def get_cadastro(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("SELECT * FROM cadastros WHERE id = %s", (id,))
        row = cur.fetchone()
        
        if not row: return jsonify({"error": "Não encontrado"}), 404
        
        # Segurança UVR
        if current_user.uvr_acesso and current_user.role != 'admin' and row[1] != current_user.uvr_acesso: 
            return jsonify({"error": "Acesso negado"}), 403

        # Mapeamento (Ajuste os índices se sua tabela for diferente)
        data = {
            "id": row[0], "uvr": row[1], "associacao": row[2], 
            "razao_social": row[4], "cnpj": row[5], "cep": row[6],
            "logradouro": row[7], "numero": row[8], "bairro": row[9],
            "cidade": row[10], "uf": row[11], "telefone": row[12],
            "tipo_atividade": row[13], "tipo_cadastro": row[14]
        }
        return jsonify(data)
    finally:
        if conn: conn.close()

# --- IMPRESSÃO DE FICHA DE CADASTRO ---

@app.route("/imprimir_ficha_cadastro/<int:id>", methods=["GET"])
@login_required
def imprimir_ficha_cadastro(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("SELECT * FROM cadastros WHERE id = %s", (id,))
        row = cur.fetchone()
        
        if not row: return "Não encontrado", 404
        
        # Dados organizados
        d = {
            "uvr": row[1], "assoc": row[2], "razao": row[4], "cnpj": row[5],
            "cep": row[6], "log": row[7] or "", "num": row[8] or "", 
            "bairro": row[9] or "", "cid": row[10] or "", "uf": row[11] or "", 
            "tel": row[12] or "", "ativ": row[13], "tipo": row[14]
        }

        # Gera PDF
        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, topMargin=1*cm, bottomMargin=1*cm, leftMargin=1.5*cm, rightMargin=1.5*cm)
        story = []
        styles = getSampleStyleSheet()
        
        story.append(Paragraph(f"Ficha Cadastral - {d['tipo']}", ParagraphStyle('T', parent=styles['Heading1'], alignment=TA_CENTER)))
        story.append(Spacer(1, 0.5*cm))

        # Função auxiliar para tabela de 2 colunas
        def criar_tabela_secao(lista_campos):
            rows = []
            col_w = 8*cm # Largura fixa pois não tem foto
            for i in range(0, len(lista_campos), 2):
                c1 = [[Paragraph(f"<b>{lista_campos[i][0]}</b>", ParagraphStyle('lbl', fontSize=8, textColor=colors.gray))],
                      [Paragraph(str(lista_campos[i][1]), ParagraphStyle('val', fontSize=10))]]
                c2 = []
                if i + 1 < len(lista_campos):
                    c2 = [[Paragraph(f"<b>{lista_campos[i+1][0]}</b>", ParagraphStyle('lbl', fontSize=8, textColor=colors.gray))],
                          [Paragraph(str(lista_campos[i+1][1]), ParagraphStyle('val', fontSize=10))]]
                rows.append([c1, c2])
            
            t = Table(rows, colWidths=[col_w, col_w])
            t.setStyle(TableStyle([('VALIGN',(0,0),(-1,-1),'TOP'), ('BOTTOMPADDING',(0,0),(-1,-1),6)]))
            return t

        story.append(Paragraph("<b>DADOS PRINCIPAIS</b>", styles['Heading4']))
        story.append(criar_tabela_secao([
            ("Razão Social/Nome", d['razao']), ("CNPJ/CPF", d['cnpj']),
            ("Tipo Cadastro", d['tipo']),      ("Atividade", d['ativ']),
            ("Telefone", d['tel']),            ("UVR", d['uvr'])
        ]))
        
        story.append(Spacer(1, 0.5*cm))
        
        story.append(Paragraph("<b>ENDEREÇO</b>", styles['Heading4']))
        story.append(criar_tabela_secao([
            ("Logradouro", f"{d['log']}, {d['num']}"), ("Bairro", d['bairro']),
            ("Cidade/UF", f"{d['cid']} - {d['uf']}"),  ("CEP", d['cep'])
        ]))
        
        doc.build(story)
        buffer.seek(0)
        filename = f"ficha_{d['razao'][:10].replace(' ','_')}.pdf"
        return Response(buffer, mimetype='application/pdf', headers={'Content-Disposition': f'inline;filename={filename}'})
    finally:
        if conn: conn.close()
# --- AÇÕES DE ESCRITA (EDITAR / EXCLUIR) ---

@app.route("/editar_cadastro", methods=["POST"])
@login_required
def editar_cadastro():
    conn = None
    try:
        dados = request.form.to_dict()
        id_cad = dados.get("id_cadastro")
        if not id_cad: return "ID não informado", 400
        
        conn = conectar_banco()
        cur = conn.cursor()
        
        # Admin: Executa direto
        if current_user.role == 'admin':
            cnpj_num = re.sub(r'[^0-9]', '', dados["cnpj"])
            cep_num = re.sub(r'[^0-9]', '', dados.get("cep",""))
            
            cur.execute("""
                UPDATE cadastros SET 
                    uvr=%s, associacao=%s, razao_social=%s, cnpj=%s, cep=%s, 
                    logradouro=%s, numero=%s, bairro=%s, cidade=%s, uf=%s, 
                    telefone=%s, tipo_atividade=%s, tipo_cadastro=%s 
                WHERE id=%s
            """, (
                dados["uvr"], dados.get("associacao",""), dados["razao_social"], cnpj_num, cep_num,
                dados.get("logradouro"), dados.get("numero"), dados.get("bairro"), dados.get("cidade"), 
                dados.get("uf"), dados.get("telefone"), dados["tipo_atividade"], dados["tipo_cadastro"], 
                int(id_cad)
            ))
            conn.commit()
            msg = "Alterações salvas com sucesso!"
        
        # Usuário: Cria solicitação
        else:
            dados_json = json.dumps(dados)
            cur.execute("""
                INSERT INTO solicitacoes_alteracao 
                (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante) 
                VALUES (%s, %s, %s, %s, %s)
            """, ('cadastros', int(id_cad), 'EDICAO', dados_json, current_user.username))
            conn.commit()
            msg = "Solicitação de edição enviada para aprovação."
        
        return pagina_sucesso_base("Processado", msg)
    except Exception as e:
        if conn: conn.rollback()
        return f"Erro: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/excluir_cadastro/<int:id>", methods=["POST"])
@login_required
def excluir_cadastro(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # Busca dados para registrar quem foi excluído (opcional, mas bom para histórico)
        cur.execute("SELECT razao_social FROM cadastros WHERE id = %s", (id,))
        row = cur.fetchone()
        nome_registro = row[0] if row else "Desconhecido"

        if current_user.role == 'admin':
            cur.execute("DELETE FROM cadastros WHERE id = %s", (id,))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Registro excluído permanentemente."})
        else:
            # Usuário cria solicitação de EXCLUSÃO
            # Salvamos apenas o nome/motivo no JSON, pois o ID já identifica o registro
            dados_json = json.dumps({"motivo": "Solicitado pelo usuário", "razao_social": nome_registro})
            cur.execute("""
                INSERT INTO solicitacoes_alteracao 
                (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante) 
                VALUES (%s, %s, %s, %s, %s)
            """, ('cadastros', id, 'EXCLUSAO', dados_json, current_user.username))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Solicitação de exclusão enviada para aprovação."})
            
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/excluir_associado/<int:id>", methods=["POST"])
@login_required
def excluir_associado(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # Busca dados para registrar quem foi excluído (histórico)
        cur.execute("SELECT nome FROM associados WHERE id = %s", (id,))
        row = cur.fetchone()
        nome_registro = row[0] if row else "Desconhecido"

        if current_user.role == 'admin':
            # Se for Admin, apaga de verdade
            cur.execute("DELETE FROM associados WHERE id = %s", (id,))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Associado excluído permanentemente."})
        else:
            # Se for Usuário Comum, cria uma SOLICITAÇÃO para o admin aprovar
            import json
            dados_json = json.dumps({"motivo": "Solicitado pelo usuário", "nome": nome_registro})
            cur.execute("""
                INSERT INTO solicitacoes_alteracao 
                (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante) 
                VALUES (%s, %s, %s, %s, %s)
            """, ('associados', id, 'EXCLUSAO', dados_json, current_user.username))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Solicitação de exclusão enviada para aprovação."})
            
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao excluir associado: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- GESTÃO DE CONTAS CORRENTES (NOVAS ROTAS) ---

@app.route("/buscar_contas_correntes_gestao", methods=["GET"])
@login_required
def buscar_contas_correntes_gestao():
    termo = request.args.get("q", "").lower()
    uvr_tela = request.args.get("uvr", "")
    
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        sql = "SELECT id, banco_nome, agencia, conta_corrente, descricao_conta, uvr, associacao FROM contas_correntes WHERE 1=1"
        params = []
        
        # Filtro de UVR (Segurança)
        if current_user.role == 'admin':
            if uvr_tela and uvr_tela != "Todas": 
                sql += " AND uvr = %s"
                params.append(uvr_tela)
        elif current_user.uvr_acesso: 
            sql += " AND uvr = %s"
            params.append(current_user.uvr_acesso)
        
        # Filtro de Texto (Nome do Banco ou Descrição)
        if termo: 
            sql += " AND (LOWER(banco_nome) LIKE %s OR LOWER(descricao_conta) LIKE %s)"
            params.extend([f"%{termo}%", f"%{termo}%"])
            
        sql += " ORDER BY banco_nome ASC, descricao_conta ASC"
        
        cur.execute(sql, tuple(params))
        
        res = []
        for r in cur.fetchall():
            res.append({
                "id": r[0], "banco": r[1], "agencia": r[2], 
                "conta": r[3], "descricao": r[4], "uvr": r[5], "associacao": r[6]
            })
        return jsonify(res)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/get_conta_corrente_detalhe/<int:id>", methods=["GET"])
@login_required
def get_conta_corrente_detalhe(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("SELECT * FROM contas_correntes WHERE id = %s", (id,))
        row = cur.fetchone()
        
        if not row: return jsonify({"error": "Não encontrado"}), 404
        
        # Mapeia os dados do banco para o JSON
        data = {
            "id": row[0], "uvr": row[1], "associacao": row[2], 
            "banco_codigo": row[3], "banco_nome": row[4],
            "agencia": row[5], "conta_corrente": row[6],
            "descricao_conta": row[7]
        }
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()



@app.route("/excluir_conta_corrente/<int:id>", methods=["POST"])
@login_required
def excluir_conta_corrente(id):
    if current_user.role != 'admin':
        return jsonify({"error": "Apenas administradores podem excluir contas."}), 403
        
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("DELETE FROM contas_correntes WHERE id = %s", (id,))
        conn.commit()
        return jsonify({"status": "sucesso", "message": "Conta excluída com sucesso."})
    except psycopg2.IntegrityError:
        if conn: conn.rollback()
        return jsonify({"error": "Não é possível excluir esta conta pois ela possui movimentações financeiras registradas."}), 400
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/get_detalhes_solicitacao/<int:id>", methods=["GET"])
@login_required
def get_detalhes_solicitacao(id):
    if current_user.role != 'admin': return jsonify({"error": "Negado"}), 403
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("SELECT id_registro, dados_novos, usuario_solicitante, data_solicitacao, tabela_alvo, tipo_solicitacao FROM solicitacoes_alteracao WHERE id = %s", (id,))
        solic = cur.fetchone()
        
        if not solic: return jsonify({"error": "Solicitação não encontrada"}), 404
        
        id_reg, dados_novos_json, usuario, data_solic, tabela, tipo = solic
        
        d_novos = {}
        if dados_novos_json:
            if isinstance(dados_novos_json, str):
                try: d_novos = json.loads(dados_novos_json)
                except: d_novos = {}
            else: d_novos = dados_novos_json
        
        if tipo == 'EXCLUSAO':
             return jsonify({
                "id_solicitacao": id, "usuario": usuario, 
                "data": data_solic.strftime('%d/%m %H:%M') if data_solic else "Data desc.",
                "tipo": "EXCLUSAO", "tabela": tabela,
                "info_extra": f"Solicitação para EXCLUIR permanentemente o registro ID {id_reg}."
            })

        sql_atual = ""
        cols = []
        labels = {}

        if tabela == 'associados':
            sql_atual = "SELECT nome, cpf, rg, data_nascimento, data_admissao, status, uvr, associacao, cep, logradouro, endereco_numero, bairro, cidade, uf, telefone FROM associados WHERE id = %s"
            cols = ["nome","cpf","rg","data_nascimento","data_admissao","status","uvr","associacao","cep","logradouro","endereco_numero","bairro","cidade","uf","telefone"]
            labels = {"nome":"Nome","cpf":"CPF","rg":"RG","data_nascimento":"Nascimento","data_admissao":"Admissão","status":"Status","uvr":"UVR","associacao":"Assoc","cep":"CEP","logradouro":"Logradouro","endereco_numero":"Núm","bairro":"Bairro","cidade":"Cidade","uf":"UF","telefone":"Tel"}
        
        elif tabela == 'cadastros':
            sql_atual = "SELECT razao_social, cnpj, tipo_cadastro, tipo_atividade, uvr, associacao, cep, logradouro, numero, bairro, cidade, uf, telefone FROM cadastros WHERE id = %s"
            cols = ["razao_social","cnpj","tipo_cadastro","tipo_atividade","uvr","associacao","cep","logradouro","numero","bairro","cidade","uf","telefone"]
            labels = {"razao_social":"Razão Social","cnpj":"CNPJ","tipo_cadastro":"Tipo","tipo_atividade":"Atividade","uvr":"UVR","associacao":"Assoc","cep":"CEP","logradouro":"Logradouro","numero":"Núm","bairro":"Bairro","cidade":"Cidade","uf":"UF","telefone":"Tel"}
        
        elif tabela == 'contas_correntes':
            sql_atual = "SELECT uvr, associacao, banco_codigo, banco_nome, agencia, conta_corrente, descricao_conta FROM contas_correntes WHERE id = %s"
            cols = ["uvr", "associacao", "banco_codigo", "banco_nome", "agencia", "conta_corrente", "descricao_conta"]
            labels = {"uvr":"UVR", "associacao":"Assoc", "banco_codigo":"Cód Banco", "banco_nome":"Nome Banco", "agencia":"Agência", "conta_corrente":"Conta", "descricao_conta":"Descrição"}
        
        # --- TRANSAÇÕES FINANCEIRAS ---
        elif tabela == 'transacoes_financeiras':
            sql_atual = """
                SELECT uvr, associacao, data_documento, tipo_transacao, tipo_atividade, 
                       numero_documento, nome_cadastro_origem AS nome_origem, 
                       valor_total_documento AS valor_total 
                FROM transacoes_financeiras WHERE id = %s
            """
            cols = ["uvr", "associacao", "data_documento", "tipo_transacao", "tipo_atividade", "numero_documento", "nome_origem", "valor_total"]
            labels = {
                "uvr": "UVR", "associacao": "Associação", "data_documento": "Data Doc.", 
                "tipo_transacao": "Tipo", "tipo_atividade": "Atividade", 
                "numero_documento": "Nº Doc", "nome_origem": "Origem/Destino", "valor_total": "Total (R$)"
            }

        if not sql_atual: return jsonify({"error": "Tabela desconhecida"}), 400

        cur.execute(sql_atual, (id_reg,))
        atual = cur.fetchone()
        d_atuais = {}
        if atual:
            for i, c in enumerate(cols): d_atuais[c] = str(atual[i]) if atual[i] is not None else ""
        else:
            for c in cols: d_atuais[c] = "(Registro não encontrado)"

        comp = []
        for k, l in labels.items():
            val_atual = d_atuais.get(k,"")
            val_novo = str(d_novos.get(k,"")) if d_novos.get(k) is not None else ""
            if val_atual == "None": val_atual = ""
            if val_novo == "None": val_novo = ""
            
            comp.append({ "campo": l, "valor_atual": val_atual, "valor_novo": val_novo, "mudou": val_atual != val_novo })
        
        # --- LÓGICA ESPECIAL PARA ITENS DA TRANSAÇÃO (GERA TABELA HTML) ---
        if tabela == 'transacoes_financeiras':
            # 1. Busca Itens do Banco (Antigos)
            cur.execute("""
                SELECT descricao, unidade, quantidade, valor_unitario, valor_total_item 
                FROM itens_transacao WHERE id_transacao = %s ORDER BY id ASC
            """, (id_reg,))
            itens_db = cur.fetchall()
            
            # 2. Busca Itens do JSON (Novos)
            itens_novos = d_novos.get('itens', [])

            # Função auxiliar para gerar HTML
            def gerar_html_tabela(lista_itens, origem):
                if not lista_itens: return '<small class="text-muted">Nenhum item</small>'
                
                html = '<table class="table table-sm table-bordered mb-0" style="font-size:0.7rem; background-color: #fff;">'
                html += '<thead class="table-light"><tr><th>Desc</th><th>Qtd</th><th>Tot</th></tr></thead><tbody>'
                
                for it in lista_itens:
                    if origem == 'db':
                        desc, un, qtd, unit, tot = it[0], it[1], it[2], it[3], it[4]
                    else: # json
                        desc = it.get('descricao')
                        un = it.get('unidade')
                        qtd = it.get('quantidade')
                        tot = it.get('valor_total_item')
                    
                    try: qtd_fmt = f"{float(qtd):g}" 
                    except: qtd_fmt = str(qtd)
                    
                    try: tot_fmt = f"{float(tot):.2f}".replace('.', ',')
                    except: tot_fmt = str(tot)

                    html += f'<tr><td>{desc}</td><td>{qtd_fmt} {un}</td><td>{tot_fmt}</td></tr>'
                
                html += '</tbody></table>'
                return html

            html_atual = gerar_html_tabela(itens_db, 'db')
            html_novo = gerar_html_tabela(itens_novos, 'json')
            
            # Compara strings HTML (não é perfeito, mas serve para indicar mudança visual)
            mudou_itens = (html_atual != html_novo)

            comp.append({
                "campo": "Detalhamento de Itens",
                "valor_atual": html_atual,
                "valor_novo": html_novo,
                "mudou": mudou_itens
            })
        # ------------------------------------------------------------------

        return jsonify({
            "id_solicitacao": id, "usuario": usuario, 
            "data": data_solic.strftime('%d/%m %H:%M') if data_solic else "Data desc.",
            "tipo": "EDICAO", "comparacao": comp, "foto_nova_base64": d_novos.get("foto_base64","")
        })
    except Exception as e:
        app.logger.error(f"Erro em get_detalhes_solicitacao: {e}")
        return jsonify({"error": f"Erro interno: {str(e)}"}), 500
    finally:
        if conn: conn.close()
        
# --- GESTÃO DE TRANSAÇÕES (CRUD) ---

@app.route("/buscar_transacoes_gestao", methods=["GET"])
@login_required
def buscar_transacoes_gestao():
    # Pega os filtros que vieram do Javascript
    data_ini = request.args.get("data_inicial")
    data_fim = request.args.get("data_final")
    tipo = request.args.get("tipo")
    uvr_tela = request.args.get("uvr")
    termo = request.args.get("q", "").lower()

    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        # SQL Base
        sql = """
            SELECT id, data_documento, tipo_transacao, nome_cadastro_origem, 
                   numero_documento, valor_total_documento, status_pagamento, uvr
            FROM transacoes_financeiras 
            WHERE 1=1
        """
        params = []

        # --- Lógica de Segurança UVR ---
        if current_user.role == 'admin':
            if uvr_tela and uvr_tela != "Todas":
                sql += " AND uvr = %s"
                params.append(uvr_tela)
        elif current_user.uvr_acesso:
            sql += " AND uvr = %s"
            params.append(current_user.uvr_acesso)
        
        # --- Aplica os outros filtros ---
        if data_ini:
            sql += " AND data_documento >= %s"
            params.append(data_ini)
        
        if data_fim:
            sql += " AND data_documento <= %s"
            params.append(data_fim)
            
        if tipo and tipo != "Todos":
            sql += " AND tipo_transacao = %s"
            params.append(tipo)

        if termo:
            sql += " AND (LOWER(nome_cadastro_origem) LIKE %s OR numero_documento LIKE %s)"
            params.extend([f"%{termo}%", f"%{termo}%"])

        # Ordenar: Mais recentes primeiro
        sql += " ORDER BY data_documento DESC, id DESC LIMIT 100"

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

        resultados = []
        for r in rows:
            # Formata a data para ficar bonitinha (dd/mm/aaaa)
            data_fmt = r[1].strftime('%d/%m/%Y') if r[1] else "-"
            # Formata o valor para dinheiro
            val_fmt = f"{float(r[5]):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

            resultados.append({
                "id": r[0],
                "data": data_fmt,
                "tipo": r[2],
                "origem": r[3],
                "doc": r[4] or "-",
                "valor": val_fmt,
                "status": r[6],
                "uvr": r[7]
            })

        return jsonify(resultados)

    except Exception as e:
        app.logger.error(f"Erro ao buscar transações: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()
@app.route("/get_transacao_detalhes/<int:id>", methods=["GET"])
@login_required
def get_transacao_detalhes(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        # 1. Busca os dados gerais da transação
        cur.execute("""
            SELECT id, uvr, associacao, tipo_transacao, tipo_atividade,
                   nome_cadastro_origem, numero_documento, data_documento,
                   valor_total_documento, status_pagamento, data_hora_registro
            FROM transacoes_financeiras WHERE id = %s
        """, (id,))
        cabecalho = cur.fetchone()

        if not cabecalho:
            return jsonify({"error": "Transação não encontrada"}), 404

        # 2. Busca os itens e faz JOIN para descobrir Grupo e Subgrupo originais
        # Usamos LEFT JOIN pois o produto pode ter sido excluído do catálogo, mas o registro histórico fica
        cur.execute("""
            SELECT it.descricao, it.unidade, it.quantidade, it.valor_unitario, it.valor_total_item,
                   ps.grupo, ps.subgrupo
            FROM itens_transacao it
            LEFT JOIN produtos_servicos ps ON it.descricao = ps.item
            WHERE it.id_transacao = %s
            ORDER BY it.id ASC
        """, (id,))
        itens_db = cur.fetchall()

        # Formata os itens para JSON
        itens = []
        for item in itens_db:
            itens.append({
                "descricao": item[0],
                "unidade": item[1],
                "quantidade": float(item[2]),
                "valor_unitario": float(item[3]),
                "valor_total": float(item[4]),
                # Se não achar no catálogo, manda string vazia ou mantém null
                "grupo": item[5] or "",
                "subgrupo": item[6] or ""
            })

        # Formata datas e valores do cabeçalho
        data_doc = cabecalho[7].strftime('%d/%m/%Y') if cabecalho[7] else "-"
        
        dados_retorno = {
            "id": cabecalho[0],
            "uvr": cabecalho[1],
            "tipo": cabecalho[3],
            "atividade": cabecalho[4],
            "origem": cabecalho[5],
            "doc": cabecalho[6] or "-",
            "data": data_doc,
            "valor_total": float(cabecalho[8]),
            "status": cabecalho[9],
            "itens": itens
        }

        return jsonify(dados_retorno)

    except Exception as e:
        app.logger.error(f"Erro ao buscar detalhes da transação: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- NOVAS ROTAS PARA GESTÃO DE PRODUTOS E SUBGRUPOS ---

@app.route("/api/subgrupos", methods=["GET", "POST"])
@login_required
def api_subgrupos():
    conn = conectar_banco()
    cur = conn.cursor()
    try:
        if request.method == "POST":
            # Cadastrar ou Editar Subgrupo
            dados = request.json
            acao = dados.get('acao')
            nome = dados.get('nome', '').strip()
            atividade = dados.get('atividade_pai', '')
            id_sub = dados.get('id')

            if not nome or not atividade:
                return jsonify({"erro": "Nome e Atividade (Grupo) são obrigatórios."}), 400

            if acao == 'novo':
                try:
                    cur.execute("INSERT INTO subgrupos (nome, atividade_pai) VALUES (%s, %s) RETURNING id", (nome, atividade))
                    conn.commit()
                    return jsonify({"sucesso": True, "id": cur.fetchone()[0]})
                except psycopg2.IntegrityError:
                    conn.rollback()
                    return jsonify({"erro": "Já existe um subgrupo com este nome para esta atividade."}), 400

            elif acao == 'editar':
                cur.execute("UPDATE subgrupos SET nome = %s WHERE id = %s", (nome, id_sub))
                # Também atualiza o texto na tabela antiga para manter consistência por enquanto
                cur.execute("UPDATE produtos_servicos SET subgrupo = %s WHERE id_subgrupo = %s", (nome, id_sub))
                conn.commit()
                return jsonify({"sucesso": True})
            
            elif acao == 'excluir':
                # Verifica se há produtos usando este subgrupo
                cur.execute("SELECT COUNT(*) FROM produtos_servicos WHERE id_subgrupo = %s", (id_sub,))
                if cur.fetchone()[0] > 0:
                    return jsonify({"erro": "Não é possível excluir: existem produtos vinculados a este subgrupo."}), 400
                
                cur.execute("DELETE FROM subgrupos WHERE id = %s", (id_sub,))
                conn.commit()
                return jsonify({"sucesso": True})

        # GET: Listar Subgrupos
        atividade_filtro = request.args.get('atividade')
        sql = "SELECT id, nome, atividade_pai FROM subgrupos WHERE 1=1"
        params = []
        if atividade_filtro:
            sql += " AND atividade_pai = %s"
            params.append(atividade_filtro)
        sql += " ORDER BY nome"
        
        cur.execute(sql, tuple(params))
        lista = [{"id": r[0], "nome": r[1], "atividade": r[2]} for r in cur.fetchall()]
        return jsonify(lista)

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro API Subgrupos: {e}")
        return jsonify({"erro": str(e)}), 500
    finally:
        conn.close()

@app.route("/api/produtos_crud", methods=["GET", "POST", "DELETE"])
@login_required
def api_produtos_crud():
    conn = conectar_banco()
    cur = conn.cursor()
    try:
        if request.method == "POST":
            # Salvar Produto
            d = request.json
            id_prod = d.get('id')
            item = d.get('item', '').strip()
            id_subgrupo = d.get('id_subgrupo')
            grupo = d.get('grupo') # Atividade
            
            # Busca nome do subgrupo para manter compatibilidade
            nome_subgrupo = ""
            if id_subgrupo:
                cur.execute("SELECT nome FROM subgrupos WHERE id = %s", (id_subgrupo,))
                row = cur.fetchone()
                if row: nome_subgrupo = row[0]

            if not id_prod: # NOVO
                # Define tipo baseado no grupo (atividade) - Simplificação
                tipo = "Despesa" # Padrão
                if "Venda de Recicláveis" in grupo or "Receitas" in grupo or "Doações" in grupo:
                    tipo = "Receita"
                
                try:
                    cur.execute("""
                        INSERT INTO produtos_servicos (tipo, tipo_atividade, grupo, subgrupo, id_subgrupo, item, data_hora_cadastro)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW())
                    """, (tipo, grupo, grupo, nome_subgrupo, id_subgrupo, item))
                    conn.commit()
                except psycopg2.IntegrityError:
                    conn.rollback()
                    return jsonify({"erro": "Já existe um item com este nome."}), 400
            else: # EDITAR
                cur.execute("""
                    UPDATE produtos_servicos 
                    SET tipo_atividade=%s, grupo=%s, subgrupo=%s, id_subgrupo=%s, item=%s
                    WHERE id=%s
                """, (grupo, grupo, nome_subgrupo, id_subgrupo, item, id_prod))
                conn.commit()
            
            return jsonify({"sucesso": True})

        elif request.method == "DELETE":
            id_prod = request.args.get('id')
            # Verifica uso em transações
            cur.execute("SELECT COUNT(*) FROM itens_transacao WHERE descricao = (SELECT item FROM produtos_servicos WHERE id = %s)", (id_prod,))
            if cur.fetchone()[0] > 0:
                 return jsonify({"erro": "Não pode excluir: Item já usado em transações financeiras."}), 400
            
            cur.execute("DELETE FROM produtos_servicos WHERE id = %s", (id_prod,))
            conn.commit()
            return jsonify({"sucesso": True})

        # GET: Listar Produtos (COM NOVOS FILTROS)
        subgrupo_filtro = request.args.get('id_subgrupo')
        grupo_filtro = request.args.get('grupo')
        tipo_filtro = request.args.get('tipo') # <--- NOVO
        
        sql = """
            SELECT p.id, p.item, p.tipo_atividade, s.nome, s.id, p.tipo 
            FROM produtos_servicos p
            LEFT JOIN subgrupos s ON p.id_subgrupo = s.id
            WHERE 1=1
        """
        params = []
        
        if tipo_filtro:
            sql += " AND p.tipo = %s"
            params.append(tipo_filtro)
            
        if grupo_filtro:
            sql += " AND p.tipo_atividade = %s"
            params.append(grupo_filtro)
            
        if subgrupo_filtro:
            sql += " AND p.id_subgrupo = %s"
            params.append(subgrupo_filtro)
            
        sql += " ORDER BY p.item"
        cur.execute(sql, tuple(params))
        
        # Agora retornamos também o campo 'tipo' para exibir na tabela
        res = [{
            "id": r[0], 
            "item": r[1], 
            "grupo": r[2], 
            "subgrupo_nome": r[3], 
            "id_subgrupo": r[4],
            "tipo": r[5] 
        } for r in cur.fetchall()]
        
        return jsonify(res)

    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"erro": str(e)}), 500
    finally:
        conn.close()
@app.route("/excluir_transacao/<int:id>", methods=["POST"])
@login_required
def excluir_transacao(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # Verifica se tem pagamentos vinculados
        cur.execute("SELECT valor_pago_recebido FROM transacoes_financeiras WHERE id = %s", (id,))
        row = cur.fetchone()
        if not row: return jsonify({"error": "Transação não encontrada"}), 404
        
        valor_pago = row[0]
        if valor_pago > 0:
            return jsonify({"error": "Não é possível excluir esta transação pois ela possui pagamentos/recebimentos registrados no Fluxo de Caixa. Estorne os pagamentos antes."}), 400

        # Pega dados para o log/solicitação
        cur.execute("SELECT numero_documento, nome_cadastro_origem FROM transacoes_financeiras WHERE id = %s", (id,))
        dados_reg = cur.fetchone()
        desc = f"NF {dados_reg[0]} - {dados_reg[1]}"

        if current_user.role == 'admin':
            cur.execute("DELETE FROM transacoes_financeiras WHERE id = %s", (id,))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Transação excluída com sucesso."})
        else:
            # Solicitação
            dados_json = json.dumps({"motivo": "Solicitado pelo usuário", "descricao": desc})
            cur.execute("""
                INSERT INTO solicitacoes_alteracao 
                (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante) 
                VALUES (%s, %s, %s, %s, %s)
            """, ('transacoes_financeiras', id, 'EXCLUSAO', dados_json, current_user.username))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Solicitação de exclusão enviada."})

    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()
# --- ROTAS DE GESTÃO DO FLUXO DE CAIXA (CRUD) ---

@app.route("/get_movimentacao_detalhes/<int:id>")
@login_required
def get_movimentacao_detalhes(id):
    conn = conectar_banco()
    cur = conn.cursor()
    try:
        # Busca dados do Fluxo e cruza com Transações para pegar Doc e Atividade
        cur.execute("""
            SELECT 
                fc.id, 
                fc.data_efetiva, 
                fc.valor_efetivo, 
                fc.tipo_movimentacao, 
                fc.numero_documento_bancario,
                STRING_AGG(DISTINCT tf.numero_documento, ', ') as docs_nfs,
                STRING_AGG(DISTINCT tf.tipo_atividade, ', ') as atividades
            FROM fluxo_caixa fc
            LEFT JOIN fluxo_caixa_transacoes_link fctl ON fc.id = fctl.id_fluxo_caixa
            LEFT JOIN transacoes_financeiras tf ON fctl.id_transacao_financeira = tf.id
            WHERE fc.id = %s
            GROUP BY fc.id, fc.data_efetiva, fc.valor_efetivo, fc.tipo_movimentacao, fc.numero_documento_bancario
        """, (id,))
        
        row = cur.fetchone()
        
        if not row: return jsonify({"error": "Não encontrado"}), 404
        
        doc_bancario = row[4] or ""
        docs_nfs = row[5] or ""
        atividades = row[6] or ""
        
        # Lógica para exibir o documento principal
        # Se tiver NF vinculada, mostra ela. Se não, mostra o doc bancário.
        display_doc = docs_nfs if docs_nfs else doc_bancario
        if not display_doc: display_doc = "-"

        # Se tiver doc bancário E nota, mostra ambos
        if docs_nfs and doc_bancario:
            display_doc = f"NF: {docs_nfs} | Bancário: {doc_bancario}"

        return jsonify({
            "id": row[0],
            "data": row[1].strftime('%d/%m/%Y'),
            "valor": float(row[2]),
            "tipo": row[3],
            "documento_exibicao": display_doc,
            "atividade": atividades or "Não informada (Manual)",
            "vinculado": (docs_nfs != "")
        })
    except Exception as e:
        app.logger.error(f"Erro ao buscar detalhes da movimentação: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route("/excluir_movimentacao/<int:id>", methods=["POST"])
@login_required
def excluir_movimentacao(id):
    if current_user.role != 'admin': 
        return jsonify({"error": "Apenas administradores podem excluir."}), 403

    conn = conectar_banco()
    cur = conn.cursor()
    try:
        # 1. Busca se há vínculos com Transações (NFs) para estornar
        cur.execute("""
            SELECT id_transacao_financeira, valor_aplicado_nesta_nf 
            FROM fluxo_caixa_transacoes_link 
            WHERE id_fluxo_caixa = %s
        """, (id,))
        links = cur.fetchall()
        
        # 2. Para cada NF vinculada, reverte o saldo
        for id_transacao, valor_estorno in links:
            cur.execute("SELECT valor_total_documento, valor_pago_recebido FROM transacoes_financeiras WHERE id = %s", (id_transacao,))
            row_t = cur.fetchone()
            
            if row_t:
                total_doc = row_t[0]
                pago_atual = row_t[1]
                
                novo_pago = pago_atual - valor_estorno
                if novo_pago < 0: novo_pago = 0
                
                # Recalcula status
                novo_status = "Aberto"
                if novo_pago > 0:
                    if novo_pago >= total_doc: novo_status = "Liquidado"
                    else: novo_status = "Parcialmente Pago/Recebido"
                
                cur.execute("""
                    UPDATE transacoes_financeiras 
                    SET valor_pago_recebido = %s, status_pagamento = %s 
                    WHERE id = %s
                """, (novo_pago, novo_status, id_transacao))

        # 3. Exclui do Fluxo de Caixa (o DELETE CASCADE no banco remove os links automaticamente)
        cur.execute("DELETE FROM fluxo_caixa WHERE id = %s", (id,))
        conn.commit()
        
        return jsonify({"status": "sucesso", "message": "Movimentação excluída e saldos estornados!"})

    except Exception as e:
        conn.rollback()
        app.logger.error(f"Erro ao excluir movimentação: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

# --- Páginas de Sucesso ---
def pagina_sucesso_base(titulo, mensagem):
    return f"""<!DOCTYPE html><html lang="pt-br"><head><meta charset="UTF-8"><title>{titulo}</title>
               <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
               </head><body class="bg-light"><div class="container py-5">
               <div class="alert alert-success text-center"><h2>{mensagem} ✅</h2>
               <a href="/" class="btn btn-primary mt-3">Voltar ao Início</a></div></div></body></html>"""

@app.route("/sucesso")
def sucesso(): return pagina_sucesso_base("Sucesso", "Cadastro de Cliente/Fornecedor realizado com sucesso!")
@app.route("/sucesso_associado")
def sucesso_associado(): return pagina_sucesso_base("Sucesso", "Associado cadastrado com sucesso!")
@app.route("/sucesso_transacao")
def sucesso_transacao(): return pagina_sucesso_base("Sucesso", "Transação financeira registrada com sucesso!")
@app.route("/sucesso_produto_servico") 
def sucesso_produto_servico(): return pagina_sucesso_base("Sucesso", "Produto/Serviço cadastrado com sucesso!")
@app.route("/sucesso_conta_corrente") 
def sucesso_conta_corrente(): return pagina_sucesso_base("Sucesso", "Conta Corrente cadastrada com sucesso!")
@app.route("/sucesso_denuncia")
def sucesso_denuncia(): return pagina_sucesso_base("Sucesso", "Denúncia registrada com sucesso!")


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO, 
                        format='%(asctime)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(funcName)s - %(message)s')
    
    if not app.debug: 
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)
        app.logger.addHandler(stream_handler)
    
    app.logger.info("Iniciando o aplicativo Flask...")
    app.run(host='0.0.0.0', port=5000, debug=True)