import base64
import re
import json
import io
import csv
import requests
import os
import psycopg2
import calendar
import secrets
import smtplib
import socket
from datetime import datetime, date, timedelta
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
from urllib.parse import unquote, urlparse
from email.message import EmailMessage
import cloudinary
import cloudinary.uploader
from cloudinary.utils import cloudinary_url

# Import do Cursor para o Banco de Dados
from psycopg2.extras import RealDictCursor

# --- FLASK IMPORTS ---
from flask import Flask, render_template, request, redirect, url_for, jsonify, Response, send_file, make_response, flash, render_template_string
from flask_login import LoginManager, UserMixin, login_user, login_required, logout_user, current_user
from werkzeug.security import generate_password_hash, check_password_hash
from flask import send_from_directory
from jinja2 import TemplateNotFound

# --- REPORTLAB IMPORTS ---
from reportlab.pdfgen import canvas 
from reportlab.lib.pagesizes import letter, A4, landscape
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, PageBreak, Image as ReportLabImage
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_RIGHT, TA_LEFT
from reportlab.lib import colors
from reportlab.lib.units import inch, cm
from reportlab.lib.utils import ImageReader
from pypdf import PdfReader, PdfWriter

# --- FIM DOS IMPORTS ---

# --- CONFIGURAÇÕES GLOBAIS ---
GRUPOS_FIXOS_SISTEMA = [
    "Operação e Produção",
    "Gestão Administrativa e Financeira",
    "Despesas de manutenção",
    "Comercialização de Materiais Recicláveis",
    "Outras Receitas",
    "Rateio dos Associados",
    "Prestação de Serviços e Parcerias",
    "Gestão Associativa"  
]

# Carrega as variáveis do arquivo .env
load_dotenv()

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 64 * 1024 * 1024  # Limite de 64MB
app.secret_key = os.getenv('SECRET_KEY', 'chave_secreta_padrao_dev')

cloudinary_setup_error = None
cloudinary_last_error = None

def _read_env(*keys):
    """Retorna o primeiro valor de ambiente não vazio e LOGA se achou ou não."""
    for key in keys:
        # Tenta pegar a variável
        value = os.getenv(key)
        
        # LOG DE DEBUG (Vai aparecer no painel do Render)
        if value:
            # Mostra apenas os primeiros 3 caracteres para segurança
            print(f"DEBUG: Variável '{key}' ENCONTRADA. Valor começa com: {value[:3]}...")
        else:
            print(f"DEBUG: Variável '{key}' NÃO encontrada ou vazia.")

        if value is None:
            continue
        
        cleaned = value.strip().strip('"').strip("'")
        if cleaned:
            return cleaned
    return None


def _is_render_env():
    return bool(_read_env("RENDER", "RENDER_SERVICE_ID"))


def _configure_cloudinary():
    """Configura Cloudinary de forma robusta para ambientes locais e Render."""
    global cloudinary_setup_error
    cloudinary_url = _read_env("CLOUDINARY_URL", "cloudinary_url")
    cloud_name = _read_env("CLOUDINARY_CLOUD_NAME", "CLOUDINARY_CLOUDNAME")
    api_key = _read_env("CLOUDINARY_API_KEY")
    api_secret = _read_env("CLOUDINARY_API_SECRET")

    try:
        if cloudinary_url:
            parsed = urlparse(cloudinary_url)
            if parsed.scheme.startswith("cloudinary") and parsed.hostname and parsed.username and parsed.password:
                cloudinary.config(
                    cloud_name=parsed.hostname,
                    api_key=parsed.username,
                    api_secret=parsed.password,
                    secure=True,
                )
            else:
                cloudinary.config(cloudinary_url=cloudinary_url, secure=True)

                
            cloudinary_setup_error = None
            return True

        if all([cloud_name, api_key, api_secret]):
            cloudinary.config(
                cloud_name=cloud_name,
                api_key=api_key,
                api_secret=api_secret,
                secure=True,
            )
            cloudinary_setup_error = None
            return True
    except Exception as e:
        cloudinary_setup_error = str(e)
        app.logger.error("Falha ao configurar Cloudinary: %s", e)
        return False

    faltantes = []
    if not cloudinary_url:
        if not cloud_name:
            faltantes.append("CLOUDINARY_CLOUD_NAME")
        if not api_key:
            faltantes.append("CLOUDINARY_API_KEY")
        if not api_secret:
            faltantes.append("CLOUDINARY_API_SECRET")
    cloudinary_setup_error = f"Variáveis ausentes: {', '.join(faltantes) if faltantes else 'CLOUDINARY_URL'}"
    app.logger.warning(
        "Cloudinary não configurado. Defina CLOUDINARY_URL ou as variáveis: %s",
        ", ".join(faltantes) if faltantes else "CLOUDINARY_URL",
    )
    return False


cloudinary_configured = _configure_cloudinary()


def _ensure_cloudinary_configured():
    global cloudinary_configured
    if cloudinary_configured:
        return True
    cloudinary_configured = _configure_cloudinary()
    return cloudinary_configured

def _extract_cloudinary_public_id(url):
    if not url or "res.cloudinary.com" not in url:
        return None
    try:
        path = url.split("/upload/", 1)[1]
        if path.startswith("v") and "/" in path:
            version, rest = path.split("/", 1)
            if version[1:].isdigit():
                path = rest
        return unquote(path.rsplit(".", 1)[0])
    except (IndexError, ValueError):
        return None

def _detect_cloudinary_resource_type(url, default="raw"):
    if not url:
        return default
    if "/image/upload/" in url:
        return "image"
    if "/video/upload/" in url:
        return "video"
    if "/raw/upload/" in url:
        return "raw"
    return default

def _delete_cloudinary_asset(url, resource_type="raw"):
    if not cloudinary_configured:
        return
    public_id = _extract_cloudinary_public_id(url)
    if public_id:
        cloudinary.uploader.destroy(public_id, resource_type=resource_type, invalidate=True)

def _upload_file_to_cloudinary(file_storage, folder, public_id=None, resource_type="auto", file_format=None):
    global cloudinary_last_error
    if not _ensure_cloudinary_configured():
        cloudinary_last_error = cloudinary_setup_error or "Cloudinary não configurado."
        return None
    options = {"folder": folder, "resource_type": resource_type}
    if public_id:
        options["public_id"] = public_id
    if file_format:
        options["format"] = file_format
    try:
        result = cloudinary.uploader.upload(file_storage, **options)
        cloudinary_last_error = None
        return result.get("secure_url")
    except Exception as e:
        cloudinary_last_error = str(e)
        app.logger.error("Falha no upload para Cloudinary (%s): %s", folder, e)
        return None

def _upload_base64_to_cloudinary(data_url, folder, public_id=None):
    global cloudinary_last_error
    if not data_url:
        return None
    if not _ensure_cloudinary_configured():
        cloudinary_last_error = cloudinary_setup_error or "Cloudinary não configurado."
        return None
    options = {"folder": folder, "resource_type": "image"}
    if public_id:
        options["public_id"] = public_id
    try:
        result = cloudinary.uploader.upload(data_url, **options)
        cloudinary_last_error = None
        return result.get("secure_url")
    except Exception as e:
        cloudinary_last_error = str(e)
        app.logger.error("Falha no upload base64 para Cloudinary (%s): %s", folder, e)
        return None

def _build_cloudinary_delivery_url(url):
    if not url or "res.cloudinary.com" not in url:
        return url
    return url

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

@app.route('/resetar_senhas_local')
def resetar_senhas_local():
    try:
        # Tentando usar a função de conexão que costuma existir no seu projeto
        # Se o seu arquivo usa outro nome, mude para o nome da função que você usa para conectar ao Neon
        conn = psycopg2.connect(os.environ.get('DATABASE_URL')) 
        cur = conn.cursor()
        
        # Gera o hash da senha 'uvr123'
        senha_hash = generate_password_hash('uvr123')
        
        # Atualiza o banco
        cur.execute("UPDATE usuarios SET password_hash = %s WHERE username IN ('uvr01', 'uvr02')", (senha_hash,))
        
        conn.commit()
        cur.close()
        conn.close()
        return "<h3>Sucesso!</h3><p>Senhas atualizadas para <b>uvr123</b>. Tente logar agora.</p>"
    except Exception as e:
        return f"Erro ao resetar: {str(e)}"
    
# --- CONFIGURAÇÃO DO BANCO DE DADOS ---
DATABASE_URL = os.getenv('DATABASE_URL')

def conectar_banco():
    """Estabelece conexão com o banco de dados."""
    if DATABASE_URL:
        # Se estiver no Neon ou Produção
        return psycopg2.connect(DATABASE_URL)
    else:
        # Se estiver rodando localmente sem .env
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

def validar_cnpj(cnpj):
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

# --- CRIAÇÃO DE TABELAS (ATUALIZADO) ---
def criar_tabelas_se_nao_existir():
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

       
        # 1. Tabelas Base (Cadastros)
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
        
        # Migração Associados
        try:
            cur.execute("ALTER TABLE associados ADD COLUMN IF NOT EXISTS foto_base64 TEXT;")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()

        # 2. Tabelas Financeiras e Transações
        cur.execute("""
            CREATE TABLE IF NOT EXISTS transacoes_financeiras (
                id SERIAL PRIMARY KEY, uvr VARCHAR(10) NOT NULL, associacao VARCHAR(50) NOT NULL,
                id_cadastro_origem INTEGER REFERENCES cadastros(id), nome_cadastro_origem VARCHAR(255) NOT NULL, 
                numero_documento VARCHAR(100), data_documento DATE NOT NULL, tipo_transacao VARCHAR(20) NOT NULL, 
                tipo_atividade VARCHAR(255) NOT NULL, valor_total_documento DECIMAL(12, 2) NOT NULL,
                data_hora_registro TIMESTAMP NOT NULL, valor_pago_recebido DECIMAL(12, 2) DEFAULT 0.00,
                status_pagamento VARCHAR(30) DEFAULT 'Aberto',
                id_patrimonio INTEGER,
                categoria_despesa_patrimonio VARCHAR(30),
                medidor_atual DECIMAL(12,2),
                tipo_medidor VARCHAR(10),
                id_motorista INTEGER,
                nome_motorista VARCHAR(255),
                litros DECIMAL(12,3),
                tipo_combustivel VARCHAR(50),
                tipo_manutencao VARCHAR(30),
                garantia_km INTEGER,
                garantia_data DATE,
                proxima_revisao_km INTEGER,
                proxima_revisao_data DATE
            )
        """)
        
        try:
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS valor_pago_recebido DECIMAL(12, 2) DEFAULT 0.00;")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS status_pagamento VARCHAR(30) DEFAULT 'Aberto';")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS id_patrimonio INTEGER;")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS categoria_despesa_patrimonio VARCHAR(30);")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS medidor_atual DECIMAL(12,2);")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS tipo_medidor VARCHAR(10);")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS id_motorista INTEGER;")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS nome_motorista VARCHAR(255);")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS litros DECIMAL(12,3);")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS tipo_combustivel VARCHAR(50);")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS tipo_manutencao VARCHAR(30);")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS garantia_km INTEGER;")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS garantia_data DATE;")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS proxima_revisao_km INTEGER;")
            cur.execute("ALTER TABLE transacoes_financeiras ADD COLUMN IF NOT EXISTS proxima_revisao_data DATE;")
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

        # 3. Produtos e Subgrupos
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
        
        try:
            cur.execute("ALTER TABLE produtos_servicos ADD COLUMN IF NOT EXISTS id_subgrupo INTEGER REFERENCES subgrupos(id);")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()

        # --- MÓDULO EPI: Tabelas base ---
        cur.execute("""
            CREATE TABLE IF NOT EXISTS epis (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(255) NOT NULL UNIQUE,
                descricao TEXT,
                unidade VARCHAR(50) NOT NULL DEFAULT 'un',
                ca_numero VARCHAR(50),
                data_hora_cadastro TIMESTAMP NOT NULL,
                ativo BOOLEAN NOT NULL DEFAULT TRUE
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS epi_movimentos (
                id SERIAL PRIMARY KEY,
                id_epi INTEGER NOT NULL REFERENCES epis(id),
                uvr VARCHAR(10) NOT NULL,
                associacao VARCHAR(50) NOT NULL,
                tipo_movimento VARCHAR(20) NOT NULL,
                quantidade DECIMAL(12, 3) NOT NULL,
                data_movimento DATE NOT NULL,
                observacao TEXT
            )
        """)


        # 4. Bancos e Fluxo de Caixa
        cur.execute("""
            CREATE TABLE IF NOT EXISTS contas_correntes (
                id SERIAL PRIMARY KEY, uvr VARCHAR(10) NOT NULL, associacao VARCHAR(50) NOT NULL,
                banco_codigo VARCHAR(10) NOT NULL, banco_nome VARCHAR(100) NOT NULL, agencia VARCHAR(10) NOT NULL,
                conta_corrente VARCHAR(20) NOT NULL, descricao_conta VARCHAR(255), data_hora_cadastro TIMESTAMP NOT NULL,
                UNIQUE (uvr, banco_codigo, agencia, conta_corrente) 
            )
        """)

        try:
            cur.execute("ALTER TABLE contas_correntes ADD COLUMN IF NOT EXISTS associacao VARCHAR(50);")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()

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
        
        # 5. Tabelas Diversas (Denúncias, Usuários, Solicitações)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS denuncias (
                id SERIAL PRIMARY KEY, numero_denuncia VARCHAR(50) UNIQUE NOT NULL, data_registro TIMESTAMP NOT NULL,
                descricao TEXT NOT NULL, status VARCHAR(50) DEFAULT 'Pendente', uvr VARCHAR(10), associacao VARCHAR(50)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS usuarios (
                id SERIAL PRIMARY KEY, username VARCHAR(50) UNIQUE NOT NULL, password_hash VARCHAR(255) NOT NULL,
                nome_completo VARCHAR(100), role VARCHAR(20) NOT NULL, uvr_acesso VARCHAR(50), ativo BOOLEAN DEFAULT TRUE,
                email VARCHAR(255), reset_token VARCHAR(255), reset_token_expira TIMESTAMP
            )
        """)
        try:
            cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS email VARCHAR(255);")
            cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS reset_token VARCHAR(255);")
            cur.execute("ALTER TABLE usuarios ADD COLUMN IF NOT EXISTS reset_token_expira TIMESTAMP;")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS solicitacoes_alteracao (
                id SERIAL PRIMARY KEY, tabela_alvo VARCHAR(50) NOT NULL, id_registro INTEGER NOT NULL,
                tipo_solicitacao VARCHAR(20) NOT NULL, dados_novos JSONB, usuario_solicitante VARCHAR(50) NOT NULL,
                data_solicitacao TIMESTAMP DEFAULT CURRENT_TIMESTAMP, status VARCHAR(20) DEFAULT 'PENDENTE', observacoes_admin TEXT
            )
        """)

        # 3. Controle de EPIs
        cur.execute("""
            CREATE TABLE IF NOT EXISTS epi_itens (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(255) NOT NULL UNIQUE,
                categoria VARCHAR(100),
                ca VARCHAR(50),
                validade_meses INTEGER,
                data_hora_cadastro TIMESTAMP NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS epi_estoque (
                id SERIAL PRIMARY KEY,
                id_item INTEGER NOT NULL REFERENCES epi_itens(id),
                uvr VARCHAR(10) NOT NULL,
                associacao VARCHAR(50),
                unidade VARCHAR(50) NOT NULL,
                quantidade DECIMAL(10, 3) NOT NULL DEFAULT 0,
                data_hora_atualizacao TIMESTAMP NOT NULL,
                UNIQUE (uvr, associacao, id_item)
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS epi_entregas (
                id SERIAL PRIMARY KEY,
                id_associado INTEGER NOT NULL REFERENCES associados(id),
                uvr VARCHAR(10) NOT NULL,
                associacao VARCHAR(50),
                data_entrega DATE NOT NULL,
                observacoes TEXT,
                usuario_registro VARCHAR(50),
                data_hora_registro TIMESTAMP NOT NULL
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS epi_entrega_itens (
                id SERIAL PRIMARY KEY,
                id_entrega INTEGER NOT NULL REFERENCES epi_entregas(id) ON DELETE CASCADE,
                id_item INTEGER NOT NULL REFERENCES epi_itens(id),
                unidade VARCHAR(50) NOT NULL,
                quantidade DECIMAL(10, 3) NOT NULL,
                data_validade DATE
            )
        """)


        cur.execute("""
            CREATE TABLE IF NOT EXISTS epis_catalogo (
                id SERIAL PRIMARY KEY,
                grupo VARCHAR(255) NOT NULL,
                epi VARCHAR(255) NOT NULL,
                ca VARCHAR(50),
                tipo_protecao VARCHAR(255),
                funcao VARCHAR(100),
                tempo_troca VARCHAR(50),
                data_hora_cadastro TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (grupo, epi, ca, funcao)
            )
        """)

        # --- NOVAS TABELAS DE DOCUMENTOS (ADICIONADAS AGORA) ---
        # Garante que as tabelas de documentos existam
        cur.execute("""
            CREATE TABLE IF NOT EXISTS tipos_documentos (
                id SERIAL PRIMARY KEY, nome VARCHAR(100) NOT NULL, categoria VARCHAR(50) NOT NULL,
                exige_competencia BOOLEAN DEFAULT FALSE, exige_validade BOOLEAN DEFAULT FALSE,
                exige_valor BOOLEAN DEFAULT FALSE, multiplos_arquivos BOOLEAN DEFAULT FALSE, descricao_ajuda TEXT
            )
        """)

        cur.execute("""
            CREATE TABLE IF NOT EXISTS documentos (
                id SERIAL PRIMARY KEY, uvr VARCHAR(10) NOT NULL, id_tipo INTEGER REFERENCES tipos_documentos(id),
                caminho_arquivo VARCHAR(255), nome_original VARCHAR(255), competencia DATE, data_validade DATE,
                valor DECIMAL(12, 2), numero_referencia VARCHAR(100), observacoes TEXT, enviado_por VARCHAR(100),
                data_envio TIMESTAMP DEFAULT NOW(), status VARCHAR(20) DEFAULT 'Pendente', motivo_rejeicao TEXT
            )
        """)

        try:
            cur.execute("ALTER TABLE documentos ADD COLUMN IF NOT EXISTS motivo_rejeicao TEXT;")
            conn.commit()
        except psycopg2.Error:
            conn.rollback()

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

_estrutura_db_garantida = False

def garantir_tipos_documentos_padrao():
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO tipos_documentos (nome, categoria, exige_competencia, exige_validade, exige_valor, multiplos_arquivos, descricao_ajuda)
            SELECT * FROM (VALUES
                ('Nota Fiscal de Serviço', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Informe número, valor e anexe autorização se necessário'),
                ('Notas Fiscais de Receitas', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Informe número, valor e anexe comprovações relacionadas à receita'),
                ('Notas Fiscais de Despesas', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Informe número, valor e anexe comprovações relacionadas à despesa'),
                ('Comprovante de Pagamento', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Comprovante de pagamento das transações financeiras'),
                ('Medição Mensal', 'Mensal – Financeiro', TRUE, FALSE, FALSE, FALSE, 'Deve estar atestada pelo fiscal'),
                ('Relatório de Associados', 'Mensal – Trabalhista', TRUE, FALSE, FALSE, FALSE, 'Lista de ativos, baixados e novos'),
                ('Recibos de Rateio', 'Mensal – Trabalhista', TRUE, FALSE, TRUE, TRUE, 'Comprovantes de pagamento aos associados'),
                ('GPS – INSS', 'Mensal – Trabalhista', TRUE, FALSE, TRUE, FALSE, 'Guia e comprovante de pagamento'),
                ('Extrato Bancário – Associação', 'Mensal – Financeiro', TRUE, FALSE, FALSE, TRUE, 'Conta principal da associação'),
                ('MTR – Manifesto de Transporte', 'Mensal – Operacional', TRUE, FALSE, FALSE, TRUE, 'Documento de transporte de resíduos'),
                ('Relatório fotográfico da carga', 'Mensal – Operacional', TRUE, FALSE, FALSE, TRUE, 'Registro fotográfico da carga transportada'),
                ('Certidão Regularidade Municipal', 'Geral – Fiscal', FALSE, TRUE, FALSE, FALSE, 'Verifique a data de validade na certidão'),
                ('Certidão Regularidade Federal', 'Geral – Fiscal', FALSE, TRUE, FALSE, FALSE, 'Verifique a data de validade na certidão')
            ) AS novos(nome, categoria, exige_competencia, exige_validade, exige_valor, multiplos_arquivos, descricao_ajuda)
            WHERE NOT EXISTS (
                SELECT 1 FROM tipos_documentos existentes WHERE existentes.nome = novos.nome
            )
        """)
        conn.commit()
    except psycopg2.Error as e:
        app.logger.error(f"Erro ao garantir tipos de documentos padrão: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

@app.before_request
def garantir_estrutura_documentos():
    global _estrutura_db_garantida
    if _estrutura_db_garantida:
        return

    try:
        criar_tabelas_se_nao_existir()
        garantir_tipos_documentos_padrao()
        _estrutura_db_garantida = True
    except Exception as e:
        app.logger.error(f"Erro ao garantir estrutura do banco: {e}")

# CHAMADA DA MIGRAÇÃO (Cole isso logo após a definição da função acima)
migrar_dados_antigos_produtos()

class User(UserMixin):
    def __init__(self, id, username, role, uvr_acesso):
        self.id = id
        self.username = username
        username_norm = str(username or "").strip().lower()
        role_norm = str(role or "").strip().lower()

        # Regra de negócio: contas UVR (ex.: uvr01, uvr02) não devem ter perfil de admin.
        if username_norm.startswith("uvr"):
            role_norm = "uvr"

        self.role = role_norm
        self.uvr_acesso = uvr_acesso


def usuario_visitante(user=None):
    alvo = user or current_user
    return str(getattr(alvo, "role", "") or "").strip().lower() == "visitante"


def bloquear_visitante(mensagem="Acesso negado. Usuários visitantes não podem editar ou excluir."):
    if usuario_visitante():
        if request.accept_mimetypes.best == "application/json" or request.path.startswith("/api/"):
            return jsonify({"error": mensagem}), 403
        return mensagem, 403
    return None


def exigir_admin():
    role_sessao = str(getattr(current_user, "role", "") or "").strip().lower()
    username_sessao = str(getattr(current_user, "username", "") or "").strip().lower()

    # Regra explícita de negócio: usuários UVR e visitantes nunca aprovam/reprovam documentos.
    if username_sessao.startswith("uvr") or role_sessao == "visitante":
        if request.accept_mimetypes.best == "application/json" or request.path.startswith("/api/"):
            return jsonify({"error": "Acesso restrito ao administrador."}), 403
        return "Acesso restrito ao administrador.", 403
    if role_sessao != "admin":
        if request.accept_mimetypes.best == "application/json" or request.path.startswith("/api/"):
            return jsonify({"error": "Acesso restrito ao administrador."}), 403
        return "Acesso restrito ao administrador.", 403

    # Defesa em profundidade: confirma o papel atual no banco e evita aprovação por sessão inconsistente.
    conn = None
    cur = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("SELECT role, username FROM usuarios WHERE id = %s", (current_user.id,))
        row = cur.fetchone()
        role_banco = str((row[0] if row else "") or "").strip().lower()
        username_banco = str((row[1] if row and len(row) > 1 else "") or "").strip().lower()
    except Exception:
        role_banco = ""
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    if username_banco.startswith("uvr") or role_banco == "visitante" or role_banco != "admin":
        if request.accept_mimetypes.best == "application/json" or request.path.startswith("/api/"):
            return jsonify({"error": "Acesso restrito ao administrador."}), 403
        return "Acesso restrito ao administrador.", 403
    return None


def enviar_email_recuperacao(destinatario, assunto, corpo):
    def valor_env(chave, padrao=""):
        valor = os.getenv(chave, padrao)
        return valor.strip() if isinstance(valor, str) else valor

    smtp_host = valor_env("SMTP_HOST")
    smtp_port_raw = valor_env("SMTP_PORT", "465")
    smtp_user = valor_env("SMTP_USER")
    smtp_password_raw = valor_env("SMTP_PASSWORD", "")
    smtp_password_sem_aspas = smtp_password_raw.strip("\"'")
    smtp_password = "".join(smtp_password_sem_aspas.split())
    smtp_from = valor_env("SMTP_FROM", smtp_user)
    smtp_security_raw = valor_env("SMTP_SECURITY", "").lower()
    smtp_security = smtp_security_raw or "auto"
    smtp_use_tls = valor_env("SMTP_USE_TLS", "").lower()
    smtp_timeout_raw = valor_env("SMTP_TIMEOUT", "20")
    smtp_ssl_fallback_port_raw = valor_env("SMTP_SSL_FALLBACK_PORT", "465")
    smtp_starttls_fallback_port_raw = valor_env("SMTP_STARTTLS_FALLBACK_PORT", "587")
    smtp_allow_cross_mode_fallback = valor_env("SMTP_ALLOW_CROSS_MODE_FALLBACK", "true").lower() == "true"
    smtp_local_hostname_raw = valor_env("SMTP_LOCAL_HOSTNAME", "")

    try:
        smtp_port = int(smtp_port_raw)
    except ValueError:
        return False, f"Configuração inválida: SMTP_PORT ({smtp_port_raw}) não é numérico."

    try:
        smtp_timeout = int(smtp_timeout_raw)
    except ValueError:
        return False, f"Configuração inválida: SMTP_TIMEOUT ({smtp_timeout_raw}) não é numérico."

    try:
        smtp_ssl_fallback_port = int(smtp_ssl_fallback_port_raw)
    except ValueError:
        return False, (
            "Configuração inválida: SMTP_SSL_FALLBACK_PORT "
            f"({smtp_ssl_fallback_port_raw}) não é numérico."
        )

    try:
        smtp_starttls_fallback_port = int(smtp_starttls_fallback_port_raw)
    except ValueError:
        return False, (
            "Configuração inválida: SMTP_STARTTLS_FALLBACK_PORT "
            f"({smtp_starttls_fallback_port_raw}) não é numérico."
        )

    # Compatibilidade legada: só usa SMTP_USE_TLS quando SMTP_SECURITY não foi definido.
    if not smtp_security_raw and smtp_use_tls in {"true", "false"}:
        smtp_security = "starttls" if smtp_use_tls == "true" else "ssl"
    elif smtp_security_raw and smtp_use_tls in {"true", "false"}:
        app.logger.warning(
            "SMTP_USE_TLS foi ignorado porque SMTP_SECURITY está definido (%s).",
            smtp_security,
        )

    if smtp_security not in {"ssl", "starttls", "plain", "auto"}:
        return False, (
            "Configuração inválida: SMTP_SECURITY deve ser 'ssl', 'starttls', 'plain' ou 'auto'."
        )

    faltantes = []
    if not smtp_host:
        faltantes.append("SMTP_HOST")
    if not smtp_user:
        faltantes.append("SMTP_USER")
    if not smtp_password:
        faltantes.append("SMTP_PASSWORD")
    if not smtp_from:
        faltantes.append("SMTP_FROM")

    if smtp_password_raw and smtp_password != smtp_password_raw:
        app.logger.warning(
            "SMTP_PASSWORD continha aspas/whitespace e foi normalizada automaticamente para autenticação SMTP."
        )

    if faltantes:
        app.logger.warning(
            "Configuração SMTP incompleta. Variáveis ausentes: %s",
            ", ".join(faltantes),
        )
        return False, (
            "Configuração de e-mail incompleta. Variáveis SMTP ausentes: "
            + ", ".join(faltantes)
            + "."
        )

    mensagem = EmailMessage()
    mensagem["Subject"] = assunto
    mensagem["From"] = smtp_from
    mensagem["To"] = destinatario
    mensagem.set_content(corpo)

    hostname_origem = smtp_local_hostname_raw or os.getenv("HOSTNAME") or os.getenv("COMPUTERNAME") or ""
    hostname_origem = hostname_origem.strip()
    if not hostname_origem:
        try:
            hostname_origem = socket.getfqdn().strip()
        except Exception:
            hostname_origem = ""

    smtp_local_hostname = re.sub(r"\s+", "-", hostname_origem)
    smtp_local_hostname = re.sub(r"[^A-Za-z0-9.-]", "-", smtp_local_hostname)
    smtp_local_hostname = re.sub(r"-{2,}", "-", smtp_local_hostname).strip(".-")
    if not smtp_local_hostname:
        smtp_local_hostname = "localhost"

    if hostname_origem and smtp_local_hostname != hostname_origem:
        app.logger.warning(
            "Hostname SMTP local inválido normalizado para EHLO. original=%s normalizado=%s",
            hostname_origem,
            smtp_local_hostname,
        )

    def enviar_com_modo(modo, porta):
        if modo == "ssl":
            with smtplib.SMTP_SSL(
                smtp_host,
                porta,
                local_hostname=smtp_local_hostname,
                timeout=smtp_timeout,
            ) as server:
                server.login(smtp_user, smtp_password)
                server.send_message(mensagem)
            return

        with smtplib.SMTP(
            smtp_host,
            porta,
            local_hostname=smtp_local_hostname,
            timeout=smtp_timeout,
        ) as server:
            server.ehlo()
            if modo == "starttls":
                if not server.has_extn("starttls"):
                    raise smtplib.SMTPNotSupportedError(
                        f"Servidor não anunciou STARTTLS em {smtp_host}:{porta}."
                    )
                server.starttls()
                server.ehlo()
            server.login(smtp_user, smtp_password)
            server.send_message(mensagem)

    tentativas = []
    if smtp_security == "auto":
        if smtp_port == 465:
            tentativas = [("ssl", smtp_port), ("starttls", 587), ("plain", 25)]
        else:
            tentativas = [
                ("starttls", smtp_port),
                ("ssl", smtp_ssl_fallback_port),
                ("plain", smtp_port),
            ]
    else:
        tentativas = [(smtp_security, smtp_port)]
        if smtp_security == "starttls":
            tentativas.append(("ssl", smtp_ssl_fallback_port))
        elif smtp_security == "ssl" and smtp_allow_cross_mode_fallback:
            tentativas.append(("starttls", smtp_starttls_fallback_port))

    erros = []
    for modo, porta in tentativas:
        try:
            enviar_com_modo(modo, porta)
            if (modo, porta) != tentativas[0]:
                app.logger.warning(
                    "SMTP usou fallback com sucesso. host=%s porta=%s modo=%s modo_configurado=%s",
                    smtp_host,
                    porta,
                    modo,
                    smtp_security,
                )
            return True, "E-mail enviado com sucesso."
        except Exception as exc:
            erros.append(f"{modo}@{porta}:{type(exc).__name__}: {exc}")
            if smtp_security != "auto":
                # Com segurança explícita, ainda permitimos fallback cruzado para reduzir falhas por bloqueio de porta.
                if len(tentativas) > 1 and modo == tentativas[0][0]:
                    app.logger.warning(
                        "SMTP modo primário falhou; tentando fallback. host=%s modo_primario=%s porta_primaria=%s erro=%s",
                        smtp_host,
                        modo,
                        porta,
                        exc,
                    )
                    continue

                app.logger.exception(
                    "Falha ao enviar e-mail de recuperação via SMTP. host=%s porta=%s security=%s",
                    smtp_host,
                    smtp_port,
                    smtp_security,
                )
                if smtp_security == "starttls":
                    if isinstance(exc, smtplib.SMTPServerDisconnected):
                        return False, (
                            "Erro ao enviar e-mail: conexão SMTP foi encerrada pelo servidor/rede. "
                            "Isso costuma indicar bloqueio/interceptação de porta no ambiente e não senha inválida. "
                            "No Render, prefira SMTP_SECURITY=ssl com SMTP_PORT=465 e remova SMTP_USE_TLS."
                        )
                    return False, (
                        "Erro ao enviar e-mail: STARTTLS não suportado e fallback SSL também falhou. "
                        "Defina SMTP_SECURITY=ssl com SMTP_PORT=465 e remova SMTP_USE_TLS, "
                        "ou verifique bloqueio de rede na porta 587."
                    )
                if isinstance(exc, smtplib.SMTPAuthenticationError):
                    return False, (
                        "Erro ao enviar e-mail: autenticação SMTP recusada. "
                        "Verifique SMTP_USER/SMTP_PASSWORD (senha de app do Gmail sem whitespace/aspas) "
                        "e se 2FA está ativo."
                    )
                if isinstance(exc, (TimeoutError, socket.timeout)):
                    return False, (
                        "Erro ao enviar e-mail: tempo de conexão SMTP esgotado (timeout). "
                        "Isso geralmente é bloqueio de rede/porta. Tente SMTP_SECURITY=starttls com SMTP_PORT=587 "
                        "ou SMTP_SECURITY=ssl com SMTP_PORT=465, e ajuste SMTP_TIMEOUT (ex.: 30)."
                    )
                return False, f"Erro ao enviar e-mail: {exc}"

    app.logger.error(
        "Falha ao enviar e-mail de recuperação via SMTP em todos os modos automáticos. "
        "host=%s tentativas=%s erros=%s",
        smtp_host,
        ", ".join([f"{modo}@{porta}" for modo, porta in tentativas]),
        " | ".join(erros),
    )
    return False, "Erro ao enviar e-mail: " + " | ".join(erros)


def renderizar_template_com_fallback(template_name, **contexto):
    try:
        return render_template(template_name, **contexto)
    except TemplateNotFound:
        conteudo = (
            "<h1>Template não encontrado</h1>"
            f"<p>Não foi possível localizar o arquivo {template_name}. "
            "Verifique se ele existe dentro da pasta templates.</p>"
        )
        return render_template_string(conteudo), 500


def obter_uvrs_existentes():
    conn = conectar_banco()
    cur = conn.cursor()
    uvrs = []
    try:
        cur.execute(
            """
            SELECT DISTINCT uvr FROM cadastros WHERE uvr IS NOT NULL AND uvr <> ''
            UNION
            SELECT DISTINCT uvr_acesso FROM usuarios WHERE uvr_acesso IS NOT NULL AND uvr_acesso <> ''
            ORDER BY uvr
            """
        )
        uvrs = [linha[0] for linha in cur.fetchall() if linha[0]]
    except Exception:
        uvrs = []
    finally:
        cur.close()
        conn.close()
    if not uvrs:
        uvrs = ["UVR 01", "UVR 02", "UVR 03"]
    return uvrs

@login_manager.user_loader
def load_user(user_id):
    conn = None
    cur = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute("SELECT id, username, role, uvr_acesso FROM usuarios WHERE id = %s", (user_id,))
        data = cur.fetchone()
        if data:
            return User(id=data[0], username=data[1], role=data[2], uvr_acesso=data[3])
    except Exception as exc:
        app.logger.error(f"Erro ao carregar usuário da sessão: {exc}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
    return None


def buscar_usuario_para_login(cur, username):
    consultas = [
        (
            """
            SELECT id, username, password_hash, role, uvr_acesso
            FROM usuarios
            WHERE LOWER(username) = LOWER(%s) AND ativo = TRUE
            """,
            True,
        ),
        (
            """
            SELECT id, username, password_hash, role, NULL AS uvr_acesso
            FROM usuarios
            WHERE LOWER(username) = LOWER(%s) AND ativo = TRUE
            """,
            False,
        ),
        (
            """
            SELECT id, username, password_hash, role, uvr_acesso
            FROM usuarios
            WHERE LOWER(username) = LOWER(%s)
            """,
            True,
        ),
        (
            """
            SELECT id, username, password_hash, role, NULL AS uvr_acesso
            FROM usuarios
            WHERE LOWER(username) = LOWER(%s)
            """,
            False,
        ),
    ]

    ultimo_erro = None
    for sql, _ in consultas:
        try:
            cur.execute(sql, (username,))
            return cur.fetchone()
        except psycopg2.errors.UndefinedColumn as exc:
            ultimo_erro = exc
            cur.connection.rollback()
            continue

    if ultimo_erro:
        raise ultimo_erro
    return None


def buscar_usuario_para_recuperacao(cur, email):
    consultas = [
        (
            """
            SELECT id, username, email
            FROM usuarios
            WHERE LOWER(email) = LOWER(%s) AND ativo = TRUE
            """,
            True,
        ),
        (
            """
            SELECT id, username, email
            FROM usuarios
            WHERE LOWER(email) = LOWER(%s)
            """,
            False,
        ),
    ]

    ultimo_erro = None
    for sql, _ in consultas:
        try:
            cur.execute(sql, (email,))
            return cur.fetchone()
        except psycopg2.errors.UndefinedColumn as exc:
            ultimo_erro = exc
            cur.connection.rollback()
            continue

    if ultimo_erro:
        raise ultimo_erro
    return None


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        password = request.form.get('password', '')

        if not username or not password:
            return render_template('login.html', erro="Usuário e senha são obrigatórios.")

        conn = None
        cur = None
        try:
            conn = conectar_banco()
            cur = conn.cursor()
            user_data = buscar_usuario_para_login(cur, username)
        except Exception as exc:
            app.logger.error(f"Erro no login para usuário '{username}': {exc}")
            return render_template(
                'login.html',
                erro="Não foi possível acessar o sistema agora. Tente novamente em instantes.",
            )
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

        if user_data:


            if check_password_hash(user_data[2], password):
                user_obj = User(id=user_data[0], username=user_data[1], role=user_data[3], uvr_acesso=user_data[4])
                login_user(user_obj)
                app.logger.info(f"Usuário {username} logado com sucesso.")
                return redirect(url_for('index'))
            return render_template('login.html', erro="Senha incorreta.")

        return render_template('login.html', erro="Usuário não encontrado.")
            
    return render_template('login.html')


@app.route('/recuperar_senha', methods=['GET', 'POST'])
def recuperar_senha():
    mensagem = None
    erro = None

    if request.method == 'POST':
        email = request.form.get('email', '').strip()
        if not email:
            erro = "Informe o e-mail cadastrado."
            return render_template('recuperar_senha.html', erro=erro, mensagem=mensagem)

        conn = None
        cur = None
        user_data = None
        try:
            conn = conectar_banco()
            cur = conn.cursor()
            user_data = buscar_usuario_para_recuperacao(cur, email)

            if user_data:
                token = secrets.token_urlsafe(32)
                expira_em = datetime.utcnow() + timedelta(hours=1)
                cur.execute(
                    """
                    UPDATE usuarios
                    SET reset_token = %s, reset_token_expira = %s
                    WHERE id = %s
                    """,
                    (token, expira_em, user_data[0]),
                )
                conn.commit()

                base_url = os.getenv("APP_BASE_URL", request.url_root).rstrip("/")
                link = f"{base_url}{url_for('redefinir_senha', token=token)}"
                corpo = (
                    f"Olá, {user_data[1]}!\n\n"
                    "Recebemos uma solicitação para redefinir sua senha. "
                    "Clique no link abaixo para criar uma nova senha:\n\n"
                    f"{link}\n\n"
                    "Se você não solicitou esta recuperação, ignore este e-mail."
                )
                ok, retorno = enviar_email_recuperacao(
                    destinatario=user_data[2],
                    assunto="Recuperação de senha - Sistema Recic3",
                    corpo=corpo,
                )
                if ok:
                    mensagem = "Se o e-mail estiver cadastrado, você receberá as instruções em instantes."
                else:
                    mensagem = "Se o e-mail estiver cadastrado, você receberá as instruções em instantes."
                    if "Configuração de e-mail incompleta" not in retorno:
                        erro = retorno
            else:
                mensagem = "Se o e-mail estiver cadastrado, você receberá as instruções em instantes."

        except Exception as exc:
            app.logger.error("Erro ao iniciar recuperação de senha para '%s': %s", email, exc)
            erro = "Não foi possível processar sua solicitação agora. Tente novamente em instantes."
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

    return render_template('recuperar_senha.html', erro=erro, mensagem=mensagem)



@app.route('/redefinir_senha/<token>', methods=['GET', 'POST'])
def redefinir_senha(token):
    conn = conectar_banco()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, username, reset_token_expira
        FROM usuarios
        WHERE reset_token = %s
        """,
        (token,),
    )
    user_data = cur.fetchone()

    if not user_data or not user_data[2] or user_data[2] < datetime.utcnow():
        cur.close()
        conn.close()
        return render_template('redefinir_senha.html', erro="Token inválido ou expirado.")

    if request.method == 'POST':
        nova_senha = request.form.get('nova_senha')
        confirmar_senha = request.form.get('confirmar_senha')

        if not nova_senha or not confirmar_senha:
            cur.close()
            conn.close()
            return render_template('redefinir_senha.html', erro="Preencha todos os campos.")

        if nova_senha != confirmar_senha:
            cur.close()
            conn.close()
            return render_template('redefinir_senha.html', erro="As senhas não conferem.")

        if len(nova_senha) < 6:
            cur.close()
            conn.close()
            return render_template('redefinir_senha.html', erro="A senha deve ter pelo menos 6 caracteres.")

        novo_hash = generate_password_hash(nova_senha)
        cur.execute(
            """
            UPDATE usuarios
            SET password_hash = %s, reset_token = NULL, reset_token_expira = NULL
            WHERE id = %s
            """,
            (novo_hash, user_data[0]),
        )
        conn.commit()
        cur.close()
        conn.close()
        return render_template('redefinir_senha.html', sucesso="Senha redefinida com sucesso! Faça login.")

    cur.close()
    conn.close()
    return render_template('redefinir_senha.html')

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


@app.route('/admin/usuarios', methods=['GET', 'POST'])
@login_required
def admin_usuarios():
    bloqueio = exigir_admin()
    if bloqueio:
        return bloqueio

    mensagem = None
    erro = None

    if request.method == 'POST':
        username = request.form.get('username', '').strip()
        nome_completo = request.form.get('nome_completo', '').strip()
        email = request.form.get('email', '').strip()
        role = request.form.get('role', '').strip().lower()
        uvr_acesso = request.form.get('uvr_acesso', '').strip()
        senha = request.form.get('senha', '').strip()

        if not username or not senha or not role:
            erro = "Usuário, senha e tipo são obrigatórios."
        elif role not in {"uvr", "visitante"}:
            erro = "Tipo de usuário inválido."
        elif role == "uvr" and not uvr_acesso:
            erro = "Informe a UVR de acesso."
        elif not email:
            erro = "Informe o e-mail para recuperação."
        else:
            conn = conectar_banco()
            cur = conn.cursor()
            try:
                senha_hash = generate_password_hash(senha)
                cur.execute(
                    """
                    INSERT INTO usuarios (username, password_hash, nome_completo, role, uvr_acesso, ativo, email)
                    VALUES (%s, %s, %s, %s, %s, TRUE, %s)
                    """,
                    (username, senha_hash, nome_completo, role, uvr_acesso or None, email),
                )
                conn.commit()
                mensagem = "Usuário criado com sucesso."
            except psycopg2.Error as e:
                conn.rollback()
                erro = f"Erro ao criar usuário: {e.pgerror or e}"
            finally:
                cur.close()
                conn.close()

    uvrs = obter_uvrs_existentes()

    conn = conectar_banco()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(
        """
        SELECT id, username, nome_completo, role, uvr_acesso, ativo, email
        FROM usuarios
        ORDER BY id
        """
    )
    usuarios = cur.fetchall()
    cur.close()
    conn.close()

    return renderizar_template_com_fallback(
        'admin_usuarios.html',
        usuarios=usuarios,
        uvrs=uvrs,
        mensagem=mensagem,
        erro=erro,
    )


@app.route('/admin/usuarios/<int:user_id>/editar', methods=['GET', 'POST'])
@login_required
def admin_editar_usuario(user_id):
    bloqueio = exigir_admin()
    if bloqueio:
        return bloqueio

    conn = conectar_banco()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    uvrs = obter_uvrs_existentes()
    cur.execute(
        """
        SELECT id, username, nome_completo, role, uvr_acesso, ativo, email
        FROM usuarios
        WHERE id = %s
        """,
        (user_id,),
    )
    usuario = cur.fetchone()

    if not usuario:
        cur.close()
        conn.close()
        return "Usuário não encontrado.", 404

    mensagem = None
    erro = None

    if request.method == 'POST':
        nome_completo = request.form.get('nome_completo', '').strip()
        email = request.form.get('email', '').strip()
        role = request.form.get('role', '').strip().lower()
        uvr_acesso = request.form.get('uvr_acesso', '').strip()
        senha = request.form.get('senha', '').strip()
        ativo = request.form.get('ativo') == 'on'

        if role not in {"admin", "uvr", "visitante"}:
            erro = "Tipo de usuário inválido."
        elif role == "uvr" and not uvr_acesso:
            erro = "Informe a UVR de acesso."
        elif not email:
            erro = "Informe o e-mail para recuperação."
        else:
            try:
                if senha:
                    senha_hash = generate_password_hash(senha)
                    cur.execute(
                        """
                        UPDATE usuarios
                        SET nome_completo = %s, role = %s, uvr_acesso = %s, ativo = %s, email = %s, password_hash = %s
                        WHERE id = %s
                        """,
                        (nome_completo, role, uvr_acesso or None, ativo, email, senha_hash, user_id),
                    )
                else:
                    cur.execute(
                        """
                        UPDATE usuarios
                        SET nome_completo = %s, role = %s, uvr_acesso = %s, ativo = %s, email = %s
                        WHERE id = %s
                        """,
                        (nome_completo, role, uvr_acesso or None, ativo, email, user_id),
                    )
                conn.commit()
                mensagem = "Usuário atualizado com sucesso."
                cur.execute(
                    """
                    SELECT id, username, nome_completo, role, uvr_acesso, ativo, email
                    FROM usuarios
                    WHERE id = %s
                    """,
                    (user_id,),
                )
                usuario = cur.fetchone()
            except psycopg2.Error as e:
                conn.rollback()
                erro = f"Erro ao atualizar usuário: {e.pgerror or e}"

    cur.close()
    conn.close()

    return renderizar_template_com_fallback(
        'admin_usuario_editar.html',
        usuario=usuario,
        uvrs=uvrs,
        mensagem=mensagem,
        erro=erro,
    )

@app.route("/", methods=["GET"])
@login_required  # <--- ADICIONE ISSO: Protege a rota
def index():
    """Renderiza a página principal com os formulários."""
    # Passamos o 'current_user' para o HTML saber quem está logado
    status_resumo = None
    conn = conectar_banco()
    cursor = conn.cursor()
    try:
        if current_user.role != 'admin':
            cursor.execute("""
                SELECT status, COUNT(*) 
                FROM documentos
                WHERE uvr = %s AND status IN ('Aprovado', 'Reprovado')
                GROUP BY status
            """, (current_user.uvr_acesso,))
            resultados = cursor.fetchall()
            aprovados = 0
            reprovados = 0
            for status, total in resultados:
                if status == 'Aprovado':
                    aprovados = total
                elif status == 'Reprovado':
                    reprovados = total
            if aprovados or reprovados:
                status_resumo = {
                    "aprovados": aprovados,
                    "reprovados": reprovados,
                }
        else:
            cursor.execute("""
                SELECT COUNT(*)
                FROM documentos
                WHERE status = 'Pendente'
            """)
            pendentes = cursor.fetchone()[0]
            if pendentes:
                status_resumo = {
                    "pendentes": pendentes,
                }
    except Exception as e:
        app.logger.error(f"Erro ao buscar resumo de documentos: {e}")
    finally:
        cursor.close()
        conn.close()

    return render_template("cadastro.html", usuario=current_user, status_resumo=status_resumo)

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
        
        # --- CORREÇÃO PARA ADMIN PODER ESCOLHER A UVR ---
        # Se NÃO for admin, forçamos a UVR do usuário logado.
        # Se FOR admin, ele usa o que veio do formulário (dados["uvr"]).
        if current_user.role != 'admin':
            if current_user.uvr_acesso:
                dados["uvr"] = current_user.uvr_acesso
        # -----------------------------------------------

        # --- CORREÇÃO: TIPO DE ATIVIDADE AUTOMÁTICO ---
        if not dados.get("tipo_atividade"):
            dados["tipo_atividade"] = "Não Informado"

        # --- LISTA DE CAMPOS OBRIGATÓRIOS ---
        required_fields = { 
            "razao_social": "Razão Social", 
            "cnpj": "CNPJ", 
            "cep": "CEP",
            "uvr": "UVR", # Agora o Admin é obrigado a selecionar
            "data_hora_cadastro": "Data/Hora", 
            "tipo_cadastro": "Tipo de Cadastro"
        }
        
        for field, msg in required_fields.items():
            if not dados.get(field): return f"{msg} é obrigatório(a).", 400

        # --- VALIDAÇÕES DE FORMATO ---
        cnpj_num = re.sub(r'[^0-9]', '', dados["cnpj"])
        # if not validar_cnpj(cnpj_num): return "CNPJ inválido.", 400 # Verifique se essa função existe
        
        cep_num = re.sub(r'[^0-9]', '', dados["cep"])
        # if not validar_cep(cep_num): return "CEP inválido.", 400 # Verifique se essa função existe

        try:
            data_hora = datetime.strptime(dados["data_hora_cadastro"], '%d/%m/%Y %H:%M:%S')
        except ValueError:
            return "Formato de Data/Hora do Cadastro inválido. Use DD/MM/AAAA HH:MM:SS", 400

        # --- INSERÇÃO NO BANCO ---
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
        # Ajuste para a mensagem de erro amigável
        if 'cnpj' in str(e).lower(): 
            return "Este CNPJ já está cadastrado nesta UVR.", 400
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

        # Campos obrigatórios (Adicionei "funcao" aqui)
        required_fields = { 
            "nome": "Nome", "cpf": "CPF", "rg": "RG",
            "data_nascimento": "Data de Nascimento", "data_admissao": "Data de Admissão",
            "status": "Status", "cep": "CEP", "telefone": "Telefone",
            "uvr": "UVR", "data_hora_cadastro": "Data/Hora",
            "funcao": "Função" 
        }
        
        for field, msg in required_fields.items():
            if not dados.get(field): return f"{msg} é obrigatório(a).", 400

        # Validações de máscara e formato
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

        # --- LÓGICA DE FOTO INTELIGENTE (REVISADA) ---
        foto_final = ""
        
        # 1. Prioridade: Verifica se o JavaScript enviou a foto processada
        foto_processada = dados.get("foto_base64", "")
        
        if foto_processada and len(foto_processada) > 100:
            foto_final = _upload_base64_to_cloudinary(
                foto_processada,
                folder="associados",
            ) or foto_processada

            foto_final = foto_processada

        
        # 2. Backup: Se o JS falhar e enviar arquivo bruto
        elif 'foto' in request.files:
            arquivo = request.files['foto']
            if arquivo and arquivo.filename:
                foto_final = _upload_file_to_cloudinary(
                    arquivo,
                    folder="associados",
                    resource_type="image",
                )
                if not foto_final:
                    conteudo_arquivo = arquivo.read()
                    encoded_string = base64.b64encode(conteudo_arquivo).decode('utf-8')
                    mime_type = arquivo.content_type or "image/jpeg"
                    foto_final = f"data:{mime_type};base64,{encoded_string}"
        # -----------------------------------------------

        conn = conectar_banco()
        cur = conn.cursor()
        
        # Gera próximo número de associado
        cur.execute("SELECT MAX(CAST(numero AS INTEGER)) FROM associados")
        res_num = cur.fetchone()
        proximo_numero = (res_num[0] + 1) if res_num and res_num[0] else 1
        numero_gerado_str = str(proximo_numero)
        
        # Inserção no Banco de Dados (ATUALIZADO COM FUNÇÃO)
        cur.execute("""
            INSERT INTO associados (
                numero, uvr, associacao, nome, cpf, rg, data_nascimento,
                data_admissao, status, cep, logradouro, endereco_numero,
                bairro, cidade, uf, telefone, data_hora_cadastro, foto_base64, 
                funcao
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            numero_gerado_str, dados["uvr"], dados.get("associacao",""), dados["nome"],
            cpf_num, dados["rg"], data_nascimento, data_admissao, dados["status"],
            cep_num, dados.get("logradouro", ""), dados.get("endereco_numero", ""), 
            dados.get("bairro", ""), dados.get("cidade", ""), dados.get("uf", ""), 
            dados["telefone"], data_hora, foto_final, 
            dados.get("funcao")
        ))
        
        conn.commit()
        return redirect(url_for("sucesso_associado"))

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro cadastro associado: {e}")
        return f"Erro interno: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/buscar_associados", methods=["GET"])
@login_required
def buscar_associados():
    # Coleta os parâmetros da URL
    termo = request.args.get("q", "").lower()
    status_filtro = request.args.get("status", "")
    
    # Suporte para nomes variados de parametros de data (para garantir compatibilidade)
    data_ini = request.args.get("data_inicial", "") or request.args.get("data_ini", "")
    data_fim = request.args.get("data_final", "") or request.args.get("data_fim", "")
    
    # Filtro de UVR vindo da tela (apenas Admin usa isso)
    uvr_filtro_tela = request.args.get("uvr", "")

    # --- NOVO: Filtro de Função ---
    funcao_filtro = request.args.get("funcao", "")

    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # SQL Base - ADICIONADO A COLUNA 'funcao' (índice 7)
        sql = "SELECT id, nome, cpf, uvr, status, associacao, data_admissao, funcao FROM associados WHERE 1=1"
        params = []
        
        # --- LÓGICA DE SEGURANÇA E FILTRO DE UVR ---
        if current_user.role == 'admin':
            # Se for Admin, ele PODE filtrar por UVR se quiser
            if uvr_filtro_tela and uvr_filtro_tela != "Todas":
                sql += " AND uvr = %s"
                params.append(uvr_filtro_tela)
        elif current_user.uvr_acesso:
            # Se NÃO for admin (e tiver UVR definida), FORÇA a UVR dele
            # Ignora totalmente o que veio da tela
            sql += " AND uvr = %s"
            params.append(current_user.uvr_acesso)
        
        # --- FILTRO DE ASSOCIAÇÃO (SE NÃO FOR ADMIN) ---
        # Adicionado para garantir que o usuário só veja da sua associação se aplicável
        if current_user.role != 'admin' and getattr(current_user, 'associacao', None):
             sql += " AND associacao = %s"
             params.append(current_user.associacao)

        # 1. Filtro de Texto (Nome ou CPF)
        if termo:
            sql += " AND (LOWER(nome) LIKE %s OR cpf LIKE %s)"
            params.append(f"%{termo}%")
            params.append(f"%{termo}%")
            
        # 2. Filtro de Status
        if status_filtro and status_filtro != "Todos":
            sql += " AND status = %s"
            params.append(status_filtro)
        
        # 3. Filtro de Função (NOVO)
        if funcao_filtro and funcao_filtro != "Todas" and funcao_filtro != "Todas as Funções":
            sql += " AND funcao = %s"
            params.append(funcao_filtro)

        # 4. Filtro de Data Inicial
        if data_ini:
            sql += " AND data_admissao >= %s"
            params.append(data_ini)
            
        # 5. Filtro de Data Final
        if data_fim:
            sql += " AND data_admissao <= %s"
            params.append(data_fim)

        # Ordenação
        sql += " ORDER BY nome ASC LIMIT 50"
        
        cur.execute(sql, tuple(params))
        resultados = cur.fetchall()
        
        lista_associados = []
        for row in resultados:
            # Tratamento da data de admissão
            data_adm_str = ""
            if row[6]: data_adm_str = row[6].strftime('%d/%m/%Y') # Formatado para BR

            lista_associados.append({
                "id": row[0],
                "nome": row[1],
                "cpf": row[2],
                "uvr": row[3],
                "status": row[4],
                "associacao": row[5],
                "data_admissao": data_adm_str,
                "funcao": row[7] if row[7] else "" # Adicionado o campo função
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
        # Usamos o RealDictCursor para o JS receber 'data.nome' e não 'data[1]'
        cur = conn.cursor(cursor_factory=RealDictCursor)
        
        cur.execute("SELECT * FROM associados WHERE id = %s", (id,))
        row = cur.fetchone()
        
        if not row:
            return jsonify({"error": "Associado não encontrado"}), 404

        # --- TRATAMENTO DE DATAS ---
        def fmt_iso(d):
            return d.strftime('%Y-%m-%d') if hasattr(d, 'strftime') else str(d) if d else ""

        def fmt_br_completo(d):
            return d.strftime('%d/%m/%Y %H:%M') if hasattr(d, 'strftime') else str(d) if d else ""

        # --- TRATAMENTO DA FOTO ---
        foto = row.get('foto_base64') or ""
        if (
            foto
            and len(foto) > 100
            and not foto.startswith('data:image')
            and not foto.startswith('http')
        ):
            foto = f"data:image/jpeg;base64,{foto}"

        # Montamos o retorno
        res = {
            "id": row['id'],
            "nome": row['nome'],
            "cpf": row['cpf'],
            "rg": row.get('rg', ''),
            "data_nascimento": fmt_iso(row['data_nascimento']),
            "data_admissao": fmt_iso(row['data_admissao']),
            "status": row['status'],
            "uvr": row['uvr'],
            "associacao": row.get('associacao', ''), # <--- ADICIONADO (IMPORTANTE PARA EDIÇÃO)
            "cep": row.get('cep', ''),               # <--- ADICIONADO (IMPORTANTE PARA EDIÇÃO)
            "logradouro": row['logradouro'],
            "numero": row.get('endereco_numero', ''),
            "endereco_numero": row.get('endereco_numero', ''),
            "bairro": row['bairro'],
            "cidade": row['cidade'],
            "uf": row['uf'],
            "telefone": row['telefone'],
            "foto_base64": foto,
            "data_cadastro": fmt_br_completo(row.get('data_hora_cadastro')),
            "funcao": row.get('funcao', '')  # <--- O CAMPO QUE FALTAVA
        }
        return jsonify(res)

    except Exception as e:
        app.logger.error(f"Erro ao buscar associado {id}: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()
        
@app.route("/editar_associado", methods=["GET", "POST"])
@login_required
def editar_associado():
    if request.method == "GET":
        return redirect(url_for('index'))
    
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    
    conn = None
    try:
        dados = request.form.to_dict()
        id_associado = dados.get("id_associado")
        if not id_associado: return "ID não encontrado.", 400

        # Tratamento de dados
        cpf_num = re.sub(r'[^0-9]', '', dados.get("cpf", ""))
        cep_num = re.sub(r'[^0-9]', '', dados.get("cep", ""))
        funcao = dados.get("funcao") # <--- Recupera a função do formulário
        
        def processar_data(d):
            if not d: return None
            try: return datetime.strptime(d, '%Y-%m-%d').date()
            except: return None

        data_nasc = processar_data(dados.get("data_nascimento"))
        data_adm = processar_data(dados.get("data_admissao"))

        # --- LÓGICA DE FOTO NA EDIÇÃO ---
        foto_final = dados.get("foto_base64", "")
        
        if foto_final and len(foto_final) > 100:
            foto_final = _upload_base64_to_cloudinary(
                foto_final,
                folder="associados",
            ) or foto_final
        else:
            if 'foto' in request.files:
                arquivo = request.files['foto']
                if arquivo and arquivo.filename:
                    foto_final = _upload_file_to_cloudinary(
                        arquivo,
                        folder="associados",
                        resource_type="image",
                    )
                    if not foto_final:
                        conteudo = arquivo.read()
                        encoded = base64.b64encode(conteudo).decode('utf-8')
                        mime = arquivo.content_type or "image/jpeg"
                        foto_final = f"data:{mime};base64,{encoded}"
        
        # --------------------------------------------

        conn = conectar_banco()
        cur = conn.cursor()

        if current_user.role == 'admin':
            # ADICIONADO: funcao=%s no SQL e a variável 'funcao' nos valores
            cur.execute("""
                UPDATE associados SET 
                    nome=%s, cpf=%s, rg=%s, data_nascimento=%s, data_admissao=%s,
                    status=%s, uvr=%s, associacao=%s, cep=%s, logradouro=%s,
                    endereco_numero=%s, bairro=%s, cidade=%s, uf=%s, telefone=%s,
                    foto_base64=%s, funcao=%s
                WHERE id=%s
            """, (
                dados["nome"], cpf_num, dados["rg"], data_nasc, data_adm,
                dados["status"], dados["uvr"], dados.get("associacao", ""), cep_num,
                dados.get("logradouro", ""), dados.get("endereco_numero", ""),
                dados.get("bairro", ""), dados.get("cidade", ""), dados.get("uf"),
                dados["telefone"], foto_final, funcao, int(id_associado)
            ))
            conn.commit()
            msg = "Alterações salvas com sucesso!"
        else:
            # Lógica para Usuário Comum (Solicitação de Alteração)
            import json
            dados_json = dados.copy()
            dados_json['foto_base64'] = foto_final 
            
            # Formata datas para string para o JSON não quebrar
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
        app.logger.error(f"Erro edição associado: {e}")
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
# --- ROTA: CADASTRO DE EPIs (COM FUNÇÕES) ---
# --- ROTA: CADASTRAR NOVO TIPO DE EPI (COM CONTROLE DE UVR) ---
@app.route("/cadastrar_epi", methods=["GET", "POST"])
@login_required
def cadastrar_epi():
    # Define a UVR padrão do usuário logado
    uvr_usuario = current_user.uvr_acesso if current_user.uvr_acesso else "GERAL"

    # --- SE FOR GET (Abrir a página) ---
    if request.method == "GET":
        return render_template("cadastrar_epi.html", usuario=current_user)

    # --- SE FOR POST (Salvar) ---
    conn = None
    try:
        dados = request.form
        nome_epi = dados.get("nome_epi", "").strip()
        lista_funcoes = request.form.getlist("funcoes") 
        
        # LÓGICA DE UVR: 
        # Se for ADMIN, ele pode ter escolhido uma UVR específica no select do formulário.
        # Se não for ADMIN ou não escolheu, usa a UVR do próprio usuário logado.
        if current_user.role == 'admin' and dados.get("uvr_cadastro"):
            uvr_para_gravar = dados.get("uvr_cadastro")
        else:
            uvr_para_gravar = uvr_usuario

        # Validações básicas
        if not nome_epi:
            flash("Nome do EPI é obrigatório.", "error")
            return redirect(url_for('cadastrar_epi'))
            
        if not lista_funcoes:
            flash("Selecione pelo menos uma Função Recomendada.", "warning")
            return redirect(url_for('cadastrar_epi'))

        # Transforma a lista de checkboxes em texto único (Ex: "Coletor / Triador")
        funcao_texto = " / ".join(lista_funcoes)

        # Lógica de Data e Validade
        data_hora_cadastro = datetime.now()
        validade_meses = dados.get("validade_meses_epi")
        validade_meses_int = None
        
        if validade_meses and validade_meses.strip():
            try:
                validade_meses_int = int(validade_meses)
            except ValueError:
                flash("Validade deve ser um número inteiro.", "error")
                return redirect(url_for('cadastrar_epi'))

        conn = conectar_banco()
        cur = conn.cursor()
        
        # INSERT incluindo a coluna 'uvr' para segmentar o catálogo
        cur.execute("""
            INSERT INTO epi_itens (nome, categoria, ca, validade_meses, funcao_indicada, uvr, data_hora_cadastro)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            nome_epi,
            dados.get("categoria_epi", "").strip() or None,
            dados.get("ca_epi", "").strip() or None,
            validade_meses_int,
            funcao_texto,
            uvr_para_gravar, # Grava a UVR (escolhida pelo admin ou automática do usuário)
            data_hora_cadastro
        ))
        
        conn.commit()
        flash(f"EPI '{nome_epi}' cadastrado com sucesso para {uvr_para_gravar}!", "success")
        return redirect(url_for("cadastrar_epi"))

    except psycopg2.IntegrityError as e:
        if conn: conn.rollback()
        # Caso tente cadastrar o mesmo nome na mesma UVR (se houver essa restrição unique)
        if 'unique constraint' in str(e).lower():
            flash("Este nome de EPI já está cadastrado nesta unidade!", "warning")
        else:
            flash(f"Erro de integridade no banco: {e}", "error")
        return redirect(url_for('cadastrar_epi'))
        
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao cadastrar EPI: {e}")
        flash(f"Erro inesperado: {e}", "error")
        return redirect(url_for('cadastrar_epi'))
        
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
        
        # Selecionamos os campos necessários
        query = "SELECT id, razao_social, tipo_cadastro FROM cadastros"
        conditions = []
        params = []

        # 1. Filtro por UVR (sempre importante para isolar os dados)
        if uvr_filter:
            conditions.append("uvr = %s")
            params.append(uvr_filter)
        
        # 2. LÓGICA INTELIGENTE PARA "AMBOS" E NOMES ANTIGOS
        if tipo_cadastro_filter:
            if tipo_cadastro_filter == 'Comprador':
                # Busca quem é 'Comprador', 'Cliente' ou 'Ambos'
                # O LIKE garante que encontre "Comprador/Algo" se existir
                conditions.append("(tipo_cadastro LIKE %s OR tipo_cadastro LIKE %s OR tipo_cadastro = 'Ambos')")
                params.append('Comprador%')
                params.append('Cliente%')
                
            elif tipo_cadastro_filter == 'Fornecedor':
                # Busca quem é 'Fornecedor', 'Fornecedor/Prestador' ou 'Ambos'
                conditions.append("(tipo_cadastro LIKE %s OR tipo_cadastro = 'Ambos')")
                params.append('Fornecedor%')
                
            else:
                # Caso haja um filtro específico diferente dos acima
                conditions.append("tipo_cadastro = %s")
                params.append(tipo_cadastro_filter)

        # Montagem dinâmica da cláusula WHERE
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
            
        # Ordenação alfabética para facilitar a escolha no dropdown
        query += " ORDER BY razao_social"
        
        cur.execute(query, tuple(params))
        
        # Mapeia os resultados para o formato JSON que o JavaScript espera
        cadastros = [
            {
                "id": row[0], 
                "razao_social": row[1], 
                "tipo_cadastro": row[2]
            } 
            for row in cur.fetchall()
        ]
        
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
        
        # MELHORIA: Adicionei a coluna 'funcao' na busca
        query = """
            SELECT id, nome, funcao 
            FROM associados 
            WHERE uvr = %s AND status = 'Ativo' 
            ORDER BY nome
        """
        cur.execute(query, (uvr_filter,))
        
        # Agora o JSON retorna também a função (ou 'Não informada' se estiver vazio)
        associados = [
            {
                "id": row[0], 
                "nome": row[1], 
                "funcao": row[2] if row[2] else ""
            } 
            for row in cur.fetchall()
        ]
        
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
            "Comercialização de Materiais Recicláveis",      
            "Outras Receitas",
            "Prestação de Serviços e Parcerias",
            "Gestão Associativa"     
        ],
        "Despesa": [
            "Despesas de manutenção", "Operação e Produção", "Gestão Administrativa e Financeira",
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

        # Lista de campos obrigatórios (Data/Hora removida para não travar)
        required_fields = { 
            "uvr_transacao": "UVR", "data_documento_transacao": "Data do Documento", 
            "tipo_transacao": "Tipo (Receita/Despesa)",
            "tipo_atividade_transacao": "Tipo de Atividade"
        }
        
        for field, msg in required_fields.items():
            if not dados.get(field):
                return f"{msg} é obrigatório(a).", 400
        
        tipo_atividade = dados.get("tipo_atividade_transacao")
        id_origem_selecionado = dados.get("fornecedor_prestador_transacao")
        nome_origem_input = dados.get("nome_fornecedor_prestador_transacao", "").strip()
        id_patrimonio_input = dados.get("id_patrimonio_transacao")
        categoria_despesa_patrimonio = dados.get("categoria_despesa_patrimonio")
        medidor_atual_input = dados.get("medidor_atual_transacao")
        tipo_medidor = dados.get("tipo_medidor_transacao")
        id_motorista_input = dados.get("motorista_transacao")
        nome_motorista_input = dados.get("nome_motorista_transacao", "").strip()
        litros_input = dados.get("litros_transacao")
        tipo_combustivel = dados.get("tipo_combustivel_transacao")
        tipo_manutencao = dados.get("tipo_manutencao_transacao")
        garantia_km_input = dados.get("garantia_km_transacao")
        garantia_data_input = dados.get("garantia_data_transacao")
        proxima_revisao_km_input = dados.get("proxima_revisao_km_transacao")
        proxima_revisao_data_input = dados.get("proxima_revisao_data_transacao")

        id_final_origem_fk = None
        nome_final_origem = ""
        id_patrimonio = None
        id_motorista = None
        medidor_atual = None
        litros = None
        garantia_km = None
        garantia_data = None
        proxima_revisao_km = None
        proxima_revisao_data = None

        if id_patrimonio_input and str(id_patrimonio_input).isdigit():
            id_patrimonio = int(id_patrimonio_input)
        if id_motorista_input and str(id_motorista_input).isdigit():
            id_motorista = int(id_motorista_input)
        if medidor_atual_input:
            medidor_atual = Decimal(str(medidor_atual_input).replace(",", "."))
        if litros_input:
            litros = Decimal(str(litros_input).replace(",", "."))
        if garantia_km_input and str(garantia_km_input).isdigit():
            garantia_km = int(garantia_km_input)
        if proxima_revisao_km_input and str(proxima_revisao_km_input).isdigit():
            proxima_revisao_km = int(proxima_revisao_km_input)
        if garantia_data_input:
            try:
                garantia_data = datetime.strptime(garantia_data_input, '%Y-%m-%d').date()
            except ValueError:
                return "Formato de data inválido para Garantia.", 400
        if proxima_revisao_data_input:
            try:
                proxima_revisao_data = datetime.strptime(proxima_revisao_data_input, '%Y-%m-%d').date()
            except ValueError:
                return "Formato de data inválido para Próxima Revisão.", 400

        if id_patrimonio and medidor_atual is None:
            return "KM/Horímetro atual é obrigatório quando há patrimônio vinculado.", 400

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
            
            # Tratamento da Data/Hora (com fallback se vier vazia)
            data_hora_str = dados.get("data_hora_cadastro_transacao")
            if data_hora_str and data_hora_str.strip():
                # Tenta formato com vírgula (padrão navegador BR) ou sem (padrão Python)
                try:
                    data_hora_registro = datetime.strptime(data_hora_str, '%d/%m/%Y %H:%M:%S')
                except ValueError:
                    try:
                        data_hora_registro = datetime.strptime(data_hora_str, '%d/%m/%Y, %H:%M:%S')
                    except ValueError:
                         data_hora_registro = datetime.now() # Desiste e usa agora
            else:
                data_hora_registro = datetime.now()
                
        except ValueError as e:
            return f"Formato de data inválido: {e}", 400

        arquivo_nf = request.files.get('nota_fiscal_upload')
        if not arquivo_nf or not arquivo_nf.filename:
            return "Anexe a nota fiscal em PDF ou imagem (JPG/PNG).", 400
        extensao_nf = os.path.splitext(arquivo_nf.filename)[1].lower()
        formatos_permitidos = {'.pdf', '.jpg', '.jpeg', '.png'}
        if extensao_nf not in formatos_permitidos:
            return "A nota fiscal deve ser enviada em PDF ou imagem (JPG/PNG).", 400
        arquivo_comprovante = request.files.get('comprovante_pagamento_upload')
        if arquivo_comprovante and arquivo_comprovante.filename:
            extensao_comprovante = os.path.splitext(arquivo_comprovante.filename)[1].lower()
            if extensao_comprovante not in formatos_permitidos:
                return "O comprovante de pagamento deve ser enviado em PDF ou imagem (JPG/PNG).", 400
        arquivo_mtr = request.files.get('mtr_upload')
        exige_mtr = dados.get("tipo_transacao") == "Receita" and dados.get("tipo_atividade_transacao") == "Comercialização de Materiais Recicláveis"
        if exige_mtr and (not arquivo_mtr or not arquivo_mtr.filename):
            return "Anexe o MTR em PDF ou imagem (JPG/PNG).", 400
        if arquivo_mtr and arquivo_mtr.filename:
            extensao_mtr = os.path.splitext(arquivo_mtr.filename)[1].lower()
            if extensao_mtr not in formatos_permitidos:
                return "O MTR deve ser enviado em PDF ou imagem (JPG/PNG).", 400
        arquivo_relatorio_fotografico = request.files.get('relatorio_fotografico_upload')
        exige_relatorio_fotografico = exige_mtr
        if exige_relatorio_fotografico and (not arquivo_relatorio_fotografico or not arquivo_relatorio_fotografico.filename):
            return "Anexe o relatório fotográfico da carga em PDF ou imagem (JPG/PNG).", 400
        if arquivo_relatorio_fotografico and arquivo_relatorio_fotografico.filename:
            extensao_relatorio = os.path.splitext(arquivo_relatorio_fotografico.filename)[1].lower()
            if extensao_relatorio not in formatos_permitidos:
                return "O relatório fotográfico da carga deve ser enviado em PDF ou imagem (JPG/PNG).", 400

        tipo_transacao = dados["tipo_transacao"]
        if dados.get("tipo_atividade_transacao") == "Rateio dos Associados":
            nome_tipo_documento = "Recibos de Rateio"
        else:
            nome_tipo_documento = "Notas Fiscais de Receitas" if tipo_transacao == "Receita" else "Notas Fiscais de Despesas"
        competencia = date(data_documento.year, data_documento.month, 1)
        numero_referencia = dados.get("numero_documento_transacao", "")
        enviado_por = current_user.username if current_user.is_authenticated else "sistema"

        # --- CORREÇÃO AQUI: USANDO O NOME CERTO DA FUNÇÃO ---
        conn = conectar_banco() 
        # ----------------------------------------------------
        
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO transacoes_financeiras
            (uvr, associacao, id_cadastro_origem, nome_cadastro_origem, numero_documento, data_documento,
             tipo_transacao, tipo_atividade, valor_total_documento, data_hora_registro, id_patrimonio, 
             categoria_despesa_patrimonio, medidor_atual, tipo_medidor, id_motorista, nome_motorista,
             litros, tipo_combustivel, tipo_manutencao, garantia_km, garantia_data, proxima_revisao_km,
             proxima_revisao_data, status_pagamento)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id
        """, (
            dados["uvr_transacao"], dados.get("associacao_transacao",""),
            id_final_origem_fk, nome_final_origem,
            dados.get("numero_documento_transacao", ""), data_documento,
            dados["tipo_transacao"], dados["tipo_atividade_transacao"],
            valor_total_documento_calculado, data_hora_registro, id_patrimonio,
            categoria_despesa_patrimonio, medidor_atual, tipo_medidor, id_motorista, nome_motorista_input,
            litros, tipo_combustivel, tipo_manutencao, garantia_km, garantia_data, proxima_revisao_km,
            proxima_revisao_data, 'Aberto' 
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

        cur.execute("SELECT id FROM tipos_documentos WHERE nome = %s", (nome_tipo_documento,))
        tipo_doc = cur.fetchone()
        if not tipo_doc:
            raise ValueError(f"Tipo de documento '{nome_tipo_documento}' não encontrado.")
        id_tipo_documento = tipo_doc[0]

        nome_original = arquivo_nf.filename
        import time
        timestamp = int(time.time())
        extensao = os.path.splitext(nome_original)[1]
        nome_arquivo_salvo = f"nf_transacao_{dados['uvr_transacao']}_{timestamp}{extensao}"
        file_format = extensao.lstrip('.') if extensao else None

        url_cloud = _upload_file_to_cloudinary(
            arquivo_nf,
            folder="documentos",
            public_id=f"nf_transacao_{dados['uvr_transacao']}_{timestamp}",
            resource_type="raw",
            file_format=file_format,
        )
        if url_cloud:
            nome_arquivo_salvo = url_cloud
        else:
            os.makedirs('uploads', exist_ok=True)
            arquivo_nf.save(os.path.join('uploads', nome_arquivo_salvo))

        observacoes_doc = f"Gerado automaticamente da transação #{id_transacao_criada}. Origem: {nome_final_origem}."
        cur.execute("""
            INSERT INTO documentos
            (uvr, id_tipo, caminho_arquivo, nome_original, competencia,
             data_validade, valor, numero_referencia, observacoes,
             enviado_por, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Pendente')
        """, (
            dados["uvr_transacao"], id_tipo_documento, nome_arquivo_salvo, nome_original,
            competencia, None, valor_total_documento_calculado,
            numero_referencia, observacoes_doc, enviado_por
        ))

        if arquivo_comprovante and arquivo_comprovante.filename:
            comprovante_anexo = _preparar_comprovante_pagamento_anexo(
                arquivo_comprovante, {
                    "uvr": dados["uvr_transacao"],
                    "data_documento": dados["data_documento_transacao"],
                    "numero_documento": numero_referencia,
                    "nome_origem": nome_final_origem,
                }, valor_total_documento_calculado, id_transacao_criada, cur
            )
            cur.execute("""
                INSERT INTO documentos
                (uvr, id_tipo, caminho_arquivo, nome_original, competencia,
                 data_validade, valor, numero_referencia, observacoes,
                 enviado_por, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Pendente')
            """, (
                dados["uvr_transacao"],
                comprovante_anexo["id_tipo_documento"],
                comprovante_anexo["caminho_arquivo"],
                comprovante_anexo["nome_original"],
                datetime.strptime(comprovante_anexo["competencia"], '%Y-%m-%d').date(),
                None,
                Decimal(str(comprovante_anexo["valor"])),
                comprovante_anexo["numero_referencia"],
                comprovante_anexo["observacoes"],
                comprovante_anexo["enviado_por"],
            ))

        if arquivo_mtr and arquivo_mtr.filename:
            mtr_anexo = _preparar_mtr_anexo(
                arquivo_mtr,
                {
                    "uvr": dados["uvr_transacao"],
                    "data_documento": dados["data_documento_transacao"],
                    "numero_documento": numero_referencia,
                    "nome_origem": nome_final_origem,
                },
                id_transacao_criada,
                cur
            )
            cur.execute("""
                INSERT INTO documentos
                (uvr, id_tipo, caminho_arquivo, nome_original, competencia,
                 data_validade, valor, numero_referencia, observacoes,
                 enviado_por, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Pendente')
            """, (
                dados["uvr_transacao"],
                mtr_anexo["id_tipo_documento"],
                mtr_anexo["caminho_arquivo"],
                mtr_anexo["nome_original"],
                datetime.strptime(mtr_anexo["competencia"], '%Y-%m-%d').date(),
                None,
                mtr_anexo["valor"],
                mtr_anexo["numero_referencia"],
                mtr_anexo["observacoes"],
                mtr_anexo["enviado_por"],
            ))

        if arquivo_relatorio_fotografico and arquivo_relatorio_fotografico.filename:
            relatorio_anexo = _preparar_relatorio_fotografico_anexo(
                arquivo_relatorio_fotografico,
                {
                    "uvr": dados["uvr_transacao"],
                    "data_documento": dados["data_documento_transacao"],
                    "numero_documento": numero_referencia,
                    "nome_origem": nome_final_origem,
                },
                id_transacao_criada,
                cur
            )
            cur.execute("""
                INSERT INTO documentos
                (uvr, id_tipo, caminho_arquivo, nome_original, competencia,
                 data_validade, valor, numero_referencia, observacoes,
                 enviado_por, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Pendente')
            """, (
                dados["uvr_transacao"],
                relatorio_anexo["id_tipo_documento"],
                relatorio_anexo["caminho_arquivo"],
                relatorio_anexo["nome_original"],
                datetime.strptime(relatorio_anexo["competencia"], '%Y-%m-%d').date(),
                None,
                relatorio_anexo["valor"],
                relatorio_anexo["numero_referencia"],
                relatorio_anexo["observacoes"],
                relatorio_anexo["enviado_por"],
            ))
        conn.commit()
        return redirect(url_for("sucesso_transacao"))
    except psycopg2.Error as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro de DB em /registrar_transacao_financeira: {e}")
        return f"Erro no banco de dados: {e}", 500
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro inesperado em /registrar_transacao_financeira: {e}", exc_info=True)
        return f"Erro ao registrar transação: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/editar_transacao", methods=["POST"])
@login_required
def editar_transacao():
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
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

        tipo_atividade = dados.get("tipo_atividade_transacao")
        id_origem_selecionado = dados.get("fornecedor_prestador_transacao")
        nome_origem_input = dados.get("nome_fornecedor_prestador_transacao", "").strip()
        nome_final_origem = ""

        if tipo_atividade == "Rateio dos Associados":
            if not id_origem_selecionado:
                nome_final_origem = nome_origem_input or "Rateio Geral Associados"
            else:
                if not nome_origem_input:
                    return "Erro: ID de associado selecionado para rateio sem nome correspondente.", 400
                nome_final_origem = nome_origem_input
        else:
            if not id_origem_selecionado:
                return "O campo 'Fornecedor / Prestador / Cliente' é obrigatório.", 400
            if not nome_origem_input:
                return "Nome do 'Fornecedor / Prestador / Cliente' não encontrado.", 400
            nome_final_origem = nome_origem_input

        if not nome_final_origem:
            return "Nome do Fornecedor/Prestador/Cliente/Associado é obrigatório.", 400

        cabecalho = {
            "uvr": dados["uvr_transacao"],
            "associacao": dados.get("associacao_transacao",""),
            "data_documento": dados["data_documento_transacao"],
            "tipo_transacao": dados["tipo_transacao"],
            "tipo_atividade": dados["tipo_atividade_transacao"],
            "numero_documento": dados.get("numero_documento_transacao", ""),
            "id_origem": dados.get("fornecedor_prestador_transacao"),
            "nome_origem": nome_final_origem,
            "id_patrimonio": dados.get("id_patrimonio_transacao"),
            "categoria_despesa_patrimonio": dados.get("categoria_despesa_patrimonio"),
            "medidor_atual": dados.get("medidor_atual_transacao"),
            "tipo_medidor": dados.get("tipo_medidor_transacao"),
            "id_motorista": dados.get("motorista_transacao"),
            "nome_motorista": dados.get("nome_motorista_transacao"),
            "litros": dados.get("litros_transacao"),
            "tipo_combustivel": dados.get("tipo_combustivel_transacao"),
            "tipo_manutencao": dados.get("tipo_manutencao_transacao"),
            "garantia_km": dados.get("garantia_km_transacao"),
            "garantia_data": dados.get("garantia_data_transacao"),
            "proxima_revisao_km": dados.get("proxima_revisao_km_transacao"),
            "proxima_revisao_data": dados.get("proxima_revisao_data_transacao"),
            "valor_total": float(valor_total_novo)
        }

        arquivo_nf = request.files.get('nota_fiscal_upload')
        anexar_documento = bool(arquivo_nf and arquivo_nf.filename)
        if anexar_documento:
            extensao_nf = os.path.splitext(arquivo_nf.filename)[1].lower()
            formatos_permitidos = {'.pdf', '.jpg', '.jpeg', '.png'}
            if extensao_nf not in formatos_permitidos:
                return "A nota fiscal deve ser enviada em PDF ou imagem (JPG/PNG).", 400
        arquivo_comprovante = request.files.get('comprovante_pagamento_upload')
        anexar_comprovante = bool(arquivo_comprovante and arquivo_comprovante.filename)
        if anexar_comprovante:
            extensao_comprovante = os.path.splitext(arquivo_comprovante.filename)[1].lower()
            formatos_permitidos = {'.pdf', '.jpg', '.jpeg', '.png'}
            if extensao_comprovante not in formatos_permitidos:
                return "O comprovante de pagamento deve ser enviado em PDF ou imagem (JPG/PNG).", 400
        arquivo_mtr = request.files.get('mtr_upload')
        anexar_mtr = bool(arquivo_mtr and arquivo_mtr.filename)
        if anexar_mtr:
            extensao_mtr = os.path.splitext(arquivo_mtr.filename)[1].lower()
            formatos_permitidos = {'.pdf', '.jpg', '.jpeg', '.png'}
            if extensao_mtr not in formatos_permitidos:
                return "O MTR deve ser enviado em PDF ou imagem (JPG/PNG).", 400
        arquivo_relatorio_fotografico = request.files.get('relatorio_fotografico_upload')
        anexar_relatorio_fotografico = bool(arquivo_relatorio_fotografico and arquivo_relatorio_fotografico.filename)
        if anexar_relatorio_fotografico:
            extensao_relatorio = os.path.splitext(arquivo_relatorio_fotografico.filename)[1].lower()
            formatos_permitidos = {'.pdf', '.jpg', '.jpeg', '.png'}
            if extensao_relatorio not in formatos_permitidos:
                return "O relatório fotográfico da carga deve ser enviado em PDF ou imagem (JPG/PNG).", 400

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

            id_patrimonio_sql = None
            if cabecalho.get("id_patrimonio") and str(cabecalho["id_patrimonio"]).isdigit():
                id_patrimonio_sql = int(cabecalho["id_patrimonio"])

            id_motorista_sql = None
            if cabecalho.get("id_motorista") and str(cabecalho["id_motorista"]).isdigit():
                id_motorista_sql = int(cabecalho["id_motorista"])

            medidor_atual_sql = None
            if cabecalho.get("medidor_atual"):
                medidor_atual_sql = Decimal(str(cabecalho["medidor_atual"]).replace(",", "."))

            litros_sql = None
            if cabecalho.get("litros"):
                litros_sql = Decimal(str(cabecalho["litros"]).replace(",", "."))

            garantia_km_sql = None
            if cabecalho.get("garantia_km") and str(cabecalho["garantia_km"]).isdigit():
                garantia_km_sql = int(cabecalho["garantia_km"])

            proxima_revisao_km_sql = None
            if cabecalho.get("proxima_revisao_km") and str(cabecalho["proxima_revisao_km"]).isdigit():
                proxima_revisao_km_sql = int(cabecalho["proxima_revisao_km"])

            garantia_data_sql = None
            if cabecalho.get("garantia_data"):
                try:
                    garantia_data_sql = datetime.strptime(cabecalho["garantia_data"], '%Y-%m-%d').date()
                except ValueError:
                    return "Formato de data inválido para Garantia.", 400

            proxima_revisao_data_sql = None
            if cabecalho.get("proxima_revisao_data"):
                try:
                    proxima_revisao_data_sql = datetime.strptime(cabecalho["proxima_revisao_data"], '%Y-%m-%d').date()
                except ValueError:
                    return "Formato de data inválido para Próxima Revisão.", 400

            if id_patrimonio_sql and medidor_atual_sql is None:
                return "KM/Horímetro atual é obrigatório quando há patrimônio vinculado.", 400

            params = [
                cabecalho['uvr'], cabecalho['associacao'], cabecalho['data_documento'],
                cabecalho['tipo_transacao'], cabecalho['tipo_atividade'], cabecalho['numero_documento'],
                id_origem_sql, cabecalho['nome_origem'],
                valor_total_novo, id_patrimonio_sql, cabecalho.get("categoria_despesa_patrimonio"),
                medidor_atual_sql, cabecalho.get("tipo_medidor"), id_motorista_sql, cabecalho.get("nome_motorista"),
                litros_sql, cabecalho.get("tipo_combustivel"), cabecalho.get("tipo_manutencao"), garantia_km_sql,
                garantia_data_sql, proxima_revisao_km_sql, proxima_revisao_data_sql, id_transacao
            ]

            cur.execute("""
                UPDATE transacoes_financeiras SET
                    uvr=%s, associacao=%s, data_documento=%s, tipo_transacao=%s,
                    tipo_atividade=%s, numero_documento=%s, 
                    id_cadastro_origem=%s, nome_cadastro_origem=%s,
                    valor_total_documento=%s, id_patrimonio=%s, categoria_despesa_patrimonio=%s,
                    medidor_atual=%s, tipo_medidor=%s, id_motorista=%s, nome_motorista=%s,
                    litros=%s, tipo_combustivel=%s, tipo_manutencao=%s, garantia_km=%s,
                    garantia_data=%s, proxima_revisao_km=%s, proxima_revisao_data=%s
                WHERE id=%s
            """, tuple(params))

            # Atualiza Itens (Estratégia: Apaga todos antigos e recria os novos)
            cur.execute("DELETE FROM itens_transacao WHERE id_transacao = %s", (id_transacao,))
            for item in itens_processados:
                cur.execute("""
                    INSERT INTO itens_transacao (id_transacao, descricao, unidade, quantidade, valor_unitario, valor_total_item)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (id_transacao, item['descricao'], item['unidade'], item['quantidade'], item['valor_unitario'], item['valor_total_item']))

            if anexar_documento:
                anexo = _preparar_documento_anexo(arquivo_nf, cabecalho, valor_total_novo, id_transacao, cur)
                _substituir_documento_transacao(
                    cur,
                    cabecalho["uvr"],
                    anexo["id_tipo_documento"],
                    anexo["caminho_arquivo"],
                    anexo["nome_original"],
                    datetime.strptime(anexo["competencia"], '%Y-%m-%d').date(),
                    anexo["valor"],
                    anexo["numero_referencia"],
                    anexo["enviado_por"],
                    anexo["observacoes"],
                )

            if anexar_comprovante:
                comprovante_anexo = _preparar_comprovante_pagamento_anexo(
                    arquivo_comprovante, cabecalho, valor_total_novo, id_transacao, cur
                )
                _substituir_documento_transacao(
                    cur,
                    cabecalho["uvr"],
                    comprovante_anexo["id_tipo_documento"],
                    comprovante_anexo["caminho_arquivo"],
                    comprovante_anexo["nome_original"],
                    datetime.strptime(comprovante_anexo["competencia"], '%Y-%m-%d').date(),
                    comprovante_anexo["valor"],
                    comprovante_anexo["numero_referencia"],
                    comprovante_anexo["enviado_por"],
                    comprovante_anexo["observacoes"],
                )

            if anexar_mtr:
                mtr_anexo = _preparar_mtr_anexo(arquivo_mtr, cabecalho, id_transacao, cur)
                _substituir_documento_transacao(
                    cur,
                    cabecalho["uvr"],
                    mtr_anexo["id_tipo_documento"],
                    mtr_anexo["caminho_arquivo"],
                    mtr_anexo["nome_original"],
                    datetime.strptime(mtr_anexo["competencia"], '%Y-%m-%d').date(),
                    mtr_anexo["valor"],
                    mtr_anexo["numero_referencia"],
                    mtr_anexo["enviado_por"],
                    mtr_anexo["observacoes"],
                )

            if anexar_relatorio_fotografico:
                relatorio_anexo = _preparar_relatorio_fotografico_anexo(
                    arquivo_relatorio_fotografico, cabecalho, id_transacao, cur
                )
                _substituir_documento_transacao(
                    cur,
                    cabecalho["uvr"],
                    relatorio_anexo["id_tipo_documento"],
                    relatorio_anexo["caminho_arquivo"],
                    relatorio_anexo["nome_original"],
                    datetime.strptime(relatorio_anexo["competencia"], '%Y-%m-%d').date(),
                    relatorio_anexo["valor"],
                    relatorio_anexo["numero_referencia"],
                    relatorio_anexo["enviado_por"],
                    relatorio_anexo["observacoes"],
                )
            
            conn.commit()
            return redirect(url_for("sucesso_transacao")) # Reutiliza página de sucesso

        else:
            # USUÁRIO COMUM: Cria solicitação de alteração
            cabecalho['itens'] = itens_processados
            cabecalho['descricao_visual'] = f"Edição NF {cabecalho['numero_documento']} - {cabecalho['nome_origem']}"
            
            if anexar_documento:
                cabecalho["documento_anexo"] = _preparar_documento_anexo(
                    arquivo_nf, cabecalho, valor_total_novo, id_transacao, cur
                )

            if anexar_comprovante:
                cabecalho["comprovante_pagamento_anexo"] = _preparar_comprovante_pagamento_anexo(
                    arquivo_comprovante, cabecalho, valor_total_novo, id_transacao, cur
                )

            if anexar_mtr:
                cabecalho["mtr_anexo"] = _preparar_mtr_anexo(arquivo_mtr, cabecalho, id_transacao, cur)

            if anexar_relatorio_fotografico:
                cabecalho["relatorio_fotografico_anexo"] = _preparar_relatorio_fotografico_anexo(
                    arquivo_relatorio_fotografico, cabecalho, id_transacao, cur
                )
            
            
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
@login_required
def get_clientes_fornecedores_com_pendencias():
    uvr = request.args.get("uvr")
    tipo_movimentacao = request.args.get("tipo_movimentacao")
    data_inicial = request.args.get("data_inicial")
    data_final = request.args.get("data_final")
    
    if not uvr or not data_inicial or not data_final:
        return jsonify({"error": "UVR e Datas são obrigatórias"}), 400

    tipo_transacao_alvo = 'Receita' if tipo_movimentacao == 'Recebimento' else 'Despesa'

    conn = conectar_banco()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    
    lista_final = {} # Usaremos Dicionário para evitar duplicatas e guardar ID+Nome

    try:
        # 1️⃣ BUSCA FINANCEIRA
        # Agora buscamos o ID também (MAX(c.id))
        query = """
            SELECT 
                TRIM(COALESCE(c.razao_social, tf.nome_cadastro_origem)) as nome,
                MAX(c.id) as id_real
            FROM transacoes_financeiras tf
            LEFT JOIN cadastros c ON tf.id_cadastro_origem = c.id
            WHERE tf.uvr = %s
              AND tf.tipo_transacao = %s
              AND tf.status_pagamento <> 'Liquidado'
              AND tf.data_documento BETWEEN %s AND %s
            GROUP BY 1
            HAVING SUM(tf.valor_total_documento - tf.valor_pago_recebido) > 0.00
        """
        cur.execute(query, (uvr, tipo_transacao_alvo, data_inicial, data_final))
        
        for row in cur.fetchall():
            if row['nome']:
                # Usa o ID real se tiver, senão usa o nome como ID provisório
                id_uso = row['id_real'] if row['id_real'] else row['nome']
                lista_final[row['nome']] = id_uso

        # 2️⃣ BUSCA DOCUMENTOS PENDENTES
        try:
            cur.execute("SELECT DISTINCT TRIM(categoria) as nome_doc FROM documentos WHERE uvr = %s AND status = 'Pendente'", (uvr,))
            for row in cur.fetchall():
                nome = row['nome_doc']
                if nome and nome not in lista_final:
                    # Tenta achar o ID desse nome na tabela de cadastros para ajudar
                    cur.execute("SELECT id FROM cadastros WHERE razao_social = %s LIMIT 1", (nome,))
                    res_id = cur.fetchone()
                    id_uso = res_id['id'] if res_id else nome
                    lista_final[nome] = id_uso
        except: pass

        # Monta resultado
        results = []
        for nome in sorted(lista_final.keys()):
            results.append({
                "id": lista_final[nome], # Aqui vai o ID numérico OU o Nome (se não tiver cadastro)
                "razao_social": nome, 
                "tipo_cadastro": "Automático", 
                "is_associado_rateio": False
            })

        return jsonify(results)
    except Exception as e:
        app.logger.error(f"Erro pendencias: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route("/get_notas_em_aberto")
def get_notas_em_aberto():
    uvr = request.args.get("uvr")
    identificador = request.args.get("id_cadastro_cf") # Pode ser ID (int) ou NOME (str)
    tipo_movimentacao = request.args.get("tipo_movimentacao") 
    data_inicial = request.args.get("data_inicial")
    data_final = request.args.get("data_final")

    if not all([uvr, identificador, tipo_movimentacao]):
        return jsonify({"error": "Parâmetros obrigatórios faltando"}), 400

    tipo_transacao_filtro = "Receita" if tipo_movimentacao == "Recebimento" else "Despesa"
    
    conn = conectar_banco()
    cur = conn.cursor()

    try:
        # Base da Query
        sql = """
            SELECT tf.id, tf.numero_documento, tf.data_documento, 
                   tf.valor_total_documento, tf.valor_pago_recebido,
                   (tf.valor_total_documento - tf.valor_pago_recebido) as valor_pendente
            FROM transacoes_financeiras tf
            LEFT JOIN cadastros c ON tf.id_cadastro_origem = c.id
            WHERE tf.uvr = %s 
              AND tf.tipo_transacao = %s 
              AND tf.status_pagamento <> 'Liquidado'
        """
        params = [uvr, tipo_transacao_filtro]

        # --- CORREÇÃO PRINCIPAL AQUI ---
        # Verifica se o 'identificador' é um número (ID) ou texto (Nome)
        if identificador.isdigit():
            # É ID: Busca pela coluna de ID
            sql += " AND tf.id_cadastro_origem = %s"
            params.append(int(identificador))
        else:
            # É TEXTO: Busca pelo Nome (na transação ou no cadastro vinculado)
            sql += " AND (tf.nome_cadastro_origem = %s OR c.razao_social = %s)"
            params.append(identificador)
            params.append(identificador)
        
        # Filtro de datas (se houver)
        if data_inicial and data_final:
            sql += " AND tf.data_documento BETWEEN %s AND %s"
            params.append(data_inicial)
            params.append(data_final)
        
        sql += " ORDER BY tf.data_documento, tf.numero_documento"
        
        cur.execute(sql, tuple(params))
        
        documentos = []
        for row in cur.fetchall():
            documentos.append({
                "id": row[0], 
                "numero_documento": row[1] or "N/D",
                "data_documento": row[2].isoformat(),
                "valor_total_documento": float(row[3]),
                "valor_pago_recebido": float(row[4]),
                "valor_restante": float(row[5]) 
            })
            
        return jsonify(documentos)
        
    except Exception as e:
        app.logger.error(f"Erro get_notas_em_aberto: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()

@app.route("/registrar_fluxo_caixa", methods=["POST"])
@login_required
def registrar_fluxo_caixa():
    conn = None
    try:
        dados = request.json
        app.logger.info(f"Registrando Fluxo: {dados}")
        conn = conectar_banco()
        cur = conn.cursor()

        uvr = dados.get("uvr")
        associacao = dados.get("associacao")
        tipo_mov = dados.get("tipo_movimentacao")
        
        id_cadastro_cf_str_from_js = dados.get("id_cadastro_cf_str") 
        is_associado_rateio = dados.get("is_associado_rateio", False)
        nome_cf_display = dados.get("nome_cadastro_cf_display")

        id_cadastro_cf_db = None
        nome_cadastro_cf_db = nome_cf_display 

        # --- CORREÇÃO PRINCIPAL: TRATAMENTO DO ID ---
        if is_associado_rateio:
            pass # Rateio geralmente não tem ID de cadastro vinculado, usa nome
        elif id_cadastro_cf_str_from_js:
            # Se for numérico, converte para INT
            if str(id_cadastro_cf_str_from_js).isdigit():
                id_cadastro_cf_db = int(id_cadastro_cf_str_from_js)
            else:
                # Se for Texto (Nome), tentamos achar o ID no banco pelo nome
                app.logger.info(f"FluxoCaixa: Recebido nome '{id_cadastro_cf_str_from_js}' em vez de ID. Buscando ID...")
                cur.execute("SELECT id FROM cadastros WHERE razao_social = %s LIMIT 1", (str(id_cadastro_cf_str_from_js),))
                row_busca = cur.fetchone()
                if row_busca:
                    id_cadastro_cf_db = row_busca[0] # Achamos o ID!
                else:
                    id_cadastro_cf_db = None # Não achamos, salva sem ID (apenas com o nome)
        else: 
             return jsonify({"error": "Identificação do Cliente/Fornecedor ausente."}), 400
        # --------------------------------------------
        
        id_conta = int(dados.get("id_conta_corrente"))
        numero_doc_bancario = dados.get("numero_documento_bancario")
        
        try:
            data_efetiva = datetime.strptime(dados.get("data_efetiva"), '%Y-%m-%d').date()
            valor_efetivo = Decimal(str(dados.get("valor_efetivo")).replace(",", "."))
            # Tratamento flexível da data/hora
            data_hora_str = dados.get("data_hora_registro_fluxo")
            try:
                data_registro = datetime.strptime(data_hora_str, '%d/%m/%Y %H:%M:%S')
            except:
                data_registro = datetime.now() # Fallback se formato vier errado
                
        except (ValueError, TypeError, InvalidOperation) as e:
            return jsonify({"error": f"Formato de data ou valor inválido: {e}"}), 400
        
        total_nfs_selecionadas_valor = Decimal('0.00')
        notas_ids_selecionadas = dados.get("ids_nfs_selecionadas", [])
        
        if not notas_ids_selecionadas:
             return jsonify({"error": "Nenhuma nota fiscal foi selecionada."}), 400

        # Calcula totais
        for id_nf_str in notas_ids_selecionadas:
            cur.execute("SELECT (valor_total_documento - valor_pago_recebido) FROM transacoes_financeiras WHERE id = %s", (int(id_nf_str),))
            nf_pendente_row = cur.fetchone()
            if nf_pendente_row and nf_pendente_row[0] is not None:
                 total_nfs_selecionadas_valor += Decimal(nf_pendente_row[0])
        
        saldo_operacao_calculado = total_nfs_selecionadas_valor - valor_efetivo
        observacoes = dados.get("observacoes")

        # Insere no Fluxo
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

        # Baixa nas NFs
        valor_restante = valor_efetivo
        for id_nf_str in notas_ids_selecionadas:
            id_transacao = int(id_nf_str)
            if valor_restante <= Decimal('0'): break 

            cur.execute("SELECT valor_pago_recebido, valor_total_documento, (valor_total_documento - valor_pago_recebido) FROM transacoes_financeiras WHERE id = %s", (id_transacao,))
            nf_data = cur.fetchone()
            if not nf_data: continue

            pago_atual, total_doc, pendente = Decimal(nf_data[0]), Decimal(nf_data[1]), Decimal(nf_data[2])
            aplicar = min(valor_restante, pendente)
            
            if aplicar > Decimal('0'):
                cur.execute("INSERT INTO fluxo_caixa_transacoes_link (id_fluxo_caixa, id_transacao_financeira, valor_aplicado_nesta_nf) VALUES (%s, %s, %s)", (id_fluxo, id_transacao, aplicar))

                novo_pago = pago_atual + aplicar
                status_final = 'Liquidado' if novo_pago >= total_doc else 'Parcialmente Pago/Recebido'
                
                cur.execute("UPDATE transacoes_financeiras SET valor_pago_recebido = %s, status_pagamento = %s WHERE id = %s", (novo_pago, status_final, id_transacao))
                valor_restante -= aplicar

        conn.commit()
        return jsonify({"status": "sucesso", "message": "Registrado com sucesso!"})
        
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro fluxo: {e}", exc_info=True)
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

        output = io.StringIO(newline="")
        writer = csv.writer(output, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        
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

def _substituir_documento_transacao(cur, uvr, id_tipo_documento, caminho_arquivo, nome_original, competencia,
                                    valor, numero_referencia, enviado_por, observacoes):
    cur.execute("""
        SELECT id, caminho_arquivo
        FROM documentos
        WHERE uvr = %s AND id_tipo = %s AND numero_referencia = %s
        ORDER BY id DESC
        LIMIT 1
    """, (uvr, id_tipo_documento, numero_referencia))
    existente = cur.fetchone()

    if existente:
        doc_id, caminho_antigo = existente
        if caminho_antigo:
            if str(caminho_antigo).startswith("http"):
                _delete_cloudinary_asset(
                    caminho_antigo,
                    resource_type=_detect_cloudinary_resource_type(caminho_antigo),
                )
            else:
                caminho_local = os.path.join('uploads', caminho_antigo)
                if os.path.exists(caminho_local):
                    os.remove(caminho_local)

        cur.execute("""
            UPDATE documentos SET
                caminho_arquivo=%s,
                nome_original=%s,
                competencia=%s,
                data_validade=%s,
                valor=%s,
                numero_referencia=%s,
                observacoes=%s,
                enviado_por=%s,
                status='Pendente',
                motivo_rejeicao=NULL
            WHERE id=%s
        """, (
            caminho_arquivo, nome_original, competencia, None, valor,
            numero_referencia, observacoes, enviado_por, doc_id
        ))
    else:
        cur.execute("""
            INSERT INTO documentos
            (uvr, id_tipo, caminho_arquivo, nome_original, competencia,
             data_validade, valor, numero_referencia, observacoes,
             enviado_por, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Pendente')
        """, (
            uvr, id_tipo_documento, caminho_arquivo, nome_original,
            competencia, None, valor, numero_referencia,
            observacoes, enviado_por
        ))

def _preparar_documento_anexo(arquivo_nf, cabecalho, valor_total, id_transacao, cur):
    data_documento = datetime.strptime(cabecalho["data_documento"], '%Y-%m-%d').date()
    if cabecalho.get("tipo_atividade") == "Rateio dos Associados":
        nome_tipo_documento = "Recibos de Rateio"
    else:
        nome_tipo_documento = "Notas Fiscais de Receitas" if cabecalho["tipo_transacao"] == "Receita" else "Notas Fiscais de Despesas"
    competencia = date(data_documento.year, data_documento.month, 1)
    numero_referencia = cabecalho.get("numero_documento", "")

    cur.execute("SELECT id FROM tipos_documentos WHERE nome = %s", (nome_tipo_documento,))
    tipo_doc = cur.fetchone()
    if not tipo_doc:
        raise ValueError(f"Tipo de documento '{nome_tipo_documento}' não encontrado.")
    id_tipo_documento = tipo_doc[0]

    nome_original = arquivo_nf.filename
    import time
    timestamp = int(time.time())
    extensao = os.path.splitext(nome_original)[1]
    nome_arquivo_salvo = f"nf_transacao_{cabecalho['uvr']}_{timestamp}{extensao}"
    file_format = extensao.lstrip('.') if extensao else None

    url_cloud = _upload_file_to_cloudinary(
        arquivo_nf,
        folder="documentos",
        public_id=f"nf_transacao_{cabecalho['uvr']}_{timestamp}",
        resource_type="raw",
        file_format=file_format,
    )
    if url_cloud:
        nome_arquivo_salvo = url_cloud
    else:
        os.makedirs('uploads', exist_ok=True)
        arquivo_nf.save(os.path.join('uploads', nome_arquivo_salvo))

    observacoes_doc = f"Gerado automaticamente da edição da transação #{id_transacao}. Origem: {cabecalho.get('nome_origem', '')}."
    return {
        "id_tipo_documento": id_tipo_documento,
        "caminho_arquivo": nome_arquivo_salvo,
        "nome_original": nome_original,
        "competencia": competencia.isoformat(),
        "valor": float(valor_total),
        "numero_referencia": numero_referencia,
        "observacoes": observacoes_doc,
        "enviado_por": current_user.username,
    }

def _preparar_mtr_anexo(arquivo_mtr, cabecalho, id_transacao, cur):
    data_documento = datetime.strptime(cabecalho["data_documento"], '%Y-%m-%d').date()
    nome_tipo_documento = "MTR – Manifesto de Transporte"
    competencia = date(data_documento.year, data_documento.month, 1)
    numero_referencia = cabecalho.get("numero_documento", "")

    cur.execute("SELECT id FROM tipos_documentos WHERE nome = %s", (nome_tipo_documento,))
    tipo_doc = cur.fetchone()
    if not tipo_doc:
        raise ValueError(f"Tipo de documento '{nome_tipo_documento}' não encontrado.")
    id_tipo_documento = tipo_doc[0]

    nome_original = arquivo_mtr.filename
    import time
    timestamp = int(time.time())
    extensao = os.path.splitext(nome_original)[1]
    nome_arquivo_salvo = f"mtr_transacao_{cabecalho['uvr']}_{timestamp}{extensao}"
    file_format = extensao.lstrip('.') if extensao else None

    url_cloud = _upload_file_to_cloudinary(
        arquivo_mtr,
        folder="documentos",
        public_id=f"mtr_transacao_{cabecalho['uvr']}_{timestamp}",
        resource_type="raw",
        file_format=file_format,
    )
    if url_cloud:
        nome_arquivo_salvo = url_cloud
    else:
        os.makedirs('uploads', exist_ok=True)
        arquivo_mtr.save(os.path.join('uploads', nome_arquivo_salvo))

    observacoes_doc = (
        f"MTR anexado automaticamente da transação #{id_transacao}. "
        f"Origem: {cabecalho.get('nome_origem', '')}."
    )
    return {
        "id_tipo_documento": id_tipo_documento,
        "caminho_arquivo": nome_arquivo_salvo,
        "nome_original": nome_original,
        "competencia": competencia.isoformat(),
        "valor": None,
        "numero_referencia": numero_referencia,
        "observacoes": observacoes_doc,
        "enviado_por": current_user.username,
    }

def _preparar_relatorio_fotografico_anexo(arquivo_relatorio, cabecalho, id_transacao, cur):
    data_documento = datetime.strptime(cabecalho["data_documento"], '%Y-%m-%d').date()
    nome_tipo_documento = "Relatório fotográfico da carga"
    competencia = date(data_documento.year, data_documento.month, 1)
    numero_referencia = cabecalho.get("numero_documento", "")

    cur.execute("SELECT id FROM tipos_documentos WHERE nome = %s", (nome_tipo_documento,))
    tipo_doc = cur.fetchone()
    if not tipo_doc:
        raise ValueError(f"Tipo de documento '{nome_tipo_documento}' não encontrado.")
    id_tipo_documento = tipo_doc[0]

    nome_original = arquivo_relatorio.filename
    import time
    timestamp = int(time.time())
    extensao = os.path.splitext(nome_original)[1]
    nome_arquivo_salvo = f"relatorio_fotografico_transacao_{cabecalho['uvr']}_{timestamp}{extensao}"
    file_format = extensao.lstrip('.') if extensao else None

    url_cloud = _upload_file_to_cloudinary(
        arquivo_relatorio,
        folder="documentos",
        public_id=f"relatorio_fotografico_transacao_{cabecalho['uvr']}_{timestamp}",
        resource_type="raw",
        file_format=file_format,
    )
    if url_cloud:
        nome_arquivo_salvo = url_cloud
    else:
        os.makedirs('uploads', exist_ok=True)
        arquivo_relatorio.save(os.path.join('uploads', nome_arquivo_salvo))

    observacoes_doc = (
        f"Relatório fotográfico anexado automaticamente da transação #{id_transacao}. "
        f"Origem: {cabecalho.get('nome_origem', '')}."
    )
    return {
        "id_tipo_documento": id_tipo_documento,
        "caminho_arquivo": nome_arquivo_salvo,
        "nome_original": nome_original,
        "competencia": competencia.isoformat(),
        "valor": None,
        "numero_referencia": numero_referencia,
        "observacoes": observacoes_doc,
        "enviado_por": current_user.username,
    }

def _preparar_comprovante_pagamento_anexo(arquivo_comprovante, cabecalho, valor_total, id_transacao, cur):
    data_documento = datetime.strptime(cabecalho["data_documento"], '%Y-%m-%d').date()
    nome_tipo_documento = "Comprovante de Pagamento"
    competencia = date(data_documento.year, data_documento.month, 1)
    numero_referencia = cabecalho.get("numero_documento", "")

    cur.execute("SELECT id FROM tipos_documentos WHERE nome = %s", (nome_tipo_documento,))
    tipo_doc = cur.fetchone()
    if not tipo_doc:
        raise ValueError(f"Tipo de documento '{nome_tipo_documento}' não encontrado.")
    id_tipo_documento = tipo_doc[0]

    nome_original = arquivo_comprovante.filename
    import time
    timestamp = int(time.time())
    extensao = os.path.splitext(nome_original)[1]
    nome_arquivo_salvo = f"comprovante_pagamento_transacao_{cabecalho['uvr']}_{timestamp}{extensao}"
    file_format = extensao.lstrip('.') if extensao else None

    url_cloud = _upload_file_to_cloudinary(
        arquivo_comprovante,
        folder="documentos",
        public_id=f"comprovante_pagamento_transacao_{cabecalho['uvr']}_{timestamp}",
        resource_type="raw",
        file_format=file_format,
    )
    if url_cloud:
        nome_arquivo_salvo = url_cloud
    else:
        os.makedirs('uploads', exist_ok=True)
        arquivo_comprovante.save(os.path.join('uploads', nome_arquivo_salvo))

    observacoes_doc = (
        f"Gerado automaticamente do comprovante de pagamento da transação #{id_transacao}. "
        f"Origem: {cabecalho.get('nome_origem', '')}."
    )
    return {
        "id_tipo_documento": id_tipo_documento,
        "caminho_arquivo": nome_arquivo_salvo,
        "nome_original": nome_original,
        "competencia": competencia.isoformat(),
        "valor": float(valor_total),
        "numero_referencia": numero_referencia,
        "observacoes": observacoes_doc,
        "enviado_por": current_user.username,
    }

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

        output = io.StringIO(newline="")
        writer = csv.writer(output, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL, lineterminator='\n')
        
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
        
        # Busca TUDO que está pendente (Associados, Cadastros E DOCUMENTOS)
        # Atenção: Adicionado o CASE para 'documentos'
        sql = """
            SELECT s.id, s.usuario_solicitante, s.data_solicitacao, s.tabela_alvo, s.tipo_solicitacao, s.dados_novos,
                   CASE 
                       WHEN s.tabela_alvo = 'associados' THEN (SELECT nome FROM associados WHERE id = s.id_registro)
                       WHEN s.tabela_alvo = 'cadastros' THEN (SELECT razao_social FROM cadastros WHERE id = s.id_registro)
                       WHEN s.tabela_alvo = 'documentos' THEN (SELECT nome_original FROM documentos WHERE id = s.id_registro)
                       WHEN s.tabela_alvo = 'epi_itens' THEN (SELECT nome FROM epi_itens WHERE id = s.id_registro)
                       WHEN s.tabela_alvo = 'epi_movimentos' THEN (
                           SELECT i.nome FROM epi_movimentos m
                           JOIN epi_itens i ON m.id_item = i.id
                           WHERE m.id = s.id_registro
                       )
                       WHEN s.tabela_alvo = 'epi_entregas' THEN (
                           SELECT i.nome
                           FROM epi_entrega_itens it
                           JOIN epi_itens i ON it.id_item = i.id
                           WHERE it.id_entrega = s.id_registro
                           LIMIT 1
                       )
                   END as nome_atual,
                   s.id_registro
            FROM solicitacoes_alteracao s
            WHERE s.status = 'PENDENTE' OR s.status = 'Pendente'
            ORDER BY s.data_solicitacao DESC
        """
        cur.execute(sql)
        res = []
        for r in cur.fetchall():
            # [0]id, [1]solicitante, [2]data, [3]tabela, [4]tipo, [5]dados, [6]nome_atual, [7]id_registro
            
            # --- CORREÇÃO DE TIPO (STR ou DICT) ---
            raw_data = r[5]
            dados = {}
            if isinstance(raw_data, str):
                try: dados = json.loads(raw_data)
                except: dados = {}
            elif isinstance(raw_data, dict):
                dados = raw_data
            # --------------------------------------

            # Lógica para exibir o "Novo Valor" ou o "Tipo de Ação" na lista
            nome_novo_ou_acao = ""
            
            # Se for EXCLUSÃO
            if str(r[4]).upper() == 'EXCLUSAO' or str(r[4]).upper() == 'EXCLUSÃO':
                nome_novo_ou_acao = "SOLICITAÇÃO DE EXCLUSÃO"
            
            # Se for EDIÇÃO
            else:
                if r[3] == 'documentos':
                    # Documentos geralmente não mudam de nome, mostramos a referência ou valor
                    ref = dados.get('numero_referencia')
                    val = dados.get('valor')
                    if ref: nome_novo_ou_acao = f"Ref: {ref}"
                    elif val: nome_novo_ou_acao = f"Novo Valor: {val}"
                    else: nome_novo_ou_acao = "Alteração de Dados/Datas"

                elif r[3] == 'epi_movimentos':
                    qtd = dados.get('quantidade')
                    if qtd is not None:
                        nome_novo_ou_acao = f"Qtd: {qtd}"
                    else:
                        nome_novo_ou_acao = "Alteração de Entrada"
                elif r[3] == 'epi_entregas':
                    qtd = dados.get('quantidade')
                    unidade = dados.get('unidade')
                    if qtd is not None and unidade:
                        nome_novo_ou_acao = f"Qtd: {qtd} {unidade}"
                    elif qtd is not None:
                        nome_novo_ou_acao = f"Qtd: {qtd}"
                    else:
                        nome_novo_ou_acao = "Alteração de Entrega"

                else:
                    # Associados e Cadastros
                    nome_novo_ou_acao = dados.get('nome') or dados.get('razao_social') or "Edição de Dados"
            
            res.append({
                "id": r[0], 
                "solicitante": r[1], 
                "data": r[2].strftime('%d/%m %H:%M') if r[2] else '-',
                "tabela": r[3],      # ex: 'documentos'
                "tipo": r[4],        # ex: 'Edicao'
                "nome_atual": r[6] or "(Item não encontrado/Já excluído)", 
                "nome_novo": nome_novo_ou_acao,
                "dados_novos": dados, # Envia o JSON completo para o modal usar se precisar
                "id_origem": r[7]
            })
        return jsonify(res)
    except Exception as e:
        print(f"Erro na API get_solicitacoes_pendentes: {e}")
        return jsonify([])
    finally:
        if conn: conn.close()

@app.route("/editar_conta_corrente", methods=["POST"])
@login_required
def editar_conta_corrente():
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
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
        tabela_raw = tabela or ""
        tabela_norm = re.sub(r"[^a-z0-9]+", "_", tabela_raw.strip().lower()).strip("_")
        tabela_aliases = {
            "epi_estoque": "epi_estoque",
            "epiestoque": "epi_estoque",
            "estoque_epi": "epi_estoque",
        }
        tabela = tabela_aliases.get(tabela_norm, tabela_norm)

        def adicionar_meses(data_base, meses):
            ano = data_base.year + (data_base.month - 1 + meses) // 12
            mes = (data_base.month - 1 + meses) % 12 + 1
            ultimo_dia = calendar.monthrange(ano, mes)[1]
            dia = min(data_base.day, ultimo_dia)
            return date(ano, mes, dia)
        
        if acao == 'aprovar':
            if tipo == 'EXCLUSAO':
                if tabela == 'epi_movimentos':
                    cur.execute("""
                        SELECT id_item, quantidade, uvr, associacao
                        FROM epi_movimentos
                        WHERE id = %s AND tipo_movimento = 'ENTRADA'
                    """, (id_reg,))
                    mov = cur.fetchone()
                    if not mov:
                        return jsonify({"error": "Entrada não encontrada."}), 404

                    id_item, qtd_mov, uvr_mov, associacao_mov = mov
                    cur.execute("DELETE FROM epi_movimentos WHERE id = %s", (id_reg,))
                    cur.execute("""
                        INSERT INTO epi_estoque (id_item, uvr, associacao, unidade, quantidade, data_hora_atualizacao)
                        VALUES (%s, %s, %s, 'un', %s, NOW())
                        ON CONFLICT (uvr, associacao, id_item)
                        DO UPDATE SET
                            quantidade = epi_estoque.quantidade + EXCLUDED.quantidade,
                            data_hora_atualizacao = NOW();
                    """, (id_item, uvr_mov, associacao_mov, -float(qtd_mov)))
                    msg = "Entrada excluída com sucesso!"
                elif tabela == 'epi_entregas':
                    cur.execute("""
                        SELECT it.id_item, it.quantidade, e.uvr, e.associacao
                        FROM epi_entregas e
                        JOIN epi_entrega_itens it ON it.id_entrega = e.id
                        WHERE e.id = %s
                    """, (id_reg,))
                    entrega = cur.fetchone()
                    if not entrega:
                        return jsonify({"error": "Entrega não encontrada."}), 404

                    id_item, qtd_entrega, uvr_entrega, associacao_entrega = entrega
                    cur.execute("DELETE FROM epi_entregas WHERE id = %s", (id_reg,))
                    cur.execute("""
                        INSERT INTO epi_estoque (id_item, uvr, associacao, unidade, quantidade, data_hora_atualizacao)
                        VALUES (%s, %s, %s, 'un', %s, NOW())
                        ON CONFLICT (uvr, associacao, id_item)
                        DO UPDATE SET
                            quantidade = epi_estoque.quantidade + EXCLUDED.quantidade,
                            data_hora_atualizacao = NOW();
                    """, (id_item, uvr_entrega, associacao_entrega, float(qtd_entrega)))
                    msg = "Entrega excluída com sucesso!"
                else:
                    # Executa a exclusão real
                    sql_del = f"DELETE FROM {tabela} WHERE id = %s"
                    cur.execute(sql_del, (id_reg,))
                    msg = "Registro excluído com sucesso!"
            else:
                # Executa a edição (APROVAÇÃO)
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

                elif tabela == 'epi_itens':
                    cur.execute("""UPDATE epi_itens SET 
                        nome=%s, categoria=%s, ca=%s, validade_meses=%s, funcao_indicada=%s, uvr=%s
                        WHERE id=%s""",
                        (d.get("nome"), d.get("categoria"), d.get("ca"), d.get("validade_meses"),
                         d.get("funcao_indicada"), d.get("uvr"), id_reg))

                elif tabela == 'epi_movimentos':
                    cur.execute("""
                        SELECT id_item, quantidade, uvr, associacao
                        FROM epi_movimentos
                        WHERE id = %s AND tipo_movimento = 'ENTRADA'
                    """, (id_reg,))
                    mov = cur.fetchone()
                    if not mov:
                        return jsonify({"error": "Entrada não encontrada."}), 404

                    id_item, qtd_antiga, uvr_mov, associacao_mov = mov

                    nova_qtd = float(d.get("quantidade", qtd_antiga))
                    cur.execute("""
                        UPDATE epi_movimentos
                        SET quantidade = %s, data_movimento = %s, marca = %s, observacao = %s
                        WHERE id = %s
                    """, (nova_qtd, d.get("data_movimento"), d.get("marca"), d.get("observacao"), id_reg))

                    delta = nova_qtd - float(qtd_antiga)
                    cur.execute("""
                        INSERT INTO epi_estoque (id_item, uvr, associacao, unidade, quantidade, data_hora_atualizacao)
                        VALUES (%s, %s, %s, 'un', %s, NOW())
                        ON CONFLICT (uvr, associacao, id_item)
                        DO UPDATE SET
                            quantidade = epi_estoque.quantidade + EXCLUDED.quantidade,
                            data_hora_atualizacao = NOW();
                    """, (id_item, uvr_mov, associacao_mov, delta))

                elif tabela == 'epi_entregas':
                    cur.execute("""
                        SELECT it.id_item, it.quantidade, it.unidade, e.uvr, e.associacao
                        FROM epi_entregas e
                        JOIN epi_entrega_itens it ON it.id_entrega = e.id
                        WHERE e.id = %s
                    """, (id_reg,))
                    entrega = cur.fetchone()
                    if not entrega:
                        return jsonify({"error": "Entrega não encontrada."}), 404

                    id_item, qtd_antiga, unidade_antiga, uvr_entrega, associacao_entrega = entrega
                    nova_qtd = float(d.get("quantidade", qtd_antiga))
                    nova_unidade = d.get("unidade") or unidade_antiga
                    data_entrega = d.get("data_entrega")
                    observacoes = d.get("observacoes")

                    data_entrega_val = None
                    if data_entrega:
                        try:
                            data_entrega_val = datetime.strptime(data_entrega, "%Y-%m-%d").date()
                        except ValueError:
                            data_entrega_val = None

                    data_validade = None
                    if data_entrega_val:
                        cur.execute("SELECT validade_meses FROM epi_itens WHERE id = %s", (id_item,))
                        validade_row = cur.fetchone()
                        validade_meses = validade_row[0] if validade_row else None
                        if validade_meses and isinstance(validade_meses, int):
                            data_validade = adicionar_meses(data_entrega_val, validade_meses)

                    cur.execute("""
                        UPDATE epi_entregas
                        SET data_entrega = COALESCE(%s, data_entrega),
                            observacoes = %s
                        WHERE id = %s
                    """, (data_entrega_val, observacoes, id_reg))

                    cur.execute("""
                        UPDATE epi_entrega_itens
                        SET quantidade = %s,
                            unidade = %s,
                            data_validade = COALESCE(%s, data_validade)
                        WHERE id_entrega = %s
                    """, (nova_qtd, nova_unidade, data_validade, id_reg))

                    delta = nova_qtd - float(qtd_antiga)
                    cur.execute("""
                        INSERT INTO epi_estoque (id_item, uvr, associacao, unidade, quantidade, data_hora_atualizacao)
                        VALUES (%s, %s, %s, 'un', %s, NOW())
                        ON CONFLICT (uvr, associacao, id_item)
                        DO UPDATE SET
                            quantidade = epi_estoque.quantidade + EXCLUDED.quantidade,
                            data_hora_atualizacao = NOW();
                    """, (id_item, uvr_entrega, associacao_entrega, -delta))

                elif tabela == 'epi_estoque':
                    cur.execute("""
                        SELECT quantidade, unidade
                        FROM epi_estoque
                        WHERE id = %s
                    """, (id_reg,))
                    estoque = cur.fetchone()
                    if not estoque:
                        return jsonify({"error": "Registro de estoque não encontrado."}), 404

                    qtd_atual, unidade_atual = estoque
                    nova_qtd = float(d.get("quantidade", qtd_atual))
                    nova_unidade = d.get("unidade") or unidade_atual

                    cur.execute("""
                        UPDATE epi_estoque
                        SET quantidade = %s, unidade = %s, data_hora_atualizacao = NOW()
                        WHERE id = %s
                    """, (nova_qtd, nova_unidade, id_reg))
                
                elif tabela == 'transacoes_financeiras':
                    # 1. Atualiza Cabeçalho
                    id_origem = d.get("id_origem")
                    if id_origem and str(id_origem).isdigit(): id_origem = int(id_origem)
                    else: id_origem = None
                    id_patrimonio = d.get("id_patrimonio")
                    if id_patrimonio and str(id_patrimonio).isdigit(): id_patrimonio = int(id_patrimonio)
                    else: id_patrimonio = None
                    id_motorista = d.get("id_motorista")
                    if id_motorista and str(id_motorista).isdigit(): id_motorista = int(id_motorista)
                    else: id_motorista = None

                    medidor_atual = None
                    if d.get("medidor_atual"):
                        medidor_atual = Decimal(str(d.get("medidor_atual")).replace(",", "."))

                    litros = None
                    if d.get("litros"):
                        litros = Decimal(str(d.get("litros")).replace(",", "."))

                    garantia_km = None
                    if d.get("garantia_km") and str(d.get("garantia_km")).isdigit():
                        garantia_km = int(d.get("garantia_km"))

                    proxima_revisao_km = None
                    if d.get("proxima_revisao_km") and str(d.get("proxima_revisao_km")).isdigit():
                        proxima_revisao_km = int(d.get("proxima_revisao_km"))

                    garantia_data = None
                    if d.get("garantia_data"):
                        garantia_data = datetime.strptime(d.get("garantia_data"), '%Y-%m-%d').date()

                    proxima_revisao_data = None
                    if d.get("proxima_revisao_data"):
                        proxima_revisao_data = datetime.strptime(d.get("proxima_revisao_data"), '%Y-%m-%d').date()

                    cur.execute("""
                        UPDATE transacoes_financeiras SET
                            uvr=%s, associacao=%s, data_documento=%s, tipo_transacao=%s,
                            tipo_atividade=%s, numero_documento=%s, 
                            id_cadastro_origem=%s, nome_cadastro_origem=%s,
                            valor_total_documento=%s, id_patrimonio=%s, categoria_despesa_patrimonio=%s,
                            medidor_atual=%s, tipo_medidor=%s, id_motorista=%s, nome_motorista=%s,
                            litros=%s, tipo_combustivel=%s, tipo_manutencao=%s, garantia_km=%s,
                            garantia_data=%s, proxima_revisao_km=%s, proxima_revisao_data=%s
                        WHERE id=%s
                    """, (
                        d["uvr"], d.get("associacao",""), d["data_documento"],
                        d["tipo_transacao"], d["tipo_atividade"], d.get("numero_documento",""),
                        id_origem, d.get("nome_origem"), d["valor_total"], id_patrimonio, d.get("categoria_despesa_patrimonio"),
                        medidor_atual, d.get("tipo_medidor"), id_motorista, d.get("nome_motorista"),
                        litros, d.get("tipo_combustivel"), d.get("tipo_manutencao"), garantia_km,
                        garantia_data, proxima_revisao_km, proxima_revisao_data, id_reg
                    ))

                    # 2. Atualiza Itens (Apaga antigos e insere os do JSON)
                    cur.execute("DELETE FROM itens_transacao WHERE id_transacao = %s", (id_reg,))
                    for item in d["itens"]:
                        cur.execute("""
                            INSERT INTO itens_transacao (id_transacao, descricao, unidade, quantidade, valor_unitario, valor_total_item)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """, (id_reg, item['descricao'], item['unidade'], item['quantidade'], item['valor_unitario'], item['valor_total_item']))

                    documento_anexo = d.get("documento_anexo")
                    if documento_anexo:
                        competencia = documento_anexo.get("competencia")
                        if competencia:
                            competencia = datetime.strptime(competencia, '%Y-%m-%d').date()
                        _substituir_documento_transacao(
                            cur,
                            d["uvr"],
                            documento_anexo["id_tipo_documento"],
                            documento_anexo["caminho_arquivo"],
                            documento_anexo["nome_original"],
                            competencia,
                            Decimal(str(documento_anexo["valor"])),
                            documento_anexo.get("numero_referencia", ""),
                            documento_anexo.get("enviado_por", current_user.username),
                            documento_anexo.get("observacoes", ""),
                        )

                    comprovante_anexo = d.get("comprovante_pagamento_anexo")
                    if comprovante_anexo:
                        competencia = comprovante_anexo.get("competencia")
                        if competencia:
                            competencia = datetime.strptime(competencia, '%Y-%m-%d').date()
                        _substituir_documento_transacao(
                            cur,
                            d["uvr"],
                            comprovante_anexo["id_tipo_documento"],
                            comprovante_anexo["caminho_arquivo"],
                            comprovante_anexo["nome_original"],
                            competencia,
                            Decimal(str(comprovante_anexo["valor"])),
                            comprovante_anexo.get("numero_referencia", ""),
                            comprovante_anexo.get("enviado_por", current_user.username),
                            comprovante_anexo.get("observacoes", ""),
                        )

                    mtr_anexo = d.get("mtr_anexo")
                    if mtr_anexo:
                        competencia = mtr_anexo.get("competencia")
                        if competencia:
                            competencia = datetime.strptime(competencia, '%Y-%m-%d').date()
                        valor_doc = mtr_anexo.get("valor")
                        if valor_doc is not None:
                            valor_doc = Decimal(str(valor_doc))
                        _substituir_documento_transacao(
                            cur,
                            d["uvr"],
                            mtr_anexo["id_tipo_documento"],
                            mtr_anexo["caminho_arquivo"],
                            mtr_anexo["nome_original"],
                            competencia,
                            valor_doc,
                            mtr_anexo.get("numero_referencia", ""),
                            mtr_anexo.get("enviado_por", current_user.username),
                            mtr_anexo.get("observacoes", ""),
                        )

                    relatorio_anexo = d.get("relatorio_fotografico_anexo")
                    if relatorio_anexo:
                        competencia = relatorio_anexo.get("competencia")
                        if competencia:
                            competencia = datetime.strptime(competencia, '%Y-%m-%d').date()
                        valor_doc = relatorio_anexo.get("valor")
                        if valor_doc is not None:
                            valor_doc = Decimal(str(valor_doc))
                        _substituir_documento_transacao(
                            cur,
                            d["uvr"],
                            relatorio_anexo["id_tipo_documento"],
                            relatorio_anexo["caminho_arquivo"],
                            relatorio_anexo["nome_original"],
                            competencia,
                            valor_doc,
                            relatorio_anexo.get("numero_referencia", ""),
                            relatorio_anexo.get("enviado_por", current_user.username),
                            relatorio_anexo.get("observacoes", ""),
                        )

                # --- LÓGICA PARA PATRIMÔNIO ---
                elif tabela == 'patrimonio':
                    # Remove campos auxiliares de visualização que não existem no banco
                    d.pop('nome_visual', None)
                    
                    # Constrói a query de UPDATE dinamicamente baseada nas chaves do JSON
                    campos = list(d.keys())
                    valores = list(d.values())
                    valores.append(id_reg) # ID para o WHERE no final
                    
                    set_clause = ", ".join([f"{campo}=%s" for campo in campos])
                    
                    sql_update = f"UPDATE patrimonio SET {set_clause} WHERE id=%s"
                    cur.execute(sql_update, tuple(valores))
                # ------------------------------

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
        
        # 1. ATUALIZAÇÃO NO SQL (Adicionado 'funcao' ao final)
        sql = """
            SELECT nome, cpf, rg, data_nascimento, data_admissao, status, 
                   uvr, associacao, logradouro, endereco_numero, bairro, cidade, 
                   uf, cep, telefone, foto_base64, numero, funcao
            FROM associados WHERE id = %s
        """
        cur.execute(sql, (id,))
        row = cur.fetchone()
        
        if not row: return "Associado não encontrado", 404

        # 2. ATUALIZAÇÃO NO DICIONÁRIO (Pegando row[17])
        dados = {
            "nome": row[0], "cpf": row[1], "rg": row[2],
            "nasc": row[3].strftime('%d/%m/%Y') if row[3] else "-",
            "admissao": row[4].strftime('%d/%m/%Y') if row[4] else "-",
            "status": row[5], "uvr": row[6], "assoc": row[7],
            "logradouro": row[8] or "", "num": row[9] or "",
            "bairro": row[10] or "", "cidade": row[11] or "",
            "uf": row[12] or "", "cep": row[13] or "",
            "tel": row[14], "foto": row[15], "matricula": row[16],
            "funcao": row[17] if row[17] else "Não informada"  # <--- CAMPO NOVO
        }

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=A4, 
                                topMargin=1*cm, bottomMargin=1*cm, 
                                leftMargin=1.5*cm, rightMargin=1.5*cm)
        
        story = []
        styles = getSampleStyleSheet()
        
        style_titulo = ParagraphStyle('FichaTitulo', parent=styles['Heading1'], alignment=TA_CENTER, fontSize=16, spaceAfter=20)
        style_label = ParagraphStyle('FichaLabel', parent=styles['Normal'], fontSize=8, textColor=colors.gray)
        style_valor = ParagraphStyle('FichaValor', parent=styles['Normal'], fontSize=10, leading=12)
        
        story.append(Paragraph(f"Ficha Cadastral do Associado - {dados['assoc']}", style_titulo))
        story.append(Spacer(1, 0.5*cm))

        # --- PROCESSAMENTO DA FOTO (3x4) ---
        img_obj = None
        if dados['foto'] and len(dados['foto']) > 100:
            try:
                img_str = dados['foto'].split(",")[1] if "," in dados['foto'] else dados['foto']
                img_data = base64.b64decode(img_str)
                imagem_io = io.BytesIO(img_data)
                
                img_reader = ImageReader(imagem_io)
                orig_w, orig_h = img_reader.getSize()
                aspect = orig_h / float(orig_w)
                
                render_w = 3.0 * cm
                render_h = render_w * aspect
                
                if render_h > 4.5 * cm:
                    render_h = 4.5 * cm
                    render_w = render_h / aspect
                
                img_obj = ReportLabImage(imagem_io, width=render_w, height=render_h)
            except Exception as e: 
                app.logger.error(f"Erro imagem PDF: {e}")

        # Listas de campos
        lista_pessoais = [
            ("Nome Completo", dados['nome']), ("Matrícula", dados['matricula']),
            ("CPF", dados['cpf']), ("RG", dados['rg']),
            ("Data Nascimento", dados['nasc']), ("Telefone", dados['tel'])
        ]
        
        # 3. ATUALIZAÇÃO VISUAL (Adicionei Função aqui)
        lista_sistema = [
            ("Função", dados['funcao']), ("UVR", dados['uvr']), # <--- Adicionado aqui
            ("Associação", dados['assoc']), ("Status", dados['status']),
            ("Data Admissão", dados['admissao'])
        ]
        
        lista_endereco = [
            ("Endereço", f"{dados['logradouro']}, {dados['num']}"), ("Bairro", dados['bairro']),
            ("Cidade/UF", f"{dados['cidade']} - {dados['uf']}"), ("CEP", dados['cep'])
        ]

        col_w = 6.5*cm if img_obj else 8.0*cm

        def criar_tabela_secao(lista_campos):
            rows = []
            for i in range(0, len(lista_campos), 2):
                c1 = lista_campos[i]
                cell1 = [Paragraph(f"<b>{c1[0]}</b>", style_label), Paragraph(str(c1[1]), style_valor)]
                cell2 = []
                if i + 1 < len(lista_campos):
                    c2 = lista_campos[i+1]
                    cell2 = [Paragraph(f"<b>{c2[0]}</b>", style_label), Paragraph(str(c2[1]), style_valor)]
                rows.append([cell1, cell2])
            
            t = Table(rows, colWidths=[col_w, col_w])
            t.setStyle(TableStyle([
                ('VALIGN', (0,0), (-1,-1), 'TOP'),
                ('LEFTPADDING', (0,0), (-1,-1), 0),
                ('BOTTOMPADDING', (0,0), (-1,-1), 6)
            ]))
            return t

        elementos_texto = []
        elementos_texto.append(Paragraph("<b>DADOS PESSOAIS</b>", styles['Heading4']))
        elementos_texto.append(criar_tabela_secao(lista_pessoais))
        elementos_texto.append(Spacer(1, 0.3*cm))
        elementos_texto.append(Paragraph("<b>DADOS DO SISTEMA</b>", styles['Heading4']))
        elementos_texto.append(criar_tabela_secao(lista_sistema))
        elementos_texto.append(Spacer(1, 0.3*cm))
        elementos_texto.append(Paragraph("<b>ENDEREÇO</b>", styles['Heading4']))
        elementos_texto.append(criar_tabela_secao(lista_endereco))

        # --- TABELA PRINCIPAL ---
        if img_obj:
            tbl_foto = Table([[img_obj]], colWidths=[render_w + 4])
            tbl_foto.setStyle(TableStyle([
                ('GRID', (0,0), (-1,-1), 0.5, colors.grey),
                ('ALIGN', (0,0), (-1,-1), 'CENTER'),
                ('VALIGN', (0,0), (-1,-1), 'TOP'),
                ('TOPPADDING', (0,0), (-1,-1), 2),
                ('BOTTOMPADDING', (0,0), (-1,-1), 2),
            ]))

            data_main = [[elementos_texto, tbl_foto]]
            widths_main = [13.0*cm, 4.0*cm]
            
            tbl_main = Table(data_main, colWidths=widths_main, rowHeights=[None])
            tbl_main.setStyle(TableStyle([
                ('VALIGN', (0,0), (-1,-1), 'TOP'),
                ('ALIGN', (1,0), (1,0), 'CENTER'),
                ('LEFTPADDING', (1,0), (1,0), 5),
            ]))
        else:
            data_main = [[elementos_texto]]
            widths_main = [17*cm]
            tbl_main = Table(data_main, colWidths=widths_main)
            tbl_main.setStyle(TableStyle([('VALIGN', (0,0), (-1,-1), 'TOP')]))

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
            if tipo in ("Cliente", "Comprador"):
                sql += " AND (tipo_cadastro LIKE %s OR tipo_cadastro LIKE %s OR tipo_cadastro = 'Ambos')"
                params.extend(["Cliente%", "Comprador%"])
            elif tipo == "Fornecedor":
                sql += " AND (tipo_cadastro LIKE %s OR tipo_cadastro = 'Ambos')"
                params.append("Fornecedor%")
            else:
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
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
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
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # 1. Busca o nome para registrar no histórico (ou log)
        cur.execute("SELECT razao_social FROM cadastros WHERE id = %s", (id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Cadastro não encontrado."}), 404
        
        nome_registro = row[0]

        # 2. Lógica: ADMIN exclui, OUTROS solicitam
        if current_user.role == 'admin':
            try:
                cur.execute("DELETE FROM cadastros WHERE id = %s", (id,))
                conn.commit()
                return jsonify({"status": "sucesso", "message": "Registro excluído permanentemente."})
            
            except psycopg2.IntegrityError:
                # Se o cliente tem vendas/notas, o banco impede a exclusão
                conn.rollback()
                return jsonify({"error": "Não é possível excluir: Este cadastro possui movimentações ou vínculos no sistema. Tente inativá-lo."}), 400
        
        else:
            # 3. Verifica se já existe uma solicitação pendente (para não duplicar)
            cur.execute("""
                SELECT id FROM solicitacoes_alteracao 
                WHERE tabela_alvo = 'cadastros' 
                  AND id_registro = %s 
                  AND tipo_solicitacao = 'EXCLUSAO' 
                  AND status = 'Pendente'
            """, (id,))
            if cur.fetchone():
                return jsonify({"error": "Já existe uma solicitação de exclusão pendente para este cadastro."}), 400

            # 4. Cria a Solicitação
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
        return jsonify({"error": f"Erro interno: {str(e)}"}), 500
    finally:
        if conn: conn.close()

@app.route("/excluir_associado/<int:id>", methods=["POST"])
@login_required
def excluir_associado(id):
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
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
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
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
        
        # 1. Busca os dados brutos da solicitação na tabela unificada
        cur.execute("""
            SELECT id_registro, dados_novos, usuario_solicitante, data_solicitacao, tabela_alvo, tipo_solicitacao 
            FROM solicitacoes_alteracao 
            WHERE id = %s
        """, (id,))
        solic = cur.fetchone()
        
        if not solic: return jsonify({"error": "Solicitação não encontrada"}), 404
        
        id_reg, dados_novos_json, usuario, data_solic, tabela, tipo = solic
        tabela_raw = tabela or ""
        tabela_norm = re.sub(r"[^a-z0-9]+", "_", tabela_raw.strip().lower()).strip("_")
        tabela_aliases = {
            "epi_estoque": "epi_estoque",
            "epiestoque": "epi_estoque",
            "estoque_epi": "epi_estoque",
        }
        tabela = tabela_aliases.get(tabela_norm, tabela_norm)
        
        # 2. Converte o JSON dos novos dados em Dicionário Python
        d_novos = {}
        if dados_novos_json:
            if isinstance(dados_novos_json, str):
                try: d_novos = json.loads(dados_novos_json)
                except: d_novos = {}
            else: d_novos = dados_novos_json
        
        # 3. Se for EXCLUSÃO, retorna resumo simples
        if tipo == 'EXCLUSAO' or tipo == 'EXCLUSÃO':
             nome_item = d_novos.get('nome_visual') or d_novos.get('descricao') or f"Registro ID {id_reg}"
             
             return jsonify({
                "id_solicitacao": id, "usuario": usuario, 
                "data": data_solic.strftime('%d/%m %H:%M') if data_solic else "Data desc.",
                "tipo": "EXCLUSAO", "tabela": tabela_raw,
                "info_extra": f"Solicitação para EXCLUIR permanentemente: {nome_item}"
            })

        # 4. Se for EDIÇÃO, prepara a comparação (DE -> PARA)
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

        elif tabela == 'epi_itens':
            sql_atual = "SELECT id, nome, categoria, ca, validade_meses, funcao_indicada, uvr FROM epi_itens WHERE id = %s"
            cols = ["nome", "categoria", "ca", "validade_meses", "funcao_indicada", "uvr"]
            labels = {
                "nome": "Nome",
                "categoria": "Categoria",
                "ca": "C.A.",
                "validade_meses": "Validade (meses)",
                "funcao_indicada": "Função Indicada",
                "uvr": "UVR"
            }

        elif tabela == 'epi_movimentos':
            sql_atual = """
                SELECT quantidade, data_movimento, marca, observacao
                FROM epi_movimentos WHERE id = %s
            """
            cols = ["quantidade", "data_movimento", "marca", "observacao"]
            labels = {
                "quantidade": "Quantidade",
                "data_movimento": "Data",
                "marca": "Marca",
                "observacao": "Observação"
            }

        elif tabela == 'epi_entregas':
            sql_atual = """
                SELECT it.quantidade, it.unidade, e.data_entrega, e.observacoes
                FROM epi_entregas e
                JOIN epi_entrega_itens it ON it.id_entrega = e.id
                WHERE e.id = %s
            """
            cols = ["quantidade", "unidade", "data_entrega", "observacoes"]
            labels = {
                "quantidade": "Quantidade",
                "unidade": "Unidade",
                "data_entrega": "Data Entrega",
                "observacoes": "Observações"
            }

        elif tabela == 'epi_estoque':
            sql_atual = """
                SELECT quantidade, unidade
                FROM epi_estoque WHERE id = %s
            """
            cols = ["quantidade", "unidade"]
            labels = {
                "quantidade": "Quantidade",
                "unidade": "Unidade"
            }

        elif tabela == 'epi_itens':
            sql_atual = "SELECT nome, categoria, ca, validade_meses, funcao_indicada, uvr FROM epi_itens WHERE id = %s"
            cols = ["nome", "categoria", "ca", "validade_meses", "funcao_indicada", "uvr"]
            labels = {
                "nome": "Nome",
                "categoria": "Categoria",
                "ca": "C.A.",
                "validade_meses": "Validade (meses)",
                "funcao_indicada": "Função Indicada",
                "uvr": "UVR"
            }

        elif tabela == 'epi_movimentos':
            sql_atual = """
                SELECT quantidade, data_movimento, marca, observacao
                FROM epi_movimentos WHERE id = %s
            """
            cols = ["quantidade", "data_movimento", "marca", "observacao"]
            labels = {
                "quantidade": "Quantidade",
                "data_movimento": "Data",
                "marca": "Marca",
                "observacao": "Observação"
            }

        # --- BLOCO ADICIONADO: DOCUMENTOS ---
        elif tabela == 'documentos':
            sql_atual = """
                SELECT competencia, data_validade, valor, numero_referencia, observacoes 
                FROM documentos WHERE id = %s
            """
            cols = ["competencia", "data_validade", "valor", "numero_referencia", "observacoes"]
            labels = {
                "competencia": "Competência", 
                "data_validade": "Data Validade", 
                "valor": "Valor (R$)", 
                "numero_referencia": "Nº Ref/Doc", 
                "observacoes": "Observações"
            }
        
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

        elif tabela == 'patrimonio':
            sql_atual = """
                SELECT uvr, descricao, tipo_bem, categoria, placa, codigo_patrimonio, 
                       status_bem, nome_responsavel, observacoes_gerais
                FROM patrimonio WHERE id = %s
            """
            cols = ["uvr", "descricao", "tipo_bem", "categoria", "placa", "codigo_patrimonio", "status_bem", "nome_responsavel", "observacoes_gerais"]
            labels = {
                "uvr": "UVR", "descricao": "Descrição", "tipo_bem": "Tipo", "categoria": "Categoria",
                "placa": "Placa", "codigo_patrimonio": "Cód. Patrimônio", "status_bem": "Status", 
                "nome_responsavel": "Responsável", "observacoes_gerais": "Observações"
            }

        if not sql_atual: 
            return jsonify({"error": f"Tabela '{tabela}' desconhecida ou não configurada para detalhes."}), 400
        if not sql_atual:
            comp = []
            for k, v in d_novos.items():
                valor_novo = "" if v is None else str(v)
                comp.append({
                    "campo": k,
                    "valor_atual": "(Sem referência)",
                    "valor_novo": valor_novo,
                    "mudou": True
                })

            return jsonify({
                "id_solicitacao": id,
                "usuario": usuario,
                "data": data_solic.strftime('%d/%m %H:%M') if data_solic else "Data desc.",
                "tipo": "EDICAO",
                "comparacao": comp,
                "info_extra": f"Tabela '{tabela_raw}' não configurada para comparação detalhada."
            })

        # Busca os dados ATUAIS no banco
        cur.execute(sql_atual, (id_reg,))
        atual = cur.fetchone()
        
        d_atuais = {}
        if atual:
            for i, c in enumerate(cols): 
                # Converte None, datas e decimais para string
                val = atual[i]
                if val is None: d_atuais[c] = ""
                elif isinstance(val, (date, datetime)): d_atuais[c] = val.strftime('%Y-%m-%d')
                else: d_atuais[c] = str(val)
        else:
            for c in cols: d_atuais[c] = "(Não encontrado)"

        # Monta a lista de comparação
        comp = []
        for k, l in labels.items():
            val_atual = d_atuais.get(k,"").strip()
            
            # Pega valor novo do JSON
            raw_novo = d_novos.get(k)
            if raw_novo is None: val_novo = ""
            else: val_novo = str(raw_novo).strip()
            
            # Normalizações para evitar falso positivo
            if val_atual.lower() == "none": val_atual = ""
            if val_novo.lower() == "none": val_novo = ""
            
            comp.append({ 
                "campo": l, 
                "valor_atual": val_atual, 
                "valor_novo": val_novo, 
                "mudou": val_atual != val_novo 
            })
        
        # --- LÓGICA PARA ITENS DA TRANSAÇÃO (TABELA HTML) ---
        if tabela == 'transacoes_financeiras':
            cur.execute("""
                SELECT descricao, unidade, quantidade, valor_unitario, valor_total_item 
                FROM itens_transacao WHERE id_transacao = %s ORDER BY id ASC
            """, (id_reg,))
            itens_db = cur.fetchall()
            itens_novos = d_novos.get('itens', [])

            def gerar_html_tabela(lista_itens, origem):
                if not lista_itens: return '<small class="text-muted">Nenhum item</small>'
                html = '<table class="table table-sm table-bordered mb-0" style="font-size:0.7rem;">'
                html += '<thead class="table-light"><tr><th>Desc</th><th>Qtd</th><th>Tot</th></tr></thead><tbody>'
                for it in lista_itens:
                    if origem == 'db':
                        desc, un, qtd, unit, tot = it[0], it[1], it[2], it[3], it[4]
                    else: # json
                        desc, un, qtd, tot = it.get('descricao'), it.get('unidade'), it.get('quantidade'), it.get('valor_total_item')
                    
                    try: qtd_fmt = f"{float(qtd):g}" 
                    except: qtd_fmt = str(qtd)
                    try: tot_fmt = f"{float(tot):.2f}".replace('.', ',')
                    except: tot_fmt = str(tot)

                    html += f'<tr><td>{desc}</td><td>{qtd_fmt} {un}</td><td>{tot_fmt}</td></tr>'
                html += '</tbody></table>'
                return html

            html_atual = gerar_html_tabela(itens_db, 'db')
            html_novo = gerar_html_tabela(itens_novos, 'json')
            
            comp.append({
                "campo": "Detalhamento de Itens",
                "valor_atual": html_atual,
                "valor_novo": html_novo,
                "mudou": (html_atual != html_novo)
            })

        # Verifica se tem foto nova
        foto_nova = d_novos.get("foto_bem_base64") or d_novos.get("foto_base64") or ""

        return jsonify({
            "id_solicitacao": id, "usuario": usuario, 
            "data": data_solic.strftime('%d/%m %H:%M') if data_solic else "Data desc.",
            "tipo": "EDICAO", 
            "comparacao": comp, 
            "foto_nova_base64": foto_nova
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
    # 1. Pega os filtros que vieram do Javascript
    data_ini = request.args.get("data_inicial")
    data_final = request.args.get("data_final")
    tipo = request.args.get("tipo")
    uvr_solicitada = request.args.get("uvr") # Filtro de UVR do Admin
    id_patrimonio = request.args.get("id_patrimonio")
    termo = request.args.get("q", "").lower()

    conn = None
    try:
        # --- CORREÇÃO FEITA AQUI: Usando conectar_banco() ---
        conn = conectar_banco() 
        cur = conn.cursor()

        # 2. SQL Base
        sql = """
            SELECT id, uvr, data_documento, tipo_transacao, nome_cadastro_origem, 
                   numero_documento, valor_total_documento, status_pagamento
            FROM transacoes_financeiras 
            WHERE 1=1
        """
        params = []

        # 3. Lógica de Segurança UVR
        if current_user.role == 'admin':
            # Se for Admin, obedece o filtro da tela (se tiver)
            if uvr_solicitada and uvr_solicitada != "Todas" and uvr_solicitada != "":
                sql += " AND uvr = %s"
                params.append(uvr_solicitada)
        else:
            # Se NÃO for Admin, trava na UVR do usuário (ignora o filtro da tela)
            sql += " AND uvr = %s"
            params.append(current_user.uvr_acesso)
        
        # 4. Aplica os outros filtros
        if data_ini:
            sql += " AND data_documento >= %s"
            params.append(data_ini)
        
        if data_final:
            sql += " AND data_documento <= %s"
            params.append(data_final)
            
        if tipo and tipo != "Todos":
            sql += " AND tipo_transacao = %s"
            params.append(tipo)

        if id_patrimonio and str(id_patrimonio).isdigit():
            sql += " AND id_patrimonio = %s"
            params.append(int(id_patrimonio))

        if termo:
            sql += " AND (LOWER(nome_cadastro_origem) LIKE %s OR numero_documento LIKE %s)"
            termo_like = f"%{termo}%"
            params.extend([termo_like, termo_like])

        # Ordenar: Mais recentes primeiro
        sql += " ORDER BY data_documento DESC, id DESC LIMIT 100"

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

        # 5. Monta o JSON para o Frontend
        resultados = []
        for r in rows:
            # Formata a data para ficar bonita (dd/mm/aaaa)
            data_fmt = r[2].strftime('%d/%m/%Y') if r[2] else "-"
            
            # Formata valor para float simples
            val_float = float(r[6]) if r[6] else 0.00

            resultados.append({
                "id": r[0],
                "uvr": r[1],
                "data_documento": data_fmt,
                "tipo_transacao": r[3],
                "nome_cadastro_origem": r[4],
                "numero_documento": r[5] or "-",
                "valor_total_documento": val_float,
                "status_pagamento": r[7]
            })

        return jsonify(resultados)

    except Exception as e:
        app.logger.error(f"Erro ao buscar transações: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

def _buscar_transacoes_para_export(filters):
    data_ini = filters.get("data_inicial")
    data_final = filters.get("data_final")
    tipo = filters.get("tipo")
    uvr_solicitada = filters.get("uvr")
    id_patrimonio = filters.get("id_patrimonio")
    termo = (filters.get("q") or "").lower()

    conn = conectar_banco()
    cur = conn.cursor()
    try:
        sql = """
            SELECT id, uvr, data_documento, tipo_transacao, nome_cadastro_origem,
                   numero_documento, valor_total_documento, status_pagamento
            FROM transacoes_financeiras
            WHERE 1=1
        """
        params = []

        if current_user.role == 'admin':
            if uvr_solicitada and uvr_solicitada != "Todas" and uvr_solicitada != "":
                sql += " AND uvr = %s"
                params.append(uvr_solicitada)
        else:
            sql += " AND uvr = %s"
            params.append(current_user.uvr_acesso)

        if data_ini:
            sql += " AND data_documento >= %s"
            params.append(data_ini)
        if data_final:
            sql += " AND data_documento <= %s"
            params.append(data_final)
        if tipo and tipo != "Todos":
            sql += " AND tipo_transacao = %s"
            params.append(tipo)

        if id_patrimonio and str(id_patrimonio).isdigit():
            sql += " AND id_patrimonio = %s"
            params.append(int(id_patrimonio))
        if termo:
            sql += " AND (LOWER(nome_cadastro_origem) LIKE %s OR numero_documento LIKE %s)"
            termo_like = f"%{termo}%"
            params.extend([termo_like, termo_like])

        sql += " ORDER BY data_documento DESC, id DESC"

        cur.execute(sql, tuple(params))
        rows = cur.fetchall()

        resultados = []
        for r in rows:
            resultados.append({
                "id": r[0],
                "uvr": r[1],
                "data_documento": r[2],
                "tipo_transacao": r[3],
                "nome_cadastro_origem": r[4],
                "numero_documento": r[5] or "-",
                "valor_total_documento": r[6] if r[6] is not None else Decimal("0.00"),
                "status_pagamento": r[7] or ""
            })
        return resultados
    finally:
        conn.close()

def _buscar_nome_frota_por_id(id_patrimonio):
    if not id_patrimonio or not str(id_patrimonio).isdigit():
        return ""

    conn = conectar_banco()
    cur = conn.cursor()
    try:
        cur.execute(
            "SELECT descricao, placa, codigo_patrimonio FROM patrimonio WHERE id = %s",
            (int(id_patrimonio),)
        )
        row = cur.fetchone()
        if not row:
            return ""

        descricao, placa, codigo = row
        identificacao = placa or codigo
        if identificacao:
            return f"{descricao} ({identificacao})"
        return descricao or ""
    finally:
        conn.close()

@app.route("/baixar_csv_transacoes", methods=["POST"])
@login_required
def baixar_csv_transacoes():
    try:
        filters = request.get_json() or {}
        data = _buscar_transacoes_para_export(filters)

        output = io.StringIO(newline="")
        writer = csv.writer(
            output,
            delimiter=';',
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            lineterminator='\n'
        )

        writer.writerow(["Data", "UVR", "Tipo", "Origem/Destino", "Nº Doc.", "Valor Total", "Status"])

        for row in data:
            data_fmt = row["data_documento"].strftime('%d/%m/%Y') if row["data_documento"] else "-"
            valor_fmt = _format_decimal(str(row["valor_total_documento"]))
            writer.writerow([
                data_fmt,
                row["uvr"],
                row["tipo_transacao"],
                row["nome_cadastro_origem"] or "-",
                row["numero_documento"],
                valor_fmt,
                row["status_pagamento"]
            ])

        filename = f"transacoes_{filters.get('uvr','todas')}_{filters.get('data_inicial','inicio')}_a_{filters.get('data_final','fim')}.csv"
        response = Response(
            '\ufeff' + output.getvalue(),
            mimetype="text/csv; charset=utf-8-sig"
        )
        response.headers["Content-Disposition"] = f"attachment; filename={filename}"
        return response

    except Exception as e:
        app.logger.error(f"Erro em /baixar_csv_transacoes: {e}", exc_info=True)
        return jsonify({"error": f"Erro ao gerar CSV: {str(e)}"}), 500

@app.route("/baixar_pdf_transacoes", methods=["POST"])
@login_required
def baixar_pdf_transacoes():
    try:
        filters = request.get_json() or {}
        data = _buscar_transacoes_para_export(filters)

        buffer = io.BytesIO()
        doc = SimpleDocTemplate(
            buffer, pagesize=landscape(A4), topMargin=1.5*inch, bottomMargin=1*inch,
            leftMargin=0.5*inch, rightMargin=0.5*inch
        )
        styles = getSampleStyleSheet()
        style_body = ParagraphStyle('BodyText', parent=styles['Normal'], alignment=TA_LEFT, leading=9, fontSize=8)
        style_center = ParagraphStyle('BodyTextCenter', parent=style_body, alignment=TA_CENTER)
        style_right = ParagraphStyle('BodyTextRight', parent=style_body, alignment=TA_RIGHT)
        style_header_table = ParagraphStyle('TableHeader', parent=style_body, fontName='Helvetica-Bold', alignment=TA_CENTER)

        subtitle_parts = []
        if filters.get("uvr"):
            subtitle_parts.append(f"UVR: {filters.get('uvr')}")
        if filters.get("tipo"):
            subtitle_parts.append(f"Tipo: {filters.get('tipo')}")

        frota_nome = (filters.get("nome_frota") or "").strip()
        if not frota_nome:
            frota_nome = _buscar_nome_frota_por_id(filters.get("id_patrimonio"))
        if frota_nome:
            subtitle_parts.append(f"Frota: {frota_nome}")

        if filters.get("data_inicial") or filters.get("data_final"):
            subtitle_parts.append(f"Período: {filters.get('data_inicial','-')} a {filters.get('data_final','-')}")
        subtitle_pdf = " | ".join(subtitle_parts)

        header_row = [
            Paragraph("Data", style_header_table),
            Paragraph("UVR", style_header_table),
            Paragraph("Tipo", style_header_table),
            Paragraph("Origem/Destino", style_header_table),
            Paragraph("Nº Doc.", style_header_table),
            Paragraph("Valor Total", style_header_table),
            Paragraph("Status", style_header_table)
        ]

        table_data = [header_row]
        for row in data:
            data_fmt = row["data_documento"].strftime('%d/%m/%Y') if row["data_documento"] else "-"
            table_data.append([
                Paragraph(data_fmt, style_center),
                Paragraph(row["uvr"], style_center),
                Paragraph(row["tipo_transacao"], style_center),
                Paragraph(row["nome_cadastro_origem"] or "-", style_body),
                Paragraph(row["numero_documento"], style_center),
                Paragraph(_format_decimal(str(row["valor_total_documento"])), style_right),
                Paragraph(row["status_pagamento"], style_center)
            ])

        report_table = Table(table_data, repeatRows=1)
        report_table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), colors.lightgrey),
            ('GRID', (0,0), (-1,-1), 0.25, colors.grey),
            ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
            ('FONTSIZE', (0,0), (-1,-1), 8),
        ]))

        story = [report_table]
        doc.build(
            story,
            onFirstPage=lambda c, d: _create_pdf_header_footer(c, d, "Relatório de Transações", subtitle_pdf),
            onLaterPages=lambda c, d: _create_pdf_header_footer(c, d, "Relatório de Transações", subtitle_pdf)
        )

        buffer.seek(0)
        filename = f"transacoes_{filters.get('uvr','todas')}_{filters.get('data_inicial','inicio')}_a_{filters.get('data_final','fim')}.pdf"
        return send_file(
            buffer,
            mimetype='application/pdf',
            as_attachment=True,
            download_name=filename
        )
    except Exception as e:
        app.logger.error(f"Erro em /baixar_pdf_transacoes: {e}", exc_info=True)
        return jsonify({"error": f"Erro ao gerar PDF: {str(e)}"}), 500

@app.route("/get_transacao_detalhes/<int:id>", methods=["GET"])
@login_required
def get_transacao_detalhes(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        # 1. Busca os dados gerais da transação
        cur.execute("""
            SELECT tf.id, tf.uvr, tf.associacao, tf.tipo_transacao, tf.tipo_atividade,
                   tf.id_cadastro_origem, tf.nome_cadastro_origem, tf.numero_documento, tf.data_documento,
                   tf.valor_total_documento, tf.status_pagamento, tf.data_hora_registro,
                   tf.id_patrimonio, p.descricao, p.placa, p.codigo_patrimonio,
                   p.controle_por, p.medidor_atual,
                   tf.categoria_despesa_patrimonio, tf.medidor_atual, tf.tipo_medidor,
                   tf.id_motorista, tf.nome_motorista, tf.litros, tf.tipo_combustivel,
                   tf.tipo_manutencao, tf.garantia_km, tf.garantia_data, tf.proxima_revisao_km,
                   tf.proxima_revisao_data
            FROM transacoes_financeiras tf
            LEFT JOIN patrimonio p ON tf.id_patrimonio = p.id
            WHERE tf.id = %s
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
        data_doc = cabecalho[8].strftime('%d/%m/%Y') if cabecalho[8] else "-"
        
        dados_retorno = {
            "id": cabecalho[0],
            "uvr": cabecalho[1],
            "tipo": cabecalho[3],
            "atividade": cabecalho[4],
            "id_origem": cabecalho[5],
            "origem": cabecalho[6],
            "doc": cabecalho[7] or "-",
            "data": data_doc,
            "valor_total": float(cabecalho[9]),
            "status": cabecalho[10],
            "id_patrimonio": cabecalho[12],
            "patrimonio": cabecalho[13] or "",
            "patrimonio_placa": cabecalho[14] or "",
            "patrimonio_codigo": cabecalho[15] or "",
            "patrimonio_controle": cabecalho[16] or "",
            "patrimonio_medidor": float(cabecalho[17]) if cabecalho[17] is not None else None,
            "categoria_despesa_patrimonio": cabecalho[18] or "",
            "medidor_atual": float(cabecalho[19]) if cabecalho[19] is not None else None,
            "tipo_medidor": cabecalho[20] or "",
            "id_motorista": cabecalho[21],
            "nome_motorista": cabecalho[22] or "",
            "litros": float(cabecalho[23]) if cabecalho[23] is not None else None,
            "tipo_combustivel": cabecalho[24] or "",
            "tipo_manutencao": cabecalho[25] or "",
            "garantia_km": cabecalho[26],
            "garantia_data": cabecalho[27].strftime('%Y-%m-%d') if cabecalho[27] else "",
            "proxima_revisao_km": cabecalho[28],
            "proxima_revisao_data": cabecalho[29].strftime('%Y-%m-%d') if cabecalho[29] else "",
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
    if request.method != "GET":
        bloqueio = bloquear_visitante()
        if bloqueio:
            return bloqueio
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
    if request.method != "GET":
        bloqueio = bloquear_visitante()
        if bloqueio:
            return bloqueio
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
@app.route('/excluir_transacao/<int:id>', methods=['POST'])
@login_required
def excluir_transacao(id):
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    # Segurança: Apenas Admin pode excluir (opcional, remova o if se quiser liberar)
    if current_user.role != 'admin':
        return jsonify({'error': 'Permissão negada. Apenas administradores podem excluir.'}), 403

    conn = conectar_banco()
    cur = conn.cursor()
    try:
        # 1. Primeiro exclui os itens da transação (para não dar erro de chave estrangeira)
        cur.execute("DELETE FROM itens_transacao WHERE id_transacao = %s", (id,))
        
        # 2. Depois exclui a transação principal
        cur.execute("DELETE FROM transacoes_financeiras WHERE id = %s", (id,))
        
        conn.commit()
        return jsonify({'message': 'Transação excluída com sucesso!'}), 200
    except Exception as e:
        conn.rollback()
        app.logger.error(f"Erro ao excluir transação {id}: {e}")
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()

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
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
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
# --- GESTÃO DE PATRIMÔNIO / FROTA ---

@app.route("/cadastrar_patrimonio", methods=["POST"])
@login_required
def cadastrar_patrimonio():
    conn = None
    try:
        dados = request.form
        
        # Checkboxes
        permite_abast = True if dados.get("permite_abastecimento") else False
        permite_manut = True if dados.get("permite_manutencao") else False
        bem_publico = True if dados.get("eh_bem_publico") else False
        uso_compartilhado = True if dados.get("uso_compartilhado") else False
        
        # Foto
        foto_final = ""
        foto_webcam = dados.get("foto_bem_base64_webcam")
        if foto_webcam and "data:image" in foto_webcam:
            foto_final = _upload_base64_to_cloudinary(
                foto_webcam,
                folder="patrimonio",
            ) or foto_webcam
        elif 'foto_bem_upload' in request.files:
            arquivo = request.files['foto_bem_upload']
            if arquivo and arquivo.filename:
                foto_final = _upload_file_to_cloudinary(
                    arquivo,
                    folder="patrimonio",
                    resource_type="image",
                )
                if not foto_final:
                    conteudo = arquivo.read()
                    encoded = base64.b64encode(conteudo).decode('utf-8')
                    mime = arquivo.content_type or "image/jpeg"
                    foto_final = f"data:{mime};base64,{encoded}"

        def data_or_none(d): return d if d else None
        
        conn = conectar_banco()
        cur = conn.cursor()
        
        cur.execute("""
            INSERT INTO patrimonio (
                uvr, associacao, tipo_bem, categoria, descricao, codigo_patrimonio, marca, modelo,
                ano_fabricacao, numero_serie_chassi, situacao_propriedade, entidade_proprietaria,
                orgao_cedente, numero_termo_comodato, data_inicio_comodato, data_fim_comodato,
                placa, renavam, combustivel, capacidade_carga, controle_por, medidor_inicial, medidor_atual,
                local_instalacao, setor_uso, nome_responsavel, nome_operador_principal,
                status_bem, estado_conservacao, permite_abastecimento, permite_manutencao,
                alerta_preventiva, observacoes_gerais, foto_bem_base64, eh_bem_publico, uso_compartilhado
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            dados["uvr_patrimonio"], dados.get("associacao_patrimonio",""), dados["tipo_bem"], dados["categoria_bem"],
            dados["descricao_bem"], dados["codigo_patrimonio"], dados["marca_bem"], dados["modelo_bem"],
            dados["ano_fabricacao"] or 0, dados["serie_chassi"], dados["situacao_propriedade"], dados["entidade_proprietaria"],
            dados["orgao_cedente"], dados["num_termo"], data_or_none(dados["data_inicio_comodato"]), data_or_none(dados["data_fim_comodato"]),
            dados["placa"], dados["renavam"], dados["combustivel"], dados["capacidade_carga"], dados["controle_por"],
            dados["medidor_inicial"] or 0, dados["medidor_inicial"] or 0,
            dados["local_instalacao"], dados["setor_uso"], dados["nome_responsavel"], dados["nome_operador"],
            dados["status_bem"], dados["estado_conservacao"], permite_abast, permite_manut,
            dados["alerta_preventiva"] or 0, dados["observacoes_gerais"], foto_final, bem_publico, uso_compartilhado
        ))
        conn.commit()
        return pagina_sucesso_base("Sucesso", "Bem/Patrimônio cadastrado com sucesso!")
        
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao cadastrar patrimônio: {e}")
        return f"Erro: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/buscar_patrimonio", methods=["GET"])
@login_required
def buscar_patrimonio():
    conn = None
    try:
        termo = request.args.get("q", "").lower()
        categoria = request.args.get("categoria", "")
        uvr = request.args.get("uvr", "")

        sql = "SELECT id, descricao, tipo_bem, placa, status_bem, nome_responsavel, medidor_atual, controle_por, categoria, codigo_patrimonio FROM patrimonio WHERE 1=1"
        params = []
        
        if current_user.role == 'admin':
            if uvr and uvr != "Todas":
                sql += " AND uvr = %s"
                params.append(uvr)
        elif current_user.uvr_acesso:
            sql += " AND uvr = %s"
            params.append(current_user.uvr_acesso)
            
        if categoria and categoria != "Todos":
            sql += " AND categoria = %s"
            params.append(categoria)
            
        if termo:
            sql += " AND (LOWER(descricao) LIKE %s OR placa LIKE %s OR codigo_patrimonio LIKE %s)"
            params.extend([f"%{termo}%", f"%{termo}%", f"%{termo}%"])
            
        sql += " ORDER BY descricao ASC"
        
        conn = conectar_banco()
        cur = conn.cursor()
        cur.execute(sql, tuple(params))
        res = []
        for r in cur.fetchall():
            medida = f"{float(r[6]):.0f} {r[7]}" if r[6] is not None else "-"
            # r[3] é placa, r[9] é código
            identificacao = r[3] if r[3] else (r[9] or "-")
            
            res.append({
                "id": r[0], "descricao": r[1], "tipo": r[2], "placa": identificacao,
                "status": r[4], "responsavel": r[5] or "-", "medidor": medida, "categoria": r[8],
                "medidor_atual": float(r[6]) if r[6] is not None else None, "controle_por": r[7]
            })
        return jsonify(res)
    except Exception as e:
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/get_patrimonio_detalhes/<int:id>")
@login_required
def get_patrimonio_detalhes(id):
    conn = conectar_banco()
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM patrimonio WHERE id = %s", (id,))
        if cur.rowcount == 0: return jsonify({"error": "Não encontrado"}), 404
        
        # Mapeia colunas dinamicamente
        columns = [desc[0] for desc in cur.description]
        row = cur.fetchone()
        data = dict(zip(columns, row))
        
        # Formata datas e decimais para JSON
        for k, v in data.items():
            if isinstance(v, (date, datetime)): data[k] = v.strftime('%Y-%m-%d')
            if isinstance(v, Decimal): data[k] = float(v)
            
        return jsonify(data)
    finally:
        conn.close()

@app.route("/editar_patrimonio", methods=["POST"])
@login_required
def editar_patrimonio():
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = None
    try:
        dados = request.form
        id_pat = dados.get("id_patrimonio")
        
        # 1. Prepara os dados (com tratamento de tipos)
        permite_abast = True if dados.get("permite_abastecimento") else False
        permite_manut = True if dados.get("permite_manutencao") else False
        bem_publico = True if dados.get("eh_bem_publico") else False
        uso_comp = True if dados.get("uso_compartilhado") else False
        
        def data_or_none(d): return d if d else None

        # Foto (Lógica: Webcam > Upload > Manter Antiga)
        foto_final = None
        foto_webcam = dados.get("foto_bem_base64_webcam")
        if foto_webcam and "data:image" in foto_webcam:
            foto_final = _upload_base64_to_cloudinary(
                foto_webcam,
                folder="patrimonio",
            ) or foto_webcam
        elif 'foto_bem_upload' in request.files:
            arquivo = request.files['foto_bem_upload']
            if arquivo and arquivo.filename:
                foto_final = _upload_file_to_cloudinary(
                    arquivo,
                    folder="patrimonio",
                    resource_type="image",
                )
                if not foto_final:
                    conteudo = arquivo.read()
                    encoded = base64.b64encode(conteudo).decode('utf-8')
                    mime = arquivo.content_type or "image/jpeg"
                    foto_final = f"data:{mime};base64,{encoded}"
        
        # Monta dicionário de dados limpos
        dados_tratados = {
            "uvr": dados["uvr_patrimonio"],
            "associacao": dados.get("associacao_patrimonio",""),
            "tipo_bem": dados["tipo_bem"], "categoria": dados["categoria_bem"],
            "descricao": dados["descricao_bem"], "codigo_patrimonio": dados["codigo_patrimonio"],
            "marca": dados["marca_bem"], "modelo": dados["modelo_bem"],
            "ano_fabricacao": dados["ano_fabricacao"] or 0, "numero_serie_chassi": dados["serie_chassi"],
            "situacao_propriedade": dados["situacao_propriedade"], "entidade_proprietaria": dados["entidade_proprietaria"],
            "orgao_cedente": dados["orgao_cedente"], "numero_termo_comodato": dados["num_termo"],
            "data_inicio_comodato": data_or_none(dados["data_inicio_comodato"]),
            "data_fim_comodato": data_or_none(dados["data_fim_comodato"]),
            "placa": dados["placa"], "renavam": dados["renavam"],
            "combustivel": dados["combustivel"], "capacidade_carga": dados["capacidade_carga"],
            "controle_por": dados["controle_por"], "medidor_inicial": dados["medidor_inicial"] or 0,
            "local_instalacao": dados["local_instalacao"], "setor_uso": dados["setor_uso"],
            "nome_responsavel": dados["nome_responsavel"], "nome_operador_principal": dados["nome_operador"],
            "status_bem": dados["status_bem"], "estado_conservacao": dados["estado_conservacao"],
            "permite_abastecimento": permite_abast, "permite_manutencao": permite_manut,
            "alerta_preventiva": dados["alerta_preventiva"] or 0, "observacoes_gerais": dados["observacoes_gerais"],
            "eh_bem_publico": bem_publico, "uso_compartilhado": uso_comp
        }
        
        if foto_final:
            dados_tratados["foto_bem_base64"] = foto_final

        conn = conectar_banco()
        cur = conn.cursor()

        # --- FLUXO DE DECISÃO ---
        if current_user.role == 'admin':
            # ADMIN: UPDATE DIRETO
            campos_sql = ", ".join([f"{k}=%s" for k in dados_tratados.keys()])
            valores = list(dados_tratados.values())
            valores.append(id_pat)
            
            cur.execute(f"UPDATE patrimonio SET {campos_sql} WHERE id=%s", tuple(valores))
            conn.commit()
            return pagina_sucesso_base("Sucesso", "Patrimônio atualizado com sucesso!")
        else:
            # USUÁRIO: SOLICITAÇÃO
            # Adiciona nome para facilitar visualização na lista de aprovação
            dados_tratados["nome_visual"] = f"{dados_tratados['descricao']} ({dados_tratados['placa'] or dados_tratados['codigo_patrimonio']})"
            
            # Serializa dados (converter date para string)
            def json_serial(obj):
                if isinstance(obj, (datetime, date)): return obj.isoformat()
                return str(obj)

            cur.execute("""
                INSERT INTO solicitacoes_alteracao 
                (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante) 
                VALUES (%s, %s, %s, %s, %s)
            """, ('patrimonio', id_pat, 'EDICAO', json.dumps(dados_tratados, default=json_serial), current_user.username))
            
            conn.commit()
            return pagina_sucesso_base("Solicitação Enviada", "As alterações foram enviadas para aprovação do administrador.")

    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro edição patrimonio: {e}")
        return f"Erro: {e}", 500
    finally:
        if conn: conn.close()

@app.route("/excluir_patrimonio/<int:id>", methods=["POST"])
@login_required
def excluir_patrimonio(id):
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # Busca descrição para o log
        cur.execute("SELECT descricao FROM patrimonio WHERE id=%s", (id,))
        row = cur.fetchone()
        desc = row[0] if row else "Item Desconhecido"

        if current_user.role == 'admin':
            cur.execute("DELETE FROM patrimonio WHERE id=%s", (id,))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Item excluído permanentemente."})
        else:
            # Solicitação
            dados_json = json.dumps({"motivo": "Solicitado pelo usuário", "nome_visual": desc})
            cur.execute("""
                INSERT INTO solicitacoes_alteracao 
                (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante) 
                VALUES (%s, %s, %s, %s, %s)
            """, ('patrimonio', id, 'EXCLUSAO', dados_json, current_user.username))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Solicitação de exclusão enviada para aprovação."})
            
    except Exception as e:
        if conn: conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()
@app.route('/baixar_relatorio_financeiro')
@login_required
def baixar_relatorio_financeiro():
    formato = request.args.get('formato')
    data_ini = request.args.get('data_inicial')
    data_final = request.args.get('data_final')
    tipo = request.args.get('tipo')
    uvr_solicitada = request.args.get('uvr')

    # Segurança de UVR
    if current_user.role == 'admin':
        uvr_filtro = uvr_solicitada
    else:
        uvr_filtro = current_user.uvr_acesso

    conn = conectar_banco()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # --- CORREÇÃO AQUI: Trocamos 'produtos_servicos' por 'produtos' ---
    sql = """
        SELECT 
            t.data_documento,
            t.uvr,
            t.tipo_transacao,
            t.nome_cadastro_origem,
            t.numero_documento,
            it.valor_total_item,
            COALESCE(p.grupo, 'Outros') as grupo,
            COALESCE(p.subgrupo, 'Geral') as subgrupo,
            it.descricao as item_desc
        FROM transacoes_financeiras t
        LEFT JOIN itens_transacao it ON t.id = it.id_transacao
        LEFT JOIN produtos p ON it.descricao = p.nome 
        WHERE 1=1
    """
    # ------------------------------------------------------------------

    params = []

    if uvr_filtro and uvr_filtro != "Todas" and uvr_filtro != "":
        sql += " AND t.uvr = %s"
        params.append(uvr_filtro)
    if data_ini:
        sql += " AND t.data_documento >= %s"
        params.append(data_ini)
    if data_final:
        sql += " AND t.data_documento <= %s"
        params.append(data_final)
    if tipo and tipo != "Todas":
        sql += " AND t.tipo_transacao = %s"
        params.append(tipo)

    sql += " ORDER BY t.data_documento DESC"
    
    cur.execute(sql, tuple(params))
    dados = cur.fetchall()
    conn.close()

    # --- GERAR CSV ---
    if formato == 'csv':
        si = io.StringIO()
        cw = csv.writer(si, delimiter=';') # Ponto e vírgula para Excel BR
        cw.writerow(['Data', 'UVR', 'Tipo', 'Origem', 'N Doc', 'Grupo', 'Subgrupo', 'Item', 'Valor'])
        
        for row in dados:
            data_fmt = row['data_documento'].strftime('%d/%m/%Y') if row['data_documento'] else '-'
            # Converte Decimal para string com vírgula
            valor_dec = row['valor_total_item']
            if isinstance(valor_dec, Decimal):
                valor_fmt = f"{valor_dec:.2f}".replace('.', ',')
            else:
                valor_fmt = str(valor_dec).replace('.', ',')

            cw.writerow([
                data_fmt, row['uvr'], row['tipo_transacao'], row['nome_cadastro_origem'],
                row['numero_documento'], row['grupo'], row['subgrupo'], row['item_desc'], valor_fmt
            ])
            
        output = make_response(si.getvalue())
        output.headers["Content-Disposition"] = f"attachment; filename=relatorio_{data_ini}_{data_final}.csv"
        output.headers["Content-type"] = "text/csv; charset=utf-8-sig" # utf-8-sig para acentos no Excel
        return output

    # --- GERAR PDF (Simples) ---
    elif formato == 'pdf':
        buffer = io.BytesIO()
        p = canvas.Canvas(buffer, pagesize=A4)

        width, height = A4
        y = height - 50

        p.setFont("Helvetica-Bold", 14)
        p.drawString(30, y, f"Relatório Financeiro ({data_ini} a {data_final})")
        y -= 30
        p.setFont("Helvetica", 9)

        # Cabeçalho
        p.drawString(30, y, "Data")
        p.drawString(85, y, "Tipo")
        p.drawString(140, y, "Descrição / Origem")
        p.drawString(450, y, "Valor (R$)")
        y -= 20
        p.line(30, y+15, 550, y+15)

        total_receitas = 0.0
        total_despesas = 0.0
        for row in dados:
            if y < 50: # Nova página se acabar o espaço
                p.showPage()
                y = height - 50
                p.setFont("Helvetica", 9)
            
            data_fmt = row['data_documento'].strftime('%d/%m/%Y') if row['data_documento'] else '-'
            valor = float(row['valor_total_item']) if row['valor_total_item'] else 0.0
            if row['tipo_transacao'] == 'Receita':
                total_receitas += valor
            elif row['tipo_transacao'] == 'Despesa':
                total_despesas += valor
            
            p.drawString(30, y, data_fmt)
            p.drawString(85, y, (row['tipo_transacao'] or '')[:10])
            # Corta texto longo para não invadir o valor
            origem_texto = (row['nome_cadastro_origem'] or '')[:50]
            p.drawString(140, y, origem_texto)
            
            p.drawRightString(550, y, f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
            y -= 15

        y -= 10
        p.line(30, y+15, 550, y+15)
        p.setFont("Helvetica-Bold", 12)
        saldo_periodo = total_receitas - total_despesas
        p.drawString(350, y, "TOTAL RECEITAS:")
        p.drawRightString(550, y, f"R$ {total_receitas:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        y -= 18
        p.drawString(350, y, "TOTAL DESPESAS:")
        p.drawRightString(550, y, f"R$ {total_despesas:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        y -= 18
        p.drawString(350, y, "SALDO DO PERÍODO:")
        p.drawRightString(550, y, f"R$ {saldo_periodo:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))

        p.showPage()
        p.save()
        buffer.seek(0)
        return send_file(buffer, as_attachment=True, download_name=f"relatorio_{data_ini}_{data_final}.pdf", mimetype='application/pdf')

    return "Formato inválido", 400
@app.route('/criar_tabela_produtos_fix')
def criar_tabela_produtos_fix():
    conn = conectar_banco()
    cur = conn.cursor()
    log = []
    try:
        # 1. Garante que a tabela nova existe
        cur.execute("""
            CREATE TABLE IF NOT EXISTS produtos (
                id SERIAL PRIMARY KEY,
                nome VARCHAR(255) NOT NULL,
                grupo VARCHAR(100),
                subgrupo VARCHAR(100),
                unidade VARCHAR(20),
                valor_padrao DECIMAL(10,2),
                tipo VARCHAR(50),
                uvr VARCHAR(20)
            );
        """)
        log.append("✅ Tabela 'produtos' verificada.")

        # 2. Migração Cirúrgica (Baseada nas colunas que você me passou)
        # Mapeamento: item -> nome, grupo -> grupo, subgrupo -> subgrupo, valor_padrao -> valor_padrao, tipo -> tipo
        # Como não tem 'unidade' na antiga, vamos definir como 'UN' por padrão.
        
        cur.execute("""
            INSERT INTO produtos (nome, grupo, subgrupo, valor_padrao, tipo, unidade)
            SELECT 
                item,             -- Vai para 'nome'
                grupo,            -- Vai para 'grupo'
                subgrupo,         -- Vai para 'subgrupo'
                valor_padrao,     -- Vai para 'valor_padrao'
                tipo,             -- Vai para 'tipo'
                'UN'              -- Unidade padrão
            FROM produtos_servicos 
            WHERE item NOT IN (SELECT nome FROM produtos);
        """)
        
        linhas_copiadas = cur.rowcount
        log.append(f"🚀 Sucesso! {linhas_copiadas} produtos foram migrados da tabela antiga para a nova.")
        
        conn.commit()
        return f"<h1>Migração Concluída!</h1><p>{'<br>'.join(log)}</p><br><a href='/'>Voltar e Testar Relatórios</a>"

    except Exception as e:
        conn.rollback()
        return f"<h1>Erro na Migração:</h1><p>{str(e)}</p>"
    finally:
        conn.close()

# Configuração dos Tipos de Documentos e suas regras
# Formato: 'Nome': {'categoria': 'X', 'pede_validade': T/F, 'pede_valor': T/F, 'pede_competencia': T/F}

TIPOS_DOCUMENTOS = {
    # --- FINANCEIROS ---
    'Nota Fiscal de Serviço': {'cat': 'Financeiro', 'validade': False, 'valor': True, 'comp': True, 'num': True},
    'Medição Mensal':         {'cat': 'Financeiro', 'validade': False, 'valor': False, 'comp': True, 'num': True},
    
    # --- RH / ASSOCIADOS ---
    'Relatório de Associados':{'cat': 'RH', 'validade': False, 'valor': False, 'comp': True, 'num': False},
    'GPS - INSS':             {'cat': 'RH', 'validade': False, 'valor': True, 'comp': True, 'num': False},
    'Recibo de Rateio':       {'cat': 'RH', 'validade': False, 'valor': True, 'comp': True, 'num': False},
    
    # --- BANCÁRIOS ---
    'Extrato Bancário':       {'cat': 'Bancário', 'validade': False, 'valor': False, 'comp': True, 'num': False},
    
    # --- AMBIENTAL ---
    'MTR - Manifesto':        {'cat': 'Ambiental', 'validade': False, 'valor': False, 'comp': True, 'num': True},
    'CDF - Certificado':      {'cat': 'Ambiental', 'validade': False, 'valor': False, 'comp': True, 'num': True},
    'Relatório fotográfico da carga': {'cat': 'Operacional', 'validade': False, 'valor': False, 'comp': True, 'num': False},
    
    # --- CERTIDÕES (GERAL) ---
    'Certidão Municipal':     {'cat': 'Fiscal', 'validade': True, 'valor': False, 'comp': False, 'num': True},
    'Certidão Estadual':      {'cat': 'Fiscal', 'validade': True, 'valor': False, 'comp': False, 'num': True},
    'Certidão Federal':       {'cat': 'Fiscal', 'validade': True, 'valor': False, 'comp': False, 'num': True},
    'CNDT (Trabalhista)':     {'cat': 'Fiscal', 'validade': True, 'valor': False, 'comp': False, 'num': True},
    'FGTS':                   {'cat': 'Fiscal', 'validade': True, 'valor': False, 'comp': False, 'num': True}
}

def _montar_consulta_documentos(args, usuario):
    f_uvr = args.get('filtro_uvr', '')
    f_mes_inicio = args.get('mes_inicio', '')
    f_mes_fim = args.get('mes_fim', '')
    f_tipo = args.get('tipo_documento', '')
    f_status = args.get('status', '')
    f_nome = args.get('filtro_nome', '').strip()

    sql = """
        SELECT DISTINCT d.*, t.nome AS nome_tipo, t.categoria,
            COALESCE(tf.nome_cadastro_origem, c.razao_social, a.nome) AS origem_destino,
            COALESCE(c.cnpj, a.cpf) AS origem_documento
        FROM documentos d
        JOIN tipos_documentos t ON d.id_tipo = t.id
        LEFT JOIN transacoes_financeiras tf
            ON tf.uvr = d.uvr
           AND tf.numero_documento = d.numero_referencia
        LEFT JOIN cadastros c ON tf.id_cadastro_origem = c.id
        LEFT JOIN associados a ON tf.id_cadastro_origem = a.id
        WHERE 1=1
    """
    params = []

    if usuario.role != 'admin':
        sql += " AND d.uvr = %s"
        params.append(usuario.uvr_acesso)
    elif f_uvr:
        sql += " AND d.uvr = %s"
        params.append(f_uvr)

    if f_tipo:
        sql += " AND d.id_tipo = %s"
        params.append(f_tipo)

    if f_status:
        sql += " AND d.status = %s"
        params.append(f_status)

    if f_nome:
        sql += """
            AND (
                d.observacoes ILIKE %s
                OR d.numero_referencia ILIKE %s
                OR d.nome_original ILIKE %s
                OR d.enviado_por ILIKE %s
                OR tf.nome_cadastro_origem ILIKE %s
                OR c.razao_social ILIKE %s
                OR c.cnpj ILIKE %s
                OR a.nome ILIKE %s
                OR a.cpf ILIKE %s
            )
        """
        filtro_nome = f"%{f_nome}%"
        params.extend([
            filtro_nome, filtro_nome, filtro_nome, filtro_nome,
            filtro_nome, filtro_nome, filtro_nome, filtro_nome, filtro_nome
        ])

    filtro_inicio = None
    filtro_fim = None
    if f_mes_inicio:
        try:
            filtro_inicio = datetime.strptime(f_mes_inicio, "%Y-%m").date()
        except ValueError:
            filtro_inicio = None
    if f_mes_fim:
        try:
            mes_fim_data = datetime.strptime(f_mes_fim, "%Y-%m").date()
            ultimo_dia = calendar.monthrange(mes_fim_data.year, mes_fim_data.month)[1]
            filtro_fim = mes_fim_data.replace(day=ultimo_dia)
        except ValueError:
            filtro_fim = None

    if filtro_inicio:
        sql += " AND COALESCE(d.competencia, d.data_envio::date) >= %s"
        params.append(filtro_inicio)

    if filtro_fim:
        sql += " AND COALESCE(d.competencia, d.data_envio::date) <= %s"
        params.append(filtro_fim)

    filtros_template = {
        "uvr": f_uvr,
        "mes_inicio": f_mes_inicio,
        "mes_fim": f_mes_fim,
        "tipo": f_tipo,
        "status": f_status,
        "nome": f_nome
    }

    return sql, params, filtros_template

def _formatar_data_documento(valor, formato="%d/%m/%Y"):
    if not valor:
        return "-"
    if isinstance(valor, (datetime, date)):
        return valor.strftime(formato)
    return str(valor)

def _formatar_competencia_documento(valor):
    if not valor:
        return "-"
    if isinstance(valor, (datetime, date)):
        return valor.strftime("%m/%Y")
    return str(valor)

def _formatar_valor_documento(valor):
    if valor is None or valor == "":
        return "-"
    try:
        valor_float = float(valor)
    except (TypeError, ValueError):
        return str(valor)
    valor_formatado = f"{valor_float:,.2f}"
    return f"R$ {valor_formatado.replace(',', 'X').replace('.', ',').replace('X', '.')}"

def _criar_pdf_cabecalho_documento(doc):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    largura, altura = A4

    c.setFont("Helvetica-Bold", 14)
    c.drawString(40, altura - 50, "Identificação do Documento")
    c.setStrokeColor(colors.HexColor("#cccccc"))
    c.line(40, altura - 58, largura - 40, altura - 58)

    c.setFont("Helvetica", 10)
    campos = [
        ("Documento", doc.get("nome_tipo")),
        ("Categoria", doc.get("categoria")),
        ("Unidade (UVR)", doc.get("uvr")),
        ("Enviado em", _formatar_data_documento(doc.get("data_envio"))),
        ("Competência", _formatar_competencia_documento(doc.get("competencia"))),
        ("Validade", _formatar_data_documento(doc.get("data_validade"))),
        ("Valor", _formatar_valor_documento(doc.get("valor"))),
        ("Nº Referência", doc.get("numero_referencia") or "-"),
        ("Origem/Destino", doc.get("origem_destino") or "-"),
        ("Origem Documento", doc.get("origem_documento") or "-"),
        ("Status", doc.get("status") or "-"),
    ]

    y = altura - 90
    for titulo, valor in campos:
        c.setFont("Helvetica-Bold", 10)
        c.drawString(40, y, f"{titulo}:")
        c.setFont("Helvetica", 10)
        c.drawString(160, y, str(valor))
        y -= 16
        if y < 80:
            c.showPage()
            y = altura - 60

    c.showPage()
    c.save()
    buffer.seek(0)
    return buffer

def _criar_pdf_imagem_documento(image_bytes):
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    largura, altura = A4
    margem = 40
    max_largura = largura - 2 * margem
    max_altura = altura - 2 * margem

    imagem = ImageReader(io.BytesIO(image_bytes))
    img_largura, img_altura = imagem.getSize()
    escala = min(max_largura / img_largura, max_altura / img_altura)
    nova_largura = img_largura * escala
    nova_altura = img_altura * escala
    x = (largura - nova_largura) / 2
    y = (altura - nova_altura) / 2

    c.drawImage(imagem, x, y, width=nova_largura, height=nova_altura, preserveAspectRatio=True, mask='auto')
    c.showPage()
    c.save()
    buffer.seek(0)
    return buffer

@app.route("/documentos", methods=['GET', 'POST'])
@login_required
def documentos():
    conn = conectar_banco()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    # =========================================================
    # 1. UPLOAD (POST)
    # =========================================================
    if request.method == 'POST':
        try:
            # --- CONTROLE DE UVR ---
            uvr_form = request.form.get('uvr')
            if current_user.role != 'admin':
                uvr = current_user.uvr_acesso
            else:
                uvr = uvr_form

            # --- CAPTURA DE CAMPOS ---
            id_tipo = request.form.get('tipo_documento')
            competencia = request.form.get('competencia')          # Formato YYYY-MM
            data_validade = request.form.get('data_validade') or None
            valor = request.form.get('valor')
            numero_referencia = request.form.get('numero_referencia')
            observacoes = request.form.get('observacoes')

            # --- TRATAMENTO COMPETÊNCIA ---
            if competencia:
                try:
                    competencia = datetime.strptime(competencia, '%Y-%m').date()
                except ValueError:
                    raise ValueError("Competência inválida. Use o formato AAAA-MM.")

            # --- TRATAMENTO VALOR FINANCEIRO ---
            if valor:
                valor = valor.replace('.', '').replace(',', '.')
            else:
                valor = None

            # --- PROCESSAMENTO DO ARQUIVO ---
            arquivo = request.files.get('arquivo_upload')
            nome_arquivo_salvo = ""
            nome_original = ""

            if arquivo and arquivo.filename:
                nome_original = arquivo.filename
                import time, os
                timestamp = int(time.time())
                extensao = os.path.splitext(nome_original)[1]
                nome_arquivo_salvo = f"doc_{uvr}_{timestamp}{extensao}"

                file_format = extensao.lstrip('.') if extensao else None


                url_cloud = _upload_file_to_cloudinary(
                    arquivo,
                    folder="documentos",
                    public_id=f"doc_{uvr}_{timestamp}",
                    resource_type="auto",
                    file_format=file_format,
                )
                if url_cloud:
                    nome_arquivo_salvo = url_cloud
                else:
                    if _is_render_env():
                        detalhe = cloudinary_last_error or cloudinary_setup_error or "Erro desconhecido."
                        raise RuntimeError(
                            "Upload no Cloudinary falhou no Render. "
                            f"Verifique CLOUDINARY_URL/CLOUDINARY_* no painel. Detalhe: {detalhe}"
                        )
                    pasta = 'uploads'
                    os.makedirs(pasta, exist_ok=True)
                    arquivo.save(os.path.join(pasta, nome_arquivo_salvo))

            # --- INSERÇÃO NO BANCO ---
            cursor.execute("""
                INSERT INTO documentos
                (uvr, id_tipo, caminho_arquivo, nome_original, competencia,
                 data_validade, valor, numero_referencia, observacoes,
                 enviado_por, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'Pendente')
            """, (
                uvr, id_tipo, nome_arquivo_salvo, nome_original,
                competencia, data_validade, valor,
                numero_referencia, observacoes,
                current_user.username
            ))

            conn.commit()
            flash('Documento enviado com sucesso! Aguardando conferência.', 'success')

        except Exception as e:
            conn.rollback()
            flash(f'Erro ao enviar documento: {str(e)}', 'danger')

        finally:
            cursor.close()
            conn.close()
            return redirect(url_for('documentos'))

    # =========================================================
    # 2. LISTAGEM + FILTROS (GET)
    # =========================================================
    try:
        # Carrega tipos para os selects (Upload e Filtro)
        cursor.execute("SELECT * FROM tipos_documentos ORDER BY categoria, nome")
        tipos_documentos = cursor.fetchall()

        sql, params, filtros_template = _montar_consulta_documentos(request.args, current_user)
        sql += " ORDER BY d.data_envio DESC LIMIT 200"

        cursor.execute(sql, tuple(params))
        documentos = cursor.fetchall()

        status_resumo = None
        if current_user.role != 'admin':
            aprovados = sum(1 for doc in documentos if doc.get('status') == 'Aprovado')
            reprovados = sum(1 for doc in documentos if doc.get('status') == 'Reprovado')
            if aprovados or reprovados:
                status_resumo = {
                    "aprovados": aprovados,
                    "reprovados": reprovados,
                }
        else:
            pendentes = sum(1 for doc in documentos if doc.get('status') == 'Pendente')
            if pendentes:
                status_resumo = {
                    "pendentes": pendentes,
                }

        # Conversão de datas para exibição formatada na tabela
        for doc in documentos:
            if isinstance(doc.get('data_envio'), str):
                try:
                    # Remove microsegundos se existirem para evitar erro no parse
                    dt_str = doc['data_envio'].split('.')[0]
                    doc['data_envio'] = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                except: 
                    pass
            if doc.get('caminho_arquivo', '').startswith("http"):
                doc['url_visualizacao'] = _build_cloudinary_delivery_url(doc['caminho_arquivo'])
            # Competência e Validade permanecem como strings para o HTML5 input ler corretamente
            # mas o jinja2 pode formatar na tabela se necessário.

        return render_template(
            'documentos.html',
            documentos=documentos,
            tipos=tipos_documentos,
            filtros=filtros_template, # Essencial para manter os filtros na tela
            status_resumo=status_resumo,
            now=date.today()
        )

    except Exception as e:
        app.logger.error(f"Erro GET /documentos: {e}")
        flash('Erro ao carregar lista de documentos.', 'danger')
        return redirect(url_for('index'))

    finally:
        if not cursor.closed:
            cursor.close()
        if not conn.closed:
            conn.close()

@app.route("/documentos/gerar_pdf_unico", methods=["POST"])
@login_required
def gerar_pdf_unico_documentos():
    conn = conectar_banco()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    def obter_bytes_documento(doc):
        caminho = doc.get("caminho_arquivo")
        if not caminho:
            return None, None
        if str(caminho).startswith("http"):
            url = _build_cloudinary_delivery_url(caminho)
            resposta = requests.get(url, timeout=30)
            resposta.raise_for_status()
            return resposta.content, caminho
        caminho_local = caminho
        if not os.path.isabs(caminho_local):
            caminho_local = os.path.join("uploads", caminho_local)
        if not os.path.exists(caminho_local):
            return None, caminho_local
        with open(caminho_local, "rb") as arquivo_local:
            return arquivo_local.read(), caminho_local

    def obter_extensao_documento(doc, referencia):
        candidatos = [
            doc.get("nome_original"),
            referencia,
            doc.get("caminho_arquivo")
        ]
        for item in candidatos:
            if not item:
                continue
            caminho_sem_query = str(item).split("?", 1)[0]
            ext = os.path.splitext(caminho_sem_query)[1].lower()
            if ext:
                return ext
        return ""

    try:
        sql, params, filtros_template = _montar_consulta_documentos(request.form, current_user)
        sql += " ORDER BY d.data_envio DESC"
        cursor.execute(sql, tuple(params))
        documentos = cursor.fetchall()

        if not documentos:
            flash("Nenhum documento encontrado para gerar o PDF.", "warning")
            return redirect(url_for("documentos", **filtros_template))

        writer = PdfWriter()
        documentos_processados = 0

        for doc in documentos:
            if isinstance(doc.get('data_envio'), str):
                try:
                    dt_str = doc['data_envio'].split('.')[0]
                    doc['data_envio'] = datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')
                except Exception:
                    pass

            try:
                bytes_doc, referencia = obter_bytes_documento(doc)
            except Exception as exc:
                app.logger.error(f"Erro ao baixar documento {doc.get('id')}: {exc}")
                continue

            if not bytes_doc:
                app.logger.warning(f"Documento sem arquivo disponível: {doc.get('id')}")
                continue

            extensao = obter_extensao_documento(doc, referencia)

            header_buffer = _criar_pdf_cabecalho_documento(doc)
            header_reader = PdfReader(header_buffer)
            for pagina in header_reader.pages:
                writer.add_page(pagina)

            if extensao == ".pdf":
                try:
                    reader = PdfReader(io.BytesIO(bytes_doc))
                    for pagina in reader.pages:
                        writer.add_page(pagina)
                except Exception as exc:
                    app.logger.error(f"Erro ao ler PDF do documento {doc.get('id')}: {exc}")
                    continue
            else:
                try:
                    image_pdf = _criar_pdf_imagem_documento(bytes_doc)
                    reader = PdfReader(image_pdf)
                    for pagina in reader.pages:
                        writer.add_page(pagina)
                except Exception as exc:
                    app.logger.error(f"Erro ao converter imagem do documento {doc.get('id')}: {exc}")
                    continue

            documentos_processados += 1

        if documentos_processados == 0:
            flash("Nenhum anexo válido foi encontrado para gerar o PDF.", "warning")
            return redirect(url_for("documentos", **filtros_template))

        buffer = io.BytesIO()
        writer.write(buffer)
        buffer.seek(0)

        nome_arquivo = f"documentos_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        return send_file(buffer, as_attachment=True, download_name=nome_arquivo, mimetype="application/pdf")

    except Exception as e:
        app.logger.error(f"Erro ao gerar PDF único de documentos: {e}", exc_info=True)
        flash("Erro ao gerar PDF único. Tente novamente.", "danger")
        return redirect(url_for("documentos"))
    finally:
        if not cursor.closed:
            cursor.close()
        if not conn.closed:
            conn.close()

@app.route('/editar_documento', methods=['POST'])
@login_required
def editar_documento():
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    id_doc = request.form.get('id_documento')

    if not id_doc:
        flash('Documento inválido.', 'danger')
        return redirect(url_for('documentos'))

    competencia = request.form.get('competencia') or None
    data_validade = request.form.get('data_validade') or None
    numero_referencia = request.form.get('numero_referencia')
    observacoes = request.form.get('observacoes')
    valor = request.form.get('valor')

    if competencia:
        try:
            competencia = datetime.strptime(competencia, '%Y-%m').date()
        except ValueError:
            flash("Competência inválida. Use o formato AAAA-MM.", 'danger')
            return redirect(url_for('documentos'))

    valor = valor.replace('.', '').replace(',', '.') if valor else None

    arquivo = request.files.get('arquivo_upload')
    novo_arquivo = None

    conn = conectar_banco()
    cursor = conn.cursor(cursor_factory=RealDictCursor)

    try:
        if arquivo and arquivo.filename:
            cursor.execute("SELECT caminho_arquivo, uvr FROM documentos WHERE id=%s", (id_doc,))
            doc = cursor.fetchone()

            if doc:


                if doc['caminho_arquivo']:
                    if doc['caminho_arquivo'].startswith("http"):
                        _delete_cloudinary_asset(
                            doc['caminho_arquivo'],
                            resource_type=_detect_cloudinary_resource_type(doc['caminho_arquivo']),
                        )
                    else:
                        if not os.path.exists('uploads'):
                            os.makedirs('uploads')

                        antigo = os.path.join('uploads', doc['caminho_arquivo'])
                        if os.path.exists(antigo):
                            os.remove(antigo)

                ext = arquivo.filename.rsplit('.', 1)[1]
                novo_arquivo = f"doc_{doc['uvr']}_{int(datetime.now().timestamp())}.{ext}"

                url_cloud = _upload_file_to_cloudinary(
                    arquivo,
                    folder="documentos",
                    public_id=novo_arquivo.rsplit(".", 1)[0],
                    resource_type="raw",
                    file_format=ext,
                )
                if url_cloud:
                    novo_arquivo = url_cloud
                else:
                    if not os.path.exists('uploads'):
                        os.makedirs('uploads')
                    arquivo.save(os.path.join('uploads', novo_arquivo))

        if novo_arquivo:
            cursor.execute("""
                UPDATE documentos SET
                    competencia=%s,
                    data_validade=%s,
                    valor=%s,
                    numero_referencia=%s,
                    observacoes=%s,
                    caminho_arquivo=%s,
                    status='Pendente',
                    motivo_rejeicao=NULL
                WHERE id=%s
            """, (competencia, data_validade, valor, numero_referencia, observacoes, novo_arquivo, id_doc))
        else:
            cursor.execute("""
                UPDATE documentos SET
                    competencia=%s,
                    data_validade=%s,
                    valor=%s,
                    numero_referencia=%s,
                    observacoes=%s,
                    status='Pendente',
                    motivo_rejeicao=NULL
                WHERE id=%s
            """, (competencia, data_validade, valor, numero_referencia, observacoes, id_doc))

        conn.commit()
        flash('Documento atualizado e enviado para nova conferência.', 'success')

    except Exception as e:
        conn.rollback()
        flash(f'Erro ao editar documento: {e}', 'danger')
    finally:
        conn.close()

    return redirect(url_for('documentos'))
    
@app.route('/api/pendencias_documentos')
@login_required
def api_pendencias_documentos():
    if current_user.role != 'admin': return jsonify([])
    
    conn = conectar_banco()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    # Busca a solicitação E os dados originais para comparar
    query = """
        SELECT s.id as id_solicitacao, s.tipo_solicitacao, s.dados_novos, s.solicitante, 
               to_char(s.data_solicitacao, 'DD/MM/YYYY HH24:MI') as data_pedida,
               d.id as id_doc, d.nome_original, d.uvr, 
               d.competencia as comp_atual, d.valor as valor_atual, 
               d.data_validade as validade_atual, d.numero_referencia as num_atual, d.observacoes as obs_atual,
               t.nome as tipo_nome
        FROM solicitacoes_documentos s
        JOIN documentos d ON s.id_documento = d.id
        JOIN tipos_documentos t ON d.id_tipo = t.id
        WHERE s.status = 'Pendente'
        ORDER BY s.data_solicitacao DESC
    """
    cursor.execute(query)
    pendencias = cursor.fetchall()
    
    # Tratamento de datas para JSON
    for p in pendencias:
        for k, v in p.items():
            if isinstance(v, (datetime, date)):
                p[k] = str(v)
                
    conn.close()
    return jsonify(pendencias)

@app.route('/processar_solicitacao_doc', methods=['POST'])
@login_required
def processar_solicitacao_doc():
    if current_user.role != 'admin': return "Acesso Negado", 403
    
    id_solicitacao = request.form.get('id_solicitacao')
    acao = request.form.get('acao') 
    
    conn = conectar_banco()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    try:
        cursor.execute("SELECT * FROM solicitacoes_alteracao WHERE id=%s", (id_solicitacao,))
        solicitacao = cursor.fetchone()
        
        if solicitacao:
            tabela = solicitacao['tabela_alvo']
            id_doc = solicitacao['id_registro']
            
            if acao == 'rejeitar':
                cursor.execute("DELETE FROM solicitacoes_alteracao WHERE id=%s", (id_solicitacao,))
                flash('Solicitação rejeitada.', 'info')
                
            elif acao == 'aprovar':
                if tabela == 'documentos':
                    tipo = str(solicitacao['tipo_solicitacao']).upper()
                    
                    if tipo == 'EXCLUSAO':
                        cursor.execute("SELECT caminho_arquivo FROM documentos WHERE id=%s", (id_doc,))
                        arq = cursor.fetchone()
                        if arq and arq['caminho_arquivo']:
                            if arq['caminho_arquivo'].startswith("http"):
                                try:
                                    _delete_cloudinary_asset(
                                        arq['caminho_arquivo'],
                                        resource_type=_detect_cloudinary_resource_type(arq['caminho_arquivo']),
                                    )
                                except Exception:
                                    pass
                            else:
                                try:
                                    os.remove(os.path.join('uploads', arq['caminho_arquivo']))
                                except Exception:
                                    pass
                        cursor.execute("DELETE FROM documentos WHERE id=%s", (id_doc,))
                        flash('Documento excluído!', 'success')
                    
                    elif tipo == 'EDICAO':
                        raw_data = solicitacao['dados_novos']
                        novos = json.loads(raw_data) if isinstance(raw_data, str) else raw_data

                        cursor.execute("""
                            UPDATE documentos 
                            SET competencia=%s, data_validade=%s, valor=%s, numero_referencia=%s, observacoes=%s
                            WHERE id=%s
                        """, (novos.get('competencia') or None, 
                              novos.get('data_validade') or None, 
                              novos.get('valor'), 
                              novos.get('numero_referencia'), 
                              novos.get('observacoes'), 
                              id_doc))
                        flash('Alterações aplicadas!', 'success')
                
                # Remove a pendência após aprovar
                cursor.execute("DELETE FROM solicitacoes_alteracao WHERE id=%s", (id_solicitacao,))

        conn.commit()
    except Exception as e:
        conn.rollback()
        flash(f'Erro: {str(e)}', 'danger')
    finally:
        conn.close()
        return redirect(url_for('index'))

# --- NOVA ROTA: EXCLUIR DOCUMENTO (SÓ ADMIN) ---
@app.route('/excluir_documento/<int:id_doc>')
@login_required
def excluir_documento(id_doc):
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = conectar_banco()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO solicitacoes_alteracao (tabela_alvo, id_registro, tipo_solicitacao, usuario_solicitante, status)
            VALUES ('documentos', %s, 'EXCLUSAO', %s, 'PENDENTE')
        """, (id_doc, current_user.username))
        
        conn.commit()
        flash('Solicitação de exclusão enviada!', 'warning')
    except Exception as e:
        conn.rollback()
        flash(f'Erro ao excluir: {e}', 'danger')
    finally:
        conn.close()
        return redirect(url_for('documentos'))

# --- NOVA ROTA API: Filtra Entidades com Saldo (Diferente de Zero) ---
@app.route('/api/entidades_com_saldo')
@login_required
def api_entidades_com_saldo():
    data_inicio = request.args.get('data_inicio')
    data_fim = request.args.get('data_fim')
    uvr = request.args.get('uvr')

    if not data_inicio or not data_fim:
        return jsonify([])

    conn = conectar_banco()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    try:
        # 1. Primeiro, vamos ver QUAIS TIPOS existem no banco nesse período
        # Isso ajuda a saber se tem algo diferente de 'Receita' e 'Despesa'
        cur.execute("SELECT DISTINCT tipo FROM transacoes WHERE data BETWEEN %s AND %s", (data_inicio, data_fim))
        tipos_encontrados = [t['tipo'] for t in cur.fetchall()]
        print(f"\n--- DIAGNÓSTICO DE SALDO ({data_inicio} a {data_fim}) ---")
        print(f"Tipos de transação encontrados: {tipos_encontrados}")

        # 2. Agora a Query que soma e mostra quem sobrou
        query = """
            SELECT 
                TRIM(entidade) as nome_entidade,
                SUM(CASE WHEN LOWER(TRIM(tipo)) = 'receita' THEN valor ELSE 0 END) as total_receita,
                SUM(CASE WHEN LOWER(TRIM(tipo)) != 'receita' THEN valor ELSE 0 END) as total_despesa,
                SUM(CASE WHEN LOWER(TRIM(tipo)) = 'receita' THEN valor ELSE -valor END) as saldo_final
            FROM transacoes 
            WHERE data BETWEEN %s AND %s
        """
        params = [data_inicio, data_fim]

        if uvr and uvr != 'Todas':
            query += " AND uvr = %s"
            params.append(uvr)

        query += """
            GROUP BY TRIM(entidade)
            HAVING ABS(SUM(CASE WHEN LOWER(TRIM(tipo)) = 'receita' THEN valor ELSE -valor END)) >= 0.01
            ORDER BY nome_entidade ASC
        """

        cur.execute(query, tuple(params))
        resultados = cur.fetchall()

        lista_nomes = []
        if not resultados:
            print(">> Nenhum saldo pendente encontrado. Lista vazia.")
        
        for row in resultados:
            nome = row['nome_entidade']
            saldo = row['saldo_final']
            # O Print abaixo vai aparecer no seu terminal e mostrar o "culpado"
            print(f">> ENTIDADE: {nome:<30} | REC: {row['total_receita']:>10} | DESP: {row['total_despesa']:>10} | SALDO: {saldo}")
            lista_nomes.append(nome)
        
        print("----------------------------------------------------------\n")

        return jsonify(lista_nomes)

    except Exception as e:
        print(f"ERRO CRÍTICO NO DEBUG: {e}")
        return jsonify({"erro": str(e)}), 500
    finally:
        conn.close()
# --- ROTA TEMPORÁRIA PARA CRIAR TABELAS ---
@app.route('/setup_banco')
def setup_banco():
    try:
        criar_tabelas_se_nao_existir()
        # Se quiser garantir, vamos inserir os tipos padrão aqui também
        conn = conectar_banco()
        cursor = conn.cursor()
        
        # Verifica se já tem tipos, se não tiver, insere
        cursor.execute("SELECT COUNT(*) FROM tipos_documentos")
        count = cursor.fetchone()[0]
        
        if count == 0:
            cursor.execute("""
                INSERT INTO tipos_documentos (nome, categoria, exige_competencia, exige_validade, exige_valor, multiplos_arquivos, descricao_ajuda) VALUES
                ('Nota Fiscal de Serviço', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Informe número, valor e anexe autorização se necessário'),
                ('Notas Fiscais de Receitas', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Informe número, valor e anexe comprovações relacionadas à receita'),
                ('Notas Fiscais de Despesas', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Informe número, valor e anexe comprovações relacionadas à despesa'),
                ('Comprovante de Pagamento', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Comprovante de pagamento das transações financeiras'),
                ('Medição Mensal', 'Mensal – Financeiro', TRUE, FALSE, FALSE, FALSE, 'Deve estar atestada pelo fiscal'),
                ('Relatório de Associados', 'Mensal – Trabalhista', TRUE, FALSE, FALSE, FALSE, 'Lista de ativos, baixados e novos'),
                ('Recibos de Rateio', 'Mensal – Trabalhista', TRUE, FALSE, TRUE, TRUE, 'Comprovantes de pagamento aos associados'),
                ('GPS – INSS', 'Mensal – Trabalhista', TRUE, FALSE, TRUE, FALSE, 'Guia e comprovante de pagamento'),
                ('Extrato Bancário – Associação', 'Mensal – Financeiro', TRUE, FALSE, FALSE, TRUE, 'Conta principal da associação'),
                ('MTR – Manifesto de Transporte', 'Mensal – Operacional', TRUE, FALSE, FALSE, TRUE, 'Documento de transporte de resíduos'),
                ('Relatório fotográfico da carga', 'Mensal – Operacional', TRUE, FALSE, FALSE, TRUE, 'Registro fotográfico da carga transportada'),
                ('Certidão Regularidade Municipal', 'Geral – Fiscal', FALSE, TRUE, FALSE, FALSE, 'Verifique a data de validade na certidão'),
                ('Certidão Regularidade Federal', 'Geral – Fiscal', FALSE, TRUE, FALSE, FALSE, 'Verifique a data de validade na certidão');
            """)
            conn.commit()
            msg = "Tabelas criadas e Tipos inseridos com sucesso!"
        else:
            cursor.execute("""
                INSERT INTO tipos_documentos (nome, categoria, exige_competencia, exige_validade, exige_valor, multiplos_arquivos, descricao_ajuda)
                SELECT * FROM (VALUES
                    ('Notas Fiscais de Receitas', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Informe número, valor e anexe comprovações relacionadas à receita'),
                    ('Notas Fiscais de Despesas', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Informe número, valor e anexe comprovações relacionadas à despesa'),
                    ('Comprovante de Pagamento', 'Mensal – Financeiro', TRUE, FALSE, TRUE, TRUE, 'Comprovante de pagamento das transações financeiras'),
                    ('Relatório fotográfico da carga', 'Mensal – Operacional', TRUE, FALSE, FALSE, TRUE, 'Registro fotográfico da carga transportada')
                ) AS novos(nome, categoria, exige_competencia, exige_validade, exige_valor, multiplos_arquivos, descricao_ajuda)
                WHERE NOT EXISTS (
                    SELECT 1 FROM tipos_documentos existentes WHERE existentes.nome = novos.nome
                )
            """)
            conn.commit()
            msg = "Tabelas já existiam. Tipos novos garantidos."
            
        conn.close()
        return f"<h1>Tudo certo!</h1><p>{msg}</p><a href='/documentos'>Ir para Documentos</a>"
    
    except Exception as e:
        return f"<h1>Erro ao criar tabelas:</h1><p>{str(e)}</p>"


# --- ROTA PARA VISUALIZAR ARQUIVOS DA PASTA UPLOADS ---
@app.route('/uploads/<path:filename>')
@login_required
def uploaded_file(filename):
    # O primeiro argumento é o nome da pasta física no seu computador ('uploads')
    return send_from_directory('uploads', filename)

@app.route('/aprovar_documento_direto/<int:id_doc>', methods=['POST'])
@login_required
def aprovar_documento_direto(id_doc):
    """Aprovação simples (o 'OK' do Admin) direto na lista de documentos."""
    bloqueio = exigir_admin()
    if bloqueio:
        return bloqueio
    
    conn = conectar_banco()
    cursor = conn.cursor()
    try:
        # Define como aprovado e limpa qualquer motivo de rejeição anterior
        cursor.execute("""
            UPDATE documentos 
            SET status = 'Aprovado', motivo_rejeicao = NULL 
           WHERE id = %s AND status = 'Pendente'
        """, (id_doc,))

        if cursor.rowcount == 0:
            flash('Este documento não está pendente para aprovação.', 'warning')
            return redirect(url_for('documentos'))
        conn.commit()
        flash('Documento conferido e aprovado com sucesso!', 'success')
    except Exception as e:
        conn.rollback()
        flash(f'Erro ao aprovar: {e}', 'danger')
    finally:
        conn.close()
        return redirect(url_for('documentos'))
    
@app.route('/reprovar_documento_com_obs', methods=['POST'])
@login_required
def reprovar_documento_com_obs():
    """Reprova o documento e salva a observação de correção para o usuário."""
    bloqueio = exigir_admin()
    if bloqueio:
        return bloqueio
    
    id_doc = request.form.get('id_documento')
    motivo = request.form.get('motivo_rejeicao')
    
    if not id_doc or not motivo:
        flash('Dados insuficientes para processar a reprovação.', 'danger')
        return redirect(url_for('documentos'))

    conn = conectar_banco()
    cursor = conn.cursor()
    try:
        # Atualiza status e grava o motivo da correção
        cursor.execute("""
            UPDATE documentos 
            SET status = 'Reprovado', motivo_rejeicao = %s 
            WHERE id = %s AND status = 'Pendente'
        """, (motivo, id_doc))

        if cursor.rowcount == 0:
            flash('Este documento não está pendente para reprovação.', 'warning')
            return redirect(url_for('documentos'))
        conn.commit()
        flash('Solicitação de correção enviada ao usuário!', 'warning')
    except Exception as e:
        conn.rollback()
        flash(f'Erro ao processar reprovação: {e}', 'danger')
    finally:
        conn.close()
        return redirect(url_for('documentos'))
@app.route('/get_cliente/<int:id>')
@login_required
def get_cliente(id):
    conn = conectar_banco()
    # Usa RealDictCursor para pegar os dados já com o nome das colunas
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT id, razao_social, cnpj, tipo_cadastro, 
                   telefone, cep, logradouro, numero, bairro, 
                   cidade, uf, uvr, data_hora_cadastro
            FROM cadastros 
            WHERE id = %s
        """, (id,))
        
        data = cur.fetchone()
        
        if data:
            # Formata a data para ficar bonitinha (dd/mm/yyyy HH:MM)
            if data['data_hora_cadastro']:
                data['data_hora_cadastro'] = data['data_hora_cadastro'].strftime('%d/%m/%Y %H:%M')
            
            return jsonify(data)
        else:
            return jsonify({'error': 'Cadastro não encontrado'}), 404
            
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        if conn: conn.close()
# --- ROTA PARA IMPRIMIR FICHA DO CLIENTE/FORNECEDOR (PDF) ---
@app.route('/imprimir_ficha_cliente/<int:id>')
@login_required
def imprimir_ficha_cliente(id):
    # 1. Busca os dados no banco
    conn = conectar_banco()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    try:
        cur.execute("""
            SELECT razao_social, cnpj, tipo_cadastro, telefone, 
                   logradouro, numero, bairro, cidade, uf, cep,
                   data_hora_cadastro, uvr
            FROM cadastros WHERE id = %s
        """, (id,))
        dados = cur.fetchone()
        
        if not dados:
            return "Cadastro não encontrado", 404

        # 2. Configura o PDF (buffer na memória)
        buffer = io.BytesIO()
        c = canvas.Canvas(buffer, pagesize=A4)
        largura, altura = A4
        
        # --- CABEÇALHO ---
        c.setFillColor(colors.darkblue)
        c.rect(0, altura - 80, largura, 80, fill=True, stroke=False)
        
        c.setFillColor(colors.white)
        c.setFont("Helvetica-Bold", 24)
        c.drawCentredString(largura / 2, altura - 50, "FICHA CADASTRAL")
        
        c.setFont("Helvetica", 12)
        tipo = dados['tipo_cadastro'].upper() if dados['tipo_cadastro'] else "GERAL"
        c.drawCentredString(largura / 2, altura - 70, f"REGISTRO DE {tipo}")

        # --- DADOS ---
        y = altura - 130
        c.setFillColor(colors.black)
        
        def desenhar_linha(titulo, valor, pos_y):
            c.setFont("Helvetica-Bold", 12)
            c.drawString(50, pos_y, titulo)
            c.setFont("Helvetica", 12)
            # Se o valor for None, coloca traço
            conteudo = str(valor) if valor else "---"
            c.drawString(200, pos_y, conteudo)
            # Linha cinza embaixo para separar
            c.setStrokeColor(colors.lightgrey)
            c.line(50, pos_y - 10, largura - 50, pos_y - 10)
            return pos_y - 30

        # Identificação
        c.setFont("Helvetica-Bold", 14)
        c.setFillColor(colors.darkblue)
        c.drawString(50, y, "1. IDENTIFICAÇÃO")
        y -= 30
        c.setFillColor(colors.black)

        y = desenhar_linha("Razão Social / Nome:", dados['razao_social'], y)
        y = desenhar_linha("CNPJ / CPF:", dados['cnpj'], y)
        y = desenhar_linha("Telefone:", dados['telefone'], y)
        
        # Formata Data
        data_formatada = "---"
        if dados['data_hora_cadastro']:
            data_formatada = dados['data_hora_cadastro'].strftime('%d/%m/%Y às %H:%M')
        y = desenhar_linha("Data de Cadastro:", data_formatada, y)
        y = desenhar_linha("UVR de Acesso:", dados['uvr'], y)

        y -= 20 # Espaço extra

        # Endereço
        c.setFont("Helvetica-Bold", 14)
        c.setFillColor(colors.darkblue)
        c.drawString(50, y, "2. LOCALIZAÇÃO")
        y -= 30
        c.setFillColor(colors.black)

        endereco_completo = f"{dados['logradouro'] or ''}, {dados['numero'] or 'S/N'}"
        y = desenhar_linha("Endereço:", endereco_completo, y)
        y = desenhar_linha("Bairro:", dados['bairro'], y)
        cidade_uf = f"{dados['cidade'] or ''} - {dados['uf'] or ''}"
        y = desenhar_linha("Cidade / UF:", cidade_uf, y)
        y = desenhar_linha("CEP:", dados['cep'], y)

        # Rodapé
        c.setFont("Helvetica-Oblique", 9)
        c.setFillColor(colors.grey)
        c.drawCentredString(largura / 2, 30, "Documento gerado eletronicamente pelo Sistema de Gestão.")

        c.showPage()
        c.save()
        buffer.seek(0)
        
        return Response(buffer, mimetype='application/pdf', 
                        headers={"Content-Disposition": f"inline; filename=ficha_{id}.pdf"})

    except Exception as e:
        return f"Erro ao gerar PDF: {e}", 500
    finally:
        if conn: conn.close()
# --- ROTA PARA EDITAR (OU SOLICITAR EDIÇÃO) DE CLIENTE ---
@app.route('/editar_cliente', methods=['POST'])
@login_required
def editar_cliente():
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = None
    try:
        dados = request.form.to_dict()
        id_registro = dados.get('id')
        
        if not id_registro:
            return "ID do registro não informado.", 400

        # Tratamentos básicos (CNPJ apenas números, etc)
        dados['cnpj'] = re.sub(r'[^0-9]', '', dados.get('cnpj', ''))
        
        # Conecta ao banco
        conn = conectar_banco()
        cur = conn.cursor()

        # 1. ADMIN: Edita direto
        if current_user.role == 'admin':
            cur.execute("""
                UPDATE cadastros 
                SET razao_social=%s, cnpj=%s, telefone=%s, 
                    cep=%s, logradouro=%s, numero=%s, bairro=%s, cidade=%s, uf=%s,
                    tipo_cadastro=%s
                WHERE id = %s
            """, (
                dados['razao_social'], dados['cnpj'], dados.get('telefone'),
                dados.get('cep'), dados.get('logradouro'), dados.get('numero'),
                dados.get('bairro'), dados.get('cidade'), dados.get('uf'),
                dados['tipo_cadastro'], id_registro
            ))
            conn.commit()
            flash("Cadastro atualizado com sucesso!", "success")
            return redirect(url_for('sucesso')) # Ou redireciona para a lista

        # 2. USUÁRIO COMUM: Cria Solicitação
        else:
            # Verifica se já tem pendência para não acumular lixo
            cur.execute("""
                SELECT id FROM solicitacoes_alteracao 
                WHERE tabela_alvo='cadastros' AND id_registro=%s AND status='Pendente'
            """, (id_registro,))
            
            if cur.fetchone():
                return "Já existe uma solicitação pendente para este cadastro.", 400

            # Prepara os dados novos em JSON
            dados_json = json.dumps(dados)
            
            cur.execute("""
                INSERT INTO solicitacoes_alteracao 
                (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante) 
                VALUES (%s, %s, %s, %s, %s)
            """, ('cadastros', id_registro, 'EDICAO', dados_json, current_user.username))
            
            conn.commit()
            
            # Avisa o usuário que foi enviado para análise
            return render_template('sucesso_generico.html', 
                                   titulo="Solicitação Enviada", 
                                   mensagem="As alterações foram enviadas para aprovação do administrador.")

    except Exception as e:
        if conn: conn.rollback()
        return f"Erro ao processar edição: {str(e)}", 500
    finally:
        if conn: conn.close()

# --- ROTA: BUSCAR LISTA DE EPIs COM FILTRO ---
@app.route("/buscar_epis", methods=["GET"])
@login_required
def buscar_epis():
    conn = None
    try:
        # Pega os parâmetros da URL
        termo = request.args.get("q", "").strip()
        categoria = request.args.get("categoria", "").strip()
        funcao = request.args.get("funcao", "").strip()
        
        # --- LÓGICA DE PERMISSÃO E FILTRO POR UVR ---
        if current_user.role == 'admin':
            # Admin pode filtrar por uma UVR específica ou ver tudo se deixar vazio
            uvr_filtro = request.args.get("uvr", "").strip()
        else:
            # Usuário comum é forçado a ver apenas o que pertence à sua UVR
            uvr_filtro = current_user.uvr_acesso

        conn = conectar_banco()
        cur = conn.cursor()
        
        # Query básica selecionando também a coluna uvr para conferência
        query = """
            SELECT id, nome, categoria, ca, validade_meses, funcao_indicada, uvr 
            FROM epi_itens 
            WHERE 1=1 
        """
        params = []

        # Aplica a trava de UVR na Query
        if uvr_filtro:
            query += " AND uvr = %s"
            params.append(uvr_filtro)

        # Filtro por Nome ou CA
        if termo:
            query += " AND (nome ILIKE %s OR ca ILIKE %s)"
            params.append(f"%{termo}%")
            params.append(f"%{termo}%")
        
        # Filtro por Categoria
        if categoria:
            query += " AND categoria = %s"
            params.append(categoria)
            
        # Filtro por Função
        if funcao:
            query += " AND funcao_indicada ILIKE %s"
            params.append(f"%{funcao}%")

        query += " ORDER BY nome ASC"

        cur.execute(query, params)
        rows = cur.fetchall()
        
        resultados = []
        for row in rows:
            resultados.append({
                "id": row[0],
                "nome": row[1],
                "categoria": row[2],
                "ca": row[3],
                "validade_meses": row[4],
                "funcao_indicada": row[5],
                "uvr": row[6] # Retorna a UVR para o frontend se necessário
            })
            
        return jsonify(resultados)

    except Exception as e:
        app.logger.error(f"Erro ao buscar EPIs: {e}")
        return jsonify({"error": str(e)}), 500
        
    finally:
        if conn and not conn.closed:
            conn.close()

# --- ROTA: DETALHES DO EPI ---
# --- ROTA: DETALHES DO EPI ---
@app.route("/get_epi_detalhe/<int:id>", methods=["GET"])
@login_required
def get_epi_detalhe(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        if current_user.role == 'admin':
            cur.execute("""
                SELECT id, nome, categoria, ca, validade_meses, funcao_indicada, uvr
                FROM epi_itens WHERE id = %s
            """, (id,))
        else:
            cur.execute("""
                SELECT id, nome, categoria, ca, validade_meses, funcao_indicada, uvr
                FROM epi_itens WHERE id = %s AND uvr = %s
            """, (id, current_user.uvr_acesso))

        row = cur.fetchone()
        if not row:
            return jsonify({"error": "EPI não encontrado"}), 404

        return jsonify({
            "id": row[0],
            "nome": row[1],
            "categoria": row[2],
            "ca": row[3],
            "validade_meses": row[4],
            "funcao_indicada": row[5],
            "uvr": row[6]
        })
    except Exception as e:
        app.logger.error(f"Erro ao buscar detalhes do EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- ROTA: EDITAR (OU SOLICITAR EDIÇÃO) DE EPI ---
@app.route("/editar_epi", methods=["POST"])
@login_required
def editar_epi():
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = None
    try:
        dados = request.form.to_dict()
        id_epi = dados.get("id_epi")
        if not id_epi:
            return jsonify({"error": "ID do EPI não informado."}), 400

        nome_epi = dados.get("nome_epi", "").strip()
        if not nome_epi:
            return jsonify({"error": "Nome do EPI é obrigatório."}), 400

        lista_funcoes = request.form.getlist("funcoes")
        if not lista_funcoes:
            return jsonify({"error": "Selecione pelo menos uma Função Recomendada."}), 400

        validade_meses = dados.get("validade_meses_epi")
        validade_meses_int = None
        if validade_meses and validade_meses.strip():
            try:
                validade_meses_int = int(validade_meses)
            except ValueError:
                return jsonify({"error": "Validade deve ser um número inteiro."}), 400

        funcao_texto = " / ".join(lista_funcoes)
        uvr_para_gravar = current_user.uvr_acesso or "GERAL"
        if current_user.role == 'admin' and dados.get("uvr_cadastro"):
            uvr_para_gravar = dados.get("uvr_cadastro")

        conn = conectar_banco()
        cur = conn.cursor()

        if current_user.role == 'admin':
            cur.execute("""
                UPDATE epi_itens SET
                    nome=%s, categoria=%s, ca=%s, validade_meses=%s, funcao_indicada=%s, uvr=%s
                WHERE id=%s
            """, (
                nome_epi,
                dados.get("categoria_epi", "").strip() or None,
                dados.get("ca_epi", "").strip() or None,
                validade_meses_int,
                funcao_texto,
                uvr_para_gravar,
                int(id_epi)
            ))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "EPI atualizado com sucesso."})

        cur.execute("SELECT uvr FROM epi_itens WHERE id = %s", (int(id_epi),))
        row_uvr = cur.fetchone()
        if not row_uvr:
            return jsonify({"error": "EPI não encontrado."}), 404
        if current_user.uvr_acesso and row_uvr[0] != current_user.uvr_acesso:
            return jsonify({"error": "Você não tem permissão para editar este EPI."}), 403

        cur.execute("""
            SELECT id FROM solicitacoes_alteracao
            WHERE tabela_alvo = 'epi_itens' AND id_registro = %s AND UPPER(status) = 'PENDENTE'
        """, (int(id_epi),))
        if cur.fetchone():
            return jsonify({"error": "Já existe uma solicitação pendente para este EPI."}), 400

        dados_json = json.dumps({
            "nome": nome_epi,
            "categoria": dados.get("categoria_epi", "").strip(),
            "ca": dados.get("ca_epi", "").strip(),
            "validade_meses": validade_meses_int,
            "funcao_indicada": funcao_texto,
            "uvr": uvr_para_gravar
        })

        cur.execute("""
            INSERT INTO solicitacoes_alteracao
            (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante)
            VALUES (%s, %s, %s, %s, %s)
        """, ('epi_itens', int(id_epi), 'EDICAO', dados_json, current_user.username))
        conn.commit()
        return jsonify({"status": "sucesso", "message": "Solicitação de edição enviada para aprovação."})
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao editar EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- ROTA: EXCLUIR (OU SOLICITAR EXCLUSÃO) DE EPI ---
@app.route("/excluir_epi/<int:id>", methods=["POST"])
@login_required
def excluir_epi(id):
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        cur.execute("SELECT nome, uvr FROM epi_itens WHERE id = %s", (id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "EPI não encontrado."}), 404

        nome_registro, uvr_epi = row
        if current_user.role != 'admin' and current_user.uvr_acesso != uvr_epi:
            return jsonify({"error": "Você não tem permissão para excluir este EPI."}), 403

        if current_user.role == 'admin':
            cur.execute("DELETE FROM epi_itens WHERE id = %s", (id,))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "EPI excluído permanentemente."})

        cur.execute("""
            SELECT id FROM solicitacoes_alteracao
            WHERE tabela_alvo = 'epi_itens' AND id_registro = %s AND UPPER(status) = 'PENDENTE'
        """, (id,))
        if cur.fetchone():
            return jsonify({"error": "Já existe uma solicitação pendente para este EPI."}), 400

        dados_json = json.dumps({"motivo": "Solicitado pelo usuário", "nome_visual": nome_registro})
        cur.execute("""
            INSERT INTO solicitacoes_alteracao
            (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante)
            VALUES (%s, %s, %s, %s, %s)
        """, ('epi_itens', id, 'EXCLUSAO', dados_json, current_user.username))
        conn.commit()
        return jsonify({"status": "sucesso", "message": "Solicitação de exclusão enviada para aprovação."})
    except psycopg2.IntegrityError:
        if conn: conn.rollback()
        return jsonify({"error": "Não é possível excluir este EPI pois ele possui registros vinculados."}), 400
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao excluir EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- ROTA: LANÇAR ENTRADA DE ESTOQUE ---
@app.route("/entrada_epi", methods=["GET", "POST"])
@login_required
def entrada_epi():
    # 1. Configuração de Contexto (UVR e Associação)
    uvr_usuario = getattr(current_user, 'uvr_acesso', None)
    if not uvr_usuario:
        uvr_usuario = "GERAL"
        
    associacao_usuario = getattr(current_user, 'associacao', None) or uvr_usuario

    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        # --- PROCESSAMENTO DO POST (GRAVAR ENTRADA) ---
        if request.method == "POST":
            # Coleta dados do formulário
            id_item = request.form.get("id_item")
            quantidade = request.form.get("quantidade")
            marca = request.form.get("marca", "").strip()
            unidade = request.form.get("unidade", "un")
            ca_epi = request.form.get("ca_epi", "").strip()
            observacao = request.form.get("observacao", "").strip()
            uvr_destino = request.form.get("uvr_destino")

            # Se for ADMIN, permite alterar a UVR de destino
            if current_user.role == 'admin' and uvr_destino:
                uvr_usuario = uvr_destino

            # Validações
            if not id_item or not quantidade:
                flash("EPI e Quantidade são obrigatórios.", "warning")
                return redirect(url_for('entrada_epi'))
            
            try:
                # Troca vírgula por ponto para evitar erro de conversão
                qtd_float = float(quantidade.replace(',', '.'))
                if qtd_float <= 0: raise ValueError
            except ValueError:
                flash("A quantidade deve ser um número maior que zero.", "error")
                return redirect(url_for('entrada_epi'))
            
            # (Opcional) Atualiza o CA no cadastro mestre se foi informado um novo
            if ca_epi:
                cur.execute("UPDATE epi_itens SET ca = %s WHERE id = %s", (ca_epi, id_item))

            # A. Inserir Movimento (Histórico)
            cur.execute("""
                INSERT INTO epi_movimentos 
                (id_item, uvr, associacao, tipo_movimento, quantidade, marca, data_movimento, observacao, usuario_responsavel)
                VALUES (%s, %s, %s, 'ENTRADA', %s, %s, NOW(), %s, %s)
            """, (id_item, uvr_usuario, associacao_usuario, qtd_float, marca, observacao, current_user.username))

            # B. Atualizar Estoque (Upsert)
            cur.execute("""
                INSERT INTO epi_estoque (id_item, uvr, associacao, unidade, quantidade, data_hora_atualizacao)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (uvr, associacao, id_item) 
                DO UPDATE SET 
                    quantidade = epi_estoque.quantidade + EXCLUDED.quantidade,
                    unidade = EXCLUDED.unidade,
                    data_hora_atualizacao = NOW()
            """, (id_item, uvr_usuario, associacao_usuario, unidade, qtd_float))

            conn.commit()
            flash(f"Entrada de {qtd_float} {unidade} registrada com sucesso!", "success")
            return redirect(url_for('entrada_epi'))

        # --- PROCESSAMENTO DO GET (CARREGAR PÁGINA) ---
        # Busca lista de EPIs para preencher o dropdown (Select)
        cur.execute("SELECT id, nome, ca, categoria FROM epi_itens ORDER BY nome ASC")
        lista_epis = cur.fetchall()
        
        return render_template("entrada_epi.html", usuario=current_user, epis=lista_epis)

    except Exception as e:
        if conn: conn.rollback()
        print(f"Erro Entrada EPI: {e}") # Log no terminal
        flash(f"Erro ao abrir entrada de EPI: {str(e)}", "error")
        return redirect(url_for('index'))  # <--- Mande para o 'index' (ou 'home')
    finally:
        if conn: conn.close()

@app.route("/buscar_entradas_epi", methods=["GET"])
@login_required
def buscar_entradas_epi():
    termo = request.args.get("q", "").strip()
    uvr_filtro = request.args.get("uvr", "").strip()
    associacao_usuario = getattr(current_user, 'associacao', None)

    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        query = """
            SELECT m.id, m.id_item, i.nome, i.ca, m.quantidade, m.data_movimento,
                   m.uvr, m.associacao, m.marca, m.observacao, e.quantidade AS saldo_atual
            FROM epi_movimentos m
            JOIN epi_itens i ON m.id_item = i.id
            LEFT JOIN epi_estoque e
                ON e.id_item = m.id_item
                AND e.uvr = m.uvr
                AND e.associacao = m.associacao
            WHERE m.tipo_movimento = 'ENTRADA'
        """
        params = []

        if current_user.role == 'admin':
            if uvr_filtro:
                query += " AND m.uvr = %s"
                params.append(uvr_filtro)
        else:
            if associacao_usuario:
                query += " AND m.associacao = %s"
                params.append(associacao_usuario)
            elif current_user.uvr_acesso:
                query += " AND m.uvr = %s"
                params.append(current_user.uvr_acesso)

        if termo:
            query += " AND (i.nome ILIKE %s OR i.ca ILIKE %s)"
            params.extend([f"%{termo}%", f"%{termo}%"])

        query += " ORDER BY m.data_movimento DESC, m.id DESC LIMIT 50"

        cur.execute(query, params)
        entradas = []
        for row in cur.fetchall():
            entradas.append({
                "id": row[0],
                "id_item": row[1],
                "nome": row[2],
                "ca": row[3],
                "quantidade": float(row[4]),
                "data_movimento": row[5].strftime('%Y-%m-%d') if row[5] else "",
                "uvr": row[6],
                "associacao": row[7],
                "marca": row[8],
                "observacao": row[9],
                "saldo_atual": float(row[10]) if row[10] is not None else 0.0
            })

        return jsonify(entradas)
    except Exception as e:
        app.logger.error(f"Erro ao buscar entradas de EPIs: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/get_entrada_epi_detalhe/<int:id>", methods=["GET"])
@login_required
def get_entrada_epi_detalhe(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        query = """
            SELECT m.id, m.id_item, i.nome, i.ca, m.quantidade, m.data_movimento,
                   m.uvr, m.associacao, m.marca, m.observacao
            FROM epi_movimentos m
            JOIN epi_itens i ON m.id_item = i.id
            WHERE m.id = %s AND m.tipo_movimento = 'ENTRADA'
        """
        params = [id]

        if current_user.role != 'admin':
            associacao_usuario = getattr(current_user, 'associacao', None)
            if associacao_usuario:
                query += " AND m.associacao = %s"
                params.append(associacao_usuario)
            elif current_user.uvr_acesso:
                query += " AND m.uvr = %s"
                params.append(current_user.uvr_acesso)

        cur.execute(query, params)
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Entrada não encontrada."}), 404

        cur.execute("""
            SELECT quantidade
            FROM epi_estoque
            WHERE id_item = %s AND uvr = %s AND associacao = %s
        """, (row[1], row[6], row[7]))
        estoque_row = cur.fetchone()
        saldo_atual = float(estoque_row[0]) if estoque_row else 0.0

        return jsonify({
            "id": row[0],
            "id_item": row[1],
            "nome": row[2],
            "ca": row[3],
            "quantidade": saldo_atual,
            "quantidade_entrada": float(row[4]),
            "data_movimento": row[5].strftime('%Y-%m-%d') if row[5] else "",
            "uvr": row[6],
            "associacao": row[7],
            "marca": row[8],
            "observacao": row[9]
        })
    except Exception as e:
        app.logger.error(f"Erro ao buscar detalhe da entrada de EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/editar_entrada_epi", methods=["POST"])
@login_required
def editar_entrada_epi():
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = None
    try:
        dados = request.form.to_dict()
        id_mov = dados.get("id_movimento")
        if not id_mov:
            return jsonify({"error": "ID da entrada não informado."}), 400

        quantidade = dados.get("quantidade")
        data_movimento = dados.get("data_movimento")
        marca = dados.get("marca", "").strip()
        observacao = dados.get("observacao", "").strip()

        if not quantidade or not data_movimento:
            return jsonify({"error": "Quantidade e Data são obrigatórios."}), 400

        try:
            nova_qtd = float(quantidade)
            if nova_qtd <= 0:
                raise ValueError
        except ValueError:
            return jsonify({"error": "Quantidade inválida."}), 400

        conn = conectar_banco()
        cur = conn.cursor()

        cur.execute("""
            SELECT id_item, quantidade, uvr, associacao
            FROM epi_movimentos
            WHERE id = %s AND tipo_movimento = 'ENTRADA'
        """, (id_mov,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Entrada não encontrada."}), 404

        id_item, qtd_antiga, uvr_mov, associacao_mov = row

        if current_user.role != 'admin':
            associacao_usuario = getattr(current_user, 'associacao', None)
            if associacao_usuario:
                if associacao_mov != associacao_usuario:
                    return jsonify({"error": "Você não tem permissão para editar esta entrada."}), 403
            elif current_user.uvr_acesso and uvr_mov != current_user.uvr_acesso:
                return jsonify({"error": "Você não tem permissão para editar esta entrada."}), 403

        if current_user.role == 'admin':
            cur.execute("""
                UPDATE epi_movimentos
                SET quantidade = %s, data_movimento = %s, marca = %s, observacao = %s
                WHERE id = %s
            """, (nova_qtd, data_movimento, marca, observacao, id_mov))

            delta = nova_qtd - float(qtd_antiga)
            cur.execute("""
                INSERT INTO epi_estoque (id_item, uvr, associacao, unidade, quantidade, data_hora_atualizacao)
                VALUES (%s, %s, %s, 'un', %s, NOW())
                ON CONFLICT (uvr, associacao, id_item)
                DO UPDATE SET
                    quantidade = epi_estoque.quantidade + EXCLUDED.quantidade,
                    data_hora_atualizacao = NOW();
            """, (id_item, uvr_mov, associacao_mov, delta))

            conn.commit()
            return jsonify({"status": "sucesso", "message": "Entrada atualizada com sucesso."})

        cur.execute("""
            SELECT id FROM solicitacoes_alteracao
            WHERE tabela_alvo = 'epi_movimentos' AND id_registro = %s AND UPPER(status) = 'PENDENTE'
        """, (int(id_mov),))
        if cur.fetchone():
            return jsonify({"error": "Já existe uma solicitação pendente para esta entrada."}), 400

        dados_json = json.dumps({
            "quantidade": nova_qtd,
            "data_movimento": data_movimento,
            "marca": marca,
            "observacao": observacao
        })

        cur.execute("""
            INSERT INTO solicitacoes_alteracao
            (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante)
            VALUES (%s, %s, %s, %s, %s)
        """, ('epi_movimentos', int(id_mov), 'EDICAO', dados_json, current_user.username))
        conn.commit()
        return jsonify({"status": "sucesso", "message": "Solicitação de edição enviada para aprovação."})
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao editar entrada de EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/excluir_entrada_epi/<int:id>", methods=["POST"])
@login_required
def excluir_entrada_epi(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        cur.execute("""
            SELECT m.id_item, m.quantidade, m.uvr, m.associacao, i.nome
            FROM epi_movimentos m
            JOIN epi_itens i ON m.id_item = i.id
            WHERE m.id = %s AND m.tipo_movimento = 'ENTRADA'
        """, (id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Entrada não encontrada."}), 404

        id_item, qtd_mov, uvr_mov, associacao_mov, nome_item = row

        if current_user.role != 'admin':
            associacao_usuario = getattr(current_user, 'associacao', None)
            if associacao_usuario:
                if associacao_mov != associacao_usuario:
                    return jsonify({"error": "Você não tem permissão para excluir esta entrada."}), 403
            elif current_user.uvr_acesso and uvr_mov != current_user.uvr_acesso:
                return jsonify({"error": "Você não tem permissão para excluir esta entrada."}), 403

        if current_user.role == 'admin':
            cur.execute("DELETE FROM epi_movimentos WHERE id = %s", (id,))
            cur.execute("""
                INSERT INTO epi_estoque (id_item, uvr, associacao, unidade, quantidade, data_hora_atualizacao)
                VALUES (%s, %s, %s, 'un', %s, NOW())
                ON CONFLICT (uvr, associacao, id_item)
                DO UPDATE SET
                    quantidade = epi_estoque.quantidade + EXCLUDED.quantidade,
                    data_hora_atualizacao = NOW();
            """, (id_item, uvr_mov, associacao_mov, -float(qtd_mov)))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Entrada excluída com sucesso."})

        cur.execute("""
            SELECT id FROM solicitacoes_alteracao
            WHERE tabela_alvo = 'epi_movimentos' AND id_registro = %s AND UPPER(status) = 'PENDENTE'
        """, (id,))
        if cur.fetchone():
            return jsonify({"error": "Já existe uma solicitação pendente para esta entrada."}), 400

        dados_json = json.dumps({"motivo": "Solicitado pelo usuário", "nome_visual": nome_item})
        cur.execute("""
            INSERT INTO solicitacoes_alteracao
            (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante)
            VALUES (%s, %s, %s, %s, %s)
        """, ('epi_movimentos', id, 'EXCLUSAO', dados_json, current_user.username))
        conn.commit()
        return jsonify({"status": "sucesso", "message": "Solicitação de exclusão enviada para aprovação."})
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao excluir entrada de EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

@app.route("/api/get_epis_filtrados")
@login_required
def get_epis_filtrados():
    termo = request.args.get("q", "").strip()
    categoria = request.args.get("categoria", "").strip()
    
    conn = conectar_banco()
    cur = conn.cursor()
    
    query = "SELECT id, nome, ca FROM epi_itens WHERE 1=1"
    params = []

    if current_user.role != 'admin':
        query += " AND uvr = %s"
        params.append(current_user.uvr_acesso)
    
    if categoria:
        query += " AND categoria = %s"
        params.append(categoria)
    if termo:
        query += " AND (nome ILIKE %s OR ca ILIKE %s)"
        params.extend([f"%{termo}%", f"%{termo}%"])
        
    query += " ORDER BY nome ASC LIMIT 50"
    cur.execute(query, params)
    epis = [{"id": r[0], "nome": r[1], "ca": r[2]} for r in cur.fetchall()]
    conn.close()
    return jsonify(epis)

# --- ROTA: CONSULTA DE SALDO DE ESTOQUE ---
@app.route("/saldo_epis")
@login_required
def saldo_epis():
    return render_template("saldo_epis.html", usuario=current_user)

# --- ROTA: CONTROLE DE ENTREGA DE EPIs ---
@app.route("/controle_entrega_epi", methods=["GET", "POST"])
@login_required
def controle_entrega_epi():
    conn = None
    try:
        associacao_usuario = getattr(current_user, 'associacao', None)

        def adicionar_meses(data_base, meses):
            ano = data_base.year + (data_base.month - 1 + meses) // 12
            mes = (data_base.month - 1 + meses) % 12 + 1
            ultimo_dia = calendar.monthrange(ano, mes)[1]
            dia = min(data_base.day, ultimo_dia)
            return date(ano, mes, dia)

        if request.method == "POST":
            dados = request.form
            
            # --- 1. DADOS DO CABEÇALHO (Comuns a toda a entrega) ---
            id_associado = dados.get("id_associado")
            id_responsavel = dados.get("id_responsavel")
            data_entrega = dados.get("data_entrega")
            observacoes = dados.get("observacoes", "").strip()
            uvr_filtro = dados.get("uvr_filtro", "")

            # --- 2. DADOS DOS ITENS (Listas vindas do HTML dinâmico) ---
            # O .getlist pega todos os inputs com name="id_item[]", etc.
            lista_ids_itens = request.form.getlist('id_item[]')
            lista_quantidades = request.form.getlist('quantidade[]')
            lista_unidades = request.form.getlist('unidade[]')

            # Validação do Cabeçalho
            if not id_associado or not id_responsavel or not data_entrega:
                flash("Associado, Responsável e Data são obrigatórios.", "warning")
                return redirect(url_for("controle_entrega_epi"))
            
            # Validação da Lista
            if not lista_ids_itens or len(lista_ids_itens) == 0:
                flash("Adicione pelo menos um EPI à entrega.", "warning")
                return redirect(url_for("controle_entrega_epi"))

            conn = conectar_banco()
            cur = conn.cursor()

            # Busca dados do Associado
            cur.execute("SELECT uvr, associacao FROM associados WHERE id = %s", (id_associado,))
            row_assoc = cur.fetchone()
            if not row_assoc:
                flash("Associado não encontrado.", "error")
                return redirect(url_for("controle_entrega_epi"))

            uvr_assoc, associacao_assoc = row_assoc

            # Permissões
            if current_user.role != 'admin':
                if associacao_usuario:
                    if associacao_assoc != associacao_usuario:
                        flash("Você não tem permissão para registrar entrega para esta associação.", "error")
                        return redirect(url_for("controle_entrega_epi"))
                elif current_user.uvr_acesso and uvr_assoc != current_user.uvr_acesso:
                    flash("Você não tem permissão para registrar entrega para esta UVR.", "error")
                    return redirect(url_for("controle_entrega_epi"))

            # Define UVR da Entrega
            if current_user.role == 'admin' and uvr_filtro:
                uvr_entrega = uvr_filtro
                if uvr_filtro != uvr_assoc:
                     flash("O associado selecionado não pertence à UVR filtrada.", "warning")
                     return redirect(url_for("controle_entrega_epi", uvr=uvr_filtro))
            else:
                uvr_entrega = uvr_assoc

            try:
                data_entrega_date = datetime.strptime(data_entrega, "%Y-%m-%d").date()
            except ValueError:
                flash("Data de entrega inválida.", "error")
                return redirect(url_for("controle_entrega_epi"))

            # --- 3. INSERE O CABEÇALHO (Uma única vez) ---
            cur.execute("""
                INSERT INTO epi_entregas 
                (id_associado, uvr, associacao, data_entrega, observacoes, usuario_registro, id_responsavel, data_hora_registro)
                VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
                RETURNING id
            """, (id_associado, uvr_entrega, associacao_assoc, data_entrega, observacoes, current_user.username, id_responsavel))
            id_entrega = cur.fetchone()[0]

            # --- 4. LOOP PARA INSERIR OS ITENS ---
            itens_inseridos = 0
            for i, id_item in enumerate(lista_ids_itens):
                # Pula se o ID for vazio (caso de linha em branco)
                if not id_item or id_item == "":
                    continue

                quantidade_str = lista_quantidades[i]
                unidade = lista_unidades[i]

                try:
                    qtd_float = float(quantidade_str)
                    if qtd_float <= 0: continue
                except:
                    continue # Pula se quantidade inválida

                # Busca Estoque e Validade Específica deste Item
                cur.execute("""
                    SELECT associacao FROM epi_estoque 
                    WHERE id_item = %s AND uvr = %s 
                    ORDER BY id ASC LIMIT 1
                """, (id_item, uvr_assoc))
                estoque_assoc_row = cur.fetchone()
                
                # Se encontrar estoque específico, usa a associação dele, senão usa a do associado
                assoc_estoque = estoque_assoc_row[0] if (estoque_assoc_row and estoque_assoc_row[0]) else associacao_assoc

                cur.execute("SELECT validade_meses FROM epi_itens WHERE id = %s", (id_item,))
                validade_row = cur.fetchone()
                validade_meses = validade_row[0] if validade_row else None
                
                data_validade = None
                if validade_meses and isinstance(validade_meses, int):
                    data_validade = adicionar_meses(data_entrega_date, validade_meses)

                # Insere o Item na Tabela de Itens
                cur.execute("""
                    INSERT INTO epi_entrega_itens 
                    (id_entrega, id_item, unidade, quantidade, data_validade)
                    VALUES (%s, %s, %s, %s, %s)
                """, (id_entrega, id_item, unidade, qtd_float, data_validade))

                # Atualiza o Estoque
                cur.execute("""
                    INSERT INTO epi_estoque (id_item, uvr, associacao, unidade, quantidade, data_hora_atualizacao)
                    VALUES (%s, %s, %s, %s, %s, NOW())
                    ON CONFLICT (uvr, associacao, id_item)
                    DO UPDATE SET 
                        quantidade = epi_estoque.quantidade + EXCLUDED.quantidade,
                        unidade = EXCLUDED.unidade,
                        data_hora_atualizacao = NOW();
                """, (id_item, uvr_entrega, assoc_estoque, unidade, -qtd_float))
                
                itens_inseridos += 1

            if itens_inseridos > 0:
                conn.commit()
                flash("Entrega registrada com sucesso!", "success")
            else:
                conn.rollback() # Se não inseriu nenhum item válido, cancela o cabeçalho
                flash("Erro: Nenhum item válido foi adicionado.", "error")

            return redirect(url_for("controle_entrega_epi"))

        # --- FIM DO POST, INÍCIO DO GET ---
        conn = conectar_banco()
        cur = conn.cursor()

        uvr_filtro = request.args.get("uvr", "").strip()

        if current_user.role == 'admin':
            if uvr_filtro:
                cur.execute(
                    "SELECT id, nome, uvr, associacao FROM associados WHERE uvr = %s ORDER BY nome ASC",
                    (uvr_filtro,)
                )
            else:
                associados = [] # Admin sem filtro não carrega lista gigante de associados
                
                # Mas precisamos da lista de UVRs para o filtro
                cur.execute("SELECT DISTINCT uvr FROM associados ORDER BY uvr ASC")
                lista_uvrs = [row[0] for row in cur.fetchall() if row[0]]
                hoje = date.today().strftime("%Y-%m-%d")
                
                # --- ADIÇÃO: BUSCAR LISTA DE FUNÇÕES ---
                cur.execute("SELECT DISTINCT funcao FROM associados WHERE funcao IS NOT NULL AND funcao <> '' ORDER BY funcao ASC")
                lista_funcoes = [row[0] for row in cur.fetchall()]

                return render_template(
                    "controle_entrega_epi.html",
                    usuario=current_user,
                    associados=associados,
                    epis=[],
                    hoje=hoje,
                    lista_uvrs=lista_uvrs,
                    uvr_selecionada="",
                    saldo_estoque={},
                    lista_funcoes=lista_funcoes # <--- PASSANDO PARA O HTML
                )
        else:
            if associacao_usuario:
                cur.execute(
                    "SELECT id, nome, uvr, associacao FROM associados WHERE associacao = %s ORDER BY nome ASC",
                    (associacao_usuario,)
                )
            else:
                cur.execute(
                    "SELECT id, nome, uvr, associacao FROM associados WHERE uvr = %s ORDER BY nome ASC",
                    (current_user.uvr_acesso,)
                )
        
        # Lista de associados carregada
        associados = cur.fetchall()

        cur.execute("SELECT id, nome, categoria, ca, validade_meses FROM epi_itens ORDER BY nome ASC")
        epis = cur.fetchall()

        cur.execute("SELECT DISTINCT uvr FROM associados ORDER BY uvr ASC")
        lista_uvrs = [row[0] for row in cur.fetchall() if row[0]]
        
        # --- ADIÇÃO: BUSCAR LISTA DE FUNÇÕES ---
        cur.execute("SELECT DISTINCT funcao FROM associados WHERE funcao IS NOT NULL AND funcao <> '' ORDER BY funcao ASC")
        lista_funcoes = [row[0] for row in cur.fetchall()]

        saldo_estoque = {}
        # Lógica de Saldo para colorir ou filtrar o dropdown
        if current_user.role == 'admin':
            if uvr_filtro:
                cur.execute("""
                    SELECT id_item, COALESCE(SUM(quantidade), 0)
                    FROM epi_estoque
                    WHERE uvr = %s
                    GROUP BY id_item
                """, (uvr_filtro,))
            else:
                cur.execute("""
                    SELECT id_item, COALESCE(SUM(quantidade), 0)
                    FROM epi_estoque
                    GROUP BY id_item
                """)
        else:
            if associacao_usuario:
                cur.execute("""
                    SELECT id_item, COALESCE(SUM(quantidade), 0)
                    FROM epi_estoque
                    WHERE associacao = %s
                    GROUP BY id_item
                """, (associacao_usuario,))
            else:
                cur.execute("""
                    SELECT id_item, COALESCE(SUM(quantidade), 0)
                    FROM epi_estoque
                    WHERE uvr = %s
                    GROUP BY id_item
                """, (current_user.uvr_acesso,))

        for row in cur.fetchall():
            saldo_estoque[row[0]] = float(row[1])

        # Opcional: Filtra EPIs com saldo zerado (remova se quiser ver todos)
        epis = [epi for epi in epis if saldo_estoque.get(epi[0], 0) > 0]

        hoje = date.today().strftime("%Y-%m-%d")
        
        return render_template(
            "controle_entrega_epi.html",
            usuario=current_user,
            associados=associados,
            epis=epis,
            hoje=hoje,
            lista_uvrs=lista_uvrs,
            uvr_selecionada=uvr_filtro,
            saldo_estoque=saldo_estoque,
            lista_funcoes=lista_funcoes # <--- PASSANDO PARA O HTML
        )
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro no controle de entrega de EPIs: {e}")
        flash(f"Erro ao registrar entrega: {e}", "error")
        return redirect(url_for("controle_entrega_epi"))
    finally:
        if conn: conn.close()

# --- API: LISTAR ENTREGAS DE EPI ---
@app.route("/api/get_entregas_epi", methods=["GET"])
@login_required
def get_entregas_epi():
    conn = None
    try:
        # Pega parâmetros da URL
        termo = request.args.get("q", "").strip()
        uvr_filtro = request.args.get("uvr", "").strip()
        data_ini = request.args.get("data_ini", "").strip()
        data_fim = request.args.get("data_fim", "").strip()
        funcao = request.args.get("funcao", "").strip()  # <--- NOVO: Captura a função
        
        associacao_usuario = getattr(current_user, 'associacao', None)

        conn = conectar_banco()
        cur = conn.cursor()

        # Adicionado a.funcao no SELECT
        query = """
            SELECT e.id, a.nome, i.nome, i.ca, it.quantidade, it.unidade,
                   e.data_entrega, e.uvr, e.associacao, it.data_validade, 
                   e.observacoes, a.funcao 
            FROM epi_entregas e
            JOIN associados a ON e.id_associado = a.id
            JOIN epi_entrega_itens it ON it.id_entrega = e.id
            JOIN epi_itens i ON it.id_item = i.id
            WHERE 1=1
        """
        params = []

        # Filtros de Permissão (Admin / Usuário Comum)
        if current_user.role == 'admin':
            if uvr_filtro:
                query += " AND e.uvr = %s"
                params.append(uvr_filtro)
        else:
            if associacao_usuario:
                query += " AND e.associacao = %s"
                params.append(associacao_usuario)
            elif current_user.uvr_acesso:
                query += " AND e.uvr = %s"
                params.append(current_user.uvr_acesso)

        # Filtro de Data
        if data_ini:
            query += " AND e.data_entrega >= %s"
            params.append(data_ini)
        if data_fim:
            query += " AND e.data_entrega <= %s"
            params.append(data_fim)

        # NOVO: Filtro de Função
        if funcao and funcao != 'Todas':
            query += " AND a.funcao = %s"
            params.append(funcao)

        # Filtro de Busca (Texto)
        if termo:
            query += " AND (a.nome ILIKE %s OR i.nome ILIKE %s OR i.ca ILIKE %s)"
            params.extend([f"%{termo}%", f"%{termo}%", f"%{termo}%"])

        query += " ORDER BY e.data_entrega DESC, e.id DESC"

        cur.execute(query, tuple(params))
        entregas = []
        for row in cur.fetchall():
            entregas.append({
                "id": row[0],
                "associado": row[1],
                "item": row[2],
                "ca": row[3],
                "quantidade": float(row[4]),
                "unidade": row[5],
                "data_entrega": row[6].strftime('%Y-%m-%d') if row[6] else "",
                "uvr": row[7],
                "associacao": row[8],
                "data_validade": row[9].strftime('%Y-%m-%d') if row[9] else "",
                "observacoes": row[10],
                "funcao": row[11]  # <--- NOVO: Retorna a função para o JSON
            })

        return jsonify(entregas)
    except Exception as e:
        app.logger.error(f"Erro ao buscar entregas de EPIs: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- API: DETALHE DA ENTREGA DE EPI ---
@app.route("/get_entrega_epi_detalhe/<int:id>", methods=["GET"])
@login_required
def get_entrega_epi_detalhe(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        cur.execute("""
            SELECT e.id, e.id_associado, a.nome, it.id_item, i.nome, i.ca,
                   it.quantidade, it.unidade, e.data_entrega, e.uvr, e.associacao,
                   it.data_validade, e.observacoes
            FROM epi_entregas e
            JOIN associados a ON e.id_associado = a.id
            JOIN epi_entrega_itens it ON it.id_entrega = e.id
            JOIN epi_itens i ON it.id_item = i.id
            WHERE e.id = %s
        """, (id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Entrega não encontrada."}), 404

        associacao_usuario = getattr(current_user, 'associacao', None)
        if current_user.role != 'admin':
            if associacao_usuario:
                if row[10] != associacao_usuario:
                    return jsonify({"error": "Você não tem permissão para acessar esta entrega."}), 403
            elif current_user.uvr_acesso and row[9] != current_user.uvr_acesso:
                return jsonify({"error": "Você não tem permissão para acessar esta entrega."}), 403

        return jsonify({
            "id": row[0],
            "id_associado": row[1],
            "associado": row[2],
            "id_item": row[3],
            "item": row[4],
            "ca": row[5],
            "quantidade": float(row[6]),
            "unidade": row[7],
            "data_entrega": row[8].strftime('%Y-%m-%d') if row[8] else "",
            "uvr": row[9],
            "associacao": row[10],
            "data_validade": row[11].strftime('%Y-%m-%d') if row[11] else "",
            "observacoes": row[12] or ""
        })
    except Exception as e:
        app.logger.error(f"Erro ao buscar detalhe da entrega de EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- API: EDITAR ENTREGA DE EPI ---
@app.route("/editar_entrega_epi", methods=["POST"])
@login_required
def editar_entrega_epi():
    conn = None
    try:
        dados = request.form.to_dict()
        id_entrega = dados.get("id_entrega")
        if not id_entrega:
            return jsonify({"error": "ID da entrega não informado."}), 400

        quantidade = dados.get("quantidade")
        unidade = dados.get("unidade", "").strip()
        data_entrega = dados.get("data_entrega", "").strip()
        observacoes = dados.get("observacoes", "").strip()

        if quantidade is None or quantidade == "":
            return jsonify({"error": "Quantidade é obrigatória."}), 400

        try:
            nova_qtd = float(quantidade)
            if nova_qtd <= 0:
                raise ValueError
        except ValueError:
            return jsonify({"error": "Quantidade inválida."}), 400

        data_entrega_val = None
        if data_entrega:
            try:
                data_entrega_val = datetime.strptime(data_entrega, "%Y-%m-%d").date()
            except ValueError:
                return jsonify({"error": "Data de entrega inválida."}), 400

        conn = conectar_banco()
        cur = conn.cursor()

        cur.execute("""
            SELECT it.id_item, it.quantidade, it.unidade, e.uvr, e.associacao
            FROM epi_entregas e
            JOIN epi_entrega_itens it ON it.id_entrega = e.id
            WHERE e.id = %s
        """, (id_entrega,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Entrega não encontrada."}), 404

        id_item, qtd_antiga, unidade_antiga, uvr_entrega, associacao_entrega = row

        if current_user.role != 'admin':
            associacao_usuario = getattr(current_user, 'associacao', None)
            if associacao_usuario:
                if associacao_entrega != associacao_usuario:
                    return jsonify({"error": "Você não tem permissão para editar esta entrega."}), 403
            elif current_user.uvr_acesso and uvr_entrega != current_user.uvr_acesso:
                return jsonify({"error": "Você não tem permissão para editar esta entrega."}), 403

        unidade_final = unidade or unidade_antiga

        def adicionar_meses(data_base, meses):
            ano = data_base.year + (data_base.month - 1 + meses) // 12
            mes = (data_base.month - 1 + meses) % 12 + 1
            ultimo_dia = calendar.monthrange(ano, mes)[1]
            dia = min(data_base.day, ultimo_dia)
            return date(ano, mes, dia)

        if current_user.role == 'admin':
            data_validade = None
            if data_entrega_val:
                cur.execute("SELECT validade_meses FROM epi_itens WHERE id = %s", (id_item,))
                validade_row = cur.fetchone()
                validade_meses = validade_row[0] if validade_row else None
                if validade_meses and isinstance(validade_meses, int):
                    data_validade = adicionar_meses(data_entrega_val, validade_meses)
            cur.execute("""
                UPDATE epi_entregas
                SET data_entrega = COALESCE(%s, data_entrega),
                    observacoes = %s
                WHERE id = %s
            """, (data_entrega_val, observacoes, id_entrega))
            cur.execute("""
                UPDATE epi_entrega_itens
                SET quantidade = %s,
                    unidade = %s,
                    data_validade = COALESCE(%s, data_validade)
                WHERE id_entrega = %s
            """, (nova_qtd, unidade_final, data_validade, id_entrega))

            delta = nova_qtd - float(qtd_antiga)
            cur.execute("""
                INSERT INTO epi_estoque (id_item, uvr, associacao, unidade, quantidade, data_hora_atualizacao)
                VALUES (%s, %s, %s, 'un', %s, NOW())
                ON CONFLICT (uvr, associacao, id_item)
                DO UPDATE SET
                    quantidade = epi_estoque.quantidade + EXCLUDED.quantidade,
                    data_hora_atualizacao = NOW();
            """, (id_item, uvr_entrega, associacao_entrega, -delta))

            conn.commit()
            return jsonify({"status": "sucesso", "message": "Entrega atualizada com sucesso."})

        cur.execute("""
            SELECT id FROM solicitacoes_alteracao
            WHERE tabela_alvo = 'epi_entregas' AND id_registro = %s AND UPPER(status) = 'PENDENTE'
        """, (int(id_entrega),))
        if cur.fetchone():
            return jsonify({"error": "Já existe uma solicitação pendente para esta entrega."}), 400

        dados_json = json.dumps({
            "quantidade": nova_qtd,
            "unidade": unidade_final,
            "data_entrega": data_entrega_val.strftime('%Y-%m-%d') if data_entrega_val else "",
            "observacoes": observacoes
        })

        cur.execute("""
            INSERT INTO solicitacoes_alteracao
            (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante)
            VALUES (%s, %s, %s, %s, %s)
        """, ('epi_entregas', int(id_entrega), 'EDICAO', dados_json, current_user.username))
        conn.commit()
        return jsonify({"status": "sucesso", "message": "Solicitação de edição enviada para aprovação."})
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao editar entrega de EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- API: EXCLUIR ENTREGA DE EPI ---
@app.route("/excluir_entrega_epi/<int:id>", methods=["POST"])
@login_required
def excluir_entrega_epi(id):
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        cur.execute("""
            SELECT it.id_item, it.quantidade, e.uvr, e.associacao, i.nome
            FROM epi_entregas e
            JOIN epi_entrega_itens it ON it.id_entrega = e.id
            JOIN epi_itens i ON it.id_item = i.id
            WHERE e.id = %s
        """, (id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Entrega não encontrada."}), 404

        id_item, qtd_entrega, uvr_entrega, associacao_entrega, nome_item = row

        if current_user.role != 'admin':
            associacao_usuario = getattr(current_user, 'associacao', None)
            if associacao_usuario:
                if associacao_entrega != associacao_usuario:
                    return jsonify({"error": "Você não tem permissão para excluir esta entrega."}), 403
            elif current_user.uvr_acesso and uvr_entrega != current_user.uvr_acesso:
                return jsonify({"error": "Você não tem permissão para excluir esta entrega."}), 403

        if current_user.role == 'admin':
            cur.execute("DELETE FROM epi_entregas WHERE id = %s", (id,))
            cur.execute("""
                INSERT INTO epi_estoque (id_item, uvr, associacao, unidade, quantidade, data_hora_atualizacao)
                VALUES (%s, %s, %s, 'un', %s, NOW())
                ON CONFLICT (uvr, associacao, id_item)
                DO UPDATE SET
                    quantidade = epi_estoque.quantidade + EXCLUDED.quantidade,
                    data_hora_atualizacao = NOW();
            """, (id_item, uvr_entrega, associacao_entrega, float(qtd_entrega)))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Entrega excluída com sucesso."})

        cur.execute("""
            SELECT id FROM solicitacoes_alteracao
            WHERE tabela_alvo = 'epi_entregas' AND id_registro = %s AND UPPER(status) = 'PENDENTE'
        """, (id,))
        if cur.fetchone():
            return jsonify({"error": "Já existe uma solicitação pendente para esta entrega."}), 400

        dados_json = json.dumps({"motivo": "Solicitado pelo usuário", "nome_visual": nome_item})
        cur.execute("""
            INSERT INTO solicitacoes_alteracao
            (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante)
            VALUES (%s, %s, %s, %s, %s)
        """, ('epi_entregas', id, 'EXCLUSAO', dados_json, current_user.username))
        conn.commit()
        return jsonify({"status": "sucesso", "message": "Solicitação de exclusão enviada para aprovação."})
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao excluir entrega de EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- API: BUSCAR DADOS DO SALDO (COM FILTROS) ---
@app.route("/api/get_saldo_epis")
@login_required
def get_saldo_epis():
    conn = None
    try:
        # Lógica de permissão de UVR
        # Admin pode escolher filtrar uma específica ou ver todas. 
        # Usuário comum só vê a dele.
        if current_user.role == 'admin':
            uvr_filtro = request.args.get("uvr", "").strip()
        else:
            uvr_filtro = current_user.uvr_acesso

        categoria = request.args.get("categoria", "").strip()
        termo = request.args.get("q", "").strip()

        conn = conectar_banco()
        cur = conn.cursor()

        # Query que faz o JOIN entre o Estoque (e) e o Catálogo de Itens (i)
        query = """
            SELECT 
                e.id,
                e.id_item,
                e.associacao,
                i.nome,
                i.categoria,
                e.quantidade,
                e.unidade,
                e.uvr,
                e.data_hora_atualizacao,
                i.ca
            FROM epi_estoque e
            JOIN epi_itens i ON e.id_item = i.id
            WHERE 1=1
        """
        params = []

        # Aplica trava de UVR (obrigatória para usuário, opcional para admin)
        if uvr_filtro:
            query += " AND e.uvr = %s"
            params.append(uvr_filtro)
        
        # Filtro de Categoria
        if categoria:
            query += " AND i.categoria = %s"
            params.append(categoria)

        # Filtro de Busca por Texto (Nome ou CA)
        if termo:
            query += " AND (i.nome ILIKE %s OR i.ca ILIKE %s)"
            params.extend([f"%{termo}%", f"%{termo}%"])

        # Ordenação por UVR e depois por Nome
        query += " ORDER BY e.uvr ASC, i.nome ASC"

        cur.execute(query, params)
        rows = cur.fetchall()

        resultados = []
        for r in rows:
            # Tratamento de data para evitar erro de 'NoneType'
            data_formatada = r[8].strftime('%d/%m/%Y %H:%M') if r[8] else "N/A"
            
            resultados.append({
                "id_estoque": r[0],
                "id_item": r[1],
                "associacao": r[2],
                "nome": r[3],
                "categoria": r[4],
                "quantidade": float(r[5]),
                "unidade": r[6],
                "uvr": r[7],
                "atualizado": data_formatada,
                "ca": r[9]
            })

        return jsonify(resultados)
    except Exception as e:
        app.logger.error(f"Erro ao buscar saldo de EPIs: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn and not conn.closed:
            conn.close()

# --- API: DETALHE DO ESTOQUE DE EPI ---
@app.route("/get_estoque_epi_detalhe/<int:id>", methods=["GET"])
@login_required
def get_estoque_epi_detalhe(id):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        cur.execute("""
            SELECT e.id, e.id_item, i.nome, i.ca, e.quantidade, e.unidade, e.uvr, e.associacao
            FROM epi_estoque e
            JOIN epi_itens i ON e.id_item = i.id
            WHERE e.id = %s
        """, (id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Registro de estoque não encontrado."}), 404

        id_estoque, id_item, nome, ca, quantidade, unidade, uvr, associacao = row

        if current_user.role != 'admin':
            associacao_usuario = getattr(current_user, 'associacao', None)
            if associacao_usuario:
                if associacao != associacao_usuario:
                    return jsonify({"error": "Você não tem permissão para acessar este estoque."}), 403
            elif current_user.uvr_acesso and uvr != current_user.uvr_acesso:
                return jsonify({"error": "Você não tem permissão para acessar este estoque."}), 403

        return jsonify({
            "id": id_estoque,
            "id_item": id_item,
            "nome": nome,
            "ca": ca,
            "quantidade": float(quantidade),
            "unidade": unidade,
            "uvr": uvr,
            "associacao": associacao
        })
    except Exception as e:
        app.logger.error(f"Erro ao buscar detalhe do estoque de EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- API: EDITAR ESTOQUE DE EPI ---
@app.route("/editar_estoque_epi", methods=["POST"])
@login_required
def editar_estoque_epi():
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = None
    try:
        dados = request.form.to_dict()
        id_estoque = dados.get("id_estoque")
        if not id_estoque:
            return jsonify({"error": "ID do estoque não informado."}), 400

        quantidade = dados.get("quantidade")
        unidade = dados.get("unidade", "").strip()

        if quantidade is None or quantidade == "":
            return jsonify({"error": "Quantidade é obrigatória."}), 400

        try:
            nova_qtd = float(quantidade)
            if nova_qtd < 0:
                raise ValueError
        except ValueError:
            return jsonify({"error": "Quantidade inválida."}), 400

        conn = conectar_banco()
        cur = conn.cursor()

        cur.execute("""
            SELECT id_item, quantidade, unidade, uvr, associacao
            FROM epi_estoque
            WHERE id = %s
        """, (id_estoque,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Registro de estoque não encontrado."}), 404

        id_item, qtd_antiga, unidade_antiga, uvr, associacao = row

        if current_user.role != 'admin':
            associacao_usuario = getattr(current_user, 'associacao', None)
            if associacao_usuario:
                if associacao != associacao_usuario:
                    return jsonify({"error": "Você não tem permissão para editar este estoque."}), 403
            elif current_user.uvr_acesso and uvr != current_user.uvr_acesso:
                return jsonify({"error": "Você não tem permissão para editar este estoque."}), 403

        unidade_final = unidade or unidade_antiga

        if current_user.role == 'admin':
            cur.execute("""
                UPDATE epi_estoque
                SET quantidade = %s, unidade = %s, data_hora_atualizacao = NOW()
                WHERE id = %s
            """, (nova_qtd, unidade_final, id_estoque))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Estoque atualizado com sucesso."})

        cur.execute("""
            SELECT id FROM solicitacoes_alteracao
            WHERE tabela_alvo = 'epi_estoque' AND id_registro = %s AND UPPER(status) = 'PENDENTE'
        """, (int(id_estoque),))
        if cur.fetchone():
            return jsonify({"error": "Já existe uma solicitação pendente para este estoque."}), 400

        dados_json = json.dumps({
            "quantidade": nova_qtd,
            "unidade": unidade_final
        })

        cur.execute("""
            INSERT INTO solicitacoes_alteracao
            (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante)
            VALUES (%s, %s, %s, %s, %s)
        """, ('epi_estoque', int(id_estoque), 'EDICAO', dados_json, current_user.username))
        conn.commit()
        return jsonify({"status": "sucesso", "message": "Solicitação de edição enviada para aprovação."})
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao editar estoque de EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- API: EXCLUIR ESTOQUE DE EPI ---
@app.route("/excluir_estoque_epi/<int:id>", methods=["POST"])
@login_required
def excluir_estoque_epi(id):
    bloqueio = bloquear_visitante()
    if bloqueio:
        return bloqueio
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()

        cur.execute("""
            SELECT e.id_item, e.quantidade, e.unidade, e.uvr, e.associacao, i.nome
            FROM epi_estoque e
            JOIN epi_itens i ON e.id_item = i.id
            WHERE e.id = %s
        """, (id,))
        row = cur.fetchone()
        if not row:
            return jsonify({"error": "Registro de estoque não encontrado."}), 404

        id_item, quantidade, unidade, uvr, associacao, nome_item = row

        if current_user.role != 'admin':
            associacao_usuario = getattr(current_user, 'associacao', None)
            if associacao_usuario:
                if associacao != associacao_usuario:
                    return jsonify({"error": "Você não tem permissão para excluir este estoque."}), 403
            elif current_user.uvr_acesso and uvr != current_user.uvr_acesso:
                return jsonify({"error": "Você não tem permissão para excluir este estoque."}), 403

        if current_user.role == 'admin':
            cur.execute("DELETE FROM epi_estoque WHERE id = %s", (id,))
            conn.commit()
            return jsonify({"status": "sucesso", "message": "Registro de estoque excluído com sucesso."})

        cur.execute("""
            SELECT id FROM solicitacoes_alteracao
            WHERE tabela_alvo = 'epi_estoque' AND id_registro = %s AND UPPER(status) = 'PENDENTE'
        """, (id,))
        if cur.fetchone():
            return jsonify({"error": "Já existe uma solicitação pendente para este estoque."}), 400

        dados_json = json.dumps({
            "motivo": "Solicitado pelo usuário",
            "nome_visual": nome_item,
            "quantidade": float(quantidade),
            "unidade": unidade
        })
        cur.execute("""
            INSERT INTO solicitacoes_alteracao
            (tabela_alvo, id_registro, tipo_solicitacao, dados_novos, usuario_solicitante)
            VALUES (%s, %s, %s, %s, %s)
        """, ('epi_estoque', id, 'EXCLUSAO', dados_json, current_user.username))
        conn.commit()
        return jsonify({"status": "sucesso", "message": "Solicitação de exclusão enviada para aprovação."})
    except Exception as e:
        if conn: conn.rollback()
        app.logger.error(f"Erro ao excluir estoque de EPI: {e}")
        return jsonify({"error": str(e)}), 500
    finally:
        if conn: conn.close()

# --- FUNÇÃO AUXILIAR DE DESENHO DO RECIBO (MEIA PÁGINA) ---
def desenhar_recibo_canvas(c, dados, y_inicial):
    """
    Desenha um recibo padrão na posição Y especificada.
    dados esperado: (id_entrega, data_entrega, nome_assoc, cpf_assoc, funcao_assoc, associacao, nome_resp, itens)
    """
    id_entrega, data_entrega, nome_assoc, cpf_assoc, funcao_assoc, associacao, nome_resp, itens = dados
    
    # Configurações de Layout
    margem_esq = 30
    largura_util = 535 # A4 width (595) - margens
    
    # Cabeçalho
    c.setFont("Helvetica-Bold", 12)
    c.drawString(margem_esq, y_inicial, "TERMO DE ENTREGA DE EPI")
    
    c.setFont("Helvetica", 9)
    c.drawRightString(margem_esq + largura_util, y_inicial, f"Nº Controle: {id_entrega:05d}")
    
    y = y_inicial - 20
    
    # Dados do Recebedor e Associação
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margem_esq, y, "Colaborador/Associado:")
    c.setFont("Helvetica", 9)
    c.drawString(margem_esq + 110, y, f"{nome_assoc} (CPF: {cpf_assoc or 'N/I'})")
    
    # Adicionando Associação ao lado
    c.setFont("Helvetica-Bold", 9)
    c.drawRightString(margem_esq + largura_util - 100, y, "Associação:")
    c.setFont("Helvetica", 9)
    c.drawRightString(margem_esq + largura_util, y, f"{associacao or 'Geral'}")

    y -= 12
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margem_esq, y, "Função:")
    c.setFont("Helvetica", 9)
    c.drawString(margem_esq + 45, y, f"{funcao_assoc or 'Não informada'}")
    
    c.setFont("Helvetica-Bold", 9)
    c.drawString(margem_esq + 250, y, "Data Entrega:")
    c.setFont("Helvetica", 9)
    if isinstance(data_entrega, str):
        try:
            data_fmt = datetime.strptime(data_entrega, '%Y-%m-%d').strftime('%d/%m/%Y')
        except:
            data_fmt = data_entrega
    elif data_entrega:
        data_fmt = data_entrega.strftime('%d/%m/%Y')
    else:
        data_fmt = datetime.now().strftime('%d/%m/%Y')
        
    c.drawString(margem_esq + 320, y, data_fmt)

    y -= 20
    
    # Texto Legal Curto
    c.setFont("Helvetica", 8)
    texto = "Declaro ter recebido os EPIs abaixo, comprometendo-me a utilizá-los e zelar por sua guarda."
    c.drawString(margem_esq, y, texto)
    
    y -= 15
    
    # Cabeçalho da Tabela de Itens
    c.setFillColorRGB(0.9, 0.9, 0.9) # Fundo cinza
    c.rect(margem_esq, y-2, largura_util, 12, fill=1, stroke=0)
    c.setFillColorRGB(0, 0, 0)
    
    c.setFont("Helvetica-Bold", 8)
    c.drawString(margem_esq + 5, y+2, "ITEM / DESCRIÇÃO")
    c.drawString(margem_esq + 320, y+2, "C.A.")
    c.drawString(margem_esq + 380, y+2, "QTD")
    c.drawString(margem_esq + 430, y+2, "VALIDADE")
    
    y -= 12
    
    # Itens
    c.setFont("Helvetica", 8)
    # Garante que itens seja uma lista de dicionários ou tuplas. 
    # Se vier da rota individual, adaptamos.
    
    for item in itens:
        # Normalização dos dados do item (suporta dict ou tupla)
        if isinstance(item, dict):
            nome_epi = item.get('nome', '')
            ca = item.get('ca')
            qtd = item.get('quantidade')
            un = item.get('unidade')
            val = item.get('validade')
        else:
            # Caso venha da query simples (nome, ca, qtd, un, validade)
            nome_epi = item[0]
            ca = item[1]
            qtd = item[2]
            un = item[3]
            val = item[4] if len(item) > 4 else None

        if len(nome_epi) > 55: nome_epi = nome_epi[:55] + "..."
        
        ca_str = ca or '-'
        qtd_str = f"{float(qtd):.2f}".replace('.', ',')
        un_str = un or 'un'
        
        val_str = "-"
        if val:
            try:
                if isinstance(val, str):
                    val_str = datetime.strptime(val, '%Y-%m-%d').strftime('%d/%m/%Y')
                else:
                    val_str = val.strftime('%d/%m/%Y')
            except: pass

        c.drawString(margem_esq + 5, y, nome_epi)
        c.drawString(margem_esq + 320, y, ca_str)
        c.drawString(margem_esq + 380, y, f"{qtd_str} {un_str}")
        c.drawString(margem_esq + 430, y, val_str)
        
        c.setLineWidth(0.5)
        c.setStrokeColorRGB(0.8, 0.8, 0.8)
        c.line(margem_esq, y-2, margem_esq + largura_util, y-2)
        c.setFillColorRGB(0, 0, 0) # Garante cor preta
        
        y -= 12
        # Limite de segurança
        if y < (y_inicial - 350): 
            c.drawString(margem_esq, y, "... (demais itens consultáveis no sistema)")
            break
            
    # Assinaturas
    y_ass = y_inicial - 320 # Posição fixa relativa ao topo do recibo
    
    c.setStrokeColorRGB(0, 0, 0)
    c.setLineWidth(0.5)
    
    # Linha Recebedor
    c.line(margem_esq + 30, y_ass, margem_esq + 230, y_ass)
    c.setFont("Helvetica", 7)
    c.drawCentredString(margem_esq + 130, y_ass - 10, f"{nome_assoc}")
    c.drawCentredString(margem_esq + 130, y_ass - 20, "Assinatura do Recebedor")
    
    # Linha Entregador
    c.line(margem_esq + 300, y_ass, margem_esq + 500, y_ass)
    nome_resp_display = nome_resp if nome_resp else "____________________"
    c.drawCentredString(margem_esq + 400, y_ass - 10, f"{nome_resp_display}")
    c.drawCentredString(margem_esq + 400, y_ass - 20, "Responsável pela Entrega")


@app.route('/imprimir_termos_lote')
@login_required
def imprimir_termos_lote():
    # Filtros da URL
    uvr = request.args.get('uvr')
    funcao = request.args.get('funcao')
    data_ini = request.args.get('data_ini')
    data_fim = request.args.get('data_fim')
    termo = request.args.get('q') # Busca textual
    
    conn = conectar_banco()
    cur = conn.cursor()
    
    # 1. Monta a Query Base de Entregas
    sql = """
        SELECT e.id, e.data_entrega, 
               rec.nome, rec.cpf, rec.funcao, e.associacao,
               resp.nome as nome_responsavel
        FROM epi_entregas e
        JOIN associados rec ON e.id_associado = rec.id
        LEFT JOIN associados resp ON e.id_responsavel = resp.id
        LEFT JOIN epi_entrega_itens ei ON e.id = ei.id_entrega
        LEFT JOIN epi_itens i ON ei.id_item = i.id
        WHERE 1=1
    """
    params = []
    
    # Filtros de Permissão
    if current_user.role != 'admin':
        if getattr(current_user, 'associacao', None):
            sql += " AND e.associacao = %s"
            params.append(current_user.associacao)
        elif current_user.uvr_acesso:
            sql += " AND e.uvr = %s"
            params.append(current_user.uvr_acesso)
    
    # Filtros Dinâmicos
    if uvr:
        sql += " AND e.uvr = %s"
        params.append(uvr)
    if funcao and funcao != 'Todas':
        sql += " AND rec.funcao = %s"
        params.append(funcao)
    if data_ini:
        sql += " AND e.data_entrega >= %s"
        params.append(data_ini)
    if data_fim:
        sql += " AND e.data_entrega <= %s"
        params.append(data_fim)
    if termo:
        term = f"%{termo}%"
        sql += " AND (rec.nome ILIKE %s OR i.nome ILIKE %s OR i.ca ILIKE %s)"
        params.extend([term, term, term])
        
    # Agrupamento para não duplicar cabeçalhos devido ao join de itens na busca
    sql += " GROUP BY e.id, rec.nome, rec.cpf, rec.funcao, e.associacao, resp.nome ORDER BY e.data_entrega DESC, rec.nome ASC"
    
    cur.execute(sql, tuple(params))
    entregas_cabecalho = cur.fetchall()
    
    if not entregas_cabecalho:
        conn.close()
        return "Nenhuma entrega encontrada para os filtros selecionados.", 404

    # 2. Busca itens para cada entrega
    lista_completa = []
    for ent in entregas_cabecalho:
        ent_id = ent[0]
        cur.execute("""
            SELECT i.nome, i.ca, ei.quantidade, ei.unidade, ei.data_validade
            FROM epi_entrega_itens ei
            JOIN epi_itens i ON ei.id_item = i.id
            WHERE ei.id_entrega = %s
        """, (ent_id,))
        # Converte para lista de tuplas compatível com o helper
        itens = cur.fetchall()
        
        # Cria uma lista com (dados..., itens)
        dados_completos = list(ent) 
        dados_completos.append(itens)
        lista_completa.append(dados_completos)

    conn.close()

    # 3. Geração do PDF
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4 
    
    y_topo = height - 50
    y_base = (height / 2) - 50
    linha_corte = height / 2
    
    contador = 0
    
    for dados in lista_completa:
        posicao = contador % 2 # 0 = topo, 1 = base
        
        if posicao == 0:
            if contador > 0: c.showPage()
            desenhar_recibo_canvas(c, dados, y_topo)
            
            # Linha de corte
            c.setDash(4, 4)
            c.setLineWidth(1)
            c.line(20, linha_corte, width - 20, linha_corte)
            c.setDash(1, 0)
            c.setFont("Helvetica", 6)
            c.drawCentredString(width/2, linha_corte + 5, "- - - - CORTE AQUI - - - -")
            
        else:
            desenhar_recibo_canvas(c, dados, y_base)
            
        contador += 1
        
    c.save()
    buffer.seek(0)
    
    nome_arquivo = f"Termos_Lote_{datetime.now().strftime('%Y%m%d_%H%M')}.pdf"
    return send_file(buffer, as_attachment=True, download_name=nome_arquivo, mimetype='application/pdf')


@app.route('/gerar_declaracao_epi/<int:id_entrega>')
@login_required
def gerar_declaracao_epi(id_entrega):
    conn = None
    try:
        conn = conectar_banco()
        cur = conn.cursor()
        
        # 1. Busca dados da Entrega
        cur.execute("""
            SELECT e.id, e.data_entrega, 
                   recebedor.nome, recebedor.cpf, recebedor.funcao, 
                   e.associacao,
                   responsavel.nome
            FROM epi_entregas e
            JOIN associados recebedor ON e.id_associado = recebedor.id
            LEFT JOIN associados responsavel ON e.id_responsavel = responsavel.id
            WHERE e.id = %s
        """, (id_entrega,))
        entrega = cur.fetchone()
        
        if not entrega:
            return "Entrega não encontrada.", 404
            
        # 2. Busca os Itens
        cur.execute("""
            SELECT ep.nome, ep.ca, it.quantidade, it.unidade, it.data_validade
            FROM epi_entrega_itens it
            JOIN epi_itens ep ON it.id_item = ep.id
            WHERE it.id_entrega = %s
        """, (id_entrega,))
        itens = cur.fetchall()
        
        # Prepara dados para o helper (tupla + lista de itens)
        dados_completos = list(entrega)
        dados_completos.append(itens)
        
        # --- PDF ---
        buffer = io.BytesIO()
        c = canvas.Canvas(buffer, pagesize=A4)
        width, height = A4
        
        # Desenha apenas 1 recibo no topo da página
        y_topo = height - 50
        desenhar_recibo_canvas(c, dados_completos, y_topo)
        
        c.showPage()
        c.save()
        buffer.seek(0)
        
        return send_file(buffer, as_attachment=True, download_name=f"Termo_EPI_{id_entrega}.pdf", mimetype='application/pdf')

    except Exception as e:
        app.logger.error(f"Erro ao gerar PDF: {e}")
        return f"Erro ao gerar documento: {str(e)}", 500
    finally:
        if conn: conn.close()

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
@app.route("/sucesso_epi")
def sucesso_epi(): return pagina_sucesso_base("Sucesso", "EPI cadastrado com sucesso!")

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
    
    #TESTE