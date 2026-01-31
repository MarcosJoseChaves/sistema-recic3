import argparse
import csv
import os
import psycopg2
from dotenv import load_dotenv


def normalizar_coluna(texto):
    return (
        texto.strip()
        .lower()
        .replace("  ", " ")
        .replace("ç", "c")
        .replace("ã", "a")
        .replace("á", "a")
        .replace("à", "a")
        .replace("â", "a")
        .replace("é", "e")
        .replace("ê", "e")
        .replace("í", "i")
        .replace("ó", "o")
        .replace("ô", "o")
        .replace("ú", "u")
    )


def carregar_conexao():
    load_dotenv()
    database_url = os.getenv("DATABASE_URL")
    if database_url:
        print("☁️ Conectando à NUVEM/Definido no .env...")
        return psycopg2.connect(database_url, sslmode="require")
    print("🔧 Conectando ao LOCAL (recic3)...")
    return psycopg2.connect(
        host="localhost",
        database="recic3",
        user="postgres",
        password="postgres",
        port="5432",
    )


def abrir_csv(caminho_csv):
    tentativas = ["utf-8-sig", "utf-8", "latin-1"]
    ultimo_erro = None
    for encoding in tentativas:
        try:
            arquivo_csv = open(caminho_csv, encoding=encoding, newline="")
            amostra = arquivo_csv.read(4096)
            arquivo_csv.seek(0)
            try:
                dialect = csv.Sniffer().sniff(amostra, delimiters=";,\t")
            except csv.Error:
                dialect = csv.excel
                dialect.delimiter = ";"
            return arquivo_csv, dialect
        except UnicodeDecodeError as exc:
            ultimo_erro = exc
    raise ValueError(f"Não foi possível ler o CSV. Erro: {ultimo_erro}")


def importar_csv(caminho_csv):
    arquivo_csv, dialect = abrir_csv(caminho_csv)
    with arquivo_csv:
        leitor = csv.DictReader(arquivo_csv, dialect=dialect)
        if not leitor.fieldnames:
            raise ValueError("CSV sem cabeçalho.")

        mapa_colunas = {}
        for coluna in leitor.fieldnames:
            coluna_normalizada = normalizar_coluna(coluna)
            mapa_colunas[coluna_normalizada] = coluna

        aliases = {
            "grupo epi": "grupo",
            "epi": "epi",
            "ca": "ca",
            "tipo de protecao": "tipo_protecao",
            "tipo de proteção": "tipo_protecao",
            "funcao": "funcao",
            "função": "funcao",
            "tempo de troca": "tempo_troca",
        }

        colunas_final = {}
        for alias, destino in aliases.items():
            if alias in mapa_colunas:
                colunas_final[destino] = mapa_colunas[alias]

        faltando = [campo for campo in ["grupo", "epi"] if campo not in colunas_final]
        if faltando:
            raise ValueError(f"Colunas obrigatórias ausentes no CSV: {', '.join(faltando)}")

        conn = carregar_conexao()
        try:
            cur = conn.cursor()
            inseridos = 0
            linhas_lidas = 0
            for linha in leitor:
                linhas_lidas += 1
                grupo = (linha.get(colunas_final.get("grupo")) or "").strip()
                epi = (linha.get(colunas_final.get("epi")) or "").strip()
                if not grupo or not epi:
                    continue

                ca = (linha.get(colunas_final.get("ca")) or "").strip() or None
                tipo_protecao = (linha.get(colunas_final.get("tipo_protecao")) or "").strip() or None
                funcao = (linha.get(colunas_final.get("funcao")) or "").strip() or None
                tempo_troca = (linha.get(colunas_final.get("tempo_troca")) or "").strip() or None

                cur.execute(
                    """
                    INSERT INTO epis_catalogo
                        (grupo, epi, ca, tipo_protecao, funcao, tempo_troca)
                    VALUES
                        (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (grupo, epi, ca, funcao) DO NOTHING
                    """,
                    (grupo, epi, ca, tipo_protecao, funcao, tempo_troca),
                )
                if cur.rowcount:
                    inseridos += 1

            conn.commit()
            print("✅ Importação concluída.")
            print(f"📊 Linhas lidas do CSV: {linhas_lidas}")
            print(f"➕ EPIs inseridos: {inseridos}")
        finally:
            conn.close()


def main():
    parser = argparse.ArgumentParser(description="Importa EPIs de um CSV para a tabela epis_catalogo.")
    parser.add_argument(
        "--arquivo",
        default="epis.csv",
        help="Caminho do CSV (padrão: epis.csv).",
    )
    args = parser.parse_args()
    importar_csv(args.arquivo)


if __name__ == "__main__":
    main()