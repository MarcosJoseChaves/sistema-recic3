"""Executa manualmente a migração legada de produtos e subgrupos."""

import argparse


CONFIRMACAO_EXIGIDA = "MIGRAR_PRODUTOS_ANTIGOS"


def main():
    parser = argparse.ArgumentParser(
        description="Executa uma migração legada que altera dados do banco configurado."
    )
    parser.add_argument(
        "--confirmar",
        help=f"Informe exatamente {CONFIRMACAO_EXIGIDA} para prosseguir.",
    )
    argumentos = parser.parse_args()
    if argumentos.confirmar != CONFIRMACAO_EXIGIDA:
        parser.error("Confirmação inválida; nenhuma migração foi executada.")

    from app import migrar_dados_antigos_produtos

    migrar_dados_antigos_produtos()


if __name__ == "__main__":
    main()
