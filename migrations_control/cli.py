"""CLI offline da infraestrutura inicial de migrations."""

from __future__ import annotations

import argparse
import json

from .errors import MigrationControlError, sanitizar_erro
from .manifest import carregar_manifesto
from .runner import MigrationRunner


COMANDOS_OFFLINE = ("validar-manifesto", "verificar-checksums", "mostrar-plano")


def criar_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="migrations-control")
    parser.add_argument(
        "comando",
        choices=COMANDOS_OFFLINE + ("preflight", "aplicar"),
    )
    parser.add_argument("--manifesto", help="Caminho local opcional do manifesto.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = criar_parser().parse_args(argv)
    try:
        if args.comando in {"preflight", "aplicar"}:
            print(json.dumps({
                "sucesso": False,
                "codigo": "CONEXAO_EXPLICITA_NAO_DISPONIVEL",
                "mensagem": "Este comando será habilitado em subetapa posterior com conexão explícita.",
            }, ensure_ascii=False))
            return 2
        manifesto = carregar_manifesto(args.manifesto)
        if args.comando == "mostrar-plano":
            plano = MigrationRunner(caminho_manifesto=args.manifesto).mostrar_plano()
            resultado = {
                "sucesso": True,
                "operacoes": [
                    {"identificador": item.identificador, "ordem": item.ordem, "estado": item.estado}
                    for item in plano
                ],
            }
        else:
            resultado = {
                "sucesso": True,
                "manifesto_versao": manifesto.versao_formato,
                "operacoes_validadas": len(manifesto.operacoes),
                "checksums_verificados": sum(op.checksum is not None for op in manifesto.operacoes),
            }
        print(json.dumps(resultado, ensure_ascii=False))
        return 0
    except MigrationControlError as erro:
        print(json.dumps({"sucesso": False, **sanitizar_erro(erro)}, ensure_ascii=False))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
