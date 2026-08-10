"""Infraestrutura controlada de migrations da baseline nuclear.

Importar este pacote não lê ambiente, não abre conexão e não executa migration.
"""

from .checksum import calcular_sha256_normalizado, normalizar_utf8_lf
from .bootstrap import executar_bootstrap_controlado
from .manifest import carregar_manifesto
from .runner import MigrationRunner

__all__ = [
    "MigrationRunner",
    "calcular_sha256_normalizado",
    "carregar_manifesto",
    "executar_bootstrap_controlado",
    "normalizar_utf8_lf",
]
