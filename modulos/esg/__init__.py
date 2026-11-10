"""Módulo de indicadores ESG Marechal Aterro Zero."""


def criar_blueprint_esg(repositorio=None):
    """Carrega Flask somente ao integrar a interface, mantendo contratos puros."""
    from .blueprint import criar_blueprint_esg as criar

    return criar(repositorio=repositorio)

__all__ = ["criar_blueprint_esg"]
