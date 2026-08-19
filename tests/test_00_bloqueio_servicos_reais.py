"""Barreira global contra acesso acidental a serviços reais durante os testes."""

import unittest
from unittest.mock import patch


def _bloquear_postgresql(*args, **kwargs):
    raise AssertionError("Teste tentou abrir uma conexão PostgreSQL real.")


def _bloquear_cloudinary(*args, **kwargs):
    raise AssertionError("Teste tentou alterar o Cloudinary real.")


# Este arquivo é carregado primeiro pela descoberta alfabética. Os patches não
# são encerrados durante a suíte: todos os mocks específicos ficam por cima desta
# barreira e, ao terminarem, retornam para uma função que falha imediatamente.
PATCH_POSTGRESQL = patch("psycopg2.connect", side_effect=_bloquear_postgresql)
PATCH_CLOUDINARY_UPLOAD = patch(
    "cloudinary.uploader.upload", side_effect=_bloquear_cloudinary
)
PATCH_CLOUDINARY_DESTROY = patch(
    "cloudinary.uploader.destroy", side_effect=_bloquear_cloudinary
)
PATCH_POSTGRESQL.start()
PATCH_CLOUDINARY_UPLOAD.start()
PATCH_CLOUDINARY_DESTROY.start()


class TestBloqueioServicosReais(unittest.TestCase):
    def test_barreiras_globais_estao_ativas(self):
        """A suíte deve falhar antes de qualquer conexão ou upload real."""
        import cloudinary.uploader
        import psycopg2

        with self.assertRaises(AssertionError):
            psycopg2.connect("postgresql://endereco-proibido")
        with self.assertRaises(AssertionError):
            cloudinary.uploader.upload(b"arquivo-proibido")
        with self.assertRaises(AssertionError):
            cloudinary.uploader.destroy("arquivo-proibido")
