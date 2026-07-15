"""Armazenamento privado de documentos no Cloudinary."""

import io
import os
import time
import uuid

import cloudinary
import cloudinary.uploader
import cloudinary.utils


class CloudinaryStorageError(Exception):
    """Falha tratada do armazenamento externo."""


class CloudinaryStorage:
    PROVEDOR = "cloudinary"

    def __init__(self):
        cloud_name = os.getenv("CLOUDINARY_CLOUD_NAME")
        api_key = os.getenv("CLOUDINARY_API_KEY")
        api_secret = os.getenv("CLOUDINARY_API_SECRET")
        if not all((cloud_name, api_key, api_secret)):
            raise CloudinaryStorageError(
                "O armazenamento de documentos não está configurado."
            )
        cloudinary.config(
            cloud_name=cloud_name,
            api_key=api_key,
            api_secret=api_secret,
            secure=True,
        )

    def enviar(self, arquivo, contrato_id, aditivo_id=None):
        pasta = f"sistema-recic3/fiscalizacao-contratos/contratos/{contrato_id}"
        if aditivo_id:
            pasta += f"/aditivos/{aditivo_id}"
        chave = f"{pasta}/{uuid.uuid4().hex}.{arquivo['extensao']}"
        try:
            resultado = cloudinary.uploader.upload(
                io.BytesIO(arquivo["conteudo"]),
                public_id=chave,
                resource_type="raw",
                type="authenticated",
                overwrite=False,
                unique_filename=False,
                use_filename=False,
            )
        except Exception as erro:
            raise CloudinaryStorageError(
                "Não foi possível enviar o documento. Tente novamente."
            ) from erro
        return {
            "armazenamento_provedor": self.PROVEDOR,
            "armazenamento_chave": resultado.get("public_id") or chave,
            "armazenamento_versao": resultado.get("version"),
        }

    def remover(self, chave):
        try:
            cloudinary.uploader.destroy(
                chave,
                resource_type="raw",
                type="authenticated",
                invalidate=True,
            )
        except Exception as erro:
            raise CloudinaryStorageError(
                "Não foi possível limpar o arquivo enviado."
            ) from erro

    def gerar_url_temporaria(self, chave, extensao, *, download=False):
        try:
            return cloudinary.utils.private_download_url(
                chave,
                extensao,
                resource_type="raw",
                type="authenticated",
                expires_at=int(time.time()) + 300,
                attachment=download,
            )
        except Exception as erro:
            raise CloudinaryStorageError(
                "Não foi possível abrir o documento agora."
            ) from erro
