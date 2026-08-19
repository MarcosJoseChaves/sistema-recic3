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


def normalizar_caminho_cloudinary(valor):
    """Normaliza um caminho sem permitir navegação entre pastas."""
    texto = str(valor or "").strip()
    if "\\" in texto:
        raise CloudinaryStorageError(
            "O caminho configurado para documentos é inválido."
        )
    segmentos = []
    for segmento in texto.split("/"):
        segmento = segmento.strip()
        if not segmento:
            continue
        if segmento in {".", ".."}:
            raise CloudinaryStorageError(
                "O caminho configurado para documentos é inválido."
            )
        segmentos.append(segmento)
    return "/".join(segmentos)


def compor_caminho_cloudinary(prefixo, subpasta):
    """Une prefixo e subpasta sem repetir o prefixo já presente."""
    prefixo_normalizado = normalizar_caminho_cloudinary(prefixo)
    subpasta_normalizada = normalizar_caminho_cloudinary(subpasta)
    if not prefixo_normalizado:
        return subpasta_normalizada
    if not subpasta_normalizada:
        return prefixo_normalizado
    if (
        subpasta_normalizada == prefixo_normalizado
        or subpasta_normalizada.startswith(f"{prefixo_normalizado}/")
    ):
        return subpasta_normalizada
    return f"{prefixo_normalizado}/{subpasta_normalizada}"


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
        ambiente = (os.getenv("APP_ENV") or "development").strip().lower()
        prefixo_configurado = os.getenv("CLOUDINARY_FOLDER_PREFIX")
        if ambiente in {"homologation", "production"} and not (
            prefixo_configurado or ""
        ).strip().strip("/"):
            raise CloudinaryStorageError(
                "O prefixo do armazenamento de documentos não está configurado."
            )
        if prefixo_configurado is None and ambiente in {"development", "testing"}:
            prefixo_configurado = f"{ambiente}/fiscalizacao-contratos"
        self.prefixo = normalizar_caminho_cloudinary(prefixo_configurado)
        if ambiente in {"homologation", "production"} and not self.prefixo:
            raise CloudinaryStorageError(
                "O prefixo do armazenamento de documentos não está configurado."
            )
        cloudinary.config(
            cloud_name=cloud_name,
            api_key=api_key,
            api_secret=api_secret,
            secure=True,
        )

    def enviar(self, arquivo, contrato_id, aditivo_id=None):
        subpasta = f"contratos/{contrato_id}"
        if aditivo_id:
            subpasta += f"/aditivos/{aditivo_id}"
        pasta = compor_caminho_cloudinary(self.prefixo, subpasta)
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
