"""Validação segura dos metadados e do conteúdo de documentos."""

import hashlib
import io
import os
import zipfile

from werkzeug.utils import secure_filename


CATEGORIAS_DOCUMENTO = (
    "Contrato",
    "Edital",
    "Termo de Referência",
    "Estudo Técnico Preliminar",
    "Proposta",
    "Planilha Orçamentária",
    "Ordem de Serviço",
    "Aditivo",
    "Apostilamento",
    "Garantia",
    "ART ou RRT",
    "Notificação",
    "Relatório",
    "Parecer",
    "Comprovante",
    "Outro",
)

MIME_POR_EXTENSAO = {
    "pdf": "application/pdf",
    "doc": "application/msword",
    "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    "xls": "application/vnd.ms-excel",
    "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "csv": "text/csv",
    "jpg": "image/jpeg",
    "jpeg": "image/jpeg",
    "png": "image/png",
    "odt": "application/vnd.oasis.opendocument.text",
    "ods": "application/vnd.oasis.opendocument.spreadsheet",
}

ASSINATURA_OLE = bytes.fromhex("D0CF11E0A1B11AE1")


class ValidacaoDocumentoError(ValueError):
    """Erro amigável de validação do formulário ou arquivo."""


def limite_upload_bytes():
    try:
        megabytes = int(os.getenv("FC_MAX_UPLOAD_MB", "20"))
    except (TypeError, ValueError):
        megabytes = 20
    if megabytes <= 0:
        megabytes = 20
    return megabytes * 1024 * 1024


def _id_positivo(valor, mensagem, *, opcional=False):
    if opcional and not valor:
        return None
    try:
        numero = int(valor)
    except (TypeError, ValueError):
        raise ValidacaoDocumentoError(mensagem)
    if numero <= 0:
        raise ValidacaoDocumentoError(mensagem)
    return numero


def normalizar_metadados_documento(formulario):
    contrato_id = _id_positivo(
        formulario.get("contrato_id"), "Selecione um contrato válido."
    )
    aditivo_id = _id_positivo(
        formulario.get("aditivo_id"),
        "Selecione um aditivo válido.",
        opcional=True,
    )
    categoria = (formulario.get("categoria") or "").strip()
    titulo = (formulario.get("titulo") or "").strip()
    if categoria not in CATEGORIAS_DOCUMENTO:
        raise ValidacaoDocumentoError("Selecione uma categoria válida.")
    if not titulo:
        raise ValidacaoDocumentoError("O título do documento é obrigatório.")
    return {
        "contrato_id": contrato_id,
        "aditivo_id": aditivo_id,
        "categoria": categoria,
        "titulo": titulo,
        "descricao": (formulario.get("descricao") or "").strip() or None,
    }


def validar_arquivo_documento(arquivo, limite_bytes=None):
    if not arquivo or not getattr(arquivo, "filename", ""):
        raise ValidacaoDocumentoError("Selecione um arquivo.")

    nome_original = secure_filename(arquivo.filename)
    if not nome_original or "." not in nome_original:
        raise ValidacaoDocumentoError("O nome do arquivo é inválido.")
    extensao = nome_original.rsplit(".", 1)[1].lower()
    if extensao not in MIME_POR_EXTENSAO:
        raise ValidacaoDocumentoError("Este tipo de arquivo não é permitido.")

    limite = limite_bytes if limite_bytes is not None else limite_upload_bytes()
    conteudo = arquivo.stream.read(limite + 1)
    try:
        if not conteudo:
            raise ValidacaoDocumentoError("O arquivo está vazio.")
        if len(conteudo) > limite:
            raise ValidacaoDocumentoError(
                f"O arquivo ultrapassa o limite de {limite // (1024 * 1024)} MB."
            )
        if not _conteudo_compativel(extensao, conteudo):
            raise ValidacaoDocumentoError(
                "O conteúdo do arquivo não corresponde ao tipo informado."
            )

        return {
            "nome_original": nome_original,
            "extensao": extensao,
            "mime_type": MIME_POR_EXTENSAO[extensao],
            "tamanho_bytes": len(conteudo),
            "sha256": hashlib.sha256(conteudo).hexdigest(),
            "conteudo": conteudo,
        }
    finally:
        arquivo.stream.seek(0)


def _conteudo_compativel(extensao, conteudo):
    if extensao == "pdf":
        return conteudo.startswith(b"%PDF-")
    if extensao in ("jpg", "jpeg"):
        return conteudo.startswith(b"\xff\xd8\xff")
    if extensao == "png":
        return conteudo.startswith(b"\x89PNG\r\n\x1a\n")
    if extensao in ("doc", "xls"):
        return conteudo.startswith(ASSINATURA_OLE)
    if extensao == "csv":
        return _parece_texto(conteudo)
    if extensao in ("docx", "xlsx", "odt", "ods"):
        return _zip_compativel(extensao, conteudo)
    return False


def _parece_texto(conteudo):
    if b"\x00" in conteudo:
        return False
    for codificacao in ("utf-8-sig", "latin-1"):
        try:
            texto = conteudo.decode(codificacao)
            controles = sum(
                1
                for caractere in texto
                if not caractere.isprintable() and caractere not in "\r\n\t"
            )
            return controles <= max(1, len(texto) // 100)
        except UnicodeDecodeError:
            continue
    return False


def _zip_compativel(extensao, conteudo):
    try:
        with zipfile.ZipFile(io.BytesIO(conteudo)) as pacote:
            nomes = set(pacote.namelist())
            if extensao == "docx":
                return "[Content_Types].xml" in nomes and any(
                    nome.startswith("word/") for nome in nomes
                )
            if extensao == "xlsx":
                return "[Content_Types].xml" in nomes and any(
                    nome.startswith("xl/") for nome in nomes
                )
            if "mimetype" not in nomes:
                return False
            tipo = pacote.read("mimetype").decode("ascii", errors="ignore")
            esperado = MIME_POR_EXTENSAO[extensao]
            return tipo == esperado
    except (zipfile.BadZipFile, KeyError, OSError):
        return False


def formatar_tamanho_bytes(valor):
    try:
        tamanho = int(valor)
    except (TypeError, ValueError):
        return "-"
    if tamanho < 1024:
        return f"{tamanho} bytes"
    if tamanho < 1024 * 1024:
        return f"{tamanho / 1024:.1f} KB".replace(".", ",")
    return f"{tamanho / (1024 * 1024):.1f} MB".replace(".", ",")
