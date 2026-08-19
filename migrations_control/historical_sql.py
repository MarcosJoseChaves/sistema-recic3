"""Adaptação estrita, somente em memória, dos wrappers históricos."""

from __future__ import annotations

import re

from .errors import MigrationExecutionError


_TRANSACAO = re.compile(r"^\s*(BEGIN(?:\s+TRANSACTION)?|COMMIT|ROLLBACK)\s*;?\s*$", re.I)


def _segmentar(sql: str) -> list[tuple[int, int, str]]:
    segmentos = []
    inicio = 0
    indice = 0
    estado = "normal"
    dollar = ""
    while indice < len(sql):
        atual = sql[indice]
        proximo = sql[indice + 1] if indice + 1 < len(sql) else ""
        if estado == "normal":
            if atual == "'": estado = "aspas_simples"
            elif atual == '"': estado = "aspas_duplas"
            elif atual == "-" and proximo == "-": estado = "comentario_linha"; indice += 1
            elif atual == "/" and proximo == "*": estado = "comentario_bloco"; indice += 1
            elif atual == "$":
                encontrado = re.match(r"\$[A-Za-z_][A-Za-z0-9_]*\$|\$\$", sql[indice:])
                if encontrado:
                    dollar = encontrado.group(0); estado = "dollar"; indice += len(dollar) - 1
            elif atual == ";":
                segmentos.append((inicio, indice + 1, sql[inicio:indice + 1]))
                inicio = indice + 1
        elif estado == "aspas_simples":
            if atual == "'" and proximo == "'": indice += 1
            elif atual == "'": estado = "normal"
        elif estado == "aspas_duplas":
            if atual == '"' and proximo == '"': indice += 1
            elif atual == '"': estado = "normal"
        elif estado == "comentario_linha":
            if atual in "\r\n": estado = "normal"
        elif estado == "comentario_bloco":
            if atual == "*" and proximo == "/": estado = "normal"; indice += 1
        elif estado == "dollar" and sql.startswith(dollar, indice):
            estado = "normal"; indice += len(dollar) - 1
        indice += 1
    if estado not in {"normal", "comentario_linha"}:
        raise MigrationExecutionError("SQL histórico possui estrutura léxica incompleta.")
    if sql[inicio:].strip():
        segmentos.append((inicio, len(sql), sql[inicio:]))
    return segmentos


def adaptar_wrapper_historico(sql: str) -> str:
    """Retira exclusivamente BEGIN inicial e COMMIT final validados."""
    if not isinstance(sql, str) or not sql.strip():
        raise MigrationExecutionError("SQL histórico vazio.")
    segmentos = _segmentar(sql)
    uteis = [segmento for segmento in segmentos if segmento[2].strip()]
    if len(uteis) < 3:
        raise MigrationExecutionError("Wrapper histórico incompleto.")
    primeiro, ultimo = uteis[0], uteis[-1]
    if not re.fullmatch(r"\s*BEGIN(?:\s+TRANSACTION)?\s*;\s*", primeiro[2], re.I):
        raise MigrationExecutionError("BEGIN externo ausente ou ambíguo.")
    if not re.fullmatch(r"\s*COMMIT\s*;\s*", ultimo[2], re.I):
        raise MigrationExecutionError("COMMIT externo ausente ou ambíguo.")
    for _, _, segmento in uteis[1:-1]:
        if _TRANSACAO.fullmatch(segmento):
            raise MigrationExecutionError("Controle transacional intermediário proibido.")
    corpo = sql[primeiro[1]:ultimo[0]]
    if not corpo.strip():
        raise MigrationExecutionError("Corpo histórico vazio.")
    return corpo
