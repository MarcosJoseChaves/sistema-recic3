-- Ajustes incrementais do modelo normativo ambiental.
-- Permite norma em RASCUNHO sem vigencia definida e preserva
-- separadamente o titulo dos quesitos da matriz normativa.

ALTER TABLE amb_normas_versoes
    ALTER COLUMN vigencia_inicio DROP NOT NULL;

ALTER TABLE amb_normas_versoes
    DROP CONSTRAINT ck_amb_normas_versoes__vigencia;

ALTER TABLE amb_normas_versoes
    ADD CONSTRAINT ck_amb_normas_versoes__vigencia
    CHECK (
        (estado = 'RASCUNHO' OR vigencia_inicio IS NOT NULL)
        AND (
            vigencia_fim IS NULL
            OR (
                vigencia_inicio IS NOT NULL
                AND vigencia_fim >= vigencia_inicio
            )
        )
    );

ALTER TABLE amb_quesitos
    ADD COLUMN titulo TEXT;

ALTER TABLE amb_quesitos
    ADD CONSTRAINT ck_amb_quesitos__titulo_preenchido
    CHECK (
        titulo IS NULL
        OR btrim(titulo) <> ''
    );
