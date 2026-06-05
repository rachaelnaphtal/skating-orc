-- Protocol info column (F, e, q, etc.) for element rule-error analysis.

ALTER TABLE element
    ADD COLUMN IF NOT EXISTS notes VARCHAR;
