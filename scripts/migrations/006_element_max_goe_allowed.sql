-- Max GOE allowed given element markings / info column (for rule-error analysis).

ALTER TABLE element
    ADD COLUMN IF NOT EXISTS max_goe_allowed NUMERIC;
