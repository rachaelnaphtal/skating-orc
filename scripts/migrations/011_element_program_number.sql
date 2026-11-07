-- Program order of an element within a skater's segment.
-- Same element name can occur twice (two 2Lze); name alone is not an identity.

ALTER TABLE element
    ADD COLUMN IF NOT EXISTS element_number INTEGER;

CREATE UNIQUE INDEX IF NOT EXISTS element_skater_segment_element_number_uidx
    ON element (skater_segment_id, element_number)
    WHERE element_number IS NOT NULL;
