-- Remove internal competition-type id references from user-facing requirement labels.
--
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/038_requirement_labels_without_type_ids.sql

UPDATE officials_analysis.international_requirement_rule
SET display_label = trim(both ' ;' from regexp_replace(
    display_label,
    ';\s*types\s+15\s*[–-]\s*17',
    '',
    'gi'
))
WHERE display_label ~* 'types\s+15\s*[–-]\s*17';

UPDATE officials_analysis.international_requirement_rule
SET display_label = trim(both ' ;' from regexp_replace(
    display_label,
    ';\s*types\s+15\s*[–-]\s*16',
    '',
    'gi'
))
WHERE display_label ~* 'types\s+15\s*[–-]\s*16';

UPDATE officials_analysis.international_requirement_rule
SET display_label = trim(both ' ;' from regexp_replace(
    display_label,
    '\(\s*types\s+15\s*[–-]\s*17\s*\)',
    '',
    'gi'
))
WHERE display_label ~* '\(\s*types\s+15';
