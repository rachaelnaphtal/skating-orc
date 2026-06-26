-- Indexes for Total Activity Across Seasons report (segment_official panel scans).
-- Safe to re-run. After applying:
--   ANALYZE public.segment_official;
--   ANALYZE public.segment;
--   ANALYZE public.competition;

CREATE INDEX IF NOT EXISTS ix_segment_discipline_type_id
    ON public.segment (discipline_type_id);
