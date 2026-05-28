-- Indexes for activity tracker / qualifying availability query patterns.
-- Safe to re-run (IF NOT EXISTS). Run after directory data is loaded:
--   psql "$DATABASE_URL" -f activityAnalysis/migrations/010_activity_tracker_query_indexes.sql
-- Then: ANALYZE officials_analysis.assignment;
--       ANALYZE officials_analysis.appointments;
--       ANALYZE officials_analysis.competition;

-- Assignment lookups: official_id IN (...), often + appointment_type_id / discipline_id.
CREATE INDEX IF NOT EXISTS idx_assignment_official_id
    ON officials_analysis.assignment (official_id);

CREATE INDEX IF NOT EXISTS idx_assignment_official_role_disc
    ON officials_analysis.assignment (official_id, appointment_type_id, discipline_id);

CREATE INDEX IF NOT EXISTS idx_assignment_competition_id
    ON officials_analysis.assignment (competition_id);

-- Competition filters by type (sectionals, championships, referee reports).
CREATE INDEX IF NOT EXISTS idx_competition_competition_type_id
    ON officials_analysis.competition (competition_type_id);

-- Appointments: eligible-official scans and per-official active rows.
CREATE INDEX IF NOT EXISTS idx_appointments_active_official_id
    ON officials_analysis.appointments (official_id)
    WHERE active IS TRUE;

CREATE INDEX IF NOT EXISTS idx_appointments_eligibility
    ON officials_analysis.appointments (appointment_type_id, level_id, discipline_id, official_id)
    WHERE active IS TRUE;

-- segment_official → competition (protocol / other-comps counts).
CREATE INDEX IF NOT EXISTS ix_segment_competition_id
    ON public.segment (competition_id);

-- Qualifying report joins (form + official).
CREATE INDEX IF NOT EXISTS qualifying_official_form_response_form_official_idx
    ON officials_analysis.qualifying_official_form_response (form_id, official_id);

CREATE INDEX IF NOT EXISTS qualifying_official_comp_avail_form_official_idx
    ON officials_analysis.qualifying_official_competition_availability (form_id, official_id);
