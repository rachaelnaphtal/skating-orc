# IJS discovery and batch load

Scripts to find USFS IJS competition index pages and load them into `public.competition` (and optionally scrape all segments into the database).

**Prerequisites**

```bash
cd /path/to/JudgingAnalysis
source venv/bin/activate
export DATABASE_URL='postgresql://...'   # required for --skip-if-in-database and for load
```

One-off loads from the **Load Competition** page in `analysis_app.py` are unchanged; these scripts are for bulk discovery and CSV-driven batch ingest.

---

## Element judge calibration (SQL)

**File:** `element_judge_calibration.sql`

Ranks judges on element GOE marking vs global expectations by **discipline**, **element type**, **GOE given**, and **panel spread** (with coarser benchmark fallbacks). Merges directory-linked judge aliases like the analytics app.

1. Edit the params block at the top (`discipline_filter`, `competition_scope`, optional date filters, min sample sizes).
2. Optionally `INSERT` into `_calibration_seasons` for one or more `competition.year` values (e.g. `2425`, `2526`). Leave that table empty to use all seasons.
3. Run the **entire** script in one session (DBeaver/pgAdmin) or:

```bash
psql "$DATABASE_URL" -f scripts/element_judge_calibration.sql
```

Result sets: sample benchmarks, coverage summary, then **judge_ranks** (`mean_abs_z` lower = closer to population norms in comparable scenarios). Temp tables `_element_scenario_benchmarks` and `_element_mark_scored` remain for ad-hoc queries until you disconnect.

**Season examples** (uncomment `INSERT` in the SQL file):

```sql
INSERT INTO _calibration_seasons (season_year) VALUES ('2425'), ('2526');
```

**Date range example** (in `_calibration_params`; uses `COALESCE(start_date, end_date)` on each competition):

```sql
DATE '2024-07-01' AS start_date_filter,
DATE '2025-06-30' AS end_date_filter,
```

You can combine seasons and dates. Competitions missing both `start_date` and `end_date` are dropped when either date filter is set.

**Sectionals + nationals** (same as Cross-Judge “Sectionals & championships”):

```sql
'sectionals_and_championships'::text AS competition_scope,
```

Includes officials types **1–9** (SPD/SYS sectionals, US Championships, US Synchro Championships). Excludes NQS (10), nonqualifying (11), and adult/collegiate (12–14). Competitions must have `officials_analysis_competition_type_id` linked at load time.

**Nationals only:**

```sql
'championships_only'::text AS competition_scope,
```

Types **4** and **8** only.

---

## 1. Discover competitions

**Script:** `discover_usfs_ijs_competitions.py`

Probes URLs of the form:

`https://ijs.usfigureskating.org/leaderboard/results/{year}/{numeric_id}/index.asp`

A row is written when the response is HTTP 200 and the page looks like a real index (at least one `Final` link). Output is flushed after each row (safe to Ctrl+C).

**Typical command**

```bash
python scripts/discover_usfs_ijs_competitions.py \
  --years 2024 \
  --start-id 30000 \
  --end-id 37500 \
  --skip-if-in-database \
  -o discovered_2024.csv
```

**Important:** Each run **overwrites** the output file (`-o`). Copy or rename the CSV before re-running if you want to keep a previous run.

### Discover flags

| Flag | Default | Description |
|------|---------|-------------|
| `--years` | `2026` | Calendar year in the URL path; comma-separated (e.g. `2024,2025`) |
| `--start-id` | *(required)* | First numeric competition ID to probe |
| `--end-id` | *(required)* | Last numeric ID (inclusive) |
| `-o` / `--output` | `discovered_ijs_competitions.csv` | Output CSV path |
| `--skip-if-in-database` | off | Skip probes when base URL already exists in `public.competition.results_url` (needs `DATABASE_URL`) |
| `--delay` | `0.75` | Seconds to wait after each HTTP probe (DB skips do not sleep) |
| `--step` | `1` | Probe every Nth ID |
| `--timeout` | `30` | HTTP timeout (seconds) |
| `--progress-every` | `1` | Print progress every N probes |
| `--quiet` | off | No progress on stderr |
| `--log-requests` | off | Log every URL and HTTP status |
| `--include-misses` | off | Also write non-hit rows to the CSV |

### Choosing ID ranges

Numeric IDs are **not** the same as season year. The path year is usually the calendar year of the event. Older events may use **slug** paths (e.g. `…/2014/2014_eastern_fs_sectionals/`) — this script only finds **numeric** IDs.

**See what you already have in the DB** (`results_url` has no `/index.asp` suffix):

```sql
SELECT
  (regexp_match(results_url, '/leaderboard/results/([0-9]{4})/([0-9]+)'))[1] AS path_year,
  min((regexp_match(results_url, '/leaderboard/results/[0-9]{4}/([0-9]+)'))[1]::int) AS min_id,
  max((regexp_match(results_url, '/leaderboard/results/[0-9]{4}/([0-9]+)'))[1]::int) AS max_id,
  count(*) AS n
FROM public.competition
WHERE results_url ~ '/leaderboard/results/[0-9]{4}/[0-9]+'
GROUP BY 1
ORDER BY 1;
```

Probe from roughly **min − 200** through **max + 200–500** for that path year, or extend upward if you are looking for new events.

Rough guide from prior discovery on `/2025/` (after sweeping 30 000–40 000):

| ID band | Typical yield |
|---------|----------------|
| 30 000–34 199 | Very sparse (a few sectionals-style outliers) |
| 34 200–36 800 | Most competitions |
| 36 801+ on `/2025/` | Often empty; try `/2026/` for newer events |

### Discover CSV columns

`year`, `competition_id`, `url`, `http_status`, `competition_name`, `start_date`, `end_date`, `location`, `fetched_at_utc`, `probe_error`

---

## 2. Load from CSV

**Script:** `load_discovered_ijs_competitions_csv.py`

Reads a discover CSV and either upserts competition metadata only, or runs a full `downloadResults.scrape()` per row (`write_to_database=True`, `write_excel=False`).

**Metadata only** (name, dates, location — no segments):

```bash
python scripts/load_discovered_ijs_competitions_csv.py discovered_2024.csv \
  --metadata-only \
  --season-year 2425 \
  --officials-analysis-competition-type-id 11
```

**Full scrape** (segments, scores, officials):

```bash
python scripts/load_discovered_ijs_competitions_csv.py discovered_2024.csv \
  --officials-analysis-competition-type-id 11 \
  --season-year 2425 \
  --quiet
```

Add `--log-file load_2024.log` to keep full DEBUG detail in a file while the terminal stays quiet.

**Dry run** (print planned actions only):

```bash
python scripts/load_discovered_ijs_competitions_csv.py discovered_2024.csv \
  --dry-run \
  --officials-analysis-competition-type-id 11 \
  --season-year 2425
```

Full scrape **requires** `--officials-analysis-competition-type-id` unless every row has `officials_analysis_competition_type_id` or `competition_type_id` in the CSV.

Eligible rows: `http_status=200`, non-empty `competition_name`, empty `probe_error`, valid `url`.

### Load flags

| Flag | Default | Description |
|------|---------|-------------|
| `csv_path` | *(positional)* | Discover output CSV |
| `--metadata-only` | off | Upsert `public.competition` only; no scrape |
| `--dry-run` | off | Print plan only |
| `--officials-analysis-competition-type-id` | none | Default `officials_analysis.competition_type` id for full scrape |
| `--season-year` | `2526` | Stored on `competition.year`; CSV column `season_year` overrides per row |
| `--limit` | none | Process at most N eligible rows (after offset) |
| `--start-offset` | `0` | Skip first N eligible rows |
| `--delay` | `0` | Seconds to sleep after each full scrape |
| `--event-regex-custom` | empty | Custom `event_regex` for every scrape |
| `--event-levels` | empty | Comma-separated level preset(s); see `event_regex_presets.py` |
| `--event-disciplines` | empty | Comma-separated discipline preset(s) |
| `--judge-filter` | empty | Same as Load Competition UI |
| `--specific-exclude` | empty | Exclude matching events |
| `--only-rule-errors` | off | Passed through to `scrape()` |
| `--pdf-folder` | empty | Only if using PDF scrape mode |
| `--default-qualifying` / `--default-nqs` | none | Metadata-only defaults when CSV omits flags |
| `--quiet` | off | WARNING+ only; per competition: `HH:MM:SS start` and `done (Xm Ys)` lines |
| `--verbose` | off | Console DEBUG (very noisy) |
| `--log-file` | empty | Also write DEBUG lines to a file |

### Batch scrape behavior (full load only)

When not using `--metadata-only` or `--dry-run`, the loader:

- Reuses one HTTP session and one DB session for the entire CSV run
- Passes CSV `start_date` / `end_date` / `location` into `scrape()` (skips an extra index fetch when both dates are present)
- Commits once per competition instead of after every segment

One-off **Load Competition** in the app does not use these options; behavior there is unchanged.

Process a slice of the CSV (e.g. parallel terminals):

```bash
python scripts/load_discovered_ijs_competitions_csv.py discovered_2024.csv \
  --start-offset 0 --limit 50 \
  --officials-analysis-competition-type-id 11 --season-year 2425
```

---

## Suggested workflow

1. **Discover** → `discovered_YYYY.csv` with `--skip-if-in-database`
2. Review CSV (names, dates, locations)
3. Optional: **`--metadata-only`** to register competitions quickly
4. **Full load** with `--officials-analysis-competition-type-id` and `--season-year`
5. Re-discover with a higher `--end-id` or another `--years` value when adding new events

**Example — probe 2026 tail for new events**

```bash
python scripts/discover_usfs_ijs_competitions.py \
  --years 2026 --start-id 36800 --end-id 37500 \
  --skip-if-in-database -o discovered_2026.csv
```

---

## Help

```bash
python scripts/discover_usfs_ijs_competitions.py --help
python scripts/load_discovered_ijs_competitions_csv.py --help
```
