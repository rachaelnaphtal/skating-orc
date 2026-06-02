# Heroku deploy (activity analysis apps)

This repo ships multiple Streamlit apps from one codebase.

`setup.sh` runs before Streamlit on every deploy. It writes `~/.streamlit/config.toml` and `~/.streamlit/secrets.toml` from Heroku config (see [`scripts/write_streamlit_secrets.py`](../scripts/write_streamlit_secrets.py)).

## Recommended: one root Procfile + `STREAMLIT_APP`

The root [`Procfile`](../Procfile) works for every app. Set which Streamlit entrypoint to run per Heroku app:

| Heroku app | `STREAMLIT_APP` | Notes |
|------------|-----------------|-------|
| Judging analysis (default) | *(unset)* → `analysis_app.py` | Existing production app |
| Activity tracker | `activityAnalysis/activity_tracker_app.py` | National qualifying activity |
| International officials | `activityAnalysis/international_officials_app.py` | ISU / international panel activity |

**No multi-procfile buildpack needed** for this approach.

```bash
heroku create your-intl-officials-app
heroku git:remote -a your-intl-officials-app -r intl-officials

heroku config:set STREAMLIT_APP=activityAnalysis/international_officials_app.py -a your-intl-officials-app

# Database (at least one is required):
heroku config:set PG_DB_URL='postgresql://...' -a your-intl-officials-app
# or Heroku Postgres addon on this app (sets DATABASE_URL), or DB_HOST/DB_NAME/DB_USERNAME/DB_PASSWORD

git push intl-officials main
```

## Alternative: multi-procfile buildpack

Use this only if you want a **different Procfile file** per app instead of `STREAMLIT_APP`.

The [multi-procfile buildpack](https://elements.heroku.com/buildpacks/heroku/heroku-buildpack-multi-procfile) copies your chosen file to `/app/Procfile` **during the build**. It must run **before** `heroku/python`. If you add it with `heroku buildpacks:add` alone, it is appended last and the build fails with “Procfile not found”.

```bash
# 1. Buildpack order matters — multi-procfile FIRST, then Python
heroku buildpacks:clear -a your-intl-officials-app
heroku buildpacks:add --index 1 heroku-community/multi-procfile -a your-intl-officials-app
heroku buildpacks:add heroku/python -a your-intl-officials-app
heroku buildpacks -a your-intl-officials-app
# Expected:
#   1. heroku-community/multi-procfile
#   2. heroku/python

# 2. Point at the international officials Procfile (must exist in git on the branch you push)
heroku config:set PROCFILE=activityAnalysis/Procfile.international_officials -a your-intl-officials-app

# 3. Database config (same as above), then deploy
git push intl-officials main
```

Available Procfile paths in this repo:

| Path | Starts |
|------|--------|
| `Procfile` | `analysis_app.py` (or `STREAMLIT_APP` override) |
| `activityAnalysis/Procfile` | Activity tracker (or `STREAMLIT_APP` override) |
| `activityAnalysis/Procfile.international_officials` | International officials app |

Build log should include: `Copied activityAnalysis/Procfile.international_officials as Procfile successfully`. If you see `PROCFILE was not set`, the config var is missing at build time. If you see `FAILED to copy a Procfile`, the path is wrong or the file was not pushed to Heroku.

**Pipelines:** slug promotion does not re-run the multi-procfile buildpack; set `PROCFILE` on the app that **builds** the slug.

## Database config

At least one of these must be set on the Heroku app:

- `PG_DB_URL=postgresql://...` (copy from your working app), or
- Heroku Postgres addon (sets `DATABASE_URL` automatically), or
- `DB_HOST`, `DB_NAME`, `DB_USERNAME`, `DB_PASSWORD` (remote host — not `localhost`)

## Migrations

Apply requirement-rule migrations on the shared database before relying on promote/maintain checks:

```bash
psql "$DATABASE_URL" -f activityAnalysis/migrations/023_competition_alternatives_intl_all.sql
psql "$DATABASE_URL" -f activityAnalysis/migrations/024_synch_do_requirement_fixes.sql
psql "$DATABASE_URL" -f activityAnalysis/migrations/025_synch_isu_tc_ts_maintain.sql
```

## Local run

```bash
source venv/bin/activate
streamlit run activityAnalysis/international_officials_app.py
```

Configure `.streamlit/secrets.toml` with `DATABASE_URL` or `connections.postgresql` like the judging analysis app.
