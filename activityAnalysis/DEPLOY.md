# Heroku deploy (activity analysis apps)

This repo ships multiple Streamlit apps from one codebase. Heroku reads the **root** [`Procfile`](../Procfile); which app starts is controlled by the `STREAMLIT_APP` config var.

`setup.sh` runs before Streamlit on every deploy. It writes `~/.streamlit/config.toml` and `~/.streamlit/secrets.toml` from Heroku config (see [`scripts/write_streamlit_secrets.py`](../scripts/write_streamlit_secrets.py)).

## Apps

| Heroku app | `STREAMLIT_APP` | Notes |
|------------|-----------------|-------|
| Judging analysis (default) | *(unset)* → `analysis_app.py` | Existing production app |
| Activity tracker | `activityAnalysis/activity_tracker_app.py` | National qualifying activity |
| International officials | `activityAnalysis/international_officials_app.py` | ISU / international panel activity |

## New international officials app

```bash
heroku create your-intl-officials-app
heroku git:remote -a your-intl-officials-app -r intl-officials

heroku config:set STREAMLIT_APP=activityAnalysis/international_officials_app.py -a your-intl-officials-app

# Same database secrets as your other deployed apps:
heroku config:set PG_DB_URL='postgresql://...' -a your-intl-officials-app
heroku config:set DB_HOST=... DB_PORT=5432 DB_NAME=... DB_USERNAME=... DB_PASSWORD=... -a your-intl-officials-app

# Optional GCS (if you use cloud storage features):
# heroku config:set GCS_SERVICE_ACCOUNT_JSON='...' -a your-intl-officials-app

git push intl-officials main
```

Apply requirement-rule migrations on the shared database before relying on promote/maintain checks:

```bash
psql "$DATABASE_URL" -f activityAnalysis/migrations/023_competition_alternatives_intl_all.sql
psql "$DATABASE_URL" -f activityAnalysis/migrations/024_synch_do_requirement_fixes.sql
psql "$DATABASE_URL" -f activityAnalysis/migrations/025_synch_isu_tc_ts_maintain.sql
```

## Activity tracker (separate Heroku app)

```bash
heroku config:set STREAMLIT_APP=activityAnalysis/activity_tracker_app.py -a your-activity-tracker-app
```

[`activityAnalysis/Procfile`](Procfile) mirrors the root Procfile with a default of the activity tracker entrypoint. Use it only if your deploy pipeline copies it to the repo root; otherwise set `STREAMLIT_APP` on the Heroku app.

## Local run

```bash
source venv/bin/activate
streamlit run activityAnalysis/international_officials_app.py
```

Configure `.streamlit/secrets.toml` with `DATABASE_URL` or `connections.postgresql` like the judging analysis app.
