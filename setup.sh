echo $DB_HOST
printenv
mkdir -p ~/.streamlit/
# Merge runtime Heroku settings with UX defaults from repo [.streamlit/config.toml].
# (Writing only [server] here used to wipe showSidebarNavigation and surfaced multipage links.)
cat > ~/.streamlit/config.toml <<STREAMLITCFG
[server]
headless = true
port = ${PORT:-8501}
enableCORS = false

[client]
showSidebarNavigation = false

STREAMLITCFG

# Write secrets.toml with valid PEM newlines (Heroku env vars often use literal \n).
python3 scripts/write_streamlit_secrets.py

cat ~/.streamlit/secrets.toml