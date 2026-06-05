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
# Heroku/reverse-proxy: without this, st.file_uploader often fails with Axios 400/403.
enableXsrfProtection = false
maxUploadSize = 200

[client]
showSidebarNavigation = false

STREAMLITCFG

# Write secrets.toml with valid PEM newlines (Heroku env vars often use literal \n).
python3 scripts/write_streamlit_secrets.py || exit 1
if [ ! -f ~/.streamlit/secrets.toml ]; then
  echo "ERROR: ~/.streamlit/secrets.toml was not created" >&2
  exit 1
fi
echo "Wrote ~/.streamlit/secrets.toml"