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

echo "

DATABASE_URL=\"$PG_DB_URL\"

[connections.postgresql]
dialect = \"postgresql\"
host = \"$DB_HOST\"
port = \"$DB_PORT\"
database = \"$DB_NAME\"
username = \"$DB_USERNAME\"
password = \"$DB_PASSWORD\"

[connections.gcs]
$GCS_CONNECTION
private_key = \"$GCS_PRIVATE_KEY\"

" > ~/.streamlit/secrets.toml

cat ~/.streamlit/secrets.toml