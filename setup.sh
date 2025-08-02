echo $DB_HOST
printenv
mkdir -p ~/.streamlit/
echo "
[server]
headless = true
port = $PORT
enableCORS = false

" > ~/.streamlit/config.toml

echo "

[connections.postgresql]
dialect = \"postgresql\"
host = \"$DB_HOST\"
port = \"$DB_PORT\"
database = \"$DB_NAME\"
username = \"$DB_USERNAME\"
password = \"$DB_PASSWORD\"

[connections.gcs]
$GCS_CONNECTION

" > ~/.streamlit/secrets.toml

cat ~/.streamlit/secrets.toml