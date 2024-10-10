# dbt + Datafold demo project
Adapted for Keboola environment

# dbt + Datafold demo project
Adapted for Keboola environment

host: https://connection.keboola.com/
target:
-( Install 'code' command in PATH)
source venv/bin/activate

code ~/.zshrc
step 1
clone repository
step 2
-- check conection env
pip install dbt-core
pip install dbt-snowflake
step 3∏
kbc dbt init
host: https://connection.keboola.com/
token:xxx
step 4
save workspaces credentials in linux file
code ~/.zshrc
step 5 test snowflake conection
dbt debug -t dev --profiles-dir .
test
step 6 
change workspaces names
step 7 
carga datos estaticos de csv a workspace 
dbt seed

dbt run -t dev  --profiles-dir .