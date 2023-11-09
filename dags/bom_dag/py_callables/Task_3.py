import snowflake.connector
from airflow.models import Variable

username = Variable.get("snowflake_user")
password = Variable.get("snowflake_pw")
account = Variable.get("snowflake_account")
warehouse = 'COMPUTE_WH'
database = 'ONEDATA_BOM_CASE_STUDY'
schema = 'SOURCE'


conn = snowflake.connector.connect(
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    user=username,
    password=password
)
cursor = conn.cursor()

callable_snowql_path = '/opt/airflow/utils/callable_snowql/task_3/'

try:
    with open(f"{callable_snowql_path}create_replace_bom_cleaned_table.snowql", 'r') as sql_file:
        create_replace_bom_cleaned_table = sql_file.read()
        cursor.execute(create_replace_bom_cleaned_table)
    print("SQL: <CREATE OR REPLACE TABLE SOURCE.BOM_CLEANED> statement executed successfully")
except Exception as e:
    print(f"Error: {str(e)}")


try:
    with open(f"{callable_snowql_path}transform_upsert_into_bom_cleaned.snowql", 'r') as sql_file:
        transform_upsert_into_bom_cleaned = sql_file.read()
        cursor.execute(transform_upsert_into_bom_cleaned)
    print("SQL: <INSERT INTO (transformations) SOURCE.BOM_CLEANED> statement executed successfully")
except Exception as e:
    print(f"Error: {str(e)}")

cursor.close()
conn.close()