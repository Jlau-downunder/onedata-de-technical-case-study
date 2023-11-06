import os
import snowflake.connector
from airflow.models import Variable

username = Variable.get("snowflake_user")
password = Variable.get("snowflake_pw")
account = Variable.get("snowflake_account")
warehouse = 'COMPUTE_WH'
database = 'ONEDATA_BOM_CASE_STUDY'
schema = 'SOURCE'
target_file_stage = 'BOM_FILE_STAGE'
target_table_stage = 'BOM_TABLE_STAGE'
source_data_store = 'BOM_SOURCE_DATA_STORE'

# Directory where CSV files are located
object_store = '/opt/airflow/data/mock_output_object_storage'

conn = snowflake.connector.connect(
    account=account,
    warehouse=warehouse,
    database=database,
    schema=schema,
    user=username,
    password=password
)


cursor = conn.cursor()

try:
    with open('/opt/airflow/utils/callable_snowql/task_2/create_replace_bom_table_stage.snowql', 'r') as sql_file:
        create_replace_bom_table_stage = sql_file.read()
        cursor.execute(create_replace_bom_table_stage)
    print(f"data staging table: '{target_table_stage}' set successfully.")
except Exception as e:
    print(f"Error: {str(e)}")


try:
    create_replace_file_stage = f"CREATE OR REPLACE STAGE {target_file_stage};"
    cursor.execute(create_replace_file_stage)
    print(f"file stage: '{target_file_stage}' set successfully.")
except Exception as e:
    print(f"Error: {str(e)}")


csv_files = [f for f in os.listdir(object_store) if f.endswith('.csv')]
# matching_file looks for the final appended .csv file created from ariflow task_1
matching_file = 'total_rows_weather_stations'
try:
    for csv_file in csv_files:
        if matching_file in csv_file:
            csv_file_path = os.path.join(object_store, csv_file)
            copy_into_file_stage_sql = f"""
                PUT file://{csv_file_path} @{target_file_stage}/
                """
            cursor.execute(copy_into_file_stage_sql)
            print(f"Loaded {csv_file} into Snowflake file stage: {target_file_stage}")

            load_files_into_table_stage = f"""
                COPY INTO {schema}.{target_table_stage}
                FROM '@{target_file_stage}/{csv_file}'
                FILE_FORMAT = (
                TYPE = 'CSV' 
                SKIP_HEADER = 1 
                TRIM_SPACE = FALSE
                SKIP_BYTE_ORDER_MARK = TRUE 
                ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE 
                VALIDATE_UTF8 = TRUE 
                DATE_FORMAT = 'YYYY-MM-DD'
                TIME_FORMAT = 'HH24:MI:SS' 
                TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF')
                """
            cursor.execute(load_files_into_table_stage)
            print(f"Loaded {csv_file} into Snowflake table stage: {target_table_stage}")

    print("< Load staged files into staging table operation complete> ")
except Exception as e:
    print(f"Error: {str(e)}")

cursor.close()
conn.close()
