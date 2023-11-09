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

callable_snowql_path = '/opt/airflow/utils/callable_snowql/task_4/'

try:
    try:
        with open(f"{callable_snowql_path}insert_into_evapo_transpiration_year_partitioned.snowql", 'r') as sql_file:
            insert_into_evapo_transpiration_year_partitioned = sql_file.read().split(';')

            for sql_insert_statements in insert_into_evapo_transpiration_year_partitioned:
                sql_insert_statements = sql_insert_statements.strip()
                if sql_insert_statements:
                    cursor.execute(sql_insert_statements)
        print("SQL: <INSERT_INTO_EVAPO_TRANSPIRATION_YEAR_PARTITIONED> statement executed successfully")
    except Exception as e:
        print(f"Error: {str(e)}")


    try:
        with open(f"{callable_snowql_path}insert_into_humidity_year_partitioned.snowql", 'r') as sql_file:
            insert_into_humidity_year_partitioned = sql_file.read().split(';')

            for sql_insert_statements in insert_into_humidity_year_partitioned:
                sql_insert_statements = sql_insert_statements.strip()
                if sql_insert_statements:
                    cursor.execute(sql_insert_statements)
        print("SQL: <INSERT_INTO_HUMIDITY_YEAR_PARTITIONED> statement executed successfully")
    except Exception as e:
        print(f"Error: {str(e)}")


    try:
        with open(f"{callable_snowql_path}insert_into_pan_transpiration_year_partitioned.snowql", 'r') as sql_file:
            insert_into_pan_transpiration_year_partitioned = sql_file.read().split(';')

            for sql_insert_statements in insert_into_pan_transpiration_year_partitioned:
                sql_insert_statements = sql_insert_statements.strip()
                if sql_insert_statements:
                    cursor.execute(sql_insert_statements)
        print("SQL: <INSERT_INTO_PAN_EVAPORATION_YEAR_PARTITIONED> statement executed successfully")
    except Exception as e:
        print(f"Error: {str(e)}")


    try:
        with open(f"{callable_snowql_path}insert_into_rain_year_partitioned.snowql", 'r') as sql_file:
            insert_into_rain_year_partitioned = sql_file.read().split(';')

            for sql_insert_statements in insert_into_rain_year_partitioned:
                sql_insert_statements = sql_insert_statements.strip()
                if sql_insert_statements:
                    cursor.execute(sql_insert_statements)
        print("SQL: <INSERT_INTO_RAIN_YEAR_PARTITIONED> statement executed successfully")
    except Exception as e:
        print(f"Error: {str(e)}")


    try:
        with open(f"{callable_snowql_path}insert_into_solar_radiation_partitioned.snowql", 'r') as sql_file:
            insert_into_solar_radiation_partitioned = sql_file.read().split(';')

            for sql_insert_statements in insert_into_solar_radiation_partitioned:
                sql_insert_statements = sql_insert_statements.strip()
                if sql_insert_statements:
                    cursor.execute(sql_insert_statements)
        print("SQL: <INSERT_INTO_SOLAR_RADIATION_PARTITIONED> statement executed successfully")
    except Exception as e:
        print(f"Error: {str(e)}")


    try:
        with open(f"{callable_snowql_path}insert_into_temperature_partitioned.snowql", 'r') as sql_file:
            insert_into_temperature_partitioned = sql_file.read().split(';')

            for sql_insert_statements in insert_into_temperature_partitioned:
                sql_insert_statements = sql_insert_statements.strip()
                if sql_insert_statements:
                    cursor.execute(sql_insert_statements)
        print("SQL: <INSERT_INTO_TEMPERATURE_PARTITIONED> statement executed successfully")
    except Exception as e:
        print(f"Error: {str(e)}")


    try:
        with open(f"{callable_snowql_path}insert_into_wind_speed_partitioned.snowql", 'r') as sql_file:
            insert_into_wind_speed_partitioned = sql_file.read().split(';')

            for sql_insert_statements in insert_into_wind_speed_partitioned:
                sql_insert_statements = sql_insert_statements.strip()
                if sql_insert_statements:
                    cursor.execute(sql_insert_statements)
        print("SQL: <INSERT_INTO_WIND_SPEED_PARTITIONED> statement executed successfully")
    except Exception as e:
        print(f"Error: {str(e)}")

    print("< INSER OPERATIONS IN TO PARTITIONED TABLES SUCCESSFUL >")
except Exception as e:
    print(f"Error: {str(e)}")


cursor.close()
conn.close()