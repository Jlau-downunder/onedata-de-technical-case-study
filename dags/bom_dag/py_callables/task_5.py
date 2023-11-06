import os
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

schema_table_union_record_count_sql = '/opt/airflow/utils/callable_snowql/task_5/table_reconciliation_checks.snowql'
with open(schema_table_union_record_count_sql, 'r') as sql_file:
    sql_statements = sql_file.read().split(';')
    index = 0 
    result_list = []  # Create an empty list to store results

    for sql in sql_statements:
        sql = sql.strip()
        if sql:
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                result = cursor.fetchall()
                result_dict = {"index": index, "sql_result": result}
                result_list.append(result_dict)
                print(f"SQL block {index}: {sql}")
                index += 1
            except Exception as e:
                print(f"Error executing SQL: {str(e)}")

print(result_list)
rain_union_count = result_list[0]['sql_result'][0][0]
print(rain_union_count)
wind_speed_union_count = result_list[1]['sql_result'][0][0]
temperature_union_count = result_list[2]['sql_result'][0][0]
evapo_transpiration_union_count = result_list[3]['sql_result'][0][0]
humidity_union_count = result_list[4]['sql_result'][0][0]
pan_evaporation_union_count = result_list[5]['sql_result'][0][0]
solar_radiation_union_count = result_list[6]['sql_result'][0][0]
bom_table_stage_count = result_list[7]['sql_result'][0][0]
bom_cleaned_table_count = result_list[8]['sql_result'][0][0]


# Union table check reconciliation
if (rain_union_count == wind_speed_union_count == temperature_union_count == 
    evapo_transpiration_union_count == humidity_union_count == 
    pan_evaporation_union_count == solar_radiation_union_count):
    print(f" SUCCESSFUL RECONCILIATION < rain_union_count:{rain_union_count}, wind_speed_union_count:{wind_speed_union_count}, temperature_union_count:{temperature_union_count}, evapo_transpiration_union_count:{evapo_transpiration_union_count}, humidity_union_count:{humidity_union_count}, pan_evaporation_union_count:{pan_evaporation_union_count}, solar_radiation_union_count:{solar_radiation_union_count} >")
else:
    print(f" UNSUCCESSFUL RECONCILIATION < rain_union_count:{rain_union_count}, wind_speed_union_count:{wind_speed_union_count}, temperature_union_count:{temperature_union_count}, evapo_transpiration_union_count:{evapo_transpiration_union_count}, humidity_union_count:{humidity_union_count}, pan_evaporation_union_count:{pan_evaporation_union_count}, solar_radiation_union_count:{solar_radiation_union_count} > ")

# stage and cleaned table reconciliation
if (bom_table_stage_count == bom_cleaned_table_count):
    print(f" SUCCESSFUL RECONCILIATION < bom_table_stage_count:{bom_table_stage_count}, bom_cleaned_table_count:{bom_cleaned_table_count} >")
else:
    print(f" UNSUCCESSFUL RECONCILIATION < bom_table_stage_count:{bom_table_stage_count}, bom_cleaned_table_count:{bom_cleaned_table_count} > ")

# all table reconciliation
if (rain_union_count == wind_speed_union_count == temperature_union_count == 
    evapo_transpiration_union_count == humidity_union_count == 
    pan_evaporation_union_count == solar_radiation_union_count == bom_table_stage_count == bom_cleaned_table_count):
    print(f" SUCCESSFUL RECONCILIATION < rain_union_count:{rain_union_count}, wind_speed_union_count:{wind_speed_union_count}, temperature_union_count:{temperature_union_count}, evapo_transpiration_union_count:{evapo_transpiration_union_count}, humidity_union_count:{humidity_union_count}, pan_evaporation_union_count:{pan_evaporation_union_count}, solar_radiation_union_count:{solar_radiation_union_count}, bom_table_stage_count:{bom_table_stage_count}, bom_cleaned_table_count:{bom_cleaned_table_count}  >")
else:
    print(f" UNSUCCESSFUL RECONCILIATION < rain_union_count:{rain_union_count}, wind_speed_union_count:{wind_speed_union_count}, temperature_union_count:{temperature_union_count}, evapo_transpiration_union_count:{evapo_transpiration_union_count}, humidity_union_count:{humidity_union_count}, pan_evaporation_union_count:{pan_evaporation_union_count}, solar_radiation_union_count:{solar_radiation_union_count}, bom_table_stage_count:{bom_table_stage_count}, bom_cleaned_table_count:{bom_cleaned_table_count}  >")

cursor.close()
conn.close()
