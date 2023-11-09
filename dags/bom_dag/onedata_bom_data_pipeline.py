from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import os

#os.chdir('opt/airflow')

dag_dir = '/opt/airflow/dags/bom_dag/py_callables/'

# Define your DAG
with DAG(
    dag_id="onedata_bom_data_pipeline",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    default_args={
        'owner': 'airflow',
    }
) as dag:

    task_0 = DummyOperator(
        task_id='Pipeline_start',
        dag=dag,
    )


    task1_script_path = f"{dag_dir}task_1.py"
    task_1 = BashOperator(
        task_id='bom_csv_data_prep',
        bash_command=f'python {task1_script_path}',
        dag=dag,
    )


    task2_script_path = f"{dag_dir}task_2.py"
    task_2 = BashOperator(
        task_id='load_data_into_file_stage',
        bash_command=f'python {task2_script_path}',
        dag=dag,
    )


    task3_script_path = f"{dag_dir}task_3.py"
    task_3 = BashOperator(
        task_id='load_stage_files_into_clean_layer',
        bash_command=f'python {task3_script_path}',
        dag=dag,
    )

    task4_script_path = f"{dag_dir}task_4.py"
    task_4 = BashOperator(
        task_id='model_and_partition_data',
        bash_command=f'python {task4_script_path}',
        dag=dag,
    )

    task5_script_path = f"{dag_dir}task_5.py"
    task_5 = BashOperator(
        task_id='table_reconciliation_checks',
        bash_command=f'python {task5_script_path}',
        dag=dag,
    )

    task_6 = DummyOperator(
        task_id='Pipeline_end',
        dag=dag,
    )



    task_0 >> task_1 >> task_2 >> task_3 >> task_4 >> task_5 >> task_6

    if __name__ == "__main__":
        dag.cli()
