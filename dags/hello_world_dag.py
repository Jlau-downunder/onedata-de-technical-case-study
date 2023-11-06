from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def helloWorld():
    print("Hello World Foo Bar1")


with DAG(dag_id="Hello_world_dag",
         start_date=datetime(2022, 1, 1),
         schedule_interval="@hourly",
         catchup = False) as dag:

    task1 = PythonOperator(
        task_id="hello_world_task", 
        python_callable=helloWorld)

    task1