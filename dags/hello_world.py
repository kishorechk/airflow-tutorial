from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow'
}

def hello_world():
    print('Hello, Airflow!!')

with DAG(
    dag_id = 'my_first_dag',
    description = 'First "Hello, Airflow" DAG!!',
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:
    start = EmptyOperator(
        task_id = 'start'
    )
    hello_task = PythonOperator(
        task_id = 'print_hello_world',
        python_callable = hello_world
    )
    end = EmptyOperator(
        task_id = 'end'
    )

start >> hello_task >> end