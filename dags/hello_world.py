from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

default_args = {
    'owner': 'admin'
}

def print_hello_world():
    print('Hello, World!!')

with DAG(
    dag_id = 'hello_world',
    description = 'First "Hello, World" DAG!!',
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:
    start = EmptyOperator(
        task_id = 'start'
    )
    print_hello_world = PythonOperator(
        task_id = 'print_hello_world',
        python_callable = print_hello_world
    )
    end = EmptyOperator(
        task_id = 'end'
    )

start >> print_hello_world >> end