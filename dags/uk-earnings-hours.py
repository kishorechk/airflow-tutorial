from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd

from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow'
}

def read_xls_file_all_employees():
    # Specify the path to the Excel file and the sheet name to read
    excel_file_path = "/data/PROV - Occupation SOC20 (4) Table 14.7a   Annual pay - Gross 2023.xls"
    sheet_name = "All"
    column_mapping = {
        'Description': 'description',
        'Code': 'code',
        '(thousand)': 'number_of_jobs_1000s',
        'Median': 'median',
        'change': 'annual_percentage_change_median',
        'Mean': 'mean',
        'change.1': 'annual_percentage_change_mane',
        '10': 'percentile10',
        '20': 'percentile20',
        '25': 'percentile25',
        '30': 'percentile30',
        '40': 'percentile40',
        '60': 'percentile50',
        '70': 'percentile60',
        '75': 'percentile70',
        '80': 'percentile80',
        '90': 'percentile90',
        'Unnamed: 17': 'info1',
        'Unnamed: 18': 'info2',
        'Unnamed: 19': 'info3'
    }
    
    # read the specified sheet into a DataFrame
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name, skiprows=4, usecols=column_mapping)
    # rename columns
    df = df.rename(columns=column_mapping)
    print(df.head) # Display the first few rows of the DataFrame

def cleanup_data(task_id, **kwargs):
    # Get the data from the XCom of the 'read_xls_all_employees' task
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids = task_id)
    if df is not None:
        # remove unncessary columns
        df = df.drop(columns=['info1', 'info2', 'info3'])
        # remove NaN rows
        df = df.dropna()
        # remove rows with 'x'
        df = df.loc[df['number_of_jobs_1000s'] != 'x']
        print(df.head) # Display the first few rows of the DataFrame
    else:
        print(f"No data found in XCom from task: {task_id}")

with DAG(
    dag_id = 'uk-earnings-hours',
    description = 'UK Earnings and hours worked, occupation by four-digit SOC',
    default_args = default_args,
    start_date = days_ago(1),
    schedule_interval = '@once'
) as dag:
    
    with TaskGroup('read_cleanup_data') as task_group:

        # BashOperator to clean up the 'data' directory
        cleanup_task = BashOperator(
            task_id="cleanup_data_dir",
            bash_command='rm -rf /data/*',
            dag=dag,
        )

        # BashOperator to download the zip file using wget
        download_task = BashOperator(
            task_id = 'download_file',
            bash_command = 'wget -O "/data/file.zip" "https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/occupation4digitsoc2010ashetable14/2023provisional/ashetable142023provisional.zip"'
        )

        # BashOperator to unzip the file
        unzip_task = BashOperator(
            task_id = 'unzip_file',
            bash_command = 'unzip -o "/data/file.zip" -d /data'
        )

        # PythonOperator to read the Excel file
        read_xls_all_employees = PythonOperator(
            task_id = 'read_xls_all_employees',
            python_callable = read_xls_file_all_employees
        )

        # PythonOperator to clean up data
        clean_all_employees = PythonOperator(
            task_id = 'clean_all_employees',
            python_callable = cleanup_data,
            op_args=['read_xls_all_employees']
        )

cleanup_task >> download_task >> unzip_task >> read_xls_all_employees >> clean_all_employees

