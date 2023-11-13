from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from sqlalchemy import create_engine

import pandas as pd
import seaborn as sns
from matplotlib import pyplot as plt

from datetime import datetime, timedelta

# Tutorial DB which is seperate instance from airflow
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@postgres_tutorial/tutorial"
# Create SQLAlchemy engine
engine = create_engine(DATABASE_URL)

default_args = {"owner": "airflow"}


def read_xls_file_all_employees(**kwargs):
    # Specify the path to the Excel file and the sheet name to read
    excel_file_path = "/data/files/PROV - Occupation SOC20 (4) Table 14.7a   Annual pay - Gross 2023.xls"
    sheet_name = "All"
    column_mapping = {
        "Description": "description",
        "Code": "code",
        "(thousand)": "number_of_jobs_1000s",
        "Median": "median",
        "change": "annual_percentage_change_median",
        "Mean": "mean",
        "change.1": "annual_percentage_change_mean",
    }

    # read the specified sheet into a DataFrame
    df = pd.read_excel(
        excel_file_path, sheet_name=sheet_name, skiprows=4, usecols=column_mapping
    )
    # rename columns
    df = df.rename(columns=column_mapping)
    # remove NaN rows
    df = df.dropna()
    # remove rows that contain 'x' in any of the specified columns
    df = df[df["number_of_jobs_1000s"] != "x"]
    print(df.head)  # Display the first few rows of the DataFrame
    return df


def save_employees(**kwargs):
    ti = kwargs["ti"]
    all_df = ti.xcom_pull(task_ids="transform.read_xls_all_employees")

    # Store the concatenated DataFrame in the database
    all_df.to_sql("all_employees", con=engine, if_exists="replace", index=False)

    print("Employees saved in database")
    return all_df


def generate_salary_bar_chart(**kwargs):
    ti = kwargs["ti"]
    all_df = ti.xcom_pull(task_ids="load.save_employees")

    # Check if DataFrame is not empty
    if all_df.empty:
        print("DataFrame is empty. Skipping bar chart generation.")
        return

    # Sort the DataFrame by the number_of_jobs_1000s column in descending order
    all_df_sorted = all_df.sort_values(by="number_of_jobs_1000s", ascending=False)

    # Select the top 10 rows
    top_10_all_df = all_df_sorted.head(20)

    # Set up the matplotlib figure
    plt.figure(figsize=(12, 6))

    # Create a bar chart for number of jobs using Seaborn
    ax = sns.barplot(
        x="description",
        y="number_of_jobs_1000s",
        data=top_10_all_df,
        color="green",
        label="Number of Jobs",
    )

    # Create a second y-axis for mean salary
    ax2 = ax.twinx()
    ax2.plot(
        top_10_all_df["description"],
        top_10_all_df["mean"],
        color="blue",
        marker="o",
        label="Mean Salary",
    )

    # Set labels and title
    ax.set_xlabel("Job Type")
    ax.set_ylabel("Number of Jobs (in thousands)")
    ax2.set_ylabel("Mean Salary")

    # Show legends
    ax.legend(loc="upper left")
    ax2.legend(loc="upper right")

    # Rotate x-axis labels for better readability
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")

    # Set title
    plt.title("Top 20 Job Types: Number of Jobs and Mean Salary by Job Type")

    # Save the plot to the "/data/plots" directory
    plt.tight_layout()
    plt.savefig("/data/plots/top_20_jobs_comparison.png")


def generate_annual_percentage_change_line_chart(**kwargs):
    ti = kwargs["ti"]
    all_df = ti.xcom_pull(task_ids="load.save_employees")

    # Check if DataFrame is not empty
    if all_df.empty:
        print("DataFrame is empty. Skipping line chart generation.")
        return

    # Sort the DataFrame by the annual_percentage_change_mean column in descending order
    all_df_sorted = all_df.sort_values(
        by="annual_percentage_change_mean", ascending=False
    )

    # Select the top 20 rows
    top_20_all_df = all_df_sorted.head(20)

    # Set up the matplotlib figure
    plt.figure(figsize=(12, 6))

    # Create a line chart for annual percentage change using Seaborn
    ax = sns.lineplot(
        x="description",
        y="annual_percentage_change_mean",
        data=top_20_all_df,
        marker="o",
        color="blue",
        label="Annual Percentage Change",
    )

    # Rotate x-axis labels for the first axis
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")

    # Create a second y-axis for mean salary
    ax2 = ax.twinx()
    ax2.plot(
        top_20_all_df["description"],
        top_20_all_df["mean"],
        color="red",
        marker="s",
        label="Mean Salary",
    )

    # Rotate x-axis labels for the second axis
    ax2.set_xticklabels(ax2.get_xticklabels(), rotation=45, ha="right")

    # Set labels and title
    ax.set_xlabel("Job Type")
    ax.set_ylabel("Annual Percentage Change")
    ax2.set_ylabel("Mean Salary")

    # Show legends
    ax.legend(loc="upper left")
    ax2.legend(loc="upper right")

    # Set title
    plt.title("Top 20 Job Types: Annual Percentage Change and Mean Salary by Job Type")

    # Save the plot to the "/data/plots" directory
    plt.tight_layout()
    plt.savefig("/data/plots/top_20_annual_percentage_change.png")


with DAG(
    dag_id="uk-earnings-hours-all",
    description="UK Earnings and hours worked, occupation by four-digit SOC",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@once",
) as dag:
    with TaskGroup("extract") as task_group:
        # BashOperator to clean up the 'data' directory
        cleanup_data_dir_task = BashOperator(
            task_id="cleanup_data_dir",
            bash_command="rm -rf /data/files/*",
            dag=dag,
        )

        # BashOperator to download the zip file using wget
        download_task = BashOperator(
            task_id="download_file",
            bash_command='wget -O "/data/files/file.zip" "https://www.ons.gov.uk/file?uri=/employmentandlabourmarket/peopleinwork/earningsandworkinghours/datasets/occupation4digitsoc2010ashetable14/2023provisional/ashetable142023provisional.zip"',
        )

        # BashOperator to unzip the file
        unzip_task = BashOperator(
            task_id="unzip_file",
            bash_command='unzip -o "/data/files/file.zip" -d /data/files/',
        )

    with TaskGroup("transform") as task_group:
        # PythonOperator to read the Excel file All employees
        read_xls_all_employees_task = PythonOperator(
            task_id="read_xls_all_employees",
            python_callable=read_xls_file_all_employees,
            provide_context=True,
        )

    with TaskGroup("load") as taskgroup:
        # PythonOperator to save employees
        save_employees_task = PythonOperator(
            task_id="save_employees",
            python_callable=save_employees,
            provide_context=True,
        )

    with TaskGroup("analyse") as task_group:
        # PythonOperator to geberate bar chart
        generate_salary_bar_chart_task = PythonOperator(
            task_id="generate_salary_bar_chart_task",
            python_callable=generate_salary_bar_chart,
            provide_context=True,
        )

        # PythonOperator to generate line chart for annual percentage change
        generate_annual_percentage_change_line_chart_task = PythonOperator(
            task_id="generate_annual_percentage_change_line_chart_task",
            python_callable=generate_annual_percentage_change_line_chart,
            provide_context=True,
        )

(
    cleanup_data_dir_task
    >> download_task
    >> unzip_task
    >> read_xls_all_employees_task
    >> save_employees_task
    >> (
        generate_salary_bar_chart_task,
        generate_annual_percentage_change_line_chart_task,
    )
)
