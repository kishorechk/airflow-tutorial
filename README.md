# Apache Airflow Data Pipeline

## Overview

This repository contains the code for building a robust data pipeline using Apache Airflow. The pipeline focuses on extracting, transforming, loading (ETL), and analyzing data related to UK earnings and hours.

## Getting Started

Follow these steps to set up the Airflow environment and run the data pipeline:

### Prerequisites

- [Docker](https://www.docker.com/) installed on your machine.

### Setup

1. Clone this repository:

   ```bash
   git clone https://github.com/your-username/airflow-data-pipeline.git
   ```

2. Navigate to the project directory:

   ```bash
   cd airflow-data-pipeline
   ```

3. Build and start the Docker containers:

   ```bash
   docker-compose up --build
   ```

4. Access the Airflow web UI at [http://localhost:8080](http://localhost:8080) (default credentials: admin/admin).

## DAG Structure

The Directed Acyclic Graph (DAG) for the data pipeline is organized into four main stages:

### 1. Extract

- **Cleanup Data Directory:** BashOperator to clean up the 'data' directory.
- **Download File:** BashOperator to download the data file from the provided URL.
- **Unzip File:** BashOperator to unzip the downloaded file.

### 2. Transform

- **Read XLS All Employees:** PythonOperator to read the Excel file containing data for all employees.

### 3. Load

- **Save Employees:** PythonOperator to save the transformed data into the PostgreSQL database.

### 4. Analyze

- **Generate Salary Bar Chart:** PythonOperator to generate a bar chart for the top 20 job types, depicting the number of jobs and mean salary.
- **Generate Annual Percentage Change Line Chart:** PythonOperator to generate a line chart for the annual percentage change and mean salary of the top 20 job types.

## Additional Libraries

- [Pandas](https://pandas.pydata.org/): Used for data manipulation and cleaning.
- [Matplotlib](https://matplotlib.org/), [Seaborn](https://seaborn.pydata.org/): Used for data visualization.

## License

This project is licensed under the [MIT License](LICENSE).
