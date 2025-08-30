from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

import logging

def print_hello():
    logging.info("A sample DAG has executed")

with DAG(
        "sample_dag",
        description = "A sample DAG",
        schedule_interval = "0 0 * * *",
        start_date = datetime(2025, 8, 29),
        catchup = False
    ) as dag:

    task1 = PythonOperator(
        task_id = "print_hello_task",
        python_callable = print_hello,
        dag = dag
    )

    task2 = EmptyOperator(
        task_id = "empty_task",
        dag = dag
    )

task1 >> task2