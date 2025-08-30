import json
from pandas import json_normalize
from datetime import datetime

from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def _extract_data(**context):
    response = context["ti"].xcom_pull(task_ids = "get_op")
    starwars_character = json_normalize(
        {
            "name": response["name"],
            "height": response["height"],
            "mass": response["mass"]
        }
    )
    starwars_character.to_csv("/tmp/starwars_character.csv", header = False, index = None)

def _store_character(**context):
    hook = PostgresHook(
        postgres_conn_id = "my_postgres_connection",
    )

    hook.copy_expert(
        filename = "/tmp/starwars_character.csv",
        sql = "COPY starwars_character FROM stdin WITH DELIMITER as ','"
    )

with DAG(
        "http_dag",
        description = "http dag",
        schedule_interval = "0 0 * * *",
        start_date = datetime(2025, 8, 30),
        catchup = False
    ) as dag:

    sql_create_query = """
        CREATE TABLE IF NOT EXISTS starwars_character(
            name TEXT NOT NULL,
            height TEXT NOT NULL,
            mass TEXT NOT NULL
        );    
    """

    task_create_table_op = SQLExecuteQueryOperator(
        task_id = "create_table_op",
        conn_id = "my_postgres_connection",
        sql = sql_create_query
    )

    task_get_op = HttpOperator(
        task_id = "get_op",
        http_conn_id = "my_http_connection",
        endpoint = "/api/people/1/",
        headers = {"Content-Type": "application/json"},
        method = "GET",
        response_filter = lambda response: json.loads(response.text),
        log_response = True,
        extra_options={"verify": False, "timeout": 30},  # SSL 검증 끄기
    )

    task_extract_data_op = PythonOperator(
        task_id = "extract_data_op",
        python_callable = _extract_data,
        provide_context = True
    )

    task_store_op = PythonOperator(
        task_id = "store_op",
        python_callable = _store_character
    )

task_create_table_op >> task_get_op >> task_extract_data_op >> task_store_op