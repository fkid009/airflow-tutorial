from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 8, 29)
}

conn_id = "my_postgres_connection"


with DAG(
        "postgres_loader",
        description = "PostgreSQL Loader Example",
        default_args = default_args,
        schedule_interval = "0 0 * * *",
        catchup = False
    ) as dag:

    sql_query = """
        INSERT INTO sample_table (key, value)
        VALUES ('hello', 'world')
    """

    postgres_task = SQLExecuteQueryOperator(
        task_id = "execute_sql_query",
        conn_id= conn_id,
        sql = sql_query,
        dag = dag
    )

    sql_sensor_query = """
        SELECT COUNT(*) FROM sample_table WHERE key = 'hello'
    """

    sql_sensor = SqlSensor(
        task_id = "wait_for_condition",
        conn_id = conn_id,
        sql = sql_sensor_query,
        mode = "poke",
        poke_interval = 30,
        dag = dag
    )

    sql_query_confirm = """
        INSERT INTO sample_table (key, value)
        VALUES ('sensor', 'confirmed')
    """

    postgres_confirm_task = SQLExecuteQueryOperator(
        task_id = "execute_sql_confirm_query",
        conn_id= conn_id,
        sql = sql_query_confirm,
        dag = dag
    )

postgres_task >> sql_sensor >> postgres_confirm_task