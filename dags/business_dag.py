from datetime import datetime
from airflow.decorators import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from dags.config import db_name, pg_conn_id, schema_name

source_table = 'source'
target_table = 'target'


@dag(
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
)
def simple_dag_sql():
    SQLExecuteQueryOperator(
        task_id="query",
        sql=f"DROP TABLE IF EXISTS {schema_name}.{target_table};"
            f"CREATE TABLE {schema_name}.{target_table} AS "
            f"SELECT first_name, count(*) AS CNT "
            f"FROM {schema_name}.{source_table} "
            f"WHERE last_name != 'Schmidt'"
            f"GROUP BY first_name",
        conn_id=pg_conn_id,
        database=db_name,
    )


dag = simple_dag_sql()
