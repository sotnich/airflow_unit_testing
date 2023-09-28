from airflow.utils.state import DagRunState
from airflow.utils.timezone import DateTime
from dags.source_dag import dag, source_table, key
from dags.config import bucket_name, schema_name


def test_simple(db_hook, s3_hook):

    execution_date = DateTime.utcnow()

    db_hook.run(f'DROP TABLE IF EXISTS {schema_name}.{source_table}')

    s3_hook.delete_objects(bucket=bucket_name, keys=s3_hook.list_keys(bucket_name=bucket_name))
    users_key = key.format(data_interval_start=execution_date)
    with open('./data/users.csv') as f:
        s3_hook.load_string(bucket_name=bucket_name, key=users_key, string_data=f.read())

    dag_run = dag.test(execution_date)
    assert dag_run.state == DagRunState.SUCCESS

    assert db_hook.get_records(f'SELECT COUNT(*) FROM {schema_name}.{source_table}')[0][0] == 2
