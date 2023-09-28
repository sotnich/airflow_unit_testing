from airflow.utils.state import DagRunState
from dags.ingest_dag import dag, key, url
from dags.config import bucket_name


def test_simple(s3_hook, request_mocker):
    s3_hook.delete_objects(bucket=bucket_name, keys=s3_hook.list_keys(bucket_name=bucket_name))

    with open('./data/users.json') as f:
        request_mocker.get(url, text=f.read())

    dag_run = dag.test()
    assert dag_run.state == DagRunState.SUCCESS

    users_key = key.format(data_interval_start=dag_run.data_interval_start)
    assert users_key in s3_hook.list_keys(bucket_name=bucket_name)

    with open('./data/users.csv') as f:
        assert f.read() == s3_hook.read_key(bucket_name=bucket_name, key=users_key)
