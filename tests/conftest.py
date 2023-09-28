import os
import pytest
import requests_mock
from dags.config import pg_conn_id, db_name, bucket_name, s3_conn_id, schema_name

project_dir = os.path.join(os.path.dirname(__file__), '..')
airflow_home_dir = os.path.join(project_dir, 'airflow')

# set environment variables to init airflow properly
os.environ.setdefault('AIRFLOW_HOME', airflow_home_dir)
os.environ['AIRFLOW__CORE__DAGS_FOLDER'] = os.path.join(project_dir, 'dags')
os.environ.setdefault('AIRFLOW_CONN_POSTGRES_DB', 'postgresql://admin:1234@localhost/postgres')
os.environ.setdefault('AIRFLOW_CONN_MINIO', 'aws://minioadmin:minioadmin@/?endpoint_url=http%3A%2F%2Flocalhost%3A9000')

# init airflow
# It is important to import anything from Airflow AFTER the environment
# variables are assigned and airflow db migrate is finished!
if not os.path.exists(airflow_home_dir):
    os.system('airflow db migrate')


def init_db(hook):
    is_db_exists = len(hook.get_records(f"SELECT 1 FROM pg_database WHERE datname = '{db_name}'")) != 0
    if not is_db_exists:
        hook.run(f"CREATE DATABASE {db_name}", autocommit=True)
    hook.schema = db_name
    hook.run(f"CREATE SCHEMA IF NOT EXISTS {schema_name}", autocommit=True)


def init_minio(hook):
    try:
        hook.create_bucket(bucket_name=bucket_name)
    except Exception as e:
        if len(e.args) == 1 and type(e.args[0]) == str and 'BucketAlreadyOwnedByYou' in e.args[0]:
            return
        raise


@pytest.fixture(scope="session", autouse=True)
def db_hook():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook(postgres_conn_id=pg_conn_id)
    init_db(hook=hook)
    return hook


@pytest.fixture(scope="session", autouse=True)
def s3_hook():
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    hook = S3Hook(aws_conn_id=s3_conn_id)
    init_minio(hook=hook)
    return hook


@pytest.fixture
def request_mocker():
    with requests_mock.Mocker() as mocker:
        yield mocker
