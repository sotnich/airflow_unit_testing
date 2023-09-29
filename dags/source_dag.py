from datetime import datetime
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from dags.config import s3_conn_id, bucket_name, db_name, pg_conn_id, schema_name

source_table = 'source'
key = "users-{data_interval_start}.csv"


@dag(
    schedule=[Dataset('minio://users')],
    start_date=datetime(2023, 1, 1),
)
def source_dag():
    @task
    def read_from_minio(**kwargs):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import pandas as pd
        import io

        hook = S3Hook(aws_conn_id=s3_conn_id)
        key_name = key.format(data_interval_start=kwargs['data_interval_start'])
        csv_str = hook.read_key(key=key_name, bucket_name=bucket_name)

        df = pd.read_csv(io.StringIO(csv_str))
        return df

    @task
    def save_to_postgres(df, **kwargs):
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import json
        hook = PostgresHook(postgres_conn_id=pg_conn_id)
        hook.schema = db_name

        # to_sql cannot save nested json
        for col in df.columns:
            if type(df[col][0]) == dict:
                df[col] = df[col].apply(json.dumps)

        df['data_interval_start'] = kwargs['data_interval_start']

        df.to_sql(name=source_table, schema=schema_name, con=hook.get_sqlalchemy_engine(), if_exists="append",
                  index=False)

    users_df = read_from_minio()
    save_to_postgres(users_df)


dag = source_dag()
