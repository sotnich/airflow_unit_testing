from datetime import datetime
from airflow.decorators import dag, task
from airflow.datasets import Dataset
from dags.config import s3_conn_id, bucket_name

url = "https://random-data-api.com/api/v2/users?response_type=json&size=50"
key = "users-{data_interval_start}.csv"


@dag(
    schedule="@hourly",
    start_date=datetime(2023, 1, 1),
)
def ingest_dag():
    @task
    def download():
        import requests
        import json
        import pandas as pd
        response = requests.get(url)
        response.raise_for_status()

        data = json.loads(response.content)

        df = pd.DataFrame(data=data)
        return df

    @task(outlets=Dataset('minio://users'))
    def save_to_minio(df, **kwargs):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        csv_str = df.to_csv(index=False)

        hook = S3Hook(aws_conn_id=s3_conn_id)
        key_name = key.format(data_interval_start=kwargs['data_interval_start'])
        hook.load_string(string_data=csv_str, key=key_name, bucket_name=bucket_name, replace=True)

    users_df = download()
    save_to_minio(users_df)


dag = ingest_dag()
