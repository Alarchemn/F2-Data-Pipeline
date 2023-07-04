"""
*************************************************************
Author = Martin Alarcon -- https://github.com/Alarchemn     *
Date = '20/08/2023'                                         *
Description = Extracting Data from F2 races                 *
*************************************************************
"""

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime
from io import StringIO
from dags.utils.utils import extract_data, transform_data, all_race_ids

import time

AWS_CONN_ID = "AWSConnection"
BUCKET_NAME = Variable.get('bucket_name')
SLEEP_SEC = 10

def etl_data(ids,bucket_name):
    # Instantiate S3Hook and retrieve the S3 client
    hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    client = hook.get_conn()

    for id in ids:
        # Extract and transform data for the given id
        df = extract_data(id)
        transform_data(df)
        
        # Create a CSV buffer and write the transformed data into it
        csv_buffer = StringIO()
        df.to_csv(csv_buffer,index=False)
        
        # Upload the transformed data to S3 with a unique key based on the race id
        client.put_object(
            Body=csv_buffer.getvalue(),
            Bucket=bucket_name,
            Key=f'race_id_{id}.csv')
        
        # Sleep for a specified duration between iterations (For multiple ids this is 'responsable')
        time.sleep(SLEEP_SEC)

default_args = {
    'owner': 'ALAR-F2',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'schedule_interval': None,

}

dag =DAG(
    dag_id='F2-DAG',
    default_args=default_args,
    description='Extract, Transform and load Formula 2 Data',
    tags=['F2-data'],
    catchup=False
)

with dag:
    f2_etl = PythonOperator(
        task_id='ETL-F2',
        python_callable=etl_data,
        op_kwargs={
            'ids': ['1057'],
            'bucket_name': BUCKET_NAME
        }
    )


