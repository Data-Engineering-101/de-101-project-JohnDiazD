from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import json
import os
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
  'owner': 'airflow',
  'depends_on_past': False,
  'start_date': datetime(2022, 3, 1),
  'retries': 0,
  'retry_delay': timedelta(minutes=5)
}


def printar():
    print("success!")

 
with DAG(
    'etl',
    default_args=default_args,
    schedule_interval=None,
    concurrency=1,
) as dag:

    downloading_rates = PythonOperator(task_id="test1", python_callable=printar)

    spark_transformation = SparkSubmitOperator(
        task_id="spark_transform",
        application="/opt/bitnami/spark/workspace/nike_sparksql.py",
        conn_id="spark_conn",
        verbose=False
    )

    spark_sw_load = SparkSubmitOperator(
        task_id="spark_load",
        application="/opt/bitnami/spark/workspace/nike_snowflake_load.py",
        conn_id="spark_conn",
        verbose=False
    )

    downloading_rates  >> spark_transformation >> spark_sw_load