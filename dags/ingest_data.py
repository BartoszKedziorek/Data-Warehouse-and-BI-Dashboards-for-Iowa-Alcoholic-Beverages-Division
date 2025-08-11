from airflow.decorators import dag, task 
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from google.cloud import bigquery


@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def ingest_data():

    # @task.pyspark(conn_id='spark_cluster')
    # def download_dataset():
    #     # client = bigquery.Client()

    #     # query = """
    #     #     SELECT * FROM
    #     #     `iowa-sales-analytic-platform.temp_dataset.temp_sales_table`
    #     # """

    #     # query_job = client.query(query)

    #     # result = query_job.result()
    #     print("IT JUST WORKS")


    download_dataset = SparkSubmitOperator(
        task_id='submit_download_dataset',
        application='/usr/local/airflow/include/scripts/read.py',
        conn_id='spark_cluster',
        deploy_mode='cluster',
        verbose=True,
        files='/opt/hadoop/etc/hadoop/yarn-site.xml,/opt/hadoop/etc/hadoop/core-site.xml'
    )

    download_dataset


ingest_data()
