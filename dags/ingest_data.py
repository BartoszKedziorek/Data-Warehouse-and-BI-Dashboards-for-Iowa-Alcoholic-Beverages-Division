from airflow.decorators import dag, task 
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, DateType, DecimalType
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from google.cloud import bigquery
import numpy as np
from decimal import Decimal
import os

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
)
def ingest_data():

    google_auth_credentials_env = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    download_dataset = SparkSubmitOperator(
        task_id='submit_download_dataset',
        application='/usr/local/airflow/include/scripts/download_dataset.py',
        conn_id='spark_cluster',
        env_vars={"GOOGLE_APPLICATION_CREDENTIALS": google_auth_credentials_env},
        deploy_mode='cluster',
        verbose=True,
        files='/opt/hadoop/etc/hadoop/yarn-site.xml,/opt/hadoop/etc/hadoop/core-site.xml,/usr/local/airflow/include/secrets/google-api-key.json#gcp-key.json'
    )

    download_dataset


ingest_data()
