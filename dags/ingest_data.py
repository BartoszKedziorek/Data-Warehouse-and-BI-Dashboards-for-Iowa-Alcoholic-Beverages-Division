from airflow.decorators import dag, task 
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, DateType, DecimalType
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from google.cloud import bigquery
from pyspark.sql.functions import max
import numpy as np
from decimal import Decimal
import os

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
)
def ingest_data():

    @task.pyspark(conn_id='spark_cluster')
    def check_any_data_in_hdfs(spark: SparkSession):
        path = "ingest/raw_sales"    
        jvm = spark._jvm
        jsc = spark._jsc

        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())

        if not fs.exists(jvm.org.apache.hadoop.fs.Path(path)):
            return False

        df = spark.read.parquet(path)

        return df.count() != 0
    

    @task.branch
    def branch_any_data_in_hdfs(val):
        if val == True:
            return 'check_new_data_in_bigquery'
        else:
            return 'submit_download_full_dataset'


    @task.pyspark(conn_id='spark_cluster')
    def check_new_data_in_bigquery(spark: SparkSession):
        client = bigquery.Client()

        bg_query = """
        SELECT MAX(date) as max_date FROM iowa-sales-analytic-platform.temp_dataset.temp_sales_table
        """
        
        max_date_bq = client.query(bg_query).result().to_dataframe()['max_date'].iloc[0]


        spark_df = spark.read.parquet('ingest/raw_sales')
    
        max_date_spark = spark_df.select(max('date').alias('max_date')).collect()[0]['max_date']

        return max_date_bq > max_date_spark

    @task.branch
    def branch_new_data_in_bigquery(val):
        if val == True:
            return 'download_new_records_from_dataset' 



    google_auth_credentials_env = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    download_full_dataset = SparkSubmitOperator(
        task_id='submit_download_full_dataset',
        application='/usr/local/airflow/include/scripts/download_full_dataset.py',
        conn_id='spark_cluster',
        env_vars={"GOOGLE_APPLICATION_CREDENTIALS": google_auth_credentials_env},
        deploy_mode='cluster',
        verbose=True,
        files='/opt/hadoop/etc/hadoop/yarn-site.xml,/opt/hadoop/etc/hadoop/core-site.xml,/usr/local/airflow/include/secrets/google-api-key.json#gcp-key.json'
    )
    
    @task.pyspark(conn_id='spark_cluster')
    def download_new_records_from_dataset(spark: SparkSession):
        print("xd")
        pass
    

    data_check = check_any_data_in_hdfs()
    branch1 = branch_any_data_in_hdfs(data_check)

    new_data_check = check_new_data_in_bigquery()
    branch2 = branch_new_data_in_bigquery(new_data_check)

    branch1 >> download_full_dataset
    branch1 >> new_data_check
    new_data_check >> branch2
    branch2 >> download_new_records_from_dataset()

    # (
    #     branch_any_data_in_hdfs(check_any_data_in_hdfs()) >>   
    #     [download_full_dataset,
    #      branch_new_data_in_bigquery(check_new_data_in_bigquery()) >> [download_new_records_from_dataset(),]    
    #     ]
    # )

 #   branch_new_data_in_bigquery(check_new_data_in_bigquery()) >> [download_new_records_from_dataset(),]


ingest_data()
