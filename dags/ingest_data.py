from airflow.decorators import dag, task 
from datetime import datetime
from google.cloud import bigquery
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, DateType, DecimalType
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from google.cloud import bigquery
from pyspark.sql.functions import max
from sqlalchemy import create_engine
import numpy as np
from decimal import Decimal
import os
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
import pyodbc
from sqlalchemy.engine import Engine

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
)
def ingest_data():

    conn: Connection =  BaseHook.get_connection('data_warehouse_presentation_layer')
    host = conn.host
    database = conn.schema
    username = conn.login
    password = conn.password
    port = conn.port

    connection_string = f'DRIVER={{/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1}};SERVER={host},{port};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'

    conn: pyodbc.Connection = pyodbc.connect(connection_string)


    @task.bash
    def zip_modules():
        return "cd /usr/local/airflow/include && zip -r /usr/local/airflow/include/scripts.zip ./scripts"

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
    
    @task.python
    def any_data_in_warehouse():
        query = """
        SELECT TOP 1 DateId FROM dbo.FLiquorSales 
        """
        result = conn.execute(query).fetchone()
    
        if result is None:
            return 'create_dim_date'
        else:
            return 'check_new_data_in_bigquery'


    @task.python
    def create_dim_date():
        pass

    @task.python
    def update_dim_date():
        pass


    @task.python
    def check_new_data_in_bigquery():
        client = bigquery.Client()

        bg_query = """
        SELECT MAX(date) as max_date FROM `bigquery-public-data.iowa_liquor_sales.sales`
        """
        
        max_date_bq = client.query(bg_query).result().to_dataframe()['max_date'].iloc[0]        

        dw_query = """
            SELECT FullDate FROM dbo.DimDateTable
            WHERE DateId = (SELECT MAX(DateId) FROM dbo.FLiquorSales)
        """

        max_date_data_warehouse = conn.execute(dw_query).fetchone()[0]

        if max_date_bq > max_date_data_warehouse:
            return 'download_new_records_from_dataset'

    google_auth_credentials_env = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    download_full_dataset = SparkSubmitOperator(
        task_id='submit_download_full_dataset',
        application='/usr/local/airflow/include/scripts/download_full_dataset.py',
        conn_id='spark_cluster',
        env_vars={"GOOGLE_APPLICATION_CREDENTIALS": google_auth_credentials_env},
        deploy_mode='cluster',
        verbose=True,
        files='/opt/hadoop/etc/hadoop/yarn-site.xml,/opt/hadoop/etc/hadoop/core-site.xml,/usr/local/airflow/include/secrets/google-api-key.json#gcp-key.json',
        py_files='/usr/local/airflow/include/scripts.zip',
    )
    
    @task.pyspark(conn_id='spark_cluster')
    def download_new_records_from_dataset(spark: SparkSession):
        print("xd")
        pass
    
    
    data_check = zip_modules() >> check_any_data_in_hdfs()
    any_data_in_hdfs_check = branch_any_data_in_hdfs(data_check)

    any_data_in_warehouse_check = any_data_in_warehouse()
    new_data_check = check_new_data_in_bigquery()

    any_data_in_hdfs_check >> download_full_dataset
    any_data_in_hdfs_check >> any_data_in_warehouse_check

    download_full_dataset >> any_data_in_warehouse_check
    new_data_check >> download_new_records_from_dataset()
    
    any_data_in_warehouse_check >> new_data_check
    any_data_in_warehouse_check >> create_dim_date()


ingest_data()
