from airflow.sdk import dag, task
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql.functions import min
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from pyspark.sql import functions as F


@dag(dag_id='update_item_dim',
     start_date=datetime(2024, 1, 1),
     schedule="@daily"
     )

def update_item_dim():

    conn: Connection =  BaseHook.get_connection('data_warehouse_presentation_layer')
    host = conn.host
    database = conn.schema
    username = conn.login
    password = conn.password
    port = conn.port

    create_item_dim = SparkSubmitOperator(
        task_id='create_item_dim',
        application='/usr/local/airflow/include/scripts/create_item_dim.py',
        conn_id='spark_cluster',
        deploy_mode='cluster',
        application_args=[host, str(port), database, username, password],
        verbose=True,
        files='/opt/hadoop/etc/hadoop/yarn-site.xml,/opt/hadoop/etc/hadoop/core-site.xml,/usr/local/airflow/include/secrets/google-api-key.json#gcp-key.json',
        jars='jars/sqljdbc_13.2/enu/jars/mssql-jdbc-13.2.0.jre11.jar'
    )

    create_item_dim

update_item_dim()