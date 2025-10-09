from airflow.sdk import dag, task
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql.functions import min
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from pyspark.sql import functions as F
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(dag_id='update_vendor_dim',
     start_date=datetime(2024, 1, 1),
     schedule="@daily",
     template_searchpath="/usr/local/airflow/include/scripts/sql"
     )

def update_vendor_dim():

    conn: Connection =  BaseHook.get_connection('data_warehouse_presentation_layer')
    host = conn.host
    database = conn.schema
    username = conn.login
    password = conn.password
    port = conn.port

    create_vendor_dim = SparkSubmitOperator(
        task_id='create_vendor_dim',
        application='/usr/local/airflow/include/scripts/create_vendor_dim.py',
        conn_id='spark_cluster',
        deploy_mode='cluster',
        application_args=[host, str(port), database, username, password],
        verbose=True,
        files='/opt/hadoop/etc/hadoop/yarn-site.xml,/opt/hadoop/etc/hadoop/core-site.xml,/usr/local/airflow/include/secrets/google-api-key.json#gcp-key.json',
        jars='jars/sqljdbc_13.2/enu/jars/mssql-jdbc-13.2.0.jre11.jar'
    )

    insert_default_value = SQLExecuteQueryOperator(
        task_id="insert_default_value_into_store_dim", conn_id="data_warehouse_presentation_layer", sql="insert_unknown_into_vendor_dim.sql"
    )

    create_vendor_dim >> insert_default_value

update_vendor_dim()