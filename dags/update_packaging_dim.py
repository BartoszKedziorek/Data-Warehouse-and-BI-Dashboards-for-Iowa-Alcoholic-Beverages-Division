from airflow.sdk import dag, task
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql.functions import min
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from pyspark.sql import functions as F
from google.cloud import bigquery
import pyodbc
from sqlalchemy import create_engine
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(dag_id='update_packaging_dim',
     start_date=datetime(2024, 1, 1),
     schedule="@daily",
     template_searchpath="/usr/local/airflow/include/scripts/sql"
     )
def update_packaging_dim():

    conn: Connection =  BaseHook.get_connection('data_warehouse_presentation_layer')
    host = conn.host
    database = conn.schema
    username = conn.login
    password = conn.password
    port = conn.port

    @task
    def create_packaging_dim():
    
        client = bigquery.Client()

        query = """
            SELECT nr.pack as `NumberOfBottlesInPack`,
                vol.bottle_volume_ml as `BottleVolumeML`
            FROM (SELECT DISTINCT pack  FROM `bigquery-public-data.iowa_liquor_sales.sales`) as `nr`
            CROSS JOIN
            (SELECT DISTINCT bottle_volume_ml  FROM `bigquery-public-data.iowa_liquor_sales.sales`) as `vol`
        """

        query_job = client.query(query)

        result = query_job.result()

        df = result.to_dataframe()

        connection_string = f'DRIVER={{/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1}};SERVER={host},{port};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'

        connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"

        engine = create_engine(connection_url, fast_executemany=True)

        df.to_sql('DimPackaging', con=engine, schema='dbo', if_exists='append', index=False)

    insert_default_value = SQLExecuteQueryOperator(
        task_id="insert_default_value_into_store_dim", conn_id="data_warehouse_presentation_layer", sql="insert_unknown_into_packaging_dim.sql"
    )

    create_packaging_dim() >> insert_default_value


update_packaging_dim()



