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
import io
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(dag_id='update_county_dim',
     start_date=datetime(2024, 1, 1),
     schedule="@daily",
     template_searchpath="/usr/local/airflow/include/scripts/sql"
     )
def update_county_dim():

    # create_county_dim = SparkSubmitOperator(
    #     task_id='create_county_dim',
    #     application='/usr/local/airflow/include/scripts/create_county_dim.py',
    #     conn_id='spark_cluster',
    #     deploy_mode='cluster',
    #     application_args=[host, str(port), database, username, password],
    #     verbose=True,
    #     files='/opt/hadoop/etc/hadoop/yarn-site.xml,/opt/hadoop/etc/hadoop/core-site.xml,/usr/local/airflow/include/secrets/google-api-key.json#gcp-key.json',
    #     jars='jars/sqljdbc_13.2/enu/jars/mssql-jdbc-13.2.0.jre11.jar'
    # )

    conn: Connection =  BaseHook.get_connection('data_warehouse_presentation_layer')
    host = conn.host
    database = conn.schema
    username = conn.login
    password = conn.password
    port = conn.port

    @task
    def create_county_dim():
    
        client = bigquery.Client()

        query = """
        SELECT county as `CountyName`, county_number as `CountyNumber`
        FROM `bigquery-public-data.iowa_liquor_sales.sales` 
        WHERE county_number IS NOT NULL
        GROUP BY county, county_number
        """
        query_job = client.query(query)

        result = query_job.result()

        df = result.to_dataframe()

        connection_string = f'DRIVER={{/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1}};SERVER={host},{port};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'

        connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"

        engine = create_engine(connection_url, fast_executemany=True)

        df.to_sql('DimCounty', con=engine, schema='dbo', if_exists='append', index=False)


    insert_default_value = SQLExecuteQueryOperator(
        task_id="insert_default_value_into_county_dim", conn_id="data_warehouse_presentation_layer", sql="insert_unknown_into_county_dim.sql"
    )


    create_county_dim() >> insert_default_value



update_county_dim()