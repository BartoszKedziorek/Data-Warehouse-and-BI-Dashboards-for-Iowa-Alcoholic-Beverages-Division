from airflow.sdk import dag, task
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pyspark.sql.functions import min
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from pyspark.sql import functions as F

@dag(dag_id='update_vendor_dim',
     start_date=datetime(2024, 1, 1),
     schedule="@daily"
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


    # @task.pyspark(conn_id='spark_cluster')
    # def create_vendor_dim(spark: SparkSession):
    #     tmp_scd: DataFrame = spark.read.parquet('ingest/raw_sales')

    #     tmp_scd.createOrReplaceGlobalTempView('raw_sales')

    #     final_scd_schema = StructType([
    #         StructField('vendor_number', IntegerType(), False),
    #         StructField('vendor_name', StringType(), False),
    #         StructField('start_date', DateType(), False),
    #         StructField('end_date', DateType(), True),
    #         StructField('is_current', BooleanType(), True)
    #     ])

    #     final_scd_df = spark.createDataFrame([], schema=final_scd_schema)

    #     tmp_scd = tmp_scd.groupBy(["vendor_number", "vendor_name"]).agg(min('date').alias('min_date1'))

    #     while tmp_scd.count() != 0:
            
    #         merged_scd = tmp_scd.join(
    #             other=tmp_scd.groupBy("vendor_number").agg(min('min_date1').alias('min_date2')),
    #             on='vendor_number',
    #             how='inner'
    #         )

    #         to_add = merged_scd.where('min_date1 = min_date2') 

    #         final_scd_df = final_scd_df.union(
    #             to_add.where('min_date1 = min_date2')
    #             .drop('min_date2')
    #             .withColumnRenamed('min_date1','start_date')
    #             .withColumn('end_date', F.lit(None).cast(DateType()))
    #             .withColumn('IsCurrent', F.lit(False))
    #         )

    #         merged_scd = merged_scd.where('min_date1 != min_date2')

    #         tmp_scd = merged_scd.select(merged_scd.vendor_number, merged_scd.vendor_name, merged_scd.min_date1) \
    #                             .withColumnRenamed('min_date1', 'date')

    #     conn: Connection =  BaseHook.get_connection('data_warehouse_presentation_layer')
    #     host = conn.host
    #     database = conn.schema
    #     username = conn.login
    #     password = conn.password
    #     port = conn.port

    #     url = f"jdbc:sqlserver://{host}:{port};database={database}"
    #     properties = {
    #         "user": f"{username}",
    #         "password": f"{password}",
    #         "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    #     }

    #     table_name = "DimVendor"
    #     final_scd_df.write.jdbc(url=url, table=table_name, properties=properties)


    create_vendor_dim



update_vendor_dim()