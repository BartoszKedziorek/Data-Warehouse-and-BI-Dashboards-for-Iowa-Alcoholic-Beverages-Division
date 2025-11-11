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
from airflow.sdk import task_group
from airflow.utils.task_group import TaskGroup
from dateutil.relativedelta import relativedelta
from pyspark.sql import functions as F
from airflow.sdk import Variable
from sqlalchemy import text

spark_submit_common_args={
    'conn_id':'spark_cluster',
    'deploy_mode':'cluster',
    'verbose':True,
    'files':'/opt/hadoop/etc/hadoop/yarn-site.xml,/opt/hadoop/etc/hadoop/core-site.xml,/usr/local/airflow/include/secrets/google-api-key.json#gcp-key.json',
    'py_files':'/usr/local/airflow/include/scripts.zip',
    'jars':'jars/sqljdbc_13.2/enu/jars/mssql-jdbc-13.2.0.jre11.jar'
}

@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    template_searchpath="/usr/local/airflow/include/scripts/sql",
    dag_display_name='Main pipeline'
)
def main_pipeline():

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

        return fs.exists(jvm.org.apache.hadoop.fs.Path(path))
    

    @task.branch(trigger_rule='one_success')
    def branch_any_data_in_hdfs(val):
        if val == True:
            return 'skip_download'
        else:
            return 'submit_download_full_dataset'
    
    @task.branch(trigger_rule='one_success')
    def any_data_in_warehouse():
        query = """
        SELECT TOP 1 DateId FROM dbo.FLiquorSales 
        """

        result = conn.execute(query).fetchone()
    
        if result is None:
            return 'load_full_data_into_warehouse'
        else:
            return 'new_data_in_hdfs_check'


    @task.pyspark(task_id='new_data_in_hdfs_check')
    def new_data_in_hdfs(spark: SparkSession):
        path = 'ingest/new_sales/'

        jvm = spark._jvm
        jsc = spark._jsc

        fs = jvm.org.apache.hadoop.fs.FileSystem.get(jsc.hadoopConfiguration())

        path_exists = fs.exists(jvm.org.apache.hadoop.fs.Path(path))
        
        if not path_exists:
            return False

        new_records = spark.read.parquet(path)

        max_date_hdfs = new_records.select(F.max('date').alias('max_date')).collect()[0]['max_date']

        dw_query = """
            SELECT FullDate FROM dbo.DimDateTable
            WHERE DateId = (SELECT MAX(DateId) FROM dbo.FLiquorSales)
        """

        max_date_data_warehouse: datetime = conn.execute(dw_query).fetchone()[0]

        Variable.set('max_date_data_warehouse', max_date_data_warehouse.strftime('%Y-%m-%d'))

        return max_date_hdfs > max_date_data_warehouse


    @task.branch(trigger_rule='one_success')
    def branch_new_data_in_hdfs(val: bool):
        if val == True:
            return 'update_data_in_warehouse'
        else:
            return 'check_new_data_in_bigquery'    



    @task.branch(trigger_rule='one_success')
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

        max_date_data_warehouse: datetime = conn.execute(dw_query).fetchone()[0]

        Variable.set('max_date_data_warehouse', max_date_data_warehouse.strftime('%Y-%m-%d'))

        if max_date_bq > max_date_data_warehouse:
            return 'download_new_records_from_dataset'

    google_auth_credentials_env = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    @task
    def skip_download():
        pass

    download_full_dataset = SparkSubmitOperator(
        task_id='submit_download_full_dataset',
        application='/usr/local/airflow/include/scripts/download_full_dataset.py',
        env_vars={"GOOGLE_APPLICATION_CREDENTIALS": google_auth_credentials_env},
        **spark_submit_common_args
    )

    download_new_records_from_dataset = SparkSubmitOperator(
        task_id='download_new_records_from_dataset',
        application='/usr/local/airflow/include/scripts/download_new_records_from_dataset.py',
        env_vars={"GOOGLE_APPLICATION_CREDENTIALS": google_auth_credentials_env},
        application_args=[Variable.get('max_date_data_warehouse'),],
        **spark_submit_common_args
    )

    with TaskGroup('load_full_data_into_warehouse') as load_full_data_into_warehouse:
    
        with TaskGroup('create_store_dim_task') as create_store_dim_task:            

            create_store_dim = SparkSubmitOperator(
                task_id='create_store_dim',
                application='/usr/local/airflow/include/scripts/create_store_dim.py',
                application_args=[host, str(port), database, username, password],
                **spark_submit_common_args
            )

            insert_default_value_store_dim = SQLExecuteQueryOperator(
                task_id="insert_default_value_into_store_dim", conn_id="data_warehouse_presentation_layer", sql="insert_unknown_into_store_dim.sql"
            )

            create_store_dim >> insert_default_value_store_dim


        with TaskGroup('create_item_dim_task') as create_item_dim_task:

            create_item_dim = SparkSubmitOperator(
                task_id='create_item_dim',
                application='/usr/local/airflow/include/scripts/create_item_dim.py',
                application_args=[host, str(port), database, username, password],
                **spark_submit_common_args
            )

            insert_default_value_item_dim = SQLExecuteQueryOperator(
                task_id="insert_default_value_into_item_dim", conn_id="data_warehouse_presentation_layer", sql="insert_unknown_into_item_dim.sql"
            )

            create_item_dim >> insert_default_value_item_dim


        with TaskGroup('create_vendor_dim_task') as create_vendor_dim_task:

            create_vendor_dim = SparkSubmitOperator(
                task_id='create_vendor_dim',
                application='/usr/local/airflow/include/scripts/create_vendor_dim.py',
                application_args=[host, str(port), database, username, password],
                **spark_submit_common_args
            )

            insert_default_value_vendor_dim = SQLExecuteQueryOperator(
                task_id="insert_default_value_into_store_dim", conn_id="data_warehouse_presentation_layer", sql="insert_unknown_into_vendor_dim.sql"
            )

            create_vendor_dim >> insert_default_value_vendor_dim


        with TaskGroup('create_county_dim_task') as create_county_dim_task:
        
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


            insert_default_value_county_dim = SQLExecuteQueryOperator(
                task_id="insert_default_value_into_county_dim", conn_id="data_warehouse_presentation_layer", sql="insert_unknown_into_county_dim.sql"
            )
        
            create_county_dim() >> insert_default_value_county_dim


        with TaskGroup('create_packaging_dim_task') as create_packaging_dim_task:
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

            insert_default_value_store_dim = SQLExecuteQueryOperator(
                task_id="insert_default_value_into_store_dim", conn_id="data_warehouse_presentation_layer", sql="insert_unknown_into_packaging_dim.sql"
            )

            create_packaging_dim() >> insert_default_value_store_dim 
        

        with TaskGroup('create_date_table_task') as create_date_table_task:
                        
            @task.pyspark(conn_id='spark_cluster', config_kwargs={
                'files':'/opt/hadoop/etc/hadoop/yarn-site.xml',
            })
            def create_date_table(spark: SparkSession):
                
                spark_df = spark.read.parquet('ingest/raw_sales')

                min_date = spark_df.select(F.min('date').alias('min_date')).collect()[0]['min_date']
                max_date = spark_df.select(F.max('date').alias('max_date')).collect()[0]['max_date']
                max_date = max_date + relativedelta(years=5)

                date_df = pd.DataFrame({'FullDate': pd.date_range(min_date, max_date)})

                date_df['DayOfYearNumber'] = date_df['FullDate'].dt.day_of_year
                date_df['DayOfMonthNumber'] = date_df['FullDate'].dt.day
                date_df['DayOfWeekNumber'] = date_df['FullDate'].dt.day_of_week + 1
                date_df['DayOfWeekName'] = date_df['FullDate'].dt.weekday
                date_df['IsWeekend'] = (date_df['FullDate'].dt.day_of_week == 5) | (date_df['FullDate'].dt.day_of_week == 6)

                def season_name_function(my_date: pd.Timestamp):
                    my_date = my_date.date()
                    
                    year = my_date.year
                    start_spring = datetime(year, 3, 21).date()
                    start_summer = datetime(year, 6, 22).date()
                    start_autumn = datetime(year, 9, 23).date()
                    start_winter = datetime(year, 12, 22).date()

                    if start_spring <= my_date < start_summer:
                        return 'spring'
                    elif start_summer <= my_date < start_autumn:
                        return 'summer'
                    elif start_autumn <= my_date < start_winter:
                        return 'autumn'
                    else:
                        return 'winter'

                date_df['AstronomicalSeasonName'] = date_df['FullDate'].map(season_name_function)


                def season_number_function(my_date: pd.Timestamp):
                    my_date = my_date.date()

                    year = my_date.year
                    start_spring = datetime(year, 3, 21).date()
                    start_summer = datetime(year, 6, 22).date()
                    start_autumn = datetime(year, 9, 23).date()
                    start_winter = datetime(year, 12, 22).date()

                    if start_spring <= my_date < start_summer:
                        return 1
                    elif start_summer <= my_date < start_autumn:
                        return 2
                    elif start_autumn <= my_date < start_winter:
                        return 3
                    else:
                        return 4
                    
                date_df['AstronomicalSeasonNumber'] = date_df['FullDate'].map(season_number_function)

                date_df['MonthNumber'] = date_df['FullDate'].dt.month
                date_df['MonthLongName'] = date_df['FullDate'].dt.month_name(locale='en_US.utf8')
                date_df['MonthShortName'] = date_df['MonthLongName'].map({
                                                                    "January": "Jan", "February": "Feb",
                                                                    "March": "Mar", "April": "Apr",
                                                                    "May": "May", "June": "Jun",
                                                                    "July": "Jul", "August": "Aug",
                                                                    "September": "Sep", "October": "Oct",
                                                                    "November": "Nov", "December": "Dec"})
                date_df['Year'] = date_df['FullDate'].dt.year
                date_df['YearMonth'] = date_df['FullDate'].dt.strftime('%Y/%m')

                conn: Connection =  BaseHook.get_connection('data_warehouse_presentation_layer')
                server = conn.host
                database = conn.schema
                username = conn.login
                password = conn.password
                port = conn.port
                connection_string = f'DRIVER={{/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1}};SERVER={server},{port};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'
                conn = pyodbc.connect(connection_string)

                connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"

                engine: Engine = create_engine(connection_url, fast_executemany=True)

                date_df.to_sql('DimDateTable', con=engine, schema='dbo', if_exists='append', index=False)

                return {'min_date': min_date, 'max_date': max_date}

            insert_default_value_date_dim = SQLExecuteQueryOperator(
                task_id="insert_default_value_into_date_table", conn_id="data_warehouse_presentation_layer", sql="insert_unknown_into_date_table.sql"
            )

            create_date_table() >> insert_default_value_date_dim

        create_liqour_sales_fact_table = SparkSubmitOperator(
            task_id='create_liqour_sales_fact_table',
            application='/usr/local/airflow/include/scripts/create_liqour_sales_fact_table.py',
            application_args=[host, str(port), database, username, password],    
            **spark_submit_common_args
        )

        create_store_dim_task >> create_item_dim_task >> create_vendor_dim_task \
            >> create_county_dim_task >> create_packaging_dim_task >> create_date_table_task \
            >> create_liqour_sales_fact_table

    with TaskGroup('update_data_in_warehouse') as update_data_in_warehouse:
        update_store_dim = SparkSubmitOperator(
            task_id='update_store_dim',
            application='/usr/local/airflow/include/scripts/update_store_dim.py',
            application_args=[host, str(port), database, username, password],    
            trigger_rule='none_failed_min_one_success',
            **spark_submit_common_args
        )

        update_vendor_dim = SparkSubmitOperator(
            task_id='update_vendor_dim',
            application='/usr/local/airflow/include/scripts/update_vendor_dim.py',
            application_args=[host, str(port), database, username, password],
            **spark_submit_common_args,    
            trigger_rule='none_failed_min_one_success'
        )

        update_item_dim = SparkSubmitOperator(
            task_id='update_item_dim',
            application='/usr/local/airflow/include/scripts/update_item_dim.py',
            application_args=[host, str(port), database, username, password],
            **spark_submit_common_args,    
            trigger_rule='none_failed_min_one_success'
        )
        
        # @task
        # def update_packaging_dim():
        
        #     client = bigquery.Client()

        #     query = """
        #         SELECT nr.pack as `NumberOfBottlesInPack`,
        #             vol.bottle_volume_ml as `BottleVolumeML`
        #         FROM (SELECT DISTINCT pack  FROM `bigquery-public-data.iowa_liquor_sales.sales`) as `nr`
        #         CROSS JOIN
        #         (SELECT DISTINCT bottle_volume_ml  FROM `bigquery-public-data.iowa_liquor_sales.sales`) as `vol`
        #     """

        #     query_job = client.query(query)

        #     result = query_job.result()

        #     df = result.to_dataframe()

        #     connection_string = f'DRIVER={{/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1}};SERVER={host},{port};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'

        #     connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"

        #     engine: Engine = create_engine(connection_url, fast_executemany=True)

        #     query = "SELECT NumberOfBottlesInPack, BottleVolumeML FROM dbo.DimPackaging"

        #     packaging_dim_database = pd.read_sql(query, engine) 

        #     merge_df = pd.merge(left=df,
        #                         right=packaging_dim_database,
        #                         how='left',
        #                         left_on=['NumberOfBottlesInPack','BottleVolumeML'],
        #                         right_on=['NumberOfBottlesInPack','BottleVolumeML'],
        #                         suffixes=('_l','_r')
        #                         )

        
        #     if len(new_records) != 0:
                
        #         new_records = merge_df.iloc[merge_df['NumberOfBottlesInPack_r'].isnull()]

        #         new_records = new_records.drop('NumberOfBottlesInPack_r', axis=1)
        #         new_records = new_records.drop('BottleVolumeML_r', axis=1)

        #         new_records = new_records.rename({
        #             'NumberOfBottlesInPack_l':'NumberOfBottlesInPack',
        #             'BottleVolumeML_l':'BottleVolumeML'
        #         })

        #         new_records.to_sql('DimPackaging', con=engine, schema='dbo', if_exists='append', index=False)
 
        # @task
        # def update_county_dim():
        #     client = bigquery.Client()

        #     query = """
        #         SELECT county as `CountyName`, county_number as `CountyNumber`
        #         FROM `bigquery-public-data.iowa_liquor_sales.sales` 
        #         WHERE county_number IS NOT NULL
        #         GROUP BY county, county_number
        #     """

        #     query_job = client.query(query)

        #     result = query_job.result()

        #     df = result.to_dataframe()

        #     connection_string = f'DRIVER={{/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1}};SERVER={host},{port};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'

        #     connection_url = f"mssql+pyodbc:///?odbc_connect={connection_string}"

        #     engine: Engine = create_engine(connection_url, fast_executemany=True)

        #     query = "SELECT CountyName, CountyNumber FROM dbo.DimCounty"

        #     county_dim_database = pd.read_sql(query, engine) 

        #     merge_df = pd.merge(left=df,
        #                         right=county_dim_database,
        #                         how='left',
        #                         left_on=['CountyName','CountyNumber'],
        #                         right_on=['CountyName','CountyNumber'],
        #                         suffixes=('_l','_r')
        #                         )

        
        #     if len(new_records) != 0:
                
        #         new_records = merge_df.iloc[merge_df['CountyName_r'].isnull()]

        #         new_records = new_records.drop('CountyName_r', axis=1)
        #         new_records = new_records.drop('CountyNumber_r', axis=1)

        #         new_records = new_records.rename({
        #             'CountyNumber_l':'CountyNumber',
        #             'CountyName_l':'CountyName'
        #         })

        #         new_records.to_sql('DimCounty', con=engine, schema='dbo', if_exists='append', index=False)


        update_store_dim >> update_vendor_dim >> update_item_dim


    skip_download_task = skip_download()
    zip_modules_task = zip_modules()
    data_check = check_any_data_in_hdfs()
    zip_modules_task >> data_check
    any_data_in_hdfs_check = branch_any_data_in_hdfs(data_check)

    any_data_in_warehouse_check = any_data_in_warehouse()
    new_data_check = check_new_data_in_bigquery()

    any_data_in_hdfs_check >> download_full_dataset
    
    any_data_in_hdfs_check >> [skip_download_task, download_full_dataset]
    [skip_download_task, download_full_dataset] >> any_data_in_warehouse_check


    

    new_data_in_hdfs_check = new_data_in_hdfs()
    check_new_data_in_hdfs_task = branch_new_data_in_hdfs(new_data_in_hdfs_check)
    new_data_check >> download_new_records_from_dataset
    check_new_data_in_hdfs_task >> [new_data_check, update_data_in_warehouse]


    download_new_records_from_dataset >> update_data_in_warehouse
    #  >> update_data_in_warehouse

    any_data_in_warehouse_check >> new_data_in_hdfs_check
    any_data_in_warehouse_check >> load_full_data_into_warehouse
    


main_pipeline()
