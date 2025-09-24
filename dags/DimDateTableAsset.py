from airflow.sdk import asset, dag, Asset, DAG
import pandas as pd
from airflow.decorators import task
from pyspark.sql import SparkSession
from pyspark.sql.functions import min, max
from datetime import datetime, date
import numpy as np
import pyodbc
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine
from airflow.models.connection import Connection

# @asset(name="DimDateTable",schedule="@daily")
# def dim_date_table_asset():
#     return 'oke'

date_table_asset = Asset(name="DimDateTable")

@task.pyspark(conn_id='spark_cluster')
def create_date_table(spark: SparkSession):
    
    spark_df = spark.read.parquet('ingest/raw_sales')

    min_date = spark_df.select(min('date').alias('min_date')).collect()[0]['min_date']
    max_date = spark_df.select(max('date').alias('max_date')).collect()[0]['max_date']

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

    engine = create_engine(connection_url, fast_executemany=True)

    date_df.to_sql('DimDateTable', con=engine, schema='dbo', if_exists='append', index=False)

    return {'min_date': min_date, 'max_date': max_date}

with DAG(dag_id='create_dim_date_table', schedule='@daily'):
    create_date_table()
