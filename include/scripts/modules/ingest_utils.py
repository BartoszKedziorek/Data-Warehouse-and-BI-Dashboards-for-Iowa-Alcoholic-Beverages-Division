from google.cloud import bigquery
import pandas as pd
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, DateType, DecimalType
from google.cloud import bigquery
from pyspark.sql import DataFrame
import numpy as np
from pyspark.sql import SparkSession
import os
from pyspark.sql import functions as F


def download_data_from_bq(spark: SparkSession, bq_client: bigquery.Client, query: str, dest_hdfs_path: str, write_mode: str):
    values = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

    query_job = bq_client.query(query)

    result = query_job.result()

    dataframes = result.to_dataframe_iterable()

    schema = StructType([
        StructField('store_location_long', StringType(), True),
        StructField('store_location_lat', StringType(), True),
        StructField('invoice_and_item_number', StringType(), False),
        StructField('date', DateType(), False),
        StructField('store_number', IntegerType(), False),
        StructField('store_name', StringType(), False),
        StructField('address', StringType(), True),
        StructField('city', StringType(), True),
        StructField('zip_code', IntegerType(), True),
        StructField('county_number', IntegerType(), True),
        StructField('county', StringType(), True),
        StructField('category', IntegerType(), False),
        StructField('category_name', StringType(), False),
        StructField('vendor_number', IntegerType(), False),
        StructField('vendor_name', StringType(), False),
        StructField('item_number', IntegerType(), False),
        StructField('item_description', StringType(), False),
        StructField('pack', IntegerType(), False),
        StructField('bottle_volume_ml', IntegerType(), False),
        StructField('state_bottle_cost', StringType(), False), # Decimal(7, 2)
        StructField('state_bottle_retail', StringType(), False), # Decimal(7, 2)
        StructField('bottles_sold', IntegerType(), False),
        StructField('sale_dollars', StringType(), False), # Decimal(9, 2) 
        StructField('volume_sold_liters', StringType(), False), # Decimal(7, 2)
        StructField('volume_sold_gallons', StringType(), False) # Decimal(7, 2)
    ])

    df: pd.DataFrame
    for df in dataframes:
        print(df.head())
        df['store_number'] = df['store_number'].astype(float).astype("Int64")
        df['zip_code'] = df['zip_code'].str.split('.').apply(lambda x:  x if x is None else x[0]).astype('Int64')
        df['county_number'] = df['county_number'].astype(float).astype('Int64')
        df['category'] = df['category'].str.split('.').apply(lambda x: x[0]).astype(int)
        df['vendor_number'] = df['vendor_number'].str.split('.').apply(lambda x:  x if x is None else x[0]).astype('Int64')
        df['item_number'] = df['item_number'].astype(int)
        df['pack'] = df['pack'].astype(int)
        df['bottle_volume_ml'] = df['bottle_volume_ml'].astype(int)    
        df['bottles_sold'] = df['bottles_sold'].astype(int)

        df: pd.DataFrame = df.replace([np.nan], [None])

        df_sp = spark.createDataFrame(df, schema=schema, verifySchema=True)

        df_sp: DataFrame = df_sp.withColumn('state_bottle_cost', df_sp['state_bottle_cost'].cast(DecimalType(precision=7, scale=2)))
        df_sp = df_sp.withColumn('state_bottle_retail', df_sp['state_bottle_retail'].cast(DecimalType(precision=7, scale=2)))
        df_sp = df_sp.withColumn('sale_dollars', df_sp['sale_dollars'].cast(DecimalType(precision=9, scale=2)))
        df_sp = df_sp.withColumn('volume_sold_liters', df_sp['volume_sold_liters'].cast(DecimalType(precision=7, scale=2)))
        df_sp = df_sp.withColumn('volume_sold_gallons', df_sp['volume_sold_gallons'].cast(DecimalType(precision=7, scale=2)))

        df_sp.write \
            .format('parquet') \
            .option('path', dest_hdfs_path) \
            .mode(write_mode) \
            .save()


def remove_one_day_changes(df: DataFrame, columnName: str,
                           natural_key: str, date_column: str) -> DataFrame:

    duplicates = df.groupBy([natural_key, date_column]) \
                            .agg(F.count_distinct(columnName).alias('count_dist')) \
                            .where('count_dist > 1')

    if duplicates.count() == 0:
        duplicates.unpersist()
        return df

    next_day = df.alias('ts') \
        .join(duplicates.alias('ds'), on=natural_key, how='inner') \
        .where(f'ts.{date_column} > ds.{date_column}') \
        .groupBy([f'ds.{natural_key}', f'ds.{date_column}']) \
        .agg(F.min(f'ts.{date_column}').alias('min_date')) \
        .select(F.col(f'ds.{natural_key}').alias(natural_key), F.col(f'ds.{date_column}').alias(date_column), F.col('min_date'))



    next_name = df.alias('ts').join(next_day.alias('nd'), on=natural_key, how='inner') \
                    .where(f'nd.min_date == ts.{date_column}') \
                    .select(F.col(f'nd.{natural_key}').alias(natural_key),
                            F.col(f'nd.{date_column}').alias(date_column),
                            F.col('nd.min_date').alias('min_date'),
                            F.col(f"ts.{columnName}").alias('new_value'))
    
    df = df.alias('ts').join(next_name.alias('nn'), on=[natural_key, date_column], how='left') \
            .select('ts.*', F.col('nn.new_value').alias('new_value'))
    
    df = df.withColumn(f'tmp_{columnName}', F.when(df['new_value'].isNotNull(), df['new_value']).otherwise(F.col(columnName)))
    
    df = df.drop(columnName).drop('new_value')

    df = df.withColumnRenamed(f'tmp_{columnName}', columnName)
    
    next_name.unpersist()
    next_day.unpersist()

    return df