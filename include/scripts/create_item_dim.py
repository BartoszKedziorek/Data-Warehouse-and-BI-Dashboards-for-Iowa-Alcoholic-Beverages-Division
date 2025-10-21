from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from pyspark.sql import functions as F
import sys
from scripts.modules.scd import create_scd_from_input


def remove_one_day_changes(df: DataFrame,
                              columnName: str) -> DataFrame:
    
    duplicates = df.groupBy(['item_number', 'date']) \
                            .agg(F.count_distinct(columnName).alias('count_dist')) \
                            .where('count_dist > 1')

    if duplicates.count() == 0:
        duplicates.unpersist()
        return df

    next_day = df.alias('ts') \
        .join(duplicates.alias('ds'), on='item_number', how='inner') \
        .where('ts.date > ds.date') \
        .groupBy(['ds.item_number', 'ds.date']) \
        .agg(F.min('ts.date').alias('min_date')) \
        .select(F.col('ds.item_number').alias('item_number'), F.col('ds.date').alias('date'), F.col('min_date'))



    next_name = df.alias('ts').join(next_day.alias('nd'), on='item_number', how='inner') \
                    .where('nd.min_date == ts.date') \
                    .select(F.col('nd.item_number').alias('item_number'),
                            F.col('nd.date').alias('date'),
                            F.col('nd.min_date').alias('min_date'),
                            F.col(f"ts.{columnName}").alias('new_value'))
    
    df = df.alias('ts').join(next_name.alias('nn'), on=['item_number', 'date'], how='left') \
            .select('ts.*', F.col('nn.new_value').alias('new_value'))
    
    df = df.withColumn(f'tmp_{columnName}', F.when(df['new_value'].isNotNull(), df['new_value']).otherwise(F.col(columnName)))
    
    df = df.drop(columnName).drop('new_value')

    df = df.withColumnRenamed(f'tmp_{columnName}', columnName)
    
    next_name.unpersist()
    next_day.unpersist()

    return df



spark: SparkSession = SparkSession.builder \
    .appName("Iowa Sales ETL") \
    .getOrCreate()


tmp_scd: DataFrame = spark.read.parquet('ingest/raw_sales')

final_scd_schema = StructType([
    StructField('item_number', IntegerType(), False),
    StructField('item_description', StringType(), False),
    StructField('category', IntegerType(), False),
    StructField('category_name', StringType(), False),
    StructField('start_date', DateType(), False),
    StructField('end_date', DateType(), True),
    StructField('is_current', BooleanType(), True)
])

final_scd_df = spark.createDataFrame([], schema=final_scd_schema)

tmp_scd = tmp_scd.fillna({'category': -1,
                          'category_name': 'unknown'})

tmp_scd = remove_one_day_changes(tmp_scd, 'item_description')
tmp_scd = remove_one_day_changes(tmp_scd, 'category')
tmp_scd = remove_one_day_changes(tmp_scd, 'category_name')

final_scd_df = create_scd_from_input(spark, tmp_scd, ['item_description', 'item_number', 'category', 'category_name'],
                                     'date', 'item_number', final_scd_schema)

final_scd_df = final_scd_df.withColumnsRenamed({
    'item_description': 'ItemName',
    'item_number':'ItemNumberDK',
    'category': 'CategoryNumberDK',
    'category_name':'CategoryName',
    'start_date':'StartDate',
    'end_date':'EndDate',
    'is_current':'IsCurrent'
})


host = sys.argv[1]
database = sys.argv[3]
username = sys.argv[4]
password = sys.argv[5]
port = sys.argv[2]
url = f"jdbc:sqlserver://{host}:{port};database={database}"
properties = {
    "user": f"{username}",
    "password": f"{password}",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "trustServerCertificate": "true",
    "encrypt": "false"
}
table_name = "DimItem"

final_scd_df.write.jdbc(url=url, table=table_name, properties=properties, mode='append')