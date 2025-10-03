from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
import pyspark.sql.functions as F
from pyspark.sql import functions as F
import sys


def remove_one_day_changes(df: DataFrame,
                              columnName: str) -> DataFrame:
    
    duplicates = df.groupBy(['store_number', 'date']) \
                            .agg(F.count_distinct(columnName).alias('count_dist')) \
                            .where('count_dist > 1')

    if duplicates.count() == 0:
        duplicates.unpersist()
        return df

    next_day = df.alias('ts') \
        .join(duplicates.alias('ds'), on='store_number', how='inner') \
        .where('ts.date > ds.date') \
        .groupBy(['ds.store_number', 'ds.date']) \
        .agg(F.min('ts.date').alias('min_date')) \
        .select(F.col('ds.store_number').alias('store_number'), F.col('ds.date').alias('date'), F.col('min_date'))



    next_name = df.alias('ts').join(next_day.alias('nd'), on='store_number', how='inner') \
                    .where('nd.min_date == ts.date') \
                    .select(F.col('nd.store_number').alias('store_number'),
                            F.col('nd.date').alias('date'),
                            F.col('nd.min_date').alias('min_date'),
                            F.col(f"ts.{columnName}").alias('new_value'))
    
    df = df.alias('ts').join(next_name.alias('nn'), on=['store_number', 'date'], how='left') \
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
    StructField('store_number', IntegerType(), False),
    StructField('store_name', StringType(), False),
    StructField('address', StringType(), False),
    StructField('city', StringType(), False),
    StructField('zip_code', IntegerType(), False),
    StructField('store_location', StringType(), False),
    StructField('start_date', DateType(), False),
    StructField('end_date', DateType(), True),
    StructField('is_current', BooleanType(), True)
])

final_scd_df = spark.createDataFrame([], schema=final_scd_schema)

tmp_scd = tmp_scd.fillna({'address':'unknown',
                          'city':'unknown',
                          'zip_code': -1,
                          'store_location': 'POINT EMPTY'})

tmp_scd = remove_one_day_changes(tmp_scd, 'store_location')
tmp_scd = remove_one_day_changes(tmp_scd, 'store_name')
tmp_scd = remove_one_day_changes(tmp_scd, 'address')
tmp_scd = remove_one_day_changes(tmp_scd, 'zip_code')
tmp_scd = remove_one_day_changes(tmp_scd, 'city')

tmp_scd = tmp_scd.groupBy(["store_number", "store_name", "address",
                            'city', 'zip_code','store_location']).agg(F.min('date').alias('min_date1'))

while tmp_scd.count() != 0:
    
    merged_scd = tmp_scd.join(
        other=tmp_scd.groupBy("store_number").agg(F.min('min_date1').alias('min_date2')),
        on='store_number',
        how='inner'
    )
    to_add = merged_scd.where('min_date1 = min_date2') 
    final_scd_df = final_scd_df.union(
        to_add.where('min_date1 = min_date2')
        .drop('min_date2')
        .withColumnRenamed('min_date1','start_date')
        .withColumn('end_date', F.lit(None).cast(DateType()))
        .withColumn('IsCurrent', F.lit(False))
    )
    merged_scd = merged_scd.where('min_date1 != min_date2')
    tmp_scd = merged_scd.select(merged_scd.store_number, merged_scd.store_name,
                                merged_scd.address, merged_scd.city,
                                merged_scd.zip_code, merged_scd.store_location, 
                                 merged_scd.min_date1)
    

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
table_name = "DimStore"


tmp_scd = final_scd_df
final_scd_df = spark.createDataFrame([], schema=final_scd_schema)
first_iteration = True

while tmp_scd.count() != 0:
    max_start_date = tmp_scd.groupBy("store_number").agg(F.max('start_date').alias('max_start_date'))

    second_max_start_date = \
        tmp_scd.join(max_start_date, 'store_number', how='inner') \
            .where('start_date != max_start_date') \
            .groupBy('store_number') \
            .agg(F.max('start_date').alias('second_max_date'))
    
    if first_iteration == True:
        add_to_scd = \
            tmp_scd.join(max_start_date, on='store_number', how='inner') \
            .where('max_start_date == start_date') \
            .select(tmp_scd.store_number, tmp_scd.store_name,
                    tmp_scd.address, tmp_scd.city,
                    tmp_scd.zip_code, tmp_scd.store_location,
                     tmp_scd.start_date, tmp_scd.end_date, tmp_scd.is_current) \
            .withColumn('is_current', F.lit(True))

        final_scd_df = final_scd_df.union(add_to_scd)

        first_iteration = False
    

    compare_dates_df = tmp_scd.join(
        max_start_date, on='store_number', how='inner'
    ).join(
        second_max_start_date, on='store_number', how='inner'
    ).where(
        'start_date == second_max_date'
    )

    compare_dates_df = compare_dates_df.drop('end_date').withColumnRenamed('max_start_date', 'end_date')

    final_scd_df = final_scd_df.union(
        compare_dates_df.select(F.col('store_number'), F.col('store_name'),
                                F.col('address'), F.col('city'),
                                F.col('zip_code'), F.col('store_location'),
                                F.col('start_date'), F.col('end_date'), F.col('is_current'))
    )

    tmp_scd = tmp_scd.join(
        max_start_date, on='store_number', how='inner'
    ).where(
        'start_date != max_start_date'
    ).drop(
        'max_start_date'
    )


final_scd_df = final_scd_df.withColumnsRenamed({
    'store_name': 'StoreName',
    'store_number':'StoreNumberDK',
    'address': 'Address',
    'city':'City',
    'zip_code': 'ZipCode',
    'store_location': 'StoreLocation',
    'start_date':'StartDate',
    'end_date':'EndDate',
    'is_current':'IsCurrent'
})

long_col = F.expr("CASE WHEN StoreLocation != 'POINT EMPTY' THEN substring(split(StoreLocation, ' ')[0], 7, length(split(StoreLocation, ' ')[0]) - 5)" +
                  "ELSE '-1.0' END")
lat_col = F.expr("CASE WHEN StoreLocation != 'POINT EMPTY' THEN substring(split(StoreLocation, ' ')[1], 0, length(split(StoreLocation, ' ')[1]) - 1)" + 
                 "ELSE '-1.0' END")

final_scd_df = final_scd_df.withColumn(
    'StoreLocationLongitude', long_col
).withColumn(
    'StoreLocationLatitude', lat_col
)




final_scd_df.write.jdbc(url=url, table=table_name, properties=properties, mode='append')