from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
from pyspark.sql.functions import min, max, col
from pyspark.sql import functions as F
import sys

spark: SparkSession = SparkSession.builder \
    .appName("Iowa Sales ETL") \
    .getOrCreate()


tmp_scd: DataFrame = spark.read.parquet('ingest/raw_sales')

tmp_scd.createOrReplaceGlobalTempView('raw_sales')

final_scd_schema = StructType([
    StructField('vendor_number', IntegerType(), False),
    StructField('vendor_name', StringType(), False),
    StructField('start_date', DateType(), False),
    StructField('end_date', DateType(), True),
    StructField('is_current', BooleanType(), True)
])
final_scd_df = spark.createDataFrame([], schema=final_scd_schema)
tmp_scd = tmp_scd.groupBy(["vendor_number", "vendor_name"]).agg(min('date').alias('min_date1'))
while tmp_scd.count() != 0:
    
    merged_scd = tmp_scd.join(
        other=tmp_scd.groupBy("vendor_number").agg(min('min_date1').alias('min_date2')),
        on='vendor_number',
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
    tmp_scd = merged_scd.select(merged_scd.vendor_number, merged_scd.vendor_name, merged_scd.min_date1)
    

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
table_name = "DimVendor"


tmp_scd = final_scd_df
final_scd_df = spark.createDataFrame([], schema=final_scd_schema)
first_iteration = True

while tmp_scd.count() != 0:
    max_start_date = tmp_scd.groupBy("vendor_number").agg(max('start_date').alias('max_start_date'))

    second_max_start_date = \
        tmp_scd.join(max_start_date, 'vendor_number', how='inner') \
            .where('start_date != max_start_date') \
            .groupBy('vendor_number') \
            .agg(max('start_date').alias('second_max_date'))
    
    if first_iteration == True:
        add_to_scd = \
            tmp_scd.join(max_start_date, on='vendor_number', how='inner') \
            .where('max_start_date == start_date') \
            .select(tmp_scd.vendor_number, tmp_scd.vendor_name, tmp_scd.start_date, tmp_scd.end_date, tmp_scd.is_current) \
            .withColumn('is_current', F.lit(True))

        final_scd_df = final_scd_df.union(add_to_scd)

        first_iteration = False
    

    compare_dates_df = tmp_scd.join(
        max_start_date, on='vendor_number', how='inner'
    ).join(
        second_max_start_date, on='vendor_number', how='inner'
    ).where(
        'start_date == second_max_date'
    )

    compare_dates_df = compare_dates_df.drop('end_date').withColumnRenamed('max_start_date', 'end_date')

    final_scd_df = final_scd_df.union(
        compare_dates_df.select(col('vendor_number'), col('vendor_name'), col('start_date'),
                                col('end_date'), col('is_current'))
    )

    tmp_scd = tmp_scd.join(
        max_start_date, on='vendor_number', how='inner'
    ).where(
        'start_date != max_start_date'
    ).drop(
        'max_start_date'
    )


final_scd_df = final_scd_df.withColumnsRenamed({
    'vendor_name': 'VendorName',
    'vendor_number':'VendorNumberDK',
    'start_date':'StartDate',
    'end_date':'EndDate',
    'is_current':'IsCurrent'
})

final_scd_df.write.jdbc(url=url, table=table_name, properties=properties, mode='append')