from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
from pyspark.sql.functions import min
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

final_scd_df = final_scd_df.withColumnsRenamed({
    'vendor_name': 'VendorName',
    'vendor_number':'VendorNumberDK',
    'start_date':'StartDate',
    'end_date':'EndDate',
    'is_current':'IsCurrent'
})

final_scd_df.write.jdbc(url=url, table=table_name, properties=properties, mode='append')