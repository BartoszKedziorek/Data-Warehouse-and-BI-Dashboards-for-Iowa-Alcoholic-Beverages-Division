from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
from pyspark.sql.functions import min, max, col
from pyspark.sql import functions as F
import sys
from scripts.modules.scd import create_scd_from_input

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

final_scd_df = create_scd_from_input(spark, tmp_scd, 
                                     ['vendor_name', 'vendor_number'],
                                     'date', 'vendor_number', final_scd_schema)

final_scd_df = final_scd_df.withColumnsRenamed({
    'vendor_name': 'VendorName',
    'vendor_number':'VendorNumberDK',
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
table_name = "DimVendor"

final_scd_df.write.jdbc(url=url, table=table_name, properties=properties, mode='append')