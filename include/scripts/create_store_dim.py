from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType, DecimalType
import pyspark.sql.functions as F
from pyspark.sql import functions as F
import sys
from scripts.modules.scd import create_scd_from_input
from scripts.modules.ingest_utils import remove_one_day_changes


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
    StructField('store_location_lat', DecimalType(9, 5), False),
    StructField('store_location_long', DecimalType(9, 5), False),
    StructField('start_date', DateType(), False),
    StructField('end_date', DateType(), True),
    StructField('is_current', BooleanType(), True)
])

final_scd_df = spark.createDataFrame([], schema=final_scd_schema)

tmp_scd = tmp_scd.fillna({'address':'unknown',
                          'city':'unknown',
                          'zip_code': -1,
                          'store_location_lat': -1.0,
                          'store_location_long': -1.0})

tmp_scd = remove_one_day_changes(tmp_scd, 'store_location_lat', 'store_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'store_location_long', 'store_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'store_name', 'store_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'address', 'store_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'zip_code', 'store_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'city', 'store_number', 'date')

final_scd_df = create_scd_from_input(tmp_scd, ['store_number', 'store_location_lat', 'store_location_long',
                                                'store_name', 'address', 'zip_code', 'city'],
                                              'date', 'store_number')


final_scd_df = final_scd_df.withColumnsRenamed({
    'store_name': 'StoreName',
    'store_number':'StoreNumberDK',
    'address': 'Address',
    'city':'City',
    'zip_code': 'ZipCode',
    'store_location_lat': 'StoreLocationLatitude',
    'store_location_long': 'StoreLocationLongitude',
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
table_name = "DimStore"

final_scd_df.write.jdbc(url=url, table=table_name, properties=properties, mode='append')