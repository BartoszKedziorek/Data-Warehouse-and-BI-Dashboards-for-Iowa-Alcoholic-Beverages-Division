from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
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

tmp_scd = remove_one_day_changes(tmp_scd, 'item_description', 'item_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'category', 'item_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'category_name', 'item_number', 'date')

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