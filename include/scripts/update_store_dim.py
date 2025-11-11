import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from scripts.modules.ingest_utils import remove_one_day_changes
from pyspark.sql.types import StructField, StructType, IntegerType, DateType, StringType, BooleanType, DecimalType
import sys
from scripts.modules.scd import get_scd_records_for_update_and_insert
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from scripts.modules.scd import load_update_entries
from copy import copy

spark: SparkSession = SparkSession.builder \
    .appName("Iowa Sales ETL") \
    .getOrCreate()

new_records: DataFrame = spark.read.parquet('ingest/new_sales')

new_store_location_col = F.expr("CASE WHEN StoreLocationLongitude != -1 THEN CONCAT('POINT (', CAST(StoreLocationLongitude AS DECIMAL(9,5)), ' ', CAST(StoreLocationLatitude AS DECIMAL(9,5))) ELSE 'POINT EMPTY' END")

final_scd_schema = StructType([
    StructField('store_number', IntegerType(), False),
    StructField('store_name', StringType(), False),
    StructField('address', StringType(), False),
    StructField('city', StringType(), False),
    StructField('zip_code', IntegerType(), False),
    StructField('store_location_lat', StringType(), False),
    StructField('store_location_long', StringType(), False),
    StructField('start_date', DateType(), False),
    StructField('end_date', DateType(), True),
    StructField('is_current', BooleanType(), True)
])

host = sys.argv[1]
database = sys.argv[3]
username = sys.argv[4]
password = sys.argv[5]
port = sys.argv[2]
url_jdbc = f"jdbc:sqlserver://{host}:{port};database={database}"
url_pyodbc = f"mssql+pyodbc:///?odbc_connect=DRIVER={{/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.5.so.1.1}};SERVER={host},{port};DATABASE={database};TrustServerCertificate=yes"
properties = {
    "user": f"{username}",
    "password": f"{password}",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "trustServerCertificate": "true",
    "encrypt": "false"
}

table_query = """(SELECT  
                  StoreNumberDK, StoreName, City, Address, ZipCode,
                  StoreLocationLongitude, StoreLocationLatitude,
                  StartDate, EndDate, IsCurrent
                   FROM [Iowa_Sales_Data_Warehouse].[dbo].[DimStore]) as [DimStore]"""

properties_for_engine = copy(properties)
del properties_for_engine['encrypt']
del properties_for_engine['driver']
engine: Engine = create_engine(url=url_pyodbc, connect_args=properties_for_engine)

columns_mapping = {
    'store_number': 'StoreNumberDK',
    'store_name': 'StoreName',
    'store_location_lat': 'StoreLocationLatitude',
    'store_location_long': 'StoreLocationLongitude',
    'address': 'Address',
    'zip_code': 'ZipCode',
    'city': 'City',
    'start_date': 'StartDate',
    'end_date': 'EndDate',
    'is_current': 'IsCurrent',

} 

attrib_cols = ['store_number', 'store_location_lat', 'store_location_long', 'store_name', 'address', 'zip_code',
               'city']

final_scd_df = spark.createDataFrame([], schema=final_scd_schema)

new_records = new_records.fillna({'address':'unknown',
                          'city':'unknown',
                          'zip_code': -1,
                          'store_location_lat': -1.0,
                          'store_location_long': -1.0})

new_records = remove_one_day_changes(new_records, 'store_location_lat', 'store_number', 'date')
new_records = remove_one_day_changes(new_records, 'store_location_long', 'store_number', 'date')
new_records = remove_one_day_changes(new_records, 'store_name', 'store_number', 'date')
new_records = remove_one_day_changes(new_records, 'address', 'store_number', 'date')
new_records = remove_one_day_changes(new_records, 'zip_code', 'store_number', 'date')
new_records = remove_one_day_changes(new_records, 'city', 'store_number', 'date')


# to_update, to_insert = get_scd_records_for_update_and_insert(spark, new_records, attrib_cols,
#                                                              'store_number', url_jdbc, properties,
#                                                              columns_mapping, table_query)

old_scd = spark.read.jdbc(url=url_jdbc, table=table_query, properties=properties)

reverse_mapping = {val:key for key, val in columns_mapping.items()}

old_scd = old_scd.withColumnsRenamed(reverse_mapping)

to_update, to_insert = get_scd_records_for_update_and_insert(old_scd, new_records,
                                                              'store_number', 'date', attrib_cols)


load_update_entries(engine, to_update, 'store_number',
                                         'StoreNumberDK', 'DimStore')

to_insert.withColumnsRenamed(columns_mapping).write.jdbc(url=url_jdbc, table='DimStore', mode='append', properties=properties)