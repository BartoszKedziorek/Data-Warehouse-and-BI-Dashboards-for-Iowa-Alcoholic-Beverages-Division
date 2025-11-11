import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from scripts.modules.ingest_utils import remove_one_day_changes
from pyspark.sql.types import StructField, StructType, IntegerType, DateType, StringType, BooleanType
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

final_scd_schema = StructType([
    StructField('item_number', IntegerType(), False),
    StructField('item_description', StringType(), False),
    StructField('category', IntegerType(), False),
    StructField('category_name', StringType(), False),
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

table_query = "DimItem"

properties_for_engine = copy(properties)
del properties_for_engine['encrypt']
del properties_for_engine['driver']
engine: Engine = create_engine(url=url_pyodbc, connect_args=properties_for_engine)

columns_mapping = {
    'item_number': 'ItemNumberDK',
    'item_description': 'ItemName',
    'category': 'CategoryNumberDK',
    'category_name': 'CategoryName',
    'start_date': 'StartDate',
    'end_date': 'EndDate',
    'is_current': 'IsCurrent'
} 

attrib_cols = ['item_number', 'item_description', 'category','category_name']

old_scd = spark.read.jdbc(url=url_jdbc, table=table_query, properties=properties)

reverse_mapping = {val:key for key, val in columns_mapping.items()}

new_records = remove_one_day_changes(new_records, 'item_description', 'item_number', 'date')
new_records = remove_one_day_changes(new_records, 'category_name', 'item_number', 'date')
new_records = remove_one_day_changes(new_records, 'category', 'item_number', 'date')

old_scd = old_scd.withColumnsRenamed(reverse_mapping)

to_update, to_insert = get_scd_records_for_update_and_insert(old_scd, new_records, 'item_number',
                                                             'date', attrib_cols)


load_update_entries(engine, to_update, 'item_number',
                                         'ItemNumberDK', 'DimItem')
to_insert.withColumnsRenamed(columns_mapping) \
    .write.jdbc(url=url_jdbc, table='DimItem', mode='append', properties=properties)