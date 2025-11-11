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
    StructField('vendor_number', IntegerType(), False),
    StructField('vendor_name', StringType(), False),
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

table_query = "DimVendor"

properties_for_engine = copy(properties)
del properties_for_engine['encrypt']
del properties_for_engine['driver']
engine: Engine = create_engine(url=url_pyodbc, connect_args=properties_for_engine)

columns_mapping = {
    'vendor_name': 'VendorName',
    'vendor_number': 'VendorNumberDK',
    'start_date': 'StartDate',
    'end_date': 'EndDate',
    'is_current': 'IsCurrent'
} 

attrib_cols = ['vendor_name', 'vendor_number']

old_scd = spark.read.jdbc(url=url_jdbc, table=table_query, properties=properties)

reverse_mapping = {val:key for key, val in columns_mapping.items()}

new_records = remove_one_day_changes(new_records, 'vendor_name', 'vendor_number', 'date')

old_scd = old_scd.withColumnsRenamed(reverse_mapping)

to_update, to_insert = get_scd_records_for_update_and_insert(old_scd, new_records, 'vendor_number',
                                                             'date', attrib_cols)


load_update_entries(engine, to_update, 'vendor_number',
                                         'VendorNumberDK', 'DimVendor')
to_insert.withColumnsRenamed(columns_mapping) \
    .write.jdbc(url=url_jdbc, table='DimVendor', mode='append', properties=properties)