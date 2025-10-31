import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from scripts.modules.scd import create_scd_from_input
from scripts.modules.ingest_utils import remove_one_day_changes
from pyspark.sql.types import StructField, StructType, IntegerType, DateType, StringType, BooleanType
import sys
from scripts.modules.utils import replace_hash_with_attributes, replace_attributes_with_hash
from scripts.modules.scd import merge_last_scd_record_with_scd_records_from_new_data_both_having_different_attibutes, \
                                merge_last_scd_record_with_oldest_scd_record_from_new_data_both_having_different_attibutes, \
                                merge_last_scd_record_with_scd_records_from_new_data_both_having_same_attibutes
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from scripts.modules.scd import load_update_entries, filter_scd_by_natural_key
from copy import copy

spark: SparkSession = SparkSession.builder \
    .appName("Iowa Sales ETL") \
    .getOrCreate()

tmp_scd: DataFrame = spark.read.parquet('ingest/new_sales')

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

tmp_scd = remove_one_day_changes(tmp_scd, 'store_location', 'store_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'store_name', 'store_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'address', 'store_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'zip_code', 'store_number', 'date')
tmp_scd = remove_one_day_changes(tmp_scd, 'city', 'store_number', 'date')

attrib_cols = ['store_number', 'store_location', 'store_name', 'address', 'zip_code',
               'city']

new_scd_reords_df = create_scd_from_input(tmp_scd, attrib_cols,
                                     'date', 'store_number')

new_scd_reords_df, hash_table_new_scd = replace_attributes_with_hash(new_scd_reords_df, ['store_location',
                                                                                          'store_name', 'address',
                                                                                            'zip_code', 'city'])

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
table_name = "DimStore"

old_scd = spark.read.jdbc(url=url_jdbc, table=table_name, properties=properties)

old_scd = old_scd.withColumnsRenamed({
    'StoreNumberDK': 'store_number',
    'StoreName':'store_name',
    'StoreLocation':'store_location',
    'City':'city',
    'Address':'address',
    'ZipCode':'zip_code',
    'StartDate':'start_date',
    'EndDate':'end_date',
    'IsCurrent':'is_current'
})


old_scd_last_records = old_scd.alias('os').join(
                                old_scd.groupBy('store_number')
                                        .agg(F.max('start_date').alias('start_date'))
                                        .alias('os_max'),
                                on=(F.col('os.store_number') == F.col('os_max.store_number'))
                                    &
                                   (F.col('os.start_date') == F.col('os_max.start_date')),
                                how='inner'
                            ).select(
                                F.col('os.store_number').alias('store_number'), F.col('os.store_name').alias('store_name'),
                                F.col('os.store_location').alias('store_location'), F.col('os.address').alias('address'),
                                F.col('os.city').alias('city'),
                                F.col('os.zip_code').alias('zip_code'), F.col('os.start_date').alias('start_date'),
                                F.col('os.end_date').alias('end_date'), F.col('os.is_current').alias('is_current')
                            )


old_scd_last_records, hash_table_old_scd = replace_attributes_with_hash(old_scd_last_records, ['store_name', 'store_location',
                                                                           'address', 'city',
                                                                           'zip_code'])

new_scd_joined_old_scd = old_scd_last_records.alias('os') \
                                            .join(
                                                new_scd_reords_df.alias('ns'),
                                                on='store_number'
                                            )

new_scd_joined_old_scd.cache()

new_scd_reords_df_minus_lastest_entries_from_old_scd = new_scd_joined_old_scd.where(
                                                                'os.hashed_attributes_value != ns.hashed_attributes_value'
                                                            ).select(
                                                                F.col('ns.store_number').alias('store_number'),
                                                                F.col('ns.start_date').alias('start_date'),
                                                                F.col('ns.end_date').alias('end_date'),
                                                                F.col('ns.is_current').alias('is_current'),
                                                                F.col('ns.hashed_attributes_value').alias('hashed_attributes_value')
                                                            )
                              

entries_count_compare_by_store_number = new_scd_reords_df_minus_lastest_entries_from_old_scd.alias('nsm') \
                                            .groupBy('store_number').agg(F.count('*').alias('ct_1')) \
                                            .join(
                                                new_scd_reords_df.alias('ns') \
                                                    .groupBy('store_number')
                                                    .agg(F.count('*').alias('ct_2')),
                                                on='store_number',
                                                how='inner'
                                            )

entries_count_compare_by_store_number.cache()

stores_with_only_new_entries = entries_count_compare_by_store_number.where('ct_1 == ct_2') \
                                                                .select(F.col('ns.store_number').alias('store_number'),
                                                                        F.col('ct_2').alias('ct'))
stores_with_only_new_entries.cache()


stores_with_only_one_new_entry = stores_with_only_new_entries.where('ct == 1').select('store_number')
stores_with_only_more_new_entries = stores_with_only_new_entries.where('ct != 1').select('store_number')


stores_with_not_only_new_entries = entries_count_compare_by_store_number.where('ct_1 != ct_2') \
                                                                    .select(F.col('ns.store_number').alias('store_number'))

scd_entries_with_only_one_new_entry = filter_scd_by_natural_key(new_scd_reords_df, stores_with_only_one_new_entry, 'store_number')
scd_entries_with_only_more_new_entries = filter_scd_by_natural_key(new_scd_reords_df, stores_with_only_more_new_entries, 'store_number')
scd_entries_with_not_only_new_entries = filter_scd_by_natural_key(new_scd_reords_df, stores_with_not_only_new_entries, 'store_number')

scd_entries_with_only_one_new_entry = replace_hash_with_attributes(scd_entries_with_only_one_new_entry, hash_table_new_scd)
scd_entries_with_only_more_new_entries = replace_hash_with_attributes(scd_entries_with_only_more_new_entries, hash_table_new_scd)
scd_entries_with_not_only_new_entries = replace_hash_with_attributes(scd_entries_with_not_only_new_entries, hash_table_new_scd)


to_update, to_insert = merge_last_scd_record_with_scd_records_from_new_data_both_having_same_attibutes(
    old_scd, scd_entries_with_not_only_new_entries,
    attrib_cols, 'store_number', split_result=True
)


properties_for_engine = copy(properties)
del properties_for_engine['encrypt']
del properties_for_engine['driver']
engine: Engine = create_engine(url=url_pyodbc, connect_args=properties_for_engine)

columns_mapping = {
    'store_number': 'StoreNumberDK',
    'store_name': 'StoreName',
    'store_location': 'StoreLocation',
    'address': 'Address',
    'zip_code': 'ZipCode',
    'city': 'City',
    'start_date': 'StartDate',
    'end_date': 'EndDate',
    'is_current': 'IsCurrent',

} 

long_col = F.expr("CASE WHEN StoreLocation != 'POINT EMPTY' THEN substring(split(StoreLocation, ' ')[0], 7, length(split(StoreLocation, ' ')[0]) - 5)" +
                  "ELSE '-1.0' END")
lat_col = F.expr("CASE WHEN StoreLocation != 'POINT EMPTY' THEN substring(split(StoreLocation, ' ')[1], 0, length(split(StoreLocation, ' ')[1]) - 1)" + 
                 "ELSE '-1.0' END")


load_update_entries(engine, to_update, 'store_number',
                                         'StoreNumberDK', 'DimStore')
to_insert.withColumnsRenamed(columns_mapping) \
    .withColumn(
        'StoreLocationLongitude', long_col
    ).withColumn(
        'StoreLocationLatitude', lat_col
    ) \
    .write.jdbc(url=url_jdbc, table='DimStore', mode='append', properties=properties)



to_update, to_insert = merge_last_scd_record_with_oldest_scd_record_from_new_data_both_having_different_attibutes(
    old_scd, scd_entries_with_only_one_new_entry,
    attrib_cols, 'store_number', split_result=True
)

load_update_entries(engine, to_update, 'store_number',
                                         'StoreNumberDK', 'DimStore')
to_insert.withColumnsRenamed(columns_mapping) \
    .withColumn(
        'StoreLocationLongitude', long_col
    ).withColumn(
        'StoreLocationLatitude', lat_col
    ) \
    .write.jdbc(url=url_jdbc, table='DimStore', mode='append', properties=properties)



to_update, to_insert = merge_last_scd_record_with_scd_records_from_new_data_both_having_different_attibutes(
    old_scd, scd_entries_with_only_more_new_entries,
    attrib_cols, 'store_number', split_result=True
)

load_update_entries(engine, to_update, 'store_number',
                                         'StoreNumberDK', 'DimStore')
to_insert.withColumnsRenamed(columns_mapping) \
    .withColumn(
        'StoreLocationLongitude', long_col
    ).withColumn(
        'StoreLocationLatitude', lat_col
    ) \
    .write.jdbc(url=url_jdbc, table='DimStore', mode='append', properties=properties)