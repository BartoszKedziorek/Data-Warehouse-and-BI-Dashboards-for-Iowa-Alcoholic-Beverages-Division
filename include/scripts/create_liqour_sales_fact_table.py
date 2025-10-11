from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructField, StringType, StructType, IntegerType, DateType, BooleanType
from pyspark.sql.functions import min, max, col
from pyspark.sql import functions as F
import sys
import pandas as pd
# import pyodbc
# from pyodbc import Connection


# def create_spark_df_for_dim_table(table_name: str, conn: Connection) -> DataFrame:
#     return spark.createDataFrame(pd.read_sql(f"""
#         SELECT * FROM dbo.{table_name}
#         """, conn))



def join_scd_dim_df(fact_df: DataFrame, dim_df: DataFrame, join_col: str, surrogate_key_col: str) -> DataFrame:
    
    columns_to_drop = list(set(dim_df.schema.names) - set([join_col, surrogate_key_col, 'EndDate', 'StartDate']))

    dim_df = dim_df.drop(*columns_to_drop)

    fact_df = fact_df.alias('fact').join(dim_df.alias('dim'), on=join_col, how='inner') \
                .where('fact.FullDate >= dim.StartDate AND (dim.EndDate IS NULL OR fact.FullDate < dim.EndDate)')
    
    columns_to_drop = [join_col, 'EndDate', 'StartDate']

    fact_df = fact_df.drop(*columns_to_drop)

    return fact_df


def join_dim_df(fact_df: DataFrame, dim_df: DataFrame, join_col: str, surrogate_key_col: str, count_yes: bool = False) -> DataFrame:
    columns_to_drop = list(set(dim_df.schema.names) - set([join_col, surrogate_key_col]))
    
    dim_df = dim_df.drop(*columns_to_drop)
    
    fact_df = fact_df.alias('fact').join(dim_df.alias('dim'), on=join_col, how='inner')
   
    fact_df = fact_df.drop(join_col)
   
    return fact_df


def fill_na_in_county_columns(df: DataFrame, county_df: DataFrame) -> DataFrame:

    name_and_number_null = df.where('CountyNumber IS NULL AND CountyName IS NULL')
    name_null = df.where('CountyName IS NULL AND CountyNumber IS NOT NULL')
    number_null = df.where('CountyName IS NOT NULL AND CountyNumber IS NULL')
    
    name_null = name_null.alias('nn').join(county_df.alias('cd'), on='CountyNumber', how='inner')
    name_null = name_null.drop(name_null['nn.CountyName'])
    name_null = name_null.withColumn('tmp', county_df['CountyName']) \
                            .drop(county_df['CountyName']) \
                            .withColumnRenamed('tmp', 'CountyName') \
                            .drop('nn.CountyNumber')
    
    number_null = number_null.alias('nn').join(county_df.alias('cd'), on='CountyName', how='inner')
    number_null = number_null.drop(number_null['nn.CountyNumber']).withColumn('tmp', county_df['CountyNumber']) \
                                .drop(county_df['CountyNumber']) \
                                .withColumnRenamed('tmp', 'CountyNumber') \
                                .drop('nn.CountyName')
    
    name_and_number_null = name_and_number_null.fillna({'CountyNumber': -1,
                                                        'CountyName':'unknown'})
    
    number_null = number_null.drop('CountyId')
    name_null = name_null.drop('CountyId')
    name_and_number_null = name_and_number_null.drop('CountyId')
    
    
    df = df.where('CountyName IS NOT NULL AND CountyNumber IS NOT NULL') \
            .unionByName(name_null) \
            .unionByName(number_null) \
            .unionByName(name_and_number_null)
    
    return df


spark: SparkSession = SparkSession.builder \
    .appName("Iowa Sales ETL") \
    .getOrCreate()


sales_df: DataFrame = spark.read.parquet('ingest/raw_sales')

sales_df = sales_df.where('sale_dollars > 0 AND sale_dollars IS NOT NULL')
sales_df = sales_df.where('bottles_sold > 0')

sales_df = sales_df.drop('sale_dollars', 'volume_sold_gallons')

server = sys.argv[1]
database = sys.argv[3]
username = sys.argv[4]
password = sys.argv[5]
port = sys.argv[2]
url = f"jdbc:sqlserver://{server}:{port};database={database}"
properties = {
    "user": f"{username}",
    "password": f"{password}",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    "trustServerCertificate": "true",
    "encrypt": "false"
}


args = [spark, server, port, database, username, password]

store_df, item_df, vendor_df = spark.read.jdbc(url=url, table='DimStore', properties=properties), \
                                spark.read.jdbc(url=url, table='DimItem', properties=properties), \
                                spark.read.jdbc(url=url, table='DimVendor', properties=properties) 

county_df, packaging_df, date_df = spark.read.jdbc(url=url, table='DimCounty', properties=properties), \
                                    spark.read.jdbc(url=url, table='DimPackaging', properties=properties), \
                                    spark.read.jdbc(url=url, table='DimDateTable', properties=properties)

rename_map = {
    'store_number': 'StoreNumberDK',
    'item_number': 'ItemNumberDK',
    'vendor_number': 'VendorNumberDK',
    'date': 'FullDate',
    'county_number': 'CountyNumber',
    'county': 'CountyName',
    'pack': 'NumberOfBottlesInPack',
    'bottle_volume_ml': 'BottleVolumeML'
}

for old_name, new_name in rename_map.items():
    sales_df = sales_df.withColumnRenamed(old_name, new_name)


sales_df = sales_df.fillna({
    'StoreNumberDK': -1,
    'VendorNumberDK': -1,
    'ItemNumberDK': -1,
})


sales_df = join_scd_dim_df(sales_df, store_df, 'StoreNumberDK','StoreId')

sales_df = join_scd_dim_df(sales_df, item_df, 'ItemNumberDK' ,'ItemId')

sales_df = join_scd_dim_df(sales_df, vendor_df, 'VendorNumberDK','VendorId')

sales_df = join_dim_df(sales_df, date_df, 'FullDate', 'DateId') # tutaj?

sales_df = fill_na_in_county_columns(sales_df, county_df)

sales_df = join_dim_df(sales_df, county_df, 'CountyNumber', 'CountyId', count_yes=True)

sales_df = sales_df.alias('fact').join(packaging_df.alias('dim'),
                                       (packaging_df['NumberOfBottlesInPack'] == sales_df['NumberOfBottlesInPack'])
                                        &
                                       (packaging_df['BottleVolumeML'] == sales_df['BottleVolumeML']),
                                       how='inner') \
                    .select(sales_df['StoreId'], sales_df['ItemId'],
                            sales_df['VendorId'], sales_df['DateId'],
                            sales_df['CountyId'], packaging_df['PackagingId'],
                              sales_df['NumberOfBottlesInPack'],
                            sales_df['BottleVolumeML'], sales_df['state_bottle_cost'],
                            sales_df['state_bottle_retail'], sales_df['bottles_sold'],
                            sales_df['invoice_and_item_number'])

rename_map = {
    'state_bottle_cost': 'StateBottleCostUSD',
    'state_bottle_retail': 'StateBottleRetailUSD',
    'bottles_sold': 'BottlesSold'
}

for old_name, new_name in rename_map.items():
    sales_df = sales_df.withColumnRenamed(old_name, new_name)

sales_df = sales_df.drop('fact.BottleVolumeML')
sales_df = sales_df.withColumnRenamed('dim.BottleVolumeML', 'BottleVolumeML')

sales_df = sales_df.withColumn('VolumeSoldLiters', sales_df['BottleVolumeML'] * sales_df['NumberOfBottlesInPack'])

sales_df = sales_df.drop('BottleVolumeML', 'NumberOfBottlesInPack')

sales_df = sales_df.withColumn('TotalCostUSD', sales_df['BottlesSold'] * sales_df['StateBottleCostUSD'])

sales_df = sales_df.withColumn('RevenueUSD', sales_df['BottlesSold'] * sales_df['StateBottleRetailUSD'])

sales_df = sales_df.withColumn('GrossProfitUSD', sales_df['RevenueUSD'] - sales_df['TotalCostUSD'])

sales_df = sales_df.withColumn('GrossProfitMargin', sales_df['GrossProfitUSD'] / sales_df['RevenueUSD'] * 100)

invoice_number_col = F.expr("substring(invoice_and_item_number, 0, length(invoice_and_item_number) - 5)")
sales_df = sales_df.withColumn('InvoiceNumber', invoice_number_col)
sales_df = sales_df.drop('invoice_and_item_number')

sales_df.write.jdbc(url=url, table='FLiquorSales', mode='append', properties=properties)