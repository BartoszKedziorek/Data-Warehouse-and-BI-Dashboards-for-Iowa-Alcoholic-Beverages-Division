import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, BooleanType, Row
import datetime
import pytest_check as check
import pyspark.sql.functions as F
from typing import List
from copy import copy
import datetime
from scripts.modules.utils import replace_attributes_with_hash, replace_hash_with_attributes


class TestUtils:
    spark: SparkSession

    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder \
                        .appName('pytest-yarn-test') \
                        .config('spark.submit.deployMode', 'client') \
                        .config('spark.yarn.am.memory','3G') \
                        .getOrCreate()
        
        cls.attributes_cols: List[str] = ['store_number', 'store_name', 'address',
                                                           'city', 'zip_code', 'store_location']

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()


    def test_create_scd_from_input_for_more_than_one_change__attrib_values_from_hash_table_mathes_values_from_input_table(self):
    
        input_df = self.spark.createDataFrame([
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2024, 11, 12),end_date=datetime.date(2024, 11, 27), is_current=False),
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2024, 11, 27), end_date=None, is_current=True)
        ])

        _, hash_math_df = replace_attributes_with_hash(self.spark, input_df, self.attributes_cols)

        test_df = input_df.withColumn('tmp_hash', F.hash(*self.attributes_cols))

        compare_df = test_df.alias('td').join(hash_math_df.alias('df'), hash_math_df['hashed_attributes_value'] == test_df['tmp_hash'], how='inner') \
                    .select(*([F.col(f'td.{col}').alias(f'td_{col}') for col in test_df.columns] +
                               [F.col(f'df.{col}').alias(f'df_{col}') for col in hash_math_df.columns]))
        compare_df.take(1)
        compare_df.cache()

        first_row = compare_df.where(F.col('td_start_date') == F.lit('2024-11-12')).collect()[0]
        second_row = compare_df.where(F.col('td_start_date') == F.lit('2024-11-27')).collect()[0]     

        check.equal(first_row.df_store_name, first_row.td_store_name)
        check.equal(first_row.df_store_number, first_row.td_store_number)
        check.equal(first_row.df_store_location, first_row.td_store_location)
        check.equal(first_row.df_address, first_row.td_address)
        check.equal(first_row.df_zip_code, first_row.td_zip_code)
        check.equal(first_row.df_city, first_row.td_city)
        
        check.equal(second_row.df_store_name, second_row.td_store_name)
        check.equal(second_row.df_store_number, second_row.td_store_number)
        check.equal(second_row.df_store_location, second_row.td_store_location)
        check.equal(second_row.df_address, second_row.td_address)
        check.equal(second_row.df_zip_code, second_row.td_zip_code)
        check.equal(second_row.df_city, second_row.td_city)


    def test_replace_hash_with_attributes__attrib_values_after_decoding_matches_original_data(self):

        input_df = self.spark.createDataFrame([
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2024, 11, 12),end_date=datetime.date(2024, 11, 27), is_current=False),
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2024, 11, 27), end_date=None, is_current=True)
        ])

        df = input_df.withColumn('hashed_attributes_value', F.hash(*self.attributes_cols)) \
                     
        hash_math_df = df.select(*self.attributes_cols) \
                     .withColumn('hashed_attributes_value', F.hash(*self.attributes_cols))

        df = df.drop(*self.attributes_cols)

        decoded_df = replace_hash_with_attributes(self.spark, df, hash_math_df)

        input_df_record = input_df.where(F.col('start_date') == F.lit('2024-11-12')).collect()[0]
        decoded_df_record = decoded_df.where(F.col('start_date') == F.lit('2024-11-12')).collect()[0]

        check.equal(input_df_record.store_name, decoded_df_record.store_name)
        check.equal(input_df_record.store_number, decoded_df_record.store_number)
        check.equal(input_df_record.store_location, decoded_df_record.store_location)
        check.equal(input_df_record.address, decoded_df_record.address)
        check.equal(input_df_record.zip_code, decoded_df_record.zip_code)
        check.equal(input_df_record.city, decoded_df_record.city)
