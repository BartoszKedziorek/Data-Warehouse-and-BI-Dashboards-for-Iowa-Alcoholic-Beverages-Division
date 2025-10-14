import pytest
from pyspark.sql import SparkSession
from scripts.modules.scd import create_scd_from_input, \
                                merge_last_scd_record_with_oldest_scd_record_from_new_data_both_having_different_attibutes
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, BooleanType, Row
import datetime
import pytest_check as check
import pyspark.sql.functions as F
from typing import List

class TestSCD:
    spark: SparkSession

    @classmethod
    def setup_class(cls):
        cls.spark = SparkSession.builder \
                        .appName('pytest-yarn-test') \
                        .config('spark.submit.deployMode', 'client') \
                        .getOrCreate()
        
        cls.attributes_cols: List[str] = ['store_number', 'store_name', 'address',
                                                           'city', 'zip_code', 'store_location']

        cls.final_scd_schema: StructType = StructType([
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

    @classmethod
    def teardown_class(cls):
        cls.spark.stop()


    def test_create_scd_from_input_for_more_than_one_change(self):

        input_df = self.spark.createDataFrame([
            Row(invoice_and_item_number='RINV-04934100006', date=datetime.date(2024, 1, 1),
                store_number=4970, store_name='JEFF\'S MARKET / WEST LIBERTY',
                address='200, E 3RD ST', city='WEST LIBERTY',
                zip_code=52776, store_location='POINT(-91.261560959 41.569567007)',
                county_number=23, county='POLK',
                category=1032100, category_name='IMPORTED VODKAS',
                vendor_number=370, vendor_name='PERNOD RICARD USA',
                item_number=34007, item_description='ABSOLUT SWEDISH VODKA 80PRF',
                pack=12, bottle_volume_ml=1000,
                state_bottle_cost='14.99', state_bottle_retail='22.49',
                bottles_sold=12, sale_dollars='539.76',
                volume_sold_liters='-24.0', volume_sold_gallons='-6.34'
                ),
            Row(invoice_and_item_number='RINV-04934100007', date=datetime.date(2024, 1, 12),
                store_number=4970, store_name='JEFF\'S MARKET / WEST LIBERTY CHANGED 1',
                address='200, E 3RD ST', city='WEST LIBERTY',
                zip_code=52776, store_location='POINT(-91.261560959 41.569567007)',
                county_number=23, county='POLK',
                category=1032100, category_name='IMPORTED VODKAS',
                vendor_number=370, vendor_name='PERNOD RICARD USA',
                item_number=34007, item_description='ABSOLUT SWEDISH VODKA 80PRF',
                pack=12, bottle_volume_ml=1000,
                state_bottle_cost='14.99', state_bottle_retail='22.49',
                bottles_sold=12, sale_dollars='539.76',
                volume_sold_liters='-24.0', volume_sold_gallons='-6.34'
                ),
            Row(invoice_and_item_number='RINV-04934100008', date=datetime.date(2024, 1, 13),
                store_number=4970, store_name='JEFF\'S MARKET / WEST LIBERTY CHANGED 1',
                address='200, E 3RD ST', city='WEST LIBERTY',
                zip_code=52776, store_location='POINT(-91.261560959 41.569567007)',
                county_number=23, county='POLK',
                category=1032100, category_name='IMPORTED VODKAS',
                vendor_number=370, vendor_name='PERNOD RICARD USA',
                item_number=34007, item_description='ABSOLUT SWEDISH VODKA 80PRF',
                pack=12, bottle_volume_ml=1000,
                state_bottle_cost='14.99', state_bottle_retail='22.49',
                bottles_sold=12, sale_dollars='539.76',
                volume_sold_liters='-24.0', volume_sold_gallons='-6.34'
                ),
            Row(invoice_and_item_number='RINV-04934100009', date=datetime.date(2024, 1, 14),
                store_number=4970, store_name='JEFF\'S MARKET / WEST LIBERTY CHANGED 1',
                address='200, E 3RD ST', city='WEST LIBERTY',
                zip_code=52776, store_location='POINT(-91.261560959 41.569567007)',
                county_number=23, county='POLK',
                category=1032100, category_name='IMPORTED VODKAS',
                vendor_number=370, vendor_name='PERNOD RICARD USA',
                item_number=34007, item_description='ABSOLUT SWEDISH VODKA 80PRF',
                pack=12, bottle_volume_ml=1000,
                state_bottle_cost='14.99', state_bottle_retail='22.49',
                bottles_sold=12, sale_dollars='539.76',
                volume_sold_liters='-24.0', volume_sold_gallons='-6.34'
                ),
            Row(invoice_and_item_number='RINV-04934100010', date=datetime.date(2024, 2, 15),
                store_number=4970, store_name='JEFF\'S MARKET / WEST LIBERTY CHANGED 2',
                address='200, E 3RD ST', city='WEST LIBERTY',
                zip_code=52776, store_location='POINT(-91.261560959 41.569567007)',
                county_number=23, county='POLK',
                category=1032100, category_name='IMPORTED VODKAS',
                vendor_number=370, vendor_name='PERNOD RICARD USA',
                item_number=34007, item_description='ABSOLUT SWEDISH VODKA 80PRF',
                pack=12, bottle_volume_ml=1000,
                state_bottle_cost='14.99', state_bottle_retail='22.49',
                bottles_sold=12, sale_dollars='539.76',
                volume_sold_liters='-24.0', volume_sold_gallons='-6.34'
                )
        ])

        df = create_scd_from_input(self.spark, input_df, self.attributes_cols,
                                    'date', 'store_number', self.final_scd_schema)

        first_scd_record = df.where(F.col('start_date') == F.to_date(F.lit('2024-1-1'))).collect()[0]
        second_scd_record = df.where(F.col('start_date') == F.to_date(F.lit('2024-1-12'))).collect()[0]
        third_scd_record = df.where(F.col('start_date') == F.lit('2024-2-15')).collect()[0]

        check.equal(first_scd_record['store_number'], 4970)
        check.equal(first_scd_record['store_name'], 'JEFF\'S MARKET / WEST LIBERTY')
        check.equal(first_scd_record['zip_code'], 52776)
        check.equal(first_scd_record['store_location'], 'POINT(-91.261560959 41.569567007)')
        check.equal(first_scd_record['address'], '200, E 3RD ST')
        check.equal(first_scd_record['start_date'], datetime.date(2024, 1, 1))
        check.equal(first_scd_record['end_date'], datetime.date(2024, 1, 12))
        check.equal(first_scd_record['is_current'], False)
        
        check.equal(second_scd_record['store_number'], 4970)
        check.equal(second_scd_record['store_name'], 'JEFF\'S MARKET / WEST LIBERTY CHANGED 1')
        check.equal(second_scd_record['zip_code'], 52776)
        check.equal(second_scd_record['store_location'], 'POINT(-91.261560959 41.569567007)')
        check.equal(second_scd_record['address'], '200, E 3RD ST')
        check.equal(second_scd_record['start_date'], datetime.date(2024, 1, 12))
        check.equal(second_scd_record['end_date'], datetime.date(2024, 2, 15))
        check.equal(second_scd_record['is_current'], False)

        check.equal(third_scd_record['store_number'], 4970)
        check.equal(third_scd_record['store_name'], 'JEFF\'S MARKET / WEST LIBERTY CHANGED 2')
        check.equal(third_scd_record['zip_code'], 52776)
        check.equal(third_scd_record['store_location'], 'POINT(-91.261560959 41.569567007)')
        check.equal(third_scd_record['address'], '200, E 3RD ST')
        check.equal(third_scd_record['start_date'], datetime.date(2024, 2, 15))
        check.equal(third_scd_record['end_date'], None)
        check.equal(third_scd_record['is_current'], True)        

    def test_merge_last_scd_record_with_oldest_scd_record_from_new_data_both_having_different_attibutes__one_store(self):
        
        old_scd = self.spark.createDataFrame([
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2024, 11, 12),end_date=datetime.date(2024, 11, 27), is_current=False),
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2024, 11, 27), end_date=None, is_current=True)
        ])

        new_records = self.spark.createDataFrame([
            Row(invoice_and_item_number='RINV-04934100010', date=datetime.date(2025, 1, 11),
                store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 2',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                county_number=23, county='POLK',
                category=1032100, category_name='IMPORTED VODKAS',
                vendor_number=370, vendor_name='PERNOD RICARD USA',
                item_number=34007, item_description='ABSOLUT SWEDISH VODKA 80PRF',
                pack=12, bottle_volume_ml=1000,
                state_bottle_cost='14.99', state_bottle_retail='22.49',
                bottles_sold=12, sale_dollars='539.76',
                volume_sold_liters='-24.0', volume_sold_gallons='-6.34'
                ),
            Row(invoice_and_item_number='RINV-04934100010', date=datetime.date(2025, 1, 12),
                store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 2',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                county_number=23, county='POLK',
                category=1032100, category_name='IMPORTED VODKAS',
                vendor_number=370, vendor_name='PERNOD RICARD USA',
                item_number=34007, item_description='ABSOLUT SWEDISH VODKA 80PRF',
                pack=12, bottle_volume_ml=1000,
                state_bottle_cost='14.99', state_bottle_retail='22.49',
                bottles_sold=12, sale_dollars='539.76',
                volume_sold_liters='-24.0', volume_sold_gallons='-6.34'
                )
        ])

        merged_scd = merge_last_scd_record_with_oldest_scd_record_from_new_data_both_having_different_attibutes(
            self.spark,old_scd, new_records, self.attributes_cols, 'date', 'store_number', self.final_scd_schema)

        first_record = merged_scd.where(F.col('start_date') == F.lit('2024-11-27')).collect()[0]
        second_record = merged_scd.where(F.col('start_date') == F.lit('2025-1-11')).collect()[0]        

        check.equal(first_record['store_number'], 2502)
        check.equal(first_record['store_name'], 'HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1')
        check.equal(first_record['zip_code'], 50021)
        check.equal(first_record['store_location'], 'POINT(-93.602561976 41.73460601)')
        check.equal(first_record['address'], '410 NORTH ANKENY BLVD')
        check.equal(first_record['start_date'], datetime.date(2024, 11, 27))
        check.equal(first_record['end_date'], datetime.date(2025, 1, 11))
        check.equal(first_record['is_current'], False)
        
        check.equal(second_record['store_number'], 2502)
        check.equal(second_record['store_name'], 'HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 2')
        check.equal(second_record['zip_code'], 50021)
        check.equal(second_record['store_location'], 'POINT(-93.602561976 41.73460601)')
        check.equal(second_record['address'], '410 NORTH ANKENY BLVD')
        check.equal(second_record['start_date'], datetime.date(2025, 1, 11))
        check.equal(second_record['end_date'], None)
        check.equal(second_record['is_current'], True)

    