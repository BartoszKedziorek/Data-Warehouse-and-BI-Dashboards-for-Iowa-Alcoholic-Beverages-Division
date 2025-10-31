import pytest
from pyspark.sql import SparkSession
from scripts.modules.scd import create_scd_from_input, \
                                merge_last_scd_record_with_oldest_scd_record_from_new_data_both_having_different_attibutes, \
                                get_oldest_records_from_scd, \
                                merge_last_scd_record_with_scd_records_from_new_data_both_having_different_attibutes, \
                               merge_last_scd_record_with_scd_records_from_new_data_both_having_same_attibutes
from pyspark.sql.types import StructType, StructField, DateType, IntegerType, StringType, BooleanType, Row
import datetime
import pytest_check as check
import pyspark.sql.functions as F
from typing import List
from copy import copy
import datetime
from scripts.modules.scd import load_update_entries
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine



class TestSCD:
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

        attributes_cols_without_natural_key = copy(cls.attributes_cols)
        attributes_cols_without_natural_key.remove('store_number')
        cls.attributes_cols_without_natural_key = attributes_cols_without_natural_key

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

        cls.test_db_connection_url_pyodbc = os.environ.get('TEST_DB_CONNECTION_URL_PYODBC')
        cls.test_db_connection_url_jdbc = os.environ.get('TEST_DB_CONNECTION_URL_JDBC')
        
        username = os.environ.get('TEST_DB_USER_NAME')
        password = os.environ.get('TEST_DB_USER_PASSWORD')

        cls.conn_properties  = {
            "user": f"{username}",
            "password": f"{password}", 
            "trustServerCertificate": "true",
        }

        cls.test_db_engine: Engine = create_engine(cls.test_db_connection_url_pyodbc,connect_args=cls.conn_properties)


    @classmethod
    def teardown_class(cls):
        cls.spark.stop()

    def setup_method(self, method):
        with self.test_db_engine.begin() as cnx:
            cnx.execute(text("DELETE FROM dbo.DimStore"))
        # self.test_db_engine.connect().execute(text("DELETE FROM dbo.DimStore"))

        scd = self.spark.createDataFrame([
            Row(StoreNumberDK=2502, StoreName='HY-VEE WINE AND SPIRITS (1022) / ANKENY',
                address='410 NORTH ANKENY BLVD', City='ANKENY',
                ZipCode=50021, StoreLocation ='POINT(-93.602561976 41.73460601)',
                StartDate=datetime.date(2024, 11, 12),EndDate=datetime.date(2024, 11, 27), IsCurrent=False),
            Row(StoreNumberDK=2502, StoreName='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1',
                address='410 NORTH ANKENY BLVD', City='ANKENY',
                ZipCode=50021, StoreLocation ='POINT(-93.602561976 41.73460601)',
                StartDate=datetime.date(2024, 11, 27), EndDate=None, IsCurrent=True),
            Row(StoreNumberDK=4970, StoreName='JEFF\'S MARKET / WEST LIBERTY',
                address='200, E 3RD ST', City='WEST LIBERTY',
                ZipCode=52776, StoreLocation ='POINT(-91.261560959 41.569567007)',
                StartDate=datetime.date(2025, 1, 11), EndDate=None, IsCurrent=True),
            Row(StoreNumberDK=1234, StoreName='Test shop 123',
                address='10 NORTH IOWA CITY BLVD', City='IOWA CITY',
                ZipCode=12345, StoreLocation ='POINT(-93.602561976 41.73460601)',
                StartDate=datetime.date(2025, 1, 15), EndDate=None, IsCurrent=True),
        ]) 

        long_col = F.expr("CASE WHEN StoreLocation != 'POINT EMPTY' THEN substring(split(StoreLocation, ' ')[0], 7, length(split(StoreLocation, ' ')[0]) - 5)" +
                  "ELSE '-1.0' END")
        lat_col = F.expr("CASE WHEN StoreLocation != 'POINT EMPTY' THEN substring(split(StoreLocation, ' ')[1], 0, length(split(StoreLocation, ' ')[1]) - 1)" + 
                        "ELSE '-1.0' END")

        scd = scd.withColumn('StoreLocationLongitude', long_col).withColumn('StoreLocationLatitude', lat_col)

        scd.write.jdbc(url=self.test_db_connection_url_jdbc, table='DimStore', mode='append', properties=self.conn_properties)


    def teardown_method(self, method):
        with self.test_db_engine.begin() as cnx:
            cnx.execute(text("DELETE FROM dbo.DimStore"))


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

        df = create_scd_from_input(input_df, self.attributes_cols,
                                    'date', 'store_number')
        df.count()
        df.cache()

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

        new_scd = self.spark.createDataFrame([
            Row(
                store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 2',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2024, 12, 12), end_date=None, is_current=True
                )
        ],
            schema=old_scd.schema
        )

        merged_scd = merge_last_scd_record_with_oldest_scd_record_from_new_data_both_having_different_attibutes(
            self.spark, old_scd, new_scd, self.attributes_cols, 'store_number')

        first_record = merged_scd.where(F.col('start_date') == F.lit('2024-11-27')).collect()[0]
        second_record = merged_scd.where(F.col('start_date') == F.lit('2024-12-12')).collect()[0]        

        check.equal(first_record['store_number'], 2502)
        check.equal(first_record['store_name'], 'HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1')
        check.equal(first_record['zip_code'], 50021)
        check.equal(first_record['store_location'], 'POINT(-93.602561976 41.73460601)')
        check.equal(first_record['address'], '410 NORTH ANKENY BLVD')
        check.equal(first_record['start_date'], datetime.date(2024, 11, 27))
        check.equal(first_record['end_date'], datetime.date(2024, 12, 12))
        check.equal(first_record['is_current'], False)
        
        check.equal(second_record['store_number'], 2502)
        check.equal(second_record['store_name'], 'HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 2')
        check.equal(second_record['zip_code'], 50021)
        check.equal(second_record['store_location'], 'POINT(-93.602561976 41.73460601)')
        check.equal(second_record['address'], '410 NORTH ANKENY BLVD')
        check.equal(second_record['start_date'], datetime.date(2024, 12, 12))
        check.equal(second_record['end_date'], None)
        check.equal(second_record['is_current'], True)

    
    def test_get_oldest_records_from_scd(self):
        scd = self.spark.createDataFrame([
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2024, 11, 12),end_date=datetime.date(2024, 11, 27), is_current=False),
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2024, 11, 27), end_date=None, is_current=True)
        ])

        oldedst_records = get_oldest_records_from_scd(scd, self.attributes_cols_without_natural_key, 'store_number')

        record = oldedst_records.where(F.col('start_date') == F.lit('2024-11-12')).collect()[0]

        check.equal(record['store_number'], 2502)
        check.equal(record['store_name'], 'HY-VEE WINE AND SPIRITS (1022) / ANKENY')
        check.equal(record['zip_code'], 50021)
        check.equal(record['store_location'], 'POINT(-93.602561976 41.73460601)')
        check.equal(record['address'], '410 NORTH ANKENY BLVD')
        check.equal(record['start_date'], datetime.date(2024, 11, 12))
        check.equal(record['end_date'], datetime.date(2024, 11, 27))
        check.equal(record['is_current'], False)



    # zmiana wartości atrybutów, w nowym scd jest dla sklepu więcej niż jedno entry
    def test_merge_last_scd_record_with_scd_records_from_new_data_both_having_different_attibutes__one_store(self):
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

        new_scd = self.spark.createDataFrame([
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 2',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2025, 1, 11), end_date=datetime.date(2025, 1, 17), is_current=False
                ),
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 3',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2025, 1, 17), end_date=datetime.date(2025, 2, 1), is_current=False
                ),
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 4',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2025, 2, 1), end_date=None, is_current=True
                ),
        ],
            schema=old_scd.schema
        )

        merged_scd = merge_last_scd_record_with_scd_records_from_new_data_both_having_different_attibutes(
            self.spark,old_scd, new_scd, self.attributes_cols, 'store_number')
        
        first_record = merged_scd.where(F.col('start_date') == F.lit('2024-11-27')).collect()[0]
        merged_scd.cache()
        second_record = merged_scd.where(F.col('start_date') == F.lit('2025-1-11')).collect()[0]
        fourth_record = merged_scd.where(F.col('start_date') == F.lit('2025-2-1')).collect()[0]

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
        check.equal(second_record['end_date'], datetime.date(2025, 1, 17))
        check.equal(second_record['is_current'], False)

        check.equal(fourth_record['store_number'], 2502)
        check.equal(fourth_record['store_name'], 'HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 4')
        check.equal(fourth_record['zip_code'], 50021)
        check.equal(fourth_record['store_location'], 'POINT(-93.602561976 41.73460601)')
        check.equal(fourth_record['address'], '410 NORTH ANKENY BLVD')
        check.equal(fourth_record['start_date'], datetime.date(2025, 2, 1))
        check.equal(fourth_record['end_date'], None)
        check.equal(fourth_record['is_current'], True)


    def test_merge_last_scd_record_with_scd_records_from_new_data_both_having_same_attibutes(self):
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

        new_scd = self.spark.createDataFrame([
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2025, 1, 11), end_date=datetime.date(2025, 1, 17), is_current=False
                ),
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 2',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2025, 1, 17), end_date=datetime.date(2025, 2, 1), is_current=False
                ),
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 3',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2025, 2, 1), end_date=None, is_current=True
                )
        ],
            schema=old_scd.schema
        )

        merged_scd = merge_last_scd_record_with_scd_records_from_new_data_both_having_same_attibutes(
            self.spark,old_scd, new_scd, self.attributes_cols, 'store_number')
        
    
        first_scd_record = merged_scd.where(F.col('start_date') == F.to_date(F.lit('2024-11-27'))).collect()[0]
        merged_scd.cache()
        second_scd_record = merged_scd.where(F.col('start_date') == F.to_date(F.lit('2025-1-17'))).collect()[0]
        third_scd_record = merged_scd.where(F.col('start_date') == F.lit('2025-2-1')).collect()[0]

        check.equal(first_scd_record['store_number'], 2502)
        check.equal(first_scd_record['store_name'], 'HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1')
        check.equal(first_scd_record['zip_code'], 50021)
        check.equal(first_scd_record['store_location'], 'POINT(-93.602561976 41.73460601)')
        check.equal(first_scd_record['address'], '410 NORTH ANKENY BLVD')
        check.equal(first_scd_record['start_date'], datetime.date(2024, 11, 27))
        check.equal(first_scd_record['end_date'], datetime.date(2025, 1, 17))
        check.equal(first_scd_record['is_current'], False)
        
        check.equal(second_scd_record['store_number'], 2502)
        check.equal(second_scd_record['store_name'], 'HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 2')
        check.equal(second_scd_record['zip_code'], 50021)
        check.equal(second_scd_record['store_location'], 'POINT(-93.602561976 41.73460601)')
        check.equal(second_scd_record['address'], '410 NORTH ANKENY BLVD')
        check.equal(second_scd_record['start_date'], datetime.date(2025, 1, 17))
        check.equal(second_scd_record['end_date'], datetime.date(2025, 2, 1))
        check.equal(second_scd_record['is_current'], False)

        check.equal(third_scd_record['store_number'], 2502)
        check.equal(third_scd_record['store_name'], 'HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 3')
        check.equal(third_scd_record['zip_code'], 50021)
        check.equal(third_scd_record['store_location'], 'POINT(-93.602561976 41.73460601)')
        check.equal(third_scd_record['address'], '410 NORTH ANKENY BLVD')
        check.equal(third_scd_record['start_date'], datetime.date(2025, 2, 1))
        check.equal(third_scd_record['end_date'], None)
        check.equal(third_scd_record['is_current'], True)      
        

    def test_load_update_entries_with_same_attributest(self):
        scd_update_records = self.spark.createDataFrame([
            Row(store_number=2502, store_name='HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1',
                address='410 NORTH ANKENY BLVD', city='ANKENY',
                zip_code=50021, store_location='POINT(-93.602561976 41.73460601)',
                start_date=datetime.date(2024, 11, 29), end_date=datetime.date(2024, 12, 22), is_current=False),
            Row(store_number=4970, store_name='JEFF\'S MARKET / WEST LIBERTY',
                address='200, E 3RD ST', city='WEST LIBERTY',
                zip_code=52776, store_location='POINT(-91.261560959 41.569567007)',
                start_date=datetime.date(2025, 1, 13), end_date=datetime.date(2025, 1, 29), is_current=False),
        ])

        load_update_entries(self.test_db_engine, scd_update_records, 'store_number',
                                                 'StoreNumberDK', 'DimStore')

        scd = self.spark.read.jdbc(url=self.test_db_connection_url_jdbc,
                                    table="""(SELECT CAST([StoreLocation] AS VARCHAR(255)) as [StoreLocation], 
                                            StoreNumberDK, StoreName, City, Address, ZipCode,
                                            StoreLocationLongitude, StoreLocationLatitude,
                                            StartDate, EndDate, IsCurrent
                                             FROM [Iowa_Sales_Data_Warehouse_test].[dbo].[DimStore]) as [DimStore]""",
                                              properties=self.conn_properties)
        scd = scd.withColumn("StoreLocation", F.expr("substring(StoreLocation, 7, length(StoreLocation))"))

        first_store_last_entry = scd.where('StoreNumberDK = 2502 AND StartDate = \'2024-11-27\'').collect()[0]
        scd.cache()
        second_store_last_entry = scd.where('StoreNumberDK = 4970 AND StartDate = \'2025-1-11\'').collect()[0]
        third_store_last_entry = scd.where('StoreNumberDK = 1234 AND StartDate = \'2025-1-15\'').collect()[0]

        check.equal(first_store_last_entry['StoreName'], 'HY-VEE WINE AND SPIRITS (1022) / ANKENY CHANGE 1')
        check.equal(first_store_last_entry['Address'], '410 NORTH ANKENY BLVD')
        check.equal(first_store_last_entry['ZipCode'], 50021)
        check.equal(first_store_last_entry['City'], 'ANKENY')
        check.equal(first_store_last_entry['StoreLocation'], '(-93.602561976 41.73460601)')
        check.equal(first_store_last_entry['StartDate'], datetime.date(2024, 11, 27))
        check.equal(first_store_last_entry['EndDate'], datetime.date(2024, 12, 22))
        check.equal(first_store_last_entry['IsCurrent'], False)
        
        check.equal(second_store_last_entry['StoreName'], 'JEFF\'S MARKET / WEST LIBERTY')
        check.equal(second_store_last_entry['Address'], '200, E 3RD ST')
        check.equal(second_store_last_entry['ZipCode'], 52776)
        check.equal(second_store_last_entry['City'], 'WEST LIBERTY')
        check.equal(second_store_last_entry['StoreLocation'], '(-91.261560959 41.569567007)')
        check.equal(second_store_last_entry['StartDate'], datetime.date(2025, 1, 11))
        check.equal(second_store_last_entry['EndDate'], datetime.date(2025, 1, 29))
        check.equal(second_store_last_entry['IsCurrent'], False)

        check.equal(third_store_last_entry['StoreName'], 'Test shop 123')
        check.equal(third_store_last_entry['Address'], '10 NORTH IOWA CITY BLVD')
        check.equal(third_store_last_entry['ZipCode'], 12345)
        check.equal(third_store_last_entry['City'], 'IOWA CITY')
        check.equal(third_store_last_entry['StoreLocation'], '(-93.602561976 41.73460601)')
        check.equal(third_store_last_entry['StartDate'], datetime.date(2025, 1, 15))
        check.equal(third_store_last_entry['EndDate'], None)
        check.equal(third_store_last_entry['IsCurrent'], True)

