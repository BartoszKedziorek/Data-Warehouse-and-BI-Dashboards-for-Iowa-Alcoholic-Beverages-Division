from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StructType
from typing import List
from copy import copy


def create_scd_from_input(spark: SparkSession, input_df: DataFrame, attributes_cols: List[str],
                           date_col: str, natural_key_column: str, final_scd_schema: StructType) -> DataFrame: 

    only_start_scd_df = spark.createDataFrame([], schema=final_scd_schema)
    min_date_agg_df = input_df.groupBy(attributes_cols).agg(F.min(date_col).alias('min_date1'))

    while min_date_agg_df.count() != 0:
        
        merged_scd = min_date_agg_df.join(
            other=min_date_agg_df.groupBy(natural_key_column).agg(F.min('min_date1').alias('min_date2')),
            on=natural_key_column,
            how='inner'
        )
        to_add = merged_scd.where('min_date1 = min_date2') 
        only_start_scd_df = only_start_scd_df.union(
            to_add.where('min_date1 = min_date2')
            .drop('min_date2')
            .withColumnRenamed('min_date1','start_date')
            .withColumn('end_date', F.lit(None).cast(DateType()))
            .withColumn('is_current', F.lit(False))
        )
        merged_scd = merged_scd.where('min_date1 != min_date2')

        select_cols = [merged_scd[col] for col in attributes_cols]
        select_cols.append('min_date1')

        min_date_agg_df = merged_scd.select(*select_cols)
        

    final_scd_df = spark.createDataFrame([], schema=final_scd_schema)
    first_iteration = True

    while only_start_scd_df.count() != 0:
        max_start_date = only_start_scd_df.groupBy(natural_key_column).agg(F.max('start_date').alias('max_start_date'))

        second_max_start_date = \
            only_start_scd_df.join(max_start_date, natural_key_column, how='inner') \
                .where('start_date != max_start_date') \
                .groupBy(natural_key_column) \
                .agg(F.max('start_date').alias('second_max_date'))
        
        if first_iteration == True:
            add_to_scd = \
                only_start_scd_df.join(max_start_date, on=natural_key_column, how='inner') \
                .where('max_start_date == start_date') \
                .select(*([only_start_scd_df[col] for col in attributes_cols] +
                         [only_start_scd_df['start_date'], only_start_scd_df['end_date'], only_start_scd_df['is_current']])) \
                .withColumn('is_current', F.lit(True))

            final_scd_df = final_scd_df.union(add_to_scd)

            first_iteration = False
        

        compare_dates_df = only_start_scd_df.join(
            max_start_date, on=natural_key_column, how='inner'
        ).join(
            second_max_start_date, on=natural_key_column, how='inner'
        ).where(
            'start_date == second_max_date'
        )

        compare_dates_df = compare_dates_df.drop('end_date').withColumnRenamed('max_start_date', 'end_date')

        final_scd_df = final_scd_df.union(
            compare_dates_df.select(*([col for col in attributes_cols] + ['start_date', 'end_date', 'is_current']))
        )

        only_start_scd_df = only_start_scd_df.join(
            max_start_date, on=natural_key_column, how='inner'
        ).where(
            'start_date != max_start_date'
        ).drop(
            'max_start_date'
        )

    return final_scd_df


def get_oldest_records_from_scd(scd_df: DataFrame, attributes_cols_without_natural_key:str,
                                 natural_key_column: str) -> DataFrame:
    
    scd_min_start_date = scd_df.groupBy(natural_key_column).agg(F.min('start_date').alias('start_date'))

    oldest_records_from_scd_df = scd_df.alias('nn') \
                                .join(scd_min_start_date.alias('nsm'),
                                      (scd_min_start_date['start_date'] == scd_df['start_date']) &
                                       (scd_min_start_date[natural_key_column] == scd_df[natural_key_column]),
                                       how='inner') \
                                .select(*([scd_df[col] for col in attributes_cols_without_natural_key] + [scd_df['start_date'], scd_df['end_date'],scd_df['is_current'], f'nn.{natural_key_column}']))
    
    return oldest_records_from_scd_df


# W przypadku kiedy mamy nowy rekord z inną wartością i jest tylko jeden
def merge_last_scd_record_with_oldest_scd_record_from_new_data_both_having_different_attibutes(
        spark: SparkSession, old_scd: DataFrame,
          new_records: DataFrame, attributes_cols: List[str],
          date_col: str, natural_key_column: str, final_scd_schema: StructType) -> DataFrame:
    
    new_scd = create_scd_from_input(spark, new_records, attributes_cols, date_col,
                                     natural_key_column, final_scd_schema)

    current_records_old_scd = old_scd.where("is_current == TRUE")
    
    attributes_cols_without_natural_key = copy(attributes_cols)
    attributes_cols_without_natural_key.remove(natural_key_column)
    
    oldest_records_from_new_scd = get_oldest_records_from_scd(new_scd, attributes_cols_without_natural_key, natural_key_column)
    
    merege_for_update_df = current_records_old_scd.join(oldest_records_from_new_scd, on=natural_key_column, how='inner')

    merege_for_update_df = merege_for_update_df.drop(current_records_old_scd['end_date']) \
                                                .withColumn('end_date', oldest_records_from_new_scd['start_date']) \
                                                .drop(current_records_old_scd['is_current']) \
                                                .withColumn('is_current', F.lit(False)) \
                                                .select(*(['is_current', 'end_date', current_records_old_scd['start_date'], current_records_old_scd[natural_key_column]] +
                                                           [current_records_old_scd[col] for col in attributes_cols_without_natural_key]))

    merge_for_insert = merege_for_update_df.unionByName(oldest_records_from_new_scd) 

    return merge_for_insert   



def merge_last_scd_record_with_scd_records_from_new_data_both_having_different_attibutes(
        spark: SparkSession, old_scd: DataFrame,
          new_records: DataFrame, attributes_cols: List[str],
          date_col: str, natural_key_column: str, final_scd_schema: StructType) -> DataFrame:
    
    new_scd = create_scd_from_input(spark, new_records, attributes_cols, date_col,
                                     natural_key_column, final_scd_schema)

    attributes_cols_without_natural_key = copy(attributes_cols)
    attributes_cols_without_natural_key.remove(natural_key_column)

    oldest_records_from_new_scd = get_oldest_records_from_scd(new_scd, attributes_cols_without_natural_key, natural_key_column)

    current_records_old_scd = old_scd.where("is_current == TRUE")

    merege_for_update_df = current_records_old_scd.join(oldest_records_from_new_scd, on=natural_key_column, how='inner')

    merege_for_update_df = merege_for_update_df.drop(current_records_old_scd['end_date']) \
                                                .withColumn('end_date', oldest_records_from_new_scd['start_date']) \
                                                .drop(current_records_old_scd['is_current']) \
                                                .withColumn('is_current', F.lit(False)) \
                                                .select(*(['is_current', 'end_date', current_records_old_scd['start_date'], current_records_old_scd[natural_key_column]] +
                                                           [current_records_old_scd[col] for col in attributes_cols_without_natural_key]))

    #not_added_from_new_scd = new_scd.exceptAll(oldest_records_from_new_scd)

    merge_for_insert_df = merege_for_update_df.unionByName(new_scd)

    return merge_for_insert_df


# najstarszy rekord z nowego scd ma takie same wartości atrybutów jak najnowszy ze startego scd 
def merge_last_scd_record_with_scd_records_from_new_data_both_having_same_attibutes(
        spark: SparkSession, old_scd: DataFrame,
          new_records: DataFrame, attributes_cols: List[str],
          date_col: str, natural_key_column: str, final_scd_schema: StructType) -> DataFrame:
    
    new_scd = create_scd_from_input(spark, new_records, attributes_cols, date_col,
                                     natural_key_column, final_scd_schema)
    
    attributes_cols_without_natural_key = copy(attributes_cols)
    attributes_cols_without_natural_key.remove(natural_key_column)

    oldest_records_from_new_scd = get_oldest_records_from_scd(new_scd, attributes_cols_without_natural_key, natural_key_column)

    current_records_old_scd = old_scd.where("is_current == TRUE")

    

    


    pass