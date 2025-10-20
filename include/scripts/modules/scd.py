from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StructType
from typing import List
from copy import copy
from scripts.modules.utils import replace_attributes_with_hash, replace_hash_with_attributes
from pyspark.sql.window import Window

def create_scd_from_input(spark: SparkSession, input_df: DataFrame, attributes_cols: List[str],
                           date_col: str, natural_key_column: str, final_scd_schema: StructType) -> DataFrame:
    
    attributes_cols_without_natural_key = copy(attributes_cols)
    attributes_cols_without_natural_key.remove(natural_key_column)

    input_df_with_hashed_attributes, hash_match_df = replace_attributes_with_hash(input_df, attributes_cols_without_natural_key)

    min_date_agg_df = input_df_with_hashed_attributes.groupBy(natural_key_column, 'hashed_attributes_value') \
                                                    .agg(F.min(date_col).alias('min_date1'))

    windowSpec = Window.partitionBy(natural_key_column).orderBy(F.col("min_date1").asc())

    min_date_agg_df_with_seq_number_column = min_date_agg_df.withColumn('sequential_number', F.row_number().over(windowSpec)) # ponumerowanie rekordów dla
        # każdego skelpu to jest tabela dla left join-a

    min_date_agg_by_natural_key_df = input_df_with_hashed_attributes.groupBy(natural_key_column).agg(F.min(date_col).alias('min_date2'))

    min_date_agg_by_nk_and_attribs_without_oldest_records = min_date_agg_df.alias('md').join(
                                                            min_date_agg_by_natural_key_df.alias('mdnk'),
                                                            on=natural_key_column,
                                                            how='inner') \
                                                            .where('md.min_date1 != mdnk.min_date2') \
                                                            .select(
                                                                F.col(f'md.{natural_key_column}').alias(natural_key_column),
                                                                F.col(f'md.hashed_attributes_value').alias('hashed_attributes_value'),
                                                                F.col(f'md.min_date1').alias('start_date')
                                                            )
    
    windowSpec = Window.partitionBy(natural_key_column).orderBy(F.col("start_date").asc())

    min_date_agg_by_nk_and_attribs_without_oldest_records_with_seq_number = min_date_agg_by_nk_and_attribs_without_oldest_records \
                                        .withColumn(
                                            'sequential_number',
                                            F.row_number().over(windowSpec)
                                        )

    raw_final_scd = min_date_agg_df_with_seq_number_column.alias('a').join(
            min_date_agg_by_nk_and_attribs_without_oldest_records_with_seq_number.alias('b'),
            (F.col(f'a.{natural_key_column}') == F.col(f'b.{natural_key_column}')) 
            &
            (F.col('a.sequential_number') == F.col('b.sequential_number')),
            how='left'
    ).select(
        F.col(f'a.{natural_key_column}').alias(natural_key_column),
        F.col(f'a.hashed_attributes_value').alias('hashed_attributes_value'),
        F.col('a.min_date1').alias('start_date'),
        F.col('b.start_date').alias('end_date')
    )

    raw_final_scd = replace_hash_with_attributes(raw_final_scd, hash_match_df)

    final_scd = raw_final_scd.withColumn('is_current', F.when(F.col('end_date').isNull(), True).otherwise(False))

    return final_scd # nieprzetestowane napisałem kod na brudno



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

    # new_scd_for_merge = new_scd.exceptAll(oldest_records_from_new_scd)

    new_scd_for_merge = new_scd.alias('nn').join(oldest_records_from_new_scd.alias('old'), on=natural_key_column, how='inner') \
                                .where(F.col('nn.start_date') != F.col('old.start_date')) \
                                .select(*([F.col(f"nn.{col}") for col in attributes_cols] + [F.col('nn.start_date'), F.col('nn.is_current'), F.col('nn.end_date')]))

    new_scd_for_merge.cache()

    second_oldest_records_from_new_scd = get_oldest_records_from_scd(new_scd_for_merge, attributes_cols_without_natural_key, natural_key_column)

    merege_for_update_df = current_records_old_scd.join(second_oldest_records_from_new_scd, on=natural_key_column, how='inner')

    merege_for_update_df = merege_for_update_df.drop(current_records_old_scd['end_date']) \
                                                .withColumn('end_date', second_oldest_records_from_new_scd['start_date']) \
                                               .drop(current_records_old_scd['is_current']) \
                                               .withColumn('is_current', F.lit(False)) \
                                               .select(*(['is_current', 'end_date', current_records_old_scd['start_date'], current_records_old_scd[natural_key_column]] +
                                                          [current_records_old_scd[col] for col in attributes_cols_without_natural_key]))
    merege_for_update_df.cache()

    merege_for_update_df = merege_for_update_df \
                        .select(sorted(merege_for_update_df.columns)) \
                        .union(new_scd_for_merge.select(sorted(new_scd_for_merge.columns)))

    return merege_for_update_df