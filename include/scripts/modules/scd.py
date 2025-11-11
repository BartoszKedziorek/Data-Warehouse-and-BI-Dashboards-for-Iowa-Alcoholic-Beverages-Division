from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StructType
from typing import List, Tuple, Dict
from copy import copy
from scripts.modules.utils import replace_attributes_with_hash, replace_hash_with_attributes
from pyspark.sql.window import Window
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

def create_scd_from_input(input_df: DataFrame, attributes_cols: List[str],
                           date_col: str, natural_key_column: str) -> DataFrame:
    
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
         old_scd: DataFrame, new_scd: DataFrame,
           attributes_cols: List[str], natural_key_column: str,
          split_result: bool = False) -> DataFrame | Tuple[DataFrame, DataFrame]:

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
    
    if split_result:
        return merege_for_update_df, oldest_records_from_new_scd

    merge_for_insert = merege_for_update_df.unionByName(oldest_records_from_new_scd) 

    return merge_for_insert   



def merge_last_scd_record_with_scd_records_from_new_data_both_having_different_attibutes(
        old_scd: DataFrame, new_scd: DataFrame,
          attributes_cols: List[str], natural_key_column: str,
            split_result: bool = False) -> DataFrame:

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
    if split_result:
        return merege_for_update_df, new_scd


    merge_for_insert_df = merege_for_update_df.unionByName(new_scd)

    return merge_for_insert_df


# najstarszy rekord z nowego scd ma takie same wartości atrybutów jak najnowszy ze startego scd 
def merge_last_scd_record_with_scd_records_from_new_data_both_having_same_attibutes(
        old_scd: DataFrame,
          new_scd: DataFrame, attributes_cols: List[str],
          natural_key_column: str,
          split_result: bool = False) -> DataFrame | Tuple[DataFrame, DataFrame]:
    
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
    
    merege_for_update_df = merege_for_update_df.select(sorted(merege_for_update_df.columns))
    new_scd_for_merge = new_scd_for_merge.select(sorted(new_scd_for_merge.columns))

    if split_result:
        return merege_for_update_df, new_scd_for_merge
    
    merege_for_update_df.cache()

    merege_for_update_df = merege_for_update_df \
                        .union(new_scd_for_merge)

    return merege_for_update_df



def load_update_entries(engine: Engine, scd_entries: DataFrame,
                         natural_key_new_entries: str, natural_key_database: str, table_name: str):
    
    iter_entries = scd_entries.select(F.col(natural_key_new_entries).alias(natural_key_new_entries),
                                      F.col('start_date').alias('start_date'),
                                      F.col('end_date').alias('end_date'))
    
    iter_entries = iter_entries.collect()


    query = """
        UPDATE {table}
        SET EndDate = '{end_date_value}', IsCurrent = CAST(0 as bit) 
        WHERE """ + \
        natural_key_database + \
        """
            = {natural_key_value} AND
            IsCurrent = CAST(1 as bit)   
        """
    
    with engine.begin() as cnx:
        for entry in iter_entries:
            cnx.execute(text(query.format(table=table_name, end_date_value=entry['end_date'],
                        natural_key_value=entry[natural_key_new_entries])))



def filter_scd_by_natural_key(scd: DataFrame, natural_keys_dataframe: DataFrame, natural_key: str):
    return scd.alias('t1').join(natural_keys_dataframe.alias('t2'), on=natural_key, how='inner') \
                           .select(F.col('t1.hashed_attributes_value').alias('hashed_attributes_value'),
                                   F.col(f't1.{natural_key}').alias(natural_key),
                                   F.col('t1.start_date').alias('start_date'),
                                   F.col('t1.end_date').alias('end_date'),
                                   F.col('t1.is_current').alias('is_current'))


def get_scd_records_for_update_and_insert(old_scd: DataFrame, new_records: DataFrame,
                                           natural_key_col: str, date_col: str,
                                           attributes_cols: List[str]) -> Tuple[DataFrame, DataFrame]:
    artificial_old_records = old_scd.select(*(attributes_cols + ['start_date']))
    artificial_old_records = artificial_old_records.withColumnRenamed('start_date', date_col)
 
    new_records = new_records.select(*(attributes_cols + [date_col]))

    all_records = artificial_old_records.unionByName(new_records)

    new_scd = create_scd_from_input(all_records, attributes_cols, date_col, natural_key_col)

    old_scd_last_records = old_scd.where('is_current == TRUE')

    to_update = old_scd_last_records.alias('o').join(
        new_scd.alias('n'),
        on=(F.col('o.start_date') == F.col('n.start_date'))
        &
        (F.col(f'o.{natural_key_col}') == F.col(f'n.{natural_key_col}')),
        how = "inner"
    ).where('n.end_date IS NOT NULL') \
    .select(
        *[F.col(f"n.{col}").alias(col) for col in new_scd.columns]
    )

    to_insert = new_scd.alias('n').join(
        old_scd.alias('o'),
        on=(F.col('o.start_date') == F.col('n.start_date'))
            &
           (F.col(f'o.{natural_key_col}') == F.col(f'n.{natural_key_col}')),
        how='leftanti'
    ).select(
        *[F.col(f"n.{col}").alias(col) for col in new_scd.columns]
    )

    return to_update, to_insert


# def get_scd_records_for_update_and_insert(spark: SparkSession, input_records: DataFrame, attrib_columns: List[str], natural_key_col: str,
#                        jdbc_url: str, jdbc_conn_properties: Dict[str, str],
#                          df_to_database_columns_mapping: Dict[str, str],
#                        dim_table_name_or_select_query: str) -> Tuple[DataFrame, DataFrame]:
#     new_scd_reords_df = create_scd_from_input(input_records, attrib_columns,
#                                      'date', natural_key_col)
    
#     attrib_columns_without_natural_key = copy(attrib_columns)
#     attrib_columns_without_natural_key.remove(natural_key_col)

#     new_scd_reords_df, hash_table_new_scd = replace_attributes_with_hash(new_scd_reords_df, attrib_columns_without_natural_key)

#     old_scd = spark.read.jdbc(url=jdbc_url, table=dim_table_name_or_select_query, properties=jdbc_conn_properties)
    
#     reverse_mapping = { old_value:old_key for old_key, old_value
#                         in df_to_database_columns_mapping.items()}
#     old_scd = old_scd.withColumnsRenamed(reverse_mapping)

#     # old_scd_last_records = old_scd.alias('os').join(
#     #                                 old_scd.groupBy(natural_key_col)
#     #                                         .agg(F.max('start_date').alias('start_date'))
#     #                                         .alias('os_max'),
#     #                                 on=(F.col(f'os.{natural_key_col}') == F.col(f'os_max.{natural_key_col}'))
#     #                                     &
#     #                                 (F.col('os.start_date') == F.col('os_max.start_date')),
#     #                                 how='inner'
#     #                             ).select(
#     #                                 *([F.col(f"os.{col_name}").alias(col_name) for col_name in attrib_columns_without_natural_key] +
#     #                                   [F.col(f"os.{natural_key_col}").alias(natural_key_col)] +
#     #                                   [F.col("os.end_date").alias('end_date'), F.col('os.start_date').alias('start_date'),
#     #                                    F.col('os.is_current').alias('is_current')] )
#     #                             )

#     old_scd_last_records = old_scd.where("is_current == TRUE")


#     old_scd_last_records, _ = replace_attributes_with_hash(old_scd_last_records, attrib_columns_without_natural_key)

#     new_scd_joined_old_scd = old_scd_last_records.alias('os') \
#                                                 .join(
#                                                     new_scd_reords_df.alias('ns'),
#                                                     on=natural_key_col
#                                                 )

#     new_scd_joined_old_scd.cache()

#     new_scd_reords_df_minus_lastest_entries_from_old_scd = new_scd_joined_old_scd.where(
#                                                                     'os.hashed_attributes_value != ns.hashed_attributes_value'
#                                                                 ).select(
#                                                                     F.col(f'ns.{natural_key_col}').alias(natural_key_col),
#                                                                     F.col('ns.start_date').alias('start_date'),
#                                                                     F.col('ns.end_date').alias('end_date'),
#                                                                     F.col('ns.is_current').alias('is_current'),
#                                                                     F.col('ns.hashed_attributes_value').alias('hashed_attributes_value')
#                                                                 )
                                

#     entries_count_compare_by_store_number = new_scd_reords_df_minus_lastest_entries_from_old_scd.alias('nsm') \
#                                                 .groupBy(natural_key_col).agg(F.count('*').alias('ct_1')) \
#                                                 .join(
#                                                     new_scd_reords_df.alias('ns') \
#                                                         .groupBy(natural_key_col)
#                                                         .agg(F.count('*').alias('ct_2')),
#                                                     on=natural_key_col,
#                                                     how='inner'
#                                                 )

#     entries_count_compare_by_store_number.cache()

#     stores_with_only_new_entries = entries_count_compare_by_store_number.where('ct_1 == ct_2') \
#                                                                     .select(F.col(f'ns.{natural_key_col}').alias(natural_key_col),
#                                                                             F.col('ct_2').alias('ct'))
#     stores_with_only_new_entries.cache()


#     stores_with_only_one_new_entry = stores_with_only_new_entries.where('ct == 1').select(natural_key_col)
#     stores_with_only_more_new_entries = stores_with_only_new_entries.where('ct != 1').select(natural_key_col)


#     stores_with_not_only_new_entries = entries_count_compare_by_store_number.where('ct_1 != ct_2 AND ct_1 != 0') \
#                                                                         .select(F.col(f'ns.{natural_key_col}').alias(natural_key_col))

#     scd_entries_with_only_one_new_entry = filter_scd_by_natural_key(new_scd_reords_df, stores_with_only_one_new_entry, natural_key_col)
#     scd_entries_with_only_more_new_entries = filter_scd_by_natural_key(new_scd_reords_df, stores_with_only_more_new_entries, natural_key_col)
#     scd_entries_with_not_only_new_entries = filter_scd_by_natural_key(new_scd_reords_df, stores_with_not_only_new_entries, natural_key_col)

#     scd_entries_with_only_one_new_entry = replace_hash_with_attributes(scd_entries_with_only_one_new_entry, hash_table_new_scd)
#     scd_entries_with_only_more_new_entries = replace_hash_with_attributes(scd_entries_with_only_more_new_entries, hash_table_new_scd)
#     scd_entries_with_not_only_new_entries = replace_hash_with_attributes(scd_entries_with_not_only_new_entries, hash_table_new_scd)


#     # to_update1, to_insert1 = merge_last_scd_record_with_scd_records_from_new_data_both_having_same_attibutes(
#     #     old_scd, scd_entries_with_not_only_new_entries,
#     #     attrib_columns, natural_key_col, split_result=True
#     # )

#     to_update2, to_insert2 = merge_last_scd_record_with_oldest_scd_record_from_new_data_both_having_different_attibutes(
#         old_scd, scd_entries_with_only_one_new_entry,
#         attrib_columns, natural_key_col, split_result=True
#     )

#     # to_update3, to_insert3 = merge_last_scd_record_with_scd_records_from_new_data_both_having_different_attibutes(
#     #     old_scd, scd_entries_with_only_more_new_entries,
#     #     attrib_columns, natural_key_col, split_result=True
#     # )

#     # to_update = to_update1.unionByName(to_update2).unionByName(to_update3)
#     # to_insert = to_insert1.unionByName(to_insert2).unionByName(to_insert3)

#     return to_update2, to_insert2


   