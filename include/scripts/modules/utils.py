from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DateType, StructType
from typing import List, Tuple
from copy import copy


def replace_attributes_with_hash(df: DataFrame, attributes_cols: List[str]) -> Tuple[DataFrame, DataFrame]:
    attributes_cols = sorted(attributes_cols)

    hash_math_df = df.select(*attributes_cols) \
                     .withColumn('hashed_attributes_value', F.hash(*attributes_cols))
    
    df = df.withColumn('hashed_attributes_value', F.hash(*attributes_cols)) 

    df = df.drop(*attributes_cols)

    hash_math_df = hash_math_df.distinct()

    return df, hash_math_df

def replace_hash_with_attributes(df_with_hashed_attributes: DataFrame, hash_math_df: DataFrame) -> DataFrame:
    merge = df_with_hashed_attributes.alias('df').join(hash_math_df.alias('hd'), 'hashed_attributes_value', how='inner') \
                                    .select(*([F.col(f'df.{col}').alias(f'{col}') for col in df_with_hashed_attributes.columns] +
                                             [F.col(f'hd.{col}').alias(f'{col}') for col in hash_math_df.columns]))
    
    cols_to_drop = [c for c in merge.columns if c.startswith('hashed_attributes_value')]

    merge = merge.drop(*cols_to_drop)
    
    return merge
