from google.cloud import bigquery
import pandas as pd
from pyspark.sql.types import StructField, StructType, StringType, DoubleType, IntegerType, DateType, DecimalType
from google.cloud import bigquery
from pyspark.sql import DataFrame
import numpy as np
from pyspark.sql import SparkSession
import os
from scripts.modules.ingest_utils import download_data_from_bq

values = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')

spark = SparkSession.builder \
    .appName("Iowa Sales ETL") \
    .getOrCreate()

client = bigquery.Client()

query = """
SELECT * FROM
`iowa-sales-analytic-platform.temp_dataset.temp_sales_table`
WHERE EXTRACT(MONTH FROM date) = 6
AND EXTRACT(YEAR FROM date) = 2025
"""

dest_path = 'ingest/raw_sales/'

download_data_from_bq(spark, client, query, dest_path, 'append')