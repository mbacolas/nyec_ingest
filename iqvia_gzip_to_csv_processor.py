from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
import uuid
from datetime import datetime
from iqvia.common.schema import *
from iqvia.common.load import *

spark = SparkSession.builder \
                    .appName("GZIP To CSV") \
                    .getOrCreate()

conf = spark.conf


file_parsing_delimiter = conf.get("spark.nyec.file_delimiter", '|')
patient_load_path = conf.get("spark.nyec.iqvia.raw_patient_ingest_path")
claim_load_path = conf.get("spark.nyec.iqvia.raw_claim_ingest_path")
raw_format_type = conf.get("spark.nyec.iqvia.file_format", 'csv')

def generate_output_path(data_set_name: str) -> str:
    return f's3://nyce-iqvia/raw/csv/{data_set_name}/'


# load_df(spark, patient_load_path, raw_patient_schema, file_delimiter=file_parsing_delimiter, format_type = raw_format_type) \
#     .coalesce(1) \
#     .write \
#     .options(header=True, delimiter=file_parsing_delimiter) \
#     .csv(generate_output_path('patient'), mode='overwrite')
#
#
# print('------------------------>>>>>>> saved patient')
# .options(header=True, delimiter=file_parsing_delimiter)\

# load_df(spark, claim_load_path, raw_claim_schema, file_delimiter=file_parsing_delimiter, format_type = raw_format_type) \

load_df(spark, claim_load_path, raw_claim_schema, file_delimiter=file_parsing_delimiter, format_type = raw_format_type)\
    .write \
    .options(header=True, delimiter=file_parsing_delimiter)\
    .csv(generate_output_path('factdx_2'), mode='overwrite')

print('------------------------>>>>>>> saved claim')
