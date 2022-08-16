from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

from iqvia.common.schema import *
from iqvia.common.load import *

spark = SparkSession.builder \
                    .appName("Raw To Parquet") \
                    .getOrCreate()

conf = spark.conf

infer_file_schema = eval(conf.get("spark.nyec.infer_schema", 'False'))
file_parsing_delimiter = conf.get("spark.nyec.file_delimiter", ',')
load_paths = conf.get("spark.nyec.ingest_path").split(',')

for p in load_paths:
    load_df(spark, p, raw_plan_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema) \
        .write.parquet(f'{p}/parquet', mode='overwrite')
