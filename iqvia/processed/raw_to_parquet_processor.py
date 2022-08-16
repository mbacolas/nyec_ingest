from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

import json
from iqvia.common.load import *

spark = SparkSession.builder \
                    .appName("Raw To Parquet") \
                    .getOrCreate()

conf = spark.conf

infer_file_schema = eval(conf.get("spark.nyec.infer_schema", 'False'))
file_parsing_delimiter = conf.get("spark.nyec.file_delimiter", ',')
dynamic_config = conf.get("spark.nyec.dynamic_config")
dynamic_config_list = json.loads(dynamic_config)


for cfg in dynamic_config_list:
    ingest_path = cfg.get('ingest_path')
    from_module = cfg.get('from')
    import_path = cfg.get('import')
    import_path_f = __import__(import_path, fromlist=(from_module))
    load_df(spark, ingest_path, import_path_f, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema) \
        .write.parquet(f'{ingest_path}/parquet', mode='overwrite')
