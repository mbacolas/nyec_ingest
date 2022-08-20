from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

from iqvia.common.schema import *
from iqvia.common.load import *

spark = SparkSession.builder \
                    .appName("Raw To Parquet") \
                    .getOrCreate()

conf = spark.conf

file_parsing_delimiter = conf.get("spark.nyec.file_delimiter", ',')

plan_load_path = conf.get("spark.nyec.iqvia.raw_plan_ingest_path")
patient_load_path = conf.get("spark.nyec.iqvia.raw_patient_ingest_path")
claim_load_path = conf.get("spark.nyec.iqvia.raw_claim_ingest_path")
procedure_load_path = conf.get("spark.nyec.iqvia.raw_procedure_ingest_path")
proc_modifier_load_path = conf.get("spark.nyec.iqvia.raw_procedure_modifier_ingest_path")
diagnosis_load_path = conf.get("spark.nyec.iqvia.raw_diagnosis_ingest_path")
drug_load_path = conf.get("spark.nyec.iqvia.raw_drug_ingest_path")
provider_load_path = conf.get("spark.nyec.iqvia.raw_provider_ingest_path")
pro_provider_load_path = conf.get("spark.nyec.iqvia.raw_pro_provider_ingest_path")

iqvia_processed_s3_prefix = conf.get("spark.nyec.iqvia.iqvia_processed_s3_prefix")


def generate_output_path(data_set_name: str) -> str:
    return f'{iqvia_processed_s3_prefix}/{data_set_name}/'


load_df(spark, plan_load_path, raw_plan_schema, file_delimiter=file_parsing_delimiter)\
        .write.parquet(generate_output_path('plan'), mode='overwrite')

load_df(spark, patient_load_path, raw_patient_schema, file_delimiter=file_parsing_delimiter)\
    .write.parquet(generate_output_path('patient'), mode='overwrite')

load_df(spark, claim_load_path, raw_claim_schema, file_delimiter=file_parsing_delimiter)\
    .write.parquet(generate_output_path('factdx'), mode='overwrite')

load_df(spark, procedure_load_path, raw_procedure_schema, file_delimiter=file_parsing_delimiter)\
    .write.parquet(generate_output_path('procedure'), mode='overwrite')

load_df(spark, proc_modifier_load_path, raw_procedure_modifier_schema, file_delimiter=file_parsing_delimiter)\
    .write.parquet(generate_output_path('proceduremodifier'), mode='overwrite')

load_df(spark, diagnosis_load_path, raw_diag_schema, file_delimiter=file_parsing_delimiter)\
    .write.parquet(generate_output_path('diagnosis'), mode='overwrite')

load_df(spark, drug_load_path, raw_drug_schema, file_delimiter=file_parsing_delimiter)\
    .write.parquet(generate_output_path('product'), mode='overwrite')

load_df(spark, provider_load_path, raw_provider_schema, file_delimiter=file_parsing_delimiter)\
    .write.parquet(generate_output_path('provider'), mode='overwrite')

load_df(spark, pro_provider_load_path, raw_pro_provider_schema, file_delimiter=file_parsing_delimiter)\
    .write.parquet(generate_output_path('professionalprovidertier'), mode='overwrite')
