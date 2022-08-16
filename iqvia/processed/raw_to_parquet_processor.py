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

plan_load_path = conf.get("spark.nyec.iqvia.plans_ingest_path")
patient_load_path = conf.get("spark.nyec.iqvia.patients_ingest_path")
claim_load_path = conf.get("spark.nyec.iqvia.claims_ingest_path")
procedure_load_path = conf.get("spark.nyec.iqvia.procs_ingest_path")
proc_modifier_load_path = conf.get("spark.nyec.iqvia.proc_mod_ingest_path")
diagnosis_load_path = conf.get("spark.nyec.iqvia.diags_ingest_path")
drug_load_path = conf.get("spark.nyec.iqvia.drugs_ingest_path")
provider_load_path = conf.get("spark.nyec.iqvia.providers_ingest_path")
pro_provider_load_path = conf.get("spark.nyec.iqvia.pro_provider_ingest_path")

plan_save_path = conf.get("spark.nyec.iqvia.processed_plan_path")
patient_save_path = conf.get("spark.nyec.iqvia.processed_patient_path")
claim_save_path = conf.get("spark.nyec.iqvia.processed_claim_path")
procedure_save_path = conf.get("spark.nyec.iqvia.processed_proc_path")
proc_modifier_save_path = conf.get("spark.nyec.iqvia.processed_proc_mod_path")
diagnosis_save_path = conf.get("spark.nyec.iqvia.processed_diag_path")
drug_save_path = conf.get("spark.nyec.iqvia.processed_drug_path")
provider_save_path = conf.get("spark.nyec.iqvia.processed_provider_path")
pro_provider_save_path = conf.get("spark.nyec.iqvia.pro_provider_processed_path")


load_paths = conf.get("spark.nyec.ingest_path").split(',')

for p in load_paths:
    load_df(spark, p, raw_plan_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema) \
        .write.parquet(f'{p}/parquet', mode='overwrite')


load_df(spark, plan_load_path, raw_plan_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
        .write.parquet(plan_save_path, mode='overwrite')

load_df(spark, patient_load_path, raw_patient_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
    .write.parquet(patient_save_path, mode='overwrite')

load_df(spark, claim_load_path, raw_claim_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
    .write.parquet(claim_save_path, mode='overwrite')

load_df(spark, procedure_load_path, raw_procedure_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
    .write.parquet(procedure_save_path, mode='overwrite')

load_df(spark, proc_modifier_load_path, raw_procedure_modifier_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
    .write.parquet(proc_modifier_save_path, mode='overwrite')

load_df(spark, diagnosis_load_path, raw_diag_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
    .write.parquet(diagnosis_save_path, mode='overwrite')

load_df(spark, drug_load_path, raw_drug_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
    .write.parquet(drug_save_path, mode='overwrite')

load_df(spark, provider_load_path, raw_provider_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
    .write.parquet(provider_save_path, mode='overwrite')

load_df(spark, pro_provider_load_path, raw_pro_provider_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
    .write.parquet(pro_provider_save_path, mode='overwrite')




