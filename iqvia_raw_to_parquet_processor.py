from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

from iqvia.common.schema import *
from iqvia.common.load import *

spark = SparkSession.builder \
                    .appName("Raw To Parquet") \
                    .getOrCreate()

conf = spark.conf

file_parsing_delimiter = conf.get("spark.nyec.file_delimiter", '|')

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

# sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")
def generate_output_path(data_set_name: str) -> str:
    return f'{iqvia_processed_s3_prefix}/{data_set_name}/'


load_df(spark, plan_load_path, raw_plan_schema, file_delimiter=file_parsing_delimiter) \
    .repartition(col('PLAN_ID')) \
    .sortWithinPartitions(col('PLAN_ID')) \
    .write\
    .parquet(generate_output_path('plan'), mode='overwrite', compression='snappy')
# .repartition(col('PLAN_ID')) \
# .sortWithinPartitions(col('PLAN_ID')) \

print('------------------------>>>>>>> saved plan')
load_df(spark, patient_load_path, raw_patient_schema, file_delimiter=file_parsing_delimiter)\
    .repartition(col('PATIENT_ID'))\
    .sortWithinPartitions(col('PATIENT_ID'))\
    .write.parquet(generate_output_path('patient'), mode='overwrite', compression='snappy')
print('------------------------>>>>>>> saved patient')
load_df(spark, claim_load_path, raw_claim_schema, file_delimiter=file_parsing_delimiter) \
    .repartition(col('PATIENT_ID')) \
    .sortWithinPartitions(col('PATIENT_ID'), col('CLAIM_ID'), col('SVC_NBR')) \
    .write.parquet(generate_output_path('factdx'), mode='overwrite', compression='snappy')
print('------------------------>>>>>>> saved claim')
load_df(spark, procedure_load_path, raw_procedure_schema, file_delimiter=file_parsing_delimiter)\
    .repartition(col('PRC_CD'))\
    .sortWithinPartitions(col('PRC_CD'))\
    .write.parquet(generate_output_path('procedure'), mode='overwrite', compression='snappy')
print('------------------------>>>>>>> saved proc')
load_df(spark, proc_modifier_load_path, raw_procedure_modifier_schema, file_delimiter=file_parsing_delimiter) \
    .repartition(col('PRC_MODR_CD')) \
    .sortWithinPartitions(col('PRC_MODR_CD')) \
    .write.parquet(generate_output_path('proceduremodifier'), mode='overwrite', compression='snappy')
print('------------------------>>>>>>> saved proc mod')
load_df(spark, diagnosis_load_path, raw_diag_schema, file_delimiter=file_parsing_delimiter) \
    .repartition(col('DIAG_CD')) \
    .sortWithinPartitions(col('DIAG_CD')) \
    .write.parquet(generate_output_path('diagnosis'), mode='overwrite', compression='snappy')
print('------------------------>>>>>>> saved diag')
load_df(spark, drug_load_path, raw_drug_schema, file_delimiter=file_parsing_delimiter) \
    .repartition(col('NDC_CD')) \
    .sortWithinPartitions(col('NDC_CD')) \
    .write.parquet(generate_output_path('product'), mode='overwrite', compression='snappy')
print('------------------------>>>>>>> saved drug')
load_df(spark, provider_load_path, raw_provider_schema, file_delimiter=file_parsing_delimiter) \
    .repartition(col('PROVIDER_ID')) \
    .sortWithinPartitions(col('PROVIDER_ID')) \
    .write.parquet(generate_output_path('provider'), mode='overwrite', compression='snappy')
print('------------------------>>>>>>> done')
# load_df(spark, pro_provider_load_path, raw_pro_provider_schema, file_delimiter=file_parsing_delimiter) \
#     .sortWithinPartitions(col('source_provider_id')) \
#     .write.parquet(generate_output_path('professionalprovidertier'), mode='overwrite')
