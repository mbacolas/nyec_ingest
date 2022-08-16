from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf

spark = SparkSession.builder \
                    .appName("Raw To Parquet") \
                    .getOrCreate()

conf = spark.conf

def load_df(spark: SparkSession, load_path: str, schema: StructType, file_delimiter=',', file_header=True,
            infer_schema=False):
    if not infer_schema:
        return spark.read \
            .schema(schema) \
            .options(inferSchema=False, delimiter=file_delimiter, header=file_header) \
            .csv(load_path)
    else:
        return spark.read \
            .options(inferSchema=True, delimiter=file_delimiter, header=file_header) \
            .csv(load_path)

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType, MapType, BooleanType

raw_plan_schema = StructType([ \
    StructField("PLAN_ID", StringType()), \
    StructField("IMS_PLN_ID", StringType()), \
    StructField("IMS_PLN_NM", StringType()), \
    StructField("IMS_PAYER_ID", StringType()), \
    StructField("IMS_PAYER_NM", StringType()), \
    StructField("PLANTRACK_ID", StringType()), \
    StructField("MODEL_TYP_CD", StringType()), \
    StructField("MODEL_TYP_NM", StringType()), \
    StructField("IMS_PBM_ADJUDICATING_ID", StringType()), \
    StructField("IMS_PBM_ADJUDICATING_NM", StringType())
])

raw_patient_schema = StructType([ \
    StructField("PATIENT_ID",StringType(), False),
    StructField("PAT_BRTH_YR_NBR",StringType(),False),
    StructField("PAT_GENDER_CD",StringType(), False)
])

raw_claim_schema = StructType([ \
    StructField("MONTH_ID",StringType(),False), \
    StructField("SVC_DT",StringType(),False), \
    StructField("PATIENT_ID",StringType(),False), \
    StructField("PAT_ZIP3",StringType(),False), \
    StructField("CLAIM_ID",StringType(),False), \
    StructField("SVC_NBR",StringType(),False), \
    StructField("DIAG_CD_POSN_NBR",StringType(),True), \
    StructField("CLAIM_TYP_CD",StringType(),False), \
    StructField("RENDERING_PROVIDER_ID",StringType(),True), \
    StructField("REFERRING_PROVIDER_ID",StringType(),True), \
    StructField("PLACE_OF_SVC_NM",StringType(),True), \
    StructField("PLAN_ID",StringType(),True), \
    StructField("PAY_TYP_DESC",StringType(),True), \
    StructField("DIAG_CD",StringType(),True), \
    StructField("DIAG_VERS_TYP_ID",StringType(),True), \
    StructField("PRC_CD",StringType(),True), \
    StructField("PRC_VERS_TYP_ID",StringType(),True), \
    StructField("PRC1_MODR_CD",StringType(),True), \
    StructField("PRC2_MODR_CD",StringType(),True), \
    StructField("PRC3_MODR_CD",StringType(),True), \
    StructField("PRC4_MODR_CD",StringType(),True), \
    StructField("NDC_CD",StringType(),True), \
    StructField("SVC_CRGD_AMT",StringType(),True), \
    StructField("UNIT_OF_SVC_AMT",StringType(),True), \
    StructField("HOSP_ADMT_DT",StringType(),True), \
    StructField("HOSP_DISCHG_DT",StringType(),True), \
    StructField("SVC_FR_DT",StringType(),False), \
    StructField("SVC_TO_DT",StringType(),True), \
    StructField("CLAIM_HOSP_REV_CD",StringType(),True), \
    StructField("FCLT_TYP_CD",StringType(),True), \
    StructField("ADMS_SRC_CD",StringType(),True), \
    StructField("ADMS_TYP_CD",StringType(),True), \
    StructField("ADMS_DIAG_CD",StringType(),True), \
    StructField("ADMS_DIAG_VERS_TYP_ID",StringType(),True)
])

raw_procedure_schema = StructType([ \
    StructField("PRC_CD", StringType()), \
    StructField("PRC_VERS_TYP_ID", StringType()), \
    StructField("PRC_SHORT_DESC", StringType()), \
    StructField("PRC_TYP_CD", StringType())
])

raw_procedure_modifier_schema = StructType([ \
    StructField("PRC_MODR_CD", StringType()), \
    StructField("PRC_MODR_DESC", StringType())
])

raw_diag_schema = StructType([ \
    StructField("DIAG_CD", StringType()), \
    StructField("DIAG_VERS_TYP_ID", StringType()), \
    StructField("DIAG_SHORT_DESC", StringType())
])

raw_drug_schema = StructType([ \
    StructField("NDC_CD", StringType()), \
    StructField("MKTED_PROD_NM", StringType()), \
    StructField("STRNT_DESC", StringType()), \
    StructField("DOSAGE_FORM_NM", StringType()), \
    StructField("USC_CD", StringType()), \
    StructField("USC_DESC", StringType())
])

raw_provider_schema = StructType([ \
    StructField("PROVIDER_ID", StringType()), \
    StructField("PROVIDER_TYP_ID", StringType()), \
    StructField("ORG_NM", StringType()), \
    StructField("IMS_RXER_ID", StringType()), \
    StructField("LAST_NM", StringType()), \
    StructField("FIRST_NM", StringType()), \
    StructField("ADDR_LINE1_TXT", StringType()), \
    StructField("ADDR_LINE2_TXT", StringType()), \
    StructField("CITY_NM", StringType()), \
    StructField("ST_CD", StringType()), \
    StructField("ZIP", StringType()), \
    StructField("PRI_SPCL_CD", StringType()), \
    StructField("PRI_SPCL_DESC", StringType()), \
    StructField("ME_NBR", StringType()), \
    StructField("NPI", StringType())
])

raw_pro_provider_schema = StructType([
    StructField("MONTH_ID", StringType()),
    StructField("RENDERING_PROVIDER_ID", StringType()),
    StructField("TIER_ID", StringType())
])


stage_procedure_schema = StructType([
    StructField("row_id", StringType()),
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("start_date_raw", StringType()),
    StructField("start_date", DateType()),
    StructField("to_date_raw", StringType()),
    StructField("to_date", DateType()),
    StructField("code_raw", StringType()),
    StructField("code", StringType()),
    StructField("code_system_raw", StringType()),
    StructField("code_system", StringType()),
    StructField("revenue_code_raw", StringType()),
    StructField("revenue_code", StringType()),
    StructField("desc", StringType()),
    StructField("source_desc", StringType()),
    StructField("mod_raw", ArrayType(StringType())),
    StructField("mod", ArrayType(StringType())),
    StructField("error", ArrayType(StringType())),
    StructField("warning", ArrayType(StringType())),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])

error_schema = StructType([
    StructField("batch_id", StringType()),
    StructField("type", StringType()),
    StructField("row_errors", StringType()),
    StructField("row_value", StringType()),
    StructField("date_created", DateType())
])

stage_drug_schema = StructType([
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("start_date_raw", StringType()),
    StructField("start_date", DateType()),
    StructField("to_date_raw", StringType()),
    StructField("to_date", DateType()),
    StructField("code_raw", StringType()),
    StructField("code", StringType()),
    StructField("code_system_raw", StringType()),
    StructField("code_system", StringType()),
    StructField("desc", StringType()),
    StructField("source_desc", StringType()),
    StructField("strength_raw", StringType()),
    StructField("strength", StringType()),
    StructField("form", StringType()),
    StructField("form_raw", StringType()),
    StructField("classification", StringType()),
    StructField("classification_raw", StringType()),
    StructField("error", StringType()),
    StructField("warning", StringType()),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])

raw_org_schema = StructType([
    StructField("source_org_oid", StringType()),
    StructField("name", StringType()),
    StructField("type", StringType()),
    StructField("active", StringType())
])

stage_org_schema = StructType([
    StructField("source_org_oid", StringType()),
    StructField("name", StringType()),
    StructField("type", StringType()),
    StructField("active", StringType()),
    StructField("error", ArrayType(StringType())),
    StructField("warning", ArrayType(StringType())),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])

stage_patient_schema = StructType([
    StructField("source_consumer_id", StringType()),
    StructField("source_org_oid", StringType()),
    StructField("type", StringType()),
    StructField("active", BooleanType()),
    StructField("dob", DateType()),
    StructField("gender", StringType()),
    StructField("error", ArrayType(StringType())),
    StructField("warning", ArrayType(StringType())),
    StructField("is_valid", BooleanType()),
    StructField("has_warnings", BooleanType()),
    StructField("batch_id", StringType())
])


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

load_df(spark, claim_load_path, raw_claim_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
    .write.parquet(claim_save_path, mode='overwrite')

load_df(spark, plan_load_path, raw_plan_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema)\
        .write.parquet(plan_save_path, mode='overwrite')

load_df(spark, patient_load_path, raw_patient_schema, file_delimiter=file_parsing_delimiter, infer_schema=infer_file_schema).repartition('PATIENT_ID')\
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




