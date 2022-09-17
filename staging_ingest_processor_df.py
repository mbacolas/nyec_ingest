from iqvia.curated.transform_functions import *
from iqvia.curated.save_functions import *
from iqvia.common.load import *
from iqvia.common.schema import *
from pyspark.sql import SQLContext
from pyspark import StorageLevel
from pymonad.either import *
import uuid
from datetime import datetime

PATIENT = 'PATIENT'
PROCEDURE = 'PROCEDURE'
PROBLEM = 'PROBLEM'
ADMITTING_PROBLEM = 'ADMITTING_PROBLEM'
DRUG = 'DRUG'
COST = 'COST'
CLAIM = 'CLAIM'
PRACTIONER = 'PRACTIONER'
PLAN = 'PLAN'

spark = SparkSession \
    .builder \
    .appName("test") \
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("fs.s3a.sse.enabled", True) \
    .config("fs.s3a.server-side-encryption-algorithm", "SSE-KMS") \
    .appName("IQVIA Ingest") \
    .getOrCreate()

conf = spark.conf
sqlContext = SQLContext(spark)


def generate_batch_id():
    from datetime import date
    curr_dt = date.today()
    curr_year = curr_dt.year
    curr_month = curr_dt.month
    curr_day = curr_dt.day
    return f'{curr_year}{curr_month}{curr_day}'


start = datetime.now()
plan_path = conf.get("spark.nyec.iqvia.raw_plan_ingest_path")
patient_path = conf.get("spark.nyec.iqvia.raw_patient_ingest_path")
claim_path = conf.get("spark.nyec.iqvia.raw_claim_ingest_path")
procedure_path = conf.get("spark.nyec.iqvia.raw_procedure_ingest_path")
proc_modifier_path = conf.get("spark.nyec.iqvia.raw_procedure_modifier_ingest_path")
diagnosis_path = conf.get("spark.nyec.iqvia.raw_diagnosis_ingest_path")
drug_path = conf.get("spark.nyec.iqvia.raw_drug_ingest_path")
provider_path = conf.get("spark.nyec.iqvia.raw_provider_ingest_path")
iqvia_curated_s3_prefix = conf.get("spark.nyec.iqvia.iqvia_curated_s3_prefix")
batch_id = conf.get("spark.nyec.iqvia.batch_id", generate_batch_id())
# batch_id = conf.get("spark.nyec.iqvia.batch_id", uuid.uuid4().hex[:12])
file_format = conf.get("spark.nyec.iqvia.file_format", 'csv')
partition_size = int(conf.get("spark.nyec.iqvia.partition_size", '3000'))


# raw_pro_provider_ingest_path = conf.get("spark.nyec.iqvia.raw_pro_provider_ingest_path")

def generate_output_path(data_set_name: str) -> str:
    return f'{iqvia_curated_s3_prefix}/{data_set_name}/'


# rdd_test = spark.sparkContext.textFile(claim_path)
# header_fields = rdd_test.first().split('|')
# expected_header = claims_header()
# assert expected_header == header_fields

### create data frames
date_created = datetime.now()
org_data = [(uuid.uuid4().hex[:12], "IQVIA", "IQVIA", "THIRD PARTY CLAIMS AGGREGATOR", True, batch_id, date_created)]
org_df = spark.createDataFrame(data=org_data, schema=raw_org_schema)

# < 256MB per partition
# .withColumn('SALT', (10*rand()).cast(IntegerType()))\
raw_plan_df = load_plan(spark, plan_path, raw_plan_schema, file_format)\
    .select(col('PLAN_ID'),
            col('IMS_PAYER_NM'),
            col('IMS_PLN_ID'),
            col('IMS_PLN_NM'))

raw_patient_df = load_patient(spark, patient_path, raw_patient_schema, file_format) \
    .withColumn('PATIENT_SALT', (100*rand()).cast(IntegerType()))\
    .persist(StorageLevel.MEMORY_AND_DISK)

# .withColumn('SALT', (10*rand()).cast(IntegerType()))\
# .limit(10 * 1000 * 1000)\
raw_claim_df = load_claim(spark, claim_path, raw_claim_schema, file_format).limit(100)
    # .persist(StorageLevel.MEMORY_AND_DISK)

raw_proc_df = load_procedure(spark, procedure_path, raw_procedure_schema, file_format)
raw_diag_df = load_diagnosis(spark, diagnosis_path, raw_diag_schema, file_format)
provider_raw = load_provider(spark, provider_path, raw_provider_schema, file_format).select(col('PROVIDER_ID'),
                                                                                                col('PROVIDER_TYP_ID'),
                                                                                                col('NPI'),
                                                                                                col('FIRST_NM'),
                                                                                                col('LAST_NM'))

raw_drug_df = load_drug(spark, drug_path, raw_drug_schema, file_format).select(col('NDC_CD'),
                                                                                col('MKTED_PROD_NM'),
                                                                                col('STRNT_DESC'),
                                                                                col('DOSAGE_FORM_NM'),
                                                                                col('USC_DESC'))

plan_list = raw_plan_df.collect()
proc_list = raw_proc_df.collect()
drug_list = raw_drug_df.collect()
problem_list = raw_diag_df.filter(raw_diag_df.DIAG_VERS_TYP_ID != -1).collect()
provider_list = provider_raw.collect()


def to_standard_code_system(version_id: str, type_cd: str, source_column_name) -> Either:
    if version_id == '2':
        return Right('ICD10')
    elif version_id == '1':
        return Right('ICD9')
    elif version_id == '-1' and type_cd == 'C':
        return Right('CPT')
    elif version_id == '-1' and type_cd == 'H':
        return Right('HCPCS')
    else:
        error = {'error': f'Invalid version_id/type_cd combination',
                 'source_column_value': f'{version_id}:{type_cd}',
                 'source_column_name': source_column_name}
        return Left(error)


def get_code_system(code_system_version: str, code_system_type: str):
    return to_standard_code_system(code_system_version, code_system_type, 'PRC_VERS_TYP_ID:PRC_TYP_CD').value


plan_cache = dict([(row['IMS_PLN_ID'], row.asDict()) for row in plan_list])
drug_cache = dict([(row['NDC_CD'], row.asDict()) for row in drug_list])
provider_cache = dict([(row['PROVIDER_ID'], row.asDict()) for row in provider_list])
problem_cache = dict(
    [(row["DIAG_CD"] + ':' + get_code_system(row["DIAG_VERS_TYP_ID"], ''), dict(DIAG_SHORT_DESC=row['DIAG_SHORT_DESC']))
     for row in problem_list if get_code_system(row["DIAG_VERS_TYP_ID"], '') is not None])

proc_cache = dict([(row["PRC_CD"] + ':' + row["PRC_VERS_TYP_ID"], row.asDict()) for row in proc_list ])

ref_cache = {PROCEDURE: proc_cache, PROBLEM: problem_cache, DRUG: drug_cache, PRACTIONER: provider_cache,
             PLAN: plan_cache}

print('------------------------>>>>>>> broadcasting reference data')
broadcast_cache = spark.sparkContext.broadcast(ref_cache)

@udf(returnType=MapType(StringType(), StringType()))
def ref_lookup(event_type: str, key: str):
    return broadcast_cache.value[event_type].get(key, {})

patient_claims_raw_rdd = raw_patient_df \
    .join(raw_claim_df, on=[raw_claim_df.PATIENT_ID_CLAIM == raw_patient_df.PATIENT_ID,], how="inner") \
    .withColumn('batch_id', lit(batch_id)) \
    .withColumn('date_created', lit(date_created)) \
    .rdd\
    .persist(StorageLevel.MEMORY_AND_DISK)



@udf(returnType=StringType())
def to_dob(year: int) -> date:
    dob = str(year)+'-01-01'
    return datetime.strptime(dob, "%Y-%m-%d").date()

udf_star_desc = udf(lambda year:to_dob(year),DateType())

@udf(returnType=ArrayType(StringType()))
def test_row(from_date, to_date, code, code_system_version):
    return ['error1', 'error2']

# @udf(returnType=StringType())
# # def check_code(code, code_system_version, ref_func):
# def check_code(result):
#     print(result)
#     return result.get('PRC_CD', 'None')

@udf(returnType=StringType())
def get_code_system(code, code_system_version):
    to_standard_code_system(code_system_version, )
    return code

@udf(returnType=DateType())
def to_date(str_date, col_name):
    return str_to_date(str_date, col_name).value

# str_to_date('20200410', 'sdfsdf').value

def get_procedure_code(mapping_broadcasted):
    def f(code, code_system_version):
        if code is None:
            code = ''

        if code_system_version is None:
            code_system_version = ''
        k = code+':'+code_system_version
        return mapping_broadcasted.value.get(k, {}).get('PRC_CD', None)
    return udf(f)

proc_cache_broadcast = spark.sparkContext.broadcast(proc_cache)

udf_test_row = udf(lambda from_date, to_date, code, code_system_version:test_row(from_date, to_date, code, code_system_version),ArrayType(StringType()))
udf_check_code = udf(lambda code, code_system:check_code(code, code_system, ref_lookup),StringType())
udf_code_system_code = udf(lambda code, code_system:get_code_system(code, code_system),StringType())
udf_test = udf(lambda code:test_func(code),StringType())
raw_claim_df.withColumn("CLAIM_TYP_CD_NEW",test_func("CLAIM_TYP_CD")).show()

import pyspark.sql.functions as F

proc_df = \
raw_claim_df.select(col('PATIENT_ID_CLAIM'),
                    col('source_org_oid'),
                    col('CLAIM_ID'),
                     col('SVC_NBR'),
                     col('SVC_FR_DT'),
                     col('SVC_TO_DT'),
                     col('PRC_CD'),
                     col('PRC_VERS_TYP_ID'),
                     col('CLAIM_HOSP_REV_CD'),
                     col('PRC1_MODR_CD'),
                     col('PRC3_MODR_CD'),
                     col('PRC4_MODR_CD')) \
                    .withColumnRenamed('PATIENT_ID_CLAIM', 'source_consumer_id')\
                    .withColumnRenamed('CLAIM_ID', 'claim_id')\
                    .withColumnRenamed('SVC_NBR', 'svc_nbr')\
                    .withColumnRenamed('PRC_CD', 'code_raw')\
                    .withColumnRenamed('PRC_VERS_TYP_ID', 'code_system_raw')\
                    .withColumnRenamed('SVC_FR_DT', 'start_date_raw')\
                    .withColumnRenamed('SVC_TO_DT', 'to_date_raw')\
                    .withColumnRenamed('CLAIM_HOSP_REV_CD', 'revenue_code_raw')\

                    .withColumn('id', lit(uuid.uuid4().hex[:12])) \
                    .withColumn('body_site', lit(None)) \
                    .withColumn('outcome', lit(None)) \
                    .withColumn('complication', lit(None)) \
                    .withColumn('note', lit(None)) \
                    .withColumn('start_date', to_date(col('SVC_FR_DT'), 'SVC_FR_DT'))\
                    .withColumn('to_date', to_date(col('SVC_FR_DT'), 'SVC_FR_DT'))\
                    .withColumn('mod_raw', )\

                    .withColumn('code', get_procedure_code(proc_cache_broadcast)(col('code_raw'), col('code_system_raw'))  ).show()
                    .withColumn('code_system', working_fun(proc_cache_broadcast)(col('code_raw'), col('code_system_raw'))  ).show()
                    .withColumn('revenue_code', working_fun(proc_cache_broadcast)(col('code_raw'), col('code_system_raw'))  ).show()
                    .withColumn('desc', working_fun(proc_cache_broadcast)(col('code_raw'), col('code_system_raw'))  ).show()
                    .withColumn('source_desc', working_fun(proc_cache_broadcast)(col('code_raw'), col('code_system_raw'))  ).show()
                    # .withColumn('code', check_code( proc_cache.get(col('PRC_CD')+':'+col('PRC_VERS_TYP_ID')) ) ).show()
