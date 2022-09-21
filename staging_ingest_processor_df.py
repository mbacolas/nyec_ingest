import json

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
format_type = conf.get("spark.nyec.iqvia.file_format", 'csv')
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
raw_plan_df = load_plan(spark, plan_path, raw_plan_schema, format_type)\
    .select(col('PLAN_ID'),
            col('IMS_PAYER_NM'),
            col('IMS_PLN_ID'),
            col('IMS_PLN_NM'))

raw_patient_df = load_patient(spark, patient_path, raw_patient_schema, format_type)

# .withColumn('SALT', (10*rand()).cast(IntegerType()))\
# .limit(10 * 1000 * 1000)\
raw_claim_df = load_claim(spark, claim_path, raw_claim_schema, file_format=format_type)\
                    .withColumn('batch_id', lit(batch_id))\
                    .withColumn('date_created', lit(date_created))

    # .persist(StorageLevel.MEMORY_AND_DISK)

raw_proc_df = load_procedure(spark, procedure_path, raw_procedure_schema, format_type)
raw_diag_df = load_diagnosis(spark, diagnosis_path, raw_diag_schema, format_type)
provider_raw = load_provider(spark, provider_path, raw_provider_schema, format_type).select(col('PROVIDER_ID'),
                                                                                                col('PROVIDER_TYP_ID'),
                                                                                                col('NPI'),
                                                                                                col('FIRST_NM'),
                                                                                                col('LAST_NM'))

raw_drug_df = load_drug(spark, drug_path, raw_drug_schema, format_type).select(col('NDC_CD'),
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
# broadcast_cache = spark.sparkContext.broadcast(ref_cache)
proc_cache_broadcast = spark.sparkContext.broadcast(proc_cache)

# @udf(returnType=MapType(StringType(), StringType()))
# def ref_lookup(event_type: str, key: str):
#     return broadcast_cache.value[event_type].get(key, {})

# patient_claims_raw_rdd = raw_patient_df \
#     .join(raw_claim_df, on=[raw_claim_df.PATIENT_ID_CLAIM == raw_patient_df.PATIENT_ID,], how="inner") \
#     .withColumn('batch_id', lit(batch_id)) \
#     .withColumn('date_created', lit(date_created)) \
#     .rdd\
#     .persist(StorageLevel.MEMORY_AND_DISK)


@udf(returnType=DateType())
def to_date(str_date, col_name):
    return str_to_date(str_date, col_name).value



# def get_local_code(code, code_system_version, mapping_broadcasted):
#     if code is None:
#         return None
#     if code_system_version is None:
#         return None
#     k = code + ':' + code_system_version
#     proc_type = mapping_broadcasted.value.get(k, {}).get('PRC_TYP_CD', None)
#     code_system = to_standard_code_system(code_system_version, proc_type, 'PRC_VERS_TYP_ID:PRC_TYP_CD').value
#     proc_code_result = find_code(code, code_system, 'PRC_CD')
#     return extract_code(proc_code_result).get('code', None)

def get_procedure_code(mapping_broadcasted):
    def f(code, code_system_version):
        if code is None:
            return None
        if code_system_version is None:
            return None
        k = code + ':' + code_system_version
        proc_type = mapping_broadcasted.value.get(k, {}).get('PRC_TYP_CD', None)
        code_system = to_standard_code_system(code_system_version, proc_type, 'PRC_VERS_TYP_ID:PRC_TYP_CD').value
        proc_code_result = find_code(code, code_system, 'PRC_CD')
        return extract_code(proc_code_result).get('code', None)
    return udf(f)

def get_procedure_desc(mapping_broadcasted):
    def f(code, code_system_version):
        if code is None:
            return None
        if code_system_version is None:
            return None
        k = code + ':' + code_system_version
        proc_type = mapping_broadcasted.value.get(k, {}).get('PRC_TYP_CD', None)
        code_system = to_standard_code_system(code_system_version, proc_type, 'PRC_VERS_TYP_ID:PRC_TYP_CD').value
        proc_code_result = find_code(code, code_system, 'PRC_CD')
        return extract_code(proc_code_result).get('desc', None)
    return udf(f)

def get_source_procedure_desc(mapping_broadcasted):
    def f(code, code_system_version):
        if code is None:
            return None
        if code_system_version is None:
            return None
        cach_key = code+':'+code_system_version
        return mapping_broadcasted.value.get(cach_key, {}).get('PRC_SHORT_DESC', None)
    return udf(f)

def get_rev_code(mapping_broadcasted):
    def f(code):
        if code is None:
            return None
        proc_code_result =  find_code(code, 'REV', 'REV')
        return extract_code(proc_code_result).get('code', None)
    return udf(f)

def get_proc_code_system(mapping_broadcasted):
    def f(code, code_system_version):
        if code is None:
            code = ''
        if code_system_version is None:
            code_system_version = ''
        k = code + ':' + code_system_version
        proc_type = mapping_broadcasted.value.get(k, {}).get('PRC_TYP_CD', None)
        code_system_result = to_standard_code_system(code_system_version, proc_type, 'PRC_VERS_TYP_ID:PRC_TYP_CD')
        return code_system_result.value
    return udf(f)
@udf(returnType=ArrayType(StringType()))
def get_proc_mods(*args):
    return [i for i in args if i is not None]


# @udf(returnType=ArrayType(MapType(StringType(), StringType())))
@udf(returnType=StringType())
def validate_row(code,
                 code_raw,
                 code_system,
                 code_system_raw,
                 revenue_code,
                 revenue_code_raw,
                 start_date,
                 start_date_raw,
                 to_date,
                 to_date_raw,
                 row_id):
    import json
    validation_errors = []
    if code is None or code_system is None:
        error = {'msg': 'invalid code or code_system', 'code_raw': code_raw, 'code_system_raw': code_system_raw}
        validation_errors.append(error)
    if revenue_code is None and revenue_code_raw is not None:
        error = {'msg': 'invalid rev_code_raw', 'rev_code_raw': revenue_code_raw}
        validation_errors.append(error)
    if start_date is None:
        error = {'msg': 'invalid start_date', 'from_date': start_date}
        validation_errors.append(error)
    if to_date is None and to_date_raw is not None:
        error = {'msg': 'invalid to_date', 'to_date': to_date}
        validation_errors.append(error)
    if len(validation_errors) > 0:
        validation_errors.append({'row_id': row_id})
    return json.dumps(validation_errors)

@udf(returnType=BooleanType())
def is_valid(errors):
    e = json.loads(errors)
    if len(e) > 0:
        return False
    else:
        return True
@udf(returnType=StringType())
def get_warnings():
    return  json.dumps([])

@udf(returnType=BooleanType())
def has_warnings(warn):
    w = json.loads(warn)
    if len(w) > 0:
        return True
    else:
        return False

    # from_result = to_date(from_date_raw, 'from_date_raw')
    # to_result = to_date(to_date_raw, 'to_date_raw', False)
    # proc_code_result = find_code(code_raw, code_system_raw, 'PRC_CD')
    # rev_error = []
    # if rev_code_raw is not None:
    #     rev_code_result = find_code(rev_code_raw, 'REV', 'CLAIM_HOSP_REV_CD')
    #     rev_error = extract_left(*[rev_code_result])
    # validation_errors = extract_left(*[from_result,
    #                                    to_result,
    #                                    proc_code_result])
    # validation_errors = validation_errors + rev_error + [{'row_id': row_id}]
# def get_proc_mods_2(mod1, mod2):
#     arr = [mod1, mod2]
#     return [i for i in arr if i is not None]
#
# arr=['1', '2', None, '4']
# x = [i for i in arr if i is not None]
# a = raw_claim_df.filter(col("PRC_CD").isNotNull())\
#     .select(col('PRC1_MODR_CD'), col('PRC2_MODR_CD'), col('PRC3_MODR_CD'), col('PRC4_MODR_CD'))\
#     .withColumn('mod_raw', array(get_proc_mods_2(col('PRC1_MODR_CD'), col('PRC2_MODR_CD'))))
# a.first()

@udf(returnType=StringType())
def generate_id():
    return uuid.uuid4().hex[:12]

proc_df = \
raw_claim_df.filter(col("PRC_CD").isNotNull())\
            .select(col('PATIENT_ID'),
                    col('source_org_oid'),
                    col('CLAIM_ID'),
                     col('SVC_NBR'),
                     col('SVC_FR_DT'),
                     col('SVC_TO_DT'),
                     col('PRC_CD'),
                     col('PRC_VERS_TYP_ID'),
                     col('CLAIM_HOSP_REV_CD'),
                     col('PRC1_MODR_CD'),
                     col('PRC2_MODR_CD'),
                     col('PRC3_MODR_CD'),
                     col('PRC4_MODR_CD'), \
                     col('RENDERING_PROVIDER_ID'), \
                     col('batch_id'), \
                     col('date_created')) \
                    .withColumnRenamed('PATIENT_ID', 'source_consumer_id')\
                    .withColumnRenamed('CLAIM_ID', 'claim_id')\
                    .withColumnRenamed('SVC_NBR', 'svc_nbr')\
                    .withColumnRenamed('PRC_CD', 'code_raw')\
                    .withColumnRenamed('PRC_VERS_TYP_ID', 'code_system_raw')\
                    .withColumnRenamed('SVC_FR_DT', 'start_date_raw')\
                    .withColumnRenamed('SVC_TO_DT', 'to_date_raw')\
                    .withColumnRenamed('CLAIM_HOSP_REV_CD', 'revenue_code_raw')\
                    .withColumnRenamed('RENDERING_PROVIDER_ID', 'provider_id')\
                    .withColumn('id', generate_id()) \
                    .withColumn('body_site', lit(None).cast("string")) \
                    .withColumn('outcome', lit(None).cast("string")) \
                    .withColumn('complication', lit(None).cast("string")) \
                    .withColumn('note', lit(None).cast("string")) \
                    .withColumn('start_date', lit(to_date(col('start_date_raw'), 'start_date_raw'))) \
                    .withColumn('to_date', lit(to_date(col('to_date_raw'), 'to_date_raw'))) \
                    .withColumn('mod_raw', get_proc_mods(col('PRC1_MODR_CD'), col('PRC2_MODR_CD'), col('PRC3_MODR_CD'), col('PRC4_MODR_CD')))\
                    .withColumn('mod', get_proc_mods(col('PRC1_MODR_CD'), col('PRC2_MODR_CD'), col('PRC3_MODR_CD'), col('PRC4_MODR_CD')))\
                    .withColumn('date_created', lit(date_created)) \
                    .withColumn('code', get_procedure_code(proc_cache_broadcast)(col('code_raw'), col('code_system_raw'))) \
                    .withColumn('code_system', get_proc_code_system(proc_cache_broadcast)(col('code_raw'), col('code_system_raw'))  ) \
                    .withColumn('revenue_code', get_rev_code(proc_cache_broadcast)(col('revenue_code_raw'))) \
                    .withColumn('source_desc', get_source_procedure_desc(proc_cache_broadcast)(col('code_raw'), col('code_system_raw'))  ) \
                    .withColumn('desc', get_procedure_desc(proc_cache_broadcast)(col('code_raw'), col('code_system_raw')) )\
                    .withColumn('error', validate_row(
                                           col('code'),
                                           col('code_raw'),
                                           col('code_system'),
                                           col('code_system_raw'),
                                           col('revenue_code'),
                                           col('revenue_code_raw'),
                                           col('start_date'),
                                           col('start_date_raw'),
                                           col('to_date'),
                                           col('to_date_raw'),
                                           col('id')))\
                    .withColumn('is_valid', is_valid(col('errors')))\
                    .withColumn('warning', get_warnings())\
                    .withColumn('has_warnings', has_warnings(col('warning')))\
                    .dropDuplicates(['source_consumer_id',
                                     'start_date_raw',
                                     'code_raw',
                                     'code_system_raw',
                                     'provider_id'])\
                    .persist(StorageLevel.MEMORY_AND_DISK)


save_procedure(proc_df, generate_output_path('procedure'))
save_procedure_modifiers(proc_df, generate_output_path('proceduremodifier'))
save_errors(proc_df, PROCEDURE, generate_output_path('error'))
