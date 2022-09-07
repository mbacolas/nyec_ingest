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


# rdd=spark.sparkContext.parallelize([Row(pat_id=0,code='r1', name='name_1'),
#                                     Row(pat_id=0,code='r2', name='name_1'),
#                                     Row(pat_id=1,code='r1', name='name_2'),
#                                     Row(pat_id=1,code='r2', name='name_2'),
#                                     Row(pat_id=2,code='r1', name='name_3'),
#                                     Row(pat_id=2,code='r3', name='name_3'),
#                                     Row(pat_id=3,code='r4', name='name_3'),
#                                     ])
# # rdd1 = rdd.keyBy(lambda r: r.pat_id)
# # rdd1.glom().collect()
# # rdd2 = rdd1.partitionBy(5, lambda k: k)
# # rdd2.glom().collect()
# rdd3 = rdd.keyBy(lambda r: (r.pat_id, r.code))
# # rdd3 = rdd.keyBy(lambda t: ((t[1].pat_id, t[1].code)))
# # rdd3 = rdd2.map(lambda t: ((t[1].pat_id, t[1].code), t[1]))
# rdd3.glom().collect()
# rdd4 = rdd3.repartitionAndSortWithinPartitions(5, lambda t: t[0])
# rdd4.glom().collect()


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
partition_size = int(conf.get("spark.nyec.iqvia.partition_size", '6000'))


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
    .repartition(partition_size, 'PATIENT_ID') \
    .sortWithinPartitions('PATIENT_ID') \
    .persist(StorageLevel.MEMORY_AND_DISK)

# .withColumn('SALT', (10*rand()).cast(IntegerType()))\
# .limit(10 * 1000 * 1000)\
raw_claim_df = load_claim(spark, claim_path, raw_claim_schema, file_format).limit(10000)\
    .withColumn('CLAIM_SALT', (100*rand()).cast(IntegerType()))\
    .repartition(partition_size, 'PATIENT_ID_CLAIM') \
    .sortWithinPartitions('PATIENT_ID_CLAIM') \
    .persist(StorageLevel.MEMORY_AND_DISK)

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


def ref_lookup(event_type: str, key: str):
    return broadcast_cache.value[event_type].get(key, {})



### create stage patient DF
# patient_rdd = raw_patient_df.withColumn('batch_id', lit(batch_id)) \
#     .withColumn('source_org_oid', lit('IQVIA')) \
#     .withColumn('date_created', lit(date_created)) \
#     .rdd
#
# currated_patient_df = to_patient(patient_rdd, df_partition_size=partition_size).persist(StorageLevel.MEMORY_AND_DISK)
#
# # currated_patient_df = currated_patient_rdd.toDF(stage_patient_schema).persist(StorageLevel.MEMORY_AND_DISK)
# save_patient(currated_patient_df, generate_output_path('patient'))
# save_errors(currated_patient_df, PATIENT, generate_output_path('error'), date_created)
#
# currated_patient_df.unpersist()

print('------------------------>>>>>>> saved patient <<<--- ')

# raw_claim_df.CLAIM_SALT == raw_patient_df.PATIENT_SALT
### create clinical events
patient_claims_raw_rdd = raw_patient_df \
    .join(raw_claim_df, on=[raw_claim_df.PATIENT_ID_CLAIM == raw_patient_df.PATIENT_ID,], how="inner") \
    .withColumn('batch_id', lit(batch_id)) \
    .withColumn('date_created', lit(date_created)) \
    .rdd

print('------------------------>>>>>>> created patient_claims_raw_rdd')

### create procedure
# procedure_df = to_procedure(patient_claims_raw_rdd, ref_lookup, df_partition_size=partition_size).persist(StorageLevel.MEMORY_AND_DISK)
# save_errors(procedure_df, PROCEDURE, generate_output_path('error'), date_created)
# save_procedure_modifiers(procedure_df.rdd, generate_output_path('proceduremodifier'))
# # currated_df = procedure_rdd.toDF(stage_procedure_schema).persist(StorageLevel.MEMORY_AND_DISK)
# save_procedure(procedure_df, generate_output_path('procedure'))
# procedure_df.unpersist(False)
# # procedure_rdd.unpersist(False)
# raw_patient_df.unpersist()
# raw_claim_df.unpersist()

print('------------------------>>>>>>> saved procs')
#
# ### problems
problem_df = to_problem(patient_claims_raw_rdd, ref_lookup, df_partition_size=partition_size) #.persist(StorageLevel.MEMORY_AND_DISK)
admitting_problem_df = to_admitting_diagnosis(patient_claims_raw_rdd) #.persist(StorageLevel.MEMORY_AND_DISK)
all_problem_df = problem_df.union(admitting_problem_df).persist(StorageLevel.MEMORY_AND_DISK)

save_errors(all_problem_df, PROBLEM, generate_output_path('error'), date_created)
save_errors(all_problem_df, PROBLEM, generate_output_path('error'), date_created)
all_problem_df.unpersist(False)
problem_df.unpersist(False)
admitting_problem_df.unpersist(False)
print('------------------------>>>>>>> saved problems')
####


### drug
drug_df = to_drug(patient_claims_raw_rdd, ref_lookup, df_partition_size=partition_size).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(drug_df, DRUG, generate_output_path('error'), date_created)
save_drug(drug_df, generate_output_path('drug'))
# drug_rdd.unpersist(False)
####
print('------------------------>>>>>>> saved drugs')
#
#
# ### cost
cost_df = to_cost(patient_claims_raw_rdd, df_partition_size=partition_size).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(cost_df, COST, generate_output_path('error'), date_created)
save_cost(cost_df, generate_output_path('cost'))
# cost_rdd.unpersist(False)
####
print('------------------------>>>>>>> saved cost')
#
#
# ### claim
claim_record_df = to_claim(patient_claims_raw_rdd, ref_lookup, df_partition_size=partition_size).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(claim_record_df, CLAIM, generate_output_path('error'), date_created)
save_claim(claim_record_df, generate_output_path('claim'))
# claim_record_rdd.unpersist(False)
###
print('------------------------>>>>>>> saved claim')


### org
save_org(org_df, generate_output_path('org'))
# org_df.unpersist()
###


### provider
practitioner_df = to_practitioner(patient_claims_raw_rdd, ref_lookup, df_partition_size=partition_size).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(practitioner_df, PRACTIONER, generate_output_path('error'), date_created)
# practitioner_df = practitioner_rdd.toDF(stage_provider_schema).persist(StorageLevel.MEMORY_AND_DISK)
practitioner_role_df = to_practitioner_role(practitioner_df)
save_provider(practitioner_df, generate_output_path('provider'))
save_provider_role(practitioner_role_df, generate_output_path('provider_role'))

patient_claims_raw_rdd.unpersist(False)
# practitioner_rdd.unpersist(False)
# currated_practitioner_role_df.unpersist(False)
###

file_paths = {'plan': plan_path,
              'patient': patient_path,
              'claim': claim_path,
              'procedure': procedure_path,
              'proc_modifier': proc_modifier_path,
              'diagnosis': diagnosis_path,
              'drug': drug_path,
              'provider': provider_path}

start = datetime.now()
end = datetime.now()
total = end - start
duration = timedelta(seconds=total.total_seconds())
columns = ['data_source', 'run_date', 'batch_id', 'file_paths', 'duration', 'start', 'end']
data = [('IQVIA', datetime.now(), batch_id, json.dumps(file_paths), str(duration), start, end)]
run_meta_df = spark.sparkContext.parallelize(data).toDF(curated_ingest_run_schema)
save_run_meta(run_meta_df, generate_output_path('run_history'))

############################ START DRUGS
# drug_rdd = to_drug(claim_rdd).persist(StorageLevel.MEMORY_AND_DISK)
#
# currated_drug_df=drug_rdd.toDF(stage_drug_schema)\
#                          .filter(col('is_valid')==True)\
#                          .select(col('source_consumer_id'),
#                              col('source_org_oid'),
#                              col('start_date'),
#                              col('to_date'),
#                              col('code'),
#                              col('code_system'),
#                              col('desc'),
#                              col('source_desc'),
#                              col('strength'),
#                              col('form'),
#                              col('classification'),
#                              col('batch_id'))\
#                          .persist(StorageLevel.MEMORY_AND_DISK)
#
############################ END  DRUGS


# a = patient_claims_raw.select('PATIENT_ID', 'REFERRING_PROVIDER_ID', 'REFERRING_LAST_NM',
#                               'REFERRING_PROVIDER_ID_REF').distinct().sort(col("REFERRING_PROVIDER_ID").asc())
# df.write.parquet("s3a://bucket-name/shri/test.parquet", mode="overwrite")

# patient_claims_raw = patient_raw.join(claim_raw,patient_raw.PATIENT_ID == patient_raw.PATIENT_ID,"inner")
#
# patient_address_df = claim_raw.select(claim_raw.PATIENT_ID.alias('mrn'), claim_raw.PAT_ZIP3.alias('zip'))
#
# claim = claim_raw.select()
# # encounter = claim_raw.select()
# diagnosis = claim_raw.select()
# procedure = claim_raw.select()
#
#
# patient_raw.write.format("jdbc")\
#     .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#     .option("driver", "org.postgresql.Driver")\
#     .option("dbtable", "public.patient_raw") \
#     .option("user", "postgres")\
#     .option("password", "mysecretpassword")\
#     .mode("append")\
#     .save()
#
#
# from pyspark.sql.functions import explode
# df2 = patient_raw.select(patient_raw.PATIENT_ID,explode(patient_raw.DOB_YEAR))
#
# from pyspark import StorageLevel
#
# clinical_event = \
#     claim_raw.rdd.map(lambda x: [{'pat_id':x.asDict()['PATIENT_ID'], 'gender': x.asDict()['GENDER']}, {'pat_id':x.asDict()['PATIENT_ID'], 'gender': x.asDict()['DOB_YEAR']}]) \
#     .flatMap(lambda x: x).toDF().persist(StorageLevel.MEMORY_AND_DISK)
#
#
#
# class Patient:
#   def __init__(self, patient_id: str, dob_year: int, gender: str, tmp:str=None):
#     self.patient_id = patient_id
#     self.dob_year = dob_year
#     self.gender = gender
#     self.tmp = tmp
#
# def test(row):
#     if row.DOB_YEAR > 1:
#         pass
#     else:
#         pass
#     return Row(PATIENT_ID=row.PATIENT_ID, DOB_YEAR=1, GENDER=str(datetime.now().microsecond), TMP='asd')
#
# from pyspark.sql import Row
# Row(Row().asDict())
# # y=patient_raw.rdd.map(lambda r: [Patient(r.PATIENT_ID, r.DOB_YEAR, r.GENDER)]).flatMap(lambda x: x).toDF()
# x=patient_raw.rdd.map(lambda r: test(r)).toDF(patient_schema)
#
# x.write.format("jdbc")\
#     .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#     .option("driver", "org.postgresql.Driver")\
#     .option("dbtable", "public.patient_raw") \
#     .option("user", "postgres")\
#     .option("password", "mysecretpassword")\
#     .mode("append")\
#     .save()
#
# prdd = patient_raw.rdd
# prdd.collect()
# dp1 = prdd.toDF()
# dp = prdd.toDF(patient_schema)
# # # patient_raw = spark.read.options(inferSchema=True,delimiter=',', header=True).csv(patient_path)
# patient_df = patient_raw.withColumnRenamed("PATIENT_ID","patient_id")\
#                 .withColumnRenamed("PAT_BRTH_YR_NBR", "dob_year") \
#                 .withColumnRenamed("PAT_GENDER_CD", "gender")
# patient_df = patient_raw.withColumn("PATIENT_ID_RAW", patient_raw.PATIENT_ID)\
#     .withColumn("PATIENT_ID_RAW", when(patient_raw.PATIENT_ID == "10","ICD10").otherwise('MANNY'))\
#     .withColumnRenamed('PATIENT_ID_RAW', 'sour_pat_id')
#
# @udf(returnType=StringType())
# def to_dob(year: int) -> date:
#     dob = str(year)+'-01-01'
#     return datetime.strptime(dob, "%Y-%m-%d").date()
#
# udf_star_desc = udf(lambda year:to_dob(year),DateType())
# #
# # test = claims_raw.select(col('CLAIM_ID'), col('SVC_NBR'), col('CLAIM_TYP_CD'), col('SVC_FR_DT'), col('SVC_TO_DT'), col('HOSP_ADMT_DT'))
# test.withColumn("CLAIM_TYP_CD",udf_star_desc(col("CLAIM_TYP_CD")))  #.select(col('CLAIM_ID'), col('SVC_NBR'), col('CLAIM_TYP_CD'), col('SVC_FR_DT'), col('SVC_TO_DT')).first()
# # test.withColumn("CLAIM_TYP_CD",func1("CLAIM_TYP_CD"))
# # claims_raw.withColumn(col('CLAIM_ID'), lambda x: 'ABC')
# #
#
# from pyspark.sql.functions import when
# df2 = test.withColumn("type", when(test.CLAIM_TYP_CD == "I","INPATIENT")
#                                  .when(test.CLAIM_TYP_CD == "P","OUTPATIENT")
#                                  # .when(test.CLAIM_TYP_CD.isNull() ,"")
#                                  .otherwise(test.CLAIM_TYP_CD)) \
#     .withColumn('sub_type', when(test.HOSP_ADMT_DT.isNull() ,"OUTPATIENT"))
#
# # df.withColumn("Cureated Name", upperCase(col("Name"))) \
# # .show(truncate=False)
#
# test = claims_raw.select(col('CLAIM_ID'), col('SVC_NBR'), transform(col('CLAIM_TYP_CD'), lambda x: x), col('SVC_FR_DT'), col('SVC_TO_DT'))
# test2 = test.select(transform('CLAIM_TYP_CD', lambda x: 'ABC'))
# test2 = test.select(transform('CLAIM_TYP_CD', func1))
#
#
#
#     return sub_type
#
# test2 = test.map(lambda x:  (func1(x)))
#
# def test(df):
#     return
# test.transform()
#
#
#
#
#
#
# #### IGNORE
#
# #.schema(patient_schema)\
# patient_df = spark.read.option("delimiter", ",")\
#                         .option("inferSchema", True)\
#                         .option("header", True)\
#                         .csv(patient_path)
#
# patient_df.columns==['PATIENT_ID', 'PAT_BRTH_YR_NBR', 'PAT_GENDER_CD']
# patient_df.columns==['PAT_BRTH_YR_NBR', 'PATIENT_ID', 'PAT_GENDER_CD']
# patient_schema.fieldNames()
# patient_df.schema.fields
