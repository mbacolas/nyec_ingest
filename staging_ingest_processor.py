from iqvia.curated.transform_functions import *
from iqvia.curated.save_functions import *
from iqvia.common.load import *
from iqvia.common.schema import *
from pyspark.sql import SQLContext
from pyspark import StorageLevel
from iqvia.claim_service import claims_header
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


spark = SparkSession\
            .builder\
            .appName("test")\
            .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
            .config("fs.s3a.sse.enabled",True)\
            .config("fs.s3a.server-side-encryption-algorithm", "SSE-KMS")\
            .appName("IQVIA Ingest") \
            .getOrCreate()

conf = spark.conf
sqlContext = SQLContext(spark)

plan_path = conf.get("spark.nyec.iqvia.raw_plan_ingest_path")
patient_path = conf.get("spark.nyec.iqvia.raw_patient_ingest_path")
claim_path = conf.get("spark.nyec.iqvia.raw_claim_ingest_path")
procedure_path = conf.get("spark.nyec.iqvia.raw_procedure_ingest_path")
proc_modifier_path = conf.get("spark.nyec.iqvia.raw_procedure_modifier_ingest_path")
diagnosis_path = conf.get("spark.nyec.iqvia.raw_diagnosis_ingest_path")
drug_path = conf.get("spark.nyec.iqvia.raw_drug_ingest_path")
provider_path = conf.get("spark.nyec.iqvia.raw_provider_ingest_path")
iqvia_curated_s3_prefix = conf.get("spark.nyec.iqvia.iqvia_curated_s3_prefix")
batch_id = conf.get("spark.nyec.iqvia.batch_id", uuid.uuid4().hex[:12])
# raw_pro_provider_ingest_path = conf.get("spark.nyec.iqvia.raw_pro_provider_ingest_path")

def generate_output_path(data_set_name: str) -> str:
    return f'{iqvia_curated_s3_prefix}/{data_set_name}/'


rdd_test = spark.sparkContext.textFile(claim_path)
header_fields = rdd_test.first().split('|')
expected_header = claims_header()
assert expected_header == header_fields


### create data frames
date_created = datetime.now()
org_data = [(uuid.uuid4().hex[:12], "IQVIA", "IQVIA", "THIRD PARTY CLAIMS AGGREGATOR", True, batch_id, date_created)]
org_df = spark.createDataFrame(data=org_data,schema=raw_org_schema)

raw_plan_df = load_plan(spark, plan_path, raw_plan_schema).repartition('PLAN_ID')
raw_patient_df = load_patient(spark, patient_path, raw_patient_schema).repartition('PATIENT_ID')
raw_claim_df = load_claim(spark, claim_path, raw_claim_schema).repartition('PATIENT_ID_CLAIM').limit(1000000)
raw_proc_df = load_procedure(spark, procedure_path, raw_procedure_schema).repartition('PRC_CD', 'PRC_VERS_TYP_ID')
raw_proc_mod_1_df = load_procedure_modifier1(spark, proc_modifier_path, raw_procedure_modifier_schema).repartition('PRC1_MODR_CD')
raw_proc_mod_2_df = load_procedure_modifier2(spark, proc_modifier_path, raw_procedure_modifier_schema).repartition('PRC2_MODR_CD')
raw_proc_mod_3_df = load_procedure_modifier3(spark, proc_modifier_path, raw_procedure_modifier_schema).repartition('PRC3_MODR_CD')
raw_proc_mod_4_df = load_procedure_modifier4(spark, proc_modifier_path, raw_procedure_modifier_schema).repartition('PRC4_MODR_CD')
raw_diag_df = load_diagnosis(spark, diagnosis_path, raw_diag_schema).repartition('DIAG_CD', 'DIAG_VERS_TYP_ID')
raw_drug_df = load_drug(spark, drug_path, raw_drug_schema).repartition('NDC_CD')
provider_raw = load_provider(spark, provider_path, raw_provider_schema)
raw_rendering_provider_df = load_rendering_provider(provider_raw).repartition('RENDERING_PROVIDER_ID')
raw_referring_provider_df = load_referring_provider(provider_raw).repartition('REFERRING_PROVIDER_ID')

raw_plan_df.collect()
States = {"DL":"Delhi", "RJ":"Rajasthan", "KA":"Karnataka"}

broadcast_states = spark.sparkContext.broadcast(States)

### end of create data frames

### create stage patient DF
patient_rdd = raw_patient_df.withColumn('batch_id', lit(batch_id))\
                            .withColumn('source_org_oid', lit('IQVIA'))\
                            .withColumn('date_created', lit(date_created))\
                            .rdd\
                            .persist(StorageLevel.MEMORY_AND_DISK)

currated_patient_rdd = to_patient(patient_rdd).persist(StorageLevel.MEMORY_AND_DISK)
currated_patient_df = currated_patient_rdd.toDF(stage_patient_schema).persist(StorageLevel.MEMORY_AND_DISK)
save_patient(currated_patient_df, generate_output_path('patient'))
save_errors(currated_patient_rdd, PATIENT, generate_output_path('error'))

patient_rdd.unpersist()
currated_patient_rdd.unpersist()
currated_patient_df.unpersist()
print('------------------------>>>>>>> saved patient')
### create clinical events
patient_claims_raw_rdd = raw_patient_df.join(raw_claim_df, on=[raw_claim_df.PATIENT_ID_CLAIM == raw_patient_df.PATIENT_ID], how="inner") \
    .join(raw_diag_df, on=[raw_claim_df.DIAG_CD == raw_diag_df.DIAG_CD, raw_claim_df.DIAG_VERS_TYP_ID == raw_diag_df.DIAG_VERS_TYP_ID],
          how="left_outer") \
    .join(raw_proc_df, on=[raw_claim_df.PRC_CD == raw_proc_df.PRC_CD, raw_claim_df.PRC_VERS_TYP_ID == raw_proc_df.PRC_VERS_TYP_ID],
          how="left_outer") \
    .join(raw_proc_mod_1_df, on=[raw_claim_df.PRC1_MODR_CD == raw_proc_mod_1_df.PRC1_MODR_CD], how="left_outer") \
    .join(raw_proc_mod_2_df, on=[raw_claim_df.PRC2_MODR_CD == raw_proc_mod_2_df.PRC2_MODR_CD], how="left_outer") \
    .join(raw_proc_mod_3_df, on=[raw_claim_df.PRC3_MODR_CD == raw_proc_mod_3_df.PRC3_MODR_CD], how="left_outer") \
    .join(raw_proc_mod_4_df, on=[raw_claim_df.PRC4_MODR_CD == raw_proc_mod_4_df.PRC4_MODR_CD], how="left_outer") \
    .join(raw_drug_df, on=[raw_claim_df.NDC_CD == raw_drug_df.NDC_CD], how="left_outer") \
    .join(raw_rendering_provider_df,
          on=[raw_claim_df.RENDERING_PROVIDER_ID == raw_rendering_provider_df.RENDERING_PROVIDER_ID], how="left_outer") \
    .join(raw_referring_provider_df,
          on=[raw_claim_df.REFERRING_PROVIDER_ID == raw_referring_provider_df.REFERRING_PROVIDER_ID], how="left_outer") \
    .join(raw_plan_df,
          on=[raw_plan_df.PLAN_ID == raw_claim_df.PLAN_ID_CLAIM], how="left_outer") \
    .withColumn('batch_id', lit(batch_id)) \
    .withColumn('date_created', lit(date_created))\
    .rdd\
    .persist(StorageLevel.MEMORY_AND_DISK)

# raw_claim_df.repartition(col('PATIENT_ID_CLAIM')).sortWithinPartitions(col('PATIENT_ID_CLAIM')).show(1)

### create procedure
procedure_rdd = to_procedure(patient_claims_raw_rdd).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(procedure_rdd, PROCEDURE, generate_output_path('error'))
save_procedure_modifiers(procedure_rdd, generate_output_path('proceduremodifier'))

currated_df = procedure_rdd.toDF(stage_procedure_schema).persist(StorageLevel.MEMORY_AND_DISK)
save_procedure(currated_df, generate_output_path('procedure'))
currated_df.unpersist(False)
procedure_rdd.unpersist(False)
print('------------------------>>>>>>> saved procs')
### problems
problem_rdd = to_problem(patient_claims_raw_rdd).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(problem_rdd, PROBLEM, generate_output_path('error'))
admitting_problem_rdd = to_admitting_diagnosis(patient_claims_raw_rdd).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(admitting_problem_rdd, PROBLEM, generate_output_path('error'))

currated_df = problem_rdd.union(admitting_problem_rdd)\
                         .toDF(stage_problem_schema)\
                         .persist(StorageLevel.MEMORY_AND_DISK)

save_problem(currated_df, generate_output_path('diagnosis'))

currated_df.unpersist(False)
problem_rdd.unpersist(False)
admitting_problem_rdd.unpersist(False)
print('------------------------>>>>>>> saved problems')
####

### drug
drug_rdd = to_drug(patient_claims_raw_rdd).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(drug_rdd, DRUG, generate_output_path('error'))
currated_df = drug_rdd.toDF(stage_drug_schema).persist(StorageLevel.MEMORY_AND_DISK)

save_drug(currated_df, generate_output_path('product'))

currated_df.unpersist(False)
drug_rdd.unpersist(False)
####
print('------------------------>>>>>>> saved drugs')
### cost
cost_rdd = to_cost(patient_claims_raw_rdd).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(cost_rdd, COST, generate_output_path('error'))
currated_df = cost_rdd.toDF(stage_cost_schema).persist(StorageLevel.MEMORY_AND_DISK)

save_cost(currated_df, generate_output_path('cost'))

currated_df.unpersist(False)
cost_rdd.unpersist(False)
####
print('------------------------>>>>>>> saved cost')
### claim
claim_record_rdd = to_claim(patient_claims_raw_rdd).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(claim_record_rdd, CLAIM, generate_output_path('error'))
currated_df = claim_record_rdd.toDF(stage_claim_schema).persist(StorageLevel.MEMORY_AND_DISK)

save_claim(currated_df, generate_output_path('claim'))

currated_df.unpersist(False)
claim_record_rdd.unpersist(False)
###
print('------------------------>>>>>>> saved claim')
### org
save_org(org_df, generate_output_path('org'))
org_df.unpersist()
###


### provider
practitioner_rdd = to_practitioner(patient_claims_raw_rdd).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(practitioner_rdd, PRACTIONER, generate_output_path('error'))
currated_df = practitioner_rdd.toDF(stage_provider_schema).persist(StorageLevel.MEMORY_AND_DISK)

practitioner_role_df = to_practitioner_role_row(currated_df)

save_provider_role(currated_df, generate_output_path('provider_role'))
save_provider(practitioner_role_df, generate_output_path('provider'))

patient_claims_raw_rdd.unpersist(False)
currated_df.unpersist(False)
practitioner_rdd.unpersist(False)
practitioner_role_df.unpersist(False)
###










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
