from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType, MapType, BooleanType
from pyspark.sql.functions import *
from datetime import datetime, timedelta, date
from pyspark import StorageLevel
from schema import *
from functions import *


spark = SparkSession.builder \
      .master("local[1]") \
      .appName("N_A") \
      .getOrCreate()

conf = spark.conf

plan_path = conf.get("spark.nyec.iqvia.plan_ingest_path")
patient_path = conf.get("spark.nyec.iqvia.patient_ingest_path")
claim_path = conf.get("spark.nyec.iqvia.claims_ingest_path")
procedure_path = conf.get("spark.nyec.iqvia.procedure_ingest_path")
proc_modifier_path = conf.get("spark.nyec.iqvia.proc_modifier_ingest_path")
diagnosis_path = conf.get("spark.nyec.iqvia.diagnosis_ingest_path")
drug_path = conf.get("spark.nyec.iqvia.drug_ingest_path")
provider_path = conf.get("spark.nyec.iqvia.provider_ingest_path")
pro_provider_path = conf.get("spark.nyec.iqvia.pro_provider_ingest_path")

plan_raw = spark.read.schema(raw_plan_schema)\
                    .options(inferSchema=False,delimiter=',', header=True)\
                    .csv(plan_path)\
                    .persist(StorageLevel.MEMORY_AND_DISK)

patient_raw = spark.read.schema(raw_patient_schema)\
                    .options(inferSchema=False,delimiter=',', header=True)\
                    .csv(patient_path)\
                    .persist(StorageLevel.MEMORY_AND_DISK)

claim_raw = spark.read.schema(raw_claim_schema)\
                    .options(inferSchema=False,delimiter=',', header=True)\
                    .csv(claim_path).withColumn('source_org_oid', lit('IQVIA'))\
                    .persist(StorageLevel.MEMORY_AND_DISK)

proc_raw = spark.read.schema(raw_procedure_schema)\
                    .options(inferSchema=False,delimiter=',', header=True)\
                    .csv(procedure_path)\
                    .persist(StorageLevel.MEMORY_AND_DISK)

proc_modifier_raw = spark.read.schema(raw_procedure_modifier_schema)\
                    .options(inferSchema=False,delimiter=',', header=True)\
                    .csv(proc_modifier_path)\
                    .persist(StorageLevel.MEMORY_AND_DISK)

diag_raw = spark.read.schema(raw_diag_schema)\
                    .options(inferSchema=False,delimiter=',', header=True)\
                    .csv(diagnosis_path)\
                    .persist(StorageLevel.MEMORY_AND_DISK)

drug_raw = spark.read.schema(raw_drug_schema)\
                    .options(inferSchema=False,delimiter=',', header=True)\
                    .csv(drug_path)\
                    .persist(StorageLevel.MEMORY_AND_DISK)

provider_raw = spark.read.schema(raw_provider_schema)\
                    .options(inferSchema=False,delimiter=',', header=True)\
                    .csv(provider_path)\
                    .persist(StorageLevel.MEMORY_AND_DISK)

pro_provider_raw = spark.read.schema(raw_pro_provider_schema)\
                    .options(inferSchema=False,delimiter=',', header=True)\
                    .csv(pro_provider_path)\
                    .persist(StorageLevel.MEMORY_AND_DISK)

a = patient_raw.join(claim_raw, on=[patient_raw.PATIENT_ID==claim_raw.PATIENT_ID, patient_raw.GENDER==claim_raw.PATIENT_ID], how="inner")


patient_claims_raw = patient_raw.join(claim_raw,claim_raw.PATIENT_ID == patient_raw.PATIENT_ID,"inner")\
                                .join(claim_raw,claim_raw.PLAN_ID == plan_raw.PLAN_ID,"inner")\
                                .join(claim_raw,claim_raw.DIAG_CD == diag_raw.DIAG_CD,"inner")\
                                .join(claim_raw,claim_raw.PRC_CD == proc_raw.PRC_CD,"inner")\
                                .join(claim_raw,claim_raw.PRC1_MODR_CD == proc_modifier_raw.PRC_MODR_CD,"inner")\
                                # .join(claim_raw,claim_raw.PRC2_MODR_CD == proc_modifier_raw.PRC_MODR_CD,"inner")\
                                # .join(claim_raw,claim_raw.PRC3_MODR_CD == proc_modifier_raw.PRC_MODR_CD,"inner")\
                                # .join(claim_raw,claim_raw.PRC4_MODR_CD == proc_modifier_raw.PRC_MODR_CD,"inner")
proc_raw.join()
proc_rdd = to_procedure(proc_raw.rdd)
print(proc_rdd.first())

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
