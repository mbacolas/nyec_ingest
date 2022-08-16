from functions import *
from iqvia.common.load import *
from iqvia.common.schema import *

PATIENT = 'PATIENT'
PROCEDURE = 'PROCEDURE'

spark = SparkSession.builder \
    .appName("IQVIA Ingest") \
    .getOrCreate()

conf = spark.conf
# --conf "spark.nyec.iqvia.plans_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/plans.csv"
# --conf "spark.nyec.iqvia.diags_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/diagnosis.csv"
# --conf "spark.nyec.iqvia.procs_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/procedure.csv"
# --conf "spark.nyec.iqvia.drugs_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/drug.csv"
# --conf "spark.nyec.iqvia.proc_mod_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/proc_modfier.csv"
# --conf "spark.nyec.iqvia.providers_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/provider.csv"
# --conf "spark.nyec.iqvia.claims_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/facts_dx.csv"
# --conf "spark.nyec.iqvia.patients_ingest_path=/Users/emmanuel.bacolas/tmp/iqvia_data/patients.csv"

plan_path = conf.get("spark.nyec.iqvia.plans_ingest_path")
patient_path = conf.get("spark.nyec.iqvia.patients_ingest_path")
claim_path = conf.get("spark.nyec.iqvia.claims_ingest_path")
procedure_path = conf.get("spark.nyec.iqvia.procs_ingest_path")
proc_modifier_path = conf.get("spark.nyec.iqvia.proc_mod_ingest_path")
diagnosis_path = conf.get("spark.nyec.iqvia.diags_ingest_path")
drug_path = conf.get("spark.nyec.iqvia.drugs_ingest_path")
provider_path = conf.get("spark.nyec.iqvia.providers_ingest_path")
batch_id = conf.get("spark.nyec.iqvia.batch_id", uuid.uuid4().hex[:12])
# pro_provider_path = conf.get("spark.nyec.iqvia.pro_provider_ingest_path")
# "s3a://sparkbyexamples/csv/zipcodes.csv"
#
# spark.sparkContext
#      .hadoopConfiguration.set("fs.s3a.access.key", "awsaccesskey value")
# service)
#  // Replace Key with your AWS secret key (You can find this on IAM
# spark.sparkContext
#      .hadoopConfiguration.set("fs.s3a.secret.key", "aws secretkey value")
# spark.sparkContext
#       .hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
# spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "mykey")
# spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "mysecret")
# spark._jsc.hadoopConfiguration().set("fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")
# spark._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
# spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.BasicAWSCredentialsProvider")
# spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "eu-west-3.amazonaws.com")


### create data frames
org_data = [("IQVIA", "IQVIA", "THIRD PARTY CLAIMS AGGREGATOR", True)]
org_df = spark.createDataFrame(data=org_data,schema=raw_org_schema)

raw_df = load_plan(spark, plan_path, raw_plan_schema)
plan_raw = raw_df.withColumn('org_type', lit('THIRD PARTY CLAIMS AGGREGATOR')).withColumn('plan_status', lit(True))

patient_raw = spark.read.schema(raw_patient_schema) \
    .options(inferSchema=False, delimiter=',', header=True) \
    .csv(patient_path) \
    .withColumn('consumer_type', lit('MEMBER')) \
    .withColumn('consumer_status', lit(True)) \
    .withColumn('source_org_oid', lit('IQVIA')) \
    .withColumn('batch_id', lit(batch_id))

claim_raw = spark.read.schema(raw_claim_schema) \
    .options(inferSchema=False, delimiter=',', header=True) \
    .csv(claim_path) \
    .withColumn("PATIENT_ID_CLAIM", col("PATIENT_ID")) \
    .withColumn('source_org_oid', lit('IQVIA')) \
    .drop(col("PATIENT_ID"))

proc_raw = spark.read.schema(raw_procedure_schema) \
    .options(inferSchema=False, delimiter=',', header=True) \
    .csv(procedure_path)

proc_modifier_raw = spark.read.schema(raw_procedure_modifier_schema) \
    .options(inferSchema=False, delimiter=',', header=True) \
    .csv(proc_modifier_path)

proc_modifier_raw_1 = proc_modifier_raw \
    .select(col('PRC_MODR_CD').alias('PRC1_MODR_CD'), col('PRC_MODR_DESC').alias('PRC1_MODR_DESC'))

proc_modifier_raw_2 = proc_modifier_raw \
    .select(col('PRC_MODR_CD').alias('PRC2_MODR_CD'), col('PRC_MODR_DESC').alias('PRC2_MODR_DESC'))

proc_modifier_raw_3 = proc_modifier_raw \
    .select(col('PRC_MODR_CD').alias('PRC3_MODR_CD'), col('PRC_MODR_DESC').alias('PRC3_MODR_DESC'))

proc_modifier_raw_4 = proc_modifier_raw \
    .select(col('PRC_MODR_CD').alias('PRC4_MODR_CD'), col('PRC_MODR_DESC').alias('PRC4_MODR_DESC'))

diag_raw = spark.read.schema(raw_diag_schema) \
    .options(inferSchema=False, delimiter=',', header=True) \
    .csv(diagnosis_path)

drug_raw = spark.read.schema(raw_drug_schema) \
    .options(inferSchema=False, delimiter=',', header=True) \
    .csv(drug_path)

provider_raw = spark.read.schema(raw_provider_schema) \
    .options(inferSchema=False, delimiter=',', header=True) \
    .csv(provider_path)

rendering_provider_raw = provider_raw \
    .select(col('PROVIDER_ID').alias('RENDERING_PROVIDER_ID_REF'),
            col('PROVIDER_TYP_ID').alias('RENDERING_PROVIDER_TYP_ID'),
            col('ORG_NM').alias('RENDERING_ORG_NM'),
            col('IMS_RXER_ID').alias('RENDERING_IMS_RXER_ID'),
            col('LAST_NM').alias('RENDERING_LAST_NM'),
            col('FIRST_NM').alias('RENDERING_FIRST_NM'),
            col('ADDR_LINE1_TXT').alias('RENDERING_ADDR_LINE1_TXT'),
            col('ADDR_LINE2_TXT').alias('RENDERING_ADDR_LINE2_TXT'),
            col('CITY_NM').alias('RENDERING_CITY_NM'),
            col('ST_CD').alias('RENDERING_ST_CD'),
            col('ZIP').alias('RENDERING_ZIP'),
            col('PRI_SPCL_CD').alias('RENDERING_PRI_SPCL_CD'),
            col('PRI_SPCL_DESC').alias('RENDERING_PRI_SPCL_DESC'),
            col('ME_NBR').alias('RENDERING_ME_NBR'),
            col('NPI').alias('RENDERING_NPI'))

referring_provider_raw = provider_raw \
    .select(col('PROVIDER_ID').alias('REFERRING_PROVIDER_ID_REF'),
            col('PROVIDER_TYP_ID').alias('REFERRING_PROVIDER_TYP_ID'),
            col('ORG_NM').alias('REFERRING_ORG_NM'),
            col('IMS_RXER_ID').alias('REFERRING_IMS_RXER_ID'),
            col('LAST_NM').alias('REFERRING_LAST_NM'),
            col('FIRST_NM').alias('REFERRING_FIRST_NM'),
            col('ADDR_LINE1_TXT').alias('REFERRING_ADDR_LINE1_TXT'),
            col('ADDR_LINE2_TXT').alias('REFERRING_ADDR_LINE2_TXT'),
            col('CITY_NM').alias('REFERRING_CITY_NM'),
            col('ST_CD').alias('REFERRING_ST_CD'),
            col('ZIP').alias('REFERRING_ZIP'),
            col('PRI_SPCL_CD').alias('REFERRING_PRI_SPCL_CD'),
            col('PRI_SPCL_DESC').alias('REFERRING_PRI_SPCL_DESC'),
            col('ME_NBR').alias('REFERRING_ME_NBR'),
            col('NPI').alias('REFERRING_NPI'))

# pro_provider_raw = spark.read.schema(raw_pro_provider_schema)\
#                     .options(inferSchema=False,delimiter=',', header=True)\
#                     .csv(pro_provider_path)\
#                     .persist(StorageLevel.MEMORY_AND_DISK)
### end of create data frames

### create org
org_df.write.parquet('/tmp/org', mode='overwrite')

### create patient
patient_rdd = patient_raw.rdd.persist(StorageLevel.MEMORY_AND_DISK)
currated_patient_df = to_patient(patient_rdd).toDF(stage_patient_schema)

valid_patients_df = currated_patient_df.filter(currated_patient_df.is_valid==True)\
                                       .select(col('source_consumer_id'),
                                               col('source_org_oid'),
                                               col('type'),
                                               col('active'),
                                               col('dob'),
                                               col('gender'),
                                               col('batch_id'))

valid_patients_df.write.parquet('/tmp/patients', mode='overwrite')

save_errors(patient_rdd, PATIENT)

# currated_patient_df.filter(currated_patient_df.is_valid==False)\
#                     .write.format("jdbc")\
#                     .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#                     .option("driver", "org.postgresql.Driver")\
#                     .option("dbtable", "public.error") \
#                     .option("user", "postgres")\
#                     .option("password", "mysecretpassword")\
#                     .mode("append")\
#                     .save()

### create clinical events
patient_claims_raw = patient_raw.join(claim_raw, on=[claim_raw.PATIENT_ID_CLAIM == patient_raw.PATIENT_ID], how="inner") \
    .join(plan_raw, on=[claim_raw.PLAN_ID == plan_raw.PLAN_ID], how="left_outer") \
    .join(diag_raw, on=[claim_raw.DIAG_CD == diag_raw.DIAG_CD, claim_raw.DIAG_VERS_TYP_ID == diag_raw.DIAG_VERS_TYP_ID],
          how="left_outer") \
    .join(proc_raw, on=[claim_raw.PRC_CD == proc_raw.PRC_CD, claim_raw.PRC_VERS_TYP_ID == proc_raw.PRC_VERS_TYP_ID],
          how="left_outer") \
    .join(proc_modifier_raw_1, on=[claim_raw.PRC1_MODR_CD == proc_modifier_raw_1.PRC1_MODR_CD], how="left_outer") \
    .join(proc_modifier_raw_2, on=[claim_raw.PRC2_MODR_CD == proc_modifier_raw_2.PRC2_MODR_CD], how="left_outer") \
    .join(proc_modifier_raw_3, on=[claim_raw.PRC3_MODR_CD == proc_modifier_raw_3.PRC3_MODR_CD], how="left_outer") \
    .join(proc_modifier_raw_4, on=[claim_raw.PRC4_MODR_CD == proc_modifier_raw_4.PRC4_MODR_CD], how="left_outer") \
    .join(drug_raw, on=[claim_raw.NDC_CD == drug_raw.NDC_CD], how="left_outer") \
    .join(rendering_provider_raw,
          on=[claim_raw.RENDERING_PROVIDER_ID == rendering_provider_raw.RENDERING_PROVIDER_ID_REF], how="left_outer") \
    .join(referring_provider_raw,
          on=[claim_raw.REFERRING_PROVIDER_ID == referring_provider_raw.REFERRING_PROVIDER_ID_REF], how="left_outer") \
    .withColumn('batch_id', lit(batch_id)) \
    .persist(StorageLevel.MEMORY_AND_DISK)
# .repartition('PATIENT_ID')\

claim_rdd = patient_claims_raw.rdd.persist(StorageLevel.MEMORY_AND_DISK)

### create procedure
procedure_rdd = to_procedure(claim_rdd).persist(StorageLevel.MEMORY_AND_DISK)
# stage_procs_df = procedure_rdd.toDF(stage_procedure_schema).persist(StorageLevel.MEMORY_AND_DISK)
save_errors(procedure_rdd, PROCEDURE)
# procedure_rdd.filter(lambda r: r.is_valid == False)\
#                            .map(lambda r: Row(batch_id=r.batch_id,
#                                                type='PROCEDURE',
#                                                row_errors=json.dumps(r.error),
#                                                row_value=json.dumps(r.asDict()),
#                                                date_created=datetime.now()))\
#                            .toDF(error_schema)\
#                            .write.format("jdbc")\
#                            .option("url", "jdbc:postgresql://localhost:5432/postgres") \
#                            .option("driver", "org.postgresql.Driver")\
#                            .option("dbtable", "public.error") \
#                            .option("user", "postgres")\
#                            .option("password", "mysecretpassword")\
#                            .mode("append")\
#                            .save()

valid_procs = procedure_rdd.toDF(stage_procedure_schema)\
                            .filter(col('is_valid') == True)\
                            .select(col('source_consumer_id'),
                                     col('source_org_oid'),
                                     col('start_date'),
                                     col('to_date'),
                                     col('code'),
                                     col('code_system'),
                                     col('revenue_code'),
                                     col('desc'),
                                     col('source_desc'),
                                     col('mod'),
                                     col('batch_id'))\
                            .persist(StorageLevel.MEMORY_AND_DISK)

valid_procs.select(col('source_org_oid'),
                   col('source_consumer_id'),
                   col('start_date'),
                   col('to_date'),
                   col('code'),
                   col('code_system'),
                   col('mod'))\
           .write.parquet('/tmp/procedures', mode='overwrite')
# in_valid_rows.write.parquet(f's3a://bucket-name/iqvia/stage/{date.today()}test.parquet', mode='overwrite')



patient_plan_rdd = patient_raw.join(claim_raw, on=[patient_raw.PLAN_ID == plan_raw.PLAN_ID], how="inner") \
    .rdd \
    .persist(StorageLevel.MEMORY_AND_DISK)


eli_rdd = to_eligibility(patient_plan_rdd)

patient_raw.join(claim_raw, on=[claim_raw.PATIENT_ID == patient_raw.PATIENT_ID], how="inner") \
    .join(claim_raw, on=[claim_raw.PLAN_ID == plan_raw.PLAN_ID], how="inner")







problem_rdd = to_problem(claim_rdd).persist(StorageLevel.MEMORY_AND_DISK)


############################ START DRUGS
drug_rdd = to_drug(claim_rdd).persist(StorageLevel.MEMORY_AND_DISK)

currated_drug_df=drug_rdd.toDF(stage_drug_schema)\
                         .filter(col('is_valid')==True)\
                         .select(col('source_consumer_id'),
                             col('source_org_oid'),
                             col('start_date'),
                             col('to_date'),
                             col('code'),
                             col('code_system'),
                             col('desc'),
                             col('source_desc'),
                             col('strength'),
                             col('form'),
                             col('classification'),
                             col('batch_id'))\
                         .persist(StorageLevel.MEMORY_AND_DISK)


############################ END  DRUGS


errors.write.format("jdbc")\
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("driver", "org.postgresql.Driver")\
    .option("dbtable", "public.error") \
    .option("user", "postgres")\
    .option("password", "mysecretpassword")\
    .mode("append")\
    .save()

patient_claims_raw.unpersist(True)
patient_claims_raw.count()
a = patient_claims_raw.select('PATIENT_ID', 'REFERRING_PROVIDER_ID', 'REFERRING_LAST_NM',
                              'REFERRING_PROVIDER_ID_REF').distinct().sort(col("REFERRING_PROVIDER_ID").asc())
claim_rdd = patient_claims_raw.rdd
proc_raw.join()
proc_rdd = to_procedure(proc_raw.rdd)
print(proc_rdd.first())

df.write.parquet("s3a://bucket-name/shri/test.parquet", mode="overwrite")

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
