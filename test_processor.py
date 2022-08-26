from iqvia.curated.transform_functions import *
from iqvia.curated.save_functions import *
from iqvia.common.load import *
from iqvia.common.schema import *
from pyspark.sql import SQLContext
from pyspark import StorageLevel
from iqvia.claim_service import claims_header
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark import StorageLevel
from iqvia.claim_service import claims_header

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
# sqlContext = SQLContext(spark)

# patient_path = conf.get("spark.nyec.iqvia.patient_ingest_path")
# claim_path = conf.get("spark.nyec.iqvia.claims_ingest_path")


n_salt_bins = 100
import random
from random import randint



raw_claim_df = load_claim(spark, '/Users/emmanuel.bacolas/Downloads/nyec/iqvia_data/factdx/FactDx_202004.dat.gz', raw_claim_schema, 'csv') #.limit(10000)
# raw_claim_df = load_claim(spark, '/tmp/claim', raw_claim_schema, 'parquet').limit(10000)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
# windowSpec  = Window.partitionBy("PATIENT_ID_CLAIM").orderBy("PATIENT_ID_CLAIM")

claims_df = raw_claim_df.select(col('PATIENT_ID_CLAIM'), col('CLAIM_ID'), col('PRC_CD'), col('SVC_FR_DT'))
salted_df = claims_df.withColumn("salt", (10*rand()).cast(IntegerType()))
rand()
# x = claims_df.partitionBy(col('PATIENT_ID_CLAIM'))
# x = claims_df.rdd.map(lambda r: (r.PATIENT_ID_CLAIM, r)).partitionBy(2)
# x = claims_df.rdd.partitionBy(10, lambda r: (r.PATIENT_ID_CLAIM, r)).first()
x = salted_df.repartition(salted_df.PATIENT_ID_CLAIM, salted_df.salt)
salted_df.repartition(salted_df.PATIENT_ID_CLAIM, salted_df.salt).explain()
y = salted_df.select(col('PATIENT_ID_CLAIM'), col('CLAIM_ID'), col('PRC_CD'), col('SVC_FR_DT'), col('salt'), spark_partition_id().alias("partition_id")).repartition(salted_df.PATIENT_ID_CLAIM, salted_df.salt)
y.explain()
salted_df.select('PATIENT_ID_CLAIM', 'salt').sort(('PATIENT_ID_CLAIM')).show(1000)
y.select('partition_id').distinct().count()


salted_df = claims_df.withColumn("salt", (100*rand()).cast(IntegerType()))
# x.rdd.getNumPartitions()
# x.select('PATIENT_ID_CLAIM').distinct().count()
# x = claims_df.repartition(10)
y = x.select(col('PATIENT_ID_CLAIM'), col('CLAIM_ID'), col('PRC_CD'), col('SVC_FR_DT'), spark_partition_id().alias("partition_id"))
y.select('partition_id').distinct().show(1000)
z = y.withColumn("row_number", row_number().over(windowSpec))

z.select('PATIENT_ID_CLAIM', 'row_number', 'partition_id').distinct().show(1000)
# y.show(1000)
# z = y.withColumn("row_number", row_number().over(windowSpec))
# z.select('PATIENT_ID_CLAIM', 'row_number', 'partition_id').distinct().show(1000)

