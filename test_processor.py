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

patient_path = conf.get("spark.nyec.iqvia.patient_ingest_path")
claim_path = conf.get("spark.nyec.iqvia.claims_ingest_path")
output_path = 's3://nyce-iqvia/processed-parquet/patient_test/'

rdd_test = spark.sparkContext.textFile('s3://nyce-iqvia/adhoc/raw/OrganizationData.csv')
header_fields = rdd_test.first().split(',')
print(header_fields)
expected_header = claims_header()
if expected_header == header_fields:
    print('********************* PASSED *********************')
else:
    print('********************* FAILED *********************')

batch_id = conf.get("spark.nyec.iqvia.batch_id", uuid.uuid4().hex[:12])
raw_patient_df = load_patient(spark, patient_path, raw_patient_schema)
raw_claim_df = load_claim(spark, claim_path, raw_claim_schema)

patient_claims_raw_rdd = raw_patient_df.join(raw_claim_df, on=[raw_claim_df.PATIENT_ID_CLAIM == raw_patient_df.PATIENT_ID], how="inner")\
                                .withColumn('batch_id', lit(batch_id)) \
                                .write.parquet(output_path, mode='overwrite')
