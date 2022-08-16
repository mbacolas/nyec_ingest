import pyspark
from pyspark import *
from pyspark import context
from pyspark.sql import context

sc = pyspark.SparkContext
# spark is from the previous example
# sc = spark.sparkContext

# A CSV dataset is pointed to by path.
# The path can be either a single CSV file or a directory of CSV files
patient_path = "/tmp/patient.csv"
patient_parquet_path = "/tmp/patient.parquet"
claim_path = "/tmp/claim.csv"
claim_parquet_path = "/tmp/claim.parquet"


# Read a csv with delimiter, the default delimiter is ","

patient_df = pyspark.read.option("delimiter", ",").option("header", True).csv(patient_path)
# patient_df = spark.read.parquet(patient_parquet_path)
# claim_df = spark.read.parquet(claim_parquet_path)
claim_df = pyspark.read.option("delimiter", ",").option("header", True).csv(claim_path)

patient_df.show(1)
patient_df.columns
claim_df.show(1)

patient = patient_df.join(claim_df,patient_df.PATIENT_ID ==  claim_df.PATIENT_ID,"inner").show(truncate=False)
patient_df = claim_df.select('PATIENT_ID', 'PAT_ZIP3').show()
# df3.write.csv("output")
