from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
import uuid
from datetime import datetime
from iqvia.common.schema import *
from pyspark.sql.functions import current_date, datediff, months_between, years
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from cancer_screening_uc.validation import *


spark = SparkSession.builder \
                    .appName("Demo") \
                    .getOrCreate()

conf = spark.conf


# MPI_ID|
# First_Name|
# Middle_Name|
# Last_Name|
# Address_1|
# Address_2|
# city|
# state|
# zipcode|
# DayPhoneNumber|
# NightPhoneNumber|
# Gender|
# DOB|
# SSN
hixny_schema = StructType([ \
    StructField("MPI_ID", StringType(), False),
    StructField("First_Name", StringType(), False),
    StructField("Middle_Name", StringType(), False),
    StructField("Last_Name", StringType(), False),
    StructField("Address_1", StringType(), False),
    StructField("Address_2", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("zipcode", StringType(), False),
    StructField("DayPhoneNumber", StringType(), False),
    StructField("NightPhoneNumber", StringType(), False),
    StructField("Gender", StringType(), False),
    StructField("DOB", StringType(), False),
    StructField("SSN", StringType(), False)
])

smpi_schema = StructType([ \
    StructField("MPIID", StringType(), False),
    StructField("first_name", StringType(), False),
    StructField("last_name", StringType(), False),
    StructField("gender", StringType(), False),
    StructField("dob", StringType(), False),
    StructField("street_addr", StringType(), False),
    StructField("city", StringType(), False),
    StructField("state", StringType(), False),
    StructField("zipcode", StringType(), False),
    StructField("phone", StringType(), False),
    StructField("ssn", StringType(), False)
])


hixny_mpi_load_path = '/Users/emmanuel.bacolas/Downloads/iIT2163_Hixny_Data_20221010.csv'
smpi_load_path = '/Users/emmanuel.bacolas/Downloads/MPIID_with_demographics_20220926.csv'

# MPI_ID|First_Name|Middle_Name|Last_Name|Address_1|Address_2|city|state|zipcode|DayPhoneNumber|NightPhoneNumber|Gender|DOB|SSN
hixny_mpi_df = spark.read \
                   .schema(hixny_schema) \
                   .options(inferSchema=False, delimiter='|', header=True) \
                   .csv(hixny_mpi_load_path)\
                   .drop('Middle_Name')\
                   .drop('Address_2')\
                   .withColumnRenamed('DayPhoneNumber', 'smpi_day_phone')\
                   .withColumnRenamed('NightPhoneNumber', 'smpi_night_phone')\
                   .withColumnRenamed('Address_1', 'hixny_street_1')\
                   .withColumnRenamed('First_Name', 'hixny_first_name')\
                   .withColumnRenamed('Last_Name', 'hixny_last_name')\
                   .withColumnRenamed('city', 'hixny_city')\
                   .withColumnRenamed('state', 'hixny_state')\
                   .withColumnRenamed('zipcode', 'hixny_zipcode')\
                   .withColumnRenamed('Gender', 'hixny_gender')\
                   .withColumnRenamed('DOB', 'hixny_dob')\
                   .withColumnRenamed('SSN', 'hixny_ssn')


hixny_mpi_df.first()

smpi_df = spark.read \
                   .schema(smpi_schema) \
                   .options(inferSchema=False, delimiter=',', header=True) \
                   .csv(smpi_load_path)\
                   .withColumnRenamed('street_addr', 'smpi_street_1')\
                   .withColumnRenamed('phone', 'smpi_phone')\
                   .withColumnRenamed('first_name', 'smpi_first_name')\
                   .withColumnRenamed('last_name', 'smpi_last_name')\
                   .withColumnRenamed('gender', 'smpi_gender')\
                   .withColumnRenamed('dob', 'smpi_dob') \
                   .withColumnRenamed('city', 'smpi_city') \
                   .withColumnRenamed('state', 'smpi_state') \
                   .withColumnRenamed('zipcode', 'smpi_zipcode') \
                   .withColumnRenamed('SSN', 'smpi_ssn')

joined_mpi_df = smpi_df.join(hixny_mpi_df, how='inner')\
                        .where((smpi_df['MPIID']==hixny_mpi_df['MPI_ID']))


@udf(returnType=BooleanType())
def is_valid(error):
    import json
    e = json.loads(error)
    if len(e) > 0:
        return False
    else:
        return True

no_match_df = joined_mpi_df.withColumn('fuzzy_error', fuzzy_validation(col('MPIID'.upper()),
                                                                 col('smpi_phone'.upper()),
                                                                 col('smpi_first_name'.upper()),
                                                                 col('smpi_last_name'.upper()),
                                                                 col('smpi_street_1'.upper()),
                                                                 col('smpi_city'.upper()),
                                                                 col('smpi_state'.upper()),
                                                                 col('smpi_zipcode'.upper()),
                                                                 col('smpi_gender'.upper()),
                                                                 col('smpi_dob'.upper()),
                                                                 col('smpi_ssn'.upper()),
                                                                 col('MPI_ID'.upper()),
                                                                 col('smpi_day_phone'.upper()),
                                                                 col('smpi_night_phone'.upper()),
                                                                 col('hixny_first_name'.upper()),
                                                                 col('hixny_last_name'.upper()),
                                                                 col('hixny_street_1'.upper()),
                                                                 col('hixny_city'.upper()),
                                                                 col('hixny_state'.upper()),
                                                                 col('hixny_zipcode'.upper()),
                                                                 col('hixny_gender'.upper()),
                                                                 col('hixny_dob'.upper()),
                                                                 col('hixny_ssn'.upper())
                                                                ))\
                            .withColumn('is_fuzzy_valid', is_valid(col('fuzzy_error')))

no_match_df.show()

import editdistance
editdistance.eval('Joaane', 'Joane')

import editdistance
import phonetics
print(phonetics.soundex('Jooaaane'))
print(phonetics.soundex('Joane'))

print(phonetics.dmetaphone('Manny'))
print(phonetics.dmetaphone('Many'))

from fuzzywuzzy import fuzz
from fuzzywuzzy import process
print(fuzz.ratio("this is a test", "this is a test!"))
print(fuzz.ratio("2010-01-01", "2010-01-02"))
print(fuzz.ratio("22 27 35 street", "22-27 35 street"))
print(fuzz.ratio("22 27 35 street", "22-27 35 st"))
print(fuzz.partial_ratio("22 27 35 street", "22-27 35 st"))
fuzz.partial_ratio("YANKEES", "NEW YORK YANKEES")

# source ~/projects/nyec_ingest/venv/bin/activate
# export JAVA_HOME=/Users/emmanuel.bacolas/.sdkman/candidates/java/current/
bin/spark-submit  --master spark://127.0.0.1 --conf spark.driver.cores=5 --conf spark.executor.cores=5  --conf spark.driver.memory=1g --conf spark.executor.memory=8g  --py-files /Users/emmanuel.bacolas/projects/nyec_ingest/iqvia.zip,/Users/emmanuel.bacolas/projects/nyec_ingest/common.zip --jars /Users/emmanuel.bacolas/Downloads/postgresql-42.4.0.jar /Users/emmanuel.bacolas/projects/nyec_ingest/staging_ingest_processor.py
# bin/pyspark  --conf spark.driver.cores=2 --conf spark.executor.cores=8  --conf spark.driver.memory=1g --conf spark.executor.memory=10g --py-files /Users/emmanuel.bacolas/projects/nyec_ingest/cancer_screening_uc.zip
