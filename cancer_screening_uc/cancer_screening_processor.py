from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
import uuid
from datetime import datetime
from iqvia.common.schema import *
from pyspark.sql.functions import current_date, datediff, months_between, years
from iqvia.common.load import *


spark = SparkSession.builder \
                    .appName("GZIP To CSV") \
                    .getOrCreate()

conf = spark.conf

patient_path = 's3://nyce-iqvia/curated/delta_patient/'
procedure_path = 's3://nyce-iqvia/curated/delta_procedure/'
colorectal_cancer_screening_path = 's3://nyce-iqvia/raw/csv/colorectal_cancer_screening.csv'

claim_df = load_df(spark, load_path='claim', schema=None, file_delimiter=None, format_type='parquet')

patient_df = load_df(spark, load_path=patient_path, schema=None, file_delimiter=None, format_type='parquet')\
                .select('source_org_oid', 'source_consumer_id', 'dob', 'gender')\
                .limit(100)

procedure_df = load_df(spark, load_path='procedure', schema=None, file_delimiter=None, format_type='parquet')\
                    .select('source_org_oid', 'source_consumer_id', 'start_date', 'code', 'code_system')\
                    .limit(100)

diagnosis_df = load_df(spark, load_path='problem', schema=None, file_delimiter=None, format_type='parquet')\
                    .select('source_org_oid', 'source_consumer_id', 'start_date', 'code', 'code_system')\
                    .limit(100)

colorectal_cancer_screening_df = load_df(spark, load_path=colorectal_cancer_screening_path, schema=None, file_delimiter=',', infer_schema=True)

fobt_df = procedure_df.join(colorectal_cancer_screening_df, how='inner')\
                      .where((procedure_df['code']==colorectal_cancer_screening_df['code']) &
                            (colorectal_cancer_screening_df.code_system==procedure_df.code_system) &
                            (colorectal_cancer_screening_df.test_name == 'FOBT') &
                            (months_between(procedure_df.start_date, current_date()) <= 12))\
                            .select('source_org_oid', 'source_consumer_id')

colonoscopy_df = procedure_df.join(colorectal_cancer_screening_df, how='inner')\
                      .where((procedure_df['code']==colorectal_cancer_screening_df['code']) &
                            (colorectal_cancer_screening_df.code_system==procedure_df.code_system) &
                            (colorectal_cancer_screening_df.test_name == 'Colonoscopy') &
                            (months_between(procedure_df.start_date, current_date()) <= 120))\
                            .select('source_org_oid', 'source_consumer_id')

sigmoidocopy_df = procedure_df.join(colorectal_cancer_screening_df, how='inner')\
                      .where((procedure_df['code']==colorectal_cancer_screening_df['code']) &
                            (colorectal_cancer_screening_df.code_system==procedure_df.code_system) &
                            (colorectal_cancer_screening_df.test_name == 'Flexible Sigmoidocopy') &
                            (months_between(procedure_df.start_date, current_date()) <= 60))\
                            .select('source_org_oid', 'source_consumer_id')

fit_dna_df = procedure_df.join(colorectal_cancer_screening_df, how='inner')\
                      .where((procedure_df['code']==colorectal_cancer_screening_df['code']) &
                            (colorectal_cancer_screening_df.code_system==procedure_df.code_system) &
                            (colorectal_cancer_screening_df.test_name == 'FIT-DNA') &
                            (months_between(procedure_df.start_date, current_date()) <= 36))\
                            .select('source_org_oid', 'source_consumer_id')

tomo_df = procedure_df.join(colorectal_cancer_screening_df, how='inner')\
                      .where((procedure_df['code']==colorectal_cancer_screening_df['code']) &
                            (colorectal_cancer_screening_df.code_system==procedure_df.code_system) &
                            (colorectal_cancer_screening_df.test_name == 'Tomo') &
                            (months_between(procedure_df.start_date, current_date()) <= 60))\
                            .select('source_org_oid', 'source_consumer_id')

numerator_df = fobt_df.union(colonoscopy_df)\
                    .union(sigmoidocopy_df)\
                    .union(fit_dna_df)\
                    .union(tomo_df)\
                    .dropDuplicates()


# denom inclusion
denom_inclusion_df = patient_df.join(procedure_df, how='inner')\
                               .join(colorectal_cancer_screening_df, how='inner')\
                               .where((patient_df.source_org_oid == procedure_df.source_org_oid) &
                                        (patient_df.source_consumer_id == procedure_df.source_consumer_id) &
                                        (months_between(patient_df.dob, current_date()) >= 50) &
                                        (months_between(patient_df.dob, current_date()) <= 75) &
                                        (colorectal_cancer_screening_df.code == procedure_df.code) &
                                        (colorectal_cancer_screening_df.code_system == procedure_df.code_system) &
                                        (colorectal_cancer_screening_df.test_name == 'Denom Inclusion'))\
                               .select('source_org_oid', 'source_consumer_id')\

denom_exclusion_proc_df = procedure_df.join(colorectal_cancer_screening_df, how='inner')\
                                      .where((procedure_df['code']==colorectal_cancer_screening_df['code']) &
                                            (colorectal_cancer_screening_df.code_system==procedure_df.code_system) &
                                            (colorectal_cancer_screening_df.test_name == 'Denom Exclusion') &
                                            ((colorectal_cancer_screening_df.code_system).isin('CPT', 'HCPCS'))
                                             )\
                                      .select('source_org_oid', 'source_consumer_id')

denom_exclusion_diag_df = diagnosis_df.join(colorectal_cancer_screening_df, how='inner')\
                                      .where((diagnosis_df['code']==colorectal_cancer_screening_df['code']) &
                                            (colorectal_cancer_screening_df.code_system==diagnosis_df.code_system) &
                                            (colorectal_cancer_screening_df.test_name == 'Denom Exclusion') &
                                            (colorectal_cancer_screening_df.code_system == 'ICD10')
                                             )\
                                      .select('source_org_oid', 'source_consumer_id')

denom_exclusion_claim_df = claim_df.join(patient_df, how='inner')\
                                      .where((claim_df['code'] == patient_df['code']) &
                                            (colorectal_cancer_screening_df.code_system==diagnosis_df.code_system) &
                                            (colorectal_cancer_screening_df.test_name == 'Denom Exclusion') &
                                            (colorectal_cancer_screening_df.code_system == 'ICD10')
                                             )\
                                      .select('source_org_oid', 'source_consumer_id')