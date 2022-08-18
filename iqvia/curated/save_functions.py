from pyspark import RDD
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
import uuid
from common.functions import *
from iqvia.common.schema import error_schema


def save_errors(rdd: RDD, row_type: str):
    rdd.filter(lambda r: r.is_included == False) \
        .map(lambda r: Row(batch_id=r.batch_id,
                           type=row_type,
                           row_errors=json.dumps(r.error),
                           row_value=json.dumps(r.asDict()),
                           date_created=datetime.now())) \
        .toDF(error_schema) \
        .write\
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.error") \
        .option("user", "postgres") \
        .option("password", "mysecretpassword") \
        .mode("append") \
        .save()

def save_org(org_df: DataFrame, output_path: str):
    org_df.select(col('source_org_oid'),
            col('name'),
            col('type'),
            col('active')) \
        .write.parquet(output_path, mode='overwrite')

def save_patient(currated_patient_df: DataFrame, output_path: str):
    currated_patient_df.filter(currated_patient_df.is_valid == True) \
        .select(col('source_consumer_id'),
                col('source_org_oid'),
                col('type'),
                col('active'),
                col('dob'),
                col('gender'),
                col('batch_id'))\
        .repartition(col('source_org_oid'), col('source_consumer_id'))\
        .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id'))\
        .write.parquet(output_path, mode='overwrite')


def save_procedure(currated_procedure_df: DataFrame, output_path: str):
    currated_procedure_df.filter(currated_procedure_df.is_valid == True) \
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
                col('batch_id')) \
        .repartition(col('source_org_oid'), col('source_consumer_id'))\
        .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id'), col('start_date'), col('code_system'),
                              col('code'))\
        .write.parquet(output_path, mode='overwrite')


def save_procedure_modifiers(currated_procedure_mods_df: DataFrame, output_path: str):
    currated_procedure_mods_df.filter(currated_procedure_mods_df.is_valid == True) \
        .select(col('source_org_oid'),
                col('source_consumer_id'),
                col('start_date'),
                col('to_date'),
                col('code'),
                col('code_system'),
                col('mod')) \
        .repartition(col('source_org_oid'), col('source_consumer_id'))\
        .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id'), col('start_date'), col('code_system'),
                              col('code'))\
        .write.parquet(output_path, mode='overwrite')


def save_problem(currated_problem_df: DataFrame, output_path: str):
    currated_problem_df.filter(currated_problem_df.is_valid == True) \
        .select(col('source_consumer_id'),
                col('source_org_oid'),
                col('start_date'),
                col('to_date'),
                col('code'),
                col('code_system'),
                col('desc'),
                col('source_desc'),
                col('is_admitting'),
                col('batch_id')) \
        .repartition(col('source_org_oid'), col('source_consumer_id'))\
        .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id'), col('start_date'), col('code_system'),
                              col('code'))\
        .write.parquet(output_path, mode='overwrite')


def save_drug(currated_drug_df: DataFrame, output_path: str):
    currated_drug_df.filter(currated_drug_df.is_valid == True) \
        .select(col('id'),
                col('source_consumer_id'),
                col('source_org_oid'),
                col('start_date'),
                col('to_date'),
                col('code'),
                col('code_system'),
                col('desc'),
                col('source_desc'),
                col("strength"),
                col("form"),
                col("classification"),
                col('batch_id')) \
        .repartition(col('source_org_oid'), col('source_consumer_id'))\
        .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id'), col('start_date'), col('code_system'),
                              col('code'))\
        .write.parquet(output_path, mode='overwrite')


def save_cost(currated_cost_df: DataFrame, output_path: str):
    currated_cost_df.filter(currated_cost_df.is_valid == True) \
        .select(col('id'),
                col('source_consumer_id'),
                col('source_org_oid'),
                col('claim_identifier'),
                col('service_number'),
                col('paid_amount'),
                col('batch_id')) \
        .repartition(col('source_org_oid'), col('source_consumer_id'))\
        .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id'), col('claim_identifier'),
                              col('service_number'))\
        .write.parquet(output_path, mode='overwrite')

def save_claim(currated_claim_df: DataFrame, output_path: str):
    currated_claim_df.filter(currated_claim_df.is_valid == True) \
        .select(col('id'),
                col('source_consumer_id'),
                col('source_org_oid'),
                col('payer_name'),
                col('payer_id'),
                col('plan_name'),
                col('plan_id'),
                col('claim_identifier'),
                col('service_number'),
                col('type'),
                col('sub_type'),
                col('start_date'),
                col('end_date'),
                col('admission_date'),
                col('discharge_date'),
                col('units_of_service'),
                col('facility_type_cd'),
                col('admission_source_cd'),
                col('admission_type_cd'),
                col('place_of_service'),
                col('batch_id')) \
        .repartition(col('source_org_oid'), col('source_consumer_id'))\
        .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id'), col('claim_identifier'),
                              col('service_number'))\
        .write.parquet(output_path, mode='overwrite')


def save_provider(currated_provider_df: DataFrame, output_path: str):
    currated_provider_df.filter(currated_provider_df.is_valid == True) \
        .select(col('id'),
                col('npi'),
                col('source_org_oid'),
                col('source_provider_id'),
                col('provider_type'),
                col('active'),
                col('batch_id')) \
        .repartition(col('npi'))\
        .write.parquet(output_path, mode='overwrite')


def save_provider_role(currated_provider_df: DataFrame, output_path: str):
    currated_provider_df.filter(currated_provider_df.is_valid == True) \
        .select(col('id'),
                col('npi'),
                col('claim_identifier'),
                col('service_number'),
                col('role'),
                col('batch_id')) \
        .write.parquet(output_path, mode='overwrite')
