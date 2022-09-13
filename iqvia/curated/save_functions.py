import json

from pyspark import RDD
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
import uuid
from common.functions import *
from iqvia.common.schema import error_schema, stage_procedure__modifier_schema
from pymonad.either import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, ArrayType, \
    MapType, BooleanType, DecimalType, TimestampType
from datetime import datetime
from datetime import date
from json import JSONEncoder
import json
import uuid

class DateTimeEncoder(JSONEncoder):
    # Override the default method
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()

# def save_errors(rdd: RDD, row_type: str, output_path: str):
#     rdd.filter(lambda r: r.is_valid == False or r.has_warnings == True) \
#         .map(lambda r: Row(batch_id=r.batch_id,
#                            type=row_type,
#                            row_errors=json.dumps(str(r.error)),
#                            row_warnings=json.dumps(str(r.warning)),
#                            row_value=json.dumps(str(r.asDict())),
#                            date_created=r.date_created)) \
#         .toDF(error_schema) \
#         .write\
#         .parquet(output_path, mode='append', compression='snappy')


def save_errors(error_df: DataFrame, row_type: str, output_path: str):
    error_df.filter((col('is_valid') != True) | (col('has_warnings') == True)) \
        .rdd\
        .map(lambda r: Row(id=uuid.uuid4().hex[:12],
                           batch_id=r.batch_id,
                           type=row_type,
                           is_valid=r.is_valid,
                           has_warnings=r.has_warnings,
                           row_errors=json.dumps(r.error),
                           row_warnings=json.dumps(r.warning),
                           row_value=json.dumps(r.asDict(), indent=4, cls=DateTimeEncoder),
                           date_created=datetime.now())) \
        .toDF(error_schema) \
        .write \
        .parquet(output_path, mode='append', compression='snappy')


        # .parquet('s3://nyce-iqvia/curated/error', mode='overwrite')
    # rdd.filter(lambda r: r.is_included == False) \
    #     .map(lambda r: Row(batch_id=r.batch_id,
    #                        type=row_type,
    #                        row_errors=json.dumps(r.error),
    #                        row_value=json.dumps(r.asDict()),
    #                        date_created=datetime.now())) \
    #     .toDF(error_schema) \
    #     .write\
    #     .format("jdbc") \
    #     .option("url", "jdbc:postgresql://nyec-rds-wflow-instance-1.c3urb70usat2.us-east-1.rds.amazonaws.com:5432/nyec_wflow") \
    #     .option("driver", "org.postgresql.Driver") \
    #     .option("dbtable", "public.error") \
    #     .option("user", "nyec_admin") \
    #     .option("password", "Packard007$") \
    #     .mode("append") \
    #     .save()

def save_run_meta(meta_df: DataFrame, output_path: str):
        meta_df.repartition(col('run_date'))\
                .sortWithinPartitions(col('data_source'))\
                .write\
                .parquet(output_path, mode='append', compression='snappy')


def save_org(org_df: DataFrame, output_path: str):
    org_df.select( col('id'),
                    col('source_org_oid'),
                    col('name'),
                    col('type'),
                    col('active'),
                    col('batch_id'),
                    col('date_created')) \
        .write\
        .parquet(output_path, mode='overwrite', compression='snappy')


def save_patient(currated_patient_df: DataFrame, output_path: str):
    currated_patient_df.filter(currated_patient_df.is_valid == True) \
        .select(col('id'),
                col('mpi'),
                col('prefix'),
                col('suffix'),
                col('first_name'),
                col('middle_name'),
                col('last_name'),
                col('dod'),
                col('ssn'),
                col('ethnicity'),
                col('race'),
                col('deceased'),
                col('marital_status'),
                col('source_consumer_id'),
                col('source_org_oid'),
                col('type'),
                col('active'),
                col('dob'),
                col('gender'),
                col('batch_id'),
                col('date_created'))\
        .repartition(6000, col('source_consumer_id'))\
        .sortWithinPartitions(col('source_consumer_id'))\
        .write\
        .parquet(output_path, mode='overwrite', compression='snappy')

    # .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id')) \
        # .repartition(col('source_org_oid'), col('source_consumer_id'))\

def save_procedure(currated_procedure_df: DataFrame, output_path: str):
    currated_procedure_df.filter(currated_procedure_df.is_valid == True) \
        .select(col('id'),
                col('body_site'),
                col('outcome'),
                col('complication'),
                col('note'),
                col('source_consumer_id'),
                col('source_org_oid'),
                col('start_date'),
                col('to_date'),
                col('code'),
                col('code_system'),
                col('revenue_code'),
                col('desc'),
                col('source_desc'),
                col('batch_id'),
                col('date_created')) \
        .repartition(6000, col('source_consumer_id'))\
        .sortWithinPartitions(col('source_consumer_id'), col('code_system'), col('code'), col('start_date'))\
        .write\
        .parquet(output_path, mode='overwrite', compression='snappy')
        # .parquet(output_path, mode='overwrite', compression='snappy')


def save_procedure_modifiers(currated_procedure_mods_rdd: RDD, output_path: str):
    currated_procedure_mods_rdd.filter(lambda r: r.is_valid == False and len(r.mod) > 0)\
                                .flatMap(lambda r: map(lambda y: Row(id=r.id,
                                                                     source_org_oid=r.source_org_oid,
                                                                     source_consumer_id=r.source_consumer_id,
                                                                     start_date=r.start_date,
                                                                     to_date=r.to_date,
                                                                     code=r.code,
                                                                     code_system=r.code_system,
                                                                     mod=y,
                                                                     batch_id=r.batch_id,
                                                                     date_created=r.date_created), r.mod))\
                                .toDF(stage_procedure__modifier_schema) \
                                .repartition(6000, col('source_consumer_id')) \
                                .sortWithinPartitions(col('source_consumer_id'),
                                                      col('code_system'),
                                                      col('code'),
                                                      col('mod')) \
                                .write\
                                .parquet(output_path, mode='overwrite', compression='snappy')

def save_problem(currated_problem_df: DataFrame, output_path: str):
    currated_problem_df.filter(currated_problem_df.is_valid == True) \
        .select(col('id'),
                col('primary'),
                col('clinical_status'),
                col('severity'),
                col('onset_date'),
                col('onset_age'),
                col('abatement_date'),
                col('abatement_age'),
                col('source_consumer_id'),
                col('source_org_oid'),
                col('start_date'),
                col('to_date'),
                col('code'),
                col('code_system'),
                col('desc'),
                col('source_desc'),
                col('is_admitting'),
                col('batch_id'),
                col('date_created')) \
        .repartition(6000, col('source_consumer_id'))\
        .sortWithinPartitions(col('source_consumer_id'), col('code_system'), col('code'), col('start_date'))\
        .write\
        .parquet(output_path, mode='overwrite', compression='snappy')


def save_drug(currated_drug_df: DataFrame, output_path: str):
    currated_drug_df.filter(currated_drug_df.is_valid == True) \
        .select(col('id'),
                col('status'),
                col('discontinued_date'),
                col('days_supply'),
                col('dispense_qty'),
                col('dosage'),
                col('dosage_unit'),
                col('refills'),
                col('dosage_instructions'),
                col('dosage_indication'),
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
                col('batch_id'),
                col('date_created')) \
        .repartition(6000, col('source_consumer_id'))\
        .sortWithinPartitions(col('source_consumer_id'), col('code_system'), col('code'), col('start_date'))\
        .write\
        .parquet(output_path, mode='overwrite', compression='snappy')


def save_cost(currated_cost_df: DataFrame, output_path: str):
    currated_cost_df.filter(currated_cost_df.is_valid == True) \
        .select(col('id'),
                col('co_payment'),
                col('deductible_amount'),
                col('coinsurance'),
                col('covered_amount'),
                col('allowed_amount'),
                col('not_covered_amount'),
                col('source_consumer_id'),
                col('source_org_oid'),
                col('claim_identifier'),
                col('service_number'),
                col('paid_amount'),
                col('batch_id'),
                col('date_created')) \
        .repartition(6000, col('source_consumer_id'))\
        .sortWithinPartitions(col('source_consumer_id'), col('claim_identifier'), col('service_number'))\
        .write\
        .parquet(output_path, mode='overwrite', compression='snappy')


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
                col('batch_id'),
                col('date_created')) \
        .repartition(6000, col('source_consumer_id'))\
        .sortWithinPartitions(col('source_consumer_id'), col('claim_identifier'), col('service_number'))\
        .write\
        .parquet(output_path, mode='overwrite', compression='snappy')


def save_provider(currated_provider_df: DataFrame, output_path: str):
    currated_provider_df\
        .filter(currated_provider_df.is_valid == True) \
        .select(col('id'),
                col('npi'),
                col('source_org_oid'),
                col('source_provider_id'),
                col('provider_type'),
                col('active'),
                col('batch_id'),
                col('date_created')) \
        .repartition(6000, 'source_provider_id') \
        .sortWithinPartitions('source_provider_id') \
        .dropDuplicates(['source_provider_id'])\
        .write\
        .parquet(output_path, mode='overwrite', compression='snappy')


def save_provider_role(currated_provider_role_df: DataFrame, output_path: str):
    currated_provider_role_df.filter(currated_provider_role_df.is_valid == True) \
        .drop(col('is_valid'))\
        .repartition(6000, col('source_provider_id'), col('claim_identifier'), col('service_number')) \
        .sortWithinPartitions(col('source_provider_id'), col('claim_identifier'), col('service_number')) \
        .write\
        .parquet(output_path, mode='overwrite', compression='snappy')
