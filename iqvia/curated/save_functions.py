from pyspark import RDD
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
import uuid
from common.functions import *
from iqvia.common.schema import error_schema


def save_errors(rdd: RDD, row_type: str):
    rdd.filter(lambda r: r.is_valid == False) \
        .map(lambda r: Row(batch_id=r.batch_id,
                           type=row_type,
                           row_errors=json.dumps(r.error),
                           row_value=json.dumps(r.asDict()),
                           date_created=datetime.now())) \
        .toDF(error_schema) \
        .write.format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "public.error") \
        .option("user", "postgres") \
        .option("password", "mysecretpassword") \
        .mode("append") \
        .save()


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
        .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id'))\
        .write.parquet(output_path, mode='overwrite')


def save_procedure_modifiers(currated_procedure_df: DataFrame, output_path: str):
    currated_procedure_df.filter(currated_procedure_df.is_valid == True) \
        .select(col('source_org_oid'),
                col('source_consumer_id'),
                col('start_date'),
                col('to_date'),
                col('code'),
                col('code_system'),
                col('mod')) \
        .repartition(col('source_org_oid'), col('source_consumer_id'))\
        .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id'))\
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
        .sortWithinPartitions(col('source_org_oid'), col('source_consumer_id'))\
        .write.parquet(output_path, mode='overwrite')