from datetime import timedelta
import airflow
from airflow import DAG
import boto3
from airflow.operators.python import PythonOperator

# from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
# from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


# from airflow.providers.amazon.aws.operators.e import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator

from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators import glue_crawler
from airflow.providers.amazon.aws.transfers import s3_to_redshift
from airflow.providers.amazon.aws.transfers.s3_to_redshift import *

execution_date = "{{ execution_date }}"

S3_BUCKET_NAME = 'nyec-scripts'
AWS_REGION = "us-east-1"
ssm_client = boto3.client("ssm", region_name=AWS_REGION)
subnetID = ssm_client.get_parameter(Name='/nyec/dev/subnetID')
from airflow.models import Variable

import uuid

def generate_uuid()->str:
    id = uuid.uuid4().hex[:12]
    print(f'===============>>> id: {id}')
    return id

def generate_date_path(data_set_name):
    # prefix = Variable.get('iqvia_raw_s3_prefix')
    prefix = 's3://nyec-dev-raw-data-bucket/iqvia/'
    from datetime import date
    curr_dt = date.today()
    curr_year = curr_dt.year
    curr_month = curr_dt.month
    curr_day = curr_dt.day
    return  f'{prefix}/{data_set_name}/20220809'
    # return  f'{prefix}/{curr_year}/{curr_month}/{curr_day}/{data_set_name}'


tast_var = generate_uuid()


def get_var():
    print(f'===============>>> {tast_var}')
    return tast_var

from airflow.models.param import Param
from airflow import DAG

TO_CURATED_SPARK_STEPS = [
    {
        'Name': 'setup - copy files',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['aws', 's3', 'cp', '--recursive', S3_URI, '/home/hadoop/']
        }
    },
    {
        'Name': 'Run Spark',
        'ActionOnFailure': 'CANCEL_AND_WAIT',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': ['spark-submit',
                     '--conf',
                     f'spark.nyec.iqvia.raw_plan_ingest_path={generate_date_path("plan")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_patient_ingest_path={generate_date_path("patient")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_claim_ingest_path={generate_date_path("factdx")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_procedure_ingest_path={generate_date_path("procedure")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_procedure_modifier_ingest_path={generate_date_path("proceduremodifier")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_diagnosis_ingest_path={generate_date_path("diagnosis")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_drug_ingest_path={generate_date_path("product")}',

                     '--conf',
                     f'spark.nyec.iqvia.pro_provider_ingest_path={generate_date_path("professionalprovidertier")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_provider_ingest_path={generate_date_path("provider")}',

                     '--conf',
                     f'spark.nyec.iqvia.batch_id={{\'batch_id\'}}',

                     '--py-files',
                     '/home/hadoop/iqvia.zip,/home/hadoop/common.zip',

                     '/home/hadoop/staging_ingest_processor.py'
                     ]
        }
    }
]


def print_var():
    print(f'===============>>> {TO_CURATED_SPARK_STEPS}')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['bulent_da_man@slalom.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'schedule_interval': None,
}

redshift_dag = DAG(
    'redshift_dag',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1,
    params={
        "batch_id": Param(uuid.uuid4().hex[:12], type='string')
     }
    # schedule_interval=timedelta(minutes=10)
)

#
# s3_patient_to_redshift = S3ToRedshiftOperator(
#     s3_bucket=S3_BUCKET_NAME,
#     s3_key=f'{iqvia_curated_s3_prefix}/patient',
#     schema='PUBLIC',
#     table='patient',
#     copy_options=['parquet'],
#     task_id='transfer_s3_to_redshift',
# )

push1 = PythonOperator(
    task_id='push1',
    # provide_context=True,
    dag=redshift_dag,
    python_callable=print_var,
)

push1
