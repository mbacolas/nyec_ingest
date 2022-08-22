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

S3_URI = "s3://{}/scripts/emr/".format(S3_BUCKET_NAME)

from airflow.models.param import Param
from airflow import DAG


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
    'schedule_interval': None
}

redshift_dag = DAG(
    'redshift_dag',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1
)

s3_patient_to_redshift = S3ToRedshiftOperator(
    s3_bucket='nyce-iqvia',
    s3_key='curated/patient/',
    schema='staging_patient',
    table='patient',
    copy_options=['parquet'],
    task_id='transfer_s3_to_redshift'
)
#redshift_conn_id='conn_redshift',


push1 = PythonOperator(
    task_id='push1',
    # provide_context=True,
    dag=redshift_dag,
    python_callable=print
)


push1 >> s3_patient_to_redshift
