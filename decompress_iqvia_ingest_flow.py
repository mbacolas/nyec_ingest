from datetime import timedelta
import airflow
from airflow import DAG
import boto3
from airflow.operators.python import PythonOperator


# from airflow.providers.amazon.aws.operators.e import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator

# from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.providers.amazon.aws.operators import glue_crawler
from airflow.providers.amazon.aws.transfers import s3_to_redshift
from airflow.providers.amazon.aws.transfers.s3_to_redshift import *

execution_date = "{{ execution_date }}"

S3_BUCKET_NAME = 'nyec-scripts'
AWS_REGION = "us-east-1"
ssm_client = boto3.client("ssm", region_name=AWS_REGION)
# subnetID = 'subnet-00031e4e4cd2b33a4'
subnetID = ssm_client.get_parameter(Name='/nyec/dev/subnetID')
from airflow.models import Variable

# iqvia_data_types = Variable.get("iqvia_data_types", deserialize_json=True)


JOB_FLOW_OVERRIDES = {
    "Name": "nyec-cluster-" + execution_date,
    "ReleaseLabel": "emr-6.7.0",
    # "ReleaseLabel": "emr-5.36.0",
    "LogUri": "s3://{}/logs/emr/".format(S3_BUCKET_NAME),
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                # "InstanceType": "c5.large",
                # "InstanceType": "m4.large",
                # "InstanceType": "m5.xlarge",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1
            },
            {
                # 70DPUs --> 150 cores
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                # "InstanceType": "r5.xlarge",
                "InstanceType": "c5d.18xlarge", #c5d.18xlarge	vCPU:72	RAM:144	Disk:1800 SSD
                # "InstanceType": "m5d.12xlarge", #48	192
                # "InstanceType": "r5.4xlarge",
                # "InstanceType": "m5.xlarge",
                # "InstanceCount": 18
                "InstanceCount": 2
            }
        ],
        # "Ec2SubnetId": "subnet-00031e4e4cd2b33a4",
        "Ec2SubnetId": subnetID['Parameter']['Value'],
        "TerminationProtected": False,
        "KeepJobFlowAliveWhenNoSteps": False
    },
    'JobFlowRole': 'EMR_EC2_DefaultRole',
    'ServiceRole': 'EMR_DefaultRole',
    'Applications': [
        {
            'Name': 'Spark'
        }
    ],
    'BootstrapActions': [
        {
            'Name': 'pip-install-dependencies',
            'ScriptBootstrapAction': {
                'Path': 's3://nyec-scripts/bootstrap/bootstrap.sh',
            }
        }
    ]
}

S3_URI = "s3://{}/scripts/emr/".format(S3_BUCKET_NAME)

def generate_date_path(data_set_name):
    # prefix = Variable.get('iqvia_raw_s3_prefix')
    prefix = ''
    from datetime import date
    curr_dt = date.today()
    curr_year = curr_dt.year
    curr_month = curr_dt.month
    curr_day = curr_dt.day
    return  f's3://nyce-iqvia/raw/2022/8/19/{data_set_name}/'
    # return  f's3://nyce-iqvia/processed-parquet/{data_set_name}/'
    # return  f'{prefix}/{curr_year}/{curr_month}/{curr_day}/{data_set_name}'

iqvia_processed_s3_prefix = Variable.get('iqvia_processed_s3_prefix')
iqvia_curated_s3_prefix = Variable.get('iqvia_curated_s3_prefix')

TO_PROCESSED_SPARK_STEPS = [
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
                     f'spark.nyec.iqvia.raw_patient_ingest_path={generate_date_path("patient")}',
                    '--conf',
                     f'spark.nyec.iqvia.raw_claim_ingest_path={generate_date_path("factdx")}',

                    '--conf',
                     f'spark.nyec.iqvia.iqvia_processed_s3_prefix={iqvia_processed_s3_prefix}',

                     '--conf',
                     f'spark.executor.memory=25g',

                     '--conf',
                     f'spark.executor.cores=10',

                     '--py-files',
                     '/home/hadoop/iqvia.zip,/home/hadoop/common.zip',

                     '/home/hadoop/iqvia_gzip_to_csv_processor.py'
                     ]
        }
    }
]


# '--conf',
# f'spark.sql.files.maxPartitionBytes=268435456',

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

decompress_iqvia_ingest_flow = DAG(
    'decompress_iqvia_ingest_flow',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1
    # schedule_interval=timedelta(minutes=10)
)



create_emr_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_test',
    dag=decompress_iqvia_ingest_flow
)

# trigger_processed_emr_job = EmrAddStepsOperator(
#     task_id='trigger_processed_emr_job',
#     job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
#     aws_conn_id='aws_default',
#     steps=TO_PROCESSED_SPARK_STEPS,
#     dag=emr_dag
# )

trigger_curated_emr_job = EmrAddStepsOperator(
    task_id='trigger_curated_emr_job',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=TO_PROCESSED_SPARK_STEPS,
    dag=decompress_iqvia_ingest_flow
)

# cluster_remover = EmrTerminateJobFlowOperator(
#     task_id='remove_cluster',
#     job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
# )

# step_checker = EmrStepSensor(
#     task_id='watch_step',
#     job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
#     step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
#     aws_conn_id='aws_default',
#     dag=emr_dag
# )

# aws_conn_id='aws_default',
# cluster_remover = EmrTerminateJobFlowOperator(
#     task_id='remove_cluster',
#     job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
#     aws_conn_id='aws_default',
#     dag=dag
# )
# def push():
#     print(f'-----------------------------------------_>>>> JOB_FLOW_OVERRIDES: {JOB_FLOW_OVERRIDES}')
#     print(f'-----------------------------------------_>>>> TO_PROCESSED_SPARK_STEPS: {TO_PROCESSED_SPARK_STEPS}')
#     print(f'-----------------------------------------_>>>> TO_CURATED_SPARK_STEPS: {TO_CURATED_SPARK_STEPS}')


# push1 = PythonOperator(
#     task_id='push',
#     provide_context=True,
#     dag=emr_dag,
#     python_callable=push,
# )

# s3_patient_to_redshift = S3ToRedshiftOperator(
#     s3_bucket=S3_BUCKET_NAME,
#     s3_key=f'{iqvia_curated_s3_prefix}/patient',
#     schema='PUBLIC',
#     table='patient',
#     copy_options=['parquet'],
#     task_id='transfer_s3_to_redshift',
# )

# create_emr_cluster >> trigger_processed_emr_job
create_emr_cluster >> trigger_curated_emr_job
# create_processed_emr_cluster >> create_emr_cluster >> [trigger_processed_emr_job, trigger_curated_emr_job]