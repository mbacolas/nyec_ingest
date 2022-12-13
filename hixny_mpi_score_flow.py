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
subnetID = 'subnet-00031e4e4cd2b33a4'
# subnetID = ssm_client.get_parameter(Name='/nyec/dev/subnetID')
from airflow.models import Variable

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
                # "InstanceType": "c5d.18xlarge", #c5d.18xlarge	vCPU:72	RAM:144	Disk:1800 SSD
                # "InstanceType": "m5d.12xlarge",
                "InstanceType": "r5d.12xlarge",
                # "InstanceType": "r5.4xlarge",
                # "InstanceType": "m5.xlarge",
                # "InstanceCount": 18
                "InstanceCount": 3
            }
        ],
        "Ec2SubnetId": "subnet-00031e4e4cd2b33a4",
        # "Ec2SubnetId": subnetID['Parameter']['Value'],
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

MPI_SCORE_SPARK_STEPS = [
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
                     f'spark.nyec.adhoc.hixny_matching_score=s3://nyec-adhoc/inbound/mpi/hixny_matching_score/}',
                     '--conf',
                     f'spark.hixny.hixny_mpi=',
                    '--conf',
                     f'spark.nyec.iqvia.raw_claim_ingest_path=s3://nyec-adhoc/inbound/mpi/hixny/MPIID_with_demographics_20220926.csv',

                     '--conf',
                     f'spark.nyec.nyec_hixny_mpi=s3://nyec-adhoc/inbound/mpi/hixny_nyec/iIT2163_Hixny_Data_20221010.csv',

                     '--conf',
                     f'spark.executor.memory=25g',

                     '--conf',
                     f'spark.executor.cores=35',

                     '--py-files',
                     '/home/hadoop/mpi_matching.zip',

                     '/home/hadoop/test_processor.py'
                     ]
        }
    }
]

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

hixny_mpi_score = DAG(
    'hixny_mpi_score',
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
    dag=hixny_mpi_score
)

trigger_matching_emr_job = EmrAddStepsOperator(
    task_id='trigger_matching_emr_job',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=MPI_SCORE_SPARK_STEPS,
    dag=hixny_mpi_score
)

create_emr_cluster >> trigger_matching_emr_job
