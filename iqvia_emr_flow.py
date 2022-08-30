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
from airflow.providers.amazon.aws.sensors.emr_job_flow import EmrJobFlowSensor
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

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
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                # "InstanceType": "r5.xlarge",
                # "InstanceType": "c5d.18xlarge", #c5d.18xlarge	vCPU:72	RAM:144	Disk:1800 SSD
                "InstanceType": "m5d.16xlarge",
                # "InstanceType": "r5.4xlarge",
                # "InstanceType": "m5.xlarge",
                # "InstanceCount": 18
                "InstanceCount": 2
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


def generate_date_path(data_set_name):
    prefix = Variable.get('iqvia_raw_s3_prefix')
    from datetime import date
    curr_dt = date.today()
    curr_year = curr_dt.year
    curr_month = curr_dt.month
    curr_day = curr_dt.day
    # return f's3://nyec-dev-raw-data-bucket/iqvia/{data_set_name}/20220809'
    return f'{prefix}/{curr_year}/{curr_month}/{curr_day}/{data_set_name}'


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
                     f'spark.nyec.iqvia.raw_pro_provider_ingest_path={generate_date_path("professionalprovidertier")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_provider_ingest_path={generate_date_path("provider")}',

                     '--conf',
                     f'spark.nyec.iqvia.iqvia_processed_s3_prefix={iqvia_processed_s3_prefix}',

                     '--conf',
                     f'spark.executor.memory=50g',

                     '--conf',
                     f'spark.executor.cores=16',

                     '--conf',
                     f'spark.sql.shuffle.partitions=6000',

                     '--conf',
                     f'spark.sql.files.maxPartitionBytes=268435456',

                     '--conf',
                     f'spark.driver.memory=16G',

                     '--conf',
                     f'spark.driver.cores=4',

                     '--conf',
                     f'spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError=\'kill -9 %p\'',
                     # f'spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError=\'kill -9 %p\'',

                     '--conf',
                     f'num-executors=8',

                     '--py-files',
                     '/home/hadoop/iqvia.zip,/home/hadoop/common.zip',

                     '/home/hadoop/iqvia_raw_to_parquet_processor.py'
                     ]
        }
    }
]

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
                     f'spark.nyec.iqvia.iqvia_curated_s3_prefix={iqvia_curated_s3_prefix}',

                     '--conf',
                     f'spark.nyec.iqvia.file_format=csv',

                     '--conf',
                     f'spark.executor.memory=50g',

                     '--conf',
                     f'spark.executor.cores=16',

                     '--conf',
                     f'spark.sql.shuffle.partitions=6000',

                     '--conf',
                     f'spark.sql.files.maxPartitionBytes=268435456',

                     '--conf',
                     f'spark.driver.memory=16G',

                     '--conf',
                     f'spark.driver.cores=4',

                     '--conf',
                     f'spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -XX:OnOutOfMemoryError=\'kill -9 %p\'',
                     # f'spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+UnlockDiagnosticVMOptions -XX:+G1SummarizeConcMark -XX:InitiatingHeapOccupancyPercent=35 -verbose:gc -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:OnOutOfMemoryError=\'kill -9 %p\'',

                     '--conf',
                     f'num-executors=8',

                     '--py-files',
                     '/home/hadoop/iqvia.zip,/home/hadoop/common.zip',

                     '/home/hadoop/staging_ingest_processor.py'
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

iqvia_emr_flow = DAG(
    'iqvia_emr_flow',
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
    dag=iqvia_emr_flow
)

# Adding a sensor to monitor the EMR cluster creation
sensor_create_emr_cluster = EmrJobFlowSensor(
    task_id='check_emr_create_cluster_job_flow',
    job_flow_id=create_emr_cluster.output,
    target_states=['WAITING'],
    dag=iqvia_emr_flow
)

# Define Processed Step & a sensor to monitor it
trigger_processed_emr_job = EmrAddStepsOperator(
    task_id='trigger_processed_emr_job',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=TO_PROCESSED_SPARK_STEPS,
    dag=iqvia_emr_flow
)

# No need to define target state within EmrStepSensor since with the default target states, sensor waits for the step to be completed
step_checker_trigger_processed_emr_job = EmrStepSensor(
    task_id='step_checker_trigger_processed_emr_job',
    job_flow_id="{{ task_instance.xcom_pull('trigger_processed_emr_job', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=iqvia_emr_flow
)

# Define a curated step & a sensor to monitor it
trigger_curated_emr_job = EmrAddStepsOperator(
    task_id='trigger_curated_emr_job',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=TO_CURATED_SPARK_STEPS,
    dag=iqvia_emr_flow
)

step_checker_trigger_curated_emr_job = EmrStepSensor(
    task_id='step_checker_trigger_curated_emr_job',
    job_flow_id="{{ task_instance.xcom_pull('trigger_curated_emr_job', key='return_value') }}",
    step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
    aws_conn_id='aws_default',
    dag=iqvia_emr_flow
)

create_emr_cluster >> sensor_create_emr_cluster >> trigger_processed_emr_job >> step_checker_trigger_processed_emr_job
create_emr_cluster >> sensor_create_emr_cluster >> trigger_curated_emr_job >> step_checker_trigger_curated_emr_job