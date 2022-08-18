from datetime import timedelta
import airflow
from airflow import DAG
import boto3

# from airflow.providers.amazon.aws.operators.emr import EmrCreateJobFlowOperator
# from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
# from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor


# from airflow.providers.amazon.aws.operators.e import EmrTerminateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import EmrCreateJobFlowOperator
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor

execution_date = "{{ execution_date }}"

S3_BUCKET_NAME = 'nyec-scripts'
AWS_REGION = "us-east-1"
ssm_client = boto3.client("ssm", region_name=AWS_REGION)
subnetID = ssm_client.get_parameter(Name='/nyec/dev/subnetID')

JOB_FLOW_OVERRIDES = {
    "Name": "nyec-cluster-" + execution_date,
    "ReleaseLabel": "emr-5.36.0",
    "LogUri": "s3://{}/logs/emr/".format(S3_BUCKET_NAME),
    "Instances": {
        "InstanceGroups": [
            {
                "Name": "Master nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "MASTER",
                "InstanceType": "m4.xlarge",
                "InstanceCount": 1
            },
            {
                "Name": "Slave nodes",
                "Market": "ON_DEMAND",
                "InstanceRole": "CORE",
                "InstanceType": "m5.xlarge",
                "InstanceCount": 1
            }
        ],
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
            'Name': 'pip-install',
            # 'Name': 'install pydeequ 1.0.1',
            'ScriptBootstrapAction': {
                'Path': 's3://nyec-scripts/bootstrap/bootstrap.sh',
            }
        }
    ]
}

S3_URI = "s3://{}/scripts/emr/".format(S3_BUCKET_NAME)

SPARK_TEST_STEPS = [
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
                     'spark.nyec.iqvia.patient_ingest_path=s3://nyce-iqvia/raw/2022/08/18/patient/20220809/',
                     '--conf',
                     'spark.nyec.iqvia.claims_ingest_path=s3://nyce-iqvia/raw/2022/08/18/factdx/20220809/',
                     '/home/hadoop/test_processor.py'
                     ]
        }
    }
]

#
# def get_parameters(self, **kwargs):
#     dag_run = kwargs.get('dag_run')
#     parameters = dag_run.conf['key']
#     return parameters

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
    'provide_context': True,
    'email': ['mannys@slalom.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'schedule_interval': None
}

emr_dag = DAG(
    'emr_test',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1
    # schedule_interval=timedelta(minutes=10)
)

cluster_creator = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_test',
    dag=emr_dag
)

step_adder = EmrAddStepsOperator(
    task_id='add_steps',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=SPARK_TEST_STEPS,
    dag=emr_dag
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

cluster_creator >> step_adder
