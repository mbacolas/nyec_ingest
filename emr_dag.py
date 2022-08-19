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

execution_date = "{{ execution_date }}"

S3_BUCKET_NAME = 'nyec-scripts'
AWS_REGION = "us-east-1"
ssm_client = boto3.client("ssm", region_name=AWS_REGION)
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
    return  f'{prefix}/{curr_year}/{curr_month}/{curr_day}/{data_set_name}'

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
                     f'spark.nyec.iqvia.raw_claims_ingest_path={generate_date_path("factdx")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_procedure_ingest_path={generate_date_path("procedure")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_procedure_modifier_ingest_path={generate_date_path("proceduremodifier")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_diagnosis_ingest_path={generate_date_path("diagnosis")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_drug_ingest_path={generate_date_path("product")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_provider_ingest_path={generate_date_path("professionalprovidertier")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_provider_ingest_path={generate_date_path("provider")}',

                    '--conf',
                     f'spark.nyec.iqvia.iqvia_processed_s3_prefix={iqvia_processed_s3_prefix}',

                     '--conf',
                     f'spark.nyec.iqvia.iqvia_curated_s3_prefix={iqvia_curated_s3_prefix}',

                     '/home/hadoop/iqvia/processed/iqvia_raw_to_parquet_processor.py'
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
                     f'spark.nyec.iqvia.raw_claims_ingest_path={generate_date_path("factdx")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_procedure_ingest_path={generate_date_path("procedure")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_procedure_modifier_ingest_path={generate_date_path("proceduremodifier")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_diagnosis_ingest_path={generate_date_path("diagnosis")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_drug_ingest_path={generate_date_path("product")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_provider_ingest_path={generate_date_path("professionalprovidertier")}',

                     '--conf',
                     f'spark.nyec.iqvia.raw_provider_ingest_path={generate_date_path("provider")}',

                     '--conf',
                     f'spark.nyec.iqvia.iqvia_curated_s3_prefix={iqvia_curated_s3_prefix}',

                     '--conf',
                     f'spark.nyec.iqvia.iqvia_curated_s3_prefix={iqvia_curated_s3_prefix}',
                     '/home/hadoop/iqvia/curated/staging_ingest_processor.py'
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
    'email': ['mannys@slalom.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'schedule_interval': None
}

emr_dag = DAG(
    'iqvia_ingest_flow',
    default_args=default_args,
    dagrun_timeout=timedelta(hours=2),
    max_active_runs=1
    # schedule_interval=timedelta(minutes=10)
)

create_procssed_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_test',
    dag=emr_dag
)

create_curated_cluster = EmrCreateJobFlowOperator(
    task_id='create_emr_cluster',
    job_flow_overrides=JOB_FLOW_OVERRIDES,
    aws_conn_id='aws_default',
    emr_conn_id='emr_test',
    dag=emr_dag
)

trigger_processed_emr_job = EmrAddStepsOperator(
    task_id='curate',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=TO_PROCESSED_SPARK_STEPS,
    dag=emr_dag
)

trigger_curated_emr_job = EmrAddStepsOperator(
    task_id='curate',
    job_flow_id="{{ task_instance.xcom_pull('create_emr_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    steps=TO_CURATED_SPARK_STEPS,
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

def push():
    # iqvia_data_types = Variable.get("iqvia_data_types", deserialize_json=True)
    # print(f'------------------------------------------ >>> {type(iqvia_data_types)}')
    print(f'------------------------------------------ >>> {SPARK_STEPS}')


push1 = PythonOperator(
    task_id='push',
    provide_context=True,
    dag=emr_dag,
    python_callable=push,
)

push1 >> [create_procssed_cluster, create_curated_cluster] >> [trigger_processed_emr_job, trigger_curated_emr_job]

#

# create_cluster >> trigger_curate_emr_job
