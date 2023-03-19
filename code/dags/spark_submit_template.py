from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor

"""
This is a template dag for running a spark app on EMR.

Your task is to fill in the TODO s and initialize the SPARK_STEPS.
A few things to take into account:
- Deploy mode should be cluster
- Master should be yarn
- Must pass CLOUD_ENV and DATA_PATH environment variables
- Must set the py files
- The staging environment must be dev

"""

# TODO: Fill in with the EMR ID. e.g. j-K6T93ZJDWA2F
EMR_ID = ""

# TODO: Fill in your name, without special characters and spaces
STUDENT_NAME = "StudentName"

default_args = {"owner": STUDENT_NAME}

# TODO: Set the folder name on S3
KEY = ""
# TODO: Source code
SRC = ""
# TODO: Packaged dependencies
PACKAGE = ""
# TODO: Script to run the pipeline
SCRIPT = ""

# Bucket for the artifacts (zips and run.py files)
BUCKET_NAME = f"rootsacademy-infra-092022-artifacts-bucket"
# Location where the pipeline will write output files
CLOUD_ENV = f"s3://rootsacademy-infra-092022-data-bucket/output/{STUDENT_NAME}/"
# Location where input data is
DATA_PATH = "s3://rootsacademy-infra-092022-data-bucket/data/"

SPARK_STEPS = None


with DAG(
    f"Spark_Pipeline_test_{STUDENT_NAME}",
    default_args=default_args,
    description="Run Spark Pipeline",
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    tags=["example v1.0"],
) as dag:

    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id=EMR_ID,
        aws_conn_id="aws_default",
        steps=SPARK_STEPS,
        params={
            "BUCKET_NAME": BUCKET_NAME,
            "STUDENT_NAME": STUDENT_NAME,
            "KEY": KEY,
            "SRC": SRC,
            "PACKAGE": PACKAGE,
            "SCRIPT": SCRIPT,
        },
    )

    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id=EMR_ID,
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    step_adder >> step_checker
