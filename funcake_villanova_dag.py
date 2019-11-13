""" DAG to harvest data from Villanova University Digital Collections OAI endpoint"""
import os
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from tulflow import harvest, tasks
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


#
# INIT SYSTEMWIDE VARIABLES
#
# check for existence of systemwide variables shared across tasks that can be
# initialized here if not found (i.e. if this is a new installation) & defaults exist
#

# Combine OAI Harvest Variables
VILLANOVA_OAI_CONFIG = Variable.get("VILLANOVA_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "http://digital.library.villanova.edu/OAI/Server",
#   "included_sets": ["dpla"],
#   "excluded_sets": [],
#   "md_prefix": "oai_dc"
# }
MDX_PREFIX = VILLANOVA_OAI_CONFIG.get("md_prefix")
INCLUDE_SETS = VILLANOVA_OAI_CONFIG.get("included_sets")
OAI_ENDPOINT = VILLANOVA_OAI_CONFIG.get("endpoint")
AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
SCRIPTS_PATH = AIRFLOW_HOME + "/dags/funcake_dags/scripts"

# Data Bucket Variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

#
# CREATE DAG
#

DEFAULT_ARGS = {
    "owner": "joey_vills",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 27),
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

VILLANOVA_HARVEST_DAG = DAG(
    dag_id="funcake_villanova_harvest",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=None
)

#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#

SET_COLLECTION_NAME = PythonOperator(
    task_id='set_collection_name',
    python_callable=datetime.now().strftime,
    op_args=["%Y-%m-%d_%H-%M-%S"],
    dag=VILLANOVA_HARVEST_DAG
)

TIMESTAMP = "{{ ti.xcom_pull(task_ids='set_collection_name') }}"

OAI_TO_S3 = PythonOperator(
    task_id='harvest_oai',
    provide_context=True,
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "metadata_prefix": MDX_PREFIX,
        "oai_endpoint": OAI_ENDPOINT,
        "records_per_file": 1000,
        "set": INCLUDE_SETS,
        "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}"
    },
    dag=VILLANOVA_HARVEST_DAG
)

XSLT_TRANSFORM = BashOperator(
        task_id="xslt_transform",
        bash_command="transform.sh ",
        dag=VILLANOVA_HARVEST_DAG,
        env={**os.environ, **{
            "PATH": os.environ.get("PATH", "") + ":" + SCRIPTS_PATH,
            # TODO: discuss how we want to handle XSLT variable.
            "XSL": "https://raw.githubusercontent.com/tulibraries/aggregator_mdx/master/transforms/villanova.xsl",
            "BUCKET": AIRFLOW_DATA_BUCKET,
            "FOLDER": VILLANOVA_HARVEST_DAG.dag_id + "/" + TIMESTAMP + "/new-updated",
            "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
            "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
            }}
        )

# S3 -> XSLT Transform -> s3
# S3 -> Schematron -> s3

#
# CREATE TASKS DEPENDENCIES WITH DAG
#
# This sets the dependencies of Tasks within the DAG.
#

SET_COLLECTION_NAME >> OAI_TO_S3 >> XSLT_TRANSFORM
