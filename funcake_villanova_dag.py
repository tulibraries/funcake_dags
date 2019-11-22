""" DAG to harvest data from Villanova University Digital Collections OAI endpoint"""
import os
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from tulflow import harvest, tasks, validate
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

"""
INIT SYSTEMWIDE VARIABLES
check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

# Combine OAI Harvest Variables
VILLANOVA_OAI_CONFIG = Variable.get("VILLANOVA_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "http://digital.library.villanova.edu/OAI/Server",
#   "included_sets": ["dpla"],
#   "excluded_sets": [], <--- OPTIONAL
#   "md_prefix": "oai_dc",
#   "xsl_branch": "my_test_branch", <--- OPTIONAL
#   "xsl_filename": "transforms/my_test_transform.xml", <--- OPTIONAL
#   "schematron_filename": "validations/test_validation", <--- OPTIONAL
# }
MDX_PREFIX   = VILLANOVA_OAI_CONFIG.get("md_prefix")
INCLUDE_SETS = VILLANOVA_OAI_CONFIG.get("included_sets")
OAI_ENDPOINT = VILLANOVA_OAI_CONFIG.get("endpoint")

VILLANOVA_XSLT_CONFIG =  Variable.get("VILLANOVA_XSLT_CONFIG", default_var={}, deserialize_json=True)
#{
#   "xsl_repository": "tulibraries/other_mdx",
#   "xsl_branch": "my_test_branch",
#   "xsl_filename": "transforms/my_test_transform.xml",
#   "schematron_filename": "validations/test_validation",
#}
XSL_BRANCH   = VILLANOVA_XSLT_CONFIG.get("xsl_branch", "master")
XSL_FILENAME = VILLANOVA_XSLT_CONFIG.get("xsl_filename", "transforms/villanova.xsl")
XSL_REPO     = VILLANOVA_XSLT_CONFIG.get("xsl_repo", "tulibraries/aggregator_mdx")
SCHEMATRON_FILENAME = VILLANOVA_XSLT_CONFIG.get("schematron_filename", "validations/padigital_reqd_fields.sch")

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
SCRIPTS_PATH = AIRFLOW_HOME + "/dags/funcake_dags/scripts"

# Data Bucket Variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# Define the DAG
DEFAULT_ARGS = {
    "owner": "joey_vills",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 27),
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

DAG = DAG(
    dag_id="funcake_villanova_harvest",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule_interval=None
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

SET_COLLECTION_NAME = PythonOperator(
    task_id='set_collection_name',
    python_callable=datetime.now().strftime,
    op_args=["%Y-%m-%d_%H-%M-%S"],
    dag=DAG
)

TIMESTAMP = "{{ ti.xcom_pull(task_ids='set_collection_name') }}"

CSV_TRANSFORM = BashOperator(
    task_id="csv_transform",
    bash_command="csv_transform.sh ",
    env={**os.environ, **{
        "PATH": os.environ.get("PATH", "") + ":" + SCRIPTS_PATH,
        "DAGID": "funcake_villanova_harvest",
        "HOME": AIRFLOW_HOME,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": DAG.dag_id + "/" + TIMESTAMP + "/new-updated",
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        }},
    dag=DAG,
    )

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
    dag=DAG
)

XSLT_TRANSFORM = BashOperator(
    task_id="xslt_transform",
    bash_command="transform.sh ",
    env={**os.environ, **{
        "PATH": os.environ.get("PATH", "") + ":" + SCRIPTS_PATH,
        # TODO: discuss how we want to handle XSLT variable.
        "XSL": "https://raw.githubusercontent.com/tulibraries/aggregator_mdx/master/transforms/villanova.xsl",
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": DAG.dag_id + "/" + TIMESTAMP + "/new-updated",
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        }},
    dag=DAG
    )

XSLT_TRANSFORM_FILTER = PythonOperator(
    task_id="xslt_transform_filter",
    provide_context=True,
    python_callable=validate.filter_s3_schematron,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed-filtered",
        "record_parent_element": "{http://www.openarchives.org/OAI/2.0/oai_dc/}dc",
        "schematron_filename": SCHEMATRON_FILENAME,
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed",
    },
    dag=DAG
)

# SET UP TASK DEPENDENCIES
SET_COLLECTION_NAME >> CSV_TRANSFORM >> OAI_TO_S3 >> XSLT_TRANSFORM
