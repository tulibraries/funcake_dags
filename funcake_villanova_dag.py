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
LOCAL FUNCTIONS
Functions / any code with processing logic should be elsewhere, tested, etc.
This is where to put functions that haven't been abstracted out yet.
"""

def slackpostonsuccess(dag, **context):
    """Task Method to Post Successful Villanova DAG Completion on Slack."""

    task_instance = context.get('task_instance')

    msg = "%(date)s DAG %(dagid)s success: %(url)s" % {
        "date": context.get('execution_date'),
        "dagid": task_instance.dag_id,
        "url": task_instance.log_url
        }

    return tasks.slackpostonsuccess(dag, msg).execute(context=context)

#
# INIT SYSTEMWIDE VARIABLES
#
# check for existence of systemwide variables shared across tasks that can be
# initialized here if not found (i.e. if this is a new installation) & defaults exist
#

AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
SCRIPTS_PATH = AIRFLOW_HOME + "/dags/funcake_dags/scripts"

# Combine OAI Harvest Variables
VILLANOVA_OAI_CONFIG = Variable.get("VILLANOVA_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "http://digital.library.villanova.edu/OAI/Server",
#   "all_sets": False, <-- OPTIONAL
#   "included_sets": ["dpla"], <-- OPTIONAL
#   "excluded_sets": [], <--- OPTIONAL
#   "md_prefix": "oai_dc",
#   "schematron_filter": "validations/test_validation", <--- OPTIONAL
#   "schematron_report": "validations/test_validation", <--- OPTIONAL
# }
ALL_SETS = VILLANOVA_OAI_CONFIG.get("all_sets")
EXCLUDED_SETS = VILLANOVA_OAI_CONFIG.get("excluded_sets")
INCLUDED_SETS = VILLANOVA_OAI_CONFIG.get("included_sets")
MD_PREFIX = VILLANOVA_OAI_CONFIG.get("md_prefix")
OAI_ENDPOINT = VILLANOVA_OAI_CONFIG.get("endpoint")
OAI_SCHEMATRON_FILTER = VILLANOVA_OAI_CONFIG.get("schematron_filter")
OAI_SCHEMATRON_REPORT = VILLANOVA_OAI_CONFIG.get("schematron_report")

VILLANOVA_XSLT_CONFIG = Variable.get("VILLANOVA_XSLT_CONFIG", deserialize_json=True)
# {
#   "xsl_repository": "tulibraries/other_mdx", <--- OPTIONAL
#   "xsl_branch": "my_test_branch", <--- OPTIONAL (DEFAULTS TO MASTER)
#   "xsl_filename": "transforms/my_test_transform.xml",
#   "schematron_filter": "validations/test_validation", <--- OPTIONAL
#   "schematron_report": "validations/test_validation", <--- OPTIONAL
# }
XSL_BRANCH = VILLANOVA_XSLT_CONFIG.get("xsl_branch", "master")
XSL_FILENAME = VILLANOVA_XSLT_CONFIG.get("xsl_filename", "transforms/villanova.xsl")
XSL_REPO = VILLANOVA_XSLT_CONFIG.get("xsl_repo", "tulibraries/aggregator_mdx")
XSL_SCHEMATRON_FILTER = VILLANOVA_XSLT_CONFIG.get("schematron_filter", None)
XSL_SCHEMATRON_REPORT = VILLANOVA_XSLT_CONFIG.get("schematron_report", None)

# Data Bucket Variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

#
# CREATE DAG
#

DEFAULT_ARGS = {
    "owner": "funcake",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 27),
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

DAG = DAG(
    dag_id="funcake_villanova_harvest",
    default_args=DEFAULT_ARGS,
    catchup=False,
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
    dag=DAG
)

HARVEST_OAI = PythonOperator(
    task_id='harvest_oai',
    provide_context=True,
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "all_sets": ALL_SETS,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "excluded_sets": EXCLUDED_SETS,
        "included_sets": INCLUDED_SETS,
        "metadata_prefix": MD_PREFIX,
        "oai_endpoint": OAI_ENDPOINT,
        "records_per_file": 10000,
        "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}"
    },
    dag=DAG
)

HARVEST_SCHEMATRON_REPORT = PythonOperator(
    task_id="harvest_schematron_report",
    provide_context=True,
    python_callable=validate.report_s3_schematron,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated-report",
        "record_parent_element": "{http://www.openarchives.org/OAI/2.0/oai_dc/}dc",
        "schematron_filename": OAI_SCHEMATRON_REPORT,
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated",
    },
    dag=DAG
)

HARVEST_ANALYSIS_REPORT = PythonOperator(
    task_id="harvest_analysis_report",
    provide_context=True,
    python_callable=validate.report_xml_analysis,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "record_parent_element": "{http://www.openarchives.org/OAI/2.0/oai_dc/}dc",
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated",
    },
    dag=DAG
)

HARVEST_FILTER = PythonOperator(
    task_id="harvest_filter",
    provide_context=True,
    python_callable=validate.filter_s3_schematron,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated-filtered",
        "record_parent_element": "{http://www.openarchives.org/OAI/2.0/oai_dc/}dc",
        "schematron_filename": OAI_SCHEMATRON_FILTER,
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated",
    },
    dag=DAG
)

XSLT_TRANSFORM = BashOperator(
    task_id="xslt_transform",
    bash_command="transform.sh ",
    env={
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated",
        "PATH": os.environ.get("PATH", "") + ":" + SCRIPTS_PATH,
        "XSL_BRANCH": XSL_BRANCH,
        "XSL_FILENAME": XSL_FILENAME,
        "XSL_REPO": XSL_REPO
        },
    dag=DAG
)

TRANSFORM_SCHEMATRON_REPORT = PythonOperator(
    task_id="transform_schematron_report",
    provide_context=True,
    python_callable=validate.report_s3_schematron,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed-report",
        "record_parent_element": "{http://www.openarchives.org/OAI/2.0/oai_dc/}dc",
        "schematron_filename": XSL_SCHEMATRON_REPORT,
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed",
    },
    dag=DAG
)

TRANSFORM_ANALYSIS_REPORT = PythonOperator(
    task_id="transform_analysis_report",
    provide_context=True,
    python_callable=validate.report_xml_analysis,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "record_parent_element": "{http://www.openarchives.org/OAI/2.0/oai_dc/}dc",
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed",
    },
    dag=DAG
)

TRANSFORM_FILTER = PythonOperator(
    task_id="transform_filter",
    provide_context=True,
    python_callable=validate.filter_s3_schematron,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed-filtered",
        "record_parent_element": "{http://www.openarchives.org/OAI/2.0/oai_dc/}dc",
        "schematron_filename": XSL_SCHEMATRON_FILTER,
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed",
    },
    dag=DAG
)

# PUBLISH = PythonOperator(
#     task_id="publish",
#     provide_context=True,
#     python_callable=validate.filter_s3_schematron,
#     op_kwargs={
#         "access_id": AIRFLOW_S3.login,
#         "access_secret": AIRFLOW_S3.password,
#         "bucket": AIRFLOW_DATA_BUCKET,
#         "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed-filtered",
#         "record_parent_element": "{http://www.openarchives.org/OAI/2.0/oai_dc/}dc",
#         "schematron_filename": XSL_SCHEMATRON_FILENAME,
#         "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed",
#     },
#     dag=DAG
# )

# NOTIFY_SLACK = PythonOperator(
#     task_id='slack_post_succ',
#     python_callable=slackpostonsuccess,
#     provide_context=True,
#     dag=DAG
# )

#
# CREATE TASKS DEPENDENCIES WITH DAG
#
# This sets the dependencies of Tasks within the DAG.
#

HARVEST_OAI.set_upstream(SET_COLLECTION_NAME)
HARVEST_SCHEMATRON_REPORT.set_upstream(HARVEST_OAI)
HARVEST_ANALYSIS_REPORT.set_upstream(HARVEST_OAI)
HARVEST_FILTER.set_upstream(HARVEST_SCHEMATRON_REPORT)
HARVEST_FILTER.set_upstream(HARVEST_ANALYSIS_REPORT)
XSLT_TRANSFORM.set_upstream(HARVEST_FILTER)
TRANSFORM_SCHEMATRON_REPORT.set_upstream(XSLT_TRANSFORM)
TRANSFORM_ANALYSIS_REPORT.set_upstream(XSLT_TRANSFORM)
TRANSFORM_FILTER.set_upstream(TRANSFORM_SCHEMATRON_REPORT)
TRANSFORM_FILTER.set_upstream(TRANSFORM_ANALYSIS_REPORT)
# NOTIFY_SLACK.set_upstream(TRANSFORM_FILTER)
# PUBLISH.set_upstream(TRANSFORM_FILTER)
