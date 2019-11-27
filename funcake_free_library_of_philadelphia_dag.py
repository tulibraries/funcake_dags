""" DAG to harvest data from Free Library of Philadelphia CSV files"""
import os
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from tulflow import harvest, tasks, transform, validate
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

    task_instance = context.get("task_instance")

    msg = "%(date)s DAG %(dagid)s success: %(url)s" % {
        "date": context.get('execution_date'),
        "dagid": task_instance.dag_id,
        "url": task_instance.log_url
    }

    return tasks.slackpostonsuccess(dag, msg).execute(context=context)

"""
INIT SYSTEMWIDE VARIABLES
check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_APP_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")
SCRIPTS_PATH = AIRFLOW_APP_HOME + "/dags/funcake_dags/scripts"

# CSV Harvest Variables
CSV_SCHEMATRON_FILTER = Variable.get("FREE_LIBRARY_CSV_SCHEMATRON_FILTER", "validations/dcingest_reqd_fields.sch")
CSV_SCHEMATRON_REPORT = Variable.get("FREE_LIBRARY_CSV_SCHEMATRON_REPORT", "validations/padigital_missing_thumbnailURL.sch")

FREE_LIBRARY_XSL_CONFIG = Variable.get("FREE_LIBRARY_XSL_CONFIG", default_var={}, deserialize_json=True)
#{
#   "schematron_filter": "validations/funcake_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
#   "xsl_branch": "master", <--- OPTIONAL
#   "xsl_filename": "transforms/dplah.xsl",
#   "xsl_repository": "tulibraries/aggregator_mdx", <--- OPTIONAL
# }
XSL_SCHEMATRON_FILTER = FREE_LIBRARY_XSL_CONFIG.get("schematron_filter", "validations/padigital_reqd_fields.sch")
XSL_SCHEMATRON_REPORT = FREE_LIBRARY_XSL_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")
XSL_BRANCH = FREE_LIBRARY_XSL_CONFIG.get("xsl_branch", "master")
XSL_FILENAME = FREE_LIBRARY_XSL_CONFIG.get("xsl_filename", "transforms/dplah.xsl")
XSL_REPO = FREE_LIBRARY_XSL_CONFIG.get("xsl_repo", "tulibraries/aggregator_mdx")

# Airflow Data S3 Bucket Variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# Publication-related Solr URL, Configset, Alias
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
FREE_LIBRARY_SOLR_CONFIGSET = Variable.get("FREE_LIBRARY_SOLR_CONFIGSET", "funcake-oai-1")
FREE_LIBRARY_TARGET_ALIAS_ENV = Variable.get("FREE_LIBRARY_TARGET_ALIAS_ENV", "qa")


# Define the DAG
DEFAULT_ARGS = {
    "owner": "dpla",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 27),
    "on_failure_callback": tasks.execute_slackpostonfail,
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

DAG = DAG(
    dag_id="funcake_free_library_of_philadelphia",
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

CSV_TRANSFORM = BashOperator(
    task_id="csv_transform",
    bash_command="csv_transform_to_s3.sh ",
    env={**os.environ, **{
        "PATH": os.environ.get("PATH", "") + ":" + SCRIPTS_PATH,
        "DAGID": "funcake_free_library_of_philadelphia",
        "HOME": AIRFLOW_USER_HOME,
        "AIRFLOW_APP_HOME": AIRFLOW_APP_HOME,
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated",
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "TIMESTAMP": "{{ ti.xcom_pull(task_ids='set_collection_name') }}"

        }},
    dag=DAG,
)

HARVEST_SCHEMATRON_REPORT = PythonOperator(
    task_id="harvest_schematron_report",
    provide_context=True,
    python_callable=validate.report_s3_schematron,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated",
        "schematron_filename": CSV_SCHEMATRON_REPORT,
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated/"
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
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated-filtered/",
        "schematron_filename": CSV_SCHEMATRON_FILTER,
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated/",
        "report_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/harvest_filter",
        "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}"
    },
    dag=DAG
)

XSL_TRANSFORM = PythonOperator(
    task_id="xsl_transform",
    provide_context=True,
    python_callable=transform.transform_s3_xsl,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed/",
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated-filtered/",
        "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}",
        "xsl_branch": XSL_BRANCH,
        "xsl_filename": XSL_FILENAME,
        "xsl_repository": XSL_REPO
    },
    dag=DAG
)

XSL_TRANSFORM_SCHEMATRON_REPORT = PythonOperator(
    task_id="xsl_transform_schematron_report",
    provide_context=True,
    python_callable=validate.report_s3_schematron,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed",
        "schematron_filename": XSL_SCHEMATRON_REPORT,
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed/"
    },
    dag=DAG
)

XSL_TRANSFORM_FILTER = PythonOperator(
    task_id="xsl_transform_filter",
    provide_context=True,
    python_callable=validate.filter_s3_schematron,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "bucket": AIRFLOW_DATA_BUCKET,
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed-filtered/",
        "schematron_filename": XSL_SCHEMATRON_FILTER,
        "source_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed/",
        "report_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed_filtered",
        "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}"
    },
    dag=DAG
)

REFRESH_COLLECTION_FOR_ALIAS = tasks.refresh_sc_collection_for_alias(
    DAG,
    sc_conn=SOLR_CONN,
    sc_coll_name=f"{FREE_LIBRARY_SOLR_CONFIGSET}-{DAG.dag_id}-{FREE_LIBRARY_TARGET_ALIAS_ENV}",
    sc_alias=f"{FREE_LIBRARY_SOLR_CONFIGSET}-{FREE_LIBRARY_TARGET_ALIAS_ENV}",
    configset=FREE_LIBRARY_SOLR_CONFIGSET
)

# PUBLISH = BashOperator(
#     task_id='combine_index',
#     bash_command=PUBLISH,
#     env={
#         "BUCKET": AIRFLOW_DATA_BUCKET,
#         "FOLDER": DAG.dag_id + "/" + TIMESTAMP,
#         "SOLR_URL": SOLR_COLL_ENDPT,
#         "SOLR_AUTH_USER": SOLR_CONN.login or "",
#         "SOLR_AUTH_PASSWORD": SOLR_CONN.password or "",
#         "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
#         "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
#         "AIRFLOW_HOME": AIRFLOW_USER_HOME,
#         "AIRFLOW_USER_HOME": AIRFLOW_USER_HOME
#     },
#     dag=FCDAG
# )
#
# SOLR_ALIAS_SWAP = tasks.swap_sc_alias(
#     DAG,
#     SOLR_CONN.conn_id,
#     COLLECTION,
#     ALIAS
# )

NOTIFY_SLACK = PythonOperator(
    task_id="slack_post_succ",
    python_callable=slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
CSV_TRANSFORM.set_upstream(SET_COLLECTION_NAME)
HARVEST_SCHEMATRON_REPORT.set_upstream(CSV_TRANSFORM)
HARVEST_FILTER.set_upstream(CSV_TRANSFORM)
XSL_TRANSFORM.set_upstream(HARVEST_SCHEMATRON_REPORT)
XSL_TRANSFORM.set_upstream(HARVEST_FILTER)
XSL_TRANSFORM_SCHEMATRON_REPORT.set_upstream(XSL_TRANSFORM)
XSL_TRANSFORM_FILTER.set_upstream(XSL_TRANSFORM)
REFRESH_COLLECTION_FOR_ALIAS.set_upstream(XSL_TRANSFORM_SCHEMATRON_REPORT)
REFRESH_COLLECTION_FOR_ALIAS.set_upstream(XSL_TRANSFORM_FILTER)
NOTIFY_SLACK.set_upstream(REFRESH_COLLECTION_FOR_ALIAS)
