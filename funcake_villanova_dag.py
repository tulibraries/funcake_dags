""" DAG to harvest data from Villanova University Digital Collections OAI endpoint"""
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

# OAI Harvest Variables
OAI_CONFIG = Variable.get("VILLANOVA_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "http://digital.library.villanova.edu/OAI/Server",
#   "md_prefix": "oai_dc",
#   "all_sets": "False", <--- OPTIONAL
#   "excluded_sets": [], <--- OPTIONAL
#   "included_sets": ["dpla"], <--- OPTIONAL
#   "schematron_filter": "validations/padigital_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
# }
OAI_ENDPOINT = OAI_CONFIG.get("endpoint")
OAI_MD_PREFIX = OAI_CONFIG.get("md_prefix")
OAI_INCLUDED_SETS = OAI_CONFIG.get("included_sets", [])
OAI_EXCLUDED_SETS = OAI_CONFIG.get("excluded_sets", [])
OAI_ALL_SETS = OAI_CONFIG.get("excluded_sets", "False")
OAI_SCHEMATRON_FILTER = OAI_CONFIG.get("schematron_filter", "validations/dcingest_reqd_fields.sch")
OAI_SCHEMATRON_REPORT = OAI_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")

XSL_CONFIG = Variable.get("VILLANOVA_XSL_CONFIG", default_var={}, deserialize_json=True)
#{
#   "schematron_filter": "validations/funcake_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
#   "xsl_branch": "master", <--- OPTIONAL
#   "xsl_filename": "transforms/dplah.xsl",
#   "xsl_repository": "tulibraries/aggregator_mdx", <--- OPTIONAL
# }
XSL_SCHEMATRON_FILTER = XSL_CONFIG.get("schematron_filter", "validations/funcake_reqd_fields.sch")
XSL_SCHEMATRON_REPORT = XSL_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")
XSL_BRANCH = XSL_CONFIG.get("xsl_branch", "master")
XSL_FILENAME = XSL_CONFIG.get("xsl_filename", "transforms/villanova.xsl")
XSL_REPO = XSL_CONFIG.get("xsl_repo", "tulibraries/aggregator_mdx")

# Airflow Data S3 Bucket Variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# Publication-related Solr URL, Configset, Alias
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
SOLR_CONFIGSET = Variable.get("VILLANOVA_SOLR_CONFIGSET", default_var="funcake-oai-0")
TARGET_ALIAS_ENV = Variable.get("VILLANOVA_TARGET_ALIAS_ENV", default_var="dev")

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
    task_id="set_collection_name",
    python_callable=datetime.now().strftime,
    op_args=["%Y-%m-%d_%H-%M-%S"],
    dag=DAG
)

OAI_TO_S3 = PythonOperator(
    task_id="harvest_oai",
    provide_context=True,
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "all_sets": OAI_ALL_SETS,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "excluded_sets": OAI_EXCLUDED_SETS,
        "included_sets": OAI_INCLUDED_SETS,
        "metadata_prefix": OAI_MD_PREFIX,
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
        "destination_prefix": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated",
        "schematron_filename": OAI_SCHEMATRON_REPORT,
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
        "schematron_filename": OAI_SCHEMATRON_FILTER,
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
    sc_coll_name=f"{SOLR_CONFIGSET}-{DAG.dag_id}-{TARGET_ALIAS_ENV}",
    sc_alias=f"{SOLR_CONFIGSET}-{TARGET_ALIAS_ENV}",
    configset=SOLR_CONFIGSET
)

PUBLISH = BashOperator(
    task_id="publish",
    bash_command=SCRIPTS_PATH + "/index.sh ",
    env={
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": DAG.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed-filtered/",
        "INDEXER": "oai_index",
        "FUNCAKE_OAI_SOLR_URL": tasks.get_solr_url(SOLR_CONN, SOLR_CONFIGSET + "-" + DAG.dag_id + "-" + TARGET_ALIAS_ENV),
        "SOLR_AUTH_USER": SOLR_CONN.login or "",
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password or "",
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "AIRFLOW_USER_HOME": AIRFLOW_USER_HOME
    },
    dag=DAG
)

NOTIFY_SLACK = PythonOperator(
    task_id="slack_post_succ",
    python_callable=slackpostonsuccess,
    provide_context=True,
    dag=DAG
)

# SET UP TASK DEPENDENCIES
OAI_TO_S3.set_upstream(SET_COLLECTION_NAME)
HARVEST_SCHEMATRON_REPORT.set_upstream(OAI_TO_S3)
HARVEST_FILTER.set_upstream(OAI_TO_S3)
XSL_TRANSFORM.set_upstream(HARVEST_SCHEMATRON_REPORT)
XSL_TRANSFORM.set_upstream(HARVEST_FILTER)
XSL_TRANSFORM_SCHEMATRON_REPORT.set_upstream(XSL_TRANSFORM)
XSL_TRANSFORM_FILTER.set_upstream(XSL_TRANSFORM)
REFRESH_COLLECTION_FOR_ALIAS.set_upstream(XSL_TRANSFORM_SCHEMATRON_REPORT)
REFRESH_COLLECTION_FOR_ALIAS.set_upstream(XSL_TRANSFORM_FILTER)
PUBLISH.set_upstream(REFRESH_COLLECTION_FOR_ALIAS)
NOTIFY_SLACK.set_upstream(PUBLISH)
