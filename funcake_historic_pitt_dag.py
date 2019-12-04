""" DAG to harvest data from Historic Pittsburgh OAI endpoint"""
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
    """Task Method to Post Successful Historic Pittsburgh DAG Completion on Slack."""

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
OAI_CONFIG = Variable.get("HISTORIC_PITT_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "http://historicpittsburgh.org/oai2",
#   "md_prefix": "oai_dc",
#   "all_sets": "False", <--- OPTIONAL
#   "excluded_sets": [], <--- OPTIONAL
#   "included_sets": [pitt_collection.317, pitt_collection.36, pitt_collection.85, pitt_collection.151, pitt_collection.236, pitt_collection.237, pitt_collection.238, pitt_collection.239, pitt_collection.283, pitt_collection.289, pitt_collection.291, pitt_collection.287, pitt_collection.290, pitt_collection.318, pitt_collection.274, pitt_collection.155, pitt_collection.34, pitt_collection.45, pitt_collection.46, pitt_collection.47, pitt_collection.48, pitt_collection.51, pitt_collection.52, pitt_collection.54, pitt_collection.56, pitt_collection.60, pitt_collection.67, pitt_collection.76, pitt_collection.81, pitt_collection.87, pitt_collection.88, pitt_collection.96, pitt_collection.310, pitt_collection.311, pitt_collection.312, pitt_collection.313, pitt_collection.314, pitt_collection.315, pitt_collection.318, pitt_collection.296, pitt_collection.308, pitt_collection.162, pitt_collection.309, pitt_collection.285, pitt_collection.286, pitt_collection.161, pitt_collection.243, pitt_collection.154, pitt_collection.37, pitt_collection.40, pitt_collection.42, pitt_collection.59, pitt_collection.74, pitt_collection.86, pitt_collection.50, pitt_collection.241, pitt_collection.62, pitt_collection.64, pitt_collection.65, pitt_collection.70, pitt_collection.44, pitt_collection.150, pitt_collection.77, pitt_collection.79, pitt_collection.110, pitt_collection.24, pitt_collection.25, pitt_collection.240, pitt_collection.61, pitt_collection.69, pitt_collection.97, pitt_collection.204, pitt_collection.106, pitt_collection.15, pitt_collection.187, pitt_collection.186, pitt_collection.101, pitt_collection.18, pitt_collection.100, pitt_collection.102, pitt_collection.19, pitt_collection.104, pitt_collection.20, pitt_collection.17, pitt_collection.103, pitt_collection.175, pitt_collection.21, pitt_collection.22, pitt_collection.203, pitt_collection.23, pitt_collection.202, pitt_collection.185, pitt_collection.197, pitt_collection.49, pitt_collection.72, pitt_collection.107, pitt_collection.109, pitt_collection.14, pitt_collection.147, pitt_collection.148, pitt_collection.156, pitt_collection.176, pitt_collection.182, pitt_collection.226, pitt_collection.247, pitt_collection.254, pitt_collection.26, pitt_collection.27, pitt_collection.28, pitt_collection.29, pitt_collection.292, pitt_collection.293, pitt_collection.294, pitt_collection.30, pitt_collection.305, pitt_collection.31, pitt_collection.32, pitt_collection.33, pitt_collection.35, pitt_collection.38, pitt_collection.41, pitt_collection.43, pitt_collection.53, pitt_collection.55, pitt_collection.57, pitt_collection.58, pitt_collection.63, pitt_collection.66, pitt_collection.68, pitt_collection.71, pitt_collection.73, pitt_collection.75, pitt_collection.78, pitt_collection.80, pitt_collection.82, pitt_collection.83, pitt_collection.84, pitt_collection.89, pitt_collection.90, pitt_collection.91, pitt_collection.92, pitt_collection.93, pitt_collection.94, pitt_collection.95, pitt_collection.98, pitt_collection.99, pitt_collection.146, pitt_collection.157], <--- OPTIONAL
#   "schematron_filter": "validations/dcingest_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
# }
OAI_ENDPOINT = OAI_CONFIG.get("endpoint")
OAI_MD_PREFIX = OAI_CONFIG.get("md_prefix")
OAI_INCLUDED_SETS = OAI_CONFIG.get("included_sets", [])
OAI_EXCLUDED_SETS = OAI_CONFIG.get("excluded_sets", [])
OAI_ALL_SETS = OAI_CONFIG.get("excluded_sets", "False")
OAI_SCHEMATRON_FILTER = OAI_CONFIG.get("schematron_filter", "validations/dcingest_reqd_fields.sch")
OAI_SCHEMATRON_REPORT = OAI_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")

XSL_CONFIG = Variable.get("HISTORIC_PITT_XSL_CONFIG", default_var={}, deserialize_json=True)
#{
#   "schematron_filter": "validations/funcake_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
#   "xsl_branch": "master", <--- OPTIONAL
#   "xsl_filename": "transforms/historicpitt.xsl",
#   "xsl_repository": "tulibraries/aggregator_mdx", <--- OPTIONAL
# }
XSL_SCHEMATRON_FILTER = XSL_CONFIG.get("schematron_filter", "validations/funcake_reqd_fields.sch")
XSL_SCHEMATRON_REPORT = XSL_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")
XSL_BRANCH = XSL_CONFIG.get("xsl_branch", "master")
XSL_FILENAME = XSL_CONFIG.get("xsl_filename", "transforms/historicpitt.xsl")
XSL_REPO = XSL_CONFIG.get("xsl_repo", "tulibraries/aggregator_mdx")

# Airflow Data S3 Bucket Variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# Publication-related Solr URL, Configset, Alias
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
SOLR_CONFIGSET = Variable.get("HISTORIC_PITT_SOLR_CONFIGSET", default_var="funcake-oai-0")
TARGET_ALIAS_ENV = Variable.get("HISTORIC_PITT_TARGET_ALIAS_ENV", default_var="dev")

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
    dag_id="funcake_historic_pitt",
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
