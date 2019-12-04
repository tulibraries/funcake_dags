"""Generic DAG to Harvest OAI & Index to SolrCloud."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from tulflow import harvest, tasks, transform, validate
from airflow.models import DagRun

"""
LOCAL FUNCTIONS
Functions / any code with processing logic should be elsewhere, tested, etc.
This is where to put functions that haven't been abstracted out yet.
"""

"""
INIT SYSTEMWIDE VARIABLES
check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

AIRFLOW_APP_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")
SCRIPTS_PATH = AIRFLOW_APP_HOME + "/dags/funcake_dags/scripts"
SOURCE_DAG = "{{ dag_run.conf['source_dag'] }}"
START_DATE = "{{ dag_run.conf['start_date'] }}"

# OAI Harvest Variables
OAI_CONFIG = "{{ dag_run.conf['OAI_CONFIG'] }}"
OAI_ENDPOINT = "{{ dag_run.conf['OAI_ENDPOINT'] }}"
OAI_MD_PREFIX = "{{ dag_run.conf['OAI_MD_PREFIX'] }}"
OAI_INCLUDED_SETS = "{{ dag_run.conf['OAI_INCLUDED_SETS'] }}"
OAI_EXCLUDED_SETS = "{{ dag_run.conf['OAI_EXCLUDED_SETS'] }}"
OAI_ALL_SETS = "{{ dag_run.conf['OAI_ALL_SETS'] }}"
OAI_SCHEMATRON_FILTER = "{{ dag_run.conf['OAI_SCHEMATRON_FILTER'] }}"
OAI_SCHEMATRON_REPORT = "{{ dag_run.conf['OAI_SCHEMATRON_REPORT'] }}"

# XSL Config Variables
XSL_CONFIG = "{{ dag_run.conf['XSL_CONFIG'] }}"
XSL_SCHEMATRON_FILTER = "{{ dag_run.conf['XSL_SCHEMATRON_FILTER'] }}"
XSL_SCHEMATRON_REPORT = "{{ dag_run.conf['XSL_SCHEMATRON_REPORT'] }}"
XSL_BRANCH = "{{ dag_run.conf['XSL_BRANCH'] }}"
XSL_FILENAME = "{{ dag_run.conf['XSL_FILENAME'] }}"
XSL_REPO = "{{ dag_run.conf['XSL_REPO'] }}"

# Publication-related Solr URL, Configset, Alias
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
SOLR_CONFIGSET = Variable.get("SOLR_CONFIGSET", default_var="funcake-oai-0")
TARGET_ALIAS_ENV = Variable.get("TARGET_ALIAS_ENV", default_var="dev")

# Data Bucket Variables
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")
S3_PREFIX = SOURCE_DAG + "/" + START_DATE

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
    dag_id="funcake_generic_oai",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1
    schedule_interval=None
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

REMOTE_TRIGGER_MESSAGE = BashOperator(
    task_id="remote_trigger_message",
    bash_command='echo "Remote trigger: \'{{ dag_run.conf["message"] }}\'"',
    dag=DAG,
)

def require_dag_run(dag_run, **context):
    if not "message" in dag_run.conf:
        raise Exception("Dag run must be triggered using the controller DAG.")

REQUIRE_DAG_RUN = PythonOperator(
        task_id="require_dag_run",
        python_callable=require_dag_run,
        provide_context=True,
        dag=DAG,
        )

# SET_COLLECTION_NAME = PythonOperator(
#     task_id="set_collection_name",
#     python_callable=datetime.now().strftime,
#     op_args=["%Y-%m-%d_%H-%M-%S"],
#     dag=DAG
# )

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
        "timestamp": START_DATE
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
        "destination_prefix": S3_PREFIX + "/new-updated",
        "schematron_filename": OAI_SCHEMATRON_REPORT,
        "source_prefix": S3_PREFIX + "/new-updated/"
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
        "destination_prefix": S3_PREFIX + "/new-updated-filtered/",
        "schematron_filename": OAI_SCHEMATRON_FILTER,
        "source_prefix": S3_PREFIX + "/new-updated/",
        "report_prefix": S3_PREFIX + "/harvest_filter",
        "timestamp": START_DATE
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
        "destination_prefix": S3_PREFIX + "/transformed/",
        "source_prefix": S3_PREFIX + "/new-updated-filtered/",
        "timestamp": START_DATE,
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
        "destination_prefix": DAG.dag_id + "/transformed",
        "schematron_filename": XSL_SCHEMATRON_REPORT,
        "source_prefix": S3_PREFIX + "/transformed/"
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
        "destination_prefix": S3_PREFIX + "/transformed-filtered/",
        "schematron_filename": XSL_SCHEMATRON_FILTER,
        "source_prefix": S3_PREFIX + "/transformed/",
        "report_prefix": S3_PREFIX + "/transformed_filtered",
        "timestamp": START_DATE
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
        "FOLDER": S3_PREFIX + "/transformed-filtered/",
        "INDEXER": "oai_index",
        "FUNCAKE_OAI_SOLR_URL": tasks.get_solr_url(SOLR_CONN, SOLR_CONFIGSET + "-" + SOURCE_DAG + "-" + TARGET_ALIAS_ENV),
        "SOLR_AUTH_USER": SOLR_CONN.login or "",
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password or "",
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "AIRFLOW_USER_HOME": AIRFLOW_USER_HOME
    },
    dag=DAG
)

NOTIFY_SLACK = tasks.slackpostonsuccess(
    DAG,
    "%(date)s DAG %(dagid)s success" % {
        "date": START_DATE,
        "dagid": SOURCE_DAG
    }
)

# SET UP TASK DEPENDENCIES
REQUIRE_DAG_RUN.set_upstream(REMOTE_TRIGGER_MESSAGE)
# SET_COLLECTION_NAME.set_upstream(REQUIRE_DAG_RUN)
OAI_TO_S3.set_upstream(REQUIRE_DAG_RUN)
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
