"""DAG to Harvest PA Digital Aggregated OAI-PMH XML & Index to SolrCloud."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from tulflow import harvest, tasks
from airflow.providers.slack.notifications.slack import send_slack_notification

slackpostonsuccess = send_slack_notification(channel="aggregator", username="airflow", text=":partygritty: {{ execution_date }} DAG {{ dag.dag_id }} success: {{ ti.log_url }}")
slackpostonfail = send_slack_notification(channel="aggregator", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ execution_date }} {{ ti.log_url }}")

"""
INIT SYSTEMWIDE VARIABLES
check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation) & defaults exist
"""

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD-WRITER")
FUNCAKE_SOLR_CONFIG = Variable.get("FUNCAKE_SOLR_CONFIG", deserialize_json=True)
# {"configset": "funcake-8", "replication_factor": 4}
CONFIGSET = FUNCAKE_SOLR_CONFIG.get("configset")
REPLICATION_FACTOR = FUNCAKE_SOLR_CONFIG.get("replication_factor")
TIMESTAMP = "{{ data_interval_start.strftime('%Y-%m-%d_%H-%M-%S') }}"
COLLECTION = CONFIGSET + "-" + TIMESTAMP
ALIAS = CONFIGSET + "-prod"
if "://" in SOLR_CONN.host:
    SOLR_COLL_ENDPT = SOLR_CONN.host + "/solr/" + COLLECTION
else:
    SOLR_COLL_ENDPT = "https://" + SOLR_CONN.host + "/solr/" + COLLECTION

# Combine OAI Harvest Variables
FUNCAKE_OAI_CONFIG = Variable.get("FUNCAKE_OAI_CONFIG", deserialize_json=True)
FUNCAKE_OAI_ENDPT = FUNCAKE_OAI_CONFIG.get("endpoint")
FUNCAKE_OAI_SET = FUNCAKE_OAI_CONFIG.get("included_sets")
FUNCAKE_MD_PREFIX = FUNCAKE_OAI_CONFIG.get("md_prefix")
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# Indexing Script to Solr
AIRFLOW_APP_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")
FUNCAKE_INDEX_BASH = AIRFLOW_HOME + "/dags/funcake_dags/scripts/index.sh "

# Define the DAG
DEFAULT_ARGS = {
    "owner": "dpla",
    "depends_on_past": False,
    "start_date": datetime(2019, 8, 27),
    "on_failure_callback": [slackpostonfail],
    "on_success_callback": [slackpostonsuccess],
    "retries": 0,
    "retry_delay": timedelta(minutes=10),
}

DAG = DAG(
    "funcake_prod_index",
    default_args=DEFAULT_ARGS,
    catchup=False,
    max_active_runs=1,
    schedule=None
)

"""
CREATE TASKS
Tasks with all logic contained in a single operator can be declared here.
Tasks with custom logic are relegated to individual Python files.
"""

HARVEST_OAI = PythonOperator(
    task_id="harvest_oai",
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "oai_endpoint": FUNCAKE_OAI_ENDPT,
        "metadata_prefix": FUNCAKE_MD_PREFIX,
        "included_sets": FUNCAKE_OAI_SET,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "records_per_file": 1000,
        "access_id": AIRFLOW_S3.login,
        "access_secret": AIRFLOW_S3.password,
        "timestamp": TIMESTAMP,
    },
    dag=DAG
)

#pylint: disable-msg=too-many-function-args
# this is ticketed for fix; either make class or dictionary for solr args
CREATE_COLLECTION = tasks.create_sc_collection(
    DAG, SOLR_CONN.conn_id,
    COLLECTION,
    REPLICATION_FACTOR,
    CONFIGSET
)

COMBINE_INDEX = BashOperator(
    task_id="combine_index",
    bash_command=FUNCAKE_INDEX_BASH,
    env={
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": DAG.dag_id + "/" + TIMESTAMP + "/new-updated/",
        "INDEXER": "funnel_cake_index",
        "SOLR_URL": SOLR_COLL_ENDPT,
        "SOLR_AUTH_USER": SOLR_CONN.login or "",
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password or "",
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "AIRFLOW_HOME": AIRFLOW_HOME,
        "AIRFLOW_USER_HOME": AIRFLOW_USER_HOME,
        "AIRFLOW_APP_HOME": AIRFLOW_APP_HOME
    },
    dag=DAG
)

SOLR_ALIAS_SWAP = tasks.swap_sc_alias(DAG, SOLR_CONN.conn_id, COLLECTION, ALIAS)

# SET UP TASK DEPENDENCIES
CREATE_COLLECTION.set_upstream(HARVEST_OAI)
COMBINE_INDEX.set_upstream(CREATE_COLLECTION)
SOLR_ALIAS_SWAP.set_upstream(COMBINE_INDEX)