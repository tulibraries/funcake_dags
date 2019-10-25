"""DAG to Harvest PA Digital Aggregated OAI-PMH XML & Index to SolrCloud."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from tulflow import harvest, tasks


#
# LOCAL FUNCTIONS
#
# Functions / any code with processing logic should be elsewhere, tested, etc.
# This is where to put functions that haven't been abstracted out yet.
#
def slackpostonsuccess(dag, **context):
    """Task Method to Post Successful FunCake Blogs Sync DAG Completion on Slack."""

    task_instance = context.get('task_instance')

    msg = "%(date)s DAG %(dagid)s success: Combine Set %(set)s to Solr Index Complete %(url)s" % {
        "date": context.get('execution_date'),
        "dagid": task_instance.dag_id,
        "set": FUNCAKE_OAI_SET,
        "url": task_instance.log_url
        }

    return tasks.slackpostonsuccess(dag, msg).execute(context=context)

#
# INIT SYSTEMWIDE VARIABLES
#
# check for existence of systemwide variables shared across tasks that can be
# initialized here if not found (i.e. if this is a new installation) & defaults exist
#

# Get Solr URL & Collection Name for indexing info; error out if not entered
SOLR_CONN = BaseHook.get_connection("SOLRCLOUD")
CONFIGSET = Variable.get("FUNCAKE_DEV_CONFIGSET")
TIMESTAMP = "{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}"
COLLECTION = CONFIGSET + "-" + TIMESTAMP
ALIAS = CONFIGSET + "-dev"
if "://" in SOLR_CONN.host:
    SOLR_COLL_ENDPT = SOLR_CONN.host + ":" + str(SOLR_CONN.port) + "/solr/" + COLLECTION
else:
    SOLR_COLL_ENDPT = "http://" + SOLR_CONN.host + ":" + str(SOLR_CONN.port) + "/solr/" + COLLECTION

# Combine OAI Harvest Variables
FUNCAKE_OAI_ENDPT = Variable.get("FUNCAKE_OAI_ENDPT")
FUNCAKE_OAI_SET = Variable.get("FUNCAKE_OAI_SET")
FUNCAKE_MD_PREFIX = Variable.get("FUNCAKE_MD_PREFIX")
AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")
AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

# Indexing Script to Solr
AIRFLOW_HOME = Variable.get("AIRFLOW_HOME")
AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")
FUNCAKE_INDEX_BASH = AIRFLOW_HOME + "/dags/funcake_dags/scripts/index.sh "

#
# CREATE DAG
#

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 27),
    'on_failure_callback': tasks.execute_slackpostonfail,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

FCDAG = DAG(
    'dev_funcake_index',
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

HARVEST_OAI = PythonOperator(
    task_id='harvest_oai',
    provide_context=True,
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "oai_endpoint": FUNCAKE_OAI_ENDPT,
        "metadata_prefix": FUNCAKE_MD_PREFIX,
        "set": FUNCAKE_OAI_SET,
        "harvest_from_date": None,
        "harvest_until_date": None,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "records_per_file": 1000,
        "s3_conn": AIRFLOW_S3,
        "timestamp": TIMESTAMP,
        "process_args": {}
    },
    dag=FCDAG
)

#pylint: disable-msg=too-many-function-args
# this is ticketed for fix; either make class or dictionary for solr args
CREATE_COLLECTION = tasks.create_sc_collection(FCDAG, SOLR_CONN.conn_id, COLLECTION, "3", CONFIGSET)

COMBINE_INDEX = BashOperator(
    task_id='combine_index',
    bash_command=FUNCAKE_INDEX_BASH,
    env={
        "BUCKET": AIRFLOW_DATA_BUCKET,
        "FOLDER": FCDAG.dag_id + "/" + TIMESTAMP,
        "SOLR_URL": SOLR_COLL_ENDPT,
        "SOLR_AUTH_USER": SOLR_CONN.login,
        "SOLR_AUTH_PASSWORD": SOLR_CONN.password,
        "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
        "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
        "AIRFLOW_HOME": AIRFLOW_HOME,
        "AIRFLOW_USER_HOME": AIRFLOW_USER_HOME
    },
    dag=FCDAG
)

SOLR_ALIAS_SWAP = tasks.swap_sc_alias(FCDAG, SOLR_CONN.conn_id, COLLECTION, ALIAS)

NOTIFY_SLACK = PythonOperator(
    task_id='slack_post_succ',
    python_callable=slackpostonsuccess,
    provide_context=True,
    dag=FCDAG
)


#
# CREATE TASKS DEPENDENCIES WITH DAG
#
# This sets the dependencies of Tasks within the DAG.
#

CREATE_COLLECTION.set_upstream(HARVEST_OAI)
COMBINE_INDEX.set_upstream(HARVEST_OAI)
COMBINE_INDEX.set_upstream(CREATE_COLLECTION)
SOLR_ALIAS_SWAP.set_upstream(COMBINE_INDEX)
NOTIFY_SLACK.set_upstream(SOLR_ALIAS_SWAP)
