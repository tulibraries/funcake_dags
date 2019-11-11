""" DAG to harvest data from Villanova University Digital Collections OAI endpoint"""
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from tulflow import harvest, tasks


#
# INIT SYSTEMWIDE VARIABLES
#
# check for existence of systemwide variables shared across tasks that can be
# initialized here if not found (i.e. if this is a new installation) & defaults exist
#


# Combine OAI Harvest Variables
VILLANOVA_OAI_CONFIG = Variable.get("VILLANOVA_OAI_CONFIG")
# {
#   "endpoint": "http://digital.library.villanova.edu/OAI/Server",
#   "include_sets": ["dpla"],
#   "exclude_sets": [],
#   "md_prefix": "oai_dc"
# }

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


SET_COLLECTION_NAME = PythonOperator(
    task_id='set_collection_name',
    python_callable=datetime.now().strftime,
    op_args=["%Y-%m-%d_%H-%M-%S"],
    dag=VILLANOVA_HARVEST_DAG
)

#TIMESTAMP = "{{ ti.xcom_pull(task_ids='set_collection_name') }}"

OAI_TO_S3 = PythonOperator(
    task_id='harvest_oai',
    provide_context=True,
    python_callable=harvest.oai_to_s3,
    op_kwargs={
        "harvest_args": VILLANOVA_OAI_CONFIG,
        "bucket_name": AIRFLOW_DATA_BUCKET,
        "records_per_file": 1000,
        "s3_conn": AIRFLOW_S3,
        "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}",
        "process_args": {}
    },
    dag=VILLANOVA_HARVEST_DAG
)

# S3 -> XSLT Transform -> s3
# S3 -> Schematron -> s3



VILLANOVA_HARVEST_DAG << SET_COLLECTION_NAME << OAI_TO_S3
