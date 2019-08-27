import airflow
from airflow import utils
from airflow import DAG
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from cob_datapipeline.task_slackpost import task_slackpostonsuccess, task_slackpostonfail
from cob_datapipeline.harvest_oai_combine import harvest_oai_combine




# fixme
try:
    collection_prefix = Variable.get("FUNCAKE_COLLECTION_PREFIX")
except Exception as ex:
    Variable.set("FUNCAKE_COLLECTION_PREFIX", "funcake_3")
    collection_prefix = Variable.get("FUNCAKE_COLLECTION_PREFIX")

#
# CREATE DAG
#
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 8, 27),
    'on_failure_callback': task_slackpostonfail,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'combine_index', default_args=default_args, catchup=False,
    max_active_runs=1, schedule_interval=None
)


#
# CREATE TASKS
#
# Tasks with all logic contained in a single operator can be declared here.
# Tasks with custom logic are relegated to individual Python files.
#
harvest_oai = PythonOperator(
    task_id='harvest_oai',
    provide_context=True,
    python_callable=harvest_oai_combine,
    op_kwargs={},
    dag=dag)

s3_upload = BashOperator(
    task_id='s3_upload',
    bash_command="{} {} {} {} {} ", #.format(ingest_command, parentsmarcfilepath,    childrenmarcfilepath, outfilepath) + ' ',
    dag=dag)

s3_download = BashOperator(
    task_id='s3_download',
    bash_command="{} {} {} {} {} ", #.format(ingest_command, parentsmarcfilepath,    childrenmarcfilepath, outfilepath) + ' ',
    dag=dag)


create_path = ''
create_collection = SimpleHttpOperator(
    task_id="create_collection",
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_CLOUD',
    endpoint=create_path,
    data={"": ""},
    xcom_push=True,
    headers={},
    dag=dag)

combine_index = BashOperator(
    task_id='combine_index',
    bash_command="{} {} {} {} {} ", #.format(ingest_command, parentsmarcfilepath,    childrenmarcfilepath, outfilepath) + ' ',
    dag=dag)

solr_alias_update = SimpleHttpOperator(
    task_id="solr_alias_update",
    method='GET',
    http_conn_id='AIRFLOW_CONN_SOLR_LEADER',
    endpoint=create_path,
    data={"stream.body": "<commit/>"},
    xcom_push=True,
    headers={},
    dag=dag)

post_slack = PythonOperator(
    task_id='slack_post_succ',
    python_callable=task_slackpostonsuccess,
    provide_context=True,
    dag=dag
)


# harvest_oai
s3_upload.set_upstream(harvest_oai)
create_collection.set_upstream(harvest_oai)
s3_download.set_upstream(s3_upload)
combine_index.set_upstream(s3_download)
combine_index.set_upstream(create_collection)
solr_alias_update.set_upstream(combine_index)
post_slack.set_upstream(solr_alias_update)
