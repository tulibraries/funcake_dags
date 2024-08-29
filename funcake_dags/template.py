"""DAG Template for DPLA OAI & Index ("Publish") to SolrCloud."""
from datetime import datetime, timedelta
import os
from tulflow import harvest, tasks, transform, validate
from tulflow.solr_api_utils import SolrApiUtils
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from funcake_dags.lib.field_counter import field_count_report
from airflow.providers.slack.notifications.slack import send_slack_notification

slackpostonsuccess = send_slack_notification(channel="aggregator", username="airflow", text=":partygritty: {{ execution_date }} DAG {{ dag.dag_id }} success: {{ ti.log_url }}")
slackpostonfail = send_slack_notification(channel="aggregator", username="airflow", text=":poop: Task failed: {{ dag.dag_id }} {{ ti.task_id }} {{ execution_date }} {{ ti.log_url }}")

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

AIRFLOW_S3 = BaseHook.get_connection("AIRFLOW_S3")

AIRFLOW_DATA_BUCKET = Variable.get("AIRFLOW_DATA_BUCKET")

AIRFLOW_APP_HOME = Variable.get("AIRFLOW_HOME")

AIRFLOW_USER_HOME = Variable.get("AIRFLOW_USER_HOME")

SCRIPTS_PATH = AIRFLOW_APP_HOME + "/dags/funcake_dags/scripts"

SOLR_CONN = BaseHook.get_connection("SOLRCLOUD-WRITER")

SOLR_CONFIGSET = Variable.get("FUNCAKE_OAI_SOLR_CONFIGSET", default_var="funcake-oai-0")


def namespace(dag_id):
    if dag_id[0:8] != "funcake_":
        dag_id = "funcake_" + dag_id
    return dag_id


def unnamespace(dag_id):
    if dag_id[0:8] == "funcake_":
        dag_id = dag_id[8:]
    return dag_id


def name(dag_id):
    return unnamespace(dag_id).upper()


def get_harvest_task(dag, config):
    if config.get("endpoint") == None:
        return BashOperator(
                task_id="harvest_aggregator_data",
                bash_command="aggregator_data_transform_to_s3.sh ",
                env={**os.environ, **{
                    "PATH": os.environ.get("PATH", "") + ":" + SCRIPTS_PATH,
                    "DAGID": dag.dag_id,
                    "HOME": AIRFLOW_USER_HOME,
                    "AIRFLOW_APP_HOME": AIRFLOW_APP_HOME,
                    "BUCKET": AIRFLOW_DATA_BUCKET,
                    "FOLDER": dag.dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated",
                    "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
                    "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
                    "TIMESTAMP": "{{ ti.xcom_pull(task_ids='set_collection_name') }}"
                    }},
                dag=dag)

    else:
        return PythonOperator(
                task_id="harvest_oai",
                python_callable=harvest.oai_to_s3,
                op_kwargs={
                    "access_id": AIRFLOW_S3.login,
                    "access_secret": AIRFLOW_S3.password,
                    "all_sets": config.get("all_sets", False),
                    "bucket_name": AIRFLOW_DATA_BUCKET,
                    "excluded_sets": config.get("excluded_sets", []),
                    "included_sets": config.get("included_sets"),
                    "metadata_prefix": config.get("md_prefix"),
                    "oai_endpoint": config.get("endpoint"),
                    "records_per_file": 10000,
                    "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}"
                    },
                dag=dag)

def create_dag(dag_id):
    dag_id = namespace(dag_id)
    dag = DAG(
            dag_id=dag_id,
            default_args=DEFAULT_ARGS,
            catchup=False,
            max_active_runs=1,
            schedule=None)

    config_name = name(dag_id) + "_HARVEST_CONFIG"
    config = Variable.get(config_name, deserialize_json=True)

    SCHEMATRON_FILTER = config.get("schematron_filter", "validations/qdcingest_reqd_fields.sch")

    SCHEMATRON_REPORT = config.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")

    SCHEMATRON_XSL_FILTER = config.get("schematron_xsl_filter", "validations/padigital_reqd_fields.sch")

    SCHEMATRON_XSL_REPORT = config.get("schematron_xsl_report", "validations/padigital_missing_thumbnailURL.sch")

    XSL_REPO = config.get("xsl_repo", "tulibraries/aggregator_mdx")

    XSL_BRANCH = config.get("xsl_branch", "main")

    XSL_FILENAME = config.get("xsl_filename")

    target_alias_env_name = name(dag_id) + "_TARGET_ALIAS_ENV"
    TARGET_ALIAS_ENV = Variable.get(target_alias_env_name, default_var="dev")

    """
    CREATE TASKS
    Tasks with all logic contained in a single operator can be declared here.
    Tasks with custom logic are relegated to individual Python files.
    """
    with dag:
        SET_COLLECTION_NAME = BashOperator(
            task_id="set_collection_name",
            bash_command='echo ' + "{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}",
            dag=dag
        )

        HARVEST = get_harvest_task(dag, config)

        HARVEST_FIELD_COUNT_REPORT = PythonOperator(
            task_id="harvest_field_count_report",
            python_callable=field_count_report,
            op_kwargs={
                "source_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated/",
                "bucket": AIRFLOW_DATA_BUCKET,
            },
            dag=dag)

        HARVEST_SCHEMATRON_REPORT = PythonOperator(
            task_id="harvest_schematron_report",
            python_callable=validate.report_s3_schematron,
            op_kwargs={
                "access_id": AIRFLOW_S3.login,
                "access_secret": AIRFLOW_S3.password,
                "bucket": AIRFLOW_DATA_BUCKET,
                "destination_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated",
                "schematron_filename": SCHEMATRON_REPORT,
                "source_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated/"
            },
            dag=dag)

        HARVEST_FILTER = PythonOperator(
            task_id="harvest_filter",
            python_callable=validate.filter_s3_schematron,
            op_kwargs={
                "access_id": AIRFLOW_S3.login,
                "access_secret": AIRFLOW_S3.password,
                "bucket": AIRFLOW_DATA_BUCKET,
                "destination_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated-filtered/",
                "report_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/harvest_filter",
                "schematron_filename": SCHEMATRON_FILTER,
                "source_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated/",
                "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}"
            },
            dag=dag)

        XSL_TRANSFORM = BashOperator(
            task_id="xsl_transform",
            bash_command=SCRIPTS_PATH + "/transform.sh ",
            env={**os.environ, **{
                "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
                "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
                "BUCKET": AIRFLOW_DATA_BUCKET,
                "DAG_ID": dag_id,
                "DAG_TS": "{{ ti.xcom_pull(task_ids='set_collection_name') }}",
                "DEST": "transformed",
                "SOURCE": "new-updated-filtered",
                "SCRIPTS_PATH": SCRIPTS_PATH,
                "XSL_BRANCH": XSL_BRANCH,
                "XSL_FILENAME": XSL_FILENAME,
                "XSL_REPO": XSL_REPO,
            }},
            dag=dag)

        XSL_TRANSFORM_SCHEMATRON_REPORT = PythonOperator(
            task_id="xsl_transform_schematron_report",
            python_callable=validate.report_s3_schematron,
            op_kwargs={
                "access_id": AIRFLOW_S3.login,
                "access_secret": AIRFLOW_S3.password,
                "bucket": AIRFLOW_DATA_BUCKET,
                "destination_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed",
                "schematron_filename": SCHEMATRON_XSL_REPORT,
                "source_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed/"
            },
            dag=dag)

        XSL_TRANSFORM_FILTER = PythonOperator(
            task_id="xsl_transform_filter",
            python_callable=validate.filter_s3_schematron,
            op_kwargs={
                "access_id": AIRFLOW_S3.login,
                "access_secret": AIRFLOW_S3.password,
                "bucket": AIRFLOW_DATA_BUCKET,
                "destination_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed-filtered/",
                "schematron_filename": SCHEMATRON_XSL_FILTER,
                "source_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed/",
                "report_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed_filtered",
                "timestamp": "{{ ti.xcom_pull(task_ids='set_collection_name') }}"
            },
            dag=dag)

        TRANSFORM_FIELD_COUNT_REPORT = PythonOperator(
            task_id="transform_field_count_report",
            python_callable=field_count_report,
            op_kwargs={
                "source_prefix": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed-filtered/",
                "bucket": AIRFLOW_DATA_BUCKET,
            },
            dag=dag)

        REFRESH_COLLECTION_FOR_ALIAS = tasks.refresh_sc_collection_for_alias(
            sc_conn=SOLR_CONN,
            sc_coll_name=f"{SOLR_CONFIGSET}-{dag_id}-{TARGET_ALIAS_ENV}",
            sc_alias=f"{SOLR_CONFIGSET}-{TARGET_ALIAS_ENV}",
            configset=SOLR_CONFIGSET,
            dag=dag)

        PUBLISH = BashOperator(
            task_id="publish",
            bash_command=SCRIPTS_PATH + "/index.sh ",
            env={**os.environ, **{
                "BUCKET": AIRFLOW_DATA_BUCKET,
                "FOLDER": dag_id + "/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed-filtered/",
                "INDEXER": "oai_index",
                "FUNCAKE_OAI_SOLR_URL": tasks.get_solr_url(SOLR_CONN, SOLR_CONFIGSET + "-" + dag_id + "-" + TARGET_ALIAS_ENV),
                "SOLR_AUTH_USER": SOLR_CONN.login or "",
                "SOLR_AUTH_PASSWORD": SOLR_CONN.password or "",
                "AWS_ACCESS_KEY_ID": AIRFLOW_S3.login,
                "AWS_SECRET_ACCESS_KEY": AIRFLOW_S3.password,
                "AIRFLOW_USER_HOME": AIRFLOW_USER_HOME
            }},
            dag=dag)

        def validate_alias(conn, collection, alias, **args):
            login = conn.login
            password = conn.password
            url = conn.host + (f":{conn.port}" if conn.port else "")
            sc = SolrApiUtils(url, auth_user=login, auth_pass=password)

            if not sc.is_collection_in_alias(collection, alias):
                raise ValueError("Expected collection " + collection + "to be in alias (" + alias + "). But not found.");

        VALIDATE_ALIAS = PythonOperator(
            task_id="validate_alias",
            python_callable=validate_alias,
            op_kwargs= {
                "conn": SOLR_CONN,
                "collection": f"{SOLR_CONFIGSET}-{dag_id}-{TARGET_ALIAS_ENV}",
                "alias": f"{SOLR_CONFIGSET}-{TARGET_ALIAS_ENV}"
                },
            dag=dag)

        # SET UP TASK DEPENDENCIES
        HARVEST.set_upstream(SET_COLLECTION_NAME)

        HARVEST_FIELD_COUNT_REPORT.set_upstream(HARVEST)

        HARVEST_SCHEMATRON_REPORT.set_upstream(HARVEST)

        HARVEST_FILTER.set_upstream(HARVEST)

        XSL_TRANSFORM.set_upstream(HARVEST_SCHEMATRON_REPORT)

        XSL_TRANSFORM.set_upstream(HARVEST_FILTER)

        XSL_TRANSFORM_SCHEMATRON_REPORT.set_upstream(XSL_TRANSFORM)

        XSL_TRANSFORM_FILTER.set_upstream(XSL_TRANSFORM)

        TRANSFORM_FIELD_COUNT_REPORT.set_upstream(XSL_TRANSFORM_FILTER)

        REFRESH_COLLECTION_FOR_ALIAS.set_upstream(XSL_TRANSFORM_SCHEMATRON_REPORT)

        REFRESH_COLLECTION_FOR_ALIAS.set_upstream(XSL_TRANSFORM_FILTER)

        PUBLISH.set_upstream(REFRESH_COLLECTION_FOR_ALIAS)

        VALIDATE_ALIAS.set_upstream(PUBLISH)
    return dag
