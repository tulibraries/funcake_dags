from funcake_dags.template import create_dag
from airflow import DAG # Required or airflow-webserver skips file.

"""
This file will automatically create a dag for every dag_id added to the dag_ids array below.

It will also assume the associated airflow variables are present in airflow. For example for the id 'westchester' the following associated airflow variables will be assumed to exist:

WESTCHESTER_HARVEST_CONFIG
WESTCHESTER_TARGET_ALIAS_ENV (can be prod, dev .. it's dev by default)

Example of Harvest Config value
{
    "endpoint": "http://digital.klnpa.org/oai/oai.php",
    "md_prefix": "oai_qdc",
    "all_sets": "False", <--- OPTIONAL
    "excluded_sets": [], <--- OPTIONAL
    "included_sets": ["aebye","ajt","amc", ...], <--- OPTIONAL
    "schematron_filter": "validations/dcingest_reqd_fields.sch",
    "schematron_report": "validations/padigital_missing_thumbnailURL.sch"
}
"""

# Note that ids will automatically be prefixed with 'funcake_' as a namespace.
# The dag_id for "westchester" will automatically be "funcake_westchester".

dag_ids = [
	"lasalle_cdm"
	]

for dag_id in dag_ids:
    dag = create_dag(dag_id)
    globals()[dag.dag_id] = dag
