"""Controller DAG to trigger funcake_generic_csv for Free Library of Philadelphia."""
import pprint
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from tulflow.tasks import conditionally_trigger

"""
INIT SYSTEMWIDE VARIABLES
Check for existence of systemwide variables shared across tasks that can be
initialized here if not found (i.e. if this is a new installation)
"""

PP = pprint.PrettyPrinter(indent=4)

# CSV Harvest Variables
CSV_SCHEMATRON_FILTER = Variable.get("FREE_LIBRARY_CSV_SCHEMATRON_FILTER")
CSV_SCHEMATRON_REPORT = Variable.get("FREE_LIBRARY_CSV_SCHEMATRON_REPORT")
XSL_CONFIG = Variable.get("FREE_LIBRARY_XSL_CONFIG", default_var={}, deserialize_json=True)
#{
#   "schematron_filter": "validations/funcake_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
#   "xsl_branch": "master", <--- OPTIONAL
#   "xsl_filename": "transforms/dplah.xsl",
#   "xsl_repository": "tulibraries/aggregator_mdx", <--- OPTIONAL
# }
XSL_SCHEMATRON_FILTER = XSL_CONFIG.get("schematron_filter", "validations/padigital_reqd_fields.sch")
XSL_SCHEMATRON_REPORT = XSL_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")
XSL_BRANCH = XSL_CONFIG.get("xsl_branch", "master")
XSL_FILENAME = XSL_CONFIG.get("xsl_filename", "transforms/dplah.xsl")
XSL_REPO = XSL_CONFIG.get("xsl_repo", "tulibraries/aggregator_mdx")

# Define the DAG
DAG = DAG(
    dag_id="funcake_free_library_of_philadelphia",
    default_args={
        "owner": "dpla",
        "start_date": datetime.utcnow(),
    },
    schedule_interval="@once",
)

# Define the single task in this controller DAG
CSV_TRIGGER = TriggerDagRunOperator(
    task_id="csv_trigger",
    trigger_dag_id="funcake_generic_csv",
    python_callable=conditionally_trigger,
    params={"condition_param": True,
            "message": "Triggering Free Library CSV DAG",
            "CSV_SCHEMATRON_FILTER": CSV_SCHEMATRON_FILTER,
            "CSV_SCHEMATRON_REPORT": CSV_SCHEMATRON_REPORT,
            "DAGID": DAG.dag_id,
            "XSL_CONFIG": XSL_CONFIG,
            "XSL_SCHEMATRON_FILTER": XSL_SCHEMATRON_FILTER,
            "XSL_SCHEMATRON_REPORT": XSL_SCHEMATRON_REPORT,
            "XSL_BRANCH": XSL_BRANCH,
            "XSL_FILENAME": XSL_FILENAME,
            "XSL_REPO": XSL_REPO,
            },
    dag=DAG
)
