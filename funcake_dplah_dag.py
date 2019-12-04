"""Controller DAG to trigger funcake_generic_oai_target_dag for DPLAH."""
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

# Combine OAI Harvest Variables
OAI_CONFIG = Variable.get("DPLAH_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "http://aggregator.padigital.org/oai",
#   "md_prefix": "oai_dc",
#   "all_sets": "True", <--- OPTIONAL
#   "excluded_sets": [], <--- OPTIONAL
#   "included_sets": [], <--- OPTIONAL
#   "schematron_filter": "validations/dcingest_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
# }
MD_PREFIX = OAI_CONFIG.get("md_prefix")
OAI_INCLUDED_SETS = OAI_CONFIG.get("included_sets")
OAI_ENDPOINT = OAI_CONFIG.get("endpoint")
OAI_EXCLUDED_SETS = OAI_CONFIG.get("excluded_sets", [])
OAI_ALL_SETS = OAI_CONFIG.get("excluded_sets", "False")
OAI_SCHEMATRON_FILTER = OAI_CONFIG.get("schematron_filter", "validations/dcingest_reqd_fields.sch")
OAI_SCHEMATRON_REPORT = OAI_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")

XSL_CONFIG = Variable.get("DPLAH_XSLT_CONFIG", default_var={}, deserialize_json=True)
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
XSL_FILENAME = XSL_CONFIG.get("xsl_filename", "transforms/dplah.xsl")
XSL_REPO = XSL_CONFIG.get("xsl_repo", "tulibraries/aggregator_mdx")

# Define the DAG
DAG = DAG(
    dag_id="funcake_dplah",
    default_args={
        "owner": "dpla",
        "start_date": datetime.utcnow(),
    },
    schedule_interval="@once",
)

# Define the single task in this controller DAG
OAI_TRIGGER = TriggerDagRunOperator(
    task_id="oai_trigger",
    trigger_dag_id="funcake_generic_oai",
    python_callable=conditionally_trigger,
    params={
        "condition_param": True,
        "message": "Triggering DPLAH OAI DAG",
        "source_dag": DAG.dag_id,
        "start_date": DAG.default_args["start_date"].strftime("%Y-%m-%d_%H-%M-%S"),
        "OAI_CONFIG": OAI_CONFIG,
        "OAI_MD_PREFIX": MD_PREFIX,
        "OAI_INCLUDED_SETS": OAI_INCLUDED_SETS,
        "OAI_ENDPOINT": OAI_ENDPOINT,
        "EXCLUDE_SETS": OAI_EXCLUDED_SETS,
        "OAI_ALL_SETS": OAI_ALL_SETS,
        "OAI_SCHEMATRON_FILTER": OAI_SCHEMATRON_FILTER,
        "OAI_SCHEMATRON_REPORT": OAI_SCHEMATRON_REPORT,
        "XSL_CONFIG": XSL_CONFIG,
        "XSL_SCHEMATRON_FILTER": XSL_SCHEMATRON_FILTER,
        "XSL_SCHEMATRON_REPORT": XSL_SCHEMATRON_REPORT,
        "XSL_BRANCH": XSL_BRANCH,
        "XSL_FILENAME": XSL_FILENAME,
        "XSL_REPO": XSL_REPO,
    },
    dag=DAG
)
