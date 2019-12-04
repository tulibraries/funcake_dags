"""Controller DAG to trigger funcake_generic_oai for Villanova."""
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
OAI_CONFIG = Variable.get("VILLANOVA_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "http://digital.library.villanova.edu/OAI/Server",
#   "included_sets": ["dpla"],
#   "excluded_sets": [], <--- OPTIONAL
#   "md_prefix": "oai_dc",
#   "xsl_branch": "my_test_branch", <--- OPTIONAL
#   "xsl_filename": "transforms/my_test_transform.xml", <--- OPTIONAL
#   "schematron_filename": "validations/test_validation", <--- OPTIONAL
# }
MDX_PREFIX   = OAI_CONFIG.get("md_prefix")
INCLUDE_SETS = OAI_CONFIG.get("included_sets")
OAI_ENDPOINT = OAI_CONFIG.get("endpoint")
EXCLUDE_SETS = OAI_CONFIG.get("excluded_sets", [])
ALL_SETS = OAI_CONFIG.get("excluded_sets", "False")
OAI_SCHEMATRON_FILTER = OAI_CONFIG.get("schematron_filter", "validations/dcingest_reqd_fields.sch")
OAI_SCHEMATRON_REPORT = OAI_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")

XSL_CONFIG = Variable.get("VILLANOVA_XSLT_CONFIG", default_var={}, deserialize_json=True)
#{
#   "schematron_filter": "validations/funcake_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
#   "xsl_branch": "master", <--- OPTIONAL
#   "xsl_filename": "transforms/dplah.xsl",
#   "xsl_repository": "tulibraries/aggregator_mdx", <--- OPTIONAL
# }
XSL_SCHEMATRON_FILTER = XSL_CONFIG.get("schematron_filter", "validations/funcake_reqd_fields.sch")
XSL_SCHEMATRON_REPORT = XSL_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")
XSL_BRANCH   = XSL_CONFIG.get("xsl_branch", "master")
XSL_FILENAME = XSL_CONFIG.get("xsl_filename", "transforms/villanova.xsl")
XSL_REPO     = XSL_CONFIG.get("xsl_repo", "tulibraries/aggregator_mdx")
SCHEMATRON_FILENAME = XSL_CONFIG.get("schematron_filename", "validations/padigital_reqd_fields.sch")

# Define the DAG
DAG = DAG(
    dag_id="funcake_villanova",
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
    params={"condition_param": True,
            "message": "Triggering Villanova OAI DAG",
            "OAI_CONFIG": OAI_CONFIG,
            "MDX_PREFIX": MDX_PREFIX,
            "INCLUDE_SETS": INCLUDE_SETS,
            "OAI_ENDPOINT": OAI_ENDPOINT,
            "EXCLUDE_SETS": EXCLUDE_SETS,
            "ALL_SETS": ALL_SETS,
            "OAI_SCHEMATRON_FILTER": OAI_SCHEMATRON_FILTER,
            "OAI_SCHEMATRON_REPORT": OAI_SCHEMATRON_REPORT,
            "XSL_CONFIG": XSL_CONFIG,
            "XSL_SCHEMATRON_FILTER": XSL_SCHEMATRON_FILTER,
            "XSL_SCHEMATRON_REPORT": XSL_SCHEMATRON_REPORT,
            "XSL_BRANCH": XSL_BRANCH,
            "XSL_FILENAME": XSL_FILENAME,
            "XSL_REPO": XSL_REPO,
            "SCHEMATRON_FILENAME": SCHEMATRON_FILENAME
            },
    dag=DAG
)
