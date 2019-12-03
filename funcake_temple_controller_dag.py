"""Controller DAG to trigger funcake_generic_oai_target_dag for Temple."""
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
OAI_CONFIG = Variable.get("TEMPLE_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "http://digital.library.temple.edu/oai/oai.php",
#   "md_prefix": "oai_qdc",
#   "all_sets": "False", <--- OPTIONAL
#   "excluded_sets": [], <--- OPTIONAL
#   "included_sets": [p15037coll1, p15037coll10, p15037coll14, p15037coll15, p15037coll17, p15037coll18, p15037coll19, p15037coll2, p15037coll3, p15037coll4, p15037coll5, p15037coll7, p16002coll1, p16002coll14, p16002coll16, p16002coll17, p16002coll19, p16002coll2, p16002coll24, p16002coll26, p16002coll28, p16002coll3, p16002coll31, p16002coll4, p16002coll5, p16002coll6, p16002coll7, p16002coll9, p245801coll0, p245801coll12, p245801coll13], <--- OPTIONAL
#   "schematron_filter": "validations/dcingest_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
# }

MD_PREFIX = OAI_CONFIG.get("md_prefix")
INCLUDED_SETS = OAI_CONFIG.get("included_sets")
OAI_ENDPOINT = OAI_CONFIG.get("endpoint")
EXCLUDED_SETS = OAI_CONFIG.get("excluded_sets", [])
ALL_SETS = OAI_CONFIG.get("excluded_sets", "False")
OAI_SCHEMATRON_FILTER = OAI_CONFIG.get("schematron_filter", "validations/dcingest_reqd_fields.sch")
OAI_SCHEMATRON_REPORT = OAI_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")

XSL_CONFIG = Variable.get("TEMPLE_XSLT_CONFIG", default_var={}, deserialize_json=True)
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
XSL_FILENAME = XSL_CONFIG.get("xsl_filename", "transforms/temple_p16002coll25.xsl")
XSL_REPO = XSL_CONFIG.get("xsl_repo", "tulibraries/aggregator_mdx")

# Define the DAG
CONTROLLER_DAG = DAG(
    dag_id="funcake_temple_controller_dag",
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
            "message": "Triggering Temple OAI DAG",
            "test": "test",
            "OAI_CONFIG": OAI_CONFIG,
            "MDX_PREFIX": MD_PREFIX,
            "INCLUDE_SETS": INCLUDED_SETS,
            "OAI_ENDPOINT": OAI_ENDPOINT,
            "EXCLUDE_SETS": EXCLUDED_SETS,
            "ALL_SETS": ALL_SETS,
            "OAI_SCHEMATRON_FILTER": OAI_SCHEMATRON_FILTER,
            "OAI_SCHEMATRON_REPORT": OAI_SCHEMATRON_REPORT,
            "XSL_CONFIG": XSL_CONFIG,
            "XSL_SCHEMATRON_FILTER": XSL_SCHEMATRON_FILTER,
            "XSL_SCHEMATRON_REPORT": XSL_SCHEMATRON_REPORT,
            "XSL_BRANCH": XSL_BRANCH,
            "XSL_FILENAME": XSL_FILENAME,
            "XSL_REPO": XSL_REPO,
            },
    dag=CONTROLLER_DAG
)
