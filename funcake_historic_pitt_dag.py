"""Controller DAG to trigger funcake_generic_oai_target_dag for Historic Pittsburgh."""
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
OAI_CONFIG = Variable.get("HISTORIC_PITT_OAI_CONFIG", deserialize_json=True)
# {
#   "endpoint": "http://historicpittsburgh.org/oai2",
#   "md_prefix": "oai_dc",
#   "all_sets": "False", <--- OPTIONAL
#   "excluded_sets": [], <--- OPTIONAL
#   "included_sets": [pitt_collection.317, pitt_collection.36, pitt_collection.85, pitt_collection.151, pitt_collection.236, pitt_collection.237, pitt_collection.238, pitt_collection.239, pitt_collection.283, pitt_collection.289, pitt_collection.291, pitt_collection.287, pitt_collection.290, pitt_collection.318, pitt_collection.274, pitt_collection.155, pitt_collection.34, pitt_collection.45, pitt_collection.46, pitt_collection.47, pitt_collection.48, pitt_collection.51, pitt_collection.52, pitt_collection.54, pitt_collection.56, pitt_collection.60, pitt_collection.67, pitt_collection.76, pitt_collection.81, pitt_collection.87, pitt_collection.88, pitt_collection.96, pitt_collection.310, pitt_collection.311, pitt_collection.312, pitt_collection.313, pitt_collection.314, pitt_collection.315, pitt_collection.318, pitt_collection.296, pitt_collection.308, pitt_collection.162, pitt_collection.309, pitt_collection.285, pitt_collection.286, pitt_collection.161, pitt_collection.243, pitt_collection.154, pitt_collection.37, pitt_collection.40, pitt_collection.42, pitt_collection.59, pitt_collection.74, pitt_collection.86, pitt_collection.50, pitt_collection.241, pitt_collection.62, pitt_collection.64, pitt_collection.65, pitt_collection.70, pitt_collection.44, pitt_collection.150, pitt_collection.77, pitt_collection.79, pitt_collection.110, pitt_collection.24, pitt_collection.25, pitt_collection.240, pitt_collection.61, pitt_collection.69, pitt_collection.97, pitt_collection.204, pitt_collection.106, pitt_collection.15, pitt_collection.187, pitt_collection.186, pitt_collection.101, pitt_collection.18, pitt_collection.100, pitt_collection.102, pitt_collection.19, pitt_collection.104, pitt_collection.20, pitt_collection.17, pitt_collection.103, pitt_collection.175, pitt_collection.21, pitt_collection.22, pitt_collection.203, pitt_collection.23, pitt_collection.202, pitt_collection.185, pitt_collection.197, pitt_collection.49, pitt_collection.72, pitt_collection.107, pitt_collection.109, pitt_collection.14, pitt_collection.147, pitt_collection.148, pitt_collection.156, pitt_collection.176, pitt_collection.182, pitt_collection.226, pitt_collection.247, pitt_collection.254, pitt_collection.26, pitt_collection.27, pitt_collection.28, pitt_collection.29, pitt_collection.292, pitt_collection.293, pitt_collection.294, pitt_collection.30, pitt_collection.305, pitt_collection.31, pitt_collection.32, pitt_collection.33, pitt_collection.35, pitt_collection.38, pitt_collection.41, pitt_collection.43, pitt_collection.53, pitt_collection.55, pitt_collection.57, pitt_collection.58, pitt_collection.63, pitt_collection.66, pitt_collection.68, pitt_collection.71, pitt_collection.73, pitt_collection.75, pitt_collection.78, pitt_collection.80, pitt_collection.82, pitt_collection.83, pitt_collection.84, pitt_collection.89, pitt_collection.90, pitt_collection.91, pitt_collection.92, pitt_collection.93, pitt_collection.94, pitt_collection.95, pitt_collection.98, pitt_collection.99, pitt_collection.146, pitt_collection.157], <--- OPTIONAL
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

XSL_CONFIG = Variable.get("HISTORIC_PITT_XSL_CONFIG", default_var={}, deserialize_json=True)
#{
#   "schematron_filter": "validations/funcake_reqd_fields.sch",
#   "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
#   "xsl_branch": "master", <--- OPTIONAL
#   "xsl_filename": "transforms/historicpitt.xsl",
#   "xsl_repository": "tulibraries/aggregator_mdx", <--- OPTIONAL
# }
XSL_SCHEMATRON_FILTER = XSL_CONFIG.get("schematron_filter", "validations/funcake_reqd_fields.sch")
XSL_SCHEMATRON_REPORT = XSL_CONFIG.get("schematron_report", "validations/padigital_missing_thumbnailURL.sch")
XSL_BRANCH = XSL_CONFIG.get("xsl_branch", "master")
XSL_FILENAME = XSL_CONFIG.get("xsl_filename", "transforms/historicpitt.xsl")
XSL_REPO = XSL_CONFIG.get("xsl_repo", "tulibraries/aggregator_mdx")

# Define the DAG
DAG = DAG(
    dag_id="funcake_historic_pitt",
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
            "message": "Triggering Historic Pittsburgh OAI DAG",
            "test": "test",
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
