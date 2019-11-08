"""Controller DAG to trigger funcake_index_dag for developement environment:"""
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
CONFIGSET = Variable.get("FUNCAKE_DEV_CONFIGSET")

# Define the DAG
CONTROLLER_DAG = DAG(
    dag_id="trigger_dev_funcake_index_dag",
    default_args={
        "owner": "airflow",
        "start_date": datetime.utcnow(),
    },
    schedule_interval="@once",
)

# Define the single task in this controller DAG
DEV_TRIGGER = TriggerDagRunOperator(
    task_id="dev_trigger",
    trigger_dag_id="funcake_index",
    python_callable=conditionally_trigger,
    params={"condition_param": True,
            "message": "Triggering Dev Funcake Index DAG",
            "CONFIGSET": CONFIGSET,
            "env": "dev"
            },
    dag=CONTROLLER_DAG
)
