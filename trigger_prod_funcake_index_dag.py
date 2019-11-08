"""Controller DAG to trigger prod_funcake_index_dag:"""

import pprint
from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.dagrun_operator import TriggerDagRunOperator
# from tulflow.tasks import conditionally_trigger

PP = pprint.PrettyPrinter(indent=4)
CONFIGSET = Variable.get("FUNCAKE_PROD_CONFIGSET")

def conditionally_trigger(context, dag_run_obj):
    """This function decides whether or not to Trigger the remote DAG"""
    c_p = context['params']['condition_param']
    print("Controller DAG : conditionally_trigger = {}".format(c_p))
    if context['params']['condition_param']:
        dag_run_obj.payload = {'message': context['params']['message']}
        pp.pprint(dag_run_obj.payload)
        return dag_run_obj

# Define the DAG
CONTROLLER_DAG = DAG(
    dag_id="trigger_prod_funcake_index_dag",
    default_args={
        "owner": "airflow",
        "start_date": datetime.utcnow(),
    },
    schedule_interval=None,
)

# Define the single task in this controller DAG
DEV_TRIGGER = TriggerDagRunOperator(
    task_id='prod_trigger',
    trigger_dag_id="funcake_index",
    python_callable=conditionally_trigger,
    params={'condition_param': True,
            'message': 'Triggering Prod Funcake Index DAG',
            'CONFIGSET': CONFIGSET,
            'env': 'prod'
            },
    dag=CONTROLLER_DAG
)
