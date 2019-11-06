import os
import unittest
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models.taskinstance import TaskInstance

class TestStringMethods(unittest.TestCase):
    def test_trigger_dag(self):
        dag = DAG(
                dag_id="example_trigger_controller_dag",
                default_args={"owner": "airflow", "start_date": datetime.now()},
                schedule_interval="@once",
                )
        task = TriggerDagRunOperator(
                task_id="test_trigger_dagrun",
                trigger_dag_id="example_trigger_target_dag",
                conf={"message": """
                ***************Hello World ****************
                ***************Hello World ****************
                ***************Hello World ****************
                ***************Hello World ****************
                ***************Hello World ****************
                ***************Hello World ****************
                ***************Hello World ****************
                ***************Hello World ****************
                ***************Hello World ****************
                ***************Hello World ****************
                ***************Hello World ****************
                """},
                dag=dag,
                )

        ti = TaskInstance(task=task, execution_date=datetime.now())
        task.execute(ti.get_template_context())
