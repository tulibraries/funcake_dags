"""Unit Tests for the TUL Cob AZ Reindex DAG."""
import os
import unittest
import airflow
from funcake_dags.funcake_dev_index_dag import DAG as FCDAGDEV
from funcake_dags.funcake_prod_index_dag import DAG as FCDAGPROD

class TestFuncakeDevIndexDAG(unittest.TestCase):
    """Primary Class for Testing the FunCake Solr Index DAG."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, FCDAGDEV.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(FCDAGDEV.dag_id, "funcake_dev_index")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "harvest_oai",
            "create_collection",
            "combine_index",
            "solr_alias_swap",
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "create_collection": ["harvest_oai"],
            "combine_index": ["create_collection"],
            "solr_alias_swap": ["combine_index"],
        }

        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in FCDAGDEV.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)

    def test_combine_index_task(self):
        """Unit test that the DAG instance can find required solr indexing bash script."""
        task = FCDAGDEV.get_task("combine_index")
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        expected_bash_path = airflow_home + "/dags/funcake_dags/scripts/index.sh "
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["AIRFLOW_HOME"], os.getcwd())
        self.assertEqual(task.env["BUCKET"], "test-s3-bucket")
        self.assertEqual(task.env["FOLDER"], "funcake_dev_index/{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}/new-updated/")
        self.assertEqual(task.env["SOLR_URL"], "https://127.0.0.1/solr/funcake-0-{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}")
        self.assertEqual(task.env["SOLR_AUTH_USER"], "puppy")
        self.assertEqual(task.env["SOLR_AUTH_PASSWORD"], "chow")
        self.assertEqual(task.env["AWS_ACCESS_KEY_ID"], "elephants-key")
        self.assertEqual(task.env["AWS_SECRET_ACCESS_KEY"], "elephants-secret")


class TestFuncakeProdIndexDAG(unittest.TestCase):
    """Primary Class for Testing the FunCake Solr Index DAG."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, FCDAGPROD.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(FCDAGPROD.dag_id, "funcake_prod_index")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "harvest_oai",
            "create_collection",
            "combine_index",
            "solr_alias_swap",
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "create_collection": ["harvest_oai"],
            "combine_index": ["create_collection"],
            "solr_alias_swap": ["combine_index"],
        }

        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in FCDAGPROD.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)

    def test_combine_index_task(self):
        """Unit test that the DAG instance can find required solr indexing bash script."""
        task = FCDAGPROD.get_task("combine_index")
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        expected_bash_path = airflow_home + "/dags/funcake_dags/scripts/index.sh "
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["AIRFLOW_HOME"], os.getcwd())
        self.assertEqual(task.env["BUCKET"], "test-s3-bucket")
        self.assertEqual(task.env["FOLDER"], "funcake_prod_index/{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}/new-updated/")
        self.assertEqual(task.env["SOLR_URL"], "https://127.0.0.1/solr/funcake-0-{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}")
        self.assertEqual(task.env["SOLR_AUTH_USER"], "puppy")
        self.assertEqual(task.env["SOLR_AUTH_PASSWORD"], "chow")
        self.assertEqual(task.env["AWS_ACCESS_KEY_ID"], "elephants-key")
        self.assertEqual(task.env["AWS_SECRET_ACCESS_KEY"], "elephants-secret")
