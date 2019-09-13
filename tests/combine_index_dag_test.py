"""Unit Tests for the TUL Cob AZ Reindex DAG."""
import os
import unittest
import airflow
from combine_index_dag import FCDAG

class TestCombineIndexDAG(unittest.TestCase):
    """Primary Class for Testing the Combine to FunCake Solr Index DAG."""

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, FCDAG.tasks))

    def test_dag_loads(self):
        """Unit test that the DAG identifier is set correctly."""
        self.assertEqual(FCDAG.dag_id, "funcake_index")

    def test_dag_tasks_present(self):
        """Unit test that the DAG instance contains the expected tasks."""
        self.assertEqual(self.tasks, [
            "harvest_oai",
            "create_collection",
            "combine_index",
            "solr_alias_swap",
            "slack_post_succ"
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "create_collection": ["harvest_oai"],
            "combine_index": ["create_collection", "harvest_oai"],
            "solr_alias_swap": ["combine_index"],
            "slack_post_succ": ["solr_alias_swap"]
        }

        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in FCDAG.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)

    def test_combine_index_task(self):
        """Unit test that the DAG instance can find required solr indexing bash script."""
        task = FCDAG.get_task("combine_index")
        airflow_home = airflow.models.Variable.get("AIRFLOW_HOME")
        expected_bash_path = airflow_home + "/dags/funcake_dags/scripts/index.sh "
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["AIRFLOW_HOME"], os.getcwd())
        self.assertEqual(task.env["BUCKET"], "test-s3-bucket")
        self.assertEqual(task.env["FOLDER"], "funcake_index/{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}")
        self.assertEqual(task.env["SOLR_URL"], "http://127.0.0.1:8983/solr/funcake-0-{{ execution_date.strftime('%Y-%m-%d_%H-%M-%S') }}")
        self.assertEqual(task.env["SOLR_AUTH_USER"], "puppy")
        self.assertEqual(task.env["SOLR_AUTH_PASSWORD"], "chow")
        self.assertEqual(task.env["AWS_ACCESS_KEY_ID"], "elephants-key")
        self.assertEqual(task.env["AWS_SECRET_ACCESS_KEY"], "elephants-secret")
