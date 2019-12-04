
import re
import unittest
import airflow
try:
    from funcake_generic_csv_dag import DAG
except:
    from funcake_dags.funcake_generic_csv_dag import DAG

class TestGenericCsvDag(unittest.TestCase):

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_basic_dag_smoke(self):
        self.assertEqual(DAG.dag_id, "funcake_generic_csv")

    def test_csv_transform_to_s3_task(self):
        task = DAG.get_task("csv_transform")
        self.assertEqual(task.bash_command, "csv_transform_to_s3.sh ")
        self.assertEqual(task.env.get("BUCKET"), "test-s3-bucket")
        self.assertEqual(task.env.get("AWS_ACCESS_KEY_ID"), "elephants-key")
        self.assertEqual(task.env.get("AWS_SECRET_ACCESS_KEY"), "elephants-secret")
        self.assertEqual(task.env.get("FOLDER"), "funcake_free_library_of_philadelphia/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated")
        assert("dags/funcake_dags/scripts" in task.env.get("PATH"))
