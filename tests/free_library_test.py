"""Unit Tests for Basic Validation of Free Library (and thus, all CSV) DAGs."""
import unittest
from funcake_dags.funcake_free_library_of_philadelphia_dag import DAG

class TestGenericCsvDag(unittest.TestCase):
    """Class to test CSV-based Funcake Harvest DAGs."""
    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_basic_dag_smoke(self):
        """Assert expected DAG exists."""
        self.assertEqual(DAG.dag_id, "funcake_free_library_of_philadelphia")

    def test_csv_transform_to_s3_task(self):
        """Test CSV Harvest & Transform to XML task within DAG."""
        task = DAG.get_task("csv_transform")
        self.assertEqual(task.bash_command, "csv_transform_to_s3.sh ")
        self.assertEqual(task.env.get("BUCKET"), "test-s3-bucket")
        self.assertEqual(task.env.get("AWS_ACCESS_KEY_ID"), "elephants-key")
        self.assertEqual(task.env.get("AWS_SECRET_ACCESS_KEY"), "elephants-secret")
        self.assertEqual(task.env.get("FOLDER"), "funcake_free_library_of_philadelphia/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated")
        assert("dags/funcake_dags/scripts" in task.env.get("PATH"))
