import re
import unittest
import airflow
from funcake_dags.funcake_villanova_dag import DAG

class TestVillanovaDag(unittest.TestCase):

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_basic_dag_smoke(self):
        self.assertEqual(DAG.dag_id, "funcake_villanova")
