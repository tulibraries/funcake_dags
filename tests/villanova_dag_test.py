
import re
import unittest
import airflow
from funcake_dags.funcake_villanova_dag import DAG, VILLANOVA_TARGET_ALIAS_ENV

class TestVillanovaDag(unittest.TestCase):

    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_basic_dag_smoke(self):
        self.assertEqual(DAG.dag_id, "funcake_villanova_harvest")
        self.assertEqual(VILLANOVA_TARGET_ALIAS_ENV, "qa")
