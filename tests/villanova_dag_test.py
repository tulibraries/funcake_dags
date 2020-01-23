"""Unit Tests for Basic Validation of Villanova (and thus, all OAI) DAGs."""
import unittest
from funcake_dags.funcake_villanova_dag import DAG

class TestVillanovaDag(unittest.TestCase):
    """Class to test OAI-based Funcake Harvest DAGs."""
    def setUp(self):
        """Method to set up the DAG Class instance for testing."""
        self.tasks = list(map(lambda t: t.task_id, DAG.tasks))

    def test_basic_dag_smoke(self):
        """Assert expected DAG exists."""
        self.assertEqual(DAG.dag_id, "funcake_villanova")
