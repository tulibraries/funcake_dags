"""Unit Tests for the TUL Cob AZ Reindex DAG."""
import unittest
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
            "list_index_files",
            "combine_index",
            "solr_commit",
            "solr_alias_swap",
            "success",
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "create_collection": ["harvest_oai"],
            "list_index_files": ["create_collection"],
            "combine_index": ["list_index_files"],
            "solr_commit": ["combine_index"],
            "solr_alias_swap": ["solr_commit"],
            "success": ["solr_alias_swap"],
        }

        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in FCDAGDEV.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)

    def test_list_index_files_task(self):
        """Unit test that the DAG instance can list index files from S3."""
        task = FCDAGDEV.get_task("list_index_files")
        self.assertEqual(task.bucket, "{{ var.value.AIRFLOW_DATA_BUCKET }}")
        self.assertEqual(task.prefix, "funcake_dev_index/{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}/new-updated/")
        self.assertEqual(task.aws_conn_id, "AIRFLOW_S3")

    def test_combine_index_task(self):
        """Unit test that the DAG instance can find required solr indexing bash script."""
        task = FCDAGDEV.get_task("combine_index")
        expected_bash_path = "{{ var.value.AIRFLOW_HOME }}/dags/funcake_dags/scripts/index.sh "
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["AIRFLOW_HOME"], "{{ var.value.AIRFLOW_HOME }}")
        self.assertEqual(task.env["BUCKET"], "{{ var.value.AIRFLOW_DATA_BUCKET }}")
        self.assertEqual(task.env["DATA"], "{{ \"'\" ~ (ti.xcom_pull(task_ids='list_index_files') | tojson) ~ \"'\" }}")
        self.assertEqual(task.env["SOLR_URL"], "{{ conn.get('SOLRCLOUD-WRITER').host if '://' in conn.get('SOLRCLOUD-WRITER').host else 'https://' + conn.get('SOLRCLOUD-WRITER').host }}/solr/{{ var.json.FUNCAKE_SOLR_CONFIG.configset }}-{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}")
        self.assertEqual(task.env["SOLR_AUTH_USER"], "{{ conn.get('SOLRCLOUD-WRITER').login or '' }}")
        self.assertEqual(task.env["SOLR_AUTH_PASSWORD"], "{{ conn.get('SOLRCLOUD-WRITER').password or '' }}")
        self.assertEqual(task.env["AWS_ACCESS_KEY_ID"], "{{ conn.get('AIRFLOW_S3').login }}")
        self.assertEqual(task.env["AWS_SECRET_ACCESS_KEY"], "{{ conn.get('AIRFLOW_S3').password }}")

    def test_solr_commit_task(self):
        """Unit test that the DAG instance includes a final Solr commit."""
        task = FCDAGDEV.get_task("solr_commit")
        self.assertEqual(task.http_conn_id, "SOLRCLOUD-WRITER")
        self.assertEqual(task.method, "GET")
        self.assertEqual(task.endpoint, "/solr/{{ var.json.FUNCAKE_SOLR_CONFIG.configset }}-{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}/update?commit=true")


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
            "list_index_files",
            "combine_index",
            "solr_commit",
            "solr_alias_swap",
            "success",
            ])

    def test_dag_task_order(self):
        """Unit test that the DAG instance contains the expected dependencies."""
        expected_task_deps = {
            "create_collection": ["harvest_oai"],
            "list_index_files": ["create_collection"],
            "combine_index": ["list_index_files"],
            "solr_commit": ["combine_index"],
            "solr_alias_swap": ["solr_commit"],
            "success": ["solr_alias_swap"],
        }

        for task, upstream_tasks in expected_task_deps.items():
            upstream_list = [up_task.task_id for up_task in FCDAGPROD.get_task(task).upstream_list]
            self.assertCountEqual(upstream_tasks, upstream_list)

    def test_list_index_files_task(self):
        """Unit test that the DAG instance can list index files from S3."""
        task = FCDAGPROD.get_task("list_index_files")
        self.assertEqual(task.bucket, "{{ var.value.AIRFLOW_DATA_BUCKET }}")
        self.assertEqual(task.prefix, "funcake_prod_index/{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}/new-updated/")
        self.assertEqual(task.aws_conn_id, "AIRFLOW_S3")

    def test_combine_index_task(self):
        """Unit test that the DAG instance can find required solr indexing bash script."""
        task = FCDAGPROD.get_task("combine_index")
        expected_bash_path = "{{ var.value.AIRFLOW_HOME }}/dags/funcake_dags/scripts/index.sh "
        self.assertEqual(task.bash_command, expected_bash_path)
        self.assertEqual(task.env["AIRFLOW_HOME"], "{{ var.value.AIRFLOW_HOME }}")
        self.assertEqual(task.env["BUCKET"], "{{ var.value.AIRFLOW_DATA_BUCKET }}")
        self.assertEqual(task.env["DATA"], "{{ \"'\" ~ (ti.xcom_pull(task_ids='list_index_files') | tojson) ~ \"'\" }}")
        self.assertEqual(task.env["SOLR_URL"], "{{ conn.get('SOLRCLOUD-WRITER').host if '://' in conn.get('SOLRCLOUD-WRITER').host else 'https://' + conn.get('SOLRCLOUD-WRITER').host }}/solr/{{ var.json.FUNCAKE_SOLR_CONFIG.configset }}-{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}")
        self.assertEqual(task.env["SOLR_AUTH_USER"], "{{ conn.get('SOLRCLOUD-WRITER').login or '' }}")
        self.assertEqual(task.env["SOLR_AUTH_PASSWORD"], "{{ conn.get('SOLRCLOUD-WRITER').password or '' }}")
        self.assertEqual(task.env["AWS_ACCESS_KEY_ID"], "{{ conn.get('AIRFLOW_S3').login }}")
        self.assertEqual(task.env["AWS_SECRET_ACCESS_KEY"], "{{ conn.get('AIRFLOW_S3').password }}")

    def test_solr_commit_task(self):
        """Unit test that the DAG instance includes a final Solr commit."""
        task = FCDAGPROD.get_task("solr_commit")
        self.assertEqual(task.http_conn_id, "SOLRCLOUD-WRITER")
        self.assertEqual(task.method, "GET")
        self.assertEqual(task.endpoint, "/solr/{{ var.json.FUNCAKE_SOLR_CONFIG.configset }}-{{ logical_date.strftime('%Y-%m-%d_%H-%M-%S') }}/update?commit=true")
