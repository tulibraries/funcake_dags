import airflow
from funcake_dags.template import create_dag
import unittest
from datetime import datetime, timedelta

AIRFLOW_APP_HOME = airflow.models.Variable.get("AIRFLOW_HOME")

SCRIPTS_PATH = AIRFLOW_APP_HOME + "/dags/funcake_dags/scripts"

class TestTemplate(unittest.TestCase):
    def setUp(self):
        airflow.models.Variable.set("FOO_HARVEST_CONFIG", {
            "xsl_branch": "main",
            "xsl_filename": "transforms/dplah.xsl",
            "xsl_repo": "tulibraries/aggregator_mdx",
            "schematron_filter": "validations/padigital_reqd_fields.sch",
            "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
            "endpoint": "foobar",
            }, serialize_json=True)

        self.dag = create_dag("funcake_foo")

    def test_create_dag(self):
        """Assert expected DAG exists."""
        self.assertEqual(self.dag.dag_id, "funcake_foo")

    def test_set_collection_name_task(self):
        task = self.dag.get_task("set_collection_name")
        self.assertIn(task.python_callable.__qualname__, "datetime.strftime")

    def test_harvest_oai_task(self):
        task = self.dag.get_task("harvest_oai")
        self.assertEqual(task.op_kwargs["bucket_name"], "test-s3-bucket")
        self.assertEqual(task.op_kwargs["oai_endpoint"], "foobar")

    def test_harvest_schematron_report_task(self):
        task = self.dag.get_task("harvest_schematron_report")
        self.assertEqual(task.op_kwargs["bucket"], "test-s3-bucket")
        self.assertEqual(task.op_kwargs["destination_prefix"], "funcake_foo/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated")
        self.assertEqual(task.op_kwargs["source_prefix"], "funcake_foo/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated/")

    def test_harves_filter_task(self):
        task = self.dag.get_task("harvest_filter")
        self.assertEqual(task.op_kwargs["bucket"], "test-s3-bucket")
        self.assertEqual(task.op_kwargs["schematron_filename"], "validations/padigital_reqd_fields.sch")
        self.assertEqual(task.op_kwargs["destination_prefix"], "funcake_foo/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated-filtered/")
        self.assertEqual(task.op_kwargs["report_prefix"], "funcake_foo/{{ ti.xcom_pull(task_ids='set_collection_name') }}/harvest_filter")
        self.assertEqual(task.op_kwargs["source_prefix"], "funcake_foo/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated/")

    def test_xsl_transform_task(self):
        task = self.dag.get_task("xsl_transform")
        self.assertEqual(task.bash_command, SCRIPTS_PATH + "/transform.sh " )
        self.assertEqual(task.env["XSL_BRANCH"], "main" )

    def test_xsl_transform_schematron_report_task(self):
        task = self.dag.get_task("xsl_transform_schematron_report")
        self.assertEqual(task.op_kwargs["schematron_filename"], "validations/padigital_missing_thumbnailURL.sch")
        self.assertEqual(task.op_kwargs["destination_prefix"], "funcake_foo/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed")
        self.assertEqual(task.op_kwargs["source_prefix"], "funcake_foo/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed/")

    def test_xsl_transform_filter_task(self):
        task = self.dag.get_task("xsl_transform_filter")
        self.assertEqual(task.op_kwargs["schematron_filename"], "validations/padigital_reqd_fields.sch")
        self.assertEqual(task.op_kwargs["destination_prefix"], "funcake_foo/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed-filtered/")
        self.assertEqual(task.op_kwargs["source_prefix"], "funcake_foo/{{ ti.xcom_pull(task_ids='set_collection_name') }}/transformed/")

    def test_refresh_alias_task(self):
        task = self.dag.get_task("refresh_sc_collection_for_alias")
        self.assertEqual(task.op_kwargs["alias"], "funcake-oai-0-dev")
        self.assertEqual(task.op_kwargs["collection"], "funcake-oai-0-funcake_foo-dev")

    def test_pulish_task(self):
        task = self.dag.get_task("publish")
        self.assertEqual(task.bash_command, SCRIPTS_PATH + "/index.sh " )
        self.assertEqual(task.env["FUNCAKE_OAI_SOLR_URL"], "http://127.0.0.1:8983/solr/funcake-oai-0-funcake_foo-dev" )

    def test_validate_alias_task(self):
        task = self.dag.get_task("validate_alias")
        self.assertEqual(task.op_kwargs["alias"], "funcake-oai-0-dev")
        self.assertEqual(task.op_kwargs["collection"], "funcake-oai-0-funcake_foo-dev")

    def test_success_slack_trigger__task(self):
        task = self.dag.get_task("success_slack_trigger")
        self.assertIn(task.python_callable.__qualname__, "slackpostonsuccess")

    def test_all_task_are_linked_to_something(self):
        for task in self.dag.tasks:
            self.assertTrue(task.upstream_list != [] or task.downstream_list != [], "Expect all tasks to be linked to eachother.")

    def test_naspace_works(self):
        dag = create_dag("foo")
        self.assertEqual(dag.dag_id, "funcake_foo")

    def test_harve_csv_task_used_when_no_endpoint_in_config(self):
        airflow.models.Variable.set("FOO_HARVEST_CONFIG", {
            "schematron_filter": "validations/padigital_reqd_fields.sch",
            "schematron_report": "validations/padigital_missing_thumbnailURL.sch",
            }, serialize_json=True)
        dag = create_dag("foo")
        task = dag.get_task("harvest_aggregator_data")
        self.assertEqual(task.env["FOLDER"], "funcake_foo/{{ ti.xcom_pull(task_ids='set_collection_name') }}/new-updated")
