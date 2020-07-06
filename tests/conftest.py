""" PyTest Configuration file. """
import os
import subprocess
import airflow
import json

def pytest_sessionstart():
    """
    Allows plugins and conftest files to perform initial configuration.
    This hook is called for every plugin and initial conftest
    file after command line options have been parsed.
    """
    repo_dir = os.getcwd()
    subprocess.run("airflow initdb", shell=True)
    subprocess.run("mkdir -p dags/funcake_dags", shell=True)
    subprocess.run("mkdir -p data", shell=True)
    subprocess.run("mkdir -p logs", shell=True)
    subprocess.run("cp *.py dags/funcake_dags", shell=True)
    subprocess.run("cp -r scripts dags/funcake_dags", shell=True)
    airflow.models.Variable.set("AIRFLOW_HOME", repo_dir)
    airflow.models.Variable.set("AIRFLOW_USER_HOME", repo_dir)
    airflow.models.Variable.set("AIRFLOW_DATA_BUCKET", "test-s3-bucket")
    airflow.models.Variable.set("AIRFLOW_LOG_DIR", repo_dir + '/logs')
    airflow.models.Variable.set("FREE_LIBRARY_CSV_SCHEMATRON_FILTER", "validations/dcingest_reqd_fields.sch")
    airflow.models.Variable.set("FREE_LIBRARY_CSV_SCHEMATRON_REPORT", "validations/padigital_missing_thumbnailURL.sch")
    airflow.models.Variable.set("FREE_LIBRARY_XSL_CONFIG", {"xsl_branch": "main", "xsl_filename": "transforms/dplah.xsl", "xsl_repo": "tulibraries/aggregator_mdx", "schematron_filter": "validations/padigital_reqd_fields.sch", "schematron_report": "validations/padigital_missing_thumbnailURL.sch"}, serialize_json=True)
    airflow.models.Variable.set("FUNCAKE_DEV_CONFIGSET", "funcake-0")
    airflow.models.Variable.set("FUNCAKE_PROD_CONFIGSET", "funcake-0")
    airflow.models.Variable.set("FUNCAKE_OAI_CONFIG", {"endpoint": "http://localhost/oai", "include_sets": ["i_love_cats"], "exclude_sets": [], "md_prefix": "kittens"}, serialize_json=True)
    airflow.models.Variable.set("FUNCAKE_SOLR_CONFIG", {"configset": "funcake-0", "replication_factor": 1}, serialize_json=True)
    airflow.models.Variable.set("VILLANOVA_OAI_CONFIG", {"endpoint": "http://localhost/oai", "included_sets": ["i_love_cats"], "excluded_sets": [], "md_prefix": "kittens"}, serialize_json=True)
    airflow.models.Variable.set("VILLANOVA_XSL_CONFIG", {"xsl_branch": "main", "xsl_filename": "transforms/villanova.xsl", "xsl_repo": "tulibraries/aggregator_mdx", "schematron_filter": "validations/padigital_reqd_fields.sch", "schematron_report": "validations/padigital_missing_thumbnailURL.sch"}, serialize_json=True)
    airflow.models.Variable.set("VILLANOVA_SOLR_CONFIGSET", "funcake-oai-0")
    airflow.models.Variable.set("VILLANOVA_TARGET_ALIAS_ENV", "qa")

    solrcloud = airflow.models.Connection(
        conn_id="SOLRCLOUD",
        conn_type="http",
        host="127.0.0.1",
        port="8983",
        login="puppy",
        password="chow"
    )
    s3 = airflow.models.Connection(
        conn_id="AIRFLOW_S3",
        conn_type="AWS",
        login="elephants-key",
        password="elephants-secret"
    )
    slack = airflow.models.Connection(
        conn_id="AIRFLOW_CONN_SLACK_WEBHOOK",
        conn_type="http",
        host="127.0.0.1/services",
        port="",
    )
    airflow_session = airflow.settings.Session()
    airflow_session.add(solrcloud)
    airflow_session.add(s3)
    airflow_session.add(slack)
    airflow_session.commit()

def pytest_sessionfinish():
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    subprocess.run("rm -rf dags", shell=True)
    subprocess.run("rm -rf data", shell=True)
    subprocess.run("rm -rf logs", shell=True)
    subprocess.run("yes | airflow resetdb", shell=True)
