""" PyTest Configuration file. """
import os
import subprocess
import airflow

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
    airflow.models.Variable.set("FUNCAKE_DEV_CONFIGSET", "funcake-0")
    airflow.models.Variable.set("FUNCAKE_PROD_CONFIGSET", "funcake-0")
    airflow.models.Variable.set("FUNCAKE_OAI_ENDPT", "http://localhost/oai")
    airflow.models.Variable.set("FUNCAKE_OAI_SET", "i_love_cats")
    airflow.models.Variable.set("FUNCAKE_MD_PREFIX", "kittens")

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
