# Funnel Cake DAGs

[![CircleCI](https://circleci.com/gh/tulibraries/funcake_dags.svg?style=svg)](https://circleci.com/gh/tulibraries/funcake_dags)
![pylint Score](https://mperlet.github.io/pybadge/badges/9.47.svg)

This is the repository for Funnel Cake (PA Digital / DPLA Data QA Interface) Airflow DAGs (Directed Acyclic Graphs, e.g., data processing workflows) for data indexing to Solr and related jobs. These DAGs are expecting to be run within an Airflow installation akin to the one built by our [TUL Airflow Playbook (private repository)](https://github.com/tulibraries/ansible-playbook-airflow).


## Prequisites

**Libraries & Packages**

- Python: Version as specified in `.python-version`, currently 3.6.8.
- Python Packages: see the [Pipfile](Pipfile)
- Docker 20.10+
- Docker-Compose: 1.27+

**Airflow Variables**

These variable are initially set in the `variables.json` file.

: `FUNCAKE_CONFIGSET`: The SolrCloud Configset identifier to use for creating new Funnel Cake Collections & updating Aliases. Based on the https://github.com/tulibraries/funcake-solr latest release
- `FUNCAKE_OAI_ENDPT`: The OAI Endpoint to be harvested for indexing.
- `FUNCAKE_OAI_SET`: The OAI set used for harvesting from the OAI Endpoint (above). If all sets wanted, set to "" (but you have to set).
- `FUNCAKE_MD_PREFIX`: The OAI metadata prefix used for harvesting from the OAI Endpoint (above). This is required, per OAI-PMH specifications.
- `AIRFLOW_DATA_BUCKET`: The AWS S3 Bucket label (label / name, not ARN or URI) the harvested OAI-PMH XML data is put into / indexed from.
- `AIRFLOW_HOME`: The Airflow system home directory path. Used here for locating our `scripts` repository when running the indexing bash script.

Other expected variables are listed in [variables.json](variables.json)

**Airflow Connections**
- `SOLRCLOUD`: An HTTP Connection used to connect to SolrCloud.
- `AIRFLOW_S3`: An AWS (not S3 with latest Airflow upgrade) Connection used to manage AWS credentials (which we use to interact with our Airflow Data S3 Bucket).


## Local Development

This project uses the UNIX `make` command to build, run, stop, configure, and test Funcake DAGS for local development. Steps to get started follow.

### Up and running

Clone https://github.com/tulibraries/ansible_playbook_airflow locally; in a shell at the top level of that repository, run `make up` then git clone/pull your working funcake_dags code to ansible_playbook_airflow/dags/funcake-dags. This shows & runs your development DAG code in the Airflow docker containers, with a webserver at http://localhost:8080.

Open a terminal and execute the following commands:

```
$ git clone https://github.com/tulibraries/funcake_dags.git
$ cd funcake_dags
$ make up
```

Wait for Airflow and associated servers to build and start up. This may take several minutes.

Open a browser and visit http://localhost:8010 to verify  Airflow server is running.

On the initial startup, the dashboard may display an empty or partial list of DAGs and the status at the top may show the Broken DAG error message indicating that a connection or variable. Create the connection and copy it's attributes from the [TUL Production Airflow Server Connections](http://localhost:8010/admin/connection/) or [TUL QA Airflow Server Connections](http://localhost:8010/admin/connection/).  Create the variable and copy it's value from the [TUL Production Airflow Server Variables](http://localhost:8010/admin/variable/) or [TUL QA Airflow Server Variables](http://localhost:8010/admin/variable/).

Specific Make targets and their descriptions are documented in [Airflow Docket Dev Setup](airflow-docker-dev-setup/README.md).


### Related Documentation and Projects

- [Airflow Docker Development Setup](https://github.com/tulibraries/airflow-docker-dev-setup)
- [Ansible Playbook Airflow](https://github.com/tulibraries/ansible-playbook-airflow)
- [Apache Airflow](https://airflow.apache.org/docs/)
- [CircleCI](https://circleci.com/docs/2.0/configuration-reference/)


## Linting & Testing

How to run the `pylint` linter on this repository:

```
# Ensure you have the correct Python & Pip running:
$ python --version
  Python 3.7.2
$ pip --version
  pip 18.1 from /home/tul08567/.pyenv/versions/3.7.2/lib/python3.7/site-packages/pip (python 3.7)
# Install Pipenv:
$ pip install pipenv
  Collecting pipenv ...
# Install requirements in Pipenv; requires envvar:
$ SLUGIFY_USES_TEXT_UNIDECODE=yes pipenv install --dev
  Pipfile.lock not found, creating ...
# Run the linter:
$ pipenv run pylint funcake_dags
  ...
```

Linting for Errors only (`pipenv run pylint cob_datapipeline -E`) is run by Travis on every PR.

How to run pytests (unit tests, largely) on this repository (run by Travis on every PR):

```
$ pipenv run pytest
```

## Deployment

CircleCI is used for continuous integration and deployment. details are in the CircleCI configuration file: [`cob_datapipeline/.circleci/config.yml`](https://github.com/tulibraries/cob_datapipeline/blob/main/.circleci/config.yml)

### Continiuous Integration (CI)

ingegration performed by CircleCI performs basic  pylint and pytest checks. These are run within a Pipenv shell to manage expected packages across all possibly environments.

### Continuous Deployment (CD)

The Funcake Airflow DAG is deployed by way of an [Airflow server Ansible playbook](tulibraries/ansible-playbook-airflow). This runs, pointing at the appropriate tulibraries/funcake_dags branch (qa or main) to clone & set up (run pip install based on the Pipenv files) within the relevant Airflow deployed environment.

Deployment to QA is triggered by a pull request.
Deployment to Productino is tribbered by issuing a release.

PRs merged to QA cause a QA environment deploy of tulibraries/ansible-playbook-airflow ansible playbook; PRs merged to main cause a Stage Environment deploy using that airflow playbook. PRs merged to main also queue up a Production Environment deploy, that waits for user input before running.

The idea is to eventually touch the airflow ansible playbook as little as possible, and have DAG changes occur here & deploy from here.
