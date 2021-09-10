# Funnel Cake DAGs

[![CircleCI](https://circleci.com/gh/tulibraries/funcake_dags.svg?style=svg)](https://circleci.com/gh/tulibraries/funcake_dags)
![pylint Score](https://mperlet.github.io/pybadge/badges/9.47.svg)

This is the repository for Funnel Cake (PA Digital / DPLA Data QA Interface) Airflow DAGs (Directed Acyclic Graphs, e.g., data processing workflows) for data indexing to Solr and related jobs. These DAGs are expecting to be run within an Airflow installation akin to the one built by our [TUL Airflow Playbook (private repository)](https://github.com/tulibraries/ansible-playbook-airflow).


## Prequisites

**Libraries & Packages**

- Python: Version as specified in `.python-version`.
- Pip: 18.1+
- Pipenv: 2020-11-15+
- Python Package Dependencies: see the [Pipfile](Pipfile)
- Docker 20.10+
- Docker-Compose: 1.27+

**Airflow Variables**
These variable are initially set in the `variables.json` file.  Variables for this project are primarily handled by the PA Digital team.  They will be the ones to add or update variables as needed.
Variables are listed in [variables.json](variables.json)

**Airflow Connections**
- `SOLRCLOUD`: An HTTP Connection used to connect to SolrCloud.
- `AIRFLOW_S3`: An AWS (not S3 with latest Airflow upgrade) Connection used to manage AWS credentials (which we use to interact with our Airflow Data S3 Bucket).
- `AIRFLOW_CONN_SLACK_WEBHOOK`: Slack failure notifications 
- `FUNCAKE_SLACK_WEBHOOK`: Slack success notifications

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

On initial startup, the dashboard may display an empty or partial list of DAGs and the status at the top may show the Broken DAG error message indicating that a connection or variable is missing. Create the connection and copy it's attributes from the [TUL Production Airflow Server Connections](http://localhost:8010/admin/connection/) or [TUL QA Airflow Server Connections](http://localhost:8010/admin/connection/).  Create the variable and copy it's value from the [TUL Production Airflow Server Variables](http://localhost:8010/admin/variable/) or [TUL QA Airflow Server Variables](http://localhost:8010/admin/variable/).

Specific Make targets and their descriptions are documented in [Airflow Docket Dev Setup](airflow-docker-dev-setup/README.md).


### Related Documentation and Projects

- [Airflow Docker Development Setup](https://github.com/tulibraries/airflow-docker-dev-setup)
- [Ansible Playbook Airflow](https://github.com/tulibraries/ansible-playbook-airflow)
- [Apache Airflow](https://airflow.apache.org/docs/)
- [CircleCI](https://circleci.com/docs/2.0/configuration-reference/)


## Linting & Testing

Perform syntax and style checks on airflow code with `pylint`

To install and configure `pylint`
```
$ pip install pipenv
$ SLUGIFY_USES_TEXT_UNIDECODE=yes pipenv install --dev
```

To `lint` the DAGs
```
$ pipenv run pylint funcake_dags
```

Use `pytest` to run unit and functional tests on this project.

```
$ pipenv run pytest
```

`lint` and `pytest` are run automatically by CircleCI on each pull request.


You may also test using `airflow-docker-dev-setup/Makefile`

First, ensure `airflow-docker-dev-setup` submodule installation

```
$ git submodule update --init --recursive
```

```
$ make -f airflow-docker-dev-setup/Makefile test
```

## Deployment

CircleCI checks (lints and tests) code and deploys to the QA and Prod servers when development branches are merged into the `main` branch. See the [CircleCI configuration file](cob_datapipeline/.circleci/config.yml) for details.
