# PA Digital Aggregator DAGs

[![CircleCI](https://circleci.com/gh/tulibraries/funcake_dags.svg?style=svg)](https://circleci.com/gh/tulibraries/funcake_dags)
![pylint Score](https://mperlet.github.io/pybadge/badges/9.47.svg)

This repository contains files related to Airflow DAGs (Directed Acyclic Graphs, e.g. data processing workflows) used for PA Digital aggregation processes.

There are two types of Airflow DAGs generated from this repository:
- **DAGs for PA Digital contributing institutions**: DAGs for each institution are generated using DAG template files, `funcake_dags/template.py` and `funcake_dags/template_dag.py`. Each DAG is customized using Airflow variables. These variables are maintained in `variables.json` and manually loaded into Airflow. Based on these template files and variables, each DAG executes a workflow that harvests, validates, transforms, and publishes metadata to an individual Solr collection, which can be accessed via a shared dev or prod OAI-PMH endpoint (based on the *target alias env* defined in variables). Related validation and transformation files can be found in [aggregator_mdx](https://github.com/tulibraries/aggregator_mdx).
- **DAGs for Funnel Cake site**: `funcake_dags/funcake_prod_index_dag.py` and `funcake_dags/funcake_dev_index_dag.py` index metadata accessible via the dev OAI-PMH endpoint into a single SolrCloud collection to be used with the Blacklight application, [Funnel Cake](https://github.com/tulibraries/funnel_cake)(for the prod and dev instances respectively).

Some DAG tasks in this repository use Temple University Libraries' centralized python library [tulflow](https://github.com/tulibraries/tulflow).

These DAGs are expecting to be run within an Airflow installation akin to the one built by our [TUL Airflow Playbook (private repository)](https://github.com/tulibraries/ansible-playbook-airflow).

## Prerequisites

**Libraries & Packages**

- Python: Version as specified in `.python-version`.
- Python Package Dependencies: see the [Pipfile](Pipfile)
- Docker 20.10+
- Docker-Compose: 1.27+

**Airflow Variables**

These variables are initially set in the `variables.json` file.  Variables for this project are primarily handled by the PA Digital team.  They will be the ones to add or update variables as needed.
Variables are listed in [variables.json](variables.json).

**Airflow Connections**
- `SOLRCLOUD`: An HTTP Connection used to connect to SolrCloud.
- `AIRFLOW_S3`: An AWS (not S3 with latest Airflow upgrade) Connection used to manage AWS credentials (which we use to interact with our Airflow Data S3 Bucket).
- `slack_api_default`: Used to report DAG run successes and failures to our internal slack channels.

## Local Development
Local development relies on the  [Airflow Docker Dev Setup submodule](https://github.com/tulibraries/airflow-docker-dev-setup).

This project uses the UNIX `make` command to build, run, stop, configure, and test DAGS for local development. These commands first run a script to change into the submodule directory that is used for the development setup. See the [Makefile](Makefile) for the complete list of commands available.
 

On initial startup, the dashboard may display an empty or partial list of DAGs and the status at the top may show the Broken DAG error message indicating that a connection or variable is missing. Create the connection and copy it's attributes from the [TUL Production Airflow Server Connections](http://localhost:8010/admin/connection/) or [TUL QA Airflow Server Connections](http://localhost:8010/admin/connection/).  Create the variable and copy it's value from the [TUL Production Airflow Server Variables](http://localhost:8010/admin/variable/) or [TUL QA Airflow Server Variables](http://localhost:8010/admin/variable/).

### Related Documentation and Projects

- [Airflow Docker Development Setup](https://github.com/tulibraries/airflow-docker-dev-setup)
- [Ansible Playbook Airflow](https://github.com/tulibraries/ansible-playbook-airflow)
- [Apache Airflow](https://airflow.apache.org/docs/)
- [CircleCI](https://circleci.com/docs/2.0/configuration-reference/)
- [tulflow](https://github.com/tulibraries/tulflow)

### Make Commands

#### rebuld-pipfile:
Used to rebuild Pipfile while making sure that airflow-constraints are met.

#### compare-dependencies:
Used to automatically check if a new dependency does not match upstream airflow contraints.



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

QA: CircleCI checks (lints and tests) code and deploys to the QA server when development branches are merged into the `main` branch. 

Production: When a development branch is merged into `main` it creates a request_prod_deploy job.  To deploy code to the production server, you will need to go to the circleCI User Interface and approve the pending job. See the [CircleCI configuration file](cob_datapipeline/.circleci/config.yml) for details.
